use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Duration;

use criterion::async_executor::AsyncExecutor;
use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use smol::LocalExecutor;
use trustworthiness_checker::benches_common::{
    monitor_outputs_untyped_dataflow_limited, monitor_outputs_untyped_semisync_limited,
};
use trustworthiness_checker::io::map;
use trustworthiness_checker::{InputStream, Value};

#[global_allocator]
static GLOBAL: __ALLOCATOR__::Jemalloc = __ALLOCATOR__::Jemalloc;

#[derive(Clone)]
struct LocalSmolExecutor {
    executor: Rc<LocalExecutor<'static>>,
}

impl LocalSmolExecutor {
    fn new() -> Self {
        Self {
            executor: Rc::new(LocalExecutor::new()),
        }
    }
}

impl AsyncExecutor for LocalSmolExecutor {
    fn block_on<T>(&self, future: impl Future<Output = T>) -> T {
        smol::block_on(self.executor.run(future))
    }
}

const WIDTH: usize = 32;
const COMPONENT_WIDTH: usize = 8;
const RECONFIGURATION_PERIOD: usize = 64;

#[derive(Clone, Copy)]
enum Variant {
    AutomaticScope,
    ExplicitComponents,
}

impl Variant {
    fn name(self) -> &'static str {
        match self {
            Self::AutomaticScope => "automatic_scope",
            Self::ExplicitComponents => "explicit_components",
        }
    }

    fn size(self) -> usize {
        match self {
            Self::AutomaticScope => 1_024,
            Self::ExplicitComponents => 1_024,
        }
    }

    fn reverse_provider(self, index: usize) -> usize {
        match self {
            Self::AutomaticScope => WIDTH - index - 1,
            Self::ExplicitComponents => {
                let component_start = index / COMPONENT_WIDTH * COMPONENT_WIDTH;
                component_start + COMPONENT_WIDTH - index % COMPONENT_WIDTH - 1
            }
        }
    }
}

fn specification_source(variant: Variant) -> String {
    let mut source = String::new();
    for index in 0..WIDTH {
        source.push_str(&format!("in x{index}: Int\n"));
        source.push_str(&format!("in defer_source{index}: Str\n"));
        source.push_str(&format!("in dynamic_source{index}: Str\n"));
    }
    match variant {
        Variant::AutomaticScope => {
            source.push_str("out result: Int\n");
            let terms = (0..WIDTH)
                .map(|index| format!("defer(defer_source{index}: Int)"))
                .chain((0..WIDTH).map(|index| format!("dynamic(dynamic_source{index}: Int)")))
                .collect::<Vec<_>>();
            source.push_str(&format!("result = {}\n", terms.join(" + ")));
        }
        Variant::ExplicitComponents => {
            for index in 0..WIDTH {
                source.push_str(&format!("out d{index}: Int\n"));
                source.push_str(&format!("out y{index}: Int\n"));
            }
            for index in 0..WIDTH {
                source.push_str(&format!("aux b{index}: Int\n"));
            }
            for index in 0..WIDTH {
                if index % COMPONENT_WIDTH == 0 {
                    source.push_str(&format!("b{index} = x{index}\n"));
                } else {
                    source.push_str(&format!("b{index} = b{} + x{index}\n", index - 1));
                }
                source.push_str(&format!(
                    "d{index} = defer(defer_source{index}: Int, {{b{index}}})\n"
                ));
                let reverse = variant.reverse_provider(index);
                source.push_str(&format!(
                    "y{index} = dynamic(dynamic_source{index}: Int, \
                     {{b{index}, b{reverse}, d{index}, d{reverse}}})\n"
                ));
            }
        }
    }
    source
}

fn defer_expression(index: usize, variant: Variant) -> Value {
    let input = match variant {
        Variant::AutomaticScope => format!("x{index}"),
        Variant::ExplicitComponents => format!("b{index}"),
    };
    Value::Str(format!("{input} + default({input}[1], 0)").into())
}

fn dynamic_expression(index: usize, reverse: bool, variant: Variant) -> Value {
    let provider = if reverse {
        variant.reverse_provider(index)
    } else {
        index
    };
    let expression = match variant {
        Variant::AutomaticScope => {
            format!("x{provider} + default(x{provider}[1], 0)")
        }
        Variant::ExplicitComponents => {
            format!("d{provider} + default(b{provider}[1], 0)")
        }
    };
    Value::Str(expression.into())
}

fn input_stream(size: usize, variant: Variant) -> InputStream<Value> {
    let mut inputs = BTreeMap::new();
    for index in 0..WIDTH {
        inputs.insert(
            format!("x{index}").into(),
            (0..size)
                .map(|tick| {
                    if (tick + index) % 8 == 7 {
                        Value::NoVal
                    } else {
                        Value::Int((tick * WIDTH + index) as i64 + 1)
                    }
                })
                .collect(),
        );
        inputs.insert(
            format!("defer_source{index}").into(),
            (0..size)
                .map(|tick| {
                    if tick == 0 {
                        defer_expression(index, variant)
                    } else {
                        Value::Deferred
                    }
                })
                .collect(),
        );
        inputs.insert(
            format!("dynamic_source{index}").into(),
            (0..size)
                .map(|tick| {
                    if tick % RECONFIGURATION_PERIOD == 0 {
                        let reverse = (tick / RECONFIGURATION_PERIOD) % 2 == 0;
                        dynamic_expression(index, reverse, variant)
                    } else {
                        Value::NoVal
                    }
                })
                .collect(),
        );
    }
    map::input_stream(inputs)
}

fn hard_dynamic_defer(c: &mut Criterion) {
    let mut group = c.benchmark_group("hard_dynamic_defer");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));

    for variant in [Variant::AutomaticScope, Variant::ExplicitComponents] {
        let source = specification_source(variant);
        let spec = __PARSE_SPEC__;
        let size = variant.size();
        group.throughput(Throughput::Elements(size as u64));

        let dataflow_executor = LocalSmolExecutor::new();
        group.bench_with_input(
            BenchmarkId::new(format!("{}_dataflow", variant.name()), size),
            &size,
            |b, &size| {
                b.to_async(dataflow_executor.clone()).iter(|| {
                    monitor_outputs_untyped_dataflow_limited(
                        dataflow_executor.executor.clone(),
                        spec.clone(),
                        input_stream(size, variant),
                        size,
                    )
                })
            },
        );

        let semisync_executor = LocalSmolExecutor::new();
        group.bench_with_input(
            BenchmarkId::new(format!("{}_semisync", variant.name()), size),
            &size,
            |b, &size| {
                b.to_async(semisync_executor.clone()).iter(|| {
                    monitor_outputs_untyped_semisync_limited(
                        semisync_executor.executor.clone(),
                        spec.clone(),
                        input_stream(size, variant),
                        size,
                    )
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, hard_dynamic_defer);
criterion_main!(benches);
