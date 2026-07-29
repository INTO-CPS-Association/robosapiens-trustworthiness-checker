use std::collections::BTreeMap;
use std::rc::Rc;

use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::SamplingMode;
use criterion::Throughput;
use criterion::async_executor::AsyncExecutor;
use criterion::{criterion_group, criterion_main};
use smol::LocalExecutor;
use trustworthiness_checker::benches_common::monitor_outputs_specialized_dataflow;
use trustworthiness_checker::benches_common::monitor_outputs_specialized_dataflow_limited;
use trustworthiness_checker::benches_common::monitor_outputs_untyped_async;
use trustworthiness_checker::benches_common::monitor_outputs_untyped_dataflow;
use trustworthiness_checker::benches_common::monitor_outputs_untyped_dataflow_limited;
use trustworthiness_checker::benches_common::monitor_outputs_untyped_little;
use trustworthiness_checker::benches_common::monitor_outputs_untyped_semisync_limited;
use trustworthiness_checker::dataflow::DataflowMonitor;
use trustworthiness_checker::dsrv_fixtures::add_defer_input_stream;
use trustworthiness_checker::dsrv_fixtures::spec_add_defer;
use trustworthiness_checker::io::map;
use trustworthiness_checker::{DsrvSpecification, InputStream, Value, VarName};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Clone)]
struct LocalSmolExecutor {
    pub executor: Rc<LocalExecutor<'static>>,
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

fn from_elem(c: &mut Criterion) {
    let sizes = vec![
        1, 10, 100, 500, 1000, 2000, 5000, 10000, 25000, // 100000,
              // 1000000,
    ];

    let mut group = c.benchmark_group("dup_defer");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));

    let spec = spec_add_defer()
        .parse::<DsrvSpecification>()
        .expect("add/defer benchmark specification should parse");
    let dynamic_spec = "in x\nin y\nin e\nout z\nz = dynamic(e)"
        .parse::<DsrvSpecification>()
        .expect("dynamic benchmark specification should parse");

    for size in sizes {
        let input_stream_fn = || add_defer_input_stream(size);
        let async_executor = LocalSmolExecutor::new();
        group.bench_with_input(
            BenchmarkId::new("dup_defer_untyped_async", size),
            &(&spec),
            |b, &spec| {
                b.to_async(async_executor.clone()).iter(|| {
                    monitor_outputs_untyped_async(
                        async_executor.executor.clone(),
                        spec.clone(),
                        input_stream_fn(),
                    )
                })
            },
        );
        let dynamic_async_executor = LocalSmolExecutor::new();
        group.bench_with_input(
            BenchmarkId::new("dynamic_untyped_async", size),
            &(&dynamic_spec),
            |b, &spec| {
                b.to_async(dynamic_async_executor.clone()).iter(|| {
                    monitor_outputs_untyped_async(
                        dynamic_async_executor.executor.clone(),
                        spec.clone(),
                        input_stream_fn(),
                    )
                })
            },
        );
        let dynamic_dataflow_executor = LocalSmolExecutor::new();
        group.bench_with_input(
            BenchmarkId::new("dynamic_untyped_dataflow", size),
            &(&dynamic_spec),
            |b, &spec| {
                b.to_async(dynamic_dataflow_executor.clone()).iter(|| {
                    monitor_outputs_untyped_dataflow(
                        dynamic_dataflow_executor.executor.clone(),
                        spec.clone(),
                        input_stream_fn(),
                    )
                })
            },
        );
        let dynamic_semisync_executor = LocalSmolExecutor::new();
        group.bench_with_input(
            BenchmarkId::new("dynamic_untyped_semisync", size),
            &(&dynamic_spec),
            |b, &spec| {
                b.to_async(dynamic_semisync_executor.clone()).iter(|| {
                    monitor_outputs_untyped_little(
                        dynamic_semisync_executor.executor.clone(),
                        spec.clone(),
                        input_stream_fn(),
                    )
                })
            },
        );
        let dataflow_executor = LocalSmolExecutor::new();
        group.bench_with_input(
            BenchmarkId::new("dup_defer_untyped_dataflow", size),
            &(&spec),
            |b, &spec| {
                b.to_async(dataflow_executor.clone()).iter(|| {
                    monitor_outputs_untyped_dataflow(
                        dataflow_executor.executor.clone(),
                        spec.clone(),
                        input_stream_fn(),
                    )
                })
            },
        );
        let specialized_dataflow_executor = LocalSmolExecutor::new();
        group.bench_with_input(
            BenchmarkId::new("dup_defer_dataflow_specialised", size),
            &(&spec),
            |b, &spec| {
                b.to_async(specialized_dataflow_executor.clone()).iter(|| {
                    monitor_outputs_specialized_dataflow(
                        specialized_dataflow_executor.executor.clone(),
                        spec.clone(),
                        input_stream_fn(),
                    )
                })
            },
        );
        let semisync_executor = LocalSmolExecutor::new();
        group.bench_with_input(
            BenchmarkId::new("dup_defer_untyped_semisync", size),
            &(&spec),
            |b, &spec| {
                b.to_async(semisync_executor.clone()).iter(|| {
                    monitor_outputs_untyped_little(
                        semisync_executor.executor.clone(),
                        spec.clone(),
                        input_stream_fn(),
                    )
                })
            },
        );
    }
    group.finish();
}

// Fixed-width, acyclic stress workloads shared by runtimes: persistent temporal
// defer expressions feed dynamic expressions whose dependencies change periodically.
const HARD_DYNAMIC_DEFER_WIDTH: usize = 32;
const HARD_DYNAMIC_COMPONENT_WIDTH: usize = 8;
const HARD_DYNAMIC_RECONFIGURATION_PERIOD: usize = 64;

#[derive(Clone, Copy)]
enum HardDynamicDeferVariant {
    AutomaticScope,
    ExplicitComponents,
}

impl HardDynamicDeferVariant {
    fn name(self) -> &'static str {
        match self {
            Self::AutomaticScope => "automatic_scope",
            Self::ExplicitComponents => "explicit_components",
        }
    }

    fn sizes(self) -> &'static [usize] {
        match self {
            Self::AutomaticScope => &[1_024],
            Self::ExplicitComponents => &[1_024, 16_384],
        }
    }

    fn reverse_provider(self, index: usize) -> usize {
        match self {
            Self::AutomaticScope => HARD_DYNAMIC_DEFER_WIDTH - index - 1,
            Self::ExplicitComponents => {
                let component_start =
                    index / HARD_DYNAMIC_COMPONENT_WIDTH * HARD_DYNAMIC_COMPONENT_WIDTH;
                component_start + HARD_DYNAMIC_COMPONENT_WIDTH
                    - index % HARD_DYNAMIC_COMPONENT_WIDTH
                    - 1
            }
        }
    }
}

fn hard_dynamic_defer_spec(variant: HardDynamicDeferVariant) -> DsrvSpecification {
    let width = HARD_DYNAMIC_DEFER_WIDTH;
    let mut source = String::new();
    for index in 0..width {
        source.push_str(&format!("in x{index}: Int\n"));
        source.push_str(&format!("in defer_source{index}: Str\n"));
        source.push_str(&format!("in dynamic_source{index}: Str\n"));
    }
    match variant {
        HardDynamicDeferVariant::AutomaticScope => {
            source.push_str("out result: Int\n");
            let terms = (0..width)
                .map(|index| format!("defer(defer_source{index}: Int)"))
                .chain((0..width).map(|index| format!("dynamic(dynamic_source{index}: Int)")))
                .collect::<Vec<_>>();
            source.push_str(&format!("result = {}\n", terms.join(" + ")));
        }
        HardDynamicDeferVariant::ExplicitComponents => {
            for index in 0..width {
                source.push_str(&format!("out d{index}: Int\n"));
                source.push_str(&format!("out y{index}: Int\n"));
            }
            for index in 0..width {
                source.push_str(&format!("aux b{index}: Int\n"));
            }
            for index in 0..width {
                if index % HARD_DYNAMIC_COMPONENT_WIDTH == 0 {
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
        .parse()
        .expect("hard dynamic/defer benchmark specification should parse")
}

fn hard_defer_expression(index: usize, variant: HardDynamicDeferVariant) -> Value {
    let input = match variant {
        HardDynamicDeferVariant::AutomaticScope => format!("x{index}"),
        HardDynamicDeferVariant::ExplicitComponents => format!("b{index}"),
    };
    Value::Str(format!("{input} + default({input}[1], 0)").into())
}

fn hard_dynamic_expression(index: usize, reverse: bool, variant: HardDynamicDeferVariant) -> Value {
    let provider = if reverse {
        variant.reverse_provider(index)
    } else {
        index
    };
    let expression = match variant {
        HardDynamicDeferVariant::AutomaticScope => {
            format!("x{provider} + default(x{provider}[1], 0)")
        }
        HardDynamicDeferVariant::ExplicitComponents => {
            format!("d{provider} + default(b{provider}[1], 0)")
        }
    };
    Value::Str(expression.into())
}

fn hard_dynamic_defer_input_stream(
    size: usize,
    variant: HardDynamicDeferVariant,
) -> InputStream<Value> {
    let mut inputs = BTreeMap::new();
    for index in 0..HARD_DYNAMIC_DEFER_WIDTH {
        inputs.insert(
            format!("x{index}").into(),
            (0..size)
                .map(|tick| {
                    if (tick + index) % 8 == 7 {
                        Value::NoVal
                    } else {
                        Value::Int(
                            i64::try_from(tick * HARD_DYNAMIC_DEFER_WIDTH + index).unwrap() + 1,
                        )
                    }
                })
                .collect(),
        );
        inputs.insert(
            format!("defer_source{index}").into(),
            (0..size)
                .map(|tick| {
                    if tick == 0 {
                        hard_defer_expression(index, variant)
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
                    if tick % HARD_DYNAMIC_RECONFIGURATION_PERIOD == 0 {
                        let reverse = (tick / HARD_DYNAMIC_RECONFIGURATION_PERIOD) % 2 == 0;
                        hard_dynamic_expression(index, reverse, variant)
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
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.measurement_time(std::time::Duration::from_secs(25));

    for variant in [
        HardDynamicDeferVariant::AutomaticScope,
        HardDynamicDeferVariant::ExplicitComponents,
    ] {
        let spec = hard_dynamic_defer_spec(variant);
        for size in variant.sizes().iter().copied() {
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
                            hard_dynamic_defer_input_stream(size, variant),
                            size,
                        )
                    })
                },
            );

            let specialized_dataflow_executor = LocalSmolExecutor::new();
            group.bench_with_input(
                BenchmarkId::new(format!("{}_dataflow_specialised", variant.name()), size),
                &size,
                |b, &size| {
                    b.to_async(specialized_dataflow_executor.clone()).iter(|| {
                        monitor_outputs_specialized_dataflow_limited(
                            specialized_dataflow_executor.executor.clone(),
                            spec.clone(),
                            hard_dynamic_defer_input_stream(size, variant),
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
                            hard_dynamic_defer_input_stream(size, variant),
                            size,
                        )
                    })
                },
            );
        }
    }
    group.finish();
}

fn dataflow_dynamic_phases(c: &mut Criterion) {
    fn compile(operator: &str) -> DataflowMonitor {
        let source = format!("in x\nin y\nin e\nout z\nz = {operator}(e)");
        let spec = source
            .parse::<DsrvSpecification>()
            .expect("dynamic phase benchmark specification should parse");
        DataflowMonitor::compile_untyped(spec).unwrap()
    }

    fn row(monitor: &DataflowMonitor) -> Vec<Value> {
        monitor
            .input_vars()
            .iter()
            .map(|name| {
                if name == &VarName::new("x") {
                    Value::Int(2)
                } else if name == &VarName::new("y") {
                    Value::Int(3)
                } else if name == &VarName::new("e") {
                    Value::Str("x + y".into())
                } else {
                    unreachable!("unexpected dynamic benchmark input {name}")
                }
            })
            .collect()
    }

    let mut group = c.benchmark_group("dataflow_dynamic_phases");
    group.sample_size(20);
    group.warm_up_time(std::time::Duration::from_millis(500));
    group.measurement_time(std::time::Duration::from_secs(2));

    for operator in ["dynamic", "defer"] {
        group.bench_function(format!("{operator}_first_compile"), |b| {
            b.iter_batched(
                || {
                    let monitor = compile(operator);
                    let input = row(&monitor);
                    (monitor, input)
                },
                |(mut monitor, input)| {
                    let mut output = vec![Value::NoVal];
                    monitor.evaluate(&input, &mut output).unwrap();
                    std::hint::black_box(output)
                },
                BatchSize::SmallInput,
            )
        });

        let mut monitor = compile(operator);
        let input = row(&monitor);
        let mut output = vec![Value::NoVal];
        monitor.evaluate(&input, &mut output).unwrap();
        group.bench_function(format!("{operator}_cached_tick"), |b| {
            b.iter(|| {
                monitor.evaluate(&input, &mut output).unwrap();
                std::hint::black_box(&output);
            })
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    from_elem,
    hard_dynamic_defer,
    dataflow_dynamic_phases
);
criterion_main!(benches);
