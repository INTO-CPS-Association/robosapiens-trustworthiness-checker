use std::rc::Rc;

use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::SamplingMode;
use criterion::async_executor::AsyncExecutor;
use criterion::{criterion_group, criterion_main};
use smol::LocalExecutor;
use trustworthiness_checker::benches_common::monitor_outputs_untyped_async;
use trustworthiness_checker::benches_common::monitor_outputs_untyped_dataflow;
use trustworthiness_checker::benches_common::monitor_outputs_untyped_little;
use trustworthiness_checker::dataflow::DataflowMonitor;
use trustworthiness_checker::dsrv_fixtures::add_defer_input_stream;
use trustworthiness_checker::dsrv_fixtures::spec_add_defer;
use trustworthiness_checker::{DsrvSpecification, Value, VarName};

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

fn dataflow_dynamic_phases(c: &mut Criterion) {
    fn compile(operator: &str) -> DataflowMonitor {
        let source = format!("in x\nin y\nin e\nout z\nz = {operator}(e)");
        let spec = source
            .parse::<DsrvSpecification>()
            .expect("dynamic phase benchmark specification should parse");
        DataflowMonitor::try_compile_untyped(spec).unwrap()
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

criterion_group!(benches, from_elem, dataflow_dynamic_phases);
criterion_main!(benches);
