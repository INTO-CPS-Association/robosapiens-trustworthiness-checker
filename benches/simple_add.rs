use std::rc::Rc;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::SamplingMode;
use criterion::async_executor::AsyncExecutor;
use criterion::{criterion_group, criterion_main};
use smol::LocalExecutor;
use trustworthiness_checker::benches_common::monitor_outputs_typed_async;
use trustworthiness_checker::benches_common::monitor_outputs_typed_dataflow;
use trustworthiness_checker::benches_common::monitor_outputs_untyped_async;
use trustworthiness_checker::benches_common::monitor_outputs_untyped_dataflow;
use trustworthiness_checker::benches_common::monitor_outputs_untyped_little;
use trustworthiness_checker::dsrv_fixtures::simple_add_input_stream;
use trustworthiness_checker::dsrv_fixtures::spec_simple_add_monitor;
use trustworthiness_checker::dsrv_fixtures::spec_simple_add_monitor_typed;
use trustworthiness_checker::lang::dsrv::type_checker::type_check;

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

    let mut group = c.benchmark_group("simple_add");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));

    let spec =
        trustworthiness_checker::lang::dsrv::parser::parse_str(spec_simple_add_monitor()).unwrap();
    let spec_typed =
        trustworthiness_checker::lang::dsrv::parser::parse_str(spec_simple_add_monitor_typed())
            .unwrap();
    let spec_typed = type_check(spec_typed.clone(), false).expect("Type check failed");

    for size in sizes {
        let input_stream_fn = || simple_add_input_stream(size);
        group.bench_with_input(
            BenchmarkId::new("simple_add_untyped_async", size),
            &(&spec),
            |b, &spec| {
                let benchmark_executor = LocalSmolExecutor::new();
                b.to_async(benchmark_executor.clone()).iter(|| {
                    monitor_outputs_untyped_async(
                        benchmark_executor.executor.clone(),
                        spec.clone(),
                        input_stream_fn(),
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("simple_add_typed_async", size),
            &(&spec_typed),
            |b, &spec_typed| {
                let benchmark_executor = LocalSmolExecutor::new();
                b.to_async(benchmark_executor.clone()).iter(|| {
                    monitor_outputs_typed_async(
                        benchmark_executor.executor.clone(),
                        spec_typed.clone(),
                        input_stream_fn(),
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("simple_add_untyped_dataflow", size),
            &(&spec),
            |b, &spec| {
                let benchmark_executor = LocalSmolExecutor::new();
                b.to_async(benchmark_executor.clone()).iter(|| {
                    monitor_outputs_untyped_dataflow(
                        benchmark_executor.executor.clone(),
                        spec.clone(),
                        input_stream_fn(),
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("simple_add_typed_dataflow", size),
            &(&spec_typed),
            |b, &spec_typed| {
                let benchmark_executor = LocalSmolExecutor::new();
                b.to_async(benchmark_executor.clone()).iter(|| {
                    monitor_outputs_typed_dataflow(
                        benchmark_executor.executor.clone(),
                        spec_typed.clone(),
                        input_stream_fn(),
                        trustworthiness_checker::core::Semantics::TypedUntimed,
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("simple_add_untyped_little", size),
            &(&spec),
            |b, &spec| {
                let benchmark_executor = LocalSmolExecutor::new();
                b.to_async(benchmark_executor.clone()).iter(|| {
                    monitor_outputs_untyped_little(
                        benchmark_executor.executor.clone(),
                        spec.clone(),
                        input_stream_fn(),
                    )
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, from_elem);
criterion_main!(benches);
