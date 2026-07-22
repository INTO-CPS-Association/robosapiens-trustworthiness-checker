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
use trustworthiness_checker::dsrv_fixtures::maple_valid_input_stream;
use trustworthiness_checker::dsrv_fixtures::spec_maple_sequence;
use trustworthiness_checker::{CheckedDsrvSpecification, DsrvSpecification};

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

    let mut group = c.benchmark_group("maple_sequence");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));

    let spec = spec_maple_sequence()
        .parse::<DsrvSpecification>()
        .expect("maple sequence benchmark specification should parse");
    let spec_typed = spec_maple_sequence()
        .parse::<CheckedDsrvSpecification>()
        .expect("maple sequence benchmark specification should type check");

    for size in sizes {
        let input_stream_fn = || maple_valid_input_stream(size);
        group.bench_with_input(
            BenchmarkId::new("maple_sequence_untyped_async", size),
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
            BenchmarkId::new("maple_sequence_untyped_semisync", size),
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
        group.bench_with_input(
            BenchmarkId::new("maple_sequence_untyped_dataflow", size),
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
            BenchmarkId::new("maple_sequence_typed_async", size),
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
            BenchmarkId::new("maple_sequence_typed_dataflow", size),
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
    }
    group.finish();
}

criterion_group!(benches, from_elem);
criterion_main!(benches);
