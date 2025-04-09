use std::rc::Rc;
use trustworthiness_checker::dep_manage::interface::DependencyKind;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::SamplingMode;
use criterion::async_executor::AsyncExecutor;
use criterion::{criterion_group, criterion_main};
use smol::LocalExecutor;
use trustworthiness_checker::benches_common::monitor_outputs_typed_async;
use trustworthiness_checker::benches_common::monitor_outputs_untyped_async;
use trustworthiness_checker::benches_common::monitor_outputs_untyped_constraints;
use trustworthiness_checker::dep_manage::interface::create_dependency_manager;
use trustworthiness_checker::lang::dynamic_lola::type_checker::type_check;
use trustworthiness_checker::lola_fixtures::maple_valid_input_stream;
use trustworthiness_checker::lola_fixtures::spec_maple_sequence;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

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

    let local_smol_executor = LocalSmolExecutor::new();

    let mut group = c.benchmark_group("maple_sequence");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));

    let spec = trustworthiness_checker::lola_specification(&mut spec_maple_sequence()).unwrap();
    let dep_manager = create_dependency_manager(DependencyKind::Empty, spec.clone());
    let dep_manager_graph = create_dependency_manager(DependencyKind::DepGraph, spec.clone());
    let spec_typed = type_check(spec.clone()).expect("Type check failed");

    for size in sizes {
        let input_stream_fn = || maple_valid_input_stream(size);
        if size <= 5000 {
            group.bench_with_input(
                BenchmarkId::new("maple_sequence_constraints", size),
                &(&spec, &dep_manager),
                |b, &(spec, dep_manager)| {
                    b.to_async(local_smol_executor.clone()).iter(|| {
                        monitor_outputs_untyped_constraints(
                            local_smol_executor.executor.clone(),
                            spec.clone(),
                            input_stream_fn(),
                            dep_manager.clone(),
                        )
                    })
                },
            );
        }
        group.bench_with_input(
            BenchmarkId::new("maple_sequence_constraints_gc", size),
            &(&spec, &dep_manager_graph),
            |b, &(spec, dep_manager_graph)| {
                b.to_async(local_smol_executor.clone()).iter(|| {
                    monitor_outputs_untyped_constraints(
                        local_smol_executor.executor.clone(),
                        spec.clone(),
                        input_stream_fn(),
                        dep_manager_graph.clone(),
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("maple_sequence_untyped_async", size),
            &(&spec, &dep_manager),
            |b, &(spec, dep_manager)| {
                b.to_async(local_smol_executor.clone()).iter(|| {
                    monitor_outputs_untyped_async(
                        local_smol_executor.executor.clone(),
                        spec.clone(),
                        input_stream_fn(),
                        dep_manager.clone(),
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("maple_sequence_typed_async", size),
            &(&spec_typed, &dep_manager),
            |b, &(spec_typed, dep_manager)| {
                b.to_async(local_smol_executor.clone()).iter(|| {
                    monitor_outputs_typed_async(
                        local_smol_executor.executor.clone(),
                        spec_typed.clone(),
                        input_stream_fn(),
                        dep_manager.clone(),
                    )
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, from_elem);
criterion_main!(benches);
