use std::rc::Rc;
use std::time::Duration;
use trustworthiness_checker::benches_common::monitor_outputs_untyped_async_limited;
use trustworthiness_checker::dep_manage::interface::DependencyKind;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::SamplingMode;
use criterion::async_executor::AsyncExecutor;
use criterion::{criterion_group, criterion_main};
use itertools::Itertools;
use smol::LocalExecutor;
use trustworthiness_checker::dep_manage::interface::create_dependency_manager;
use trustworthiness_checker::lola_fixtures::spec_deferred_and;
use trustworthiness_checker::lola_fixtures::spec_direct_and;
use trustworthiness_checker::lola_fixtures::{
    input_streams_paper_benchmark, input_streams_paper_benchmark_direct,
};

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
        1, 25000, 50000, 75000, 100000,
        // 1000000,
    ];

    let local_smol_executor = LocalSmolExecutor::new();

    let mut group = c.benchmark_group("dyn_paper");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(std::time::Duration::from_secs(10));

    let spec_direct = trustworthiness_checker::lola_specification(&mut spec_direct_and()).unwrap();
    let spec = trustworthiness_checker::lola_specification(&mut spec_deferred_and()).unwrap();
    let dep_manager = create_dependency_manager(DependencyKind::Empty, spec.clone());
    // let dep_manager_graph = create_dependency_manager(DependencyKind::DepGraph, spec.clone());
    let percents = vec![0, 25, 50, 75, 100];

    for size in sizes.iter() {
        let input_stream_fn = || input_streams_paper_benchmark_direct(*size);
        group.bench_with_input(
            BenchmarkId::new("dyn_paper_direct", size),
            &(&spec_direct, &dep_manager),
            |b, &(spec, dep_manager)| {
                b.to_async(local_smol_executor.clone()).iter(|| {
                    monitor_outputs_untyped_async_limited(
                        local_smol_executor.executor.clone(),
                        spec.clone(),
                        input_stream_fn(),
                        dep_manager.clone(),
                        *size,
                    )
                })
            },
        );
    }

    for (size, percent) in sizes.into_iter().cartesian_product(percents) {
        let input_stream_fn = || input_streams_paper_benchmark(percent, size);
        group.bench_with_input(
            BenchmarkId::new(format!("dyn_paper_{}", percent), size),
            &(&spec, &dep_manager),
            |b, &(spec, dep_manager)| {
                b.to_async(local_smol_executor.clone()).iter(|| {
                    monitor_outputs_untyped_async_limited(
                        local_smol_executor.executor.clone(),
                        spec.clone(),
                        input_stream_fn(),
                        dep_manager.clone(),
                        size,
                    )
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, from_elem);
criterion_main!(benches);
