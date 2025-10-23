use std::rc::Rc;
use trustworthiness_checker::dep_manage::interface::DependencyKind;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::SamplingMode;
use criterion::async_executor::AsyncExecutor;
use criterion::{criterion_group, criterion_main};
use smol::LocalExecutor;
use trustworthiness_checker::benches_common::monitor_outputs_untyped_async;
use trustworthiness_checker::dep_manage::interface::create_dependency_manager;
use trustworthiness_checker::lola_fixtures::input_streams_add_defer;
use trustworthiness_checker::lola_fixtures::spec_add_defer;

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

    let mut group = c.benchmark_group("dup_defer");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));

    let spec = trustworthiness_checker::lola_specification(&mut spec_add_defer()).unwrap();
    let dep_manager = create_dependency_manager(DependencyKind::Empty, spec.clone());

    for size in sizes {
        let input_stream_fn = || input_streams_add_defer(size);
        group.bench_with_input(
            BenchmarkId::new("dup_defer_untyped_async", size),
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
    }
    group.finish();
}

criterion_group!(benches, from_elem);
criterion_main!(benches);
