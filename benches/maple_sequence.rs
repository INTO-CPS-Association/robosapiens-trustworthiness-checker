use std::future::Future;
use std::rc::Rc;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::SamplingMode;
use criterion::async_executor::AsyncExecutor;
use criterion::{criterion_group, criterion_main};
use smol::LocalExecutor;
use trustworthiness_checker::LOLASpecification;
use trustworthiness_checker::Monitor;
use trustworthiness_checker::dep_manage::interface::DependencyKind;
use trustworthiness_checker::dep_manage::interface::DependencyManager;
use trustworthiness_checker::dep_manage::interface::create_dependency_manager;
use trustworthiness_checker::io::testing::null_output_handler::NullOutputHandler;
use trustworthiness_checker::lang::dynamic_lola::type_checker::TypedLOLASpecification;
use trustworthiness_checker::lang::dynamic_lola::type_checker::type_check;
use trustworthiness_checker::lola_fixtures::{maple_valid_input_stream, spec_maple_sequence};

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

async fn monitor_outputs_untyped_constraints(
    executor: Rc<LocalExecutor<'static>>,
    spec: LOLASpecification,
    dep_manager: DependencyManager,
    num_outputs: usize,
) {
    let mut input_streams = maple_valid_input_stream(num_outputs);
    let output_handler = Box::new(NullOutputHandler::new(
        executor.clone(),
        spec.output_vars.clone(),
    ));
    let async_monitor = trustworthiness_checker::runtime::constraints::ConstraintBasedMonitor::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
        output_handler,
        dep_manager,
    );
    async_monitor.run().await;
}

async fn monitor_outputs_untyped_async(
    executor: Rc<LocalExecutor<'static>>,
    spec: LOLASpecification,
    dep_manager: DependencyManager,
    num_outputs: usize,
) {
    let mut input_streams = maple_valid_input_stream(num_outputs);
    let output_handler = Box::new(NullOutputHandler::new(
        executor.clone(),
        spec.output_vars.clone(),
    ));
    let async_monitor = trustworthiness_checker::runtime::asynchronous::AsyncMonitorRunner::<
        _,
        _,
        trustworthiness_checker::semantics::UntimedLolaSemantics,
        trustworthiness_checker::LOLASpecification,
    >::new(
        executor.clone(),
        spec,
        &mut input_streams,
        output_handler,
        dep_manager,
    );
    async_monitor.run().await;
}

async fn monitor_outputs_typed_async(
    executor: Rc<LocalExecutor<'static>>,
    spec: TypedLOLASpecification,
    dep_manager: DependencyManager,
    num_outputs: usize,
) {
    let mut input_streams = maple_valid_input_stream(num_outputs);
    let output_handler = Box::new(NullOutputHandler::new(
        executor.clone(),
        spec.output_vars.clone(),
    ));
    let async_monitor = trustworthiness_checker::runtime::asynchronous::AsyncMonitorRunner::<
        _,
        _,
        trustworthiness_checker::semantics::TypedUntimedLolaSemantics,
        _,
    >::new(
        executor.clone(),
        spec,
        &mut input_streams,
        output_handler,
        dep_manager,
    );
    async_monitor.run().await;
}

async fn monitor_outputs_untyped_queuing(
    executor: Rc<LocalExecutor<'static>>,
    spec: LOLASpecification,
    dep_manager: DependencyManager,
    num_outputs: usize,
) {
    let mut input_streams = maple_valid_input_stream(num_outputs);
    let output_handler = Box::new(NullOutputHandler::new(
        executor.clone(),
        spec.output_vars.clone(),
    ));
    let async_monitor = trustworthiness_checker::runtime::queuing::QueuingMonitorRunner::<
        _,
        _,
        trustworthiness_checker::semantics::UntimedLolaSemantics,
        trustworthiness_checker::LOLASpecification,
    >::new(
        executor.clone(),
        spec,
        &mut input_streams,
        output_handler,
        dep_manager,
    );
    async_monitor.run().await;
}

async fn monitor_outputs_typed_queuing(
    executor: Rc<LocalExecutor<'static>>,
    spec: TypedLOLASpecification,
    dep_manager: DependencyManager,
    num_outputs: usize,
) {
    let mut input_streams = maple_valid_input_stream(num_outputs);
    let output_handler = Box::new(NullOutputHandler::new(
        executor.clone(),
        spec.output_vars.clone(),
    ));
    let async_monitor = trustworthiness_checker::runtime::queuing::QueuingMonitorRunner::<
        _,
        _,
        trustworthiness_checker::semantics::TypedUntimedLolaSemantics,
        _,
    >::new(
        executor.clone(),
        spec,
        &mut input_streams,
        output_handler,
        dep_manager,
    );
    async_monitor.run().await;
}

fn from_elem(c: &mut Criterion) {
    let sizes = vec![
        1, 10, 100, 500, 1000, 2000, 5000, 10000, 25000, // 100000,
              // 1000000,
    ];

    let local_smol_executor = LocalSmolExecutor::new();

    // Parse specifications and create dependency managers once
    let spec = trustworthiness_checker::lola_specification(&mut spec_maple_sequence()).unwrap();
    let dep_manager_empty = create_dependency_manager(DependencyKind::Empty, spec.clone());
    let dep_manager_graph = create_dependency_manager(DependencyKind::DepGraph, spec.clone());
    let spec_typed = type_check(spec.clone()).expect("Type check failed");

    let mut group = c.benchmark_group("maple_sequence");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));

    for size in sizes {
        if size <= 5000 {
            group.bench_with_input(
                BenchmarkId::new("maple_sequence_constraints", size),
                &(size, &spec, &dep_manager_empty),
                |b, &(size, spec, dep_manager)| {
                    b.to_async(local_smol_executor.clone()).iter(|| {
                        monitor_outputs_untyped_constraints(
                            local_smol_executor.executor.clone(),
                            spec.clone(),
                            dep_manager.clone(),
                            size,
                        )
                    })
                },
            );
        }
        group.bench_with_input(
            BenchmarkId::new("maple_sequence_constraints_graph", size),
            &(size, &spec, &dep_manager_graph),
            |b, &(size, spec, dep_manager)| {
                b.to_async(local_smol_executor.clone()).iter(|| {
                    monitor_outputs_untyped_constraints(
                        local_smol_executor.executor.clone(),
                        spec.clone(),
                        dep_manager.clone(),
                        size,
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("maple_sequence_untyped_async", size),
            &(size, &spec, &dep_manager_empty),
            |b, &(size, spec, dep_manager)| {
                b.to_async(local_smol_executor.clone()).iter(|| {
                    monitor_outputs_untyped_async(
                        local_smol_executor.executor.clone(),
                        spec.clone(),
                        dep_manager.clone(),
                        size,
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("maple_sequence_typed_async", size),
            &(size, &spec_typed, &dep_manager_empty),
            |b, &(size, spec, dep_manager)| {
                b.to_async(local_smol_executor.clone()).iter(|| {
                    monitor_outputs_typed_async(
                        local_smol_executor.executor.clone(),
                        spec.clone(),
                        dep_manager.clone(),
                        size,
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("maple_sequence_untyped_queuing", size),
            &(size, &spec, &dep_manager_empty),
            |b, &(size, spec, dep_manager)| {
                b.to_async(local_smol_executor.clone()).iter(|| {
                    monitor_outputs_untyped_queuing(
                        local_smol_executor.executor.clone(),
                        spec.clone(),
                        dep_manager.clone(),
                        size,
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("maple_sequence_typed_queuing", size),
            &(size, &spec_typed, &dep_manager_empty),
            |b, &(size, spec, dep_manager)| {
                b.to_async(local_smol_executor.clone()).iter(|| {
                    monitor_outputs_typed_queuing(
                        local_smol_executor.executor.clone(),
                        spec.clone(),
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
