use std::collections::BTreeMap;
use std::rc::Rc;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::SamplingMode;
use criterion::async_executor::AsyncExecutor;
use criterion::{criterion_group, criterion_main};
use futures::StreamExt;
use smol::LocalExecutor;
use std::fmt::Debug;
use trustworthiness_checker::LOLASpecification;
use trustworthiness_checker::Monitor;
use trustworthiness_checker::OutputStream;
use trustworthiness_checker::Value;
use trustworthiness_checker::dep_manage::interface::DependencyKind;
use trustworthiness_checker::dep_manage::interface::DependencyManager;
use trustworthiness_checker::dep_manage::interface::create_dependency_manager;
use trustworthiness_checker::io::testing::null_output_handler::NullOutputHandler;
use trustworthiness_checker::lang::dynamic_lola::type_checker::TypedLOLASpecification;
use trustworthiness_checker::lang::dynamic_lola::type_checker::type_check;
use trustworthiness_checker::lola_fixtures::{
    input_streams_simple_add_typed, input_streams_simple_add_untyped,
};
use trustworthiness_checker::runtime::constraints::runtime::ConstraintBasedRuntime;
// use trustworthiness_checker::semantics::untimed_typed_lola::to_typed_stream;

pub fn to_typed_stream<T: TryFrom<Value, Error = ()> + Debug>(
    stream: OutputStream<Value>,
) -> OutputStream<T> {
    Box::pin(stream.map(|x| x.try_into().expect("Type error")))
}

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

pub fn spec_simple_add_monitor() -> &'static str {
    "in x\n\
     in y\n\
     out z\n\
     z = x + y"
}

pub fn spec_simple_add_monitor_typed() -> &'static str {
    "in x: Int\n\
     in y: Int\n\
     out z: Int\n\
     z = x + y"
}

async fn monitor_outputs_untyped_constraints(
    executor: Rc<LocalExecutor<'static>>,
    spec: LOLASpecification,
    dep_manager: DependencyManager,
    num_outputs: usize,
) {
    let mut input_streams = input_streams_simple_add_untyped(num_outputs);
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
    let mut input_streams = input_streams_simple_add_untyped(num_outputs);
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
        executor,
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
    let mut input_streams = input_streams_simple_add_typed(num_outputs);
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
        executor,
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
    let mut input_streams = input_streams_simple_add_untyped(num_outputs);
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
        spec.clone(),
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
    let mut input_streams = input_streams_simple_add_typed(num_outputs);
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
        executor,
        spec,
        &mut input_streams,
        output_handler,
        dep_manager,
    );
    async_monitor.run().await;
}

fn monitor_outputs_untyped_constraints_no_overhead(
    num_outputs: usize,
    dependency_kind: DependencyKind,
) {
    let size = num_outputs as i64;
    let mut xs = (0..size).map(|i| Value::Int(i * 2));
    let mut ys = (0..size).map(|i| Value::Int(i * 2 + 1));
    let spec = trustworthiness_checker::lola_specification(&mut spec_simple_add_monitor()).unwrap();
    let mut runtime =
        ConstraintBasedRuntime::new(create_dependency_manager(dependency_kind, spec.clone()));
    runtime.store_from_spec(spec);

    for _ in 0..size {
        let inputs = BTreeMap::from([
            ("x".into(), xs.next().unwrap()),
            ("y".into(), ys.next().unwrap()),
        ]);
        runtime.step(inputs.iter());
        runtime.cleanup();
    }
}

fn from_elem(c: &mut Criterion) {
    let sizes = vec![
        1, 10, 100, 500, 1000, 2000, 5000, 10000, 25000, // 100000,
              // 1000000,
    ];

    let local_smol_executor = LocalSmolExecutor::new();

    let mut group = c.benchmark_group("simple_add");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));

    let spec =
        trustworthiness_checker::lola_specification(&mut spec_simple_add_monitor_typed()).unwrap();
    let dep_manager = create_dependency_manager(DependencyKind::Empty, spec.clone());
    let dep_manager_graph = create_dependency_manager(DependencyKind::DepGraph, spec.clone());
    let spec_typed = type_check(spec.clone()).expect("Type check failed");

    for size in sizes {
        if size <= 5000 {
            group.bench_with_input(
                BenchmarkId::new("simple_add_constraints", size),
                &(size, &spec, &dep_manager),
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
            BenchmarkId::new("simple_add_constraints_gc", size),
            &(size, &spec, &dep_manager_graph),
            |b, &(size, spec, dep_manager_graph)| {
                b.to_async(local_smol_executor.clone()).iter(|| {
                    monitor_outputs_untyped_constraints(
                        local_smol_executor.executor.clone(),
                        spec.clone(),
                        dep_manager_graph.clone(),
                        size,
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("simple_add_constraints_nooverhead_gc", size),
            &size,
            |b, &size| {
                b.iter(|| {
                    monitor_outputs_untyped_constraints_no_overhead(size, DependencyKind::DepGraph)
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("simple_add_untyped_async", size),
            &(size, &spec, &dep_manager),
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
            BenchmarkId::new("simple_add_typed_async", size),
            &(size, &spec_typed, &dep_manager),
            |b, &(size, spec_typed, dep_manager)| {
                b.to_async(local_smol_executor.clone()).iter(|| {
                    monitor_outputs_typed_async(
                        local_smol_executor.executor.clone(),
                        spec_typed.clone(),
                        dep_manager.clone(),
                        size,
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("simple_add_untyped_queuing", size),
            &(size, &spec, &dep_manager),
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
            BenchmarkId::new("simple_add_typed_queuing", size),
            &(size, &spec_typed, &dep_manager),
            |b, &(size, spec_typed, dep_manager)| {
                b.to_async(local_smol_executor.clone()).iter(|| {
                    monitor_outputs_typed_queuing(
                        local_smol_executor.executor.clone(),
                        spec_typed.clone(),
                        dep_manager.clone(),
                        size,
                    )
                })
            },
        );
        // group.bench_with_input(
        //     BenchmarkId::new("simple_add_baseline", size),
        //     &size,
        //     |b, &size| b.to_async(&tokio_rt).iter(|| baseline(size)),
        // );
    }
    group.finish();
}

criterion_group!(benches, from_elem);
criterion_main!(benches);
