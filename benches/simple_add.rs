use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::SamplingMode;
use criterion::{criterion_group, criterion_main};
use futures::StreamExt;
use trustworthiness_checker::LOLASpecification;
use trustworthiness_checker::Monitor;
use trustworthiness_checker::dep_manage;
use trustworthiness_checker::dep_manage::interface::DependencyKind;
use trustworthiness_checker::dep_manage::interface::DependencyManager;
use trustworthiness_checker::dep_manage::interface::create_dependency_manager;
use trustworthiness_checker::io::testing::null_output_handler::NullOutputHandler;
use trustworthiness_checker::lang::dynamic_lola::type_checker::TypedLOLASpecification;
use trustworthiness_checker::lang::dynamic_lola::type_checker::type_check;
use trustworthiness_checker::lola_fixtures::{
    input_streams_simple_add_typed, input_streams_simple_add_untyped,
};
use trustworthiness_checker::semantics::untimed_typed_lola::to_typed_stream;

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

async fn baseline(num_outputs: usize) {
    let mut input_streams = input_streams_simple_add_typed(num_outputs);
    let _ = to_typed_stream::<i64>(input_streams.remove(&"x".into()).unwrap())
        .zip(to_typed_stream::<i64>(
            input_streams.remove(&"y".into()).unwrap(),
        ))
        .map(|(x, y)| x + y)
        .collect::<Vec<_>>()
        .await;
}

async fn monitor_outputs_untyped_constraints(
    spec: LOLASpecification,
    dep_manager: DependencyManager,
    num_outputs: usize,
) {
    let mut input_streams = input_streams_simple_add_untyped(num_outputs);
    let output_handler = Box::new(NullOutputHandler::new(spec.output_vars.clone()));
    let async_monitor = trustworthiness_checker::runtime::constraints::ConstraintBasedMonitor::new(
        spec.clone(),
        &mut input_streams,
        output_handler,
        dep_manager,
    );
    async_monitor.run().await;
}

async fn monitor_outputs_untyped_async(
    spec: LOLASpecification,
    dep_manager: DependencyManager,
    num_outputs: usize,
) {
    let mut input_streams = input_streams_simple_add_untyped(num_outputs);
    let output_handler = Box::new(NullOutputHandler::new(spec.output_vars.clone()));
    let async_monitor = trustworthiness_checker::runtime::asynchronous::AsyncMonitorRunner::<
        _,
        _,
        trustworthiness_checker::semantics::UntimedLolaSemantics,
        trustworthiness_checker::LOLASpecification,
    >::new(spec, &mut input_streams, output_handler, dep_manager);
    async_monitor.run().await;
}

async fn monitor_outputs_typed_async(
    spec: TypedLOLASpecification,
    dep_manager: DependencyManager,
    num_outputs: usize,
) {
    let mut input_streams = input_streams_simple_add_typed(num_outputs);
    let output_handler = Box::new(NullOutputHandler::new(spec.output_vars.clone()));
    let async_monitor = trustworthiness_checker::runtime::asynchronous::AsyncMonitorRunner::<
        _,
        _,
        trustworthiness_checker::semantics::TypedUntimedLolaSemantics,
        _,
    >::new(spec, &mut input_streams, output_handler, dep_manager);
    async_monitor.run().await;
}

async fn monitor_outputs_untyped_queuing(
    spec: LOLASpecification,
    dep_manager: DependencyManager,
    num_outputs: usize,
) {
    let mut input_streams = input_streams_simple_add_untyped(num_outputs);
    let output_handler = Box::new(NullOutputHandler::new(spec.output_vars.clone()));
    let async_monitor = trustworthiness_checker::runtime::queuing::QueuingMonitorRunner::<
        _,
        _,
        trustworthiness_checker::semantics::UntimedLolaSemantics,
        trustworthiness_checker::LOLASpecification,
    >::new(
        spec.clone(),
        &mut input_streams,
        output_handler,
        dep_manager,
    );
    async_monitor.run().await;
}

async fn monitor_outputs_typed_queuing(
    spec: TypedLOLASpecification,
    dep_manager: DependencyManager,
    num_outputs: usize,
) {
    let mut input_streams = input_streams_simple_add_typed(num_outputs);
    let output_handler = Box::new(NullOutputHandler::new(spec.output_vars.clone()));
    let async_monitor = trustworthiness_checker::runtime::queuing::QueuingMonitorRunner::<
        _,
        _,
        trustworthiness_checker::semantics::TypedUntimedLolaSemantics,
        _,
    >::new(spec, &mut input_streams, output_handler, dep_manager);
    async_monitor.run().await;
}

fn from_elem(c: &mut Criterion) {
    let sizes = vec![
        1, 10, 100, 500, 1000, 2000, 5000, 10000, 25000, // 100000,
              // 1000000,
    ];

    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("simple_add");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));

    let spec = trustworthiness_checker::lola_specification(&mut spec_simple_add_monitor_typed()).unwrap();
    let dep_manager = create_dependency_manager(DependencyKind::Empty, spec.clone());
    let dep_manager_graph = create_dependency_manager(DependencyKind::DepGraph, spec.clone());
    let spec_typed = type_check(spec.clone()).expect("Type check failed");

    for size in sizes {
        if size <= 5000 {
            group.bench_with_input(
                BenchmarkId::new("simple_add_constraints", size),
                &(size, &spec, &dep_manager),
                |b, &(size, spec, dep_manager)| {
                    b.to_async(&tokio_rt).iter(|| {
                        monitor_outputs_untyped_constraints(spec.clone(), dep_manager.clone(), size)
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("simple_add_constraints_gc", size),
                &(size, &spec, &dep_manager_graph),
                |b, &(size, spec, dep_manager_graph)| {
                    b.to_async(&tokio_rt).iter(|| {
                        monitor_outputs_untyped_constraints(spec.clone(), dep_manager_graph.clone(), size)
                    })
                },
            );
        }
        group.bench_with_input(
            BenchmarkId::new("simple_add_untyped_async", size),
            &(size, &spec, &dep_manager),
            |b, &(size, spec, dep_manager)| {
                b.to_async(&tokio_rt)
                    .iter(|| monitor_outputs_untyped_async(spec.clone(), dep_manager.clone(), size))
            },
        );
        group.bench_with_input(
            BenchmarkId::new("simple_add_typed_async", size),
            &(size, &spec_typed, &dep_manager),
            |b, &(size, spec_typed, dep_manager)| {
                b.to_async(&tokio_rt)
                    .iter(|| monitor_outputs_typed_async(spec_typed.clone(), dep_manager.clone(), size))
            },
        );
        group.bench_with_input(
            BenchmarkId::new("simple_add_untyped_queuing", size),
            &(size, &spec, &dep_manager),
            |b, &(size, spec, dep_manager)| {
                b.to_async(&tokio_rt)
                    .iter(|| monitor_outputs_untyped_queuing(spec.clone(), dep_manager.clone(), size))
            },
        );
        group.bench_with_input(
            BenchmarkId::new("simple_add_typed_queuing", size),
            &(size, &spec_typed, &dep_manager),
            |b, &(size, spec_typed, dep_manager)| {
                b.to_async(&tokio_rt)
                    .iter(|| monitor_outputs_typed_queuing(spec_typed.clone(), dep_manager.clone(), size))
            },
        );
        group.bench_with_input(
            BenchmarkId::new("simple_add_baseline", size),
            &size,
            |b, &size| b.to_async(&tokio_rt).iter(|| baseline(size)),
        );
    }
    group.finish();
}

criterion_group!(benches, from_elem);
criterion_main!(benches);
