use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::SamplingMode;
use criterion::{criterion_group, criterion_main};
use trustworthiness_checker::Monitor;
use trustworthiness_checker::dep_manage::interface::DependencyKind;
use trustworthiness_checker::dep_manage::interface::create_dependency_manager;
use trustworthiness_checker::io::testing::null_output_handler::NullOutputHandler;
use trustworthiness_checker::lang::dynamic_lola::type_checker::type_check;
use trustworthiness_checker::lola_fixtures::{maple_valid_input_stream, spec_maple_sequence};

async fn monitor_outputs_untyped_constraints(num_outputs: usize) {
    let mut input_streams = maple_valid_input_stream(num_outputs);
    let spec = trustworthiness_checker::lola_specification(&mut spec_maple_sequence()).unwrap();
    let output_handler = Box::new(NullOutputHandler::new(spec.output_vars.clone()));
    let async_monitor = trustworthiness_checker::runtime::constraints::ConstraintBasedMonitor::new(
        spec.clone(),
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    async_monitor.run().await;
}

async fn monitor_outputs_untyped_async(num_outputs: usize) {
    let mut input_streams = maple_valid_input_stream(num_outputs);
    let spec = trustworthiness_checker::lola_specification(&mut spec_maple_sequence()).unwrap();
    let output_handler = Box::new(NullOutputHandler::new(spec.output_vars.clone()));
    let async_monitor = trustworthiness_checker::runtime::asynchronous::AsyncMonitorRunner::<
        _,
        _,
        trustworthiness_checker::semantics::UntimedLolaSemantics,
        trustworthiness_checker::LOLASpecification,
    >::new(
        spec.clone(),
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    async_monitor.run().await;
}

async fn monitor_outputs_typed_async(num_outputs: usize) {
    let mut input_streams = maple_valid_input_stream(num_outputs);
    let spec_untyped =
        trustworthiness_checker::lola_specification(&mut spec_maple_sequence()).unwrap();
    let spec = type_check(spec_untyped.clone()).expect("Type check failed");
    let output_handler = Box::new(NullOutputHandler::new(spec.output_vars.clone()));
    let async_monitor = trustworthiness_checker::runtime::asynchronous::AsyncMonitorRunner::<
        _,
        _,
        trustworthiness_checker::semantics::TypedUntimedLolaSemantics,
        _,
    >::new(
        spec,
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec_untyped),
    );
    async_monitor.run().await;
}

async fn monitor_outputs_untyped_queuing(num_outputs: usize) {
    let mut input_streams = maple_valid_input_stream(num_outputs);
    let spec = trustworthiness_checker::lola_specification(&mut spec_maple_sequence()).unwrap();
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
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    async_monitor.run().await;
}

async fn monitor_outputs_typed_queuing(num_outputs: usize) {
    let mut input_streams = maple_valid_input_stream(num_outputs);
    let spec_untyped =
        trustworthiness_checker::lola_specification(&mut spec_maple_sequence()).unwrap();
    let spec = type_check(spec_untyped.clone()).expect("Type check failed");
    let output_handler = Box::new(NullOutputHandler::new(spec.output_vars.clone()));
    let async_monitor = trustworthiness_checker::runtime::queuing::QueuingMonitorRunner::<
        _,
        _,
        trustworthiness_checker::semantics::TypedUntimedLolaSemantics,
        _,
    >::new(
        spec,
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec_untyped),
    );
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

    let mut group = c.benchmark_group("maple_sequence");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));

    for size in sizes {
        if size <= 5000 {
            group.bench_with_input(
                BenchmarkId::new("maple_sequence_constraints", size),
                &size,
                |b, &size| {
                    b.to_async(&tokio_rt)
                        .iter(|| monitor_outputs_untyped_constraints(size))
                },
            );
        }
        group.bench_with_input(
            BenchmarkId::new("maple_sequence_untyped_async", size),
            &size,
            |b, &size| {
                b.to_async(&tokio_rt)
                    .iter(|| monitor_outputs_untyped_async(size))
            },
        );
        group.bench_with_input(
            BenchmarkId::new("maple_sequence_typed_async", size),
            &size,
            |b, &size| {
                b.to_async(&tokio_rt)
                    .iter(|| monitor_outputs_typed_async(size))
            },
        );
        group.bench_with_input(
            BenchmarkId::new("maple_sequence_untyped_queuing", size),
            &size,
            |b, &size| {
                b.to_async(&tokio_rt)
                    .iter(|| monitor_outputs_untyped_queuing(size))
            },
        );
        group.bench_with_input(
            BenchmarkId::new("maple_sequence_typed_queuing", size),
            &size,
            |b, &size| {
                b.to_async(&tokio_rt)
                    .iter(|| monitor_outputs_typed_queuing(size))
            },
        );
    }
    group.finish();
}

criterion_group!(benches, from_elem);
criterion_main!(benches);
