use std::{collections::BTreeMap, pin::Pin};

// use criterion::async_executor::TokioExecutor;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::SamplingMode;
use criterion::{criterion_group, criterion_main};
use futures::stream::{self, BoxStream};
use trustworthiness_checker::OutputStream;
use trustworthiness_checker::dependencies::traits::DependencyKind;
use trustworthiness_checker::dependencies::traits::create_dependency_manager;
use trustworthiness_checker::io::testing::null_output_handler::NullOutputHandler;
use trustworthiness_checker::lang::dynamic_lola::type_checker::type_check;
use trustworthiness_checker::{Monitor, Value, VarName};

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

pub fn input_streams_concrete(size: usize) -> BTreeMap<VarName, BoxStream<'static, Value>> {
    let size = size as i64;
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        VarName("x".into()),
        Box::pin(stream::iter((0..size).map(|x| Value::Int(2 * x))))
            as Pin<Box<dyn futures::Stream<Item = Value> + std::marker::Send>>,
    );
    input_streams.insert(
        VarName("y".into()),
        Box::pin(stream::iter((0..size).map(|y| Value::Int(2 * y + 1))))
            as Pin<Box<dyn futures::Stream<Item = Value> + std::marker::Send>>,
    );
    input_streams
}

pub fn input_streams_typed(size: usize) -> BTreeMap<VarName, OutputStream<Value>> {
    let mut input_streams = BTreeMap::new();
    let size = size as i64;
    input_streams.insert(
        VarName("x".into()),
        Box::pin(stream::iter((0..size).map(|x| Value::Int(2 * x)))) as OutputStream<Value>,
    );
    input_streams.insert(
        VarName("y".into()),
        Box::pin(stream::iter((0..size).map(|y| Value::Int(2 * y + 1)))),
    );
    input_streams
}

async fn monitor_outputs_untyped_constraints(num_outputs: usize) {
    let mut input_streams = input_streams_concrete(num_outputs);
    let spec = trustworthiness_checker::lola_specification(&mut spec_simple_add_monitor()).unwrap();
    let output_handler = Box::new(NullOutputHandler::new(spec.output_vars.clone()));
    let async_monitor = trustworthiness_checker::runtime::constraints::ConstraintBasedMonitor::new(
        spec.clone(),
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, Box::new(spec)),
    );
    async_monitor.run().await;
}

async fn monitor_outputs_untyped_async(num_outputs: usize) {
    let mut input_streams = input_streams_concrete(num_outputs);
    let spec = trustworthiness_checker::lola_specification(&mut spec_simple_add_monitor()).unwrap();
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
        create_dependency_manager(DependencyKind::Empty, Box::new(spec)),
    );
    async_monitor.run().await;
}

async fn monitor_outputs_typed_async(num_outputs: usize) {
    let mut input_streams = input_streams_typed(num_outputs);
    let spec_untyped =
        trustworthiness_checker::lola_specification(&mut spec_simple_add_monitor_typed()).unwrap();
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
        create_dependency_manager(DependencyKind::Empty, Box::new(spec_untyped)),
    );
    async_monitor.run().await;
}

async fn monitor_outputs_untyped_queuing(num_outputs: usize) {
    let mut input_streams = input_streams_concrete(num_outputs);
    let spec = trustworthiness_checker::lola_specification(&mut spec_simple_add_monitor()).unwrap();
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
        create_dependency_manager(DependencyKind::Empty, Box::new(spec)),
    );
    async_monitor.run().await;
}

async fn monitor_outputs_typed_queuing(num_outputs: usize) {
    let mut input_streams = input_streams_typed(num_outputs);
    let spec_untyped =
        trustworthiness_checker::lola_specification(&mut spec_simple_add_monitor_typed()).unwrap();
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
        create_dependency_manager(DependencyKind::Empty, Box::new(spec_untyped)),
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

    let mut group = c.benchmark_group("simple_add");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));

    for size in sizes {
        group.bench_with_input(
            BenchmarkId::new("simple_add_constraints", size),
            &size,
            |b, &size| {
                b.to_async(&tokio_rt)
                    .iter(|| monitor_outputs_untyped_constraints(size))
            },
        );
        group.bench_with_input(
            BenchmarkId::new("simple_add_untyped_async", size),
            &size,
            |b, &size| {
                b.to_async(&tokio_rt)
                    .iter(|| monitor_outputs_untyped_async(size))
            },
        );
        group.bench_with_input(
            BenchmarkId::new("simple_add_typed_async", size),
            &size,
            |b, &size| {
                b.to_async(&tokio_rt)
                    .iter(|| monitor_outputs_typed_async(size))
            },
        );
        group.bench_with_input(
            BenchmarkId::new("simple_add_untyped_queuing", size),
            &size,
            |b, &size| {
                b.to_async(&tokio_rt)
                    .iter(|| monitor_outputs_untyped_queuing(size))
            },
        );
        group.bench_with_input(
            BenchmarkId::new("simple_add_typed_queuing", size),
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
