use std::{collections::BTreeMap, pin::Pin};

// use criterion::async_executor::TokioExecutor;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::SamplingMode;
use criterion::{criterion_group, criterion_main};
use futures::{
    stream::{self, BoxStream},
    StreamExt,
};
use trustworthiness_checker::core::TypeCheckableSpecification;
use trustworthiness_checker::{lola_streams::LOLAStream, ConcreteStreamData, Monitor, VarName};

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

pub fn input_streams_concrete(
    size: usize,
) -> BTreeMap<VarName, BoxStream<'static, ConcreteStreamData>> {
    let size = size as i64;
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        VarName("x".into()),
        Box::pin(stream::iter(
            (0..size).map(|x| ConcreteStreamData::Int(2 * x)),
        )) as Pin<Box<dyn futures::Stream<Item = ConcreteStreamData> + std::marker::Send>>,
    );
    input_streams.insert(
        VarName("y".into()),
        Box::pin(stream::iter(
            (0..size).map(|y| ConcreteStreamData::Int(2 * y + 1)),
        )) as Pin<Box<dyn futures::Stream<Item = ConcreteStreamData> + std::marker::Send>>,
    );
    input_streams
}

pub fn input_streams_typed(size: usize) -> BTreeMap<VarName, LOLAStream> {
    let mut input_streams = BTreeMap::new();
    let size = size as i64;
    input_streams.insert(
        VarName("x".into()),
        LOLAStream::Int(Box::pin(stream::iter((0..size).map(|x| (2 * x))))),
    );
    input_streams.insert(
        VarName("y".into()),
        LOLAStream::Int(Box::pin(stream::iter((0..size).map(|y| 2 * y + 1)))),
    );
    input_streams
}

async fn monitor_outputs_untyped_constraints(num_outputs: usize) {
    let input_streams = input_streams_concrete(num_outputs);
    let spec = trustworthiness_checker::lola_specification(&mut spec_simple_add_monitor()).unwrap();
    let mut async_monitor =
        trustworthiness_checker::constraint_based_runtime::ConstraintBasedMonitor::new(
            spec,
            input_streams,
        );
    let _outputs: Vec<BTreeMap<VarName, ConcreteStreamData>> = async_monitor
        .monitor_outputs()
        .take(num_outputs)
        .collect()
        .await;
}

async fn monitor_outputs_untyped_async(num_outputs: usize) {
    let input_streams = input_streams_concrete(num_outputs);
    let spec = trustworthiness_checker::lola_specification(&mut spec_simple_add_monitor()).unwrap();
    let mut async_monitor = trustworthiness_checker::async_runtime::AsyncMonitorRunner::<
        _,
        _,
        trustworthiness_checker::UntimedLolaSemantics,
        trustworthiness_checker::LOLASpecification,
    >::new(spec, input_streams);
    let _outputs: Vec<BTreeMap<VarName, ConcreteStreamData>> = async_monitor
        .monitor_outputs()
        .take(num_outputs)
        .collect()
        .await;
}

async fn monitor_outputs_typed_async(num_outputs: usize) {
    let input_streams = input_streams_typed(num_outputs);
    let spec =
        trustworthiness_checker::lola_specification(&mut spec_simple_add_monitor_typed()).unwrap();
    let spec = spec.type_check().expect("Type check failed");
    let mut async_monitor = trustworthiness_checker::async_runtime::AsyncMonitorRunner::<
        _,
        _,
        trustworthiness_checker::TypedUntimedLolaSemantics,
        _,
    >::new(spec, input_streams);
    let _outputs: Vec<BTreeMap<VarName, _>> = async_monitor
        .monitor_outputs()
        .take(num_outputs)
        .collect()
        .await;
}

async fn monitor_outputs_untyped_queuing(num_outputs: usize) {
    let input_streams = input_streams_concrete(num_outputs);
    let spec = trustworthiness_checker::lola_specification(&mut spec_simple_add_monitor()).unwrap();
    let mut async_monitor = trustworthiness_checker::queuing_runtime::QueuingMonitorRunner::<
        _,
        _,
        trustworthiness_checker::UntimedLolaSemantics,
        trustworthiness_checker::LOLASpecification,
    >::new(spec, input_streams);
    let _outputs: Vec<BTreeMap<VarName, ConcreteStreamData>> = async_monitor
        .monitor_outputs()
        .take(num_outputs)
        .collect()
        .await;
}

async fn monitor_outputs_typed_queuing(num_outputs: usize) {
    let input_streams = input_streams_typed(num_outputs);
    let spec =
        trustworthiness_checker::lola_specification(&mut spec_simple_add_monitor_typed()).unwrap();
    let spec = spec.type_check().expect("Type check failed");
    let mut async_monitor = trustworthiness_checker::queuing_runtime::QueuingMonitorRunner::<
        _,
        _,
        trustworthiness_checker::TypedUntimedLolaSemantics,
        _,
    >::new(spec, input_streams);
    let _outputs: Vec<BTreeMap<VarName, _>> = async_monitor
        .monitor_outputs()
        // .take(num_outputs)
        .collect()
        .await;
}

fn from_elem(c: &mut Criterion) {
    let sizes = vec![1, 10, 100, 1000, 10000, 100000, 1000000];

    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("simple_add");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(30));

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
