use std::collections::{BTreeMap, BTreeSet};
use std::hint::black_box;
use std::rc::Rc;
use std::time::Duration;

use criterion::async_executor::AsyncExecutor;
use criterion::{BenchmarkId, Criterion, SamplingMode, criterion_group, criterion_main};
use futures::StreamExt;
use mstlo::{
    Algorithm, DelayedQualitative, DelayedQuantitative, FormulaDefinition, Step, StlMonitor,
    SynchronizationStrategy, Variables, parse_stl,
};
use smol::LocalExecutor;
use trustworthiness_checker::core::{Runtime, RuntimeSpec, Semantics};
use trustworthiness_checker::io::testing::{ManualOutputHandler, NullOutputHandler};
use trustworthiness_checker::lang::mstlo::{MstloSpecification, parse_named_properties};
use trustworthiness_checker::runtime::{GeneralRuntimeBuilder, RuntimeBuilder};
use trustworthiness_checker::{InputStream, io::map};
use trustworthiness_checker::{UntypedDsrvSpecification, Value, VarName, dsrv_specification};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Clone)]
struct LocalSmolExecutor {
    executor: Rc<LocalExecutor<'static>>,
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

fn dsrv_time_dependent_spec() -> UntypedDsrvSpecification {
    dsrv_specification(
        &mut r#"
in x: Float
out always_x: Bool
aux above: Bool
above = x > 3.0
always_x = above && default(above[1], true) && default(above[2], true)
"#,
    )
    .expect("DSRV time-dependent benchmark spec should parse")
}

fn mstlo_time_dependent_spec() -> MstloSpecification {
    parse_named_properties("always_x: G[0,2](x > 3)")
        .expect("MSTLO time-dependent benchmark spec should parse")
}

fn mstlo_time_dependent_formula() -> FormulaDefinition {
    parse_stl("G[0,2](x > 3)").expect("MSTLO time-dependent benchmark formula should parse")
}

fn dsrv_input(size: usize) -> InputStream<Value> {
    let values = (0..size)
        .map(|idx| Value::Float(if idx % 8 == 3 { 2.0 } else { 5.0 }))
        .collect::<Vec<_>>();
    map::input_stream(BTreeMap::from([(VarName::new("x"), values)]))
}

fn timed_value(time_ms: i64, value: f64) -> Value {
    Value::List(vec![Value::Int(time_ms), Value::Float(value)].into())
}

fn mstlo_input(size: usize) -> InputStream<Value> {
    let values = (0..size)
        .map(|idx| {
            let value = if idx % 8 == 3 { 2.0 } else { 5.0 };
            timed_value((idx as i64) * 1000, value)
        })
        .collect::<Vec<_>>();
    map::input_stream(BTreeMap::from([(VarName::new("x"), values)]))
}

fn mstlo_direct_input(size: usize) -> Vec<Step<f64>> {
    (0..size)
        .map(|idx| {
            let value = if idx % 8 == 3 { 2.0 } else { 5.0 };
            Step::new("x", value, Duration::from_millis((idx as u64) * 1000))
        })
        .collect()
}

async fn run_dsrv(
    executor: Rc<LocalExecutor<'static>>,
    spec: UntypedDsrvSpecification,
    input: InputStream<Value>,
    runtime_spec: RuntimeSpec,
) {
    let output = Box::new(NullOutputHandler::new(
        executor.clone(),
        BTreeSet::from([VarName::new("always_x")]),
    ));

    let runtime = GeneralRuntimeBuilder::new()
        .executor(executor)
        .model(spec)
        .input(input)
        .output(output)
        .runtime(runtime_spec)
        .semantics(Semantics::GradualTypedUntimed)
        .build()
        .await;

    runtime.run().await.expect("DSRV benchmark runtime failed");
}

async fn run_dsrv_counted(
    executor: Rc<LocalExecutor<'static>>,
    spec: UntypedDsrvSpecification,
    input: InputStream<Value>,
    runtime_spec: RuntimeSpec,
    expected_outputs: usize,
) {
    let mut output = Box::new(ManualOutputHandler::new(
        executor.clone(),
        BTreeSet::from([VarName::new("always_x")]),
    ));
    let outputs = output.get_output();

    let runtime = GeneralRuntimeBuilder::new()
        .executor(executor)
        .model(spec)
        .input(input)
        .output(output)
        .runtime(runtime_spec)
        .semantics(Semantics::GradualTypedUntimed)
        .build()
        .await;

    let (runtime_result, outputs) = futures::join!(runtime.run(), outputs.collect::<Vec<_>>());
    runtime_result.expect("DSRV counted benchmark runtime failed");
    assert_eq!(
        outputs.len(),
        expected_outputs,
        "DSRV counted benchmark produced the wrong number of outputs"
    );
    black_box(outputs.len());
}

async fn run_mstlo(
    executor: Rc<LocalExecutor<'static>>,
    spec: MstloSpecification,
    input: InputStream<Value>,
    semantics: Semantics,
) {
    let output = Box::new(NullOutputHandler::new(
        executor.clone(),
        BTreeSet::from([VarName::new("always_x")]),
    ));

    let runtime = GeneralRuntimeBuilder::new()
        .executor(executor)
        .model(spec)
        .input(input)
        .output(output)
        .semantics(semantics)
        .build()
        .await;

    runtime.run().await.expect("MSTLO benchmark runtime failed");
}

fn mstlo_direct_monitor_builder(formula: FormulaDefinition) -> mstlo::StlMonitorBuilder<f64, f64> {
    StlMonitor::builder()
        .formula(formula)
        .algorithm(Algorithm::Incremental)
        .synchronization_strategy(SynchronizationStrategy::None)
        .variables(Variables::new())
}

fn run_mstlo_direct_qual(formula: FormulaDefinition, input: Vec<Step<f64>>) {
    let mut monitor = mstlo_direct_monitor_builder(formula)
        .semantics(DelayedQualitative)
        .build()
        .expect("Failed to build direct MSTLO qualitative monitor");

    let mut outputs = 0usize;
    for step in input {
        for verdict in monitor.update(&step).into_verdicts() {
            black_box(verdict);
            outputs += 1;
        }
    }
    black_box(outputs);
}

fn run_mstlo_direct_quant(formula: FormulaDefinition, input: Vec<Step<f64>>) {
    let mut monitor = mstlo_direct_monitor_builder(formula)
        .semantics(DelayedQuantitative)
        .build()
        .expect("Failed to build direct MSTLO quantitative monitor");

    let mut outputs = 0usize;
    for step in input {
        for verdict in monitor.update(&step).into_verdicts() {
            black_box(verdict);
            outputs += 1;
        }
    }
    black_box(outputs);
}

fn compare_time_dependent_property_diagnostics(c: &mut Criterion) {
    let size = 1_000;
    let local_smol_executor = LocalSmolExecutor::new();
    let dsrv_spec = dsrv_time_dependent_spec();

    let mut group = c.benchmark_group("time_dependent_property_diagnostics");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(2));

    // group.bench_with_input(
    //     BenchmarkId::new("dsrv_counted_window_async", size),
    //     &size,
    //     |b, &size| {
    //         b.to_async(local_smol_executor.clone()).iter(|| {
    //             run_dsrv_counted(
    //                 local_smol_executor.executor.clone(),
    //                 black_box(dsrv_spec.clone()),
    //                 dsrv_input(size),
    //                 RuntimeSpec::Async,
    //                 size,
    //             )
    //         })
    //     },
    // );

    group.bench_with_input(
        BenchmarkId::new("dsrv_counted_window_semisync", size),
        &size,
        |b, &size| {
            b.to_async(local_smol_executor.clone()).iter(|| {
                run_dsrv_counted(
                    local_smol_executor.executor.clone(),
                    black_box(dsrv_spec.clone()),
                    dsrv_input(size),
                    RuntimeSpec::SemiSync,
                    size,
                )
            })
        },
    );

    group.finish();
}

fn compare_time_dependent_property(c: &mut Criterion) {
    let sizes = [100, 1_000, 5_000, 10_000];
    let local_smol_executor = LocalSmolExecutor::new();
    let dsrv_spec = dsrv_time_dependent_spec();

    let mstlo_spec = mstlo_time_dependent_spec();

    let mstlo_formula = mstlo_time_dependent_formula();

    let mut group = c.benchmark_group("time_dependent_property");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));

    for size in sizes {
        // group.bench_with_input(
        //     BenchmarkId::new("dsrv_default_window_async", size),
        //     &size,
        //     |b, &size| {
        //         b.to_async(local_smol_executor.clone()).iter(|| {
        //             run_dsrv(
        //                 local_smol_executor.executor.clone(),
        //                 dsrv_spec.clone(),
        //                 dsrv_input(size),
        //                 RuntimeSpec::Async,
        //             )
        //         })
        //     },
        // );

        group.bench_with_input(
            BenchmarkId::new("dsrv_default_window_semisync", size),
            &size,
            |b, &size| {
                b.to_async(local_smol_executor.clone()).iter(|| {
                    run_dsrv(
                        local_smol_executor.executor.clone(),
                        dsrv_spec.clone(),
                        dsrv_input(size),
                        RuntimeSpec::SemiSync,
                    )
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mstlo_globally_window_qual", size),
            &size,
            |b, &size| {
                b.to_async(local_smol_executor.clone()).iter(|| {
                    run_mstlo(
                        local_smol_executor.executor.clone(),
                        mstlo_spec.clone(),
                        mstlo_input(size),
                        Semantics::DelayedQualitative,
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("mstlo_globally_window_quant", size),
            &size,
            |b, &size| {
                b.to_async(local_smol_executor.clone()).iter(|| {
                    run_mstlo(
                        local_smol_executor.executor.clone(),
                        mstlo_spec.clone(),
                        mstlo_input(size),
                        Semantics::DelayedQuantitative,
                    )
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mstlo_direct_globally_window_qual", size),
            &size,
            |b, &size| {
                b.iter(|| {
                    run_mstlo_direct_qual(
                        black_box(mstlo_formula.clone()),
                        black_box(mstlo_direct_input(size)),
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("mstlo_direct_globally_window_quant", size),
            &size,
            |b, &size| {
                b.iter(|| {
                    run_mstlo_direct_quant(
                        black_box(mstlo_formula.clone()),
                        black_box(mstlo_direct_input(size)),
                    )
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    compare_time_dependent_property,
    compare_time_dependent_property_diagnostics
);
criterion_main!(benches);
