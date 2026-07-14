use std::collections::{BTreeMap, BTreeSet};
use std::hint::black_box;
use std::rc::Rc;
use std::time::Duration;

use criterion::async_executor::AsyncExecutor;
use criterion::{BenchmarkId, Criterion, SamplingMode, criterion_group, criterion_main};
use mstlo::{
    Algorithm, DelayedQualitative, DelayedQuantitative, FormulaDefinition, Step, StlMonitor,
    SynchronizationStrategy, Variables, parse_stl,
};
use smol::LocalExecutor;
use trustworthiness_checker::core::{Runtime, RuntimeSpec, Semantics, Specification};
use trustworthiness_checker::io::testing::NullOutputHandler;
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

fn dsrv_threshold_spec() -> UntypedDsrvSpecification {
    dsrv_specification(
        &mut r#"
in x: Float
out always_x: Bool
always_x = x > 3.0
"#,
    )
    .expect("DSRV threshold benchmark spec should parse")
}

fn mstlo_threshold_spec() -> MstloSpecification {
    parse_named_properties("always_x: x > 3").expect("MSTLO threshold benchmark spec should parse")
}

fn mstlo_threshold_multi_output_spec(outputs: usize) -> MstloSpecification {
    let spec = (0..outputs)
        .map(|idx| format!("out_{idx}: x > 3"))
        .collect::<Vec<_>>()
        .join("\n");
    parse_named_properties(&spec).expect("MSTLO multi-output threshold benchmark spec should parse")
}

fn mstlo_threshold_formula() -> FormulaDefinition {
    parse_stl("x > 3").expect("MSTLO threshold benchmark formula should parse")
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

fn timed_map_value(time_ms: i64, value: f64) -> Value {
    Value::Map(BTreeMap::from([
        ("time".into(), Value::Int(time_ms)),
        ("value".into(), Value::Float(value)),
    ]))
}

fn mstlo_map_input(size: usize) -> InputStream<Value> {
    let values = (0..size)
        .map(|idx| {
            let value = if idx % 8 == 3 { 2.0 } else { 5.0 };
            timed_map_value((idx as i64) * 1000, value)
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

fn mstlo_value_input(size: usize) -> Vec<Value> {
    (0..size)
        .map(|idx| {
            let value = if idx % 8 == 3 { 2.0 } else { 5.0 };
            timed_value((idx as i64) * 1000, value)
        })
        .collect()
}

async fn run_dsrv_with_semantics(
    executor: Rc<LocalExecutor<'static>>,
    spec: UntypedDsrvSpecification,
    input: InputStream<Value>,
    runtime_spec: RuntimeSpec,
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
        .runtime(runtime_spec)
        .semantics(semantics)
        .build()
        .await;

    runtime
        .run()
        .await
        .expect("DSRV threshold benchmark runtime failed");
}

async fn run_dsrv(
    executor: Rc<LocalExecutor<'static>>,
    spec: UntypedDsrvSpecification,
    input: InputStream<Value>,
    runtime_spec: RuntimeSpec,
) {
    run_dsrv_with_semantics(
        executor,
        spec,
        input,
        runtime_spec,
        Semantics::GradualTypedUntimed,
    )
    .await;
}

async fn run_mstlo(
    executor: Rc<LocalExecutor<'static>>,
    spec: MstloSpecification,
    input: InputStream<Value>,
    semantics: Semantics,
) {
    let output = Box::new(NullOutputHandler::new(executor.clone(), spec.output_vars()));

    let runtime = GeneralRuntimeBuilder::new()
        .executor(executor)
        .model(spec)
        .input(input)
        .output(output)
        .semantics(semantics)
        .build()
        .await;

    runtime
        .run()
        .await
        .expect("MSTLO threshold benchmark runtime failed");
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
        .expect("Failed to build direct MSTLO qualitative threshold monitor");

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
        .expect("Failed to build direct MSTLO quantitative threshold monitor");

    let mut outputs = 0usize;
    for step in input {
        for verdict in monitor.update(&step).into_verdicts() {
            black_box(verdict);
            outputs += 1;
        }
    }
    black_box(outputs);
}

fn run_mstlo_direct_multi_qual(formula: FormulaDefinition, input: Vec<Step<f64>>, outputs: usize) {
    let mut monitors = (0..outputs)
        .map(|_| {
            mstlo_direct_monitor_builder(formula.clone())
                .semantics(DelayedQualitative)
                .build()
                .expect("Failed to build direct MSTLO qualitative threshold monitor")
        })
        .collect::<Vec<_>>();

    let mut verdicts = 0usize;
    for step in input {
        for monitor in monitors.iter_mut() {
            for verdict in monitor.update(&step).into_verdicts() {
                black_box(verdict);
                verdicts += 1;
            }
        }
    }
    black_box(verdicts);
}

fn run_mstlo_direct_with_value_adapter_qual(formula: FormulaDefinition, input: Vec<Value>) {
    let mut monitor = mstlo_direct_monitor_builder(formula)
        .semantics(DelayedQualitative)
        .build()
        .expect("Failed to build direct MSTLO qualitative threshold monitor");

    let mut outputs = 0usize;
    for value in input {
        let Value::List(values) = value else {
            panic!("expected compact MSTLO value input")
        };
        let [time, value]: [Value; 2] = values
            .into_iter()
            .collect::<Vec<_>>()
            .try_into()
            .unwrap_or_else(|_| panic!("expected two-element MSTLO value input"));
        let time_ms = match time {
            Value::Int(time_ms) => time_ms,
            other => panic!("expected integer MSTLO time input, got {other:?}"),
        };
        let value = match value {
            Value::Float(value) => value,
            Value::Int(value) => value as f64,
            other => panic!("expected numeric MSTLO value input, got {other:?}"),
        };
        let step = Step::new("x", value, Duration::from_millis(time_ms as u64));

        for verdict in monitor.update(&step).into_verdicts() {
            let output_value = Value::Map(BTreeMap::from([
                (
                    "time".into(),
                    Value::Int(verdict.timestamp.as_millis() as i64),
                ),
                ("value".into(), Value::Bool(verdict.value)),
            ]));
            black_box(output_value);
            outputs += 1;
        }
    }
    black_box(outputs);
}

fn compare_threshold_property_diagnostics(c: &mut Criterion) {
    let size = 1_000;
    let local_smol_executor = LocalSmolExecutor::new();
    let dsrv_spec = dsrv_threshold_spec();
    let mstlo_spec = mstlo_threshold_spec();
    let mstlo_formula = mstlo_threshold_formula();

    let mut group = c.benchmark_group("threshold_property_diagnostics");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(2));

    group.bench_with_input(
        BenchmarkId::new("dsrv_semisync_untyped", size),
        &size,
        |b, &size| {
            b.to_async(local_smol_executor.clone()).iter(|| {
                run_dsrv_with_semantics(
                    local_smol_executor.executor.clone(),
                    black_box(dsrv_spec.clone()),
                    dsrv_input(size),
                    RuntimeSpec::SemiSync,
                    Semantics::Untimed,
                )
            })
        },
    );

    group.bench_with_input(
        BenchmarkId::new("dsrv_dataflow_untyped", size),
        &size,
        |b, &size| {
            b.to_async(local_smol_executor.clone()).iter(|| {
                run_dsrv_with_semantics(
                    local_smol_executor.executor.clone(),
                    black_box(dsrv_spec.clone()),
                    dsrv_input(size),
                    RuntimeSpec::Dataflow(Default::default()),
                    Semantics::Untimed,
                )
            })
        },
    );

    group.bench_with_input(
        BenchmarkId::new("dsrv_semisync_gradual_typed", size),
        &size,
        |b, &size| {
            b.to_async(local_smol_executor.clone()).iter(|| {
                run_dsrv_with_semantics(
                    local_smol_executor.executor.clone(),
                    black_box(dsrv_spec.clone()),
                    dsrv_input(size),
                    RuntimeSpec::SemiSync,
                    Semantics::GradualTypedUntimed,
                )
            })
        },
    );

    group.bench_with_input(
        BenchmarkId::new("dsrv_dataflow_gradual_typed", size),
        &size,
        |b, &size| {
            b.to_async(local_smol_executor.clone()).iter(|| {
                run_dsrv_with_semantics(
                    local_smol_executor.executor.clone(),
                    black_box(dsrv_spec.clone()),
                    dsrv_input(size),
                    RuntimeSpec::Dataflow(Default::default()),
                    Semantics::GradualTypedUntimed,
                )
            })
        },
    );

    group.bench_with_input(
        BenchmarkId::new("mstlo_runtime_qual_list_input", size),
        &size,
        |b, &size| {
            b.to_async(local_smol_executor.clone()).iter(|| {
                run_mstlo(
                    local_smol_executor.executor.clone(),
                    black_box(mstlo_spec.clone()),
                    mstlo_input(size),
                    Semantics::DelayedQualitative,
                )
            })
        },
    );

    group.bench_with_input(
        BenchmarkId::new("mstlo_runtime_qual_map_input", size),
        &size,
        |b, &size| {
            b.to_async(local_smol_executor.clone()).iter(|| {
                run_mstlo(
                    local_smol_executor.executor.clone(),
                    black_box(mstlo_spec.clone()),
                    mstlo_map_input(size),
                    Semantics::DelayedQualitative,
                )
            })
        },
    );

    group.bench_with_input(
        BenchmarkId::new("mstlo_direct_value_adapter_qual", size),
        &size,
        |b, &size| {
            b.iter(|| {
                run_mstlo_direct_with_value_adapter_qual(
                    black_box(mstlo_formula.clone()),
                    black_box(mstlo_value_input(size)),
                )
            })
        },
    );

    for outputs in [1usize, 2, 4, 8] {
        let spec = mstlo_threshold_multi_output_spec(outputs);
        group.bench_with_input(
            BenchmarkId::new("mstlo_runtime_output_streams", outputs),
            &outputs,
            |b, &_outputs| {
                b.to_async(local_smol_executor.clone()).iter(|| {
                    run_mstlo(
                        local_smol_executor.executor.clone(),
                        black_box(spec.clone()),
                        mstlo_input(size),
                        Semantics::DelayedQualitative,
                    )
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mstlo_direct_output_streams", outputs),
            &outputs,
            |b, &outputs| {
                b.iter(|| {
                    run_mstlo_direct_multi_qual(
                        black_box(mstlo_formula.clone()),
                        black_box(mstlo_direct_input(size)),
                        outputs,
                    )
                })
            },
        );
    }

    group.finish();
}

fn compare_threshold_property(c: &mut Criterion) {
    let sizes = [100, 1_000, 5_000, 10_000];
    let local_smol_executor = LocalSmolExecutor::new();
    let dsrv_spec = dsrv_threshold_spec();
    let mstlo_spec = mstlo_threshold_spec();
    let mstlo_formula = mstlo_threshold_formula();

    let mut group = c.benchmark_group("threshold_property");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));

    for size in sizes {
        // group.bench_with_input(BenchmarkId::new("dsrv_async", size), &size, |b, &size| {
        //     b.to_async(local_smol_executor.clone()).iter(|| {
        //         run_dsrv(
        //             local_smol_executor.executor.clone(),
        //             dsrv_spec.clone(),
        //             dsrv_input(size),
        //             RuntimeSpec::Async,
        //         )
        //     })
        // });

        group.bench_with_input(
            BenchmarkId::new("dsrv_semisync", size),
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
            BenchmarkId::new("dsrv_dataflow", size),
            &size,
            |b, &size| {
                b.to_async(local_smol_executor.clone()).iter(|| {
                    run_dsrv(
                        local_smol_executor.executor.clone(),
                        dsrv_spec.clone(),
                        dsrv_input(size),
                        RuntimeSpec::Dataflow(Default::default()),
                    )
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mstlo_runtime_qual", size),
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
            BenchmarkId::new("mstlo_runtime_quant", size),
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
            BenchmarkId::new("mstlo_direct_qual", size),
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
            BenchmarkId::new("mstlo_direct_quant", size),
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
    compare_threshold_property,
    compare_threshold_property_diagnostics
);
criterion_main!(benches);
