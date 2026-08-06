use std::collections::BTreeMap;
use std::hint::black_box;
use std::rc::Rc;
use std::time::Duration;

use criterion::async_executor::AsyncExecutor;
use criterion::{BenchmarkId, Criterion, SamplingMode, criterion_group, criterion_main};
use mstlo::{
    Algorithm, DelayedQualitative, FormulaDefinition, Semantics as MstloSemantics, Step,
    StlMonitor, SynchronizationStrategy, TimeInterval, Variables,
};
use smol::LocalExecutor;
use trustworthiness_checker::core::{Runtime, Specification};
use trustworthiness_checker::io::map;
use trustworthiness_checker::io::testing::NullOutputHandler;
use trustworthiness_checker::lang::mstlo::MstloSpecification;
use trustworthiness_checker::runtime::RuntimeBuilder;
use trustworthiness_checker::runtime::mstlo::{MstloRuntimeBuilder, MstloTimedValue, MstloValue};
use trustworthiness_checker::{InputStream, VarName};

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

fn robot_signals(robots: usize) -> Vec<&'static str> {
    (0..robots)
        .map(|robot| Box::leak(format!("robot_{robot}").into_boxed_str()) as &'static str)
        .collect()
}

fn confined_formula(signal: &'static str) -> FormulaDefinition {
    FormulaDefinition::GreaterThan(signal, 0.0)
}

fn dwell_formula(signal: &'static str) -> FormulaDefinition {
    FormulaDefinition::Globally(
        TimeInterval {
            start: Duration::ZERO,
            end: Duration::from_millis(100),
        },
        Box::new(FormulaDefinition::Implies(
            Box::new(FormulaDefinition::GreaterThan(signal, 0.0)),
            Box::new(FormulaDefinition::Eventually(
                TimeInterval {
                    start: Duration::ZERO,
                    end: Duration::from_millis(20),
                },
                Box::new(FormulaDefinition::LessThan(signal, 0.0)),
            )),
        )),
    )
}

fn occupancy_formula(signals: &[&'static str]) -> FormulaDefinition {
    signals
        .iter()
        .copied()
        .map(|signal| confined_formula(signal))
        .reduce(|left, right| FormulaDefinition::Or(Box::new(left), Box::new(right)))
        .expect("occupancy benchmark requires at least one robot")
}

fn per_robot_spec(
    signals: &[&'static str],
    formula: fn(&'static str) -> FormulaDefinition,
) -> MstloSpecification {
    MstloSpecification::new(
        signals
            .iter()
            .enumerate()
            .map(|(robot, signal)| (VarName::new(&format!("property_{robot}")), formula(signal)))
            .collect(),
    )
}

fn occupancy_spec(signals: &[&'static str]) -> MstloSpecification {
    MstloSpecification::single(VarName::new("occupancy"), occupancy_formula(signals))
}

fn shared_signal_spec(monitors: usize, signal: &'static str) -> MstloSpecification {
    MstloSpecification::new(
        (0..monitors)
            .map(|monitor| {
                (
                    VarName::new(&format!("property_{monitor}")),
                    confined_formula(signal),
                )
            })
            .collect(),
    )
}

fn direct_trace(signals: &[&'static str], samples: usize) -> Vec<Step<f64>> {
    (0..samples)
        .flat_map(|sample| {
            signals.iter().enumerate().map(move |(robot, signal)| {
                let value = if (sample + robot) % 8 == 3 { -1.0 } else { 1.0 };
                Step::new(*signal, value, Duration::from_millis(sample as u64))
            })
        })
        .collect()
}

fn runtime_input(signals: &[&'static str], samples: usize) -> InputStream<MstloTimedValue> {
    let values = signals
        .iter()
        .enumerate()
        .map(|(robot, signal)| {
            let values = (0..samples)
                .map(|sample| {
                    let value = if (sample + robot) % 8 == 3 { -1.0 } else { 1.0 };
                    MstloTimedValue::new(
                        Duration::from_millis(sample as u64),
                        MstloValue::Float(value),
                    )
                })
                .collect();
            (VarName::new(signal), values)
        })
        .collect::<BTreeMap<_, Vec<_>>>();
    map::typed_input_stream(values)
}

fn direct_monitor(formula: FormulaDefinition) -> StlMonitor<f64, bool> {
    StlMonitor::builder()
        .formula(formula)
        .algorithm(Algorithm::Incremental)
        .synchronization_strategy(SynchronizationStrategy::ZeroOrderHold)
        .variables(Variables::new())
        .semantics(DelayedQualitative)
        .build()
        .expect("fan-out benchmark monitor should build")
}

fn consume_update(monitor: &mut StlMonitor<f64, bool>, step: &Step<f64>) -> usize {
    monitor.update(step).into_verdicts().len()
}

fn run_direct_fanout(formulae: &[FormulaDefinition], trace: &[Step<f64>]) {
    let mut monitors = formulae
        .iter()
        .cloned()
        .map(direct_monitor)
        .collect::<Vec<_>>();
    let mut outputs = 0usize;
    for step in trace {
        for monitor in &mut monitors {
            outputs += consume_update(monitor, step);
        }
    }
    black_box(outputs);
}

fn run_direct_signal_index(
    formulae: &[FormulaDefinition],
    signals: &[&'static str],
    trace: &[Step<f64>],
) {
    let mut monitors = formulae
        .iter()
        .cloned()
        .map(direct_monitor)
        .collect::<Vec<_>>();
    let mut by_signal = BTreeMap::<&'static str, Vec<usize>>::new();
    for (index, signal) in signals.iter().copied().enumerate() {
        by_signal.entry(signal).or_default().push(index);
    }

    let mut outputs = 0usize;
    for step in trace {
        if let Some(indices) = by_signal.get(step.signal) {
            for &index in indices {
                outputs += consume_update(&mut monitors[index], step);
            }
        }
    }
    black_box(outputs);
}

/// Mirrors the production route: referenced samples plus one timestamp clock
/// sample for monitors that did not receive a referenced sample at that time.
fn run_direct_clocked(
    formulae: &[FormulaDefinition],
    signals: &[&'static str],
    trace: &[Step<f64>],
) {
    let mut monitors = formulae
        .iter()
        .cloned()
        .map(direct_monitor)
        .collect::<Vec<_>>();
    let mut by_signal = BTreeMap::<&'static str, Vec<usize>>::new();
    for (index, signal) in signals.iter().copied().enumerate() {
        by_signal.entry(signal).or_default().push(index);
    }

    let mut last_timestamp = None;
    let mut outputs = 0usize;
    for step in trace {
        let relevant = by_signal.get(step.signal);
        if last_timestamp.map_or(true, |last| step.timestamp > last) {
            for index in 0..monitors.len() {
                let receives_real_step = relevant.is_some_and(|indices| indices.contains(&index));
                if !receives_real_step {
                    outputs += consume_update(&mut monitors[index], step);
                }
            }
            last_timestamp = Some(step.timestamp);
        }
        if let Some(indices) = relevant {
            for &index in indices {
                outputs += consume_update(&mut monitors[index], step);
            }
        }
    }
    black_box(outputs);
}

async fn run_runtime(
    executor: Rc<LocalExecutor<'static>>,
    specification: MstloSpecification,
    input: InputStream<MstloTimedValue>,
) {
    let output = Box::new(NullOutputHandler::<MstloTimedValue>::new(
        executor.clone(),
        specification.output_vars(),
    ));
    let runtime = MstloRuntimeBuilder::<MstloTimedValue>::new()
        .executor(executor)
        .model(specification)
        .input(input)
        .output(output)
        .semantics(MstloSemantics::DelayedQualitative)
        .synchronization_strategy(SynchronizationStrategy::ZeroOrderHold)
        .build()
        .await;
    runtime
        .run()
        .await
        .expect("MSTLO fan-out benchmark runtime failed");
}

fn benchmark_many_monitors(c: &mut Criterion) {
    const SAMPLES: usize = 2_000;
    let mut group = c.benchmark_group("mstlo_many_monitors");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(2));

    for robots in [1usize, 10, 50] {
        let signals = robot_signals(robots);
        let trace = direct_trace(&signals, SAMPLES);
        let confined = signals
            .iter()
            .copied()
            .map(confined_formula)
            .collect::<Vec<_>>();
        let dwell = signals
            .iter()
            .copied()
            .map(dwell_formula)
            .collect::<Vec<_>>();
        let occupancy = vec![occupancy_formula(&signals)];
        let confined_spec = per_robot_spec(&signals, confined_formula);
        let dwell_spec = per_robot_spec(&signals, dwell_formula);
        let occupancy_spec = occupancy_spec(&signals);
        let shared_signal_spec = shared_signal_spec(robots, signals[0]);

        group.bench_with_input(
            BenchmarkId::new("direct_confined_fanout", robots),
            &robots,
            |b, _| b.iter(|| run_direct_fanout(black_box(&confined), black_box(&trace))),
        );
        group.bench_with_input(
            BenchmarkId::new("direct_confined_signal_index", robots),
            &robots,
            |b, _| {
                b.iter(|| {
                    run_direct_signal_index(black_box(&confined), &signals, black_box(&trace))
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("direct_dwell_fanout", robots),
            &robots,
            |b, _| b.iter(|| run_direct_fanout(black_box(&dwell), black_box(&trace))),
        );
        group.bench_with_input(
            BenchmarkId::new("direct_dwell_clocked", robots),
            &robots,
            |b, _| b.iter(|| run_direct_clocked(black_box(&dwell), &signals, black_box(&trace))),
        );
        group.bench_with_input(
            BenchmarkId::new("direct_occupancy_single", robots),
            &robots,
            |b, _| b.iter(|| run_direct_fanout(black_box(&occupancy), black_box(&trace))),
        );

        for (name, specification) in [
            ("runtime_confined", confined_spec.clone()),
            ("runtime_dwell", dwell_spec.clone()),
            ("runtime_occupancy", occupancy_spec.clone()),
        ] {
            let benchmark_executor = LocalSmolExecutor::new();
            group.bench_with_input(BenchmarkId::new(name, robots), &robots, |b, _| {
                b.to_async(benchmark_executor.clone()).iter(|| {
                    run_runtime(
                        benchmark_executor.executor.clone(),
                        black_box(specification.clone()),
                        runtime_input(&signals, SAMPLES),
                    )
                })
            });
        }

        let benchmark_executor = LocalSmolExecutor::new();
        group.bench_with_input(
            BenchmarkId::new("runtime_shared_signal", robots),
            &robots,
            |b, _| {
                b.to_async(benchmark_executor.clone()).iter(|| {
                    run_runtime(
                        benchmark_executor.executor.clone(),
                        black_box(shared_signal_spec.clone()),
                        runtime_input(&signals[..1], SAMPLES),
                    )
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, benchmark_many_monitors);
criterion_main!(benches);
