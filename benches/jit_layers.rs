#![cfg(feature = "jit")]

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::hint::black_box;
use std::time::Duration;

use criterion::measurement::WallTime;
use criterion::{
    BatchSize, BenchmarkGroup, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use trustworthiness_checker::dataflow::{
    DataflowMonitor, JitConfig, JitPlan, TypedDataflowMonitor, TypedJitMonitor, TypedMonitor,
};
use trustworthiness_checker::{CheckedDsrvSpecification, DsrvSpecification, Value};

const HOTNESS_EVENTS: u64 = 1_024;
const WARM_EVENTS: usize = 10_000;
const TIMED_EVENTS: usize = 100_000;

#[derive(Clone, Copy)]
struct Scenario {
    name: &'static str,
    source: &'static str,
    input_count: usize,
}

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "arithmetic",
        source: include_str!("jit_layers/fixtures/arithmetic.dsrv"),
        input_count: 2,
    },
    Scenario {
        name: "chain32",
        source: include_str!("jit_layers/fixtures/chain32.dsrv"),
        input_count: 1,
    },
    Scenario {
        name: "conditional",
        source: include_str!("jit_layers/fixtures/conditional.dsrv"),
        input_count: 2,
    },
    Scenario {
        name: "threshold",
        source: include_str!("jit_layers/fixtures/threshold.dsrv"),
        input_count: 1,
    },
    Scenario {
        name: "window3",
        source: include_str!("jit_layers/fixtures/window3.dsrv"),
        input_count: 1,
    },
    Scenario {
        name: "accumulator",
        source: include_str!("jit_layers/fixtures/accumulator.dsrv"),
        input_count: 1,
    },
];

const BOUNDARY_SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "one_input_one_op",
        source: "in x: Int\nout result: Int\nresult = x + 1",
        input_count: 1,
    },
    Scenario {
        name: "two_inputs_three_ops",
        source: include_str!("jit_layers/fixtures/arithmetic.dsrv"),
        input_count: 2,
    },
    Scenario {
        name: "one_input_chain32",
        source: include_str!("jit_layers/fixtures/chain32.dsrv"),
        input_count: 1,
    },
    Scenario {
        name: "three_stream_fused_run",
        source: "in x: Int\nout first: Int\nout second: Int\nout result: Int\nfirst = x + 1\nsecond = first * 2\nresult = second - 3",
        input_count: 1,
    },
    Scenario {
        name: "three_stream_temporal_plan",
        source: "in x: Int\naux base: Int\naux delayed: Int\nout result: Int\nbase = x + 1\ndelayed = default(base[1], 0) + base\nresult = delayed * 2",
        input_count: 1,
    },
];

fn parse(source: &str) -> CheckedDsrvSpecification {
    source
        .parse()
        .expect("benchmark specification should type check")
}

fn build(specification: CheckedDsrvSpecification, config: JitConfig) -> DataflowMonitor {
    DataflowMonitor::compile_checked_with_jit(specification, config)
        .expect("integrated JIT benchmark should compile")
}

fn compile(source: &str, config: JitConfig) -> DataflowMonitor {
    build(parse(source), config)
}

fn compile_untyped(source: &str) -> DataflowMonitor {
    DataflowMonitor::compile_untyped(
        source
            .parse::<DsrvSpecification>()
            .expect("benchmark specification should parse"),
    )
    .expect("untyped benchmark specification should compile")
}

fn compile_checked_canonical(source: &str) -> DataflowMonitor {
    let mut monitor = DataflowMonitor::compile_checked(parse(source))
        .expect("checked benchmark specification should compile");
    monitor.set_quickening(false);
    monitor
}

fn compile_checked_quickened(source: &str) -> DataflowMonitor {
    DataflowMonitor::compile_checked(parse(source))
        .expect("checked benchmark specification should compile")
}

fn rows(input_count: usize, events: usize) -> Vec<Vec<Value>> {
    (0..events)
        .map(|tick| {
            (0..input_count)
                .map(|input| Value::Int(((tick * (input * 6 + 1) + input * 3) % 1_000) as i64))
                .collect()
        })
        .collect()
}

fn evaluate_rows(monitor: &mut DataflowMonitor, rows: &[Vec<Value>], output: &mut [Value]) {
    for row in rows {
        monitor
            .evaluate(black_box(row), output)
            .expect("benchmark event should evaluate");
        black_box(&output);
    }
}

fn direct_rows_one(events: usize) -> Vec<(i64,)> {
    (0..events).map(|tick| ((tick % 1_000) as i64,)).collect()
}

fn direct_rows_two(events: usize) -> Vec<(i64, i64)> {
    (0..events)
        .map(|tick| ((tick % 1_000) as i64, ((tick * 7 + 3) % 1_000) as i64))
        .collect()
}

trait DirectBenchmarkOutput {
    fn into_value(self) -> Value;
}

impl DirectBenchmarkOutput for (i64,) {
    fn into_value(self) -> Value {
        Value::Int(self.0)
    }
}

impl DirectBenchmarkOutput for (bool,) {
    fn into_value(self) -> Value {
        Value::Bool(self.0)
    }
}

fn verify_direct<M>(scenario: Scenario, direct_rows: &[M::Input], mut monitor: M) -> M
where
    M: TypedMonitor,
    M::Output: DirectBenchmarkOutput,
{
    let mut reference = compile_checked_canonical(scenario.source);
    let value_rows = rows(scenario.input_count, direct_rows.len());
    let mut expected = vec![Value::NoVal; reference.output_vars().len()];
    assert_eq!(
        expected.len(),
        1,
        "{} should have one output",
        scenario.name
    );
    for (tick, (row, value_row)) in direct_rows.iter().zip(&value_rows).enumerate() {
        reference.evaluate(value_row, &mut expected).unwrap();
        let actual = monitor.evaluate(row).into_value();
        assert_eq!(
            actual, expected[0],
            "{} direct mismatch at event {tick}",
            scenario.name
        );
    }
    monitor
}

fn benchmark_direct<M, F>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    label: &str,
    rows: &[M::Input],
    build: F,
) where
    M: TypedMonitor,
    F: Fn() -> M,
{
    group.bench_function(label, |b| {
        b.iter_batched(
            || {
                let mut monitor = build();
                for row in &rows[..WARM_EVENTS] {
                    black_box(monitor.evaluate(black_box(row)));
                }
                monitor
            },
            |mut monitor| {
                for row in &rows[WARM_EVENTS..] {
                    black_box(monitor.evaluate(black_box(row)));
                }
            },
            BatchSize::PerIteration,
        )
    });
}

fn verify_direct_routes(scenario: Scenario) {
    macro_rules! verify_routes {
        ($input:ty, $output:ty, $rows:expr) => {{
            let rows = $rows;
            verify_direct(
                scenario,
                &rows,
                TypedJitMonitor::<$input, $output>::compile_checked(parse(scenario.source))
                    .expect("fixture should support eager direct native execution"),
            );
            let warmed = verify_direct(
                scenario,
                &rows,
                TypedDataflowMonitor::<$input, $output>::compile_checked_with_jit(
                    parse(scenario.source),
                    JitConfig::after_events(HOTNESS_EVENTS),
                )
                .expect("fixture should support warmed direct native execution"),
            );
            assert!(
                warmed.is_direct_jit_active(),
                "{} should activate direct native execution after {HOTNESS_EVENTS} events",
                scenario.name
            );
        }};
    }

    match scenario.name {
        "arithmetic" | "conditional" => verify_routes!(
            (i64, i64),
            (i64,),
            direct_rows_two(HOTNESS_EVENTS as usize + 128)
        ),
        "threshold" | "window3" => verify_routes!(
            (i64,),
            (bool,),
            direct_rows_one(HOTNESS_EVENTS as usize + 128)
        ),
        "chain32" | "accumulator" => verify_routes!(
            (i64,),
            (i64,),
            direct_rows_one(HOTNESS_EVENTS as usize + 128)
        ),
        other => unreachable!("unknown direct benchmark fixture {other}"),
    }
}

fn verify() {
    for scenario in SCENARIOS.iter().chain(BOUNDARY_SCENARIOS) {
        let mut canonical = compile_checked_canonical(scenario.source);
        let mut untyped = compile_untyped(scenario.source);
        let mut quickened = compile_checked_quickened(scenario.source);
        let mut eager = compile(scenario.source, JitConfig::eager());
        let mut hot = compile(scenario.source, JitConfig::after_events(HOTNESS_EVENTS));
        let rows = rows(scenario.input_count, HOTNESS_EVENTS as usize + 128);
        let mut expected = vec![Value::NoVal; canonical.output_vars().len()];
        let mut untyped_output = expected.clone();
        let mut quickened_output = expected.clone();
        let mut eager_output = expected.clone();
        let mut hot_output = expected.clone();
        for row in &rows {
            canonical.evaluate(row, &mut expected).unwrap();
            untyped.evaluate(row, &mut untyped_output).unwrap();
            quickened.evaluate(row, &mut quickened_output).unwrap();
            eager.evaluate(row, &mut eager_output).unwrap();
            hot.evaluate(row, &mut hot_output).unwrap();
            assert_eq!(
                untyped_output, expected,
                "{} untyped mismatch",
                scenario.name
            );
            assert_eq!(
                quickened_output, expected,
                "{} quickened mismatch",
                scenario.name
            );
            assert_eq!(eager_output, expected, "{} eager mismatch", scenario.name);
            assert_eq!(hot_output, expected, "{} hot mismatch", scenario.name);
        }
        assert_eq!(
            eager.jit_report().unwrap().plan(),
            JitPlan::WholeSchedule,
            "{} eager Value JIT should use a whole-schedule plan",
            scenario.name
        );
        assert_eq!(
            hot.jit_report().unwrap().plan(),
            JitPlan::WholeSchedule,
            "{} warmed Value JIT should use a whole-schedule plan",
            scenario.name
        );
    }
}

fn bench_configuration(c: &mut Criterion) {
    verify();
    for scenario in SCENARIOS {
        verify_direct_routes(*scenario);
        let specification = parse(scenario.source);
        let event_rows = rows(scenario.input_count, WARM_EVENTS + TIMED_EVENTS);

        let mut startup = c.benchmark_group(format!("jit/startup_backend/{}", scenario.name));
        startup.sampling_mode(SamplingMode::Flat).sample_size(20);
        startup
            .warm_up_time(Duration::from_secs(1))
            .measurement_time(Duration::from_secs(3));
        for (name, config) in [
            ("all_no_hotness", JitConfig::eager()),
            ("all_with_hotness", JitConfig::after_events(HOTNESS_EVENTS)),
        ] {
            startup.bench_function(name, |b| {
                b.iter_batched(
                    || specification.clone(),
                    |specification| black_box(build(specification, config)),
                    BatchSize::PerIteration,
                )
            });
        }
        startup.finish();

        let mut sustained = c.benchmark_group(format!("jit/sustained/{}", scenario.name));
        sustained.sampling_mode(SamplingMode::Flat).sample_size(20);
        sustained
            .warm_up_time(Duration::from_secs(1))
            .measurement_time(Duration::from_secs(3));
        sustained.throughput(Throughput::Elements(TIMED_EVENTS as u64));
        for (name, build_monitor) in [
            (
                "untyped_value",
                compile_untyped as fn(&str) -> DataflowMonitor,
            ),
            ("checked_canonical_value", compile_checked_canonical),
            ("checked_quickened_value", compile_checked_quickened),
        ] {
            sustained.bench_function(name, |b| {
                b.iter_batched(
                    || {
                        let mut monitor = build_monitor(scenario.source);
                        let mut output = vec![Value::NoVal; monitor.output_vars().len()];
                        evaluate_rows(&mut monitor, &event_rows[..WARM_EVENTS], &mut output);
                        (monitor, output)
                    },
                    |(mut monitor, mut output)| {
                        evaluate_rows(&mut monitor, &event_rows[WARM_EVENTS..], &mut output)
                    },
                    BatchSize::PerIteration,
                )
            });
        }
        sustained.bench_function("native_value_eager", |b| {
            b.iter_batched(
                || {
                    let mut monitor = compile(scenario.source, JitConfig::eager());
                    let mut output = vec![Value::NoVal; monitor.output_vars().len()];
                    evaluate_rows(&mut monitor, &event_rows[..WARM_EVENTS], &mut output);
                    (monitor, output)
                },
                |(mut monitor, mut output)| {
                    evaluate_rows(&mut monitor, &event_rows[WARM_EVENTS..], &mut output)
                },
                BatchSize::PerIteration,
            )
        });

        macro_rules! benchmark_direct_routes {
            ($input:ty, $output:ty, $rows:expr) => {{
                let direct_rows = $rows;
                benchmark_direct::<TypedJitMonitor<$input, $output>, _>(
                    &mut sustained,
                    "native_direct_eager",
                    &direct_rows,
                    || {
                        TypedJitMonitor::compile_checked(parse(scenario.source))
                            .expect("fixture should support eager direct native execution")
                    },
                );
                benchmark_direct::<TypedDataflowMonitor<$input, $output>, _>(
                    &mut sustained,
                    "native_direct_warmed",
                    &direct_rows,
                    || {
                        TypedDataflowMonitor::compile_checked_with_jit(
                            parse(scenario.source),
                            JitConfig::after_events(HOTNESS_EVENTS),
                        )
                        .expect("fixture should support warmed direct native execution")
                    },
                );
            }};
        }
        match scenario.name {
            "arithmetic" | "conditional" => benchmark_direct_routes!(
                (i64, i64),
                (i64,),
                direct_rows_two(WARM_EVENTS + TIMED_EVENTS)
            ),
            "threshold" | "window3" => benchmark_direct_routes!(
                (i64,),
                (bool,),
                direct_rows_one(WARM_EVENTS + TIMED_EVENTS)
            ),
            "chain32" | "accumulator" => benchmark_direct_routes!(
                (i64,),
                (i64,),
                direct_rows_one(WARM_EVENTS + TIMED_EVENTS)
            ),
            other => unreachable!("unknown direct benchmark fixture {other}"),
        }
        sustained.finish();

        for events in [1usize, 256, 1_024, 4_096] {
            let lifetime_rows = rows(scenario.input_count, events);
            let mut lifetime =
                c.benchmark_group(format!("jit/source_to_events/{}/{events}", scenario.name));
            lifetime.sampling_mode(SamplingMode::Flat).sample_size(20);
            lifetime
                .warm_up_time(Duration::from_secs(1))
                .measurement_time(Duration::from_secs(3));
            for (name, config) in [
                ("all_no_hotness", JitConfig::eager()),
                ("all_with_hotness", JitConfig::after_events(HOTNESS_EVENTS)),
            ] {
                lifetime.bench_function(name, |b| {
                    b.iter(|| {
                        let mut monitor = compile(black_box(scenario.source), config);
                        let mut output = vec![Value::NoVal; monitor.output_vars().len()];
                        evaluate_rows(&mut monitor, &lifetime_rows, &mut output);
                        black_box((monitor, output));
                    })
                });
            }
            lifetime.finish();
        }
    }
}

fn bench_boundary(c: &mut Criterion) {
    for scenario in BOUNDARY_SCENARIOS {
        let event_rows = rows(scenario.input_count, WARM_EVENTS + TIMED_EVENTS);
        let mut group = c.benchmark_group(format!("jit/boundary/{}", scenario.name));
        group.sampling_mode(SamplingMode::Flat).sample_size(20);
        group
            .warm_up_time(Duration::from_secs(1))
            .measurement_time(Duration::from_secs(3));
        group.throughput(Throughput::Elements(TIMED_EVENTS as u64));
        group.bench_function("checked", |b| {
            b.iter_batched(
                || {
                    let mut monitor =
                        DataflowMonitor::compile_checked(parse(scenario.source)).unwrap();
                    let mut output = vec![Value::NoVal; monitor.output_vars().len()];
                    evaluate_rows(&mut monitor, &event_rows[..WARM_EVENTS], &mut output);
                    (monitor, output)
                },
                |(mut monitor, mut output)| {
                    evaluate_rows(&mut monitor, &event_rows[WARM_EVENTS..], &mut output)
                },
                BatchSize::PerIteration,
            )
        });
        group.bench_function("integrated_fused", |b| {
            b.iter_batched(
                || {
                    let mut monitor = compile(scenario.source, JitConfig::eager());
                    let mut output = vec![Value::NoVal; monitor.output_vars().len()];
                    evaluate_rows(&mut monitor, &event_rows[..WARM_EVENTS], &mut output);
                    (monitor, output)
                },
                |(mut monitor, mut output)| {
                    evaluate_rows(&mut monitor, &event_rows[WARM_EVENTS..], &mut output)
                },
                BatchSize::PerIteration,
            )
        });
        group.finish();
    }
}

criterion_group!(benches, bench_configuration, bench_boundary);
criterion_main!(benches);
