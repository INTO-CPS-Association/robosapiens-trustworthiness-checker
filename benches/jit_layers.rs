#![cfg(feature = "jit")]

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::hint::black_box;
use std::time::Duration;

use criterion::{BatchSize, Criterion, SamplingMode, Throughput, criterion_group, criterion_main};
use trustworthiness_checker::dataflow::{DataflowMonitor, JitConfig};
use trustworthiness_checker::{CheckedDsrvSpecification, Value};

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

fn verify() {
    for scenario in SCENARIOS.iter().chain(BOUNDARY_SCENARIOS) {
        let checked_spec = parse(scenario.source);
        let mut checked = DataflowMonitor::compile_checked(checked_spec.clone()).unwrap();
        let mut eager = build(checked_spec.clone(), JitConfig::eager());
        let mut hot = build(checked_spec, JitConfig::after_events(HOTNESS_EVENTS));
        let rows = rows(scenario.input_count, HOTNESS_EVENTS as usize + 128);
        let mut expected = vec![Value::NoVal; checked.output_vars().len()];
        let mut eager_output = expected.clone();
        let mut hot_output = expected.clone();
        for row in &rows {
            checked.evaluate(row, &mut expected).unwrap();
            eager.evaluate(row, &mut eager_output).unwrap();
            hot.evaluate(row, &mut hot_output).unwrap();
            assert_eq!(eager_output, expected, "{} eager mismatch", scenario.name);
            assert_eq!(hot_output, expected, "{} hot mismatch", scenario.name);
        }
    }
}

fn bench_configuration(c: &mut Criterion) {
    verify();
    for scenario in SCENARIOS {
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
        for (name, config) in [
            ("all_no_hotness", JitConfig::eager()),
            ("all_with_hotness", JitConfig::after_events(HOTNESS_EVENTS)),
        ] {
            sustained.bench_function(name, |b| {
                b.iter_batched(
                    || {
                        let mut monitor = compile(scenario.source, config);
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
