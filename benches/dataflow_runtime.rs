use std::hint::black_box;
use std::time::Duration;

use criterion::{
    BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use trustworthiness_checker::dataflow::{ContextTransferPolicy, DataflowMonitor};
use trustworthiness_checker::{DsrvSpecification, Value, VarName};

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const CACHED_TICKS: u64 = 1_024;

fn compile(source: &str) -> DataflowMonitor {
    let spec = source
        .parse::<DsrvSpecification>()
        .expect("benchmark specification should parse");
    DataflowMonitor::compile_untyped(spec).expect("benchmark monitor should compile")
}

fn input_row(monitor: &DataflowMonitor, values: &[(&str, Value)]) -> Vec<Value> {
    monitor
        .input_vars()
        .iter()
        .map(|var| {
            values
                .iter()
                .find_map(|(name, value)| (var == &VarName::new(name)).then(|| value.clone()))
                .unwrap_or_else(|| panic!("missing benchmark input `{var}`"))
        })
        .collect()
}

fn evaluate_repeatedly(monitor: &mut DataflowMonitor, input: &[Value], output: &mut [Value]) {
    for _ in 0..CACHED_TICKS {
        monitor.evaluate(black_box(input), output).unwrap();
    }
    black_box(output);
}

fn evaluate_alternating(
    monitor: &mut DataflowMonitor,
    first: &[Value],
    second: &[Value],
    output: &mut [Value],
) {
    debug_assert_eq!(CACHED_TICKS % 2, 0);
    for tick in 0..CACHED_TICKS {
        let input = if tick % 2 == 0 { first } else { second };
        monitor.evaluate(black_box(input), output).unwrap();
    }
    black_box(output);
}

fn assert_outputs(monitor: &DataflowMonitor, output: &[Value], expected: &[(&str, Value)]) {
    for (name, expected) in expected {
        let index = monitor
            .output_vars()
            .iter()
            .position(|variable| variable == &VarName::new(name))
            .unwrap_or_else(|| panic!("missing benchmark output `{name}`"));
        assert_eq!(&output[index], expected, "unexpected output for `{name}`");
    }
}

fn chain_spec(streams: usize, dynamic: bool) -> String {
    assert!(streams > 0);
    let mut source = String::from("in x: Int\nin source: Str\n");
    for index in 0..streams - 1 {
        source.push_str(&format!("aux s{index}: Int\n"));
    }
    source.push_str("out result: Int\nout dynamic_result: Int\n");
    for index in 0..streams {
        let name = if index + 1 == streams {
            "result".to_owned()
        } else {
            format!("s{index}")
        };
        let operand = if index == 0 {
            "x".to_owned()
        } else {
            format!("s{}", index - 1)
        };
        source.push_str(&format!("{name} = {operand} + 1\n"));
    }
    if dynamic {
        source.push_str("dynamic_result = dynamic(source: Int)\n");
    } else {
        source.push_str("dynamic_result = x\n");
    }
    source
}

fn bench_steady_state(c: &mut Criterion) {
    let mut group = c.benchmark_group("dataflow/steady_state");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(CACHED_TICKS));

    for streams in [1_usize, 8, 32, 128] {
        for (name, dynamic) in [("static", false), ("dynamic", true)] {
            let mut monitor = compile(&chain_spec(streams, dynamic));
            let input = input_row(
                &monitor,
                &[("x", Value::Int(1)), ("source", Value::Str("x".into()))],
            );
            let mut output = vec![Value::NoVal; monitor.output_vars().len()];
            monitor.evaluate(&input, &mut output).unwrap();
            group.bench_with_input(BenchmarkId::new(name, streams), &streams, |b, _| {
                b.iter(|| evaluate_repeatedly(&mut monitor, &input, &mut output));
            });
        }
    }
    group.finish();
}

fn expression_source_spec(expression_source_streams: usize) -> String {
    let mut source = String::from("in choose: Bool\nin left: Str\nin right: Str\nin x: Int\n");
    for index in 0..expression_source_streams {
        source.push_str(&format!("aux source{index}: Str\n"));
    }
    source.push_str("out result: Int\nsource0 = if choose then left else right\n");
    for index in 1..expression_source_streams {
        source.push_str(&format!("source{index} = source{}\n", index - 1));
    }
    source.push_str(&format!(
        "result = dynamic(source{}: Int)\n",
        expression_source_streams - 1
    ));
    source
}

fn bench_expression_source_evaluation(c: &mut Criterion) {
    let mut group = c.benchmark_group("dataflow/expression_source_evaluation");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(CACHED_TICKS));

    for streams in [1_usize, 8, 32] {
        let mut monitor = compile(&expression_source_spec(streams));
        let input = input_row(
            &monitor,
            &[
                ("choose", Value::Bool(true)),
                ("left", Value::Str("x".into())),
                ("right", Value::Str("x + 1".into())),
                ("x", Value::Int(1)),
            ],
        );
        let mut output = vec![Value::NoVal; monitor.output_vars().len()];
        monitor.evaluate(&input, &mut output).unwrap();
        group.bench_with_input(BenchmarkId::from_parameter(streams), &streams, |b, _| {
            b.iter(|| evaluate_repeatedly(&mut monitor, &input, &mut output));
        });
    }
    group.finish();
}

struct ScheduleUpdateCase {
    monitor: DataflowMonitor,
    input: Vec<Value>,
    output: Vec<Value>,
}

fn schedule_update_case(reversal: bool) -> ScheduleUpdateCase {
    let spec = "in x: Int\nin a_source: Str\nin b_source: Str\nout a: Int\nout b: Int\n\
                a = dynamic(a_source: Int)\nb = dynamic(b_source: Int)";
    let mut monitor = compile(spec);
    let initial = input_row(
        &monitor,
        &[
            ("x", Value::Int(10)),
            ("a_source", Value::Str("x".into())),
            ("b_source", Value::Str("a + 1".into())),
        ],
    );
    let mut output = vec![Value::NoVal; 2];
    monitor.evaluate(&initial, &mut output).unwrap();
    let input = input_row(
        &monitor,
        &[
            ("x", Value::Int(20)),
            (
                "a_source",
                Value::Str(if reversal { "b + 1" } else { "x + 1" }.into()),
            ),
            (
                "b_source",
                Value::Str(if reversal { "x" } else { "a + 1" }.into()),
            ),
        ],
    );
    ScheduleUpdateCase {
        monitor,
        input,
        output,
    }
}

fn bench_schedule_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("dataflow/schedule_update");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    for (name, reversal) in [
        ("scheduled_order_valid", false),
        ("scheduled_order_repair", true),
    ] {
        group.bench_function(name, |b| {
            b.iter_batched(
                || schedule_update_case(reversal),
                |mut case| {
                    case.monitor
                        .evaluate(black_box(&case.input), &mut case.output)
                        .unwrap();
                    black_box(case.output)
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn single_dynamic_spec(operator: &str) -> String {
    format!("in x: Int\nin source: Str\nout result: Int\nresult = {operator}(source: Int)")
}

fn dependency_transition_spec() -> &'static str {
    "in x: Int\nin source: Str\naux left: Int\naux right: Int\nout result: Int\n\
     left = x + 1\nright = x + 2\n\
     result = dynamic(source: Int) + (left - left) + (right - right)"
}

fn two_dynamic_spec() -> &'static str {
    "in x: Int\nin a_source: Str\nin b_source: Str\nout a: Int\nout b: Int\n\
     a = dynamic(a_source: Int)\nb = dynamic(b_source: Int)"
}

fn reconfiguration_spec(streams: usize) -> String {
    assert!(streams > 0);
    let mut source = String::from("in x: Int\n");
    for index in 0..streams.saturating_sub(1) {
        source.push_str(&format!("aux s{index}: Int\n"));
    }
    source.push_str("out result: Int\n");
    for index in 0..streams {
        let name = if index + 1 == streams {
            "result".to_owned()
        } else {
            format!("s{index}")
        };
        let operand = if index == 0 {
            "x[4]".to_owned()
        } else {
            format!("s{}", index - 1)
        };
        source.push_str(&format!("{name} = {operand}\n"));
    }
    source
}

fn warm_transfer_monitor(monitor: &mut DataflowMonitor) {
    let input = input_row(monitor, &[("x", Value::Int(1))]);
    let mut output = vec![Value::NoVal; monitor.output_vars().len()];
    for _ in 0..8 {
        monitor.evaluate(&input, &mut output).unwrap();
    }
}

fn bench_runtime_reconfiguration(c: &mut Criterion) {
    let mut group = c.benchmark_group("dataflow/runtime_reconfiguration");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    for streams in [1_usize, 8, 16, 32] {
        let source = reconfiguration_spec(streams);
        group.bench_with_input(
            BenchmarkId::new("candidate_compile_and_transfer", streams),
            &source,
            |b, source| {
                let mut old = compile(source);
                warm_transfer_monitor(&mut old);
                let context = old.export_context().unwrap();
                b.iter(|| {
                    let mut candidate = compile(black_box(source));
                    let report = candidate
                        .import_context(&context, ContextTransferPolicy::Compatible)
                        .unwrap();
                    black_box(report);
                    black_box(candidate);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("state_transfer_only", streams),
            &source,
            |b, source| {
                let mut old = compile(source);
                warm_transfer_monitor(&mut old);
                let context = old.export_context().unwrap();
                let mut candidate = compile(source);
                b.iter(|| {
                    let report = candidate
                        .import_context(&context, ContextTransferPolicy::Compatible)
                        .unwrap();
                    black_box(report);
                });
            },
        );
    }
    group.finish();
}

fn bench_dynamic_transitions(c: &mut Criterion) {
    let mut group = c.benchmark_group("dataflow/dynamic_transitions");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(CACHED_TICKS));

    {
        let mut monitor = compile(&single_dynamic_spec("dynamic"));
        let activation = input_row(
            &monitor,
            &[
                ("x", Value::Int(10)),
                ("source", Value::Str("x + 1".into())),
            ],
        );
        let stable = input_row(&monitor, &[("x", Value::Int(10)), ("source", Value::NoVal)]);
        let mut output = vec![Value::NoVal; monitor.output_vars().len()];
        monitor.evaluate(&activation, &mut output).unwrap();
        assert_outputs(&monitor, &output, &[("result", Value::Int(11))]);
        monitor.evaluate(&stable, &mut output).unwrap();
        assert_outputs(&monitor, &output, &[("result", Value::Int(11))]);

        group.bench_function("stable_dynamic", |b| {
            b.iter(|| evaluate_repeatedly(&mut monitor, &stable, &mut output));
        });
    }

    {
        let mut monitor = compile(&single_dynamic_spec("defer"));
        let activation = input_row(
            &monitor,
            &[
                ("x", Value::Int(10)),
                ("source", Value::Str("x + 1".into())),
            ],
        );
        let activated = input_row(&monitor, &[("x", Value::Int(10)), ("source", Value::NoVal)]);
        let mut output = vec![Value::NoVal; monitor.output_vars().len()];
        monitor.evaluate(&activation, &mut output).unwrap();
        assert_outputs(&monitor, &output, &[("result", Value::Int(11))]);
        monitor.evaluate(&activated, &mut output).unwrap();
        assert_outputs(&monitor, &output, &[("result", Value::Int(11))]);

        group.bench_function("activated_defer", |b| {
            b.iter(|| evaluate_repeatedly(&mut monitor, &activated, &mut output));
        });
    }

    {
        let mut monitor = compile(dependency_transition_spec());
        let first = input_row(
            &monitor,
            &[
                ("x", Value::Int(10)),
                ("source", Value::Str("left + 1".into())),
            ],
        );
        let second = input_row(
            &monitor,
            &[
                ("x", Value::Int(10)),
                ("source", Value::Str("left + 2".into())),
            ],
        );
        let mut output = vec![Value::NoVal; monitor.output_vars().len()];
        monitor.evaluate(&first, &mut output).unwrap();
        assert_outputs(&monitor, &output, &[("result", Value::Int(12))]);
        monitor.evaluate(&second, &mut output).unwrap();
        assert_outputs(&monitor, &output, &[("result", Value::Int(13))]);

        group.bench_function("alternating_same_dependency", |b| {
            b.iter(|| {
                evaluate_alternating(&mut monitor, &first, &second, &mut output);
            });
        });
    }

    {
        let mut monitor = compile(dependency_transition_spec());
        let first = input_row(
            &monitor,
            &[
                ("x", Value::Int(10)),
                ("source", Value::Str("left + 1".into())),
            ],
        );
        let second = input_row(
            &monitor,
            &[
                ("x", Value::Int(10)),
                ("source", Value::Str("right + 1".into())),
            ],
        );
        let mut output = vec![Value::NoVal; monitor.output_vars().len()];
        monitor.evaluate(&first, &mut output).unwrap();
        assert_outputs(&monitor, &output, &[("result", Value::Int(12))]);
        monitor.evaluate(&second, &mut output).unwrap();
        assert_outputs(&monitor, &output, &[("result", Value::Int(13))]);

        group.bench_function("alternating_changed_dependency_valid_schedule", |b| {
            b.iter(|| {
                evaluate_alternating(&mut monitor, &first, &second, &mut output);
            });
        });
    }

    {
        let mut monitor = compile(two_dynamic_spec());
        let first = input_row(
            &monitor,
            &[
                ("x", Value::Int(10)),
                ("a_source", Value::Str("x + 1".into())),
                ("b_source", Value::Str("a + 1".into())),
            ],
        );
        let second = input_row(
            &monitor,
            &[
                ("x", Value::Int(10)),
                ("a_source", Value::Str("x + 2".into())),
                ("b_source", Value::Str("a + 2".into())),
            ],
        );
        let mut output = vec![Value::NoVal; monitor.output_vars().len()];
        monitor.evaluate(&first, &mut output).unwrap();
        assert_outputs(
            &monitor,
            &output,
            &[("a", Value::Int(11)), ("b", Value::Int(12))],
        );
        monitor.evaluate(&second, &mut output).unwrap();
        assert_outputs(
            &monitor,
            &output,
            &[("a", Value::Int(12)), ("b", Value::Int(14))],
        );
        monitor.evaluate(&first, &mut output).unwrap();

        group.bench_function("schedule_valid", |b| {
            b.iter(|| {
                evaluate_alternating(&mut monitor, &second, &first, &mut output);
            });
        });
    }

    {
        let mut monitor = compile(two_dynamic_spec());
        let forward = input_row(
            &monitor,
            &[
                ("x", Value::Int(10)),
                ("a_source", Value::Str("x + 1".into())),
                ("b_source", Value::Str("a + 1".into())),
            ],
        );
        let reverse = input_row(
            &monitor,
            &[
                ("x", Value::Int(10)),
                ("a_source", Value::Str("b + 1".into())),
                ("b_source", Value::Str("x + 1".into())),
            ],
        );
        let mut output = vec![Value::NoVal; monitor.output_vars().len()];
        monitor.evaluate(&forward, &mut output).unwrap();
        assert_outputs(
            &monitor,
            &output,
            &[("a", Value::Int(11)), ("b", Value::Int(12))],
        );
        monitor.evaluate(&reverse, &mut output).unwrap();
        assert_outputs(
            &monitor,
            &output,
            &[("a", Value::Int(12)), ("b", Value::Int(11))],
        );
        monitor.evaluate(&forward, &mut output).unwrap();

        group.bench_function("schedule_repair", |b| {
            b.iter(|| {
                evaluate_alternating(&mut monitor, &reverse, &forward, &mut output);
            });
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(30);
    targets = bench_steady_state, bench_expression_source_evaluation, bench_schedule_update,
        bench_dynamic_transitions, bench_runtime_reconfiguration
}
criterion_main!(benches);
