use std::hint::black_box;
use std::time::Duration;

use criterion::{
    BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use trustworthiness_checker::dataflow::DataflowMonitor;
use trustworthiness_checker::{DsrvSpecification, Value, VarName};

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

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(30);
    targets = bench_steady_state, bench_expression_source_evaluation, bench_schedule_update
}
criterion_main!(benches);
