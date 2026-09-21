use std::hint::black_box;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use trustworthiness_checker::DsrvSpecification;
use trustworthiness_checker::Value;
use trustworthiness_checker::dataflow::{DataflowMonitor, DataflowProgram};

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const COUNTER: &str = "in x: Int\nout z: Int\nz = default(z[1], 0) + x";
const TEMPORAL: &str = "in x: Int\nout z: Int\nz = default(x[4], 0) + x";
const DYNAMIC: &str = "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";

fn program(source: &str) -> DataflowProgram {
    DataflowProgram::compile_with_semantics(
        trustworthiness_checker::dsrv_fixtures::elaborate_for(
            source
                .parse::<DsrvSpecification>()
                .expect("benchmark specification should parse"),
            trustworthiness_checker::core::Semantics::Untimed,
        ),
        trustworthiness_checker::core::Semantics::Untimed,
    )
    .expect("benchmark specification should compile")
}

fn evaluate_short_trace(monitor: &mut DataflowMonitor, dynamic: bool) {
    let mut output = [Value::NoVal];
    if dynamic {
        for (value, source) in [(1, "x"), (2, "x[2]"), (3, "x"), (4, "x[4]")] {
            monitor
                .evaluate(&[Value::Int(value), Value::Str(source.into())], &mut output)
                .expect("benchmark trace should evaluate");
        }
    } else {
        for value in [1, 2, 3, 4] {
            monitor
                .evaluate(&[Value::Int(value)], &mut output)
                .expect("benchmark trace should evaluate");
        }
    }
}

fn dataflow_lifecycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("dataflow_lifecycle");
    group.sample_size(20);

    for (name, source) in [
        ("scalar", COUNTER),
        ("temporal", TEMPORAL),
        ("dynamic", DYNAMIC),
    ] {
        let compiled = program(source);
        let dynamic = name == "dynamic";
        group.bench_function(format!("{name}/elaborate_compile_plus_instantiate"), |b| {
            b.iter(|| {
                let specification = black_box(source)
                    .parse::<DsrvSpecification>()
                    .expect("benchmark specification should parse");
                let specification = trustworthiness_checker::dsrv_fixtures::elaborate_for(
                    specification,
                    trustworthiness_checker::core::Semantics::Untimed,
                );
                black_box(
                    DataflowMonitor::compile_with_semantics(
                        specification,
                        trustworthiness_checker::core::Semantics::Untimed,
                    )
                    .unwrap(),
                )
            })
        });
        group.bench_function(format!("{name}/instantiate_from_program"), |b| {
            b.iter_batched(
                || compiled.clone(),
                |compiled| black_box(DataflowMonitor::from_program(compiled)),
                BatchSize::SmallInput,
            )
        });
        group.bench_function(format!("{name}/reset_only_after_warm_trace"), |b| {
            b.iter_batched_ref(
                || {
                    let mut monitor = DataflowMonitor::from_program(compiled.clone());
                    evaluate_short_trace(&mut monitor, dynamic);
                    monitor
                },
                |monitor| {
                    monitor.reset();
                    black_box(&mut *monitor);
                },
                BatchSize::SmallInput,
            )
        });
        group.bench_function(format!("{name}/trace_plus_reset"), |b| {
            b.iter_batched(
                || DataflowMonitor::from_program(compiled.clone()),
                |mut monitor| {
                    evaluate_short_trace(&mut monitor, dynamic);
                    monitor.reset();
                    black_box(monitor);
                },
                BatchSize::SmallInput,
            )
        });
        group.bench_function(format!("{name}/repeated_trace_reset"), |b| {
            b.iter_batched(
                || DataflowMonitor::from_program(compiled.clone()),
                |mut monitor| {
                    for _ in 0..16 {
                        evaluate_short_trace(&mut monitor, dynamic);
                        monitor.reset();
                    }
                    black_box(monitor);
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

criterion_group!(benches, dataflow_lifecycle);
criterion_main!(benches);
