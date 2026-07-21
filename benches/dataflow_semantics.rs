use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Duration;

use criterion::async_executor::AsyncExecutor;
use criterion::{BenchmarkId, Criterion, SamplingMode, criterion_group, criterion_main};
use smol::LocalExecutor;
use trustworthiness_checker::core::Runtime;
use trustworthiness_checker::dataflow::DataflowMonitor;
use trustworthiness_checker::io::map;
use trustworthiness_checker::io::testing::LimitedNullOutputHandler;
use trustworthiness_checker::lang::dsrv::ast::CheckedDsrvSpecification;
use trustworthiness_checker::lang::dsrv::type_checker::type_check;
use trustworthiness_checker::runtime::builder::{RuntimeBuilder, SemiSyncValueConfig};
use trustworthiness_checker::runtime::dataflow::DataflowRuntimeBuilder;
use trustworthiness_checker::runtime::semi_sync::SemiSyncRuntimeBuilder;
use trustworthiness_checker::semantics::UntimedDsrvSemantics;
use trustworthiness_checker::{
    DsrvSpecification, InputStream, Value, VarName, lang::dsrv::parser::parse_str,
};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn evaluate_monitor(monitor: &mut DataflowMonitor, columns: &[Vec<Value>]) -> Vec<Vec<Value>> {
    let len = columns.first().map_or(0, Vec::len);
    let mut output = vec![Vec::new(); monitor.output_vars().len()];
    let mut input_row = vec![Value::NoVal; columns.len()];
    let mut output_row = vec![Value::NoVal; output.len()];
    for tick in 0..len {
        for (value, column) in input_row.iter_mut().zip(columns) {
            *value = column[tick].clone();
        }
        monitor.evaluate(&input_row, &mut output_row).unwrap();
        for (column, value) in output.iter_mut().zip(&output_row) {
            column.push(value.clone());
        }
    }
    output
}

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

async fn monitor_recursive_outputs_semisync(
    executor: Rc<LocalExecutor<'static>>,
    spec: DsrvSpecification,
    input_stream: InputStream<Value>,
    output_limit: usize,
) {
    let output_handler = Box::new(LimitedNullOutputHandler::new(
        executor.clone(),
        spec.output_vars().clone(),
        output_limit,
    ));
    let monitor = SemiSyncRuntimeBuilder::<SemiSyncValueConfig, UntimedDsrvSemantics>::new()
        .executor(executor.clone())
        .model(spec)
        .input(input_stream)
        .output(output_handler)
        .build()
        .await;
    monitor.run().await.expect("Error running monitor");
}

async fn monitor_recursive_outputs_dataflow(
    executor: Rc<LocalExecutor<'static>>,
    spec: DsrvSpecification,
    input_stream: InputStream<Value>,
    output_limit: usize,
) {
    let output_handler = Box::new(LimitedNullOutputHandler::new(
        executor.clone(),
        spec.output_vars().clone(),
        output_limit,
    ));
    let monitor = DataflowRuntimeBuilder::<DsrvSpecification>::new()
        .executor(executor.clone())
        .model(spec)
        .input(input_stream)
        .output(output_handler)
        .build()
        .await;
    monitor.run().await.expect("Error running monitor");
}

async fn monitor_recursive_outputs_typed_semisync(
    executor: Rc<LocalExecutor<'static>>,
    spec: CheckedDsrvSpecification,
    input_stream: InputStream<Value>,
    output_limit: usize,
) {
    let output_handler = Box::new(LimitedNullOutputHandler::new(
        executor.clone(),
        spec.output_vars().clone(),
        output_limit,
    ));
    let monitor = SemiSyncRuntimeBuilder::<
        trustworthiness_checker::runtime::builder::CheckedSemiSyncValueConfig,
        trustworthiness_checker::semantics::CheckedUntimedDsrvSemantics,
    >::new()
    .executor(executor.clone())
    .model(spec)
    .input(input_stream)
    .output(output_handler)
    .build()
    .await;
    monitor.run().await.expect("Error running monitor");
}

async fn monitor_recursive_outputs_typed_dataflow(
    executor: Rc<LocalExecutor<'static>>,
    spec: CheckedDsrvSpecification,
    input_stream: InputStream<Value>,
    output_limit: usize,
) {
    let output_handler = Box::new(LimitedNullOutputHandler::new(
        executor.clone(),
        spec.output_vars().clone(),
        output_limit,
    ));
    let monitor = DataflowRuntimeBuilder::<CheckedDsrvSpecification>::new()
        .executor(executor.clone())
        .model(spec)
        .input(input_stream)
        .output(output_handler)
        .build()
        .await;
    monitor.run().await.expect("Error running monitor");
}

fn recursive_spec() -> DsrvSpecification {
    let spec = "in x\n\
                    in y\n\
                    out z\n\
                    z = if x % 5 == 0 then default(z[1], 0) + y * 3 else default(z[1], 0) + x + y";
    parse_str(spec).expect("recursive benchmark spec should parse")
}

fn typed_recursive_spec() -> CheckedDsrvSpecification {
    let spec = "in x: Int\n\
                    in y: Int\n\
                    out z: Int\n\
                    z = if x % 5 == 0 then default(z[1], 0) + y * 3 else default(z[1], 0) + x + y";
    let spec = parse_str(spec).expect("typed recursive benchmark spec should parse");
    type_check(spec, false).expect("typed recursive benchmark spec should type check")
}

fn arithmetic_spec() -> DsrvSpecification {
    let spec = "in x\n\
                    in y\n\
                    out z\n\
                    z = (x + y) * 3 - (x % 7)";
    parse_str(spec).expect("arithmetic benchmark spec should parse")
}

fn typed_arithmetic_spec() -> CheckedDsrvSpecification {
    let spec = "in x: Int\n\
                    in y: Int\n\
                    out z: Int\n\
                    z = (x + y) * 3 - (x % 7)";
    let spec = parse_str(spec).expect("typed arithmetic benchmark spec should parse");
    type_check(spec, false).expect("typed arithmetic benchmark spec should type check")
}

fn if_arithmetic_spec() -> DsrvSpecification {
    let spec = "in x\n\
                    in y\n\
                    out z\n\
                    z = if x % 5 == 0 then y * 3 else x + y";
    parse_str(spec).expect("if arithmetic benchmark spec should parse")
}

fn typed_if_arithmetic_spec() -> CheckedDsrvSpecification {
    let spec = "in x: Int\n\
                    in y: Int\n\
                    out z: Int\n\
                    z = if x % 5 == 0 then y * 3 else x + y";
    let spec = parse_str(spec).expect("typed if arithmetic benchmark spec should parse");
    type_check(spec, false).expect("typed if arithmetic benchmark spec should type check")
}

fn stream_dependency_spec() -> DsrvSpecification {
    let spec = "in x\n\
                    in y\n\
                    out w\n\
                    aux z\n\
                    z = (x + y) * 3 - (x % 7)\n\
                    w = z + (z % 5)";
    parse_str(spec).expect("stream dependency benchmark spec should parse")
}

fn typed_stream_dependency_spec() -> CheckedDsrvSpecification {
    let spec = "in x: Int\n\
                    in y: Int\n\
                    out w: Int\n\
                    aux z: Int\n\
                    z = (x + y) * 3 - (x % 7)\n\
                    w = z + (z % 5)";
    let spec = parse_str(spec).expect("typed stream dependency benchmark spec should parse");
    type_check(spec, false).expect("typed stream dependency benchmark spec should type check")
}

fn function_heavy_spec() -> DsrvSpecification {
    let spec = "in n\n\
                    in bias\n\
                    out direct\n\
                    out mapped\n\
                    out sum\n\
                    out partialResult\n\
                    aux xs\n\
                    aux addBias\n\
                    xs = List(n, n + 1, n + 2, n + 3, n + 4)\n\
                    direct = (\\x: Int -> x + bias)(n)\n\
                    mapped = List.map(\\x: Int -> x + bias, xs)\n\
                    sum = List.fold(\\acc: Int, x: Int -> acc + x + bias, 0, xs)\n\
                    addBias = partial(\\x: Int, y: Int -> x + y, bias)\n\
                    partialResult = addBias(n)";
    parse_str(spec).expect("function-heavy benchmark spec should parse")
}

fn typed_function_heavy_spec() -> CheckedDsrvSpecification {
    let spec = "in n: Int\n\
                    in bias: Int\n\
                    out direct: Int\n\
                    out mapped: List<Int>\n\
                    out sum: Int\n\
                    aux xs: List<Int>\n\
                    xs = List(n, n + 1, n + 2, n + 3, n + 4)\n\
                    direct = (\\x: Int -> x + bias)(n)\n\
                    mapped = List.map(\\x: Int -> x + bias, xs)\n\
                    sum = List.fold(\\acc: Int, x: Int -> acc + x + bias, 0, xs)";
    let spec = parse_str(spec).expect("typed function-heavy benchmark spec should parse");
    type_check(spec, false).expect("typed function-heavy benchmark spec should type check")
}

fn recursive_function_if_spec() -> DsrvSpecification {
    let spec = "in n\n\
                    in bias\n\
                    out recursive\n\
                    recursive = fix(\\self: (Int -> Int), k: Int -> if k == 0 then bias else self(k - 1) + 1)(n)";
    parse_str(spec).expect("recursive function-if benchmark spec should parse")
}

fn typed_recursive_function_if_spec() -> CheckedDsrvSpecification {
    let spec = "in n: Int\n\
                    in bias: Int\n\
                    out recursive: Int\n\
                    recursive = fix(\\self: (Int -> Int), k: Int -> if k == 0 then bias else (self(k - 1) + 1))(n)";
    let spec = parse_str(spec).expect("typed recursive function-if benchmark spec should parse");
    type_check(spec, false).expect("typed recursive function-if benchmark spec should type check")
}

fn direct_function_if_spec() -> DsrvSpecification {
    let spec = "in n\n\
                    in bias\n\
                    out direct\n\
                    direct = (\\k: Int -> if k == 0 then bias else k + bias)(n)";
    parse_str(spec).expect("direct function-if benchmark spec should parse")
}

fn typed_direct_function_if_spec() -> CheckedDsrvSpecification {
    let spec = "in n: Int\n\
                    in bias: Int\n\
                    out direct: Int\n\
                    direct = (\\k: Int -> if k == 0 then bias else k + bias)(n)";
    let spec = parse_str(spec).expect("typed direct function-if benchmark spec should parse");
    type_check(spec, false).expect("typed direct function-if benchmark spec should type check")
}

fn recursive_inputs(size: usize) -> InputStream<Value> {
    let size = size as i64;
    map::input_stream(BTreeMap::from([
        (
            VarName::new("x"),
            (0..size).map(|x| Value::Int(x % 17)).collect(),
        ),
        (
            VarName::new("y"),
            (0..size).map(|y| Value::Int((2 * y + 1) % 23)).collect(),
        ),
    ]))
}

fn arithmetic_inputs(size: usize) -> InputStream<Value> {
    let size = size as i64;
    map::input_stream(BTreeMap::from([
        (
            VarName::new("x"),
            (0..size).map(|x| Value::Int(x % 10_007)).collect(),
        ),
        (
            VarName::new("y"),
            (0..size)
                .map(|y| Value::Int((3 * y + 11) % 8_191))
                .collect(),
        ),
    ]))
}

fn arithmetic_input_columns(size: usize) -> Vec<Vec<Value>> {
    let size = size as i64;
    vec![
        (0..size).map(|x| Value::Int(x % 10_007)).collect(),
        (0..size)
            .map(|y| Value::Int((3 * y + 11) % 8_191))
            .collect(),
    ]
}

fn function_input_columns(size: usize) -> Vec<Vec<Value>> {
    let size = size as i64;
    vec![
        (0..size).map(|n| Value::Int(n % 10_007)).collect(),
        (0..size).map(|n| Value::Int((n * 7 + 3) % 97)).collect(),
    ]
}

fn recursive_function_input_columns(size: usize) -> Vec<Vec<Value>> {
    let size = size as i64;
    vec![
        (0..size).map(|n| Value::Int(n % 8)).collect(),
        (0..size).map(|n| Value::Int((n * 7 + 3) % 97)).collect(),
    ]
}

fn compare_dataflow_semantics(c: &mut Criterion) {
    let spec = recursive_spec();
    let typed_spec = typed_recursive_spec();

    let mut group = c.benchmark_group("dataflow_semantics_recursive");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(1));

    for size in [1_000, 10_000, 50_000] {
        group.bench_with_input(
            BenchmarkId::new("semisync_untyped_runtime", size),
            &size,
            |b, &size| {
                let benchmark_executor = LocalSmolExecutor::new();
                b.to_async(benchmark_executor.clone()).iter(|| {
                    monitor_recursive_outputs_semisync(
                        benchmark_executor.executor.clone(),
                        spec.clone(),
                        recursive_inputs(size),
                        size,
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("dataflow_untyped_runtime", size),
            &size,
            |b, &size| {
                let benchmark_executor = LocalSmolExecutor::new();
                b.to_async(benchmark_executor.clone()).iter(|| {
                    monitor_recursive_outputs_dataflow(
                        benchmark_executor.executor.clone(),
                        spec.clone(),
                        recursive_inputs(size),
                        size,
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("semisync_typed_runtime", size),
            &size,
            |b, &size| {
                let benchmark_executor = LocalSmolExecutor::new();
                b.to_async(benchmark_executor.clone()).iter(|| {
                    monitor_recursive_outputs_typed_semisync(
                        benchmark_executor.executor.clone(),
                        typed_spec.clone(),
                        recursive_inputs(size),
                        size,
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("dataflow_typed_runtime", size),
            &size,
            |b, &size| {
                let benchmark_executor = LocalSmolExecutor::new();
                b.to_async(benchmark_executor.clone()).iter(|| {
                    monitor_recursive_outputs_typed_dataflow(
                        benchmark_executor.executor.clone(),
                        typed_spec.clone(),
                        recursive_inputs(size),
                        size,
                    )
                })
            },
        );
    }

    group.finish();

    let spec = arithmetic_spec();
    let typed_spec = typed_arithmetic_spec();
    let mut group = c.benchmark_group("dataflow_semantics_arithmetic");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(1));

    for size in [1_000, 10_000, 50_000] {
        group.bench_with_input(
            BenchmarkId::new("semisync_untyped_runtime", size),
            &size,
            |b, &size| {
                let benchmark_executor = LocalSmolExecutor::new();
                b.to_async(benchmark_executor.clone()).iter(|| {
                    monitor_recursive_outputs_semisync(
                        benchmark_executor.executor.clone(),
                        spec.clone(),
                        arithmetic_inputs(size),
                        size,
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("dataflow_untyped_runtime", size),
            &size,
            |b, &size| {
                let benchmark_executor = LocalSmolExecutor::new();
                b.to_async(benchmark_executor.clone()).iter(|| {
                    monitor_recursive_outputs_dataflow(
                        benchmark_executor.executor.clone(),
                        spec.clone(),
                        arithmetic_inputs(size),
                        size,
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("semisync_typed_runtime", size),
            &size,
            |b, &size| {
                let benchmark_executor = LocalSmolExecutor::new();
                b.to_async(benchmark_executor.clone()).iter(|| {
                    monitor_recursive_outputs_typed_semisync(
                        benchmark_executor.executor.clone(),
                        typed_spec.clone(),
                        arithmetic_inputs(size),
                        size,
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("dataflow_typed_runtime", size),
            &size,
            |b, &size| {
                let benchmark_executor = LocalSmolExecutor::new();
                b.to_async(benchmark_executor.clone()).iter(|| {
                    monitor_recursive_outputs_typed_dataflow(
                        benchmark_executor.executor.clone(),
                        typed_spec.clone(),
                        arithmetic_inputs(size),
                        size,
                    )
                })
            },
        );
    }

    group.finish();

    let spec = stream_dependency_spec();
    let typed_spec = typed_stream_dependency_spec();
    let mut group = c.benchmark_group("dataflow_semantics_stream_dependency");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(1));

    for size in [1_000, 10_000, 50_000] {
        group.bench_with_input(
            BenchmarkId::new("semisync_untyped_runtime", size),
            &size,
            |b, &size| {
                let benchmark_executor = LocalSmolExecutor::new();
                b.to_async(benchmark_executor.clone()).iter(|| {
                    monitor_recursive_outputs_semisync(
                        benchmark_executor.executor.clone(),
                        spec.clone(),
                        arithmetic_inputs(size),
                        size,
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("dataflow_untyped_runtime", size),
            &size,
            |b, &size| {
                let benchmark_executor = LocalSmolExecutor::new();
                b.to_async(benchmark_executor.clone()).iter(|| {
                    monitor_recursive_outputs_dataflow(
                        benchmark_executor.executor.clone(),
                        spec.clone(),
                        arithmetic_inputs(size),
                        size,
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("semisync_typed_runtime", size),
            &size,
            |b, &size| {
                let benchmark_executor = LocalSmolExecutor::new();
                b.to_async(benchmark_executor.clone()).iter(|| {
                    monitor_recursive_outputs_typed_semisync(
                        benchmark_executor.executor.clone(),
                        typed_spec.clone(),
                        arithmetic_inputs(size),
                        size,
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("dataflow_typed_runtime", size),
            &size,
            |b, &size| {
                let benchmark_executor = LocalSmolExecutor::new();
                b.to_async(benchmark_executor.clone()).iter(|| {
                    monitor_recursive_outputs_typed_dataflow(
                        benchmark_executor.executor.clone(),
                        typed_spec.clone(),
                        arithmetic_inputs(size),
                        size,
                    )
                })
            },
        );
    }

    group.finish();

    let mut group = c.benchmark_group("function_call_binding");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(300));
    group.measurement_time(Duration::from_secs(2));

    for terms in [8, 64, 512] {
        for checked in [false, true] {
            let phase = if checked { "checked" } else { "untyped" };
            let mut bind =
                trustworthiness_checker::benches_common::function_binding_benchmark(terms, checked);
            group.bench_function(BenchmarkId::new(phase, terms), |b| {
                b.iter(|| std::hint::black_box(bind()))
            });
        }
    }
    group.finish();

    let arithmetic_spec = arithmetic_spec();
    let typed_arithmetic_spec = typed_arithmetic_spec();
    let if_arithmetic_spec = if_arithmetic_spec();
    let typed_if_arithmetic_spec = typed_if_arithmetic_spec();
    let function_spec = function_heavy_spec();
    let typed_function_spec = typed_function_heavy_spec();
    let recursive_function_if_spec = recursive_function_if_spec();
    let typed_recursive_function_if_spec = typed_recursive_function_if_spec();
    let direct_function_if_spec = direct_function_if_spec();
    let typed_direct_function_if_spec = typed_direct_function_if_spec();
    let mut group = c.benchmark_group("dataflow_semantics_function_evaluation");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(1));

    for size in [100, 1_000, 5_000] {
        group.bench_with_input(
            BenchmarkId::new("dataflow_untyped_arithmetic_monitor", size),
            &size,
            |b, &size| {
                b.iter(|| {
                    let mut monitor =
                        DataflowMonitor::try_compile_untyped(arithmetic_spec.clone()).unwrap();
                    evaluate_monitor(&mut monitor, &arithmetic_input_columns(size))
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("dataflow_untyped_function_monitor", size),
            &size,
            |b, &size| {
                b.iter(|| {
                    let mut monitor =
                        DataflowMonitor::try_compile_untyped(function_spec.clone()).unwrap();
                    evaluate_monitor(&mut monitor, &function_input_columns(size))
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("dataflow_typed_arithmetic_monitor", size),
            &size,
            |b, &size| {
                b.iter(|| {
                    let mut monitor =
                        DataflowMonitor::try_compile_checked(typed_arithmetic_spec.clone())
                            .unwrap();
                    evaluate_monitor(&mut monitor, &arithmetic_input_columns(size))
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("dataflow_untyped_if_arithmetic_monitor", size),
            &size,
            |b, &size| {
                b.iter(|| {
                    let mut monitor =
                        DataflowMonitor::try_compile_untyped(if_arithmetic_spec.clone()).unwrap();
                    evaluate_monitor(&mut monitor, &arithmetic_input_columns(size))
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("dataflow_typed_if_arithmetic_monitor", size),
            &size,
            |b, &size| {
                b.iter(|| {
                    let mut monitor =
                        DataflowMonitor::try_compile_checked(typed_if_arithmetic_spec.clone())
                            .unwrap();
                    evaluate_monitor(&mut monitor, &arithmetic_input_columns(size))
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("dataflow_typed_function_monitor", size),
            &size,
            |b, &size| {
                b.iter(|| {
                    let mut monitor =
                        DataflowMonitor::try_compile_checked(typed_function_spec.clone()).unwrap();
                    evaluate_monitor(&mut monitor, &function_input_columns(size))
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("dataflow_untyped_direct_if_function_monitor", size),
            &size,
            |b, &size| {
                b.iter(|| {
                    let mut monitor =
                        DataflowMonitor::try_compile_untyped(direct_function_if_spec.clone())
                            .unwrap();
                    evaluate_monitor(&mut monitor, &recursive_function_input_columns(size))
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("dataflow_typed_direct_if_function_monitor", size),
            &size,
            |b, &size| {
                b.iter(|| {
                    let mut monitor =
                        DataflowMonitor::try_compile_checked(typed_direct_function_if_spec.clone())
                            .unwrap();
                    evaluate_monitor(&mut monitor, &recursive_function_input_columns(size))
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("dataflow_untyped_recursive_if_function_monitor", size),
            &size,
            |b, &size| {
                b.iter(|| {
                    let mut monitor =
                        DataflowMonitor::try_compile_untyped(recursive_function_if_spec.clone())
                            .unwrap();
                    evaluate_monitor(&mut monitor, &recursive_function_input_columns(size))
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("dataflow_typed_recursive_if_function_monitor", size),
            &size,
            |b, &size| {
                b.iter(|| {
                    let mut monitor = DataflowMonitor::try_compile_checked(
                        typed_recursive_function_if_spec.clone(),
                    )
                    .unwrap();
                    evaluate_monitor(&mut monitor, &recursive_function_input_columns(size))
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, compare_dataflow_semantics);
criterion_main!(benches);
