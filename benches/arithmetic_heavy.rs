use std::collections::BTreeMap;
use std::rc::Rc;

use criterion::async_executor::AsyncExecutor;
use criterion::{BenchmarkId, Criterion, SamplingMode, criterion_group, criterion_main};
use smol::LocalExecutor;
use trustworthiness_checker::benches_common::{
    monitor_outputs_typed_dataflow, monitor_outputs_typed_semisync,
    monitor_outputs_untyped_dataflow, monitor_outputs_untyped_little,
};
use trustworthiness_checker::io::map;
use trustworthiness_checker::{
    CheckedDsrvSpecification, DsrvSpecification, InputStream, Value, VarName,
};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const STAGES: usize = 64;

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

fn arithmetic_specification() -> String {
    let mut source = String::from("in x: Int\nin y: Int\n");
    for stage in 0..STAGES {
        let role = if stage + 1 == STAGES { "out" } else { "aux" };
        source.push_str(&format!("{role} value_{stage}: Int\n"));
    }

    source.push_str("value_0 = x + y\n");
    for stage in 1..STAGES {
        let previous = format!("value_{}", stage - 1);
        let expression = match stage % 4 {
            0 => format!("{previous} + y"),
            1 => format!("{previous} * 3"),
            2 => format!("{previous} - x"),
            3 => format!("{previous} % 1000003"),
            _ => unreachable!(),
        };
        source.push_str(&format!("value_{stage} = {expression}\n"));
    }
    source
}

fn arithmetic_input(size: usize) -> InputStream<Value> {
    let x = (0..size)
        .map(|index| Value::Int((index % 997) as i64))
        .collect();
    let y = (0..size)
        .map(|index| Value::Int(((index * 17 + 3) % 991) as i64))
        .collect();
    map::input_stream(BTreeMap::from([
        (VarName::new("x"), x),
        (VarName::new("y"), y),
    ]))
}

fn arithmetic_heavy(c: &mut Criterion) {
    let sizes = [1_000, 10_000, 25_000];
    let source = arithmetic_specification();
    let untyped = source
        .parse::<DsrvSpecification>()
        .expect("arithmetic-heavy specification should parse");
    let checked = source
        .parse::<CheckedDsrvSpecification>()
        .expect("arithmetic-heavy specification should type check");

    let mut group = c.benchmark_group("arithmetic_heavy");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));

    for size in sizes {
        group.bench_with_input(BenchmarkId::new("dataflow", size), &size, |b, &size| {
            let benchmark_executor = LocalSmolExecutor::new();
            b.to_async(benchmark_executor.clone()).iter(|| {
                monitor_outputs_untyped_dataflow(
                    benchmark_executor.executor.clone(),
                    untyped.clone(),
                    arithmetic_input(size),
                )
            })
        });
        group.bench_with_input(
            BenchmarkId::new("dataflow_specialised", size),
            &size,
            |b, &size| {
                let benchmark_executor = LocalSmolExecutor::new();
                b.to_async(benchmark_executor.clone()).iter(|| {
                    monitor_outputs_typed_dataflow(
                        benchmark_executor.executor.clone(),
                        checked.clone(),
                        arithmetic_input(size),
                        trustworthiness_checker::core::Semantics::TypedUntimed,
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("semisync_untyped", size),
            &size,
            |b, &size| {
                let benchmark_executor = LocalSmolExecutor::new();
                b.to_async(benchmark_executor.clone()).iter(|| {
                    monitor_outputs_untyped_little(
                        benchmark_executor.executor.clone(),
                        untyped.clone(),
                        arithmetic_input(size),
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("semisync_typed", size),
            &size,
            |b, &size| {
                let benchmark_executor = LocalSmolExecutor::new();
                b.to_async(benchmark_executor.clone()).iter(|| {
                    monitor_outputs_typed_semisync(
                        benchmark_executor.executor.clone(),
                        checked.clone(),
                        arithmetic_input(size),
                    )
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, arithmetic_heavy);
criterion_main!(benches);
