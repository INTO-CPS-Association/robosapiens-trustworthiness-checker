use std::collections::{BTreeMap, BTreeSet};
use std::hint::black_box;
use std::rc::Rc;
use std::{num::NonZeroUsize, time::Duration};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::StreamExt;
use smol::LocalExecutor;

use trustworthiness_checker::benches_common::{
    monitor_outputs_untyped_async_limited, monitor_outputs_untyped_semisync_limited,
};
use trustworthiness_checker::core::{InputBatch, InputStream, InputUpdate, input};
use trustworthiness_checker::io::map;
use trustworthiness_checker::io::{
    InputPipeline, InputPolicy, InputReduction, InputSource, InputSources, InputWindow,
    OutputBackendConfig, OutputPipeline,
};
use trustworthiness_checker::runtime::RuntimeBuilder;
use trustworthiness_checker::{DsrvSpecification, Runtime, Value, VarName};

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const ROWS: usize = 256;

fn variable(index: usize) -> VarName {
    VarName::new(&format!("v{index}"))
}

fn packed_batch(rows: usize, width: usize) -> InputBatch<Value> {
    let columns = (0..width)
        .map(|index| {
            (
                variable(index),
                (0..rows)
                    .map(|row| Value::Int((row * width + index) as i64))
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    smol::block_on(async {
        let mut stream = map::typed_input_stream(columns).unwrap();
        stream.next().await.unwrap().unwrap()
    })
}

fn packed_stream(rows: usize, width: usize, offset: i64) -> InputStream<Value> {
    let columns = (0..width)
        .map(|index| {
            (
                variable(index),
                (0..rows)
                    .map(|row| Value::Int(offset + (row * width + index) as i64))
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    map::typed_input_stream(columns).unwrap()
}

fn singleton_batch(variable_name: &str, start: usize, count: usize) -> InputBatch<Value> {
    InputBatch::from_ticks(
        (start..start + count)
            .map(|index| {
                vec![InputUpdate::new(
                    VarName::new(variable_name),
                    Value::Int(index as i64),
                )]
            })
            .collect(),
    )
    .unwrap()
}

fn singleton_stream(
    variable_name: &'static str,
    batches: usize,
    updates_per_batch: usize,
    offset: usize,
) -> InputStream<Value> {
    Box::pin(futures::stream::iter((0..batches).map(move |batch| {
        Ok(singleton_batch(
            variable_name,
            offset + batch * updates_per_batch,
            updates_per_batch,
        ))
    })))
}

async fn count_ticks(mut stream: InputStream<Value>) -> usize {
    let mut ticks = 0;
    while let Some(batch) = stream.next().await {
        ticks += batch.unwrap().tick_count();
    }
    ticks
}

async fn count_updates(mut stream: InputStream<Value>) -> usize {
    let mut updates = 0;
    while let Some(batch) = stream.next().await {
        updates += batch.unwrap().update_count();
    }
    updates
}

async fn consume_input_stream(mut stream: InputStream<Value>) -> usize {
    let mut ticks = 0;
    while let Some(batch) = stream.next().await {
        ticks += batch.unwrap().ticks().len();
    }
    ticks
}

fn bench_packed_iteration(c: &mut Criterion) {
    let mut group = c.benchmark_group("input/packed_iteration");
    for (width, rows) in [(2, ROWS), (8, ROWS), (32, 64)] {
        let batch = packed_batch(rows, width);
        group.throughput(Throughput::Elements((rows * width) as u64));
        group.bench_with_input(
            BenchmarkId::new(format!("width_{width}"), rows),
            &batch,
            |benchmark, batch| {
                benchmark.iter(|| {
                    let sum = batch.ticks().fold(0_i64, |sum, row| {
                        sum + row.iter().fold(0_i64, |row_sum, update| {
                            row_sum
                                + match update.value {
                                    Value::Int(value) => *value,
                                    _ => 0,
                                }
                        })
                    });
                    black_box(sum)
                });
            },
        );
    }
    group.finish();
}

fn bench_dataflow_packed_rows(c: &mut Criterion) {
    let mut group = c.benchmark_group("input/dataflow_packed_rows");
    for rows in [64, ROWS, 1024] {
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(rows),
            &rows,
            |benchmark, &rows| {
                benchmark.iter_batched(
                    || prepare_dataflow(rows),
                    |runtime| {
                        black_box(smol::block_on(runtime.run()).unwrap());
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn prepare_dataflow(rows: usize) -> trustworthiness_checker::runtime::dataflow::DataflowRuntime {
    let executor = Rc::new(LocalExecutor::new());
    let spec = "in x\nin y\nout sum\nsum = x + y"
        .parse::<DsrvSpecification>()
        .unwrap();
    let input = map::input_stream(BTreeMap::from([
        (
            VarName::new("x"),
            (0..rows).map(|row| Value::Int(row as i64)).collect(),
        ),
        (
            VarName::new("y"),
            (0..rows).map(|row| Value::Int((2 * row) as i64)).collect(),
        ),
    ]));
    let output = smol::block_on(
        OutputPipeline::from_backend(OutputBackendConfig::null()).build(
            spec.output_vars(),
            spec.aux_vars(),
            None,
        ),
    )
    .unwrap();
    smol::block_on(
        trustworthiness_checker::runtime::dataflow::DataflowRuntimeBuilder::<DsrvSpecification>::new()
            .executor(executor)
            .model(spec)
            .input(input.into())
            .output_writer(output)
            .build(),
    )
}

fn bench_packed_row_consumption(c: &mut Criterion) {
    let mut group = c.benchmark_group("input/packed_row_consumption");
    for (name, width) in [("narrow", 2), ("wide", 16)] {
        group.throughput(Throughput::Elements((ROWS * width) as u64));
        group.bench_function(name, |benchmark| {
            benchmark.iter_batched(
                || packed_stream(ROWS, width, 0),
                |stream| {
                    let count = smol::block_on(async { consume_input_stream(stream).await });
                    black_box(count)
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_singleton_consumption(c: &mut Criterion) {
    let mut group = c.benchmark_group("input/singleton_ticks");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("width_one", |benchmark| {
        benchmark.iter_batched(
            || singleton_stream("x", 4, ROWS / 4, 0),
            |stream| black_box(smol::block_on(async { count_ticks(stream).await })),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_input_composition(c: &mut Criterion) {
    let mut group = c.benchmark_group("input/composition");
    group.throughput(Throughput::Elements((ROWS * 3) as u64));
    group.bench_function("three_singleton_sources", |benchmark| {
        benchmark.iter_batched(
            || {
                input::compose_input_streams(vec![
                    singleton_stream("x", 4, ROWS / 4, 0),
                    singleton_stream("y", 4, ROWS / 4, 0),
                    singleton_stream("z", 4, ROWS / 4, 0),
                ])
            },
            |stream| black_box(smol::block_on(async { count_updates(stream).await })),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("two_packed_sources", |benchmark| {
        benchmark.iter_batched(
            || {
                input::compose_input_streams(vec![
                    packed_stream(ROWS, 2, 0),
                    packed_stream(ROWS, 2, 10),
                ])
            },
            |stream| black_box(smol::block_on(async { count_ticks(stream).await })),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("mixed_without_widening", |benchmark| {
        benchmark.iter_batched(
            || {
                input::compose_input_streams(vec![
                    singleton_stream("x", 4, ROWS / 4, 0),
                    packed_stream(ROWS, 2, 10),
                ])
            },
            |stream| black_box(smol::block_on(async { count_ticks(stream).await })),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn coalescing_pipeline(
    child_count: usize,
    width: usize,
    updates_per_batch: usize,
    update_limit: Option<usize>,
) -> (InputPipeline<Value>, BTreeSet<VarName>) {
    let mut sources = InputSources::new();
    let mut input_vars = BTreeSet::new();
    for child_index in 0..child_count {
        let child_vars = (0..width)
            .map(|offset| variable(child_index * width + offset))
            .collect::<Vec<_>>();
        input_vars.extend(child_vars.iter().cloned());
        let mut batches = Vec::new();
        for batch_index in 0..4 {
            let mut ticks = Vec::with_capacity(updates_per_batch);
            for update in 0..updates_per_batch {
                ticks.push(
                    child_vars
                        .iter()
                        .enumerate()
                        .map(|(offset, var)| {
                            InputUpdate::new(
                                var.clone(),
                                Value::Int(
                                    (batch_index * updates_per_batch + update + offset) as i64,
                                ),
                            )
                        })
                        .collect(),
                );
            }
            batches.push(InputBatch::from_ticks(ticks).unwrap());
        }
        sources = sources.insert(
            format!("source{child_index}"),
            InputSource::in_memory_ticks(batches),
        );
    }
    let window = InputWindow::new(
        Some(Duration::ZERO),
        update_limit.and_then(NonZeroUsize::new),
    )
    .unwrap();
    let pipeline = InputPipeline::from_sources(sources)
        .with_policy(InputPolicy::WindowToStep {
            window,
            reduction: InputReduction::LastUpdateWins,
        })
        .unwrap();
    (pipeline, input_vars)
}

fn bench_pipeline_windows(c: &mut Criterion) {
    let mut group = c.benchmark_group("input/windows");
    for (name, child_count, width, updates_per_batch, update_limit) in [
        ("narrow_repeated", 2, 1, 16, None),
        ("wide_repeated", 3, 4, 16, None),
        ("wide_limited", 4, 4, 16, Some(16)),
        ("many_children_limited", 6, 2, 8, Some(8)),
    ] {
        let update_count = child_count * width * updates_per_batch * 4;
        group.throughput(Throughput::Elements(update_count as u64));
        group.bench_function(name, |benchmark| {
            benchmark.iter_batched(
                || coalescing_pipeline(child_count, width, updates_per_batch, update_limit),
                |(pipeline, input_vars)| {
                    let stream =
                        smol::block_on(async { pipeline.build(input_vars).await.unwrap() });
                    black_box(smol::block_on(async {
                        consume_input_stream(Box::pin(stream)).await
                    }))
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_async_and_semisync_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("input/runtime_fanout");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("async", |benchmark| {
        benchmark.iter(|| run_async_runtime(ROWS));
    });
    group.bench_function("semi_sync", |benchmark| {
        benchmark.iter(|| run_semi_sync_runtime(ROWS));
    });
    group.finish();
}

fn runtime_input(rows: usize) -> InputStream<Value> {
    map::input_stream(BTreeMap::from([(
        VarName::new("x"),
        (0..rows).map(|row| Value::Int(row as i64)).collect(),
    )]))
}

fn run_async_runtime(rows: usize) {
    let executor = Rc::new(LocalExecutor::new());
    let spec = "in x\nout y\ny = x".parse::<DsrvSpecification>().unwrap();
    smol::block_on(executor.run(monitor_outputs_untyped_async_limited(
        executor.clone(),
        spec,
        runtime_input(rows),
        rows,
    )));
}

fn run_semi_sync_runtime(rows: usize) {
    let executor = Rc::new(LocalExecutor::new());
    let spec = "in x\nout y\ny = x".parse::<DsrvSpecification>().unwrap();
    smol::block_on(executor.run(monitor_outputs_untyped_semisync_limited(
        executor.clone(),
        spec,
        runtime_input(rows),
        rows,
    )));
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(30);
    targets = bench_packed_iteration,
        bench_dataflow_packed_rows,
        bench_packed_row_consumption,
        bench_singleton_consumption,
        bench_input_composition,
        bench_pipeline_windows,
        bench_async_and_semisync_fanout
}
criterion_main!(benches);
