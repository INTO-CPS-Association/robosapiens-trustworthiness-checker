#!/usr/bin/env python3
"""Inject selected dashboard benchmarks into a historical checkout."""

from __future__ import annotations

import argparse
from pathlib import Path


def function_block(source: str, name: str) -> tuple[int, int, str]:
    start = source.index(f"pub async fn {name}(")
    brace = source.index("{", start)
    depth = 0
    for index in range(brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return start, index + 1, source[start : index + 1]
    raise ValueError(f"unterminated function {name}")


def ensure_limited_semisync(repo: Path) -> None:
    path = repo / "src/benches_common.rs"
    source = path.read_text()
    target = "monitor_outputs_untyped_semisync_limited"
    if target in source:
        return
    _, end, block = function_block(source, "monitor_outputs_untyped_async_limited")
    block = block.replace("monitor_outputs_untyped_async_limited", target, 1)
    if "RuntimeSpec::Async" in block:
        block = block.replace("RuntimeSpec::Async", "RuntimeSpec::SemiSync", 1)
    else:
        block = block.replace("Runtime::Async", "Runtime::SemiSync", 1)
    path.write_text(source[:end] + "\n\n" + block + source[end:])


def ensure_dataflow_helpers(repo: Path) -> None:
    path = repo / "src/benches_common.rs"
    source = path.read_text()
    if "use crate::core::ExecutionPolicy;" not in source:
        anchor = "use crate::core::OutputHandler;"
        source = source.replace(anchor, "use crate::core::ExecutionPolicy;\n" + anchor, 1)
    for suffix in ["_limited", ""]:
        source_name = f"monitor_outputs_untyped_async{suffix}"
        target_name = f"monitor_outputs_untyped_dataflow{suffix}"
        if f"pub async fn {target_name}(" in source:
            continue
        _, end, block = function_block(source, source_name)
        block = block.replace(source_name, target_name, 1).replace(
            "RuntimeSpec::Async",
            "RuntimeSpec::Dataflow(ExecutionPolicy::Buffered)",
            1,
        )
        source = source[:end] + "\n\n" + block + source[end:]
    path.write_text(source)


def benchmark_block(source: str, benchmark_id: str) -> tuple[int, int, str]:
    marker = f"BenchmarkId::new({benchmark_id}"
    marker_index = source.index(marker)
    start = source.rfind("        group.bench_with_input(", 0, marker_index)
    if start < 0:
        raise ValueError(f"could not find benchmark block for {benchmark_id}")
    terminator = "\n        );"
    end = source.index(terminator, marker_index) + len(terminator)
    return start, end, source[start:end]


def ensure_import(source: str, source_function: str, target_function: str) -> str:
    target_import = f"use trustworthiness_checker::benches_common::{target_function};"
    if target_import in source:
        return source
    source_import = f"use trustworthiness_checker::benches_common::{source_function};"
    if source_import not in source:
        raise ValueError(f"missing import for {source_function}")
    return source.replace(source_import, source_import + "\n" + target_import, 1)


def clone_benchmark(
    path: Path,
    source_id: str,
    target_id: str,
    source_function: str,
    target_function: str,
) -> None:
    source = path.read_text()
    if target_id in source:
        return
    source = ensure_import(source, source_function, target_function)
    _, end, block = benchmark_block(source, source_id)
    clone = block.replace(source_id, target_id, 1).replace(
        source_function, target_function
    )
    path.write_text(source[:end] + "\n" + clone + source[end:])


def ensure_bench_target(repo: Path, name: str) -> None:
    cargo = repo / "Cargo.toml"
    source = cargo.read_text()
    declaration = f'[[bench]]\nname = "{name}"\nharness = false'
    if declaration not in source:
        source += "\n" + declaration + "\n"
    if "[profile.bench-fast]" not in source:
        source += '\n[profile.bench-fast]\ninherits = "release"\nlto = false\ncodegen-units = 16\n'
    cargo.write_text(source)


def old_pipeline_source(allocator: str) -> str:
    return f"""use std::hint::black_box;
use std::time::Duration;

use criterion::{{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main}};
use trustworthiness_checker::dataflow::DataflowMonitor;
use trustworthiness_checker::lang::core::dependency_graph::{{
    DependencyGraphRoots, DependencyGraphSpec,
}};
use trustworthiness_checker::lang::dsrv::lalr_parser::parse_str;
use trustworthiness_checker::lang::dsrv::type_checker::type_check;

#[global_allocator]
static GLOBAL: {allocator}::Jemalloc = {allocator}::Jemalloc;

fn compilation_input(assignments: usize) -> String {{
    let mut source = String::from("in input: Int\\n");
    for index in 0..assignments {{
        let previous = if index == 0 {{ "input".to_owned() }} else {{ format!("value{{}}", index - 1) }};
        source.push_str(&format!("aux value{{index}}: Int\\n"));
        source.push_str(&format!("value{{index}} = ({{previous}} + {{index}}) * 3 - ({{previous}} % 7)\\n"));
    }}
    source.push_str("out result: Int\\n");
    source.push_str(&format!("result = value{{}} + 1\\n", assignments - 1));
    source
}}

fn compilation_phases(c: &mut Criterion) {{
    let assignments = 1024;
    let source = compilation_input(assignments);
    let parsed = parse_str(&source).expect("benchmark input should parse");
    let typed = type_check(parsed.clone()).expect("benchmark input should type check");
    DataflowMonitor::try_compile_typed(typed.clone()).expect("benchmark input should compile");
    let mut group = c.benchmark_group("compilation_phases");
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(750));
    group.measurement_time(Duration::from_secs(3));
    group.bench_with_input(BenchmarkId::new("lalr_parse", assignments), &source, |b, source| {{
        b.iter(|| black_box(parse_str(black_box(source)).unwrap()))
    }});
    group.bench_function(BenchmarkId::new("strict_type_check", assignments), |b| {{
        b.iter_batched(
            || parse_str(&source).unwrap(),
            |spec| black_box(type_check(spec).unwrap()),
            BatchSize::SmallInput,
        )
    }});
    group.bench_function(BenchmarkId::new("typed_dependency_graph", assignments), |b| {{
        b.iter(|| black_box(black_box(&typed).dependency_graph_for(DependencyGraphRoots::AllStreams)))
    }});
    group.bench_with_input(
        BenchmarkId::new("parse_typecheck_dependency_compile_typed", assignments),
        &source,
        |b, source| b.iter(|| {{
            let parsed = parse_str(black_box(source)).unwrap();
            let typed = type_check(parsed).unwrap();
            black_box(typed.dependency_graph_for(DependencyGraphRoots::AllStreams));
            black_box(DataflowMonitor::try_compile_typed(typed).unwrap())
        }}),
    );
    group.finish();
}}

criterion_group!(benches, compilation_phases);
criterion_main!(benches);
"""


def ensure_pipeline(repo: Path) -> None:
    path = repo / "benches/backfill_compilation_phases.rs"
    if not path.exists():
        cargo = (repo / "Cargo.toml").read_text()
        allocator = (
            "tikv_jemallocator" if "tikv-jemallocator" in cargo else "jemallocator"
        )
        path.write_text(old_pipeline_source(allocator))
    ensure_bench_target(repo, "backfill_compilation_phases")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repo", type=Path)
    parser.add_argument("--benchmarks", required=True)
    args = parser.parse_args()
    repo = args.repo
    requested = set(args.benchmarks.split(","))

    ensure_bench_target(repo, "dup_defer")
    ensure_bench_target(repo, "dyn_paper")
    ensure_bench_target(repo, "maple_sequence")
    if requested & {"dup-dataflow", "dyn-dataflow", "maple-dataflow"}:
        ensure_dataflow_helpers(repo)

    if "dup-semisync" in requested:
        clone_benchmark(
            repo / "benches/dup_defer.rs",
            '"dup_defer_untyped_async"',
            '"dup_defer_untyped_semisync"',
            "monitor_outputs_untyped_async",
            "monitor_outputs_untyped_little",
        )
    if "dup-dataflow" in requested:
        clone_benchmark(
            repo / "benches/dup_defer.rs",
            '"dup_defer_untyped_async"',
            '"dup_defer_untyped_dataflow"',
            "monitor_outputs_untyped_async",
            "monitor_outputs_untyped_dataflow",
        )

    if "dyn-semisync" in requested:
        ensure_limited_semisync(repo)
        clone_benchmark(
            repo / "benches/dyn_paper.rs",
            'format!("dyn_paper_{}", percent)',
            'format!("dyn_paper_{}_semisync", percent)',
            "monitor_outputs_untyped_async_limited",
            "monitor_outputs_untyped_semisync_limited",
        )
    if "dyn-dataflow" in requested:
        clone_benchmark(
            repo / "benches/dyn_paper.rs",
            'format!("dyn_paper_{}", percent)',
            'format!("dyn_paper_{}_dataflow", percent)',
            "monitor_outputs_untyped_async_limited",
            "monitor_outputs_untyped_dataflow_limited",
        )

    if "maple-semisync" in requested:
        clone_benchmark(
            repo / "benches/maple_sequence.rs",
            '"maple_sequence_untyped_async"',
            '"maple_sequence_untyped_semisync"',
            "monitor_outputs_untyped_async",
            "monitor_outputs_untyped_little",
        )
    if "maple-dataflow" in requested:
        clone_benchmark(
            repo / "benches/maple_sequence.rs",
            '"maple_sequence_untyped_async"',
            '"maple_sequence_untyped_dataflow"',
            "monitor_outputs_untyped_async",
            "monitor_outputs_untyped_dataflow",
        )

    if "pipeline" in requested:
        ensure_pipeline(repo)


if __name__ == "__main__":
    main()
