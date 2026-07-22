#!/usr/bin/env python3
"""Build, pin, run and collect injected historical Criterion benchmarks."""

from __future__ import annotations

import argparse
import json
import stat
import subprocess
from pathlib import Path
from typing import Any

EXPECTED = {
    "dup-semisync": ("dup_defer", "dup_defer/dup_defer_untyped_semisync/25000"),
    "dup-dataflow": ("dup_defer", "dup_defer/dup_defer_untyped_dataflow/25000"),
    "dyn-async": ("dyn_paper", "dyn_paper/dyn_paper_50/100000"),
    "dyn-semisync": ("dyn_paper", "dyn_paper/dyn_paper_50_semisync/100000"),
    "dyn-dataflow": ("dyn_paper", "dyn_paper/dyn_paper_50_dataflow/100000"),
    "maple-async": (
        "maple_sequence",
        "maple_sequence/maple_sequence_untyped_async/25000",
    ),
    "maple-semisync": (
        "maple_sequence",
        "maple_sequence/maple_sequence_untyped_semisync/25000",
    ),
    "maple-dataflow": (
        "maple_sequence",
        "maple_sequence/maple_sequence_untyped_dataflow/25000",
    ),
}
PIPELINE = [
    "compilation_phases/lalr_parse/1024",
    "compilation_phases/strict_type_check/1024",
    "compilation_phases/typed_dependency_graph/1024",
    "compilation_phases/parse_typecheck_dependency_compile_typed/1024",
]


def p_core() -> str:
    output = subprocess.run(
        ["lscpu", "-e=CPU,MAXMHZ"], check=True, text=True, capture_output=True
    ).stdout
    candidates = []
    for line in output.splitlines()[1:]:
        fields = line.split()
        if len(fields) == 2 and fields[1] != "-":
            candidates.append((float(fields[1]), int(fields[0])))
    if not candidates:
        return "0"
    return str(max(candidates)[1])


def build(repo: Path, target: str) -> Path:
    subprocess.run(
        [
            "cargo",
            "bench",
            "--profile",
            "bench-fast",
            "--bench",
            target,
            "--no-run",
        ],
        cwd=repo,
        check=True,
    )
    deps = repo / "target/bench-fast/deps"
    candidates = [
        path
        for path in deps.glob(f"{target.replace('-', '_')}-*")
        if path.is_file() and not path.suffix and path.stat().st_mode & stat.S_IXUSR
    ]
    if not candidates:
        raise FileNotFoundError(f"could not find executable for benchmark {target}")
    return max(candidates, key=lambda path: path.stat().st_mtime_ns).resolve()


def run(executable: Path, cpu: str, benchmark_filter: str, repo: Path) -> None:
    subprocess.run(
        ["taskset", "-c", cpu, str(executable), benchmark_filter, "--bench"],
        cwd=repo,
        check=True,
    )


def read_results(repo: Path, expected: set[str]) -> list[dict[str, Any]]:
    results = []
    for estimate_path in (repo / "target/criterion").glob("**/new/estimates.json"):
        benchmark_path = estimate_path.with_name("benchmark.json")
        if not benchmark_path.is_file():
            continue
        benchmark = json.loads(benchmark_path.read_text())
        name = benchmark["full_id"]
        if name not in expected:
            continue
        estimates = json.loads(estimate_path.read_text())
        results.append(
            {
                "name": name,
                "value": estimates["median"]["point_estimate"],
                "range": f"± {estimates['std_dev']['point_estimate']}",
                "unit": "ns/iter",
            }
        )
    found = {result["name"] for result in results}
    missing = expected - found
    if missing:
        raise RuntimeError(f"missing Criterion results: {sorted(missing)}")
    return sorted(results, key=lambda result: result["name"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repo", type=Path)
    parser.add_argument("--benchmarks", required=True)
    parser.add_argument("--sha", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    requested = set(args.benchmarks.split(","))
    targets: dict[str, list[str]] = {}
    expected = set()
    for request in requested:
        if request == "pipeline":
            targets.setdefault("backfill_compilation_phases", []).append(
                "compilation_phases/"
            )
            expected.update(PIPELINE)
        else:
            target, name = EXPECTED[request]
            targets.setdefault(target, []).append(name)
            expected.add(name)

    cpu = p_core()
    for target, filters in targets.items():
        executable = build(args.repo, target)
        for benchmark_filter in filters:
            run(executable, cpu, benchmark_filter, args.repo)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(
            {"sha": args.sha, "benches": read_results(args.repo, expected)}, indent=2
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
