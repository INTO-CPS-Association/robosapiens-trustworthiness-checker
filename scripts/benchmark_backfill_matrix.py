#!/usr/bin/env python3
"""Select missing historical dashboard benchmarks from benchmark-data."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any

PREFIX = "window.BENCHMARK_DATA = "
DATAFLOW_INTRODUCTION = "9f0f006d16a999c6297fa2ef10f17f23f2bb86ba"


def load_data(path: Path) -> dict[str, Any]:
    text = path.read_text()
    if not text.startswith(PREFIX):
        raise ValueError(f"{path} does not contain benchmark dashboard data")
    return json.loads(text.removeprefix(PREFIX).removesuffix(";\n").removesuffix(";"))


def is_ancestor(ancestor: str, commit: str) -> bool:
    return (
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", ancestor, commit],
            check=False,
        ).returncode
        == 0
    )


def has(benches: set[str], name: str) -> bool:
    return name in benches


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data", type=Path)
    args = parser.parse_args()

    entries = next(iter(load_data(args.data)["entries"].values()))
    matrix = []
    for run in entries:
        sha = run["commit"]["id"]
        benches = {bench["name"] for bench in run["benches"]}
        requested: list[str] = []

        always_available = {
            "dup-semisync": "dup_defer/dup_defer_untyped_semisync/25000",
            "dyn-async": "dyn_paper/dyn_paper_50/100000",
            "dyn-semisync": "dyn_paper/dyn_paper_50_semisync/100000",
            "maple-async": "maple_sequence/maple_sequence_untyped_async/25000",
            "maple-semisync": "maple_sequence/maple_sequence_untyped_semisync/25000",
        }
        requested.extend(
            benchmark
            for benchmark, name in always_available.items()
            if not has(benches, name)
        )

        if is_ancestor(DATAFLOW_INTRODUCTION, sha):
            dataflow_benchmarks = {
                "dup-dataflow": "dup_defer/dup_defer_untyped_dataflow/25000",
                "dyn-dataflow": "dyn_paper/dyn_paper_50_dataflow/100000",
                "maple-dataflow": "maple_sequence/maple_sequence_untyped_dataflow/25000",
            }
            requested.extend(
                benchmark
                for benchmark, name in dataflow_benchmarks.items()
                if not has(benches, name)
            )
            pipeline = [
                "compilation_phases/lalr_parse/1024",
                "compilation_phases/strict_type_check/1024",
                "compilation_phases/typed_dependency_graph/1024",
                "compilation_phases/parse_typecheck_dependency_compile_typed/1024",
            ]
            if any(name not in benches for name in pipeline):
                requested.append("pipeline")

        if requested:
            matrix.append({"sha": sha, "benchmarks": ",".join(requested)})

    print(json.dumps({"include": matrix}, separators=(",", ":")))


if __name__ == "__main__":
    main()
