#!/usr/bin/env python3
"""Merge missing historical benchmark measurements into dashboard data."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

PREFIX = "window.BENCHMARK_DATA = "

# Each family is gated by its old specialised key, which identifies a pre-taxonomy run. This
# prevents an old untyped `dataflow` measurement from being confused with the new canonical tier.
TIER_MIGRATIONS = [
    (
        "maple_sequence/maple_sequence_dataflow_specialised/25000",
        [
            (
                "maple_sequence/maple_sequence_untyped_dataflow/25000",
                "maple_sequence/maple_sequence_dataflow_untyped/25000",
            ),
            (
                "maple_sequence/maple_sequence_dataflow_specialised/25000",
                "maple_sequence/maple_sequence_dataflow_quickened/25000",
            ),
        ],
    ),
    (
        "dyn_paper/dyn_paper_50_dataflow_specialised/100000",
        [
            (
                "dyn_paper/dyn_paper_50_dataflow/100000",
                "dyn_paper/dyn_paper_50_dataflow_untyped/100000",
            ),
            (
                "dyn_paper/dyn_paper_50_dataflow_specialised/100000",
                "dyn_paper/dyn_paper_50_dataflow_quickened/100000",
            ),
        ],
    ),
    (
        "dup_defer/dup_defer_dataflow_specialised/25000",
        [
            (
                "dup_defer/dup_defer_untyped_dataflow/25000",
                "dup_defer/dup_defer_dataflow_untyped/25000",
            ),
            (
                "dup_defer/dup_defer_dataflow_specialised/25000",
                "dup_defer/dup_defer_dataflow_quickened/25000",
            ),
        ],
    ),
    (
        "arithmetic_heavy/dataflow_specialised/25000",
        [
            (
                "arithmetic_heavy/dataflow/25000",
                "arithmetic_heavy/dataflow_untyped/25000",
            ),
            (
                "arithmetic_heavy/dataflow_specialised/25000",
                "arithmetic_heavy/dataflow_quickened/25000",
            ),
        ],
    ),
    (
        "threshold_property/dsrv_dataflow_specialised/10000",
        [
            (
                "threshold_property/dsrv_dataflow_specialised/10000",
                "threshold_property/dsrv_dataflow_quickened/10000",
            ),
        ],
    ),
    (
        "time_dependent_property/dsrv_default_window_dataflow_specialised/10000",
        [
            (
                "time_dependent_property/dsrv_default_window_dataflow_specialised/10000",
                "time_dependent_property/dsrv_default_window_dataflow_quickened/10000",
            ),
        ],
    ),
]
for variant in ["automatic_scope", "explicit_components"]:
    old_prefix = f"hard_dynamic_defer/{variant}"
    TIER_MIGRATIONS.append(
        (
            f"{old_prefix}_dataflow_specialised/1024",
            [
                (f"{old_prefix}_dataflow/1024", f"{old_prefix}_dataflow_untyped/1024"),
                (
                    f"{old_prefix}_dataflow_specialised/1024",
                    f"{old_prefix}_dataflow_quickened/1024",
                ),
            ],
        )
    )


def migrate_tier_names(entries: list[dict[str, Any]]) -> int:
    migrated = 0
    for run in entries:
        benches = {bench["name"]: bench for bench in run["benches"]}
        for marker, renames in TIER_MIGRATIONS:
            if marker not in benches:
                continue
            for old_name, new_name in renames:
                bench = benches.pop(old_name, None)
                if bench is None:
                    continue
                if new_name not in benches:
                    bench["name"] = new_name
                    benches[new_name] = bench
                    migrated += 1
        run["benches"] = sorted(benches.values(), key=lambda bench: bench["name"])
    return migrated


def load_data(path: Path) -> dict[str, Any]:
    text = path.read_text()
    if not text.startswith(PREFIX):
        raise ValueError(f"{path} does not contain benchmark dashboard data")
    return json.loads(text.removeprefix(PREFIX).removesuffix(";\n").removesuffix(";"))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data", type=Path)
    parser.add_argument("results", type=Path)
    args = parser.parse_args()

    data = load_data(args.data)
    entries = next(iter(data["entries"].values()))
    migrated = migrate_tier_names(entries)
    runs = {run["commit"]["id"]: run for run in entries}
    added = 0
    failures = []
    for result_path in sorted(args.results.glob("**/*.json")):
        result = json.loads(result_path.read_text())
        failures.extend(
            f"{result['sha']}:{benchmark}" for benchmark in result.get("failures", [])
        )
        run = runs.get(result["sha"])
        if run is None:
            raise ValueError(f"no historical run for {result['sha']}")
        existing = {bench["name"] for bench in run["benches"]}
        for bench in result["benches"]:
            if bench["name"] not in existing:
                run["benches"].append(bench)
                existing.add(bench["name"])
                added += 1
        run["benches"].sort(key=lambda bench: bench["name"])

    args.data.write_text(PREFIX + json.dumps(data, indent=2) + "\n")
    print(f"Migrated {migrated} historical benchmark tier names")
    print(f"Added {added} historical benchmark measurements")
    if failures:
        print(f"Skipped {len(failures)} failed historical benchmarks:")
        print("\n".join(failures))


if __name__ == "__main__":
    main()
