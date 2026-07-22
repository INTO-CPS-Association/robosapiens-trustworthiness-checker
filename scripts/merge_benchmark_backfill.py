#!/usr/bin/env python3
"""Merge missing historical benchmark measurements into dashboard data."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

PREFIX = "window.BENCHMARK_DATA = "


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
    print(f"Added {added} historical benchmark measurements")
    if failures:
        print(f"Skipped {len(failures)} failed historical benchmarks:")
        print("\n".join(failures))


if __name__ == "__main__":
    main()
