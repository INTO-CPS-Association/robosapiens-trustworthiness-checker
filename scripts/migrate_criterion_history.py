#!/usr/bin/env python3
"""Convert archived Criterion reports to github-action-benchmark data.js."""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any


ENTRY_NAME = "RoboSAPIENS Criterion benchmarks"


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], check=True, text=True, capture_output=True
    ).stdout.strip()


def commit_metadata(sha: str, repository_url: str) -> tuple[dict[str, Any], int]:
    fields = git(
        "show",
        "-s",
        "--format=%H%x00%an%x00%ae%x00%cn%x00%ce%x00%cI%x00%B",
        sha,
    ).split("\0", 6)
    commit_sha, author_name, author_email, committer_name, committer_email, timestamp, message = fields
    date = int(datetime.fromisoformat(timestamp).timestamp() * 1000)
    return (
        {
            "author": {"name": author_name, "email": author_email},
            "committer": {"name": committer_name, "email": committer_email},
            "id": commit_sha,
            "message": message.strip(),
            "timestamp": timestamp,
            "url": f"{repository_url.rstrip('/')}/commit/{commit_sha}",
        },
        date,
    )


def read_benchmarks(report_dir: Path) -> list[dict[str, Any]]:
    benches = []
    for estimate_path in sorted(report_dir.glob("**/new/estimates.json")):
        benchmark_path = estimate_path.with_name("benchmark.json")
        if not benchmark_path.is_file():
            continue

        estimates = json.loads(estimate_path.read_text())
        benchmark = json.loads(benchmark_path.read_text())
        median = estimates["median"]["point_estimate"]
        std_dev = estimates["std_dev"]["point_estimate"]
        benches.append(
            {
                "name": benchmark["full_id"],
                "value": median,
                "range": f"± {std_dev}",
                "unit": "ns/iter",
            }
        )
    return benches


def migrate(reports_root: Path, repository_url: str) -> dict[str, Any]:
    entries = []
    for report_dir in sorted(path for path in reports_root.iterdir() if path.is_dir()):
        sha = report_dir.name
        benches = read_benchmarks(report_dir)
        if not benches:
            continue
        commit, date = commit_metadata(sha, repository_url)
        entries.append({"commit": commit, "date": date, "tool": "cargo", "benches": benches})

    entries.sort(key=lambda entry: (entry["date"], entry["commit"]["id"]))
    last_update = max((entry["date"] for entry in entries), default=0)
    return {"lastUpdate": last_update, "entries": {ENTRY_NAME: entries}}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--reports-root",
        required=True,
        type=Path,
        help="Directory containing one Criterion report directory per commit SHA",
    )
    parser.add_argument("--repository-url", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    data = migrate(args.reports_root, args.repository_url)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        "window.BENCHMARK_DATA = " + json.dumps(data, indent=2) + ";\n"
    )
    count = len(data["entries"][ENTRY_NAME])
    print(f"Wrote {count} historical runs to {args.output}")


if __name__ == "__main__":
    main()
