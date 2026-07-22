#!/usr/bin/env python3
"""Validate data.js using github-action-benchmark's parsing rules."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

SCRIPT_PREFIX = "window.BENCHMARK_DATA = "


def load(path: Path) -> dict[str, Any]:
    script = path.read_text()
    if not script.startswith(SCRIPT_PREFIX):
        raise ValueError(f"{path} does not start with {SCRIPT_PREFIX!r}")

    data = json.loads(script[len(SCRIPT_PREFIX) :])
    if not isinstance(data.get("entries"), dict):
        raise ValueError(f"{path} does not contain a benchmark entries object")
    return data


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path)
    args = parser.parse_args()

    data = load(args.path)
    run_count = sum(len(runs) for runs in data["entries"].values())
    print(f"Validated {run_count} benchmark runs in {args.path}")


if __name__ == "__main__":
    main()
