"""Small end-to-end performance benchmark for the packaged checker FMU."""

from __future__ import annotations

import argparse
from pathlib import Path
from statistics import median
from time import perf_counter

import numpy as np
from fmpy import simulate_fmu


def run_once(fmu: Path, steps: int, step_size: float) -> float:
    times = np.arange(steps + 1, dtype=np.float64) * step_size
    inputs = np.empty(
        steps + 1,
        dtype=[
            ("time", np.float64),
            ("velocity", np.float64),
            ("emergency_stop", np.bool_),
        ],
    )
    inputs["time"] = times
    inputs["velocity"] = np.where(np.arange(steps + 1) % 2 == 0, 3.0, 8.0)
    inputs["emergency_stop"] = False

    started = perf_counter()
    result = simulate_fmu(
        filename=str(fmu),
        start_time=0.0,
        stop_time=steps * step_size,
        step_size=step_size,
        output_interval=step_size,
        input=inputs,
        output=["verdict"],
        validate=False,
    )
    elapsed = perf_counter() - started

    if len(result) != steps + 1:
        raise RuntimeError(f"expected {steps + 1} samples, received {len(result)}")
    return elapsed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fmu", type=Path, required=True, help="FMU artefact to benchmark")
    parser.add_argument("--steps", type=int, default=1_000)
    parser.add_argument("--step-size", type=float, default=0.001)
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()

    if not args.fmu.is_file():
        parser.error(f"FMU not found: {args.fmu}")
    if args.steps < 1 or args.step_size <= 0 or args.repetitions < 1:
        parser.error("steps, step-size, and repetitions must be positive")

    run_once(args.fmu.resolve(), min(args.steps, 10), args.step_size)
    elapsed = [
        run_once(args.fmu.resolve(), args.steps, args.step_size)
        for _ in range(args.repetitions)
    ]
    typical = median(elapsed)
    simulated_seconds = args.steps * args.step_size

    print(f"FMU: {args.fmu}")
    print(f"Steps: {args.steps:,} x {args.repetitions} repetitions")
    print(f"Median wall time: {typical:.3f} s")
    print(f"Throughput: {args.steps / typical:,.0f} steps/s")
    print(f"Real-time factor: {simulated_seconds / typical:.2f}x")


if __name__ == "__main__":
    main()
