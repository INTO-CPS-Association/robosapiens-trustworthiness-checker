"""Black-box verdict tests for the packaged velocity-safety FMU."""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pytest
from fmpy import simulate_fmu


@pytest.fixture(scope="module")
def fmu_path() -> Path:
    path = Path(os.environ["TC_FMU_PATH"]).resolve()
    assert path.is_file(), f"FMU does not exist: {path}"
    return path


@pytest.mark.parametrize(
    ("velocity", "emergency_stop", "expected_verdict"),
    [
        (3, False, True),
        (8, False, False),
        (8, True, True),
    ],
)
def test_packaged_checker_produces_safety_verdicts(
    fmu_path: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    velocity: float,
    emergency_stop: bool,
    expected_verdict: bool,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("PYTHONPATH", raising=False)
    monkeypatch.delenv("VIRTUAL_ENV", raising=False)
    inputs = np.array(
        [(0.0, velocity, emergency_stop), (1.0, velocity, emergency_stop)],
        dtype=[
            ("time", np.float64),
            ("velocity", np.float64),
            ("emergency_stop", np.bool_),
        ],
    )
    result = simulate_fmu(
        filename=str(fmu_path),
        start_time=0.0,
        stop_time=1.0,
        step_size=1.0,
        input=inputs,
        output=["verdict"],
        validate=True,
    )
    assert bool(result["verdict"][-1]) is expected_verdict


def test_packaged_checker_preserves_order_across_synchronous_steps(
    fmu_path: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("PYTHONPATH", raising=False)
    monkeypatch.delenv("VIRTUAL_ENV", raising=False)
    inputs = np.array(
        [(0.0, 3.0, False), (1.0, 8.0, False), (2.0, 8.0, True), (3.0, 3.0, False)],
        dtype=[
            ("time", np.float64),
            ("velocity", np.float64),
            ("emergency_stop", np.bool_),
        ],
    )
    result = simulate_fmu(
        filename=str(fmu_path),
        start_time=0.0,
        stop_time=3.0,
        step_size=1.0,
        output_interval=1.0,
        input=inputs,
        output=["verdict"],
        validate=True,
    )
    assert [bool(value) for value in result["verdict"]] == [True, True, False, True]
