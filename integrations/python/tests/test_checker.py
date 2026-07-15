"""Contract tests for monitoring system-output traces with the Python API."""

from __future__ import annotations

from collections.abc import Iterable

import pytest


tc = pytest.importorskip(
    "trustworthiness_checker",
    reason="build/install the PyO3 extension with maturin before running binding tests",
)


INSTANTANEOUS_SAFETY_MONITOR = """
in velocity: Int
in emergency_stop: Bool

out within_velocity_limit: Bool
out verdict: Bool

within_velocity_limit = velocity <= 5
verdict = within_velocity_limit || emergency_stop
"""


HISTORY_SAFETY_MONITOR = """
in position_error: Int

out within_tolerance: Bool
out verdict: Bool

within_tolerance = position_error <= 2
verdict = within_tolerance && default(within_tolerance[1], true)
"""


SYNCHRONOUS_ACCUMULATOR_MONITOR = """
in increment: Int

out total: Int
out ticks: Int

total = default(total[1], 0) + increment
ticks = default(ticks[1], 0) + 1
"""


ATOMIC_BATCH_MONITOR = """
in left: Int
in right: Int

out difference: Int
out ticks: Int

difference = left - right
ticks = default(ticks[1], 0) + 1
"""


def monitor_trace(model: str, trace: Iterable[dict[str, object]]) -> list[dict[str, object]]:
    """Submit one observed system-output sample per checker tick."""
    runtime = tc.TcRuntime.from_text(model)
    verdicts = []
    for system_outputs in trace:
        runtime.provide_inputs(system_outputs)
        output = runtime.next_output(timeout=1.0)
        assert output is not None, "checker did not produce a verdict for an input tick"
        verdicts.append(output)
    return verdicts


def test_checker_verdicts_follow_observed_system_outputs() -> None:
    outputs = monitor_trace(
        INSTANTANEOUS_SAFETY_MONITOR,
        [
            {"velocity": 3, "emergency_stop": False},
            {"velocity": 8, "emergency_stop": False},
            {"velocity": 8, "emergency_stop": True},
        ],
    )

    assert [output["within_velocity_limit"] for output in outputs] == [True, False, False]
    assert [output["verdict"] for output in outputs] == [True, False, True]


def test_checker_verdict_can_depend_on_previous_system_behaviour() -> None:
    outputs = monitor_trace(
        HISTORY_SAFETY_MONITOR,
        [
            {"position_error": 1},
            {"position_error": 4},
            {"position_error": 1},
            {"position_error": 1},
        ],
    )

    assert [output["verdict"] for output in outputs] == [True, False, False, True]


def test_synchronous_submissions_commit_state_before_the_next_tick() -> None:
    runtime = tc.TcRuntime.from_text(SYNCHRONOUS_ACCUMULATOR_MONITOR)

    # Do not consume outputs between submissions. Each provide_inputs call must
    # still finish its dataflow tick before the next one is accepted.
    runtime.provide_inputs({"increment": 2})
    runtime.provide_inputs({"increment": 3})
    runtime.provide_inputs({"increment": 5})

    outputs = [runtime.next_output(timeout=1.0) for _ in range(3)]
    assert all(output is not None for output in outputs)
    assert [output["total"] for output in outputs] == [2, 5, 10]
    assert [output["ticks"] for output in outputs] == [1, 2, 3]


def test_multi_variable_submission_is_one_atomic_logical_tick() -> None:
    runtime = tc.TcRuntime.from_text(ATOMIC_BATCH_MONITOR)

    runtime.provide_inputs({"left": 9, "right": 4})
    first = runtime.next_output(timeout=1.0)
    runtime.provide_inputs({"left": 2, "right": 7})
    second = runtime.next_output(timeout=1.0)

    assert first == {"difference": 5, "ticks": 1}
    assert second == {"difference": -5, "ticks": 2}
    assert runtime.next_output(timeout=0.0) is None


def test_each_synchronous_submission_produces_one_ordered_verdict() -> None:
    runtime = tc.TcRuntime.from_text(INSTANTANEOUS_SAFETY_MONITOR)
    trace = [
        {"velocity": 2, "emergency_stop": False},
        {"velocity": 9, "emergency_stop": False},
        {"velocity": 9, "emergency_stop": True},
    ]

    for observed_outputs in trace:
        runtime.provide_inputs(observed_outputs)

    verdicts = [runtime.next_output(timeout=1.0) for _ in trace]
    assert [output["verdict"] for output in verdicts] == [True, False, True]
    assert runtime.next_output(timeout=0.0) is None


def test_checker_rejects_outputs_not_declared_by_the_monitor() -> None:
    runtime = tc.TcRuntime.from_text(INSTANTANEOUS_SAFETY_MONITOR)

    with pytest.raises(ValueError, match="unknown model inputs"):
        runtime.provide_inputs(
            {"velocity": 3, "emergency_stop": False, "undeclared_sensor": 12}
        )


def test_checker_returns_none_when_no_system_output_tick_is_available() -> None:
    runtime = tc.TcRuntime.from_text(INSTANTANEOUS_SAFETY_MONITOR)

    assert runtime.next_output(timeout=0.0) is None


@pytest.mark.parametrize("timeout", [float("nan"), float("inf"), -float("inf")])
def test_checker_rejects_non_finite_output_timeouts(timeout: float) -> None:
    runtime = tc.TcRuntime.from_text(INSTANTANEOUS_SAFETY_MONITOR)
    with pytest.raises(ValueError, match="finite and non-negative"):
        runtime.next_output(timeout=timeout)


def test_checker_rejects_invalid_monitor_specification() -> None:
    with pytest.raises(RuntimeError, match="failed to initialise"):
        tc.TcRuntime.from_text("this is not a monitor specification")
