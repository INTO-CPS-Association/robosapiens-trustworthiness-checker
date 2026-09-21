"""Contract tests for monitoring system-output traces with the Python API."""

from __future__ import annotations

import math
from collections.abc import Iterable
from pathlib import Path

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


CAUSAL_MONITOR = """
in velocity: Int

out verdict: Bool

verdict = velocity <= 5
"""


CAUSAL_RESERVED_OUTPUT_NAMES_MONITOR = """
in x: Int

out values: Int
out causality: Int

values = x
causality = x + 1
"""


CAUSAL_FLOAT_MONITOR = """
in measurement: Float

out result: Float

result = measurement
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


def test_parse_failure_is_not_a_semantic_analysis_error() -> None:
    with pytest.raises(RuntimeError) as raised:
        tc.TcRuntime.from_text("this is not a monitor specification")
    assert not isinstance(raised.value, tc.SemanticAnalysisError)


ILL_TYPED_MONITOR = """
in velocity: Int

out verdict: Bool

verdict = velocity
"""

REDUNDANT_CAST_MONITOR = """use experimental::{casts}
in velocity: Int
out verdict: Int
verdict = velocity as Int
"""

REDUNDANT_CAST_WITH_ERROR_MONITOR = """use experimental::{casts}
out y: Int
out z: Bool
y = 1 as Int
z = 1
"""


def assert_redundant_cast_warning(warning: object, model: str) -> None:
    assert warning.code == "dsrv.redundant-cast"
    assert warning.message == "cast from Int to Int is redundant"
    assert warning.span is not None
    start, end = warning.span
    assert model[start:end] == "velocity as Int"
    with pytest.raises(AttributeError):
        warning.message = "changed"


@pytest.mark.parametrize(
    "semantics", ["typed-untimed", "gradual-typed-untimed", "untimed", "causal"]
)
def test_semantic_check_failure_raises_with_its_warnings(semantics: str) -> None:
    with pytest.raises(tc.SemanticAnalysisError, match="failed semantic checking") as raised:
        tc.TcRuntime.from_text(ILL_TYPED_MONITOR, semantics)
    assert isinstance(raised.value, RuntimeError)
    assert raised.value.warnings == ()
    assert isinstance(raised.value.warnings, tuple)


def test_semantic_check_failure_exposes_its_real_warnings() -> None:
    with pytest.raises(tc.SemanticAnalysisError) as raised:
        tc.TcRuntime.from_text(REDUNDANT_CAST_WITH_ERROR_MONITOR)
    assert isinstance(raised.value.warnings, tuple)
    assert len(raised.value.warnings) == 1
    warning = raised.value.warnings[0]
    assert warning.code == "dsrv.redundant-cast"
    assert warning.message == "cast from Int to Int is redundant"
    start, end = warning.span
    assert REDUNDANT_CAST_WITH_ERROR_MONITOR[start:end] == "1 as Int"


def test_a_checked_model_exposes_its_warnings_as_an_immutable_tuple(tmp_path: Path) -> None:
    path = tmp_path / "monitor.dsrv"
    path.write_text(INSTANTANEOUS_SAFETY_MONITOR)
    for runtime in [
        tc.TcRuntime(INSTANTANEOUS_SAFETY_MONITOR),
        tc.TcRuntime.from_text(INSTANTANEOUS_SAFETY_MONITOR),
        tc.TcRuntime.from_path(path),
        tc.TcRuntime.from_text(INSTANTANEOUS_SAFETY_MONITOR, "causal"),
    ]:
        assert isinstance(runtime, tc.TcRuntime)
        assert runtime.warnings == ()
        assert isinstance(runtime.warnings, tuple)
        with pytest.raises(AttributeError):
            runtime.warnings = ()


def test_successful_construction_exposes_real_warnings_for_every_constructor(
    tmp_path: Path,
) -> None:
    path = tmp_path / "monitor.dsrv"
    path.write_text(REDUNDANT_CAST_MONITOR)
    for runtime in [
        tc.TcRuntime(REDUNDANT_CAST_MONITOR),
        tc.TcRuntime.from_text(REDUNDANT_CAST_MONITOR),
        tc.TcRuntime.from_path(path),
    ]:
        assert isinstance(runtime.warnings, tuple)
        assert len(runtime.warnings) == 1
        assert_redundant_cast_warning(runtime.warnings[0], REDUNDANT_CAST_MONITOR)
        with pytest.raises(AttributeError):
            runtime.warnings = ()


CAST_WARNING_MONITOR = """
use experimental::{casts}
in x: Int
out y: Int = x as Int
"""


def test_redundant_cast_warning_is_converted_on_success() -> None:
    runtime = tc.TcRuntime.from_text(CAST_WARNING_MONITOR)
    assert len(runtime.warnings) == 1
    warning = runtime.warnings[0]
    assert warning.code == "dsrv.redundant-cast"
    assert warning.message == "cast from Int to Int is redundant"
    assert CAST_WARNING_MONITOR[slice(*warning.span)] == "x as Int"


def test_redundant_cast_warning_is_converted_on_semantic_failure() -> None:
    model = CAST_WARNING_MONITOR + "\nout broken: Bool = 1\n"
    with pytest.raises(tc.SemanticAnalysisError) as raised:
        tc.TcRuntime.from_text(model)
    assert len(raised.value.warnings) == 1
    warning = raised.value.warnings[0]
    assert warning.code == "dsrv.redundant-cast"
    assert warning.message == "cast from Int to Int is redundant"
    assert model[slice(*warning.span)] == "x as Int"


def test_semantic_warning_records_cannot_be_constructed_from_python() -> None:
    with pytest.raises(TypeError):
        tc.SemanticWarning()


@pytest.mark.parametrize(
    ("selector", "roles"),
    [
        ("causal", []),
        ("causal-set", []),
        ("role-causal-set", ["direct"]),
        ("role-causal-antichain", ["direct"]),
    ],
)
def test_canonical_causal_selectors_use_structured_role_aware_output(
    selector: str, roles: list[str]
) -> None:
    runtime = tc.TcRuntime.from_text(CAUSAL_MONITOR, semantics=selector)
    runtime.provide_inputs({"velocity": 8})

    output = runtime.next_output(timeout=1.0)

    assert output is not None
    assert output["values"] == {"verdict": False}
    assert output["causality"]["verdict"] == {
        "alternatives": [
            {
                "causes": [
                    {"input": "velocity", "logical_tick": 0, "roles": roles}
                ],
            }
        ]
    }


def test_causal_result_does_not_reserve_model_output_names() -> None:
    runtime = tc.TcRuntime.from_text(
        CAUSAL_RESERVED_OUTPUT_NAMES_MONITOR,
        semantics="role-causal-antichain",
    )
    runtime.provide_inputs({"x": 4})

    output = runtime.next_output(timeout=1.0)

    assert output is not None
    assert output["values"] == {"values": 4, "causality": 5}
    assert set(output["causality"]) == {"values", "causality"}


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_causal_outputs_preserve_non_finite_python_floats(value: float) -> None:
    runtime = tc.TcRuntime.from_text(CAUSAL_FLOAT_MONITOR, semantics="causal")
    runtime.provide_inputs({"measurement": value})

    output = runtime.next_output(timeout=1.0)

    assert output is not None
    result = output["values"]["result"]
    assert isinstance(result, float)
    if math.isnan(value):
        assert math.isnan(result)
    else:
        assert result == value


@pytest.mark.parametrize(
    "selector",
    [" causal-set ", "CAUSAL-SET", "role_causal_set"],
)
def test_causal_selectors_require_canonical_spelling(selector: str) -> None:
    with pytest.raises(ValueError, match="unsupported semantics"):
        tc.TcRuntime.from_text(CAUSAL_MONITOR, semantics=selector)


UNION_MONITOR = """
use experimental::{tagged_unions}

type Cycle = Union<Idle, Active: Int>
type Step = Union<Stayed, Moved: Cycle, Held: List<Int>>

in x: Int

out moved: Step
out stayed: Step
out held: Step

moved = Moved(Active(x))
stayed = Stayed
held = Held([x, x + 1])
"""


def union_outputs() -> dict[str, object]:
    runtime = tc.TcRuntime.from_text(UNION_MONITOR)
    runtime.provide_inputs({"x": 3})
    output = runtime.next_output(timeout=1.0)
    assert output is not None
    return output


def test_union_values_convert_to_tagged_python_records() -> None:
    output = union_outputs()

    stayed = output["stayed"]
    assert isinstance(stayed, tc.UnionValue)
    assert stayed.tag == "Stayed"
    assert stayed.payload is None

    moved = output["moved"]
    assert moved.tag == "Moved"
    assert isinstance(moved.payload, tc.UnionValue)
    assert moved.payload.tag == "Active"
    assert moved.payload.payload == 3

    held = output["held"]
    assert held.tag == "Held"
    assert held.payload == [3, 4]


def test_union_values_are_immutable_and_not_constructible() -> None:
    value = union_outputs()["moved"]
    with pytest.raises(AttributeError):
        value.tag = "Stayed"
    with pytest.raises(AttributeError):
        value.payload = None
    with pytest.raises(TypeError):
        tc.UnionValue("Stayed", None)


def test_union_value_repr_shows_tag_and_payload() -> None:
    output = union_outputs()
    assert repr(output["stayed"]) == "UnionValue(tag='Stayed', payload=None)"
    assert (
        repr(output["moved"])
        == "UnionValue(tag='Moved', payload=UnionValue(tag='Active', payload=3))"
    )
    assert repr(output["held"]) == "UnionValue(tag='Held', payload=[3, 4])"


def test_union_values_support_structural_pattern_matching() -> None:
    assert tc.UnionValue.__match_args__ == ("tag", "payload")

    def describe(value: object) -> str:
        match value:
            case tc.UnionValue("Stayed", None):
                return "stayed"
            case tc.UnionValue("Moved", tc.UnionValue("Active", speed)):
                return f"moving at {speed}"
            case tc.UnionValue(tag=tag, payload=payload):
                return f"{tag}: {payload}"
        return "not a union"

    output = union_outputs()
    assert describe(output["stayed"]) == "stayed"
    assert describe(output["moved"]) == "moving at 3"
    assert describe(output["held"]) == "Held: [3, 4]"
    assert describe(3) == "not a union"
