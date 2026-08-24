"""Isolated contract tests for the mapping-driven UniFMU adapter."""

from __future__ import annotations

import importlib.util
import json
import sys
from collections import deque
from collections.abc import Mapping
from os import PathLike
from pathlib import Path
from types import ModuleType
from typing import ClassVar
from uuid import uuid4

import pytest


class DeferredValue:
    pass


class NoValue:
    pass


class FakeRuntime:
    instances: ClassVar[list[FakeRuntime]] = []
    initial_outputs: ClassVar[list[dict[str, object] | None]] = []

    def __init__(self, model: str | PathLike[str], *, semantics: str) -> None:
        self.model = Path(model)
        self.semantics = semantics
        self.provided_inputs: list[dict[str, object]] = []
        self.requested_timeouts: list[float | None] = []
        self.outputs: deque[dict[str, object] | None] = deque(
            type(self).initial_outputs
        )
        FakeRuntime.instances.append(self)

    def provide_inputs(
        self, inputs: Mapping[str, object] | None = None, **kwargs: object
    ) -> None:
        values = dict(inputs or {})
        values.update(kwargs)
        self.provided_inputs.append(values)

    def next_output(self, timeout: float | None = None) -> dict[str, object] | None:
        self.requested_timeouts.append(timeout)
        return self.outputs.popleft() if self.outputs else None


class FakeExtension(ModuleType):
    DeferredValue: type[DeferredValue]
    NoValue: type[NoValue]
    TcRuntime: type[FakeRuntime]


def causal_output(values: dict[str, object]) -> dict[str, object]:
    return {
        "values": values,
        "causality": {name: {"alternatives": []} for name in values},
    }


@pytest.mark.parametrize(
    "semantics",
    ["causal", "causal-set", "role-causal-set", "role-causal-antichain"],
)
def test_adapter_accepts_canonical_causal_selectors(
    adapter, monkeypatch, semantics
) -> None:
    monkeypatch.setenv("TC_CAUSAL_SEMANTICS", semantics)
    model = adapter.Model()
    assert model.causal_semantics == semantics
    assert FakeRuntime.instances[-1].semantics == semantics


@pytest.fixture
def adapter(monkeypatch, tmp_path):
    FakeRuntime.instances.clear()
    FakeRuntime.initial_outputs = []
    fake_extension = FakeExtension("trustworthiness_checker")
    fake_extension.DeferredValue = DeferredValue
    fake_extension.NoValue = NoValue
    fake_extension.TcRuntime = FakeRuntime
    monkeypatch.setitem(sys.modules, "trustworthiness_checker", fake_extension)

    adapter_dir = Path(__file__).resolve().parents[1] / "adapter"
    module_name = f"tc_unifmu_model_{uuid4().hex}"
    spec = importlib.util.spec_from_file_location(module_name, adapter_dir / "model.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, module_name, module)
    spec.loader.exec_module(module)

    interface = {
        "model_name": "velocity_safety_checker",
        "variables": [
            {
                "name": "velocity",
                "value_reference": 0,
                "fmi_type": "Real",
                "causality": "input",
                "start": 0.0,
            },
            {
                "name": "emergency_stop",
                "value_reference": 1,
                "fmi_type": "Boolean",
                "causality": "input",
                "start": False,
            },
            {
                "name": "verdict",
                "value_reference": 2,
                "fmi_type": "Boolean",
                "causality": "output",
                "start": False,
            },
        ],
    }
    (tmp_path / "interface.json").write_text(json.dumps(interface), encoding="utf-8")
    (tmp_path / "spec.dsrv").write_text("test specification", encoding="utf-8")
    monkeypatch.setattr(module, "_RESOURCES", tmp_path)
    return module


def test_adapter_loads_generated_interface(adapter) -> None:
    model = adapter.Model()
    assert model.fmi2GetReal([0]) == (adapter.Fmi2Status.ok, [0.0])
    assert model.fmi2GetBoolean([1, 2]) == (adapter.Fmi2Status.ok, [False, False])
    assert FakeRuntime.instances[-1].model.name == "spec.dsrv"
    assert FakeRuntime.instances[-1].semantics == "causal-set"


def test_adapter_forwards_observations_and_publishes_boolean_verdict(adapter) -> None:
    FakeRuntime.initial_outputs = [causal_output({"verdict": True})]
    model = adapter.Model()
    assert model.fmi2SetReal([0], [8.0]) == adapter.Fmi2Status.ok
    assert model.fmi2SetBoolean([1], [True]) == adapter.Fmi2Status.ok
    assert model.fmi2DoStep(0.0, 1.0, False) == adapter.Fmi2Status.ok
    assert FakeRuntime.instances[-1].provided_inputs == [
        {"velocity": 8.0, "emergency_stop": True}
    ]
    assert model.fmi2GetBoolean([2]) == (adapter.Fmi2Status.ok, [True])


def test_adapter_retains_flat_outputs_for_ordinary_semantics(
    adapter, monkeypatch
) -> None:
    monkeypatch.setenv("TC_CAUSAL_SEMANTICS", "typed-untimed")
    FakeRuntime.initial_outputs = [{"verdict": True}]
    model = adapter.Model()
    assert model.causal_semantics == "typed-untimed"
    assert model.fmi2DoStep(0.0, 1.0, False) == adapter.Fmi2Status.ok
    assert model.fmi2GetBoolean([2]) == (adapter.Fmi2Status.ok, [True])


def test_adapter_writes_fmi_timed_causal_side_channel(
    adapter, monkeypatch, tmp_path
) -> None:
    causal_log = tmp_path / "results" / "causal-semantics.jsonl"
    monkeypatch.setenv("TC_CAUSAL_LOG", str(causal_log))
    FakeRuntime.initial_outputs = [
        {
            "values": {"verdict": False},
            "causality": {
                "verdict": {
                    "alternatives": [
                        {
                            "causes": [
                                {
                                    "input": "velocity",
                                    "logical_tick": 0,
                                    "roles": [],
                                },
                                {
                                    "input": "emergency_stop",
                                    "logical_tick": 0,
                                    "roles": ["activation"],
                                },
                            ],
                        }
                    ]
                }
            },
        }
    ]
    model = adapter.Model()
    assert model.fmi2DoStep(1.2, 0.1, False) == adapter.Fmi2Status.ok
    record = json.loads(causal_log.read_text(encoding="utf-8"))
    assert record["logical_tick"] == 0
    assert record["semantics"] == "causal-set"
    assert record["communication_point"] == pytest.approx(1.2)
    assert record["fmi_time"] == pytest.approx(1.3)
    assert record["outputs"] == {"verdict": False}
    assert record["causality"]["verdict"]["alternatives"][0]["causes"] == [
        {"input": "velocity", "logical_tick": 0, "roles": []},
        {"input": "emergency_stop", "logical_tick": 0, "roles": ["activation"]},
    ]


@pytest.mark.parametrize("absent", [DeferredValue(), NoValue()])
def test_adapter_retains_previous_verdict_when_output_is_absent(
    adapter, absent
) -> None:
    FakeRuntime.initial_outputs = [causal_output({"verdict": absent})]
    model = adapter.Model()
    model.values["verdict"] = True
    assert model.fmi2DoStep(0.0, 1.0, False) == adapter.Fmi2Status.ok
    assert model.fmi2GetBoolean([2])[1] == [True]


def test_adapter_reports_missing_synchronous_output_as_error(adapter) -> None:
    model = adapter.Model()
    assert model.fmi2DoStep(0.0, 1.0, False) == adapter.Fmi2Status.error


def test_initialisation_evaluates_configured_inputs(adapter) -> None:
    FakeRuntime.initial_outputs = [causal_output({"verdict": True})]
    model = adapter.Model()
    model.fmi2SetReal([0], [3.0])
    assert model.fmi2ExitInitializationMode() == adapter.Fmi2Status.ok
    assert model.fmi2GetBoolean([2]) == (adapter.Fmi2Status.ok, [True])


def test_adapter_rejects_wrong_types_outputs_and_unknown_references(adapter) -> None:
    model = adapter.Model()
    assert model.fmi2SetInteger([0], [8]) == adapter.Fmi2Status.error
    assert model.fmi2SetBoolean([2], [True]) == adapter.Fmi2Status.error
    assert model.fmi2GetInteger([99]) == (adapter.Fmi2Status.error, [])


def test_reset_recreates_checker_and_restores_start_values(adapter) -> None:
    FakeRuntime.initial_outputs = [causal_output({"verdict": True})]
    model = adapter.Model()
    model.fmi2SetReal([0], [9.0])
    model.fmi2DoStep(0.0, 1.0, False)
    previous_runtime = model.runtime
    assert model.fmi2Reset() == adapter.Fmi2Status.ok
    assert model.runtime is not previous_runtime
    assert model.fmi2GetReal([0])[1] == [0.0]
    assert model.fmi2GetBoolean([2])[1] == [False]
