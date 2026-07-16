"""Isolated contract tests for the mapping-driven UniFMU adapter."""

from __future__ import annotations

import importlib.util
import json
import sys
from collections import deque
from pathlib import Path
from types import ModuleType
from uuid import uuid4

import pytest


class DeferredValue:
    pass


class NoValue:
    pass


class FakeRuntime:
    instances: list["FakeRuntime"] = []
    initial_outputs: list[dict[str, object] | None] = []

    def __init__(self, model, *, semantics) -> None:
        self.model = Path(model)
        self.semantics = semantics
        self.provided_inputs: list[dict[str, object]] = []
        self.requested_timeouts: list[float | None] = []
        self.outputs = deque(type(self).initial_outputs)
        type(self).instances.append(self)

    def provide_inputs(self, inputs=None, **kwargs) -> None:
        values = dict(inputs or {})
        values.update(kwargs)
        self.provided_inputs.append(values)

    def next_output(self, timeout=None):
        self.requested_timeouts.append(timeout)
        return self.outputs.popleft() if self.outputs else None


@pytest.fixture
def adapter(monkeypatch, tmp_path):
    FakeRuntime.instances.clear()
    FakeRuntime.initial_outputs = []
    fake_extension = ModuleType("trustworthiness_checker")
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
            {"name": "velocity", "value_reference": 0, "fmi_type": "Real", "causality": "input", "start": 0.0},
            {"name": "emergency_stop", "value_reference": 1, "fmi_type": "Boolean", "causality": "input", "start": False},
            {"name": "verdict", "value_reference": 2, "fmi_type": "Boolean", "causality": "output", "start": False},
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


def test_adapter_forwards_observations_and_publishes_boolean_verdict(adapter) -> None:
    FakeRuntime.initial_outputs = [{"verdict": True}]
    model = adapter.Model()
    assert model.fmi2SetReal([0], [8.0]) == adapter.Fmi2Status.ok
    assert model.fmi2SetBoolean([1], [True]) == adapter.Fmi2Status.ok
    assert model.fmi2DoStep(0.0, 1.0, False) == adapter.Fmi2Status.ok
    assert FakeRuntime.instances[-1].provided_inputs == [
        {"velocity": 8.0, "emergency_stop": True}
    ]
    assert model.fmi2GetBoolean([2]) == (adapter.Fmi2Status.ok, [True])


@pytest.mark.parametrize("absent", [DeferredValue(), NoValue()])
def test_adapter_retains_previous_verdict_when_output_is_absent(adapter, absent) -> None:
    FakeRuntime.initial_outputs = [{"verdict": absent}]
    model = adapter.Model()
    model.values["verdict"] = True
    assert model.fmi2DoStep(0.0, 1.0, False) == adapter.Fmi2Status.ok
    assert model.fmi2GetBoolean([2])[1] == [True]


def test_adapter_reports_missing_synchronous_output_as_error(adapter) -> None:
    model = adapter.Model()
    assert model.fmi2DoStep(0.0, 1.0, False) == adapter.Fmi2Status.error


def test_initialisation_evaluates_configured_inputs(adapter) -> None:
    FakeRuntime.initial_outputs = [{"verdict": True}]
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
    FakeRuntime.initial_outputs = [{"verdict": True}]
    model = adapter.Model()
    model.fmi2SetReal([0], [9.0])
    model.fmi2DoStep(0.0, 1.0, False)
    previous_runtime = model.runtime
    assert model.fmi2Reset() == adapter.Fmi2Status.ok
    assert model.runtime is not previous_runtime
    assert model.fmi2GetReal([0])[1] == [0.0]
    assert model.fmi2GetBoolean([2])[1] == [False]
