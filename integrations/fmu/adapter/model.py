from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from trustworthiness_checker import DeferredValue, NoValue, TcRuntime

_RESOURCES = Path(__file__).resolve().parent


class Fmi2Status:
    ok = 0
    warning = 1
    discard = 2
    error = 3
    fatal = 4
    pending = 5


class Model:
    """Mapping-driven UniFMU adapter for a packaged DSRV checker."""

    def __init__(self, _log_callback=None) -> None:
        self._log_callback = _log_callback
        self.output_timeout = 0.1
        with (_RESOURCES / "interface.json").open(encoding="utf-8") as interface_file:
            self.interface = json.load(interface_file)
        self.variables = {
            variable["value_reference"]: variable
            for variable in self.interface["variables"]
        }
        self.inputs = [
            variable
            for variable in self.variables.values()
            if variable["causality"] == "input"
        ]
        self.outputs = [
            variable
            for variable in self.variables.values()
            if variable["causality"] == "output"
        ]
        self.values = {
            variable["name"]: _coerce(variable["fmi_type"], variable["start"])
            for variable in self.variables.values()
        }
        self._new_runtime()

    def _new_runtime(self) -> None:
        self.runtime = TcRuntime(
            _RESOURCES / "spec.dsrv",
            semantics="typed-untimed",
        )

    def fmi2SetDebugLogging(self, categories, logging_on) -> int:
        del categories, logging_on
        return Fmi2Status.ok

    def fmi2SetupExperiment(self, start_time, stop_time, tolerance) -> int:
        del start_time, stop_time, tolerance
        return Fmi2Status.ok

    def fmi2EnterInitializationMode(self) -> int:
        return Fmi2Status.ok

    def fmi2ExitInitializationMode(self) -> int:
        return self._evaluate()

    def fmi2Terminate(self) -> int:
        return Fmi2Status.ok

    def fmi2Reset(self) -> int:
        self.values = {
            variable["name"]: _coerce(variable["fmi_type"], variable["start"])
            for variable in self.variables.values()
        }
        self._new_runtime()
        return Fmi2Status.ok

    def fmi2SerializeFmuState(self):
        return Fmi2Status.error, b""

    def fmi2DeserializeFmuState(self, state) -> int:
        del state
        return Fmi2Status.error

    def _get(self, references, fmi_type):
        try:
            variables = [self.variables[reference] for reference in references]
            if any(variable["fmi_type"] != fmi_type for variable in variables):
                return Fmi2Status.error, []
            return Fmi2Status.ok, [self.values[variable["name"]] for variable in variables]
        except KeyError:
            return Fmi2Status.error, []

    def _set(self, references, values, fmi_type) -> int:
        if len(references) != len(values):
            return Fmi2Status.error
        try:
            variables = [self.variables[reference] for reference in references]
            if any(
                variable["fmi_type"] != fmi_type or variable["causality"] != "input"
                for variable in variables
            ):
                return Fmi2Status.error
            for variable, value in zip(variables, values):
                self.values[variable["name"]] = _coerce(fmi_type, value)
            return Fmi2Status.ok
        except (KeyError, TypeError, ValueError):
            return Fmi2Status.error

    def fmi2GetReal(self, references):
        return self._get(references, "Real")

    def fmi2SetReal(self, references, values) -> int:
        return self._set(references, values, "Real")

    def fmi2GetInteger(self, references):
        return self._get(references, "Integer")

    def fmi2SetInteger(self, references, values) -> int:
        return self._set(references, values, "Integer")

    def fmi2GetBoolean(self, references):
        return self._get(references, "Boolean")

    def fmi2SetBoolean(self, references, values) -> int:
        return self._set(references, values, "Boolean")

    def fmi2GetString(self, references):
        return self._get(references, "String")

    def fmi2SetString(self, references, values) -> int:
        return self._set(references, values, "String")

    def fmi2DoStep(self, current_time: float, step_size: float, no_step_prior: bool) -> int:
        del current_time, step_size, no_step_prior
        return self._evaluate()

    def _evaluate(self) -> int:
        self.runtime.provide_inputs(
            {variable["name"]: self.values[variable["name"]] for variable in self.inputs}
        )
        output = self.runtime.next_output(timeout=self.output_timeout)
        if output is None:
            return Fmi2Status.error
        for variable in self.outputs:
            if variable["name"] not in output:
                return Fmi2Status.error
            value = output[variable["name"]]
            if not _is_absent_value(value):
                self.values[variable["name"]] = _coerce(variable["fmi_type"], value)
        return Fmi2Status.ok


def _coerce(fmi_type: str, value: Any) -> Any:
    return {
        "Real": float,
        "Integer": int,
        "Boolean": bool,
        "String": str,
    }[fmi_type](value)


def _is_absent_value(value: Any) -> bool:
    return value is None or isinstance(value, (DeferredValue, NoValue))
