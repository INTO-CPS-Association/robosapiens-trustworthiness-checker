from __future__ import annotations

import json
import os
from collections.abc import Sequence
from pathlib import Path
from typing import Any, TypedDict, cast

from trustworthiness_checker import DeferredValue, NoValue, TcRuntime

_RESOURCES = Path(__file__).resolve().parent


class Variable(TypedDict):
    name: str
    value_reference: int
    fmi_type: str
    causality: str
    start: Any


class Interface(TypedDict):
    variables: list[Variable]


class Fmi2Status:
    ok = 0
    warning = 1
    discard = 2
    error = 3
    fatal = 4
    pending = 5


class Model:
    """Mapping-driven UniFMU adapter for a packaged DSRV checker."""

    def __init__(self, _log_callback: object | None = None) -> None:
        self._log_callback = _log_callback
        self.output_timeout = 0.1
        with (_RESOURCES / "interface.json").open(encoding="utf-8") as interface_file:
            self.interface = cast(Interface, json.load(interface_file))
        self.variables: dict[int, Variable] = {
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
        causal_log = os.environ.get("TC_CAUSAL_LOG")
        self.causal_log = Path(causal_log) if causal_log else None
        self.causal_semantics = os.environ.get("TC_CAUSAL_SEMANTICS", "causal-set")
        self.causal_failure_limit = int(os.environ.get("TC_CAUSAL_FAILURE_LIMIT", "10"))
        self._causal_failure_counts: dict[str, int] = {}
        self.start_time = 0.0
        self.logical_tick = 0
        self._new_runtime()

    def _new_runtime(self) -> None:
        self.runtime = TcRuntime(
            _RESOURCES / "spec.dsrv",
            semantics=self.causal_semantics,
        )

    def fmi2SetDebugLogging(self, categories, logging_on) -> int:
        del categories, logging_on
        return Fmi2Status.ok

    def fmi2SetupExperiment(self, start_time, stop_time, tolerance) -> int:
        del stop_time, tolerance
        self.start_time = float(start_time)
        self.logical_tick = 0
        self._causal_failure_counts.clear()
        return Fmi2Status.ok

    def fmi2EnterInitializationMode(self) -> int:
        return Fmi2Status.ok

    def fmi2ExitInitializationMode(self) -> int:
        return self._evaluate(
            fmi_time=self.start_time,
            communication_point=self.start_time,
            phase="initialization",
        )

    def fmi2Terminate(self) -> int:
        return Fmi2Status.ok

    def fmi2Reset(self) -> int:
        self.values = {
            variable["name"]: _coerce(variable["fmi_type"], variable["start"])
            for variable in self.variables.values()
        }
        self.logical_tick = 0
        self._causal_failure_counts.clear()
        self._new_runtime()
        return Fmi2Status.ok

    def fmi2SerializeFmuState(self):
        return Fmi2Status.error, b""

    def fmi2DeserializeFmuState(self, state) -> int:
        del state
        return Fmi2Status.error

    def _get(self, references: Sequence[int], fmi_type: str) -> tuple[int, list[Any]]:
        try:
            variables = [self.variables[reference] for reference in references]
            if any(variable["fmi_type"] != fmi_type for variable in variables):
                return Fmi2Status.error, []
            return Fmi2Status.ok, [
                self.values[variable["name"]] for variable in variables
            ]
        except KeyError:
            return Fmi2Status.error, []

    def _set(
        self, references: Sequence[int], values: Sequence[Any], fmi_type: str
    ) -> int:
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

    def fmi2GetReal(self, references: Sequence[int]) -> tuple[int, list[Any]]:
        return self._get(references, "Real")

    def fmi2SetReal(self, references: Sequence[int], values: Sequence[Any]) -> int:
        return self._set(references, values, "Real")

    def fmi2GetInteger(self, references: Sequence[int]) -> tuple[int, list[Any]]:
        return self._get(references, "Integer")

    def fmi2SetInteger(self, references: Sequence[int], values: Sequence[Any]) -> int:
        return self._set(references, values, "Integer")

    def fmi2GetBoolean(self, references: Sequence[int]) -> tuple[int, list[Any]]:
        return self._get(references, "Boolean")

    def fmi2SetBoolean(self, references: Sequence[int], values: Sequence[Any]) -> int:
        return self._set(references, values, "Boolean")

    def fmi2GetString(self, references: Sequence[int]) -> tuple[int, list[Any]]:
        return self._get(references, "String")

    def fmi2SetString(self, references: Sequence[int], values: Sequence[Any]) -> int:
        return self._set(references, values, "String")

    def fmi2DoStep(
        self, current_time: float, step_size: float, no_step_prior: bool
    ) -> int:
        del no_step_prior
        return self._evaluate(
            fmi_time=float(current_time) + float(step_size),
            communication_point=float(current_time),
            phase="do-step",
        )

    def _evaluate(
        self,
        *,
        fmi_time: float,
        communication_point: float,
        phase: str,
    ) -> int:
        self.runtime.provide_inputs(
            {
                variable["name"]: self.values[variable["name"]]
                for variable in self.inputs
            }
        )
        output = self.runtime.next_output(timeout=self.output_timeout)
        if output is None:
            return Fmi2Status.error
        if _is_causal_semantics(self.causal_semantics):
            if not isinstance(output, dict):
                return Fmi2Status.error
            if "values" not in output or "causality" not in output:
                return Fmi2Status.error
            values = output["values"]
            causality = output["causality"]
        else:
            # Ordinary semantics remains flat for compatibility. Causal
            # runtimes use the structured result and never reserve a model
            # variable name for metadata.
            values = output
            causality = {}
        for variable in self.outputs:
            if variable["name"] not in values:
                return Fmi2Status.error
            value = values[variable["name"]]
            if not _is_absent_value(value):
                self.values[variable["name"]] = _coerce(variable["fmi_type"], value)
        self._write_causal_record(
            fmi_time=fmi_time,
            communication_point=communication_point,
            phase=phase,
            outputs=values,
            causality=causality,
        )
        self.logical_tick += 1
        return Fmi2Status.ok

    def _write_causal_record(
        self,
        *,
        fmi_time: float,
        communication_point: float,
        phase: str,
        outputs: dict[str, Any],
        causality: dict[str, Any],
    ) -> None:
        if self.causal_log is None:
            return
        selected = {}
        for name, explanations in causality.items():
            if outputs.get(name) is not False:
                continue
            count = self._causal_failure_counts.get(name, 0)
            if count >= self.causal_failure_limit:
                continue
            selected[name] = explanations
            self._causal_failure_counts[name] = count + 1
        if not selected:
            return
        self.causal_log.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "semantics": self.causal_semantics,
            "logical_tick": self.logical_tick,
            "fmi_time": fmi_time,
            "communication_point": communication_point,
            "phase": phase,
            "outputs": {name: outputs[name] for name in selected},
            "causality": selected,
        }
        with self.causal_log.open("a", encoding="utf-8") as log:
            json.dump(record, log, separators=(",", ":"))
            log.write("\n")


def _is_causal_semantics(semantics: str) -> bool:
    return semantics in {
        "causal",
        "causal-set",
        "role-causal-set",
        "role-causal-antichain",
    }


def _coerce(fmi_type: str, value: Any) -> Any:
    return {
        "Real": float,
        "Integer": int,
        "Boolean": bool,
        "String": str,
    }[fmi_type](value)


def _is_absent_value(value: Any) -> bool:
    return value is None or isinstance(value, (DeferredValue, NoValue))
