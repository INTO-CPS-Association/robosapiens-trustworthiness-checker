# Python Bindings

The native extension exposes the Rust checker as `trustworthiness_checker`.
Its Rust dependency on the core crate is aliased to `tc_core` internally and is
built without the main binary's optional system integrations and allocator.

Submit one complete observation per synchronous logical tick and retrieve the
corresponding output:

```python
from trustworthiness_checker import TcRuntime

checker = TcRuntime.from_path("spec.dsrv")
checker.provide_inputs({"velocity": 3.0, "emergency_stop": False})
verdict = checker.next_output(timeout=1.0)
```

The model is parsed, then semantically checked, before any runtime is built.
Typed semantics (`"typed-untimed"`, the default) checks strictly; the gradual,
untimed and causal semantics check gradually. The warnings that checking
proves are kept on the runtime as an immutable tuple of `SemanticWarning`
records, in report order:

```python
for warning in checker.warnings:
    print(warning.code, warning.message, warning.span)
```

Each record is read-only. `code` is the stable name of the warning rule,
`message` is human-readable and may change wording, and `span` is the
`(start_byte, end_byte)` range of the model text it applies to, or `None`.
Warnings are never printed or logged by the binding.

A model that fails semantic checking raises `SemanticAnalysisError`, a
subclass of `RuntimeError`. Its `warnings` attribute is the same kind of
tuple, holding the warnings the failed analysis proved:

```python
from trustworthiness_checker import SemanticAnalysisError

try:
    checker = TcRuntime.from_text(model)
except SemanticAnalysisError as error:
    for warning in error.warnings:
        print(warning.code, warning.span)
    raise
```

A model that does not parse, a file that cannot be read, and a failure to
build the runtime still raise `RuntimeError` or `OSError` as before, and an
unknown semantics still raises `ValueError`.

Use `semantics="causal"` or `semantics="causal-set"` for the default
unclassified support, `semantics="role-causal-set"` for one compact
role-annotated explanation, or `semantics="role-causal-antichain"` for
inclusion-minimal role-annotated alternatives. Ordinary semantics returns its
existing flat mapping. Causal semantics returns a structured mapping so
metadata cannot collide with a model output name:

```python
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
                        }
                    ],
                }
            ]
        }
    },
}
```

A value of a tagged union (`use experimental::{tagged_unions}`) is returned as
a `UnionValue`. Its read-only `tag` is the alternative's name, and `payload`
is the payload converted as any other value is, a nested `UnionValue` included,
or `None` for a tag without one. Union values are immutable and only the
checker creates them. They support structural pattern matching:

```python
from trustworthiness_checker import UnionValue

match output["step"]:
    case UnionValue("Stayed", None):
        ...
    case UnionValue("Moved", UnionValue("Active", speed)):
        ...
```

This is the Python representation only; JSON output keeps its
`{"$tag": …, "payload": …}` form.

`DeferredValue` and `NoValue` remain explicit Python objects in
`result["values"]`. Other values use the same native Python representation as
ordinary semantics, including `float("nan")` and positive or negative infinity.
Causal input ticks are assigned by the Rust runtime from the model declaration;
callers do not need to provide a duplicate input-name set. The structured causal
result replaces the former reserved `"__causality__"` key.

Build and test the binding from the repository root with:

```bash
uv sync --project integrations/python --locked --group dev \
    --reinstall-package trustworthiness-checker
uv run --no-sync --project integrations/python \
    pytest integrations/python/tests
```
