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

`DeferredValue` and `NoValue` remain explicit Python objects in
`result["values"]`. Other values use the same native Python representation as
ordinary semantics, including `float("nan")` and positive or negative infinity.
Causal input ticks are assigned by the Rust runtime from the model declaration;
callers do not need to provide a duplicate input-name set. The structured causal
result replaces the former reserved `"__causality__"` key.

Build and test the binding from the repository root with:

```bash
uv run --project integrations/python --locked --group dev maturin develop
uv run --project integrations/python --locked --group dev pytest integrations/python/tests
```
