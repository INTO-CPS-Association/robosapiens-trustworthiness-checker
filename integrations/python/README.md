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

Build and test the binding from the repository root with:

```bash
uv run --project integrations/python --locked --group dev maturin develop
uv run --project integrations/python --locked --group dev pytest integrations/python/tests
```
