# Integrations and deployment

The TC is a foreground process with a model path and CLI options. It can also be embedded through the Python binding, packaged as an FMI 2.0 Co-Simulation FMU, or built into the repository's Debian-based deploy image. Choose the integration from the observation and lifecycle contract you need.

## Integration choices

| Surface | Use it when | Main contract |
|---|---|---|
| CLI | A supervisor or shell owns the process | One model plus one input selection; results and diagnostics use the selected channels. |
| Python | A Python process supplies synchronous observations | `TcRuntime.provide_inputs()` supplies one logical tick; `next_output(timeout=...)` returns one output mapping or `None`. |
| FMU | An FMI 2.0 Co-Simulation master owns stepping | The packaged adapter exposes typed DSRV `Int`, `Float`, `Bool`, and `Str` variables through FMI. |
| Docker deploy image | A container runtime owns the foreground CLI | The entrypoint is `/usr/local/bin/trustworthiness_checker`; model and input/config files must be mounted. |

The standalone [Python README](https://github.com/INTO-CPS-Association/robosapiens-trustworthiness-checker/blob/main/integrations/python/README.md), [FMU README](https://github.com/INTO-CPS-Association/robosapiens-trustworthiness-checker/blob/main/integrations/fmu/README.md), and [integration index](https://github.com/INTO-CPS-Association/robosapiens-trustworthiness-checker/blob/main/integrations/README.md) remain the detailed package-level contracts.

## Python binding

The Python API supplies one complete observation per synchronous logical tick:

```python
from trustworthiness_checker import TcRuntime

checker = TcRuntime.from_path("spec.dsrv")
checker.provide_inputs({"velocity": 3.0, "emergency_stop": False})
verdict = checker.next_output(timeout=1.0)
```

`provide_inputs` returns after the input tick is submitted; `next_output` returns one output mapping or `None` on timeout. Unknown input names raise `ValueError`. Ordinary results are flat dictionaries. Causal modes return `{"values": ..., "causality": ...}` and expose `DeferredValue` and `NoValue` explicitly. Build and test from the repository root with the locked integration environment:

```sh
uv run --project integrations/python --locked --group dev maturin develop
uv run --project integrations/python --locked --group dev pytest integrations/python/tests
```

## FMU packaging

The FMU adapter currently targets CPython 3.12, UniFMU 0.14.x, and FMI 2.0 Co-Simulation. Build the checked-in velocity-safety example from the repository root:

```sh
integrations/fmu/scripts/build.sh \
  --spec-dir integrations/fmu/examples/velocity-safety
```

The artefact is written to `integrations/fmu/dist/trustworthiness_checker.fmu`. The adapter generates FMI metadata from the selected DSRV specification; use the integration's validation scripts for packaging and black-box checks. Build a separate artifact for each target platform.

## Docker and supervisors

Build the deploy image from the repository root:

```sh
docker build -f docker/DockerfileDeploy \
  -t trustworthiness-checker:deploy .
```

The image does not contain repository examples. Mount the model, trace, or configuration paths that the process reads, and use service hostnames rather than `localhost` when a broker is a sibling container.

This running-total program reads each `x` value and adds it to the previous result:

```dsrv
in x
out z
z = default(z[1], 0) + x
```

The repository stores it as `examples/counter.dsrv` with input in `examples/counter.input`. Running it in the container needs no published port:

```sh
docker run --rm \
  -v "$PWD/examples:/work/examples:ro" \
  trustworthiness-checker:deploy \
  /work/examples/counter.dsrv \
  --input-file /work/examples/counter.input \
  --output-stdout
```

The checker is the foreground entrypoint. The current image provides no checker-specific readiness endpoint or healthcheck, and the process has no documented graceful-signal protocol. Treat the first expected result as an application observation, not process existence; do not infer remote persistence from container exit or local output flush. See [Run the checker as a Docker service](../tutorials/docker-service.md), [Extended Windows usage](../tutorials/windows.md), and the [process contract](../reference/process-contract.md) for platform invocation, stop, logging, and networking boundaries.

## External services

- MQTT and Redis are live dependencies for their corresponding sources and destinations. The default generic routes use model variable names; route catalogs or named configurations set deployment-specific names.
- ROS 2 requires a sourced ROS installation and the `ros_interfaces` overlay before `cargo run --features ros`.
- The Docker Compose file is a development/EMQX workspace; its `dev` service runs `sleep infinity`, not the production checker command.

For internal runtime and I/O ownership, use the [dataflow runtime I/O architecture](../architecture/dataflow/runtime-io.md).
