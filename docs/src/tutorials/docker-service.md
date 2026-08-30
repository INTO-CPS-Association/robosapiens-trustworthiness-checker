# Run the checker as a Docker service process

## Outcome

Run the Trustworthiness Checker as a foreground process under a supervisor or container runtime. This page establishes the process contract first, then runs a two-input addition example in the repository's deploy image.

## Process contract

The checker invocation is a foreground command whose DSRV file and options are supplied as process arguments. The example program adds two inputs:

```dsrv
in x
in y
out z
z = x + y
```

The repository stores this program as `examples/simple_add.dsrv` and provides two input pairs in `examples/simple_add.input`. Run it locally from the repository root before packaging it:

```bash
cargo run -- \
  examples/simple_add.dsrv \
  --input-file examples/simple_add.input \
  --output-stdout
```

The checked-in input produces integer values `3` and `7`; stdout uses the
current debug representation:

```text
z[0] = Int(3)
z[1] = Int(7)
```

The exact output boundary depends on the selected runtime and destination:
`--output-stdout` writes results to stdout, while tracing diagnostics go to
stderr unless `--log-file` is supplied. A finite file run can complete; a live
transport run is a foreground process that remains available for input.

There is no verified readiness endpoint, image healthcheck, or user-facing
readiness protocol. Process existence is therefore not readiness. Supervisors
must choose an application-specific observation, such as the first expected
result or a log observation, and must account for the selected input
transport.

No explicit checker graceful-signal contract has been established here. Do not
promise that termination flushes local or remote destinations, and do not treat
an exit or a container stop as proof of remote persistence. The image does not
establish persisted application state.

## Build the deploy image

Run from the repository root with a Docker-compatible engine:

```bash
docker build -f docker/DockerfileDeploy -t trustworthiness-checker:deploy .
```

The image entrypoint is `/usr/local/bin/trustworthiness_checker`. The production
binary is built with the repository's `jemalloc` feature. The image does not copy
the repository's `examples/` directory, so mount any model, input, or
configuration files that the process must read.

## Run the addition example in a container

```bash
docker run --rm \
  --name trustworthiness-checker \
  -v "$PWD/examples:/work/examples:ro" \
  trustworthiness-checker:deploy \
  /work/examples/simple_add.dsrv \
  --input-file /work/examples/simple_add.input \
  --output-stdout
```

The checker is the container's foreground entrypoint/PID 1. The expected result
values are `3` and `7` on stdout. No port publishing is needed for this
stdout-only run; the image exposes no ports.

## Logs, networking, and stop behavior

- `docker logs` observes the container's stdout and stderr. If logs must remain
  available after exit, omit `--rm`, then inspect them with
  `docker logs trustworthiness-checker`.
- A live MQTT or Redis deployment needs a shared Docker network and
  container-visible service names. The generic shortcut modes use the compiled
  default host `localhost`; there is no `MQTT_HOSTNAME` environment override.
  Use `--input-config` and, when needed, `--output-config` with an explicit
  `host` set to the sibling service name. Publishing a host port is separate
  from putting the checker and broker on the same network.
- `docker stop trustworthiness-checker` supplies Docker's normal termination
  behavior only. This image documents no checker-specific graceful shutdown,
  readiness, flush, or persistence guarantee.
- If a stopped container must be removed, use `docker rm -f
  trustworthiness-checker`; do not infer that forced removal preserved pending
  output.

The read-only example mount is input material, not an application state store.
Add an explicit writable, durable mount only for a configuration or destination
that the selected checker options actually use.

## Troubleshooting

- **The model or input is missing:** verify the read-only mount and use paths
  inside the container, such as `/work/examples/simple_add.dsrv`.
- **`docker logs` is unavailable:** the container was likely started with
  `--rm` and has already exited; rerun without `--rm` while diagnosing.
- **A live broker cannot be reached:** verify the shared network and the
  configured source/destination `host` and port. `localhost` inside the checker
  container is the checker container, not a sibling broker.
- **A supervisor needs readiness or graceful shutdown:** provide those policies
  outside this image; neither is a declared image contract.

For result routing and destination completion semantics, see
[output architecture](../output.md).
