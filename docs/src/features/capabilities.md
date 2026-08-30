# Capabilities

The Trustworthiness Checker (TC) turns a monitor specification and a stream of observations into named output streams. Choose the input and runtime according to whether the task is a finite replay, a live integration, a reconfiguration, or a multi-node deployment.

## Main capabilities

| Need | Supported path | First page |
|---|---|---|
| Replay a known trace | Timestamped file input; the process exits after the trace | [Add two input streams from a trace](../tutorials/finite-trace.md) |
| Author a monitor | DSRV declarations, equations, history indexing, and selected dynamic/deferred constructs | [Write a DSRV monitor](../tutorials/write-dsrv-monitor.md) |
| Observe a live system | MQTT, Redis Pub/Sub, Redis knowledge keys, ROS 2, or named source configurations | [Inputs](inputs.md) |
| Route verdicts | stdout, MQTT, Redis, ROS 2, or an explicit multi-destination output configuration | [Outputs](outputs.md) |
| Change a monitor while it runs | Reconfigurable DSRV runtime with a live control-capable source | [Reconfigure a running monitor](../tutorials/reconfigure-running-monitor.md) |
| Split monitoring work | Static distribution graphs or selected scheduler-backed distributed modes | [Distributed configuration](../reference/distributed-configuration.md) |
| Embed or package the checker | Python API, FMI 2.0 Co-Simulation FMU, or the release Docker image | [Integrations and deployment](integrations-and-deployment.md) |

## User-visible model

A model declares input and output streams. Each input update is composed into a logical tick; an equation is evaluated at that tick, and ordinary output values are sent to the selected destination. A physical batch or transport message is not automatically a new model-time rule. File rows with the same timestamp form one simultaneous tick; MQTT, Redis, and ROS messages normally become independent updates.

The default CLI route is:

```text
MODEL + exactly one input selection → monitor evaluation → stdout
```

Output selection is optional because stdout is the fallback. A finite file source ends naturally. A live source keeps the foreground process waiting for input until the process or source terminates or reports an error.

## Boundaries to plan for

- Only one input mode is selected by the CLI. Use `--input-config` when several named sources must own different variables.
- `--language mstlo` selects the MSTLO runtime; it is not a DSRV runtime alias. Do not pass `--runtime` with MSTLO.
- Reconfigurable runtimes require a live, control-capable input route; `--input-file` is rejected for those runtimes.
- Output admission, a local flush, broker/client acknowledgement, and remote consumer observation are different milestones. The TC does not promise a cross-transport transaction.
- There is no separate readiness endpoint in the current process contract. A first expected result is stronger evidence than process existence alone.

For exact option values, use the [generated CLI reference](../reference/cli.md). For internal ownership and phase boundaries, use the [dataflow architecture](../architecture/dataflow/index.md) rather than this catalogue.
