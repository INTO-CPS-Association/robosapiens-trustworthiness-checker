# Capabilities

The Trustworthiness Checker (TC) turns a monitor specification and a stream of observations into named output streams. Choose the input and runtime according to whether the task is a finite replay, a live integration, a reconfiguration, or a multi-node deployment.

## Main capabilities

| Need | Supported path | First page |
|---|---|---|
| Replay a known trace | Timestamped file input; the process exits after the trace | [Write and run DSRV models](../tutorials/write-dsrv-monitor.md) |
| Author a monitor | DSRV declarations, equations, history indexing, and selected dynamic/deferred constructs | [Write and run DSRV models](../tutorials/write-dsrv-monitor.md) |
| Observe a live system | MQTT, Redis Pub/Sub, Redis knowledge keys, ROS 2, or named source configurations | [Inputs](inputs.md) |
| Route verdicts | stdout, MQTT, Redis, ROS 2, or an explicit multi-destination output configuration | [Outputs](outputs.md) |
| Change a monitor while it runs | Reconfigurable DSRV runtime with a live control-capable source | [Reconfigure a running monitor](../tutorials/reconfigure-running-monitor.md) |
| Split monitoring work | Static distribution graphs or selected scheduler-backed distributed modes | [Distributed configuration](../reference/distributed-configuration.md) |
| Embed or package the checker | Python API, FMI 2.0 Co-Simulation FMU, or the release Docker image | [Integrations and deployment](integrations-and-deployment.md) |
