# Start here

- [Introduction](./introduction.md)
- [Getting started](./getting-started.md)

# Tutorials

- [Add two input streams from a trace](./tutorials/finite-trace.md)
- [Write a DSRV monitor](./tutorials/write-dsrv-monitor.md)
- [Monitor timed signals with MSTLO](./tutorials/mstlo-monitor.md)
- [Monitor live MQTT input](./tutorials/live-mqtt.md)
- [Redis knowledge-state input](./redis-knowledge-input.md)
- [Use ROS input and output](./tutorials/ros-input-output.md)
- [Reconfigure a running monitor](./tutorials/reconfigure-running-monitor.md)
- [Run distributed monitoring](./tutorials/distributed-monitoring.md)
- [Run the checker as a Docker service](./tutorials/docker-service.md)
- [Extended Windows usage](./tutorials/windows.md)
- [General usage](./general-usage.md)

# Features

- [Capabilities](./features/capabilities.md)
- [Languages and runtimes](./features/languages-and-runtimes.md)
- [Inputs](./features/inputs.md)
- [Outputs](./features/outputs.md)
- [Integrations and deployment](./features/integrations-and-deployment.md)

# Reference

- [CLI reference](./reference/cli.md)
- [Input configuration](./reference/input-configuration.md)
- [Output configuration](./reference/output-configuration.md)
- [Reconfiguration](./reference/reconfiguration.md)
- [Distributed configuration](./reference/distributed-configuration.md)
- [Process contract](./reference/process-contract.md)

# Architecture

- [Input architecture](./input-architecture.md)
- [Dataflow architecture](./architecture/dataflow/index.md)
  - [Canonical semantics]()
    - [Execution model](./architecture/dataflow/model.md)
    - [Compilation](./architecture/dataflow/compilation.md)
    - [Runtime ownership](./architecture/dataflow/runtime-ownership.md)
    - [Scheduling](./architecture/dataflow/scheduling.md)
    - [Tick execution](./architecture/dataflow/tick-execution.md)
  - [Stateful language mechanisms]()
    - [Temporal state](./architecture/dataflow/temporal-state.md)
    - [Language state](./architecture/dataflow/language-state.md)
    - [Dynamic properties](./architecture/dataflow/dynamic-properties.md)
  - [Physical execution]()
    - [Fusion and regions](./architecture/dataflow/fusion.md)
    - [The scalar IR](./architecture/dataflow/scalar-ir.md)
    - [Execution tiers](./architecture/dataflow/execution-tiers.md)
    - [Typed monitors](./architecture/dataflow/typed-monitors.md)
  - [External ownership]()
    - [Runtime adapter](./architecture/dataflow/runtime-adapter.md)
    - [Input and output sessions](./architecture/dataflow/runtime-io.md)
  - [Replacement and containment]()
    - [Root cutover](./architecture/dataflow/reconfigurable-runtime.md)
    - [Replacement identity](./architecture/dataflow/replacement-contract.md)
    - [Context transfer](./architecture/dataflow/context-transfer.md)
    - [Failure and termination](./architecture/dataflow/failure-model.md)
  - [Implementation mapping](./architecture/dataflow/implementation-guide.md)
- [Output architecture](./output.md)
- [Reconfiguration architecture](./reconfiguration.md)
