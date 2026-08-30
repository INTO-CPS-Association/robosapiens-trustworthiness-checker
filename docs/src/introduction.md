# Introduction

Welcome to the RoboSAPIENS Trustworthiness Checker (TC) documentation.

This book contains two layers:

- a user guide for running and configuring the TC; and
- an architecture guide describing how the dataflow interpreter and its runtimes work.

## User guide

- [General Usage](./general-usage.md)
- [Run and test on Windows](./windows.md)
- [Whole-monitor runtime reconfiguration](./reconfiguration.md)
- [Distributed Monitoring](./distributed-monitoring.md)
- [Distributed Monitoring Counter Example](./distributed-monitoring-counter-tri.md)
- [Distributed Monitoring Simple Add Example](./distributed-monitoring-simple-add.md)

## Architecture

Start with [Dataflow architecture](./architecture/dataflow/index.md) for the conceptual model, then follow the layered chapters through compilation, runtime ownership, tick execution, dynamic properties, and the interpreter/JIT execution tiers.

The later chapters leave the synchronous monitor and cover the runtime that drives it: the [runtime adapter](./architecture/dataflow/runtime-adapter.md) and [input/output boundary](./architecture/dataflow/runtime-io.md) shared by both variants, then the [reconfigurable runtime](./architecture/dataflow/reconfigurable-runtime.md), its [replacement contract](./architecture/dataflow/replacement-contract.md), [context transfer](./architecture/dataflow/context-transfer.md), and the [failure and termination](./architecture/dataflow/failure-model.md) model that ties every layer together.

The work presented here is supported by the RoboSAPIENS project funded by the European Commission's Horizon Europe programme under grant agreement number 101133807.
