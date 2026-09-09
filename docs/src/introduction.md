# Introduction

RoboSAPIENS develops methods that let robots adapt their controllers and configurations when they encounter changes that were not fully anticipated at design time, while keeping those adaptations bounded by safety, robustness, and performance requirements. The project combines open-ended adaptation with assurance rather than treating adaptation alone as success [1, 2].

The RoboSAPIENS Trustworthiness Checker (TC) is a runtime-monitoring component for that setting. It evaluates observations from a robot or a surrounding software system against an executable monitor specification and exposes named result streams. Those results can provide runtime evidence to an adaptation or assurance process. The TC does not itself perform the complete adaptation loop, and a monitor verdict alone does not guarantee that a robotic system is trustworthy.

## What the checker does

A TC run combines three user-visible parts:

1. a **monitor specification** states the properties or stream computations to evaluate;
2. an **input source** supplies observations as logical ticks or timed signal samples;
3. an **output destination** exposes the resulting values or verdicts.

The checker supports finite replay and long-lived monitoring. A finite timestamped file is useful for testing a property against a known trace. Live sources connect the same monitoring role to MQTT, Redis Pub/Sub, selected Redis knowledge keys, ROS 2, or a named multi-source configuration. Results can go to stdout, MQTT, Redis, ROS 2, or an explicit multi-destination configuration. The [input](features/inputs.md) and [output](features/outputs.md) catalogues describe the observable formats and route choices.

The current implementation also includes DSRV runtime reconfiguration through control-capable live sources, selected distributed-monitoring paths, input-window policies, a Python binding, FMI 2.0 Co-Simulation FMU packaging, and a Docker deploy image. Start with the [capability catalogue](features/capabilities.md) for the supported paths and their important restrictions.

## Two monitor languages

**DSRV** is the default stream runtime-verification language. A DSRV model declares named input and output streams and defines each output from current or historical stream values. The language also includes `defer` and `dynamic` constructs for monitors whose data dependencies or properties change while they run. DynSRV describes the dynamic-property context and language semantics [3]; the [DSRV tutorial](tutorials/write-dsrv-monitor.md) documents the syntax and behavior implemented by this repository.

**MSTLO** monitors Signal Temporal Logic properties over timestamped signals. It is a separate TC language and runtime, with choices for monitoring algorithm, synchronization, and variable bindings. The mstlo paper describes its online STL monitoring approach [4]; the [MSTLO tutorial](tutorials/mstlo-monitor.md) shows the TC property-file format, millisecond timestamps, and emitted verdicts.

See [languages and runtimes](features/languages-and-runtimes.md) when choosing between DSRV, MSTLO, finite execution, live execution, reconfiguration, and distributed monitoring.

## Choose a path

- [Getting started](getting-started.md) installs Rust with rustup and runs a small DSRV running-total example on Linux or Unix. It also points native Windows and WSL users to the relevant setup.
- [Tutorials](tutorials/write-dsrv-monitor.md) build from one finite trace to DSRV and MSTLO authoring, live transports, reconfiguration, distributed monitoring, and Docker operation.
- [Feature catalogues](features/capabilities.md) answer what the TC supports across inputs, outputs, languages, runtimes, integrations, and deployment.
- [Reference](reference/cli.md) gives generated CLI facts and focused configuration, wire, and process contracts.
- [Architecture](architecture/dataflow/index.md) explains internal dataflow execution, then links to input, output, and reconfiguration architecture.

## References

1. [RoboSAPIENS project website](https://robosapiens-eu.tech/), “Redefining the future of robotics trustworthy adaptation.”
2. P. G. Larsen et al., “Robotic safe adaptation in unprecedented situations: the RoboSAPIENS project,” *Research Directions: Cyber-Physical Systems*, vol. 2, 2024. [doi:10.1017/cbp.2024.4](https://doi.org/10.1017/cbp.2024.4).
3. M. H. Kristensen, T. Wright, C. Gomes, L. Esterle, and P. G. Larsen, “DynSRV: Dynamically Updated Properties for Stream Runtime Verification,” *Runtime Verification*, 2025. [doi:10.1007/978-3-032-05435-7_7](https://doi.org/10.1007/978-3-032-05435-7_7).
4. A. K. Thomsen et al., “mstlo: Efficient Online Monitoring of Signal Temporal Logic,” 2026. [arXiv:2605.26847](https://arxiv.org/abs/2605.26847).

The work presented here is supported by the RoboSAPIENS project, funded by the European Commission's Horizon Europe programme under grant agreement number 101133807.
