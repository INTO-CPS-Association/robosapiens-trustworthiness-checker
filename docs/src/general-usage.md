# General usage: choose an input route

After completing [Getting started](getting-started.md), use this page to choose the shortest command for a monitoring route. The focused
tutorials contain the complete prerequisites, observations, lifetime, and
cleanup for each route.

## Command shape

The command examples on this page use a POSIX shell on Linux or Unix. Run from the repository root:

```sh
cargo run -- <MODEL> <ONE_INPUT_SELECTION> [ONE_OUTPUT_SELECTION]
```

Exactly one input selection is required. The common selections are:

| Task | Input selection | Process shape |
|---|---|---|
| Replay a checked-in or user-created trace | `--input-file PATH` | Finite; exits after the file |
| Subscribe to MQTT topics named after model inputs | `--mqtt-input` | Live; waits for messages |
| Subscribe to Redis Pub/Sub channels | `--redis-input` | Live; waits for messages |
| Read selected Redis knowledge keys | `--redis-knowledge-input` | Live; key changes and optional startup snapshot |
| Read ROS 2 topics from a mapping | `--input-ros-file PATH` | Live; requires `--features ros` |
| Combine named local sources | `--input-config PATH` | Usually live; source lifetime applies |

With no output option, results go to stdout. Use `--output-stdout` to make that
choice explicit, or choose one output route such as `--mqtt-output`,
`--redis-output`, `--output-ros-file PATH`, or `--output-config PATH`.

The default language is `dsrv`, with `gradual-typed-untimed` semantics, the
`async` runtime, and buffered execution. See `cargo run -- --help` for the
complete option set.

## Compatibility boundaries

- `--language mstlo` selects the MSTLO runtime; do not also provide
  `--runtime`. Redis knowledge input is not compatible with MSTLO because it
  produces ordinary `Value` input. ROS MSTLO input instead requires the
  generated `MstloTimedValue` codec.
- `--input-file` is finite replay input and cannot carry reconfiguration for a
  reconfigurable runtime. Use a live, control-capable source when runtime
  reconfiguration is required.
- `--input-window-mode` needs `--input-window-ms` or
  `--input-window-update-limit`. `batch` preserves logical ticks;
  `atomic-step` uses last-update-wins within the local window. It is not a
  transaction.
- `--redis-input` means Redis Pub/Sub channels. `--redis-knowledge-input` means
  selected keys plus keyspace notifications; it does not derive keys from
  variable names.

## Choose a focused page

- [Extended Windows usage](tutorials/windows.md) — WSL, native PowerShell setup and invocation, Docker Desktop transport examples, and Wine-based testing of a cross-compiled Windows build.
- [Write and run DSRV models](tutorials/write-dsrv-monitor.md) — typed declarations, timestamped values, simultaneous inputs, ticks, history, `defer`, and cautious `dynamic` guidance.
- [Monitor timed signals with MSTLO](tutorials/mstlo-monitor.md) — STL formulas, embedded dense-time timestamps, delayed and early verdicts, robustness, and synchronization.
- [Live MQTT](tutorials/live-mqtt.md) — broker, topics, JSON5 inputs, and wrapped
  MQTT outputs.
- [ROS input/output](tutorials/ros-input-output.md) — ROS 2 features, mappings,
  live messages, and MSTLO codec separation.
- [Redis knowledge input](redis-knowledge-input.md) — one-key state and a
  mixed Redis/MAPLE-K example.
- [Input configuration](reference/input-configuration.md) and [output
  configuration](reference/output-configuration.md) — route catalogues and
  multi-destination schemas.
- [Reconfigure a running monitor](tutorials/reconfigure-running-monitor.md) and
  [run distributed monitoring](tutorials/distributed-monitoring.md) — live
  control and graph-localized workflows.
