The generated rows above are parser metadata. The following constraints come from CLI validation and adapter construction.

## Invocation and selection

Run the main binary as `trustworthiness-checker MODEL INPUT_SELECTION [OUTPUT_SELECTION]`. From a repository checkout, use `cargo run -- MODEL ...`. Exactly one member of the `InputMode` group is required. The `OutputSelection` group is optional: when no output member is selected, the adapter falls back to stdout.

The default values are DSRV, `gradual-typed-untimed`, the `async` runtime, and buffered execution. The default distributed mode is centralised. These defaults are parser defaults; a parsed command still has to pass runtime, language, feature, and resource validation.

## Compatibility rules

- `--language mstlo` selects the MSTLO runtime. Omit `--runtime`; an explicit runtime is rejected.
- DSRV accepts `untimed`, `typed-untimed`, or `gradual-typed-untimed` through the CLI. The DSRV `distributed` runtime accepts only `untimed`.
- For DSRV, `--execution-policy synchronous` requires the `dataflow` or `reconf-dataflow` runtime. MSTLO carries the selected policy in its automatically selected runtime. The policy is an input/evaluation boundary, not a transport transaction.
- `--input-file` is rejected with `reconf-semi-sync` and `reconf-dataflow`. Reconfiguration needs a live, control-capable source.
- `--reconf-topic` and `--no-context-transfer` require a reconfigurable runtime. The selected control route is the configured route, the CLI override, or `reconf` when no route is supplied.
- `--input-window-mode` requires `--input-window-ms` or `--input-window-update-limit`. For file input, `atomic-step` also requires an update limit.
- Redis knowledge options require `--redis-knowledge-input` or `--input-config`; `--redis-knowledge-source` requires `--input-config`. Redis knowledge input is not supported for MSTLO.
- MQTT input and output use rumqttc, with `--mqtt-protocol 3.1.1|5` selecting the protocol for shortcut inputs and outputs. Named MQTT sources and destinations use their own `protocol` field; an input source without the field inherits the CLI selection, while an output destination without it defaults to `3.1.1`. The former implementation-selector flags are no longer accepted.
- `--distributed-work` is present in generated parser metadata and requires `--local-node`, but the current adapter/runtime construction does not consume it to establish the waiting behavior described by its Clap help. Do not use it as an operational contract.

## Features and resources

MQTT is always compiled; Redis is enabled by default. ROS routes, ROS distribution, and ROS scheduling require `--features ros` plus a sourced ROS 2 environment and interface overlay. SAT-backed distribution solving requires `--features sat`. Parsing an option does not prove that its external broker, ROS graph, or feature-gated backend can be opened.

## Observable process behavior

The stdout sink writes `<variable>[<zero-based row>] = <Debug-formatted value>`. It suppresses auxiliary and `NoVal` outputs. Tracing defaults to stderr and can be appended to a file with `--log-file PATH`. File input is finite; live transports remain in the foreground until source/process termination or error. The checker currently has no separate readiness endpoint, custom signal handler, or cross-destination transaction.

Use the task pages for complete workflows: [writing and running DSRV models](../tutorials/write-dsrv-monitor.md), [live MQTT](../tutorials/live-mqtt.md), [ROS input/output](../tutorials/ros-input-output.md), [Redis knowledge input](../redis-knowledge-input.md), [reconfiguration](../tutorials/reconfigure-running-monitor.md), [distributed monitoring](../tutorials/distributed-monitoring.md), and [Docker service](../tutorials/docker-service.md).
