# Reconfiguration Runtime

One of the novel features of the TC is the ability to reconfigure a running monitor at runtime. In practice, this means that the TC can receive an updated specification while it is already running and then rebuild the monitoring pipeline around the new specification.

This is useful when the property to be monitored changes during execution, for example because new streams become relevant, old streams are removed, or a different monitoring task should take over without restarting the full deployment.

## How it works

The reconfigurable semi-sync runtime is built from a reusable `InputPipeline`,
not from a pre-opened ordinary `InputStream`. The pipeline keeps an owned local
source set, route catalogs, source ownership, and the optional input window. The
runtime opens one input generation at a time and uses a private control adapter
to listen for reconfiguration messages.

Ordinary `InputStream` values contain only `InputBatch` data. The control route
is not added as a fake model variable and does not pass through data mapping.
Internally, the adapter carries either a data batch or a terminal
`Reconfigure(MonitorConfig)` item. When a window is configured, ordinary and
reconfigurable inputs both use the same private window driver: ordinary input
has data events only, while the reconfigurable adapter adds the control event.
This keeps control-plane messages out of ordinary runtimes while giving both
paths the same batch, atomic-step, timer, and flush behavior.

A control message is a generation barrier:

1. Data accepted before the message is emitted normally.
2. The shared private window driver flushes pending batch or atomic-step state.
3. The private reconfiguration item is delivered and the window driver
   terminates.
4. No later data from the old source generation is polled or emitted.
5. The old input and runtime tasks are dropped before the replacement pipeline
   is opened.

The replacement specification is parsed and validated, its generation-local
input bindings are resolved against the reusable, owned local `InputSources`
set, output routes are updated, and the next generation starts. A replacement is rebuilt
even when its input and output sets have the same shape.

## Reconfiguration message format

Reconfiguration messages are JSON5. Standard JSON is accepted because it is a
subset of JSON5.

Each message must contain the new specification in `spec`. Input and output
routes use the same compact route form as route catalog files: a string route,
or a two-element array containing `[route, codec]`.

```json
{
  "spec": "in x: Int\nout z: Int\nz = x",
  "inputs": {
    "x": "/robot/input/x"
  },
  "outputs": {
    "z": "/robot/output/z"
  }
}
```

For ROS routes, the codec is the ROS message type required by the route:

```json
{
  "spec": "in x: Int\nout z: Int\nz = x",
  "inputs": {
    "x": ["/x", "Int32"]
  },
  "outputs": {
    "z": ["/z", "Int32"]
  }
}
```

In a single-source deployment, `inputs` can be omitted entirely. The runtime
then resolves the next specification's variables from the local source
catalog and default source:

```json
{
  "spec": "in x: Int\nout z: Int\nz = x"
}
```

With a named multi-source local source set, use `sources` to make ownership
explicit:

```json
{
  "spec": "in alarm: Bool\nin pose\nout safe: Bool\nsafe = alarm",
  "sources": {
    "robot-mqtt": {
      "alarm": "/robot/alarm"
    },
    "robot-ros": {
      "pose": ["/robot/pose", "Pose2D"]
    }
  }
}
```

`inputs` and `sources` are alternatives. `source` may accompany `inputs` to
select one named source for all of those bindings. If no explicit input routes
are present, the owned local source set supplies them from its catalogs and
default. Each model input must have exactly one source owner. `outputs` is
optional and uses the same compact route representation to override output
routes for the new generation.

For a manually authored message, put route and codec information inside the
compact `inputs`, `sources`, or `outputs` objects; the owned local source set
remains responsible for transport configuration.

## Control route and source configuration

For a single configured live source, that source is selected automatically; it
does not need a `reconfiguration_route` marker. Its control route is chosen in
this order: CLI `--reconf-topic`, the source's configured
`reconfiguration_route`, or the built-in default `reconf` route:

```bash
cargo run -- --runtime reconf-semi-sync \
  examples/simple_add.dsrv \
  --mqtt-input \
  --reconf-topic my-reconfig \
  --output-stdout
```

Publish a compact `MonitorConfig` JSON5 payload to `my-reconfig`. The same
pattern works with `--redis-input` and its Redis route.

A named multi-source input configuration keeps source transport settings,
model-data route catalogs, and the fixed control route local to the monitor.
With multiple configured sources, exactly one source must declare
`reconfiguration_route` when the reconfigurable runtime is selected:

```json
{
  "default": "robot-mqtt",
  "sources": {
    "robot-mqtt": {
      "kind": "mqtt",
      "host": "localhost",
      "reconfiguration_route": "monitor/reconfigure",
      "routes": {
        "alarm": "/robot/alarm"
      }
    },
    "robot-ros": {
      "kind": "ros",
      "routes": {
        "pose": ["/robot/pose", "Pose2D"]
      }
    }
  }
}
```

Start the runtime with:

```bash
cargo run --features ros -- --runtime reconf-semi-sync \
  examples/simple_add.dsrv \
  --input-config examples/input-config.json \
  --output-stdout
```

`--reconf-topic` overrides the declared route but never changes the selected
source. A multi-source config with no declaration or more than one declaration
fails clearly. A source with an empty `routes` object can be a dedicated
control-only provider; it is still opened for every generation while other
sources carry model data. `--input-config` cannot be combined with another
input-selection mode.

## Input windows

Input windows are configured with `--input-window-ms`,
`--input-window-update-limit`, and `--input-window-mode batch|atomic-step`.
The default mode when a bound is supplied is `batch`.

The update limit is a flush threshold and soft bound. The shared private driver
accepts each complete logical tick and flushes once the accumulated update count
reaches or exceeds the threshold; it never splits an atomic logical tick. A
wide tick can therefore make either mode's output exceed the nominal limit.

- A **batch** window accumulates data while preserving every logical tick and
  simultaneous boundary. Its crate-private storage may concatenate several
  physical segments.
- An **atomic-step** window reduces all ticks in the window to one simultaneous
  tick using last-update-wins for each variable.

Both modes flush pending data at end-of-stream and at a terminal
reconfiguration barrier. The atomic-step mode is deliberately stronger than
batching: it changes a window of independent updates into one evaluation step. File input
cannot be used with this runtime, and file atomic-step input has an additional
CLI update-limit requirement in ordinary runtimes.

## Context transfer

Context transfer is enabled by default. During replacement, retained history
is kept by variable identity for variables that still exist in the new model.
The histories are aligned to the longest retained history with `NoVal` on the
left, allowing compatible temporal context to survive changes to the
specification. Use `--no-context-transfer` when the replacement must start
without prior history.

Context transfer does not keep the old input source alive. The old generation
is dropped before the replacement sources are opened, so old-generation data
cannot feed the new model.

## Unsupported file reconfiguration

File input is a finite replay source and has no live control route. It is
supported by ordinary runtimes through `--input-file`, but it cannot be used
with `--runtime reconf-semi-sync`:

```text
--input-file cannot be used with --runtime reconf-semi-sync
```

In-memory row and tick sources likewise provide ordinary data only. Use MQTT,
Redis, ROS, or a manual library source with a configured control channel for
runtime reconfiguration. File-based reconfiguration is not implemented.

## Development status

- Basic reconfiguration: Monitor can switch to a new specification without restarting the process.
- ✅ Runtime reconfiguration through `--runtime reconf-semi-sync`.
- ✅ Configurable control route through source-local `reconfiguration_route` and the `--reconf-topic` override.
- ✅ ROS2 reconfiguration with compact route/codec bindings.
- ✅ MQTT reconfiguration.
- ✅ Redis reconfiguration.
- ✅ Context transfer, enabled by default and disableable with `--no-context-transfer`.
- ⚠️ A short delay can occur during reconfiguration while external source and output resources reconnect.
- ❌ File-based reconfiguration.
