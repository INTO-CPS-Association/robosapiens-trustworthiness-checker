# General Usage

This page gives a short overview of the standard command-line workflow for running the Trustworthiness Checker (TC) with the most common input and output configurations.

## Basic usage
The most basic usage of the TC is to run it locally on a model and an input trace.

Run the TC locally from the repository with `cargo run -- <other options>`. The `--` separates Cargo's own arguments from the arguments passed to the TC itself.

The general shape of the command is:

```bash
cargo run -- <model> <input-options> [output-options] [extra options]
```

In the simplest case, provide a model, and an input source:

```bash
cargo run -- examples/simple_add.dsrv --input-file examples/simple_add.input
```

This starts the TC with the model in `examples/simple_add.dsrv`, reads the trace from `examples/simple_add.input`, and prints the monitoring result to standard output. Printing to standard output can also be explicitly specified with `--output-stdout`.

Additional flags can be added for language selection, parser choice, runtime settings, or distributed monitoring.

### Getting help

To see the available command-line options, run:

```bash
cargo run -- --help
```

The help output shows the supported input and output flags together with the available option values.

## MQTT Usage

For MQTT-based monitoring, use `--mqtt-input` and `--mqtt-output`.

```bash
cargo run -- examples/simple_add.dsrv --mqtt-input --mqtt-output
```

With these flags, the TC maps stream names in the specification directly to MQTT topics. For example, if the model has input streams `x` and `y`, the TC subscribes to the topics `x` and `y`. If the model produces an output stream `z`, the TC publishes the result on the topic `z`.

### Message format

Publish MQTT payloads as JSON5 values that match the expected stream type.
Standard JSON is accepted because it is valid JSON5. Finite output values are
emitted as compact JSON; non-finite floating-point values use JSON5
`Infinity` and `NaN` literals so they are not silently converted to `null`. On Linux systems, we suggest using MQTT Explorer to publish and inspect messages. For the `simple_add.lola` example, publish the following value to topic `x` and then to topic `y`:

```json
42
```

The result is then published on topic `z` as:

```json
{
    "value": 84
}
```

File-input trace values use the same JSON5 decoder. The timestamp and
`variable = value` framing remains unchanged; the value to the right of `=` is
JSON5.

### Input route catalogs

The generic `--mqtt-input` and `--redis-input` modes are single-source defaults:
the current specification supplies the input variables and their route names.
Use a compact route catalog when transport routes differ from model variable
names:

```json
{
  "x": "/robot/input/x",
  "pose": ["/robot/pose", "Pose2D"]
}
```

Pass this form with `--input-mqtt-file`, `--input-redis-file`, or
`--input-ros-file`. MQTT and Redis normally use string routes with their JSON5
codec; ROS routes include the message codec in the two-element array. The same
compact object shape is used by the corresponding output route-file options.

For several named sources, put source ownership and route catalogs in one
`--input-config` file:

```json
{
  "default": "robot-mqtt",
  "sources": {
    "robot-mqtt": {
      "kind": "mqtt",
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

A variable may be owned by only one source in this owned local source set.
`--input-config` is exclusive with the other input-selection flags. See
[Reconfiguration](./reconfiguration.md) for the optional control route and
compact monitor-configuration messages.

### Input windows

Input windows are applied after source composition. Configure a time bound,
update bound, or both:

```bash
cargo run -- examples/simple_add.dsrv --mqtt-input --output-stdout \
  --input-window-ms 25 --input-window-mode batch
```

`batch` preserves logical tick and simultaneous-step boundaries. `atomic-step`
reduces all updates in a window to one simultaneous step using last-update-wins
for each variable. If a window bound is supplied without a mode, `batch` is
used. The update limit is a flush threshold and soft bound, not a hard maximum:
the window flushes after accepting a complete logical tick that reaches or
exceeds the threshold. One atomic logical tick is never split, so a wide
simultaneous tick can exceed the nominal limit.

## Output destinations and stages

The single-destination shortcuts remain concise:

```bash
cargo run -- examples/simple_add.dsrv --input-file examples/simple_add.input --output-stdout
cargo run -- examples/simple_add.dsrv --mqtt-input --mqtt-output
cargo run -- examples/simple_add.dsrv --redis-input --redis-output
```

Use `--output-config` for multiple destinations, explicit mirroring, or buffering
and coalescing. The file is JSON5 and contains only local configuration. This
valid example makes the default destination primary and mirrors explicitly:

```json5
{
  default: "telemetry",
  shared_stages: [],
  destinations: {
    telemetry: {
      kind: "mqtt",
      host: "localhost",
      port: 1883,
      routes: { alarm: "/robot/alarm", verdict: "/robot/verdict" },
      stages: [
        { kind: "buffer", max_batches: 64, max_updates: 4096 },
        { kind: "coalesce", max_delay_ms: 2, update_limit: 256 }
      ]
    },
    archive: {
      kind: "redis",
      host: "localhost",
      port: 6379,
      mirror: true,
      routes: { alarm: "monitor:alarm", verdict: "monitor:verdict" }
    }
  }
}
```

`telemetry` is the primary destination; `archive` receives both values only
because `mirror: true` is explicit. A secondary destination must also declare a
`partition`, legacy `variables`, `mirror: true`, or a route catalog. The
`variables`, `partition`, and `mirror` fields cannot be combined. `limited-null`
requires a positive `limit`, the manual output backend is programmatic-only, and
MQTT output always uses Paho.

The runtime emits logical ticks independently of the destination. A packed
producer stays packed; a semi-sync producer stays row-oriented; singleton-heavy
runtimes preserve every width-one tick. Buffering is bounded and blocking, with
`max_updates` acting as a pressure threshold that can overshoot by one
indivisible physical batch. Coalescing count limits are flush thresholds
evaluated between incoming physical batches: a complete physical batch may
contain enough ticks or updates to overshoot either limit, because coalescing
never splits an incoming physical batch and preserves every logical tick. Timed
coalescing drives the backend from its worker when a deadline or count limit
fires, even if the producer is idle. Within one destination, duplicate MQTT
topics and Redis channels are rejected; the same topic or channel may be reused
by a distinct destination. The router waits for every destination's readiness
and scans/clones selected values per destination, so destination stages do not
provide full pressure isolation. A flush or close barrier drains shared stages,
routing, destination stages, and all destinations. Cross-transport transactions
are not promised: one external destination may observe a batch before another
fails.

See [Output Architecture](./output.md) for routing, mirroring, backpressure,
and benchmark methodology and workload tradeoffs.

## ROS2 Usage

For ROS2-based monitoring, run the TC with the `ros` feature enabled. I.e., `cargo run --features ros -- <other options>`

Before starting the TC, source your ROS2 installation in the terminal, for example:

```bash
source /opt/ros/<distro>/setup.bash
```

Then build the custom messages from the ROS interface workspace and source the generated overlay:

```bash
cd ros_interfaces
colcon build
source install/setup.bash
cd ..
```

You can then start the TC with a ROS mapping file. The current CLI names are `--input-ros-file` and `--output-ros-file`:

```bash
cargo run --features ros -- examples/simple_add.dsrv \
  --input-ros-file examples/ros/simple_add_mapping.json \
  --output-ros-file examples/ros/simple_add_output_mapping.json
```

In a second terminal, source ROS again and subscribe to the z topic:

```bash
source /opt/ros/<distro>/setup.bash
ros2 topic echo /z
```

In a third terminal, publish to the x and y topics:
```bash
source /opt/ros/<distro>/setup.bash
ros2 topic pub /x std_msgs/msg/Int32 "{data: 1}"
ros2 topic pub /y std_msgs/msg/Int32 "{data: 1}"
```

The second terminal should now show the result.

### MSTLO ROS values

MSTLO uses the generated `robo_sapiens_interfaces/msg/MstloTimedValue` message rather than JSON over ROS. Build and source the interface overlay before compiling with `--features ros`, then use `MstloTimedValue` in both input and output mapping files. The example mapping is [examples/ros/mstlo_timed_value_mapping.json](../../examples/ros/mstlo_timed_value_mapping.json):

```json
{
  "x": ["/signals/x", "MstloTimedValue"]
}
```

An MSTLO file sample keeps the outer delivery tick separate from the inner signal timestamp:

```text
0: x = {"time": 0, "value": 7.0}
1: x = {"time": 1000, "value": 4.0}
```

The ROS message carries quantitative, qualitative, and robustness-interval outputs in its tagged fields. `NoVal` is an internal sparse-stream marker and is never published.

## Redis usage

The Trustworthiness Checker supports two Redis input modes:

- Redis Pub/Sub channels for transient events.
- Redis knowledge-state input for current selected-key values.

See [Redis knowledge-state input](./redis-knowledge-input.md) for a complete
Docker walkthrough, CLI examples, and a representative MAPLE-K configuration.
