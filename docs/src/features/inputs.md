# Inputs

Select exactly one CLI input mode. A finite file provides timestamped rows and lets the checker exit; MQTT, Redis, Redis knowledge, ROS 2, and named multi-source inputs are live sources whose process lifetime follows the source.

## Input modes

| Mode | CLI selection | Wire or file shape | Process shape |
|---|---|---|---|
| Finite trace | `--input-file PATH` | `timestamp: variable = JSON5 value` rows | Finite; exits at end of file |
| Generic MQTT | `--mqtt-input` | JSON/JSON5 payload on a topic named after the model input | Live; one decoded message normally becomes one update |
| MQTT route catalog | `--input-mqtt-file PATH` | Compact variable-to-route JSON5 object | Live; routes can differ from variable names |
| Redis Pub/Sub | `--redis-input` | JSON/JSON5 payload on a channel named after the model input | Live; one decoded message normally becomes one update |
| Redis route catalog | `--input-redis-file PATH` | Compact variable-to-channel JSON5 object | Live |
| Redis knowledge state | `--redis-knowledge-input` | Selected Redis keys plus keyspace notifications | Live; startup snapshot is enabled by default |
| ROS 2 route catalog | `--input-ros-file PATH` | Compact route object with `[topic, message type/codec]` entries | Live; requires `--features ros` |
| Named sources | `--input-config PATH` | JSON5 source registry with MQTT, Redis, Redis knowledge, or ROS sources | Source-dependent; variables have one owner |

`--input-config` is exclusive with the other input-selection flags. Generic MQTT and Redis routes default to the model variable names. Redis knowledge mappings are explicit: use `--redis-knowledge-key INPUT=KEY` or a `redis-knowledge` source's `keys` object; the checker never derives a key implicitly from a variable name.

## Route catalogs

The compact route form is a JSON5 object. A string supplies a route. A two-element array supplies a route and codec for transports, such as ROS, whose mapping needs one:

```json5
{
  x: "/robot/input/x",
  pose: ["/robot/pose", "Pose2D"],
}
```

MQTT and Redis inputs always decode JSON/JSON5 values; use string routes for them because an input codec in the two-element form is not applied by those adapters. ROS routes require a message-type codec, so the two-element form is meaningful there. The corresponding output route-file options use the same compact shape, but their codec behavior is output-specific. Validate a route catalog by passing it to the matching CLI option; parsing succeeds before the transport is opened.

## Logical ticks and windows

A file timestamp groups assignments into one simultaneous logical tick. A transport message normally creates an independent `InputBatch::update`; messages from several sources are composed in the order observed by this process, not a distributed total order.

For example, this program adds two MQTT inputs:

```dsrv
in x
in y
out z
z = x + y
```

The repository stores it as `examples/simple_add.dsrv`. Use an input window when nearby MQTT arrivals need local grouping:

```sh
cargo run -- examples/simple_add.dsrv \
  --mqtt-input \
  --output-stdout \
  --input-window-ms 25 \
  --input-window-mode batch
```

`batch` preserves logical ticks. `atomic-step` reduces updates in a window to one simultaneous step using last-update-wins for each variable. A window threshold is a flush threshold; a complete logical tick is never split, so an indivisible tick may cross the nominal update limit.

## Redis knowledge state

Redis Pub/Sub is for transient events. Redis knowledge input reads current selected keys and uses keyspace notifications as invalidation signals. Its defaults are database `2` and an enabled startup snapshot. A missing key becomes `NoVal`; the stdout sink suppresses that value rather than printing a replacement. The complete walkthrough is [Redis knowledge-state input](../redis-knowledge-input.md).

## Deeper references

- [Input configuration reference](../reference/input-configuration.md) — JSON5 source schema and validation boundaries.
- [Live MQTT tutorial](../tutorials/live-mqtt.md), [ROS input/output](../tutorials/ros-input-output.md), and [Redis knowledge input](../redis-knowledge-input.md) — transport tasks.
- [Input architecture](../input-architecture.md) — source ownership, resolution, composition, and reconfiguration boundaries.
