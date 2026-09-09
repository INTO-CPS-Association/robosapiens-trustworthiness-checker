# Input configuration reference

Use the CLI input group to select one finite file, one generic live source, one compact route catalog, or one named multi-source JSON5 configuration. The parser validates file shape before opening transports; successful parsing does not prove that a broker, ROS graph, or Redis server is reachable.

## CLI selectors

| Selector | Configuration | Notes |
|---|---|---|
| `--input-file PATH` | Timestamped text rows | Finite; incompatible with reconfigurable runtimes. |
| `--mqtt-input` | Model variable names become MQTT topics | Live; uses rumqttc without an extra Cargo feature. |
| `--input-mqtt-file PATH` | Compact `{ variable: route }` JSON5 object | Live MQTT route catalog. |
| `--redis-input` | Model variable names become Redis Pub/Sub channels | Live; requires Redis support. |
| `--input-redis-file PATH` | Compact route catalog | Live Redis route catalog. |
| `--redis-knowledge-input` | CLI key mappings and Redis knowledge defaults | Selected keys, not Pub/Sub channels; requires ordinary `Value` input. |
| `--input-ros-file PATH` | Compact route/codec catalog | Requires `--features ros`. |
| `--input-config PATH` | Named source registry below | Exclusive with the other selectors. |

Use `--mqtt-protocol 3.1.1|5` to choose the wire protocol for MQTT input selectors; the default is `3.1.1`. A named MQTT source can override that choice with its own `protocol` field; when the field is omitted, the source uses the CLI selection.

Exactly one selector is required. `--redis-port` and `--mqtt-port` apply to simple sources and to configured sources whose own port is omitted.

## Structured input values

A DSRV input declared as `Struct<level: Int, status: Struct<valid: Bool>>` receives an object such as `{level: 2, status: {valid: true}}`. Each named field has its own declared type; nested `Struct` types describe nested objects. See [Receiving structured input](../tutorials/write-dsrv-monitor.md#receiving-structured-input) for a complete model, trace, and run.

For `--input-file`, put the object after the input variable's `=`:

```text
0: reading = {level: 2, status: {valid: true}}
```

The input-file value is JSON5. Field names can be quoted or unquoted, string values need quotes, and Boolean values are `true` or `false`. Write each assignment on one physical line. An assignment replaces the entire input value for its logical tick; fields are not merged with an earlier object. The assignment name must be a declared input variable, so nested fields belong inside its object rather than on separate `reading.level = ...` lines.

### Declared fields and supplied fields

- Use `Struct<level: Int, valid: Bool>` when named fields have distinct types. `Map<Int>` instead describes an object whose values all have the same type, with keys chosen independently of a fixed field list.
- Select declared fields in DSRV equations with dot notation, for example `reading.status.valid`. A field absent from the declared struct type is a type-checking error.
- `Struct<level: Int, ...>` permits additional fields when type-checking struct expressions in a model. The listed fields retain their declared types. The ellipsis does not make them optional or declare types for the additional fields.
- Incoming JSON5 objects are decoded independently of these model checks. Extra incoming fields are currently accepted even without `...`; a struct annotation is not an input schema validator. Supply all fields the equations read, with values of the declared types. Accessing a missing field fails monitoring with `Missing key for map get: FIELD`; a wrong field type can fail when the value is used. Missing fields do not inherit previous values.

To construct a struct inside a DSRV equation, use an object expression such as `{level: x, valid: true}` or the explicit constructor `Struct("level": x, "valid": true)`. These are expression fragments: `x` refers to a stream in the model. An input file instead supplies concrete JSON5 values; it cannot contain the `Struct(...)` constructor or evaluate `x` as a stream reference.

### MQTT and Redis payloads

For JSON/JSON5 MQTT and Redis inputs, the route identifies the input stream, so send the object itself without the file's `timestamp: reading =` prefix. For example:

```json
{"level": 2, "status": {"valid": true}}
```

MQTT input unwraps a top-level `value` field when one is present. To supply a struct that itself has a field named `value`, wrap the whole struct once more: `{"value": {"value": 2, "valid": true}}`. File input and Redis Pub/Sub decode the object directly and need no such wrapper.

## Compact route catalog

A route catalog is a JSON5 object keyed by checker variable:

```json5
{
  x: "/robot/input/x",
  pose: ["/robot/pose", "geometry_msgs/msg/Pose"],
}
```

A string is the normal route form for MQTT and Redis. Their optional format in `[route, format]` must be `json` or `json5`; both accept JSON5 input values. ROS requires the two-element form, with its message type as the format. Route values must be non-empty. Input and output route-file options share this representation; each transport validates its supported formats before opening resources.

## Named source configuration

`--input-config` accepts this resource-free source registry:

```json5
{
  default: "robot-events",
  sources: {
    "robot-events": {
      kind: "mqtt",
      host: "localhost",
      port: 1883,
      routes: {
        alarm: "/robot/alarm",
      },
      reconfiguration_route: "reconf",
    },
    "robot-knowledge": {
      kind: "redis-knowledge",
      host: "localhost",
      database: 2,
      publish_initial: true,
      keys: {
        current_plan: "robot:plan:current",
      },
      retry: {
        max_attempts: null,
        initial_delay_ms: 250,
        max_delay_ms: 5000,
      },
    },
  },
}
```

### Source fields

| Field | Applies to | Meaning |
|---|---|---|
| `default` | root | Optional source ID used when a binding does not name an owner. It must exist in `sources`. |
| `sources` | root | Non-empty map of unique source IDs. A model variable may be owned by only one configured source. |
| `kind` | source | `mqtt`, `redis`, `redis-knowledge`, or `ros`. |
| `host`, `port` | MQTT/Redis sources | Optional connection address; omitted values use the adapter's defaults/CLI port. |
| `protocol` | MQTT | `"3.1.1"` (default) or `"5"`. |
| `routes` | MQTT/Redis/ROS | Variable-to-route map. ROS routes normally include a codec. |
| `reconfiguration_route` | MQTT/Redis/ROS | Optional control route; at most one configured source may declare it. Redis knowledge cannot be a control source. |
| `database` | `redis-knowledge` | Redis database; default `2`. |
| `publish_initial` | `redis-knowledge` | Emit the selected-key startup snapshot into the checker input stream; default `true`. This is not a Redis Pub/Sub publication. |
| `keys` | `redis-knowledge` | Required variable-to-key map. Key names are explicit and unique. |
| `retry` | MQTT/Redis sources | Optional retry object: `max_attempts` (`null` means forever), `initial_delay_ms` (default `250`), and `max_delay_ms` (default `5000`). |

MQTT and plain Redis input retry transient transport failures indefinitely by default. An explicit retry policy can bound the total attempts, including the initial attempt. A rejected MQTT 5 protocol acknowledgement is terminal for the shared driver and is not retried as a transient connection failure. Redis knowledge uses its selected-key recovery policy; see its linked guide for snapshot behavior. Retry limits are separate from `--io-shutdown-timeout-ms`, whose omitted value permits unlimited graceful shutdown.

Unknown fields are rejected. A named source is structurally validated before it is opened; variable ownership, route codecs, feature availability, and external connection are later boundaries.

## Redis knowledge CLI form

This DSRV program exposes the current `robot_mode` value as `observed_mode`:

```dsrv
in robot_mode: Str
out observed_mode: Str
observed_mode = robot_mode
```

The repository stores it as `examples/redis-knowledge/robot-mode.dsrv`. Run it against the selected Redis key with:

```sh
cargo run -- examples/redis-knowledge/robot-mode.dsrv \
  --redis-knowledge-input \
  --redis-knowledge-key robot_mode=robot:mode \
  --redis-knowledge-database 2 \
  --redis-port 6379 \
  --output-stdout
```

The default database is `2`, the startup snapshot is enabled, and a missing key is represented as `NoVal`. The complete state/event distinction and cleanup steps are in [Redis knowledge-state input](../redis-knowledge-input.md).

## Input windows

Window options apply after source composition:

- `--input-window-ms MILLISECONDS` sets a maximum delay;
- `--input-window-update-limit UPDATES` sets a flush threshold;
- `--input-window-mode batch|atomic-step` selects the reduction.

A mode needs at least one bound. `batch` preserves logical ticks. `atomic-step` applies last-update-wins to updates in each window; an indivisible logical tick is never split.

For deeper source ownership and composition semantics, see [input architecture](../input-architecture.md).
