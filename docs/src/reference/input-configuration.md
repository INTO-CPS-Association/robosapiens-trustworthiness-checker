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
- Incoming JSON5 objects are decoded independently of these model checks. Extra incoming fields are currently accepted even without `...`; a struct annotation is not an input schema validator. Supply all fields the equations read, with values of the declared types. Accessing a missing field fails monitoring with `Missing key for map get: FIELD`; a wrong field type can fail when the value is used. Missing fields do not inherit previous values. Timed RPIO observation sources enforce the complete declared type at admission; see [RPIO observation journals](#rpio-observation-journals).

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
| `kind` | source | `mqtt`, `redis`, `redis-knowledge`, `ros`, or one of the [RPIO observation](#rpio-observation-journals) kinds. |
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

## RPIO observation journals

RPIO records describe callback steps and the immutable Knowledge versions they
name. The selected native wire format is schema version 2: phase records carry
version references, and separate Knowledge records supply each referenced
value and writer. Activation tokens are strings within a callback run;
Knowledge write sequences belong to a store epoch. Neither is a global record
position.

| Kind | Input value | Reads |
|---|---|---|
| `rpio-observation-file` | `rpio::journal::Record<K, E, M>` | Ordered `files` of native JSON Lines records; no clock ticks. |
| `rpio-observation` | `rpio::journal::Record<K, E, M>` | Separate Redis `phase_stream` and `knowledge_stream`; no clock ticks. |
| `rpio-observation-clock-file` | `rpio::journal::timing::Observation<K, E, M, D>` | Native `files`, with optional replay clock samples. |
| `rpio-observation-clock` | `rpio::journal::timing::Observation<K, E, M, D>` | Separate Redis phase/Knowledge streams; optional periodic Redis `TIME` samples. |

Use a timed observation kind when the model needs branded instants or complete
admission against its declared input type, including when no clock samples
are required. Its value has this shape:

```text
Struct<record: Option<Struct<time: std::time::Instant<D>, value: Record>>,
       clock: Option<Struct<clock: Str, time: std::time::Instant<D>, origin: Str>>>
```

A record's instant comes from its native `arrival.journal_time`; the source
keeps that value when resolving references. Clock-only ticks have `record:
None`. Knowledge values are tagged by key; triggers and event details retain their
explicit union tags, and event envelopes retain their declared structs. Timed-source admission checks every declared field:
integer-written Float fields are converted to Float, absent optional struct
fields become `None`, supplied Options require their explicit `Some`/`None`
tags, and newtype brands must match. Closed structs reject undeclared fields.
`Any` retains concrete values without imposing a domain schema.

| Field | Applies to | Meaning |
|---|---|---|
| `variable` | all four kinds | Input variable owned by this source. |
| `files` | file kinds | Non-empty ordered list of native JSON Lines capture paths. |
| `capture_checks` | file kinds | Optional frozen metadata, one entry per file in the same order: `{records, sha256, journal_clock}`. `records` counts nonblank records; SHA-256 covers exact file bytes. |
| `domain` | timed kinds | Optional brand of `D`, e.g. `JournalTime`; inferred from the declared Observation type when omitted. A supplied brand must agree. |
| `journal_clock` | timed kinds | Physical clock label for the captured instants. File manifests must agree; live entries must carry this label. |
| `clock_samples` | timed file | Inline samples or a JSON Lines filename: `{time, through, with_record?}`. Alternative to `clock_values`/`steps`. |
| `clock_values` | timed file | Inline sample times; without `steps`, all records are delivered first, followed by all samples. |
| `steps` | timed file | Inline `{step:["journal","clock"], time?}` steps or a JSON Lines filename; schedules `clock_values` and records, consuming all supplied members. |
| `host`, `port`, `database` | live kinds | Redis address; defaults are `localhost`, `6379`, database `0`. CLI Redis port applies when source port is omitted. |
| `phase_stream`, `knowledge_stream` | live kinds | Defaults: `maple:observation:phase`, `maple:observation:knowledge`. |
| `clock_period_ms` | timed live | Optional positive period. Omitted or `null` means no periodic samples. When present, `journal_clock` must be `redis.server.time`. |
| `provenance` | timed kinds | Optional JSON Lines output describing each delivered tick's origins. |
| `remove` | file kinds | Observation ablation capabilities: `writers`, `identity`, `causality`, `reads`, `steps`. |
| `unrecorded` | file kinds | `refuse` (default) rejects absent Knowledge definitions; `incomplete` exposes unresolved evidence explicitly. |

`epoch`, `namespace`, `file`, `after` and top-level `through` are not accepted
fields for these source kinds. Sample `through` is a count of admitted capture
records, independent of Knowledge's global write sequence and Redis IDs.

### Order and capture completeness

File sources resolve references against immutable Knowledge definitions and
preserve capture order. An identical repeated definition is idempotent;
contradictory value/writer definitions of the same reference are errors. A
frozen `capture_checks` manifest rejects changed bytes, record counts and
missing final line feeds before evaluation. Without the manifest, a valid
shorter capture does not establish the intended trial extent.

Live sources start both Redis cursors at `0-0`. Redis IDs are opaque transport
cursors. Within each completed fetched snapshot, each stream retains its
Redis-ID order even when its clock goes backwards. Only the two stream heads
are compared by `journal_time`, with Knowledge first on equal timestamps.
This is not a globally numbered journal:
a missing numeric ID or Knowledge write sequence is not a record-gap proof.
Use a trial manifest and expected source/output counts to establish experiment
completion. Unresolvable references and incomplete fetched snapshots report
errors rather than quietly completing; already proven output may drain.

The resolved property view is a separate contract under
`rpio::journal::view`, with later timing/Knowledge/MAPLE-K helpers below that
namespace and the coordinator in `rpio::journal::state`. An application
adapter supplies processing positions, the physical clock label and message
projections. The view's compatibility field `ActivationId.start_seq` holds
the original string token, not a numeric Start position. Native source input
and the resolved property view must be adapted explicitly.

### Clock samples

A replay sample is released after `through` records across the configured
files. Samples must be nondecreasing in coverage and within the capture's
record count; coverage zero precedes the first record. `with_record: true`
combines a sample with that record's tick and must be the first sample at a
nonempty prefix. Its clock label is `journal_clock`, and origin is `Replay`.

A `clock_samples` file is checked completely before output, then read one line
at a time with a second-pass digest check. Lines are bounded to 1 MiB. A
complete final JSON sample without a line feed is accepted. This streaming
bound applies to file clock samples; native capture records are materialised
for exact reference resolution.

Live periodic samples atomically read Redis `TIME` and both stream high-water
IDs. A sample waits until both covered prefixes have been delivered and never
overtakes buffered evidence. Due samples are served even during continuous
input. Missed periods coalesce into one outstanding sample; during silence,
samples advance time. Live sample origin is `Periodic`.

### Finite example: two records and a later sample

This model exposes whether each tick carries a record or a clock:

```dsrv
{{#include ../../../examples/rpio-observation/records-and-clock.dsrv}}
```

From the repository root, run:

```sh
cargo run --bin trustworthiness_checker -- \
  examples/rpio-observation/records-and-clock.dsrv \
  --input-config examples/rpio-observation/records-and-clock.json5 \
  --output-stdout
```

The configuration reads the two-record native service capture and releases a
sample at 12 seconds after both records. It exits after three ticks:
`recorded[0]` and `recorded[1]` are `true`, `recorded[2]` is `false`;
`clocked[0]` and `clocked[1]` are `false`, `clocked[2]` is `true`.

### Provenance

Timed sources optionally write one JSON line per delivered logical tick:

```json
{"tick":4,"origins":[{"member":"journal","kind":"phase","cursor":"trial.jsonl:4","time":12.0},{"member":"clock","kind":"clock","clock":"fixture.journal","time":20.0}]}
```

Kinds are `phase`, `knowledge`, `incomplete` or `clock`. File cursors identify
physical `file:line`; live cursors retain Redis stream IDs. Clock origins name
the clock label. Ticks begin at zero and, for a model with this source as its
only input, correspond to stdout row numbers, including silent rows.
Provenance records delivery into the input pipeline, so the last provenance
line does not alone prove evaluation or output completion. A stopped/error
run must be reconciled with expected counts before it is accepted as a trial.

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
