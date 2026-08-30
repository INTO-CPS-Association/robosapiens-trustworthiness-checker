# Output configuration reference

Use a shortcut for one destination or `--output-config PATH` for explicit routing and several destinations. The file is JSON5 and is parsed and validated before destination clients are opened.

## Shortcuts

| Selector | Destination | Encoding |
|---|---|---|
| no output selector or `--output-stdout` | stdout | `name[index] = DebugValue` with zero-based logical output index |
| `--mqtt-output` / `--output-mqtt-file PATH` | MQTT | `{"value": <JSON value>}`; output MQTT uses Paho |
| `--redis-output` / `--output-redis-file PATH` | Redis Pub/Sub | `<JSON/JSON5 value>` without the MQTT envelope |
| `--output-ros-file PATH` | ROS 2 | Message type is supplied by each route codec; requires `--features ros` |
| `--output-config PATH` | JSON5 destination registry | Supports local and transport-backed destinations |

`--output-config` is checked before the shortcut selections. Do not assume a local `flush` proves remote persistence or consumer observation.

## File shape

```json5
{
  default: "telemetry",
  shared_stages: [
    { kind: "buffer", max_batches: 64, max_updates: 4096 },
  ],
  destinations: {
    telemetry: {
      kind: "mqtt",
      host: "localhost",
      port: 1883,
      routes: {
        verdict: "/robot/verdict",
      },
      stages: [
        { kind: "coalesce", max_delay_ms: 2, update_limit: 256 },
      ],
    },
    archive: {
      kind: "redis",
      host: "localhost",
      port: 6379,
      mirror: true,
      routes: {
        verdict: "monitor:verdict",
      },
    },
  },
}
```

## Root fields

| Field | Required | Meaning |
|---|---|---|
| `destinations` | yes | Non-empty map of destination IDs to destination configuration. IDs must be non-empty. |
| `default` | no | Destination that owns otherwise-unassigned model outputs. It must name an existing destination. With several destinations, a unique unqualified destination may be inferred when `default` is omitted. |
| `shared_stages` | no | Buffer/coalesce stages applied before destination routing. |

## Destination fields

| Field | Meaning |
|---|---|
| `kind` | `stdout`, `null`, `limited-null`, `mqtt`, `redis`, or `ros`. |
| `host`, `port` | Supported for MQTT and Redis. The `port` overrides the CLI port for that destination. |
| `limit` | Required and positive for `limited-null`; invalid for other kinds. |
| `routes` | Variable-to-route catalog. MQTT/Redis codecs, if present, must be `json` or `json5`; ROS routes require a codec. |
| `variables` | Legacy explicit variable selection. It cannot be combined with `partition` or `mirror`. |
| `partition` | Explicit variable partition. It cannot be combined with `variables` or `mirror`. |
| `mirror` | Mirror all model outputs assigned to a primary destination. It cannot be combined with `variables` or `partition`. |
| `stages` | Destination-local `buffer` or `coalesce` stages. |

A secondary destination in a multi-destination file must establish its role with `routes`, `variables`, `partition`, or `mirror: true`; it cannot silently receive all values. Route variables must belong to the destination's selected variables when `variables` is used.

## Stages

```json5
{
  kind: "buffer",
  max_batches: 64,
  max_updates: 4096,
}
```

A buffer needs positive `max_batches` and, when present, positive `max_updates`.

```json5
{
  kind: "coalesce",
  max_delay_ms: 2,
  tick_limit: 128,
  update_limit: 256,
}
```

A coalesce stage needs a delay, `tick_limit`, or `update_limit`; supplied count limits must be positive. Coalescing changes physical batching but preserves logical ticks. It does not implement last-update-wins.

The selectors `variables`, `partition`, and `mirror` are mutually exclusive. Destination readiness is coupled during local admission, and there is no cross-destination rollback or transaction. See [outputs](../features/outputs.md) and [output architecture](../output.md).
