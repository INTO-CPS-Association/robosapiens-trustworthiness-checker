# Outputs

Choose where model output values are observed. If no output flag is selected, the CLI uses stdout. A single-destination shortcut is enough when results have one destination; use `--output-config` for explicit routing, delivery policies, mirroring, or several destinations.

## Destinations

| Destination | CLI selection | Observable encoding |
|---|---|---|
| stdout | `--output-stdout`, or no output selection | `<variable>[<zero-based row>] = <Debug-formatted value>` |
| MQTT | `--mqtt-output` or `--output-mqtt-file PATH` | Compact JSON envelope such as `{"value":42}`. |
| Redis | `--redis-output` or `--output-redis-file PATH` | JSON/JSON5 value itself, such as `42`; no MQTT `value` envelope. |
| ROS 2 | `--output-ros-file PATH` | Message type and route come from the mapping; requires `--features ros`. |
| Multiple destinations | `--output-config PATH` | Each configured destination receives its selected output projection. |

The stdout sink increments its row index for each logical output tick. It suppresses auxiliary variables and `NoVal` values. For example, this DSRV program keeps a running total:

```dsrv
in x
out z
z = default(z[1], 0) + x
```

With four input values of `1`, stdout is:

```text
z[0] = Int(1)
z[1] = Int(2)
z[2] = Int(3)
z[3] = Int(4)
```

MQTT and Redis route files use the compact variable-to-route object described in the [input catalogue](inputs.md). ROS entries include the message codec.

## Multi-destination behavior

An output configuration names a default destination and a map of destinations. A secondary destination must establish its role with `partition`, `mirror: true`, or a route catalog. `partition` and `mirror` are mutually exclusive on one destination. `limited-null` is a bounded test sink and requires a positive `limit`.

Each destination has one delivery policy. Direct delivery is the default; a bounded queue enables background delivery, optionally with coalescing that preserves logical ticks. In the Rust API, `feed` admits output and `send` also flushes. MQTT flush waits for protocol acknowledgement. Neither flush nor close proves that a remote consumer has persisted or observed the message. Multi-destination output has no cross-transport transaction or rollback: one destination may observe a batch before another fails.

## Choose a task

- [Add two input streams from a trace](../tutorials/finite-trace.md) — inspect exact stdout.
- [Live MQTT](../tutorials/live-mqtt.md) — publish JSON5 values and observe `{"value":...}`.
- [ROS input and output](../tutorials/ros-input-output.md) — configure message types with the ROS feature.
- [Output configuration reference](../reference/output-configuration.md) — use JSON5 destinations, routes, selectors, and delivery policies.
- [Output architecture](../output.md) — read the deeper routing, readiness, flush, and failure contract.
