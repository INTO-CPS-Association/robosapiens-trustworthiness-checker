# Output configuration reference

Use a shortcut for one destination or `--output-config PATH` for explicit routing and several destinations. The file is JSON5 and is parsed and validated before destination clients are opened.

## Shortcuts

| Selector | Destination | Encoding |
|---|---|---|
| no output selector or `--output-stdout` | stdout | `name[index] = value` with zero-based logical output index; the value is written as DSRV source (see [below](#values-written-to-stdout)) |
| `--mqtt-output` / `--output-mqtt-file PATH` | MQTT | `{"value": <JSON value>}` |
| `--redis-output` / `--output-redis-file PATH` | Redis Pub/Sub | JSON value without the MQTT envelope |
| `--output-ros-file PATH` | ROS 2 | Message type supplied by each route format; requires `--features ros` |
| `--output-config PATH` | JSON5 destination registry | Supports local and transport destinations |

`--output-config` is checked before the shortcut selections. MQTT uses rumqttc and is available without an additional Cargo feature. MQTT output shortcuts use `--mqtt-protocol 3.1.1|5`, defaulting to `3.1.1`. Configured MQTT destinations use their own `protocol` field, defaulting to `3.1.1` when it is omitted.

## Values written to stdout

Each value is written as the DSRV expression that builds it, so a reported
value can be pasted back into a specification:

```text
count[0] = 3
average[0] = 1.5
label[0] = "ready"
samples[0] = [1, 2, 3]
reading[0] = Map("model": 2, "entropy": 0.5)
state[0] = Moving(3)
```

A union value is written as a bare constructor, without naming its union: the
value carries no schema, and the type expected where it is used resolves the
tag. It is source for a specification that has `use
experimental::{tagged_unions}`, which is the only kind that could have built
it.

Two marks are not source, because they are states a running monitor is in
rather than things a specification can say: `⊥` for a value that cannot yet
be computed, and `no_val` for a stream that has no value this tick. A stream
with no value is left out of the output rather than written, so only `⊥`
appears in practice. A non-finite `Float` is written as `inf`, `-inf` or
`NaN`, which DSRV has no literals for.

A string is escaped the way the grammar spells escapes (`\t`, `\n`, `\'`,
`\"`, `\\`). A string containing one of those characters does not yet read
back as itself, because the parser keeps an escape as the characters that
spell it rather than decoding it.

## File shape

This example assigns otherwise-unassigned outputs to MQTT and mirrors them to Redis. The `verdict` output has an explicit route on both destinations.

```json5
{
  default: "telemetry",
  destinations: {
    telemetry: {
      kind: "mqtt",
      host: "localhost",
      port: 1883,
      routes: { verdict: "/robot/verdict" },
      delivery: {
        queue: { max_batches: 64, max_updates: 4096 },
        coalesce: { max_delay_ms: 2, update_limit: 256 },
      },
    },
    archive: {
      kind: "redis",
      host: "localhost",
      port: 6379,
      mirror: true,
      routes: { verdict: "monitor:verdict" },
    },
  },
}
```

## Root fields

| Field | Required | Meaning |
|---|---|---|
| `destinations` | yes | Non-empty map of destination IDs to destination configuration. IDs must be non-empty. |
| `default` | no | Destination that owns otherwise-unassigned model outputs. It must name an existing destination. With several destinations, a unique unqualified destination may be inferred when `default` is omitted. |

## Destination fields

| Field | Meaning |
|---|---|
| `kind` | `stdout`, `null`, `limited-null`, `mqtt`, `redis`, or `ros`. |
| `host`, `port` | Supported for MQTT and Redis. The `port` overrides the CLI port for that destination. |
| `protocol` | MQTT wire protocol: `"3.1.1"` (default) or `"5"`. |
| `retry` | MQTT/Redis retry policy described below. |
| `limit` | Required and positive for `limited-null`; invalid for other kinds. |
| `routes` | Variable-to-route catalog. MQTT/Redis formats, if present, must be `json` or `json5`; ROS routes require a message-type format. |
| `partition` | Explicit, non-empty set of variables assigned to this destination. Cannot be combined with `mirror`. |
| `mirror` | Mirror all model outputs assigned to a primary destination. Defaults to `false`. |
| `delivery` | One destination-local delivery policy. Omitted or empty means direct output. |

A secondary destination must establish its role with `routes`, `partition`, or `mirror: true`; it cannot silently receive all values. Unknown fields are rejected. The former `variables`, `stages`, and root `shared_stages` fields are no longer accepted.

## Delivery policy

`delivery.queue` enables background delivery with bounded admission:

```json5
{ queue: { max_batches: 64, max_updates: 4096 } }
```

`max_batches` is a required positive hard limit. Optional `max_updates` must be positive and is a pressure threshold: one indivisible batch may cross it. Queued and in-flight batches remain charged until delivery completes. A full destination applies backpressure rather than dropping observations.

`delivery.coalesce` combines physical batches while preserving every logical tick:

```json5
{ coalesce: { max_delay_ms: 2, tick_limit: 128, update_limit: 256 } }
```

At least one positive delay or count threshold is required. Coalescing emits when a threshold or deadline is reached, on flush, close, or rebind, and when admission pressure requires progress. It does not apply last-update-wins reduction. Coalescing without an explicit queue uses a queue of 32 batches with no update-count threshold. Specify both objects to choose its admission limits, as in the complete example above.

Each destination makes progress within its capacity. A slow destination eventually backpressures the producer; a terminal destination error stops the session. Outputs already accepted by other destinations are not rolled back.

## Retry and shutdown

An explicit retry object has this shape:

```json5
{
  max_attempts: 6,
  initial_delay_ms: 250,
  max_delay_ms: 5000,
}
```

`max_attempts` includes the initial attempt. It must be positive; `null` or omission within a retry object means unlimited attempts. Delays double up to `max_delay_ms`, which must be at least the positive initial delay. Omitting the entire output `retry` object uses six attempts, starting at 250 ms and capped at 5 seconds.

MQTT recovery belongs to the transport driver and retains protocol retransmission state. Built-in MQTT output publishes each result at QoS 1 (at least once). A Rust `MqttClient::publish` request for QoS 2 is rejected before transport submission on MQTT 5 because rumqttc does not expose rejected `PubRec` acknowledgements; negative MQTT 5 protocol acknowledgements are terminal for the driver. Redis output retries transient connection-establishment failures. A Redis publish error is terminal because retrying could repeat a value that already reached the server. Neither transport retries entire output batches at the session layer.

`--io-shutdown-timeout-ms` limits the whole graceful I/O shutdown, including input draining and output close. Omission means no time limit. Expiry reports incomplete shutdown and cancels remaining work; it does not extend the retry budget.

In the Rust API, `feed` admits a batch, `send` also flushes, and `close` drains and cleans up. Repeated close calls retain the same result. MQTT completion waits for the selected QoS acknowledgement; channel output completion means handoff to its channel. Neither proves that a remote application consumed or persisted the value. See [outputs](../features/outputs.md) and [output architecture](../output.md).
