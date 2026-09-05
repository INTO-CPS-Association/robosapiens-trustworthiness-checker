# Reconfiguration reference

Root reconfiguration replaces a running monitor specification through a live control-capable input route. It is available with `--runtime reconf-semi-sync` and `--runtime reconf-dataflow`; a finite `--input-file` cannot carry the control stream.

## CLI controls

| Option | Meaning |
|---|---|
| `--runtime reconf-semi-sync` | Replace the DSRV monitor/evaluation generation while retaining the opened input and output sessions. |
| `--runtime reconf-dataflow` | Use the reconfigurable dataflow runtime. `--execution-policy` selects its policy. |
| `--reconf-topic ROUTE` | Override the selected source's control route. Without it, use the source's configured route or `reconf`. |
| `--no-context-transfer` | Disable context transfer between old and new runtimes. It is valid only with a reconfigurable runtime. |

The selected source must support control. MQTT, Redis Pub/Sub, and ROS sources can be control-capable; file and Redis knowledge sources are not control sources. With several configured sources, exactly one source declares `reconfiguration_route`.

## Wire envelope

The control payload is JSON5 and has this shape:

```json5
{
  specification: "in x\nin y\nout z\nz = x + y + 10",
  input: {},
  output: {},
}
```

`specification` is required and non-empty. `spec` is accepted as a legacy alias. `input` and `output` default to empty objects, which retains the current local route catalogs in the common spec-only case.

An input configuration can use one of these forms:

```json5
{ source: "robot-events", inputs: { x: "/robot/x" } }
{ inputs: { x: "/robot/x" } }
{ sources: { "robot-events": { x: "/robot/x" } } }
```

An output configuration can use the corresponding forms:

```json5
{ destination: "telemetry", outputs: { z: "/robot/z" } }
{ outputs: { z: "/robot/z" } }
{ destinations: { telemetry: { z: "/robot/z" } } }
```

Within one object, `inputs` and `sources` are mutually exclusive; `source` requires `inputs` and cannot be combined with `sources`. The output forms follow the same rule for `outputs`, `destinations`, and `destination`. Unknown fields are rejected.

## Application boundary

The envelope is structurally validated before it is compiled or resolved against durable local sources and destinations. A successful decode is not an acknowledgement that the replacement is valid for the selected runtime or that external resources opened.

At the local control barrier, already-pending data is emitted before the request. Both reconfigurable runtimes retain their opened input and output sessions. The dataflow runtime applies a planned monitor change in place; the semisynchronous runtime replaces the monitor/evaluation generation and transfers compatible history. Reconfiguration is locally ordered but not globally atomic. Planning, session rebind, monitor application, or acknowledgement failure can terminate the owner loop, and already-applied effects are not rolled back.

The dataflow runtime can continue draining data already admitted to the old side after local control delivery, before the replacement becomes active:

![A delivered local control barrier followed by old-side drain and the first tick under the replacement definition](../assets/user/reconfiguration-observation-ticks.svg)

**Reading rule.** Regular marks are logical data ticks. Dashed vertical guides are physical control-delivery and local-activation boundaries, not extra ticks. The old-side interval is specific to the in-place dataflow monitor cutover; the semisynchronous runtime changes monitor generation while retaining its I/O sessions. Neither model creates a global order across independent producers.

There is no general remote acknowledgement in the CLI. Use an output observation appropriate to the destination and do not interpret local acknowledgement as remote persistence.

See [Reconfigure a running monitor](../tutorials/reconfigure-running-monitor.md), [input configuration](input-configuration.md), [output configuration](output-configuration.md), and [reconfiguration architecture](../reconfiguration.md).
