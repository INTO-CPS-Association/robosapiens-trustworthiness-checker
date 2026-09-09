# Reconfigure a running monitor

## Outcome

Replace a running monitor's DSRV specification through its live MQTT control
route. This run is long-lived: the checker stays in the foreground until you
stop it. The example uses the default generic MQTT input backend and stdout
output; it does not use file input.

You need a reachable MQTT broker and `mosquitto_pub` (or an equivalent MQTT publisher).

## Prerequisites

Run the commands from the repository root. Start an MQTT broker at
`localhost:1883`, or set the MQTT host using the repository's supported
configuration before starting the checker. The generic MQTT defaults are:

- data topics are the model variable names (`x` and `y` here);
- the generic MQTT broker host is the compiled default `localhost`; use
  `--input-config` with an explicit `host` for another hostname;
- the port is `1883` unless `--mqtt-port` or the selected source configuration supplies it; and
- the reconfiguration topic is `reconf` unless `--reconf-topic` is supplied.

Keep data and control topics distinct.

## Start with an addition rule

The checker initially adds two MQTT inputs:

```dsrv
in x: Int
in y: Int
out z: Int
z = x + y
```

The repository stores this initial program as `examples/simple_add.dsrv`. In terminal 1, start it with a reconfiguration-capable runtime:

```bash
RUST_LOG=info cargo run -- \
  examples/simple_add.dsrv \
  --mqtt-input \
  --output-stdout \
  --runtime reconf-semi-sync \
  --reconf-topic reconf
```

Leave this process running and publish data from terminal 2.

## Send data and a replacement

MQTT input payloads are JSON5 values, so scalar values can be sent directly:

```bash
mosquitto_pub -h localhost -p 1883 -t x -m '1'
mosquitto_pub -h localhost -p 1883 -t y -m '2'
```

The replacement keeps the same inputs and output but adds 10 to their sum:

```dsrv
in x: Int
in y: Int
out z: Int
z = x + y + 10
```

The current reconfiguration wire envelope is JSON5. Send that replacement with the same local input/output routes:

```bash
mosquitto_pub -h localhost -p 1883 -t reconf -m '{"specification":"in x\nin y\nout z\nz = x + y + 10","input":{},"output":{}}'
```

Then send another pair of values:

```bash
mosquitto_pub -h localhost -p 1883 -t x -m '2'
mosquitto_pub -h localhost -p 1883 -t y -m '3'
```

`specification` must be a non-empty string. `input` and `output` are optional
objects; in this example the empty objects keep the existing local route
catalog. The accepted input forms are `source` plus `inputs`, `inputs`, or
`sources`. The accepted output forms are `destination` plus `outputs`,
`outputs`, or `destinations`. `spec` is accepted as a legacy alias for
`specification`; unknown fields are rejected.

## Observe the result

Stdout uses the form `<variable>[<row>] = <debug value>`. For integer values,
the current debug representation is `Int(...)`. You should observe a result
containing the initial value `3` and, after the control message, a result
containing `15`:

```text
z[<row>] = Int(3)
...
z[<row>] = Int(15)
```

The row numbers and exact interleaving are not a contract for this asynchronous
MQTT demonstration. With INFO logging enabled, also look for an INFO message
describing the model change and the message `Starting reconfigured runtime`.
There is no separate user-facing reconfiguration acknowledgement.

A control message is locally ordered at the observed control barrier: pending
local data before that barrier is emitted first, then the semisynchronous
runtime rebinds its persistent I/O sessions and starts the replacement monitor
generation. Compatible variable history can be transferred when context
transfer is enabled. This is **not** a global transaction. There is no
cross-process atomicity, and input changes, output changes, and remote delivery
can be observed at different completion points. If the replacement fails to
parse or typecheck, the process returns an error; external effects already
applied are not rolled back.

## Completion and cleanup

The task is complete when the replacement log and a post-replacement result are
visible. Stop the foreground checker using the process supervisor or terminal
control appropriate to your environment. The runbook does not establish a
flush, persistence, or graceful-shutdown guarantee for that stop. Clean up any
broker that you started separately.

## Troubleshooting

- **No output:** verify that the broker is reachable at the configured host and
  port, and that the published topics are exactly `x`, `y`, and `reconf`.
- **The control message has no effect:** publish the envelope as JSON5 to the
  control topic and use `specification` (or the supported legacy `spec` alias)
  rather than an invented field name.
- **The process exits after the control message:** inspect its error output. A
  replacement that fails parsing or typechecking terminates the process; it is
  not rolled back.
- **Trying to use `--input-file`:** file input is rejected for this runtime with
  `--input-file cannot be used with --runtime reconf-semi-sync`. A finite file
  cannot carry a live reconfiguration control stream.

For the deeper runtime boundary, see [reconfiguration architecture](../reconfiguration.md).
