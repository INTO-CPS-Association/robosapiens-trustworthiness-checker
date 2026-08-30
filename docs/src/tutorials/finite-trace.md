# Tutorial: add two input streams from a trace

This example introduces DSRV equations with two inputs. The Trustworthiness Checker reads two pairs of numbers from a timestamped file, adds each pair, writes the results to stdout, and exits when the input is consumed.

## Prerequisites

- Run the commands from the repository root.
- Have a Rust toolchain available for Cargo.
- Use the addition example and input trace described below.

## Add `x` and `y`

The DSRV program declares two inputs, one output, and an equation for the output:

```dsrv
in x
in y
out z
z = x + y
```

`x` and `y` are supplied by the input trace. At each logical tick, the equation exposes their sum as `z`. The repository stores this program as `examples/simple_add.dsrv` and its two input pairs as `examples/simple_add.input`.

```sh
cargo run --quiet -- examples/simple_add.dsrv \
  --input-file examples/simple_add.input \
  --output-stdout
```

The Trustworthiness Checker stdout is exactly:

```text
z[0] = Int(3)
z[1] = Int(7)
```

The index is the zero-based output row. Cargo returns after both input pairs have been processed. No process or external service needs to be cleaned up.

## Input rows and logical ticks

The input file is timestamped. Assignments with the same timestamp are one
simultaneous logical tick:

```text
0: x = 1
   y = 2
1: x = 3
   y = 4
```

A timestamp must not decrease. Repeated timestamp lines remain in the same tick;
if the same selected variable is assigned more than once at that timestamp, the
last assignment is used. Variables not selected by the model are ignored. A
selected variable missing from a tick has `NoVal`, rather than an implicit zero,
and the stdout sink suppresses `NoVal` output.

![Two finite input ticks and their corresponding monitor outputs](../assets/user/finite-monitor-ticks.svg)

**Reading rule.** Cells at the same horizontal position belong to the same logical tick, and assignments grouped in one input cell are simultaneous. A gap would be an absent logical position rather than another assignment within a neighboring tick.

Gaps between timestamps are expanded into missing-value ticks. File rows still
represent simultaneous logical ticks even when the input is processed in a
packed batch internally.

## Build a running total with history

The next program adds each `x` value to its previous output:

```dsrv
in x
out z
z = default(z[1], 0) + x
```

The repository stores the program as `examples/counter.dsrv` and supplies four consecutive values in `examples/counter.input`. Run it from the repository root:

```sh
cargo run --quiet -- examples/counter.dsrv \
  --input-file examples/counter.input \
  --output-stdout
```

The exact stdout is:

```text
z[0] = Int(1)
z[1] = Int(2)
z[2] = Int(3)
z[3] = Int(4)
```

At tick zero there is no prior `z`; `default(z[1], 0)` supplies zero. At later
ticks, `z[1]` means the value from the preceding logical tick, not the preceding
assignment line or physical batch item. This command also exits after the four
input ticks.

For live sources and their long-lived process contract, see the
[MQTT](live-mqtt.md), [ROS](ros-input-output.md), and
[Redis knowledge](../redis-knowledge-input.md) tutorials.
