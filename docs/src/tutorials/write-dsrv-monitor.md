# Tutorial: write and run DSRV models

DSRV is the Trustworthiness Checker's default language for **stream runtime verification**: computing results and checking properties as sequences of observations arrive. A model declares named input streams and defines other streams through equations. Each stream has a value at a logical tick, and an equation can use values from the current tick or stored history.

These equations can express both derived measurements and Boolean properties. For example, a model can:

- combine two sensor readings from the same tick;
- accumulate a running total and check whether it remains below a limit;
- compare a reading with its previous value to detect a change.

An output can be an `Int` total, a `Bool` verdict, or another declared type. The equation determines what that result means: `false` indicates a violation only if the output represents a condition that should hold. DSRV also lets an input carry an expression, so some calculations can be supplied while the monitor is running.

DSRV's built-in temporal unit is a **logical tick**, an ordered position in the stream. Referring to the previous tick does not by itself imply that one second has elapsed. A DSRV model can also express elapsed-time properties by receiving a clock or timestamp as an input and using it explicitly in its equations. When a requirement is naturally written with bounded temporal operators over timestamped numeric signals, MSTLO provides those operators and their associated online monitoring semantics directly; see the [MSTLO tutorial](mstlo-monitor.md).

Here you will run an addition model, build a running total, and check a Boolean property of that total. All three use finite input files and print their results to stdout. You will also supply a calculation as input using `defer` and learn how `dynamic` handles subsequent changes.

## Prerequisites

- Run the commands from the repository root.
- Have a Rust toolchain available for Cargo.
- Use the models and input traces included in the repository.
- The commands use Unix shell line continuations. In PowerShell, put each command on one line.

## Adding two input streams

The first model declares two inputs, one output, and an equation that adds the current inputs:

```dsrv
in x: Int
in y: Int
out z: Int
z = x + y
```

`in` names values supplied by the input source; `out` names results to expose. The equation defines `z` at each tick as the sum of that tick's `x` and `y`. The `: Int` annotations make the model's interface explicit, and the type checker verifies the addition before monitoring begins.

The repository stores this model as `examples/simple_add.dsrv`. Its trace, `examples/simple_add.input`, supplies two pairs of integer values:

```text
0: x = 1
   y = 2
1: x = 3
   y = 4
```

Assignments grouped under the same timestamp form one simultaneous logical tick. The model therefore adds `1 + 2` at tick 0 and `3 + 4` at tick 1. Run it with:

```sh
cargo run --quiet -- examples/simple_add.dsrv \
  --input-file examples/simple_add.input \
  --output-stdout
```

The exact stdout is:

```text
z[0] = Int(3)
z[1] = Int(7)
```

Here `z[0]` in stdout labels the result at tick 0, and `Int(3)` displays its type and value. The process exits after both input pairs have been evaluated. It creates no external resources that require cleanup. DSRV and its default runtime are selected automatically; no language or runtime flag is needed for these examples.

### Type declarations and checking

Write explicit types on input and output declarations by default. They document the values a source must provide, let the checker reject incompatible equations early, and keep transport mappings aligned with the model. Common scalar types are `Int`, `Float`, `Bool`, and `Str`; compound types include `List<T>`, `Map<T>`, tuples, and `Struct<...>`.

For example, changing `y` to `Str` while retaining `z = x + y` makes the addition model ill-typed; you can inspect that model in `examples/simple_add_illtyped.dsrv`. Use `Any` only when a stream is intentionally heterogeneous or its concrete type cannot be declared. The default `gradual-typed-untimed` semantics checks explicit annotations and infers information for declarations that lack one.

### Reading input rows as logical ticks

The leading timestamps in a DSRV input file identify logical ticks. They must not decrease. Repeated timestamp lines remain in the same tick; if the same declared input is assigned more than once at that timestamp, the last assignment is used. Variables that the model does not declare as inputs are ignored. If a declared input is missing from a tick, the checker does not substitute an implicit zero and may produce no visible output for a calculation that needs it.

{{#include ../assets/user/finite-monitor-ticks.svg}}

**Reading rule.** Cells at the same horizontal position belong to the same logical tick, and assignments grouped in one input cell are simultaneous. A gap is an absent logical position rather than another assignment within a neighboring tick.

Gaps between timestamps are expanded into missing-value ticks: a jump from `1:` to `3:` leaves tick 2 present but without input values. The timestamps do not make the process wait for wall-clock time to pass.

## Accumulating a running total

This model adds each new `x` value to its previous result:

```dsrv
in x: Int
out z: Int
z = default(z[1], 0) + x
```

In a DSRV equation, `z[1]` reads `z` from one logical tick earlier. This is a relative history offset; the `z[1]` label in stdout instead identifies the result at absolute tick 1. Using history lets the equation refer to its own previous result without depending on its current result.

At the first tick, `z[1]` has no history and yields `Deferred`. `default(z[1], 0)` supplies zero for that state, so the total begins with the first `x` value. At later ticks, the equation adds the current `x` to the preceding total.

The repository stores this program as `examples/counter.dsrv`. Its accompanying input, `examples/counter.input`, supplies `x = 1` at four consecutive timestamps. Run both with:

```sh
cargo run --quiet -- examples/counter.dsrv \
  --input-file examples/counter.input \
  --output-stdout
```

The exact Trustworthiness Checker stdout is:

```text
z[0] = Int(1)
z[1] = Int(2)
z[2] = Int(3)
z[3] = Int(4)
```

The process exits after the fourth input value. It needs only Cargo and the files included in the repository.

## Checking a property of the total

To check whether the accumulated total is still below 3, keep the calculation and expose a Boolean condition:

```dsrv
in x: Int
aux total: Int
out below_limit: Bool
total = default(total[1], 0) + x
below_limit = total < 3
```

`aux` defines a named stream for use within the model without exposing it as an output. `below_limit` uses the **current** `total`, so the new input is included before the condition is evaluated. Both equations describe values at each tick; they are not instructions to update variables one after another.

The repository stores this variant as `examples/counter_threshold.dsrv`. Run it against the same four inputs:

```sh
cargo run --quiet -- examples/counter_threshold.dsrv \
  --input-file examples/counter.input \
  --output-stdout
```

The exact stdout is:

```text
below_limit[0] = Bool(true)
below_limit[1] = Bool(true)
below_limit[2] = Bool(false)
below_limit[3] = Bool(false)
```

| Logical tick | Input `x` | Current `total` | `total < 3` |
|---|---|---|---|
| 0 | 1 | 1 | `true` |
| 1 | 1 | 2 | `true` |
| 2 | 1 | 3 | `false` |
| 3 | 1 | 4 | `false` |

The strict comparison becomes false when the total reaches 3. It checks the condition separately at each tick; a false result does not stop the monitor. This run exits after the fourth input, with no external resources to clean up.

### Handling missing values and unavailable history

The examples so far provide every input at every tick. DSRV distinguishes a missing current value from a result that is waiting for information:

| State | Meaning | Behavior in these examples |
|---|---|---|
| `NoVal` | No input value is available at the current tick | Stdout omits a `NoVal` result. It is not an implicit zero. |
| `Deferred` | A result is waiting for information, such as history before the start of the trace | `default(total[1], 0)` replaces this state with zero. |

`default` handles `Deferred`; it does not fill missing `NoVal` inputs. A running total with gaps therefore needs an explicit policy for missing observations. Do not assume that the initialization in this example supplies that policy.

## Receiving an expression with `defer`

The equations above are fixed when the model is loaded. Use `defer` when the expression to evaluate will arrive later as an input. For example, this model waits for an expression on `e`, then uses it to calculate `z` from the continuing input stream:

```dsrv
in x: Int
in e: Expr<Int>
out z: Int
z = defer(e)
```

`Expr<Int>` declares that `e` carries an expression whose result must be an `Int`. In the input file, the expression is written in quotes, as in `"x + 1"`; its declared type records what it must produce. The received expression is parsed and type-checked when it arrives. Because the result type belongs to the source declaration, `defer(e)` needs no local type ascription. A Boolean property source would use `Expr<Bool>` and produce a `Bool` output.

The repository stores this program as `examples/dups/defer.dsrv`. At timestamp 1, its input file, `examples/dups/defer.input`, supplies `e = "x + 1"`; later rows continue to supply `x`. Run the example from the repository root:

```sh
cargo run --quiet -- examples/dups/defer.dsrv \
  --input-file examples/dups/defer.input \
  --output-stdout
```

This run emits one visible line for every output index from 1 through 14. The beginning and end of the output are:

```text
z[1] = Int(2)
z[2] = Int(3)
...
z[14] = Int(15)
```

There is no visible `z[0]` line because `e` is absent at tick zero and the stdout sink suppresses the resulting `NoVal`. At tick 1, `defer` accepts `"x + 1"` and produces `2`. It then keeps evaluating that expression with each new `x`; later expressions on `e` do not replace it.

## Updating an expression with `dynamic`

Use `dynamic` when later expressions should replace the active calculation. In the preceding model, replace the equation for `z` with this equation fragment:

```dsrv
z = dynamic(e)
```

The input declaration remains `in e: Expr<Int>`. As with `defer`, each accepted expression must produce an `Int`. An optional second argument restricts the streams that the expression may read: `dynamic(e, {x})` makes only `x` available to it.

The following four-tick example compares the two operations. Here the expression input is named `p`: it starts as `"x + 1"`, changes to `"x * 2"`, then returns to `"x + 1"`. `dynamic(p)` replaces its expression when the text changes; returning to old text starts a fresh evaluation. `defer(p)` keeps the first accepted expression and continues evaluating it as `x` changes.

{{#include ../assets/user/dynamic-defer-user-ticks.svg}}

**Reading rule.** Cells aligned at one horizontal position belong to the same logical tick. The thin spans show how long each expression remains active. At tick 2, `dynamic(p)` uses `"x * 2"` and produces `6`, while `defer(p)` still uses `"x + 1"` and produces `4`.

`dynamic` and `defer` operate within a model. To replace the monitor configuration through a control route, see [Reconfigure a running monitor](reconfigure-running-monitor.md).

For the language's dynamically updated-property context, see [Kristensen et al., “DynSRV: Dynamically Updated Properties for Stream Runtime Verification” (2025)](https://doi.org/10.1007/978-3-032-05435-7_7).

## Troubleshooting

- If a model does not parse, check declaration names, equation syntax, and the
  `dsrv` file path before changing runtime options.
- If the checker does not exit after reading a file, confirm that the selected input is `--input-file`; MQTT, Redis, and ROS sources remain active while waiting for messages.
- If a result is absent, check whether the current value is `NoVal` or
  `Deferred`. Stdout suppresses `NoVal`; a `Deferred` value has its own
  Debug-style encoding and is not a numeric fallback.
