# Tutorial: monitor timed signals with MSTLO

Signal Temporal Logic (STL) expresses requirements about how numeric signals change over time. An STL property combines numeric predicates, such as `temperature < 80`, with Boolean operators and time-bounded temporal operators. It can express requirements such as:

- a value must remain within a safe range for the next five seconds;
- a request must receive a response within 200 milliseconds;
- a signal must eventually cross a threshold while another signal remains stable.

MSTLO is an online monitor for STL. It consumes timestamped signal samples as they arrive and evaluates an STL property at successive points in signal time. Depending on the selected semantics, it can report Boolean satisfaction, a numeric robustness margin, an early verdict once the answer is certain, or bounds on the robustness while part of the relevant time interval remains unobserved.

MSTLO is a direct fit when a requirement is naturally written with bounded temporal operators over timestamped numeric signals. DSRV can also express elapsed-time properties when a clock or timestamp is supplied as an input and used explicitly in its equations; it is also an option for models organized around logical ticks or structured values such as lists, maps, tuples, and records. This tutorial checks whether a signal stays above a threshold for two seconds and whether it exceeds another threshold at least once. You will run a finite trace, read timestamped Boolean verdicts, and see why a delayed verdict needs samples from its future interval.

## Prerequisites

- Run commands from the repository root.
- Have Rust 1.95, Cargo, and the native build toolchain installed.
- The commands below use Unix shell line continuations. In PowerShell, put each command on one line.

## Defining timed properties

Start with this condition: **at a given time, `x` must remain above `3` throughout the next two seconds.** Write it as a named STL property:

```text
always_x: G[0,2](x > 3)
```

`G[0,2](p)` means that `p` holds *globally*: throughout the interval from the current time through two seconds later, including both endpoints. Here the predicate is the strict comparison `x > 3`, so `x = 3` would violate it. The name `always_x` becomes the output stream name.

The example also checks a compound condition: **`x` must exceed `5` at least once in the next two seconds and remain positive throughout that interval.** The complete model in `examples/simple_stl.mstlo` is:

```text
always_x: G[0,2](x > 3)
combo: (F[0,2](x > 5)) && (G[0,2](x > 0))
```

`F[0,2](p)` means that `p` holds *eventually*: at least once in the interval. `&&` requires both sub-formulas to hold. These properties are evaluated repeatedly along the signal, with a new interval starting at each verdict timestamp.

Each property starts with `property_name: formula`. `#` starts a comment, and a formula may continue on following lines. MSTLO interval literals use **seconds** and may have fractional bounds. Future operators must be bounded so the delayed monitor can determine how much future signal it needs.

## Provide timestamped samples

The accompanying `examples/simple_stl.input` file supplies six samples one second apart:

```text
0: x = {"time": 0, "value": 5.0}
1: x = {"time": 1000, "value": 4.0}
2: x = {"time": 2000, "value": 6.0}
3: x = {"time": 3000, "value": 2.0}
4: x = {"time": 4000, "value": 5.0}
5: x = {"time": 5000, "value": 7.0}
```

Keep the file's logical ticks separate from the signal's time:

| Example | Meaning | Unit |
|---|---|---|
| Leading `2:` | File-input ordering and logical tick boundary | Logical tick |
| `"time": 2000` | Signal timestamp used by MSTLO | Milliseconds |
| `G[0,2]` | Future interval relative to a verdict timestamp | Seconds |

For example, the line beginning `2:` supplies `x = 6` at signal time `2000` ms. A two-second formula interval spans `2000` ms; it does not mean two file ticks. The samples happen to be one second apart here, but timestamps can express other spacings. MSTLO inputs require non-negative integer millisecond timestamps and numeric values.

## Run delayed qualitative monitoring

Choose `delayed-qualitative` to obtain Boolean verdicts after each two-second interval is fully observed. The TC integrates the [`mstlo`](https://github.com/INTO-CPS-Association/mstlo) library through `--language mstlo`, which also selects its runtime. Omit `--runtime` for this language.

Run the model and trace:

```sh
cargo run --quiet -- examples/simple_stl.mstlo \
  --input-file examples/simple_stl.input \
  --output-stdout \
  --language mstlo \
  --semantics delayed-qualitative \
  --execution-policy synchronous \
  --mstlo-synchronization none
```

The command prints the following stdout and exits when the finite input has been processed:

```text
always_x[0] = {"time":0,"value":true}
combo[1] = {"time":0,"value":true}
always_x[2] = {"time":1000,"value":false}
combo[3] = {"time":1000,"value":true}
always_x[4] = {"time":2000,"value":false}
combo[5] = {"time":2000,"value":true}
always_x[6] = {"time":3000,"value":false}
combo[7] = {"time":3000,"value":true}
```

This single-signal run uses `--mstlo-synchronization none` because no alignment between different signals is needed. The synchronous execution policy processes each input tick before admitting the next one.

## Read the delayed verdicts

In `always_x[2] = {"time":1000,"value":false}`:

- `always_x` identifies the property;
- `[2]` is the zero-based stdout event index, shared across all properties;
- `"time":1000` says the verdict applies at signal time `1000` ms;
- `false` says that the property is violated over the interval starting there.

For these properties, the monitor needs samples through **verdict time + 2000 ms**. Start by following `always_x`:

| Verdict time (ms) | Required interval (ms) | Values of `x` in the interval | `always_x` |
|---|---|---|---|
| 0 | 0–2000 | 5, 4, 6 | `true` |
| 1000 | 1000–3000 | 4, 6, 2 | `false` |
| 2000 | 2000–4000 | 6, 2, 5 | `false` |
| 3000 | 3000–5000 | 2, 5, 7 | `false` |

The verdict at `0` is emitted only after the sample at `2000` ms arrives. Its timestamp remains `0`: it describes the start of the interval, not the time when the answer became available. Each later interval contains `x = 2`, which violates `x > 3`.

{{#include ../assets/user/mstlo-delayed-horizon.svg}}

**Reading rule.** Cells at the same horizontal position have the same embedded MSTLO timestamp. A horizon bracket identifies the future samples needed for one verdict. The dashed guide marks when enough physical input has arrived to emit that verdict; the output cell remains aligned with the earlier timestamp to which the verdict applies.

Only verdicts through `3000` ms appear: evaluating a delayed two-second horizon at `4000` or `5000` ms would require samples beyond the end of this finite trace. End of file does not invent those future samples.

Now follow `combo` in the same output. Every complete interval contains either `6` or `7`, satisfying `F[0,2](x > 5)`, and every value is positive, satisfying `G[0,2](x > 0)`. Its four verdicts are therefore all `true`. In particular, the value `2` violates `always_x` but still satisfies the positivity condition in `combo`.

## Compare online semantics

The TC exposes the four MSTLO semantics described by Thomsen et al.:

| CLI value | Output | When a verdict is available |
|---|---|---|
| `delayed-qualitative` | Boolean satisfaction | After the bounded future horizon is fully observed. |
| `delayed-quantitative` | Real-valued robustness | After the horizon; positive values indicate satisfaction and negative values violation. |
| `eager-qualitative` | Boolean satisfaction | As soon as the partial signal makes a Boolean verdict definitive; otherwise it waits. |
| `robustness-interval` | Lower and upper robustness bounds | Refines an interval as samples arrive and may emit multiple updates for one verdict timestamp. |

Keep `--semantics` explicit when adapting the command. A different mode can change both the shape and the number of output events; the event indices above are specific to the delayed qualitative run.

`--mstlo-algorithm incremental` is the default and reuses monitor state between samples. `naive` re-evaluates from the available signal and is mainly useful as an alternate algorithmic mode. Algorithm choice should not change the property being monitored.

## Parameterize a property

To choose a threshold at startup without editing the formula, use a `$` parameter. The model in `examples/simple_stl_threshold.mstlo` contains:

```text
always_x: G[0,2](x > $threshold)
```

Run it over the same signal, binding `threshold` to `3`. This time, choose quantitative semantics to measure the margin above or below the threshold:

```sh
cargo run --quiet -- examples/simple_stl_threshold.mstlo \
  --input-file examples/simple_stl.input \
  --output-stdout \
  --language mstlo \
  --semantics delayed-quantitative \
  --execution-policy synchronous \
  --mstlo-synchronization none \
  --mstlo-vars threshold=3
```

The exact stdout is:

```text
always_x[0] = {"time":0,"value":1.0}
always_x[1] = {"time":1000,"value":-1.0}
always_x[2] = {"time":2000,"value":-1.0}
always_x[3] = {"time":3000,"value":-1.0}
```

For `G[0,2](x > $threshold)`, robustness is the smallest `x - threshold` over the interval. The first interval's minimum is `4`, giving a margin of `1`; each later interval contains `2`, giving `-1`. The verdict timestamps and delayed horizons are the same as before. Only one property is running now, so its event indices are consecutive.

Every referenced parameter needs a numeric binding. Supply several bindings with repeatable or space-delimited `--mstlo-vars name=value` arguments.

## Extend to multiple signals

When a formula combines signals sampled at different times, choose how to align them with `--mstlo-synchronization`: `zero-order-hold` keeps the most recent sample and is the default; `linear` interpolates between samples; `none` disables interpolation. This choice changes the values seen by the formula and can change its verdicts.

Preserve source timestamps when moving to MQTT or ROS: MSTLO values carry both `time` and `value`. See [Inputs](../features/inputs.md) for the supported transport payloads and the ROS `MstloTimedValue` mapping, and the [CLI reference](../reference/cli.md) for synchronization options.

## Troubleshooting

If no verdict appears, first check that the embedded timestamps cover the formula's future horizon. Two seconds require `2000` ms of signal, regardless of how many file ticks have arrived.

For rejected commands, models, or input:

| Symptom | Check |
|---|---|
| Property-file format or name error | Start each property with a unique, non-empty `name:` followed by its formula. A continuation line is allowed after a property has started. |
| STL parse error naming a property and line | Check that formula's syntax and interval bounds. |
| Invalid timestamp or signal value | Supply a non-negative integer `time` in milliseconds and a numeric `value`. |
| Runtime option rejected | Omit `--runtime`; `--language mstlo` selects the runtime. |
| Redis knowledge input rejected | Use a supported timestamped input source. Redis knowledge input produces ordinary DSRV values. |

For all option spellings and defaults, see the [generated CLI reference](../reference/cli.md). For supported transports and the ROS `MstloTimedValue` mapping, see [Inputs](../features/inputs.md) and [Languages and runtimes](../features/languages-and-runtimes.md).

## Background

MSTLO implements online monitoring for STL, a temporal logic for real-valued signals. The paper introduces its delayed qualitative and quantitative semantics, eager Boolean verdicts, Robust Satisfaction Intervals, dense-time synchronization, runtime DSL, and incremental monitoring algorithm. Those concepts motivate the choices explained above; the TC repository and its pinned `mstlo` crate define the exact CLI, file format, transport payloads, and current limitations.

Andreas Kaag Thomsen, Niels Viggo Stark Madsen, Valdemar Tang Evans, Thomas David Wright, Lukas Esterle, and Peter Gorm Larsen, “mstlo: Efficient Online Monitoring of Signal Temporal Logic,” arXiv:2605.26847, 2026. [arXiv:2605.26847](https://arxiv.org/abs/2605.26847).
