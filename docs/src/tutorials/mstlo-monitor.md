# Tutorial: monitor timed signals with MSTLO

Use MSTLO when the property is stated over real-valued, timestamped signals and bounded future time. This tutorial runs two Signal Temporal Logic (STL) properties over a checked-in finite trace, explains why delayed verdicts appear later than their timestamps, and shows where to choose other online semantics.

The TC integrates the [`mstlo`](https://github.com/INTO-CPS-Association/mstlo) Rust library as a separate language and runtime. Select it with `--language mstlo`; do not pass `--runtime mstlo` or combine it with a DSRV runtime selection.

## Prerequisites

- Run commands from the repository root.
- Have Rust 1.95, Cargo, and the native build toolchain installed.


## The monitoring question

The model contains two named properties:

```text
always_x: G[0,2](x > 3)
combo: (F[0,2](x > 5)) && (G[0,2](x > 0))
```

Each non-empty model line has the form `property_name: formula`; the property name becomes an output stream name. `#` starts a comment, and a formula may continue on following lines. The repository stores these two properties as `examples/simple_stl.mstlo`.

The STL operators in this example are:

- `G[0,2](p)`: globally — `p` must hold throughout the interval from now through two seconds in the future;
- `F[0,2](p)`: eventually — `p` must hold at least once in that two-second interval;
- `&&`: both sub-formulas must hold.

MSTLO interval literals are measured in seconds and may use fractional bounds. The library supports bounded future operators because their upper bound determines how much future signal a delayed monitor must observe.

## Timed input

The accompanying `examples/simple_stl.input` file supplies six samples one second apart:

```text
0: x = {"time": 0, "value": 5.0}
1: x = {"time": 1000, "value": 4.0}
2: x = {"time": 2000, "value": 6.0}
3: x = {"time": 3000, "value": 2.0}
4: x = {"time": 4000, "value": 5.0}
5: x = {"time": 5000, "value": 7.0}
```

There are two time coordinates here. The leading file timestamps `0` through `5` preserve file-input order and logical tick boundaries. The embedded `time` field is the MSTLO dense-time timestamp in milliseconds; it is the time used by `G[0,2]` and `F[0,2]`. MSTLO inputs must have non-negative integer millisecond timestamps and numeric values.

## Run delayed qualitative monitoring

Run the two named properties and six timestamped samples shown above. The repository stores them as `examples/simple_stl.mstlo` and `examples/simple_stl.input`:

```sh
cargo run --quiet -- examples/simple_stl.mstlo \
  --input-file examples/simple_stl.input \
  --output-stdout \
  --language mstlo \
  --semantics delayed-qualitative \
  --execution-policy synchronous \
  --mstlo-synchronization none
```

The exact stdout is:

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

The bracketed number is the zero-based stdout event index, shared by all property outputs. The JSON `time` is the timestamp to which the verdict applies. Because the selected semantics are delayed, `always_x` at time `0` is emitted only after the sample at `2000` ms completes its two-second horizon. It is `true` because `x` is `5`, `4`, and `6` over that interval. The verdict at `1000` ms is `false` because its horizon includes `x = 2` at `3000` ms.

{{#include ../assets/user/mstlo-delayed-horizon.svg}}

**Reading rule.** Cells at the same horizontal position have the same embedded MSTLO timestamp. A horizon bracket identifies the future samples needed for one verdict. The dashed guide marks when enough physical input has arrived to emit that verdict; the output cell remains aligned with the earlier timestamp to which the verdict applies.

Only verdicts through `3000` ms appear: evaluating a delayed two-second horizon at `4000` or `5000` ms would require samples beyond the end of this finite trace. End of file does not invent those future samples.


## Choose an online semantics

The TC exposes the four MSTLO semantics described by Thomsen et al.:

| CLI value | Output | When a verdict is available |
|---|---|---|
| `delayed-qualitative` | Boolean satisfaction | After the bounded future horizon is fully observed. |
| `delayed-quantitative` | Real-valued robustness | After the horizon; positive values indicate satisfaction and negative values violation. |
| `eager-qualitative` | Boolean satisfaction | As soon as the partial signal makes a Boolean verdict definitive; otherwise it waits. |
| `robustness-interval` | Lower and upper robustness bounds | Refines an interval as samples arrive and may emit multiple updates for one verdict timestamp. |

For the first run, keep the semantics explicit. The general CLI default is shared with DSRV and maps to an MSTLO mode internally; an explicit MSTLO value makes the output contract visible in commands and scripts.

`--mstlo-algorithm incremental` is the default and reuses monitor state between samples. `naive` re-evaluates from the available signal and is mainly useful as an alternate algorithmic mode. Algorithm choice should not change the property being monitored.

## Multiple signals and synchronization

This example uses one signal, so `--mstlo-synchronization none` is sufficient. For formulas over asynchronously timestamped signals, select how a value is supplied between samples:

- `zero-order-hold` — keep the most recent sample; this is the CLI default;
- `linear` — linearly interpolate between samples;
- `none` — do not interpolate.

Synchronization changes the signal presented to the STL formula, so it is part of the monitor's observable semantics rather than only a performance setting. Preserve source timestamps when using MQTT or ROS; MSTLO transport payloads carry `{time, value}` rather than an un-timestamped DSRV value.

## Parameters and common errors

A formula may refer to a parameter with a `$` prefix, such as `x > $threshold`. Supply bindings with repeatable or space-delimited `--mstlo-vars name=value` arguments. Every referenced parameter must have a numeric binding accepted by the MSTLO monitor builder.

Common startup failures are actionable:

- a model line without `name: formula` is rejected with its line number;
- duplicate or empty property names are rejected;
- malformed STL is reported with the property name and line;
- negative MSTLO input timestamps and non-numeric signal values are rejected;
- `--runtime` is rejected for MSTLO because `--language mstlo` selects the runtime;
- Redis knowledge input is unsupported because it produces ordinary DSRV values rather than timestamped MSTLO values.

For all option spellings and defaults, see the [generated CLI reference](../reference/cli.md). For supported transports and the ROS `MstloTimedValue` mapping, see [Inputs](../features/inputs.md) and [Languages and runtimes](../features/languages-and-runtimes.md).

## Background

MSTLO implements online monitoring for STL, a temporal logic for real-valued signals. The paper introduces its delayed qualitative and quantitative semantics, eager Boolean verdicts, Robust Satisfaction Intervals, dense-time synchronization, runtime DSL, and incremental monitoring algorithm. Those concepts motivate the choices explained above; the TC repository and its pinned `mstlo` crate define the exact CLI, file format, transport payloads, and current limitations.

Andreas Kaag Thomsen, Niels Viggo Stark Madsen, Valdemar Tang Evans, Thomas David Wright, Lukas Esterle, and Peter Gorm Larsen, “mstlo: Efficient Online Monitoring of Signal Temporal Logic,” arXiv:2605.26847, 2026. [arXiv:2605.26847](https://arxiv.org/abs/2605.26847).
