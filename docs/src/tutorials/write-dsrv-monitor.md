# Tutorial: write a DSRV monitor

DSRV programs name input streams, output streams, and equations over logical ticks. This tutorial starts with a running total, then shows a property supplied through an input with `defer`, and finally a deliberately incomplete `dynamic` syntax fragment.

## Build a running total

This program adds each new `x` value to its previous result:

```dsrv
in x
out z
z = default(z[1], 0) + x
```

`in x` declares an input stream and `out z` declares the result to expose. `z[1]` reads the previous logical tick; at the first tick it has no history, so `default(..., 0)` selects zero.

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

### Tick and value distinctions

- A logical tick is the model's position in the input stream. Same-timestamp
  file assignments are simultaneous, so an equation sees the selected values
  for that tick together.
- A current value and a previous-tick value are different: `z` is current, while
  `z[1]` is history.
- `NoVal` means that an asynchronous input has no value for the current step;
  it is not the integer zero. The stdout sink omits `NoVal` lines.
- `Deferred` is a separate runtime state for a value that cannot yet be
  computed, such as a computation waiting for history. It is not the same as a
  missing input and should not be used as a numeric fallback.

See [add two input streams from a trace](finite-trace.md) for the input-file format and its timestamp rules.

## Deferred expressions with `defer`

The next program treats input `e` as the expression used to calculate `z`:

```dsrv
in x
in e
out z
z = defer(e)
```

The repository stores this program as `examples/dups/defer.dsrv`. At timestamp 1, its input file, `examples/dups/defer.input`, supplies `e = "x + 1"`; later rows continue to supply `x`. Run the example from the repository root:

```sh
cargo run --quiet -- examples/dups/defer.dsrv \
  --input-file examples/dups/defer.input \
  --output-stdout
```

The current default runtime emits one visible line for every output index from
1 through 14. The beginning and end of the output are:

```text
z[1] = Int(2)
z[2] = Int(3)
...
z[14] = Int(15)
```

There is no visible `z[0]` line because `e` is absent at tick zero and the stdout sink suppresses the resulting `NoVal`. Exact activation and replacement behavior for deferred expressions can vary by runtime, so test the runtime and input combination you intend to deploy.

## `dynamic` syntax: illustrative fragment

The parser also has a language-level `dynamic` construct for declaring a
property source and, optionally, the streams it may update:

```dsrv
in source: Int
dynamic(source : Int)
dynamic(source : Int, {x, y})
```

This is an **illustrative fragment**, not a complete runnable program. The repository currently has no complete example that establishes a successful run for this construct under the default runtime. The
language's dynamically updated-property context is described by
[DynSRV](https://doi.org/10.1007/978-3-032-05435-7_7); that paper is background
for the language concept, not authority for this repository's CLI or exact
runtime behavior.

A focused comparison makes the activation difference visible. In the figure,
`x` has values 1, 2, 3, and 4 while property source `p` changes from `"x + 1"`
to `"x * 2"` and then back. `dynamic(p)` replaces its body when the source text
changes; returning to old text creates a fresh body rather than reviving the old
evaluator. `defer(p)` seals the first accepted body and ignores later source
strings while continuing to evaluate that body.

{{#include ../assets/user/dynamic-defer-user-ticks.svg}}

**Reading rule.** Cells aligned at one x-position belong to the same logical
tick. Thin spans identify active body lifetimes; they are not serial phases.
The comparison is a semantic trace, separate from the checked-in `defer`
command above.

Do not confuse language-level `dynamic`/`defer` with CLI reconfiguration. A
reconfiguration source changes the monitor configuration through a configured
control route; it is a separate input/service operation.

For adjacent stream runtime-verification history, see [LoLA](https://doi.org/10.1109/TIME.2005.26).

## Common first diagnostics

- If a model does not parse, check declaration names, equation syntax, and the
  `dsrv` file path before changing runtime options.
- If the checker does not exit after reading a file, confirm that the selected input is `--input-file`; MQTT, Redis, and ROS sources remain active while waiting for messages.
- If a result is absent, check whether the current value is `NoVal` or
  `Deferred`. Stdout suppresses `NoVal`; a `Deferred` value has its own
  Debug-style encoding and is not a numeric fallback.
