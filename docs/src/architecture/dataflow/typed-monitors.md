# Typed monitors

`DataflowMonitor` exchanges `&[Value]` and `&mut [Value]` with its caller. That is the general interface: it carries every language value, sparse `NoVal` and `Deferred` ticks included, and every runtime and reconfiguration path uses it.

`TypedDataflowMonitor<I, O>` is the alternative for callers that own their row types. It binds a Rust tuple to a checked specification once, positionally against the specification's declared inputs and outputs, and then drives the *same* monitor underneath. During warm execution it encodes and decodes reusable `Value` rows. With JIT enabled, its bound tuple layout lets whole-schedule compilation also produce a direct entry that bypasses those conversions.

## Principal entities

| Entity | Responsibility |
|---|---|
| typed monitor (`TypedDataflowMonitor<I, O>`) | Binds tuple rows to a checked specification and drives the ordinary monitor lifecycle. Promotes itself to a native entry when the monitor's own hotness policy fires. |
| row traits (`TypedInput`, `TypedOutput`, `TypedScalar`) | Sealed traits describing a positional tuple of scalar fields. Implemented for `i64`, `f64`, and `bool`. |
| bound layout (`TypedIoLayout`, `TypedInterface`) | The one-time positional match between tuple fields and declared variables, resolved to environment slots and byte offsets. |
| native entry (`PreparedDirectJit`) | A compiled whole-schedule artifact bound to raw row pointers and its own state allocation. |
| always-native monitor (`TypedJitMonitor<I, O>`) | The narrower `jit`-only alternative, for callers that need native compilation to succeed at construction or not at all. |

## Warming and native promotion

{{#include ../../assets/dataflow/typed-row-path.svg}}

**Reading rule.** Read the two lanes as the same call at two moments in the monitor's life, not as two different APIs. The endpoints are identical and so are the values produced; only the middle changes. The dashed arrow is the one-way promotion, and it is conditional — a monitor whose schedule compiled only in part stays in the upper lane, because a partially native tick is still an ordinary tick.

Every typed monitor begins in the warming phase; some later move to the native one. The two paragraphs below describe each in turn.

While **warming**, `TypedDataflowMonitor::evaluate` writes the tuple's fields into a reusable `Vec<Value>` input row through the bound layout, calls `DataflowMonitor::evaluate`, and decodes the output row back into the output tuple. Every language feature is available because the ordinary monitor is doing the work; the typed layer is an encoder and a decoder.

The bound tuple layout is supplied before JIT activation. Whole-schedule compilation produces the `Value` and direct entries in the same artifact. When an enabled direct entry is available, the monitor moves that artifact and its warmed state into `PreparedDirectJit`; promotion does not compile a replacement. Later ticks call it with row pointers, without constructing or decoding input or output `Value`s.

Preparation retains the warm executor until extraction succeeds. If preparation fails, `try_evaluate` reports `DirectActivation` and keeps the ordinary monitor available for the next call. The failed promotion happens after the current logical tick has run, so retrying the same input would advance another tick.

The promotion is one-way and invisible to the caller. The same `evaluate` call means "encode, run, decode" before it and "call the artifact" after it, and produces the same values either way.

## What the typed interface excludes

The typed interface is narrower than `Value` rows in three ways, each rejected at a different point:

- **At most eight scalar fields per row**, of type `i64`, `f64`, or `bool`. `TypedInput` and `TypedOutput` are implemented for the unit tuple and for arities one through eight, so a wider row simply does not satisfy the trait bound and fails to compile. A specification needing non-scalar types or more interface variables uses `Value` rows.
- **The row must match the specification.** A tuple whose arity or field kinds disagree with the declared variables is rejected when the layout binds, as `TypedBindingError::FieldCount` or `::TypeMismatch`. This is a different check from the arity limit above: one is about what a row *can* be, the other about whether this row fits this specification.
- **Sparse ticks are not representable.** A tuple field has no `NoVal`. A tick whose output would be `NoVal` or `Deferred` is reported as `TypedEvaluationError::NonConcreteOutput` rather than approximated by some stand-in value.

## Controlling native compilation

`JitConfig` selects *when* native compilation happens, not whether a different semantics applies. `JitConfig::eager()` compiles at construction; `JitConfig::after_events(n)` runs `n` interpreted ticks first, which keeps short-lived monitors off the compiler entirely.

`JitReport` is the observable result, and `JitPlan` names what was selected: `Pending` below the threshold, `WholeSchedule` when one artifact evaluates the complete static schedule, `Regions` when only eligible regions compiled, `Unavailable` when nothing usable was produced. An unsupported stream is normal and non-fatal — execution stays canonical and the report records it rather than discarding it.

The `Value`-free path requires an enabled whole-schedule artifact with a compatible direct entry. The report alone does not trigger promotion. Under `Regions`, a typed monitor keeps encoding and decoding rows, because the monitor as a whole still runs through the ordinary tick.

## Implementation mapping

- typed rows, sealed traits, and the field description: `src/dataflow/typed/row.rs`;
- positional binding, byte offsets, and scalar load/store: `src/dataflow/typed/layout.rs`;
- the warming and promotion lifecycle: `src/dataflow/typed/monitor.rs`;
- the always-native variant: `src/dataflow/typed/jit.rs`;
- activation policy and the observable report: `src/dataflow/jit_api.rs`;
- the bound artifact and its raw entry: `src/dataflow/execution/jit/runtime/mod.rs`.

Focused tests are in `src/dataflow/typed/tests.rs`, including the division-by-zero panic contract of the native entry.

Continue with [execution tiers](execution-tiers.md) for how a whole-schedule artifact is selected, or return to the [execution model](model.md) for the `Value` row interface this one narrows.
