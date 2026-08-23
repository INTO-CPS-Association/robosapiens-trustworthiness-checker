# Dataflow execution model

[← Previous: Dataflow architecture](index.md) · [Next: Compilation](compilation.md) →

The dataflow monitor turns a set of stream equations into a synchronous machine. Compilation determines what can be fixed ahead of time; evaluation repeatedly advances that machine by one logical instant.

This page establishes the model shared by the temporal and language-state layers. See [Temporal state](temporal-state.md) for history and commit semantics, and [Language state](language-state.md) for conditionals, functions, and recursion.

## A tick is one coherent row

A **tick** is one successful evaluation of a complete input row. The monitor accepts one entry for every declared input, computes every required stream at most once, and projects the declared outputs from the completed row. There is no observable sub-tick in which some output streams belong to the new row while others still belong to the old one.

Declaration order is not execution order. If `alert` reads `total`, and `total` reads `scaled`, the monitor evaluates `scaled`, then `total`, then `alert`, even if they were declared in the opposite order.

```text
in x: Int
out alert: Bool
out total: Int
out scaled: Int

alert  = total > 20
total  = default(total[1], 0) + scaled
scaled = x * 2
```

![Three streams with same-tick edges and a historical self-edge](../../assets/dataflow/example-streams.svg)

On the first two ticks this machine produces:

| tick | `x` | `scaled` | previous `total` | `total` | `alert` |
| ---: | ---: | -------: | ----------------: | ------: | :------ |
| 1 | 4 | 8 | unavailable, use `0` | 8 | `false` |
| 2 | 8 | 16 | 8 | 24 | `true` |

A tick that fails after execution has begun does not commit temporal history or publish an output row. The monitor then ends, because evaluator-local state elsewhere in the row may already have advanced and is not restored.

## Absence is different from unavailability

The value domain contains two special cases that must not be conflated:

- **No event on this tick** means the row contains no new sample for a stream. Sparse stream lifting may reuse a previously observed operand, depending on the operator. This is distinct from the language's ordinary unit value.
- **Not computable yet** means an expression has a value in principle, but the monitor does not yet have enough state to produce it. A positive delay has this result while its history fills.

In the implementation these cases are `Value::NoVal` and `Value::Deferred`, respectively.

`NoVal` is primarily a sparse-row marker. Many lifted operators retain their last non-`NoVal` operand, but this behavior is operator-specific rather than a global replacement rule. `Deferred` is a real special result: it normally propagates, can replace a retained value, and can itself be stored as a historical sample. Operators such as `default` explicitly interpret it; they do not treat it as an absent event.

This distinction matters at API boundaries and inside stateful expressions. For example, a newly created `x[2]` yields `Deferred`, not `NoVal`, because the expression exists and is advancing but lacks two committed samples.

## Current dependencies and historical dependencies

A stream reference has one of two scheduling meanings:

- A **same-tick dependency** requires the producer's current result. The producer must run before the consumer in this tick.
- A **historical dependency** reads state committed by an earlier tick. It does not require the producer's current result to exist before the read.

The distinction is what permits guarded feedback. In the example, `total` reads current `scaled`, so `scaled` precedes `total`. The reference `total[1]` reads previous `total`; it does not form a same-tick cycle.

A positive delay whose operand is a direct stream reference contributes no same-tick scheduling edge for that reference. Its current operand is captured only after the complete row has been computed. A compound delayed expression can still have current dependencies: in `(a + 1)[1]`, the addition must compute its current operand before that value can be retained for a future tick.

Compilation rejects cycles in the same-tick graph. Positive delayed self-reference and mutually delayed streams are legal because their feedback crosses the tick boundary described in [Temporal state](temporal-state.md).

## Stable places, replaceable order

Correctness depends on separating **identity** from **execution order**. Three kinds of place remain stable while a monitor lives:

1. A computed stream has a stable logical identity and owns one persistent evaluator.
2. Every declared input and computed stream has a stable cell in the current environment row.
3. Every operation in an expression program has a stable position in that program's value and state arrays.

The scheduler may reorder computed streams, but it does not move their evaluators, environment cells, or operation state. Output order is independent as well: outputs are a projection through saved environment cells, not a second evaluation pass.

![Stable environment slots and output projection](../../assets/dataflow/environment-layout.svg)

Operation identity is local to a program, not global. A useful top-level state identity is therefore “stream plus operation.” Nested branches, function calls, and runtime-defined expressions own their own program and evaluator context. Replacing a runtime-defined expression creates a new evaluator and therefore a new state lifetime, even if an immutable compiled template is reused.

## Static structure and dynamic scheduling

Most of the machine is static:

- equations are lowered into immutable expression programs;
- statically visible same-tick dependencies are retained;
- stream evaluators and environment locations are allocated once; and
- temporal and language state remains attached to those evaluators.

A monitor with no `dynamic` or `defer` expressions keeps one dependency order and can execute the row directly.

Reconfiguration makes only a bounded part of the model dynamic. A runtime source string is compiled into a nested expression program. The names actually read by that active program become its **active dependencies** and are merged with the fixed dependencies of the containing stream. The scheduler repairs its cached order only when those active edges require it. `dynamic` may replace its active program; `defer` accepts its first program and then seals it.

![Potential and active dynamic dependencies](../../assets/dataflow/dynamic-dependencies.svg)

Thus “dynamic graph” does not mean that the monitor's streams or state arena are rebuilt every tick. The outer graph's identities remain fixed; active dependency edges and the scheduled order may change, and an individual reconfiguration point may replace its nested evaluator. A new same-tick cycle is rejected before affected streams advance.

## Mapping to the implementation

The compilation and execution pipeline is summarized below.

![Compilation and repeated evaluation pipeline](../../assets/dataflow/pipeline.svg)

| Concept | Implementation | Scope and invariant |
| :------ | :------------- | :------------------ |
| One synchronous machine | `DataflowMonitor` | One successful `evaluate` call is one tick. It owns the environment row, monitor plan, scheduler, and execution state. |
| Logical computed stream | `StreamId` | Indexes the fixed evaluator arena and dependency sets. Schedule changes reorder IDs; they do not redefine them. |
| Current-row location | `EnvironmentSlot` | Assigned once by `EnvironmentLayout`. Inputs occupy the initial slots; computed streams publish to their assigned slots. |
| Operation location | `NodeId` | Indexes an `EvaluationGraph`'s ordered operation vector and the matching `node_values` and `node_states`. It is graph-local. |
| Immutable expression | `StreamProgram` | Contains a bound graph, shared environment layout, fallibility classification, and temporal-commit flag. |
| Fixed monitor structure | `MonitorPlan` | Retains stable stream slots, static dependencies, reconfiguration metadata, temporal streams, and initial ordering data. |
| Mutable ordering | `Scheduler` and scheduled plan bundles | Merge static and active dependencies while routing execution over the unchanged evaluator arena. |

Lowering first represents external references by variable name. Binding replaces those names with `EnvironmentSlot` values and assigns each operation its `NodeId`. The named dependency graph used during compilation can then be discarded: the monitor retains compact static dependency sets, stable bound programs, and the scheduler state needed to incorporate runtime edges.

The canonical interpreter, quickened execution, and optional JIT are execution tiers over this same model. They may use different physical layouts, but they preserve stream publication points, stable state ownership, dependency order, and the temporal commit boundary.

## What a tick guarantees

Four properties define the machine, and everything in the rest of this guide exists to preserve them:

- a successful evaluation advances exactly one logical tick, and evaluates each computed stream at most once within it;
- current reads observe current producers; historical reads observe only committed earlier ticks;
- `NoVal` and `Deferred` stay semantically distinct; and
- schedule replacement never resets or relocates evaluator-owned state.

Outputs follow from the first: they are projected from stable environment locations once the tick completes, not computed by a second pass.

[← Previous: Dataflow architecture](index.md) · [Next: Compilation](compilation.md) →
