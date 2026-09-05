# Dynamic properties

`dynamic` and `defer` evaluate source text at runtime and activate a nested expression inside an `Evaluator` owned by the enclosing `DataflowMonitor`. The monitor itself is unchanged; only the nested body, its evaluator state, and its active dependency edges can change.

{{#include ../../assets/dataflow/reconfigurable-expressions.svg}}

**Reading rule.** Solid edges are inputs used by the currently active body. Dashed edges are permitted by scope but inactive for that body. Each reconfiguration point has its own source value and evaluator owner.

## Scope and active dependencies

A scope is a set of names a received formula *may* read. It is not a dependency. Only the names a formula actually reads become active edges, and that distinction is what keeps the graph acyclic when scopes overlap.

The figure above draws this specification:

```dsrv
in sensor: Int
in baseline: Int
in enabled: Bool
in limit_source: Str
in rule_source: Str
in gate_source: Str
out score: Int
out limit: Int
out decision: Bool
score    = sensor - baseline
limit    = defer(limit_source: Int, {score, baseline})
decision = dynamic(rule_source: Bool, {score, limit})
           && dynamic(gate_source: Bool, {enabled})
```

With the three source inputs currently carrying `"score + 10"`, `"score > limit"`, and `"enabled"`:

| point | allowed names | current formula | active edges |
|---|---|---|---|
| in `limit` | `score`, `baseline` | `score + 10` | `score → limit` |
| first in `decision` | `score`, `limit` | `score > limit` | `score → decision`, `limit → decision` |
| second in `decision` | `enabled` | `enabled` | `enabled → decision` |

`baseline` is permitted and unused, so it constrains nothing. The order must place `score` before `limit` and `limit` before `decision`; `enabled` is an input already present when the tick starts, so its edge orders no computed streams. One stream can contain more than one point, and the surrounding `&&` never changes.

An automatic scope — `dynamic(source: T)` with no brace list — admits every declared variable except the enclosing stream. That is a wider permission set, not a wider dependency set. If two automatically scoped streams could each read the other, activating every permitted edge would invent a cycle on ticks where only one direction is used; activating only what the formulas read does not.

## What compilation must fix in advance

Three restrictions exist so that source values can be produced before the schedule is decided. Violating any of them is a `DataflowCompilationError::UnsupportedReconfiguration`:

- a source operand must bind to a constant or an outer environment slot, never to a node-local result;
- a reconfigurable expression may not sit inside a lazy `if` branch;
- any computed stream needed to produce a source must itself use static evaluation and contain no reconfigurable expression.

Together these guarantee the source-prerequisite closure can run once, before the barrier, and then be omitted from the main range. Nested reconfiguration — an active body that itself contains `dynamic` or `defer` — is rejected at evaluation time as `UnsupportedNestedReconfiguration`.

## Where activation happens in a tick

Compilation computes the fixed streams required to obtain each source value, and those source-prerequisite streams run first. Bodies are compiled or selected at the [source barrier](tick-execution.md#tick-and-source-barriers), which is where their exact current dependencies are extracted and the `Scheduler` is asked to retain or repair the order.

[Tick execution](tick-execution.md#tick-and-source-barriers) defines that boundary and its visibility rules. For this page, the relevant guarantee is that activation is complete before any stream of the main range runs. A body is never installed halfway through the row it affects.

The [scheduling architecture](scheduling.md#dynamic-edges-and-repair) shows how the resulting order changes across consecutive ticks without moving evaluator state.

The same source-value sequence produces different evaluator lifetimes for the two operators:

{{#include ../../assets/dataflow/dynamic-defer-ticks.svg}}

**Reading rule.** Time runs left to right through source-stream ticks. `dynamic(p)` reuses an evaluator only while source text remains equal; a changed string creates a fresh activation, and returning to an older string does not revive its former state. `defer(p)` seals the first string and advances that same evaluator on later ticks while ignoring replacement strings.

## `dynamic` activation lifetime

A source value equal to the active definition retains the current evaluator. Changed source text activates a replacement evaluator. Compatible state may transfer from the immediately previous active body; otherwise the body starts cold.

The distinction is visible when temporal state exists inside and outside the dynamic body:

{{#include ../../assets/dataflow/dynamic-history.svg}}

**Reading rule.** The nested local `DelayState` ring belongs to the replaced evaluator. Compatible state can move only from the immediately previous active evaluator; otherwise the replacement starts cold and a positive delay yields `Value::Deferred` while filling. A direct downstream `z[1]` read uses monitor `HistoryStore`, which remains the same owner and continues to record the stream's produced values.

## History starts when the body activates

A newly activated temporal expression does not inherit the samples that passed before it existed. It begins recording on its activation tick, which is why a delay inside a fresh body yields `Deferred` for a while even though the stream it reads has been running.

For `z = dynamic(source: Int)` where the source becomes `"x[2]"` only after two rows have passed:

| tick | `x` | `source` | `z` | samples held by the active `x[2]` |
|---:|---:|---|---|---|
| 0 | 10 | `NoVal` | `NoVal` | no active expression |
| 1 | 20 | `NoVal` | `NoVal` | no active expression |
| 2 | 30 | `"x[2]"` | `Deferred` | 30 |
| 3 | 40 | `NoVal` | `Deferred` | 30, 40 |
| 4 | 50 | `NoVal` | 30 | 40, 50, after producing 30 |

The body never sees 10 or 20. This is activation-local by design: a new body does not borrow history from another body or from the equation that preceded it.

Temporal operators in the *fixed* specification are unaffected, because they are different owners. Given

```dsrv
z        = dynamic(source: Int)
previous = z[1]
```

the `z[1]` in `previous` belongs to the fixed definition and keeps recording across every reconfiguration of `z`:

| tick | active body for `z` | `z` | `previous` |
|---:|---|---:|---|
| 0 | `"x"` | 10 | `Deferred` |
| 1 | `"x"` | 20 | 10 |
| 2 | `"x + 100"` | 130 | 20 |

`previous` returns 20 at tick 2 — a value produced by a body that no longer exists. State continues when its temporal operator continues, not when its source text does.

## `defer` sealing

`defer` waits until its first active definition and then keeps that evaluator. Before activation no nested evaluator exists. Later source values, including `Deferred`, do not replace the sealed body; the active body still evaluates so its temporal and lifted state advances. When the source is `NoVal` or `Deferred`, an active `defer` retains its last non-`NoVal` published result, while `dynamic` propagates the effective special value.

After a successful activation tick, source prerequisites used only to discover the deferred body can be released from later source ranges. Release occurs after the common execution boundary.

## Source values and evaluator advancement

Once a body is active it advances on every tick in which the enclosing node evaluates — including ticks whose raw source is `NoVal` or `Deferred`. Source lifting happens first: `NoVal` repeats the previously retained source value, while a string or `Deferred` becomes the new retained value.

An effective string exposes the active body's result. An effective `Deferred` still advances the body internally, preserving its temporal alignment, but makes `dynamic` emit `Deferred`. That distinction matters: the body is not paused, so its delay rings stay aligned with logical time.

Before any string has been accepted there is nothing to advance — `NoVal` yields `NoVal` and `Deferred` yields `Deferred`. A non-string, non-special source is `InvalidExpressionSource`.

## Memory policy

The published language definition compares three strategies for making a newly activated temporal reference solvable. This implementation is the third.

| strategy | memory bound | history available to a newly active property |
|---|---|---|
| retain the entire history | unbounded in trace length | any earlier sample, so a reference can resolve immediately |
| statically declared dependencies | bounded by declared limits | up to each declared limit; beyond it, never |
| dynamically updated dependencies | bounded, but the bound changes on activation | an existing deep-enough dependency may already hold the samples; otherwise retention grows from the activating tick |

The first is unavailable because the monitor keeps no complete trace archive. The second is not exposed: an explicit scope such as `dynamic(source: Int, {x})` permits the name `x`, but it cannot declare "retain four samples of `x`". The bound comes from the active compiled body, and no construct accepts a separate memory-strategy selector.

What an activation *does* add is depth to the monitor's own bounded history for the variables it reads. That history is maintained independently of the body's local ring, so it can survive as context for a later replacement even after the body that caused it is gone.

## Scope, caching, and failure

Runtime bodies may read only variables admitted by their compiled scope. Checked monitors type-check each active body against the retained checked environment and expected result type.

Caches may retain immutable compiled `StreamProgram` values. They do not retain a bank of mutable evaluators for old activations. A later nested resolution failure makes the current tick fail and the enclosing monitor terminal; earlier nested changes in the same pass are not rolled back.

## Implementation mapping

The implementation mapping leads through `src/dataflow/execution/reconfigurable_expressions.rs`, `src/dataflow/execution/evaluator/reconfiguration.rs`, `src/dataflow/execution/evaluator/expression_state.rs`, and `src/dataflow/monitor/reconfiguration.rs`, with focused evaluator and monitor tests.

Continue with [tick execution](tick-execution.md), [replacement identity](replacement-contract.md), or [context transfer](context-transfer.md).
