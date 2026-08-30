# Dynamic properties

`dynamic` and `defer` evaluate source text at runtime and activate a nested expression inside an `Evaluator` owned by the enclosing `DataflowMonitor`. The monitor remains the same machine; only the nested body, its evaluator state, and its active dependency edges can change.

![Three runtime-defined expressions show exact active inputs and permitted but inactive scope edges](../../assets/dataflow/reconfigurable-expressions.svg)

**Reading rule.** Solid edges are inputs used by the currently active body. Dashed edges are permitted by scope but inactive for that body. Each reconfiguration point has its own source value and evaluator owner.

## One tick has two ranges

Compilation computes the fixed streams required to obtain each source value. At runtime those source-prerequisite streams execute first. At the [source barrier](tick-execution.md#tick-and-source-barriers), the `DataflowMonitor` compiles or selects active bodies, extracts exact current dependencies, asks its `Scheduler` to retain or repair the active order, and then executes the disjoint main range.

Compile-time scope is a permission set; the selected body contributes the exact active edges for this tick. The [scheduling architecture](scheduling.md#dynamic-edges-and-repair) compares those views across consecutive ticks and shows how `Scheduler` changes order without moving evaluator state.

There is no temporal commit between source and main ranges. Together they execute every computed stream exactly once, and staged temporal state commits only at the later [tick barrier](tick-execution.md#tick-and-source-barriers).

The same source-value sequence produces different evaluator lifetimes for the two operators:

![Dynamic replaces an evaluator when source text changes, while defer retains the evaluator activated by the first source string](../../assets/dataflow/dynamic-defer-ticks.svg)

**Reading rule.** Time runs left to right through source-stream ticks. `dynamic(p)` reuses an evaluator only while source text remains equal; a changed string creates a fresh activation, and returning to an older string does not revive its former state. `defer(p)` seals the first string and advances that same evaluator on later ticks while ignoring replacement strings.

## `dynamic` activation lifetime

A source value equal to the active definition retains the current evaluator. Changed source text activates a replacement evaluator. Compatible state may transfer from the immediately previous active body; otherwise the body starts cold.

The distinction is visible when temporal state exists inside and outside the dynamic body:

![Replacing a dynamic body transfers compatible local delay state from its immediate predecessor or starts cold while fixed monitor history continues](../../assets/dataflow/dynamic-history.svg)

The nested local `DelayState` ring belongs to the replaced evaluator. Compatible state can move only from the immediately previous active evaluator; otherwise the replacement starts cold and a positive delay yields `Value::Deferred` while filling. A direct downstream `z[1]` read uses monitor `HistoryStore`, which remains the same owner and continues to record the stream's produced values.

## `defer` sealing

`defer` waits until its first active definition and then keeps that evaluator. Before activation no nested evaluator exists. Later source values, including `Deferred`, do not replace the sealed body; the active body still evaluates so its temporal and lifted state advances. When the source is `NoVal` or `Deferred`, an active `defer` retains its last non-`NoVal` published result, while `dynamic` propagates the effective special value.

After a successful activation tick, source prerequisites used only to discover the deferred body can be released from later source ranges. Release occurs after the common execution boundary.

## Scope, caching, and failure

Runtime bodies may read only variables admitted by their compiled scope. Checked monitors type-check each active body against the retained checked environment and expected result type.

Caches may retain immutable compiled `StreamProgram` values. They do not retain a bank of mutable evaluators for old activations. A later nested resolution failure makes the current tick fail and the enclosing monitor terminal; earlier nested changes in the same pass are not rolled back.

## Implementation mapping

The implementation mapping leads through `src/dataflow/execution/dynamic_expressions.rs`, evaluator reconfiguration, monitor evaluation and reconfiguration, and focused evaluator and monitor tests.

Continue with [tick execution](tick-execution.md), [replacement identity](replacement-contract.md), or [context transfer](context-transfer.md).
