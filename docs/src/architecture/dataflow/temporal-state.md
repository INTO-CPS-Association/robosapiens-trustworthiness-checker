# Temporal state

Historical reads observe samples committed by earlier successful ticks. Current computation and historical visibility are separated by staging and one post-row temporal commit, but the storage owner depends on where the delay was bound.

{{#include ../../assets/dataflow/history-retention.svg}}

**Reading rule.** Each panel reads state committed through tick `n-1`. A direct top-level delay of an external environment variable reads the monitor's `HistoryStore`; internal, runtime-defined, and recursive delays read evaluator-local `DelayState` rings. Dashed paths stage or record successful row-`n` samples at the common commit, making them visible at tick `n+1`. An unfilled positive delay yields `Value::Deferred`.

## Why staging is required

Evaluators run sequentially, but the language tick is synchronous. If a delay made its current operand historical as soon as its node ran, a later evaluator in the same row could observe a current value as though it belonged to the past. The common commit prevents that temporal aliasing.

A top-level direct delay of another environment variable—for example, `prior_total = total[1]`—is bound through `HistoryAccess` to monitor-owned `HistoryStore` storage. It reads the already committed history of that external variable; its node-local commit is a no-op because the monitor records completed values centrally.

A delay inside a runtime-defined body, function body, or other internal graph owns a `NodeState::Delay(DelayState)` ring in its containing `EvaluatorState`. It reads that ring during evaluation and stages its completed current operand. A `RecursiveDelay` uses the same local storage type but stages the enclosing stream's completed output.

## The running example

For `total = default(total[1], 0) + scaled`, `total[1]` is a `RecursiveDelay` because it reads the enclosing stream's own prior output. Tick 1 finds no committed local sample, so the delay produces `Value::Deferred` and `default` selects `0`; `total` then produces `8`. After the complete row succeeds, the recursive delay commits `total = 8` to its local `DelayState` ring. Tick 2 reads `8`, never the partially computed tick-2 value.

The bound graph makes the recursive representation and its post-output staging path explicit:

{{#include ../../assets/dataflow/evaluation-graph.svg}}

**Reading rule.** [Compilation](compilation.md) reads this figure for how binding places operations; read it here for the dashed path only. The recursive delay is consumed during the forward pass, but the value it will return next tick is not written when its node runs — it is staged, and becomes historical at the commit after the whole row. That gap between the solid and dashed paths is the temporal boundary this page is about.

## Delay state ownership

Monitor history is bounded per outer environment variable according to effective history requirements. It serves direct top-level external reads and can move destructively between compatible root monitors during context transfer.

Local `DelayState` belongs to the evaluator occurrence that implements the delay. Function call sites, recursive frames, and nested dynamic evaluators therefore have distinct temporal lifetimes even when they share immutable `StreamProgram` text. Positive offset determines local ring capacity.

New local state starts without samples unless compatible evaluator state is explicitly transferred, and returns `Value::Deferred` until enough activation-local samples have committed. Which replacements can donate that state is a property of the activation, not of history: see [dynamic properties](dynamic-properties.md#dynamic-activation-lifetime).

## Retained environment and monitor history

Reconfigurable evaluation also carries a retained sparse environment used to lift outer values needed by nested expressions. That row may retain `Value::Deferred` or the last available outer value, but it is neither `HistoryStore` nor a `DelayState` ring and does not backfill newly activated temporal operators.

## Which tier executes a delay

Everything above is canonical semantics, and it holds whichever tier runs the row. A delay is an ordinary scalar instruction, so a quickened region executes it over registers while its ring stays here, in the canonical arena. A native whole-schedule kernel can go further and commit the plan's temporal state itself, which makes it the one case where retention is owned outside this page's model until it is materialized back.

Neither changes when a value becomes historical: the commit is still the post-row barrier described above. [Fusion and the scalar IR](fusion.md) motivates the second representation, [fusion and regions](fusion.md#which-tier-can-run-a-fused-temporal-operation) says which tier can run a fused temporal operation, and [execution tiers](execution-tiers.md#whole-schedule-kernels) covers the kernels and how their state comes back.

## Failure and replacement

A failed tick performs no successful temporal commit and no monitor-history commit. The monitor then becomes terminal, so partially mutated non-temporal state is not reused by a later tick.

Root replacement moves local delay state only with semantically compatible mapped stream owners. Matching `HistoryStore` entries are destructively taken according to target requirements. Unmapped streams and histories start cold.

## Implementation mapping

The ownership split is implemented by `HistoryStore` and `HistoryAccess` in monitor history code, `NodeState::Delay(DelayState)` in evaluator state, delay binding in the compiler, and the common commit path in `DataflowMonitor` and `Evaluator`.

Continue with [language state](language-state.md) for other persistent operators and [context transfer](context-transfer.md) for replacement behavior.
