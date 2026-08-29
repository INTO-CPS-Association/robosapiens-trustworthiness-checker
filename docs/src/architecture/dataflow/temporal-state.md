# Temporal state

[← Previous: Tick execution](tick-execution.md) · [Next: Language state](language-state.md) →

Temporal operators turn a row evaluator into a state machine. Their central rule is simple: **the current row is computed before it becomes history**. This page explains that visibility boundary, then maps it to delay nodes and evaluator ownership.

For the surrounding tick and identity model, see [Dataflow execution model](model.md). Conditional and function state are covered in [Language state](language-state.md).

## History changes at the tick boundary

During a tick, every historical read must see the same past: the state committed by successful earlier ticks. If a delay wrote its current sample immediately, an expression evaluated later in the row could mistake a current value for a historical one.

The monitor therefore separates temporal work into two phases:

1. **Stage while evaluating.** Delays read existing history and record that a new sample must be captured. Staged samples are not yet visible through historical reads.
2. **Commit after the row.** Once all source and main computations have produced the current row, the monitor writes every staged sample into its owning history buffer. Those samples become readable on the next tick.

![Historical reads and the post-row commit](../../assets/dataflow/history-retention.svg)

This boundary governs temporal visibility only. A failed tick commits no staged delay writes, but lifting state and other node-local state may already have changed, which is why an execution-time error ends the monitor.

## Three related operators

### Ordinary delay

A positive delay such as `x[2]` reads the sample committed two successful ticks earlier. While evaluating, it reads its ring and marks a capture as pending. At commit, it resolves its operand against the completed environment row and pushes that current value into the ring.

This late operand read is deliberate. It allows the producer of a directly delayed stream to occur later in the current schedule, because the delay's current result does not depend on that producer's current value. A zero offset is different: it is a lifted current read and allocates no history ring.

A positive delay returns `Deferred` until its own ring is full:

| tick | current `x` | history before commit | `x[3]` |
| ---: | ----------: | :-------------------- | :----- |
| 1 | 10 | empty | `Deferred` |
| 2 | 20 | `[10]` | `Deferred` |
| 3 | 30 | `[10, 20]` | `Deferred` |
| 4 | 40 | `[10, 20, 30]` | 10 |

The ring stores samples exactly as observed, including `NoVal` and `Deferred`. Output lifting is applied when a stored sample emerges; it does not rewrite the history itself.

### Recursive delay

A guarded self-reference such as `total[1]` cannot capture its operand in the ordinary way: the value to retain is the enclosing stream's result, which is known only after the body finishes. It therefore behaves as a recursive feedback register:

- the forward pass reads previously committed output history;
- after the body result is known, that result is staged for the recursive delay; and
- the common post-row commit pushes the staged result.

Only a positive self-delay is accepted. Direct recursion and a zero-delay self-reference are same-tick recursion and are rejected during binding.

![Bound graph with recursive-delay staging](../../assets/dataflow/evaluation-graph.svg)

Ordinary and recursive delays share the same commit boundary. This permits mutually delayed streams to capture one another's completed current values without introducing a same-tick cycle or making evaluation order observable.

### Default

`default(input, fallback)` handles temporal unavailability; it is not itself a history buffer. It first applies sparse lifting to `input`:

- a new non-`NoVal` input becomes the retained input;
- `NoVal` reuses the retained input when one exists; and
- if no input has ever been observed, the effective value remains `NoVal`.

The fallback is selected only when that **effective input is `Deferred`**. A known value passes through, and `NoVal` without prior state remains `NoVal`. This is why `default(total[1], 0)` supplies `0` while the delay warms up but does not generally mean “replace every absent event with zero.”

`Default` retains only its last input for lifting. It does not stage a write and is not in the temporal commit set by itself.

## One commit across source and main execution

Reconfigurable monitors sometimes need computed streams to produce the source strings for `dynamic` or `defer`. Their execution is split into two ranges:

1. the **source prelude** evaluates exactly the prerequisite streams needed to obtain expression sources;
2. reconfiguration is resolved and the active dependency order is repaired; and
3. the **main range** evaluates every remaining stream exactly once.

These ranges are phases of one logical tick, not separate ticks. Beginning the source prelude marks a tick in progress. Temporal operators encountered there may stage writes, but no temporal state is committed between the ranges. After the main range succeeds, one commit traversal covers the active plan's complete temporal stream set.

Three consequences follow:

- a temporal source stream is not advanced again in the main range;
- main-range historical reads cannot observe source-range writes from the same tick;
- schedule repair happens before stateful main execution, so reordering never causes a stream to run twice; and
- when a sealed `defer` releases a source prerequisite into the main range on a later tick, its evaluator and history stay in place.

Static monitors use the same logical rule without a source barrier: evaluate the main schedule, then commit once. A native complete-tick plan may physically perform the commit itself, but it must preserve the same all-stream barrier.

## State belongs to the operation that needs it

There is no monitor-wide trace archive. State is distributed through persistent evaluators:

| Owner | Retained state |
| :---- | :------------- |
| Positive ordinary or recursive delay | Fixed-capacity ring, read/write position, fill count, lifted last output, and staged-write state. |
| Conditional branch | A separate nested state tree for each branch; commit descends into both. |
| Direct persistent function call | A nested evaluator and captures-plus-parameters environment; its staged writes join the enclosing commit. |
| Instantiated first-class temporal function | A call-site evaluator that owns and advances its own temporal state. |
| Active `dynamic` or `defer` expression | A nested evaluator, plus an outer-environment shadow maintained by the enclosing node; a replacement may receive explicitly mapped owners from its immediate donor, and active `defer` also has a separate retained published-result slot. |
| Other stateful/lifted operators | Only the last values and control flags required by that operator. |

The top-level evaluator stores current operation results separately from persistent operation state. Nested graphs repeat the same structure. Commit traversal follows ownership: it descends into both conditional states, persistent direct-call evaluators, and active runtime-defined evaluators that require temporal commit.

The environment shadow used by a runtime-defined expression contains current-or-lifted outer values so the nested evaluator can consume the current tick. It is not historical storage. Temporal operators in the runtime-defined body remain evaluator-local. The body's projected history requirements separately retain bounded outer-variable context for a later root reconfiguration; they do not backfill the active evaluator. For an active `defer`, the node's published-result slot is separate again: a body `NoVal` reuses the last non-`NoVal` result under outer lifting.

## History ownership during root reconfiguration

`DataflowMonitor` owns a `HistoryStore` for its effective positive history requirements. Those requirements combine static programs with active runtime-defined bodies after their slots have been projected into the current outer layout. Root reconfiguration uses the environment correspondence in `ReconfigurationMapping` to match live histories by variable identity, then destructively replaces the target history with the source owner and resizes it to the target depth. A deeper target does not invent older samples, a shallower target keeps only the target-visible suffix, and unmatched requirements start cold.

## Lifetimes and memory bounds

Direct positive delays in top-level stream programs can use one monitor history, sized to the maximum effective depth. Active runtime-defined bodies additionally contribute context-retention bounds, while their executing delay operators keep local rings. Positive delays of internal values and recursive outputs are also evaluator-local. Storage is therefore bounded by retained outer-variable context plus the local rings and small fixed state records of retained evaluators.

Important lifetime rules are:

- a statically compiled delay lives as long as its owning evaluator;
- a persistent branch or call evaluator keeps its delay rings across outer ticks;
- replacing a `dynamic` expression never restores state from its template cache; exact or uniquely mapped evaluator-local owners may move from the immediately preceding active body, while other owners are dropped and start cold;
- an activated `defer` keeps one evaluator timeline until its enclosing state is reset or dropped; and
- history retained by another equation is not shared with a new delay during ordinary execution; a root context handoff can move only explicitly mapped `HistoryStore` ownership.

A newly active runtime expression starts its own `x[k]` delay timeline unless that exact or compatible delay owner is moved from the immediately preceding active body. Monitor-retained context for `x` never backfills the nested delay. That retained context becomes observable when a root reconfiguration installs a new top-level specification whose matching variable history is transferred. This distinction preserves property-level evaluator semantics while allowing mapped top-level stream histories to continue across a root reconfiguration.

Offsets are converted to platform-sized indices and rings are allocated eagerly. The compiler currently exposes no configurable maximum history size, so a specification or runtime-defined expression can request a large allocation. Memory remains bounded for a fixed set of retained evaluators and offsets, but the chosen bound is input-dependent for `dynamic` expressions and can change when an evaluator is replaced.

## Implementation mapping

| Concept | Implementation |
| :------ | :------------- |
| Per-program state tree | `EvaluatorState`, with parallel `node_values` and `node_states` vectors indexed by `NodeId`. |
| Delay storage and staging | `NodeState::Delay(DelayState)` or its scalar equivalent. |
| Guarded stream feedback | Binding rewrites a positive delay of the current output to `StreamOp::RecursiveDelay`. The graph records its node IDs for post-output staging. |
| Stage during evaluation | `DelayState::read_and_stage_write` for ordinary delay; `stage_recursive_delays` after an enclosing result is known. |
| Commit after the row | `commit_staged_temporal_state`, reached through the monitor execution plan's temporal stream list. |
| Commit eligibility | `StreamProgram::requires_temporal_commit`, computed recursively through branches, direct calls, and reconfiguration nodes. |
| Split tick coordination | `MonitorExecution::evaluate_source_prelude` begins the tick; `evaluate_main_and_commit` finishes it and commits the active plan. |

Any new temporal construct must define all four parts explicitly: what it reads during evaluation, what it stages, when ownership is nested or persistent, and how the commit traversal reaches it.

[← Previous: Tick execution](tick-execution.md) · [Next: Language state](language-state.md) →
