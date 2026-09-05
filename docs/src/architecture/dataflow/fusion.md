# Fusion and regions

Canonical evaluation is defined node by node. Every node reads its operands from the arena, dispatches on the `Value` it finds there, and writes its result back for the next node to read. That is the semantics [canonical execution](index.md#canonical-execution) guarantees, and it is also the cost: a tick pays for a dispatch and two arena accesses per node, however simple the operation.

Most of that cost is avoidable. In the synchronous dataflow tradition, a set of equations that has been scheduled into a dependency-valid order can be compiled into straight-line code over machine values, rather than interpreted as a graph. Lustre compilers have generated sequential step functions this way since the language's early implementations, and the clock-directed code generation of Biernacki et al. shows how a clocked set of equations becomes modular sequential step code.

This layer does the same thing for a `DataflowMonitor`. **Fusion** groups streams the schedule has placed next to each other into a single unit of execution, and the **scalar IR** is the one representation those units are expressed in, so that the quickened interpreter and the native backend agree on what they are running.

## A larger example

The [running example](index.md#the-running-example) is too small to fuse more than once. This page uses a companion specification, which is not part of that scenario:

```dsrv
in x: Int
in y: Int
in flag: Bool
in lbl: Str
out scaled: Int
out offset: Int
out merged: Int
out blended: Int
out delta: Int
out ratio: Int
out total: Int
out echoed: Str
out level: Int
out alert: Bool
scaled  = x * 2
offset  = y + 5
merged  = scaled + offset
blended = default(blended[1], 0) + (if flag then merged else scaled)
delta   = blended - merged
ratio   = delta * 3
total   = default(total[1], 0) + ratio
echoed  = lbl
level   = total + ratio
alert   = level > 20
```

Ten computed streams. Seven are static arithmetic; `blended` and `total` carry a recursive delay; `echoed` carries a `Str`.

*Scalar* means a value with a fixed unboxed representation, one the machine can hold in a register rather than behind a pointer. That is the property fusion depends on. Which types currently have such a representation is a narrower question: the type checker admits `Int`, `Float` and `Bool`, while `Str`, `List` and `Map` do not, so `echoed` is not fused today.

The durable part is that the question is asked of the checked type, before any row arrives, rather than of values as they go past.

## Fusing a scheduled graph

{{#include ../../assets/dataflow/fusion-partition.svg}}

**Reading rule.** The upper view is the dataflow graph, the lower one the scheduler's order, and a numbered badge marks the same region in both. Eligibility is a property of a stream on its own — `scaled` is static and scalar wherever it appears. Adjacency is not: it exists only once the scheduler has committed to an order. A region needs both, which is why the graph alone cannot be partitioned.

A dataflow graph fixes precedence, not sequence. It says `merged` must follow `scaled` and `offset`; it does not say which of the ten streams runs third. Until something chooses, no two streams are neighbours and "a contiguous run" names nothing.

The [scheduler](scheduling.md) supplies that order, and the partition follows from it. A **stream region** is a maximal contiguous run of eligible streams: `scaled`, `offset` and `merged` become one, as do `delta` and `ratio`, and `level` and `alert`. A stream that cannot join one becomes a **graph step** of its own and ends the run it interrupts, which is why the ten streams become six steps rather than two.

## Inside a graph step

A graph step still runs fused code. Fusion applies inside it to node ranges rather than to whole streams, and those ranges are called **islands**.

{{#include ../../assets/dataflow/graph-step-islands.svg}}

**Reading rule.** These are the four nodes of `blended` in graph order. A conditional is not an island operation, so node 2 splits the graph into two islands rather than disqualifying it. Node 3 takes one operand from a register, because node 1 is consumed only by the other island of the same region and so is never written to the arena, and the other from the arena, because a canonical node produced it. Only node 3, the stream's result, is exported.

`blended` therefore runs as scalar code, a canonical node, and scalar code again, in that order, writing two node values to the arena instead of four. `total` has no canonical node at all: its delay, default and addition are all island operations, so its whole graph is one island. `echoed` is the opposite case, a `Str` stream with no scalar form, and its graph step contains no island at all.

## Stream regions and graph regions

A stream region is `ScalarRegion::Streams`, and its members are whole streams, each publishing its result to an environment slot. Discovery lowers each candidate once and assembles the accepted prefix into one region, so the quickened plan is built once rather than per member.

A **graph region** is `ScalarRegion::Graph`: *all* the scalar islands of a single stream whose graph is otherwise canonical, separated by canonical node runs. They form one region rather than one region per island so that a value produced by an earlier island reaches a later one in a register, instead of round-tripping through the canonical arena.

Two different predicates decide these two things, and a conditional separates them. `is_island_node` segments a graph, and does not accept an `If`: inside a graph, a conditional ends an island run, which is what splits `blended` above. `supports_program` decides whether the engine can run a finished program, and does accept `EagerSelect`. So a whole stream whose graph is one conditional is admitted as a stream-region member, while a conditional buried inside a longer graph still breaks that graph into islands.

`GraphIsland::exports` names only the nodes that *canonical* execution still reads. A node consumed solely by a later island of the same region is never exported — it stays in a register. `segment_stream_graph` returns the islands together with a `GraphSegment` sequence that alternates `Island` with `Canonical` node runs in graph order.

## Why the two scopes decline differently

A stream region checks its whole input set before executing anything and declines the row as a unit. It can do this because all of its inputs exist before the region starts.

A graph region's members cannot. A member's boundary inputs are canonical node values that do not exist until the canonical run before it has finished, so there is no moment at which the region as a whole could preflight. Each member therefore checks its own boundary and may decline this row alone; the declined range is evaluated canonically and then synchronized back, so the members after it still read live registers.

A stream region therefore preflights as a unit, while a graph region preflights one member at a time. Neither scope abandons an instruction range after partially executing it.

## Temporal operations, and temporal streams

The word *temporal* answers differently at two levels.

**A temporal operation is fusable.** `Delay`, `RecursiveDelay` and `Default` lower to `ScalarSsaInstruction::Temporal`, and `supports_program` accepts them. They are ordinary instructions: the engine dispatches on the operation, and the result lands in a register like any other. An ordinary temporal stream is therefore one island covering its whole graph, not a set of islands arranged around its temporal nodes.

**A temporal stream does not join a stream region.** That follows from what a stream region currently is, not from anything about delays: its members publish to environment slots and are preflighted as a unit, and a stream carrying temporal state does not fit that contract. Such a stream becomes a `Graph` step of its own, and what is fused is the inside of that step.

So `total = default(total[1], 0) + ratio` is never a member of a stream region, and is nonetheless fused end to end.

### What the operation keeps, and where

Executing a temporal instruction on a different tier does not move its state:

| Part | Where it lives |
|---|---|
| the operation's result this tick | a register in the executing region |
| the retained ring | the canonical arena, which the region borrows |
| the write that makes this tick's value historical | the ordinary post-row [tick barrier](tick-execution.md#tick-and-source-barriers) |

### Which tier can run a fused temporal operation

Being fusable does not mean every tier will run it, and the two consumers differ here more than anywhere else on this page.

**Quickening runs them.** `supports_program` accepts `Temporal`, so a temporal stream's island is executed by the scalar engine over registers, reading and staging through the canonical arena.

**Per-region native compilation does not see them.** `ScalarRegion::programs()`, which is what the native backend compiles from, yields whole-stream programs and nothing for a `Graph` region. Islands read and write the canonical arena, and the region ABI does not expose it, so `NativeExecution::Regions` covers stream regions only. A temporal stream is always a graph step, so this route never reaches one.

**A whole-schedule kernel does.** `NativeExecution::WholeTemporal` compiles a complete static schedule, temporal streams included, into one function that also commits the plan's temporal state. That is the only route by which a delay executes as native code, and the only case in which temporal state is native-owned until it is materialized back. [Execution tiers](execution-tiers.md#native-execution) covers the guard and fallback rules that come with it.

The asymmetry is a property of the region ABI as it stands, not of temporal operations: an island cannot be compiled because it needs the arena, not because it holds a delay.

`NodeState` remains the single implementation of delay, recursion and default semantics. A quickened or native tier reads and stages through it rather than keeping a copy, which is why a region can be discarded, rebuilt for a repaired schedule, or replaced by another tier without any temporal state moving. It is also why [`synchronize`](execution-tiers.md#who-owns-a-regions-state) is fallible rather than infallible: a region whose temporal state the scalar domain cannot currently express declines, and canonical execution stays authoritative. The fallibility is the design commitment; which states fall outside is a property of the domain as it stands.

## What fusion changes, and what it preserves

A fused region runs its members over one register file and touches the arena only where a value must outlive it. It does not change the answer. The same streams are evaluated in the same order, the same environment slots are published, temporal state stays in the canonical arena, and a failure is reported at the same point. [Execution tiers](execution-tiers.md) states that contract and names who enforces it.

Fusion is also decided once, not per row. Eligibility comes from the type checker's `ScalarSignature`, not from watching values at runtime, so a region either exists in the plan or does not.

## Implementation mapping

- region legality, the two scopes, island discovery, and `segment_stream_graph`: `src/dataflow/execution/scalar_region.rs`;
- partitioning a schedule into regions and steps: `src/dataflow/execution/monitor_execution/plan.rs`;
- the delay algorithm shared by `Value` and scalar representations, `DelayState<T>` over `DeferrableStreamData`: `src/dataflow/execution/evaluator_state.rs`.

`documented_fusion_example_partitions_into_six_steps` in `src/dataflow/execution/monitor_execution/tests.rs` pins the partition and the segment counts both figures on this page show. Focused tests in `src/dataflow/execution/quickening/tests.rs` cover island discovery, register passing between islands, and per-member declining; `a_temporal_stream_quickens_as_one_region` pins the claim that delays do not split a graph, and `history_backed_delays_promote_into_the_fused_temporal_kernel` in `src/dataflow/execution/jit/backend/tests.rs` pins the whole-schedule route.

Continue with [the scalar IR](scalar-ir.md), which gives the representation every region is expressed in.
