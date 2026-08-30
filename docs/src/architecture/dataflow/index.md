# Dataflow architecture

[Next: Execution model](model.md) →

The dataflow subsystem compiles synchronous stream equations into a machine that advances one coherent input row at a time. Its architecture is easiest to understand by separating four concerns: immutable meaning, persistent language state, mutable scheduling, and replaceable execution routing.

This page stays at that conceptual level. The following pages first establish the synchronous execution model, then descend through compilation, runtime ownership, and tick execution before examining temporal state, language state, dynamic properties, and acceleration tiers in detail. The later pages leave the synchronous core and cover the asynchronous runtime that drives it, in both its ordinary and reconfigurable variants.

The guide therefore spans three source areas: `src/dataflow/` for immutable programs and stateful monitors, `src/runtime/dataflow.rs` for the runtime that drives them, and `src/io/` for the boundary at which inputs and outputs are resolved and opened.

## Architecture at a glance


**What to notice.** Compilation fixes the meaning and identity of the machine. Runtime state remains attached to those identities across ticks. Scheduling may change the order in which streams run, and execution routing may select a different physical path, but neither is allowed to redefine the program or relocate its state.

## Four boundaries to keep separate

### Immutable semantics

Compilation turns each stream equation into an ordered expression program, resolves names to stable locations, records same-tick dependencies, identifies temporal work, and validates where runtime-defined expressions may appear. These artifacts describe what evaluation means; they are not the place where history or lifting state lives.

The outer stream set is fixed for the lifetime of a compiled monitor. Runtime reconfiguration can replace a nested expression and can reveal new same-tick edges, but it does not add outer streams, resize the environment, or create a new identity for an existing stream.

### Persistent state

Every logical stream has one long-lived evaluator. Stateful operations—delays, lifted operators, branches, calls, recursion support, and active runtime-defined expressions—retain their mutable state below that evaluator.

This ownership is semantic. Reordering streams must not reset a delay, exchange state between call sites, or revive the state of a previously replaced runtime expression. Shared immutable programs are safe; shared mutable evaluator state generally is not.

### Mutable scheduling

The scheduler answers one question: which logical stream may run next while preserving every active same-tick dependency?

For a fully static monitor, the answer is fixed after compilation. For a reconfigurable monitor, the scheduler combines fixed edges with the exact edges of currently active runtime expressions. It may repair its cached order when an active expression changes, but the identities being ordered remain unchanged.

### Replaceable routing

An execution route is a physical view of a valid schedule. It may partition work into interpreter steps, compact scalar runs, or native artifacts. Routes can be replaced or reused from a cache when schedule order changes.

A route is therefore disposable. It can refer to stable programs, value locations, and state locations, but it must not become the sole owner of canonical language state. Falling back to a more general executor must recover the same logical machine, not start a new one.

## Stable identity, replaceable order

The central architectural distinction is between **where something lives** and **when it runs**.

Three identity domains remain stable:

1. **Stream identity** selects the persistent evaluator for one computed stream.
2. **Environment identity** selects the current-row cell for an input or computed stream.
3. **Operation identity** selects an operation and its matching current value and persistent state within one expression program.

Execution order is a list over stream identities. Replacing that list does not move evaluators, environment cells, or operation state. Outputs are another stable projection over environment identities rather than an additional evaluation pass.

This rule is what lets dynamic dependency repair and execution-tier changes coexist with temporal semantics. History belongs to stable operations; the scheduler only decides when their containing streams are eligible to advance.

## Two tick shapes, one semantic contract

A static monitor has one computation range: load inputs, evaluate the dependency-valid stream order, commit temporal writes, and project outputs.

A reconfigurable monitor introduces a barrier:

1. evaluate the computed prerequisites needed to obtain runtime expression sources;
2. resolve active expressions and repair dependency order;
3. evaluate every remaining stream; and
4. commit once, then release any newly sealed source prerequisites for the next tick.

The source and main ranges are disjoint and together contain every logical stream exactly once. They are phases of one tick, not independent ticks. In particular, there is no temporal commit at the barrier.

## Two runtimes, one monitor

`DataflowMonitor` is a synchronous row function; it does not drive itself. Two runtimes wrap it, sharing one adapter and differing only in what they are allowed to replace while running.

| Concern | Ordinary runtime | Reconfigurable runtime |
|---|---|---|
| Builder | `DataflowRuntimeBuilder` | `ReconfigurableDataflowRuntimeBuilder` |
| Runtime spec | `RuntimeSpec::Dataflow(policy)` | `RuntimeSpec::ReconfDataflow(policy)` |
| Input | A caller-supplied `InputStream<Value>` | An `InputPipelineSession`; multiple source streams are retained, drained, added, and composed at the ordered barrier |
| Output | A caller-supplied `OutputWriter` | An `OutputPipelineSession`; fixed destinations update bindings/interfaces in place |
| Flush policy | Selected `ExecutionPolicy` | Selected `ExecutionPolicy` (CLI default `Buffered`; direct reconfigurable builder default `Synchronous`) |
| Executor | Accepted and ignored; the engine and writer are polled cooperatively in the caller's task | Required for opening the reconfigurable output pipeline and its worker-backed stages |
| Definition | Fixed for the process | Replaceable at a global command barrier |
| Failure scope | Engine or writer error ends the run | Additionally, any plan-application or acknowledgement failure terminates the owner loop |

Both variants use the same `DirectDataflowEngine`, the same packed `OutputBatch` representation, and the same `OutputWriter` backpressure path. The reconfigurable variant carries its selected `ExecutionPolicy` and adds a typed control item, resource-free planning, a serial cutover that applies incremental input/output plans, and context transfer — nothing about ordinary tick evaluation changes.

`RuntimeSpec::ReconfSemiSync` is a separate supported implementation in `src/runtime/reconfigurable_semi_sync.rs`. It shares neither this evaluator nor its failure policy and is not described by this guide.

## Correctness boundaries

Six boundaries hold regardless of which physical path executes a tick:

- **Row boundary:** one successful public evaluation publishes one coherent output row.
- **Dependency boundary:** a same-tick consumer runs after its current producer; a historical read observes only an earlier committed tick.
- **Publication boundary:** each logical stream publishes its current result once to its stable row location.
- **Commit boundary:** all temporal writes for the row become visible together after successful computation.
- **State boundary:** mutable language state stays with persistent evaluators, not schedules or cached routes.
- **Failure boundary:** a failed executing tick publishes no outputs, commits no temporal history, and ends the monitor.

These are semantic constraints, not descriptions of one interpreter. Any quickened or native path must preserve the same boundaries, including during deoptimization and replay.

## Reading this guide

Use the pages in this order for a top-down architecture review:

1. [Dataflow execution model](model.md) develops synchronous rows, absence, unavailability, and current versus historical dependencies without assuming implementation details.
2. [Compilation](compilation.md) follows source expressions through lowering, dependency discovery, binding, executable programs, monitor planning, and source-prerequisite closure construction.
3. [Runtime ownership](runtime-ownership.md) identifies which objects own immutable meaning, persistent state, mutable scheduling, and replaceable routes.
4. [Tick execution](tick-execution.md) compares static and reconfigurable phase order and explains commit, release, and terminal failure behavior.
5. [Temporal state](temporal-state.md), [Language state](language-state.md), and [Dynamic properties](dynamic-properties.md) examine the principal stateful semantics.
6. [Execution tiers](execution-tiers.md) explains canonical, quickened, and native physical execution.
7. [The dataflow runtime adapter](runtime-adapter.md) leaves the synchronous core and describes how ticks are actually driven, buffered, and delivered.
8. [Input and output boundary](runtime-io.md) defines input sessions, resolved output interfaces, request-specific writers, and the flush/close barrier at cutover.
9. [The reconfigurable runtime](reconfigurable-runtime.md) describes the serial owner loop, resource-free planning, incremental input/output application, and nested expression reconfiguration.
10. [The replacement contract](replacement-contract.md) defines semantic keys, activation timing, and semantic/interface identity.
11. [Context transfer](context-transfer.md) explains what state survives a replacement and why.
12. [Failure and termination](failure-model.md) assembles the containment ladder from node deoptimization to runtime termination.
13. [Concept-to-code map](implementation-guide.md) connects every concept above to the files and types that implement it.

## Map to the current implementation

| Architectural concept | Current implementation | Primary guide |
|---|---|---|
| Immutable compilation result | `DataflowProgram` | [Compilation](compilation.md) |
| Public synchronous machine | `DataflowMonitor::from_program` and `DataflowMonitor` | [Tick execution](tick-execution.md) |
| Immutable expression semantics | `StreamProgram` and `BoundEvaluationGraph` | [Compilation](compilation.md) |
| Fixed monitor structure | `MonitorPlan` | [Compilation](compilation.md) |
| Stable stream, row, and operation identities | `StreamId`, `EnvironmentSlot`, and `NodeId` | [Runtime ownership](runtime-ownership.md) |
| Persistent per-stream state | `EvaluatorArena`, `Evaluator`, `EvaluatorTierStates`, and `EvaluatorState` | [Runtime ownership](runtime-ownership.md) |
| Active dependency order | `Scheduler` and `ReconfigurableExpressionState` | [Tick execution](tick-execution.md) |
| Replaceable schedule-specific routing | `ExecutionEngine`, `PlanBundle`, and `ScheduledExecutionPlan` | [Runtime ownership](runtime-ownership.md) |
| Runtime-defined nested programs | `DynamicExpressionState` and its active `Evaluator` | [Dynamic properties](dynamic-properties.md) |
| Shared temporal visibility boundary | `evaluate_main_and_commit` and `commit_active_plan` | [Temporal state](temporal-state.md) |
| Physical acceleration | evaluator-local quickening/`JittedGraphEvaluator` tiers, `QuickPlan`/`QuickStep`, and Jit's fused artifacts | [Execution tiers](execution-tiers.md) |
| Asynchronous tick driver and packed output batches | `DataflowRuntime`, `DirectDataflowEngine`, `OutputBatch`, and `OutputWriter` | [Runtime adapter](runtime-adapter.md) |
| Input sessions and typed control | `InputPipeline`, `ReconfigurableInput`, and `ReconfigurableInputItem` | [Input and output boundary](runtime-io.md) |
| Resolved output interfaces and opening | `OutputBackendBuilder`, `ResolvedOutput`, and `OutputInterface` | [Input and output boundary](runtime-io.md) |
| Output flush, replacement, and terminal cleanup | `flush_reconfiguration_barrier`, `finish_dataflow_output`, `reconfiguration_failure`, and `finish_writer` | [Input and output boundary](runtime-io.md) |
| Serial root replacement and nested reconfiguration | `run_reconfigurable_dataflow`, `plan_runtime_reconfiguration`, `apply_runtime_reconfiguration`, and source-barrier installation | [The reconfigurable runtime](reconfigurable-runtime.md) |
| Safe activation points and semantic identity | `DataflowMonitor::reconfigure`, `DefinitionKey`, `StreamStateKey`, `MonitorRevision`, and `InterfaceRevision` | [The replacement contract](replacement-contract.md) |
| State carried across a replacement | `DataflowMonitor::context_transfer_from`, `ReconfigurationMapping`, and `ContextTransferPolicy` | [Context transfer](context-transfer.md) |
| Failure containment and terminal policy | `DataflowMonitor::failed` and owner-loop termination | [Failure and termination](failure-model.md) |

[Next: Execution model](model.md) →
