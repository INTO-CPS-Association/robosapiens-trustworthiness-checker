# Dataflow runtime ownership

[← Previous: Compilation](compilation.md) · [Next: Tick execution](tick-execution.md) →

The runtime preserves correctness by keeping semantic ownership separate from execution order. Immutable programs may be shared, persistent language state stays in a fixed evaluator arena, scheduling mutates dependency order, and an execution engine replaces only schedule-specific routing.

## Ownership at a glance


**What to notice.** `DataflowMonitor` coordinates several owners rather than placing all mutable concerns in one plan. `EvaluatorArena` remains fixed and owns one evaluator per stream, whose tier states hold canonical and evaluator-local derived state. `Scheduler` and `ReconfigurableExpressionState` evolve dependency control. `ExecutionEngine` may exchange `PlanBundle` values and schedule-wide fused artifacts without moving the arena; evaluator-local per-stream artifacts stay with their owning evaluators.

## Four categories of runtime object

| Category | Examples | May be replaced? | Owns canonical language state? |
|---|---|---:|---:|
| Immutable semantics | `StreamProgram`, `EnvironmentLayout`, `MonitorPlan` | Shared for the monitor lifetime | No |
| Persistent state | `EvaluatorArena`, `Evaluator`, `EvaluatorTierStates`, `EvaluatorState` | Only when the owning semantic lifetime ends | **Yes** |
| Mutable scheduling | `Scheduler`, `ReconfigurableExpressionState` | Mutated in place as active edges and sealed expressions change | No evaluator state |
| Replaceable routing | `ScheduledExecutionPlan`, `PlanBundle`, `QuickPlan`, fused JIT artifacts | Yes; active and cached routes may be exchanged | No canonical language state |

Fused routes may own compact scratch data or a native physical representation of temporal state. That representation is subordinate to stable semantic state identities and must be materialized before canonical fallback. Per-stream native state and artifacts instead stay in the owning `Evaluator.tier_states`; neither is an independent history model.

## `DataflowMonitor` is the orchestration owner

`DataflowMonitor` is the outermost owner *of semantics*, but it is not the outermost owner in the process. It is itself owned by a `DataflowRuntime`, which supplies the input, drives the ticks, and delivers the outputs; see [Runtime adapter](runtime-adapter.md). Nothing in this page's ownership model is affected by that, which is the point: the monitor is a self-contained synchronous machine.

`DataflowMonitor` owns the public machine boundary and the monitor-wide data needed to execute it:

- input and output variable order;
- the saved output-slot projection;
- stable stream names;
- `MonitorPlan`;
- `ReconfigurableExpressionState`;
- `Scheduler`;
- the current environment row;
- an optional retained environment row for reconfigurable monitors;
- `MonitorExecution`; and
- the terminal `failed` flag.

This division matters. `DataflowMonitor` decides phase order—load, source evaluation, resolution, schedule update, main execution, sealing, and output projection—but it does not itself own each stream's node state. That state lives under `MonitorExecution` in the evaluator arena.

The monitor plan remains immutable. The scheduler can consult its static dependency sets and the reconfiguration state can consult its fixed points and source closures, but neither modifies the compiled meaning of a stream.

## `MonitorExecution` contains stable execution state and replaceable routes

`MonitorExecution` is the bridge between monitor-level tick phases and physical execution. It owns:

- `EvaluatorArena`;
- the stable `StreamSlots` mapping;
- the fixed list of temporal streams;
- `ExecutionEngine`; and
- `tick_in_progress`, which enforces one source prelude per logical tick.

This is an intentional split inside one owner. The arena is schedule-independent and persistent. The engine is schedule-specific and replaceable. A schedule change updates the engine's active route while the same arena continues to supply evaluators by stable stream identity.

## `EvaluatorArena` owns one persistent evaluator per stream

At monitor construction, `EvaluatorArena::new` consumes the compiled program vector and creates exactly one `Evaluator` for each logical computed stream. The evaluator array remains indexed by `StreamId` for the monitor lifetime.

Each `Evaluator` pairs:

- an `Rc<StreamProgram>` containing immutable semantics; and
- `tier_states: EvaluatorTierStates`, containing:
  - canonical `EvaluatorState`;
  - optional quickening state;
  - optional `quick_plan`; and
  - with JIT, optional evaluator-local `native: JittedGraphEvaluator` state/artifact.

The arena's boxed evaluator collection is indexed by stable `StreamId`. The per-stream native tier is reached through its owning `Evaluator.tier_states`, not stored as a boxed array in the JIT coordinator.

Delay rings, lifted operands, branch timelines, persistent call evaluators, active runtime expressions, and deoptimization state all remain reachable through this stable evaluator. Reordering a schedule changes only which `StreamId` is visited next.

The arena also owns `published_scalars`, compact per-stream publication scratch used by quickened execution. This array is likewise indexed by stable `StreamId`; it supplements the canonical environment row rather than replacing canonical publication.

Top-level evaluator-owned `quick_plan` values are detached because schedule-specific published-source routing belongs to the active `PlanBundle` and its `QuickPlan`. Nested evaluators retain their `quick_plan`: they execute inside their owning stream evaluator rather than as independent top-level schedule entries.

## Stable IDs are ownership coordinates

The runtime uses several identities with deliberately different scopes:

| Identity | Scope | Stable meaning |
|---|---|---|
| `EnvironmentSlot` | Shared top-level layout, or one nested local layout | Location of an input, stream result, capture, or parameter in an environment row. |
| `StreamId` | One compiled monitor | Index of the persistent top-level evaluator and its canonical stream output slot. |
| `NodeId` | One evaluation graph | Index of an operation and the corresponding `node_values` and `node_states` entries. |
| `PlanStateSlot` | One scheduled semantic view | Pair of stable `(StreamId, NodeId)` identities used by execution tiers. |
| `PlanId` | One schedule-specific route | Identity of a particular ordered plan and source/main boundary, not of stream state. |

A schedule position is not an identity. Indexing persistent state by “the third stream in the current order” would attach state to replaceable routing and is therefore incorrect. The third schedule entry must first name a stable `StreamId`, which then selects the arena entry.

Likewise, `NodeId` is not globally unique. Nested branches, function bodies, and runtime-defined expressions have independent graphs and state trees. A useful top-level coordinate is `(stream, node)`, with ownership traversal providing the additional context for nested programs.

## Immutable semantics are safe to share

`StreamProgram` is reference counted because the same immutable program may be referenced by an evaluator, a scheduled plan, a function definition, or a cached dynamic-expression template. It contains no `EvaluatorState`.

`MonitorPlan` is also semantic metadata rather than an execution snapshot. Its stream slots, fixed dependency graph, reconfigurable-expression plan, and temporal stream set do not change when a `defer` seals or an active dynamic expression changes dependencies. Mutable `ReconfigurableExpressionState` records which expressions and source prerequisites remain live.

This distinction is particularly important for runtime-defined expressions. A cached `DynamicExpressionTemplate` may reuse an `Rc<StreamProgram>`, but reactivation after another source was active creates a fresh target `Evaluator`. Cache reuse does not restore an earlier activation; only owners matched to the immediately preceding active body can move into that target under the selected context-transfer policy.

## Mutable scheduling owns edges and order, not state

`Scheduler` starts from the static dependency graph and the currently live source-stream set. For reconfigurable streams it also owns the exact active dynamic producer sets and the workspaces used to repair order.

When a point's `dependency_slots` change, the containing stream's active producer union is rebuilt. The scheduler marks the order dirty only when required, then validates and repairs the order with static and dynamic edges together. A runtime cycle is rejected before main-range evaluation.

`ReconfigurableExpressionState` complements the scheduler by tracking:

- sealed `defer` points;
- pending post-tick releases;
- source-prerequisite reference counts;
- live-point reference counts;
- the current resolution-stream set; and
- the current source range and source order.

These objects control eligibility. They never move an `Evaluator`, rewrite a bound slot, or own a delay ring.

## `ExecutionEngine` owns replaceable routing

`ExecutionEngine` is the single plan-cache and tier-selection boundary. It owns:

- one active `PlanBundle`;
- up to four previous bundles;
- the next `PlanId`; and
- `Jit`, the coordinator for activation, execution-mode selection, fused scalar/temporal artifacts, and schedule-wide replay state.

A `PlanBundle` pairs an immutable backend-neutral `ScheduledExecutionPlan` with a derived `QuickPlan`. The semantic plan orders stable streams, records the source/main split, carries program references, maps outputs and state to stable slots, describes effects, and names the commit set. The quick plan partitions each range into `QuickStep::ScalarRun` and `QuickStep::Graph` entries.

When schedule ranges change, `select_schedule_ranges` first looks for an equivalent cached source/main order. A hit swaps bundles. A miss builds a new semantic plan and quick plan, assigns a new `PlanId`, moves the previous active bundle into the bounded cache, and notifies the JIT coordinator.

None of those operations reconstructs the evaluator arena. A schedule-cache hit is routing reuse, not state restoration.

![One semantic schedule with canonical, quickened, and native execution views](../../assets/dataflow/execution-layout.svg)

**What to notice.** All physical executors consume the same stable stream and state identities. Quickened routing and fused native artifacts may be schedule-specific, while evaluator-local per-stream native tiers remain with their owning evaluators. Canonical publication and the evaluator arena remain the common fallback boundary.

## Current and retained environment ownership

`DataflowMonitor` owns the canonical current environment row. Inputs occupy the initial slots; every computed stream publishes to its fixed stream slot. Same-tick dependencies ensure consumers read current producers after publication.

Reconfigurable monitors additionally allocate a retained row. At the start of each such tick, the current row is cleared to `NoVal`, inputs are loaded, and non-`NoVal` values update retention. Stream publication follows the same rule. Active nested expressions merge current-or-retained values only for the slots their template uses.

The retained row is not temporal history. It supports sparse outer-environment lifting and stores `Deferred` as a real value, but it cannot seed a newly activated delay ring. An active `defer`'s retained published result is a separate node-local value: it can republish a prior non-`NoVal` body result, but it is not copied into the outer row or the evaluator's history. Historical state remains evaluator-owned.

Static monitors allocate no retained row. They overwrite input slots and every computed stream slot on each tick, so the complete current row is refreshed without a preliminary clear.

## What may change without changing meaning

A runtime update may safely:

- replace the active stream order;
- move a stream between source and main ranges on a later tick;
- swap in a cached `PlanBundle`;
- derive a new quick plan;
- compile, disable, or replace native artifacts; or
- replace the nested evaluator at a `dynamic` activation point.

It must not accidentally:

- renumber top-level `StreamId` values;
- change an `EnvironmentSlot` assignment;
- move evaluator-local `EvaluatorTierStates` into a scheduled plan;
- store per-stream native artifacts in the JIT coordinator or index them by transient schedule position;
- share evaluator state merely because two call sites share a program;
- preserve an earlier dynamic activation through template caching rather than an explicit donor mapping; or
- allow native physical state to diverge from its canonical `(stream, node)` identity.

[← Previous: Compilation](compilation.md) · [Next: Tick execution](tick-execution.md) →
