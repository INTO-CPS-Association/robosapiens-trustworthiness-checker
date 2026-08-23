# Concept-to-code map

[← Previous: Failure and termination](failure-model.md)

The preceding pages describe the architecture. This one connects it to the source: which files implement each concept, a reading order for following one tick through them, and the properties that hold across the whole subsystem.

![Compilation and execution pipeline](../../assets/dataflow/pipeline.svg)

## Mental model

Keep five layers separate while reading or changing the runtime:

1. **Semantic program:** bound graphs, slots, operator effects, and canonical evaluator state.
2. **Dependency control:** static edges, runtime dynamic edges, source prerequisites, and the source/main split.
3. **Scheduled execution:** backend-neutral ordered plans and schedule-specific quickening steps.
4. **Physical acceleration:** compact scalar state and optional Cranelift artifacts with explicit fallback.
5. **Runtime orchestration:** asynchronous input acquisition, tick driving, output buffering, and — for the reconfigurable variant — replacement.

Most correctness bugs come from letting a lower layer silently redefine a higher one. In particular, plans do not own language state, scopes are not dependencies, and native temporal storage is only a physical representation of evaluator-owned history.

Layer 5 has the opposite failure mode: it must not redefine anything below it. Batching, buffering, windowing, and cutover change *when* work happens, never what a tick means.

## Concept-to-code map

### Compilation and semantic IR

| Concept | Primary files | Important types / functions |
|---|---|---|
| AST lowering and dynamic/defer nodes | `src/dataflow/compiler/lower.rs` | `lower_expression`, `lower_dynamic_expression`, `UnboundDynamicExpressionSpec` |
| Scope resolution, binding, nested restrictions | `src/dataflow/compiler/bind.rs` | `resolve_automatic_dynamic_scopes`, `restrict_dynamic_scopes`, `bind_graph`, `validate_persistent_function` |
| Compilation pipeline and static dependencies | `src/dataflow/compiler/pipeline.rs` | `LoweredDataflow`, `NamedDependencies`, `build`, `into_monitor` |
| Bound graph and program semantics | `src/dataflow/ir.rs` | `EvaluationGraph`, `BoundEvaluationGraph`, `StreamOp`, `StreamProgram`, `DynamicExpressionMode`, `ScalarSignature` |
| Stable variable storage | `src/dataflow/environment.rs` | `EnvironmentLayout`, `EnvironmentSlot` |
| Compilation/evaluation failures | `src/dataflow/error.rs` | `DataflowCompilationError`, `DataflowEvaluationError` |

`EvaluationGraph` node order and `NodeId` identity are canonical. Optimizers may derive instructions or SSA, but they must continue to map results and state back to those identities.

![Stable environment layout](../../assets/dataflow/environment-layout.svg)

### Monitor planning and dynamic control

| Concept | Primary file | Important types / functions |
|---|---|---|
| Stable stream IDs and slots | `src/dataflow/execution_plan.rs` | `StreamId`, `StreamSlots`, `StreamSet` |
| Static dependency graph | `src/dataflow/execution_plan.rs` | `DependencyGraph` |
| Source prerequisites and early-resolution validation | `src/dataflow/execution_plan.rs` | `ReconfigurationPoint`, `ExpressionSource`, `source_prerequisite_closure`, `ReconfigurationPlan::build` |
| `defer` sealing and prerequisite ownership | `src/dataflow/execution_plan.rs` | `ReconfigurationState`, `mark_defer_activated`, `apply_pending_releases`, `source_user_refcounts`, `live_point_refcounts` |
| Exact active dynamic edges and order repair | `src/dataflow/scheduler.rs` | `DynamicDependencyCollector`, `Scheduler`, `repair_scheduled_order`, `build_execution_schedule` |
| Tick orchestration | `src/dataflow/monitor.rs` | `DataflowMonitor`, `execute_tick`, `resolve_reconfiguration_points`, `apply_defer_sealing` |

`DataflowMonitor::execute_tick` is the best place to verify phase order. `ReconfigurationPlan` describes what can be resolved early; `ReconfigurationState` describes which points and prerequisite streams remain live; `Scheduler` merges static and active same-tick dependencies.

![Reconfiguration points](../../assets/dataflow/reconfiguration-points.svg)

### Dynamic-expression state

| Concept | Primary file | Important types / functions |
|---|---|---|
| Runtime parse/type/scope/bind | `src/dataflow/execution/dynamic_expressions.rs` | `compile_dynamic_expression` |
| Activation and replacement | `src/dataflow/execution/dynamic_expressions.rs` | `DynamicExpressionActivation`, `update_active_expression_with_change` |
| Exact slot classification | `src/dataflow/execution/dynamic_expressions.rs` | `CompiledDynamicExpression::{dependency_slots, environment_slots}` |
| Four-entry immutable template LRU | `src/dataflow/execution/stream_state.rs` | `DYNAMIC_EXPRESSION_CACHE_CAPACITY`, `DynamicExpressionState::{cached_template, cache_template}`, `DynamicExpressionTemplate` |
| Fresh evaluator per activation | `src/dataflow/execution/dynamic_expressions.rs` | `StreamEvaluator::new` in `update_active_expression_with_change` |
| Retained environment merge | `src/dataflow/execution/stream_state.rs` | `DynamicExpressionState::update_environment` |
| Point resolution without evaluating the nested expression | `src/dataflow/execution/stream_evaluator.rs` | `resolve_reconfiguration_point`, `reconfiguration_point_dependency_slots` |

The important split is `free_vars` versus `same_tick_free_vars`: the first populates the nested environment, while the second controls outer stream ordering.

### Canonical state and temporal semantics

| Concept | Primary files | Important types / functions |
|---|---|---|
| Per-stream ownership | `src/dataflow/execution/stream_evaluator.rs` | `StreamEvaluator`, `EvaluationContext` |
| Node values and persistent state | `src/dataflow/execution/stream_state.rs` | `StreamState`, `NodeState`, `DelayState`, `ScalarDelayState` |
| Canonical evaluation | `src/dataflow/execution/interpreter.rs` | `evaluate_nodes`, `try_evaluate_nodes`, `evaluate_node` |
| Stage/commit boundary | `src/dataflow/execution/interpreter.rs` | `stage_recursive_delays`, `commit_staged_temporal_state` |
| Retained top-level row | `src/dataflow/monitor.rs` | `environment_values`, `retained_environment_values`, `load_reconfigurable_inputs` |

State must stay with evaluators, not schedules. The monitor can replace a `PlanBundle`, but the evaluator arena and stable environment slots remain unchanged.

### Scheduled plans and quickening

| Concept | Primary file | Important types / functions |
|---|---|---|
| Backend-neutral plan contract | `src/dataflow/execution/scheduled_plan.rs` | `ScheduledExecutionPlan`, `PlanId`, `PlannedStream`, `PlanValueSlot`, `PlanStateSlot`, `PlanEffects`, `TemporalPlan` |
| Plan cache and range execution | `src/dataflow/execution/monitor_execution.rs` | `MonitorExecution`, `ExecutionEngine`, `PlanBundle`, `select_schedule_ranges` |
| Quick step partitioning | `src/dataflow/execution/monitor_execution.rs` | `QuickPlan`, `QuickStep::{ScalarRun, Graph}`, `build_quick_range` |
| Quick instruction selection | `src/dataflow/execution/quickening/plan.rs` | `Plan`, `SingleScalarPlan`, `Instruction`, `Source` |
| Quick execution and per-node deopt | `src/dataflow/execution/quickening/interpreter.rs` | `execute`, `execute_single`, `deopt_single` |
| Compact values and state | `src/dataflow/execution/quickening/scalar.rs`, `state.rs` | `ScalarValue`, quickening `State`, quickening `NodeState` |

A top-level evaluator's local quick plan is detached because `PlanBundle` owns schedule-specific published-source routing. Nested evaluators retain local plans because they execute outside monitor schedule steps.

### Native execution and replay

| Concept | Primary file | Important types / functions |
|---|---|---|
| Tier selection and artifact ownership | `src/dataflow/execution/jit/mod.rs` | `Jit`, `Activation`, `NativePlan`, `FusedTickOutcome`, `GraphTickOutcome` |
| Fused/per-stream runtime bridge | `src/dataflow/execution/jit/runtime.rs` | `JittedRunEvaluator`, `JittedTemporalRunEvaluator`, `JittedGraphEvaluator`, `JittedRunOutcome` |
| Temporal promotion and packed state | `src/dataflow/execution/jit/runtime.rs` | `NativeTemporalState::{promote, materialize}` |
| Scheduled scalar temporal operations | `src/dataflow/execution/jit/scheduled_state.rs` | `ScheduledTemporalPlan::{promote, evaluate, commit, materialize, deopt}` |
| Cranelift lowering and side-exit placement | `src/dataflow/execution/jit/backend.rs` | `Lowering`, `compile_run`, `compile_temporal_run`, `compile_graphs` |
| Whole-plan non-committing replay | `src/dataflow/execution/monitor_execution.rs` | `EvaluatorArena::replay_canonical`, `evaluate_canonical_run` |
| Per-stream replay | `src/dataflow/execution/jit/runtime.rs` | `fallback_current`, `replay_previous`, `evaluate_non_boundary` |

The JIT coordinator owns every compiled artifact. `MonitorExecution` sees only outcome enums and decides whether to return, replay, run canonically, or commit.

### Runtime orchestration and I/O

| Concept | Primary file | Important types / functions |
|---|---|---|
| Ordinary runtime and tick driver | `src/runtime/dataflow.rs` | `DataflowRuntime`, `DataflowRuntimeBuilder`, `run_direct_dataflow_engine`, `DirectDataflowEngine` |
| Input row assembly and packed output batches | `src/runtime/dataflow.rs` | `evaluate_tick`, `select_packed_layout`, `evaluate_packed_row`, `push_output_row`, `flush` |
| Flush policy and batch threshold | `src/runtime/dataflow.rs` | `ExecutionPolicy`, `DATAFLOW_RUNTIME_BATCH_SIZE`, `DirectDataflowEngine::flush` |
| Reconfigurable owner loop and cutover | `src/runtime/dataflow.rs` | `run_reconfigurable_dataflow`, `replace_root`, `ReconfigurationRuntimeState` |
| Output resolution and generation opening | `src/io/builders/output_backend_builder.rs`, `src/io/output/pipeline.rs`, `src/core/output.rs` | `OutputBackendBuilder`, `ResolvedOutput`, `OutputInterface`, `OutputBackendBuilder::resolve`, `OutputBackendBuilder::open` |
| Output drain and terminal cleanup | `src/runtime/dataflow.rs`, `src/runtime/output.rs` | `drain_previous_output`, `terminate_after_output_drain`, `finish_writer`, `OutputWriter::{flush, close}` |
| Acknowledgement barrier | `src/runtime/dataflow.rs` | `ReconfigurationAck`, `ReconfigurationAckSink`, `acknowledge_reconfiguration` |
| Typed control items and generations | `src/io/reconfigurable_input.rs` | `ReconfigurableInputItem`, `ReconfigurableInput::open`, `apply_barrier_stage` |
| Route resolution and single-source contract | `src/io/builders/input_stream_factory.rs` | `InputPipeline::resolve`, `resolve_reconfiguration_source`, `open_reconfigurable` |
| Window stages | `src/io/aggregation.rs` | `WindowEvent`, `drive_window`, `InputTimer` |
| Output backend construction | `src/io/builders/output_backend_builder.rs` | `OutputBackendBuilder`, `OutputPipeline`, `OutputDestination`, `try_build` |
| Runtime selection | `src/runtime/builder.rs`, `src/cli/args.rs` | `RuntimeSpec::{Dataflow, ReconfDataflow}`, `GeneralRuntimeBuilder::acknowledgements` |

The adapter owns no semantics. If a change here alters which values appear on an output stream or in what order, it is in the wrong layer.

### Replacement, transfer, and identity

| Concept | Primary file | Important types / functions |
|---|---|---|
| Shared replacement contract | `src/dataflow/reconfiguration.rs` | `validate_replacement`, `ReplacementTarget`, `ActivationFrontier`, `ReconfigurationError` |
| Portable addressing | `src/dataflow/reconfiguration.rs` | `RegionAddress`, `StateKey`, `DefinitionKey` |
| Semantic and interface history | `src/dataflow/reconfiguration.rs` | `RevisionId`, `InterfaceEpoch`, `checked_next` |
| Context snapshot and application | `src/dataflow/monitor.rs` | `DataflowContext`, `export_context`, `import_context` |
| Transfer mechanics | `src/dataflow/execution/stream_evaluator.rs` | `transfer_from`, `transfer_compatible_from`, `replace_reconfiguration_point` |
| Transfer policy and reporting | `src/dataflow/mod.rs` | `ContextTransferPolicy`, `ContextTransferReport`, `TransferDecision` |
| Effective interface comparison | `src/runtime/dataflow.rs` | `replace_root` compares compiled input/output variable sets with the active monitor's sets, and resolved candidate input/output with the active input/output state. |

`validate_replacement` is mutation-free by design; every caller supplies its own terminal policy. `import_context` is the opposite and poisons the replacement on failure.

## Reading the code

To follow one tick from semantics down to optimization:

0. **`src/runtime/dataflow.rs` module docs** — read the module header to see who calls the monitor, then skim `DirectDataflowEngine` to see what the adapter does and does not own.
1. **Architecture guide and `src/dataflow/mod.rs`** — read [the architecture overview](index.md) and [execution model](model.md), then use the concise module Rustdoc for the public API contract.
2. **`src/dataflow/monitor.rs`** — trace `evaluate` and `execute_tick`; write down the phase ordering.
3. **`src/dataflow/execution_plan.rs`** — inspect stable identities, source-prerequisite closure construction, and `ReconfigurationState` reference counts.
4. **`src/dataflow/scheduler.rs`** — follow an active dependency update through dirty detection and iterative topological repair.
5. **`src/dataflow/execution/dynamic_expressions.rs`** and **`stream_state.rs`** — follow source text through cache lookup, compilation, activation, fresh evaluator creation, and retained-environment update.
6. **`src/dataflow/execution/scheduled_plan.rs`** — identify the backend-neutral contract and temporal state slots.
7. **`src/dataflow/execution/monitor_execution.rs`** — follow `PlanBundle` construction, source/main quick steps, publication, commit, and fused fallback replay.
8. **`src/dataflow/execution/quickening/plan.rs`** then **`quickening/interpreter.rs`** — compare instruction selection with per-node deoptimization.
9. **`src/dataflow/execution/jit/mod.rs`** — verify selection order and artifact invalidation.
10. **`src/dataflow/execution/jit/runtime.rs`**, **`scheduled_state.rs`**, then **`backend.rs`** — trace promotion, native execution, side exits, replay, and generated commit placement.
11. **Collocated tests** plus `src/dataflow/tests.rs` — use differential and lifecycle tests to verify the inferred contract.
12. **`src/dataflow/reconfiguration.rs`** — read the shared replacement contract, then `export_context`/`import_context` in `monitor.rs`.
13. **`src/runtime/dataflow.rs` owner loop** — trace `run_reconfigurable_dataflow` and `replace_root`, noting where `drain_previous_output` falls relative to every fallible step.

Shorter routes: steps 2–7 cover `dynamic` and `defer`; steps 6–10 cover fused replay and the temporal JIT; steps 0, 2, 12, and 13 cover reconfiguration.

## Verification architecture

Tests in this subsystem are organised around the fact that canonical execution is the oracle for everything else.

| Test group | Location | Verifies |
|---|---|---|
| Differential compilation proptest | `src/dataflow/tests.rs` | Untyped, checked, recompiled, and stream-augmented monitors produce identical `(ok, output)` traces for the same generated rows, and both arity mismatches are reported before state advances. |
| Collocated tier tests | alongside each execution module | Quickened and native results match canonical results for the same programs and state progression. |
| Lifecycle tests | `src/dataflow/tests.rs` | Activation, replacement, template-cache reuse, `defer` sealing, and post-release schedule changes. |
| Reconfigurable dataflow tests | `src/runtime/dataflow.rs` tests | Focused coverage includes strict nested-transfer rejection and replacement behavior, including inheritance of the quickening setting. |
| Builder/manual transport tests | `tests/runtime_tests.rs` | Cover reconfigurable dataflow construction and the acknowledgement barrier, especially `test_general_builder_constructs_reconf_dataflow_for_supported_semantics`. |
| Transport integration tests | `tests/` | MQTT, ROS, Redis, and CLI behaviour against real transports, behind their features. |

These are focused tests rather than a reusable reconfiguration harness. The builder test uses manual transports and waits for a `ReconfigurationAck` before sending the next data row, exercising the producer acknowledgement barrier directly.

When adding a tier or an orchestration path, prefer extending the differential tests over writing a bespoke expected-output test. A hand-written expectation records what the new path does; a differential test records that it agrees with the oracle.

## Load-bearing invariants

Most of the detail in this guide follows from eight properties. If a change breaks one of these, it breaks the model rather than one path through it.

**Identity.** Every logical stream has one stable `StreamId`, one environment output slot, and one persistent evaluator. `NodeId` indexes the matching operation, value, and state. Plans, quick plans, and compiled artifacts own no canonical language state, so a schedule-cache hit or rebuild cannot reset it.

**Dependency order.** Static and active dynamic same-tick dependencies both order producer before consumer. A historical read creates no same-tick edge.

**One evaluation per stream.** Source and main ranges are disjoint and cover every logical stream exactly once. Reconfiguration resolves between them, after source publication and before any main-range state advances.

**One commit per row.** Evaluation reads only history committed before the current tick. Ordinary and recursive writes stage until the shared commit boundary, which a failed tick never reaches — unless a complete native artifact implements that same boundary internally.

**The two slot sets.** `environment_slots` holds all free variables; `dependency_slots` holds exactly the same-tick ones. The first populates a nested environment, the second orders outer streams. Conflating them either under-orders the schedule or over-approximates it.

**Tiers are unobservable.** `NoVal`, `Deferred`, lifting, and output publication agree across canonical, quickened, and native execution. Canonical state is complete before canonical execution resumes, and replay of an already successful native row neither stages nor commits its temporal writes again.

**One tick in, one row out.** One logical input tick produces exactly one `evaluate` call. A successful evaluation contributes exactly one value to every output stream; a failed one contributes to none. Flush policy changes timing only.

**One live definition.** The reconfigurable owner loop holds one monitor, one input generation, and one generation-specific `OutputWriter`; the reusable `InputPipeline` and `OutputBackendBuilder` are configuration, not opened resources. Draining the old writer by submitting pending rows and completing `flush`/`close` precedes every fallible replacement step.

## Continue reading

- [Execution model](model.md) for ticks, dependency order, environments, and stable evaluator identity.
- [Temporal state](temporal-state.md) for staging, commit, history ownership, and delay semantics.
- [Language state](language-state.md) for conditionals, functions, captures, calls, and recursion.
- [Dynamic properties](dynamic-properties.md) for activation, exact dependencies, template caching, sealing, and retained values.
- [Execution tiers](execution-tiers.md) for `PlanBundle`, quickening, JIT selection, artifact lifetime, temporal promotion, and replay.
- [Runtime adapter](runtime-adapter.md) for tick driving, packed output batches, writer backpressure, and shutdown.
- [Input and output boundary](runtime-io.md) for input generations, resolved output interfaces, generation-specific writers, and the flush/close handoff barrier.
- [The reconfigurable runtime](reconfigurable-runtime.md) for the owner loop and root cutover.
- [The replacement contract](replacement-contract.md) and [Context transfer](context-transfer.md) for safe activation points and surviving state.
- [Failure and termination](failure-model.md) for the containment ladder shared by every layer.

[← Previous: Failure and termination](failure-model.md)
