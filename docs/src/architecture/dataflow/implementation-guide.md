# Implementation mapping

This page maps established architecture concepts and entities to the source and focused tests that implement them. It is a location and verification map, not a change procedure.

## Synchronous semantics

| Concept | Primary implementation |
|---|---|
| public synchronous machine (`DataflowMonitor`) | `src/dataflow/monitor.rs`, `src/dataflow/monitor/evaluation.rs` |
| compilation pipeline | `src/dataflow/compiler/` |
| immutable operations and `StreamProgram` values | `src/dataflow/ir.rs` |
| complete compiled definition (`DataflowProgram`, `MonitorPlan`) | `src/dataflow/program.rs`, `src/dataflow/monitor_plan.rs` |
| environment slots and row storage | `src/dataflow/environment.rs` |
| stream identity (`StreamId`, `StreamSlots`, `StreamSet`) | `src/dataflow/stream_id.rs` |
| current dependency scheduling (`Scheduler`) | `src/dataflow/scheduler.rs` |
| active expression lifecycle (`ExpressionActivationState`) | `src/dataflow/expression_activation.rs` |
| monitor history and its static requirements | `src/dataflow/monitor/history.rs`, `src/dataflow/history.rs`, `src/dataflow/history_requirements.rs` |

These pages are the architecture reference. `src/dataflow/mod.rs` states the module's own public contract and holds the executable `x`, `scaled`, `total`, `alert` example; where the two overlap, the book is authoritative and `mod.rs` links to it.

## Evaluator ownership and language state

| Concept | Primary implementation |
|---|---|
| persistent evaluator arena (`MonitorExecution`, `EvaluatorArena`) | `src/dataflow/execution/monitor_execution.rs` |
| canonical operation evaluation (`Evaluator`) | `src/dataflow/execution/node_evaluation.rs`, `src/dataflow/execution/evaluator/lifecycle.rs` |
| node values and operator state (`EvaluatorState`) | `src/dataflow/execution/evaluator_state.rs` |
| evaluator lifecycle façade | `src/dataflow/execution/evaluator.rs`, `src/dataflow/execution/evaluator/lifecycle.rs` |
| temporal staging and commit | `src/dataflow/execution/temporal_commit.rs`, `src/dataflow/execution/monitor_execution/tick.rs` |
| lifting | `src/dataflow/execution/lifting.rs` |
| functions and recursive frames | `src/dataflow/execution/functions.rs` |
| reconfigurable node queries | `src/dataflow/execution/evaluator/expression_state.rs` |
| nested environment projection | `src/dataflow/execution/environment_projection.rs` |
| nested dynamic/defer evaluators | `src/dataflow/execution/reconfigurable_expressions.rs`, `src/dataflow/execution/evaluator/reconfiguration.rs` |

Focused evaluator and monitor tests are in `src/dataflow/execution/evaluator/tests.rs`, `src/dataflow/execution/monitor_execution/tests.rs`, and `src/dataflow/monitor/tests.rs`.

## Physical execution

| Concept | Primary implementation |
|---|---|
| shared backend-neutral scalar IR (`ScalarProgram`) | `src/dataflow/execution/scalar_ir.rs` |
| region legality and island discovery (`ScalarRegion`) | `src/dataflow/execution/scalar_region.rs` |
| schedule-specific route (`ScheduledExecutionPlan`, `ExecutionPlan`) | `src/dataflow/execution/scheduled_plan.rs`, `src/dataflow/execution/monitor_execution/plan.rs` |
| quickened region execution and state handoff | `src/dataflow/execution/quickening/region.rs` |
| JIT coordination and artifacts | `src/dataflow/execution/jit/` |
| typed rows and the direct native entry | `src/dataflow/typed/` |
| native activation policy and report (`JitConfig`, `JitReport`) | `src/dataflow/jit_api.rs` |
| tier selection and state handoff | `src/dataflow/execution/monitor_execution/tiers.rs`, `src/dataflow/execution/quickening/region.rs` |

Differential and focused tests in these modules compare optimized behavior with canonical evaluation and exercise deoptimization and state materialization.

## Runtime and I/O boundaries

| Concept | Primary implementation |
|---|---|
| asynchronous row adapter and output buffering (`DataflowRuntime`, `DirectDataflowEngine`) | `src/runtime/dataflow.rs` |
| logical input batches | `src/core/input.rs` |
| input resolution and planning | `src/io/builders/input_stream_factory.rs`, `src/io/config/types.rs` |
| input windows | `src/io/aggregation.rs` |
| typed control and persistent input sessions (`ReconfigurableInputItem`, `InputPipelineSession`) | `src/io/reconfigurable_input.rs` |
| logical output batches and writer lifecycle | `src/core/output.rs` |
| output resolution, routing, sessions, and handoff (`OutputPipeline`, `OutputPipelineSession`) | `src/io/output/pipeline.rs` |
| per-destination delivery policy, queue, coalescing, barriers, and cleanup | `src/io/output/delivery.rs` |

Transport-backed integration coverage is in `tests/test_mqtt_io.rs`, `tests/test_redis_io.rs`, and `tests/test_ros_io.rs` under their feature requirements. General runtime integration is in `tests/runtime_tests.rs`.

## Root and nested replacement

| Concept | Primary implementation |
|---|---|
| root planning, application, and acknowledgement (`RuntimeReconfigurationPlan`) | `src/runtime/dataflow.rs` |
| monitor plan selection and application (`MonitorReconfigurationPlan`) | `src/dataflow/monitor/reconfiguration.rs` |
| semantic keys and revisions | `src/dataflow/reconfiguration.rs` |
| target-indexed stream/environment mapping | `src/dataflow/reconfiguration_mapping.rs` |
| execution-state transfer | `src/dataflow/execution/monitor_execution/reconfiguration.rs` |
| nested evaluator replacement | `src/dataflow/execution/evaluator/reconfiguration.rs` |
| separate semisynchronous replacement (`ReconfSemiSyncRuntime`) | `src/runtime/reconfigurable_semi_sync.rs` |

Mapping tests cover exact, changed, added, removed, and reordered streams. Evaluator tests cover warm exact nested activation, cold changed bodies, free-variable changes, and reported preservation. Runtime tests cover unchanged and changed stream sets, context transfer, disabled transfer, type errors, and builder policy propagation.

Return to the [dataflow architecture](index.md) or follow any focused page from its concept row above.
