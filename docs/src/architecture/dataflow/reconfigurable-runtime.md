# Root cutover

A `DataflowRuntime` configured by `ReconfigurableDataflowRuntimeBuilder` handles one control request at a time in the same owner loop that drives data ticks. It first constructs a complete resource-free candidate, then applies input, output, and monitor changes in a fixed order. Application has no rollback.

## Planning

| # | Planning phase | Responsible entity | Produces or validates |
|---:|---|---|---|
| 1 | Validate request structure | `DataflowRuntime` | A structurally valid `ReconfigurationRequest`. |
| 2 | Parse and compile replacement | reconfiguration compiler | A candidate `DataflowProgram` without live state. |
| 3 | Resolve and plan input | `InputPipeline` | Candidate `ResolvedInput` and `InputPipelineReconfigurationPlan`. |
| 4 | Resolve and plan output | `OutputPipeline` | Candidate `ResolvedOutput` and `OutputPipelineReconfigurationPlan`. |
| 5 | Plan monitor replacement | `DataflowMonitor` | `MonitorReconfigurationPlan::RetainExact`, `InstallCold`, or `Transfer`. |
| 6 | Assemble candidate | `DataflowRuntime` | One complete `RuntimeReconfigurationPlan`. |

All six phases are resource-free. A failure leaves active owner structures unchanged, although `DataflowRuntime` treats the failed command as terminal.

The monitor plan is one of three forms:

- `MonitorReconfigurationPlan::RetainExact` keeps the healthy active monitor when the definition is exact and policy permits retention.
- `MonitorReconfigurationPlan::InstallCold` constructs initialized state when transfer is disabled or the active monitor cannot donate state.
- `MonitorReconfigurationPlan::Transfer` constructs a target monitor and a validated semantic mapping from the active definition.

## Application

| # | Application phase | Responsible entity | Applied effect |
|---:|---|---|---|
| 1 | Detach removed or changed input owners | `InputPipelineSession` | Stop new ingress while retaining already-admitted items for draining. |
| 2 | Drain admitted old input | `DataflowRuntime` and old `DataflowMonitor` | Evaluate retained items under the old definition and interface. |
| 3 | Flush pending engine rows | `DirectDataflowEngine` and `OutputWriter` | Submit output produced by the old side of the barrier. |
| 4 | Open additions and commit input | `InputPipelineSession` | Install added source owners and activate the candidate `ResolvedInput`. |
| 5 | Flush affected output ownership | `OutputPipelineSession` | Establish the required destination-local or shared-stage barrier. |
| 6 | Update output interfaces and selection | `OutputPipelineSession` | Apply supported interfaces, routes, codecs, and router variable sets sequentially. |
| 7 | Apply monitor replacement | `DataflowMonitor` | Retain, cold-install, or transfer semantic state according to the plan. |
| 8 | Rebuild transient row layouts | `DirectDataflowEngine` | Recreate reusable input/output rows and cached slot mappings around the active monitor. |
| 9 | Send acknowledgement | `DataflowRuntime` | Publish revision and change flags after all local application phases complete. |

Mutation begins at phase 1. Every later phase can fail after earlier effects have occurred, and acknowledgement is emitted only after phase 9 is reached.

{{#include ../../assets/dataflow/root-cutover-ticks.svg}}

**Reading rule.** The first dashed line is the locally delivered control barrier, not the instant at which all old work disappears. Removed or changed owners can still hold admitted batches; `DataflowRuntime` evaluates those logical ticks with the old `DataflowMonitor` and flushes their output before applying candidate input, output, and monitor state. The second dashed line marks local candidate activation after the serial input commit, output flush/update, monitor application, row rebuild, and acknowledgement; neither line is a rollback boundary.

The old monitor evaluates every row drained from removed sources. Its resulting pending output rows cross the writer boundary before the candidate input and output interfaces become active.

## Monitor replacement and revisions

Applying a monitor plan advances `MonitorRevision` for an accepted root activation, including exact retention. `InterfaceRevision` advances only when the effective input or output interface changed. The acknowledgement reports both revisions and both change flags, not the detailed transfer report.

After monitor application, the direct engine rebuilds reusable rows and cached slot layouts for the replacement monitor. This transient rebuild does not own language state.

## No root transaction

The cutover is serial but not atomic across subsystems. Examples of reachable partial state include:

- removed input owners already detached when an addition fails;
- input committed before output interface application fails;
- an earlier destination interface updated before a later destination fails;
- I/O changes applied before monitor transfer fails;
- the entire local cutover applied before acknowledgement delivery fails.

Cleanup runs on failure, but prior effects are not reversed. The runtime terminates rather than continuing from a partially applied command.

## Separate nested mechanism

`dynamic` and `defer` changes occur inside `DataflowMonitor::evaluate`. They resolve nested bodies and repair the active schedule before the tick's main range. They neither replace the root `InputPipelineSession` and `OutputPipelineSession` nor use this planning/application sequence.

## Implementation mapping

The implementation mapping centers on `plan_runtime_reconfiguration`, `apply_runtime_reconfiguration`, and `run_reconfigurable_dataflow` in `src/runtime/dataflow.rs`, with plans supplied by input, output, and monitor modules.

Continue with [replacement identity](replacement-contract.md), [context transfer](context-transfer.md), and [failure containment](failure-model.md).
