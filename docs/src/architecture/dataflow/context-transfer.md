# Context transfer

[← Previous: The replacement contract](replacement-contract.md) · [Next: Failure and termination](failure-model.md) →

A root replacement compares two immutable `DataflowProgram` values. The outgoing `DataflowMonitor` owns the live evaluator state; the program values own only bound semantics, monitor planning, layout, history requirements, and `DefinitionKey` identity.

`plan_runtime_reconfiguration` compiles the requested definition and asks the active monitor for a `MonitorReconfigurationPlan`. The plan is pure. It is accompanied by the complete resolved input and output, and it does not create a stateful target monitor.

## The three monitor plans

`MonitorReconfigurationPlan` has exactly three variants:

| Plan | When | What the root cutover does |
|---|---|---|
| `RetainExact` | The target `DefinitionKey` equals the active key and the active monitor is healthy under `MatchingStreamState`. | Keeps the live `DataflowMonitor`; no target monitor and no monitor context mapping are materialized. |
| `InstallCold { target }` | The policy is `None`, or the active monitor has failed. | Calls `DataflowMonitor::from_program(target)` and installs a fresh monitor with initialized state. Monitor context mapping is skipped. |
| `Transfer { target, mapping, policy }` | The definition changed while transfer is enabled and the active monitor is healthy. | Builds a target monitor from `target`, then applies the supplied mapping and policy. |

`None` skips **monitor** context mapping, but the runtime still resolves the complete candidate input and output interfaces. A failed active monitor never donates state: its replacement is installed cold even when its definition key happens to match the target.

`ReconfigurationMapping::between` analyses the two immutable programs before `DataflowMonitor::from_program(target)` is called. It is an internal, target-indexed correspondence for streams, executable evaluator-owner moves, and environment slots. History is deliberately not represented by a second static mapping: effective history requirements can come from transferred active expressions and are known only from monitor state.

## Destructive handoff

Root replacement has separate preparation and application phases. `prepare_context_transfer` checks tick state, mapping structure, active-expression projections, dependencies, schedule viability, retained-row layout, and the transfer report without moving persistent owners. It returns a private `PreparedContextTransfer` containing the validated mapping, candidate scheduler and reconfiguration state, and any executable environment projections.

`DataflowMonitor::context_transfer_from` consumes that prepared value at the tick barrier. This application path has no recovery branch: it moves evaluator owners, installs prepared projections, moves retained values and histories, and publishes the prepared scheduler and control state. The source monitor is consumed by the root cutover; state moves directly between monitor owners.

The handoff is valid only between ticks. If either monitor has a tick in progress, preparation returns `DataflowStateError::TickInProgress`. A matched stream moves its complete `Evaluator.tier_states` (`EvaluatorTierStates`) as one aggregate, so canonical `EvaluatorState`, optional quickening state, `quick_plan`, and evaluator-local `JittedGraphEvaluator` state/artifact cannot separate. There is no node-level rewriting: a stream either moves whole or starts cold. Schedule-owned plans, JIT coordinator activation, fused artifacts, and schedule-wide replay state remain target-owned.

## Mapping rules

`ReconfigurationMapping` is indexed by target dense identities, while correspondence is established semantically:

- Top-level streams are matched independently, by variable name. A pair whose `StreamStateKey` values are equal is recorded as `StreamMapping::Exact`; every other target stream is `StreamMapping::Unmapped` and starts cold.
- There is no compatible node-level rewriting. A changed stream has no partial correspondence to exploit.
- Environment slots map by variable name, not by equal slot number, so unrelated declarations do not disturb a match.
- Active reconfigurable-expression state and dependencies are restored only for matched stream owners.
- Each transferred active body keeps the environment layout against which its nested evaluator was compiled. A prepared `EnvironmentProjection` maps the body's nested slots to current outer slots by variable identity and stores projected dependency and history slots.

The result is reported per stream with `StreamStateTransferOutcome::Transferred` or `StreamStateTransferOutcome::Initialized`.

## History ownership

Each monitor owns a `HistoryStore`. Before history movement, the target recomputes effective requirements from its static program plus the projected requirements of transferred active bodies. The environment part of `ReconfigurationMapping` then identifies the same variable in the source monitor, and the handoff moves the actual live history owner when both sides have a useful binding.

History is matched independently of stream-state matching, by variable name **and** declared type, so a stream that starts cold can still consume retained input history. The moved history is resized to the target's effective depth. A shallower target keeps only the most recent target-visible values, while a deeper target preserves the available suffix without inventing older samples. Static programs and active bodies can contribute to one context-retention bound whose depth is their maximum. Active bodies still execute their own evaluator-local delays; the shared history exists so a later root specification can reuse the bounded context. Unmatched requirements remain cold.

A history whose effective depth becomes zero is retired immediately: its binding is removed and its storage is dropped. If a later specification needs that variable again, its history is reallocated then.

History ownership is separate from the retained current-row environment used by reconfigurable expressions. Context transfer materializes and resets evaluator-local native state, while each compiled per-stream artifact remains bound to its target evaluator's program and environment ABI.

## Transfer reports

`ContextTransferReport` reports at stream granularity. `streams` holds one `StreamStateTransfer` per target stream, naming the variable and whether its state was `Transferred` or `Initialized`; `retained_history` names the variables whose history survived.

Nested bodies do not appear in the root report. A nested body's fate is decided one level down by the same key comparison, so the root report stays a flat, readable list of stream names rather than a tree of owner paths.

[← Previous: The replacement contract](replacement-contract.md) · [Next: Failure and termination](failure-model.md) →
