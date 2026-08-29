# The replacement contract

[← Previous: The reconfigurable runtime](reconfigurable-runtime.md) · [Next: Context transfer](context-transfer.md) →

Root replacement and nested `dynamic`/`defer` reconfiguration happen at different boundaries, but both validate identity and timing before changing state. Root replacement starts from an immutable `DataflowProgram`; nested reconfiguration changes one active body inside an existing `DataflowMonitor`.

`src/dataflow/reconfiguration.rs` defines the stable identities and report values. Dense `StreamId`, `EnvironmentSlot`, and `NodeId` values remain local to a compiled program and are not treated as portable names.

## Semantic identity

Two precomputed 128-bit fingerprints carry semantic identity. Both are derived once, from a canonical descriptor, and compared as opaque values afterwards.

| Key | Scope | Includes |
|---|---|---|
| `DefinitionKey` | A whole `DataflowProgram`. | Input/output/stream structure and every stream's canonical bound graph descriptor. |
| `StreamStateKey` | One `StreamProgram`. | The bound graph: operators, referenced variable names and declared types, temporal offsets, nested bodies, captures, and `dynamic`/`defer` metadata. |

Neither key includes schedule order, quickening, JIT settings, environment slot numbers, or any other derived layout. Two programs that differ only in unrelated declarations therefore keep matching `StreamStateKey` values for the streams they share.

`DataflowMonitor::plan_reconfiguration` compares the target `DefinitionKey` with the active key and also checks whether the active monitor is healthy. An identical healthy target under `MatchingStreamState` returns `RetainExact`; a changed target returns `Transfer` with a `ReconfigurationMapping`; a failed active monitor or `None` returns `InstallCold`. The decision is made from the immutable target program and does not build a stateful target monitor.

## Safe boundaries

Root replacement and nested reconfiguration are admitted at different points, and both are internal to the monitor:

- `DataflowMonitor::reconfigure` applies a whole-program replacement between ticks. It is the only public entry point; there are no separately addressable editor or barrier values.
- A reconfigurable expression is installed between the source and main execution ranges of the tick that produced its new source text, before its owner executes.

A root replacement protects the whole monitor; a nested installation protects only one owner. A nested installation cannot replace the root monitor, and a root replacement cannot be applied while a tick is in progress.

## Validation and revisions

`plan_runtime_reconfiguration` validates the request, compiles it, and resolves both interfaces without opening any resource. Structurally invalid input is rejected there. The runtime then applies the already-resolved plan; the monitor is the sole revision authority, and requests do not carry a base revision.

`MonitorRevision` and `InterfaceRevision` describe different histories:

| Identity | Records | Advances when |
|---|---|---|
| `MonitorRevision` | Successful monitor and nested semantic installation history. | Once for each accepted root request, including an exact definition-only request; also when a nested body changes successfully. |
| `InterfaceRevision` | Effective external input/output binding history. | The effective input or output bindings change. |

`ReconfigurationReport` and `ReconfigurationAck` expose `monitor_changed` and `interface_changed` separately from the revision values. Therefore an exact root command can retain the live monitor, report `monitor_changed = false`, and still install the next `MonitorRevision` while leaving `InterfaceRevision` unchanged. Revision overflow is a terminal error rather than a reused identity.

## Transfer reporting

`ContextTransferReport` reports at stream granularity only. It has a deterministic `streams` list of `StreamStateTransfer` values and a `retained_history` list of variable names:

| `StreamStateTransferOutcome` | Meaning |
|---|---|
| `Transferred` | The stream's evaluator state moved across from the matching active stream. |
| `Initialized` | The stream starts cold. |

Nested bodies do not appear in the root report; their transfer is decided by the same key comparison one level down.

`ContextTransferPolicy::None` clears evaluator state and variable history. `ContextTransferPolicy::MatchingStreamState` transfers a whole evaluator state when the target stream's name and `StreamStateKey` both match an active stream, and otherwise starts that stream cold. Retained history is matched independently, by variable name and declared type. The transfer mechanics and history rules are described in [Context transfer](context-transfer.md).

[← Previous: The reconfigurable runtime](reconfigurable-runtime.md) · [Next: Context transfer](context-transfer.md) →
