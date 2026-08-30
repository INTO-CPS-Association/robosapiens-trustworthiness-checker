# The reconfigurable runtime

[← Previous: Input and output boundary](runtime-io.md) · [Next: The replacement contract](replacement-contract.md) →

The reconfigurable dataflow runtime uses the same `DirectDataflowEngine`, packed `OutputBatch` path, and `OutputWriter` backpressure as the ordinary runtime. It adds one persistent owner loop that can change the monitor definition and its resolved I/O while the process keeps running.

At any instant the loop owns one live monitor and one live pair of sessions:

```text
one owner loop
  ├─ one active DataflowMonitor
  ├─ one InputPipelineSession
  └─ one OutputPipelineSession
```

`RuntimeSpec::ReconfDataflow(policy)` selects this implementation and carries its `ExecutionPolicy`. `RuntimeSpec::ReconfSemiSync` is a separate implementation in `src/runtime/reconfigurable_semi_sync.rs`; it has different execution and replacement semantics.

## The typed loop

Each iteration awaits the next item from the live `InputPipelineSession`:

| Item | Action |
|---|---|
| `ReconfigurableInputItem::Data(batch)` | Evaluate the batch with `DirectDataflowEngine`, submit output according to the selected policy, and refresh the monitor revision. |
| `ReconfigurableInputItem::Reconfigure(request)` | Resolve and apply a root reconfiguration, then continue with the live session state. |
| `None` | Flush and close the current output session, then return successfully. |
| `Err(error)` | Attempt output cleanup, then return the input error. |

The control item carries a parsed `ReconfigurationRequest`, not a model variable. The owner loop does not decode the transport payload.

## Resource-free planning

`plan_runtime_reconfiguration` is the first root-reconfiguration phase. It owns only the request and performs no transport or output I/O. Before the cutover awaits anything, it:

1. validates the request structure;
2. compiles the specification into an immutable `DataflowProgram`;
3. resolves the complete target `ResolvedInput` and `ResolvedOutput` values; and
4. asks `DataflowMonitor::plan_reconfiguration` for a `MonitorReconfigurationPlan`.

It produces a private `RuntimeReconfigurationPlan` holding the monitor plan and the concrete input/output pipeline plans. Nothing in the plan owns a resource, so a planning failure leaves the active runtime untouched.

The monitor result is a pure plan, not a stateful target monitor. Its variants are:

| Plan | Condition | Later action |
|---|---|---|
| `RetainExact` | The target `DefinitionKey` equals the active key and the active monitor is healthy under `MatchingStreamState`. | Retain the live monitor; do not materialize a target or create monitor context mapping. |
| `InstallCold { target }` | Transfer policy is `None`, or the active monitor has failed. | Materialize `DataflowMonitor::from_program(target)` with fresh state; skip monitor context mapping. |
| `Transfer { target, mapping, policy }` | The definition changed while transfer is enabled. | Materialize the target from its program, then apply the authoritative `ReconfigurationMapping` with `context_transfer_from`. |

`ReconfigurationMapping::between(active_program, target_program)` is created while both programs are immutable, before target monitor construction. It pairs same-name streams whose `StreamStateKey` values match.

## Root cutover order

Every accepted request plans both pipeline changes, including the possibility that both plans are empty. The owner applies a typed control item in this order:

```text
ReconfigurableInputItem::Reconfigure(ReconfigurationRequest)
→ plan_runtime_reconfiguration (program, monitor plan, input/output plans)
→ drain removed input streams through the old monitor
→ submit pending engine rows
→ retain unchanged input streams and install additions
→ flush only changed output owners (or one shared stage)
→ update existing output interfaces and routing
→ apply MonitorReconfigurationPlan and rebuild the monitor layout
→ send ReconfigurationAck
```

The durable output destination registry is fixed by `OutputPipeline`; a request
cannot create or remove an output owner. Existing owners must support in-place
interface updates. Input changes retain unchanged source streams, drain removed
streams, and open only additions; a changed source ID is break-drain-make.
Planning can fail
without mutating the monitor or opening resources. Once application begins, any
error is terminal: there is no rollback or replacement fallback. The row
submission and affected-owner flushes form the handoff boundary for output
already accepted by the engine.

Applying the monitor plan is where it becomes stateful. `RetainExact` keeps the live monitor's execution state. `InstallCold` and `Transfer` call `DataflowMonitor::from_program`; only the latter then performs the destructive context handoff. A failed root operation terminates the owner loop after its cleanup path; it does not restore a previous monitor by copying state back.

`DataflowMonitor` is the sole revision authority. Every accepted request advances `MonitorRevision`; `InterfaceRevision` advances only when the effective monitor or I/O interface actually changed. Acknowledgement is sent only after the monitor and both pipeline plans have been applied, and a failed acknowledgement is terminal.

## The input barrier remains live

The input window stage flushes pending data before forwarding `ReconfigurableInputItem::Reconfigure(ReconfigurationRequest)`. The control item is a barrier in the ordered item stream, not an end-of-stream marker: the replacement `InputPipelineSession` opened after the cutover continues to deliver data and further control items.

Active model-data bindings may span multiple source owners. Exactly one source carries the control route, and the session composes all active source streams into one observed order without claiming a total order between independent transports. Source moves use break-drain-make and therefore require external producer quiescence or transport replay during the subscription gap.

## Nested expression reconfiguration

Root replacement changes the whole `DataflowProgram` at a tick boundary. Nested `dynamic`/`defer` reconfiguration changes one body inside a tick, at the source barrier between the source and main execution ranges, while the surrounding `DataflowMonitor` remains in place:

```text
unchanged fast path
→ compile the local body
→ transfer compatible state from the old body
→ install the body once
→ update dependencies and schedule
→ advance MonitorRevision
```

The unchanged path keeps the active evaluator and its history. A changed source whose compiled body key matches the active body transfers that whole nested evaluator; otherwise the body installs cold. There is no transaction over a sequence of nested changes: a later failure may leave earlier nested changes applied, and the monitor then becomes failed according to the normal evaluation error policy. The first `defer` activation has no donor. Sealing and source release occur after the successful temporal commit.

## What reconfigurability costs

| Cost | When it applies |
|---|---|
| Retained environment row | Every reconfigurable monitor. |
| Current-row clear to `NoVal` | Every reconfigurable tick. |
| Source range and resolution barrier | While live points need computed prerequisites. |
| Whole-plan fused JIT blocked | While a non-empty source barrier exists. |
| Compilation, transfer, and schedule repair | When a source actually changes. |

Stable static and dynamic ticks retain their fast paths. Unchanged dynamic ticks and sealed `defer` ticks do not rebuild evaluators or schedulers. When all `defer` prerequisites have been released, the source range can become empty and JIT selection may promote the monitor to a fused artifact.

## Packed output batches carry no revision fence

Packed `OutputBatch` values are not revision-filtered. A nested change may advance `MonitorRevision` during a valid tick, but that tick's successfully computed output remains valid and is not discarded as stale.

## Transport support

| Transport | Reconfigurable? |
|---|---|
| Manual | Yes, subject to the source/control ordering contract. |
| MQTT | Yes, subject to feature and connection setup. |
| ROS | Yes, subject to feature and connection setup. |
| Redis Pub/Sub | Yes; Redis knowledge sources cannot carry control. |
| File | **No** |

File input has no live control route and cannot be used by the reconfigurable runtime. Independent MQTT topics still need external coordination when a stronger order than the transport's observed item order is required.

[← Previous: Input and output boundary](runtime-io.md) · [Next: The replacement contract](replacement-contract.md) →
