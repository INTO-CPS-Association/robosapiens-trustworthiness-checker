# Input and output boundary

[← Previous: The dataflow runtime adapter](runtime-adapter.md) · [Next: The reconfigurable runtime](reconfigurable-runtime.md) →

The ordinary runtime consumes an `InputStream<Value>` and an opened `OutputWriter`. The reconfigurable runtime retains an `InputPipeline` and an `OutputBackendBuilder`, then opens a live `InputPipelineSession` and `OutputPipelineSession` around their resolved plans.

This page describes how those plans and sessions are resolved, opened, and updated, and how a typed control item reaches the owner loop.

## Resolutions and sessions

`InputPipeline` and `OutputBackendBuilder` are reusable configuration; `ResolvedInput` and `ResolvedOutput` describe one active binding set. An `InputPipelineSession` and an `OutputPipelineSession` are the opened resources for one such pair.

Every accepted reconfiguration replaces both sessions completely, including a request that changes neither interface. Opened resources never overlap: the old output is flushed and closed and the old input is dropped before the replacement is opened.

```text
active InputPipelineSession + OutputPipelineSession
                  │
                  ├─ flush and close old output, drop old input
                  └─ open complete replacement input and output
```

## Input resolution and control binding

`InputPipeline` is reusable configuration: registered sources, route catalogs, codecs, and window stages. It opens no source until a `ResolvedInput` is passed to an open method.

`ReconfigurableInput` wraps the pipeline with one validated `ReconfigurationControl`. Construction resolves the selected control source and route; `open_session` then opens that source and creates an `InputPipelineSession` holding the active resolution and its control-aware stream.

`InputPipeline::resolve` is pure. It resolves the model's current input variables against an optional `InputConfiguration` and attaches the pipeline identity and configuration fingerprint to the resulting `ResolvedInput`. Root reconfiguration calls it before any replacement resource is opened.

Only the selected control source is opened for a reconfigurable input. A resolved model with no bindings may use the control source alone. If model bindings exist, they must belong to the same source as the control route; a multi-source live input is rejected because data and control need one source-owned ordered stream.

## The typed control boundary

A reconfigurable input uses the crate-private item type:

```rust
pub(crate) enum ReconfigurableInputItem<V> {
    Data(InputBatch<V>),
    Reconfigure(ReconfigurationRequest),
}
```

Transport adapters parse JSON5 into a `ReconfigurationRequest` before yielding the control item. The request contains the new specification and its nested input/output configuration. It is not a model variable, and the owner loop performs no transport decoding.

The same live stream can yield data after a control item. For manual and ROS-style independent data/control fanouts, the external controller must establish the required ordering; a poll priority or a quiet interval is not an ordering guarantee.

## One source, one order

`InputPipeline::open_reconfigurable` validates the whole resolved input before looking up or opening a transport:

| Resolved model bindings | Result |
|---|---|
| None | Open the selected control source with no model bindings. |
| One source equal to the control source | Accepted. |
| One source different from the control source | Rejected before opening. |
| More than one source | Rejected before opening. |

The check prevents a control route on one source from being treated as an ordering barrier for data pending on another. It does not create a cross-transport total order. MQTT and Redis can expose a source-owned item stream; independent subscriptions still require external coordination.

## The window barrier stage

When an input window is configured, `apply_barrier_stage` converts typed items to window events and back:

```text
Data(batch)       → WindowEvent::Data(batch)       → Data(batch)
Reconfigure(req)  → WindowEvent::Control(req)      → Reconfigure(req)
```

The stage flushes pending batch or atomic-step data before forwarding a control item. The control item is a logical barrier, not an end-of-stream marker: the replacement session opened after the cutover continues to deliver data and further control items.

## Replacement resolution

Both resolvers are resource-free and run before anything is closed or opened.

`InputPipeline::resolve` produces the complete target `ResolvedInput`. `OutputBackendBuilder::resolve` produces the complete target `ResolvedOutput`, containing model projections, destination interfaces, stages, pipeline identity, and a fingerprint. `OutputBackendBuilder::open` opens the resolved destinations and builds the writer; `build` is the resolve-then-open convenience path, and `open_session` is the reconfigurable path.

Because planning owns no resource, a request that fails validation or resolution leaves the active sessions untouched. Once the cutover starts, any failure is terminal.

## The output handoff barrier

The dataflow owner submits pending engine rows and calls the active output session's flush before closing it. This ensures rows already accepted by the writer reach its sink boundary before the old destinations go away. The session is then closed, on normal runtime shutdown as well as at every cutover.

```text
OutputPipelineSession::send(batch)  → submit a packed batch
OutputPipelineSession::flush()       → flush accepted batches
OutputPipelineSession::close()       → finalize destinations
```

A later send, flush, or close failure is terminal. Cleanup still attempts to preserve already accepted rows where the backend allows.

## Root acknowledgements

`acknowledge_reconfiguration` publishes a `ReconfigurationAck` with `monitor_changed`, `interface_changed`, `monitor_revision`, and `interface_revision`. It is sent after pure resolution, output flushing, monitor-plan application, live input/output-plan application or required fallback opens, and active-state installation.

The acknowledgement confirms the local cutover/open handoff. It does not guarantee a later backend publish or remote transport delivery. A producer should wait for the acknowledgement before sending rows for the new interface.

## Deferred construction failures

`RuntimeBuilder::build` can retain startup failures and return them from `run`. During a root command, pure resolution failures do not mutate the monitor or open replacement resources, but the owner still follows its terminal cleanup policy. Input/output open failures are possible only on initial opening or on a required replacement fallback; partial output opens are cleaned up by the builder.

[← Previous: The dataflow runtime adapter](runtime-adapter.md) · [Next: The reconfigurable runtime](reconfigurable-runtime.md) →
