# Input and output boundary

[← Previous: The dataflow runtime adapter](runtime-adapter.md) · [Next: The reconfigurable runtime](reconfigurable-runtime.md) →

The ordinary runtime consumes an `InputStream<Value>` and an opened `OutputWriter`. The reconfigurable runtime retains an `InputPipeline` and an `OutputBackendBuilder`, then opens a live `InputPipelineSession` and `OutputPipelineSession` around their resolved plans.

This page describes how those plans and sessions are resolved, opened, and updated, and how a typed control item reaches the owner loop.

## Resolutions and sessions

`InputPipeline` and `OutputBackendBuilder` are reusable configuration; `ResolvedInput` and `ResolvedOutput` describe one active binding set. An `InputPipelineSession` and an `OutputPipelineSession` are the opened resources for one such pair.

Every accepted reconfiguration resolves and plans both sessions, including a
request that changes neither interface. Unchanged source streams and output
owners remain live. Input additions are inserted into the composed stream.
Removal stops the source's bounded local relay, then consumes that relay to real
EOF before break-before-make replacement. Output owners are fixed by
`OutputPipeline` and changed interfaces are updated in place.

```text
active InputPipelineSession + OutputPipelineSession
                  │
                  ├─ drain removed input sources under the old monitor
                  ├─ retain unchanged streams and insert additions
                  └─ flush/update only affected output owners
```

## Input resolution and control binding

`InputPipeline` is reusable configuration: registered sources, route catalogs, codecs, and window stages. It opens no source until a `ResolvedInput` is passed to an open method.

`ReconfigurableInput` wraps the pipeline with one validated `ReconfigurationControl`. Construction resolves the selected control source and route; `open_session` opens every resolved data source, opens the selected source with control enabled, and creates an `InputPipelineSession` that retains those source streams by `SourceId`.

`InputPipeline::resolve` is pure. It resolves the model's current input variables against an optional `InputConfiguration` and attaches the pipeline identity and configuration fingerprint to the resulting `ResolvedInput`. Root reconfiguration calls it before any replacement resource is opened.

A resolved model may use any number of data sources. Exactly one configured source owns the control route; it is opened as a control-only source when it has no active model binding. Source composition defines the runtime's observed item order but does not claim a total order between independent transports.

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

## One logical boundary over multiple sources

`ReconfigurableInput::open_session` validates every resolved source before opening any transport. It opens each source independently behind a bounded relay and composes whole `InputBatch` values; the single-source case returns that source directly, preserving packed input without composition overhead.

The control item begins one logical runtime transition. Independent transports still have no inherent total order. Applications requiring a deterministic source move must quiesce the affected producer, issue the command, wait for acknowledgement, and then resume it. Each dataflow source is opened behind a bounded local relay. Removal stops that relay from polling its transport and then consumes every item already admitted to the relay before genuine EOF. Messages still held by a remote broker or transport queue remain outside this local boundary; a future distributed handoff may strengthen it with durable transport cursors.

## The window barrier stage

When an input window is configured, `apply_barrier_stage` converts typed items to window events and back:

```text
Data(batch)       → WindowEvent::Data(batch)       → Data(batch)
Reconfigure(req)  → WindowEvent::Control(req)      → Reconfigure(req)
```

The stage flushes pending batch or atomic-step data before forwarding a control item. The control item is a logical barrier, not an end-of-stream marker: the persistent session retains unchanged sources, installs additions after removals finish, and continues to deliver data and further control items.

## Target resolution and pipeline plans

Both resolvers are resource-free and run before anything is closed or opened.

`InputPipeline::resolve` produces the complete target `ResolvedInput`. `OutputBackendBuilder::resolve` produces the complete target `ResolvedOutput`, containing model projections, destination interfaces, stages, pipeline identity, and a fingerprint. `OutputBackendBuilder::open` opens the resolved destinations and builds the writer; `build` is the resolve-then-open convenience path, and `open_session` is the reconfigurable path.

Because planning owns no resource, a request that fails validation or resolution leaves the active sessions untouched. Once the cutover starts, any failure is terminal.

## The output handoff barrier

The dataflow owner submits pending engine rows before applying a pipeline
plan. For an output change, the session flushes only the affected destination
owners, or flushes the shared stage once when one exists, before updating their
interfaces and routing. Unrelated destinations are not flushed. The session is
closed on normal runtime shutdown or when a terminal cutover failure ends the
runtime.

```text
OutputPipelineSession::send(batch)                    → submit a packed batch
OutputPipelineSession::apply_reconfiguration(plan)    → targeted flush/update
OutputPipelineSession::flush()                        → flush the whole session
OutputPipelineSession::close()                        → finalize destinations
```

A later send, flush, or close failure is terminal. Cleanup still attempts to preserve already accepted rows where the backend allows.

## Root acknowledgements

`acknowledge_reconfiguration` publishes a `ReconfigurationAck` with
`monitor_changed`, `interface_changed`, `monitor_revision`, and
`interface_revision`. It is sent after pure resolution, pending-row submission,
input/output plan application, monitor-plan application, and active-state
installation.

The acknowledgement confirms the local cutover/open handoff. It does not guarantee a later backend publish or remote transport delivery. A producer should wait for the acknowledgement before sending rows for the new interface.

## Deferred construction failures

`RuntimeBuilder::build` can retain startup failures and return them from `run`.
During a root command, pure resolution failures do not mutate the monitor or
open resources, but the owner still follows its terminal cleanup policy. Input
stream open failures can occur when an added or replacement source is applied;
output interface failures occur when an existing backend rejects an update. There is
no output replacement fallback. Partial initial output opens are cleaned up by
the builder.

[← Previous: The dataflow runtime adapter](runtime-adapter.md) · [Next: The reconfigurable runtime](reconfigurable-runtime.md) →
