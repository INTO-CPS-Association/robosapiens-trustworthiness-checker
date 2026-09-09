# I/O ownership and lifecycle

This page explains how active input and output resources are retained, stopped, and cleaned up. For the role of the whole layer and the path from observations to results, start with [I/O architecture](io.md). The runtime coordinates resource changes across input, output, and model evaluation.

Input and output share infrastructure, while exposing different operations: an input owner yields observations and can become a drain; an output writer accepts results and can be flushed and closed. The [input architecture](input-architecture.md) and [output architecture](output.md) explain each direction in detail.

## Shared I/O infrastructure

| Responsibility | Shared mechanism and direction-specific contract |
|---|---|
| Logical data | `InputBatch` carries observed ticks; `OutputBatch` carries computed ticks. Both use shared segment storage, traversal, mapping, and normalization. |
| Planning | Input resolves sources and bindings; output resolves destinations, bindings, and delivery policies. Both use opaque routes and formats, durable generations, and live-session identities. |
| Live ownership | `OpenedInput` yields observations and owns source lifetime. `OutputWriter` accepts results and owns a sink. Their session types retain sources or destination writers across reconfiguration. |
| Failure | Distinct `InputError` and `OutputError` types share `ErrorDetails` for classification, context, causes, and cleanup diagnostics. |
| Recovery and termination | Input stops ingress and drains; output completes delivery and closes. Transport-local retries use `RetryPolicy`, and coordinated cleanup shares one `ShutdownDeadline`. |

The public batch and error types remain distinct. Sharing their implementation does not make input reduction interchangeable with output coalescing: input policy may reduce observations into a model step, whereas output coalescing preserves every logical output tick.

A source or destination description owns reusable configuration. Resolution produces a resource-free plan. Opening acquires the transport resources, and an opened session retains those resources while supported bindings change. A `PipelineGeneration` identifies durable configuration; a `SessionId` and `SessionRevision` identify one live session and its accepted transitions. A stale plan is rejected before application.

Route addresses and `FormatId` values cross the shared layer as opaque identifiers. Native transport implementations interpret their wire representations. The shared router selects variables and preserves logical ticks; it does not parse transport payloads.

## Admission and completion

Consider a source that has admitted two ticks, `x = 1` followed by the simultaneous tick `x = 2, y = 3`. Stopping ingress prevents further admission, but graceful input draining still exposes those two ticks in order. The runtime can evaluate them and submit their results to an output writer.

Output admission and delivery completion are separate boundaries. `feed` can return while a queued destination still holds the result. `flush` waits for that destination's completion contract; for built-in MQTT output, this includes the QoS 1 acknowledgement. The [Sink recap](output.md#rust-sinks-readiness-admission-and-completion) explains the Rust operations, and the [completion table](output.md#transport-completion) identifies what each backend confirms.

Input composition preserves locally observed order across independent producers. Output fan-out has independent destination effects. Neither operation creates a distributed total order or a transaction across transports.

## Graceful shutdown

The runtime's coordinated shutdown follows this order:

| # | Phase | Owner and observable result |
|---:|---|---|
| 1 | Stop ingress | Input owners stop admitting new observations. |
| 2 | Drain admitted input | The drain yields retained observations and flushes applicable input-window state until EOF or error. The runtime processes them while evaluation can continue. |
| 3 | Finish output | The runtime submits pending result rows and waits for output completion. |
| 4 | Close resources | Destination owners perform cleanup, and the runtime reports the retained failure and any cleanup errors. |

One absolute deadline covers these phases. Each wait uses the remaining time; moving to another source, destination, or cleanup operation does not restart the timeout. The CLI's `--io-shutdown-timeout-ms` configures this budget, and omission means no time limit. Retry limits are separate and cannot extend shutdown. Expiry reports incomplete shutdown and aborts or cancels remaining work, so it does not promise that every admitted observation was evaluated or every result delivered.

| Operation | Lifecycle contract |
|---|---|
| `OpenedInput::into_drain()` | Consumes the live input owner, stops ingress, and returns an `InputDrain` stream. Completion requires polling the drain to EOF; a returned drain does not progress by itself. |
| `into_drain_with_deadline(deadline)` | Uses the supplied absolute deadline. Expiry is yielded as an input error before the drain ends. |
| `OutputWriter::close().await` | Drives the sink's final delivery and cleanup using the writer's configured timeout. Repeated close calls return the stored result. |
| `close_with_deadline(deadline)` | Uses the coordinator's existing deadline. Expiry aborts the sink and retains a close error. |
| Drop an input owner or drain | Releases ownership and requests cancellation; it provides no awaited drain-completion result. |
| Drop an output writer | May arrange detached cleanup or let a delivery worker drain after its sender closes. This fallback requires continued executor progress and supplies no completion result to the caller. |

These are the contracts of the lifecycle-aware openers. Converting an arbitrary plain `InputStream` into `OpenedInput` cannot supply a producer stop protocol; that conversion returns an empty drain. The pipeline and production channel openers provide explicit source ownership for graceful draining.

## Recovery and terminal failure

Transport owners classify failures and apply retries. `RetryPolicy` supplies the attempt limit and backoff; it does not decide whether replay is safe. Attempt limits include the initial attempt, and successful recovery resets the tracked failure sequence.

MQTT recovery retains protocol retransmission state in its driver. Plain Redis input reconnects and restores subscriptions. Redis output can retry connection establishment, but a failed publish is terminal because the server may already have received it. Redis knowledge has its own [selected-key recovery semantics](redis-knowledge-input.md). Sessions do not recover by replaying whole input or output batches.

The [input](reference/input-configuration.md) and [output](reference/output-configuration.md#retry-and-shutdown) configuration references specify retry defaults and limits. An unlimited retry policy still yields to cancellation or an expired shutdown deadline. Terminal transport failures leave the runtime to stop the session and attempt cleanup.

An output worker can fail after `feed` has returned; subsequent readiness, flush, rebind, or close observes that failure. The writer retains the first failure and reports cleanup failures separately. A reconfiguration failure can leave an earlier source or destination change applied. The runtime stops rather than rolling back or resuming ordinary evaluation with that partial state.

## Owner lifetimes during reconfiguration

A control boundary separates already-admitted old-binding observations from new-binding observations. The input session's consuming rebind callback evaluates old-side data, then the session applies source changes and returns. Pending old output is flushed before output interfaces and the monitor are replaced. Input additions may already be open at that point; opening a source does not mean its new observations have been evaluated.

Destination owners remain fixed throughout an output session. Supported route and binding changes are applied in place after completion barriers. Session revisions advance only after the coordinated application succeeds. The [root reconfiguration architecture](reconfiguration.md) gives the full order and partial-failure consequences.

## Implementation mapping

- Shared batch storage and traversal: `src/core/batch.rs`, with distinct contracts in `src/core/input.rs` and `src/core/output.rs`.
- Shared diagnostic representation: `src/core/io_error.rs`; direction-specific propagation remains with the input owners and output writer.
- Ordinary input ownership and drain: `src/io/builders/input_stream_factory.rs`; retained source sessions: `src/io/reconfigurable_input.rs`.
- Production channel input and output: `src/io/channel.rs`, also used by the Python/FMI binding.
- Output admission, completion, and close: `src/core/output.rs`; delivery workers and completion credits: `src/io/output/delivery.rs`.
- Retry timing and limits: `src/io/retry.rs`; transport implementations classify recoverable failures.
- Pipeline/session identities and the absolute shutdown deadline: `src/io/lifecycle.rs`.
- Coordinated runtime cleanup: `src/runtime/dataflow.rs`, `src/runtime/reconfigurable_semi_sync.rs`, and `src/runtime/output.rs`.
