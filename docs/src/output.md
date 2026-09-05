# Output architecture

The output subsystem accepts complete logical output ticks from a runtime and turns them into configured delivery through local or transport-backed destination owners. It is the architecture layer that hides destination selection, route/codec interfaces, stage ownership, fan-out, backpressure, and writer lifecycle behind one runtime-facing `OutputWriter`.

## Scope of the abstraction

| Concern | Output subsystem responsibility |
|---|---|
| runtime output forms | Accept singleton, simultaneous, packed-row, and mixed `OutputBatch` segments without redefining their logical ticks. |
| destination configuration | Resolve model outputs and request bindings onto stable `OutputDestination` owners, routes, codecs, and explicit mirrors. |
| resource ownership | Separate resource-free `ResolvedOutput` planning from opened backend clients, publishers, stage workers, and session state. |
| delivery topology | Place shared `OutputStage` wrappers before direct/routed delivery and destination-local wrappers around individual backend owners. |
| fan-out and pressure | Select each destination's variables while exposing one readiness contract to the producer. |
| lifecycle and replacement | Order send, flush, close, cleanup, sticky failure, and supported in-place interface handoff through `OutputWriter` and `OutputPipelineSession`. |

The subsystem does not choose the runtime's logical output shape, provide an atomic transaction across destinations, or prove that a remote service persisted or consumed an admitted batch.

```mermaid
flowchart TB
    accTitle: Output architecture from runtime rows to external destination owners
    accDescr: Runtime families preserve their native logical tick shapes in OutputBatch. Resource-free OutputPipeline resolution combines model outputs with a stable destination registry to produce ResolvedOutput. Opening creates backend owners and wraps them in destination-local stages. Runtime batches pass through shared stages, then either a direct one-destination writer or a session and multi-destination router. Reconfiguration can update supported interfaces and selected variables on fixed session owners.

    subgraph producers["Runtime producers"]
        direction TB
        dataflow["Dataflow packed rows"]
        semisync["Semisynchronous rows"]
        mstlo["MSTLO sparse ticks"]
        async["Asynchronous or distributed ticks"]
    end

    subgraph planning["Resource-free output planning"]
        direction TB
        model["Model outputs and request bindings"]
        registry["OutputDestinations registry"]
        resolve["OutputPipeline::resolve"]
        plan["ResolvedOutput"]
        model --> resolve
        registry --> resolve --> plan
    end

    subgraph delivery["Opened delivery architecture"]
        direction TB
        open["Open backend owners and apply destination-local stages"]
        batch["OutputBatch logical ticks"]
        shared["Shared OutputStage wrappers"]
        path{"Opened writer path"}
        direct["Direct one-destination writer"]
        router["OutputPipelineSession router"]
        local["Destination-local OutputStage wrappers"]
        owners["Opened backend owners"]
        open --> local
        batch --> shared --> path
        path -->|"one non-session destination"| direct
        path -->|"session or several destinations"| router
        direct --> local
        router -->|"selected values per destination"| local
        local --> owners
    end

    dataflow --> batch
    semisync --> batch
    mstlo --> batch
    async --> batch
    plan -->|"validated plan"| open
    control["Reconfiguration request"] -. "flush, then update supported interfaces and selection" .-> router
```

**Reading rule.** Solid arrows separate the resource-free planning path from runtime batch delivery and show the two opened writer alternatives. Shared stages are outside routing; destination-local stages wrap each backend owner. The dashed edge is session control, not output data. The focused routing figure below expands router readiness and selection without changing these connection points.

## Principal entities

| Entity | Responsibility |
|---|---|
| logical output (`OutputBatch`) | Carries ordered logical ticks in the native singleton, simultaneous, packed-row, or mixed representation chosen by the runtime. |
| destination registry (`OutputDestinations`) | Holds stable destination identities and unopened `OutputDestination` backend, selection, route, and local-stage configuration. |
| output pipeline (`OutputPipeline`) | Resolves model outputs and request bindings, opens backend owners, and places shared stages outside destination-local stages. |
| resolved output plan (`ResolvedOutput`) | Records complete ownership, mirroring, routes, codecs, stages, interfaces, and destination selection without opening resources. |
| stage wrapper (`OutputStage`) | Applies bounded buffering or logical-tick-preserving coalescing either before routing or within one destination owner. |
| backend interface (`OutputBackendConfig`, `OutputInterface`) | Separates reusable backend configuration from the resolved routes supported by an opened owner. |
| runtime-facing sink (`OutputWriter`) | Applies readiness, send, flush, and close ordering to complete output batches. |
| live output session (`OutputPipelineSession`) | Retains fixed opened destination owners and router state so supported interfaces and selected variables can change in place. |

## The logical output contract

`OutputBatch<V>` stores an ordered sequence of non-empty logical ticks. A tick may be a singleton update, a simultaneous set of updates, or a row in fixed-layout packed storage. Physical concatenation, mapping, and variable selection preserve logical boundaries.

Output shape belongs to the producing runtime:

| Producer | Native output representation |
|---|---|
| dataflow runtime | packed fixed-layout rows |
| semisynchronous runtime | simultaneous rows |
| MSTLO runtime | sparse singleton ticks; a variable may appear in more than one tick |
| asynchronous and distributed runtimes | singleton ticks in observed merge order |

```mermaid
flowchart TB
    accTitle: Physical output segments preserve one logical tick sequence
    accDescr: Singleton tick runs, simultaneous Tick segments, and fixed-layout PackedRows segments remain in their native forms inside OutputBatchStorage. OutputBatch exposes all of them as one ordered sequence of nonempty logical ticks to OutputWriter consumers.

    singleton["SingletonTicks: independent width-one ticks"] --> storage["OutputBatchStorage"]
    simultaneous["Tick: one simultaneous update set"] --> storage
    packed["PackedRows: fixed-layout row sequence"] --> storage
    storage --> batch["OutputBatch"]
    batch --> ticks["Ordered logical ticks"]
    ticks --> writer["OutputWriter consumer"]
```

**Reading rule.** Solid arrows map physical segment forms onto the common logical contract. The forms may coexist in one batch, but they do not merge simultaneous ticks, invent model time, or require packed rows to be expanded before iteration.

A batch is not an external transaction. Successful admission to an `OutputWriter` also does not prove that a remote service has persisted or consumed the values.

## Resolution and opening

`OutputPipeline` owns durable destination and stage configuration. `OutputPipeline::resolve` combines that configuration with model outputs, auxiliary variables, and request-local bindings to produce a complete immutable `ResolvedOutput`.

Resolution establishes primary ownership, explicit mirroring, routes, codecs, shared stages, destination-local stages, and destination selection. Every model output has at least one primary owner. Additional delivery is explicit; a default destination does not mirror to every configured destination.

Resolution opens no backend. Opening validates the resolved structure again, then creates destination owners in deterministic order. If a later destination fails to open, already-opened owners are closed before the error is returned. `OutputPipeline::open` uses a direct writer when exactly one destination is resolved and otherwise opens a session-backed router. `OutputPipeline::open_session` always retains the router because its fixed owners may receive new bindings during reconfiguration.

This example makes the planning, ownership, and completion boundaries explicit with a local null destination:

```rust
{{#include ../../tests/docs_examples.rs:output_pipeline_open}}
```

`resolve` is resource-free; `open` creates the destination owner. `send` admits the complete batch under the writer's readiness contract, while `flush` is the downstream completion barrier. Even that barrier states completion only at the opened backend's contract and does not imply remote persistence for a transport-backed destination.

## Shared stages, routing, and destination stages

The router precomputes the union of variables accepted by its active destinations and rejects any update outside that resolved set. Its detailed admission and selection path is:

```mermaid
flowchart TB
    accTitle: Router admission and destination-local delivery
    accDescr: After shared stages, poll_ready waits for every active destination writer. start_send validates that every update belongs to the resolved destination union. A one-owner router forwards the original OutputBatch. A multi-owner router clones each destination's selected variables, skips empty selected batches, then sends through that owner's destination-local stages to its backend. Any pending destination can hold global admission.

    batch["OutputBatch after shared stages"] --> ready["poll_ready: every active destination writer"]
    ready --> validate["start_send: validate resolved variable union"]
    validate --> count{"Active owner count"}
    count -->|"one"| original["Forward original OutputBatch"]
    count -->|"several"| select["select_variables_cloned for each owner"]
    select --> selected["Nonempty selected OutputBatch"]
    select -. "empty selection" .-> skip["No send to that owner"]
    original --> local["Destination-local stages"]
    selected --> local
    local --> backend["Opened backend owner"]
    pressure["One pending owner holds producer admission"]
    ready -. "global readiness coupling" .-> pressure
```

**Reading rule.** Solid arrows carry admitted batches; the one-owner and multi-owner branches are alternatives. Dashed arrows show consequences rather than batch delivery: empty projections are skipped, while readiness remains coupled across all active owners. Destination-local stages preserve each owner's order but do not provide cross-destination rollback.

Selection preserves logical tick boundaries; ticks containing no selected values disappear from that destination's batch:

{{#include assets/output-routing-ticks.svg}}

**Reading rule.** Columns retain the original `OutputBatch` tick positions. A destination receives only nonempty projections, so its local sequence can omit an original tick, but values from separate original ticks never become simultaneous. The two destination lanes are independent delivery sequences, not an atomic cross-destination commit.

There is no cross-destination observation order or atomic commit.

## Stage semantics and bounds

Two stage families are implemented:

- `OutputStage::Buffer` is bounded, FIFO, reliable, and blocking. `max_batches` bounds retained physical batches. `max_updates` is a pressure threshold over queued and in-flight updates; one indivisible admitted batch may cross it.
- `OutputStage::Coalesce` changes physical batching only. It preserves every logical tick and never applies last-update-wins. Tick and update limits are flush thresholds evaluated between incoming physical batches, so one complete batch may overshoot them.

Timed stages own worker activity on the local executor. Flush and close are ordered barriers through every wrapper. No output worker is detached from its owning writer.

## Backpressure and failure

`OutputWriter` exposes sink readiness to its producer. In a multi-destination session, readiness waits for every destination writer, including one that will receive no values from the next batch. Destination-local buffering can absorb work after routing but does not remove this global admission coupling.

A destination may already have observed a batch when another destination fails. The pipeline provides no cross-transport rollback. The first operation failure becomes sticky: later sends fail fast, while close still attempts to flush and close every stage and destination and retains cleanup failures separately.

The writer interaction distinguishes admission from the later completion barrier:

```mermaid
sequenceDiagram
    accTitle: Output admission and downstream completion are separate
    accDescr: A producer sends through OutputPipelineSession and OutputWriter to OutputRouter. The router waits for every destination writer to become ready, then submits each nonempty destination projection. A successful send returns after the pipeline is ready for more work. A later flush waits for every destination writer. A destination failure is retained and does not reverse an earlier destination submission.

    participant producer as Runtime producer
    participant session as OutputPipelineSession
    participant writer as OutputWriter
    participant router as OutputRouter
    participant destinations as Destination writers

    producer->>session: send(batch)
    session->>writer: send(batch)
    writer->>router: poll_ready()
    loop Every active destination owner
        router->>destinations: poll_ready()
        destinations-->>router: Ready, Pending, or error
    end
    router-->>writer: all ready
    writer->>router: start_send(batch)
    loop Every nonempty destination projection
        router->>destinations: start_send(selected batch)
    end
    alt every selected submission succeeds
        destinations-->>router: all selected submissions accepted
        writer->>router: poll_ready() after feed
        router-->>writer: ready for more work
        writer-->>session: admission complete
        session-->>producer: send complete
        producer->>session: flush()
        session->>writer: flush()
        writer->>router: poll_flush()
        loop Every active destination owner
            router->>destinations: poll_flush()
            destinations-->>router: completion or error
        end
        router-->>writer: downstream flush complete
        writer-->>session: flush complete
        session-->>producer: completion barrier passed
    else a later destination fails
        destinations-->>router: error after an earlier submission
        router-->>writer: retained error
        writer-->>session: error
        session-->>producer: error, earlier delivery remains
    end
```

**Reading rule.** Solid arrows are calls or batch submissions; dashed arrows are readiness results, returns, or completion signals. `send` completes after admission and a subsequent readiness poll, not after a universal downstream flush. `flush` separately waits through every active destination. The loop order does not create an atomic cross-destination commit, and an error does not reverse an earlier owner’s accepted batch.

## Live interface handoff

A reconfigurable `DataflowRuntime` keeps a fixed registry of opened destination owners in an `OutputPipelineSession`. A request may change bindings, routes, codecs, or destination selection supported by those owners. It cannot create or remove durable destination owners, replace local backend configuration, or change local stage configuration.

Application first flushes affected owners, or the shared writer when shared stages require a global barrier. It then updates supported `OutputInterface` handles and the router's selected variables. There is no hidden close-and-reopen fallback for a backend that cannot update its interface.

Updates are sequential rather than transactional. If a later destination fails, an earlier destination update can remain applied. This partial-application boundary is part of the [root cutover](architecture/dataflow/reconfigurable-runtime.md).

## Implementation mapping

- `OutputUpdate`, `OutputBatch`, segment preservation, `OutputWriter`, and sticky writer lifecycle: `src/core/output.rs`.
- `OutputDestination`, `OutputDestinations`, `OutputPipeline`, `ResolvedOutput`, router selection, and `OutputPipelineSession`: `src/io/output/pipeline.rs`.
- Buffer and coalescing `OutputStage` wrappers, bounds, workers, and barriers: `src/io/output/stages.rs` and `src/io/output/pump.rs`.
- Runtime-native batching and handoff: `src/runtime/dataflow.rs`, `src/runtime/semi_sync.rs`, `src/runtime/mstlo.rs`, `src/runtime/asynchronous.rs`, `src/runtime/distributed.rs`, and `src/runtime/output.rs`.
- Focused output batch, stage, routing, backpressure, opening-cleanup, and reconfiguration tests live beside these implementations.

Continue with the [input architecture](input-architecture.md), [runtime adapter](architecture/dataflow/runtime-adapter.md), or [failure containment](architecture/dataflow/failure-model.md).
