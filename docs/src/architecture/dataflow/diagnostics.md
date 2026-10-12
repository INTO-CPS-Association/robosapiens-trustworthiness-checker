# Diagnostics and observation

The `diagnostics` cargo feature adds profiling, execution tracing, source attribution and structured memory estimates to the dataflow runtimes. This page describes where that code lives, which owners call into it, and the invariants that keep it an observer. The commands, flags and report fields are in [Profile and trace dataflow runs](../../developer-tools/diagnostics.md).

The facility rests on one rule: observation reads state and never decides anything. A run records the same outputs, errors, commit boundaries, schedules and tier selections with the feature compiled out, compiled in and idle, or recording.

## Principal entities

| Entity | Responsibility |
|---|---|
| generic core (`crate::diagnostics`) | Engine-independent building blocks: `DiagnosticsConfig`, `DurationStats`, `PipelineProfile` with its `PipelinePhase` and `PhaseKind`, `Timeline`, `ValueSizer` and the memory types, `DiagnosticsHub` with its `SourceRecorder`s, and the Chrome trace export. |
| dataflow recorder (`dataflow::diagnostics`) | The only place a monitor's diagnostics are recorded and reported: `DataflowRecorder`, the versioned model map, the memory visitor, dependency aggregation and `DataflowDiagnosticsReport`. |
| execution probe (`ExecutionProbe`) | The hook surface lent to `MonitorExecution` and its step loop. Holds the recorder when the feature is on. |
| node sites (`NodeSites`, `DebugSources`) | Source attribution carried beside graphs and the program. A sidecar, not part of any graph's identity. |
| source probe (`SourceProbe`, `io::source_diagnostics`) | The hook an input source calls to record its own phases. |
| runtime probe (`RuntimeProbe`, `runtime::diagnostics`) | The runtime adapter's pipeline phase timing, and assembly of `RunReport`, the Chrome trace and the artifact files (`RunArtifacts`). |

## Dependency direction

Each recorder depends on the generic core. The core depends on none of them.

```text
crate::diagnostics                 generic core: config, stats, timeline, sizer, hub, chrome
        ^                ^
        |                |
dataflow::diagnostics   io::source_diagnostics        engine-specific recording next to its owner
        ^                ^
        |                |
        runtime::diagnostics                          assembles one run's report and artifacts
                 ^
                 |
                CLI (--diagnostics-*, DiagnosticsHub construction)
```

`crate::Value` does not depend on any recorder. The memory estimator reads values only through their public, non-mutating API. The input owner records decode, projection and conformance work through the hub; it does not know about the monitor. The monitor does not know about the hub: `runtime::diagnostics` is where a monitor's `DataflowDiagnosticsReport`, the pipeline profile and every registered source record are combined. The CLI only builds a `DiagnosticsHub` from its arguments and passes clones to the input pipeline and runtime builder.

## The probe pattern

Each owner reaches the recorder through a small probe type whose two forms are selected at compile time:

- **Without the feature**, `ExecutionProbe`, `NodeSites`, `DebugSources`, `RuntimeProbe` and `SourceProbe` are zero-sized, `Stamp` is `()`, and every hook is an empty inlined function. The hooks vanish from the compiled evaluator.
- **With the feature**, the probe holds an `Option<Box<recorder>>` (a shared `SourceRecorder` for sources), and a `Stamp` is `Option<Instant>`. A hook first asks for a stamp, which is `None` when nothing is recording, then passes it back with the measurement. A probe with no recorder installed costs one branch per hook and reads no clock.

Labels and names are not formatted on the step path. The recorder prepares them once, when a plan is first observed.

A monitor records nothing until `DataflowMonitor::enable_diagnostics` installs a recorder in its execution owner. A source records nothing until `InputPipeline::diagnostics` attaches a recorder when the source opens. A runtime times its phases only when its builder was given a hub.

## Hook owners

The hooks are single calls into the diagnostics modules. Each owner calls only the ones that describe its own work.

| Owner | Hooks |
|---|---|
| `DataflowMonitor` | Tick begin and end with outcome and revision, history commit, summaries of selected stream values before history commit moves them, `dynamic`/`defer` activation at the source barrier, schedule updates, revision changes, definition installation on reset and each reconfiguration, a memory snapshot before reconfiguration, and the explicit and periodic snapshots. |
| `MonitorExecution` | Observation of the active plan, per-step timing with the executor path that actually ran each step, the commit, plan switches (`built` or `cached`), and native tier activation. |
| canonical node evaluation | Collection counters at list append, concatenation, `List.map`/`filter`/`fold` and map update sites. A thread-local counting cell is owned by a stack of per-tick scopes. A recording monitor's scope counts and an unobserved monitor's scope suspends counting; leaving a scope, normally or by unwinding, restores the enclosing scope's counters and armed state. A host function that evaluates another monitor during a tick therefore attributes the inner operations to the inner recorder only, and the outer tick resumes with its own counts. |
| lowering and binding | The `NodeSites` sidecar: lowering assigns each graph node the span and origin of the frontend expression that produced it, and binding carries it forward. |
| `DataflowProgram` | The source archive the program was expanded from, kept as `DebugSources` for resolving spans to files, lines and snippets. It is not part of the definition key. |
| `DirectDataflowEngine` | The pipeline phases: `compile`, `input_wait`, `row_preparation`, `monitor_evaluation`, `output_packing`, `output_send`, `output_finish`, `input_drain`, and `reconfiguration` in the reconfigurable runtime. |
| RPIO batches | The source-owned phases of RPIO journal inputs: decode (replay) or wait (live), projection, conformance and provenance, with counters for ticks delivered. |
| `InputPipeline` | Attaches a source recorder, named by variable and kind, to the sources that record their own phases when they open. |

`MonitorExecution` holds the probe. The monitor walks its owners read-only to build memory snapshots, calling `MonitorExecution::visit_memory` for the evaluators, regions, plans and caches it owns. When root reconfiguration replaces the monitor wholesale, the execution owner moves the probe across to the candidate, so one recorder spans every definition the run executed.

## Source attribution does not enter identity

Lowering records a site for every graph node: a frontend span and a `NodeOrigin`. The sites live in a `NodeSites` vector on the evaluation graph, index-aligned with its nodes. They are a sidecar:

- `NodeSites` implements `PartialEq` as always equal, so two graphs that differ only in where their nodes came from compare equal.
- It is not written into the canonical graph descriptor, so it contributes nothing to stream state keys or definition keys.
- Scheduling, plan identity and caches are computed from the graph and never read it.
- Without the feature it is zero-sized and lowering records nothing.

Attribution is linear in the graph: lowering keeps a stack of nodes that still await a site, and each finished expression pops only the suffix it owns, so a deep left-nested expression does not rescan its nodes. A node's call site is its primary span. For code inlined from another module's `def`, that is the call. Its definition site is where the inlined code was written, when the frontend recorded one. Inlining removes the call boundary from the graph, so a definition site attributes nodes and never a separately timed invocation. Nodes built programmatically or generated by lowering carry no origin and stay unlocated.

Spans resolve to files, lines and snippets from the archive held by the program when a definition is recorded (at enable time and on each definition change), never on the step path. Line numbers come from one index of line starts per file, built on first use, and a snippet examines at most a few hundred characters of its span, so neither costs more for a larger file or span. The model map is bounded by a node budget: capture stops visiting, formatting and resolving once it is spent.

## Semantic invariants

Observation does not:

- advance ticks or evaluate anything extra;
- change schedules, or repair them to make a report simpler;
- disable quickening, or change which expressions are quickened;
- select or deselect native tiers;
- materialize native state into the canonical arena.

The last point has a visible consequence. A memory snapshot of a stream whose state a native tier owns reads the canonical arena as it stands, marks those components opaque with a note that the copy may be stale, and reports `execution/native_artifacts` as opaque. Making the numbers exact would call `materialize`, which changes who owns the state, so snapshots accept the staleness instead.

The memory traversal uses no `unsafe` code (the diagnostics modules forbid it). The capture list of a function value is shared and rewritten while the function runs, so it is not traversed: it contributes its slot capacity, counted in `opaque_values`, and is reported as opaque. Its contents therefore appear in no snapshot, which is a deliberate limit rather than an oversight.

Timing a fused scalar region or a whole-schedule native artifact yields one aggregate with its member streams. Per-stream figures for them would have to be invented, so none are.

Reports are built on demand from the recorder's aggregates by `DataflowMonitor::diagnostics_report`. Building one does not mutate the monitor. File I/O occurs in one place, when a runtime finishes or a caller invokes `RunArtifacts::write`. A write failure is logged and cannot change the run's result.

## Versioning

Stream indices are only meaningful within one definition. The recorder therefore keeps a `DefinitionRecord` per definition version and scopes every figure to it:

- a version is created when diagnostics are enabled, when the monitor is reset, and on each successful root reconfiguration, each recorded as a `definition_installed` event;
- a rejected reconfiguration records `reconfiguration_rejected` and installs nothing;
- a physical plan is identified by the key `(definition_version, plan id, generation)`, so plans from different definitions never merge, and a plan is described the first time it is observed;
- tick, step, `dynamic_activation` and `plan_switch` events carry the definition version and the plan identity and generation then in effect (a `plan_switch` names the plan it switched to and the one it left); a `dynamic`/`defer` expression is keyed by definition version, owning stream and graph node, never by an index that every definition reuses;
- a memory snapshot records the definition version it describes, and the dependency section takes byte figures only from a retained snapshot of the current version. When none exists they are `null`, never taken from an earlier definition's snapshot that merely shares a stream name;
- stream names in the report are resolved through the version they were recorded under, and the dependency section describes the current definition only.

Plans that were described but never executed, such as one replaced at the source barrier of its first tick, are omitted from the report.

## Tick identity

A tick index is zero-based. The monitor's recorder counts every evaluation that reached the tick hook, succeeded or failed, and the runtime adapter counts the ticks it has admitted. Row preparation, monitor evaluation and output packing of one tick carry the index of that tick, as do the monitor's `tick` span and its step spans, so a trace range of `[0, 1)` holds the first tick's spans on every track. Waits and flushes carry the index of the next tick to be admitted at the time they ran. The `compile` span happens before any tick is admitted and carries tick 0, the identity of the first admitted tick's spans. A rejected evaluation after a terminal failure is not a tick on either side.

## Bounded recording

Every buffer has an explicit bound, and the bound shows in the report rather than in a silent loss:

| Buffer | Bound | When full |
|---|---|---|
| each `Timeline` (pipeline, monitor, each source) | `trace.capacity` entries | Later entries are dropped and counted. Early entries are kept, so the start of a run is complete. A dropped entry is never constructed. |
| tick and step spans, value summaries | `trace.ticks` | Not recorded outside the range. Aggregates still cover every tick. |
| a value summary | `trace.value_chars` | Truncated with an ellipsis. |
| memory snapshots | `memory.max_snapshots` | A periodic snapshot is counted in `periodic_skipped` and not traversed. A final, pre-reconfiguration or explicit snapshot replaces the most recent periodic one (`periodic_replaced`); with none to replace it is refused, its label is listed in `mandatory_skipped` (bounded, with a total in `mandatory_skipped_count`) and nothing is traversed or returned for it. `snapshots_built` counts traversals actually done. |
| one snapshot's traversal | `memory.budget` depth and work | Every value visited (map values included), map key and map-root identity comparison costs a unit of work and is charged before it is done. Once the budget is spent no further value, child, owner or evaluator node is entered, and the estimates reached after that are marked truncated. |
| a definition's node list | `limits.max_model_nodes` | Traversal, label formatting and source resolution stop, including inside branch and match-arm graphs; later nodes are neither visited nor listed. The definition's `node_count` is what the capture visited, flagged `node_count_is_lower_bound` when the budget was reached. Operation labels clip names to 48 characters. |
| source positions | `limits.max_source_scan_bytes` | One total across files for indexing line starts and counting columns. Past it a node keeps file, span and snippet and omits line and column, flagged `position_truncated`. |
| definitions, plans, dynamic expressions, dependency outputs | `limits` | The first definition and the most recent ones are kept; entries beyond a bound are counted in `dropped`. |
| input-source recorders | `limits.max_sources` | A source opened beyond the bound still records privately; it is not reported and `sources_dropped` counts it. |

Aggregates are fixed-size, keyed by compact identities (definition version, plan, step index), and do not grow with the number of ticks.

## What remains incomplete

The report states the main limits itself in `coverage`.

- Fused regions and whole-schedule native artifacts are aggregates with no per-stream split.
- Inlined definitions have no timed call boundary.
- Nodes of `dynamic`/`defer` bodies compiled at runtime are not in the model map.
- `TypedDataflowMonitor` is not instrumented, and its direct native entry bypasses the recorded monitor after promotion.
- Only RPIO journal sources record source phases, and a live source's decode time is inside `input_wait`.
- There is no per-node timing.
- Collection counters cover list appends, concatenations, scans and map updates only; values cloned into lifted operands, history or output rows are not counted.
- There is no allocator integration; memory is a structured estimate. Persistent map storage is a logical per-root figure: cloning a map handle is O(1), but interior nodes shared between distinct roots cannot be observed, so each root is charged its entries.
- Per-stream charges deduplicate by first visit, so a later stream sharing a root shows `shared_bytes` (the root's shell only: the children of a shared list, map, union or newtype are not re-walked) and a larger `bytes_with_shared_roots`, not a charge. That figure is partial, overlaps between streams, and is reported per stream only: every upstream dependency aggregate leaves it `null`, because summing it would count shared roots once per stream and de-duplicating it would need a recursive walk. Upstream totals sum the charged `bytes`. Dependency aggregates are diagnostic totals, not predictions of what removing an output would save.
- Map-root de-duplication compares against only the 32 most recent same-sized roots. A cloned handle seen again outside that window is charged again, counted in `unresolved_map_identities`; the flag is carried to the stream (`map_identity_uncertain`) and to dependency costs, and while it is set the charges are not guaranteed globally unique.
- Function capture lists and native-owned state are opaque.
- Elapsed times are wall-clock; they include preemption and, for waits, the work of other tasks.
- Runtimes other than the dataflow runtimes ignore diagnostics.
- Buffers are bounded, so a long run with a small capacity reports less timeline than it executed.
