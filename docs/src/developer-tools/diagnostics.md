# Profile and trace dataflow runs

The `diagnostics` cargo feature records where a dataflow run spends its time and what its stream state retains. A run with `--diagnostics-dir DIR` writes two files there: `summary.json`, a machine-readable report, and `trace.json`, a timeline that Perfetto and `chrome://tracing` open. Both come from one run and share a time origin.

Recording only observes. Outputs, errors, commit boundaries, schedules and tier selection are the same with and without it. [Diagnostics and observation](../architecture/dataflow/diagnostics.md) describes how that is kept true.

## Build with the feature

```sh
cargo build --features diagnostics
cargo build --features diagnostics,jit
```

Without the feature the hooks compile to nothing and the `--diagnostics-*` flags do not exist. Add `jit` to profile native execution. The default `dev` profile is enough to learn the report. For timings that resemble production, build the release profile with the allocator you deploy with:

```sh
cargo build --release --features jemalloc,diagnostics
```

Diagnostics is an opt-in facility: even with the feature compiled in, nothing is recorded until `--diagnostics-dir` is given.

## Command-line flags

| Flag | Default | Effect |
|---|---|---|
| `--diagnostics-dir DIR` | none | Turns recording on and names the directory that receives `summary.json` and `trace.json`. Without it the other flags have no effect. |
| `--diagnostics-trace-ticks RANGE` | `0..256` | Zero-based ticks that receive tick and step spans and value summaries. `RANGE` is `START..END`, `START..`, `..END`, `all` or `none`; `END` is exclusive. |
| `--diagnostics-trace-streams A,B` | none | Streams whose values are summarized on each traced tick. Empty selects none. |
| `--diagnostics-trace-capacity N` | `100000` | Maximum retained timeline entries per recorder. Later entries are dropped and counted. |
| `--diagnostics-memory-every N` | none | Take a structured memory snapshot every `N` successful ticks. A final snapshot is requested when the run finishes and one before each root reconfiguration. Retention is capped: at the cap a periodic snapshot is skipped and a final, pre-reconfiguration or explicit one replaces the most recent periodic snapshot; if every retained snapshot is one of those, the request is refused and reported (see [Memory snapshots](#memory-snapshots)). |
| `--diagnostics-no-step-timing` | off | Do not time individual execution steps. Tick, commit and pipeline phase totals are still kept. |
| `--diagnostics-label TEXT` | none | A label copied into the run identity and the trace metadata. |

Aggregates (`count`, `total_ns`, `mean_ns`, `max_ns`) cover every tick. The tick range limits only what is written to the timeline. Structural events such as schedule, revision, plan and reconfiguration changes are recorded at any tick, subject only to the capacity.

Diagnostics are recorded by the `dataflow` and `reconf-dataflow` runtimes. Any other runtime logs a warning and ignores the flags.

## Run the demo

`examples/diagnostics/profile_demo.dsrv` has list and map state that grows with the trace, `def`s inlined from a second module (`demo_lib.dsrv`), a recursive accumulator and a runtime-supplied `dynamic` expression:

```dsrv
{{#include ../../../examples/diagnostics/profile_demo.dsrv}}
```

From the repository root:

```sh
cargo build --features diagnostics
./target/debug/trustworthiness_checker examples/diagnostics/profile_demo.dsrv \
    --input-file examples/diagnostics/profile_demo.input \
    --output-stdout \
    --diagnostics-dir /tmp/tc-diag \
    --diagnostics-trace-streams level,recent,custom \
    --diagnostics-memory-every 10
```

The input file supplies 40 ticks. The command prints the outputs as usual and then writes `/tmp/tc-diag/summary.json` and `/tmp/tc-diag/trace.json`. A failure to write them is logged as a warning and does not change the run's result. The excerpts below are abbreviated from one such run. Counts and structure are stable from run to run; times and, to a lesser degree, byte figures vary with the machine and build, and should be read as illustrative.

## Open the trace

Open `trace.json` at [ui.perfetto.dev](https://ui.perfetto.dev) or in Chromium at `chrome://tracing`. The file uses the Chrome trace event format. Spans are complete events and instants are thread-scoped; timestamps are microseconds from the start of the session. Each track is a named thread of one process:

| Track | Contents |
|---|---|
| `pipeline` | Runtime phases: `compile`, `input_wait`, `row_preparation`, `monitor_evaluation`, `output_packing`, `output_send`, `output_finish`, `input_drain`, and `reconfiguration`. Each span's arguments carry its tick and whether it is `synchronous_work` or `elapsed_wait`. |
| `monitor` | One `tick` span per traced tick (with `tick`, `revision` and `failed`), plus structural events: `definition_installed`, `plan_described`, `plan_switch`, `schedule_changed`, `revision_changed`, `dynamic_activation`, `tier_activation`, `tick_failed`. With `--diagnostics-trace-streams`, a `values` instant per traced tick. |
| `monitor steps` | One span per physical step per traced tick, named by its stream (or `region[...]`, or `whole-schedule native`). Arguments give the tick, step index, phase (`source`, `main` or `whole`) and the executor `path` that ran it. |
| `input <source>` | One track per input source that records its own phases; see [Limits](#limits). |

The `values` instants show the selected streams, bounded by a character limit with the variant and size of lists, tuples and maps first:

```json
{"name": "values", "args": {"tick": 1, "values": {"custom": "91", "level": "40",
 "recent": "list[2] [11, 40]"}}, "ph": "i", …}
```

Tick numbers are zero-based on every track. Row preparation, monitor evaluation and output packing of a tick carry the index of that tick, as do the monitor's `tick` span and its step spans, so `--diagnostics-trace-ticks 0..1` selects every span of the first tick. Waits and flushes (`input_wait`, `output_send`, `output_finish`, `input_drain`) carry the index of the next tick to be admitted when they ran. The `compile` span happens before any tick is admitted and carries tick 0, the same identity the first admitted tick's pipeline spans have.

The trace metadata (`otherData`) holds the report `schema`, the `label`, and `dropped_timeline_events`, the number of entries all timelines dropped.

## Read `summary.json`

The document starts with `"schema": "tc-diagnostics/1"`. Its top-level keys are `run`, `error`, `pipeline`, `sources`, `sources_dropped`, `dataflow` and `notes`. Use `python3 -m json.tool summary.json | less` or `jq` to browse it.

### Run and pipeline

`run` identifies the run: `started_unix_ms` and `elapsed_ns` (both measured from the session epoch, `DiagnosticsConfig::epoch`, which is also the origin of every timeline; a runtime or `RunArtifacts::for_monitor` reports the time since then, not since export), `crate_version`, the compiled `features`, `debug_assertions` (a build setting that says nothing about optimization level: a profile can enable it with optimizations on, or disable it without), your `metadata` (the specification path and command line) and the effective `step_timing`, `trace` and `memory` settings. `error` is `null` for a successful run and the error text otherwise; the report is written either way.

`pipeline.phases` aggregates the runtime adapter's work. Each entry has a `kind`:

- `synchronous_work` is code that ran without yielding: `compile`, `input_decode`, `input_projection`, `input_conformance`, `input_provenance`, `row_preparation`, `monitor_evaluation`, `output_packing`.
- `elapsed_wait` is the time an `await` took to complete: `input_wait`, `output_send`, `output_finish`, `input_drain`, `reconfiguration`.

The demo's phases, with times in nanoseconds:

```json
{"phase": "compile",            "kind": "synchronous_work", "count": 1,  "total_ns": 659722, …}
{"phase": "input_wait",         "kind": "elapsed_wait",      "count": 1,  "total_ns": 28388, …}
{"phase": "row_preparation",    "kind": "synchronous_work", "count": 40, "total_ns": 9545, …}
{"phase": "monitor_evaluation", "kind": "synchronous_work", "count": 40, "total_ns": 3902726,
 "mean_ns": 97568, "max_ns": 1751537}
{"phase": "output_packing",     "kind": "synchronous_work", "count": 41, "total_ns": 26000, …}
{"phase": "output_send",        "kind": "elapsed_wait",      "count": 1,  "total_ns": 532837, …}
```

`pipeline.counters` holds counts and maxima such as `logical_ticks`, `input_batches`, `output_rows` and `max_rows_per_output_batch`. The maximum evaluation here is 1.75 ms against a 98 microsecond mean: the first tick activates the `dynamic` expression and builds a plan, which the sections below attribute.

### Sources

`sources` has one entry per input source that records its own phases, each with its own `profile` (phases and counters such as `record_ticks`, `clock_only_ticks`, `empty_ticks`, `clock_samples`, `delivered_ticks`) and `timeline_dropped`. The demo reads a plain input file, which does not record, so `sources` is empty. See [Limits](#limits).

### `dataflow.ticks`

```json
"ticks": {"attempted": 40, "succeeded": 40, "failed": 0,
          "duration": {"count": 40, "total_ns": 3694945, "mean_ns": 92374, "max_ns": 1749698},
          "commit":   {"count": 40, "total_ns": 19292, …},
          "committed_streams": 160,
          "history_commit": {"count": 40, "total_ns": 12849, …}}
```

`duration` times each tick inside the monitor. The pipeline's `monitor_evaluation` times the whole call from the runtime adapter and is slightly larger. `commit` is the single temporal commit at the end of a successful tick, and `history_commit` is the move of retained values into history.

### Plans and steps

`dataflow.plans` lists each physical plan that executed, keyed by `(definition_version, plan, generation)`, with its steps in execution order. A step has a `phase` (`source` for the `dynamic`/`defer` prelude, `main` for the scheduled range), an `index`, and a `kind`:

- `graph`: one stream's canonical graph, with the number of quickened scalar `islands` inside it.
- `scalar_region`: a fused scalar region over whole streams, with its `members`.
- `whole_schedule_native`: one native artifact covering every stream, recorded once per plan.

Each step has `total` and a `paths` map keyed by the executor that actually ran it on each tick:

| Path | Meaning |
|---|---|
| `canonical` | A graph step evaluated entirely by the canonical interpreter. |
| `islands` | A graph step whose scalar islands all ran quickened. |
| `islands_declined` | A graph step where at least one island declined the row and ran canonically. |
| `region_native` | A scalar region run by its native artifact. |
| `region_quickened` | A scalar region run by the quickened interpreter. |
| `region_canonical` | A scalar region whose row the faster tiers rejected, run canonically. |
| `whole_native` | The whole schedule ran in one native artifact. |
| `whole_native_fallback` | The whole-schedule artifact deoptimized and the tick was replayed canonically. |

A fallback therefore shows as a path, not as an error. In the demo, `total` runs through `islands` while the other five streams are `canonical`:

```json
{"phase": "main", "index": 2, "kind": "graph", "stream": "total", "islands": 1,
 "total": {"count": 40, "total_ns": 26285, "mean_ns": 657, "max_ns": 3266},
 "paths": {"islands": {"count": 40, "total_ns": 26285, …}}}
```

### Streams and regions

`dataflow.streams` sums, per stream, the steps that evaluate exactly that stream, across plans. Each carries `paths` and, when the frontend located it, the `site` of the expression that produces the stream:

```json
{"stream": "level", "total": {"count": 40, "total_ns": 93912, "mean_ns": 2348, "max_ns": 5940},
 "paths": {"canonical": {…}},
 "site": {"file": "examples/diagnostics/profile_demo.dsrv", "line": 19, "column": 9,
          "snippet": "clamp(score, 0, 100)"}}
```

`dataflow.regions` holds fused scalar regions and whole-schedule artifacts as one aggregate each, with `members`. No per-stream split is inferred. It is empty in the demo; it fills when quickening fuses streams into a region or with the `jit` feature.

### Definitions and source sites

`dataflow.definitions` has one record per definition the monitor ran, with a `version`, the `installed_at_tick`, the `definition_key`, the `inputs`, and one record per stream. Each stream record gives its `id`, `name`, `role` (`output` or `auxiliary`), environment `slot`, `state_key`, the stream-level `site`, `node_count`, `unlocated_nodes`, and a `nodes` list. Version 0 is the definition installed when diagnostics were enabled; a monitor reset and each reconfiguration start another.

Each listed node has a `path` inside the stream (`3` is top-level node 3, `3/then/1` is node 1 of its `then` branch, `5/fn/0` is node 0 of the function bound at node 5), an `op` label, a `call_site` and a `definition_site`. For code inlined from a `def`, the call site is the call and the definition site is where that code was written:

```json
{"path": "0/fn/0/fn/0", "op": "binary Multiply",
 "call_site":       {"file": "examples/diagnostics/profile_demo.dsrv", "line": 18, "snippet": "weighted_sum(samples, gain)", …},
 "definition_site": {"file": "examples/diagnostics/demo_lib.dsrv",     "line": 4,  "snippet": "x * k", …}}
```

`dataflow.definition_sites` regroups this by definition. Each entry names a `def` body and lists the streams and call sites that inlined it, with the number of `nodes` each contributed. Here `clamp` (`demo_lib.dsrv:3`) was inlined into `level` as 6 nodes and `weighted_sum` (`demo_lib.dsrv:4`) into `score` as 6 nodes. It attributes nodes, not time: inlining removes the call boundary, so nothing is timed per call.

Nodes created programmatically carry no source location and count under `unlocated_nodes`. The node budget (`limits.max_model_nodes`, shared across a definition's streams) bounds the whole capture: once it is spent traversal, label formatting and source resolution stop, and `nodes_truncated` marks each stream that hit it. A definition's `node_count` is the number of nodes the capture visited, counted while it walked and not by a separate scan, and `node_count_is_lower_bound` is true when the node budget was reached and completeness was not established: the flag is conservative, so it is also set when the count happens to equal the size of the graph, and no second walk is made to tell the two apart. Operation labels clip names, keys and function displays to 48 characters. Line and column come from an index of line starts, and all source scanning in one capture shares `limits.max_source_scan_bytes` (16 MiB): past it a located node keeps its `file`, span and `snippet` but has no `line` or `column`, flagged `position_truncated`.

### Dynamic, scheduler and tiers

`dataflow.dynamic` times each `dynamic`/`defer` activation, grouped `by_outcome` (for example `activated` and `replaced`) and `by_expression`. An activation covers source resolution, body lookup or compilation, environment projection and state transfer at the source barrier. The demo's rule changes once mid-run, giving one `activated` (1.5 ms, the first-tick maximum seen above) and one `replaced` (0.3 ms).

`dataflow.scheduler` records `updates` (the timed schedule updates, one per tick in the demo), `changes` (how many changed the schedule) and `plan_switches` grouped by outcome: `built` for a new plan, `cached` for a reused one.

`dataflow.tiers.activations` times native tier activations, and with `jit` a `jit` object summarizes the compiled plan, artifacts and any backend error. Without `jit` it has count 0.

### Events

`dataflow.events` counts structural events by name. The demo reports:

```json
"events": {"definition_installed": 1, "dynamic_activation": 2, "plan_described": 2,
           "plan_switch": 1, "revision_changed": 2, "schedule_changed": 1}
```

Other names that can appear are `tick_failed`, `defer_sealed`, `quickening_changed`, `tier_activation`, `reset`, `reconfigured` and `reconfiguration_rejected`. A `revision_changed` event with cause `dynamic` marks a tick on which a `dynamic` expression changed the semantic revision.

### Collection operations

`dataflow.collections` counts the list and map operations the canonical evaluator performed on recorded ticks. Collections never run in the scalar tiers, so these counters cover every collection operation of the monitor:

```json
"collections": {"list_appends": 40, "list_append_buffer_replacements": 39,
                "list_append_replaced_entries": 780, "list_concats": 0,
                "list_concat_lhs_buffer_replacements": 0, "list_concat_lhs_replaced_entries": 0,
                "list_concat_rhs_appended_entries": 0, "list_concat_operand_reuses": 0,
                "list_scans": 40, "list_scan_entries": 200, "max_list_len": 40,
                "map_updates": 80, "max_map_len": 3, …}
```

A buffer replacement is a list append or concatenation whose result does not reuse the left operand's buffer, observed by comparing buffer pointers after the operation. It estimates copying, not an exact copy count: a replaced buffer usually means its entries were copied, but a unique buffer that reallocated to grow also shows as replaced. In the demo, `recent` appends to the list its own delay state still holds, so every append after the first replaces the previous buffer: 39 replacements of 1 to 39 entries, 780 entries in total (the first append starts from an empty list). That quadratic growth is the kind of cost the memory snapshots alone do not reveal.

A concatenation with an empty operand returns the other operand unchanged, so nothing moves: it is counted in `list_concat_operand_reuses` and adds nothing to the entry counts. Otherwise the right operand's entries are appended into the left buffer and counted in `list_concat_rhs_appended_entries`, as transferred rather than copied as a list. `list_scans` counts `List.map`, `List.filter` and `List.fold` applications, and `list_scan_entries` the entries of the scanned lists counted before each scan: an upper bound on the element callbacks, since a callback error stops a scan early. `map_updates` counts inserts and removals only: a persistent map copies the path to the changed entry, never the whole map, and cloning a map handle is never counted.

Counters are kept per tick scope. A monitor driven from inside another monitor's tick (for example by a host function) counts into its own recorder, and the outer tick resumes with its own counts afterwards.

### Memory snapshots

`dataflow.memory.snapshots` holds the retained snapshots, at most `max_snapshots` (32 by default). Each has a `label`, a `kind` (`periodic`, `final`, `before_reconfiguration` or `explicit`; an explicit snapshot's label is the text passed to it), the `definition_version` it describes, the number of successful `tick`s before it, its `components`, a `total`, the `work` done (values visited, keys and map-identity comparisons) and `budget_exhausted`. Stream names are only comparable within one `definition_version`. The demo takes four periodic snapshots (ticks 10 to 40) and a `final` one.

Retention is part of the report. Periodic snapshots are due only after a successful tick on the configured cadence; once the cap is reached a periodic snapshot is counted in `periodic_skipped` and its owners are not traversed. A `final`, `before_reconfiguration` or explicit snapshot replaces the most recent periodic one (`periodic_replaced`), so the earliest periodic snapshots stay as a baseline. When every retained snapshot is one of those mandatory kinds the request is refused: nothing is traversed, the store is unchanged, the label is added to `mandatory_skipped` (up to a bound; `mandatory_skipped_count` totals them) and `record_memory_snapshot` returns an error rather than an older snapshot. `snapshots_built` counts the traversals actually done.

Components are aggregated per stream and category rather than per node, so a snapshot stays small. The `path` names the owner, for example `streams/recent/scratch`:

| Category | Contents |
|---|---|
| `scratch` | Per-node value scratch of a stream's canonical evaluator, overwritten every tick. |
| `state` | Retained operator state: delay rings, lifted operands, latches, retained branch values. |
| `call_state` | Persistent function-call, match-arm and branch evaluators nested in a stream. |
| `dynamic_body` | The active `dynamic`/`defer` body evaluator and its projected environment. |
| `history` | Shared named-variable history rings. |
| `environment` | The monitor's environment row and retained row. |
| `program` | Immutable graphs, plans and caches, shared rather than per session. |
| `accelerator` | Quickened region registers and native artifacts, visible size only. |

Every component and the `total` carry the same fields:

| Field | Meaning |
|---|---|
| `fixed_bytes` | Storage counted from layouts and capacities: slots, rings, scratch vectors, boxes. |
| `total_bytes` | What the owner is charged: `fixed_bytes + unique_bytes + map_logical_bytes`. Charges add up to the snapshot total without double counting. |
| `bytes_with_shared_roots` | `total_bytes + shared_bytes`: the charge plus the shells of shared roots charged to an earlier owner. A partial figure, not a reachable total: the children of a shared list, map, union or newtype are not re-walked, so what hangs below a shared root is absent. It overlaps between owners and must never be summed. |
| `unique_bytes` | List, tuple, string and union/newtype payload first reached in this snapshot. |
| `shared_bytes` | The shells (buffer header and capacity) of list, tuple, string, union and newtype roots reached through a handle already charged elsewhere in this snapshot. Only the shell: children of a shared root are not re-walked. |
| `map_logical_bytes` | A logical per-root estimate of persistent map entry storage. Cloning a map handle is O(1) and a handle matched against an earlier root is not charged again, but matching covers only the 32 most recent same-sized roots (see `unresolved_map_identities`), and interior B+tree nodes shared between distinct roots cannot be observed, so each distinct root is charged its entries. See [Measured versus estimated](#measured-versus-estimated). |
| `list_entries`, `map_entries` | Entries first reached. |
| `max_list_len`, `max_map_len` | Longest list and map seen. |
| `opaque_values` | Function values and the capture lists of function nodes. They are counted (a capture list by its slot capacity) but their contents are not traversed. |
| `unresolved_map_identities` | Map roots charged without comparing against every earlier same-sized root (only the 32 most recent are compared). Such a root may be a clone charged twice; nonzero means charges are not guaranteed globally unique. |
| `truncated` | A depth or work budget stopped traversal below this point. |

The demo's final snapshot:

```json
{"label": "final", "tick": 40,
 "total": {"fixed_bytes": 14851, "unique_bytes": 2136, "shared_bytes": 7792,
           "map_logical_bytes": 560, "list_entries": 86, "map_entries": 10,
           "max_list_len": 40, "max_map_len": 3, "opaque_values": 2, "truncated": false},
 "work": 189, "budget_exhausted": false}
```

and the stream that grows, `recent`:

```json
{"path": "streams/recent/scratch", "category": "scratch", "stream": "recent",
 "fixed_bytes": 144, "unique_bytes": 1928, "shared_bytes": 952, "list_entries": 79, "max_list_len": 40, …}
{"path": "streams/recent/state",   "category": "state",   "stream": "recent",
 "fixed_bytes": 488, "unique_bytes": 0,    "shared_bytes": 2880, "max_list_len": 40, …}
```

The 40-element list is charged once, to the scratch that holds it; the state ring and the history entry that hold the same buffer report it as `shared_bytes`. Comparing snapshots from tick 10 to tick 40 shows `recent` growing with the trace. A component with `"opaque": true` exists but cannot be inspected without changing it, and its `note` says why: `streams/score/callables` counts only handles of host or language callables, and `execution/native_artifacts` appears when a native tier owns state. A stream whose state a native tier owns carries a note that the canonical copy shown may be stale. Snapshots never materialize native state to make it exact.

### Dependencies

`dataflow.dependencies` describes the current definition's data flow and aggregates cost along it. Its `streams` list gives each stream's `same_tick` reads, `historical` reads (`x[k]`) and, for `dynamic`/`defer` bodies, the `dynamic` dependencies of the body currently installed:

```json
{"stream": "custom", "output": true, "same_tick": ["rule"], "historical": [], "dynamic": ["level"]}
```

`outputs` gives, per output, the cost of its upstream closure over all three kinds of edge. `local` is the output's own step time and bytes, and is never repeated in the upstream totals. Byte figures come from the latest retained snapshot of the *current* definition version, named by `memory_snapshot` (and `memory_snapshot_tick`). When no snapshot of that version exists, after a reconfiguration for instance, `memory_snapshot` and every byte figure are `null`: the figures are unavailable, and an earlier definition's snapshot is never used even when a stream kept its name. Each cost has `bytes` (charged, which adds up across streams without double counting) and, in `local` only, `bytes_with_shared_roots`: a partial per-stream figure that adds the shells of shared roots charged to another stream, does not re-walk their children, and overlaps between streams. **Every upstream aggregate (`unique_upstream`, `shared_upstream`) reports `bytes_with_shared_roots` as `null`**: summing it would count a shared root once per stream, and removing the overlap would need a recursive walk of shared subtrees that the snapshot deliberately does not do. A cost also carries `opaque` and `truncated` flags when part of the stream could not be inspected or a budget cut it short, and `map_identity_uncertain` when a map root was charged without a complete identity check (map roots are compared against only the 32 most recent same-sized roots, so a clone outside that window is charged again and the charges are not guaranteed globally unique); the flags propagate into the aggregates they feed. The rest of the closure splits into streams no other output reaches (`unique_upstream`, `unique_streams`) and streams other outputs also reach (`shared_upstream`, with `shared_streams` naming the other outputs that reach each). `unique_region_ns` and `shared_region_ns` count fused regions whose members all lie in the unique closure and those that do not.

```json
{"output": "level", "local": {"step_ns": 93912, "bytes": 1200, "bytes_with_shared_roots": 1400},
 "unique_upstream": {"step_ns": 0, "bytes": 0, "bytes_with_shared_roots": null},
 "shared_upstream": {"step_ns": 668641, "bytes": 2184, "bytes_with_shared_roots": null},
 "shared_streams": [{"stream": "score", "also_reached_by": ["total", "recent", "summary", "custom"]}, …],
 "inputs": ["samples", "gain"]}
```

Every output in the demo reaches `score` and `level`, so those costs are shared. `level` and `total` have no unique upstream, while `recent`, `summary` and `custom` each have only themselves. Shared figures recur under every output that reaches them and are never divided. They are diagnostic totals, not a prediction of the time or memory saved by removing an output: evaluation order, fusion and retention change with the specification.

### Timeline and coverage

`dataflow.timeline` reports `retained`, `dropped` and `capacity` for the monitor's timeline; each source reports its own `timeline_dropped`. A nonzero `dropped` means later entries were discarded; the start of the run is complete, and aggregates are unaffected. Raise `--diagnostics-trace-capacity` or narrow `--diagnostics-trace-ticks` to retain more of what you need.

`dataflow.coverage` lists, in the report itself, what the numbers do not cover. The same limits are collected under [Limits](#limits).

## Use diagnostics from library code

The CLI wires these pieces together; an embedding application can do the same. The entry points exist only with the `diagnostics` feature.

- `DiagnosticsConfig` describes what to capture (output directory, step timing, trace selection and bounds, memory cadence, retention cap and budgets, registry `limits`, label, metadata). `DiagnosticsHub::new` takes it and owns it for one run, along with the recorders of input sources. Nothing is global: a run without a hub records nothing.
- `DataflowRuntimeBuilder::diagnostics(hub)`, the reconfigurable runtime builder's `diagnostics(hub)` and `GeneralRuntimeBuilder::diagnostics(hub)` hand the hub to a runtime, which times its pipeline phases, enables recording on its monitor and writes `summary.json` and `trace.json` when it finishes and the configuration names an output directory. `GeneralRuntimeBuilder` ignores the hub when the selected runtime is not a dataflow runtime.
- `InputPipeline::diagnostics(hub)` registers recorders with sources that record their own phases when they open.
- For a monitor driven directly, `DataflowMonitor::enable_diagnostics(&config)` installs a recorder, replacing any earlier one. `diagnostics_enabled`, `diagnostics_report` (a `DataflowDiagnosticsReport` with `to_json`), `diagnostics_timeline` and `record_memory_snapshot(label)` read and extend it. `RunArtifacts::for_monitor(&hub, &monitor, pipeline)` builds the same report and Chrome trace as the runtime does, and `RunArtifacts::write(directory)` writes them. The optional `pipeline` argument carries phases the caller timed itself.

A report is built on demand from the recorded aggregates; building it does not change the monitor. File output happens only when `RunArtifacts::write` is called or a runtime finishes.

## Measured versus estimated

- **Durations are wall-clock elapsed time**, taken with `std::time::Instant`. They are not CPU time. On a loaded machine a synchronous span still includes preemption.
- **An `elapsed_wait` includes whatever the awaited future did while it was polled**, and time the executor spent on other tasks. For example, a replayed journal decodes records synchronously inside the poll, so decoding appears inside the wait unless the source records it as `input_decode`.
- **Memory figures are structured estimates.** They are assembled from explicit owners, layouts, capacities and the payloads of retained values. They are not allocator measurements and are unrelated to process RSS. Use an allocator profiler when you need those.
- **Lists, tuples, long strings and union/newtype payloads are deduplicated by buffer identity.** A buffer is charged once per snapshot, to the first owner that reaches it. Later handles to the same buffer add to `shared_bytes`. Strings that fit inline have no heap payload and are not charged.
- **Maps are persistent B+trees with reference-counted nodes.** Cloning a map copies a handle. A root is matched by pointer equality against only the 32 most recent same-sized roots: an identical handle reached again within that window counts as `shared_bytes` and is not charged again, while a clone reached outside it can be charged again and increments `unresolved_map_identities` (charges are then not guaranteed globally unique). A map updated from another root shares most interior nodes with it, but that sharing is not observable through the public API. The entry storage of each distinct root is therefore reported as `map_logical_bytes`, a logical footprint that may count interior nodes shared between distinct roots more than once. Read it as an upper estimate of the map's size, not as bytes held.
- **Traversal is bounded.** A depth limit of 32 and a work limit of 2,000,000 units per snapshot are the defaults, and one budget spans the whole snapshot. A value visited, a map key and a map-identity comparison each cost one unit, charged before the work is done, so a snapshot never does more than the limit however large the state is. Lengths, capacities and string sizes are read, not scanned. When the work limit stops traversal, `budget_exhausted` is set and everything not yet reached is marked truncated or not inspected; a depth stop sets `depth_truncated`. Map identity is compared against only the 32 most recent roots of the same size; an older root may be charged again, counted in `unresolved_map_identities`.
- **Timing semantics.** A span is the wall-clock time between two `Instant` readings. Synchronous spans time code that did not yield; `elapsed_wait` spans time an `await`. Step spans cover the whole step including quickened islands; a failed step is timed up to the error and recorded as outcome `failed`, not as an executor path.

## Limits

- Fused scalar regions and whole-schedule native artifacts are timed as one aggregate with their members. There is no per-stream split. `dataflow.streams` includes only steps that evaluate one stream.
- Inlined `def` bodies have no timed call boundary. `definition_sites` attributes nodes to definitions, not time.
- Nodes of `dynamic`/`defer` bodies compiled at runtime are not in the model map. Their activation is timed under `dynamic`, and their evaluator memory appears under `dynamic_body`.
- A `TypedDataflowMonitor` is not instrumented, and once it is promoted to its direct native entry, evaluation bypasses the recorded monitor.
- Only RPIO journal sources record their own phases. Replay reports `input_decode` (the time of polling its stream, which reads and decodes a record synchronously and so covers both), `input_projection`, `input_conformance` and `input_provenance`; a live journal reports `input_wait`, an awaited read whose time includes decoding inside the transport, and the same later phases. A conformance or provenance check that fails is still timed, and counted in `conformance_errors` or `provenance_errors`. Other sources appear only through the pipeline's `input_wait`.
- Recording buffers and registries are bounded. When a timeline is full it keeps the earliest entries, drops later ones and counts them in `dropped`. Periodic memory snapshots beyond the cap are counted in `periodic_skipped`; a mandatory snapshot replaces a periodic one, or is refused and listed in `mandatory_skipped`. Definitions, plans, dynamic expressions and dependency outputs beyond their `limits` are counted in `dropped`, and input sources beyond the registry bound in `sources_dropped`. Detailed spans cover only the traced tick range.
- Function capture lists and native-owned state are opaque to memory snapshots. A function's captured values are not charged, so a closure that retains a large list is under-reported.
- Persistent map storage is a logical per-root estimate; sharing between roots after an update is not observable.
- Runtimes other than `dataflow` and `reconf-dataflow` ignore diagnostics.
- There is no per-node timing and no allocator integration. Collection counters cover list appends, concatenations, scans and map updates in the canonical evaluator; other copies, such as values cloned into lifted operands or history, are not counted.

The generated [CLI reference](../reference/cli.md) lists the flags of a default build only, so the diagnostics flags appear in the `--help` output of a build with the feature and in the table above.
