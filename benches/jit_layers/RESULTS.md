# JIT optimization-layer results

The unoptimized integrated graph JIT and original six-way suite are archived at
commit `2c7d8a06` and branch `archive/dataflow-integrated-jit-v1`.

## Configuration

- Date: 2026-07-31.
- CPU: Intel Core i7-13700F, executable pinned to logical CPU 10, a 5.2 GHz
  P-core. Compilation was not pinned.
- Profile: `bench-fast`.
- Criterion: flat sampling, 20 samples; 10,000 untimed events and 100,000 timed
  events for sustained results.
- Archived integrated-layer executable:
  `target/bench-fast/deps/integrated_jit_layers-a3191e1bb27a446d`.
- Six-way executable:
  `target/bench-fast/deps/streamir_comparison-611617dbfb79bc47`.
- Every scenario was checked event-by-event against the canonical monitor for
  both activation policies before benchmarks were registered.

## Post-refactor validation

The implementation and public benchmark name are now simply `jit`. The
interpreter tier uses the `jit` feature. The validation executable was
`target/bench-fast/deps/jit_layers-f1e2a1409c0943f5`, pinned to the same CPU 10.

| Scenario | Eager ns/event | 1,024-event hotness ns/event | Archived eager ns/event |
|---|---:|---:|---:|
| arithmetic | 15.68 | 15.71 | 15.72 |
| accumulator | 41.92 | 41.71 | 44.33 |

The refactor and diagnostics add no measurable hot-path regression. A separate
six-way validation run registered the interpreter tier under the collision-free
Criterion key `jit` and measured arithmetic at 16.71 ns/event. All six scenarios
are still compared event-by-event before any requested benchmark filter runs.

The combined implementation includes all six planned layers:

1. Boundary-scaling and source-to-N-event benchmarks.
2. Checked constant folding, integer-add reassociation, dead-node elimination,
   CSE through demand-driven code generation, and one load per input.
3. Batched graph-module finalization plus an independently selectable hotness
   policy.
4. A raw scalar environment for fused native execution and raw publication
   between dependent streams.
5. One native function/call for a complete pure scalar run.
6. Resumable presence side exits and evaluator-scheduled scalar temporal state
   feeding a native region.

`all_no_hotness` means all of the above except delayed activation: native code
is built eagerly. `all_with_hotness` uses the same generated code after 1,024
canonical events.

## Sustained execution

Values are nanoseconds per event. The archived v1 column is from the immediately
preceding pinned run; small cross-run machine drift is visible in the unchanged
comparison implementations.

| Scenario | Archived v1 graph JIT | All, eager | All, 1,024-event hotness | Eager improvement |
|---|---:|---:|---:|---:|
| arithmetic | 20.01 | 15.72 | 15.87 | 1.27x |
| chain32 | 20.65 | 13.88 | 14.23 | 1.49x |
| conditional | 20.00 | 15.76 | 15.97 | 1.27x |
| threshold | 19.48 | 13.47 | 13.79 | 1.45x |
| window3 | 124.92 (canonical fallback) | 72.73 | 71.57 | 1.72x |
| accumulator | 54.33 (canonical fallback) | 44.33 | 45.20 | 1.23x |

Hotness has no intended steady-state effect after the 10,000-event warmup. The
small eager/hot differences are run-to-run measurement noise.

## Backend startup

Microseconds to construct a monitor from an already checked specification:

| Scenario | All, eager | All, hotness | Startup reduction |
|---|---:|---:|---:|
| arithmetic | 65.77 | 1.43 | 46.1x |
| chain32 | 32.71 | 4.84 | 6.8x |
| conditional | 69.98 | 1.79 | 39.1x |
| threshold | 23.25 | 1.22 | 19.0x |
| window3 | 35.71 | 2.02 | 17.7x |
| accumulator | 29.81 | 1.34 | 22.3x |

The eager pure-run compiler is compiled only once; it does not also build
unused per-graph functions. The optimizer reduces `chain32` backend startup
from the archived 180.63 microseconds to 32.71 microseconds.

## Source-to-N-event lifetime

Each cell is `eager / hotness` in microseconds and includes parsing, monitor
construction, and all N events. At exactly 1,024 events the hot configuration
has not compiled yet; activation occurs before event 1,025.

| Scenario | 1 event | 256 events | 1,024 events | 4,096 events |
|---|---:|---:|---:|---:|
| arithmetic | 100.41 / 16.93 | 106.48 / 34.28 | 133.82 / 87.11 | 181.70 / 245.33 |
| chain32 | 55.57 / 23.73 | 59.64 / 124.67 | 70.94 / 431.87 | 118.45 / 514.35 |
| conditional | 109.75 / 19.65 | 117.68 / 45.02 | 132.00 / 121.07 | 189.78 / 288.03 |
| threshold | 38.24 / 13.80 | 42.54 / 20.99 | 53.85 / 42.16 | 97.69 / 112.19 |
| window3 | 64.55 / 21.72 | 85.35 / 54.97 | 143.20 / 152.09 | 369.79 / 429.10 |
| accumulator | 52.86 / 17.71 | 64.62 / 32.32 | 100.63 / 74.69 | 241.66 / 254.95 |

The fixed 1,024-event policy is useful when most monitors die before the
threshold, but it is not a generally optimal threshold. `chain32` repays eager
compilation well before 256 events, so interpreting 1,024 events is especially
costly. Once delayed compilation occurs, its earlier interpreted work is an
additive cost that cannot be recovered relative to having compiled eagerly.
A production policy should estimate graph/run complexity and either select
eager compilation or use a much lower per-run threshold.

## Boundary scaling

Nanoseconds per event, with the same full `DataflowMonitor::evaluate` API:

| Shape | Checked | Integrated fused | Speedup |
|---|---:|---:|---:|
| one input, one operation | 26.67 | 14.29 | 1.87x |
| two inputs, three operations | 70.09 | 15.92 | 4.40x |
| one input, 32-operation chain | 388.62 | 14.31 | 27.16x |
| three dependent scalar streams | 67.79 | 24.42 | 2.78x |

The one-operation and 32-operation results are essentially identical, so the
remaining 14 ns floor is monitor/API and native-boundary work rather than
generated arithmetic. The three-stream case makes one fused native call but
must materialize three public stream values, exposing publication cost.


## Scheduled scalar temporal state (option 2)

The pre-change implementation is preserved at commit `3161b1b7` on branch
`archive/dataflow-jit-before-scheduled-state`. In the replacement, fixed and
recursive delays plus defaults are explicit evaluation/commit steps around a
native scalar region. The compact state remains evaluator-owned, promotes the
canonical history when a hot monitor activates, and converts back on permanent
deoptimization. The existing post-row commit barrier remains authoritative;
there is no artifact-owned state or per-event transactional snapshot.

Pinned sustained results from the same `jit_layers` executable are:

| Scenario | Archived eager ns/event | Scheduled eager ns/event | Scheduled hot ns/event | Eager speedup |
|---|---:|---:|---:|---:|
| window3 | 72.73 | 55.53 | 55.36 | 1.31x |
| accumulator | 44.33 | 34.92 | 34.81 | 1.27x |

Backend startup remains effectively unchanged. The post-change eager/hot
figures are 34.94/2.05 microseconds for `window3` and 28.91/1.35 microseconds
for `accumulator`, compared with the earlier 35.71/2.02 and 29.81/1.34
microseconds respectively.

The unchanged fused arithmetic path measured about 16.3 ns/event in two
immediate repeats, versus 15.68 ns/event in the earlier recorded run. Because
the scheduled temporal plan is not executed by a fused pure-scalar monitor,
this roughly 4% absolute difference is most plausibly cross-build/code-layout
or machine drift, but it is recorded rather than presented as proof of zero
regression.

## Three-tier scheduled-plan refactor

The scheduled-state implementation above is preserved at commit `d9cc8ca4` on
branch `archive/dataflow-jit-scheduled-state-v2`. The replacement introduces an
explicit `PlanBundle` (`PlanId`, semantic `ScheduledPlan`, and schedule-wide
`QuickPlan`) and one `ExecutionEngine` owning plan selection, the four-entry
schedule cache, and optional JIT coordination. Quickening metadata was removed
from `StreamProgram`; top-level quick code belongs to the plan while evaluator
state remains evaluator-owned. The old `specialization` module/interface was
renamed `quickening`. Fused artifacts are validated by `PlanId`, so native
eligibility no longer changes or rebuilds the quick plan.

Validation used `cargo test --profile bench-fast --features jit --lib
dataflow::`; all 143 dataflow tests passed. This covers differential checked/JIT
evaluation, fixed and recursive temporal state, hot activation, sparse presence
side exits, node deoptimization, dynamic schedules, and plan-cache reuse.

The following backend-startup medians are microseconds. Both revisions were
built independently with `bench-fast`; only their benchmark executables were
pinned to logical CPU 10. “Eager” includes Cranelift compilation, while “hot”
builds canonical + quick tiers and defers native compilation for 1,024 events.

| Scenario | Archived eager | Plan eager | Archived hot | Plan hot |
|---|---:|---:|---:|---:|
| arithmetic | 65.88 | 64.75 | 1.458 | 1.418 |
| chain32 | 31.32 | 30.57 | 4.799 | 4.442 |
| conditional | 67.69 | 67.77 | 1.800 | 1.755 |
| threshold | 22.36 | 22.05 | 1.282 | 1.197 |
| window3 | 35.06 | 34.21 | 2.056 | 1.974 |
| accumulator | 29.14 | 28.82 | 1.341 | 1.313 |

Construction is unchanged to modestly faster: eager changes range from +0.1%
to -2.4%, and deferred-native construction from -2.1% to -7.4%. The largest
hot-start improvement is `chain32`, where the backend-independent quick plan is
no longer rebuilt merely because a native graph exists.

Sustained runs used 10,000 warm events and 100,000 timed events. Repeated,
interleaved runs exposed substantial frequency/thermal drift on this machine:
for example the unchanged archived accumulator varied from 34.86 to 41.93
ns/event, and archived window3 from 55.10 to 57.48 ns/event. The closest
non-throttled paired medians were:

| Scenario | Archived ns/event | Plan ns/event | Interpretation |
|---|---:|---:|---|
| arithmetic | 15.69 | 15.46 | no material regression |
| chain32 | 13.84 | 13.76 | no material regression |
| conditional | 15.78 | 15.96 | about +1.1% |
| threshold | 13.63 | 13.62 | unchanged |
| window3 | 55.10 | 55.49 | about +0.7% |
| accumulator | 34.86 | 35.42 | about +1.6% |

Given the observed cross-repeat spread, these data support “no architectural
throughput regression above roughly 2%,” not sub-percent claims. The plan
refactor does not yet turn a temporal plan into one native tick function:
`window3` and `accumulator` still use evaluator-scheduled Rust temporal state
around a native scalar region. That is the next performance architecture step,
now isolated behind the plan/JIT boundary rather than encoded in the graph IR.

## Complete temporal-kernel stage

The three-tier plan stage immediately above is preserved at commit `b00a1042`
on branch `archive/dataflow-three-tier-plan-v1`. This next stage lowers the
eligible delay/default boundary, surrounding scalar graph, output store, and
temporal commit into one native call. Its raw state buffer is evaluator-owned;
hot activation promotes existing history, while a side exit materializes the
same state and reconstructs scalar lifting state for canonical continuation.
The complete kernel replaces, rather than accompanies, the old scalar-only
artifact for that stream.

The table deliberately keeps this stage separate from the scheduled-plan
numbers. These are the closest interleaved eager (`all_no_hotness`) medians from
independently built `bench-fast` binaries, with only each resulting executable
pinned to logical CPU 10. Each timed iteration contains 100,000 events after
10,000 warm events.

| Scenario | Three-tier plan ns/event | Complete temporal kernel ns/event | Change |
|---|---:|---:|---:|
| arithmetic | 15.62 | 15.63 | +0.1% control |
| chain32 | 13.85 | 13.59 | -1.9% control |
| conditional | 15.60 | 15.83 | +1.5% control |
| threshold | 13.71 | 13.54 | -1.2% control |
| window3 | 56.10 | 26.31 | **-53.1% (2.13x)** |
| accumulator | 35.87 | 25.47 | **-29.0% (1.41x)** |

Only `window3` and `accumulator` select the new kernel, so the four scalar rows
are controls. Their -1.9% to +1.5% spread is consistent with the machine/code-
layout variation already observed above and gives no evidence of a scalar-path
regression. Deferred activation reaches essentially the same temporal steady
state after its 1,024-event threshold:

| Scenario | Three-tier plan hot ns/event | Complete kernel hot ns/event | Hot speedup |
|---|---:|---:|---:|
| window3 | 55.62 | 26.38 | 2.11x |
| accumulator | 35.95 | 25.00 | 1.44x |

The startup tradeoff is real and is shown separately. “Eager” includes
Cranelift compilation; “hot” constructs the canonical and quick tiers but
defers native compilation. The implementation avoids compiling a duplicate
scalar fallback, reducing initial prototype compilation by 25-27%, but the
larger temporal functions still cost more to compile than scalar regions.

| Scenario | Plan eager us | Complete eager us | Eager cost | Plan hot us | Complete hot us |
|---|---:|---:|---:|---:|---:|
| window3 | 35.10 | 123.68 | 3.52x | 1.984 | 1.925 |
| accumulator | 29.54 | 77.45 | 2.62x | 1.306 | 1.299 |

This stage is intentionally narrower than a general plan-wide temporal JIT.
It covers the benchmark shapes: scalar input delays or one-tick recursive
delay, `default`, and the enclosing scalar graph. Unsupported temporal shapes
continue through the scheduled temporal-state path, so planning, graph IR,
quickening, and native lowering remain independently replaceable.

## Strict scheduler-plan architecture

The per-stream complete-kernel implementation above is preserved at commit
`e763adae` on branch `archive/dataflow-complete-temporal-kernel-v1`. This stage
makes the scheduler-produced `ScheduledExecutionPlan` the semantic execution
contract for every tier. It contains stable plan, value, and temporal-state
identities; ordered stream programs and publication slots; explicit temporal
evaluation and commit operations; and conservative failure/state effects.

Canonical execution and quickening interpret derived physical views of that
same plan. The JIT receives the plan rather than an independent graph list and
schedule. When the complete schedule is supported and infallible, it lowers all
streams, raw intermediate publications, temporal reads, and end-of-tick state
writes into one native function. Per-stream scalar native regions remain a
schedule-independent fallback. A native side exit materializes packed state
through the plan's stable state mapping and replays the tick canonically; it
does not expose a half-committed plan. Cached schedules retain their plan IDs,
while schedule-wide native artifacts are invalidated when the active plan
changes.

The table keeps this architectural stage separate from the per-stream kernel
stage. Both revisions were built independently with `bench-fast`; only the
resulting executables were pinned to logical CPU 10. Values are medians after
10,000 warm events and 100,000 timed events.

| Scenario | Per-stream kernel ns/event | Strict plan ns/event | Change |
|---|---:|---:|---:|
| arithmetic | 15.35 | 15.43 | +0.5% control |
| chain32 | 13.66 | 14.12 | +3.4% control |
| conditional | 15.40 | 15.53 | +0.8% control |
| threshold | 13.54 | 13.94 | +2.9% control |
| window3 | 26.67 | 15.99 | **-40.0% (1.67x)** |
| accumulator | 25.06 | 14.78 | **-41.0% (1.70x)** |

Only the temporal rows use the new schedule-wide artifact. The scalar controls
span +0.5% to +3.4%; this is somewhat above the closest earlier paired spread,
so it should be treated as a small possible code-layout/object-layout cost, not
claimed as zero regression. Eager and delayed activation converge after the
hotness threshold:

| Scenario | Strict eager ns/event | Strict hot ns/event |
|---|---:|---:|
| window3 | 15.99 | 15.98 |
| accumulator | 14.78 | 14.77 |

The richer plan adds a small cost when native compilation is deferred, but the
schedule-wide compiler does not increase eager construction in these two
cases:

| Scenario | Per-stream eager us | Strict eager us | Per-stream hot us | Strict hot us |
|---|---:|---:|---:|---:|
| window3 | 123.72 | 119.02 | 1.962 | 2.122 |
| accumulator | 77.26 | 74.78 | 1.323 | 1.459 |

The hot-plan construction increase is 8.1% for `window3` and 10.3% for
`accumulator`, or 0.16 and 0.14 microseconds in absolute terms. Eager
construction improved by 3-4% in this run despite compiling the wider plan.

A new boundary case places a scalar producer before the temporal stream and a
scalar consumer after it. The checked interpreter measured 93.01 ns/event and
the single plan artifact 23.49 ns/event, a 3.96x speedup. This specifically
checks that the optimization crosses stream boundaries instead of disguising a
per-stream temporal call behind plan-shaped metadata.

Validation used `cargo test --profile bench-fast --features jit --lib
dataflow::`; all 145 dataflow tests passed. A non-JIT `cargo check --profile
bench-fast` also passed. The current native plan deliberately supports only
infallible scalar schedules with the implemented delay/default forms. Other
plans still execute the same semantic plan through quickened or canonical
physical views; broadening native coverage does not require changing the graph
IR or scheduler/executor contract.
