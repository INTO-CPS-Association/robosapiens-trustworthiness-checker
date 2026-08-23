# Output pipeline benchmarks

## Scope and status

`benches/output_pipeline.rs` is a Criterion target for comparing the local
output writer fast path with output stages. It uses deterministic in-memory
backends; it does not exercise MQTT, Redis, ROS, serialization, a broker, or a
network. The target is useful for relative implementation measurements only
when run under a controlled local setup.

This revision intentionally records no performance baseline. The compile check
and the focused runs used while correcting the harness were correctness smoke
checks, not a CPU-pinned full-matrix measurement session. In particular, the
numbers formerly recorded in this document are removed rather than carried
forward under a changed accounting model.

The target's setup assertions verify all of the following before a case is
measured:

- the requested logical tick count;
- the requested update count for the selected shape;
- one unique synthetic logical ID for every requested tick;
- the requested maximum logical ticks per producer physical batch; and
- the resolved per-destination logical-ID and update expectations.

The `mixed` shape cycles one width-one singleton tick, one simultaneous tick,
and one one-row packed tick. It therefore deliberately has a shape-dependent
update count, but never a shape-dependent logical tick multiplier.

## Workloads

The default target contains the following cases. These are workload dimensions,
not performance claims:

- direct writer versus one-destination pipeline without stages for singleton,
  simultaneous, packed, and mixed shapes, widths 1, 8, 32, and 128, with 64
  logical ticks grouped into producer batches of at most 16 ticks;
- buffer and buffer/coalesce stage orders for 256 singleton logical ticks,
  buffer capacities 1, 8, 64, and 256, plus a count-only coalesce-only case;
- count-only coalescing with tick limits 4, 16, 64, and 256, update limits 32,
  256, and 4096, and timed delay bounds of 1 ms and 5 ms;
- producer-native packed batches versus singleton batches followed by generic
  coalescing;
- two- and four-destination partitioning and partial mirroring for 128 packed
  logical ticks of width 32; and
- one two-destination failure-cleanup case.

The full target is intentionally broader than a short interactive check. Use a
single Criterion filter when validating a change; do not treat a focused run as
a baseline for the unrun matrix.

## Metric definitions

The harness keeps producer and destination accounting separate.

### Producer submission and admission

For each input physical batch, the producer records a submission timestamp,
then calls `OutputWriter::feed`. A successful return from `feed` is the
producer-side admission point. The elapsed `feed` wait is copied to each
logical tick in that physical batch and is reported internally as an admission
wait. It is a backpressure proxy: the harness does not inspect an output
stage's private queue, so it does not report an exact queue depth or exact peak
queue size.

`flush` and `close` are awaited after all feeds. Criterion's operation timing
therefore includes producer feeds, the flush barrier, and the close barrier.

### Backend start and completion

Each synthetic backend assigns a physical-batch ID when its sink operation
first starts. It extracts the logical IDs from the batch before recording the
backend-start event. Backend completion is recorded only after interface
validation, synthetic CPU work, and the configured async delay finish.

Start and completion latency samples are one sample per logical tick delivered
to that destination, not one sample per physical batch. A coalesced physical
batch carrying 64 logical ticks therefore contributes 64 logical latency
samples and one physical-batch count. Latency is measured from the logical
submission timestamp to the corresponding backend event; the separate
admission-wait series identifies the producer admission interval.

The physical-ID-to-logical-ID map is retained for invariant checks. It prevents
coalescing from consuming one timestamp and then labeling one physical batch as
one p50/p95/p99 observation.

### Multi-destination counts

Destination statistics are keyed by resolved destination ID. For each successful case, setup computes the expected logical IDs and
update count after applying the resolved destination bindings. The run asserts
those values independently for every destination at backend start and
completion. Partition totals therefore describe each destination's delivery;
they are not silently compared with the single producer total. Mirroring may
legitimately cause the sum across destinations to exceed the producer total.

## Synthetic cost model and limits

`SyntheticCost` has three explicit components:

- `per_batch_work`: a deterministic arithmetic loop run once per backend
  physical batch;
- `per_update_work`: the same kind of loop run once for every update in that
  physical batch; and
- `delay`: one bounded `smol::Timer` delay per backend physical batch.

The current cases use zero per-update work. Stage/coalescing cases use a 50 µs
per-batch delay and a small fixed per-batch arithmetic loop; the direct/fast
path and routing cases use zero synthetic cost. No claim about a per-update
cost is made by those cases. If a per-update experiment is added, its value
must be stated as synthetic loop iterations and kept identical across the
compared paths.

These costs are not transport costs. Timer granularity, the local executor,
CPU frequency, compiler optimization, interface validation, allocation, and
Rust scheduling all affect the result. The harness has no broker queue, remote
service, serialization, network contention, or transport acknowledgement.
A result from this target must not be generalized to Redis, MQTT, ROS, or a
particular deployment without a separate transport-backed experiment.

## Reproducibility and provenance

Use the repository's `bench-fast` profile for both compilation and benchmark
execution:

```sh
cargo bench --profile bench-fast --bench output_pipeline --no-run
```

Cargo prints the resulting executable path, normally of the form
`target/bench-fast/deps/output_pipeline-<hash>`. Run a focused group using that
path, for example:

```sh
target/bench-fast/deps/output_pipeline-<hash> 'mixed/width_8' --bench
target/bench-fast/deps/output_pipeline-<hash> 'output/routing/partition/2' --bench
target/bench-fast/deps/output_pipeline-<hash> 'output/coalescing/tick_limit/64' --bench
```

For a performance run, identify an otherwise-idle P-core with
`lscpu -e=CPU,CORE,MAXMHZ` and pin only the resulting benchmark executable:

```sh
lscpu -e=CPU,CORE,MAXMHZ
taskset -c <P_CORE> target/bench-fast/deps/output_pipeline-<hash> 'output/stages' --bench
```

Do not pin `cargo` or `rustc`. Record the exact source revision, feature set,
CPU/core selection, filter, sample configuration, and system load alongside
any numbers that are intentionally published. A compile-only check or a
focused correctness smoke run is not sufficient provenance for a performance
claim.

The corrected harness was compile-checked with the command above and exercised
with focused mixed-shape, partition-routing, and tick-coalescing filters. No
full matrix was run for this documentation update, and no timing values from
those smoke runs are retained here.

## Interpretation

Use Criterion's timing output together with the harness invariants:

- compare cases with the same logical tick/update workload and the same
  `SyntheticCost`;
- distinguish producer admission wait from backend start and completion
  latency;
- inspect physical batch counts separately from logical tick counts;
- interpret admission wait as a backpressure proxy, never as queue depth; and
- keep any conclusion scoped to this deterministic local sink.

The target does not by itself decide whether buffering or coalescing is useful
for a production transport. That decision requires a controlled workload for
the actual backend and explicit transport-level observations.
