# Benchmark dashboard contract

The key dashboard separates end-to-end runtime measurements from sustained monitor measurements. Runtime routes include input handling, the asynchronous runtime, output handling, the first 1,024 interpreted events on delayed-JIT routes, and any native compilation during the measured workload.

The `jit/sustained/<fixture>/<route>` matrix uses six fixed fixtures. Each iteration runs 10,000 untimed setup events followed by 100,000 timed events. The `untyped_value`, `checked_canonical_value`, `checked_quickened_value`, and `native_value_eager` routes use `DataflowMonitor::evaluate` with `Value` rows. The `native_direct_*` routes use the tuple-typed monitor interface. `native_direct_warmed` is configured to activate after 1,024 events and is checked to be native before timing. The six current route names are:

- `untyped_value`
- `checked_canonical_value`
- `checked_quickened_value`
- `native_value_eager`
- `native_direct_eager`
- `native_direct_warmed`

Dashboard history is classified by the source commit timestamp, rather than the later date on which a backfill may have collected the result. Source inspection establishes these benchmark-ID epochs:

- Before `242b5bd9`, plain threshold and time-dependent `dataflow` IDs denote untyped execution.
- From `242b5bd9` until `33fa9871`, those plain IDs denote checked quickened execution. Explicit `_untyped` measurements in the same run remain a distinct series.
- From `33fa9871`, the plain IDs denote checked canonical execution.
- Surviving pre-`33fa9871` plain dynamic-paper and hard-dynamic/defer IDs denote untyped execution. Specialised-era results were already migrated to explicit quickened IDs during backfill.
- MAPLE and deferred-expression `_untyped_dataflow` IDs are historical spellings of `_dataflow_untyped`. The MAPLE `typed_dataflow` route introduced at `242b5bd9` is the historical checked quickened route.

The former sustained `all_no_hotness` and `all_with_hotness` routes both reached the same native Value-interface steady state after setup. Historical values from either spelling feed `native_value_eager`, with an exact current measurement taking priority. They are not aliases of the typed direct warmed route.

Build the sustained benchmark with the repository's benchmark profile and jemalloc:

```sh
cargo bench --profile bench-fast --features jit,jemalloc --bench jit_layers --no-run
```

Choose an otherwise-idle performance core from `lscpu -e=CPU,CORE,MAXMHZ`, then pin only the compiled benchmark executable and select the sustained group:

```sh
taskset -c <P_CORE> target/bench-fast/deps/<jit_layers-benchmark-binary> 'jit/sustained' --bench
```

The benchmark itself supplies the 10,000-event untimed setup and 100,000-event timed workload. Keep the same core, filter, profile, features, and machine load when comparing runs.
