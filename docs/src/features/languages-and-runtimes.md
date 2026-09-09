# Languages and runtimes

The TC accepts two specification languages. DSRV is the default stream-monitoring language; MSTLO selects a separate Signal Temporal Logic runtime. Runtime choice affects input timing, output shape, and whether a finite file or live control route is appropriate.

## Languages

### DSRV

Use the default `dsrv` language for declarations such as `in x`, `out z`, and equations over current and historical stream values. The CLI's DSRV-compatible semantics are `untimed`, `typed-untimed`, and `gradual-typed-untimed`; the default is `gradual-typed-untimed`. The distributed DSRV runtime currently accepts only `untimed`.

Start with [writing and running DSRV models](../tutorials/write-dsrv-monitor.md). Language-level `dynamic` and `defer` are distinct from root monitor reconfiguration.

### MSTLO

Start with [Monitor timed signals with MSTLO](../tutorials/mstlo-monitor.md) for a checked-in STL model, timestamped trace, exact delayed verdicts, and the semantics/synchronization choices. Select MSTLO with `--language mstlo`. The MSTLO runtime is selected automatically, so omit `--runtime`. Its main CLI-specific controls are:

- `--mstlo-algorithm naive|incremental`;
- `--mstlo-synchronization none|zero-order-hold|linear`; and
- repeatable `--mstlo-vars name=value` bindings.

Redis knowledge input produces ordinary DSRV `Value` values and is not supported for MSTLO. ROS MSTLO mappings use the generated `MstloTimedValue` message rather than the ordinary DSRV ROS value mapping.

## DSRV runtime choices

| Runtime | Use when | Important boundary |
|---|---|---|
| `async` | An asynchronous DSRV execution path is required | Uses asynchronous input/output handling. |
| `dataflow` | Default DSRV execution for a finite or live monitor | `buffered` is the default execution policy; `synchronous` is also supported. |
| `semi-sync` | A semisynchronous DSRV execution path is required | Do not treat it as a reconfiguration runtime. |
| `distributed` | A distributed DSRV deployment is configured | DSRV semantics are restricted to `untimed`; the distributed deployment needs its transport and work assignment configured. |
| `reconf-semi-sync` | Replace the running DSRV generation through a live control route | File input is rejected; a control-capable live source is required. |
| `reconf-dataflow` | Use in-place dataflow reconfiguration | File input is rejected; reconfiguration flags apply only here or to `reconf-semi-sync`. |

The `--execution-policy` values are `buffered` and `synchronous`. They describe when supported runtimes accept the next logical tick; they do not turn transport delivery into a transaction.

## Selecting a runtime

A running-total program demonstrates the default DSRV runtime:

```dsrv
in x: Int
out z: Int
z = default(z[1], 0) + x
```

The repository stores it as `examples/counter.dsrv` with four input values in `examples/counter.input`:

```sh
cargo run -- examples/counter.dsrv \
  --input-file examples/counter.input \
  --output-stdout
```

For live reconfiguration, start from an addition rule:

```dsrv
in x: Int
in y: Int
out z: Int
z = x + y
```

The repository stores it as `examples/simple_add.dsrv`. Start it with a live source so the control message can arrive:

```sh
cargo run -- examples/simple_add.dsrv \
  --mqtt-input \
  --output-stdout \
  --runtime reconf-semi-sync \
  --reconf-topic reconf
```

The second command remains in the foreground and is completed by a later control message or process termination; see [Reconfigure a running monitor](../tutorials/reconfigure-running-monitor.md).

For exact accepted values and Clap-visible defaults, see the [CLI reference](../reference/cli.md). For runtime ownership and cutover mechanics, see [dataflow architecture](../architecture/dataflow/index.md) and [reconfiguration architecture](../reconfiguration.md).
