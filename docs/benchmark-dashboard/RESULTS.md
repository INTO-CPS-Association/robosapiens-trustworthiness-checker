# Local quickening/JIT key-benchmark results

These are historical local measurements; [README.md](README.md) defines the current dashboard routes and measurement boundaries.

Measured on 2026-08-01 using `bench-fast`. Each benchmark executable was built
normally and then pinned to logical CPU 10, a 5.2 GHz P-core on an Intel Core
i7-13700F. The existing Criterion sampling, warmup, and measurement settings
were retained.

The specialised and JIT entries both start from the same already-checked DSRV
specification and use the same buffered Dataflow runtime and output handler.
“Specialised” constructs and executes the scheduler-plan quickening tier. “JIT”
starts in that tier and requests native compilation after 1,024 logical events;
the measured duration includes quickened warmup and any native compilation.

Values are median time for the complete input workload.

| Key workload | Inputs | Dataflow untyped | Dataflow quickened | Dataflow JIT | JIT / quickened |
|---|---:|---:|---:|---:|---:|
| MAPLE sequence | 25,000 | 18.485 ms | 16.413 ms | 15.824 ms | 1.04x |
| Arithmetic-heavy, 64 stages | 25,000 | 52.694 ms | 32.815 ms | 11.138 ms | 2.95x |
| Dynamic expression, 50% dynamic | 100,000 | 50.816 ms | 49.381 ms | 49.475 ms | 1.00x |
| Deferred expression | 25,000 | 5.203 ms | 5.243 ms | 5.251 ms | 1.00x |
| Hard dynamic/defer, automatic scope | 1,024 | 17.953 ms | 17.760 ms | 17.619 ms | 1.01x* |
| Hard dynamic/defer, explicit components | 1,024 | 27.693 ms | 26.838 ms | 26.910 ms | 1.00x* |
| Threshold property | 10,000 | 0.908 ms | 0.731 ms | 0.647 ms | 1.13x |
| Three-step temporal window | 10,000 | 1.983 ms | 1.574 ms | 0.908 ms | 1.73x |

`*` The 1,024-event workloads finish exactly at the hotness threshold, before
activation on event 1,025. Their JIT columns therefore measure quickening plus
the pending-hotness check, not native execution; the small differences are
measurement noise. The automatic-scope benchmark was repeated after one
unstable run and the table uses the final stable, closely paired medians.

The dynamic and deferred workloads cross the threshold but show no material
gain, indicating that the current native coverage does not accelerate their
runtime-compiled expression work. The arithmetic and temporal cases amortize
delayed compilation well, while MAPLE and the simple threshold property show
smaller end-to-end improvements because runtime/input/output overhead occupies
a larger fraction of the measurement.
