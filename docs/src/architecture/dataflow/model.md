# Dataflow execution model

A `DataflowMonitor` executes a compiled dataflow graph as a stateful row function. One successful call to `DataflowMonitor::evaluate` consumes one value for every declared input, evaluates every computed stream once in dependency order, and produces one value for every declared output. Calls are logical ticks, and operator state survives between them.

## The dataflow graph

At the language level, the graph contains declared inputs and computed streams as vertices. A directed current edge points from a value producer to a stream that consumes that value in the same tick. The implementation preloads input vertices into the current environment row and schedules only computed-stream vertices.

| Graph concept | Meaning |
|---|---|
| input vertex | A declared input whose current value is loaded before computed streams run. |
| computed-stream vertex | One stream equation, compiled to a `StreamProgram` and evaluated by its persistent `Evaluator`. |
| current edge | A same-tick producer-to-consumer requirement; it constrains `Scheduler` order. |
| historical dependency | A read from committed earlier-tick state, such as `total[1]`; it does not constrain same-tick order. |
| active graph | The fixed current edges plus exact edges of currently active `dynamic` or `defer` bodies. |
| schedule | A topological order of computed-stream vertices for one active graph, represented by `ScheduledExecutionPlan`. |

The graph is the semantic dependency structure, not the stored row and not the execution order. `EnvironmentSlot` fixes where a value is published, while a schedule says when its evaluator runs. Either can remain stable while active dynamic edges cause the other ordering to be repaired.

## Running example

```dsrv
in x: Int
out alert: Bool
out total: Int
out scaled: Int
alert  = total > 20
total  = default(total[1], 0) + scaled
scaled = x * 2
```

![The running example has current edges from x through scaled, total, and alert, plus a historical self-edge on total](../../assets/dataflow/example-streams.svg)

**Reading rule.** Solid arrows are same-tick dependencies. The dashed loop reads the previous committed value of `total`; it does not create a same-tick cycle.

The declaration order is deliberately the reverse of the required evaluation order. Compilation derives `scaled → total → alert` from current dependencies.

The central public operation is direct row evaluation. The caller allocates one output row using `output_vars()` and reuses the same `DataflowMonitor` so temporal state survives between calls:

```rust
use trustworthiness_checker::{DsrvSpecification, Value, VarName};
use trustworthiness_checker::dataflow::DataflowMonitor;

let source = "in x: Int\n\
    out alert: Bool\n\
    out total: Int\n\
    out scaled: Int\n\
    alert = total > 20\n\
    total = default(total[1], 0) + scaled\n\
    scaled = x * 2";
let spec = source.parse::<DsrvSpecification>()?;
let mut monitor = DataflowMonitor::compile_untyped(spec)?;
let outputs = monitor.output_vars().to_vec();
let output_index = |name: &str| {
    outputs
        .iter()
        .position(|variable| variable == &VarName::new(name))
        .expect("declared output")
};
let mut row = vec![Value::NoVal; outputs.len()];

monitor.evaluate(&[Value::Int(4)], &mut row)?;
assert_eq!(row[output_index("total")], Value::Int(8));
assert_eq!(row[output_index("alert")], Value::Bool(false));

monitor.evaluate(&[Value::Int(8)], &mut row)?;
assert_eq!(row[output_index("total")], Value::Int(24));
assert_eq!(row[output_index("alert")], Value::Bool(true));
```

Each `evaluate` call is one logical tick, not one stream evaluation. The monitor schedules all computed streams, fills the complete output row, and commits successful temporal writes before the next call can observe them.

## Two ticks

![Two logical tick positions align every stream value around the post-row temporal commit](../../assets/dataflow/two-tick-evaluation.svg)

**Reading rule.** Every value cell in the `t1` column belongs to one logical tick, as does every cell in `t2`; vertical position names the stream rather than an execution substep. The compact commit cell lies between those tick positions. Its dashed arrow shows that tick 1's `total = 8` becomes historical only at tick 2; `total[1]` is a historical read and does not add a current scheduling edge. Tick 2's `total = 24` is committed for tick 3, which lies beyond the figure.

During tick 1, `total[1]` produces `Value::Deferred` because it has no committed sample, so `default` selects `0`. The complete row is `scaled = 8`, `total = 8`, and `alert = false`. Only after that row succeeds does the temporal commit make `8` historical. Tick 2 reads that committed value, produces `scaled = 16`, `total = 24`, and `alert = true`, then commits `24` for the following tick.

## Values that are not ordinary data

`Value::NoVal` means that no event for a variable is present on the current sparse row. It is not a missing input slot: callers still supply one value per declared input.

`Value::Deferred` means that an expression cannot yet produce a value, for example while a positive delay is filling. It is a real language value with lifting rules, not transport absence.

These values can propagate differently through operators and state. Their meaning is established by the evaluator, not by input batching.

## Current and historical dependencies

A current dependency constrains this tick's evaluation order. A positive historical read observes retained state and therefore does not add a current edge. Compound delay operands can still require current prerequisites before the operand is staged.

The `Scheduler` orders computed streams, not inputs. Inputs already occupy the current environment row. Each computed stream writes exactly once to its stable environment slot, and later same-tick consumers read that slot.

## Stable row locations

The monitor keeps one fixed environment layout for the compiled definition. Inputs occupy initial slots; computed streams occupy stable slots chosen during compilation. Output order is a saved projection over those slots, not another evaluation pass.

A schedule can change without moving row locations or evaluator state. This distinction becomes essential when active `dynamic` expressions reveal different current dependencies.

## One success boundary

A successful tick has four visible consequences:

1. every scheduled stream produced a current value;
2. the complete output row was projected;
3. temporal writes became committed history;
4. monitor-level history retained the successful row where required.

A failed tick produces no output row and no monitor-history commit. Temporal staging does not make current values historical before the common commit. Other evaluator-local mutations are not described as a general rollback transaction.

## From model to implementation

`DataflowProgram` contains the immutable compiled definition. `DataflowMonitor` owns the current row, active scheduler, monitor history, and `MonitorExecution`. `MonitorExecution` owns persistent evaluators keyed by stable stream identity.

Continue with [compilation](compilation.md) to see how the definition is established, [scheduling](scheduling.md) for dependency-valid execution order, then [runtime ownership](runtime-ownership.md) for the state and identity boundaries.
