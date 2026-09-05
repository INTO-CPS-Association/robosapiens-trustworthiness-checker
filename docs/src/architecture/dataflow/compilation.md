# Dataflow compilation

Compilation fixes a `DataflowMonitor` definition's immutable meaning and stable identities. It lowers stream equations, distinguishes current from historical dependencies, chooses an initial valid order, binds names to row locations, and packages the result as shared `StreamProgram` values, a `MonitorPlan`, and a `DataflowProgram`.

| # | Compilation phase | Responsible entity | Produces or establishes |
|---:|---|---|---|
| 1 | Lower stream equations | compiler pipeline | Ordered `EvaluationGraph<VarName>` values with operands before consumers, and a `ScalarSignature` for every node whose checked type is scalar. |
| 2 | Discover dependencies | compiler pipeline | All free-variable references for scope validation and same-tick references for scheduling. |
| 3 | Validate and order current dependencies | compiler pipeline | Static cycle rejection and one valid initial stream order. |
| 4 | Allocate row locations | `EnvironmentLayout` | Stable `EnvironmentSlot` identities for inputs and computed streams. |
| 5 | Bind and validate | compiler pipeline | Slot-bound operations, captures, recursive delays, and validated nested-expression scope. |
| 6 | Package streams | `StreamProgram` | Immutable per-stream operation programs without mutable evaluator state. |
| 7 | Assemble the definition | `MonitorPlan` and `DataflowProgram` | Monitor-wide structure, output projection, temporal work, reconfiguration points, and semantic identity. |

The numbered phases run once per definition. The runtime `Scheduler` may later replace the order established in phase 3, but it does not repeat compilation or reassign the stable identities established in phase 4. Nor does it revisit the signatures from phase 1: which work can be accelerated is decided here and never re-decided at runtime.

## Lowering preserves operation order

Typed and untyped ASTs lower to `EvaluationGraph<VarName>` values. Operands precede consumers, so `NodeId` identifies both an operation's position and its matching value/state slot within that graph. External references still use `VarName` until binding.

Typed compilation retains expected types and the checked environment used to validate runtime-defined nested expressions. Untyped compilation lowers expressions without that later type-checking contract.

## Dependency discovery has two views

Compilation collects all free variables to validate scope and separately collects same-tick free variables for scheduling. A direct positive historical read does not add a same-tick edge. Dependencies required to compute a compound delayed operand remain current prerequisites.

Static current cycles are rejected. Positive delayed self-reference is legal because binding converts it to a recursive delay whose read comes from committed history.

## Binding establishes stable locations

`EnvironmentLayout` assigns slots to declared inputs and computed streams. Binding replaces external names with `EnvironmentSlot` values and validates nested graphs, function captures, recursion, and reconfigurable-expression scope.

The `total` equation in the running example becomes an exact bound graph:

{{#include ../../assets/dataflow/evaluation-graph.svg}}

**Reading rule.** Solid arrows are forward-pass reads. The dashed path stages the completed stream result for the recursive delay and commits it after the whole row.

## Immutable stream and monitor artifacts

Each bound graph becomes a shared `StreamProgram`. The program contains canonical operations, layout access, evaluation mode, temporal metadata, and a `ScalarSignature` for every node the checker gave a scalar type. Mutable node values and operator state are not stored in the shared program; each `Evaluator` owns those.

Those signatures are the sole input to region legality, so which parts of a graph can be accelerated is settled here, by the type checker, and not later by observing values at runtime. A specification compiled without checked types has no signatures and therefore no scalar regions.

`MonitorPlan` records monitor-wide structure, including stable stream identities, static dependencies, output projection, temporal work, reconfiguration points, and source-prerequisite closures. `DataflowProgram` owns the immutable monitor definition and its semantic fingerprint.

For a monitor with runtime-defined expressions, compilation records what each expression is permitted to read and which fixed streams are needed to obtain its source value. The active nested body and its exact current dependencies are runtime state, not compilation output.

## What compilation guarantees

A successful compilation establishes:

- every referenced name is valid in its scope;
- the fixed same-tick graph is acyclic;
- current consumers can be ordered after their producers;
- historical self-reference is represented explicitly;
- environment and stream identities are stable for the definition;
- output projection names stable environment slots;
- nested expression sources have validated scopes and prerequisites;
- immutable programs can be shared without sharing mutable evaluator state.

Compilation does not open transports, allocate runtime history from prior definitions, or promise that the initial schedule remains active after nested dependency changes.

These artifacts fix what the physical tiers may then do with the running example. `scaled` and `alert` compile to straight-line scalar graphs; `total` compiles to the bound graph above, carrying a `RecursiveDelay` and its `NodeState` ring. [Fusion and regions](fusion.md) partitions the three on that distinction.

## Implementation mapping

The pipeline is implemented under `src/dataflow/compiler/`, with immutable artifacts in `src/dataflow/ir.rs`, `src/dataflow/program.rs`, and `src/dataflow/monitor_plan.rs`. Compilation and monitor tests cover dependency ordering, cycle rejection, binding, recursive delays, checked dynamic bodies, and output projection.

Continue with [scheduling](scheduling.md) for the runtime dependency order, [runtime ownership](runtime-ownership.md) for the objects created around these artifacts, or [tick execution](tick-execution.md) for their repeated use.
