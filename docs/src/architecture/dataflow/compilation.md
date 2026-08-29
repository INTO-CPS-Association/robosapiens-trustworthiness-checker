# Dataflow compilation

[← Previous: Execution model](model.md) · [Next: Runtime ownership](runtime-ownership.md) →

Compilation turns a typed or untyped DSRV specification into an immutable `DataflowProgram`. The program contains immutable expression programs, layout, history requirements, semantic identity, and a fixed monitor plan. It deliberately leaves active runtime dependencies and schedule-specific execution routes to a stateful `DataflowMonitor`.

## Pipeline


**What to notice.** Lowering and dependency discovery still use variable names. A single environment layout is allocated only after the static graph has been ordered; binding then replaces external names with stable slots. `StreamProgram` captures per-expression semantics, while `MonitorPlan` captures monitor-wide dependencies, reconfiguration metadata, source closure, and temporal commit membership.

## 1. Start from the AST contract

The two public compilation paths differ only in what semantic information the input expressions already carry:

- `compile_untyped` accepts a `DsrvSpecification` and lowers untyped expressions directly.
- `compile_checked` accepts a `CheckedDsrvSpecification`; checked result types, scalar signatures, and the shared type environment remain available to later runtime compilation of `dynamic` and `defer` source strings.

Both paths collect the declared inputs, outputs, and computed stream names before processing equations. Inputs need stable row locations but no evaluator program. Every computed stream must have an expression, and every declared output must ultimately resolve to an input or computed-stream location. `DataflowProgram::compile_checked` and `DataflowProgram::compile_untyped` return this immutable result; `DataflowMonitor::from_program` is the separate step that allocates evaluator, scheduler, environment, and `HistoryStore` state.

Parsing an untyped specification does not implicitly select the checked path. The chosen specification type determines whether runtime-defined expressions receive runtime type checking.

## 2. Lower expressions into ordered, unbound graphs

`compiler/lower.rs` walks each AST expression through an `ExprCursor` and builds an `UnboundEvaluationGraph`. Operands are lowered before their consumers, so a node may refer only to an earlier `NodeId` in the same graph.

An unbound reference has one of three forms:

| Reference | Meaning during lowering |
|---|---|
| `Const(Value)` | A literal embedded directly in the program. |
| `External(VarName)` | A name resolved outside the current graph body. |
| `Node(NodeId)` | The result of an earlier operation in this graph. |

Branches and function bodies are lowered recursively into their own graphs. Their node identities are local to those graphs. Dynamic and deferred expressions become explicit reconfiguration nodes whose source operand, `ReconfigurableExpressionScope`, kind, and—on the checked path—`ReconfigurableExpressionTyping` are retained.

Lowering also attaches `ScalarSignature` metadata where checked operand and result types prove that a unary or binary node is eligible for typed scalar execution. This metadata is an optimization hint beside the canonical operation; it does not replace the operation or change its `NodeId`.

## 3. Discover availability and same-tick dependencies separately

`LoweredDataflow::build` computes two related but different free-variable sets for every stream.

### All free variables

`free_vars` answers whether every external name used by the graph is available in the specification. It descends through branches and function definitions, excludes function parameters, and preserves captures. An unavailable name is rejected before ordering or binding.

### Same-tick free variables

`same_tick_free_vars` answers which current producers must run before this stream. These names form the static scheduling edges retained by the monitor.

A direct positive delay does not add an edge for its delayed operand because its current result reads committed history. Lowering explains the compound case: in `(a + 1)[1]`, the addition is an earlier graph node and its external read of `a` is still discovered as same-tick work, even though the enclosing delay node does not traverse its operand again. Guarded feedback therefore crosses the temporal boundary without hiding computation needed to produce a compound delayed sample.

Dependency discovery includes both branches of a conditional. Ordinary branch state advances on every outer tick, and schedule safety cannot depend on which value the condition selects at runtime.

Automatic reconfigurable scopes are resolved before these checks. At the top level, the allowed set contains declared inputs and streams except the containing stream itself; explicit scopes remain restrictions on that set. This scope is an authorization boundary for a future runtime expression, not a conservative dependency edge to every allowed name.

## 4. Reject static cycles and choose the initial order

The named same-tick dependency sets are converted to a temporary dependency graph. Topological ordering places current producers before consumers and rejects a same-tick cycle.

Positive delayed self-reference is not a same-tick edge and is therefore eligible for guarded feedback. Direct self-reference and zero-delay recursion are rejected later during binding. Direct mutually delayed references likewise do not form a same-tick cycle, although compound delayed operands may still contribute current edges as described above.

The resulting order is more than an initial execution convenience: it supplies stable top-level stream indices and contiguous computed-stream slots. Runtime scheduling may later reorder those identities, but compilation does not renumber them.

## 5. Allocate the environment and bind names

Compilation creates one `EnvironmentLayout` from all inputs followed by computed streams in the initial static order. Inputs occupy the initial contiguous slots; each computed stream receives one stable output slot after them.

Binding consumes each unbound graph and replaces every external `VarName` with its `EnvironmentSlot`. It also:

- restricts reconfigurable scopes to names present in the actual environment;
- validates nested branch and function restrictions;
- resolves function captures and local captures-plus-parameters layouts;
- rejects unsupported temporal function contexts;
- converts a positive delay of the containing stream into `RecursiveDelay`; and
- records recursive-delay node IDs for post-output staging.

Direct or zero-delay references to the containing output cannot be bound as guarded feedback and return a `StreamProgramError`.

![Bound evaluation graph with stable node identities and recursive-delay staging](../../assets/dataflow/evaluation-graph.svg)

**What to notice.** `NodeId` is graph-local and indexes the operation, current value, and persistent state at the same position. The recursive delay reads committed history during the forward pass; only after the graph result exists is that result staged for the shared end-of-tick commit.

## 6. Package each bound graph as a `StreamProgram`

A bound graph becomes an immutable, reference-counted `StreamProgram`. It contains:

- the `BoundEvaluationGraph`;
- the shared `EnvironmentLayout`;
- an `EvaluationMode` classification derived from whether graph evaluation can return a runtime error; and
- `requires_temporal_commit`, computed recursively through delays, branches, direct persistent calls, and reconfiguration nodes.

The program owns no mutable node values or language state. Multiple evaluator contexts may safely share a program because each evaluator creates its own canonical `EvaluatorState` within `EvaluatorTierStates`. Program sharing is therefore independent from state sharing.

The fallibility and temporal flags are semantic summaries used by planning and execution tiers. An optimization may derive a faster physical representation, but it must preserve the bound graph's publication, failure, and commit behavior.

## 7. Build the monitor-wide `MonitorPlan`

`MonitorPlan::build` translates named monitor structure into compact stable identities. It records four fixed components:

| Component | Responsibility |
|---|---|
| `StreamSlots` | Maps each stable `StreamId` to its canonical environment output slot. |
| `DependencyGraph` | Retains static same-tick dependencies and identifies streams containing reconfiguration points. |
| `ReconfigurableExpressionPlan` | Records each expression, its source, containing stream and node, initial resolution set, and source prerequisites. |
| `temporal_streams` | Lists every stream whose evaluator must participate in the logical commit traversal. |

The temporary named topological graph can now be discarded. The compact static dependency sets remain because the runtime scheduler must combine them with exact dependencies learned from active runtime expressions.

### Source-prerequisite closure

A `dynamic` or unsealed `defer` source must be known before its containing stream advances. The reconfigurable-expression plan classifies each bound source as:

- a constant, which needs no source-range stream;
- an input slot, already loaded before execution; or
- a computed-stream slot, which contributes that producer plus its transitive static same-tick dependency closure.

`source_prerequisite_closure` walks only the fixed dependency graph. It does not attempt to predict dependencies inside the future source string. Those exact active edges become available only after runtime compilation at the source barrier.

The plan rejects source operands that are node-local intermediates or otherwise cannot be resolved before fallible lazy evaluation. Every stream in the computed source closure must be free of reconfiguration points; under the current fallibility model these source programs are infallible. These restrictions ensure that source production can run once, dependency resolution can succeed or fail at a clear barrier, and no main-range state needs to advance before schedule repair.

![Reconfiguration points and their computed source prerequisites](../../assets/dataflow/reconfiguration-points.svg)

**What to notice.** Source prerequisites answer “what must run to obtain the source text?” They are distinct from the active dependencies of the expression described by that text. The former are fixed at compilation; the latter are exact runtime edges that may change on activation.

## 8. Finish the immutable `DataflowProgram`

Declared outputs are resolved to stable `EnvironmentSlot` values and saved in API output order. Output production is therefore a projection from the completed environment row, not another expression pass.

`LoweredDataflow::into_program` packages the immutable result:

- input, output, and computed-stream names;
- bound `StreamProgram` values and their shared `EnvironmentLayout`;
- the monitor-wide `MonitorPlan`;
- statically analysed history requirements; and
- the canonical `DefinitionKey`.

No evaluator, scheduler, current-row values, active expression state, or history storage is part of this compilation result. `DataflowMonitor::from_program` later creates those per-monitor objects and chooses its initial execution route. Active dynamic dependencies, repaired orders, schedule-specific quick plans, and native artifacts remain runtime concerns.

## What compilation establishes

By the time compilation finishes, four things are fixed in the `DataflowProgram`:

- **Order within a graph.** Operands precede consumers, and each `NodeId` indexes matching operation, value, and state entries.
- **The two dependency sets.** All free variables are validated for availability, but only same-tick free variables become edges — so a positive historical read never creates a false cycle, and an automatic scope authorizes names without over-approximating dependencies.
- **Identity.** Environment and stream identities are assigned once and survive every later schedule change.
- **Summaries the runtime relies on.** `StreamProgram` records nested fallibility and temporal commit requirements; the source closure records the computed source producer and its transitive static prerequisites. Reconfigurable-expression source, scope, kind, and checked typing metadata remain in the immutable programs and monitor plan; active bodies and dependencies are stateful monitor data.

Output ordering is a slot projection, independent of the order evaluators actually run in.

[← Previous: Execution model](model.md) · [Next: Runtime ownership](runtime-ownership.md) →
