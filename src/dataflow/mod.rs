//! Compile-and-evaluate dataflow monitoring semantics.
//!
//! # Conceptual model
//!
//! A [`DataflowMonitor`] turns a DSRV specification into a synchronous machine. One successful call
//! to [`DataflowMonitor::evaluate`] is exactly one logical tick: the caller supplies one [`Value`]
//! for every declared input and receives one value for every declared output. [`Value::NoVal`] is
//! the sparse-row marker for "no event for this variable on this tick"; [`Value::Deferred`] is a
//! real special value meaning that an expression cannot yet produce a value (for example, while a
//! delay is filling). Operator state, including delay history and lifted operands, survives from
//! one call to the next.
//!
//! ## Organising concepts
//!
//! This module follows the synchronous dataflow model exemplified by Lustre [[2]]: equations define
//! streams, current producer-to-consumer dependencies form a directed graph, and one logical tick
//! evaluates that graph in a dependency-valid order before temporal state is committed. DSRV extends
//! that foundation with sparse/special values, runtime-defined expressions, and live replacement.
//!
//! A **dataflow graph** contains declared inputs and computed streams as vertices. A current edge
//! `a → b` means that `b` consumes `a` from the same tick. Inputs are loaded before execution, so the
//! implementation schedules only computed-stream vertices. A positive historical read such as
//! `a[1]` reads committed earlier-tick state and does not add a current scheduling edge.
//!
//! | Concept | Responsibility | Main Rust entities |
//! |:--------|:---------------|:-------------------|
//! | **Graph** | Represent stream equations and current dependency edges independently of execution order. | `DataflowProgram`, `StreamProgram` |
//! | **Scheduling** | Produce a topological order for the fixed and currently active dynamic edges. | `Scheduler`, `ScheduledExecutionPlan` |
//! | **Independent evaluators** | Retain canonical operation and language state for each computed stream at a stable identity. | `MonitorExecution`, `Evaluator`, `EvaluatorState` |
//! | **Canonical execution** | Define special values, errors, publication, history, and the common temporal commit. | [`DataflowMonitor`] and canonical interpreter |
//! | **Quickening** | Accelerate eligible scalar operations with a mixed route that preserves canonical state and fallback. | `QuickPlan`, `PlanBundle` |
//! | **JIT tier** | When enabled, execute eligible scheduled work through guarded native artifacts with canonical fallback. | JIT coordinator and artifacts |
//!
//! Scheduling and acceleration are deliberately separate. The `Scheduler` may replace physical order
//! when active dependencies change, while evaluator-owned state remains attached to stable stream
//! identities. Quickening and JIT are accelerators over that scheduled machine; neither defines a
//! second language semantics.
//!
//! ## Running example
//!
//! Consider three interdependent output streams declared in reverse dependency order:
//!
//! ```text
//! in x: Int
//! out alert: Bool
//! out total: Int
//! out scaled: Int
//! alert  = total > 20
//! total  = default(total[1], 0) + scaled
//! scaled = x * 2
//! ```
//!
//! Conceptually, `scaled` doubles the current input, `total` adds that value to its previous output
//! (using `0` before any previous output exists), and `alert` tests the new total. The solid arrows
//! below are same-tick dependencies. The dashed loop is different: `total[1]` reads state retained
//! from the previous tick, so it does not create a same-tick dependency cycle.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/example-streams.svg")]
//! <figcaption>Solid arrows are same-tick dependencies; the dashed loop is retained previous-tick state.</figcaption>
//! </figure>
//!
//! The equations therefore have this two-tick trace:
//!
//! | tick | `x` | `scaled` | previous `total` | `total` | `alert` |
//! |-----:|----:|---------:|-----------------:|--------:|:--------|
//! | 1 | 4 | 8 | none, use 0 | 8 | false |
//! | 2 | 8 | 16 | 8 | 24 | true |
//!
//! The declaration order does not control evaluation: compilation discovers the dependencies and
//! orders the computed streams as `scaled, total, alert`.
//!
//! ## Pipeline
//!
//! | Stage | Main artifact | Responsibility |
//! |:------|:--------------|:---------------|
//! | **1. Read the model** | `DsrvSpecification` or `CheckedDsrvSpecification` | Supply untyped expressions or expressions with checked types. |
//! | **2. Lower expressions** | `UnboundEvaluationGraph` | Convert syntax into ordered operations whose external references are still `VarName` values. |
//! | **3. Order and bind streams** | `BoundEvaluationGraph` | Topologically order same-tick dependencies and replace names with stable `EnvironmentSlot` values. |
//! | **4. Build the definition** | `DataflowProgram`, `StreamProgram`, and `MonitorPlan` | Bind immutable programs, fixed layout metadata, reconfigurable expressions, dependencies, and temporal effects. |
//! | **5. Schedule execution** | `MonitorExecution` and `Scheduler` | Run the evaluator arena in the active dependency order. |
//! | **6. Run ticks** | `DataflowMonitor` | Load input rows, evaluate streams, commit temporal state, and project output rows. |
//!
//! The definition is compiled once and evaluated many times. Compilation lowers each equation into a
//! `LoweredDataflow`, derives a temporary [`crate::lang::core::DepGraph`] from same-tick free
//! variables, topologically orders the streams, assigns environment slots, validates and binds
//! references, and creates immutable `StreamProgram`s. It also builds a `DataflowProgram` containing
//! stable stream slots, static dependencies, `ReconfigurableExpressionKind` metadata,
//! source-prerequisite closures, the temporal commit set, and the initial order. A
//! [`DataflowMonitor`] created from that program owns the mutable `Scheduler` and `MonitorExecution`; the
//! latter keeps the evaluator arena while the former maintains the active dependency order across ticks.
//!
//! Evaluation repeats the right-hand side of the figure for every logical tick: load the input row,
//! resolve each active reconfigurable expression and its exact dependencies, execute `Evaluator`s in
//! dependency order through `EvaluationEnvironment`, commit temporal writes, and project outputs.
//! This compile-once translation from synchronous stream equations to a dependency-ordered sequential
//! machine follows the approach of Lustre [[2]], applied to the DSRV dynamic-property semantics
//! described in the language's original publication [[1]]. [`DataflowMonitor`] is the synchronous row
//! interface; [`crate::runtime::dataflow::DataflowRuntimeBuilder`] adapts it to
//! [`crate::core::InputStream`] and the runtime-facing [`crate::core::OutputWriter`].
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/pipeline.svg")]
//! <figcaption>Compilation creates the ordered monitor; evaluation reuses it for each logical input row.</figcaption>
//! </figure>
//!
//! ## Typed and untyped entry points
//!
//! [`DataflowMonitor::compile_checked`] accepts a [`crate::CheckedDsrvSpecification`]. Its equations
//! are already type checked, and that checked type environment and each expected result type are
//! retained so later `dynamic`/`defer` source strings are type checked when they become active.
//! [`DataflowMonitor::compile_untyped`] accepts a [`DsrvSpecification`], lowers its untyped
//! expressions directly, and runtime-compiles dynamic definitions without a type-checking pass.
//! Parsing a [`DsrvSpecification`] alone does not make it typed.
//!
//! `TryFrom<DsrvSpecification>` and `TryFrom<CheckedDsrvSpecification>` are exact conveniences for
//! those two methods. The generic [`crate::runtime::dataflow::DataflowRuntimeBuilder`] uses the same
//! conversions, so choosing the model type also chooses checked versus unchecked compilation.
//!
//! ## Executing the example
//!
//! This executable version exercises compilation and the public monitor interface directly:
//!
//! ```
//! use trustworthiness_checker::{DsrvSpecification, Value, VarName};
//! use trustworthiness_checker::dataflow::DataflowMonitor;
//!
//! let source = "in x: Int\n\
//!     out alert: Bool\n\
//!     out total: Int\n\
//!     out scaled: Int\n\
//!     alert = total > 20\n\
//!     total = default(total[1], 0) + scaled\n\
//!     scaled = x * 2";
//! let spec = source.parse::<DsrvSpecification>().expect("valid DSRV specification");
//! let mut monitor = DataflowMonitor::compile_untyped(spec).expect("valid dataflow");
//! let outputs = monitor.output_vars().to_vec();
//! let output_index = |name: &str| {
//!     outputs.iter().position(|var| var == &VarName::new(name)).expect("declared output")
//! };
//! let mut row = vec![Value::NoVal; outputs.len()];
//!
//! monitor.evaluate(&[Value::Int(4)], &mut row).unwrap();
//! assert_eq!(row[output_index("scaled")], Value::Int(8));
//! assert_eq!(row[output_index("total")], Value::Int(8));
//! assert_eq!(row[output_index("alert")], Value::Bool(false));
//!
//! monitor.evaluate(&[Value::Int(8)], &mut row).unwrap();
//! assert_eq!(row[output_index("scaled")], Value::Int(16));
//! assert_eq!(row[output_index("total")], Value::Int(24));
//! assert_eq!(row[output_index("alert")], Value::Bool(true));
//! ```
//!
//! # Compilation, programs, and state
//!
//! ## Lower and order
//!
//! `compiler::lower` lowers typed or untyped AST expressions. Its private
//! `EvaluationGraphBuilder` pushes operands before consumers, producing an
//! `ir::EvaluationGraph<VarName>`. The same generic representation is used
//! recursively for lazy branches and function bodies. Before binding, its
//! `ir::DataRef<VarName>` operands have these meanings:
//!
//! - `Const(Value)` embeds a literal or special value.
//! - `External(VarName)` is an unresolved reference outside this program body.
//! - `Node(NodeId)` reads an earlier operation result in this body.
//!
//! `compiler::pipeline::LoweredDataflow::build` collects every body's statically visible free
//! variables and rejects names absent from the specification. It separately collects immediate
//! free variables for scheduling: an operand read through a positive `SIndex` is historical and
//! therefore does not add a same-tick edge. A temporary [`crate::lang::core::DepGraph`] is built
//! from those immediate dependencies rather than directly from the AST because lowering has already
//! resolved function parameters and captures. A `dynamic` or `defer` source operand is visible here,
//! but dependencies in its eventual source string are not until that source is compiled. Direct
//! external operands of positive delays are historical; dependencies needed to compute a compound
//! delay operand remain conservatively ordered in the current tick. `DepGraph::topological_streams`
//! orders immediate dependencies before their consumers and reports a same-tick cycle. Inputs are
//! graph leaves already present in the environment and do not need programs. Positive delayed
//! self-references and direct mutually delayed stream cycles belong to persistent history rather
//! than this graph.
//!
//! `LoweredDataflow::into_program` consumes the named graphs. It assigns a slot to every declared
//! input followed by every computed stream in the initial dependency order, binds each graph, and
//! creates the immutable `DataflowProgram`. The temporary named dependency graph is discarded, but
//! its per-stream static dependency sets are retained in the plan so a monitor's scheduler can merge
//! them with active runtime dependencies.
//! `EnvironmentLayout` maps each `VarName` to an `EnvironmentSlot` used during binding.
//! `DataflowMonitor::environment_values` is the matching fixed-size row: input values occupy its
//! initial slots and each `Evaluator` writes its result to its assigned slot.
//!
//! In the example, tick 2 begins with the API input row `[x = 8]`. Evaluation fills that row in
//! dependency order to `[x = 8, scaled = 16, total = 24, alert = true]`. The output API has its own
//! order, exposed by [`DataflowMonitor::output_vars`]. Specifications expose variables from ordered
//! sets, so this example reports `[alert, scaled, total]`; compilation saves the corresponding
//! environment slots `[3, 1, 2]`. Producing the output row is a projection of existing environment
//! slots, not another evaluation or a reordering of evaluators. Callers must align slices with
//! [`DataflowMonitor::input_vars`] and `output_vars()` rather than assume declaration or dependency
//! order.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/environment-layout.svg")]
//! <figcaption>Environment slots remain stable even if runtime dependencies reorder evaluators; outputs project their own API order through saved slots.</figcaption>
//! </figure>
//!
//! `EnvironmentLayout` owns the immutable `VarName -> EnvironmentSlot` mapping and assigns contiguous
//! slots at construction. Binding consumes a graph and produces an
//! `ir::EvaluationGraph<EnvironmentSlot>`, replacing each `DataRef::External(VarName)` with
//! `DataRef::External(EnvironmentSlot)`. Every top-level `StreamProgram` shares the layout via `Rc`;
//! function bodies use a local captures-then-parameters layout, and runtime-compiled dynamic programs
//! bind against the outer layout after scope validation.
//!
//! `DataflowProgram` owns the immutable monitor definition, including the fixed `MonitorPlan` and
//! output projection. `DataflowMonitor` owns the mutable row as `environment_values: Vec<Value>`, the
//! mutable `Scheduler`, and a `MonitorExecution`. `MonitorExecution` owns the persistent `Evaluator`s.
//! During one program, `EvaluationEnvironment` borrows the row and layout, carries retained values
//! when a reconfigurable expression needs them, and supplies the optional recursive-call closure. It
//! resolves bound `DataRef::External` values directly from the row. Dependency scheduling guarantees
//! that a computed slot is filled before a same-tick read; evaluator state and bound slots do not
//! depend on the order chosen by the scheduler.
//!
//! ## Bind and validate
//!
//! `compiler::bind::UnboundEvaluationGraph::bind_graph` shares the completed `EnvironmentLayout`,
//! validates nested graphs, and converts every external `VarName` into an `EnvironmentSlot`. An
//! occurrence of the current output is accepted only as the operand of a positive `Delay` (the
//! lowered form of `sindex`); `compiler::bind::bind_op` replaces that operation with
//! `RecursiveDelay`. Direct or zero-delay recursion returns a [`StreamProgramError`], so a bound
//! recursive delay stores a `NonZeroU64`. Binding also resolves reconfigurable scopes, prepares function
//! capture layouts, and records the exact recursive-delay node IDs used by the post-output commit.
//!
//! The program figure shows the bound body for `total`. The semantic contents of an
//! `EvaluationGraph` are an ordered
//! `Vec<StreamOp>`, a `DataRef` identifying its result, and the IDs of recursive delays. `NodeId`
//! is an index into that operation vector and its matching value/state vectors. The positive
//! self-reference has become `RecursiveDelay`; it reads previous-output history during the forward
//! pass. Only after `Add` produces the body output does `stage_recursive_delays` retain that output as
//! a pending write. A positive ordinary `SIndex` similarly marks its current
//! operand for capture. The monitor applies both kinds of pending writes after every stream has
//! produced its current-tick value.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/evaluation-graph.svg")]
//! <figcaption>Solid arrows are forward-pass reads; the dashed path is the post-output recursive-delay commit.</figcaption>
//! </figure>
//!
//! The bound graph becomes an `ir::StreamProgram`. Its canonical execution data is the graph, an
//! `Rc<EnvironmentLayout>`, an
//! `EvaluationMode::{Infallible, Fallible}` classification, and a cached `requires_temporal_commit`
//! flag. Sharing the program is important for function call sites and runtime-compiled programs:
//! each `Evaluator` can own state without cloning operation vectors or layouts. The later
//! [scheduled plans and native execution](#scheduled-plans-quickening-and-native-execution) section
//! describes the optional metadata stored beside these semantic fields.
//!
//! ## Execute one tick
//!
//! `Evaluator` owns one `Rc<StreamProgram>` and one `EvaluatorTierStates`. The evaluator tier states keep canonical
//! `EvaluatorState`, optional quickening state, and an evaluator-local adaptive plan together so an
//! exact context handoff cannot separate a derived representation from its semantic owner. State is
//! local and mutable; root and nested transfer move selected owners destructively rather than using
//! monitor-wide snapshots, transactions, or copy-on-write state. `EvaluatorState` has two vectors
//! indexed by `NodeId`:
//!
//! - `node_values: Vec<Value>` is the current result of each operation. A forward pass overwrites
//!   these slots on every evaluation, so later nodes can resolve `DataRef::Node` in constant time.
//! - `node_states: Vec<NodeState>` retains operator-specific lifting and cross-tick data. Its
//!   variants include `Delay`, `LazyIf`, `Function`, `PersistentCall`, and `Dynamic`, in addition to
//!   the lifting state used by ordinary operators.
//!
//! ### Temporal state across a tick
//!
//! A logical tick needs both the current environment row and history from completed earlier ticks.
//! Updating a delay ring as soon as its node runs would mix those time frames: another node evaluated
//! later in the same tick could observe a current value as though it belonged to the past. The runtime
//! therefore separates computing a tick from making that tick historical.
//!
//! During evaluation, delays read only committed history. Storage has two owners. A direct top-level
//! delay of an external environment variable reads the monitor's bounded `HistoryStore` through
//! `HistoryAccess`; its local delay commit is a no-op. Runtime-defined, internal, and recursive delays
//! use evaluator-local `NodeState::Delay(DelayState)` rings. Local ordinary delays stage their
//! completed operands, while recursive delays stage the enclosing stream result after it is known.
//!
//! After all scheduled streams have produced the current row, the monitor performs the **temporal
//! commit**. It records completed monitor values in `HistoryStore` and pushes staged local samples into
//! `DelayState` rings. Those samples become visible as history on the next logical tick. This is a
//! temporal visibility boundary, not a general transaction: non-temporal state changes are not rolled
//! back if evaluation fails.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/history-retention.svg")]
//! <figcaption>A direct top-level external delay reads monitor HistoryStore, while internal and recursive delays read evaluator-local DelayState rings. Successful current samples become historical only at the common post-row commit.</figcaption>
//! </figure>
//!
//! The canonical path uses `evaluate_nodes` for infallible programs. Fallible programs use
//! `try_evaluate_nodes`, which handles dynamic nodes and delegates ordinary operations to the same
//! evaluator. `EvaluationEnvironment` contains the environment row, its layout, retained values when
//! needed, and an optional recursive-call closure. After traversal the evaluator reads
//! `EvaluationGraph::output`, records recursive self-delay writes, and returns one stream value.
//! `evaluate_and_commit` is used where a nested invocation owns its complete tick; top-level
//! `Evaluator`s record temporal writes for the monitor's common commit phase.
//!
//! `DataflowMonitor::execute_tick` has these logical phases:
//!
//! 1. Load the complete input slice. A monitor with reconfiguration clears the environment row to
//!    `NoVal` and updates retained input values; a static monitor can overwrite input slots in place.
//! 2. Evaluate, once and in static order, the transitive source-prerequisite closure needed to produce
//!    `dynamic`/`defer` source values.
//! 3. At the source barrier, resolve every active reconfigurable expression without evaluating its new
//!    body. Changed `Dynamic` bodies are installed in their owning evaluator, and their active
//!    same-tick dependency slots are collected.
//! 4. Ask `Scheduler` to retain its cached order when valid or repair it with iterative DFS; reject
//!    a same-tick runtime cycle.
//! 5. Evaluate every stream not already consumed as a source prerequisite exactly once in the active
//!    dependency order, and write each result to its stable slot.
//! 6. Commit temporal state only for streams marked by `MonitorPlan::temporal_streams`; a deferred
//!    deferred expression releases its source prerequisites after the successful main evaluation.
//! 7. Back in `evaluate`, project `output_slots` into the caller's output slice.
//!
//! The common commit lets mutually delayed streams capture each other's completed current values
//! without creating a same-tick scheduling cycle. Runtime-compiled programs and persistent direct
//! function applications participate through nested `EvaluatorState`. Dynamic dependencies are
//! resolved at the barrier before stateful execution, so no stream advances twice merely because the
//! order changed.
//!
//! Input/output slice-count errors are validation failures before `execute_tick`; they do **not**
//! poison the monitor and the caller may retry with correctly sized slices. Any error returned after
//! `execute_tick` starts (runtime compilation/type/scope failure, invalid source, unsupported nested
//! reconfiguration, or dynamic cycle) is terminal: `failed` is set, later calls return
//! [`DataflowEvaluationError::MonitorFailed`], no temporal commit or output projection occurs for the
//! failed tick, and arbitrary already-mutated evaluator state is not rolled back.
//!
//! ## History management
//!
//! ### Monitor history and evaluator-local rings
//!
//! The dataflow runtime does not retain complete environment rows. It keeps bounded monitor history
//! only for outer environment variables required by direct top-level historical reads or projected
//! nested requirements. Such a delay binds to `HistoryAccess` and reads `HistoryStore` rather than
//! owning a duplicate local ring.
//!
//! Runtime-defined and internal positive `Delay` nodes, and every `RecursiveDelay`, own
//! `NodeState::Delay(DelayState)`: a circular buffer with exactly as many entries as the requested
//! offset, together with read/write cursors, lifted output, and pending-write state. For example, a
//! local or recursive `x[3]` behaves as follows:
//!
//! | logical tick | current `x` | ring before commit | result |
//! |-------------:|------------:|:-------------------|:-------|
//! | 1 | 10 | empty | `Deferred` |
//! | 2 | 20 | `[10]` | `Deferred` |
//! | 3 | 30 | `[10, 20]` | `Deferred` |
//! | 4 | 40 | `[10, 20, 30]` | 10 |
//!
//! An offset of zero lifts the current value without allocating a ring. A positive delay returns
//! `Deferred` until its storage has received enough samples. `Deferred` is stored as a sample; when a
//! stored `NoVal` emerges, ordinary output lifting applies. Separate local syntactic indices such as
//! `x[2] + x[2]` own independent equivalent `DelayState` rings. Direct top-level external indices can
//! instead share the monitor's per-variable `HistoryStore` while retaining independent lifting state.
//!
//! ### How delay nodes participate
//!
//! A direct top-level external delay reads `HistoryStore`; monitor commit records the completed
//! environment value centrally, and the delay's local commit is a no-op. A local positive ordinary
//! delay reads its `DelayState` ring and stages its completed operand. A `RecursiveDelay` reads its
//! local ring during the forward pass and stages the enclosing stream's completed output. The common
//! post-row traversal commits both monitor history and local staged values.
//!
//! If a tick fails before temporal commit, monitor history and pending local writes are not committed. Other node state
//! may already have changed because evaluation errors are terminal and the complete evaluator is not
//! transactionally rolled back.
//!
//! ### State ownership and memory bounds
//!
//! State belongs to the node or nested evaluator that implements an operation:
//!
//! | owner | retained state |
//! |:------|:---------------|
//! | Direct top-level delay of an external environment variable | A binding into the monitor's bounded `HistoryStore`; runtime-defined, internal, and recursive delays keep local rings. |
//! | Ordinary `if` | Independent boxed `EvaluatorState` values for both branches. |
//! | Persistent function call site | A nested `Evaluator`, including delay rings in the function body. |
//! | Recursive function call | Resettable frames used for the active recursive evaluation. |
//! | `dynamic` or `defer` | Source/result lifting state, an outer-environment shadow, and an optional active `Evaluator`. |
//! | Other lifted operations | Their required last operands, values, or control flags. |
//!
//! History storage is proportional to the maximum effective depth of shared outer-variable histories
//! plus local positive offsets across top-level and retained nested evaluators. Reconfiguring a
//! `dynamic` body releases local state owners that are not transferred into the new evaluator. An
//! activated `defer` evaluator remains retained until its enclosing state is reset or dropped. Offsets
//! are converted from `u64` to `usize`; the compiler does not currently impose a configurable maximum
//! history size.
//!
//! These structures also encode scheduling invariants: an immediate stream read observes a producer
//! scheduled earlier, a historical read observes retained node state and captures from the completed
//! row, and a program node reads an earlier node slot. Ordinary lazy branches advance independent
//! state, persistent function call sites retain nested evaluators, recursive frames reset per
//! invocation, and recursive delays commit only after their enclosing output is known.
//!
//! # `if` handling
//!
//! ## Representation and dependencies
//!
//! An `if` is lazy rather than a normal eager operation. `compiler::lower` emits the condition into the
//! enclosing program, but builds the two alternatives as separate `ir::EvaluationGraph` values inside a
//! `ir::StreamOp::If`. Free-variable collection, validation, and binding visit both bodies, so
//! stream dependencies are conservative: a stream used by either branch must be available before
//! the stream containing the `if`, even if that branch is not selected on a particular tick.
//!
//! ## Selection and branch state
//!
//! `NodeState::LazyIf(LazyIfState)` gives each branch its own boxed `EvaluatorState` and retained
//! output, and also retains the last condition. In ordinary stream evaluation, both branches run on
//! every tick in which the enclosing stream is evaluated, regardless of the condition. This keeps
//! their independent temporal state aligned. Each branch output is lifted separately; if either
//! current-or-retained output is `NoVal`, the whole `if` produces `NoVal`. Otherwise `Bool(true)`
//! selects the then-value, `Bool(false)` selects the else-value, and a `Deferred` or `NoVal`
//! condition propagates. A `Deferred` value from the unselected branch does not affect the selected
//! result. Checked compilation normally prevents any other condition type.
//!
//! Branch state is local and recursively owned. Cloning an `Evaluator` clones its branch state, while
//! normal evaluation mutates the unique live state directly. Context transfer moves matching branch
//! owners only after preparation validates the complete rewrite; it does not require a monitor-wide
//! state snapshot.
//!
//! Recursive function evaluation is the supported exception to advancing both branches. It first
//! applies `retain_last_value` to the condition, so `NoVal` after an earlier Boolean can reuse that
//! Boolean. When `EvaluationEnvironment` contains the recursive callback, an effective `Bool(true)`
//! evaluates only the then-branch and `Bool(false)` evaluates only the else-branch. `Deferred`, or
//! effective `NoVal` without a retained Boolean, returns without evaluating either branch. This
//! genuinely lazy selection lets a recursive base case return without entering the recursive branch.
//!
//! Reconfigurable expressions are not supported inside lazy branches: compilation rejects an `if`
//! branch containing `dynamic` or `defer`. Supported branch evaluation is therefore infallible and
//! has no branch-error suppression or rollback semantics. After an ordinary enclosing stream
//! evaluates successfully, temporal commits recurse into both branch states.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/lazy-if.svg")]
//! <figcaption>Ordinary stream evaluation advances both independent branch states. Recursive evaluation first retains the last Boolean across NoVal, follows only the effective Boolean-selected branch, and skips both branches for Deferred or NoVal without a retained Boolean.</figcaption>
//! </figure>
//!
//! # Function handling
//!
//! ## Definition and binding
//!
//! `compiler::lower` lowers a lambda to `ir::StreamOp::Function` containing an
//! `ir::UnboundFunction`: parameter names, an `UnboundEvaluationGraph`, and display text. Binding
//! converts it to `ir::StreamFunction`, whose `Rc<StreamProgram>` is paired with capture source
//! `EnvironmentSlot`s. A general application becomes `ir::StreamOp::Apply`; partial application and
//! `List.map`, `List.filter`, and `List.fold` have dedicated operations but invoke the same
//! [`RuntimeFunction`] representation.
//!
//! `compiler::bind::bind_function` computes the body's free variables, removes its parameters, and resolves
//! each remaining capture to a source slot in the enclosing environment. It then gives the body a
//! compact local environment with captures first and parameters second, and binds all body
//! references to those slots. Captures are therefore selected once during compilation without
//! copying values into the immutable program.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/function-binding.svg")]
//! <figcaption>Binding stores capture source environment slots and gives the shared body program a captures-first local layout.</figcaption>
//! </figure>
//!
//! Temporal support depends on the call mode:
//!
//! | form | lowered operations | temporal body support |
//! |:-----|:-------------------|:----------------------|
//! | Literal lambda called directly | `DirectApply` | Persistent call-site evaluator; `sindex`, `init`, `when`, `update`, and `latch` are supported. `dynamic`/`defer` is rejected. |
//! | First-class/data-dependent function called by ordinary application | `Function` + `Apply` | The `RuntimeFunction` advertises whether it needs a call-site instance; `Apply` keeps that instance while the function definition is unchanged. The same temporal operators are supported; `dynamic`/`defer` is rejected. |
//! | Directly applied `fix` lambda | `RecursiveApply` + `RecursiveCall` | The recursive body is validated as an isolated recursive invocation: all temporal operators, including `dynamic`/`defer`, are rejected. |
//! | Partial application or `List.map`/`List.filter`/`List.fold` callback | dedicated operation | A statically visible temporal lambda is rejected during binding; a temporal function arriving dynamically is likewise unsupported. |
//!
//! Nested literal direct applications have their own persistent evaluators and follow the
//! `DirectApply` row. A nested lambda definition is validated according to how that nested function
//! itself will be called, rather than inheriting the enclosing body's call mode.
//!
//! ## Application and recursion
//!
//! When a `Function` node is evaluated, it emits a [`RuntimeFunction`] with stable definition
//! identity and refreshes that function's shared capture vector from the current outer environment.
//! A general `Apply` uses `NodeState::CallLift` to retain the function and arguments and, for a
//! temporal dataflow function, an instantiated callable/evaluator that persists until the active
//! function definition changes. A `DirectApply` instead owns `NodeState::PersistentCall`: its nested
//! evaluator, captures-then-parameters environment, and lifted arguments persist directly in the
//! enclosing stream state. Its temporal writes are staged and committed by the enclosing monitor.
//!
//! Direct recursive evaluation uses the private `execution::functions::RecursiveCall`. Every
//! invocation obtains a reset `CallFrame`, fills parameter slots in a captures-first environment,
//! evaluates and commits the shared non-fallible body, and returns the frame to a pool for that
//! outer recursive call. Resetting a recursive frame clears its per-invocation values and lifting
//! state.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/function-call.svg")]
//! <figcaption>Normal Apply carries one callable's state across ticks; RecursiveApply isolates active recursion depths in reset frames returned to a per-call pool.</figcaption>
//! </figure>
//!
//! Application stream-lifts the function operand and every argument independently; `Deferred` or
//! `NoVal` propagates before a call. For a directly applied `fix` lambda, `compiler::lower` emits
//! `ir::StreamOp::RecursiveApply` and rewrites self calls as `ir::StreamOp::RecursiveCall`.
//! `evaluate_recursive_apply` supplies the callback through `EvaluationEnvironment`. The recursive
//! `if` rule above evaluates only the selected branch, so a base case can return without evaluating
//! the recursive branch. Other function values use the general `Fix` wrapper.
//!
//! This example shows both capture-by-current-tick and direct recursive application. `bias` is
//! captured anew when each function node is evaluated, while every recursive call in that tick
//! sees the same captured value:
//!
//! ```
//! use trustworthiness_checker::{DsrvSpecification, Value, VarName};
//! use trustworthiness_checker::dataflow::DataflowMonitor;
//!
//! let source = "in bias: Int\nin n: Int\nout direct: Int\nout recursive: Int\n\
//!     direct = (\\x: Int -> x + bias)(n)\n\
//!     recursive = fix(\\self: (Int -> Int), k: Int -> if k == 0 then bias else self(k - 1) + 1)(n)";
//! let spec = source.parse::<DsrvSpecification>().unwrap();
//! let mut monitor = DataflowMonitor::compile_untyped(spec).unwrap();
//! let input_vars = monitor.input_vars().to_vec();
//! let output_vars = monitor.output_vars().to_vec();
//! let input = |bias: i64, n: i64| {
//!     input_vars.iter().map(|var| {
//!         if var == &VarName::new("bias") { Value::Int(bias) } else { Value::Int(n) }
//!     }).collect::<Vec<_>>()
//! };
//! let output_index = |name: &str| {
//!     output_vars.iter().position(|var| var == &VarName::new(name)).unwrap()
//! };
//! let mut output = vec![Value::NoVal; output_vars.len()];
//!
//! monitor.evaluate(&input(10, 3), &mut output).unwrap();
//! assert_eq!(output[output_index("direct")], Value::Int(13));
//! assert_eq!(output[output_index("recursive")], Value::Int(13));
//! monitor.evaluate(&input(2, 5), &mut output).unwrap();
//! assert_eq!(output[output_index("direct")], Value::Int(7));
//! assert_eq!(output[output_index("recursive")], Value::Int(7));
//! ```
//!
//! # Reconfigurable expressions
//!
//! At the model level, a **reconfigurable expression** is a `dynamic(...)` or `defer(...)` expression
//! whose formula arrives through a string-valued input. The surrounding equation is fixed, while the
//! body is compiled at a source barrier. In the bound IR, each point carries a
//! `ReconfigurableExpressionKind` identifying whether it is `Dynamic` (re-evaluated when its source text changes) or `Deferred`
//! (activated once). It also fixes the source operand, result type, and names the body may use.
//!
//! The figure depicts this model:
//!
//! ```text
//! in sensor: Int
//! in baseline: Int
//! in enabled: Bool
//! in limit_source: Str
//! in rule_source: Str
//! in gate_source: Str
//!
//! out score: Int
//! out limit: Int
//! out decision: Bool
//!
//! score    = sensor - baseline
//! limit    = defer(limit_source: Int, {score, baseline})
//! decision = dynamic(rule_source: Bool, {score, limit})
//!            && dynamic(gate_source: Bool, {enabled})
//! ```
//!
//! Suppose the three source inputs currently carry `"score + 10"`, `"score > limit"`, and
//! `"enabled"`. Each scope lists **potential dependencies**; the names actually read by the current
//! formula become **active dependencies**:
//!
//! | point | allowed names | current formula | active dataflow edges |
//! |:------|:--------------|:----------------|:----------------------|
//! | `RP1` in `limit` | `score`, `baseline` | `score + 10` | `score -> limit` |
//! | `RP2` in `decision` | `score`, `limit` | `score > limit` | `score -> decision`, `limit -> decision` |
//! | `RP3` in `decision` | `enabled` | `enabled` | `enabled -> decision` |
//!
//! Thus the current computed-stream order must place `score` before `limit` and `limit` before
//! `decision`. The input `enabled` is already present at the start of
//! the tick, so its edge does not order two computed streams. If a later `dynamic` source uses a
//! different allowed name, the corresponding active edges change; the `defer` point accepts only its
//! first formula. The surrounding `&&` and the rest of the model remain fixed, and a stream such as
//! `decision` can contain more than one point.
//!
//! At the start of a tick, the monitor evaluates the source-prerequisite closure, then reaches a
//! source barrier. It activates or reconfigures each eligible point and collects the active reads
//! before scheduling affected streams. When the containing stream later evaluates, the point behaves
//! like an ordinary subexpression and contributes its current value to the fixed surrounding
//! equation.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/reconfigurable-expressions.svg")]
//! <figcaption>Orange boxes are the three reconfigurable expressions; solid arrows show current producer-to-consumer value flow, while the one dashed arrow is an allowed value source that the current formula does not use.</figcaption>
//! </figure>
//!
//! ## Scope and scheduling
//!
//! `dynamic(source: T)` treats the current string value of `source` as a DSRV expression. `compiler::lower`
//! lowers the construct to `ir::StreamOp::Dynamic` with a `ir::DynamicExpressionSpec`. The spec records
//! the source operand, optional checked type information, allowed variables, and
//! `ir::ReconfigurableExpressionKind::Dynamic`. Because activation can fail at evaluation time, a graph
//! containing this node (including a nested branch graph) has `EvaluationMode::Fallible`.
//!
//! Scope resolution happens while the containing specification is compiled:
//!
//! - An automatic scope, as in `dynamic(source: T)`, may refer to every declared input or computed
//!   stream except the stream containing the expression. Merely making a variable available does
//!   not create a dependency; only a received expression that actually reads it adds an active edge.
//! - An explicit scope, as in `dynamic(source: T, {x, intermediate})`, is an allow-list. It restricts
//!   which names a received expression may read, but unused names do not constrain scheduling.
//! - Runtime lowering restricts any nested requested scopes to the outer allow-list. Nevertheless,
//!   an active expression that itself contains `dynamic` or `defer` is currently rejected with
//!   [`DataflowEvaluationError::UnsupportedNestedReconfiguration`]; nested reconfiguration is not
//!   supported.
//!
//! The preceding figure uses ordinary value-flow arrows from producer to consumer: `score` flows to
//! `limit`, then to `decision`. The scheduler stores the inverse relationship—each
//! consumer points to the producers it reads. The next figure switches explicitly to that dependency
//! convention and uses a smaller model whose two dynamic streams may read each other in either
//! direction, making schedule repair visible.
//!
//! A scope defines a **potential graph**, not the scheduler's active graph. In this figure dependency
//! arrows follow the scheduler's convention: `A -> B` means “stream `A` reads stream `B`.” Execution
//! therefore schedules `B` before `A`, opposite the direction in which the arrow is traversed.
//! Automatic scopes for dynamic outputs `a` and `b` permit `a -> x`, `a -> b`, `b -> x`, and
//! `b -> a`. Activating all four potential reads would invent an `a`/`b` cycle even on ticks where
//! only one direction is used. The monitor instead activates only the names actually read by each
//! active expression.
//!
//! ```text
//! in x: Int
//! in a_source: Str
//! in b_source: Str
//! out a: Int
//! out b: Int
//! a = dynamic(a_source: Int)
//! b = dynamic(b_source: Int)
//! ```
//!
//! Here `n` and `n + 1` are consecutive **runtime ticks**—two calls that supply two logical input
//! rows—not stages within one evaluation. Suppose the cached order begins as `[a, b]`. On runtime
//! tick `n`, sources `a_source = "b + 1"` and `b_source = "x"` activate `a -> b -> x`. Because `a`
//! reads `b`, `[a, b]` is invalid and is repaired to dependency-first order `[b, a]` before either
//! stream evaluates. On the next runtime tick, both source values change to `a_source = "x"` and
//! `b_source = "a + 1"`, activating `b -> a -> x`. The old `[b, a]` order is now invalid and is
//! repaired to `[a, b]`. All source changes for a runtime tick are resolved before the streams
//! advance, so scheduling sees one coherent active graph rather than intermediate edge sets.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/dynamic-dependencies.svg")]
//! <figcaption>The full graph and matrix show the same compile-time permissions; each runtime tick activates a subset and repairs the cached dependency-first schedule when necessary.</figcaption>
//! </figure>
//!
//! The `MonitorPlan` records each reconfigurable expression and the transitive source-prerequisite closure, while the
//! `Scheduler` applies the tick phases listed above. Reconfiguration resolution activates or
//! reconfigures local `Evaluator`s and collects exact same-tick dependencies without evaluating the
//! active bodies. The scheduler first checks its current order and only runs its allocation-reusing
//! iterative DFS when repair is needed; a cycle produces
//! [`DataflowEvaluationError::DynamicDependencyCycle`]. `EnvironmentLayout` and bound
//! `EnvironmentSlot` values remain stable; only stream IDs in the scheduled order change.
//!
//! Early dependency resolution imposes compile-time restrictions: every expression source must bind
//! directly to a constant or outer environment slot, not a node-local result; a reconfigurable expression
//! cannot be hidden in a fallible lazy branch; and any computed stream needed to produce a source
//! (including static prerequisites) must be infallible and contain no reconfigurable expression. Violations
//! produce [`DataflowCompilationError::UnsupportedReconfiguration`]. These constraints ensure source
//! streams can run once before scheduling and then be omitted from the main execution order.
//!
//! ## Temporal history of runtime-defined expressions
//!
//! Temporal history belongs to the `EvaluatorState` owner that records it. A delay in a newly active
//! body starts empty unless `ContextTransferPolicy::MatchingStreamState` finds an exact body to carry
//! over; a delay that remains alive keeps the samples it has recorded. The dataflow monitor does not
//! maintain a monitor-wide archive from which a new body is backfilled.
//!
//! ### How history becomes available
//!
//! Consider a monitor whose expression source becomes `"x[2]"` only after two rows have already been
//! processed:
//!
//! ```text
//! in x: Int
//! in source: Str
//! out z: Int
//!
//! z = dynamic(source: Int)
//! ```
//!
//! | tick | `x` | `source` | `z` | state recorded by the active `x[2]` |
//! |-----:|----:|:---------|:----|:------------------------------------|
//! | 0 | 10 | `NoVal` | `NoVal` | no active expression |
//! | 1 | 20 | `NoVal` | `NoVal` | no active expression |
//! | 2 | 30 | `"x[2]"` | `Deferred` | 30 |
//! | 3 | 40 | `NoVal` | `Deferred` | 30, 40 |
//! | 4 | 50 | `NoVal` | 30 | 40, 50 after producing 30 |
//!
//! The new delay does not receive 10 or 20. It sees 30 as its current operand on the activation tick,
//! returns `Deferred`, and stages 30 as its first sample. At tick 3 it stages 40. At tick 4 the sample
//! from tick 2 is two ticks old, so the expression can return 30. History is available to a new
//! temporal expression as samples accumulate from its activation tick onward.
//!
//! ### Where the state lives
//!
//! A `dynamic` or `defer` node compiles its source string into a `StreamProgram` and evaluates it in a
//! nested `Evaluator`. That evaluator owns the operation-local state and delay rings of the active body.
//! The surrounding `NodeState::Dynamic` separately owns source and result lifting state, an active
//! `EnvironmentProjection`, and an environment shadow in the nested evaluator's compiled layout.
//!
//! The projection contains compact nested-slot/outer-slot pairs plus projected dependency and history
//! slots. It lets a new expression use the current value of `x` immediately and lets an exact root
//! transfer keep an old compiled body after the outer dense layout changes. Projected history slots
//! contribute bounded context-retention requirements for a later root transfer; they do not backfill
//! the active body's local delay rings.
//!
//! Reusing an evaluator preserves its operation state and delay rings. A changed `Dynamic` source
//! constructs a new local evaluator from a cached immutable template or a fresh compilation.
//! `ContextTransferPolicy::MatchingStreamState` transfers state only when the replacement body is
//! semantically identical; changed owners start cold. `None` keeps the new evaluator cold. Temporal
//! operators in the fixed surrounding specification are unaffected.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/dynamic-history.svg")]
//! <figcaption>A changed active body gets a local evaluator; only an exact body match can continue state, while fixed surrounding temporal operators always keep their own state.</figcaption>
//! </figure>
//!
//! ## Source-barrier activation and context transfer
//!
//! During source-barrier reconfiguration, a new string is parsed as a DSRV expression, optionally
//! type checked, lowered, checked against the resolved allow-list, and bound to the existing
//! `EnvironmentLayout`. Its same-tick free variables become active dependency slots; all free
//! variables are scope checked before the body is installed.
//!
//! Root monitor reconfiguration follows the same ownership rule. `DataflowMonitor::reconfigure`
//! compiles the target program and prepares a target-indexed `ReconfigurationMapping` that pairs
//! same-name streams whose `StreamStateKey` values match. `prepare_context_transfer` validates
//! those matches, retained values, active-expression projections, dynamic dependencies, history
//! requirements, and deferred-expression lifecycle state before any state moves.
//! `context_transfer_from` then performs the infallible destructive handoff at the tick barrier.
//! The input control barrier flushes earlier data before the command, the runtime replaces its
//! input and output, and acknowledgement is last.
//!
//! `ReconfigurableExpressionKind::Dynamic` compares source text. The first accepted string activates a
//! fresh evaluator; equal text reuses the current evaluator, its dependencies, and its temporal
//! state. Different text creates a new evaluator. The source text is not a semantic cache key: the
//! template cache shares immutable programs, not evaluator state. The new evaluator may transfer
//! state from the old one only when the compiled bodies are semantically identical; a changed or
//! absent owner has no history to recover. Returning to earlier text therefore does not restore an
//! old evaluator instance.
//!
//! The owning `Evaluator` prepares a replacement body before it takes the old active body from the
//! dynamic node. If parsing or binding fails, the live active body remains installed; a changed body
//! installs cold. An exact match destructively moves the body state and reuses environment-shadow
//! storage. It does not use a monitor-wide state image.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/dynamic-lifecycle.svg")]
//! <figcaption>Equal source text keeps one evaluator timeline. Changed text installs a replacement that can take compatible state only from the immediately previous evaluator; otherwise it starts cold, and returning to older text never revives an archived evaluator.</figcaption>
//! </figure>
//!
//! ## Source values and evaluator advancement
//!
//! Once active, the evaluator advances whenever the enclosing dynamic node evaluates,
//! including ticks whose raw source is `NoVal` or `Deferred`. Source lifting happens first: `NoVal`
//! repeats the previous source value if one exists, whereas a string or `Deferred` becomes the new
//! retained source value. A `NoVal` after a string therefore behaves like that retained string; a
//! `NoVal` after `Deferred` behaves like retained `Deferred`.
//!
//! An effective string exposes the active evaluator's result. An effective `Deferred` still advances
//! the evaluator internally, preserving its temporal alignment, but makes `dynamic` emit `Deferred`.
//! The active body's own operators perform any result lifting; `defer` separately retains its last
//! non-`NoVal` body result. A changed `dynamic` body therefore has no implicit result history beyond
//! the state owners explicitly transferred into its new evaluator.
//!
//! Before any string is accepted, there is no evaluator to advance: effective `NoVal` yields `NoVal`
//! and `Deferred` yields `Deferred`. Any non-string, non-special source produces
//! [`DataflowEvaluationError::InvalidExpressionSource`]. Parse, type, scope, binding, nested
//! reconfiguration, and dependency-cycle errors are terminal to the monitor.
//!
//! For `z = dynamic(source: Int)`, the following sequence reuses an `x[1]` evaluator, activates
//! an incompatible `x[0]` body, and then starts another `x[1]` evaluator:
//!
//! ```
//! # use trustworthiness_checker::{DsrvSpecification, Value, VarName};
//! # use trustworthiness_checker::dataflow::DataflowMonitor;
//! # let spec = "in source: Str\nin x: Int\nout z: Int\nz = dynamic(source: Int)"
//! #     .parse::<DsrvSpecification>().unwrap();
//! # let mut monitor = DataflowMonitor::compile_untyped(spec).unwrap();
//! # let input_vars = monitor.input_vars().to_vec();
//! # let row = |source: Value, x: i64| input_vars.iter().map(|var| {
//! #     if var == &VarName::new("source") { source.clone() } else { Value::Int(x) }
//! # }).collect::<Vec<_>>();
//! # let mut output = vec![Value::NoVal];
//! monitor.evaluate(&row(Value::Str("x[1]".into()), 10), &mut output).unwrap();
//! assert_eq!(output, [Value::Deferred]);
//!
//! monitor.evaluate(&row(Value::NoVal, 20), &mut output).unwrap();
//! assert_eq!(output, [Value::Int(10)]); // The same evaluator retained 10.
//!
//! monitor.evaluate(&row(Value::Str("x[0]".into()), 30), &mut output).unwrap();
//! assert_eq!(output, [Value::Int(30)]); // Different text activates a new evaluator.
//!
//! monitor.evaluate(&row(Value::Str("x[1]".into()), 40), &mut output).unwrap();
//! assert_eq!(output, [Value::Deferred]); // Returning to x[1] starts fresh.
//! ```
//!
//! ## History outside the reconfigurable expression
//!
//! A delay in the fixed surrounding specification has its own lifetime. For example:
//!
//! ```text
//! z        = dynamic(source: Int)
//! previous = z[1]
//! ```
//!
//! | tick | active expression for `z` | `z` | `previous` |
//! |-----:|:--------------------------|----:|:-----------|
//! | 0 | `"x"` | 10 | `Deferred` |
//! | 1 | `"x"` | 20 | 10 |
//! | 2 | `"x + 100"` | 130 | 20 |
//!
//! Reconfiguring the expression that computes `z` changes only state inside that active body. It does
//! not change the `z[1]` operator, which belongs to the fixed definition of `previous`. Consequently,
//! `previous` can return the value produced by `z` before the reconfiguration. This is the same
//! ownership rule as above: state continues when its temporal operator continues.
//!
//! # `defer` handling
//!
//! `defer(source: T)` uses the same parsing, checking, scope validation, binding, environment shadow,
//! and nested-`Evaluator` representation as `dynamic`. `compiler::lower` marks it as
//! `ReconfigurableExpressionKind::Deferred`. Automatic and explicit scopes have the same availability and
//! cycle rules described above.
//!
//! Its activation policy differs: before activation, `NoVal` yields `NoVal` and `Deferred` yields
//! `Deferred`. The first accepted string activates one fresh evaluator and may reorder the monitor.
//! That evaluator and its active dependencies then remain fixed. Later strings do not recompile the
//! definition; every later source value merely accompanies another tick of the same active evaluator.
//! DSRV syntax has no retained-history length argument for `defer`: its optional type annotation and
//! explicit scope control typing and name availability, not historical backfill.
//!
//! Thus `defer` has exactly one evaluator timeline. Positive-delay history starts on the activation
//! tick and remains continuous across later strings, `NoVal`, and `Deferred`. If `"x[1]"` is the
//! first accepted definition, a later string such as `"x * 100"` does not change the program: the
//! next output still comes from the continuing `x[1]` timeline. On post-activation source `NoVal` or
//! `Deferred`, the sealed evaluator still advances and `defer` retains its last non-`NoVal`
//! published result.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/defer-lifecycle.svg")]
//! <figcaption>The first accepted string creates one evaluator. Its state advances across every later source value without recompilation; NoVal or Deferred source ticks retain the last non-NoVal published defer result.</figcaption>
//! </figure>
//!
//! For `z = defer(source: Int)`, the later `"x * 100"` source does not reconfigure the active
//! `x[1]` evaluator, and a later `Deferred` source does not interrupt its history:
//!
//! ```
//! # use trustworthiness_checker::{DsrvSpecification, Value, VarName};
//! # use trustworthiness_checker::dataflow::DataflowMonitor;
//! # let spec = "in source: Str\nin x: Int\nout z: Int\nz = defer(source: Int)"
//! #     .parse::<DsrvSpecification>().unwrap();
//! # let mut monitor = DataflowMonitor::compile_untyped(spec).unwrap();
//! # let input_vars = monitor.input_vars().to_vec();
//! # let row = |source: Value, x: i64| input_vars.iter().map(|var| {
//! #     if var == &VarName::new("source") { source.clone() } else { Value::Int(x) }
//! # }).collect::<Vec<_>>();
//! # let mut output = vec![Value::NoVal];
//! monitor.evaluate(&row(Value::Str("x[1]".into()), 20), &mut output).unwrap();
//! assert_eq!(output, [Value::Deferred]);
//!
//! monitor.evaluate(&row(Value::Str("x * 100".into()), 30), &mut output).unwrap();
//! assert_eq!(output, [Value::Int(20)]); // The original x[1] remains active.
//!
//! monitor.evaluate(&row(Value::Deferred, 40), &mut output).unwrap();
//! assert_eq!(output, [Value::Int(30)]); // The same history continues.
//! ```
//!

//! ## Memory policy and the published strategies
//!
//! Dataflow retains one bounded history per outer environment variable in `HistoryStore`. Static
//! analysis supplies the initial depths. Active `dynamic` and `defer` bodies contribute projected
//! runtime requirements, and the monitor keeps the maximum depth required by all current consumers.
//! Internal and recursive delays remain evaluator-local.
//!
//! The published language definition [[1]] calls an expression *solvable* when the monitor has retained
//! enough of every referenced stream, and has progressed far enough since those dependencies were
//! introduced, to evaluate the requested temporal indices. It compares three memory strategies:
//!
//! | published strategy | memory bound | history available to a newly active property |
//! |:-------------------|:-------------|:---------------------------------------------|
//! | **1. Retain the entire history** | Unbounded in the trace length. | Any earlier retained sample can be used, so a temporal reference can resolve immediately when the trace is long enough and its operands are available. |
//! | **2. Statically specify dynamic-property dependencies** | Bounded by declared stream and offset limits. | History is available up to each declared limit; a reference beyond that limit may never become solvable. |
//! | **3. Dynamically update dependencies** | Bounded, but the bound may change when a property becomes active. | An existing sufficiently deep dependency may already have retained the requested samples. Otherwise retention grows from the tick that introduces the new dependency, and `x[k]` may remain unavailable until enough later ticks have passed. |
//!
//! Property-level execution remains activation-local: a newly installed `x[k]` body does not borrow
//! history from another body or equation. Activating it does, however, add depth `k` to the monitor's
//! bounded context-retention requirement for `x`. The resulting history is maintained independently of
//! the body's local ring and can therefore survive as useful context for a later specification change.
//! Removing one consumer shrinks the retained bound only to the maximum still required by the others.
//!
//! Root context transfer preserves bounded histories for variables mapped between the old and new
//! monitor definitions. After active-body slots are projected into the target layout, effective
//! target depths are recomputed and matching live histories move by variable identity. A new
//! top-level temporal expression can then be immediately solvable when the old monitor retained
//! enough context. `MatchingStreamState` preserves the available bounded suffix for exact streams;
//! `None` installs a cold monitor. This history movement is independent of evaluator-local owner
//! matching.
//!
//! Strategy 1 is not available: Dataflow has no complete trace archive. Strategy 2 is also not exposed
//! by the language. An explicit scope such as `dynamic(source: Int, {x})` or
//! `defer(source: Int, {x})` permits the name `x`; it cannot declare a bound such as “retain four
//! samples of `x`.” The bound comes from the active compiled expression, and no construct accepts a
//! separate memory-strategy selector.
//!
//! # Scheduled plans, quickening, and native execution
//!
//! Everything above describes the canonical dataflow machine: bound graphs, stable environment
//! slots, dependency scheduling, persistent evaluator state, and the common temporal commit. That
//! model is sufficient to understand the language semantics and correctness of the interpreter.
//! This section elaborates the three-tier execution architecture. The semantic graph and scheduler
//! remain backend unaware. `MonitorExecution` combines a scheduler order with the already-bound
//! programs to make one backend-neutral `ScheduledExecutionPlan`; canonical execution, quickening,
//! and the optional JIT derive different physical executables from that same semantic plan.
//! Evaluators, rather than plans or compiled artifacts, own canonical language state; a native
//! artifact may temporarily hold a packed representation with an explicit plan-slot mapping.
//!
//! ## Type-specialized scalar plans
//!
//! Checked lowering records an optional `ScalarSignature` beside each graph operation. It contains
//! the exact input and output kinds for supported unary and binary AST nodes. Untyped lowering and
//! non-scalar nodes record `None`. These annotations do not change the graph's semantics: every
//! canonical operation remains present.
//!
//! Planning, not the graph IR, creates immutable `execution::quickening::Plan` values for
//! infallible programs with eligible scalar work. A plan has exactly one instruction per graph
//! node. A supported checked unary or binary operation becomes a scalar instruction; an eligible
//! `if` may carry plans for its branches; every other node becomes a `Canonical` instruction.
//! `StreamProgram` deliberately contains no quickening or JIT artifact.
//!
//! `Evaluator` always owns its canonical `EvaluatorState`. When planning finds quickened work, the
//! evaluator also owns a parallel `quickening::State` containing compact node values, lifting state,
//! and any persistent deoptimization decisions. The canonical state remains the semantic authority.
//!
//! Scalar instructions operate on `ScalarValue::{Int, Float, Bool, NoVal, Deferred}`. The direct set
//! is deliberately small:
//!
//! | shape | directly handled operations |
//! |:------|:----------------------------|
//! | Boolean unary | `not` |
//! | Integer unary | negation and absolute value |
//! | Floating-point unary | negation, absolute value, sine, cosine, and tangent |
//! | Numeric binary | addition, subtraction, multiplication, division, and modulo, including checked integer failure and mixed numeric promotion |
//! | Boolean binary | conjunction, disjunction, and implication |
//! | Scalar comparison | equality and ordered numeric/Boolean comparisons |
//!
//! Each operand is independently selected as a scalar constant, an earlier scalar node, a scalar
//! published by an earlier stream in the current execution layout, or a canonical `DataRef`.
//! Unsupported collections, maps, functions, temporal operators, and reconfiguration nodes continue
//! through `execution::interpreter::evaluate_node`. The result is a mixed quickening overlay rather
//! than a second semantic IR.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/specialization-overlay.svg")]
//! <figcaption>The optional scalar plan and state run beside the complete canonical graph and state; canonical execution remains available at every node.</figcaption>
//! </figure>
//!
//! ## Schedule-specific plan bundles
//!
//! `MonitorExecution` owns every `Evaluator` in a fixed `EvaluatorArena`, together with one optional
//! published scalar slot per logical stream. The immutable `MonitorPlan` remains the logical
//! description: stable stream slots, static dependencies, reconfiguration metadata, and the temporal
//! commit set. A `PlanBundle` provides selectable, schedule-specific routing over that stable state.
//! It owns no evaluator or language state. Its semantic `ScheduledExecutionPlan` records a unique
//! `PlanId`, ordered stream/program references, stable publication slots, stable `(stream, node)` state
//! identities, fallibility and temporal effects, and the logical commit set. Hotness and schedule-wide
//! native artifacts are tied to that identity.
//!
//! A bundle pairs that semantic plan with a derived `QuickPlan`. Sharing a semantic plan does not
//! require sharing a physical instruction format: canonical execution follows its ordered stream
//! descriptors, the quick plan records `Graph` and `ScalarRun` steps, and native lowering
//! constructs SSA. The quick view uses:
//!
//! - A `Graph` step enters the normal graph-level evaluator. It may still execute a mixed scalar
//!   plan internally.
//! - A stream is eligible for direct scalar execution only when its complete graph is one unary or
//!   binary node and that node is the output.
//! - Consecutive eligible streams form a `ScalarRun`, avoiding repeated general graph traversal
//!   while retaining a descriptor and publication boundary for every logical stream.
//!
//! Every interpreted step publishes its result as a canonical `Value` in the stable environment slot. A scalar
//! result is additionally published in compact form for later streams in the same layout. Fan-out,
//! intermediate outputs, canonical instructions, and nested evaluators therefore retain their
//! ordinary observation points. A rich graph ends a direct run but can publish a scalar result that
//! allows a later stream to specialize.
//!
//! The initial schedule creates the first bundle. When dynamic dependencies change the order,
//! `ExecutionEngine` selects a matching cached bundle or builds a new one; it retains at most four
//! previous bundles. Since plans contain identities, immutable program references, effects, and
//! routing but no mutable evaluator state, selecting a different bundle cannot reset delay rings,
//! branch state, function frames, active dynamic expressions, or deoptimization decisions.
//! Schedule-independent per-stream native regions can be reused; a whole-plan artifact is rebuilt for
//! a different `PlanId`.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/execution-layout.svg")]
//! <figcaption>The logical monitor plan and fixed evaluator arena survive schedule changes; only the small scheduled plan is selected or rebuilt.</figcaption>
//! </figure>
//!
//! ## Example quickened scheduled plan
//!
//! Recall the three equations:
//!
//! | stream | equation | relevant shape |
//! |:-------|:---------|:---------------|
//! | `scaled` | `x * 2` | One stateless scalar operation. |
//! | `total` | `default(total[1], 0) + scaled` | Temporal state and `default`, followed by scalar addition. |
//! | `alert` | `total > 20` | One stateless scalar comparison. |
//!
//! The example near the start of this page uses the untyped entry point, which provides no scalar
//! signatures and therefore executes all three streams as canonical `Graph` steps. Compiling the
//! same specification through [`DataflowMonitor::compile_checked`] allows the quickening planner to
//! select scalar instructions.
//!
//! Three labels are enough to read the resulting plan:
//!
//! - **`ScalarRun`** means that the complete current-tick computation of each enclosed stream can
//!   use the direct scalar executor.
//! - **`Graph`** means that the general graph traversal is required. Individual operations inside
//!   it may still be scalar.
//! - **publish** means that the stream result is written to the ordinary canonical environment and,
//!   when scalar, also made available in compact form to later planned streams.
//!
//! With those definitions, the plan is:
//!
//! ```text
//! input x
//!   │
//!   ▼
//! ScalarRun [scaled]
//!   scalar: x * 2
//!   publish scaled
//!   │
//!   │ published scaled
//!   ▼
//! Graph [total]
//!   canonical: read previous total, then apply default(..., 0)
//!   scalar:    add published scaled
//!   publish total
//!   stage total as the next previous value
//!   │
//!   │ published total
//!   ▼
//! ScalarRun [alert]
//!   scalar: total > 20
//!   publish alert
//!   │
//!   ▼
//! temporal commit, then output projection
//! ```
//!
//! The dependency order is still `scaled, total, alert`; quickening has not reordered the
//! language. `total` separates the two direct runs because its complete graph cannot use the narrow
//! one-operation executor. It is not wholly unspecialized, however. Its temporal read and `default`
//! use canonical state, after which the addition consumes the canonical base value and the published
//! scalar `scaled`.
//!
//! The `total` result is then published in both representations. The canonical copy preserves the
//! ordinary observable stream boundary; the compact copy lets `alert` perform its comparison without
//! reading and converting the canonical value. Only after all three current-tick results exist does
//! the temporal commit make the staged `total` visible as `total[1]` on the next tick.
//!
//! Tick 2 walks through that same plan as follows:
//!
//! | action | value flow | state effect |
//! |:-------|:-----------|:-------------|
//! | Load input | `x = 8` | No temporal state changes. |
//! | Run `scaled` | Scalar multiplication produces and publishes `16`. | No temporal state. |
//! | Enter `total` | Canonical history returns the previous `total`, `8`; `default` therefore also produces `8`. | The old history remains readable throughout the tick. |
//! | Finish `total` | Scalar addition combines canonical `8` with published `scaled = 16`, producing and publishing `24`. | `24` is staged, but not committed yet. |
//! | Run `alert` | Scalar comparison consumes published `total = 24` and publishes `true`. | No temporal state. |
//! | Finish the tick | Commit staged `24`, then project `[alert, scaled, total]` as `[true, 16, 24]`. | Tick 3 will observe `total[1] = 24`. |
//!
//! ## Node-local deoptimization
//!
//! A scalar instruction deoptimizes when a runtime operand cannot be represented with its checked
//! kind—for example, when the public monitor API supplies a value inconsistent with the checked
//! model. On the first mismatch, the instruction transfers its retained scalar lifting operands
//! into the corresponding canonical `NodeState`, marks only that quickening node
//! `Deoptimized`, and evaluates the canonical operation for the current tick. Later ticks enter that
//! canonical operation directly.
//!
//! Other nodes and streams remain specialized. Scalar results are always mirrored into canonical
//! `node_values`; canonical and deoptimized stream results are converted back into published scalars
//! when their actual values permit it. Downstream quickening can therefore continue after a
//! local fallback.
//!
//! ## Interaction with the full language
//!
//! Type quickening does not define separate semantics for richer language features:
//!
//! - Temporal nodes remain canonical and use the same temporal commit described above. Scalar nodes
//!   before or after them can still use the mixed plan.
//! - A non-recursive `if` keeps canonical selection and independent branch `EvaluatorState` values,
//!   while either branch may carry its own plan. An `if` containing a recursive call stays canonical
//!   because active recursive depths use reset frames. At monitor level every `if` stream is a `Graph`
//!   step, but it can publish a scalar output for a later stream.
//! - Persistent and recursive function frames use ordinary `Evaluator` construction, so an eligible
//!   checked function body can own a local plan. Resetting a recursive frame clears per-invocation
//!   values and lifting state; a persistent deoptimization decision remains local to that instantiated
//!   evaluator.
//! - A checked, runtime-compiled `dynamic` or `defer` expression can build its own plan when its
//!   active program is infallible. The enclosing stream remains a fallible `Graph` step because
//!   parsing, type checking, scope validation, and source-barrier installation can fail.
//!
//! ## Complete temporal kernels
//!
//! The native backend can compile a complete infallible scheduled plan containing a checked subset:
//! scalar graphs, positive fixed delays of scalar inputs, recursive delays, and their `default`
//! nodes. One generated function loads external inputs, keeps cross-stream publications in a raw
//! environment, evaluates every stream in scheduler order, writes observable stream slots, and
//! commits the plan's temporal state. Intermediate publications therefore remain logical without
//! requiring canonical `Value` materialization. On success `MonitorExecution` returns directly;
//! there is no second monitor-level commit loop.
//!
//! Native writes are emitted at the plan's logical commit barrier, only after every checked scalar
//! operation in every scheduled stream has succeeded. A presence,
//! representation, or arithmetic side exit therefore observes the pre-tick native state. The
//! runtime materializes that state into the canonical delay/default nodes, replays the previous
//! successful native row to reconstruct lifting state, and continues the current tick through the
//! same semantic plan in the canonical tier. The native layout maps each packed range back to the
//! plan's stable state slots. Complete plans do not eagerly compile a duplicate scalar-only native
//! fallback, and hot activation performs the inverse state promotion without resetting history.
//!
//! ## Tier boundary
//!
//! The optional JIT consumes the active `ScheduledExecutionPlan` directly and first attempts a
//! schedule-wide artifact. Eligible temporal schedules use one complete tick call. If the entire
//! plan is not lowerable, schedule-independent scalar regions derived from its stream descriptors
//! may still be reused while dispatch through the quickening interpreter preserves the logical
//! commit barrier. If native execution is pending or unavailable, the engine executes quickened instructions;
//! instructions that cannot preserve the checked scalar contract enter the canonical evaluator
//! with retained state transferred explicitly. Dynamic and otherwise fallible programs remain
//! canonical. Native compilation is feature-gated, but the canonical and quick tiers, planner,
//! plan cache, and state contract are always built.
//!
//! In the seven-phase logical tick described earlier, only phase 5 is elaborated physically. The
//! scheduler supplies the active dependency order, `MonitorExecution` selects the corresponding
//! bundle. Canonical and quick execution interpret its order and commit set; native execution may
//! compile both into one function while preserving their observation points. Source resolution,
//! cycle rejection, error handling, and output projection are unchanged.
//!
//! # References
//!
//! 1. M. H. Kristensen, T. Wright, C. Gomes, L. Esterle, and P. G. Larsen,
//!    “DynSRV: Dynamically Updated Properties for Stream Runtime Verification,” in *Runtime
//!    Verification*, 2025. [doi:10.1007/978-3-032-05435-7_7][1]
//! 2. N. Halbwachs, P. Caspi, P. Raymond, and D. Pilaud, “The Synchronous Data Flow Programming
//!    Language LUSTRE,” *Proceedings of the IEEE*, 79(9), 1305–1320, 1991.
//!    [doi:10.1109/5.97300][2]
//!
//! [1]: https://doi.org/10.1007/978-3-032-05435-7_7
//! [2]: https://doi.org/10.1109/5.97300
//!
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use crate::core::{RuntimeFunction, StreamType, Value};
use crate::lang::dsrv::ast::{DsrvSpecification, Expr};
use crate::lang::dsrv::type_checker::{StreamTypeEnvironment, TCType};
use crate::{Specification, VarName};
use ecow::{EcoString, EcoVec};

use self::environment::{EnvironmentLayout, EnvironmentSlot};

mod compiler;
mod environment;
mod error;
mod execution;
mod expression_activation;
mod history;
mod history_requirements;
mod ir;
#[cfg(feature = "jit")]
mod jit_api;
mod monitor;
mod monitor_plan;
mod program;
mod reconfiguration;
mod reconfiguration_mapping;
mod scheduler;
mod stream_id;
#[cfg(test)]
mod tests;
pub mod typed;

pub use typed::{
    TypedBindingError, TypedDataflowMonitor, TypedEvaluationError, TypedField, TypedInput,
    TypedInterface, TypedKind, TypedMonitor, TypedOutput, TypedScalar,
};
// Only the always-native monitor needs the feature; `TypedDataflowMonitor` warms on the
// canonical tier and is available either way.
pub use error::{
    DataflowCompilationError, DataflowEvaluationError, DataflowStateError, StreamProgramError,
};
#[cfg(feature = "jit")]
pub use typed::TypedJitMonitor;

#[cfg(feature = "jit")]
pub use jit_api::{JitConfig, JitPlan, JitReport};
pub use monitor::DataflowMonitor;
pub(crate) use monitor::MonitorReconfigurationPlan;
pub use monitor_plan::ReconfigurableExpressionId;
pub use program::DataflowProgram;
pub use reconfiguration::{
    ContextTransferPolicy, ContextTransferReport, DefinitionKey, InterfaceRevision,
    MonitorRevision, ReconfigurationReport, StreamStateKey, StreamStateTransfer,
    StreamStateTransferOutcome,
};
#[cfg(test)]
#[allow(unused_imports)]
pub(in crate::dataflow) use reconfiguration_mapping::ReconfigurationMappingError;
#[allow(unused_imports)]
pub(in crate::dataflow) use reconfiguration_mapping::{
    EnvironmentMapping, ReconfigurationMapping, StreamMapping,
};
