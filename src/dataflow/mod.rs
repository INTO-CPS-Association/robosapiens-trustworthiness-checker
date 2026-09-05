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
//! | **Physical execution** | Partition scheduler order into scalar regions, islands, and canonical graph steps. | `ExecutionPlan`, `ScalarRegion` |
//! | **Quickening and JIT** | Select typed interpreted or native executors for scalar regions without changing their boundaries. | `QuickenedRegionState`, native region coordinator |
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
//! ## Row representation: `Value` rows or typed rows
//!
//! [`DataflowMonitor`] exchanges `&[Value]` and `&mut [Value]` with its caller. That is the general
//! interface: it carries every language value, sparse `NoVal`/`Deferred` ticks included, and it is
//! what the async runtime and every reconfiguration path use.
//!
//! [`TypedDataflowMonitor`] is the alternative for callers that own their row types. It binds a
//! Rust tuple of `i64`, `f64`, and `bool` fields to a checked specification once, positionally
//! against the specification's input and output variables, and then drives the *same* monitor
//! lifecycle underneath. Nothing about scheduling, reconfiguration, hotness, or tier selection
//! changes; only `Value` construction at the API boundary is removed. With the `jit` feature and
//! [`TypedDataflowMonitor::compile_checked_with_jit`], reaching a whole-schedule native artifact
//! additionally moves the warmed state into a direct native entry, after which a tick neither
//! builds nor decodes a [`Value`] at all.
//!
//! The typed interface is narrower on purpose. Rows are at most eight scalar fields, and a tick
//! whose output is `NoVal` or `Deferred` is not representable, so it is reported as
//! `TypedEvaluationError::NonConcreteOutput` rather than approximated. Specifications that need
//! sparse outputs, non-scalar types, or more than eight interface variables use the `Value` rows.
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
//! `EvaluationMode::{Static, Dynamic}` classification, and a cached `requires_temporal_commit`
//! flag. Sharing the program is important for function call sites and runtime-compiled programs:
//! each `Evaluator` can own state without cloning operation vectors or layouts. The later
//! [scheduled plans and native execution](#scheduled-plans-quickening-and-native-execution) section
//! describes the optional metadata stored beside these semantic fields.
//!
//! ## Execute one tick
//!
//! `Evaluator` owns one `Rc<StreamProgram>` and one canonical `EvaluatorState`. The evaluator holds
//! no accelerator state of its own: quickened and native state belong to the execution plan's
//! regions, which materialize back into this arena at every transition. State is local and mutable;
//! root and nested transfer move selected owners destructively rather than using monitor-wide
//! snapshots, transactions, or copy-on-write state. `EvaluatorState` has two vectors indexed by
//! `NodeId`:
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
//! The canonical path uses `evaluate_nodes` for static programs. Dynamic programs use
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
//! branch containing `dynamic` or `defer`. Supported branch evaluation therefore uses the static
//! evaluator and has no branch-error suppression or rollback semantics. After an ordinary enclosing stream
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
//! evaluates and commits the shared static body, and returns the frame to a pool for that
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
//! A **reconfigurable expression** is a `dynamic(...)` or `defer(...)` occurrence whose formula
//! arrives at runtime through a string-valued input. The surrounding equation is fixed; only the
//! nested body, its evaluator state, and its active same-tick edges can change.
//!
//! This section states the observable contract at the API boundary. The architecture — how a scope
//! permits without depending, what compilation must fix in advance, when a newly activated
//! temporal reference becomes solvable, and how this relates to the published memory strategies —
//! is documented in the book under
//! [dynamic properties](https://github.com/your-org/robosapiens-trustworthiness-checker)
//! (`docs/src/architecture/dataflow/dynamic-properties.md`).
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/reconfigurable-expressions.svg")]
//! <figcaption>Orange boxes are the three reconfigurable expressions; solid arrows show current producer-to-consumer value flow, while the one dashed arrow is an allowed value source that the current formula does not use.</figcaption>
//! </figure>
//!
//! ## Activation policy
//!
//! The two operators differ only in when they accept a body, and that difference is the whole
//! observable contrast:
//!
//! | | `dynamic` | `defer` |
//! |---|---|---|
//! | first accepted string | activates a body | activates a body and **seals** |
//! | equal later string | keeps the same evaluator | ignored; body already sealed |
//! | different later string | activates a replacement | ignored; body already sealed |
//! | source `NoVal` after a string | behaves as the retained string | advances the sealed body |
//! | source `NoVal`/`Deferred` result | propagates the special value | retains its last non-`NoVal` result |
//!
//! A replacement body may take state from the immediately previous evaluator only when the two
//! compile identically; otherwise it starts cold. Source text is not a state key, so returning to
//! an earlier string never revives an earlier evaluator.
//!
//! Before any string is accepted there is no evaluator to advance: `NoVal` yields `NoVal` and
//! `Deferred` yields `Deferred`. A non-string, non-special source is
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
//! ## `defer` handling
//!
//! `defer(source: T)` uses the same parsing, checking, scope validation, binding, environment
//! shadow, and nested-`Evaluator` representation as `dynamic`. Only its activation policy differs,
//! as tabulated above: the first accepted string seals one evaluator, and every later source value
//! merely accompanies another tick of that same body. Positive-delay history therefore starts on
//! the activation tick and remains continuous.
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
//! # Execution tiers
//!
//! Everything above describes the canonical machine: bound graphs, stable environment slots,
//! dependency scheduling, persistent evaluator state, and the common temporal commit. That model
//! is sufficient to understand the language semantics and the correctness of the interpreter.
//!
//! Three physical routes execute it. Canonical graph evaluation always exists and defines meaning.
//! Quickening executes *scalar regions* — runs of whole streams, or islands inside one stream's
//! graph — decided from the `ScalarSignature` values compilation recorded, never from observing
//! values at runtime. With the `jit` feature, guarded native artifacts accelerate eligible work and
//! may, for an eligible static schedule, evaluate and commit a whole tick in one call.
//!
//! Exactly one executor owns a region's state at a time. Entering an accelerator synchronizes state
//! from the canonical arena; leaving it materializes state back. Every schedule change, context
//! transfer, and tier activation materializes first, so the canonical arena is always a complete
//! description of the monitor.
//!
//! Optimization replaces routing, never meaning. No tier defines a second interpretation of a tick,
//! and none of them changes publication order, the commit barrier, or which errors are visible.
//!
//! The architecture is documented in the book: `docs/src/architecture/dataflow/scalar-ir.md` for
//! the shared IR and what a region is, `execution-tiers.md` for tier selection and the state
//! handoff, and `typed-monitors.md` for the caller-side path that removes `Value` from the
//! boundary entirely.
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
