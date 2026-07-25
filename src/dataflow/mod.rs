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
//! | **4. Build executables** | `StreamProgram` and `StreamEvaluator` | Pair each immutable bound program with its persistent per-stream state. |
//! | **5. Plan execution** | `ExecutionPlan` and `Scheduler` | Record static structure and maintain the active runtime evaluation order. |
//! | **6. Run ticks** | `DataflowMonitor` | Load input rows, evaluate streams, commit temporal state, and project output rows. |
//!
//! The monitor is compiled once and evaluated many times. Compilation lowers each equation into a
//! `compiler::pipeline::LoweredDataflow`, derives a temporary [`crate::lang::core::DepGraph`] from
//! same-tick free variables, topologically orders the streams, assigns environment slots, validates
//! and binds references, and creates one stateful evaluator per computed stream. It also builds an
//! immutable `execution_plan::ExecutionPlan`: stable stream slots, static dependencies, the
//! reconfiguration points and prerequisite source streams, the streams requiring a temporal commit,
//! and fast-path flags. The monitor separately owns the mutable `scheduler::Scheduler`, whose cached
//! order, active dynamic edges, and iterative-DFS workspace are reused across ticks.
//!
//! Evaluation repeats the right-hand side of the figure for every logical tick: load the input row,
//! resolve runtime definitions and their exact dependencies, execute stream programs in dependency
//! order while filling stable environment slots, commit temporal writes, and project outputs. This
//! compile-once translation from synchronous stream equations to a dependency-ordered sequential
//! machine follows the approach of Lustre [[2]], applied to the DSRV language and dynamic-property
//! semantics introduced as DynSRV [[1]]. [`DataflowMonitor`] is the synchronous row interface;
//! [`crate::runtime::dataflow::DataflowRuntimeBuilder`] adapts it to [`crate::core::InputStream`] and
//! [`crate::core::OutputHandler`].
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
//! retained so later `dynamic`/`defer` source strings are type checked at installation time.
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
//! `LoweredDataflow::into_monitor` consumes the named graphs. It assigns a slot to every declared
//! input followed by every computed stream in the initial dependency order, binds each graph,
//! creates its `StreamEvaluator`, and builds the `ExecutionPlan`. The temporary named dependency
//! graph is discarded, but its per-stream static dependency sets are retained in the plan so the
//! scheduler can merge them with active runtime dependencies.
//! `EnvironmentLayout` maps each `VarName` to an `EnvironmentSlot` used during binding.
//! `DataflowMonitor::environment_values` is the matching fixed-size row: input values occupy its
//! initial slots and each stream evaluator writes its result to its assigned slot.
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
//! <figcaption>Environment slots remain stable even if runtime dependencies reorder evaluators; outputs project their own API order through saved IDs.</figcaption>
//! </figure>
//!
//! `EnvironmentLayout` owns the immutable `VarName -> EnvironmentSlot` mapping and assigns contiguous
//! slots at construction. Binding consumes a graph and produces an
//! `ir::EvaluationGraph<EnvironmentSlot>`, replacing each `DataRef::External(VarName)` with
//! `DataRef::External(EnvironmentSlot)`. Every top-level `StreamProgram` shares the layout via `Rc`;
//! function bodies use a local captures-then-parameters layout, and runtime-compiled dynamic programs
//! bind against the outer layout after scope validation.
//!
//! `DataflowMonitor` owns the mutable row as `environment_values: Vec<Value>`, a stable
//! `Vec<StreamEvaluator>`, the immutable `ExecutionPlan`, the mutable `Scheduler`, and the output
//! projection in `output_slots`. The scheduler stores stream IDs rather than moving evaluators.
//! During one program, `EvaluationContext` borrows the complete row and resolves bound
//! `DataRef::External` directly against it. Dependency scheduling guarantees that a computed slot is
//! filled before a same-tick read. The shared layout and bound slots never change, even when the
//! scheduler repairs evaluation order.
//!
//! ## Bind and validate
//!
//! `compiler::bind::UnboundEvaluationGraph::bind_graph` shares the completed `EnvironmentLayout`,
//! validates nested graphs, and converts every external `VarName` into an `EnvironmentSlot`. An
//! occurrence of the current output is accepted only as the operand of a positive `Delay` (the
//! lowered form of `sindex`); `compiler::bind::bind_op` replaces that operation with
//! `RecursiveDelay`. Direct or zero-delay recursion returns a [`StreamProgramError`], so a bound
//! recursive delay stores a `NonZeroU64`. Binding also resolves dynamic scopes, prepares function
//! capture layouts, and records the exact recursive-delay node IDs used by the post-output commit.
//!
//! The program figure shows the bound body for `total`. A `EvaluationGraph` owns an ordered
//! `Vec<StreamOp>`, a `DataRef` identifying its result, and the IDs of recursive delays. `NodeId`
//! is an index into that operation vector and its matching value/state vectors. The positive
//! self-reference has become `RecursiveDelay`; it reads previous-output history during the forward
//! pass. Only after `Add` produces the body output does `execution::interpreter::stage_recursive_delays`
//! retain that output as a pending write. A positive ordinary `SIndex` similarly marks its current
//! operand for capture. The monitor applies both kinds of pending writes after every stream has
//! produced its current-tick value.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/evaluation-graph.svg")]
//! <figcaption>Solid arrows are forward-pass reads; the dashed path is the post-output recursive-delay commit.</figcaption>
//! </figure>
//!
//! The bound graph becomes an `ir::StreamProgram`. This immutable structure owns the graph, an
//! `Rc<EnvironmentLayout>`, an `EvaluationMode::{Infallible, Fallible}` classification, and a cached
//! `requires_temporal_commit` flag. Sharing the program is important for function call sites and
//! runtime-compiled programs: each evaluator can own state without cloning operation vectors or
//! layouts.
//!
//! ## Execute one tick
//!
//! `execution::stream_evaluator::StreamEvaluator` pairs an `Rc<StreamProgram>` with one mutable
//! `execution::stream_state::StreamState`. `StreamState` has two parallel vectors indexed by `NodeId`:
//!
//! - `node_values: Vec<Value>` is the current result of each operation. A forward pass overwrites
//!   these slots on every evaluation, so later nodes can resolve `DataRef::Node` in constant time.
//! - `node_states: Vec<NodeState>` retains operator-specific lifting and cross-tick data. Its
//!   variants include `Delay`, `LazyIf`, `Function`, `PersistentCall`, and `Dynamic`, in addition to
//!   the lifting state used by ordinary operators.
//!
//! `StreamEvaluator::evaluate_infallible_and_stage` uses
//! `execution::interpreter::evaluate_nodes`; `evaluate_and_stage` selects that same fast path for an
//! infallible program or `try_evaluate_nodes` for a fallible one. `EvaluationContext` contains the
//! environment row, its layout, and an optional recursive-call callback. After traversal the evaluator
//! reads `EvaluationGraph::output`, stages recursive self-delays, and returns one stream value.
//! `evaluate_and_commit` is used where a nested invocation owns its complete tick; top-level stream
//! evaluators stage writes for the monitor's common commit phase.
//!
//! `DataflowMonitor::execute_tick` has these exact phases:
//!
//! 1. Load the complete input slice. A monitor with reconfiguration first clears the whole
//!    environment row to `NoVal`; a static monitor can overwrite in place because every stream slot
//!    will be recomputed.
//! 2. Evaluate, once and in static order, the infallible prerequisite streams needed to produce
//!    `dynamic`/`defer` source values.
//! 3. Resolve every reconfiguration point without evaluating its installed expression, collecting
//!    the active expression's same-tick dependency slots.
//! 4. Ask `Scheduler` to retain its cached order when valid or repair it with iterative DFS; reject
//!    a same-tick runtime cycle.
//! 5. Evaluate every stream not already consumed as a source prerequisite exactly once, using the
//!    infallible static fast path when possible, and write each result to its stable slot.
//! 6. Commit staged temporal state only for streams marked by `ExecutionPlan::temporal_streams`.
//! 7. Back in `evaluate`, project `output_slots` into the caller's output slice.
//!
//! The common commit lets mutually delayed streams capture each other's completed current values
//! without creating a same-tick scheduling cycle. Runtime-compiled programs and persistent direct
//! function applications participate through nested state. Dynamic dependencies are resolved before
//! stateful execution, so no stream advances twice merely because the order changed.
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
//! Top-level delays do not retain complete environment rows. Each positive `Delay` or
//! `RecursiveDelay` node owns a `NodeState::Delay(DelayState)`: a circular `values` buffer with
//! exactly as many entries as the requested offset, write/fill cursors, lifted last output, and
//! pending-write state. For example, `x[3]` behaves as follows:
//!
//! | tick | current `x` | retained before push | result |
//! |-----:|------------:|:---------------------|:-------|
//! | 1 | 10 | empty | `Deferred` |
//! | 2 | 20 | `[10]` | `Deferred` |
//! | 3 | 30 | `[10, 20]` | `Deferred` |
//! | 4 | 40 | `[10, 20, 30]` | 10 |
//!
//! An offset of zero lifts the current value without allocating a ring. Positive offsets store
//! `Deferred` as a sample; when a stored `NoVal` emerges, ordinary output lifting applies. A
//! positive ordinary delay reads history and stages a capture during node evaluation, then pushes
//! its operand during the post-row commit. A `RecursiveDelay` similarly reads during the forward
//! pass and stages the enclosing stream value after output computation; the same post-row traversal
//! applies that write. A failed tick never reaches this commit traversal, so pending ordinary and
//! recursive writes do not alter retained history.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/history-retention.svg")]
//! <figcaption>Each index owns a fixed-size ring; recursive history is staged after output and committed after the row, and dynamic history belongs to the installed evaluator.</figcaption>
//! </figure>
//!
//! Nested and repeated indices own separate rings, so retained storage is bounded by the sum of
//! offsets in active programs rather than shared per variable. A dynamic or deferred definition owns
//! a nested evaluator: reuse preserves its history, replacement drops it, and a new definition starts
//! without samples from before installation. Other stateful operations retain only their documented
//! last values or flags. Offsets are converted from `u64` to `usize` and allocated eagerly; the
//! compiler does not yet impose a configurable maximum history size.
//!
//! These structures encode the scheduling invariants visible in the diagrams: an immediate stream
//! read observes a producer scheduled earlier, a historical stream read observes retained state and
//! captures from the completed row, and a program node reads an earlier node slot. Ordinary lazy
//! branches advance independent state (recursive calls select one branch), persistent function call
//! sites retain nested evaluators (recursive frames reset per invocation), and recursive delays are
//! committed only after their enclosing output is known.
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
//! `NodeState::LazyIf(LazyIfState)` gives each branch its own persistent `StreamState` and retained
//! output, and also retains the last condition. In ordinary stream evaluation, both branches run on
//! every tick in which the enclosing stream is evaluated, regardless of the condition. This keeps
//! their independent temporal state aligned. Each branch output is lifted separately; if either
//! current-or-retained output is `NoVal`, the whole `if` produces `NoVal`. Otherwise `Bool(true)`
//! selects the then-value, `Bool(false)` selects the else-value, and a `Deferred` or `NoVal`
//! condition propagates. A `Deferred` value from the unselected branch does not affect the selected
//! result. Checked compilation normally prevents any other condition type.
//!
//! Recursive function evaluation is the supported exception to advancing both branches. When
//! `EvaluationContext` contains the recursive callback, `Bool(true)` evaluates only the then-branch
//! and `Bool(false)` evaluates only the else-branch. A `Deferred` or `NoVal` condition returns that
//! value without evaluating either branch. This genuinely lazy selection lets a recursive base case
//! return without entering the recursive branch.
//!
//! Reconfiguration points are not supported inside lazy branches: compilation rejects an `if`
//! branch containing `dynamic` or `defer`. Supported branch evaluation is therefore infallible and
//! has no branch-error suppression or rollback semantics. After an ordinary enclosing stream
//! evaluates successfully, temporal commits recurse into both branch states.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/lazy-if.svg")]
//! <figcaption>Ordinary stream evaluation advances both independent branch states; recursive evaluation follows only a Boolean-selected branch and skips both branches for a deferred or absent condition.</figcaption>
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
//! outer recursive call.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/function-call.svg")]
//! <figcaption>Normal Apply carries one callable's state across ticks; RecursiveApply isolates active recursion depths in reset frames returned to a per-call pool.</figcaption>
//! </figure>
//!
//! Application stream-lifts the function operand and every argument independently; `Deferred` or
//! `NoVal` propagates before a call. For a directly applied `fix` lambda, `compiler::lower` emits
//! `ir::StreamOp::RecursiveApply` and rewrites self calls as `ir::StreamOp::RecursiveCall`.
//! `evaluate_recursive_apply` supplies the callback through `EvaluationContext`. The recursive
//! `if` rule above evaluates only the selected branch, so a base case can return without evaluating
//! the recursive branch. Non-specialized function values still use the general `Fix` wrapper.
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
//! # `dynamic` handling
//!
//! ## Reconfiguration points
//!
//! At the model level, a **reconfiguration point** is simply a `dynamic(...)` or `defer(...)`
//! expression whose formula arrives through a string-valued input. The surrounding equation is fixed,
//! but the orange expression box in the figure can be supplied at runtime. Each point fixes three
//! things: the source input carrying the formula, the formula's result type, and the names that formula
//! is allowed to use.
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
//! At the start of a tick, the monitor installs or reuses each formula and collects these active
//! reads before evaluating the affected streams. When the containing stream later evaluates, the
//! point behaves like an ordinary subexpression and contributes its current value to the fixed
//! surrounding equation.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/reconfiguration-points.svg")]
//! <figcaption>Orange boxes are the three reconfiguration points; solid arrows show current producer-to-consumer value flow, while the one dashed arrow is an allowed value source that the current formula does not use.</figcaption>
//! </figure>
//!
//! ## Scope and scheduling
//!
//! `dynamic(source: T)` treats the current string value of `source` as a DSRV expression. `compiler::lower`
//! lowers the construct to `ir::StreamOp::Dynamic` with a `ir::DynamicExpressionSpec`. The spec records
//! the source operand, optional checked type information, allowed variables, and
//! `ir::DynamicExpressionMode::Dynamic`. Because installation can fail at evaluation time, a graph
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
//!   an installed expression that itself contains `dynamic` or `defer` is currently rejected with
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
//! `b -> a`. Installing all four potential reads would invent an `a`/`b` cycle even on ticks where
//! only one direction is used. The monitor instead activates only the names actually read by each
//! installed expression.
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
//! The `ExecutionPlan` determines source prerequisites and the `Scheduler` applies the tick phases
//! listed above. Reconfiguration resolution installs or reuses programs and collects exact same-tick
//! dependencies without evaluating the installed programs. The scheduler first checks its current
//! order and only runs its allocation-reusing iterative DFS when repair is needed; a cycle produces
//! [`DataflowEvaluationError::DynamicDependencyCycle`]. `EnvironmentLayout` and bound
//! `EnvironmentSlot` values remain stable; only stream IDs in the scheduled order change.
//!
//! Early dependency resolution imposes compile-time restrictions: every expression source must bind
//! directly to a constant or outer environment slot, not a node-local result; a reconfiguration point
//! cannot be hidden in a fallible lazy branch; and any computed stream needed to produce a source
//! (including static prerequisites) must be infallible and contain no reconfiguration point. Violations
//! produce [`DataflowCompilationError::UnsupportedReconfiguration`]. These constraints ensure source
//! streams can run once before scheduling and then be omitted from the main execution order.
//!
//! ## Runtime compilation and state
//!
//! During reconfiguration, `update_active_expression` compares the string with
//! `DynamicExpressionState::active_expression`. A new string is parsed as a DSRV expression,
//! optionally checked using the `StreamTypeEnvironment` and expected `TCType` retained by a checked
//! `DynamicExpressionSpec`, lowered, checked against the resolved allow-list, and bound to the existing
//! outer `EnvironmentLayout`. Its **same-tick** free variables become dependency slots; all free
//! variables are still scope checked. The resulting `StreamProgram` is installed in a new
//! `StreamEvaluator`. Repeating the same string reuses that evaluator, dependencies, and temporal
//! state. Changing the string replaces those three and clears the dynamic node's retained result, so
//! delay history such as `x[1]` begins at installation.
//!
//! `DynamicExpressionState` also owns `last_source_value`, `last_result`, and two outer-environment
//! vectors: `last_environment_values: Vec<Option<Value>>` and a private shadow
//! `environment_values: Vec<Value>`. Before evaluating (and, when needed, committing) the installed
//! program, every outer slot is lifted into that shadow: `NoVal` retains that slot's previous value,
//! while `Deferred` replaces it. The installed evaluator reads and captures history from this shadow,
//! not directly from the monitor row. The shadow survives a source replacement, but the installed
//! evaluator's own node/delay history does not.
//!
//! The source and result are also lifted. In `Dynamic` mode, `NoVal` or `Deferred` still advances an
//! installed evaluator to keep its timeline current, but the source special value is the node's raw
//! result (`NoVal` therefore retains the prior dynamic output; `Deferred` replaces it). A string
//! returns the installed expression's result. Before installation, `NoVal` yields `NoVal` and
//! `Deferred` yields `Deferred`. A changed string clears the retained result, so an initial `NoVal`
//! from the replacement cannot leak the old definition's output. Any non-string, non-special source
//! produces [`DataflowEvaluationError::InvalidExpressionSource`]. Installation errors are terminal to
//! the monitor.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/dynamic-lifecycle.svg")]
//! <figcaption>Equal source strings preserve evaluator state; a changed string installs a fresh evaluator.</figcaption>
//! </figure>
//!
//! This example compiles once for the first two ticks, preserving the active program, then replaces it
//! when the source changes:
//!
//! ```
//! use trustworthiness_checker::{DsrvSpecification, Value, VarName};
//! use trustworthiness_checker::dataflow::DataflowMonitor;
//!
//! let source = "in source: Str\nin x: Int\nout z: Int\n\
//!                   z = dynamic(source: Int)";
//! let spec = source.parse::<DsrvSpecification>().unwrap();
//! let mut monitor = DataflowMonitor::compile_untyped(spec).unwrap();
//! let input_vars = monitor.input_vars().to_vec();
//! let row = |source: Value, x: i64| {
//!     input_vars.iter().map(|var| {
//!         if var == &VarName::new("source") { source.clone() } else { Value::Int(x) }
//!     }).collect::<Vec<_>>()
//! };
//! let mut output = vec![Value::NoVal];
//!
//! monitor.evaluate(&row(Value::Str("x + 1".into()), 10), &mut output).unwrap();
//! assert_eq!(output, [Value::Int(11)]);
//! monitor.evaluate(&row(Value::Str("x + 1".into()), 20), &mut output).unwrap();
//! assert_eq!(output, [Value::Int(21)]);
//! monitor.evaluate(&row(Value::Str("x * 2".into()), 3), &mut output).unwrap();
//! assert_eq!(output, [Value::Int(6)]);
//! ```
//!
//! # `defer` handling
//!
//! `defer(source: T)` follows the same parse, type-check, scope-check, bind, and execution path as
//! `dynamic`. `compiler::lower` represents it with the same `ir::DynamicExpressionSpec`, using
//! `ir::DynamicExpressionMode::Defer`. Automatic and explicit scopes have the same availability and
//! cycle rules described above. The difference is activation: the first string value installs the
//! program and may reorder the monitor, while later string values replace neither the program nor its
//! active dependencies. In other words, the source stream supplies a deferred definition once
//! rather than a continuously reconfigurable definition.
//!
//! Before activation, `NoVal` yields `NoVal` and `Deferred` yields `Deferred`; there is no program to
//! tick. After activation, every tick evaluates the installed program, including ticks whose source
//! is special. A `NoVal` expression result repeats the installed expression's last result, while
//! `Deferred` replaces it. Later source strings still tick the original program rather than
//! recompiling them. Consequently, the installed expression's state and output timeline remain
//! continuous after the definition arrives:
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/defer-lifecycle.svg")]
//! <figcaption>The first accepted string fixes the program; every later tick advances that same evaluator.</figcaption>
//! </figure>
//!
//! ```
//! use trustworthiness_checker::{DsrvSpecification, Value, VarName};
//! use trustworthiness_checker::dataflow::DataflowMonitor;
//!
//! let source = "in source: Str\nin x: Int\nout z: Int\n\
//!                   z = defer(source: Int)";
//! let spec = source.parse::<DsrvSpecification>().unwrap();
//! let mut monitor = DataflowMonitor::compile_untyped(spec).unwrap();
//! let input_vars = monitor.input_vars().to_vec();
//! let row = |source: Value, x: i64| {
//!     input_vars.iter().map(|var| {
//!         if var == &VarName::new("source") { source.clone() } else { Value::Int(x) }
//!     }).collect::<Vec<_>>()
//! };
//! let mut output = vec![Value::NoVal];
//!
//! monitor.evaluate(&row(Value::Deferred, 1), &mut output).unwrap();
//! assert_eq!(output, [Value::Deferred]);
//! monitor.evaluate(&row(Value::Str("x + 1".into()), 2), &mut output).unwrap();
//! assert_eq!(output, [Value::Int(3)]);
//! // A later string is ignored: the installed `x + 1` definition remains active.
//! monitor.evaluate(&row(Value::Str("x * 100".into()), 3), &mut output).unwrap();
//! assert_eq!(output, [Value::Int(4)]);
//! monitor.evaluate(&row(Value::NoVal, 4), &mut output).unwrap();
//! assert_eq!(output, [Value::Int(5)]);
//! ```
//!
//! # Output buffering and asynchronous delivery
//!
//! [`DataflowMonitor::evaluate`] returns one fixed-width output row immediately for each logical tick.
//! The asynchronous dataflow runtime transposes successive rows into one `Vec<Value>` buffer per
//! declared output stream. A flush sends each vector through that output's channel; the receiving side
//! flattens the vectors back into an ordered stream of individual values for the
//! [`crate::core::OutputHandler`]. Buffering therefore changes transport granularity, not monitor
//! values or logical time.
//!
//! ## Flush policies
//!
//! | policy | when buffers flush | intended effect |
//! |:-------|:-------------------|:----------------|
//! | [`crate::core::ExecutionPolicy::Buffered`] (default) | After 256 logical ticks, or once more at input EOF for a partial batch. | Amortize channel and output-handler overhead across many values. |
//! | `ExecutionPolicy::Synchronous` | After every logical tick. | Ensure that tick's output batch has entered the bounded channels before polling the next input tick. |
//!
//! [`crate::runtime::dataflow::DataflowRuntimeBuilder::controlled_input`] selects synchronous policy,
//! so the runtime does not poll the next controlled input tick until the current output batches have
//! reached the channel boundary. “Synchronous” does not mean the external sink has already persisted
//! or consumed the values; the output handler still runs asynchronously.
//!
//! ## Backpressure and shutdown
//!
//! Each declared output has a bounded channel holding at most 1024 **batches**. A flush awaits channel
//! capacity, so a slow output handler eventually stops the engine from polling further input. If an
//! output receiver closes during a normal flush, the engine treats that as successful early
//! termination. If the output handler itself finishes first, its result wins and the engine future is
//! dropped.
//!
//! At normal input EOF, the engine attempts one final flush of its non-empty partial buffers, drops
//! its senders, and waits for the output handler to drain and finish. Input-stream errors, undeclared
//! variables, and monitor errors terminate the run; values accumulated since the previous successful
//! flush may therefore remain undelivered. EOF does not synthesize extra logical ticks, so delayed
//! monitor values are not drained after the last input row.
//!
//! ## Which inputs produce one buffered output row?
//!
//! The runtime converts [`crate::core::InputBatch`] values into logical ticks before appending outputs
//! to the buffers. `InputBatch::events` treats every event as a separate one-event tick;
//! `InputBatch::step` groups its events into one simultaneous tick; and internally packed fixed-width
//! steps represent several simultaneous ticks. For each tick, `DataflowEngine` starts with an
//! all-`NoVal` row, writes that tick's named events, calls the monitor once, and appends exactly one
//! value to every output buffer. Omitted inputs therefore receive `NoVal`. Undeclared event names are
//! runtime errors, while duplicate names in an atomic step are rejected when the batch is built.
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
mod execution_plan;
mod ir;
mod monitor;
mod scheduler;

#[cfg(test)]
mod tests;

pub use error::{DataflowCompilationError, DataflowEvaluationError, StreamProgramError};
pub use monitor::DataflowMonitor;
