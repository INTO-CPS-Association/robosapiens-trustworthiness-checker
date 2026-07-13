//! Compile-and-evaluate dataflow monitoring semantics.
//!
//! # Conceptual model
//!
//! A [`DataflowMonitor`] turns a DSRV specification into a synchronous machine. One call to
//! [`DataflowMonitor::evaluate`] is one logical tick: the caller supplies one [`Value`] for every
//! declared input and receives one value for every declared output. `Value::NoVal` means that an
//! input had no event on this tick, whereas `Value::Deferred` means that an expression cannot yet
//! produce a value (for example, while a delay is filling). Operator state, including delay
//! history and lifted operands, survives from one call to the next.
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
//! The monitor is compiled once and evaluated many times. Compilation lowers each equation into a
//! `compiler::compile::LoweredProgram`, derives a temporary [`crate::lang::core::DepGraph`] from
//! its free variables, topologically orders the plans, assigns environment slots, validates and
//! binds references, and creates one stateful executor per computed stream. Evaluation then repeats
//! the right-hand side of the figure for every logical tick: load the input row, execute stream
//! plans in dependency order while filling stable environment slots, and project declared outputs.
//! Ordinary monitors retain their initial order. A runtime-compiled expression can add or remove
//! active dependencies, in which case the monitor transactionally derives a new order for that
//! tick as described under [`dynamic`](#dynamic-handling).
//! The outer asynchronous runtime only normalizes provider data into these rows and batches the
//! resulting output values.
//! This compile-once translation from synchronous stream equations to a dependency-ordered
//! sequential machine follows the approach of Lustre [[2]], applied to the DSRV language and
//! dynamic-property semantics introduced as DynSRV [[1]].
//! `TryFrom` conversions let typed and untyped specifications compile into a monitor,
//! [`DataflowMonitor`] provides the synchronous row interface, and
//! [`crate::runtime::dataflow::DataflowRuntimeBuilder`] adapts that monitor to
//! [`crate::core::InputStream`] and [`crate::core::OutputHandler`].
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/pipeline.svg")]
//! <figcaption>Compilation creates the ordered monitor; evaluation reuses it for each logical input row.</figcaption>
//! </figure>
//!
//! ## Executing the example
//!
//! This executable version exercises compilation and the public monitor interface directly:
//!
//! ```
//! use trustworthiness_checker::{Value, VarName, dsrv_specification};
//! use trustworthiness_checker::dataflow::DataflowMonitor;
//!
//! let mut source = "in x: Int\n\
//!     out alert: Bool\n\
//!     out total: Int\n\
//!     out scaled: Int\n\
//!     alert = total > 20\n\
//!     total = default(total[1], 0) + scaled\n\
//!     scaled = x * 2";
//! let spec = dsrv_specification(&mut source).expect("valid DSRV specification");
//! let mut monitor = DataflowMonitor::try_compile_untyped(spec).expect("valid dataflow");
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
//! # Compilation, plans, and state
//!
//! ## Lower and order
//!
//! `compiler::lower` lowers typed or untyped AST expressions. Its private `PlanBuilder` pushes operands before
//! consumers, producing a `plan::PlanBody<VarName>`. The same generic representation is used
//! recursively for lazy branches and function bodies. Before binding, its
//! `plan::DataRef<VarName>` operands have these meanings:
//!
//! - `Const(Value)` embeds a literal or special value.
//! - `External(VarName)` is an unresolved reference outside this plan body.
//! - `Node(NodeId)` reads an earlier operation result in this body.
//!
//! `compiler::compile::LoweredProgram::build` collects every body's statically visible free
//! variables and rejects names absent from the specification. A temporary
//! [`crate::lang::core::DepGraph`] is built from the lowered bodies rather than directly from the
//! AST because lowering has already resolved function parameters and captures. A `dynamic` or
//! `defer` source operand is visible here, but dependencies in its eventual source string are not.
//! `DepGraph::topological_streams` orders dependencies before their consumers and reports a
//! same-tick cycle. Inputs are graph leaves already present in the environment and do not need plans.
//! A stream's own name is excluded from this graph because only a positive delayed self-reference
//! is legal, and that dependency belongs to persistent history rather than the current environment.
//!
//! `LoweredProgram::into_monitor` consumes the graph and bodies. It assigns a slot to every declared
//! input followed by every computed stream in the initial dependency order, binds each body,
//! creates its `PlanExecutor`, and returns the monitor. The temporary graph is discarded, but its
//! static dependency sets are retained so active runtime dependencies can later be merged and
//! sorted again.
//! `EnvironmentLayout` maps each `VarName` to an `EnvironmentId` used during binding.
//! `DataflowMonitor::environment_values` is the matching fixed-size row: input values occupy its
//! initial slots and each stream executor writes its result to its assigned slot.
//!
//! In the example, tick 2 begins with the API input row `[x = 8]`. Evaluation fills that row in
//! dependency order to `[x = 8, scaled = 16, total = 24, alert = true]`. The output API has its own
//! order, exposed by [`DataflowMonitor::output_vars`]. The figure illustrates the reported order
//! `[alert, total, scaled]`, for which compilation saves environment IDs with indices `[3, 2, 1]`.
//! Producing the output row is a projection of existing environment slots, not another evaluation
//! or a reordering of executors. Callers must align slices with [`DataflowMonitor::input_vars`] and
//! `output_vars()` rather than assume declaration or dependency order.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/environment-layout.svg")]
//! <figcaption>Environment slots remain stable even if runtime dependencies reorder executors; outputs project their own API order through saved IDs.</figcaption>
//! </figure>
//!
//! `EnvironmentLayout` owns the immutable `VarName -> EnvironmentId` mapping and assigns contiguous
//! IDs at construction.
//! Binding consumes that body and produces a `plan::PlanBody<EnvironmentId>`, replacing each
//! `DataRef::External(VarName)` with `DataRef::External(EnvironmentId)`, where `EnvironmentId` is a
//! small index newtype. Every top-level `ExecutablePlan` shares the layout via
//! `Rc`; function bodies use a local captures-then-parameters layout, and runtime-compiled dynamic
//! plans bind against the outer layout after scope validation.
//!
//! `DataflowMonitor` owns the mutable row as `environment_values: Vec<Value>`, a stable
//! `Vec<PlanExecutor>`, a separate executor-order vector, and the output projection in
//! `output_ids`. During one plan, `EvalContext` borrows the complete row and resolves bound
//! `DataRef::External` directly against it. Topological scheduling guarantees that a computed slot
//! is filled before a plan reads it. Each tick replaces the row values while the shared layout and
//! bound indices remain unchanged, even when the executor order changes.
//!
//! ## Bind and validate
//!
//! `compiler::bind::PlanBody::bind` shares the completed `EnvironmentLayout`, validates nested bodies, and
//! converts every external `VarName` into a compact `EnvironmentId`. An occurrence of the current
//! output is accepted only as the operand of a positive `SIndex`; `compiler::bind::bind_op` replaces that
//! operation with `RecursiveSIndex`. Direct or zero-delay recursion returns a
//! [`PlanValidationError`]; a bound recursive delay therefore stores a `NonZeroU64`. Binding also
//! resolves dynamic scopes, prepares function capture layouts,
//! and records the exact recursive delay node IDs used by the post-output commit.
//!
//! The plan figure shows the bound body for `total`. A `PlanBody` owns an ordered
//! `Vec<DataflowOp>`, a `DataRef` identifying its result, and the IDs of recursive delays. `NodeId`
//! is an index into that operation vector and its matching result/state vectors. The positive
//! self-reference has become `RecursiveSIndex`; it reads previous-output history during the forward
//! pass. Only after `Add` produces the body output does `execution::interpreter::commit_recursive_delays` push
//! that output into the marked delay. An ordinary `SIndex` over another operand can read and push
//! its operand immediately when its node is evaluated.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/plan-body.svg")]
//! <figcaption>Solid arrows are forward-pass reads; the dashed path is the post-output recursive-delay commit.</figcaption>
//! </figure>
//!
//! The bound body becomes a `plan::ExecutablePlan`. This immutable structure owns the body, an
//! `Rc<EnvironmentLayout>`, and an `EvaluationKind` classification selecting static or fallible
//! traversal. Sharing the plan is important for function frames and runtime-compiled plans:
//! each invocation can own state without cloning operation vectors or layouts.
//!
//! ## Execute one tick
//!
//! `execution::plan_executor::PlanExecutor` pairs an `Rc<ExecutablePlan>` with one mutable
//! `execution::state::DataflowState`. `DataflowState` has two parallel vectors indexed by `NodeId`:
//!
//! - `nodes: Vec<Value>` is the current result of each operation. A forward pass overwrites these
//!   slots on every evaluation, so later nodes can resolve `DataRef::Node` in constant time.
//! - `states: Vec<NodeState>` retains only cross-tick data. Stateless operations use
//!   `NodeState::Stateless`; delays, lifted operators, lazy branches, and dynamic nodes use matching
//!   variants with their histories or nested state.
//!
//! `PlanExecutor::evaluate` creates an `execution::plan_executor::EvalContext` containing the shared environment
//! row, its layout, and an optional recursive-function callback. Static plans call
//! `execution::interpreter::eval_nodes_at`; plans marked fallible call `execution::interpreter::try_eval_nodes_at`, which
//! handles dynamic nodes and delegates ordinary operations to the same evaluator. The executor then
//! reads `PlanBody::output`, commits recursive delays, and returns one stream value.
//!
//! Finally, `DataflowMonitor` runs its executors in topological order and writes each result to its
//! stable environment slot. The environment row is replaced on the next
//! monitor tick, but each executor's `DataflowState` remains. A monitor containing runtime-compiled
//! expressions snapshots those states before evaluation so a newly discovered order can retry the
//! same tick without advancing temporal state twice. An evaluation error permanently fails the
//! monitor because successfully rolling back an arbitrary user-visible failure is not part of the
//! monitor contract.
//!
//! ## History management
//!
//! Dataflow does not retain complete environment rows. Each `SIndex` node owns a
//! `NodeState::Delay` containing an `execution::state::SIndexValueHistory`: a circular buffer with
//! exactly as many entries as the requested offset. For example, `x[3]` behaves as follows:
//!
//! | tick | current `x` | retained before push | result |
//! |-----:|------------:|:---------------------|:-------|
//! | 1 | 10 | empty | `Deferred` |
//! | 2 | 20 | `[10]` | `Deferred` |
//! | 3 | 30 | `[10, 20]` | `Deferred` |
//! | 4 | 40 | `[10, 20, 30]` | 10 |
//!
//! An offset of zero lifts the current value without allocating a ring. Positive offsets store
//! `Deferred` as a sample; when a stored `NoVal` emerges, ordinary output lifting applies. An
//! ordinary delay reads and pushes its operand during node evaluation. A `RecursiveSIndex` reads
//! during the forward pass, but `execution::interpreter::commit_recursive_delays` cannot push the
//! enclosing stream value until its output has been computed.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/history-retention.svg")]
//! <figcaption>Each index owns a fixed-size ring; recursive history commits after output, and dynamic history belongs to the installed executor.</figcaption>
//! </figure>
//!
//! Nested and repeated indices own separate rings, so retained storage is bounded by the sum of
//! offsets in active plans rather than shared per variable. A dynamic or deferred definition owns
//! a nested executor: reuse preserves its history, replacement drops it, and a new definition starts
//! without samples from before installation. Other stateful operations retain only their documented
//! last values or flags. Offsets are converted from `u64` to `usize` and allocated eagerly; the
//! compiler does not yet impose a configurable maximum history size.
//!
//! These structures encode the scheduling invariants visible in the diagrams: a stream reads an
//! environment slot whose producer was scheduled earlier, a plan node reads an earlier node slot,
//! lazy branches advance independent state, function calls reset isolated frames, and recursive
//! delays are committed only after their enclosing output is known.
//!
//! # `if` handling
//!
//! ## Representation and dependencies
//!
//! An `if` is lazy rather than a normal eager operation. `compiler::lower` emits the condition into the
//! enclosing plan, but builds the two alternatives as separate `plan::PlanBody` values inside a
//! `plan::DataflowOp::If`. Free-variable collection, validation, and binding visit both bodies, so
//! stream dependencies are conservative: a stream used by either branch must be available before
//! the stream containing the `if`, even if that branch is not selected on a particular tick.
//!
//! ## Selection and branch state
//!
//! `execution::state::NodeState::LazyIf` owns one `execution::state::DataflowState` for each branch. At evaluation,
//! both bodies advance their independent temporal state against the current outer tick, and the
//! already-computed condition selects one result:
//!
//! - `Bool(true)` returns the `then` result and suppresses errors from the `else` body.
//! - `Bool(false)` returns the `else` result and suppresses errors from the `then` body.
//! - `NoVal` or `Deferred` is returned after both branch timelines advance; errors from both
//!   bodies are suppressed.
//! - A different condition value is invalid (and is normally prevented by typed compilation).
//!
//! The condition and each branch output are stream-lifted independently. If either branch has no
//! current or retained value, the combined result is `NoVal`; otherwise the condition selects the
//! branch value. An unselected `Deferred` does not affect the selected result.
//!
//! The selected body reads the same outer environment and logical input row as its enclosing plan.
//! Its nodes use its own result buffer, and any recursive delay nodes in that branch are committed
//! after the branch result is known. If either branch contains `dynamic` or `defer`, the enclosing
//! executable is classified as fallible. Both paths advance, but only an error from the selected
//! path is observable. This is the sense in which the conditional remains lazy.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/lazy-if.svg")]
//! <figcaption>Each branch owns independent persistent state and advances on every tick; selection remains lazy with respect to errors.</figcaption>
//! </figure>
//!
//! # Function handling
//!
//! ## Definition and binding
//!
//! `compiler::lower` lowers a lambda to `plan::DataflowOp::Function` containing a
//! `plan::DataflowFunctionDef<VarName>`: parameter names, an unbound body plan, display text, and
//! capture metadata. Binding converts it to `DataflowFunctionDef<EnvironmentId>` with a shared
//! executable body plan. An application becomes `plan::DataflowOp::Apply`; partial
//! application and `List.map`, `List.filter`, and `List.fold` have dedicated operations but invoke
//! the same runtime function representation.
//!
//! `compiler::bind::bind_function` computes the body's free variables, removes its parameters, and resolves
//! each remaining capture to a source slot in the enclosing environment. It then gives the body a
//! compact local environment with captures first and parameters second, and binds all body
//! references to those slots. Captures are therefore selected once during compilation without
//! copying values into the immutable plan.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/function-binding.svg")]
//! <figcaption>Binding stores capture source environment IDs and gives the shared body plan a captures-first local layout.</figcaption>
//! </figure>
//!
//! Function bodies may contain ordinary value operations and lazy `if` expressions, but binding
//! rejects `sindex`, `init`, `when`, `update`, `latch`, and `dynamic`/`defer` inside a function body.
//! Those operators require a persistent stream timeline, whereas a function call is an isolated
//! value evaluation.
//!
//! ## Application and recursion
//!
//! When the `Function` node is evaluated on a tick, `execution::functions::DataflowFunctionCall::new` snapshots
//! the current capture values into a call template and wraps the call in a [`RuntimeFunction`]. Each
//! invocation obtains a `execution::functions::DataflowFunctionFrame`, fills its parameter slots, resets its
//! `execution::plan_executor::PlanExecutor`, and evaluates the shared body plan. Frames are pooled after calls, so
//! repeated calls reuse allocations but never leak node state or arguments between invocations.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/function-call.svg")]
//! <figcaption>Capture values belong to the current tick; argument and node state belongs to one resettable invocation frame.</figcaption>
//! </figure>
//!
//! Application stream-lifts the function operand and every argument independently. `Deferred`
//! still propagates before calling the function. A normal call uses `plan::DataflowOp::Apply`. For a directly applied `fix` lambda,
//! `compiler::lower` instead emits `plan::DataflowOp::DirectFixApply` and rewrites calls to the self parameter
//! as `plan::DataflowOp::RecursiveCall`. `execution::functions::eval_direct_fix_apply_op` supplies the recursive
//! callback through `execution::plan_executor::EvalContext`. Because `if` is lazy, a recursive base-case branch can
//! return without evaluating the recursive branch. Non-specialized function values still use the
//! general `fix` wrapper in `functions`.
//!
//! This example shows both capture-by-current-tick and direct recursive application. `bias` is
//! captured anew when each function node is evaluated, while every recursive call in that tick
//! sees the same captured value:
//!
//! ```
//! use trustworthiness_checker::{UntypedDsrvSpecification, Value, VarName};
//! use trustworthiness_checker::lang::core::parser::SpecParser;
//! use trustworthiness_checker::lang::dsrv::lalr_parser::LALRParser;
//! use trustworthiness_checker::dataflow::DataflowMonitor;
//!
//! let mut source = "in bias: Int\nin n: Int\nout direct: Int\nout recursive: Int\n\
//!     direct = (\\x: Int -> x + bias)(n)\n\
//!     recursive = fix(\\self: (Int -> Int), k: Int -> if k == 0 then bias else self(k - 1) + 1)(n)";
//! let spec = <LALRParser as SpecParser<UntypedDsrvSpecification>>::parse(&mut source).unwrap();
//! let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();
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
//! ## Scope and scheduling
//!
//! `dynamic(source: T)` treats the current string value of `source` as a DSRV expression. `compiler::lower`
//! lowers the construct to `plan::DataflowOp::Dynamic` with a `plan::DynamicSpec`. The spec records
//! the source operand, target type information for typed specifications, allowed variables, and
//! `plan::DataflowDynamicMode::Dynamic`. Because parsing can fail at evaluation time, a body
//! containing this node (including in a lazy branch) is marked fallible.
//!
//! Scope resolution happens while the containing specification is compiled:
//!
//! - An automatic scope, as in `dynamic(source: T)`, may refer to every declared input or computed
//!   stream except the stream containing the expression. Merely making a variable available does
//!   not create a dependency; only a received expression that actually reads it adds an active edge.
//! - An explicit scope, as in `dynamic(source: T, {x, intermediate})`, is an allow-list. It restricts
//!   which names a received expression may read, but unused names do not constrain scheduling.
//! - A dynamic expression compiled inside another dynamic expression inherits the intersection of
//!   its own requested scope and its parent's scope. It cannot use nesting to recover a variable
//!   hidden by the outer allow-list.
//!
//! A source string can therefore introduce a dependency that the previously active expression did
//! not use, including a dependency on another runtime-compiled stream. Consider:
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
//! If the sources are `a_source = "b + 1"` and `b_source = "x"`, the active order must be `b, a`.
//! If they later change together to `a_source = "x"` and `b_source = "a + 1"`, it must become
//! `a, b`. Adding every possible scope edge statically would report a false `a`/`b` cycle, so the
//! monitor instead schedules from the free variables of the active definitions.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/dynamic-dependencies.svg")]
//! <figcaption>Changing both definitions replaces their active edges as one transaction and reverses the executor order without changing environment IDs.</figcaption>
//! </figure>
//!
//! Scheduling a tick containing runtime-compiled expressions is transactional:
//!
//! 1. The monitor snapshots executor state and evaluates using the current order. New definitions
//!    are parsed, checked, and bound, and every active plan reports its actual free variables.
//! 2. Those runtime edges replace the previous active edges and are merged with the static graph.
//!    Changes from multiple `dynamic` expressions on the same tick enter this graph together.
//! 3. The monitor rejects a same-tick cycle with
//!    [`DataflowEvalError::DynamicDependencyCycle`]. Otherwise it topologically sorts the graph.
//! 4. If the order changed, the speculative state is discarded and the same input row is evaluated
//!    again from the snapshot. This repeats when nested runtime compilation reveals another edge.
//!    Only an evaluation whose observed dependencies agree with its order is committed.
//!
//! This retry is why delay and lifting state advances exactly once even on a reordering tick.
//! `EnvironmentLayout` and bound `EnvironmentId` values remain stable; only the index vector used
//! to visit top-level executors changes. A definition that would introduce a cycle cannot be
//! activated. As with other evaluation errors, a cycle permanently fails the monitor.
//!
//! ## Runtime compilation and state
//!
//! At evaluation, `execution::dynamic::eval_dynamic_value` compares the string with
//! `execution::state::DynamicState::active`. A new string is parsed with [`LALRParser`], optionally type
//! checked using the `TypeInfo` and result type retained by a typed `DynamicSpec`, lowered by
//! `compiler::lower`, checked against the resolved allow-list, and bound to the existing environment
//! layout. Its free variables become the active scheduling edges for the containing stream. The
//! resulting `plan::ExecutablePlan` is installed in a new `execution::plan_executor::PlanExecutor`.
//! Repeating the same string reuses that executor, its dependencies, and its temporal state;
//! changing the string replaces all three and starts the new expression with fresh state. Thus
//! history such as `x[1]` starts when that source expression is first introduced, not when the
//! enclosing monitor started.
//!
//! The definition source and the installed plan's result both use ordinary stream lifting:
//! `NoVal` repeats the last source or result, while `Deferred` is retained as a real sample. Thus a
//! `NoVal` source reuses and ticks the current definition. A changed source string installs a fresh
//! executor and clears the old definition's retained result; if the new expression first produces
//! `NoVal`, the dynamic output is therefore `NoVal`, not the previous definition's result. Before
//! any source string has been accepted, an initial special value is returned (and a following
//! `NoVal` repeats it). Any other source value is an
//! [`DataflowEvalError::InvalidDynamicValue`]. Parse, type, scope, or binding failures are also
//! evaluation errors and permanently fail the enclosing monitor.
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/dynamic-lifecycle.svg")]
//! <figcaption>Equal source strings preserve executor state; a changed string installs a fresh executor.</figcaption>
//! </figure>
//!
//! This example compiles once for the first two ticks, preserving the active plan, then replaces it
//! when the source changes:
//!
//! ```
//! use trustworthiness_checker::{Value, VarName, dsrv_specification};
//! use trustworthiness_checker::dataflow::DataflowMonitor;
//!
//! let mut source = "in source: Str\nin x: Int\nout z: Int\n\
//!                   z = dynamic(source: Int)";
//! let spec = dsrv_specification(&mut source).unwrap();
//! let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();
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
//! `dynamic`. `compiler::lower` represents it with the same `plan::DynamicSpec`, using
//! `plan::DataflowDynamicMode::Defer`. Automatic and explicit scopes have the same availability and
//! cycle rules described above. The difference is activation: the first string value installs the
//! plan and may reorder the monitor, while later string values replace neither the plan nor its
//! active dependencies. In other words, the source stream supplies a deferred definition once
//! rather than a continuously reconfigurable definition.
//!
//! Before activation, source lifting applies to `NoVal` and `Deferred`, but there is no plan to
//! tick. After activation, every tick evaluates the installed plan. A `NoVal` result repeats the
//! installed expression's last result, while `Deferred` replaces it. Later source strings still
//! tick the original plan rather than recompiling them. Consequently, the installed expression's
//! state and output timeline remain continuous after the definition arrives:
//!
//! <figure style="margin:1.25rem 0">
#![doc = include_str!("../../docs/src/assets/dataflow/defer-lifecycle.svg")]
//! <figcaption>The first accepted string fixes the plan; every later tick advances that same executor.</figcaption>
//! </figure>
//!
//! ```
//! use trustworthiness_checker::{Value, VarName, dsrv_specification};
//! use trustworthiness_checker::dataflow::DataflowMonitor;
//!
//! let mut source = "in source: Str\nin x: Int\nout z: Int\n\
//!                   z = defer(source: Int)";
//! let spec = dsrv_specification(&mut source).unwrap();
//! let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();
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

use crate::core::{RuntimeFunction, StreamType, StreamTypeAscription, Value};
use crate::lang::core::parser::ExprParser;
use crate::lang::dsrv::ast::{SpannedExpr, UntypedDsrvSpecification};
use crate::lang::dsrv::lalr_parser::LALRParser;
use crate::lang::dsrv::type_checker::{
    SExprTE, TCType, TypeCheckable, TypeInfo, TypedDsrvSpecification,
};
use crate::{Specification, VarName};
use ecow::{EcoString, EcoVec};

use self::environment::EnvironmentLayout;

mod compiler;
mod environment;
mod error;
mod execution;
mod monitor;
mod plan;

#[cfg(test)]
mod tests;

pub use error::{DataflowCompileError, DataflowEvalError, PlanValidationError};
pub use monitor::DataflowMonitor;
