# Language state

State retained by each `Evaluator` gives every stateful operation a lifetime independent of immutable `StreamProgram` sharing. Lifting, conditional branches, calls, and recursion each retain state at the operation occurrence that owns them.

## Lifting state

Ordinary operators lift over `NoVal` and `Deferred` according to their semantics. The evaluator stores operand and result state where an operation must combine values across sparse ticks. This state is indexed by graph-local `NodeId` inside one evaluator; an equal operation in another stream or call site has a separate owner.

## Conditional timelines

{{#include ../../assets/dataflow/lazy-if.svg}}

**Reading rule.** Ordinary conditionals advance both persistent branch evaluators before selecting a result. In recursive call context, the condition first applies retained lifting state: a `Value::NoVal` after an earlier Boolean can reuse that Boolean and advance only its selected branch. `Value::Deferred`, or an effective `Value::NoVal` with no retained Boolean, advances neither branch. Runtime-defined expressions are rejected inside lazy recursive branches.

This difference is part of language semantics, not an optimization. Branch-local delays and calls therefore have timelines determined by the applicable conditional form.

For `if condition then x + 1 else y + 2`, both sums advance even when only one is selected. After retention, an uninitialized `NoVal` branch makes the result `NoVal`; an unselected `Deferred` branch does not make the selected concrete result deferred. Recursive conditionals retain their lazy canonical execution policy. These rules are distinct from strict binary propagation, where `NoVal` takes precedence over `Deferred` after retaining each operand.

## Function binding

A function definition separates immutable body meaning from invocation state. Binding resolves captures to outer environment slots, places captures before parameters in a local layout, and packages the bound body as a shared `StreamProgram`.

{{#include ../../assets/dataflow/function-binding.svg}}

**Reading rule.** Capture mapping and local slot order are immutable. Mutable body state is created by the evaluator that invokes the shared program.

## Calls and recursion

A persistent call site retains one callable evaluator while the active function identity remains the same. Changing function identity creates fresh call-site state.

Recursive calls cannot share one mutable evaluator across active depths. They use a frame pool: each active depth acquires a frame with its own local row and evaluator state, resets it for that invocation, evaluates, and returns the frame to the pool as recursion unwinds.

{{#include ../../assets/dataflow/function-call.svg}}

**Reading rule.** The upper timeline follows one persistent call site across ticks. The lower layout separates simultaneously active recursion depths; returning a frame makes it reusable but does not merge states between depths.

## State identity and replacement

Root transfer matches whole stream evaluators by semantic state keys. Call-site, branch, lifting, and delay state move only as part of a compatible evaluator owner. A changed stream program starts cold rather than attempting field-by-field similarity.

Nested expression replacement applies the same principle within an active dynamic operation. Immutable body programs may be cached, but mutable evaluator instances belong to activations and are not shared through that cache.

## Resource behavior

`EvaluatorState` vectors are fixed by the bound graph. Delay rings are bounded by declared depth. Recursive frame storage grows to the maximum active recursion depth reached by that evaluator and reuses returned frames. Nested evaluator and route caches have separate lifetimes described in [dynamic properties](dynamic-properties.md) and [execution tiers](execution-tiers.md).

## Implementation mapping

The implementation mapping is concentrated in `src/dataflow/execution/evaluator_state.rs`, `src/dataflow/execution/functions.rs`, `src/dataflow/execution/node_evaluation.rs`, and evaluator tests.
