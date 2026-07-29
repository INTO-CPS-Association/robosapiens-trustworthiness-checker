//! A deliberately small specialization overlay for checked scalar operations.
//!
//! The canonical evaluation graph, `StreamState`, and `Value` slots remain the
//! semantic authority. A [`Plan`] selects scalar instructions where possible
//! and canonical instructions everywhere else; [`State`] retains only the
//! corresponding scalar lift and deoptimization state.
//!
//! The important invariants are:
//!
//! - canonical instructions call the canonical `evaluate_node`;
//! - specialized results are always mirrored into canonical `node_values`;
//! - canonical results are converted only at a scalar consumer boundary;
//! - scalar node-to-node edges are used only when the producer is scalar;
//! - `NoVal` and `Deferred` retain exactly the canonical lifting semantics;
//! - a runtime type mismatch deoptimizes one instruction, not its graph;
//! - deoptimization transfers retained lift state into canonical `NodeState`;
//! - immutable plans are shared by every evaluator of a function program;
//! - fallible dynamic graphs continue to use the canonical traversal.
//!
//! Lazy `if` branches recursively carry specialization plans because they are
//! separate evaluation graphs. Branches containing recursive self-calls remain
//! canonical: recursive frames are short-lived, and allocating specialization
//! state for each frame costs more than the small scalar body saves.
//!
//! Temporal storage, collections, maps, functions, and dynamic expressions
//! intentionally remain canonical. Extending the specialized set should
//! require benchmark evidence strong enough to justify duplicating the
//! relevant state transition rather than merely proving that it can be done.

mod interpreter;
mod plan;
mod scalar;
mod state;

pub(in crate::dataflow) use interpreter::{DirectResult, execute, execute_single};
pub(in crate::dataflow) use plan::{Plan, SingleScalarPlan};
pub(in crate::dataflow) use scalar::ScalarValue;
pub(in crate::dataflow) use state::State;
