//! One stream's program paired with its canonical state.
//!
//! [`Evaluator`] is deliberately thin: an `Rc<StreamProgram>` (immutable, shared with call sites and
//! runtime-compiled bodies) and a `Box<EvaluatorState>` (mutable, this stream's alone). It holds no
//! accelerator state — quickened registers and native artifacts belong to the execution plan's
//! regions, not here, which is what lets a tier change without disturbing language state.
//!
//! [`EvaluationEnvironment`] is the per-call context threaded alongside it: the environment row, its
//! layout, retained values where a reconfigurable body needs them, and the recursive-call closure
//! when one is in scope.

mod expression_state;
mod lifecycle;
mod reconfiguration;
#[cfg(test)]
mod tests;

use super::super::environment::EnvironmentLayout;
use super::super::history::HistoryId;
use super::super::ir::{BoundRef, StreamProgram};
use super::evaluator_state::EvaluatorState;
use crate::core::Value;
use ecow::EcoVec;
use std::rc::Rc;

#[derive(Clone, Copy)]
pub(in crate::dataflow) struct EvaluationEnvironment<'a> {
    pub(in crate::dataflow) environment_values: &'a [Value],
    pub(in crate::dataflow) environment_layout: &'a Rc<EnvironmentLayout>,
    pub(in crate::dataflow) retained_environment_values: Option<&'a [Value]>,
    pub(in crate::dataflow) recursive_call: Option<&'a dyn Fn(EcoVec<Value>) -> Value>,
}

impl EvaluationEnvironment<'_> {
    pub(in crate::dataflow) fn read_value(
        self,
        state: &EvaluatorState,
        operand: &BoundRef,
    ) -> Value {
        match operand {
            BoundRef::Const(value) => value.clone(),
            BoundRef::External(slot) => self.environment_values[slot.index()].clone(),
            BoundRef::Node(node) => state
                .node_values
                .get(node.index())
                .unwrap_or_else(|| panic!("dataflow node {:?} was not evaluated", node))
                .clone(),
        }
    }
}

/// Owns one stream program and its canonical evaluator state.
///
/// The canonical arena is the semantic authority for every node the active execution plan does not
/// cover with a region; region state owns the rest until it is materialized back here.
#[derive(Clone)]
#[repr(C)]
pub(in crate::dataflow) struct Evaluator {
    pub(in crate::dataflow) program: Rc<StreamProgram>,
    pub(in crate::dataflow) canonical: Box<EvaluatorState>,
}

impl Evaluator {
    pub(in crate::dataflow) fn new(program: Rc<StreamProgram>) -> Self {
        Self::new_with_history(program, &[])
    }

    pub(in crate::dataflow) fn new_with_history(
        program: Rc<StreamProgram>,
        history_bindings: &[Option<HistoryId>],
    ) -> Self {
        program
            .graph
            .debug_assert_valid(program.environment_layout.len());
        let canonical = Box::new(EvaluatorState::new_with_history(
            &program.graph,
            history_bindings,
        ));
        debug_assert_eq!(canonical.node_values.len(), program.graph.nodes.len());
        debug_assert_eq!(canonical.node_states.len(), program.graph.nodes.len());
        Self { program, canonical }
    }

    #[inline]
    pub(in crate::dataflow) fn state_mut(&mut self) -> &mut EvaluatorState {
        self.canonical.as_mut()
    }
}
