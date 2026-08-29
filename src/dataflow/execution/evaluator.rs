mod canonical;
mod expressions;
mod reconfiguration;
#[cfg(test)]
mod tests;
mod tier_states;
mod tiered;

use super::super::environment::EnvironmentLayout;
use super::super::history::HistoryId;
use super::super::ir::{BoundRef, StreamProgram};
use super::evaluator_state::EvaluatorState;
use crate::core::Value;
use ecow::EcoVec;
use std::rc::Rc;
use tier_states::EvaluatorTierStates;

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

/// Owns one stream program and its persistent evaluator tier states.
#[derive(Clone)]
#[repr(C)]
pub(in crate::dataflow) struct Evaluator {
    pub(in crate::dataflow) program: Rc<StreamProgram>,
    pub(in crate::dataflow) tier_states: EvaluatorTierStates,
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
        let tier_states = EvaluatorTierStates::new(&program, history_bindings);
        Self {
            program,
            tier_states,
        }
    }

    #[cfg_attr(not(feature = "jit"), allow(dead_code))]
    #[inline]
    pub(in crate::dataflow) fn state_mut(&mut self) -> &mut EvaluatorState {
        self.tier_states.canonical.as_mut()
    }
}
