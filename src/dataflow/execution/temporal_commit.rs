//! The tick barrier: staging temporal writes, then committing them.
//!
//! A tick must not let a value it computes become history to itself. Evaluation therefore only
//! *stages* temporal writes, and this module makes them historical afterwards, once every stream in
//! the row has produced its value. That is what lets mutually delayed streams read each other's
//! completed values without a same-tick scheduling cycle.
//!
//! [`stage_recursive_delays`] runs during the forward pass; [`commit_staged_temporal_state_with_history`]
//! runs once after the row. [`discard_staged_temporal_state`] is the failure path — a tick that
//! fails commits nothing, so staged writes are dropped rather than applied.

use super::super::history::HistoryAccess;
use super::super::ir::*;
use super::super::*;
use super::evaluator::*;
use super::evaluator_state::*;
use super::node_evaluation::function_history_bindings;
use super::quickening::ScalarValue;

pub(in crate::dataflow) fn stage_recursive_delays(
    delays: &[NodeId],
    state: &mut EvaluatorState,
    output: &Value,
) {
    for delay in delays {
        match &mut state.node_states[delay.index()] {
            NodeState::Delay(history) => history.stage_recursive_value(output.clone()),
            NodeState::ScalarDelay(history) => {
                let output = ScalarValue::from_untyped_value(output)
                    .expect("scheduled scalar delay received a non-scalar output");
                history.stage_recursive_value(output);
            }
            _ => unreachable!("recursive delay node has incompatible runtime state"),
        }
    }
}

pub(in crate::dataflow) fn commit_staged_temporal_state_with_history(
    body: &BoundEvaluationGraph,
    state: &mut EvaluatorState,
    context: EvaluationEnvironment<'_>,
    history_access: Option<HistoryAccess<'_>>,
) {
    for (index, op) in body.nodes.iter().enumerate() {
        match op {
            StreamOp::Delay { input, offset } if *offset > 0 => {
                let current = context.read_value(state, input);
                match &mut state.node_states[index] {
                    NodeState::Delay(history) => history.commit_staged_write(current),
                    NodeState::ScalarDelay(history) => {
                        let current = ScalarValue::from_untyped_value(&current)
                            .unwrap_or_else(|| panic!("scalar delay commit received {current:?}"));
                        history.commit_staged_write(current);
                    }
                    _ => unreachable!("delay node has incompatible runtime state"),
                }
            }
            StreamOp::RecursiveDelay { .. } => match &mut state.node_states[index] {
                NodeState::Delay(history) => history.commit_recursive_value(),
                NodeState::ScalarDelay(history) => history.commit_recursive_value(),
                _ => unreachable!("recursive delay node has incompatible runtime state"),
            },
            StreamOp::If {
                then_branch,
                else_branch,
                ..
            } => {
                let NodeState::LazyIf(lazy_if) = &mut state.node_states[index] else {
                    unreachable!("if node has incompatible runtime state")
                };
                commit_staged_temporal_state_with_history(
                    then_branch,
                    lazy_if.then_state.as_mut(),
                    context,
                    history_access,
                );
                commit_staged_temporal_state_with_history(
                    else_branch,
                    lazy_if.else_state.as_mut(),
                    context,
                    history_access,
                );
            }
            StreamOp::DirectApply {
                func,
                args: argument_refs,
            } => {
                let NodeState::PersistentCall {
                    evaluator,
                    environment_values,
                    ..
                } = &mut state.node_states[index]
                else {
                    unreachable!("direct application node has incompatible runtime state")
                };
                let capture_count = func.capture_slots.len();
                for (slot, source) in environment_values[..capture_count]
                    .iter_mut()
                    .zip(&func.capture_slots)
                {
                    *slot = context.environment_values[source.index()].clone();
                }
                if let Some(history_access) = history_access {
                    let history_bindings =
                        function_history_bindings(history_access, func, argument_refs);
                    evaluator.commit_temporal_state_with_history(
                        environment_values,
                        None,
                        Some(history_access.with_bindings(&history_bindings)),
                    );
                } else {
                    evaluator.commit_temporal_state(environment_values);
                }
            }
            StreamOp::Reconfigurable(_) => {
                let NodeState::Reconfigurable(expression) = &mut state.node_states[index] else {
                    unreachable!("reconfigurable node has incompatible runtime state")
                };
                if expression
                    .active_expression
                    .as_ref()
                    .is_some_and(|active| active.evaluator.program.requires_temporal_commit())
                {
                    expression.update_environment(
                        context.environment_values,
                        context.retained_environment_values,
                    );
                    let active = expression
                        .active_expression
                        .as_mut()
                        .expect("active reconfigurable expression disappeared before commit");
                    active.evaluator.commit_temporal_state_with_history(
                        &expression.environment_values,
                        None,
                        None,
                    );
                }
            }
            _ => {}
        }
    }
}

pub(in crate::dataflow) fn discard_staged_temporal_state(
    body: &BoundEvaluationGraph,
    state: &mut EvaluatorState,
) {
    for (index, op) in body.nodes.iter().enumerate() {
        match op {
            StreamOp::Delay { offset, .. } if *offset > 0 => match &mut state.node_states[index] {
                NodeState::Delay(history) => history.discard_staged_write(),
                NodeState::ScalarDelay(history) => history.discard_staged_write(),
                _ => unreachable!("delay node has incompatible runtime state"),
            },
            StreamOp::RecursiveDelay { .. } => match &mut state.node_states[index] {
                NodeState::Delay(history) => history.discard_recursive_value(),
                NodeState::ScalarDelay(history) => history.discard_recursive_value(),
                _ => unreachable!("recursive delay node has incompatible runtime state"),
            },
            StreamOp::If {
                then_branch,
                else_branch,
                ..
            } => {
                let NodeState::LazyIf(lazy_if) = &mut state.node_states[index] else {
                    unreachable!("if node has incompatible runtime state")
                };
                discard_staged_temporal_state(then_branch, lazy_if.then_state.as_mut());
                discard_staged_temporal_state(else_branch, lazy_if.else_state.as_mut());
            }
            StreamOp::DirectApply { .. } => {
                let NodeState::PersistentCall { evaluator, .. } = &mut state.node_states[index]
                else {
                    unreachable!("direct application node has incompatible runtime state")
                };
                evaluator.discard_staged_temporal_state();
            }
            StreamOp::Reconfigurable(_) => {
                let NodeState::Reconfigurable(expression) = &mut state.node_states[index] else {
                    unreachable!("reconfigurable node has incompatible runtime state")
                };
                if let Some(active) = expression.active_expression.as_mut() {
                    active.evaluator.discard_staged_temporal_state();
                }
            }
            _ => {}
        }
    }
}
