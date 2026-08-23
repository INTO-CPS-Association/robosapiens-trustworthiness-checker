//! Scheduled scalar temporal operations over evaluator-owned canonical state.
//!
//! Evaluation steps read the previous temporal state and materialize typed scalar boundary values
//! for a native region. Commit steps remain explicit in the scheduler's semantic plan and run only
//! at its logical end-of-tick barrier. This interpreter is the fallback physical view of those
//! steps; a schedule-wide native artifact may use a packed state layout mapped to the same stable
//! plan slots.

use crate::dataflow::execution::quickening::ScalarValue;
use crate::dataflow::execution::scheduled_plan::{TemporalCommit, TemporalOperation, TemporalPlan};
use crate::dataflow::execution::stream_evaluator::EvaluationContext;
use crate::dataflow::execution::stream_state::{NodeState, StreamState};
use crate::dataflow::ir::{BoundRef, NodeId};

#[derive(Clone)]
pub(super) struct ScheduledTemporalPlan {
    plan: TemporalPlan,
}

impl ScheduledTemporalPlan {
    pub(super) fn build(plan: &TemporalPlan, boundary_nodes: &[NodeId]) -> Option<Self> {
        if boundary_nodes.is_empty() {
            return plan.operations.is_empty().then(Self::empty);
        }
        boundary_nodes
            .iter()
            .copied()
            .eq(plan.nodes())
            .then(|| Self { plan: plan.clone() })
    }

    fn empty() -> Self {
        Self {
            plan: TemporalPlan::default(),
        }
    }

    pub(super) fn promote(&self, state: &mut StreamState) -> bool {
        for step in self.plan.operations.iter() {
            let node = step.node();
            let replacement = match &state.node_states[node.index()] {
                NodeState::Delay(history) => match history.to_scalar() {
                    Some(history) => Some(NodeState::ScalarDelay(history)),
                    None => {
                        self.deopt(state);
                        return false;
                    }
                },
                NodeState::Default { last_input } => {
                    let last_input = match last_input {
                        Some(value) => match ScalarValue::from_untyped_value(value) {
                            Some(value) => Some(value),
                            None => {
                                self.deopt(state);
                                return false;
                            }
                        },
                        None => None,
                    };
                    Some(NodeState::ScalarDefault { last_input })
                }
                NodeState::ScalarDelay(_) | NodeState::ScalarDefault { .. } => None,
                _ => {
                    self.deopt(state);
                    return false;
                }
            };
            if let Some(replacement) = replacement {
                state.node_states[node.index()] = replacement;
            }
        }
        true
    }

    #[inline]
    pub(super) fn evaluate(
        &self,
        state: &mut StreamState,
        context: EvaluationContext<'_>,
        scalar_values: &mut [Option<ScalarValue>],
    ) -> bool {
        for step in self.plan.operations.iter() {
            let (node, value) = match step {
                TemporalOperation::Delay {
                    state: slot,
                    input,
                    offset,
                } => {
                    let node = slot.node;
                    let value = if *offset == 0 {
                        let Some(input) = read_scalar(input, state, context, scalar_values) else {
                            return false;
                        };
                        let NodeState::ScalarDelay(history) = &mut state.node_states[node.index()]
                        else {
                            unreachable!("scheduled delay has incompatible state")
                        };
                        history.retain_current_value(input)
                    } else {
                        let NodeState::ScalarDelay(history) = &mut state.node_states[node.index()]
                        else {
                            unreachable!("scheduled delay has incompatible state")
                        };
                        history.read_and_stage_write()
                    };
                    (node, value)
                }
                TemporalOperation::RecursiveDelay { state: slot, .. } => {
                    let node = slot.node;
                    let NodeState::ScalarDelay(history) = &mut state.node_states[node.index()]
                    else {
                        unreachable!("scheduled recursive delay has incompatible state")
                    };
                    (node, history.read_delayed_value())
                }
                TemporalOperation::Default {
                    state: slot,
                    input,
                    fallback,
                } => {
                    let node = slot.node;
                    let Some(input) = read_scalar(input, state, context, scalar_values) else {
                        return false;
                    };
                    let NodeState::ScalarDefault { last_input } =
                        &mut state.node_states[node.index()]
                    else {
                        unreachable!("scheduled default has incompatible state")
                    };
                    let input = match input {
                        ScalarValue::NoVal => last_input.unwrap_or(ScalarValue::NoVal),
                        input => {
                            *last_input = Some(input);
                            input
                        }
                    };
                    let value = if input == ScalarValue::Deferred {
                        let Some(fallback) = read_scalar(fallback, state, context, scalar_values)
                        else {
                            return false;
                        };
                        fallback
                    } else {
                        input
                    };
                    (node, value)
                }
            };
            scalar_values[node.index()] = Some(value);
        }
        true
    }

    #[inline]
    pub(super) fn commit(
        &self,
        state: &mut StreamState,
        context: EvaluationContext<'_>,
        scalar_values: &[Option<ScalarValue>],
    ) -> bool {
        for step in self.plan.commits.iter() {
            match step {
                TemporalCommit::Delay { state: slot, input } => {
                    let node = slot.node;
                    let Some(value) = read_scalar(input, state, context, scalar_values) else {
                        self.deopt(state);
                        return false;
                    };
                    let NodeState::ScalarDelay(history) = &mut state.node_states[node.index()]
                    else {
                        unreachable!("scheduled delay commit has incompatible state")
                    };
                    history.commit_staged_write(value);
                }
                TemporalCommit::RecursiveDelay { state: slot } => {
                    let node = slot.node;
                    let NodeState::ScalarDelay(history) = &mut state.node_states[node.index()]
                    else {
                        unreachable!("scheduled recursive commit has incompatible state")
                    };
                    history.commit_recursive_value();
                }
            }
        }
        true
    }

    pub(super) fn has_temporal_state(&self) -> bool {
        !self.plan.commits.is_empty()
    }

    pub(super) fn has_scheduled_state(&self) -> bool {
        !self.plan.operations.is_empty()
    }

    pub(super) fn materialize(
        &self,
        state: &mut StreamState,
        scalar_values: &[Option<ScalarValue>],
    ) {
        for step in self.plan.operations.iter() {
            let node = step.node();
            if let Some(value) = scalar_values[node.index()] {
                state.node_values[node.index()] = value.into_value();
            }
        }
    }

    pub(super) fn deopt(&self, state: &mut StreamState) {
        for step in self.plan.operations.iter() {
            let node = step.node();
            let replacement = match &state.node_states[node.index()] {
                NodeState::ScalarDelay(history) => Some(NodeState::Delay(history.to_canonical())),
                NodeState::ScalarDefault { last_input } => Some(NodeState::Default {
                    last_input: last_input.map(ScalarValue::into_value),
                }),
                _ => None,
            };
            if let Some(replacement) = replacement {
                state.node_states[node.index()] = replacement;
            }
        }
    }
}

#[inline]
fn read_scalar(
    reference: &BoundRef,
    state: &StreamState,
    context: EvaluationContext<'_>,
    scalar_values: &[Option<ScalarValue>],
) -> Option<ScalarValue> {
    if let BoundRef::Node(node) = reference
        && let Some(value) = scalar_values[node.index()]
    {
        return Some(value);
    }
    match reference {
        BoundRef::Const(value) => ScalarValue::from_untyped_value(value),
        BoundRef::External(slot) => {
            ScalarValue::from_untyped_value(&context.environment_values[slot.index()])
        }
        BoundRef::Node(node) => ScalarValue::from_untyped_value(&state.node_values[node.index()]),
    }
}
