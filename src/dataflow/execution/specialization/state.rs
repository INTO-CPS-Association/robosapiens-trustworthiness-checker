use super::super::super::ir::ScalarKind;
use super::super::stream_state::NodeState as CanonicalNodeState;
use super::plan::{Instruction, Plan};
use super::scalar::ScalarValue;

/// Persistent state for one instantiated specialization plan.
#[derive(Clone)]
pub(in crate::dataflow) struct State {
    pub(super) nodes: Vec<Node>,
}

#[derive(Clone)]
pub(super) struct Node {
    pub(super) value: Option<ScalarValue>,
    pub(super) state: NodeState,
}

#[derive(Clone)]
pub(super) enum NodeState {
    Canonical,
    If {
        then_state: Option<State>,
        else_state: Option<State>,
    },
    Unary {
        last_input: Option<ScalarValue>,
    },
    Binary {
        last_left: Option<ScalarValue>,
        last_right: Option<ScalarValue>,
    },
    Deoptimized {
        output_kind: ScalarKind,
    },
}

impl State {
    pub(in crate::dataflow) fn new(plan: &Plan) -> Self {
        Self {
            nodes: plan
                .instructions
                .iter()
                .map(|instruction| Node {
                    value: None,
                    state: match instruction {
                        Instruction::Canonical => NodeState::Canonical,
                        Instruction::If {
                            then_plan,
                            else_plan,
                        } => NodeState::If {
                            then_state: then_plan.as_ref().map(State::new),
                            else_state: else_plan.as_ref().map(State::new),
                        },
                        Instruction::Unary { .. } => NodeState::Unary { last_input: None },
                        Instruction::Binary { .. } => NodeState::Binary {
                            last_left: None,
                            last_right: None,
                        },
                    },
                })
                .collect(),
        }
    }

    pub(in crate::dataflow) fn reset(&mut self) {
        for node in &mut self.nodes {
            node.value = None;
            match &mut node.state {
                NodeState::Unary { last_input } => *last_input = None,
                NodeState::Binary {
                    last_left,
                    last_right,
                } => {
                    *last_left = None;
                    *last_right = None;
                }
                NodeState::If {
                    then_state,
                    else_state,
                } => {
                    if let Some(state) = then_state {
                        state.reset();
                    }
                    if let Some(state) = else_state {
                        state.reset();
                    }
                }
                NodeState::Canonical | NodeState::Deoptimized { .. } => {}
            }
        }
    }
}

impl NodeState {
    pub(super) fn restore_canonical_state(self, state: &mut CanonicalNodeState) {
        match (self, state) {
            (
                Self::Unary { last_input },
                CanonicalNodeState::UnaryLift {
                    last_input: canonical,
                },
            ) => *canonical = last_input.map(ScalarValue::into_value),
            (
                Self::Binary {
                    last_left,
                    last_right,
                },
                CanonicalNodeState::BinaryLift {
                    last_left: canonical_left,
                    last_right: canonical_right,
                },
            ) => {
                *canonical_left = last_left.map(ScalarValue::into_value);
                *canonical_right = last_right.map(ScalarValue::into_value);
            }
            _ => unreachable!("specialization state has incompatible canonical state"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_preserves_deoptimization_and_clears_transient_state() {
        let mut state = State {
            nodes: vec![
                Node {
                    value: Some(ScalarValue::Int(1)),
                    state: NodeState::Deoptimized {
                        output_kind: ScalarKind::Int,
                    },
                },
                Node {
                    value: Some(ScalarValue::Bool(true)),
                    state: NodeState::If {
                        then_state: Some(State {
                            nodes: vec![Node {
                                value: Some(ScalarValue::Bool(false)),
                                state: NodeState::Deoptimized {
                                    output_kind: ScalarKind::Bool,
                                },
                            }],
                        }),
                        else_state: Some(State {
                            nodes: vec![Node {
                                value: Some(ScalarValue::Int(2)),
                                state: NodeState::Unary {
                                    last_input: Some(ScalarValue::Int(3)),
                                },
                            }],
                        }),
                    },
                },
            ],
        };

        state.reset();

        assert!(state.nodes.iter().all(|node| node.value.is_none()));
        assert!(matches!(
            state.nodes[0].state,
            NodeState::Deoptimized {
                output_kind: ScalarKind::Int
            }
        ));
        let NodeState::If {
            then_state: Some(then_state),
            else_state: Some(else_state),
        } = &state.nodes[1].state
        else {
            panic!("reset changed the conditional specialization state");
        };
        assert!(matches!(
            then_state.nodes[0].state,
            NodeState::Deoptimized {
                output_kind: ScalarKind::Bool
            }
        ));
        assert!(then_state.nodes[0].value.is_none());
        assert!(matches!(
            else_state.nodes[0].state,
            NodeState::Unary { last_input: None }
        ));
        assert!(else_state.nodes[0].value.is_none());
    }
}
