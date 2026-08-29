//! A deliberately small quickening overlay for checked scalar operations.
//!
//! The canonical evaluation graph, `EvaluatorState`, and `Value` slots remain the
//! semantic authority. A [`Plan`] selects scalar instructions where possible
//! and canonical instructions everywhere else; [`State`] retains only the
//! corresponding scalar lift and deoptimization state.
//!
//! The important invariants are:
//!
//! - canonical instructions call the history-aware canonical evaluator;
//! - quickened results are always mirrored into canonical `node_values`;
//! - canonical results are converted only at a scalar consumer boundary;
//! - scalar node-to-node edges are used only when the producer is scalar;
//! - `NoVal` and `Deferred` retain exactly the canonical lifting semantics;
//! - a runtime type mismatch deoptimizes one instruction, not its graph;
//! - deoptimization transfers retained lift state into canonical `NodeState`;
//! - evaluator-owned plans move with their quickening state and may be shared by evaluator clones;
//! - fallible dynamic graphs continue to use the canonical traversal.
//!
//! Lazy `if` branches recursively carry quickening plans because they are
//! separate evaluation graphs. Branches containing recursive self-calls remain
//! canonical: recursive frames are short-lived, and allocating quickening
//! state for each frame costs more than the small scalar body saves.
//!
//! Temporal storage, collections, maps, functions, and dynamic expressions
//! intentionally remain canonical. Extending the quickened set should
//! require benchmark evidence strong enough to justify duplicating the
//! relevant state transition rather than merely proving that it can be done.

use super::super::super::ir::ScalarKind;
use super::super::evaluator_state::{EvaluatorState, NodeState as CanonicalNodeState};
use super::plan::{Instruction, Plan};
use super::scalar::ScalarValue;

/// Persistent state for one instantiated quickening plan.
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
            nodes: plan.instructions.iter().map(Node::new).collect(),
        }
    }

    #[cold]
    #[inline(never)]
    pub(in crate::dataflow) fn materialize_into(self, canonical: &mut EvaluatorState) {
        debug_assert_eq!(self.nodes.len(), canonical.node_states.len());
        for (node, canonical) in self.nodes.into_iter().zip(&mut canonical.node_states) {
            match node.state {
                NodeState::Unary { .. } | NodeState::Binary { .. } => {
                    node.state.restore_canonical_state(canonical);
                }
                NodeState::If {
                    then_state,
                    else_state,
                } => {
                    let CanonicalNodeState::LazyIf(branches) = canonical else {
                        unreachable!("quickening state has incompatible canonical state")
                    };
                    if let Some(state) = then_state {
                        state.materialize_into(&mut branches.then_state);
                    }
                    if let Some(state) = else_state {
                        state.materialize_into(&mut branches.else_state);
                    }
                }
                NodeState::Canonical | NodeState::Deoptimized { .. } => {}
            }
        }
    }

    #[cold]
    #[inline(never)]
    pub(in crate::dataflow) fn synchronize_from(&mut self, canonical: &EvaluatorState) {
        debug_assert_eq!(self.nodes.len(), canonical.node_states.len());
        for (node, canonical) in self.nodes.iter_mut().zip(&canonical.node_states) {
            node.value = None;
            match (&mut node.state, canonical) {
                (
                    NodeState::Unary { last_input },
                    CanonicalNodeState::UnaryLift {
                        last_input: canonical,
                    },
                ) => {
                    *last_input = canonical.as_ref().and_then(ScalarValue::from_untyped_value);
                }
                (
                    NodeState::Binary {
                        last_left,
                        last_right,
                    },
                    CanonicalNodeState::BinaryLift {
                        last_left: canonical_left,
                        last_right: canonical_right,
                    },
                ) => {
                    *last_left = canonical_left
                        .as_ref()
                        .and_then(ScalarValue::from_untyped_value);
                    *last_right = canonical_right
                        .as_ref()
                        .and_then(ScalarValue::from_untyped_value);
                }
                (
                    NodeState::If {
                        then_state,
                        else_state,
                    },
                    CanonicalNodeState::LazyIf(branches),
                ) => {
                    if let Some(state) = then_state {
                        state.synchronize_from(&branches.then_state);
                    }
                    if let Some(state) = else_state {
                        state.synchronize_from(&branches.else_state);
                    }
                }
                (NodeState::Canonical | NodeState::Deoptimized { .. }, _) => {}
                _ => unreachable!("quickening state has incompatible canonical state"),
            }
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

impl Node {
    fn new(instruction: &Instruction) -> Self {
        let state = match instruction {
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
        };
        Self { value: None, state }
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
            _ => unreachable!("quickening state has incompatible canonical state"),
        }
    }
}

#[cfg(test)]
pub(in crate::dataflow) fn node_value(state: &State, index: usize) -> Option<ScalarValue> {
    state.nodes.get(index).and_then(|node| node.value)
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
            panic!("reset changed the conditional quickening state");
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
