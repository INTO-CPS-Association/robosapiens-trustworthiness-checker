use super::super::super::ir::*;
use super::super::super::*;
use super::super::interpreter::{evaluate_node, evaluate_nodes, stage_recursive_delays};
use super::super::lifting::retain_last_value;
use super::super::stream_evaluator::EvaluationContext;
use super::super::stream_state::{NodeState as CanonicalNodeState, StreamState};
use super::plan::{Instruction, Plan, SingleScalarPlan, Source};
use super::scalar::{ScalarValue, apply_binary, apply_unary, retain_last};
use super::state::{Node, NodeState, State};

/// Executes a specialization plan while preserving canonical graph state.
pub(in crate::dataflow) fn execute(
    state: &mut State,
    plan: &Plan,
    graph: &BoundEvaluationGraph,
    canonical: &mut StreamState,
    context: EvaluationContext<'_>,
    published_scalars: &[Option<ScalarValue>],
) {
    for (index, op) in graph.nodes.iter().enumerate() {
        let node = NodeId::new(index);
        let (previous, current) = state.nodes.split_at_mut(index);
        let current = &mut current[0];
        let instruction = &plan.instructions[index];
        let outcome = match (instruction, &mut current.state) {
            (Instruction::Canonical, NodeState::Canonical) => {
                let value = evaluate_node(node, op, canonical, context);
                canonical.node_values[index] = value;
                continue;
            }
            (
                Instruction::If {
                    then_plan,
                    else_plan,
                },
                NodeState::If {
                    then_state,
                    else_state,
                },
            ) => {
                let value = evaluate_if(
                    node,
                    op,
                    canonical,
                    context,
                    then_plan.as_ref(),
                    else_plan.as_ref(),
                    then_state.as_mut(),
                    else_state.as_mut(),
                    published_scalars,
                );
                canonical.node_values[index] = value;
                continue;
            }
            (
                Instruction::Unary {
                    op,
                    input,
                    input_kind,
                    ..
                },
                NodeState::Unary { last_input },
            ) => read_source(
                input,
                *input_kind,
                previous,
                canonical,
                context,
                published_scalars,
            )
            .map(|value| {
                let value = retain_last(value, last_input);
                if value.is_special() {
                    value
                } else {
                    apply_unary(*op, value)
                }
            })
            .map_or(Outcome::Deopt, Outcome::Value),
            (
                Instruction::Binary {
                    op,
                    left,
                    right,
                    left_kind,
                    right_kind,
                    ..
                },
                NodeState::Binary {
                    last_left,
                    last_right,
                },
            ) => match (
                read_source(
                    left,
                    *left_kind,
                    previous,
                    canonical,
                    context,
                    published_scalars,
                ),
                read_source(
                    right,
                    *right_kind,
                    previous,
                    canonical,
                    context,
                    published_scalars,
                ),
            ) {
                (Some(left), Some(right)) => {
                    let left = retain_last(left, last_left);
                    let right = retain_last(right, last_right);
                    Outcome::Value(
                        if left == ScalarValue::NoVal || right == ScalarValue::NoVal {
                            ScalarValue::NoVal
                        } else if left == ScalarValue::Deferred || right == ScalarValue::Deferred {
                            ScalarValue::Deferred
                        } else {
                            apply_binary(*op, left, right)
                        },
                    )
                }
                _ => Outcome::Deopt,
            },
            (_, NodeState::Deoptimized { output_kind }) => {
                let value = evaluate_node(node, op, canonical, context);
                current.value = ScalarValue::from_value(&value, *output_kind);
                canonical.node_values[index] = value;
                continue;
            }
            _ => unreachable!("specialization instruction has incompatible runtime state"),
        };

        match outcome {
            Outcome::Value(value) => {
                current.value = Some(value);
                canonical.node_values[index] = value.into_value();
            }
            Outcome::Deopt => {
                let output_kind = instruction.output_kind();
                let specialized =
                    std::mem::replace(&mut current.state, NodeState::Deoptimized { output_kind });
                specialized.restore_canonical_state(&mut canonical.node_states[index]);
                let value = evaluate_node(node, op, canonical, context);
                current.value = ScalarValue::from_value(&value, output_kind);
                canonical.node_values[index] = value;
            }
        }
    }
}

/// The one representation produced by direct scalar execution.
pub(in crate::dataflow) enum DirectResult {
    Scalar(ScalarValue),
    Canonical(Value),
}

/// Execute a whole-stream, one-node scalar plan without general stream selection.
///
/// This deliberately keeps the small unary/binary dispatch separate from the
/// general traversal above. Sharing that traversal makes the common one-node
/// stream pay for node selection and measurably regresses scalar chains.
#[inline]
pub(in crate::dataflow) fn execute_single(
    state: &mut State,
    plan: &SingleScalarPlan,
    graph: &BoundEvaluationGraph,
    canonical: &mut StreamState,
    context: EvaluationContext<'_>,
    published_scalars: &[Option<ScalarValue>],
) -> DirectResult {
    let instruction = &plan.instruction;
    let state = &mut state.nodes[0];
    let outcome = match (instruction, &mut state.state) {
        (
            Instruction::Unary {
                op,
                input,
                input_kind,
                ..
            },
            NodeState::Unary { last_input },
        ) => read_source(
            input,
            *input_kind,
            &[],
            canonical,
            context,
            published_scalars,
        )
        .map(|value| {
            let value = retain_last(value, last_input);
            if value.is_special() {
                value
            } else {
                apply_unary(*op, value)
            }
        })
        .map_or(Outcome::Deopt, Outcome::Value),
        (
            Instruction::Binary {
                op,
                left,
                right,
                left_kind,
                right_kind,
                ..
            },
            NodeState::Binary {
                last_left,
                last_right,
            },
        ) => match (
            read_source(left, *left_kind, &[], canonical, context, published_scalars),
            read_source(
                right,
                *right_kind,
                &[],
                canonical,
                context,
                published_scalars,
            ),
        ) {
            (Some(left), Some(right)) => {
                let left = retain_last(left, last_left);
                let right = retain_last(right, last_right);
                Outcome::Value(
                    if left == ScalarValue::NoVal || right == ScalarValue::NoVal {
                        ScalarValue::NoVal
                    } else if left == ScalarValue::Deferred || right == ScalarValue::Deferred {
                        ScalarValue::Deferred
                    } else {
                        apply_binary(*op, left, right)
                    },
                )
            }
            _ => Outcome::Deopt,
        },
        (_, NodeState::Deoptimized { output_kind }) => {
            let output_kind = *output_kind;
            return DirectResult::Canonical(evaluate_deoptimized_single(
                state,
                output_kind,
                graph,
                canonical,
                context,
            ));
        }
        _ => unreachable!("single scalar plan has incompatible runtime state"),
    };

    match outcome {
        Outcome::Value(value) => {
            state.value = Some(value);
            canonical.node_values[0] = value.into_value();
            DirectResult::Scalar(value)
        }
        Outcome::Deopt => {
            DirectResult::Canonical(deopt_single(state, instruction, graph, canonical, context))
        }
    }
}

#[cold]
#[inline(never)]
fn deopt_single(
    state: &mut Node,
    instruction: &Instruction,
    graph: &BoundEvaluationGraph,
    canonical: &mut StreamState,
    context: EvaluationContext<'_>,
) -> Value {
    let output_kind = instruction.output_kind();
    let specialized = std::mem::replace(&mut state.state, NodeState::Deoptimized { output_kind });
    specialized.restore_canonical_state(&mut canonical.node_states[0]);
    evaluate_deoptimized_single(state, output_kind, graph, canonical, context)
}

#[cold]
#[inline(never)]
fn evaluate_deoptimized_single(
    state: &mut Node,
    output_kind: ScalarKind,
    graph: &BoundEvaluationGraph,
    canonical: &mut StreamState,
    context: EvaluationContext<'_>,
) -> Value {
    let value = evaluate_node(NodeId::new(0), &graph.nodes[0], canonical, context);
    state.value = ScalarValue::from_value(&value, output_kind);
    canonical.node_values[0] = value.clone();
    value
}

enum Outcome {
    Value(ScalarValue),
    Deopt,
}

#[inline]
fn read_source(
    source: &Source,
    kind: ScalarKind,
    nodes: &[Node],
    state: &StreamState,
    context: EvaluationContext<'_>,
    published_scalars: &[Option<ScalarValue>],
) -> Option<ScalarValue> {
    match source {
        Source::Constant(value) => Some(*value),
        Source::ScalarNode(node) => nodes[node.index()].value,
        Source::Published(stream) => published_scalars
            .get(*stream)
            .copied()
            .flatten()
            .filter(|value| value.has_kind(kind)),
        Source::Canonical(BoundRef::Const(value)) => ScalarValue::from_value(value, kind),
        Source::Canonical(BoundRef::External(slot)) => {
            ScalarValue::from_value(&context.environment_values[slot.index()], kind)
        }
        Source::Canonical(BoundRef::Node(node)) => ScalarValue::from_value(
            state
                .node_values
                .get(node.index())
                .unwrap_or_else(|| panic!("dataflow node {:?} was not evaluated", node)),
            kind,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn evaluate_if(
    node: NodeId,
    op: &BoundOp,
    canonical: &mut StreamState,
    context: EvaluationContext<'_>,
    then_plan: Option<&Plan>,
    else_plan: Option<&Plan>,
    then_state: Option<&mut State>,
    else_state: Option<&mut State>,
    published_scalars: &[Option<ScalarValue>],
) -> Value {
    let BoundOp::If {
        cond,
        then_branch,
        else_branch,
    } = op
    else {
        unreachable!("specialized if instruction referenced a non-if node")
    };
    let condition = context.read_value(canonical, cond);
    let CanonicalNodeState::LazyIf(lazy) = &mut canonical.node_states[node.index()] else {
        unreachable!("if node has incompatible runtime state")
    };
    let condition = retain_last_value(condition, &mut lazy.last_condition);

    if context.recursive_call.is_some() {
        return match condition {
            Value::Bool(true) => evaluate_branch(
                then_branch,
                &mut lazy.then_state,
                then_plan,
                then_state,
                context,
                published_scalars,
            ),
            Value::Bool(false) => evaluate_branch(
                else_branch,
                &mut lazy.else_state,
                else_plan,
                else_state,
                context,
                published_scalars,
            ),
            Value::Deferred => Value::Deferred,
            Value::NoVal => Value::NoVal,
            other => panic!("if condition must be bool, got {other:?}"),
        };
    }

    let then_value = evaluate_branch(
        then_branch,
        &mut lazy.then_state,
        then_plan,
        then_state,
        context,
        published_scalars,
    );
    let then_value = retain_last_value(then_value, &mut lazy.last_then_value);
    let else_value = evaluate_branch(
        else_branch,
        &mut lazy.else_state,
        else_plan,
        else_state,
        context,
        published_scalars,
    );
    let else_value = retain_last_value(else_value, &mut lazy.last_else_value);

    if then_value == Value::NoVal || else_value == Value::NoVal {
        return Value::NoVal;
    }
    match condition {
        Value::Bool(true) => then_value,
        Value::Bool(false) => else_value,
        Value::Deferred => Value::Deferred,
        Value::NoVal => Value::NoVal,
        other => panic!("if condition must be bool, got {other:?}"),
    }
}

fn evaluate_branch(
    graph: &BoundEvaluationGraph,
    canonical: &mut StreamState,
    plan: Option<&Plan>,
    state: Option<&mut State>,
    context: EvaluationContext<'_>,
    published_scalars: &[Option<ScalarValue>],
) -> Value {
    match (plan, state) {
        (Some(plan), Some(state)) => {
            execute(state, plan, graph, canonical, context, published_scalars)
        }
        (None, None) => evaluate_nodes(&graph.nodes, canonical, context),
        _ => unreachable!("branch specialization plan and state must be present together"),
    }
    let output = context.read_value(canonical, &graph.output);
    stage_recursive_delays(&graph.recursive_delays, canonical, &output);
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{BinaryOperator, UnaryOperator};
    use std::rc::Rc;

    fn evaluate(
        graph: &BoundEvaluationGraph,
        plan: &Plan,
        specialization: &mut State,
        canonical: &mut StreamState,
        environment_values: &[Value],
        environment_layout: &Rc<EnvironmentLayout>,
    ) -> Value {
        execute(
            specialization,
            plan,
            graph,
            canonical,
            EvaluationContext {
                environment_values,
                environment_layout,
                retained_environment_values: None,
                recursive_call: None,
            },
            &[],
        );
        canonical.node_values.last().unwrap().clone()
    }

    #[test]
    fn canonical_collection_nodes_can_feed_a_specialized_scalar_node() {
        let graph = BoundEvaluationGraph::new(
            vec![
                BoundOp::List(vec![BoundRef::Const(Value::Int(1))]),
                BoundOp::LLen {
                    list: BoundRef::Node(NodeId::new(0)),
                },
                BoundOp::Binary {
                    op: BinaryOperator::Add,
                    lhs: BoundRef::Node(NodeId::new(1)),
                    rhs: BoundRef::Const(Value::Int(4)),
                },
            ],
            vec![
                None,
                None,
                Some(ScalarSignature::Binary {
                    left: ScalarKind::Int,
                    right: ScalarKind::Int,
                    output: ScalarKind::Int,
                }),
            ],
            BoundRef::Node(NodeId::new(2)),
        );
        let plan = Plan::new(&graph).unwrap();
        let mut specialization = State::new(&plan);
        let mut canonical = StreamState::new(&graph);
        let layout = Rc::new(EnvironmentLayout::default());

        assert_eq!(
            evaluate(
                &graph,
                &plan,
                &mut specialization,
                &mut canonical,
                &[],
                &layout,
            ),
            Value::Int(5)
        );
        assert!(matches!(
            specialization.nodes[2].state,
            NodeState::Binary { .. }
        ));
    }

    #[test]
    fn a_runtime_type_mismatch_deoptimizes_only_the_affected_node() {
        let graph = BoundEvaluationGraph::new(
            vec![
                BoundOp::Binary {
                    op: BinaryOperator::Equal,
                    lhs: BoundRef::External(EnvironmentSlot::new(0)),
                    rhs: BoundRef::Const(Value::Int(1)),
                },
                BoundOp::Unary {
                    op: UnaryOperator::Not,
                    arg: BoundRef::Node(NodeId::new(0)),
                },
            ],
            vec![
                Some(ScalarSignature::Binary {
                    left: ScalarKind::Int,
                    right: ScalarKind::Int,
                    output: ScalarKind::Bool,
                }),
                Some(ScalarSignature::Unary {
                    input: ScalarKind::Bool,
                    output: ScalarKind::Bool,
                }),
            ],
            BoundRef::Node(NodeId::new(1)),
        );
        let plan = Plan::new(&graph).unwrap();
        let mut specialization = State::new(&plan);
        let mut canonical = StreamState::new(&graph);
        let layout = Rc::new(EnvironmentLayout::from_variables([VarName::new("x")]));

        assert_eq!(
            evaluate(
                &graph,
                &plan,
                &mut specialization,
                &mut canonical,
                &[Value::Bool(true)],
                &layout,
            ),
            Value::Bool(true)
        );
        assert!(matches!(
            specialization.nodes[0].state,
            NodeState::Deoptimized { .. }
        ));
        assert!(matches!(
            specialization.nodes[1].state,
            NodeState::Unary { .. }
        ));
    }
}
