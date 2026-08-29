use super::plan::{Instruction, Plan, Source};
use super::scalar::{supports_binary, supports_unary};
use crate::Value;
use crate::dataflow::execution::evaluator::EvaluationEnvironment;
use crate::dataflow::execution::evaluator_state::EvaluatorState;
use crate::dataflow::ir::{BoundEvaluationGraph, BoundOp, BoundRef, NodeId, ScalarKind};

pub(in crate::dataflow) fn is_adaptive_candidate(graph: &BoundEvaluationGraph) -> bool {
    graph.nodes.len() == 1
        && graph.output == BoundRef::Node(NodeId::new(0))
        && matches!(
            graph.nodes[0],
            BoundOp::Unary { .. } | BoundOp::Binary { .. }
        )
}

#[cold]
#[inline(never)]
pub(in crate::dataflow) fn plan_from_observed_single(
    graph: &BoundEvaluationGraph,
    state: &EvaluatorState,
    context: EvaluationEnvironment<'_>,
) -> Option<Plan> {
    if !is_adaptive_candidate(graph) {
        return None;
    }

    let output_kind = scalar_kind(&state.node_values[0])?;
    let mut no_published_source = |_| None;
    let instruction = match &graph.nodes[0] {
        BoundOp::Unary { op, arg } => {
            let input_kind = scalar_kind(&context.read_value(state, arg))?;
            supports_unary(*op, input_kind, output_kind).then(|| Instruction::Unary {
                op: *op,
                input: Source::new(arg, input_kind, &[], &mut no_published_source),
                input_kind,
                output_kind,
            })?
        }
        BoundOp::Binary { op, lhs, rhs } => {
            let left_kind = scalar_kind(&context.read_value(state, lhs))?;
            let right_kind = scalar_kind(&context.read_value(state, rhs))?;
            supports_binary(*op, left_kind, right_kind, output_kind).then(|| {
                Instruction::Binary {
                    op: *op,
                    left: Source::new(lhs, left_kind, &[], &mut no_published_source),
                    right: Source::new(rhs, right_kind, &[], &mut no_published_source),
                    left_kind,
                    right_kind,
                    output_kind,
                }
            })?
        }
        _ => return None,
    };
    Some(Plan {
        instructions: vec![instruction],
    })
}

fn scalar_kind(value: &Value) -> Option<ScalarKind> {
    match value {
        Value::Int(_) => Some(ScalarKind::Int),
        Value::Float(_) => Some(ScalarKind::Float),
        Value::Bool(_) => Some(ScalarKind::Bool),
        _ => None,
    }
}
