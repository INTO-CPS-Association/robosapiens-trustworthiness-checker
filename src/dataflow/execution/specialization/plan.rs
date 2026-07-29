use super::super::super::ir::*;
use super::super::super::*;
use super::scalar::{ScalarValue, supports_binary, supports_unary};
use crate::core::{BinaryOperator, UnaryOperator};

/// Immutable instruction selection over a canonical evaluation graph.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::dataflow) struct Plan {
    pub(super) instructions: Vec<Instruction>,
}

/// A schedule-specific plan for a whole-stream unary or binary program.
///
/// The private instruction field makes the direct executor's eligibility
/// invariant structural rather than a per-call assertion over a general plan.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::dataflow) struct SingleScalarPlan {
    pub(super) instruction: Instruction,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum Instruction {
    Canonical,
    If {
        then_plan: Option<Plan>,
        else_plan: Option<Plan>,
    },
    Unary {
        op: UnaryOperator,
        input: Source,
        input_kind: ScalarKind,
        output_kind: ScalarKind,
    },
    Binary {
        op: BinaryOperator,
        left: Source,
        right: Source,
        left_kind: ScalarKind,
        right_kind: ScalarKind,
        output_kind: ScalarKind,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum Source {
    Constant(ScalarValue),
    ScalarNode(NodeId),
    Published(usize),
    Canonical(BoundRef),
}

impl Plan {
    pub(in crate::dataflow) fn new(graph: &BoundEvaluationGraph) -> Option<Self> {
        Self::with_published_sources(graph, |_| None)
    }

    pub(in crate::dataflow) fn with_published_sources(
        graph: &BoundEvaluationGraph,
        mut published_source: impl FnMut(EnvironmentSlot) -> Option<usize>,
    ) -> Option<Self> {
        Self::build(graph, &mut published_source)
    }

    pub(in crate::dataflow) fn try_into_single_scalar(
        mut self,
        graph: &BoundEvaluationGraph,
    ) -> Result<SingleScalarPlan, Self> {
        let eligible = graph.nodes.len() == 1
            && graph.output == BoundRef::Node(NodeId::new(0))
            && matches!(
                self.instructions.as_slice(),
                [Instruction::Unary { .. } | Instruction::Binary { .. }]
            );
        if !eligible {
            return Err(self);
        }
        Ok(SingleScalarPlan {
            instruction: self.instructions.pop().unwrap(),
        })
    }

    fn build(
        graph: &BoundEvaluationGraph,
        published_source: &mut impl FnMut(EnvironmentSlot) -> Option<usize>,
    ) -> Option<Self> {
        let mut instructions = Vec::with_capacity(graph.nodes.len());

        for (op, signature) in graph.nodes.iter().zip(&graph.scalar_signatures) {
            let instruction = match (op, signature) {
                (BoundOp::Unary { op, arg }, Some(ScalarSignature::Unary { input, output }))
                    if supports_unary(*op, *input, *output) =>
                {
                    Instruction::Unary {
                        op: *op,
                        input: Source::new(arg, *input, &instructions, published_source),
                        input_kind: *input,
                        output_kind: *output,
                    }
                }
                (
                    BoundOp::Binary { op, lhs, rhs },
                    Some(ScalarSignature::Binary {
                        left,
                        right,
                        output,
                    }),
                ) if supports_binary(*op, *left, *right, *output) => Instruction::Binary {
                    op: *op,
                    left: Source::new(lhs, *left, &instructions, published_source),
                    right: Source::new(rhs, *right, &instructions, published_source),
                    left_kind: *left,
                    right_kind: *right,
                    output_kind: *output,
                },
                (
                    BoundOp::If {
                        then_branch,
                        else_branch,
                        ..
                    },
                    _,
                ) if !has_recursive_call(then_branch) && !has_recursive_call(else_branch) => {
                    let then_plan = Self::build(then_branch, published_source);
                    let else_plan = Self::build(else_branch, published_source);
                    if then_plan.is_some() || else_plan.is_some() {
                        Instruction::If {
                            then_plan,
                            else_plan,
                        }
                    } else {
                        Instruction::Canonical
                    }
                }
                _ => Instruction::Canonical,
            };
            instructions.push(instruction);
        }

        instructions
            .iter()
            .any(|instruction| !matches!(instruction, Instruction::Canonical))
            .then_some(Self { instructions })
    }

    #[cfg(test)]
    pub(in crate::dataflow) fn published_source_count(&self) -> usize {
        self.instructions
            .iter()
            .map(Instruction::published_source_count)
            .sum()
    }
}

impl Instruction {
    #[inline]
    pub(super) fn output_kind(&self) -> ScalarKind {
        match self {
            Self::Unary { output_kind, .. } | Self::Binary { output_kind, .. } => *output_kind,
            Self::Canonical | Self::If { .. } => {
                unreachable!("only scalar instructions can deopt")
            }
        }
    }

    #[inline]
    fn has_scalar_output(&self) -> bool {
        matches!(self, Self::Unary { .. } | Self::Binary { .. })
    }

    #[cfg(test)]
    fn published_source_count(&self) -> usize {
        match self {
            Self::Canonical => 0,
            Self::If {
                then_plan,
                else_plan,
            } => then_plan
                .iter()
                .chain(else_plan)
                .map(Plan::published_source_count)
                .sum(),
            Self::Unary { input, .. } => usize::from(matches!(input, Source::Published(_))),
            Self::Binary { left, right, .. } => {
                usize::from(matches!(left, Source::Published(_)))
                    + usize::from(matches!(right, Source::Published(_)))
            }
        }
    }
}

impl SingleScalarPlan {
    #[cfg(test)]
    pub(in crate::dataflow) fn published_source_count(&self) -> usize {
        self.instruction.published_source_count()
    }
}

impl Source {
    fn new(
        reference: &BoundRef,
        kind: ScalarKind,
        instructions: &[Instruction],
        published_source: &mut impl FnMut(EnvironmentSlot) -> Option<usize>,
    ) -> Self {
        match reference {
            BoundRef::Const(value) => ScalarValue::from_value(value, kind)
                .map(Self::Constant)
                .unwrap_or_else(|| Self::Canonical(reference.clone())),
            BoundRef::Node(node) if instructions[node.index()].has_scalar_output() => {
                Self::ScalarNode(*node)
            }
            BoundRef::External(slot) => published_source(*slot)
                .map(Self::Published)
                .unwrap_or_else(|| Self::Canonical(reference.clone())),
            _ => Self::Canonical(reference.clone()),
        }
    }
}

fn has_recursive_call(graph: &BoundEvaluationGraph) -> bool {
    graph.nodes.iter().any(|op| match op {
        BoundOp::RecursiveCall { .. } => true,
        BoundOp::If {
            then_branch,
            else_branch,
            ..
        } => has_recursive_call(then_branch) || has_recursive_call(else_branch),
        _ => false,
    })
}
