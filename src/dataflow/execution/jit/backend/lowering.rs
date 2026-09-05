use std::collections::BTreeMap;

use crate::core::{BinaryOperator, UnaryOperator, Value};
use crate::dataflow::execution::scalar_ir::{
    ScalarProgram, ScalarSsaInstruction, ScalarValueDefinition, ScalarValueId,
};
use crate::dataflow::execution::scheduled_plan::{TemporalOperation, TemporalPlan};

use crate::dataflow::ir::{
    BoundEvaluationGraph, BoundOp, BoundRef, NodeId, ScalarKind, ScalarSignature,
};

use super::artifact::{InputSource, InputSpec, TemporalNodeLayout, TemporalStateLayout};
use super::ir::{
    IntegerDivision, LoweredGraph, LoweredNode, LoweredProgram, ScalarRef, TemporalOp,
    TemporalProgram, TemporalSource,
};

/// Whether a scalar lowering may leave temporal instructions to the temporal kernel.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TemporalBoundaries {
    /// Record the node as a boundary and read its result as an input.
    Lowered,
    /// Refuse the program. The scalar region ABI exposes no temporal state, and boundary node ids
    /// are local to their enclosing graph, so a nested select branch refuses them too.
    Rejected,
}

#[derive(Default)]
pub(super) struct Lowering {
    inputs: Vec<InputSpec>,
    input_indices: BTreeMap<InputSource, u32>,
    boundary_nodes: Vec<NodeId>,
}

fn classify_integer_division(
    op: BinaryOperator,
    left: ScalarKind,
    right: ScalarKind,
    divisor: ScalarRef,
) -> IntegerDivision {
    if !matches!(op, BinaryOperator::Divide | BinaryOperator::Modulo)
        || left != ScalarKind::Int
        || right != ScalarKind::Int
    {
        IntegerDivision::None
    } else if matches!(divisor, ScalarRef::Constant { bits, .. } if bits != 0) {
        IntegerDivision::NonZero
    } else {
        IntegerDivision::Checked
    }
}

impl Lowering {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn lower_scalar_program(
        mut self,
        program: &ScalarProgram,
    ) -> Option<LoweredProgram> {
        let graph = self.lower_scalar_graph(program, TemporalBoundaries::Rejected)?;
        Some(LoweredProgram {
            graph,
            inputs: self.inputs,
            boundary_nodes: Vec::new(),
            temporal: None,
        })
    }

    pub(super) fn lower_temporal_run(
        self,
        canonical: &BoundEvaluationGraph,
        temporal_plan: &TemporalPlan,
    ) -> Option<LoweredProgram> {
        self.lower_temporal_program(canonical, temporal_plan)
    }

    fn lower_temporal_program(
        mut self,
        canonical: &BoundEvaluationGraph,
        temporal_plan: &TemporalPlan,
    ) -> Option<LoweredProgram> {
        let output_kind = infer_output_kind(canonical)?;
        let program = ScalarProgram::from_bound_graph(canonical, output_kind)?;
        let graph = self.lower_scalar_graph(&program, TemporalBoundaries::Lowered)?;
        self.boundary_nodes.sort_unstable();
        self.boundary_nodes.dedup();
        let temporal = if self.boundary_nodes.is_empty() {
            None
        } else {
            Some(self.lower_temporal(canonical, output_kind, temporal_plan)?)
        };
        Some(LoweredProgram {
            graph,
            inputs: self.inputs,
            boundary_nodes: self.boundary_nodes,
            temporal,
        })
    }

    /// Lowers the shared scalar SSA program, folding constants and reassociating integer adds.
    ///
    /// This is the only scalar lowering path: the quick tier, the scalar region backend and the
    /// whole-schedule temporal kernel all start from the same [`ScalarProgram`], and optimization
    /// happens once, here, on the native node representation.
    fn lower_scalar_graph(
        &mut self,
        program: &ScalarProgram,
        temporal: TemporalBoundaries,
    ) -> Option<LoweredGraph> {
        let mut nodes = Vec::with_capacity(program.instructions.len());
        // Maps SSA instruction indexes to native references. Constant-folded instructions need no
        // native node at all, so this is deliberately not just a kind table.
        let mut instruction_refs = Vec::with_capacity(program.instructions.len());
        for instruction in program.instructions.iter() {
            // A temporal instruction keeps its state outside the scalar ABI. The temporal kernel
            // compiles those shapes and reads this graph's result for them as an input; the scalar
            // region ABI exposes no such state at all and rejects them.
            if let ScalarSsaInstruction::Temporal {
                node, output_kind, ..
            } = instruction
            {
                if temporal == TemporalBoundaries::Rejected {
                    return None;
                }
                self.boundary_nodes.push(*node);
                instruction_refs.push(self.lower_input(InputSource::Node(*node), *output_kind)?);
                continue;
            }
            let (node, output_kind) = match instruction {
                ScalarSsaInstruction::Unary {
                    op,
                    input,
                    input_kind,
                    output_kind,
                    ..
                } => {
                    if !supported_unary(*op, *input_kind, *output_kind) {
                        return None;
                    }
                    (
                        LoweredNode::Unary {
                            op: *op,
                            arg: self.lower_scalar_value(program, *input, &instruction_refs)?,
                            input_kind: *input_kind,
                            output_kind: *output_kind,
                        },
                        *output_kind,
                    )
                }
                ScalarSsaInstruction::Binary {
                    op,
                    left,
                    right,
                    left_kind,
                    right_kind,
                    output_kind,
                    ..
                } => {
                    if !supported_binary(*op, *left_kind, *right_kind, *output_kind) {
                        return None;
                    }
                    let lhs = self.lower_scalar_value(program, *left, &instruction_refs)?;
                    let rhs = self.lower_scalar_value(program, *right, &instruction_refs)?;
                    (
                        LoweredNode::Binary {
                            op: *op,
                            lhs,
                            rhs,
                            left_kind: *left_kind,
                            right_kind: *right_kind,
                            output_kind: *output_kind,
                            division: classify_integer_division(*op, *left_kind, *right_kind, rhs),
                        },
                        *output_kind,
                    )
                }
                ScalarSsaInstruction::Temporal { .. } => unreachable!("handled above"),
                ScalarSsaInstruction::EagerSelect {
                    condition,
                    then_program,
                    else_program,
                    output_kind,
                    ..
                } => (
                    LoweredNode::If {
                        condition: self.lower_scalar_value(
                            program,
                            *condition,
                            &instruction_refs,
                        )?,
                        // Boundary node ids are local to their enclosing graph, so a nested
                        // branch keeps any temporal operation on the canonical path.
                        then_graph: Box::new(
                            self.lower_scalar_graph(then_program, TemporalBoundaries::Rejected)?,
                        ),
                        else_graph: Box::new(
                            self.lower_scalar_graph(else_program, TemporalBoundaries::Rejected)?,
                        ),
                    },
                    *output_kind,
                ),
            };
            let node = reassociate_integer_add(node, &nodes);
            let reference = fold_node(&node).unwrap_or_else(|| {
                let index = nodes.len() as u32;
                nodes.push(node);
                ScalarRef::Node {
                    index,
                    kind: output_kind,
                }
            });
            instruction_refs.push(reference);
        }
        let output = self.lower_scalar_value(program, program.output, &instruction_refs)?;
        Some(LoweredGraph {
            nodes,
            output,
            output_kind: program.output_kind(),
        })
    }

    fn lower_scalar_value(
        &mut self,
        program: &ScalarProgram,
        value: ScalarValueId,
        instruction_refs: &[ScalarRef],
    ) -> Option<ScalarRef> {
        let ssa_value = program.values.get(value.index())?;
        let expected = ssa_value.ty.kind;
        match &ssa_value.definition {
            ScalarValueDefinition::Constant(Value::Int(value)) if expected == ScalarKind::Int => {
                Some(ScalarRef::Constant {
                    bits: *value,
                    kind: ScalarKind::Int,
                })
            }
            ScalarValueDefinition::Constant(Value::Float(value))
                if expected == ScalarKind::Float =>
            {
                Some(ScalarRef::Constant {
                    bits: value.to_bits() as i64,
                    kind: ScalarKind::Float,
                })
            }
            ScalarValueDefinition::Constant(Value::Bool(value)) if expected == ScalarKind::Bool => {
                Some(ScalarRef::Constant {
                    bits: i64::from(*value),
                    kind: ScalarKind::Bool,
                })
            }
            ScalarValueDefinition::Constant(_) => None,
            ScalarValueDefinition::External(slot) => {
                self.lower_input(InputSource::External(*slot), expected)
            }
            ScalarValueDefinition::CanonicalNode(node) => {
                self.lower_input(InputSource::Node(*node), expected)
            }
            ScalarValueDefinition::Instruction(instruction) => {
                let reference = *instruction_refs.get(*instruction)?;
                (reference.kind() == expected).then_some(reference)
            }
        }
    }

    fn lower_input(&mut self, source: InputSource, expected: ScalarKind) -> Option<ScalarRef> {
        if let Some(&index) = self.input_indices.get(&source) {
            (self.inputs[index as usize].kind == expected).then_some(ScalarRef::Input {
                index,
                kind: expected,
            })
        } else {
            let index = self.inputs.len() as u32;
            self.inputs.push(InputSpec {
                source,
                kind: expected,
            });
            self.input_indices.insert(source, index);
            Some(ScalarRef::Input {
                index,
                kind: expected,
            })
        }
    }

    /// Builds the state layout for the first complete temporal-kernel subset.  Keeping this
    /// lowering next to scalar lowering makes both backends consume the same bound graph and kind
    /// decisions. Unsupported temporal shapes simply retain the scheduled Rust state driver.
    fn lower_temporal(
        &mut self,
        canonical: &BoundEvaluationGraph,
        output_kind: ScalarKind,
        temporal_plan: &TemporalPlan,
    ) -> Option<TemporalProgram> {
        if !self
            .boundary_nodes
            .iter()
            .copied()
            .eq(temporal_plan.nodes())
        {
            return None;
        }
        let mut kinds = vec![None; canonical.nodes.len()];
        for input in &self.inputs {
            if let InputSource::Node(node) = input.source {
                let slot = &mut kinds[node.index()];
                if slot.is_some_and(|kind| kind != input.kind) {
                    return None;
                }
                *slot = Some(input.kind);
            }
        }
        for operation in temporal_plan.operations.iter() {
            if let TemporalOperation::RecursiveDelay { state, .. } = operation {
                kinds[state.node.index()] = Some(output_kind);
            }
        }
        // Defaults preserve their input kind. Propagate from scalar consumers back through the
        // default to its delay until the small boundary graph is fully typed.
        for _ in 0..self.boundary_nodes.len() {
            for operation in temporal_plan.operations.iter() {
                let TemporalOperation::Default { state, input, .. } = operation else {
                    continue;
                };
                let node = state.node;
                let BoundRef::Node(input) = input else {
                    return None;
                };
                match (kinds[node.index()], kinds[input.index()]) {
                    (Some(kind), None) => kinds[input.index()] = Some(kind),
                    (None, Some(kind)) => kinds[node.index()] = Some(kind),
                    (Some(left), Some(right)) if left != right => return None,
                    _ => {}
                }
            }
        }

        let mut next = 0usize;
        let mut layouts = Vec::new();
        let mut ops = Vec::new();
        for operation in temporal_plan.operations.iter() {
            let node = operation.node();
            let kind = kinds[node.index()]?;
            match operation {
                TemporalOperation::Delay { input, offset, .. } if *offset > 0 => {
                    let len = usize::try_from(*offset).ok()?;
                    let source = self.temporal_source(input, kind)?;
                    let cursor = next;
                    let filled = next + 1;
                    let last_bits = next + 2;
                    let last_tag = next + 3;
                    let cells = next + 4;
                    next = cells.checked_add(len)?;
                    layouts.push(TemporalNodeLayout::Delay {
                        node,
                        kind,
                        cursor,
                        filled,
                        last_bits,
                        last_tag,
                        cells,
                        len,
                    });
                    ops.push(TemporalOp::Delay {
                        node,
                        kind,
                        source,
                        recursive: false,
                        cursor,
                        filled,
                        last_bits,
                        last_tag,
                        cells,
                        len,
                    });
                }
                TemporalOperation::RecursiveDelay { offset, .. } => {
                    let len = usize::try_from(*offset).ok()?;
                    let cursor = next;
                    let filled = next + 1;
                    let last_bits = next + 2;
                    let last_tag = next + 3;
                    let cells = next + 4;
                    next = cells.checked_add(len)?;
                    layouts.push(TemporalNodeLayout::Delay {
                        node,
                        kind,
                        cursor,
                        filled,
                        last_bits,
                        last_tag,
                        cells,
                        len,
                    });
                    ops.push(TemporalOp::Delay {
                        node,
                        kind,
                        source: TemporalSource::Output,
                        recursive: true,
                        cursor,
                        filled,
                        last_bits,
                        last_tag,
                        cells,
                        len,
                    });
                }
                TemporalOperation::Default {
                    input, fallback, ..
                } => {
                    let BoundRef::Node(input) = input else {
                        return None;
                    };
                    if !temporal_plan.operations.iter().any(|operation| {
                        operation.node() == *input
                            && matches!(
                                operation,
                                TemporalOperation::Delay { .. }
                                    | TemporalOperation::RecursiveDelay { .. }
                            )
                    }) {
                        return None;
                    }
                    let fallback = self.temporal_source(fallback, kind)?;
                    let last_bits = next;
                    let last_tag = next + 1;
                    next += 2;
                    layouts.push(TemporalNodeLayout::Default {
                        node,
                        kind,
                        last_bits,
                        last_tag,
                    });
                    ops.push(TemporalOp::Default {
                        node,
                        kind,
                        input: *input,
                        fallback,
                        last_bits,
                        last_tag,
                    });
                }
                _ => return None,
            }
        }
        Some(TemporalProgram {
            ops,
            state: TemporalStateLayout {
                len: next,
                nodes: layouts.into_boxed_slice(),
            },
        })
    }

    fn temporal_source(
        &mut self,
        reference: &BoundRef,
        kind: ScalarKind,
    ) -> Option<TemporalSource> {
        match reference {
            BoundRef::Const(Value::Int(value)) if kind == ScalarKind::Int => {
                Some(TemporalSource::Constant { bits: *value, kind })
            }
            BoundRef::Const(Value::Float(value)) if kind == ScalarKind::Float => {
                Some(TemporalSource::Constant {
                    bits: value.to_bits() as i64,
                    kind,
                })
            }
            BoundRef::Const(Value::Bool(value)) if kind == ScalarKind::Bool => {
                Some(TemporalSource::Constant {
                    bits: i64::from(*value),
                    kind,
                })
            }
            BoundRef::External(slot) => {
                let ScalarRef::Input { index, .. } =
                    self.lower_input(InputSource::External(*slot), kind)?
                else {
                    unreachable!()
                };
                Some(TemporalSource::Input { index })
            }
            BoundRef::Const(_) | BoundRef::Node(_) => None,
        }
    }
}

/// Fold operations whose result is independent of runtime inputs using the wrapping integer
/// semantics of the optimized tiers.
fn fold_node(node: &LoweredNode) -> Option<ScalarRef> {
    let constant = |bits, kind| ScalarRef::Constant { bits, kind };
    match node {
        LoweredNode::Unary {
            op,
            arg: ScalarRef::Constant { bits, kind },
            output_kind,
            ..
        } => match (op, kind, output_kind) {
            (UnaryOperator::Not, ScalarKind::Bool, ScalarKind::Bool) => {
                Some(constant(i64::from(*bits == 0), ScalarKind::Bool))
            }
            (UnaryOperator::Negate, ScalarKind::Int, ScalarKind::Int) => {
                Some(constant(bits.wrapping_neg(), ScalarKind::Int))
            }
            (UnaryOperator::Absolute, ScalarKind::Int, ScalarKind::Int) => {
                Some(constant(bits.wrapping_abs(), ScalarKind::Int))
            }
            _ => None,
        },
        LoweredNode::Binary {
            op,
            lhs:
                ScalarRef::Constant {
                    bits: lhs,
                    kind: lhs_kind,
                },
            rhs:
                ScalarRef::Constant {
                    bits: rhs,
                    kind: rhs_kind,
                },
            output_kind,
            ..
        } if *lhs_kind != ScalarKind::Float && *rhs_kind != ScalarKind::Float => {
            use BinaryOperator as Op;
            let folded = match op {
                Op::Add => Some(lhs.wrapping_add(*rhs)),
                Op::Subtract => Some(lhs.wrapping_sub(*rhs)),
                Op::Multiply => Some(lhs.wrapping_mul(*rhs)),
                Op::Divide | Op::Modulo => None,
                Op::And => Some(*lhs & *rhs),
                Op::Or => Some(*lhs | *rhs),
                Op::Implication => Some((!*lhs & 1) | *rhs),
                Op::Equal => Some(i64::from(lhs == rhs)),
                Op::Less => Some(i64::from(lhs < rhs)),
                Op::LessEqual => Some(i64::from(lhs <= rhs)),
                Op::Greater => Some(i64::from(lhs > rhs)),
                Op::GreaterEqual => Some(i64::from(lhs >= rhs)),
                Op::Concatenate => None,
            }?;
            Some(constant(folded, *output_kind))
        }
        _ => None,
    }
}

/// Collapse the common `((x + c1) + c2)` shape. The superseded node remains in the arena but is
/// removed by demand-driven code generation if nothing else references it.
fn reassociate_integer_add(mut node: LoweredNode, nodes: &[LoweredNode]) -> LoweredNode {
    let LoweredNode::Binary {
        op: BinaryOperator::Add,
        lhs,
        rhs,
        left_kind: ScalarKind::Int,
        right_kind: ScalarKind::Int,
        output_kind: ScalarKind::Int,
        ..
    } = &mut node
    else {
        return node;
    };
    if lhs.int_constant().is_some() && rhs.int_constant().is_none() {
        std::mem::swap(lhs, rhs);
    }
    let Some(outer_constant) = rhs.int_constant() else {
        return node;
    };
    let ScalarRef::Node { index, .. } = *lhs else {
        return node;
    };
    let Some(LoweredNode::Binary {
        op: BinaryOperator::Add,
        lhs: inner_lhs,
        rhs: inner_rhs,
        left_kind: ScalarKind::Int,
        right_kind: ScalarKind::Int,
        output_kind: ScalarKind::Int,
        ..
    }) = nodes.get(index as usize)
    else {
        return node;
    };
    let (base, inner_constant) = if let Some(value) = inner_rhs.int_constant() {
        (*inner_lhs, Some(value))
    } else {
        (*inner_rhs, inner_lhs.int_constant())
    };
    let Some(inner_constant) = inner_constant else {
        return node;
    };
    let combined = inner_constant.wrapping_add(outer_constant);
    *lhs = base;
    *rhs = ScalarRef::Constant {
        bits: combined,
        kind: ScalarKind::Int,
    };
    node
}

fn supported_unary(op: UnaryOperator, input: ScalarKind, output: ScalarKind) -> bool {
    matches!(
        (op, input, output),
        (UnaryOperator::Negate, ScalarKind::Int, ScalarKind::Int)
            | (UnaryOperator::Absolute, ScalarKind::Int, ScalarKind::Int)
            | (UnaryOperator::Negate, ScalarKind::Float, ScalarKind::Float)
            | (
                UnaryOperator::Absolute,
                ScalarKind::Float,
                ScalarKind::Float
            )
            | (UnaryOperator::Not, ScalarKind::Bool, ScalarKind::Bool)
    )
}

fn supported_binary(
    op: BinaryOperator,
    left: ScalarKind,
    right: ScalarKind,
    output: ScalarKind,
) -> bool {
    use BinaryOperator as Op;
    let numeric = |kind| matches!(kind, ScalarKind::Int | ScalarKind::Float);
    match op {
        Op::Add | Op::Subtract | Op::Multiply => {
            numeric(left)
                && numeric(right)
                && output
                    == if left == ScalarKind::Int && right == ScalarKind::Int {
                        ScalarKind::Int
                    } else {
                        ScalarKind::Float
                    }
        }
        Op::Divide => {
            numeric(left)
                && numeric(right)
                && output
                    == if left == ScalarKind::Int && right == ScalarKind::Int {
                        ScalarKind::Int
                    } else {
                        ScalarKind::Float
                    }
        }
        Op::Modulo => {
            left == ScalarKind::Int && right == ScalarKind::Int && output == ScalarKind::Int
        }
        Op::And | Op::Or | Op::Implication => {
            left == ScalarKind::Bool && right == ScalarKind::Bool && output == ScalarKind::Bool
        }
        Op::Equal => {
            output == ScalarKind::Bool
                && ((numeric(left) && numeric(right))
                    || (left == ScalarKind::Bool && right == ScalarKind::Bool))
        }
        Op::Less | Op::LessEqual | Op::Greater | Op::GreaterEqual => {
            output == ScalarKind::Bool
                && ((numeric(left) && numeric(right))
                    || (left == ScalarKind::Bool && right == ScalarKind::Bool))
        }
        Op::Concatenate => false,
    }
}

fn infer_output_kind(graph: &BoundEvaluationGraph) -> Option<ScalarKind> {
    let mut node_kinds = Vec::with_capacity(graph.nodes.len());
    for (op, signature) in graph.nodes.iter().zip(&graph.scalar_signatures) {
        let kind = match (op, signature) {
            (_, Some(ScalarSignature::Unary { output, .. }))
            | (_, Some(ScalarSignature::Binary { output, .. })) => Some(*output),
            (
                BoundOp::If {
                    then_branch,
                    else_branch,
                    ..
                },
                _,
            ) => {
                let then_kind = infer_output_kind(then_branch)?;
                Some((infer_output_kind(else_branch)? == then_kind).then_some(then_kind)?)
            }
            _ => None,
        };
        node_kinds.push(kind);
    }
    match &graph.output {
        BoundRef::Const(Value::Int(_)) => Some(ScalarKind::Int),
        BoundRef::Const(Value::Float(_)) => Some(ScalarKind::Float),
        BoundRef::Const(Value::Bool(_)) => Some(ScalarKind::Bool),
        BoundRef::Node(node) => node_kinds.get(node.index()).copied().flatten(),
        BoundRef::Const(_) | BoundRef::External(_) => None,
    }
}
