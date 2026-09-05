//! Backend-neutral scalar instruction graphs.
//!
//! This layer records the logical scalar portion of a bound evaluation graph. It deliberately has
//! no register, native-value, lifting, temporal, or side-exit state. Consumers are expected to
//! derive those physical details independently while retaining the canonical node identities in
//! this graph.

use std::ops::Range;

use crate::core::{BinaryOperator, UnaryOperator, Value};
use crate::dataflow::environment::EnvironmentSlot;
use crate::dataflow::ir::{
    BoundEvaluationGraph, BoundOp, BoundRef, NodeId, ScalarKind, ScalarSignature,
};

/// The identity of a value in a [`ScalarProgram`].
///
/// Unlike [`NodeId`], this identifies constants and external inputs as well as instruction results.
/// Canonical node identity is retained separately on instruction-result definitions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(in crate::dataflow) struct ScalarValueId(usize);

impl ScalarValueId {
    #[inline]
    const fn new(index: usize) -> Self {
        Self(index)
    }

    #[inline]
    pub(in crate::dataflow) const fn index(self) -> usize {
        self.0
    }
}

/// Whether a scalar SSA value can carry a stream-presence marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::dataflow) enum ScalarPresence {
    AlwaysPresent,
    MaybeSpecial,
}

/// The semantic type of a scalar SSA value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::dataflow) struct ScalarValueType {
    pub(in crate::dataflow) kind: ScalarKind,
    pub(in crate::dataflow) presence: ScalarPresence,
}

/// The definition of a scalar SSA value.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::dataflow) enum ScalarValueDefinition {
    Constant(Value),
    External(EnvironmentSlot),
    /// A value produced by a canonical node of the enclosing graph that this program does not
    /// contain. Only island programs, which cover a node range rather than a whole graph, have
    /// these boundary inputs.
    CanonicalNode(NodeId),
    Instruction(usize),
}

/// A typed scalar SSA value.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::dataflow) struct ScalarValue {
    pub(in crate::dataflow) ty: ScalarValueType,
    pub(in crate::dataflow) canonical_node: Option<NodeId>,
    pub(in crate::dataflow) definition: ScalarValueDefinition,
}

/// A temporal operation over scalar values.
///
/// This mirrors [`crate::dataflow::execution::scheduled_plan::TemporalOperation`]; the scheduled
/// plan keeps describing the same nodes for the commit barrier and for native state layout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::dataflow) enum ScalarTemporalOp {
    /// `x[offset]`. `external` records a direct delay of an environment variable, which may read
    /// the monitor's shared history instead of a local ring.
    Delay {
        input: ScalarValueId,
        offset: u64,
        external: Option<EnvironmentSlot>,
    },
    /// A recursive `s[offset]`; the enclosing stream's result is staged after evaluation.
    RecursiveDelay,
    Default {
        input: ScalarValueId,
        fallback: ScalarValueId,
    },
}

/// A backend-neutral scalar SSA instruction.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::dataflow) enum ScalarSsaInstruction {
    Unary {
        result: ScalarValueId,
        node: NodeId,
        op: UnaryOperator,
        input: ScalarValueId,
        input_kind: ScalarKind,
        output_kind: ScalarKind,
    },
    Binary {
        result: ScalarValueId,
        node: NodeId,
        op: BinaryOperator,
        left: ScalarValueId,
        right: ScalarValueId,
        left_kind: ScalarKind,
        right_kind: ScalarKind,
        output_kind: ScalarKind,
    },
    /// A temporal operation whose retained state stays in the canonical evaluator arena.
    ///
    /// The scalar engine dispatches these by type and keeps their results in registers, but it does
    /// not own their delay rings: the canonical `NodeState` remains the single implementation of
    /// delay, recursion, and default semantics, and the tick commit barrier is unchanged.
    Temporal {
        result: ScalarValueId,
        node: NodeId,
        op: ScalarTemporalOp,
        output_kind: ScalarKind,
    },
    /// Evaluates both nested regions, then selects one result using the retained condition.
    ///
    /// This deliberately models ordinary non-recursive dataflow `if`; recursive function bodies,
    /// whose canonical semantics evaluate only the selected branch, are not admitted here.
    EagerSelect {
        result: ScalarValueId,
        node: NodeId,
        condition: ScalarValueId,
        then_program: Box<ScalarProgram>,
        else_program: Box<ScalarProgram>,
        output_kind: ScalarKind,
    },
}

impl ScalarSsaInstruction {
    #[inline]
    pub(in crate::dataflow) fn result(&self) -> ScalarValueId {
        match self {
            Self::Unary { result, .. }
            | Self::Binary { result, .. }
            | Self::Temporal { result, .. }
            | Self::EagerSelect { result, .. } => *result,
        }
    }

    #[inline]
    fn node(&self) -> NodeId {
        match self {
            Self::Unary { node, .. }
            | Self::Binary { node, .. }
            | Self::Temporal { node, .. }
            | Self::EagerSelect { node, .. } => *node,
        }
    }

    #[inline]
    fn output_kind(&self) -> ScalarKind {
        match self {
            Self::Unary { output_kind, .. }
            | Self::Binary { output_kind, .. }
            | Self::Temporal { output_kind, .. }
            | Self::EagerSelect { output_kind, .. } => *output_kind,
        }
    }

    fn visit_inputs(&self, mut visit: impl FnMut(ScalarValueId, ScalarKind)) {
        match self {
            Self::Unary {
                input, input_kind, ..
            } => visit(*input, *input_kind),
            Self::Binary {
                left,
                right,
                left_kind,
                right_kind,
                ..
            } => {
                visit(*left, *left_kind);
                visit(*right, *right_kind);
            }
            Self::EagerSelect { condition, .. } => visit(*condition, ScalarKind::Bool),
            Self::Temporal {
                op, output_kind, ..
            } => match op {
                ScalarTemporalOp::Delay { input, .. } => visit(*input, *output_kind),
                ScalarTemporalOp::Default { input, fallback } => {
                    visit(*input, *output_kind);
                    visit(*fallback, *output_kind);
                }
                ScalarTemporalOp::RecursiveDelay => {}
            },
        }
    }
}

/// A verified, backend-neutral scalar SSA program.
///
/// Values are defined exactly once. Instruction inputs must refer to constants, external inputs, or
/// dominating instruction results. Physical registers, native ABI offsets, and canonical evaluator
/// state are deliberately absent from this representation.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::dataflow) struct ScalarProgram {
    pub(in crate::dataflow) values: Box<[ScalarValue]>,
    pub(in crate::dataflow) instructions: Box<[ScalarSsaInstruction]>,
    pub(in crate::dataflow) output: ScalarValueId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScalarProgramError {
    InvalidOutput,
    InvalidResult,
    InvalidDefinition,
    InvalidInput,
    NonDominatingInput,
    TypeMismatch,
    CanonicalIdentityMismatch,
}

/// Which value a built program publishes as its result.
#[derive(Clone, Copy)]
enum ProgramOutput {
    /// The enclosing graph's output reference, converted to this kind.
    Graph(ScalarKind),
    /// The last instruction result, used by island programs whose exports are chosen by the
    /// region planner rather than by the graph output.
    LastInstruction,
}

impl ScalarProgram {
    /// Extracts and verifies a complete scalar evaluation graph.
    pub(in crate::dataflow) fn from_bound_graph(
        graph: &BoundEvaluationGraph,
        output_kind: ScalarKind,
    ) -> Option<Self> {
        Self::build(
            graph,
            0..graph.nodes.len(),
            ProgramOutput::Graph(output_kind),
        )
    }

    /// Extracts one straight-line island covering `nodes` of an otherwise canonical graph.
    ///
    /// Operands naming nodes outside the range become [`ScalarValueDefinition::CanonicalNode`]
    /// boundary inputs, so the island composes with canonical execution of the surrounding nodes.
    pub(in crate::dataflow) fn from_bound_graph_island(
        graph: &BoundEvaluationGraph,
        nodes: Range<usize>,
    ) -> Option<Self> {
        Self::build(graph, nodes, ProgramOutput::LastInstruction)
    }

    fn build(
        graph: &BoundEvaluationGraph,
        nodes: Range<usize>,
        output: ProgramOutput,
    ) -> Option<Self> {
        // A whole graph may legitimately have no nodes: `out y = x` publishes an external directly.
        // An island always covers at least one node, which `ProgramOutput::LastInstruction` checks.
        if graph.nodes.len() != graph.scalar_signatures.len() || nodes.end > graph.nodes.len() {
            return None;
        }
        let island = nodes.start != 0 || nodes.end != graph.nodes.len();

        let node_kinds = infer_node_kinds(graph)?;
        let mut values = Vec::with_capacity(nodes.len() * 2 + 1);
        let mut instructions = Vec::with_capacity(nodes.len());
        let mut node_values = vec![None; graph.nodes.len()];

        for index in nodes.clone() {
            let operation = &graph.nodes[index];
            let signature = &graph.scalar_signatures[index];
            let node = NodeId::new(index);
            let instruction_index = instructions.len();
            let instruction = match (operation, signature) {
                (BoundOp::Unary { op, arg }, Some(ScalarSignature::Unary { input, output })) => {
                    let input_value =
                        intern_bound_ref(arg, *input, &nodes, &node_values, &mut values, island)?;
                    let result =
                        push_instruction_value(node, *output, instruction_index, &mut values);
                    ScalarSsaInstruction::Unary {
                        result,
                        node,
                        op: *op,
                        input: input_value,
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
                ) => {
                    let left_value =
                        intern_bound_ref(lhs, *left, &nodes, &node_values, &mut values, island)?;
                    let right_value =
                        intern_bound_ref(rhs, *right, &nodes, &node_values, &mut values, island)?;
                    let result =
                        push_instruction_value(node, *output, instruction_index, &mut values);
                    ScalarSsaInstruction::Binary {
                        result,
                        node,
                        op: *op,
                        left: left_value,
                        right: right_value,
                        left_kind: *left,
                        right_kind: *right,
                        output_kind: *output,
                    }
                }
                (
                    BoundOp::If {
                        cond,
                        then_branch,
                        else_branch,
                    },
                    None,
                ) if !island => {
                    let output = *node_kinds.get(index)?.as_ref()?;
                    let condition = intern_bound_ref(
                        cond,
                        ScalarKind::Bool,
                        &nodes,
                        &node_values,
                        &mut values,
                        island,
                    )?;
                    let then_program = Self::from_bound_graph(then_branch, output)?;
                    let else_program = Self::from_bound_graph(else_branch, output)?;
                    let result =
                        push_instruction_value(node, output, instruction_index, &mut values);
                    ScalarSsaInstruction::EagerSelect {
                        result,
                        node,
                        condition,
                        then_program: Box::new(then_program),
                        else_program: Box::new(else_program),
                        output_kind: output,
                    }
                }
                (BoundOp::Delay { input, offset }, None) => {
                    let output = *node_kinds.get(index)?.as_ref()?;
                    let input_value =
                        intern_bound_ref(input, output, &nodes, &node_values, &mut values, island)?;
                    ScalarSsaInstruction::Temporal {
                        result: push_instruction_value(
                            node,
                            output,
                            instruction_index,
                            &mut values,
                        ),
                        node,
                        op: ScalarTemporalOp::Delay {
                            input: input_value,
                            offset: *offset,
                            external: match input {
                                BoundRef::External(slot) => Some(*slot),
                                _ => None,
                            },
                        },
                        output_kind: output,
                    }
                }
                (BoundOp::RecursiveDelay { .. }, None) => {
                    let output = *node_kinds.get(index)?.as_ref()?;
                    ScalarSsaInstruction::Temporal {
                        result: push_instruction_value(
                            node,
                            output,
                            instruction_index,
                            &mut values,
                        ),
                        node,
                        op: ScalarTemporalOp::RecursiveDelay,
                        output_kind: output,
                    }
                }
                (BoundOp::Default { input, fallback }, None) => {
                    let output = *node_kinds.get(index)?.as_ref()?;
                    let input_value =
                        intern_bound_ref(input, output, &nodes, &node_values, &mut values, island)?;
                    let fallback_value = intern_bound_ref(
                        fallback,
                        output,
                        &nodes,
                        &node_values,
                        &mut values,
                        island,
                    )?;
                    ScalarSsaInstruction::Temporal {
                        result: push_instruction_value(
                            node,
                            output,
                            instruction_index,
                            &mut values,
                        ),
                        node,
                        op: ScalarTemporalOp::Default {
                            input: input_value,
                            fallback: fallback_value,
                        },
                        output_kind: output,
                    }
                }
                _ => return None,
            };
            node_values[index] = Some(instruction.result());
            instructions.push(instruction);
        }

        let output = match output {
            ProgramOutput::Graph(output_kind) => intern_bound_ref(
                &graph.output,
                output_kind,
                &nodes,
                &node_values,
                &mut values,
                island,
            )?,
            ProgramOutput::LastInstruction => instructions.last()?.result(),
        };
        let program = Self {
            values: values.into_boxed_slice(),
            instructions: instructions.into_boxed_slice(),
            output,
        };
        program.verify().ok()?;
        Some(program)
    }

    fn verify(&self) -> Result<(), ScalarProgramError> {
        self.values
            .get(self.output.index())
            .ok_or(ScalarProgramError::InvalidOutput)?;

        for (value_index, value) in self.values.iter().enumerate() {
            match &value.definition {
                ScalarValueDefinition::Constant(constant) => {
                    if value.canonical_node.is_some()
                        || !(scalar_value_kind(constant) == Some(value.ty.kind)
                            || matches!(constant, Value::NoVal | Value::Deferred))
                    {
                        return Err(ScalarProgramError::InvalidDefinition);
                    }
                }
                ScalarValueDefinition::External(_) | ScalarValueDefinition::CanonicalNode(_) => {
                    if value.canonical_node.is_some() {
                        return Err(ScalarProgramError::InvalidDefinition);
                    }
                }
                ScalarValueDefinition::Instruction(instruction_index) => {
                    let instruction = self
                        .instructions
                        .get(*instruction_index)
                        .ok_or(ScalarProgramError::InvalidDefinition)?;
                    if instruction.result().index() != value_index {
                        return Err(ScalarProgramError::InvalidDefinition);
                    }
                }
            }
        }

        for (instruction_index, instruction) in self.instructions.iter().enumerate() {
            let result = self
                .values
                .get(instruction.result().index())
                .ok_or(ScalarProgramError::InvalidResult)?;
            if result.definition != ScalarValueDefinition::Instruction(instruction_index) {
                return Err(ScalarProgramError::InvalidResult);
            }
            if result.canonical_node != Some(instruction.node()) {
                return Err(ScalarProgramError::CanonicalIdentityMismatch);
            }
            if result.ty.kind != instruction.output_kind() {
                return Err(ScalarProgramError::TypeMismatch);
            }
            if let ScalarSsaInstruction::EagerSelect {
                then_program,
                else_program,
                output_kind,
                ..
            } = instruction
            {
                then_program.verify()?;
                else_program.verify()?;
                if then_program.output_kind() != *output_kind
                    || else_program.output_kind() != *output_kind
                {
                    return Err(ScalarProgramError::TypeMismatch);
                }
            }

            let mut input_error = None;
            instruction.visit_inputs(|input, expected_kind| {
                if input_error.is_some() {
                    return;
                }
                let Some(value) = self.values.get(input.index()) else {
                    input_error = Some(ScalarProgramError::InvalidInput);
                    return;
                };
                if value.ty.kind != expected_kind {
                    input_error = Some(ScalarProgramError::TypeMismatch);
                    return;
                }
                if let ScalarValueDefinition::Instruction(definition) = value.definition
                    && definition >= instruction_index
                {
                    input_error = Some(ScalarProgramError::NonDominatingInput);
                }
            });
            if let Some(error) = input_error {
                return Err(error);
            }
        }
        Ok(())
    }

    #[inline]
    pub(in crate::dataflow) fn output_kind(&self) -> ScalarKind {
        self.values[self.output.index()].ty.kind
    }
}

/// Computes each node's scalar output kind, leaving `None` where no kind is established.
///
/// Only the type checker's operation signatures seed this. Declared stream types in the environment
/// layout are deliberately not consulted: an untyped program keeps its annotations but never
/// verified them, and a temporal node typed from an unverified annotation could be handed a value
/// the scalar engine cannot represent.
///
/// Temporal nodes carry no signature, so their kinds come from the operations around them: a delay
/// and its input share a kind, a default agrees with both its input and its fallback, and a
/// recursive delay is typed only by its consumers. Those constraints run to a fixpoint because they
/// propagate backwards as well as forwards.
fn infer_node_kinds(graph: &BoundEvaluationGraph) -> Option<Vec<Option<ScalarKind>>> {
    if graph.nodes.len() != graph.scalar_signatures.len() {
        return None;
    }
    let mut kinds: Vec<Option<ScalarKind>> = Vec::with_capacity(graph.nodes.len());
    for (operation, signature) in graph.nodes.iter().zip(&graph.scalar_signatures) {
        let kind = match (operation, signature) {
            (_, Some(ScalarSignature::Unary { output, .. }))
            | (_, Some(ScalarSignature::Binary { output, .. })) => Some(*output),
            (
                BoundOp::If {
                    then_branch,
                    else_branch,
                    ..
                },
                None,
            ) => match (
                infer_graph_output_kind(then_branch),
                infer_graph_output_kind(else_branch),
            ) {
                // An unknown branch kind leaves this node untyped rather than untyping the graph:
                // the nodes around a canonical conditional may still form islands.
                (Some(then_kind), Some(else_kind)) if then_kind == else_kind => Some(then_kind),
                _ => None,
            },
            _ => None,
        };
        kinds.push(kind);
    }

    for _ in 0..=graph.nodes.len() {
        let mut changed = false;
        for (index, operation) in graph.nodes.iter().enumerate() {
            match (operation, &graph.scalar_signatures[index]) {
                (BoundOp::Unary { arg, .. }, Some(ScalarSignature::Unary { input, .. })) => {
                    changed |= constrain(&mut kinds, arg, Some(*input));
                }
                (
                    BoundOp::Binary { lhs, rhs, .. },
                    Some(ScalarSignature::Binary { left, right, .. }),
                ) => {
                    changed |= constrain(&mut kinds, lhs, Some(*left));
                    changed |= constrain(&mut kinds, rhs, Some(*right));
                }
                _ => {}
            }
            let operands: &[&BoundRef] = match operation {
                BoundOp::Delay { input, .. } => &[input],
                BoundOp::Default { input, fallback } => &[input, fallback],
                _ => &[],
            };
            for operand in operands {
                let operand_kind = reference_kind(&kinds, operand);
                let node_kind = kinds[index];
                changed |= constrain(&mut kinds, operand, node_kind);
                if node_kind.is_none() && operand_kind.is_some() {
                    kinds[index] = operand_kind;
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    Some(kinds)
}

/// Records `kind` for a node reference that has no kind yet. Conflicts are left alone: program
/// construction re-checks every operand kind and rejects the mismatch there.
fn constrain(
    kinds: &mut [Option<ScalarKind>],
    reference: &BoundRef,
    kind: Option<ScalarKind>,
) -> bool {
    let (Some(kind), BoundRef::Node(node)) = (kind, reference) else {
        return false;
    };
    match kinds.get_mut(node.index()) {
        Some(slot @ None) => {
            *slot = Some(kind);
            true
        }
        _ => false,
    }
}

/// The kind of a temporal operand, considered only where it traces back to a signature.
///
/// A literal is deliberately not a source of truth here: `default(s[1], 0)` in an untyped program
/// would otherwise type its delay from the fallback literal alone, and nothing has checked that the
/// delayed stream actually carries integers.
fn reference_kind(kinds: &[Option<ScalarKind>], reference: &BoundRef) -> Option<ScalarKind> {
    match reference {
        BoundRef::Node(node) => kinds.get(node.index()).copied().flatten(),
        BoundRef::Const(_) | BoundRef::External(_) => None,
    }
}

fn infer_graph_output_kind(graph: &BoundEvaluationGraph) -> Option<ScalarKind> {
    let node_kinds = infer_node_kinds(graph)?;
    match &graph.output {
        BoundRef::Const(value) => scalar_value_kind(value),
        BoundRef::Node(node) => node_kinds.get(node.index()).copied().flatten(),
        BoundRef::External(_) => None,
    }
}

fn push_instruction_value(
    node: NodeId,
    kind: ScalarKind,
    instruction: usize,
    values: &mut Vec<ScalarValue>,
) -> ScalarValueId {
    let id = ScalarValueId::new(values.len());
    values.push(ScalarValue {
        ty: ScalarValueType {
            kind,
            presence: ScalarPresence::MaybeSpecial,
        },
        canonical_node: Some(node),
        definition: ScalarValueDefinition::Instruction(instruction),
    });
    id
}

fn intern_bound_ref(
    reference: &BoundRef,
    expected: ScalarKind,
    nodes: &Range<usize>,
    node_values: &[Option<ScalarValueId>],
    values: &mut Vec<ScalarValue>,
    island: bool,
) -> Option<ScalarValueId> {
    if let BoundRef::Node(node) = reference {
        if let Some(id) = node_values.get(node.index()).copied().flatten() {
            return (values.get(id.index())?.ty.kind == expected).then_some(id);
        }
        if !island || nodes.contains(&node.index()) {
            return None;
        }
        let id = ScalarValueId::new(values.len());
        values.push(ScalarValue {
            ty: ScalarValueType {
                kind: expected,
                presence: ScalarPresence::MaybeSpecial,
            },
            canonical_node: None,
            definition: ScalarValueDefinition::CanonicalNode(*node),
        });
        return Some(id);
    }

    let (definition, presence) = match reference {
        BoundRef::Const(value)
            if scalar_value_kind(value) == Some(expected)
                || matches!(value, Value::NoVal | Value::Deferred) =>
        {
            (
                ScalarValueDefinition::Constant(value.clone()),
                if matches!(value, Value::NoVal | Value::Deferred) {
                    ScalarPresence::MaybeSpecial
                } else {
                    ScalarPresence::AlwaysPresent
                },
            )
        }
        BoundRef::External(slot) => (
            ScalarValueDefinition::External(*slot),
            ScalarPresence::MaybeSpecial,
        ),
        BoundRef::Const(_) | BoundRef::Node(_) => return None,
    };
    let id = ScalarValueId::new(values.len());
    values.push(ScalarValue {
        ty: ScalarValueType {
            kind: expected,
            presence,
        },
        canonical_node: None,
        definition,
    });
    Some(id)
}

fn scalar_value_kind(value: &Value) -> Option<ScalarKind> {
    match value {
        Value::Int(_) => Some(ScalarKind::Int),
        Value::Float(_) => Some(ScalarKind::Float),
        Value::Bool(_) => Some(ScalarKind::Bool),
        Value::NoVal
        | Value::Deferred
        | Value::Str(_)
        | Value::Function(_)
        | Value::List(_)
        | Value::Tuple(_)
        | Value::Map(_)
        | Value::Unit => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extraction_preserves_canonical_nodes_and_operand_kinds() {
        let graph = BoundEvaluationGraph::new(
            vec![
                BoundOp::Unary {
                    op: UnaryOperator::Negate,
                    arg: BoundRef::External(EnvironmentSlot::new(3)),
                },
                BoundOp::Binary {
                    op: BinaryOperator::Add,
                    lhs: BoundRef::Node(NodeId::new(0)),
                    rhs: BoundRef::Const(Value::Int(2)),
                },
            ],
            vec![
                Some(ScalarSignature::Unary {
                    input: ScalarKind::Int,
                    output: ScalarKind::Int,
                }),
                Some(ScalarSignature::Binary {
                    left: ScalarKind::Int,
                    right: ScalarKind::Int,
                    output: ScalarKind::Int,
                }),
            ],
            BoundRef::Node(NodeId::new(1)),
        );

        let program = ScalarProgram::from_bound_graph(&graph, ScalarKind::Int).unwrap();
        assert_eq!(program.instructions.len(), 2);
        assert_eq!(
            program.values[program.output.index()].ty.kind,
            ScalarKind::Int
        );
        assert_eq!(
            program.values[program.output.index()].canonical_node,
            Some(NodeId::new(1))
        );
        let ScalarSsaInstruction::Binary { left, right, .. } = &program.instructions[1] else {
            panic!("expected binary SSA instruction");
        };
        assert_eq!(
            program.values[left.index()].canonical_node,
            Some(NodeId::new(0))
        );
        assert_eq!(
            program.values[right.index()].ty.presence,
            ScalarPresence::AlwaysPresent
        );
    }

    #[test]
    fn verifier_rejects_non_dominating_ssa_inputs() {
        let graph = BoundEvaluationGraph::new(
            vec![BoundOp::Unary {
                op: UnaryOperator::Negate,
                arg: BoundRef::External(EnvironmentSlot::new(0)),
            }],
            vec![Some(ScalarSignature::Unary {
                input: ScalarKind::Int,
                output: ScalarKind::Int,
            })],
            BoundRef::Node(NodeId::new(0)),
        );
        let mut program = ScalarProgram::from_bound_graph(&graph, ScalarKind::Int).unwrap();
        let result = program.instructions[0].result();
        let ScalarSsaInstruction::Unary { input, .. } = &mut program.instructions[0] else {
            unreachable!();
        };
        *input = result;

        assert_eq!(
            program.verify(),
            Err(ScalarProgramError::NonDominatingInput)
        );
    }

    #[test]
    fn eager_select_preserves_nested_ssa_regions_and_local_node_ids() {
        let graph = BoundEvaluationGraph::new(
            vec![BoundOp::If {
                cond: BoundRef::Const(Value::Bool(true)),
                then_branch: BoundEvaluationGraph::new(
                    vec![BoundOp::Binary {
                        op: BinaryOperator::Add,
                        lhs: BoundRef::Const(Value::Int(1)),
                        rhs: BoundRef::Const(Value::Int(2)),
                    }],
                    vec![Some(ScalarSignature::Binary {
                        left: ScalarKind::Int,
                        right: ScalarKind::Int,
                        output: ScalarKind::Int,
                    })],
                    BoundRef::Node(NodeId::new(0)),
                ),
                else_branch: BoundEvaluationGraph::new(
                    vec![BoundOp::Binary {
                        op: BinaryOperator::Subtract,
                        lhs: BoundRef::Const(Value::Int(3)),
                        rhs: BoundRef::Const(Value::Int(1)),
                    }],
                    vec![Some(ScalarSignature::Binary {
                        left: ScalarKind::Int,
                        right: ScalarKind::Int,
                        output: ScalarKind::Int,
                    })],
                    BoundRef::Node(NodeId::new(0)),
                ),
            }],
            vec![None],
            BoundRef::Node(NodeId::new(0)),
        );

        let program = ScalarProgram::from_bound_graph(&graph, ScalarKind::Int).unwrap();
        let ScalarSsaInstruction::EagerSelect {
            condition,
            then_program,
            else_program,
            output_kind,
            ..
        } = &program.instructions[0]
        else {
            panic!("expected eager-select SSA instruction");
        };
        assert_eq!(program.values[condition.index()].ty.kind, ScalarKind::Bool);
        assert_eq!(*output_kind, ScalarKind::Int);
        assert_eq!(then_program.instructions[0].node(), NodeId::new(0));
        assert_eq!(else_program.instructions[0].node(), NodeId::new(0));
    }
}
