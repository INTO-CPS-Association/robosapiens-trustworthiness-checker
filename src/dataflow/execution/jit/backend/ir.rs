use crate::core::{BinaryOperator, UnaryOperator};
use crate::dataflow::ir::{NodeId, ScalarKind};

use super::artifact::{InputSpec, TemporalStateLayout};

#[derive(Clone, Copy)]
pub(super) enum ScalarRef {
    Constant { bits: i64, kind: ScalarKind },
    Input { index: u32, kind: ScalarKind },
    Node { index: u32, kind: ScalarKind },
}

pub(super) struct LoweredGraph {
    pub(super) nodes: Vec<LoweredNode>,
    pub(super) output: ScalarRef,
    pub(super) output_kind: ScalarKind,
}

pub(super) enum LoweredNode {
    Unary {
        op: UnaryOperator,
        arg: ScalarRef,
        input_kind: ScalarKind,
        output_kind: ScalarKind,
    },
    Binary {
        op: BinaryOperator,
        lhs: ScalarRef,
        rhs: ScalarRef,
        left_kind: ScalarKind,
        right_kind: ScalarKind,
        output_kind: ScalarKind,
        division: IntegerDivision,
    },
    If {
        condition: ScalarRef,
        then_graph: Box<LoweredGraph>,
        else_graph: Box<LoweredGraph>,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum IntegerDivision {
    None,
    NonZero,
    Checked,
}

pub(super) struct LoweredProgram {
    pub(super) graph: LoweredGraph,
    pub(super) inputs: Vec<InputSpec>,
    pub(super) boundary_nodes: Vec<NodeId>,
    pub(super) temporal: Option<TemporalProgram>,
}

impl LoweredProgram {
    pub(super) fn requires_division_guard(&self) -> bool {
        graph_requires_division_guard(&self.graph)
    }

    pub(super) fn output_kind(&self) -> ScalarKind {
        self.graph.output_kind
    }
}

pub(super) struct TemporalProgram {
    pub(super) ops: Vec<TemporalOp>,
    pub(super) state: TemporalStateLayout,
}

pub(super) enum TemporalOp {
    Delay {
        node: NodeId,
        kind: ScalarKind,
        source: TemporalSource,
        recursive: bool,
        cursor: usize,
        filled: usize,
        last_bits: usize,
        last_tag: usize,
        cells: usize,
        len: usize,
    },
    Default {
        node: NodeId,
        kind: ScalarKind,
        input: NodeId,
        fallback: TemporalSource,
        last_bits: usize,
        last_tag: usize,
    },
}

#[derive(Clone, Copy)]
pub(super) enum TemporalSource {
    Constant { bits: i64, kind: ScalarKind },
    Input { index: u32 },
    Output,
}

fn graph_requires_division_guard(graph: &LoweredGraph) -> bool {
    graph.nodes.iter().any(|node| match node {
        LoweredNode::Binary {
            division: IntegerDivision::Checked,
            ..
        } => true,
        LoweredNode::If {
            then_graph,
            else_graph,
            ..
        } => graph_requires_division_guard(then_graph) || graph_requires_division_guard(else_graph),
        LoweredNode::Unary { .. } | LoweredNode::Binary { .. } => false,
    })
}

impl ScalarRef {
    pub(super) fn kind(self) -> ScalarKind {
        match self {
            Self::Constant { kind, .. } | Self::Input { kind, .. } | Self::Node { kind, .. } => {
                kind
            }
        }
    }

    pub(super) fn int_constant(self) -> Option<i64> {
        match self {
            Self::Constant {
                bits,
                kind: ScalarKind::Int,
            } => Some(bits),
            _ => None,
        }
    }
}
