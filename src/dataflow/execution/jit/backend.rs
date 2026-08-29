//! Cranelift lowering and code generation for typed scalar graphs and complete runs.
//!
//! This backend consumes already bound canonical IR and returns typed native artifacts. It has no
//! monitor lifecycle, scheduling, hotness, or deoptimization policy; those live in the coordinator
//! and runtime modules. Unsupported IR is represented by the absence of a lowered program, while
//! backend failures are returned as diagnostics rather than silently converted to ineligibility.
//!
//! Floating-point operations intentionally use Cranelift's native IEEE-754 operations. The
//! prototype therefore does not guard quirks around NaNs, signed zero, or mixed `Int`/`Float`
//! conversion and comparison. Those cases may differ from today's generic `Value` operations.
//! Transcendental functions and floating-point remainder are not lowered yet.

use std::collections::BTreeMap;
use std::rc::Rc;

use cranelift_codegen::ir::condcodes::{FloatCC, IntCC};
use cranelift_codegen::ir::immediates::Ieee64;
use cranelift_codegen::ir::{self, AbiParam, InstBuilder, MemFlagsData, types};
use cranelift_codegen::settings::{self, Configurable};
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{FuncId, Linkage, Module};

use crate::core::{BinaryOperator, UnaryOperator};
use crate::dataflow::environment::EnvironmentSlot;
use crate::dataflow::execution::scheduled_plan::ScheduledExecutionPlan;
use crate::dataflow::execution::scheduled_plan::{TemporalOperation, TemporalPlan};
use crate::dataflow::execution_plan::StreamSlots;
use crate::dataflow::ir::{
    BoundEvaluationGraph, BoundOp, BoundRef, NodeId, ScalarKind, ScalarSignature, StreamOp,
};
use crate::dataflow::*;

pub(super) const STATUS_OK: i32 = 0;
const STATUS_FALLBACK: i32 = 1;

pub(super) type GraphFn = unsafe extern "C" fn(*const i64, *mut i64) -> i32;
pub(super) type TemporalRunFn = unsafe extern "C" fn(*mut i64, *mut i64) -> i32;
pub(super) type RunFn = unsafe extern "C" fn(*mut i64) -> i32;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum InputSource {
    External(EnvironmentSlot),
    Node(NodeId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct InputSpec {
    pub(super) source: InputSource,
    pub(super) kind: ScalarKind,
}

pub(super) struct CompiledGraph {
    _module_owner: Rc<CompiledModule>,
    pub(super) function: GraphFn,
    pub(super) inputs: Box<[InputSpec]>,
    pub(super) output_kind: ScalarKind,
    pub(super) boundary_nodes: Box<[NodeId]>,
}

#[derive(Clone)]
pub(super) struct TemporalStateLayout {
    pub(super) len: usize,
    pub(super) nodes: Box<[TemporalNodeLayout]>,
}

#[derive(Clone)]
pub(super) enum TemporalNodeLayout {
    Delay {
        node: NodeId,
        kind: ScalarKind,
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
        last_bits: usize,
        last_tag: usize,
    },
}

struct CompiledModule {
    _module: JITModule,
}

pub(super) struct CompiledRun {
    _module: JITModule,
    pub(super) function: RunFn,
    pub(super) external_inputs: Box<[InputSpec]>,
    pub(super) outputs: Box<[(EnvironmentSlot, ScalarKind, usize)]>,
    pub(super) environment_len: usize,
}

pub(super) struct CompiledTemporalRun {
    _module: JITModule,
    pub(super) function: TemporalRunFn,
    pub(super) external_inputs: Box<[InputSpec]>,
    pub(super) outputs: Box<[(EnvironmentSlot, ScalarKind, usize)]>,
    pub(super) states: Box<[TemporalRunStateLayout]>,
    pub(super) state_len: usize,
    pub(super) environment_len: usize,
}

#[derive(Clone)]
pub(super) struct TemporalRunStateLayout {
    pub(super) stream: usize,
    pub(super) offset: usize,
    pub(super) layout: TemporalStateLayout,
}

pub(super) fn decode(value: i64, kind: ScalarKind) -> Value {
    match kind {
        ScalarKind::Int => Value::Int(value),
        ScalarKind::Bool => Value::Bool(value != 0),
        ScalarKind::Float => Value::Float(f64::from_bits(value as u64)),
    }
}

#[derive(Clone, Copy)]
enum ScalarRef {
    Constant { bits: i64, kind: ScalarKind },
    Input { index: u32, kind: ScalarKind },
    Node { index: u32, kind: ScalarKind },
}

struct LoweredGraph {
    nodes: Vec<LoweredNode>,
    output: ScalarRef,
    output_kind: ScalarKind,
}

enum LoweredNode {
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
    },
    If {
        condition: ScalarRef,
        then_graph: Box<LoweredGraph>,
        else_graph: Box<LoweredGraph>,
    },
}

pub(super) struct LoweredProgram {
    graph: LoweredGraph,
    inputs: Vec<InputSpec>,
    boundary_nodes: Vec<NodeId>,
    temporal: Option<TemporalProgram>,
}

struct TemporalProgram {
    ops: Vec<TemporalOp>,
    state: TemporalStateLayout,
}

enum TemporalOp {
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
enum TemporalSource {
    Constant { bits: i64, kind: ScalarKind },
    Input { index: u32 },
    Output,
}

#[derive(Default)]
pub(super) struct Lowering {
    inputs: Vec<InputSpec>,
    input_indices: BTreeMap<InputSource, u32>,
    boundary_nodes: Vec<NodeId>,
}

impl Lowering {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn lower_scalar(self, canonical: &BoundEvaluationGraph) -> Option<LoweredProgram> {
        self.lower_program(canonical, None)
    }

    pub(super) fn lower_temporal_run(
        self,
        canonical: &BoundEvaluationGraph,
        temporal_plan: &TemporalPlan,
    ) -> Option<LoweredProgram> {
        self.lower_program(canonical, Some(temporal_plan))
    }

    fn lower_program(
        mut self,
        canonical: &BoundEvaluationGraph,
        temporal_plan: Option<&TemporalPlan>,
    ) -> Option<LoweredProgram> {
        let output_kind = infer_output_kind(canonical)?;
        let graph = self.lower_graph(canonical, output_kind)?;
        self.boundary_nodes.sort_unstable();
        self.boundary_nodes.dedup();
        let temporal = if self.boundary_nodes.is_empty() {
            None
        } else if let Some(temporal_plan) = temporal_plan {
            Some(self.lower_temporal(canonical, output_kind, temporal_plan)?)
        } else {
            None
        };
        Some(LoweredProgram {
            graph,
            inputs: self.inputs,
            boundary_nodes: self.boundary_nodes,
            temporal,
        })
    }

    fn lower_graph(
        &mut self,
        graph: &BoundEvaluationGraph,
        expected_output: ScalarKind,
    ) -> Option<LoweredGraph> {
        if graph.is_fallible() {
            return None;
        }

        let mut nodes = Vec::with_capacity(graph.nodes.len());
        // Maps canonical node IDs to optimized references. Constant-folded canonical nodes do
        // not need a native node at all, so this is deliberately not just a kind table.
        let mut node_refs = Vec::with_capacity(graph.nodes.len());
        for (canonical_index, (op, signature)) in
            graph.nodes.iter().zip(&graph.scalar_signatures).enumerate()
        {
            if matches!(
                op,
                StreamOp::Delay { .. } | StreamOp::RecursiveDelay { .. } | StreamOp::Default { .. }
            ) && boundary_operands_are_available(op, &node_refs)
            {
                let node = NodeId::new(canonical_index);
                self.boundary_nodes.push(node);
                node_refs.push(None);
                continue;
            }
            let (node, output_kind) = match (op, signature) {
                (StreamOp::Unary { op, arg }, Some(ScalarSignature::Unary { input, output }))
                    if supported_kind(*input)
                        && supported_kind(*output)
                        && supported_unary(*op, *input, *output) =>
                {
                    (
                        LoweredNode::Unary {
                            op: *op,
                            arg: self.lower_ref(arg, *input, &node_refs)?,
                            input_kind: *input,
                            output_kind: *output,
                        },
                        *output,
                    )
                }
                (
                    StreamOp::Binary { op, lhs, rhs },
                    Some(ScalarSignature::Binary {
                        left,
                        right,
                        output,
                    }),
                ) if supported_kind(*left)
                    && supported_kind(*right)
                    && supported_kind(*output)
                    && supported_binary(*op, *left, *right, *output) =>
                {
                    (
                        LoweredNode::Binary {
                            op: *op,
                            lhs: self.lower_ref(lhs, *left, &node_refs)?,
                            rhs: self.lower_ref(rhs, *right, &node_refs)?,
                            left_kind: *left,
                            right_kind: *right,
                            output_kind: *output,
                        },
                        *output,
                    )
                }
                (
                    StreamOp::If {
                        cond,
                        then_branch,
                        else_branch,
                    },
                    _,
                ) => {
                    // Boundary node ids are local to their enclosing graph. Keep temporal
                    // operations in nested branches on the canonical path until lowering has a
                    // branch-local representation for those boundaries.
                    if then_branch.has_temporal_state() || else_branch.has_temporal_state() {
                        return None;
                    }
                    let then_kind = infer_output_kind(then_branch)?;
                    let else_kind = infer_output_kind(else_branch)?;
                    if then_kind != else_kind || !supported_kind(then_kind) {
                        return None;
                    }
                    (
                        LoweredNode::If {
                            condition: self.lower_ref(cond, ScalarKind::Bool, &node_refs)?,
                            then_graph: Box::new(self.lower_graph(then_branch, then_kind)?),
                            else_graph: Box::new(self.lower_graph(else_branch, else_kind)?),
                        },
                        then_kind,
                    )
                }
                _ => return None,
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
            node_refs.push(Some(reference));
        }

        let output = self.lower_ref(&graph.output, expected_output, &node_refs)?;
        Some(LoweredGraph {
            nodes,
            output,
            output_kind: expected_output,
        })
    }

    fn lower_ref(
        &mut self,
        reference: &BoundRef,
        expected: ScalarKind,
        node_refs: &[Option<ScalarRef>],
    ) -> Option<ScalarRef> {
        match reference {
            BoundRef::Const(Value::Int(value)) if expected == ScalarKind::Int => {
                Some(ScalarRef::Constant {
                    bits: *value,
                    kind: expected,
                })
            }
            BoundRef::Const(Value::Float(value)) if expected == ScalarKind::Float => {
                Some(ScalarRef::Constant {
                    bits: value.to_bits() as i64,
                    kind: expected,
                })
            }
            BoundRef::Const(Value::Bool(value)) if expected == ScalarKind::Bool => {
                Some(ScalarRef::Constant {
                    bits: i64::from(*value),
                    kind: expected,
                })
            }
            BoundRef::Const(_) => None,
            BoundRef::Node(node) => match node_refs.get(node.index()).copied()? {
                Some(reference) if reference.kind() == expected => Some(reference),
                Some(_) => None,
                None => self.lower_input(InputSource::Node(*node), expected),
            },
            BoundRef::External(slot) => self.lower_input(InputSource::External(*slot), expected),
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

fn boundary_operands_are_available(op: &BoundOp, node_refs: &[Option<ScalarRef>]) -> bool {
    let mut available = true;
    op.for_each_operand(|operand| {
        if let BoundRef::Node(node) = operand
            && node_refs.get(node.index()).is_some_and(Option::is_some)
        {
            available = false;
        }
    });
    available
}

impl ScalarRef {
    fn kind(self) -> ScalarKind {
        match self {
            Self::Constant { kind, .. } | Self::Input { kind, .. } | Self::Node { kind, .. } => {
                kind
            }
        }
    }

    fn int_constant(self) -> Option<i64> {
        match self {
            Self::Constant {
                bits,
                kind: ScalarKind::Int,
            } => Some(bits),
            _ => None,
        }
    }
}

/// Fold operations whose result is independent of runtime inputs. Checked integer operations are
/// folded only when they do not overflow, preserving the native side-exit in every other case.
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
            (UnaryOperator::Negate, ScalarKind::Int, ScalarKind::Int) => bits
                .checked_neg()
                .map(|value| constant(value, ScalarKind::Int)),
            (UnaryOperator::Absolute, ScalarKind::Int, ScalarKind::Int) => bits
                .checked_abs()
                .map(|value| constant(value, ScalarKind::Int)),
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
                Op::Add => lhs.checked_add(*rhs),
                Op::Subtract => lhs.checked_sub(*rhs),
                Op::Multiply => lhs.checked_mul(*rhs),
                Op::Divide => lhs.checked_div(*rhs),
                Op::Modulo => lhs.checked_rem(*rhs),
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
    let Some(combined) = inner_constant.checked_add(outer_constant) else {
        return node;
    };
    *lhs = base;
    *rhs = ScalarRef::Constant {
        bits: combined,
        kind: ScalarKind::Int,
    };
    node
}

fn supported_kind(kind: ScalarKind) -> bool {
    matches!(kind, ScalarKind::Int | ScalarKind::Float | ScalarKind::Bool)
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
        Op::Add | Op::Subtract | Op::Multiply | Op::Divide => {
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

pub(super) fn compile_scalar_graphs(
    programs: Vec<Option<LoweredProgram>>,
) -> Result<Vec<Option<Rc<CompiledGraph>>>, String> {
    let result_len = programs.len();
    if programs.iter().all(Option::is_none) {
        return Ok(vec![None; result_len]);
    }
    let mut flag_builder = settings::builder();
    flag_builder
        .set("opt_level", "speed")
        .map_err(|error| error.to_string())?;
    let isa = cranelift_native::builder()
        .map_err(|error| error.to_string())?
        .finish(settings::Flags::new(flag_builder))
        .map_err(|error| error.to_string())?;
    let mut module = JITModule::new(JITBuilder::with_isa(
        isa,
        cranelift_module::default_libcall_names(),
    ));

    let mut defined = Vec::new();
    for (index, program) in programs.into_iter().enumerate() {
        let Some(program) = program else {
            continue;
        };
        debug_assert!(program.temporal.is_none());
        let function_id = define_graph_function(&mut module, &program, index)?;
        defined.push((index, function_id, program));
    }
    module
        .finalize_definitions()
        .map_err(|error| error.to_string())?;
    let functions = defined
        .iter()
        .map(|(_, function_id, _)| module.get_finalized_function(*function_id))
        .collect::<Vec<_>>();
    let module = Rc::new(CompiledModule { _module: module });
    let mut result = vec![None; result_len];
    for ((index, _, program), function) in defined.into_iter().zip(functions) {
        // SAFETY: `define_graph_function` declares exactly `GraphFn`, and the shared module
        // remains alive through every `CompiledGraph` that owns one of its pointers.
        let function = unsafe { std::mem::transmute::<*const u8, GraphFn>(function) };
        result[index] = Some(Rc::new(CompiledGraph {
            _module_owner: Rc::clone(&module),
            function,
            inputs: program.inputs.into_boxed_slice(),
            output_kind: program.graph.output_kind,
            boundary_nodes: program.boundary_nodes.into_boxed_slice(),
        }));
    }
    Ok(result)
}

fn define_graph_function(
    module: &mut JITModule,
    program: &LoweredProgram,
    index: usize,
) -> Result<FuncId, String> {
    let frontend_config = module.target_config();
    let pointer_type = frontend_config.pointer_type();
    let mut signature = module.make_signature();
    signature.params.push(AbiParam::new(pointer_type));
    signature.params.push(AbiParam::new(pointer_type));
    signature.returns.push(AbiParam::new(types::I32));
    let function_id = module
        .declare_function(
            &format!("dsrv_jitted_graph_{index}"),
            Linkage::Export,
            &signature,
        )
        .map_err(|error| error.to_string())?;

    let mut context = module.make_context();
    context.func.signature = signature;
    let mut function_builder_context = FunctionBuilderContext::new();
    {
        let mut builder = FunctionBuilder::new(&mut context.func, &mut function_builder_context);
        let entry = builder.create_block();
        let fallback = builder.create_block();
        builder.append_block_params_for_function_params(entry);
        builder.switch_to_block(entry);
        builder.seal_block(entry);
        let inputs = builder.block_params(entry)[0];
        let output = builder.block_params(entry)[1];

        // Materialize each external input once. Besides producing smaller CLIF for graphs that
        // reuse an input, this avoids depending on alias analysis to discover that the output
        // store cannot invalidate earlier input loads.
        let input_values = program
            .inputs
            .iter()
            .enumerate()
            .map(|(index, input)| NativeValue {
                value: builder.ins().load(
                    native_type(input.kind),
                    MemFlagsData::trusted(),
                    inputs,
                    (index as i32) * 8,
                ),
                kind: input.kind,
            })
            .collect();

        let value = GraphCodegen {
            builder: &mut builder,
            input_values,
            fallback,
        }
        .compile(&program.graph);
        builder
            .ins()
            .store(MemFlagsData::trusted(), value.value, output, 0);
        let ok = builder.ins().iconst(types::I32, STATUS_OK as i64);
        builder.ins().return_(&[ok]);

        builder.switch_to_block(fallback);
        builder.seal_block(fallback);
        let fallback_status = builder.ins().iconst(types::I32, STATUS_FALLBACK as i64);
        builder.ins().return_(&[fallback_status]);
        builder.finalize(frontend_config);
    }

    module
        .define_function(function_id, &mut context)
        .map_err(|error| error.to_string())?;
    module.clear_context(&mut context);
    Ok(function_id)
}

#[derive(Clone, Copy)]
struct TemporalValue {
    value: NativeValue,
    available: ir::Value,
}

fn read_temporal_source(
    builder: &mut FunctionBuilder<'_>,
    source: TemporalSource,
    inputs: &[Option<NativeValue>],
    output: Option<NativeValue>,
) -> NativeValue {
    match source {
        TemporalSource::Constant { bits, kind } => NativeValue {
            value: match kind {
                ScalarKind::Float => builder.ins().f64const(Ieee64::with_bits(bits as u64)),
                ScalarKind::Int | ScalarKind::Bool => builder.ins().iconst(types::I64, bits),
            },
            kind,
        },
        TemporalSource::Input { index } => inputs[index as usize].unwrap(),
        TemporalSource::Output => output.unwrap(),
    }
}

fn load_i64(builder: &mut FunctionBuilder<'_>, base: ir::Value, index: usize) -> ir::Value {
    builder.ins().load(
        types::I64,
        MemFlagsData::trusted(),
        base,
        (index as i32) * 8,
    )
}

fn store_i64(builder: &mut FunctionBuilder<'_>, base: ir::Value, index: usize, value: ir::Value) {
    builder
        .ins()
        .store(MemFlagsData::trusted(), value, base, (index as i32) * 8);
}

fn store_native(
    builder: &mut FunctionBuilder<'_>,
    base: ir::Value,
    index: usize,
    value: NativeValue,
) {
    builder.ins().store(
        MemFlagsData::trusted(),
        value.value,
        base,
        (index as i32) * 8,
    );
}

/// Compiles one complete scheduler-produced plan. Scalar results remain in the raw environment
/// between streams, and every temporal write is sunk past every checked operation in the plan so a
/// side exit observes the pre-tick state.
pub(super) fn compile_temporal_run(
    plan: &ScheduledExecutionPlan,
) -> Result<Option<CompiledTemporalRun>, String> {
    if !plan.is_infallible() || !plan.has_temporal_state() {
        return Ok(None);
    }
    let mut lowered = Vec::with_capacity(plan.streams.len());
    let mut state_len = 0usize;
    let mut states = Vec::new();
    for planned in plan.streams.iter() {
        let Some(program) =
            Lowering::new().lower_temporal_run(&planned.program.graph, &planned.temporal)
        else {
            return Ok(None);
        };
        if let Some(temporal) = &program.temporal {
            states.push(TemporalRunStateLayout {
                stream: planned.stream.index(),
                offset: state_len,
                layout: temporal.state.clone(),
            });
            state_len = state_len
                .checked_add(temporal.state.len)
                .ok_or_else(|| "native temporal state layout overflow".to_owned())?;
        }
        lowered.push((planned, program));
    }
    if states.is_empty()
        || !plan
            .commit_streams
            .iter()
            .all(|stream| states.iter().any(|layout| layout.stream == stream.index()))
    {
        return Ok(None);
    }

    let mut external_inputs = BTreeMap::<EnvironmentSlot, ScalarKind>::new();
    for (_, program) in &lowered {
        for input in &program.inputs {
            let InputSource::External(slot) = input.source else {
                continue;
            };
            if plan.stream_slots.stream(slot).is_none() {
                if let Some(previous) = external_inputs.insert(slot, input.kind)
                    && previous != input.kind
                {
                    return Ok(None);
                }
            }
        }
    }

    let mut flag_builder = settings::builder();
    flag_builder
        .set("opt_level", "speed")
        .map_err(|error| error.to_string())?;
    let isa = cranelift_native::builder()
        .map_err(|error| error.to_string())?
        .finish(settings::Flags::new(flag_builder))
        .map_err(|error| error.to_string())?;
    let mut module = JITModule::new(JITBuilder::with_isa(
        isa,
        cranelift_module::default_libcall_names(),
    ));
    let frontend_config = module.target_config();
    let pointer_type = frontend_config.pointer_type();
    let mut signature = module.make_signature();
    signature.params.push(AbiParam::new(pointer_type));
    signature.params.push(AbiParam::new(pointer_type));
    signature.returns.push(AbiParam::new(types::I32));
    let function_id = module
        .declare_function(
            "dsrv_jitted_scheduled_temporal_plan",
            Linkage::Export,
            &signature,
        )
        .map_err(|error| error.to_string())?;
    let mut context = module.make_context();
    context.func.signature = signature;
    let mut function_builder_context = FunctionBuilderContext::new();
    {
        let mut builder = FunctionBuilder::new(&mut context.func, &mut function_builder_context);
        let entry = builder.create_block();
        let fallback = builder.create_block();
        builder.append_block_params_for_function_params(entry);
        builder.switch_to_block(entry);
        builder.seal_block(entry);
        let environment = builder.block_params(entry)[0];
        let state = builder.block_params(entry)[1];
        let mut pending = Vec::new();
        let mut next_state_offset = 0usize;

        for (planned, program) in &lowered {
            let mut input_values = program
                .inputs
                .iter()
                .map(|input| match input.source {
                    InputSource::External(slot) => Some(NativeValue {
                        value: builder.ins().load(
                            native_type(input.kind),
                            MemFlagsData::trusted(),
                            environment,
                            (slot.index() as i32) * 8,
                        ),
                        kind: input.kind,
                    }),
                    InputSource::Node(_) => None,
                })
                .collect::<Vec<_>>();
            let mut temporal_values = vec![
                None;
                program
                    .boundary_nodes
                    .last()
                    .map_or(0, |node| node.index() + 1)
            ];
            let state_offset = next_state_offset;
            if let Some(temporal) = &program.temporal {
                next_state_offset += temporal.state.len;
                emit_temporal_reads(
                    &mut builder,
                    state,
                    state_offset,
                    program,
                    temporal,
                    &mut input_values,
                    &mut temporal_values,
                )?;
            }
            let input_values = input_values
                .into_iter()
                .collect::<Option<Vec<_>>>()
                .expect("scheduled temporal lowering supplies every scalar input");
            let temporal_inputs = input_values.clone();
            let value = GraphCodegen {
                builder: &mut builder,
                input_values,
                fallback,
            }
            .compile(&program.graph);
            builder.ins().store(
                MemFlagsData::trusted(),
                value.value,
                environment,
                (planned.output.environment().index() as i32) * 8,
            );
            if let Some(temporal) = &program.temporal {
                pending.push(PendingTemporalCommit {
                    temporal,
                    state_offset,
                    temporal_values,
                    temporal_inputs,
                    output: value,
                });
            }
        }

        // The plan's logical commit barrier is lowered here. Nothing before this point mutates
        // externally owned temporal state, so failure in any stream remains tick-atomic.
        for commit in pending {
            emit_temporal_commits(&mut builder, state, commit);
        }
        let ok = builder.ins().iconst(types::I32, STATUS_OK as i64);
        builder.ins().return_(&[ok]);
        builder.switch_to_block(fallback);
        builder.seal_block(fallback);
        let fallback_status = builder.ins().iconst(types::I32, STATUS_FALLBACK as i64);
        builder.ins().return_(&[fallback_status]);
        builder.finalize(frontend_config);
    }
    module
        .define_function(function_id, &mut context)
        .map_err(|error| error.to_string())?;
    module.clear_context(&mut context);
    module
        .finalize_definitions()
        .map_err(|error| error.to_string())?;
    let function = module.get_finalized_function(function_id);
    // SAFETY: the function is declared with exactly `TemporalRunFn`'s two-pointer ABI and remains
    // alive in the returned module.
    let function = unsafe { std::mem::transmute::<*const u8, TemporalRunFn>(function) };
    let external_inputs = external_inputs
        .into_iter()
        .map(|(slot, kind)| InputSpec {
            source: InputSource::External(slot),
            kind,
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let outputs = lowered
        .iter()
        .map(|(planned, program)| {
            (
                planned.output.environment(),
                program.graph.output_kind,
                planned.stream.index(),
            )
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Ok(Some(CompiledTemporalRun {
        _module: module,
        function,
        external_inputs,
        outputs,
        states: states.into_boxed_slice(),
        state_len,
        environment_len: plan.environment_len,
    }))
}

struct PendingTemporalCommit<'a> {
    temporal: &'a TemporalProgram,
    state_offset: usize,
    temporal_values: Vec<Option<TemporalValue>>,
    temporal_inputs: Vec<NativeValue>,
    output: NativeValue,
}

fn emit_temporal_reads(
    builder: &mut FunctionBuilder<'_>,
    state: ir::Value,
    state_offset: usize,
    program: &LoweredProgram,
    temporal: &TemporalProgram,
    input_values: &mut [Option<NativeValue>],
    temporal_values: &mut Vec<Option<TemporalValue>>,
) -> Result<(), String> {
    for operation in &temporal.ops {
        match operation {
            TemporalOp::Delay {
                node,
                kind,
                cursor,
                filled,
                cells,
                len,
                ..
            } => {
                let cursor_value = load_i64(builder, state, state_offset + *cursor);
                let filled_value = load_i64(builder, state, state_offset + *filled);
                let available = builder.ins().icmp_imm_s(
                    IntCC::SignedGreaterThanOrEqual,
                    filled_value,
                    *len as i64,
                );
                let byte_offset = builder.ins().imul_imm_s(cursor_value, 8);
                let cells_offset = builder
                    .ins()
                    .iconst(types::I64, ((state_offset + *cells) * 8) as i64);
                let address = builder.ins().iadd(state, cells_offset);
                let address = builder.ins().iadd(address, byte_offset);
                let value =
                    builder
                        .ins()
                        .load(native_type(*kind), MemFlagsData::trusted(), address, 0);
                temporal_values.resize(temporal_values.len().max(node.index() + 1), None);
                temporal_values[node.index()] = Some(TemporalValue {
                    value: NativeValue { value, kind: *kind },
                    available,
                });
            }
            TemporalOp::Default {
                node,
                kind,
                input,
                fallback,
                ..
            } => {
                let delayed = temporal_values[input.index()]
                    .expect("temporal plan orders a delay before its default");
                let fallback = read_temporal_source(builder, *fallback, input_values, None);
                let value =
                    builder
                        .ins()
                        .select(delayed.available, delayed.value.value, fallback.value);
                let available = builder.ins().iconst(types::I8, 1);
                temporal_values.resize(temporal_values.len().max(node.index() + 1), None);
                temporal_values[node.index()] = Some(TemporalValue {
                    value: NativeValue { value, kind: *kind },
                    available,
                });
            }
        }
    }
    for (index, input) in program.inputs.iter().enumerate() {
        if let InputSource::Node(node) = input.source {
            let value = temporal_values[node.index()]
                .ok_or_else(|| "scheduled temporal input was not lowered".to_owned())?
                .value;
            input_values[index] = Some(value);
        }
    }
    Ok(())
}

fn emit_temporal_commits(
    builder: &mut FunctionBuilder<'_>,
    state: ir::Value,
    pending: PendingTemporalCommit<'_>,
) {
    let inputs = pending
        .temporal_inputs
        .iter()
        .copied()
        .map(Some)
        .collect::<Vec<_>>();
    for operation in &pending.temporal.ops {
        match operation {
            TemporalOp::Delay {
                kind,
                source,
                recursive,
                cursor,
                filled,
                last_bits,
                last_tag,
                cells,
                len,
                node,
            } => {
                let delayed = pending.temporal_values[node.index()].unwrap();
                if !recursive {
                    store_native(
                        builder,
                        state,
                        pending.state_offset + *last_bits,
                        delayed.value,
                    );
                    let concrete = builder.ins().iconst(types::I64, 2);
                    let deferred = builder.ins().iconst(types::I64, 1);
                    let tag = builder.ins().select(delayed.available, concrete, deferred);
                    store_i64(builder, state, pending.state_offset + *last_tag, tag);
                }
                let source = read_temporal_source(builder, *source, &inputs, Some(pending.output));
                let cursor_index = pending.state_offset + *cursor;
                let cursor_value = load_i64(builder, state, cursor_index);
                let byte_offset = builder.ins().imul_imm_s(cursor_value, 8);
                let cells_offset = builder
                    .ins()
                    .iconst(types::I64, ((pending.state_offset + *cells) * 8) as i64);
                let address = builder.ins().iadd(state, cells_offset);
                let address = builder.ins().iadd(address, byte_offset);
                builder
                    .ins()
                    .store(MemFlagsData::trusted(), source.value, address, 0);
                let next_cursor = builder.ins().iadd_imm_s(cursor_value, 1);
                let wraps = builder
                    .ins()
                    .icmp_imm_s(IntCC::Equal, next_cursor, *len as i64);
                let zero = builder.ins().iconst(types::I64, 0);
                let next_cursor = builder.ins().select(wraps, zero, next_cursor);
                store_i64(builder, state, cursor_index, next_cursor);
                let filled_index = pending.state_offset + *filled;
                let filled_value = load_i64(builder, state, filled_index);
                let incremented = builder.ins().iadd_imm_s(filled_value, 1);
                let full = builder.ins().icmp_imm_s(
                    IntCC::SignedGreaterThanOrEqual,
                    incremented,
                    *len as i64,
                );
                let capacity = builder.ins().iconst(types::I64, *len as i64);
                let next_filled = builder.ins().select(full, capacity, incremented);
                store_i64(builder, state, filled_index, next_filled);
                debug_assert_eq!(source.kind, *kind);
            }
            TemporalOp::Default {
                input,
                last_bits,
                last_tag,
                ..
            } => {
                let delayed = pending.temporal_values[input.index()].unwrap();
                store_native(
                    builder,
                    state,
                    pending.state_offset + *last_bits,
                    delayed.value,
                );
                let concrete = builder.ins().iconst(types::I64, 2);
                let deferred = builder.ins().iconst(types::I64, 1);
                let tag = builder.ins().select(delayed.available, concrete, deferred);
                store_i64(builder, state, pending.state_offset + *last_tag, tag);
            }
        }
    }
}

pub(super) fn compile_run(
    graphs: &[(&BoundEvaluationGraph, EnvironmentSlot, usize)],
    stream_slots: StreamSlots,
) -> Result<Option<CompiledRun>, String> {
    if graphs.is_empty() {
        return Ok(None);
    }
    let mut lowered = Vec::with_capacity(graphs.len());
    for &(graph, output_slot, stream) in graphs {
        let Some(program) = Lowering::new().lower_scalar(graph) else {
            return Ok(None);
        };
        if !program.boundary_nodes.is_empty()
            || program
                .inputs
                .iter()
                .any(|input| matches!(input.source, InputSource::Node(_)))
        {
            return Ok(None);
        }
        lowered.push((program, output_slot, stream));
    }

    let mut external_inputs = BTreeMap::<EnvironmentSlot, ScalarKind>::new();
    for (program, _, _) in &lowered {
        for input in &program.inputs {
            let InputSource::External(slot) = input.source else {
                unreachable!()
            };
            if stream_slots.stream(slot).is_none() {
                if let Some(previous) = external_inputs.insert(slot, input.kind)
                    && previous != input.kind
                {
                    return Ok(None);
                }
            }
        }
    }

    let mut flag_builder = settings::builder();
    flag_builder
        .set("opt_level", "speed")
        .map_err(|error| error.to_string())?;
    let isa = cranelift_native::builder()
        .map_err(|error| error.to_string())?
        .finish(settings::Flags::new(flag_builder))
        .map_err(|error| error.to_string())?;
    let mut module = JITModule::new(JITBuilder::with_isa(
        isa,
        cranelift_module::default_libcall_names(),
    ));
    let frontend_config = module.target_config();
    let pointer_type = frontend_config.pointer_type();
    let mut signature = module.make_signature();
    signature.params.push(AbiParam::new(pointer_type));
    signature.returns.push(AbiParam::new(types::I32));
    let function_id = module
        .declare_function("dsrv_jitted_scalar_run", Linkage::Export, &signature)
        .map_err(|error| error.to_string())?;
    let mut context = module.make_context();
    context.func.signature = signature;
    let mut function_builder_context = FunctionBuilderContext::new();
    {
        let mut builder = FunctionBuilder::new(&mut context.func, &mut function_builder_context);
        let entry = builder.create_block();
        let fallback = builder.create_block();
        builder.append_block_params_for_function_params(entry);
        builder.switch_to_block(entry);
        builder.seal_block(entry);
        let environment = builder.block_params(entry)[0];

        for (program, output_slot, _) in &lowered {
            let input_values = program
                .inputs
                .iter()
                .map(|input| {
                    let InputSource::External(slot) = input.source else {
                        unreachable!()
                    };
                    NativeValue {
                        value: builder.ins().load(
                            native_type(input.kind),
                            MemFlagsData::trusted(),
                            environment,
                            (slot.index() as i32) * 8,
                        ),
                        kind: input.kind,
                    }
                })
                .collect();
            let value = GraphCodegen {
                builder: &mut builder,
                input_values,
                fallback,
            }
            .compile(&program.graph);
            builder.ins().store(
                MemFlagsData::trusted(),
                value.value,
                environment,
                (output_slot.index() as i32) * 8,
            );
        }
        let ok = builder.ins().iconst(types::I32, STATUS_OK as i64);
        builder.ins().return_(&[ok]);
        builder.switch_to_block(fallback);
        builder.seal_block(fallback);
        let fallback_status = builder.ins().iconst(types::I32, STATUS_FALLBACK as i64);
        builder.ins().return_(&[fallback_status]);
        builder.finalize(frontend_config);
    }
    module
        .define_function(function_id, &mut context)
        .map_err(|error| error.to_string())?;
    module.clear_context(&mut context);
    module
        .finalize_definitions()
        .map_err(|error| error.to_string())?;
    let function = module.get_finalized_function(function_id);
    // SAFETY: the signature above is exactly `RunFn`; `module` is retained by `CompiledRun`.
    let function = unsafe { std::mem::transmute::<*const u8, RunFn>(function) };
    let outputs = lowered
        .iter()
        .map(|(program, slot, stream)| (*slot, program.graph.output_kind, *stream))
        .collect();
    let external_inputs = external_inputs
        .into_iter()
        .map(|(slot, kind)| InputSpec {
            source: InputSource::External(slot),
            kind,
        })
        .collect();
    Ok(Some(CompiledRun {
        _module: module,
        function,
        external_inputs,
        outputs,
        environment_len: stream_slots.start().index() + stream_slots.len(),
    }))
}

struct GraphCodegen<'a, 'b> {
    builder: &'a mut FunctionBuilder<'b>,
    input_values: Vec<NativeValue>,
    fallback: ir::Block,
}

#[derive(Clone, Copy)]
struct NativeValue {
    value: ir::Value,
    kind: ScalarKind,
}

impl GraphCodegen<'_, '_> {
    fn compile(&mut self, graph: &LoweredGraph) -> NativeValue {
        let mut values = vec![None; graph.nodes.len()];
        self.read(graph.output, graph, &mut values)
    }

    fn compile_node(
        &mut self,
        index: usize,
        graph: &LoweredGraph,
        values: &mut [Option<NativeValue>],
    ) -> NativeValue {
        if let Some(value) = values[index] {
            return value;
        }
        let value = match &graph.nodes[index] {
            LoweredNode::Unary {
                op,
                arg,
                input_kind,
                output_kind,
            } => {
                let arg = self.read(*arg, graph, values);
                debug_assert_eq!(arg.kind, *input_kind);
                let value = match (op, input_kind, output_kind) {
                    (UnaryOperator::Not, ScalarKind::Bool, ScalarKind::Bool) => {
                        self.builder.ins().bxor_imm_u(arg.value, 1)
                    }
                    (UnaryOperator::Negate, ScalarKind::Int, ScalarKind::Int) => {
                        let zero = self.builder.ins().iconst(types::I64, 0);
                        let (value, overflow) = self.builder.ins().ssub_overflow(zero, arg.value);
                        self.fallback_if(overflow);
                        value
                    }
                    (UnaryOperator::Absolute, ScalarKind::Int, ScalarKind::Int) => {
                        let is_min =
                            self.builder
                                .ins()
                                .icmp_imm_s(IntCC::Equal, arg.value, i64::MIN);
                        self.fallback_if(is_min);
                        self.builder.ins().iabs(arg.value)
                    }
                    (UnaryOperator::Negate, ScalarKind::Float, ScalarKind::Float) => {
                        self.builder.ins().fneg(arg.value)
                    }
                    (UnaryOperator::Absolute, ScalarKind::Float, ScalarKind::Float) => {
                        self.builder.ins().fabs(arg.value)
                    }
                    _ => unreachable!("lowering admitted an unsupported unary operation"),
                };
                NativeValue {
                    value,
                    kind: *output_kind,
                }
            }
            LoweredNode::Binary {
                op,
                lhs,
                rhs,
                left_kind,
                right_kind,
                output_kind,
            } => {
                let lhs = self.read(*lhs, graph, values);
                let rhs = self.read(*rhs, graph, values);
                debug_assert_eq!(lhs.kind, *left_kind);
                debug_assert_eq!(rhs.kind, *right_kind);
                self.binary(*op, lhs, rhs, *output_kind)
            }
            LoweredNode::If {
                condition,
                then_graph,
                else_graph,
            } => {
                let condition = self.read(*condition, graph, values);
                debug_assert_eq!(condition.kind, ScalarKind::Bool);
                // Canonical non-recursive `if` advances both branch graphs every tick.
                let then_value = self.compile(then_graph);
                let else_value = self.compile(else_graph);
                debug_assert_eq!(then_value.kind, else_value.kind);
                let condition = self
                    .builder
                    .ins()
                    .icmp_imm_s(IntCC::NotEqual, condition.value, 0);
                NativeValue {
                    value: self
                        .builder
                        .ins()
                        .select(condition, then_value.value, else_value.value),
                    kind: then_value.kind,
                }
            }
        };
        values[index] = Some(value);
        value
    }

    fn read(
        &mut self,
        reference: ScalarRef,
        graph: &LoweredGraph,
        nodes: &mut [Option<NativeValue>],
    ) -> NativeValue {
        match reference {
            ScalarRef::Constant { bits, kind } => NativeValue {
                value: match kind {
                    ScalarKind::Float => {
                        self.builder.ins().f64const(Ieee64::with_bits(bits as u64))
                    }
                    ScalarKind::Int | ScalarKind::Bool => {
                        self.builder.ins().iconst(types::I64, bits)
                    }
                },
                kind,
            },
            ScalarRef::Input { index, kind } => {
                let value = self.input_values[index as usize];
                debug_assert_eq!(value.kind, kind);
                value
            }
            ScalarRef::Node { index, kind } => {
                let value = self.compile_node(index as usize, graph, nodes);
                debug_assert_eq!(value.kind, kind);
                value
            }
        }
    }

    fn binary(
        &mut self,
        op: BinaryOperator,
        lhs: NativeValue,
        rhs: NativeValue,
        output_kind: ScalarKind,
    ) -> NativeValue {
        use BinaryOperator as Op;
        let value = if output_kind == ScalarKind::Float {
            let lhs = self.to_float(lhs);
            let rhs = self.to_float(rhs);
            match op {
                Op::Add => self.builder.ins().fadd(lhs, rhs),
                Op::Subtract => self.builder.ins().fsub(lhs, rhs),
                Op::Multiply => self.builder.ins().fmul(lhs, rhs),
                Op::Divide => self.builder.ins().fdiv(lhs, rhs),
                _ => unreachable!("lowering admitted an unsupported float operation"),
            }
        } else if matches!(
            op,
            Op::Equal | Op::Less | Op::LessEqual | Op::Greater | Op::GreaterEqual
        ) && (lhs.kind == ScalarKind::Float || rhs.kind == ScalarKind::Float)
        {
            let lhs = self.to_float(lhs);
            let rhs = self.to_float(rhs);
            let condition = match op {
                Op::Equal => FloatCC::Equal,
                Op::Less => FloatCC::LessThan,
                Op::LessEqual => FloatCC::LessThanOrEqual,
                Op::Greater => FloatCC::GreaterThan,
                Op::GreaterEqual => FloatCC::GreaterThanOrEqual,
                _ => unreachable!(),
            };
            self.compare_float(condition, lhs, rhs)
        } else {
            let lhs = lhs.value;
            let rhs = rhs.value;
            match op {
                Op::Add => {
                    let (value, overflow) = self.builder.ins().sadd_overflow(lhs, rhs);
                    self.fallback_if(overflow);
                    value
                }
                Op::Subtract => {
                    let (value, overflow) = self.builder.ins().ssub_overflow(lhs, rhs);
                    self.fallback_if(overflow);
                    value
                }
                Op::Multiply => {
                    let (value, overflow) = self.builder.ins().smul_overflow(lhs, rhs);
                    self.fallback_if(overflow);
                    value
                }
                Op::Divide | Op::Modulo => {
                    let zero = self.builder.ins().icmp_imm_s(IntCC::Equal, rhs, 0);
                    let min = self.builder.ins().iconst(types::I64, i64::MIN);
                    let lhs_is_min = self.builder.ins().icmp(IntCC::Equal, lhs, min);
                    let rhs_is_negative_one = self.builder.ins().icmp_imm_s(IntCC::Equal, rhs, -1);
                    let overflow = self.builder.ins().band(lhs_is_min, rhs_is_negative_one);
                    let invalid = self.builder.ins().bor(zero, overflow);
                    self.fallback_if(invalid);
                    if op == Op::Divide {
                        self.builder.ins().sdiv(lhs, rhs)
                    } else {
                        self.builder.ins().srem(lhs, rhs)
                    }
                }
                Op::And => self.builder.ins().band(lhs, rhs),
                Op::Or => self.builder.ins().bor(lhs, rhs),
                Op::Implication => {
                    let not_lhs = self.builder.ins().bxor_imm_u(lhs, 1);
                    self.builder.ins().bor(not_lhs, rhs)
                }
                Op::Equal => self.compare(IntCC::Equal, lhs, rhs),
                Op::Less => self.compare(IntCC::SignedLessThan, lhs, rhs),
                Op::LessEqual => self.compare(IntCC::SignedLessThanOrEqual, lhs, rhs),
                Op::Greater => self.compare(IntCC::SignedGreaterThan, lhs, rhs),
                Op::GreaterEqual => self.compare(IntCC::SignedGreaterThanOrEqual, lhs, rhs),
                Op::Concatenate => unreachable!("concatenation is not JIT eligible"),
            }
        };
        NativeValue {
            value,
            kind: output_kind,
        }
    }

    fn to_float(&mut self, value: NativeValue) -> ir::Value {
        match value.kind {
            ScalarKind::Float => value.value,
            ScalarKind::Int => self.builder.ins().fcvt_from_sint(types::F64, value.value),
            ScalarKind::Bool => unreachable!("booleans are not converted to floats"),
        }
    }

    fn compare(&mut self, condition: IntCC, lhs: ir::Value, rhs: ir::Value) -> ir::Value {
        let value = self.builder.ins().icmp(condition, lhs, rhs);
        self.builder.ins().uextend(types::I64, value)
    }

    fn compare_float(&mut self, condition: FloatCC, lhs: ir::Value, rhs: ir::Value) -> ir::Value {
        let value = self.builder.ins().fcmp(condition, lhs, rhs);
        self.builder.ins().uextend(types::I64, value)
    }

    fn fallback_if(&mut self, condition: ir::Value) {
        let next = self.builder.create_block();
        self.builder
            .ins()
            .brif(condition, self.fallback, &[], next, &[]);
        self.builder.switch_to_block(next);
        self.builder.seal_block(next);
    }
}

fn native_type(kind: ScalarKind) -> ir::Type {
    match kind {
        ScalarKind::Int | ScalarKind::Bool => types::I64,
        ScalarKind::Float => types::F64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::monitor::test_support::jit_artifact_count;
    use crate::dataflow::{DataflowMonitor, JitConfig, JitPlan};
    use crate::{CheckedDsrvSpecification, DsrvSpecification};

    fn compile_pair(source: &str) -> (DataflowMonitor, DataflowMonitor) {
        let checked = source
            .parse::<CheckedDsrvSpecification>()
            .expect("test specification should type check");
        let canonical = source
            .parse::<DsrvSpecification>()
            .expect("test specification should parse")
            .try_into()
            .expect("untyped monitor should compile");
        let jitted = DataflowMonitor::compile_checked_with_jit(checked, JitConfig::eager())
            .expect("checked monitor should compile");
        assert!(
            jit_artifact_count(&jitted) > 0,
            "test specification should contain at least one JIT-eligible graph"
        );
        (canonical, jitted)
    }

    fn assert_rows(source: &str, rows: &[Vec<Value>]) {
        let (mut canonical, mut jitted) = compile_pair(source);
        let mut canonical_output = vec![Value::NoVal; canonical.output_vars().len()];
        let mut jitted_output = canonical_output.clone();
        for row in rows {
            canonical.evaluate(row, &mut canonical_output).unwrap();
            jitted.evaluate(row, &mut jitted_output).unwrap();
            assert_eq!(jitted_output, canonical_output);
        }
    }

    fn assert_complete_temporal_kernel(source: &str) {
        let checked = source.parse::<CheckedDsrvSpecification>().unwrap();
        let monitor = DataflowMonitor::compile_checked_with_jit(checked, JitConfig::eager())
            .expect("temporal kernel should compile");
        assert_eq!(
            monitor
                .jit_report()
                .unwrap()
                .complete_temporal_kernel_streams(),
            [0]
        );
    }

    #[test]
    fn complete_arithmetic_graph_runs_natively() {
        assert_rows(
            "in x: Int\nin y: Int\nout result: Int\nresult = (x + y) * 3 - (x % 7)",
            &[
                vec![Value::Int(5), Value::Int(2)],
                vec![Value::Int(11), Value::Int(7)],
            ],
        );
    }

    #[test]
    fn hotness_activates_before_the_tick_after_the_threshold() {
        let checked = "in x: Int\nout result: Int\nresult = x + 1"
            .parse::<CheckedDsrvSpecification>()
            .expect("test specification should type check");
        let mut monitor =
            DataflowMonitor::compile_checked_with_jit(checked, JitConfig::after_events(2))
                .expect("checked monitor should compile");
        let mut output = [Value::NoVal];

        assert_eq!(jit_artifact_count(&monitor), 0);
        assert_eq!(monitor.jit_report().unwrap().plan(), JitPlan::Pending);
        monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();
        assert_eq!(jit_artifact_count(&monitor), 0);
        monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
        assert_eq!(jit_artifact_count(&monitor), 0);
        monitor.evaluate(&[Value::Int(3)], &mut output).unwrap();
        assert!(jit_artifact_count(&monitor) > 0);
        let report = monitor.jit_report().unwrap();
        assert_eq!(report.plan(), JitPlan::Fused);
        assert_eq!(report.compiled_artifacts(), 1);
        assert!(report.unsupported_streams().is_empty());
        assert!(report.scheduled_temporal_streams().is_empty());
        assert_eq!(report.backend_error(), None);
        assert_eq!(output, [Value::Int(4)]);
    }

    #[test]
    fn unsupported_streams_are_reported_and_interpreted() {
        let checked = "in x: Str\nout result: Str\nresult = x"
            .parse::<CheckedDsrvSpecification>()
            .expect("test specification should type check");
        let mut monitor = DataflowMonitor::compile_checked_with_jit(checked, JitConfig::eager())
            .expect("checked monitor should compile");
        let report = monitor.jit_report().unwrap();
        assert_eq!(report.plan(), JitPlan::Unavailable);
        assert_eq!(report.compiled_artifacts(), 0);
        assert_eq!(report.unsupported_streams(), [0]);
        assert!(report.scheduled_temporal_streams().is_empty());

        let mut output = [Value::NoVal];
        monitor
            .evaluate(&[Value::Str("hello".into())], &mut output)
            .unwrap();
        assert_eq!(output, [Value::Str("hello".into())]);
    }

    #[test]
    fn pure_conditional_graph_runs_natively() {
        assert_rows(
            "in x: Int\nin y: Int\nout result: Int\nresult = if x > 0 then y * 3 else x + y",
            &[
                vec![Value::Int(1), Value::Int(4)],
                vec![Value::Int(-2), Value::Int(9)],
            ],
        );
    }

    #[test]
    fn first_special_value_replays_lifting_state_before_fallback() {
        assert_rows(
            "in x: Int\nout result: Int\nresult = (x + 1) * 2",
            &[
                vec![Value::Int(3)],
                vec![Value::Int(8)],
                vec![Value::NoVal],
                vec![Value::Int(4)],
                vec![Value::NoVal],
            ],
        );
    }

    #[test]
    fn native_arithmetic_failure_returns_to_the_canonical_semantics() {
        let (mut canonical, mut jitted) =
            compile_pair("in x: Int\nout result: Int\nresult = x + 1");
        let row = [Value::Int(i64::MAX)];
        let mut output = [Value::NoVal];
        let canonical_failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            canonical.evaluate(&row, &mut output).unwrap()
        }));
        let jitted_failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            jitted.evaluate(&row, &mut output).unwrap()
        }));
        assert!(canonical_failure.is_err());
        assert!(jitted_failure.is_err());
    }

    #[test]
    fn dependent_streams_publish_native_results_to_the_environment() {
        assert_rows(
            "in x: Int\nout first: Int\nout second: Int\nfirst = x + 1\nsecond = first * 2",
            &[vec![Value::Int(3)], vec![Value::Int(8)]],
        );
    }

    #[test]
    fn floating_point_graph_runs_natively() {
        assert_rows(
            "in x: Float\nin y: Float\nout result: Bool\nresult = (x + y) * 1.5 > y",
            &[
                vec![Value::Float(2.0), Value::Float(4.0)],
                vec![Value::Float(-8.0), Value::Float(3.5)],
            ],
        );
    }

    #[test]
    fn mixed_numeric_graph_converts_integers_natively() {
        assert_rows(
            "in x: Int\nin y: Float\nout result: Float\nresult = x + y * 2.0",
            &[
                vec![Value::Int(2), Value::Float(4.25)],
                vec![Value::Int(-8), Value::Float(3.5)],
            ],
        );
    }

    #[test]
    fn floating_point_inputs_replay_before_special_value_fallback() {
        assert_rows(
            "in x: Float\nout result: Float\nresult = (x + 0.5) * 2.0",
            &[
                vec![Value::Float(3.25)],
                vec![Value::Float(-1.5)],
                vec![Value::NoVal],
                vec![Value::Float(8.0)],
            ],
        );
    }

    #[test]
    fn fixed_delays_and_defaults_run_as_one_complete_temporal_kernel() {
        let source = "in x: Int\nout result: Bool\nresult = x > 3 && default(x[1], 4) > 3 && default(x[2], 4) > 3";
        assert_complete_temporal_kernel(source);
        assert_rows(
            source,
            &[
                vec![Value::Int(5)],
                vec![Value::Int(6)],
                vec![Value::Int(7)],
                vec![Value::Int(1)],
                vec![Value::Int(9)],
                vec![Value::NoVal],
                vec![Value::Int(8)],
            ],
        );
    }

    #[test]
    fn recursive_delay_accumulator_runs_as_one_complete_temporal_kernel() {
        let source = "in x: Int\nout result: Int\nresult = default(result[1], 0) + x";
        assert_complete_temporal_kernel(source);
        assert_rows(
            source,
            &[
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(3)],
                vec![Value::Int(4)],
                vec![Value::NoVal],
                vec![Value::Int(5)],
            ],
        );
    }

    #[test]
    fn scheduler_plan_fuses_scalar_streams_around_temporal_state() {
        let source = "in x: Int\n\
            aux base: Int\n\
            aux delayed: Int\n\
            out result: Int\n\
            base = x + 1\n\
            delayed = default(base[1], 0) + base\n\
            result = delayed * 2";
        let checked = source.parse::<CheckedDsrvSpecification>().unwrap();
        let monitor = DataflowMonitor::compile_checked_with_jit(checked, JitConfig::eager())
            .expect("the complete scheduled plan should compile");
        let report = monitor.jit_report().unwrap();
        assert_eq!(report.plan(), JitPlan::Fused);
        assert_eq!(report.compiled_artifacts(), 1);
        assert_eq!(report.complete_temporal_kernel_streams(), [1]);
        assert_rows(
            source,
            &[
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(3)],
                vec![Value::NoVal],
                vec![Value::Int(5)],
            ],
        );
    }

    #[test]
    fn hot_activation_preserves_scheduled_temporal_state() {
        for source in [
            "in x: Int\nout result: Bool\nresult = x > 3 && default(x[1], 4) > 3 && default(x[2], 4) > 3",
            "in x: Int\nout result: Int\nresult = default(result[1], 0) + x",
        ] {
            let checked = source
                .parse::<CheckedDsrvSpecification>()
                .expect("test specification should type check");
            let canonical = DataflowMonitor::compile_checked(checked.clone()).unwrap();
            let jitted =
                DataflowMonitor::compile_checked_with_jit(checked, JitConfig::after_events(3))
                    .unwrap();
            let rows = [
                vec![Value::Int(5)],
                vec![Value::Int(6)],
                vec![Value::Int(7)],
                vec![Value::Int(8)],
                vec![Value::NoVal],
                vec![Value::Int(9)],
            ];
            let mut canonical = canonical;
            let mut jitted = jitted;
            let mut expected = [Value::NoVal];
            let mut actual = [Value::NoVal];
            for row in rows {
                canonical.evaluate(&row, &mut expected).unwrap();
                jitted.evaluate(&row, &mut actual).unwrap();
                assert_eq!(actual, expected);
            }
            assert!(
                jitted
                    .jit_report()
                    .expect("JIT should have activated")
                    .scheduled_temporal_streams()
                    .is_empty()
            );
            assert_eq!(
                jitted
                    .jit_report()
                    .unwrap()
                    .complete_temporal_kernel_streams(),
                [0]
            );
        }
    }
}
