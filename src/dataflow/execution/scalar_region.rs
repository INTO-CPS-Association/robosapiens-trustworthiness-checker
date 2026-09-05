//! Semantic discovery of the scalar regions an execution plan may quicken or compile.
//!
//! A region is either a contiguous run of whole streams in scheduler order, or one straight-line
//! island inside a single stream's otherwise canonical graph. Both are expressed as
//! [`ScalarProgram`]s, so quickening and native lowering derive their executor-specific artifacts
//! from the same backend-neutral representation. This module owns the legality rules; it owns no
//! register layout, no evaluator state, and no backend artifact.

use std::ops::Range;

use crate::core::{StreamType, Value};
use crate::dataflow::environment::{EnvironmentLayout, EnvironmentSlot};
use crate::dataflow::execution::scalar_ir::ScalarProgram;
use crate::dataflow::execution::scheduled_plan::{PlannedStream, ScheduledExecutionPlan};
use crate::dataflow::ir::{
    BoundEvaluationGraph, BoundOp, BoundRef, NodeId, ScalarKind, ScalarSignature, StreamProgram,
};
use crate::dataflow::stream_id::{StreamId, StreamSlots};

/// One backend-neutral scalar region owned by the execution plan.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::dataflow) enum ScalarRegion {
    /// A contiguous run of whole streams, each publishing to its own environment slot.
    Streams(Box<[ScalarRegionStream]>),
    /// Every scalar island of one stream's otherwise canonical graph.
    Graph(GraphRegion),
}

/// All islands of a single stream graph, in graph order.
///
/// They form one region rather than one region each so that a value produced by an earlier island
/// reaches a later one in a register instead of a round trip through the canonical node arena.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::dataflow) struct GraphRegion {
    pub(in crate::dataflow) stream: StreamId,
    pub(in crate::dataflow) node_count: usize,
    pub(in crate::dataflow) islands: Box<[GraphIsland]>,
}

#[derive(Clone, Debug, PartialEq)]
pub(in crate::dataflow) struct ScalarRegionStream {
    pub(in crate::dataflow) stream: StreamId,
    pub(in crate::dataflow) output: EnvironmentSlot,
    pub(in crate::dataflow) output_kind: ScalarKind,
    pub(in crate::dataflow) program: ScalarProgram,
}

/// A scalar island: a node range whose surrounding graph stays canonical.
///
/// `exports` are the island nodes whose values *canonical* execution still reads. Nodes consumed
/// only by later islands of the same region travel by register and are deliberately not exported.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::dataflow) struct GraphIsland {
    pub(in crate::dataflow) nodes: Range<usize>,
    pub(in crate::dataflow) program: ScalarProgram,
    pub(in crate::dataflow) exports: Box<[NodeId]>,
}

/// One step of a stream graph's execution: a region member, or a canonical node run.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::dataflow) enum GraphSegment {
    Island(usize),
    Canonical(Range<usize>),
}

impl ScalarRegion {
    /// Builds one region covering a complete plan, when the whole schedule is eligible.
    #[cfg(any(test, feature = "jit"))]
    pub(in crate::dataflow) fn new(plan: &ScheduledExecutionPlan) -> Option<Self> {
        if plan.has_source_barrier() || plan.streams.len() != plan.stream_slots.len() {
            return None;
        }
        Self::from_streams(plan, &plan.streams)
    }

    #[cfg(any(test, feature = "jit"))]
    pub(in crate::dataflow) fn from_streams(
        plan: &ScheduledExecutionPlan,
        planned_streams: &[PlannedStream],
    ) -> Option<Self> {
        let (end, region) = Self::from_stream_prefix(plan, planned_streams, |_| true)?;
        (end == planned_streams.len()).then_some(region)
    }

    /// Builds the longest eligible prefix, lowering each stream exactly once.
    pub(in crate::dataflow) fn from_stream_prefix(
        plan: &ScheduledExecutionPlan,
        planned_streams: &[PlannedStream],
        supported: impl Fn(&ScalarProgram) -> bool,
    ) -> Option<(usize, Self)> {
        if planned_streams.is_empty() {
            return None;
        }

        let mut streams = Vec::with_capacity(planned_streams.len());
        let mut output_kinds = vec![None; plan.stream_slots.len()];
        for planned in planned_streams {
            let stream = planned.stream;
            if stream.index() >= output_kinds.len() || output_kinds[stream.index()].is_some() {
                break;
            }
            let program = planned.program.as_ref();
            let graph = &program.graph;
            if graph.contains_reconfigurable_expression()
                || graph.has_temporal_state()
                || !planned.temporal.operations.is_empty()
                || !planned.temporal.commits.is_empty()
                || graph.nodes.len() != graph.scalar_signatures.len()
            {
                break;
            }
            let output = planned.output.environment();
            if output.index() >= plan.environment_len {
                break;
            }
            let Some(output_kind) = infer_output_kind(
                &graph.output,
                &graph.scalar_signatures,
                program.environment_layout.as_ref(),
                output,
                plan.stream_slots,
                &output_kinds,
            )
            .or_else(|| infer_graph_output_kind(graph)) else {
                break;
            };
            let Some(scalar) = ScalarProgram::from_bound_graph(graph, output_kind) else {
                break;
            };
            if !supported(&scalar) {
                break;
            }
            output_kinds[stream.index()] = Some(output_kind);
            streams.push(ScalarRegionStream {
                stream,
                output,
                output_kind,
                program: scalar,
            });
        }
        (!streams.is_empty()).then(|| {
            let end = streams.len();
            (end, Self::Streams(streams.into_boxed_slice()))
        })
    }

    #[cfg_attr(not(feature = "jit"), allow(dead_code))]
    pub(in crate::dataflow) fn streams(&self) -> &[ScalarRegionStream] {
        match self {
            Self::Streams(streams) => streams,
            Self::Graph(_) => &[],
        }
    }

    /// The whole-stream programs a native backend may compile. Islands compile no native code:
    /// they read and write the canonical arena, which the region ABI does not expose.
    #[cfg_attr(not(feature = "jit"), allow(dead_code))]
    pub(in crate::dataflow) fn programs(
        &self,
    ) -> impl Iterator<Item = (&ScalarProgram, EnvironmentSlot, usize)> + '_ {
        self.streams()
            .iter()
            .map(|stream| (&stream.program, stream.output, stream.stream.index()))
    }

    #[cfg_attr(not(feature = "jit"), allow(dead_code))]
    pub(in crate::dataflow) fn outputs(
        &self,
    ) -> impl Iterator<Item = (StreamId, EnvironmentSlot, ScalarKind)> + '_ {
        self.streams()
            .iter()
            .map(|stream| (stream.stream, stream.output, stream.output_kind))
    }
}

/// Splits one stream's graph into a scalar region of islands plus canonical node runs.
///
/// Typed straight-line operations form islands, and so do `Delay`, `RecursiveDelay` and `Default`
/// (see `is_island_node`); conditional, dynamic, collection, and every other canonical operation
/// splits them. Graph order is preserved exactly, so canonical nodes still observe island results
/// and islands still observe canonical results. `supported`
/// decides which candidate islands the executing engine can actually run, and is applied before
/// exports are computed: a rejected island's nodes become canonical consumers.
pub(in crate::dataflow) fn segment_stream_graph(
    stream: StreamId,
    program: &StreamProgram,
    supported: impl Fn(&ScalarProgram) -> bool,
) -> Option<(GraphRegion, Box<[GraphSegment]>)> {
    let graph = &program.graph;
    let node_count = graph.nodes.len();
    if !program.uses_static_evaluation() || node_count != graph.scalar_signatures.len() {
        return None;
    }

    let mut extracted = Vec::new();
    let mut index = 0;
    while index < node_count {
        if !is_island_node(graph, index) {
            index += 1;
            continue;
        }
        let start = index;
        while index < node_count && is_island_node(graph, index) {
            index += 1;
        }
        let nodes = start..index;
        if let Some(scalar) = ScalarProgram::from_bound_graph_island(graph, nodes.clone())
            .filter(|scalar| supported(scalar))
        {
            extracted.push((nodes, scalar));
        }
    }
    if extracted.is_empty() {
        return None;
    }

    let accepted = extracted
        .iter()
        .map(|(nodes, _)| nodes.clone())
        .collect::<Vec<_>>();
    let islands = extracted
        .into_iter()
        .map(|(nodes, program)| {
            let exports = exported_nodes(graph, &accepted, &nodes);
            GraphIsland {
                nodes,
                program,
                exports,
            }
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();

    let mut segments = Vec::with_capacity(islands.len() * 2 + 1);
    let mut next = 0;
    for (member, island) in islands.iter().enumerate() {
        if next < island.nodes.start {
            segments.push(GraphSegment::Canonical(next..island.nodes.start));
        }
        segments.push(GraphSegment::Island(member));
        next = island.nodes.end;
    }
    if next < node_count {
        segments.push(GraphSegment::Canonical(next..node_count));
    }

    Some((
        GraphRegion {
            stream,
            node_count,
            islands,
        },
        segments.into_boxed_slice(),
    ))
}

/// Whether a node may join an island.
///
/// Typed unary and binary operations qualify on their signature. Temporal operations qualify
/// structurally: they carry no signature, and `ScalarProgram` decides whether their kind resolves.
fn is_island_node(graph: &BoundEvaluationGraph, index: usize) -> bool {
    matches!(
        (&graph.nodes[index], &graph.scalar_signatures[index]),
        (BoundOp::Unary { .. }, Some(ScalarSignature::Unary { .. }))
            | (BoundOp::Binary { .. }, Some(ScalarSignature::Binary { .. }))
            | (
                BoundOp::Delay { .. } | BoundOp::RecursiveDelay { .. } | BoundOp::Default { .. },
                None,
            )
    )
}

/// The island nodes whose values canonical execution still reads.
///
/// `BoundRef::Node` only ever names a node of its own graph, so scanning every operand of every
/// node outside *every* island, plus the graph output, is a complete canonical consumer set.
/// Staged recursive delays and temporal commits read those same operands. Nodes consumed only by
/// another island of the same region are omitted: those values travel by register.
fn exported_nodes(
    graph: &BoundEvaluationGraph,
    islands: &[Range<usize>],
    nodes: &Range<usize>,
) -> Box<[NodeId]> {
    let mut exported = vec![false; nodes.len()];
    let mut mark = |reference: &BoundRef| {
        if let BoundRef::Node(node) = reference
            && nodes.contains(&node.index())
        {
            exported[node.index() - nodes.start] = true;
        }
    };
    for (index, operation) in graph.nodes.iter().enumerate() {
        // The tick commit barrier re-reads a positive delay's input from the canonical arena after
        // evaluation, so that operand must be published even when the delay itself is an island
        // instruction.
        if matches!(operation, BoundOp::Delay { offset, .. } if *offset > 0) {
            operation.for_each_operand(&mut mark);
            continue;
        }
        if !islands.iter().any(|island| island.contains(&index)) {
            operation.for_each_operand(&mut mark);
        }
    }
    mark(&graph.output);
    exported
        .into_iter()
        .enumerate()
        .filter(|(_, exported)| *exported)
        .map(|(offset, _)| NodeId::new(nodes.start + offset))
        .collect()
}

fn infer_graph_output_kind(graph: &BoundEvaluationGraph) -> Option<ScalarKind> {
    let mut node_kinds = Vec::with_capacity(graph.nodes.len());
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
            ) => {
                let then_kind = infer_graph_output_kind(then_branch)?;
                (infer_graph_output_kind(else_branch)? == then_kind).then_some(then_kind)
            }
            _ => None,
        };
        node_kinds.push(kind);
    }
    match &graph.output {
        BoundRef::Const(value) => scalar_kind_from_value(value),
        BoundRef::Node(node) => node_kinds.get(node.index()).copied().flatten(),
        BoundRef::External(_) => None,
    }
}

fn infer_output_kind(
    output: &BoundRef,
    scalar_signatures: &[Option<ScalarSignature>],
    environment_layout: &EnvironmentLayout,
    output_slot: EnvironmentSlot,
    stream_slots: StreamSlots,
    stream_outputs: &[Option<ScalarKind>],
) -> Option<ScalarKind> {
    match output {
        BoundRef::Const(value) => scalar_kind_from_value(value)
            .or_else(|| scalar_kind_from_stream_type(environment_layout.stream_type(output_slot))),
        BoundRef::Node(node) => match scalar_signatures.get(node.index()).copied().flatten()? {
            ScalarSignature::Unary { output, .. } | ScalarSignature::Binary { output, .. } => {
                Some(output)
            }
        },
        BoundRef::External(slot) => {
            if let Some(stream) = stream_slots.stream(*slot) {
                stream_outputs.get(stream.index()).copied().flatten()
            } else {
                scalar_kind_from_stream_type(environment_layout.stream_type(*slot))
            }
        }
    }
}

fn scalar_kind_from_value(value: &Value) -> Option<ScalarKind> {
    match value {
        Value::Int(_) => Some(ScalarKind::Int),
        Value::Float(_) => Some(ScalarKind::Float),
        Value::Bool(_) => Some(ScalarKind::Bool),
        _ => None,
    }
}

fn scalar_kind_from_stream_type(type_: Option<&StreamType>) -> Option<ScalarKind> {
    match type_? {
        StreamType::Int => Some(ScalarKind::Int),
        StreamType::Float => Some(ScalarKind::Float),
        StreamType::Bool => Some(ScalarKind::Bool),
        _ => None,
    }
}
