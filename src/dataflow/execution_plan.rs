use super::compiler::pipeline::NamedDependencies;
use super::environment::EnvironmentSlot;
use super::error::DataflowCompilationError;
use super::ir::{BoundRef, NodeId, StreamProgram};
use super::{Value, VarName};
use std::collections::BTreeMap;
use std::ops::Range;
use std::rc::Rc;

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct StreamId(usize);

impl StreamId {
    #[inline]
    pub(super) fn new(index: usize) -> Self {
        Self(index)
    }

    #[inline]
    pub(super) fn index(self) -> usize {
        self.0
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct StreamSlots {
    start: EnvironmentSlot,
    len: usize,
}

impl StreamSlots {
    pub(super) fn new(start: EnvironmentSlot, len: usize) -> Self {
        Self { start, len }
    }

    #[inline]
    pub(super) fn slot(self, stream: StreamId) -> EnvironmentSlot {
        debug_assert!(stream.index() < self.len);
        EnvironmentSlot::new(self.start.index() + stream.index())
    }

    #[inline]
    pub(super) fn stream(self, slot: EnvironmentSlot) -> Option<StreamId> {
        let index = slot.index().checked_sub(self.start.index())?;
        (index < self.len).then(|| StreamId::new(index))
    }

    #[inline]
    pub(super) fn start(self) -> EnvironmentSlot {
        self.start
    }

    pub(super) fn len(self) -> usize {
        self.len
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct StreamSet {
    pub(in crate::dataflow) streams: Vec<StreamId>,
}

impl StreamSet {
    pub(super) fn empty() -> Self {
        Self {
            streams: Vec::new(),
        }
    }

    pub(super) fn from_streams(streams: impl IntoIterator<Item = StreamId>) -> Self {
        let mut streams = streams.into_iter().collect::<Vec<_>>();
        streams.sort_unstable();
        streams.dedup();
        Self { streams }
    }

    #[inline]
    pub(super) fn contains(&self, stream: StreamId) -> bool {
        self.streams.binary_search(&stream).is_ok()
    }

    #[inline]
    pub(super) fn iter(&self) -> impl Iterator<Item = StreamId> + '_ {
        self.streams.iter().copied()
    }

    #[inline]
    pub(super) fn as_slice(&self) -> &[StreamId] {
        &self.streams
    }
}

#[derive(Clone)]
pub(super) enum ExpressionSource {
    Constant(Value),
    Environment(EnvironmentSlot),
}

impl ExpressionSource {
    #[inline]
    pub(super) fn read_value(&self, environment_values: &[Value]) -> Value {
        match self {
            Self::Constant(value) => value.clone(),
            Self::Environment(slot) => environment_values[slot.index()].clone(),
        }
    }
}

#[derive(Clone)]
pub(super) struct ReconfigurationPoint {
    pub(super) stream: StreamId,
    pub(super) node: NodeId,
    pub(super) source: ExpressionSource,
}

pub(super) struct DependencyGraph {
    static_dependencies: Vec<StreamSet>,
    reconfigurable_streams: StreamSet,
}

impl DependencyGraph {
    fn build(
        stream_vars: &[VarName],
        named_dependencies: &NamedDependencies,
        programs: &[Rc<StreamProgram>],
    ) -> Self {
        let stream_ids_by_name = stream_vars
            .iter()
            .enumerate()
            .map(|(index, name)| (name, StreamId::new(index)))
            .collect::<BTreeMap<_, _>>();
        let mut static_dependencies = (0..programs.len())
            .map(|_| StreamSet::empty())
            .collect::<Vec<_>>();

        for (consumer, dependencies) in named_dependencies {
            let consumer = stream_ids_by_name[consumer];
            static_dependencies[consumer.index()] = StreamSet::from_streams(
                dependencies
                    .iter()
                    .filter_map(|dependency| stream_ids_by_name.get(dependency).copied()),
            );
        }

        let reconfigurable_streams =
            StreamSet::from_streams(programs.iter().enumerate().filter_map(|(index, program)| {
                program
                    .has_reconfiguration_points()
                    .then(|| StreamId::new(index))
            }));

        Self {
            static_dependencies,
            reconfigurable_streams,
        }
    }

    pub(super) fn static_dependencies(&self, stream: StreamId) -> &StreamSet {
        &self.static_dependencies[stream.index()]
    }

    #[inline]
    pub(super) fn reconfigurable_streams(&self) -> &StreamSet {
        &self.reconfigurable_streams
    }

    pub(super) fn stream_count(&self) -> usize {
        self.static_dependencies.len()
    }
}

pub(super) struct ReconfigurationPlan {
    evaluation_streams: StreamSet,
    evaluation_order: Vec<StreamId>,
    points: Vec<ReconfigurationPoint>,
    point_ranges_by_stream: Vec<Range<usize>>,
}

impl ReconfigurationPlan {
    fn build(
        stream_slots: StreamSlots,
        dependencies: &DependencyGraph,
        stream_vars: &[VarName],
        programs: &[Rc<StreamProgram>],
    ) -> Result<Self, DataflowCompilationError> {
        for stream in dependencies.reconfigurable_streams().iter() {
            if !programs[stream.index()].can_resolve_dependencies_before_evaluation() {
                return Err(DataflowCompilationError::UnsupportedReconfiguration {
                    stream: stream_vars[stream.index()].clone(),
                    reason: "expression sources must be constants or environment values outside fallible lazy branches",
                });
            }
        }

        let mut points = Vec::new();
        let mut point_ranges_by_stream = Vec::with_capacity(programs.len());
        for (index, program) in programs.iter().enumerate() {
            let stream = StreamId::new(index);
            let start = points.len();
            for (node, source) in program.reconfiguration_points() {
                let source = match source {
                    BoundRef::Const(value) => ExpressionSource::Constant(value.clone()),
                    BoundRef::External(slot) => ExpressionSource::Environment(*slot),
                    BoundRef::Node(_) => {
                        return Err(DataflowCompilationError::UnsupportedReconfiguration {
                            stream: stream_vars[index].clone(),
                            reason: "node-local expression sources cannot be resolved before stream evaluation",
                        });
                    }
                };
                points.push(ReconfigurationPoint {
                    stream,
                    node,
                    source,
                });
            }
            point_ranges_by_stream.push(start..points.len());
        }

        let stream_count = programs.len();
        let mut included = vec![false; stream_count];
        let mut stack = Vec::with_capacity(stream_count);
        for point in &points {
            let ExpressionSource::Environment(source) = point.source else {
                continue;
            };
            let Some(producer) = stream_slots.stream(source) else {
                if source.index() >= stream_slots.start.index() {
                    return Err(DataflowCompilationError::UnsupportedReconfiguration {
                        stream: stream_vars[point.stream.index()].clone(),
                        reason: "expression source references an invalid environment slot",
                    });
                }
                continue;
            };
            stack.push(producer);
            while let Some(stream) = stack.pop() {
                if included[stream.index()] {
                    continue;
                }
                included[stream.index()] = true;
                stack.extend(dependencies.static_dependencies(stream).iter());
            }
        }

        let evaluation_streams = StreamSet::from_streams(
            included
                .iter()
                .enumerate()
                .filter_map(|(index, included)| included.then(|| StreamId::new(index))),
        );
        for stream in evaluation_streams.iter() {
            let program = &programs[stream.index()];
            if program.has_reconfiguration_points() {
                return Err(DataflowCompilationError::UnsupportedReconfiguration {
                    stream: stream_vars[stream.index()].clone(),
                    reason: "expression-source evaluation streams cannot contain reconfiguration points",
                });
            }
            debug_assert!(program.is_infallible());
        }
        let evaluation_order = (0..stream_count)
            .map(StreamId::new)
            .filter(|stream| evaluation_streams.contains(*stream))
            .collect();

        Ok(Self {
            evaluation_streams,
            evaluation_order,
            points,
            point_ranges_by_stream,
        })
    }

    #[inline]
    pub(super) fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    #[inline]
    pub(super) fn evaluation_order(&self) -> &[StreamId] {
        &self.evaluation_order
    }

    #[inline]
    pub(super) fn contains_evaluation_stream(&self, stream: StreamId) -> bool {
        self.evaluation_streams.contains(stream)
    }

    #[inline]
    pub(super) fn points_for(&self, stream: StreamId) -> &[ReconfigurationPoint] {
        &self.points[self.point_ranges_by_stream[stream.index()].clone()]
    }
}

#[cfg(test)]
pub(super) mod test_support {
    use super::*;

    pub(in crate::dataflow) fn dependency_graph_without_static_dependencies(
        stream_count: usize,
    ) -> DependencyGraph {
        DependencyGraph {
            static_dependencies: (0..stream_count).map(|_| StreamSet::empty()).collect(),
            reconfigurable_streams: StreamSet::from_streams((0..stream_count).map(StreamId::new)),
        }
    }

    pub(in crate::dataflow) fn empty_reconfiguration_plan(
        stream_count: usize,
    ) -> ReconfigurationPlan {
        ReconfigurationPlan {
            evaluation_streams: StreamSet::empty(),
            evaluation_order: Vec::new(),
            points: Vec::new(),
            point_ranges_by_stream: vec![0..0; stream_count],
        }
    }
}

pub(super) struct MonitorPlan {
    pub(super) stream_slots: StreamSlots,
    pub(super) dependencies: DependencyGraph,
    pub(super) reconfiguration: ReconfigurationPlan,
    pub(super) temporal_streams: StreamSet,
}

impl MonitorPlan {
    pub(super) fn build(
        stream_slots: StreamSlots,
        stream_vars: &[VarName],
        named_dependencies: &NamedDependencies,
        programs: &[Rc<StreamProgram>],
    ) -> Result<Self, DataflowCompilationError> {
        debug_assert_eq!(stream_slots.len(), programs.len());
        debug_assert_eq!(stream_vars.len(), programs.len());

        let dependencies = DependencyGraph::build(stream_vars, named_dependencies, programs);
        let reconfiguration =
            ReconfigurationPlan::build(stream_slots, &dependencies, stream_vars, programs)?;
        let temporal_streams =
            StreamSet::from_streams(programs.iter().enumerate().filter_map(|(index, program)| {
                program
                    .requires_temporal_commit()
                    .then(|| StreamId::new(index))
            }));
        Ok(Self {
            stream_slots,
            dependencies,
            reconfiguration,
            temporal_streams,
        })
    }
}
