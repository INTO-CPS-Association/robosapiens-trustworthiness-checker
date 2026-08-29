use super::compiler::pipeline::NamedDependencies;
use super::environment::EnvironmentSlot;
use super::error::DataflowCompilationError;
use super::ir::{BoundRef, NodeId, ReconfigurableExpressionKind, StreamProgram};
use super::reconfiguration::ExpressionStateKey;
use super::{Value, VarName};
use std::collections::BTreeMap;
use std::ops::Range;
use std::rc::Rc;

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReconfigurableExpressionId(usize);

impl ReconfigurableExpressionId {
    #[inline]
    pub(super) fn new(index: usize) -> Self {
        Self(index)
    }

    /// Return the dense plan index carried by this expression identity.
    #[inline]
    pub fn index(self) -> usize {
        self.0
    }
}

#[derive(Clone)]
pub(super) struct ReconfigurableExpression {
    pub(super) id: ReconfigurableExpressionId,
    pub(super) stream: StreamId,
    pub(super) node: NodeId,
    pub(super) address: ExpressionStateKey,
    pub(super) source: ExpressionSource,
    pub(super) kind: ReconfigurableExpressionKind,
    source_prerequisites: StreamSet,
}

impl ReconfigurableExpression {
    #[inline]
    pub(super) fn id(&self) -> ReconfigurableExpressionId {
        self.id
    }

    pub(super) fn address(&self) -> &ExpressionStateKey {
        &self.address
    }
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
                    .has_reconfigurable_expressions()
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

fn source_prerequisite_closure(producer: StreamId, dependencies: &DependencyGraph) -> StreamSet {
    let stream_count = dependencies.stream_count();
    let mut included = vec![false; stream_count];
    let mut stack = Vec::with_capacity(stream_count);
    stack.push(producer);
    while let Some(stream) = stack.pop() {
        if included[stream.index()] {
            continue;
        }
        included[stream.index()] = true;
        stack.extend(dependencies.static_dependencies(stream).iter());
    }
    StreamSet::from_streams(
        included
            .iter()
            .enumerate()
            .filter_map(|(index, included)| included.then(|| StreamId::new(index))),
    )
}

pub(super) struct ReconfigurableExpressionPlan {
    evaluation_streams: StreamSet,
    evaluation_order: Vec<StreamId>,
    expressions: Vec<ReconfigurableExpression>,
    expression_ranges_by_stream: Vec<Range<usize>>,
}

impl ReconfigurableExpressionPlan {
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

        let mut expressions = Vec::new();
        let mut expression_ranges_by_stream = Vec::with_capacity(programs.len());
        for (index, program) in programs.iter().enumerate() {
            let stream = StreamId::new(index);
            let start = expressions.len();
            let mut owner_occurrences = BTreeMap::<String, usize>::new();
            for (node, source, kind) in program.reconfigurable_expressions() {
                let (source, owner) = match source {
                    BoundRef::Const(value) => (
                        ExpressionSource::Constant(value.clone()),
                        "constant".to_owned(),
                    ),
                    BoundRef::External(slot) => {
                        let owner = program
                            .environment_layout
                            .variable(*slot)
                            .map(|variable| variable.name())
                            .unwrap_or_else(|| format!("slot:{}", slot.index()));
                        (ExpressionSource::Environment(*slot), owner)
                    }
                    BoundRef::Node(_) => {
                        return Err(DataflowCompilationError::UnsupportedReconfiguration {
                            stream: stream_vars[index].clone(),
                            reason: "node-local expression sources cannot be resolved before stream evaluation",
                        });
                    }
                };
                let occurrence = owner_occurrences.entry(owner.clone()).or_insert(0);
                let address = ExpressionStateKey::new(&stream_vars[index], owner, *occurrence);
                *occurrence += 1;
                expressions.push(ReconfigurableExpression {
                    id: ReconfigurableExpressionId::new(expressions.len()),
                    stream,
                    node,
                    address,
                    source,
                    kind,
                    source_prerequisites: StreamSet::empty(),
                });
            }
            expression_ranges_by_stream.push(start..expressions.len());
        }

        let stream_count = programs.len();
        let mut all_prerequisites = vec![false; stream_count];
        for expression in &mut expressions {
            let ExpressionSource::Environment(source) = expression.source else {
                continue;
            };
            let Some(producer) = stream_slots.stream(source) else {
                if source.index() >= stream_slots.start.index() {
                    return Err(DataflowCompilationError::UnsupportedReconfiguration {
                        stream: stream_vars[expression.stream.index()].clone(),
                        reason: "expression source references an invalid environment slot",
                    });
                }
                continue;
            };

            expression.source_prerequisites = source_prerequisite_closure(producer, dependencies);
            for prerequisite in expression.source_prerequisites.iter() {
                all_prerequisites[prerequisite.index()] = true;
            }
        }

        let evaluation_streams = StreamSet::from_streams(
            all_prerequisites
                .iter()
                .enumerate()
                .filter_map(|(index, included)| included.then(|| StreamId::new(index))),
        );
        for stream in evaluation_streams.iter() {
            let program = &programs[stream.index()];
            if program.has_reconfigurable_expressions() {
                return Err(DataflowCompilationError::UnsupportedReconfiguration {
                    stream: stream_vars[stream.index()].clone(),
                    reason: "expression-source evaluation streams cannot contain reconfigurable expressions",
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
            expressions,
            expression_ranges_by_stream,
        })
    }

    #[inline]
    pub(super) fn is_empty(&self) -> bool {
        self.expressions.is_empty()
    }

    #[cfg(test)]
    #[inline]
    pub(super) fn expressions(&self) -> &[ReconfigurableExpression] {
        &self.expressions
    }

    #[inline]
    pub(super) fn initial_source_prerequisite_order(&self) -> &[StreamId] {
        &self.evaluation_order
    }

    #[inline]
    pub(super) fn initial_source_prerequisites(&self) -> &StreamSet {
        &self.evaluation_streams
    }

    #[inline]
    pub(super) fn initial_source_order(&self) -> &[StreamId] {
        self.initial_source_prerequisite_order()
    }

    #[inline]
    pub(super) fn initial_source_streams(&self) -> &StreamSet {
        self.initial_source_prerequisites()
    }

    #[inline]
    pub(super) fn expressions_for(&self, stream: StreamId) -> &[ReconfigurableExpression] {
        &self.expressions[self.expression_ranges_by_stream[stream.index()].clone()]
    }

    /// Resolve a plan identity to the exact dense execution location it owns.
    ///
    /// The identity is not trusted as an unchecked vector index: the descriptor at that index must
    /// carry the same identity. This keeps a stale or misaddressed expression from being interpreted as
    /// another stream/node pair by the execution layer.
    #[cfg(test)]
    #[inline]
    pub(super) fn lookup(
        &self,
        expression_id: ReconfigurableExpressionId,
    ) -> Option<(StreamId, NodeId)> {
        self.expressions
            .get(expression_id.index())
            .filter(|expression| expression.id() == expression_id)
            .map(|expression| (expression.stream, expression.node))
    }
}

#[derive(Clone)]
pub(super) struct ReconfigurableExpressionState {
    sealed_deferred_expressions: Vec<bool>,
    pending_releases: Vec<ReconfigurableExpressionId>,
    release_prerequisites: Vec<Option<StreamSet>>,
    expression_streams: Vec<StreamId>,
    source_user_refcounts: Vec<usize>,
    live_expression_refcounts: Vec<usize>,
    resolution_streams: StreamSet,
    source_streams: StreamSet,
    source_order: Vec<StreamId>,
}

impl ReconfigurableExpressionState {
    pub(super) fn new(plan: &ReconfigurableExpressionPlan, stream_count: usize) -> Self {
        let mut source_user_refcounts = vec![0usize; stream_count];
        let mut live_expression_refcounts = vec![0usize; stream_count];
        let mut release_prerequisites = Vec::with_capacity(plan.expressions.len());
        let mut expression_streams = Vec::with_capacity(plan.expressions.len());
        for expression in &plan.expressions {
            debug_assert_eq!(expression.id.index(), release_prerequisites.len());
            expression_streams.push(expression.stream);
            live_expression_refcounts[expression.stream.index()] += 1;
            for stream in expression.source_prerequisites.iter() {
                source_user_refcounts[stream.index()] += 1;
            }
            release_prerequisites.push(
                matches!(expression.kind, ReconfigurableExpressionKind::Deferred)
                    .then(|| expression.source_prerequisites.clone()),
            );
        }

        let resolution_streams = StreamSet::from_streams(
            live_expression_refcounts
                .iter()
                .enumerate()
                .filter_map(|(index, count)| (*count != 0).then(|| StreamId::new(index))),
        );
        let source_streams = StreamSet::from_streams(
            source_user_refcounts
                .iter()
                .enumerate()
                .filter_map(|(index, count)| (*count != 0).then(|| StreamId::new(index))),
        );
        debug_assert_eq!(&source_streams, plan.initial_source_streams());

        Self {
            sealed_deferred_expressions: vec![false; plan.expressions.len()],
            pending_releases: Vec::new(),
            release_prerequisites,
            expression_streams,
            source_user_refcounts,
            live_expression_refcounts,
            resolution_streams,
            source_streams,
            source_order: plan.initial_source_order().to_vec(),
        }
    }

    #[inline]
    pub(super) fn resolution_stream(&self, index: usize) -> Option<StreamId> {
        self.resolution_streams.as_slice().get(index).copied()
    }

    #[inline]
    pub(super) fn source_order(&self) -> &[StreamId] {
        &self.source_order
    }

    #[inline]
    pub(super) fn source_streams(&self) -> &StreamSet {
        &self.source_streams
    }

    #[inline]
    pub(super) fn is_sealed(&self, expression_id: ReconfigurableExpressionId) -> bool {
        self.sealed_deferred_expressions
            .get(expression_id.index())
            .copied()
            .unwrap_or(false)
    }

    pub(super) fn mark_deferred_activated(
        &mut self,
        expression_id: ReconfigurableExpressionId,
    ) -> bool {
        let index = expression_id.index();
        let Some(prerequisites) = self.release_prerequisites.get(index) else {
            return false;
        };
        if prerequisites.is_none() || self.sealed_deferred_expressions[index] {
            return false;
        }

        self.sealed_deferred_expressions[index] = true;
        self.pending_releases.push(expression_id);
        true
    }

    pub(super) fn sealed_addresses(
        &self,
        plan: &ReconfigurableExpressionPlan,
    ) -> Vec<ExpressionStateKey> {
        plan.expressions
            .iter()
            .filter(|expression| self.is_sealed(expression.id()))
            .map(|expression| expression.address().clone())
            .collect()
    }

    /// Reconstruct lifecycle-derived routing for an imported root monitor. Deferred-source release
    /// remains on the pending path below so a failed tick cannot release sources early.
    pub(super) fn restore_sealed_addresses(
        &mut self,
        plan: &ReconfigurableExpressionPlan,
        sealed: &[ExpressionStateKey],
    ) {
        for expression in &plan.expressions {
            if sealed.iter().any(|address| address == expression.address()) {
                self.mark_deferred_activated(expression.id());
            }
        }
        self.apply_pending_releases();
    }

    pub(super) fn apply_pending_releases(&mut self) -> bool {
        if self.pending_releases.is_empty() {
            return false;
        }

        let mut membership_changed = false;
        let mut resolution_membership_changed = false;
        for expression_id in self.pending_releases.drain(..) {
            let expression_stream = self.expression_streams[expression_id.index()];
            let live_count = &mut self.live_expression_refcounts[expression_stream.index()];
            debug_assert!(*live_count != 0);
            *live_count -= 1;
            resolution_membership_changed |= *live_count == 0;

            let prerequisites = self.release_prerequisites[expression_id.index()]
                .as_ref()
                .expect("only deferred expressions can have pending source releases");
            for stream in prerequisites.iter() {
                let count = &mut self.source_user_refcounts[stream.index()];
                debug_assert!(*count != 0);
                *count -= 1;
                if *count == 0 {
                    membership_changed = true;
                }
            }
        }

        if resolution_membership_changed {
            self.resolution_streams
                .streams
                .retain(|stream| self.live_expression_refcounts[stream.index()] != 0);
        }
        if membership_changed {
            self.source_streams
                .streams
                .retain(|stream| self.source_user_refcounts[stream.index()] != 0);
            self.source_order
                .retain(|stream| self.source_user_refcounts[stream.index()] != 0);
        }
        membership_changed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expression(
        id: usize,
        kind: ReconfigurableExpressionKind,
        prerequisites: &[usize],
    ) -> ReconfigurableExpression {
        ReconfigurableExpression {
            id: ReconfigurableExpressionId::new(id),
            stream: StreamId::new(id),
            node: NodeId::new(0),
            address: ExpressionStateKey::new(&VarName::new("stream"), "test", id),
            source: ExpressionSource::Constant(Value::Int(0)),
            kind,
            source_prerequisites: StreamSet::from_streams(
                prerequisites.iter().copied().map(StreamId::new),
            ),
        }
    }

    fn plan(
        expressions: Vec<ReconfigurableExpression>,
        stream_count: usize,
    ) -> ReconfigurableExpressionPlan {
        let mut refcounts = vec![0usize; stream_count];
        for expression in &expressions {
            for stream in expression.source_prerequisites.iter() {
                refcounts[stream.index()] += 1;
            }
        }
        let source_streams = StreamSet::from_streams(
            refcounts
                .iter()
                .enumerate()
                .filter_map(|(index, count)| (*count != 0).then(|| StreamId::new(index))),
        );
        let source_order = source_streams.as_slice().to_vec();
        let expression_count = expressions.len();
        ReconfigurableExpressionPlan {
            evaluation_streams: source_streams,
            evaluation_order: source_order,
            expressions,
            expression_ranges_by_stream: (0..stream_count)
                .map(|index| index.min(expression_count)..(index + 1).min(expression_count))
                .collect(),
        }
    }

    #[test]
    fn expression_ids_are_dense_and_stream_ranges_use_the_dense_storage() {
        let plan = plan(
            vec![
                expression(0, ReconfigurableExpressionKind::Dynamic, &[]),
                expression(1, ReconfigurableExpressionKind::Deferred, &[]),
            ],
            2,
        );

        assert_eq!(
            plan.expressions()
                .iter()
                .map(|expression| expression.id().index())
                .collect::<Vec<_>>(),
            [0, 1]
        );
        assert_eq!(plan.expressions_for(StreamId::new(0))[0].id().index(), 0);
        assert_eq!(plan.expressions_for(StreamId::new(1))[0].id().index(), 1);
    }

    #[test]
    fn expression_lookup_rejects_mismatched_dense_ids() {
        let mut mismatched = expression(0, ReconfigurableExpressionKind::Dynamic, &[]);
        mismatched.id = ReconfigurableExpressionId::new(1);
        let malformed = plan(vec![mismatched], 1);

        assert_eq!(
            malformed.lookup(ReconfigurableExpressionId::new(0)),
            None,
            "an ID must match the descriptor stored at its dense index"
        );

        let plan = plan(
            vec![
                expression(0, ReconfigurableExpressionKind::Dynamic, &[]),
                expression(1, ReconfigurableExpressionKind::Dynamic, &[]),
            ],
            2,
        );
        assert_eq!(
            plan.lookup(ReconfigurableExpressionId::new(0)),
            Some((StreamId::new(0), NodeId::new(0)))
        );
        assert_ne!(
            plan.lookup(ReconfigurableExpressionId::new(0)),
            Some((StreamId::new(1), NodeId::new(0)))
        );
    }

    #[test]
    fn source_prerequisite_closure_is_transitive_and_per_expression() {
        let dependencies = DependencyGraph {
            static_dependencies: vec![
                StreamSet::empty(),
                StreamSet::from_streams([StreamId::new(0)]),
                StreamSet::from_streams([StreamId::new(1)]),
                StreamSet::empty(),
            ],
            reconfigurable_streams: StreamSet::empty(),
        };

        assert_eq!(
            source_prerequisite_closure(StreamId::new(2), &dependencies)
                .iter()
                .map(StreamId::index)
                .collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert_eq!(
            source_prerequisite_closure(StreamId::new(3), &dependencies)
                .iter()
                .map(StreamId::index)
                .collect::<Vec<_>>(),
            [3]
        );
    }

    #[test]
    fn deferred_source_release_is_delayed_until_explicitly_applied() {
        let plan = plan(
            vec![expression(
                0,
                ReconfigurableExpressionKind::Deferred,
                &[0, 1],
            )],
            2,
        );
        let mut state = ReconfigurableExpressionState::new(&plan, 2);
        let expression_id = plan.expressions()[0].id;

        assert!(state.mark_deferred_activated(expression_id));
        assert!(state.is_sealed(expression_id));
        assert_eq!(state.source_streams().as_slice().len(), 2);
        assert_eq!(state.source_order().len(), 2);

        assert!(state.apply_pending_releases());
        assert!(state.source_streams().as_slice().is_empty());
        assert!(state.source_order().is_empty());
        assert!(!state.apply_pending_releases());
    }

    #[test]
    fn shared_deferred_and_dynamic_sources_use_refcounts() {
        let plan = plan(
            vec![
                expression(0, ReconfigurableExpressionKind::Deferred, &[0, 1]),
                expression(1, ReconfigurableExpressionKind::Dynamic, &[1, 2]),
            ],
            3,
        );
        let mut state = ReconfigurableExpressionState::new(&plan, 3);

        assert!(state.mark_deferred_activated(plan.expressions()[0].id));
        assert!(state.apply_pending_releases());
        assert_eq!(
            state
                .source_streams()
                .iter()
                .map(StreamId::index)
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(
            state
                .source_order()
                .iter()
                .copied()
                .map(StreamId::index)
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert!(!state.mark_deferred_activated(plan.expressions()[0].id));
        assert!(!state.mark_deferred_activated(plan.expressions()[1].id));
    }

    #[test]
    fn one_shared_deferred_release_does_not_change_source_membership() {
        let plan = plan(
            vec![
                expression(0, ReconfigurableExpressionKind::Deferred, &[1]),
                expression(1, ReconfigurableExpressionKind::Deferred, &[1]),
            ],
            2,
        );
        let mut state = ReconfigurableExpressionState::new(&plan, 2);

        state.mark_deferred_activated(plan.expressions()[0].id);
        assert!(!state.apply_pending_releases());
        assert!(state.source_streams().contains(StreamId::new(1)));

        state.mark_deferred_activated(plan.expressions()[1].id);
        assert!(state.apply_pending_releases());
        assert!(!state.source_streams().contains(StreamId::new(1)));
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

    pub(in crate::dataflow) fn empty_reconfigurable_expression_plan(
        stream_count: usize,
    ) -> ReconfigurableExpressionPlan {
        ReconfigurableExpressionPlan {
            evaluation_streams: StreamSet::empty(),
            evaluation_order: Vec::new(),
            expressions: Vec::new(),
            expression_ranges_by_stream: vec![0..0; stream_count],
        }
    }
}

pub(super) struct MonitorPlan {
    pub(super) stream_slots: StreamSlots,
    pub(super) dependencies: DependencyGraph,
    pub(super) reconfigurable_expressions: ReconfigurableExpressionPlan,
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
        let reconfigurable_expressions = ReconfigurableExpressionPlan::build(
            stream_slots,
            &dependencies,
            stream_vars,
            programs,
        )?;
        let temporal_streams =
            StreamSet::from_streams(programs.iter().enumerate().filter_map(|(index, program)| {
                program
                    .requires_temporal_commit()
                    .then(|| StreamId::new(index))
            }));
        Ok(Self {
            stream_slots,
            dependencies,
            reconfigurable_expressions,
            temporal_streams,
        })
    }
}
