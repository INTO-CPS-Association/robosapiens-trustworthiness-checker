//! The immutable plan compiled once per monitor definition.
//!
//! [`MonitorPlan`] is everything compilation can decide before the first tick: the fixed
//! [`DependencyGraph`] over same-tick edges, the [`ReconfigurableExpressionPlan`] describing every
//! `dynamic`/`defer` occurrence and its source prerequisites, and the set of streams that need a
//! temporal commit.
//!
//! Everything here is immutable for the life of the program. The matching mutable per-tick
//! bookkeeping lives in [`super::expression_activation`], which reads this plan but is never read
//! by it.

use super::compiler::pipeline::NamedDependencies;
use super::environment::EnvironmentSlot;
use super::error::DataflowCompilationError;
use super::ir::{BoundRef, NodeId, ReconfigurableExpressionKind, StreamProgram};
use super::reconfiguration::ExpressionStateKey;
use super::stream_id::{StreamId, StreamSet, StreamSlots};
use super::{Value, VarName};
use std::collections::BTreeMap;
use std::ops::Range;
use std::rc::Rc;

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
    pub(super) source_prerequisites: StreamSet,
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

/// Same-tick producer-to-consumer edges, fixed at compile time.
///
/// Only *immediate* reads are edges here. A positive historical read such as `a[1]` observes
/// committed state from an earlier tick, so it constrains nothing about this tick's order and is
/// deliberately absent. Edges contributed by a live `dynamic`/`defer` body are not here either —
/// those are discovered per tick and merged by the [`super::scheduler::Scheduler`].
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
    pub(super) expressions: Vec<ReconfigurableExpression>,
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
                    reason: "expression sources must be constants or environment values outside dynamically evaluated lazy branches",
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
                if source.index() >= stream_slots.start().index() {
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
            debug_assert!(program.uses_static_evaluation());
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

/// Everything compilation can decide before the first tick.
///
/// Built once by [`MonitorPlan::build`] and then immutable: the fixed dependency edges, the
/// description of every reconfigurable expression, and the set of streams needing a temporal
/// commit. A monitor's mutable per-tick state lives elsewhere and refers back to this.
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

    /// One synthetic expression descriptor. Shared with the `expression_activation` tests, which
    /// cannot build one themselves because the descriptor's fields are private to this module.
    pub(in crate::dataflow) fn reconfigurable_expression(
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

    /// A plan over synthetic expressions, with source refcounts derived the same way
    /// `ReconfigurableExpressionPlan::build` derives them.
    pub(in crate::dataflow) fn reconfigurable_expression_plan(
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
}

#[cfg(test)]
mod tests {
    use super::test_support::{
        reconfigurable_expression as expression, reconfigurable_expression_plan as plan,
    };
    use super::*;

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
}
