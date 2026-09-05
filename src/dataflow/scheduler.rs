//! Dependency-valid ordering over computed streams.
//!
//! The [`Scheduler`] holds one order and keeps it valid. Fixed edges come from the compiled
//! [`DependencyGraph`]; a reconfigurable expression contributes the edges its *current* body
//! actually reads, collected through a [`DynamicDependencyCollector`]. Most ticks change nothing,
//! so the scheduler checks its cached order first and runs its allocation-reusing iterative DFS
//! only when an added edge genuinely violates it.
//!
//! Order is all this module owns. It holds no evaluator state and no execution plan: moving a
//! stream within the order never moves the language state attached to that stream's identity.

use super::VarName;
use super::environment::EnvironmentSlot;
use super::error::DataflowEvaluationError;
use super::monitor_plan::{DependencyGraph, ReconfigurableExpressionPlan};
use super::stream_id::{StreamId, StreamSet, StreamSlots};
use std::cell::Cell;
use std::rc::Rc;

pub(super) struct DynamicDependencyCollector {
    active_streams: StreamSet,
    pending_streams: StreamSet,
    stream_slots: StreamSlots,
    consumer: StreamId,
    positions_by_stream: Rc<[Cell<usize>]>,
    order_dirty: Rc<Cell<bool>>,
}

impl DynamicDependencyCollector {
    fn new(
        stream_slots: StreamSlots,
        consumer: StreamId,
        positions_by_stream: Rc<[Cell<usize>]>,
        order_dirty: Rc<Cell<bool>>,
    ) -> Self {
        Self {
            active_streams: StreamSet::empty(),
            pending_streams: StreamSet::empty(),
            stream_slots,
            consumer,
            positions_by_stream,
            order_dirty,
        }
    }

    fn begin_update(&mut self) {
        self.pending_streams.streams.clear();
    }

    #[inline]
    pub(super) fn extend(&mut self, dependency_slots: &[EnvironmentSlot]) {
        self.pending_streams.streams.extend(
            dependency_slots
                .iter()
                .filter_map(|slot| self.stream_slots.stream(*slot)),
        );
    }

    pub(super) fn finish(&mut self) -> bool {
        self.pending_streams.streams.sort_unstable();
        self.pending_streams.streams.dedup();
        if self.pending_streams == self.active_streams {
            return false;
        }

        if !self.order_dirty.get() {
            let consumer_position = self.positions_by_stream[self.consumer.index()].get();
            let has_violating_addition = self
                .pending_streams
                .as_slice()
                .iter()
                .filter(|producer| !self.active_streams.contains(**producer))
                .any(|producer| {
                    self.positions_by_stream[producer.index()].get() >= consumer_position
                });
            self.order_dirty.set(has_violating_addition);
        }
        std::mem::swap(&mut self.active_streams, &mut self.pending_streams);
        true
    }

    fn as_slice(&self) -> &[StreamId] {
        self.active_streams.as_slice()
    }
}

pub(super) struct ExecutionSchedule {
    evaluation_order: Vec<StreamId>,
    uses_static_order: bool,
}

impl ExecutionSchedule {
    #[inline]
    pub(super) fn evaluation_order(&self) -> &[StreamId] {
        &self.evaluation_order
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VisitState {
    Unvisited,
    Visiting,
    Complete,
}

#[derive(Clone, Copy)]
struct DfsFrame {
    stream: StreamId,
    next_static_dependency: usize,
    next_dynamic_dependency: usize,
}

pub(super) trait ActiveSourceStreams {
    fn active_source_streams(&self) -> &StreamSet;
}

impl ActiveSourceStreams for StreamSet {
    fn active_source_streams(&self) -> &StreamSet {
        self
    }
}

impl ActiveSourceStreams for ReconfigurableExpressionPlan {
    fn active_source_streams(&self) -> &StreamSet {
        self.initial_source_streams()
    }
}

/// Maintains a dependency-valid stream order and reusable iterative-repair workspace.
pub(super) struct Scheduler {
    dynamic_dependencies: Vec<DynamicDependencyCollector>,
    scheduled_order: Vec<StreamId>,
    positions_by_stream: Rc<[Cell<usize>]>,
    order_dirty: Rc<Cell<bool>>,
    visit_states: Vec<VisitState>,
    dfs_stack: Vec<DfsFrame>,
    repaired_order: Vec<StreamId>,
    execution_schedule: ExecutionSchedule,
    #[cfg(test)]
    update_schedule_call_count: usize,
}

impl Scheduler {
    pub(super) fn new(
        stream_slots: StreamSlots,
        dependencies: &DependencyGraph,
        source_streams: &impl ActiveSourceStreams,
    ) -> Self {
        let stream_count = dependencies.stream_count();
        let positions_by_stream = (0..stream_count)
            .map(Cell::new)
            .collect::<Rc<[Cell<usize>]>>();
        let order_dirty = Rc::new(Cell::new(false));
        let mut scheduler = Self {
            dynamic_dependencies: (0..stream_count)
                .map(|index| {
                    DynamicDependencyCollector::new(
                        stream_slots,
                        StreamId::new(index),
                        Rc::clone(&positions_by_stream),
                        Rc::clone(&order_dirty),
                    )
                })
                .collect(),
            scheduled_order: (0..stream_count).map(StreamId::new).collect(),
            positions_by_stream,
            order_dirty,
            visit_states: vec![VisitState::Unvisited; stream_count],
            dfs_stack: Vec::with_capacity(stream_count),
            repaired_order: Vec::with_capacity(stream_count),
            execution_schedule: ExecutionSchedule {
                evaluation_order: Vec::with_capacity(stream_count),
                uses_static_order: false,
            },
            #[cfg(test)]
            update_schedule_call_count: 0,
        };
        for (position, stream) in scheduler.scheduled_order.iter().copied().enumerate() {
            scheduler.positions_by_stream[stream.index()].set(position);
        }
        scheduler.build_execution_schedule(source_streams.active_source_streams());
        scheduler
    }

    #[inline]
    pub(super) fn begin_dynamic_dependency_update(
        &mut self,
        stream: StreamId,
    ) -> &mut DynamicDependencyCollector {
        let collector = &mut self.dynamic_dependencies[stream.index()];
        collector.begin_update();
        collector
    }

    /// Rebuild one collector from portable environment bindings.  This is used only while
    /// importing a root context; stable evaluation continues to use compact stream sets.
    pub(super) fn restore_dynamic_dependencies(
        &mut self,
        stream: StreamId,
        dependency_slots: &[EnvironmentSlot],
    ) {
        let dependencies = self.begin_dynamic_dependency_update(stream);
        dependencies.extend(dependency_slots);
        dependencies.finish();
    }

    pub(super) fn update_schedule(
        &mut self,
        dependencies: &DependencyGraph,
        source_streams: &impl ActiveSourceStreams,
        stream_vars: &[VarName],
    ) -> Result<bool, DataflowEvaluationError> {
        #[cfg(test)]
        {
            self.update_schedule_call_count += 1;
        }
        if !self.order_dirty.get() {
            return Ok(false);
        }
        self.repair_scheduled_order(dependencies, stream_vars)?;
        std::mem::swap(&mut self.scheduled_order, &mut self.repaired_order);
        self.repaired_order.clear();
        for (position, stream) in self.scheduled_order.iter().copied().enumerate() {
            self.positions_by_stream[stream.index()].set(position);
        }
        self.order_dirty.set(false);
        self.build_execution_schedule(source_streams.active_source_streams());
        Ok(true)
    }

    #[inline]
    pub(super) fn execution_schedule(&self) -> &ExecutionSchedule {
        &self.execution_schedule
    }

    #[cfg(test)]
    pub(super) fn update_schedule_call_count(&self) -> usize {
        self.update_schedule_call_count
    }

    #[cfg(test)]
    pub(super) fn reset_update_schedule_call_count(&mut self) {
        self.update_schedule_call_count = 0;
    }

    pub(super) fn refresh_main_execution_schedule(&mut self, source_streams: &StreamSet) -> bool {
        let previous_order = self.execution_schedule.evaluation_order.clone();
        self.build_execution_schedule(source_streams);
        self.execution_schedule.evaluation_order != previous_order
    }

    fn repair_scheduled_order(
        &mut self,
        dependencies: &DependencyGraph,
        stream_vars: &[VarName],
    ) -> Result<(), DataflowEvaluationError> {
        self.visit_states.fill(VisitState::Unvisited);
        self.dfs_stack.clear();
        self.repaired_order.clear();

        for root_position in 0..self.scheduled_order.len() {
            let root = self.scheduled_order[root_position];
            if self.visit_states[root.index()] == VisitState::Complete {
                continue;
            }
            self.visit_states[root.index()] = VisitState::Visiting;
            self.dfs_stack.push(DfsFrame {
                stream: root,
                next_static_dependency: 0,
                next_dynamic_dependency: 0,
            });

            while let Some(frame) = self.dfs_stack.last_mut() {
                let static_dependencies = dependencies.static_dependencies(frame.stream).as_slice();
                let dependency = if frame.next_static_dependency < static_dependencies.len() {
                    let dependency = static_dependencies[frame.next_static_dependency];
                    frame.next_static_dependency += 1;
                    Some(dependency)
                } else {
                    let dynamic_dependencies =
                        self.dynamic_dependencies[frame.stream.index()].as_slice();
                    if frame.next_dynamic_dependency < dynamic_dependencies.len() {
                        let dependency = dynamic_dependencies[frame.next_dynamic_dependency];
                        frame.next_dynamic_dependency += 1;
                        Some(dependency)
                    } else {
                        None
                    }
                };

                let Some(dependency) = dependency else {
                    let stream = frame.stream;
                    self.dfs_stack.pop();
                    self.visit_states[stream.index()] = VisitState::Complete;
                    self.repaired_order.push(stream);
                    continue;
                };

                match self.visit_states[dependency.index()] {
                    VisitState::Unvisited => {
                        self.visit_states[dependency.index()] = VisitState::Visiting;
                        self.dfs_stack.push(DfsFrame {
                            stream: dependency,
                            next_static_dependency: 0,
                            next_dynamic_dependency: 0,
                        });
                    }
                    VisitState::Visiting => {
                        return Err(DataflowEvaluationError::DynamicDependencyCycle(
                            stream_vars[dependency.index()].clone(),
                        ));
                    }
                    VisitState::Complete => {}
                }
            }
        }
        Ok(())
    }

    fn build_execution_schedule(&mut self, source_streams: &StreamSet) {
        let schedule = &mut self.execution_schedule;
        schedule.evaluation_order.clear();
        schedule.evaluation_order.extend(
            self.scheduled_order
                .iter()
                .copied()
                .filter(|stream| !source_streams.contains(*stream)),
        );
        schedule.uses_static_order = source_streams.as_slice().is_empty()
            && schedule.evaluation_order.len() == self.scheduled_order.len()
            && schedule
                .evaluation_order
                .iter()
                .enumerate()
                .all(|(position, stream)| position == stream.index());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::monitor_plan::test_support::{
        dependency_graph_without_static_dependencies, empty_reconfigurable_expression_plan,
    };

    fn set_dynamic_dependencies(
        scheduler: &mut Scheduler,
        consumer: usize,
        producers: &[usize],
    ) -> bool {
        let dependencies = scheduler.begin_dynamic_dependency_update(StreamId::new(consumer));
        let slots = producers
            .iter()
            .copied()
            .map(EnvironmentSlot::new)
            .collect::<Vec<_>>();
        dependencies.extend(&slots);
        dependencies.finish()
    }

    #[test]
    fn scheduled_order_is_retained_when_dynamic_edges_are_satisfied() {
        let graph = dependency_graph_without_static_dependencies(3);
        let reconfiguration = empty_reconfigurable_expression_plan(3);
        let mut scheduler = Scheduler::new(
            StreamSlots::new(EnvironmentSlot::new(0), 3),
            &graph,
            &reconfiguration,
        );
        assert!(set_dynamic_dependencies(&mut scheduler, 2, &[0, 1]));

        assert!(
            !scheduler
                .update_schedule(
                    &graph,
                    &reconfiguration,
                    &["a".into(), "b".into(), "c".into()],
                )
                .unwrap()
        );

        assert_eq!(
            scheduler
                .execution_schedule()
                .evaluation_order()
                .iter()
                .map(|stream| stream.index())
                .collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert!(scheduler.execution_schedule.uses_static_order);
    }

    #[test]
    fn unchanged_dynamic_dependencies_do_not_dirty_the_order() {
        let graph = dependency_graph_without_static_dependencies(3);
        let reconfiguration = empty_reconfigurable_expression_plan(3);
        let mut scheduler = Scheduler::new(
            StreamSlots::new(EnvironmentSlot::new(0), 3),
            &graph,
            &reconfiguration,
        );
        assert!(set_dynamic_dependencies(&mut scheduler, 0, &[2]));
        assert!(
            scheduler
                .update_schedule(
                    &graph,
                    &reconfiguration,
                    &["a".into(), "b".into(), "c".into()],
                )
                .unwrap()
        );

        assert!(!set_dynamic_dependencies(&mut scheduler, 0, &[2]));
        assert!(
            !scheduler
                .update_schedule(
                    &graph,
                    &reconfiguration,
                    &["a".into(), "b".into(), "c".into()],
                )
                .unwrap()
        );
    }

    #[test]
    fn removing_dynamic_dependencies_does_not_dirty_the_order() {
        let graph = dependency_graph_without_static_dependencies(3);
        let reconfiguration = empty_reconfigurable_expression_plan(3);
        let mut scheduler = Scheduler::new(
            StreamSlots::new(EnvironmentSlot::new(0), 3),
            &graph,
            &reconfiguration,
        );
        set_dynamic_dependencies(&mut scheduler, 0, &[2]);
        scheduler
            .update_schedule(
                &graph,
                &reconfiguration,
                &["a".into(), "b".into(), "c".into()],
            )
            .unwrap();

        assert!(set_dynamic_dependencies(&mut scheduler, 0, &[]));
        assert!(
            !scheduler
                .update_schedule(
                    &graph,
                    &reconfiguration,
                    &["a".into(), "b".into(), "c".into()],
                )
                .unwrap()
        );
        assert_eq!(
            scheduler
                .execution_schedule()
                .evaluation_order()
                .iter()
                .map(|stream| stream.index())
                .collect::<Vec<_>>(),
            [2, 0, 1]
        );
    }

    #[test]
    fn iterative_repair_orders_dynamic_dependencies_before_consumers() {
        let graph = dependency_graph_without_static_dependencies(3);
        let reconfiguration = empty_reconfigurable_expression_plan(3);
        let mut scheduler = Scheduler::new(
            StreamSlots::new(EnvironmentSlot::new(0), 3),
            &graph,
            &reconfiguration,
        );
        set_dynamic_dependencies(&mut scheduler, 0, &[2]);

        assert!(
            scheduler
                .update_schedule(
                    &graph,
                    &reconfiguration,
                    &["a".into(), "b".into(), "c".into()],
                )
                .unwrap()
        );

        let order = scheduler.execution_schedule().evaluation_order();
        let producer = order.iter().position(|stream| stream.index() == 2).unwrap();
        let consumer = order.iter().position(|stream| stream.index() == 0).unwrap();
        assert!(producer < consumer);
        assert!(!scheduler.execution_schedule.uses_static_order);
    }

    #[test]
    fn refreshing_source_streams_rebuilds_only_the_main_schedule() {
        let graph = dependency_graph_without_static_dependencies(3);
        let initial_sources = StreamSet::from_streams([StreamId::new(1)]);
        let mut scheduler = Scheduler::new(
            StreamSlots::new(EnvironmentSlot::new(0), 3),
            &graph,
            &initial_sources,
        );
        assert_eq!(
            scheduler
                .execution_schedule()
                .evaluation_order()
                .iter()
                .map(|stream| stream.index())
                .collect::<Vec<_>>(),
            [0, 2]
        );

        assert!(scheduler.refresh_main_execution_schedule(&StreamSet::empty()));
        assert_eq!(
            scheduler
                .execution_schedule()
                .evaluation_order()
                .iter()
                .map(|stream| stream.index())
                .collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert!(!scheduler.refresh_main_execution_schedule(&StreamSet::empty()));
    }

    #[test]
    fn dynamic_cycles_are_rejected() {
        let graph = dependency_graph_without_static_dependencies(2);
        let reconfiguration = empty_reconfigurable_expression_plan(2);
        let mut scheduler = Scheduler::new(
            StreamSlots::new(EnvironmentSlot::new(0), 2),
            &graph,
            &reconfiguration,
        );
        set_dynamic_dependencies(&mut scheduler, 0, &[1]);
        set_dynamic_dependencies(&mut scheduler, 1, &[0]);

        assert!(matches!(
            scheduler.update_schedule(&graph, &reconfiguration, &["a".into(), "b".into()]),
            Err(DataflowEvaluationError::DynamicDependencyCycle(_))
        ));
    }

    #[test]
    fn repair_uses_an_explicit_stack_for_long_chains() {
        let stream_count = 4_096;
        let graph = dependency_graph_without_static_dependencies(stream_count);
        let reconfiguration = empty_reconfigurable_expression_plan(stream_count);
        let mut scheduler = Scheduler::new(
            StreamSlots::new(EnvironmentSlot::new(0), stream_count),
            &graph,
            &reconfiguration,
        );
        for consumer in 0..stream_count - 1 {
            set_dynamic_dependencies(&mut scheduler, consumer, &[consumer + 1]);
        }
        let names = (0..stream_count)
            .map(|stream| VarName::from(format!("s{stream}")))
            .collect::<Vec<_>>();

        scheduler
            .update_schedule(&graph, &reconfiguration, &names)
            .unwrap();

        let order = scheduler.execution_schedule().evaluation_order();
        assert_eq!(order[0].index(), stream_count - 1);
        assert_eq!(order[stream_count - 1].index(), 0);
    }
}
