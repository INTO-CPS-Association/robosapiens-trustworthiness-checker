use super::VarName;
use super::environment::EnvironmentSlot;
use super::error::DataflowEvaluationError;
use super::execution_plan::{
    DependencyGraph, ReconfigurationPlan, StreamId, StreamSet, StreamSlots,
};

pub(super) struct DynamicDependencyCollector {
    streams: StreamSet,
    stream_slots: StreamSlots,
}

impl DynamicDependencyCollector {
    fn new(stream_slots: StreamSlots) -> Self {
        Self {
            streams: StreamSet::empty(),
            stream_slots,
        }
    }

    fn clear(&mut self) {
        self.streams.streams.clear();
    }

    #[inline]
    pub(super) fn extend(&mut self, dependency_slots: &[EnvironmentSlot]) {
        self.streams.streams.extend(
            dependency_slots
                .iter()
                .filter_map(|slot| self.stream_slots.stream(*slot)),
        );
    }

    #[inline]
    pub(super) fn finish(&mut self) {
        self.streams.streams.sort_unstable();
        self.streams.streams.dedup();
    }

    fn as_slice(&self) -> &[StreamId] {
        self.streams.as_slice()
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

/// Maintains a dependency-valid stream order and reusable iterative-repair workspace.
pub(super) struct Scheduler {
    dynamic_dependencies: Vec<DynamicDependencyCollector>,
    scheduled_order: Vec<StreamId>,
    positions_by_stream: Vec<usize>,
    visit_states: Vec<VisitState>,
    dfs_stack: Vec<DfsFrame>,
    repaired_order: Vec<StreamId>,
    execution_schedule: ExecutionSchedule,
}

impl Scheduler {
    pub(super) fn new(
        stream_slots: StreamSlots,
        dependencies: &DependencyGraph,
        reconfiguration: &ReconfigurationPlan,
    ) -> Self {
        let stream_count = dependencies.stream_count();
        let mut scheduler = Self {
            dynamic_dependencies: (0..stream_count)
                .map(|_| DynamicDependencyCollector::new(stream_slots))
                .collect(),
            scheduled_order: (0..stream_count).map(StreamId::new).collect(),
            positions_by_stream: vec![0; stream_count],
            visit_states: vec![VisitState::Unvisited; stream_count],
            dfs_stack: Vec::with_capacity(stream_count),
            repaired_order: Vec::with_capacity(stream_count),
            execution_schedule: ExecutionSchedule {
                evaluation_order: Vec::with_capacity(stream_count),
                uses_static_order: false,
            },
        };
        scheduler.build_execution_schedule(reconfiguration);
        scheduler
    }

    #[inline]
    pub(super) fn begin_dynamic_dependency_update(
        &mut self,
        stream: StreamId,
    ) -> &mut DynamicDependencyCollector {
        let collector = &mut self.dynamic_dependencies[stream.index()];
        collector.clear();
        collector
    }

    pub(super) fn update_schedule(
        &mut self,
        dependencies: &DependencyGraph,
        reconfiguration: &ReconfigurationPlan,
        stream_vars: &[VarName],
    ) -> Result<bool, DataflowEvaluationError> {
        if self.scheduled_order_is_valid(dependencies) {
            return Ok(false);
        }
        self.repair_scheduled_order(dependencies, stream_vars)?;
        std::mem::swap(&mut self.scheduled_order, &mut self.repaired_order);
        self.repaired_order.clear();
        self.build_execution_schedule(reconfiguration);
        Ok(true)
    }

    #[inline]
    pub(super) fn execution_schedule(&self) -> &ExecutionSchedule {
        &self.execution_schedule
    }

    fn scheduled_order_is_valid(&mut self, dependencies: &DependencyGraph) -> bool {
        for (position, &stream) in self.scheduled_order.iter().enumerate() {
            self.positions_by_stream[stream.index()] = position;
        }
        for consumer in dependencies.reconfigurable_streams().iter() {
            let consumer_position = self.positions_by_stream[consumer.index()];
            if self.dynamic_dependencies[consumer.index()]
                .as_slice()
                .iter()
                .any(|producer| self.positions_by_stream[producer.index()] >= consumer_position)
            {
                return false;
            }
        }
        true
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

    fn build_execution_schedule(&mut self, reconfiguration: &ReconfigurationPlan) {
        let schedule = &mut self.execution_schedule;
        schedule.evaluation_order.clear();
        schedule.evaluation_order.extend(
            self.scheduled_order
                .iter()
                .copied()
                .filter(|stream| !reconfiguration.contains_evaluation_stream(*stream)),
        );
        schedule.uses_static_order = reconfiguration.evaluation_order().is_empty()
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
    use crate::dataflow::execution_plan::test_support::{
        dependency_graph_without_static_dependencies, empty_reconfiguration_plan,
    };

    fn set_dynamic_dependencies(scheduler: &mut Scheduler, consumer: usize, producers: &[usize]) {
        let dependencies = scheduler.begin_dynamic_dependency_update(StreamId::new(consumer));
        let slots = producers
            .iter()
            .copied()
            .map(EnvironmentSlot::new)
            .collect::<Vec<_>>();
        dependencies.extend(&slots);
        dependencies.finish();
    }

    #[test]
    fn scheduled_order_is_retained_when_dynamic_edges_are_satisfied() {
        let graph = dependency_graph_without_static_dependencies(3);
        let reconfiguration = empty_reconfiguration_plan(3);
        let mut scheduler = Scheduler::new(
            StreamSlots::new(EnvironmentSlot::new(0), 3),
            &graph,
            &reconfiguration,
        );
        set_dynamic_dependencies(&mut scheduler, 2, &[0, 1]);

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
    fn iterative_repair_orders_dynamic_dependencies_before_consumers() {
        let graph = dependency_graph_without_static_dependencies(3);
        let reconfiguration = empty_reconfiguration_plan(3);
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
    fn dynamic_cycles_are_rejected() {
        let graph = dependency_graph_without_static_dependencies(2);
        let reconfiguration = empty_reconfiguration_plan(2);
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
        let reconfiguration = empty_reconfiguration_plan(stream_count);
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
