use super::super::super::environment::EnvironmentSlot;
use super::super::super::execution_plan::{StreamId, StreamSlots};
use super::super::quickening;
use super::super::scheduled_plan::{PlanId, PlannedStream, ScheduledExecutionPlan};
use super::{EXECUTION_LAYOUT_CACHE_SIZE, MonitorExecution};
use std::rc::Rc;

pub(super) struct PlanBundle {
    pub(super) semantic: Box<ScheduledExecutionPlan>,
    pub(super) quick: QuickPlan,
}

pub(super) struct QuickPlan {
    pub(super) source_steps: Box<[QuickStep]>,
    pub(super) main_steps: Box<[QuickStep]>,
}

pub(super) enum QuickStep {
    ScalarRun(Box<[ScalarStep]>),
    Graph(GraphStep),
}

pub(super) struct GraphStep {
    pub(super) stream: StreamId,
    pub(super) slot: EnvironmentSlot,
    pub(super) schedule_plan: Option<quickening::Plan>,
    pub(super) adaptive_candidate: bool,
}

pub(super) struct ScalarStep {
    pub(super) stream: StreamId,
    pub(super) slot: EnvironmentSlot,
    pub(super) plan: quickening::SingleScalarPlan,
}

impl PlanBundle {
    pub(super) fn new(semantic: ScheduledExecutionPlan, quickening: bool) -> Self {
        let mut available = vec![false; semantic.stream_slots.len()];
        let source_steps = Self::build_quick_range(
            &semantic,
            semantic.source_streams(),
            &mut available,
            quickening,
        );
        let steps = Self::build_quick_range(
            &semantic,
            semantic.main_streams(),
            &mut available,
            quickening,
        );

        Self {
            semantic: Box::new(semantic),
            quick: QuickPlan {
                source_steps,
                main_steps: steps,
            },
        }
    }

    fn build_quick_range(
        semantic: &ScheduledExecutionPlan,
        planned_streams: &[PlannedStream],
        available: &mut [bool],
        quickening: bool,
    ) -> Box<[QuickStep]> {
        let mut steps = Vec::with_capacity(planned_streams.len());
        let mut scalar_run = Vec::new();

        for planned in planned_streams {
            let stream = planned.stream;
            let program = planned.program.as_ref();
            let schedule_plan = (quickening && !planned.effects.may_fail)
                .then(|| {
                    quickening::Plan::with_published_sources(&program.graph, |slot| {
                        semantic
                            .stream_slots
                            .stream(slot)
                            .filter(|producer| available[producer.index()])
                            .map(StreamId::index)
                    })
                })
                .flatten();
            let slot = planned.output.environment();
            let (single_scalar_plan, schedule_plan) = match schedule_plan {
                Some(plan) => match plan.try_into_single_scalar(&program.graph) {
                    Ok(plan) => (Some(plan), None),
                    Err(plan) => (None, Some(plan)),
                },
                None => (None, None),
            };
            if let Some(plan) = single_scalar_plan {
                scalar_run.push(ScalarStep { stream, slot, plan });
            } else {
                if !scalar_run.is_empty() {
                    steps.push(QuickStep::ScalarRun(
                        std::mem::take(&mut scalar_run).into_boxed_slice(),
                    ));
                }
                steps.push(QuickStep::Graph(GraphStep {
                    stream,
                    slot,
                    adaptive_candidate: quickening
                        && schedule_plan.is_none()
                        && quickening::is_adaptive_candidate(&program.graph),
                    schedule_plan,
                }));
            }
            available[stream.index()] = true;
        }
        if !scalar_run.is_empty() {
            steps.push(QuickStep::ScalarRun(scalar_run.into_boxed_slice()));
        }
        steps.into_boxed_slice()
    }
}

impl MonitorExecution {
    pub(in crate::dataflow) fn select_schedule_ranges(
        &mut self,
        source_order: &[StreamId],
        main_order: &[StreamId],
        stream_slots: StreamSlots,
    ) {
        let matches = |plan: &PlanBundle| {
            plan.semantic.source_stream_count == source_order.len()
                && plan
                    .semantic
                    .source_order()
                    .eq(source_order.iter().copied())
                && plan.semantic.main_order().eq(main_order.iter().copied())
        };
        if matches(&self.engine.active_plan) {
            return;
        }
        if let Some(cached) = self.engine.cached_plans.iter().position(matches) {
            std::mem::swap(
                &mut self.engine.active_plan,
                &mut self.engine.cached_plans[cached],
            );
        } else {
            let semantic = ScheduledExecutionPlan::from_metadata(
                PlanId(self.engine.next_plan_id),
                Rc::clone(&self.engine.active_plan.semantic.metadata),
                stream_slots,
                source_order,
                main_order,
                &self.temporal_streams,
            );
            self.engine.next_plan_id += 1;
            let new_plan = PlanBundle::new(semantic, self.engine.quickening);
            let previous = std::mem::replace(&mut self.engine.active_plan, new_plan);
            if self.engine.cached_plans.len() == EXECUTION_LAYOUT_CACHE_SIZE {
                self.engine.cached_plans.remove(0);
            }
            self.engine.cached_plans.push(previous);
        }
        self.engine.jit.schedule_changed(
            &self.engine.active_plan.semantic,
            &mut self.evaluators.evaluators,
        );
    }
}
