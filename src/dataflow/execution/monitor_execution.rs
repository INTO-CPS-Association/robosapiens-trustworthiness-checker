//! Monitor-level ownership and replaceable, cacheable scheduled plans.
//!
//! A fixed arena owns one persistent evaluator per logical stream. An execution
//! plan only chooses an evaluation order and replaces eligible environment
//! reads with compact values published by earlier planned streams. Layout rebuilds
//! therefore reorder stream IDs without moving temporal, function,
//! dynamic-expression, or deoptimization state.
//!
//! Logical results are published after every planned stream. Canonical instructions and
//! nested evaluators consequently observe the canonical environment and do not
//! form fusion barriers.
//!
//! Planning is backend independent. A [`PlanBundle`] owns the semantic schedule and a
//! schedule-wide quickened artifact. The optional native tier is coordinated by
//! [`ExecutionEngine`], but it consumes the same ordered programs and never changes the plan.
//! Canonical state remains in the evaluator arena. A native tier may temporarily use a packed
//! physical layout, but that layout maps to stable plan state slots and is materialized before
//! canonical replay.

use super::super::execution_plan::{StreamId, StreamSlots};
use super::super::ir::{NodeId, StreamProgram};
use super::super::*;
use super::interpreter::stage_recursive_delays;
use super::jit::{
    FusedTickOutcome, GraphTickOutcome, Jit, NativeCommitContext, NativeGraphContext,
};
use super::quickening::{self, ScalarValue};
use super::scheduled_plan::{PlanId, ScheduledExecutionPlan};
use super::stream_evaluator::{RegionReplacement, StreamEvaluator};

const EXECUTION_LAYOUT_CACHE_SIZE: usize = 4;

#[derive(Clone)]
struct EvaluatorArena {
    evaluators: Box<[StreamEvaluator]>,
    published_scalars: Box<[Option<ScalarValue>]>,
}

pub(in crate::dataflow) struct MonitorExecution {
    evaluators: EvaluatorArena,
    stream_slots: StreamSlots,
    temporal_streams: Box<[StreamId]>,
    engine: ExecutionEngine,
    tick_in_progress: bool,
}

/// The single tier-selection and plan-cache boundary for a monitor.
struct ExecutionEngine {
    active_plan: PlanBundle,
    cached_plans: Vec<PlanBundle>,
    next_plan_id: u64,
    quickening: bool,
    jit: Jit,
}

impl MonitorExecution {
    pub(in crate::dataflow) fn new_with_source_prelude(
        programs: Vec<Rc<StreamProgram>>,
        stream_slots: StreamSlots,
        source_order: &[StreamId],
        main_order: &[StreamId],
        temporal_streams: &[StreamId],
    ) -> Self {
        let semantic = ScheduledExecutionPlan::new(
            PlanId(0),
            &programs,
            stream_slots,
            source_order,
            main_order,
            temporal_streams,
        );
        let active_plan = PlanBundle::new(semantic, true);
        let mut evaluators = EvaluatorArena::new(programs);
        evaluators.detach_top_level_quick_plans();
        Self {
            evaluators,
            stream_slots,
            temporal_streams: temporal_streams.to_vec().into_boxed_slice(),
            engine: ExecutionEngine {
                active_plan,
                cached_plans: Vec::new(),
                next_plan_id: 1,
                quickening: true,
                jit: Jit::disabled(),
            },
            tick_in_progress: false,
        }
    }

    pub(in crate::dataflow) fn set_quickening(&mut self, enabled: bool) {
        if self.engine.quickening == enabled {
            return;
        }
        self.engine.quickening = enabled;
        self.engine.active_plan =
            PlanBundle::new((*self.engine.active_plan.semantic).clone(), enabled);
        self.engine.cached_plans.clear();
    }

    #[cfg(test)]
    pub(crate) fn quickening_enabled(&self) -> bool {
        self.engine.quickening
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn enable_jit(&mut self, config: JitConfig) {
        self.engine
            .jit
            .configure(&self.engine.active_plan.semantic, config);
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn jit_report(&self) -> Option<&JitReport> {
        self.engine.jit.report()
    }

    pub(in crate::dataflow) fn tick_in_progress(&self) -> bool {
        self.tick_in_progress
    }

    pub(in crate::dataflow) fn snapshot_evaluators(&self) -> Vec<StreamEvaluator> {
        self.engine.jit.snapshot_evaluators(
            &self.evaluators.evaluators,
            &self.engine.active_plan.semantic,
        )
    }

    pub(in crate::dataflow) fn reset_after_context_transfer(&mut self) {
        self.evaluators.published_scalars.fill(None);
        for evaluator in &mut self.evaluators.evaluators {
            evaluator.invalidate_derived_state();
        }
        self.engine.jit.reset_after_context_transfer();
    }

    pub(in crate::dataflow) fn transfer_evaluator(
        &mut self,
        stream: StreamId,
        source: &StreamEvaluator,
    ) -> bool {
        self.evaluators.evaluators[stream.index()].transfer_from(source)
    }

    pub(in crate::dataflow) fn transfer_compatible_evaluator(
        &mut self,
        stream: StreamId,
        source: &StreamEvaluator,
    ) -> bool {
        self.evaluators.evaluators[stream.index()].transfer_compatible_from(source)
    }

    /// Install a nested replacement body directly into the owning evaluator.
    #[inline]
    pub(in crate::dataflow) fn replace_reconfiguration_point(
        &mut self,
        stream: StreamId,
        node: NodeId,
        source_value: Value,
        transfer: ContextTransferPolicy,
    ) -> Result<RegionReplacement, DataflowEvaluationError> {
        self.evaluators.evaluators[stream.index()].replace_reconfiguration_point(
            node,
            source_value,
            transfer,
        )
    }

    pub(in crate::dataflow) fn install_reconfiguration_point(
        &mut self,
        stream: StreamId,
        node: NodeId,
        replacement: RegionReplacement,
    ) -> Result<(), DataflowEvaluationError> {
        self.evaluators.evaluators[stream.index()].install_reconfiguration_point(node, replacement)
    }

    pub(in crate::dataflow) fn reconfiguration_point_dependency_slots(
        &self,
        stream: StreamId,
        node: NodeId,
    ) -> &[EnvironmentSlot] {
        self.evaluators.evaluators[stream.index()].reconfiguration_point_dependency_slots(node)
    }

    pub(in crate::dataflow) fn reconfiguration_point_requires_update(
        &self,
        stream: StreamId,
        node: NodeId,
        source_value: &Value,
    ) -> bool {
        self.evaluators.evaluators[stream.index()]
            .reconfiguration_point_requires_update(node, source_value)
    }

    #[inline]
    fn commit_temporal_state(
        &mut self,
        stream: StreamId,
        environment_values: &[Value],
        retained_environment_values: Option<&[Value]>,
    ) {
        let evaluator = &mut self.evaluators.evaluators[stream.index()];
        if self.engine.jit.commit_graph(
            stream.index(),
            NativeCommitContext {
                state: &mut evaluator.state,
                environment_values,
                environment_layout: &evaluator.program.environment_layout,
                retained_environment_values,
            },
        ) {
            return;
        }
        evaluator.commit_temporal_state_with_retained_environment(
            environment_values,
            retained_environment_values,
        );
    }

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
            let programs = self.evaluators.programs_rc();
            let semantic = ScheduledExecutionPlan::new(
                PlanId(self.engine.next_plan_id),
                &programs,
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
        self.engine
            .jit
            .schedule_changed(&self.engine.active_plan.semantic);
    }

    #[inline]
    pub(in crate::dataflow) fn evaluate_source_prelude(
        &mut self,
        environment_values: &mut [Value],
        mut retained_environment_values: Option<&mut [Value]>,
    ) -> Result<(), DataflowEvaluationError> {
        self.begin_tick();
        let result = self.evaluators.evaluate_quick_steps::<true>(
            &mut self.engine.jit,
            &self.engine.active_plan.quick.source_steps,
            environment_values,
            retained_environment_values.as_deref_mut(),
            self.stream_slots,
            false,
        );
        if result.is_err() {
            self.tick_in_progress = false;
        }
        result
    }

    #[inline]
    pub(in crate::dataflow) fn evaluate_main_and_commit(
        &mut self,
        environment_values: &mut [Value],
        mut retained_environment_values: Option<&mut [Value]>,
    ) -> Result<(), DataflowEvaluationError> {
        if !self.tick_in_progress {
            self.begin_tick();
        }
        let result = self.evaluators.evaluate_quick_steps::<true>(
            &mut self.engine.jit,
            &self.engine.active_plan.quick.main_steps,
            environment_values,
            retained_environment_values.as_deref_mut(),
            self.stream_slots,
            true,
        );
        if result.is_ok() {
            self.commit_active_plan(environment_values, retained_environment_values.as_deref());
        }
        self.tick_in_progress = false;
        result
    }

    #[inline]
    pub(in crate::dataflow) fn evaluate(
        &mut self,
        environment_values: &mut [Value],
        _retained_environment_values: Option<&mut [Value]>,
    ) -> Result<(), DataflowEvaluationError> {
        debug_assert!(!self.engine.active_plan.semantic.has_source_barrier());
        self.begin_tick();
        let result = self.evaluate_no_source_tick(environment_values);
        self.tick_in_progress = false;
        result
    }

    fn begin_tick(&mut self) {
        assert!(
            !self.tick_in_progress,
            "source prelude executed twice in one logical tick"
        );
        self.tick_in_progress = true;
        if self.engine.jit.activation_due() {
            #[cfg(feature = "jit")]
            self.engine
                .jit
                .activate_pending(&self.engine.active_plan.semantic);
        }
    }

    fn evaluate_no_source_tick(
        &mut self,
        environment_values: &mut [Value],
    ) -> Result<(), DataflowEvaluationError> {
        debug_assert!(!self.engine.active_plan.semantic.has_source_barrier());
        match self.engine.jit.evaluate_fused(
            &mut self.evaluators.evaluators,
            environment_values,
            &mut self.evaluators.published_scalars,
        ) {
            FusedTickOutcome::Success | FusedTickOutcome::SuccessCommitted => return Ok(()),
            FusedTickOutcome::Canonical => {
                if let Some(mut replay_environment) = self.engine.jit.take_replay_environment() {
                    self.evaluators.replay_canonical(
                        &self.engine.active_plan.semantic,
                        &mut replay_environment,
                    );
                }

                self.evaluators
                    .evaluate_canonical_run(&self.engine.active_plan.semantic, environment_values);
                self.commit_active_plan(environment_values, None);
                return Ok(());
            }
            FusedTickOutcome::NotHandled => {}
        }
        let result = self.evaluators.evaluate_quick_steps::<false>(
            &mut self.engine.jit,
            &self.engine.active_plan.quick.main_steps,
            environment_values,
            None,
            self.stream_slots,
            true,
        );
        if result.is_ok() {
            self.commit_active_plan(environment_values, None);
        }
        result
    }

    #[inline]
    fn commit_active_plan(
        &mut self,
        environment_values: &[Value],
        retained_environment_values: Option<&[Value]>,
    ) {
        for index in 0..self.engine.active_plan.semantic.commit_streams.len() {
            let stream = self.engine.active_plan.semantic.commit_streams[index];
            self.commit_temporal_state(stream, environment_values, retained_environment_values);
        }
    }

    #[cfg(all(test, feature = "jit"))]
    pub(in crate::dataflow) fn jit_artifact_count(&self) -> usize {
        self.engine.jit.compiled_artifact_count()
    }
}

struct PlanBundle {
    semantic: Box<ScheduledExecutionPlan>,
    quick: QuickPlan,
}

struct QuickPlan {
    source_steps: Box<[QuickStep]>,
    main_steps: Box<[QuickStep]>,
}

enum QuickStep {
    ScalarRun(Box<[ScalarStep]>),
    Graph(GraphStep),
}

struct GraphStep {
    stream: StreamId,
    slot: EnvironmentSlot,
    quickening_plan: Option<quickening::Plan>,
}

struct ScalarStep {
    stream: StreamId,
    slot: EnvironmentSlot,
    plan: quickening::SingleScalarPlan,
}

impl PlanBundle {
    fn new(semantic: ScheduledExecutionPlan, quickening: bool) -> Self {
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
        planned_streams: &[super::scheduled_plan::PlannedStream],
        available: &mut [bool],
        quickening: bool,
    ) -> Box<[QuickStep]> {
        let mut steps = Vec::with_capacity(planned_streams.len());
        let mut scalar_run = Vec::new();

        for planned in planned_streams {
            let stream = planned.stream;
            let program = planned.program.as_ref();
            let quickening_plan = (quickening && !planned.effects.may_fail)
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
            let (single_scalar_plan, quickening_plan) = match quickening_plan {
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
                    quickening_plan,
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

impl EvaluatorArena {
    fn new(programs: Vec<Rc<StreamProgram>>) -> Self {
        let published_scalars = vec![None; programs.len()].into_boxed_slice();
        let evaluators = programs
            .into_iter()
            .map(StreamEvaluator::new)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            evaluators,
            published_scalars,
        }
    }

    fn detach_top_level_quick_plans(&mut self) {
        for evaluator in &mut self.evaluators {
            evaluator.detach_top_level_quick_plan();
        }
    }

    #[inline]
    fn evaluator_with_published(
        &mut self,
        stream: usize,
    ) -> (&mut StreamEvaluator, &[Option<ScalarValue>]) {
        (&mut self.evaluators[stream], &self.published_scalars)
    }

    fn evaluate_quick_steps<const RETAIN_VALUES: bool>(
        &mut self,
        jit: &mut Jit,
        steps: &[QuickStep],
        environment_values: &mut [Value],
        mut retained_environment_values: Option<&mut [Value]>,
        stream_slots: StreamSlots,
        allow_complete_temporal_kernels: bool,
    ) -> Result<(), DataflowEvaluationError> {
        // Published sources only name streams earlier in the combined two-range order. Availability
        // intentionally carries across the source barrier, while scalar runs cannot cross it.
        for step in steps {
            match step {
                QuickStep::ScalarRun(run) => self.evaluate_scalar_run::<RETAIN_VALUES>(
                    run,
                    environment_values,
                    retained_environment_values.as_deref_mut(),
                ),
                QuickStep::Graph(step) => self.evaluate_graph::<RETAIN_VALUES>(
                    jit,
                    step,
                    environment_values,
                    retained_environment_values.as_deref_mut(),
                    stream_slots,
                    allow_complete_temporal_kernels,
                )?,
            }
        }
        Ok(())
    }

    #[inline]
    fn evaluate_scalar_run<const RETAIN_VALUES: bool>(
        &mut self,
        run: &[ScalarStep],
        environment_values: &mut [Value],
        mut retained_environment_values: Option<&mut [Value]>,
    ) {
        for step in run {
            let index = step.stream.index();
            let (evaluator, published_scalars) = self.evaluator_with_published(index);
            let result = evaluator.evaluate_single_scalar_with_plan(
                environment_values,
                &step.plan,
                published_scalars,
            );
            let value = match result {
                quickening::DirectResult::Scalar(value) => {
                    self.publish(index, Some(value));
                    value.into_value()
                }
                quickening::DirectResult::Canonical(value) => {
                    self.publish(index, ScalarValue::from_untyped_value(&value));
                    value
                }
            };
            Self::publish_environment_value::<RETAIN_VALUES>(
                environment_values,
                retained_environment_values.as_deref_mut(),
                step.slot,
                value,
            );
        }
    }

    #[inline]
    fn evaluate_graph<const RETAIN_VALUES: bool>(
        &mut self,
        jit: &mut Jit,
        step: &GraphStep,
        environment_values: &mut [Value],
        retained_environment_values: Option<&mut [Value]>,
        stream_slots: StreamSlots,
        allow_complete_temporal_kernel: bool,
    ) -> Result<(), DataflowEvaluationError> {
        let index = step.stream.index();
        let (evaluator, published_scalars) = self.evaluator_with_published(index);
        let value = if evaluator.program.is_infallible() {
            match jit.evaluate_graph(
                index,
                NativeGraphContext {
                    graph: &evaluator.program.graph,
                    state: &mut evaluator.state,
                    environment_values,
                    environment_layout: &evaluator.program.environment_layout,
                    published_scalars,
                    stream_slots,
                    allow_complete_temporal_kernel,
                },
            ) {
                GraphTickOutcome::Value(value) => {
                    stage_recursive_delays(
                        &evaluator.program.graph.recursive_delays,
                        &mut evaluator.state,
                        &value,
                    );
                    value
                }
                GraphTickOutcome::Canonical => {
                    evaluator.evaluate_canonical_infallible(environment_values)
                }
                GraphTickOutcome::NotHandled => evaluator.evaluate_infallible_and_stage_with_plan(
                    environment_values,
                    step.quickening_plan.as_ref(),
                    published_scalars,
                ),
            }
        } else if let Some(retained) = retained_environment_values.as_deref() {
            evaluator.evaluate_and_stage_with_retained_environment(environment_values, retained)?
        } else {
            evaluator.evaluate_and_stage(environment_values)?
        };
        self.publish(index, ScalarValue::from_untyped_value(&value));
        Self::publish_environment_value::<RETAIN_VALUES>(
            environment_values,
            retained_environment_values,
            step.slot,
            value,
        );
        Ok(())
    }

    fn publish_environment_value<const RETAIN_VALUES: bool>(
        environment_values: &mut [Value],
        retained_environment_values: Option<&mut [Value]>,
        slot: EnvironmentSlot,
        value: Value,
    ) {
        if RETAIN_VALUES
            && value != Value::NoVal
            && let Some(retained) = retained_environment_values
        {
            retained[slot.index()] = value.clone();
        }
        environment_values[slot.index()] = value;
    }

    fn replay_canonical(
        &mut self,
        plan: &ScheduledExecutionPlan,
        environment_values: &mut [Value],
    ) {
        for planned in plan.streams.iter() {
            let stream = planned.stream;
            let evaluator = &mut self.evaluators[stream.index()];
            let value = evaluator.evaluate_canonical_infallible(environment_values);
            // Replay restores node and lifting state from the last native row; native temporal
            // state is already committed, so this replay is not another logical tick.
            evaluator.discard_staged_temporal_state();
            environment_values[planned.output.environment().index()] = value;
        }
    }

    fn evaluate_canonical_run(
        &mut self,
        plan: &ScheduledExecutionPlan,
        environment_values: &mut [Value],
    ) {
        for planned in plan.streams.iter() {
            let stream = planned.stream;
            let index = stream.index();
            let value = self.evaluators[index].evaluate_canonical_infallible(environment_values);
            self.published_scalars[index] = ScalarValue::from_untyped_value(&value);
            environment_values[planned.output.environment().index()] = value;
        }
    }

    #[inline]
    fn publish(&mut self, stream: usize, value: Option<ScalarValue>) {
        self.published_scalars[stream] = value;
    }

    fn programs_rc(&self) -> Vec<Rc<StreamProgram>> {
        self.evaluators
            .iter()
            .map(|evaluator| Rc::clone(&evaluator.program))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::monitor::test_support::execution;
    use crate::{CheckedDsrvSpecification, DsrvSpecification};

    #[derive(Debug, PartialEq, Eq)]
    enum LayoutSnapshot {
        ScalarRun(Vec<usize>),
        Graph(usize),
    }

    fn layout_snapshot(monitor: &DataflowMonitor) -> Vec<LayoutSnapshot> {
        execution(monitor)
            .engine
            .active_plan
            .quick
            .main_steps
            .iter()
            .map(|step| match step {
                QuickStep::ScalarRun(run) => {
                    LayoutSnapshot::ScalarRun(run.iter().map(|step| step.stream.index()).collect())
                }
                QuickStep::Graph(step) => LayoutSnapshot::Graph(step.stream.index()),
            })
            .collect()
    }

    fn input_row(monitor: &DataflowMonitor, values: &[(&str, Value)]) -> Vec<Value> {
        monitor
            .input_vars()
            .iter()
            .map(|variable| {
                values
                    .iter()
                    .find_map(|(name, value)| {
                        (variable == &VarName::new(name)).then(|| value.clone())
                    })
                    .unwrap()
            })
            .collect()
    }

    fn execution_with_ranges(
        monitor: &DataflowMonitor,
        source_order: &[StreamId],
        main_order: &[StreamId],
    ) -> MonitorExecution {
        let current = execution(monitor);
        MonitorExecution::new_with_source_prelude(
            current.evaluators.programs_rc(),
            current.stream_slots,
            source_order,
            main_order,
            &current.temporal_streams,
        )
    }

    fn steps_snapshot(steps: &[QuickStep]) -> Vec<LayoutSnapshot> {
        steps
            .iter()
            .map(|step| match step {
                QuickStep::ScalarRun(run) => {
                    LayoutSnapshot::ScalarRun(run.iter().map(|step| step.stream.index()).collect())
                }
                QuickStep::Graph(step) => LayoutSnapshot::Graph(step.stream.index()),
            })
            .collect()
    }

    #[test]
    fn disabling_quickening_uses_only_canonical_graph_steps() {
        let specification = "in x: Int\n\
            aux a: Int\n\
            out b: Int\n\
            a = x + 1\n\
            b = a * 2"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();
        monitor.set_quickening(false);
        let execution = execution(&monitor);

        assert!(!execution.engine.quickening);
        for step in execution
            .engine
            .active_plan
            .quick
            .source_steps
            .iter()
            .chain(execution.engine.active_plan.quick.main_steps.iter())
        {
            assert!(matches!(
                step,
                QuickStep::Graph(GraphStep {
                    quickening_plan: None,
                    ..
                })
            ));
        }
    }

    #[test]
    fn scalar_streams_form_one_execution_run() {
        let specification = "in x: Int\n\
            aux a: Int\n\
            aux b: Int\n\
            out c: Int\n\
            a = x + 1\n\
            b = a * 2\n\
            c = b - 3"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let monitor = DataflowMonitor::compile_checked(specification).unwrap();

        assert_eq!(
            layout_snapshot(&monitor),
            [LayoutSnapshot::ScalarRun(vec![0, 1, 2])]
        );
    }

    #[test]
    fn graph_stream_splits_scalar_runs() {
        let specification = "in x: Int\n\
            in choose: Bool\n\
            aux a: Int\n\
            aux b: Int\n\
            aux c: Int\n\
            aux d: Int\n\
            out e: Int\n\
            a = x + 1\n\
            b = a + 1\n\
            c = if choose then b else x\n\
            d = c + 1\n\
            e = d + 1"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let monitor = DataflowMonitor::compile_checked(specification).unwrap();

        assert_eq!(
            layout_snapshot(&monitor),
            [
                LayoutSnapshot::ScalarRun(vec![0, 1]),
                LayoutSnapshot::Graph(2),
                LayoutSnapshot::ScalarRun(vec![3, 4]),
            ]
        );
    }

    #[test]
    fn temporal_stream_splits_scalar_runs() {
        let specification = "in x: Int\n\
            aux current: Int\n\
            aux delayed: Int\n\
            out result: Int\n\
            current = x + 1\n\
            delayed = default(current[1], 0) + 1\n\
            result = delayed * 2"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let monitor = DataflowMonitor::compile_checked(specification).unwrap();

        assert_eq!(
            layout_snapshot(&monitor),
            [
                LayoutSnapshot::ScalarRun(vec![0]),
                LayoutSnapshot::Graph(1),
                LayoutSnapshot::ScalarRun(vec![2]),
            ]
        );
    }

    #[test]
    fn semantic_plan_records_schedule_publication_effects_and_state_identity() {
        let specification = "in x: Int\n\
            aux base: Int\n\
            out result: Int\n\
            base = x + 1\n\
            result = default(base[1], 0) + base"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let monitor = DataflowMonitor::compile_checked(specification).unwrap();
        let plan = &execution(&monitor).engine.active_plan.semantic;

        assert_eq!(
            plan.order().map(StreamId::index).collect::<Vec<_>>(),
            [0, 1]
        );
        assert_eq!(plan.streams[0].output.environment().index(), 1);
        assert_eq!(plan.streams[1].output.environment().index(), 2);
        assert!(!plan.streams[0].effects.reads_temporal_state);
        assert!(plan.streams[1].effects.reads_temporal_state);
        assert!(plan.streams[1].effects.writes_temporal_state);
        assert_eq!(plan.commit_streams.as_ref(), [StreamId::new(1)]);
        let state = plan.streams[1].temporal.operations[0].state();
        assert_eq!(state.stream, StreamId::new(1));
        assert_eq!(state.node, NodeId::new(0));
    }

    #[test]
    fn source_boundary_is_part_of_cached_plan_identity() {
        let specification = "in x: Int\n\
            aux source: Int\n\
            out result: Int\n\
            source = x + 1\n\
            result = source * 2"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let monitor = DataflowMonitor::compile_checked(specification).unwrap();
        let mut execution =
            execution_with_ranges(&monitor, &[StreamId::new(0)], &[StreamId::new(1)]);
        let source_plan = execution.engine.active_plan.semantic.id;

        execution.select_schedule_ranges(
            &[],
            &[StreamId::new(0), StreamId::new(1)],
            execution.stream_slots,
        );
        let main_plan = execution.engine.active_plan.semantic.id;
        assert_ne!(main_plan, source_plan);
        assert_eq!(execution.engine.active_plan.semantic.source_stream_count, 0);
        assert_eq!(
            execution
                .engine
                .active_plan
                .semantic
                .order()
                .map(StreamId::index)
                .collect::<Vec<_>>(),
            [0, 1]
        );

        execution.select_schedule_ranges(
            &[StreamId::new(0)],
            &[StreamId::new(1)],
            execution.stream_slots,
        );
        assert_eq!(execution.engine.active_plan.semantic.id, source_plan);
    }

    #[test]
    fn source_and_main_have_separate_scalar_runs() {
        let specification = "in x: Int\n\
            aux source: Int\n\
            aux middle: Int\n\
            out result: Int\n\
            source = x + 1\n\
            middle = source * 2\n\
            result = middle - 3"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let monitor = DataflowMonitor::compile_checked(specification).unwrap();
        let execution = execution_with_ranges(
            &monitor,
            &[StreamId::new(0)],
            &[StreamId::new(1), StreamId::new(2)],
        );

        assert_eq!(
            steps_snapshot(&execution.engine.active_plan.quick.source_steps),
            [LayoutSnapshot::ScalarRun(vec![0])]
        );
        assert_eq!(
            steps_snapshot(&execution.engine.active_plan.quick.main_steps),
            [LayoutSnapshot::ScalarRun(vec![1, 2])]
        );
    }

    #[test]
    fn source_scalar_publication_is_available_to_main_range() {
        let specification = "in x: Int\n\
            aux source: Int\n\
            out result: Int\n\
            source = x + 1\n\
            result = source * 2"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let monitor = DataflowMonitor::compile_checked(specification).unwrap();
        let mut execution =
            execution_with_ranges(&monitor, &[StreamId::new(0)], &[StreamId::new(1)]);
        let mut environment =
            vec![Value::NoVal; execution.engine.active_plan.semantic.environment_len];
        environment[0] = Value::Int(3);

        execution
            .evaluate_source_prelude(&mut environment, None)
            .unwrap();
        let source_slot = execution.stream_slots.slot(StreamId::new(0)).index();
        assert_eq!(environment[source_slot], Value::Int(4));
        environment[source_slot] = Value::NoVal;
        execution
            .evaluate_main_and_commit(&mut environment, None)
            .unwrap();
        assert_eq!(
            environment[execution.stream_slots.slot(StreamId::new(1)).index()],
            Value::Int(8)
        );
    }

    #[test]
    fn temporal_source_moved_to_main_is_evaluated_once_per_tick() {
        let specification = "in x: Int\n\
            aux delayed: Int\n\
            out result: Int\n\
            delayed = default(x[1], 0)\n\
            result = delayed"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let monitor = DataflowMonitor::compile_checked(specification).unwrap();
        let mut execution =
            execution_with_ranges(&monitor, &[StreamId::new(0)], &[StreamId::new(1)]);
        #[cfg(feature = "jit")]
        execution.enable_jit(JitConfig::eager());
        let mut environment =
            vec![Value::NoVal; execution.engine.active_plan.semantic.environment_len];
        let output = execution.stream_slots.slot(StreamId::new(1)).index();

        environment[0] = Value::Int(10);
        execution
            .evaluate_source_prelude(&mut environment, None)
            .unwrap();
        execution
            .evaluate_main_and_commit(&mut environment, None)
            .unwrap();
        assert_eq!(environment[output], Value::Int(0));

        execution.select_schedule_ranges(
            &[],
            &[StreamId::new(0), StreamId::new(1)],
            execution.stream_slots,
        );
        for (input, expected) in [(20, 10), (30, 20)] {
            environment[0] = Value::Int(input);
            execution
                .evaluate_source_prelude(&mut environment, None)
                .unwrap();
            execution
                .evaluate_main_and_commit(&mut environment, None)
                .unwrap();
            assert_eq!(environment[output], Value::Int(expected));
        }
    }

    #[cfg(feature = "jit")]
    #[test]
    fn temporal_source_barrier_prohibits_fused_kernel() {
        let specification = "in x: Int\n\
            out result: Bool\n\
            result = x > 3 && default(x[1], 4) > 3 && default(x[2], 4) > 3"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let monitor = DataflowMonitor::compile_checked(specification).unwrap();
        let mut execution = execution_with_ranges(&monitor, &[StreamId::new(0)], &[]);
        execution.enable_jit(JitConfig::eager());

        let report = execution.jit_report().unwrap();
        assert_eq!(report.plan(), JitPlan::PerStream);
        assert_eq!(report.compiled_artifacts(), 1);
    }

    #[cfg(feature = "jit")]
    #[test]
    fn source_barrier_uses_and_retains_global_per_stream_artifacts() {
        let specification = "in x: Int\n\
            aux source: Int\n\
            out result: Int\n\
            source = x + 1\n\
            result = source * 2"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let monitor = DataflowMonitor::compile_checked(specification).unwrap();
        let mut execution =
            execution_with_ranges(&monitor, &[StreamId::new(0)], &[StreamId::new(1)]);
        execution.enable_jit(JitConfig::eager());

        let report = execution.jit_report().unwrap();
        assert_eq!(report.plan(), JitPlan::PerStream);
        assert_eq!(report.compiled_artifacts(), 2);
        assert!(report.unsupported_streams().is_empty());
        let artifacts = execution.jit_artifact_count();

        execution.select_schedule_ranges(
            &[],
            &[StreamId::new(0), StreamId::new(1)],
            execution.stream_slots,
        );
        assert_eq!(execution.jit_artifact_count(), artifacts);
        assert_eq!(execution.jit_report().unwrap().plan(), JitPlan::PerStream);
    }

    #[cfg(feature = "jit")]
    #[test]
    fn hotness_advances_once_across_both_ranges() {
        let specification = "in x: Int\n\
            aux source: Int\n\
            out result: Int\n\
            source = x + 1\n\
            result = source * 2"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let monitor = DataflowMonitor::compile_checked(specification).unwrap();
        let mut execution =
            execution_with_ranges(&monitor, &[StreamId::new(0)], &[StreamId::new(1)]);
        execution.enable_jit(JitConfig::after_events(1));
        let mut environment =
            vec![Value::NoVal; execution.engine.active_plan.semantic.environment_len];

        environment[0] = Value::Int(3);
        execution
            .evaluate_source_prelude(&mut environment, None)
            .unwrap();
        execution
            .evaluate_main_and_commit(&mut environment, None)
            .unwrap();
        assert_eq!(execution.jit_report().unwrap().plan(), JitPlan::Pending);

        environment[0] = Value::Int(4);
        execution
            .evaluate_source_prelude(&mut environment, None)
            .unwrap();
        assert_eq!(execution.jit_report().unwrap().plan(), JitPlan::PerStream);
        execution
            .evaluate_main_and_commit(&mut environment, None)
            .unwrap();
    }

    #[cfg(feature = "jit")]
    #[test]
    fn jit_reports_unsupported_streams_from_both_ranges() {
        let specification = "in x: Str\n\
            aux source: Str\n\
            out result: Str\n\
            source = x\n\
            result = source"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let monitor = DataflowMonitor::compile_checked(specification).unwrap();
        let mut execution =
            execution_with_ranges(&monitor, &[StreamId::new(0)], &[StreamId::new(1)]);
        execution.enable_jit(JitConfig::eager());

        let report = execution.jit_report().unwrap();
        assert_eq!(report.plan(), JitPlan::Unavailable);
        assert_eq!(report.unsupported_streams(), [0, 1]);
    }

    #[test]
    fn dynamic_schedule_reuses_cached_plan_identity() {
        let specification = "in x: Int\n\
            in a_source: Str\n\
            in b_source: Str\n\
            out a: Int\n\
            out b: Int\n\
            a = dynamic(a_source: Int)\n\
            b = dynamic(b_source: Int)"
            .parse::<DsrvSpecification>()
            .unwrap();
        let mut monitor = DataflowMonitor::compile_untyped(specification).unwrap();
        let mut output = [Value::NoVal, Value::NoVal];
        let forward_plan_id = execution(&monitor).engine.active_plan.semantic.id;

        let reverse = input_row(
            &monitor,
            &[
                ("x", Value::Int(10)),
                ("a_source", Value::Str("b + 1".into())),
                ("b_source", Value::Str("x".into())),
            ],
        );
        monitor.evaluate(&reverse, &mut output).unwrap();
        assert_eq!(
            layout_snapshot(&monitor),
            [LayoutSnapshot::Graph(1), LayoutSnapshot::Graph(0)]
        );
        assert_eq!(execution(&monitor).engine.cached_plans.len(), 1);
        assert_ne!(
            execution(&monitor).engine.active_plan.semantic.id,
            forward_plan_id
        );

        let forward = input_row(
            &monitor,
            &[
                ("x", Value::Int(20)),
                ("a_source", Value::Str("x".into())),
                ("b_source", Value::Str("a + 1".into())),
            ],
        );
        monitor.evaluate(&forward, &mut output).unwrap();
        assert_eq!(
            layout_snapshot(&monitor),
            [LayoutSnapshot::Graph(0), LayoutSnapshot::Graph(1)]
        );
        assert_eq!(execution(&monitor).engine.cached_plans.len(), 1);
        assert_eq!(
            execution(&monitor).engine.active_plan.semantic.id,
            forward_plan_id
        );

        monitor.evaluate(&forward, &mut output).unwrap();
        assert_eq!(execution(&monitor).engine.cached_plans.len(), 1);
    }
}
