use super::super::super::environment::EnvironmentSlot;
use super::super::super::execution_plan::{StreamId, StreamSlots};
use super::super::super::history::{HistoryAccess, HistoryId};
use super::super::super::ir::StreamProgram;
use super::super::evaluator::Evaluator;
use super::super::interpreter::stage_recursive_delays;
use super::super::jit::{FusedTickOutcome, GraphTickOutcome, Jit};
use super::super::quickening::{self, ScalarValue};
use super::super::scheduled_plan::ScheduledExecutionPlan;
use super::plan::{GraphStep, PlanBundle, QuickStep, ScalarStep};
use super::{EvaluatorArena, MonitorExecution};
use crate::core::Value;
use crate::dataflow::DataflowEvaluationError;
#[cfg(feature = "jit")]
use crate::dataflow::{JitConfig, JitReport};
use std::rc::Rc;

impl MonitorExecution {
    pub(super) fn activate_execution_tiers(&mut self) {
        if self.engine.jit.activation_due() {
            #[cfg(feature = "jit")]
            self.engine.jit.activate_pending(
                &self.engine.active_plan.semantic,
                &mut self.evaluators.evaluators,
            );
        }
    }

    #[inline]
    pub(super) fn evaluate_source_range(
        &mut self,
        environment_values: &mut [Value],
        retained_environment_values: Option<&mut [Value]>,
        history_access: Option<HistoryAccess<'_>>,
    ) -> Result<(), DataflowEvaluationError> {
        self.evaluators.evaluate_steps::<true>(
            &mut self.engine.jit,
            &self.engine.active_plan.quick.source_steps,
            environment_values,
            retained_environment_values,
            self.stream_slots,
            history_access,
        )
    }

    #[inline]
    pub(super) fn evaluate_main_range(
        &mut self,
        environment_values: &mut [Value],
        retained_environment_values: Option<&mut [Value]>,
        history_access: Option<HistoryAccess<'_>>,
    ) -> Result<(), DataflowEvaluationError> {
        self.evaluators.evaluate_steps::<true>(
            &mut self.engine.jit,
            &self.engine.active_plan.quick.main_steps,
            environment_values,
            retained_environment_values,
            self.stream_slots,
            history_access,
        )
    }

    pub(super) fn evaluate_unbarriered_tick(
        &mut self,
        environment_values: &mut [Value],
        history_access: Option<HistoryAccess<'_>>,
    ) -> Result<(), DataflowEvaluationError> {
        debug_assert!(!self.engine.active_plan.semantic.has_source_barrier());
        match self.engine.jit.evaluate_fused(
            &mut self.evaluators.evaluators,
            environment_values,
            &mut self.evaluators.published_scalars,
            history_access,
        ) {
            FusedTickOutcome::Completed | FusedTickOutcome::CompletedAndCommitted => {
                return Ok(());
            }
            FusedTickOutcome::CanonicalFallback => {
                if let Some(mut replay_environment) = self.engine.jit.take_replay_environment() {
                    self.evaluators.replay_canonical(
                        &self.engine.active_plan.semantic,
                        &mut replay_environment,
                    );
                }
                self.evaluators
                    .evaluate_canonical_run(&self.engine.active_plan.semantic, environment_values);
                self.commit_active_plan(environment_values, None, None);
                return Ok(());
            }
            FusedTickOutcome::NotAvailable => {}
        }
        let result = self.evaluators.evaluate_steps::<false>(
            &mut self.engine.jit,
            &self.engine.active_plan.quick.main_steps,
            environment_values,
            None,
            self.stream_slots,
            history_access,
        );
        if result.is_ok() {
            self.commit_active_plan(environment_values, None, history_access);
        }
        result
    }

    /// Configures schedule-owned quickening for top-level stream evaluators. Nested evaluators own
    /// their plans independently because they execute outside the `PlanBundle` quick plan.
    pub(in crate::dataflow) fn set_quickening(&mut self, enabled: bool) {
        if self.engine.quickening == enabled {
            return;
        }
        self.evaluators.materialize_quickening();
        self.evaluators.detach_top_level_quick_plans();
        self.engine.quickening = enabled;
        self.engine.active_plan =
            PlanBundle::new((*self.engine.active_plan.semantic).clone(), enabled);
        if enabled {
            self.evaluators.rebuild_top_level_quickening();
        }
        self.engine.cached_plans.clear();
    }

    #[cfg(test)]
    pub(crate) fn quickening_enabled(&self) -> bool {
        self.engine.quickening
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn enable_jit(&mut self, config: JitConfig) {
        self.engine.jit.configure(
            &self.engine.active_plan.semantic,
            &mut self.evaluators.evaluators,
            config,
        );
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn jit_report(&self) -> Option<&JitReport> {
        self.engine.jit.report()
    }

    #[cfg(all(test, feature = "jit"))]
    pub(in crate::dataflow) fn jit_artifact_count(&self) -> usize {
        self.engine
            .jit
            .compiled_artifact_count(&self.evaluators.evaluators)
    }

    pub(in crate::dataflow) fn reset_after_context_transfer(&mut self) {
        self.evaluators.published_scalars.fill(None);
        #[cfg(feature = "jit")]
        for evaluator in &mut self.evaluators.evaluators {
            evaluator.reset_native_tier();
        }
        self.engine.jit.reset_after_context_transfer();
    }

    #[inline]
    pub(super) fn commit_temporal_state(
        &mut self,
        stream: StreamId,
        environment_values: &[Value],
        retained_environment_values: Option<&[Value]>,
        history_access: Option<HistoryAccess<'_>>,
    ) {
        let evaluator = &mut self.evaluators.evaluators[stream.index()];
        if history_access.is_none() {
            if self.engine.jit.commit_graph(
                evaluator,
                environment_values,
                retained_environment_values,
            ) {
                return;
            }
        }
        evaluator.commit_temporal_state_with_history(
            environment_values,
            retained_environment_values,
            history_access,
        );
    }
}

impl EvaluatorArena {
    pub(super) fn new_with_history(
        programs: Vec<Rc<StreamProgram>>,
        history_bindings: &[Option<HistoryId>],
    ) -> Self {
        let published_scalars = vec![None; programs.len()].into_boxed_slice();
        let evaluators = programs
            .into_iter()
            .map(|program| Evaluator::new_with_history(program, history_bindings))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            evaluators,
            published_scalars,
        }
    }

    pub(super) fn detach_top_level_quick_plans(&mut self) {
        for evaluator in &mut self.evaluators {
            evaluator.detach_top_level_quick_plan();
        }
    }

    fn materialize_quickening(&mut self) {
        for evaluator in &mut self.evaluators {
            evaluator.materialize_quickening();
        }
    }

    fn rebuild_top_level_quickening(&mut self) {
        for evaluator in &mut self.evaluators {
            evaluator.rebuild_top_level_quickening();
        }
    }

    #[cfg(test)]
    pub(super) fn delay_ring_lengths(&self) -> Vec<usize> {
        self.evaluators
            .iter()
            .flat_map(|evaluator| evaluator.tier_states.canonical.delay_ring_lengths())
            .collect()
    }

    #[inline]
    fn evaluator_with_published(
        &mut self,
        stream: usize,
    ) -> (&mut Evaluator, &[Option<ScalarValue>]) {
        (&mut self.evaluators[stream], &self.published_scalars)
    }

    pub(super) fn evaluate_steps<const RETAIN_VALUES: bool>(
        &mut self,
        jit: &mut Jit,
        steps: &[QuickStep],
        environment_values: &mut [Value],
        mut retained_environment_values: Option<&mut [Value]>,
        stream_slots: StreamSlots,
        history_access: Option<HistoryAccess<'_>>,
    ) -> Result<(), DataflowEvaluationError> {
        // Published sources only name streams earlier in the combined two-range order. Availability
        // intentionally carries across the source barrier, while scalar runs cannot cross it.
        for step in steps {
            match step {
                QuickStep::ScalarRun(run) => self.evaluate_scalar_run::<RETAIN_VALUES>(
                    run,
                    environment_values,
                    retained_environment_values.as_deref_mut(),
                    history_access,
                ),
                QuickStep::Graph(step) => self.evaluate_graph::<RETAIN_VALUES>(
                    jit,
                    step,
                    environment_values,
                    retained_environment_values.as_deref_mut(),
                    stream_slots,
                    history_access,
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
        history_access: Option<HistoryAccess<'_>>,
    ) {
        for step in run {
            let index = step.stream.index();
            let (evaluator, published_scalars) = self.evaluator_with_published(index);
            let result = if let Some(history_access) = history_access {
                evaluator.evaluate_single_scalar_with_history(
                    environment_values,
                    &step.plan,
                    published_scalars,
                    Some(history_access),
                )
            } else {
                evaluator.evaluate_single_scalar_with_plan(
                    environment_values,
                    &step.plan,
                    published_scalars,
                )
            };
            let value = match result {
                quickening::DirectScalarResult::Scalar(value) => {
                    self.publish(index, Some(value));
                    value.into_value()
                }
                quickening::DirectScalarResult::Canonical(value) => {
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
        history_access: Option<HistoryAccess<'_>>,
    ) -> Result<(), DataflowEvaluationError> {
        let index = step.stream.index();
        let (evaluator, published_scalars) = self.evaluator_with_published(index);
        let value = if evaluator.program.is_infallible() {
            let outcome = if history_access.is_some() {
                GraphTickOutcome::NotAvailable
            } else {
                let outcome = jit.evaluate_graph(
                    evaluator,
                    environment_values,
                    published_scalars,
                    stream_slots,
                );
                if let GraphTickOutcome::Value(value) = &outcome {
                    stage_recursive_delays(
                        &evaluator.program.graph.recursive_delays,
                        evaluator.tier_states.canonical.as_mut(),
                        value,
                    );
                }
                outcome
            };
            match outcome {
                GraphTickOutcome::Value(value) => value,
                GraphTickOutcome::CanonicalFallback => match history_access {
                    Some(history_access) => evaluator.evaluate_canonical_infallible_with_history(
                        environment_values,
                        Some(history_access),
                    ),
                    None => evaluator.evaluate_canonical_infallible(environment_values),
                },
                GraphTickOutcome::NotAvailable => match history_access {
                    Some(history_access) => evaluator.evaluate_infallible_and_stage_with_history(
                        environment_values,
                        step.schedule_plan.as_ref(),
                        published_scalars,
                        Some(history_access),
                        false,
                    ),
                    None => evaluator.evaluate_infallible_and_stage_with_plan(
                        environment_values,
                        step.schedule_plan.as_ref(),
                        published_scalars,
                        step.adaptive_candidate,
                    ),
                },
            }
        } else if let Some(history_access) = history_access {
            if let Some(retained) = retained_environment_values.as_deref() {
                evaluator.evaluate_and_stage_with_retained_environment_and_history(
                    environment_values,
                    retained,
                    Some(history_access),
                )?
            } else {
                evaluator
                    .evaluate_and_stage_with_history(environment_values, Some(history_access))?
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

    pub(super) fn replay_canonical(
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

    pub(super) fn evaluate_canonical_run(
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

    #[cfg(test)]
    pub(super) fn programs_rc(&self) -> Vec<Rc<StreamProgram>> {
        self.evaluators
            .iter()
            .map(|evaluator| Rc::clone(&evaluator.program))
            .collect()
    }
}
