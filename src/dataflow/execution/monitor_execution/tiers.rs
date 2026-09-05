//! Tier selection, and the state handoffs that make it safe.
//!
//! Canonical, quickened and native execution are alternative routes to the same one-tick contract.
//! This module picks the route for each step and, crucially, moves semantic state along with it:
//! nothing may change executor while a different executor still owns its state.
//!
//! `materialize_authoritative_state` is the funnel enforcing that rule. Every schedule repair,
//! context transfer, and tier activation calls it first, so whichever tier last mutated state has
//! written it back to the canonical arena before the route changes underneath it.

use std::ops::Range;

use super::super::super::environment::EnvironmentSlot;
use super::super::super::history::{HistoryAccess, HistoryId};
use super::super::super::ir::StreamProgram;
use super::super::super::stream_id::StreamId;
use super::super::evaluator::Evaluator;

use super::super::evaluator_state::EvaluatorState;
use super::super::jit::{Jit, NativeRegionOutcome, WholeTickOutcome};
#[cfg(feature = "jit")]
use super::super::jit::{NativeScalarRegion, NativeTemporalMonitor, PreparedDirectJit};
use super::super::quickening::{
    CanonicalArena, QuickenedRegionPlan, QuickenedRegionState, ScalarValue,
};
use super::super::scheduled_plan::ScheduledExecutionPlan;
use super::plan::{ExecutableSegment, ExecutionPlan, ExecutionStep, GraphStep, PlanIdentity};
use super::{AuthoritativeTier, EvaluatorArena, MonitorExecution, RegionAuthority, RegionStates};
use crate::core::Value;
use crate::dataflow::DataflowEvaluationError;
#[cfg(feature = "jit")]
use crate::dataflow::typed::TypedIoLayout;
#[cfg(feature = "jit")]
use crate::dataflow::{JitConfig, JitReport};
use std::rc::Rc;

#[derive(Clone, Copy, Default)]
pub(super) struct ExecutionUsage {
    regions: bool,
}

impl MonitorExecution {
    pub(super) fn activate_execution_tiers(&mut self) {
        if !self.engine.jit.activation_due() {
            return;
        }

        // Activation replaces region executors, so materialize their current authority first.
        self.materialize_authoritative_state();
        #[cfg(feature = "jit")]
        self.engine.jit.activate_pending(
            &self.engine.active_plan.semantic,
            &self.engine.active_plan.regions,
        );
        self.configure_active_region_states();
    }

    /// Materialize whichever tier most recently changed semantic state.
    ///
    /// Native state has priority over the schedule-wide native state. The latter is intentionally
    /// not materialized after a native tick because it is then an old snapshot of the same
    /// evaluator state and would overwrite the canonical handoff.
    pub(super) fn materialize_authoritative_state(&mut self) {
        match self.authoritative_tier {
            AuthoritativeTier::WholeNative => {
                self.engine.jit.materialize_into(
                    &mut self.evaluators.evaluators,
                    &self.engine.active_plan.semantic,
                );
                self.authoritative_tier = AuthoritativeTier::Canonical;
            }
            AuthoritativeTier::Regions(identity) => {
                let active_identity = self.engine.active_plan.identity;
                debug_assert_eq!(identity, active_identity);
                self.evaluators.materialize_regions(
                    &mut self.engine.jit,
                    identity,
                    &self.engine.active_plan.quickened_regions,
                );
                self.authoritative_tier = AuthoritativeTier::Canonical;
            }
            AuthoritativeTier::Canonical => {}
        }
    }

    /// Install fresh quickened region state from the canonical evaluators.
    pub(super) fn configure_active_region_states(&mut self) {
        let identity = self.engine.active_plan.identity;
        let regions = &self.engine.active_plan.quickened_regions;
        self.evaluators
            .configure_regions(identity, regions, self.engine.quickening);
        self.authoritative_tier = if self.engine.quickening && !regions.is_empty() {
            AuthoritativeTier::Regions(identity)
        } else {
            AuthoritativeTier::Canonical
        };
    }

    #[inline]
    pub(super) fn evaluate_source_range(
        &mut self,
        environment_values: &mut [Value],
        retained_environment_values: Option<&mut [Value]>,
        history_access: Option<HistoryAccess<'_>>,
    ) -> Result<(), DataflowEvaluationError> {
        let usage = self.evaluators.evaluate_steps::<true>(
            &mut self.engine.jit,
            self.engine.active_plan.identity,
            self.engine.quickening,
            &self.engine.active_plan.quickened_regions,
            &self.engine.active_plan.source_steps,
            environment_values,
            retained_environment_values,
            history_access,
        )?;
        self.record_execution_usage(usage);
        Ok(())
    }

    #[inline]
    pub(super) fn evaluate_main_range(
        &mut self,
        environment_values: &mut [Value],
        retained_environment_values: Option<&mut [Value]>,
        history_access: Option<HistoryAccess<'_>>,
    ) -> Result<(), DataflowEvaluationError> {
        let usage = self.evaluators.evaluate_steps::<true>(
            &mut self.engine.jit,
            self.engine.active_plan.identity,
            self.engine.quickening,
            &self.engine.active_plan.quickened_regions,
            &self.engine.active_plan.main_steps,
            environment_values,
            retained_environment_values,
            history_access,
        )?;
        self.record_execution_usage(usage);
        Ok(())
    }

    pub(super) fn evaluate_unbarriered_tick(
        &mut self,
        environment_values: &mut [Value],
        history_access: Option<HistoryAccess<'_>>,
    ) -> Result<(), DataflowEvaluationError> {
        debug_assert!(!self.engine.active_plan.semantic.has_source_barrier());
        match self.engine.jit.evaluate_whole(
            &mut self.evaluators.evaluators,
            environment_values,
            &mut self.evaluators.published_scalars,
            history_access,
        ) {
            WholeTickOutcome::Completed | WholeTickOutcome::CompletedAndCommitted => {
                self.authoritative_tier = AuthoritativeTier::WholeNative;
                return Ok(());
            }
            WholeTickOutcome::CanonicalFallback => {
                if let Some(mut replay_environment) = self.engine.jit.take_replay_environment() {
                    self.evaluators.replay_canonical(
                        &self.engine.active_plan.semantic,
                        &mut replay_environment,
                    );
                }
                self.evaluators
                    .evaluate_canonical_run(&self.engine.active_plan.semantic, environment_values);
                self.authoritative_tier = AuthoritativeTier::Canonical;
                self.commit_active_plan(environment_values, None, None);
                return Ok(());
            }
            WholeTickOutcome::NotAvailable => {}
        }
        let result = self.evaluators.evaluate_steps::<false>(
            &mut self.engine.jit,
            self.engine.active_plan.identity,
            self.engine.quickening,
            &self.engine.active_plan.quickened_regions,
            &self.engine.active_plan.main_steps,
            environment_values,
            None,
            history_access,
        );
        match result {
            Ok(usage) => {
                self.record_execution_usage(usage);
                self.commit_active_plan(environment_values, None, history_access);
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    fn record_execution_usage(&mut self, usage: ExecutionUsage) {
        if usage.regions && !matches!(self.authoritative_tier, AuthoritativeTier::WholeNative) {
            self.authoritative_tier = AuthoritativeTier::Regions(self.engine.active_plan.identity);
        }
    }

    /// Turns the quick tier on or off. Region selection depends on the flag, so the active plan is
    /// rebuilt and its region state reinstalled from the canonical arena.
    pub(in crate::dataflow) fn set_quickening(&mut self, enabled: bool) {
        if self.engine.quickening == enabled {
            return;
        }
        self.materialize_authoritative_state();
        self.engine.quickening = enabled;
        let generation = self.engine.next_plan_generation;
        self.engine.next_plan_generation = self
            .engine
            .next_plan_generation
            .checked_add(1)
            .expect("execution plan generation overflow");
        self.engine.active_plan = ExecutionPlan::new(
            (*self.engine.active_plan.semantic).clone(),
            enabled,
            generation,
        );
        self.configure_active_region_states();
        self.engine.cached_plans.clear();
    }

    #[cfg(test)]
    pub(crate) fn quickening_enabled(&self) -> bool {
        self.engine.quickening
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn enable_jit(&mut self, config: JitConfig) {
        // Native compilation starts from materialized region state.
        self.materialize_authoritative_state();
        self.engine.jit.configure(
            &self.engine.active_plan.semantic,
            &self.engine.active_plan.regions,
            config,
            None,
        );
        self.configure_active_region_states();
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn enable_typed_jit(
        &mut self,
        config: JitConfig,
        layout: TypedIoLayout,
    ) {
        self.materialize_authoritative_state();
        self.engine.jit.configure(
            &self.engine.active_plan.semantic,
            &self.engine.active_plan.regions,
            config,
            Some(layout),
        );
        self.configure_active_region_states();
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn jit_report(&self) -> Option<&JitReport> {
        self.engine.jit.report()
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn direct_entry_ready(&self) -> bool {
        self.engine.jit.direct_entry_ready()
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn into_direct_jit(
        mut self,
        layout: TypedIoLayout,
        history_access: Option<HistoryAccess<'_>>,
    ) -> Result<PreparedDirectJit, ()> {
        if self.tick_in_progress {
            return Err(());
        }
        self.materialize_authoritative_state();
        let plan = &self.engine.active_plan.semantic;
        if plan.has_temporal_state() {
            let evaluator = NativeTemporalMonitor::compile(plan, Some(&layout))
                .map_err(|_| ())?
                .ok_or(())?;
            evaluator
                .into_prepared_direct(&mut self.evaluators.evaluators, history_access)
                .map_err(|_| ())
        } else {
            let evaluator = NativeScalarRegion::compile(plan, Some(&layout))
                .map_err(|_| ())?
                .ok_or(())?;
            evaluator.into_prepared_direct().map_err(|_| ())
        }
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn take_prepared_direct(
        &mut self,
        history_access: Option<HistoryAccess<'_>>,
    ) -> Result<Option<PreparedDirectJit>, ()> {
        if self.tick_in_progress {
            return Err(());
        }
        self.materialize_authoritative_state();
        let prepared = self
            .engine
            .jit
            .take_prepared_direct(&mut self.evaluators.evaluators, history_access);
        if prepared.is_err() {
            self.configure_active_region_states();
        }
        prepared
    }

    #[cfg(all(test, feature = "jit"))]
    pub(in crate::dataflow) fn fail_next_direct_extraction(&mut self) {
        self.engine.jit.fail_next_direct_extraction();
    }

    #[cfg(all(test, feature = "jit"))]
    pub(in crate::dataflow) fn jit_artifact_count(&self) -> usize {
        self.engine.jit.compiled_artifact_count()
    }

    pub(in crate::dataflow) fn reset_after_context_transfer(&mut self) {
        // Native executors keep no canonical retention of their own, so clearing their runtime
        // bookkeeping is the whole handoff; region state is reinstalled from the transferred arena.
        self.engine.jit.reset_after_context_transfer();

        self.evaluators.published_scalars.fill(None);
        self.configure_active_region_states();
    }

    #[inline]
    pub(super) fn commit_temporal_state(
        &mut self,
        stream: StreamId,
        environment_values: &[Value],
        retained_environment_values: Option<&[Value]>,
        history_access: Option<HistoryAccess<'_>>,
    ) {
        self.evaluators.evaluators[stream.index()].commit_temporal_state_with_history(
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
            region_states: None,
        }
    }

    pub(super) fn configure_regions(
        &mut self,
        identity: PlanIdentity,
        regions: &[QuickenedRegionPlan],
        quickening: bool,
    ) {
        if regions.is_empty() {
            self.region_states = None;
            return;
        }
        let mut authority = Vec::with_capacity(regions.len());
        let states = regions
            .iter()
            .map(|plan| {
                let mut state = QuickenedRegionState::new(plan);
                // A region whose temporal state is not yet scalar stays canonical; the next tick
                // that reaches it retries the handoff.
                let synchronized = plan.synchronize(&mut state, &mut self.evaluators);
                authority.push(if quickening && synchronized {
                    RegionAuthority::Quickened
                } else {
                    RegionAuthority::Canonical
                });
                state
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        self.region_states = Some(RegionStates {
            identity,
            authority: authority.into_boxed_slice(),
            states,
        });
    }

    pub(super) fn materialize_regions(
        &mut self,
        jit: &mut Jit,
        identity: PlanIdentity,
        regions: &[QuickenedRegionPlan],
    ) -> bool {
        let Some(owner) = &self.region_states else {
            return false;
        };
        if owner.identity != identity || owner.states.len() != regions.len() {
            debug_assert_eq!(owner.identity, identity);
            return false;
        }
        for (index, (plan, state)) in regions.iter().zip(owner.states.iter()).enumerate() {
            match owner.authority[index] {
                RegionAuthority::Canonical => {}
                RegionAuthority::Quickened => {
                    plan.materialize(state, &mut self.evaluators);
                }
                RegionAuthority::Native => {
                    jit.materialize_region_into(index, &mut self.evaluators);
                }
            }
        }
        jit.reset_after_context_transfer();
        true
    }

    /// Runs one schedule-owned region, preferring native and then quickened execution.
    ///
    /// `None` means the current row does not belong to the region, so the canonical evaluators own
    /// the tick instead.
    #[inline]
    fn evaluate_region(
        &mut self,
        jit: &mut Jit,
        identity: PlanIdentity,
        quickening: bool,
        region: usize,
        plan: &QuickenedRegionPlan,
        environment_values: &mut [Value],
    ) -> Option<()> {
        if !plan.prepare_inputs(environment_values, &self.published_scalars) {
            return None;
        }
        let owner = self.region_states.as_ref()?;
        if owner.identity != identity || region >= owner.states.len() {
            debug_assert_eq!(owner.identity, identity);
            return None;
        }
        let outcome = jit.evaluate_region(region, &mut self.evaluators, environment_values);
        let Self {
            evaluators,
            published_scalars,
            region_states,
        } = self;
        let owner = region_states.as_mut()?;
        match outcome {
            NativeRegionOutcome::Completed => {
                owner.authority[region] = RegionAuthority::Native;
                Some(())
            }
            NativeRegionOutcome::Deoptimized | NativeRegionOutcome::Unavailable
                if quickening && owner.enter_quickened(region, plan, evaluators) =>
            {
                let executed = plan.execute_prepared(
                    &mut owner.states[region],
                    environment_values,
                    published_scalars,
                    &mut CanonicalArena::empty(),
                );
                debug_assert!(executed, "a stream region has no canonical boundary inputs");
                Some(())
            }
            NativeRegionOutcome::Deoptimized | NativeRegionOutcome::Unavailable => None,
        }
    }

    fn evaluate_region_canonical(
        &mut self,
        jit: &mut Jit,
        identity: PlanIdentity,
        region: usize,
        plan: &QuickenedRegionPlan,
        environment_values: &mut [Value],
        history_access: Option<HistoryAccess<'_>>,
    ) {
        let owner = self
            .region_states
            .as_mut()
            .filter(|owner| owner.identity == identity)
            .expect("active scalar region state is missing");
        match owner.authority[region] {
            RegionAuthority::Canonical => {}
            RegionAuthority::Quickened => {
                plan.materialize(&owner.states[region], &mut self.evaluators);
            }
            RegionAuthority::Native => {
                jit.materialize_region_into(region, &mut self.evaluators);
            }
        }
        owner.authority[region] = RegionAuthority::Canonical;

        for (stream, slot, _) in plan.outputs() {
            let index = stream.index();
            let value = self.evaluators[index]
                .evaluate_static_and_stage(environment_values, history_access);
            self.published_scalars[index] = ScalarValue::from_untyped_value(&value);
            environment_values[slot.index()] = value;
        }
    }

    /// Runs one member of a graph step's region, or its canonical nodes when the row's values do
    /// not match the member's declared kinds.
    ///
    /// A member that declines the row is handed to canonical evaluation and then synchronized back,
    /// so the members after it keep reading live registers and the region state stays a faithful
    /// copy of the canonical arena for the nodes it covers.
    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn evaluate_member(
        &mut self,
        identity: PlanIdentity,
        region: usize,
        member: usize,
        plan: &QuickenedRegionPlan,
        stream: usize,
        nodes: Range<usize>,
        environment_values: &mut [Value],
        history_access: Option<HistoryAccess<'_>>,
    ) {
        let Self {
            evaluators,
            published_scalars,
            region_states,
        } = self;
        let owner = region_states
            .as_mut()
            .filter(|owner| owner.identity == identity)
            .expect("active scalar region state is missing");
        if !owner.enter_quickened(region, plan, evaluators) {
            evaluators[stream].evaluate_canonical_nodes(nodes, environment_values, history_access);
            return;
        }
        let executed = {
            let EvaluatorState {
                node_values,
                node_states,
            } = evaluators[stream].canonical.as_mut();
            let mut arena = CanonicalArena {
                node_values,
                node_states,
                history: history_access,
            };
            plan.execute_member(
                member,
                &mut owner.states[region],
                environment_values,
                published_scalars,
                &mut arena,
            )
        };
        if executed {
            return;
        }
        plan.materialize_member(
            member,
            &owner.states[region],
            evaluators[stream].state_mut(),
        );
        evaluators[stream].evaluate_canonical_nodes(nodes, environment_values, history_access);
        if !plan.synchronize_member(
            member,
            &mut owner.states[region],
            evaluators[stream].state_mut(),
        ) {
            // The member declined and its temporal state is no longer scalar, so the region can no
            // longer be entered as it stands. Returning it to the canonical arena makes the next
            // tick retry the whole handoff.
            owner.authority[region] = RegionAuthority::Canonical;
        }
    }

    #[cfg(test)]
    pub(super) fn delay_ring_lengths(&self) -> Vec<usize> {
        self.evaluators
            .iter()
            .flat_map(|evaluator| evaluator.canonical.delay_ring_lengths())
            .collect()
    }

    pub(super) fn evaluate_steps<const RETAIN_VALUES: bool>(
        &mut self,
        jit: &mut Jit,
        identity: PlanIdentity,
        quickening: bool,
        quickened_regions: &[QuickenedRegionPlan],
        steps: &[ExecutionStep],
        environment_values: &mut [Value],
        mut retained_environment_values: Option<&mut [Value]>,
        history_access: Option<HistoryAccess<'_>>,
    ) -> Result<ExecutionUsage, DataflowEvaluationError> {
        let mut usage = ExecutionUsage::default();
        // Published sources only name streams earlier in the combined two-range order. Availability
        // intentionally carries across the source barrier, while scalar regions cannot cross it.
        for step in steps {
            match step {
                ExecutionStep::ScalarRegion(region) => {
                    usage.regions = true;
                    let plan = &quickened_regions[*region];
                    if self
                        .evaluate_region(
                            jit,
                            identity,
                            quickening,
                            *region,
                            plan,
                            environment_values,
                        )
                        .is_none()
                    {
                        self.evaluate_region_canonical(
                            jit,
                            identity,
                            *region,
                            plan,
                            environment_values,
                            history_access,
                        );
                    }
                    for (stream, slot, _) in plan.outputs() {
                        let value = &environment_values[slot.index()];
                        self.published_scalars[stream.index()] =
                            ScalarValue::from_untyped_value(value);
                        if RETAIN_VALUES
                            && value != &Value::NoVal
                            && let Some(retained) = retained_environment_values.as_deref_mut()
                        {
                            retained[slot.index()] = value.clone();
                        }
                    }
                }
                ExecutionStep::Graph(step) => {
                    self.evaluate_graph::<RETAIN_VALUES>(
                        identity,
                        quickening,
                        quickened_regions,
                        step,
                        environment_values,
                        retained_environment_values.as_deref_mut(),
                        history_access,
                        &mut usage,
                    )?;
                }
            }
        }
        Ok(usage)
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn evaluate_graph<const RETAIN_VALUES: bool>(
        &mut self,
        identity: PlanIdentity,
        quickening: bool,
        quickened_regions: &[QuickenedRegionPlan],
        step: &GraphStep,
        environment_values: &mut [Value],
        retained_environment_values: Option<&mut [Value]>,
        history_access: Option<HistoryAccess<'_>>,
        usage: &mut ExecutionUsage,
    ) -> Result<(), DataflowEvaluationError> {
        let index = step.stream.index();
        let value = if let Some(region) = step.region {
            // Islands are only planned while quickening is enabled, and toggling it rebuilds the plan.
            debug_assert!(quickening);
            usage.regions = true;
            for segment in step.segments.iter() {
                match segment {
                    ExecutableSegment::Canonical(nodes) => {
                        self.evaluators[index].evaluate_canonical_nodes(
                            nodes.clone(),
                            environment_values,
                            history_access,
                        );
                    }
                    // A graph region's members cannot preflight as a unit: a member's canonical
                    // boundary values do not exist until the canonical run before it has finished.
                    // So each member checks its own boundary and may decline this row alone —
                    // `nodes` is the canonical range that then runs in its place, after which the
                    // member is synchronized back so later members still read live registers.
                    ExecutableSegment::Island { member, nodes } => self.evaluate_member(
                        identity,
                        region,
                        *member,
                        &quickened_regions[region],
                        index,
                        nodes.clone(),
                        environment_values,
                        history_access,
                    ),
                }
            }
            self.evaluators[index].finish_static_graph(environment_values)
        } else {
            let evaluator = &mut self.evaluators[index];
            if evaluator.program.uses_static_evaluation() {
                evaluator.evaluate_static_and_stage(environment_values, history_access)
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
                evaluator
                    .evaluate_and_stage_with_retained_environment(environment_values, retained)?
            } else {
                evaluator.evaluate_and_stage(environment_values)?
            }
        };
        self.published_scalars[index] = ScalarValue::from_untyped_value(&value);
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
            let value = evaluator.evaluate_static_and_stage(environment_values, None);
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
            let value = self.evaluators[index].evaluate_static_and_stage(environment_values, None);
            self.published_scalars[index] = ScalarValue::from_untyped_value(&value);
            environment_values[planned.output.environment().index()] = value;
        }
    }

    #[cfg(test)]
    pub(super) fn programs_rc(&self) -> Vec<Rc<StreamProgram>> {
        self.evaluators
            .iter()
            .map(|evaluator| Rc::clone(&evaluator.program))
            .collect()
    }
}
