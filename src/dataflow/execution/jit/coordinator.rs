//! Coordination boundary between canonical monitor execution and the optional native tier.
//!
//! The execution plan owns region boundaries and publication order. This module compiles native
//! executors for those regions, selects whole-schedule temporal kernels where possible, and manages
//! cold state handoffs without exposing Cranelift details to the execution loop.

use crate::dataflow::execution::evaluator::Evaluator;
use crate::dataflow::execution::quickening::ScalarValue;
use crate::dataflow::execution::scalar_region::ScalarRegion;
use crate::dataflow::execution::scheduled_plan::ScheduledExecutionPlan;

use crate::dataflow::history::HistoryAccess;
#[cfg(feature = "jit")]
use crate::dataflow::typed::TypedIoLayout;
use crate::dataflow::*;

use super::outcomes::{NativeRegionOutcome, WholeTickOutcome};

#[cfg(feature = "jit")]
use super::runtime::{
    NativeRunOutcome, NativeScalarRegion, NativeTemporalMonitor, PreparedDirectJit,
};

pub(in crate::dataflow) struct Jit {
    #[cfg(feature = "jit")]
    activation: Activation,
    #[cfg(feature = "jit")]
    execution: NativeExecution,
    #[cfg(feature = "jit")]
    compiled_plan_id: Option<u64>,
    #[cfg(feature = "jit")]
    replay_environment: Option<Vec<Value>>,
    #[cfg(feature = "jit")]
    report: Option<JitReport>,
    #[cfg(feature = "jit")]
    direct_layout: Option<TypedIoLayout>,
    #[cfg(all(test, feature = "jit"))]
    fail_direct_extraction: bool,
}

#[cfg(feature = "jit")]
enum Activation {
    Disabled,
    Pending { remaining_events: u64 },
    Active,
}

#[cfg(feature = "jit")]
enum NativeExecution {
    None,
    WholeScalar {
        evaluator: NativeScalarRegion,
    },
    WholeTemporal {
        evaluator: Box<NativeTemporalMonitor>,
    },
    Regions {
        evaluators: Box<[Option<NativeScalarRegion>]>,
    },
}

impl Jit {
    // Construction and activation control.
    pub(in crate::dataflow) fn disabled() -> Self {
        Self {
            #[cfg(feature = "jit")]
            activation: Activation::Disabled,
            #[cfg(feature = "jit")]
            execution: NativeExecution::None,
            #[cfg(feature = "jit")]
            compiled_plan_id: None,
            #[cfg(feature = "jit")]
            replay_environment: None,
            #[cfg(feature = "jit")]
            report: None,
            #[cfg(feature = "jit")]
            direct_layout: None,
            #[cfg(all(test, feature = "jit"))]
            fail_direct_extraction: false,
        }
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn configure(
        &mut self,
        plan: &ScheduledExecutionPlan,
        regions: &[ScalarRegion],
        config: JitConfig,
        direct_layout: Option<TypedIoLayout>,
    ) {
        self.execution = NativeExecution::None;
        self.compiled_plan_id = None;
        self.replay_environment = None;
        self.direct_layout = direct_layout;
        if let Some(remaining_events) = config.hotness_threshold() {
            self.activation = Activation::Pending { remaining_events };
            self.report = Some(JitReport::pending());
        } else {
            self.activate(plan, regions);
        }
    }

    /// Advances the cheap hotness counter before a tick. Compilation inputs are requested by the
    /// interpreter only when this returns true, keeping allocation and schedule cloning out of
    /// the active native hot path.
    #[inline(always)]
    pub(in crate::dataflow) fn activation_due(&mut self) -> bool {
        #[cfg(feature = "jit")]
        {
            match &mut self.activation {
                Activation::Pending { remaining_events } if *remaining_events == 0 => true,
                Activation::Pending { remaining_events } => {
                    *remaining_events -= 1;
                    false
                }
                Activation::Disabled | Activation::Active => false,
            }
        }
        #[cfg(not(feature = "jit"))]
        {
            false
        }
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn activate_pending(
        &mut self,
        plan: &ScheduledExecutionPlan,
        regions: &[ScalarRegion],
    ) {
        debug_assert!(matches!(self.activation, Activation::Pending { .. }));
        self.activate(plan, regions);
    }

    /// Keeps native artifacts tied to the execution plan they were compiled from.
    pub(in crate::dataflow) fn schedule_changed(
        &mut self,
        plan: &ScheduledExecutionPlan,
        regions: &[ScalarRegion],
    ) {
        #[cfg(feature = "jit")]
        {
            if matches!(self.activation, Activation::Active)
                && self.compiled_plan_id != Some(plan.id.0)
            {
                self.activate(plan, regions);
            }
        }
        #[cfg(not(feature = "jit"))]
        {
            let _ = (plan, regions);
        }
    }

    // Report and observation.
    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn report(&self) -> Option<&JitReport> {
        self.report.as_ref()
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn direct_entry_ready(&self) -> bool {
        match &self.execution {
            NativeExecution::WholeScalar { evaluator } => evaluator.has_direct_entry(),
            NativeExecution::WholeTemporal { evaluator } => evaluator.has_direct_entry(),
            NativeExecution::None | NativeExecution::Regions { .. } => false,
        }
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn take_prepared_direct(
        &mut self,
        evaluators: &mut [Evaluator],
        history_access: Option<HistoryAccess<'_>>,
    ) -> Result<Option<PreparedDirectJit>, ()> {
        #[cfg(test)]
        if std::mem::take(&mut self.fail_direct_extraction) {
            return Err(());
        }
        let execution = std::mem::replace(&mut self.execution, NativeExecution::None);
        match execution {
            NativeExecution::WholeScalar { evaluator } => match evaluator.into_prepared_direct() {
                Ok(prepared) => Ok(Some(prepared)),
                Err(evaluator) => {
                    self.execution = NativeExecution::WholeScalar { evaluator };
                    Err(())
                }
            },
            NativeExecution::WholeTemporal { evaluator } => {
                match (*evaluator).into_prepared_direct(evaluators, history_access) {
                    Ok(prepared) => Ok(Some(prepared)),
                    Err(evaluator) => {
                        self.execution = NativeExecution::WholeTemporal {
                            evaluator: Box::new(evaluator),
                        };
                        Err(())
                    }
                }
            }
            execution => {
                self.execution = execution;
                Ok(None)
            }
        }
    }

    #[cfg(all(test, feature = "jit"))]
    pub(in crate::dataflow) fn fail_next_direct_extraction(&mut self) {
        self.fail_direct_extraction = true;
    }

    pub(in crate::dataflow) fn take_replay_environment(&mut self) -> Option<Vec<Value>> {
        #[cfg(feature = "jit")]
        {
            self.replay_environment.take()
        }
        #[cfg(not(feature = "jit"))]
        {
            None
        }
    }

    // Hot execution.
    #[inline(always)]
    pub(in crate::dataflow) fn evaluate_whole(
        &mut self,
        evaluators: &mut [Evaluator],
        environment_values: &mut [Value],
        published_scalars: &mut [Option<ScalarValue>],
        history_access: Option<HistoryAccess<'_>>,
    ) -> WholeTickOutcome {
        // A complete native tick returns with its values in the environment; if it exits, the
        // monitor replays canonically, where the ordinary publication path restores this cache.
        #[cfg(feature = "jit")]
        if let NativeExecution::WholeScalar { evaluator, .. } = &mut self.execution {
            return match evaluator.evaluate(environment_values) {
                NativeRunOutcome::Completed => WholeTickOutcome::Completed,
                NativeRunOutcome::Fallback { replay_environment } => {
                    if replay_environment.is_some() || self.replay_environment.is_none() {
                        self.replay_environment = replay_environment;
                    }
                    WholeTickOutcome::CanonicalFallback
                }
            };
        }
        #[cfg(feature = "jit")]
        if let NativeExecution::WholeTemporal { evaluator, .. } = &mut self.execution {
            return match evaluator.evaluate(evaluators, environment_values, history_access) {
                NativeRunOutcome::Completed => WholeTickOutcome::CompletedAndCommitted,
                NativeRunOutcome::Fallback { replay_environment } => {
                    if replay_environment.is_some() || self.replay_environment.is_none() {
                        self.replay_environment = replay_environment;
                    }
                    WholeTickOutcome::CanonicalFallback
                }
            };
        }
        let _ = (
            evaluators,
            environment_values,
            published_scalars,
            history_access,
        );
        WholeTickOutcome::NotAvailable
    }

    #[inline(always)]
    pub(in crate::dataflow) fn evaluate_region(
        &mut self,
        region: usize,
        evaluators: &mut [Evaluator],
        environment_values: &mut [Value],
    ) -> NativeRegionOutcome {
        #[cfg(feature = "jit")]
        if let NativeExecution::Regions {
            evaluators: regions,
            ..
        } = &mut self.execution
            && let Some(evaluator) = regions.get_mut(region).and_then(Option::as_mut)
        {
            return match evaluator.evaluate(environment_values) {
                NativeRunOutcome::Completed => NativeRegionOutcome::Completed,
                NativeRunOutcome::Fallback { replay_environment } => {
                    if let Some(environment) = replay_environment {
                        evaluator.replay_into(evaluators, environment);
                    } else {
                        evaluator.materialize_into(evaluators);
                    }
                    NativeRegionOutcome::Deoptimized
                }
            };
        }
        let _ = (region, evaluators, environment_values);
        NativeRegionOutcome::Unavailable
    }

    // State handoff.
    /// Materialize every authoritative native representation into the live evaluator arena.
    ///
    /// This is the destructive handoff used when the source evaluator is about to be consumed:
    /// once canonical state has been restored, the native runtime bookkeeping may be reset.
    pub(in crate::dataflow) fn materialize_into(
        &mut self,
        evaluators: &mut [Evaluator],
        plan: &ScheduledExecutionPlan,
    ) {
        #[cfg(feature = "jit")]
        {
            self.materialize_native_state(evaluators, plan);
            self.reset_after_context_transfer();
        }
        #[cfg(not(feature = "jit"))]
        let _ = (evaluators, plan);
    }

    pub(in crate::dataflow) fn materialize_region_into(
        &mut self,
        region: usize,
        evaluators: &mut [Evaluator],
    ) -> bool {
        #[cfg(feature = "jit")]
        if let NativeExecution::Regions {
            evaluators: regions,
            ..
        } = &mut self.execution
            && let Some(evaluator) = regions.get_mut(region).and_then(Option::as_mut)
        {
            return evaluator.materialize_into(evaluators);
        }
        let _ = (region, evaluators);
        false
    }

    pub(in crate::dataflow) fn reset_after_context_transfer(&mut self) {
        #[cfg(feature = "jit")]
        {
            self.replay_environment = None;
            match &mut self.execution {
                NativeExecution::WholeScalar { evaluator, .. } => {
                    evaluator.reset_after_context_transfer();
                }
                NativeExecution::WholeTemporal { evaluator, .. } => {
                    evaluator.reset_after_context_transfer();
                }
                NativeExecution::Regions { evaluators, .. } => {
                    for evaluator in evaluators.iter_mut().flatten() {
                        evaluator.reset_after_context_transfer();
                    }
                }
                NativeExecution::None => {}
            }
        }
    }

    // Private activation, materialization, and replay helpers.
    #[cfg(feature = "jit")]
    /// Compiles native artifacts for `plan`.
    ///
    /// Native executors hold no canonical retention of their own, so compilation reads nothing
    /// from the evaluator arena; the cold paths reconstruct canonical state by replay instead.
    fn activate(&mut self, plan: &ScheduledExecutionPlan, regions: &[ScalarRegion]) {
        self.compiled_plan_id = Some(plan.id.0);
        let scalar_error = match NativeScalarRegion::compile(plan, self.direct_layout.as_ref()) {
            Ok(Some(evaluator)) => {
                self.execution = NativeExecution::WholeScalar { evaluator };
                self.report = Some(JitReport::compiled(
                    JitPlan::WholeSchedule,
                    1,
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    None,
                ));
                self.activation = Activation::Active;
                return;
            }
            Ok(None) => None,
            Err(error) => Some(format!("whole scalar plan: {error}")),
        };
        let temporal_error = match NativeTemporalMonitor::compile(plan, self.direct_layout.as_ref())
        {
            Ok(Some(evaluator)) => {
                self.execution = NativeExecution::WholeTemporal {
                    evaluator: Box::new(evaluator),
                };
                self.report = Some(JitReport::compiled(
                    JitPlan::WholeSchedule,
                    1,
                    Vec::new(),
                    Vec::new(),
                    plan.commit_streams
                        .iter()
                        .map(|stream| stream.index())
                        .collect(),
                    scalar_error,
                ));
                self.activation = Activation::Active;
                return;
            }
            Ok(None) => None,
            Err(error) => Some(format!("scheduled temporal plan: {error}")),
        };
        let prior_error = match (scalar_error, temporal_error) {
            (Some(scalar), Some(temporal)) => Some(format!("{scalar}; {temporal}")),
            (Some(error), None) | (None, Some(error)) => Some(error),
            (None, None) => None,
        };
        let mut errors = prior_error.into_iter().collect::<Vec<_>>();
        let native_regions = regions
            .iter()
            .map(|region| {
                match NativeScalarRegion::compile_region(
                    region,
                    plan.stream_slots,
                    plan.environment_len,
                    None,
                ) {
                    Ok(Some(evaluator)) => Some(evaluator),
                    Ok(None) => None,
                    Err(error) => {
                        errors.push(format!("scalar region: {error}"));
                        None
                    }
                }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let compiled_artifacts = native_regions.iter().flatten().count();
        let supported_streams = regions
            .iter()
            .zip(native_regions.iter())
            .filter(|(_, native)| native.is_some())
            .flat_map(|(region, _)| region.outputs().map(|(stream, _, _)| stream.index()))
            .collect::<std::collections::BTreeSet<_>>();
        let unsupported_streams = plan
            .streams
            .iter()
            .map(|stream| stream.stream.index())
            .filter(|stream| !supported_streams.contains(stream))
            .collect();
        let report_plan = if compiled_artifacts == 0 {
            JitPlan::Unavailable
        } else {
            JitPlan::Regions
        };
        self.execution = if compiled_artifacts == 0 {
            NativeExecution::None
        } else {
            NativeExecution::Regions {
                evaluators: native_regions,
            }
        };
        self.report = Some(JitReport::compiled(
            report_plan,
            compiled_artifacts,
            unsupported_streams,
            Vec::new(),
            Vec::new(),
            (!errors.is_empty()).then(|| errors.join("; ")),
        ));
        self.activation = Activation::Active;
    }

    #[cfg(feature = "jit")]
    fn materialize_native_state(
        &mut self,
        evaluators: &mut [Evaluator],
        plan: &ScheduledExecutionPlan,
    ) {
        match &mut self.execution {
            NativeExecution::WholeScalar { evaluator, .. } => {
                if let Some(mut environment) = evaluator
                    .snapshot_replay_environment()
                    .or_else(|| self.replay_environment.clone())
                {
                    Self::replay_canonical_snapshot(evaluators, plan, &mut environment);
                }
            }
            NativeExecution::WholeTemporal { evaluator, .. } => {
                evaluator.snapshot_into(evaluators);
                if let Some(mut environment) = evaluator
                    .snapshot_replay_environment()
                    .or_else(|| self.replay_environment.clone())
                {
                    Self::replay_canonical_snapshot(evaluators, plan, &mut environment);
                }
            }
            NativeExecution::Regions {
                evaluators: regions,
                ..
            } => {
                for region in regions.iter_mut().flatten() {
                    region.materialize_into(evaluators);
                }
            }
            NativeExecution::None => {}
        }
    }

    #[cfg(feature = "jit")]
    fn replay_canonical_snapshot(
        evaluators: &mut [Evaluator],
        plan: &ScheduledExecutionPlan,
        environment: &mut [Value],
    ) {
        for planned in plan.streams.iter() {
            let stream = planned.stream.index();
            let evaluator = &mut evaluators[stream];
            let value = evaluator.evaluate_static_and_stage(environment, None);
            // Replay restores semantic state from an already committed native row. It must not become
            // another logical tick, so discard only the staged temporal writes from this replay.
            evaluator.discard_staged_temporal_state();
            environment[planned.output.environment().index()] = value;
        }
    }

    // Test-only inspection.
    #[cfg(all(test, feature = "jit"))]
    pub(in crate::dataflow) fn compiled_artifact_count(&self) -> usize {
        match &self.execution {
            NativeExecution::None => 0,
            NativeExecution::WholeScalar { .. } | NativeExecution::WholeTemporal { .. } => 1,
            NativeExecution::Regions { evaluators, .. } => evaluators.iter().flatten().count(),
        }
    }
}

#[cfg(all(test, feature = "jit"))]
mod tests {
    use super::*;
    use crate::dataflow::execution::evaluator::Evaluator;
    use crate::dataflow::execution::evaluator_state::{reset_state_clone_count, state_clone_count};
    use crate::dataflow::execution::quickening::ScalarValue;
    use crate::dataflow::execution::scheduled_plan::PlanId;
    use crate::dataflow::stream_id::StreamId;
    use crate::dataflow::{DataflowProgram, JitConfig};
    use crate::{CheckedDsrvSpecification, Value};

    #[test]
    fn materialize_into_replays_fused_state_in_live_evaluators() {
        let specification = "in x: Int\n\
            aux a: Int\n\
            out result: Int\n\
            a = x + 1\n\
            result = a * 2"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let program = DataflowProgram::compile_checked(specification).unwrap();
        let programs = program.stream_programs().to_vec();
        let stream_slots = program.monitor_plan().stream_slots;
        let stream_order = (0..programs.len()).map(StreamId::new).collect::<Vec<_>>();
        let plan = ScheduledExecutionPlan::new(
            PlanId(0),
            &programs,
            stream_slots,
            &[],
            &stream_order,
            &[],
        );
        let mut evaluators = programs
            .iter()
            .cloned()
            .map(Evaluator::new)
            .collect::<Vec<_>>();
        let mut jit = Jit::disabled();
        jit.configure(&plan, &[], JitConfig::eager(), None);

        let mut environment = vec![Value::NoVal; plan.environment_len];
        environment[0] = Value::Int(3);
        let mut published_scalars = vec![Some(ScalarValue::Int(-1)), Some(ScalarValue::Int(-2))];
        assert!(matches!(
            jit.evaluate_whole(
                &mut evaluators,
                &mut environment,
                &mut published_scalars,
                None,
            ),
            WholeTickOutcome::Completed
        ));
        assert_eq!(
            published_scalars.as_slice(),
            &[Some(ScalarValue::Int(-1)), Some(ScalarValue::Int(-2)),]
        );

        reset_state_clone_count();
        jit.materialize_into(&mut evaluators, &plan);

        assert_eq!(state_clone_count(), 0);
        assert_eq!(evaluators[0].canonical.node_values[0], Value::Int(4));
        assert_eq!(evaluators[1].canonical.node_values[0], Value::Int(8));
    }

    #[test]
    fn active_jit_recompiles_after_an_unavailable_plan_changes() {
        let old_program = DataflowProgram::compile_checked(
            "in x: Str\nout result: Str\nresult = x"
                .parse::<CheckedDsrvSpecification>()
                .unwrap(),
        )
        .unwrap();
        let old_programs = old_program.stream_programs().to_vec();
        let old_plan = ScheduledExecutionPlan::new(
            PlanId(0),
            &old_programs,
            old_program.monitor_plan().stream_slots,
            &[],
            &[StreamId::new(0)],
            &[],
        );
        let mut jit = Jit::disabled();
        jit.configure(&old_plan, &[], JitConfig::eager(), None);
        assert!(matches!(jit.execution, NativeExecution::None));

        let new_program = DataflowProgram::compile_checked(
            "in x: Int\nout result: Int\nresult = x + 1"
                .parse::<CheckedDsrvSpecification>()
                .unwrap(),
        )
        .unwrap();
        let new_programs = new_program.stream_programs().to_vec();
        let new_plan = ScheduledExecutionPlan::new(
            PlanId(1),
            &new_programs,
            new_program.monitor_plan().stream_slots,
            &[],
            &[StreamId::new(0)],
            &[],
        );
        jit.schedule_changed(&new_plan, &[]);

        assert!(matches!(jit.execution, NativeExecution::WholeScalar { .. }));
        assert_eq!(jit.compiled_plan_id, Some(1));
    }

    #[test]
    fn fused_temporal_completion_updates_environment_without_publishing() {
        let specification = "in x: Int\n\
            out result: Int\n\
            result = default(result[1], 0) + x"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let program = DataflowProgram::compile_checked(specification).unwrap();
        let programs = program.stream_programs().to_vec();
        let stream_slots = program.monitor_plan().stream_slots;
        let stream_order = (0..programs.len()).map(StreamId::new).collect::<Vec<_>>();
        let plan = ScheduledExecutionPlan::new(
            PlanId(0),
            &programs,
            stream_slots,
            &[],
            &stream_order,
            program.monitor_plan().temporal_streams.as_slice(),
        );
        let mut evaluators = programs
            .iter()
            .cloned()
            .map(Evaluator::new)
            .collect::<Vec<_>>();
        let mut jit = Jit::disabled();
        jit.configure(&plan, &[], JitConfig::eager(), None);

        let output_slot = stream_slots.slot(StreamId::new(0));
        let mut environment = vec![Value::NoVal; plan.environment_len];
        let mut published_scalars = vec![Some(ScalarValue::Int(-1))];
        for (input, expected) in [(1, 1), (2, 3)] {
            environment[0] = Value::Int(input);
            assert!(matches!(
                jit.evaluate_whole(
                    &mut evaluators,
                    &mut environment,
                    &mut published_scalars,
                    None,
                ),
                WholeTickOutcome::CompletedAndCommitted
            ));
            assert_eq!(environment[output_slot.index()], Value::Int(expected));
            assert_eq!(published_scalars.as_slice(), &[Some(ScalarValue::Int(-1))]);
        }
    }
}
