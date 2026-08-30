//! Coordination boundary between canonical monitor execution and the optional native tier.
//!
//! The interpreter owns schedules, canonical state, and publication. This module coordinates JIT
//! activation and owns schedule-wide fused artifacts; evaluators own their per-stream native tiers.
//! Its outcomes describe only what the interpreter must do next, keeping Cranelift and native
//! evaluator types out of the execution loop.

use crate::dataflow::execution::evaluator::Evaluator;
use crate::dataflow::execution::quickening::ScalarValue;
use crate::dataflow::execution::scheduled_plan::ScheduledExecutionPlan;
use crate::dataflow::execution_plan::StreamSlots;
use crate::dataflow::history::HistoryAccess;
use crate::dataflow::*;

use super::outcomes::{FusedTickOutcome, GraphTickOutcome};
#[cfg(feature = "jit")]
use super::runtime::{
    JittedGraphEvaluator, JittedRunEvaluator, JittedTemporalRunEvaluator, NativeRunOutcome,
};

pub(in crate::dataflow) struct Jit {
    #[cfg(feature = "jit")]
    activation: Activation,
    #[cfg(feature = "jit")]
    execution: NativeExecution,
    #[cfg(feature = "jit")]
    replay_environment: Option<Vec<Value>>,
    #[cfg(feature = "jit")]
    report: Option<JitReport>,
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
    FusedScalar {
        evaluator: JittedRunEvaluator,
        plan_id: u64,
    },
    FusedTemporal {
        evaluator: Box<JittedTemporalRunEvaluator>,
        plan_id: u64,
    },
    PerStream,
}

impl Jit {
    pub(in crate::dataflow) fn disabled() -> Self {
        Self {
            #[cfg(feature = "jit")]
            activation: Activation::Disabled,
            #[cfg(feature = "jit")]
            execution: NativeExecution::None,
            #[cfg(feature = "jit")]
            replay_environment: None,
            #[cfg(feature = "jit")]
            report: None,
        }
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn configure(
        &mut self,
        plan: &ScheduledExecutionPlan,
        evaluators: &mut [Evaluator],
        config: JitConfig,
    ) {
        for evaluator in evaluators.iter_mut() {
            evaluator.install_native_tier(None);
        }
        self.execution = NativeExecution::None;
        self.replay_environment = None;
        if let Some(remaining_events) = config.hotness_threshold() {
            self.activation = Activation::Pending { remaining_events };
            self.report = Some(JitReport::pending());
        } else {
            self.activate(plan, evaluators);
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
        evaluators: &mut [Evaluator],
    ) {
        debug_assert!(matches!(self.activation, Activation::Pending { .. }));
        self.activate(plan, evaluators);
    }

    /// Keeps a fused artifact tied to the schedule and source boundary it was compiled from.
    /// Per-stream artifacts do not depend on either and remain valid across plan changes.
    pub(in crate::dataflow) fn schedule_changed(
        &mut self,
        plan: &ScheduledExecutionPlan,
        evaluators: &mut [Evaluator],
    ) {
        #[cfg(feature = "jit")]
        {
            let recompile = matches!(
                &self.execution,
                NativeExecution::FusedScalar { plan_id: compiled, .. }
                    | NativeExecution::FusedTemporal { plan_id: compiled, .. }
                    if *compiled != plan.id.0
            );
            if recompile {
                self.activate(plan, evaluators);
            }
        }
        #[cfg(not(feature = "jit"))]
        {
            let _ = (plan, evaluators);
        }
    }

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

    pub(in crate::dataflow) fn reset_after_context_transfer(&mut self) {
        #[cfg(feature = "jit")]
        {
            self.replay_environment = None;
            match &mut self.execution {
                NativeExecution::FusedScalar { evaluator, .. } => {
                    evaluator.reset_after_context_transfer();
                }
                NativeExecution::FusedTemporal { evaluator, .. } => {
                    evaluator.reset_after_context_transfer();
                }
                NativeExecution::PerStream | NativeExecution::None => {}
            }
        }
    }

    #[cfg(feature = "jit")]
    fn materialize_native_state(
        &self,
        evaluators: &mut [Evaluator],
        plan: &ScheduledExecutionPlan,
    ) {
        match &self.execution {
            NativeExecution::FusedScalar { evaluator, .. } => {
                if let Some(mut environment) = evaluator.snapshot_replay_environment() {
                    Self::replay_canonical_snapshot(evaluators, plan, &mut environment);
                }
            }
            NativeExecution::FusedTemporal { evaluator, .. } => {
                evaluator.snapshot_into(evaluators);
                if let Some(mut environment) = evaluator.snapshot_replay_environment() {
                    Self::replay_canonical_snapshot(evaluators, plan, &mut environment);
                }
            }
            NativeExecution::PerStream => {
                for evaluator in evaluators {
                    evaluator.materialize_native_tier();
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
            let value = evaluator.evaluate_canonical_infallible(environment);
            // Replay restores semantic state from an already committed native row. It must not become
            // another logical tick, so discard only the staged temporal writes from this replay.
            evaluator.discard_staged_temporal_state();
            environment[planned.output.environment().index()] = value;
        }
    }

    #[cfg(feature = "jit")]
    fn activate(&mut self, plan: &ScheduledExecutionPlan, evaluators: &mut [Evaluator]) {
        let fused_error = match JittedRunEvaluator::compile(plan) {
            Ok(Some(evaluator)) => {
                self.execution = NativeExecution::FusedScalar {
                    evaluator,
                    plan_id: plan.id.0,
                };
                self.report = Some(JitReport::compiled(
                    JitPlan::Fused,
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
            Err(error) => Some(format!("fused plan: {error}")),
        };
        let temporal_error = match JittedTemporalRunEvaluator::compile(plan) {
            Ok(Some(evaluator)) => {
                self.execution = NativeExecution::FusedTemporal {
                    evaluator: Box::new(evaluator),
                    plan_id: plan.id.0,
                };
                self.report = Some(JitReport::compiled(
                    JitPlan::Fused,
                    1,
                    Vec::new(),
                    Vec::new(),
                    plan.commit_streams
                        .iter()
                        .map(|stream| stream.index())
                        .collect(),
                    fused_error,
                ));
                self.activation = Activation::Active;
                return;
            }
            Ok(None) => None,
            Err(error) => Some(format!("scheduled temporal plan: {error}")),
        };
        let prior_error = match (fused_error, temporal_error) {
            (Some(scalar), Some(temporal)) => Some(format!("{scalar}; {temporal}")),
            (Some(error), None) | (None, Some(error)) => Some(error),
            (None, None) => None,
        };
        match JittedGraphEvaluator::compile_many(plan) {
            Ok(native_tiers) => {
                let unsupported_streams = native_tiers
                    .iter()
                    .enumerate()
                    .filter_map(|(stream, evaluator)| evaluator.is_none().then_some(stream))
                    .collect::<Vec<_>>();
                let compiled_artifacts = native_tiers.len() - unsupported_streams.len();
                let scheduled_temporal_streams = native_tiers
                    .iter()
                    .enumerate()
                    .filter_map(|(stream, evaluator)| {
                        evaluator
                            .as_ref()
                            .is_some_and(JittedGraphEvaluator::has_scheduled_temporal_state)
                            .then_some(stream)
                    })
                    .collect::<Vec<_>>();

                let report_plan = if compiled_artifacts == 0 {
                    JitPlan::Unavailable
                } else {
                    JitPlan::PerStream
                };
                debug_assert_eq!(evaluators.len(), native_tiers.len());
                for (evaluator, native) in evaluators.iter_mut().zip(native_tiers) {
                    evaluator.install_native_tier(native);
                }
                self.execution = NativeExecution::PerStream;
                self.report = Some(JitReport::compiled(
                    report_plan,
                    compiled_artifacts,
                    unsupported_streams,
                    scheduled_temporal_streams,
                    Vec::new(),
                    prior_error,
                ));
            }
            Err(error) => {
                self.execution = NativeExecution::None;
                let mut causes = prior_error.into_iter().collect::<Vec<_>>();
                causes.push(format!("per-stream plan: {error}"));
                let error = causes.join("; ");
                self.report = Some(JitReport::compiled(
                    JitPlan::Unavailable,
                    0,
                    (0..plan.stream_slots.len()).collect(),
                    Vec::new(),
                    Vec::new(),
                    Some(error),
                ));
            }
        }
        self.activation = Activation::Active;
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn report(&self) -> Option<&JitReport> {
        self.report.as_ref()
    }

    #[inline(always)]
    pub(in crate::dataflow) fn evaluate_fused(
        &mut self,
        evaluators: &mut [Evaluator],
        environment_values: &mut [Value],
        published_scalars: &mut [Option<ScalarValue>],
        history_access: Option<HistoryAccess<'_>>,
    ) -> FusedTickOutcome {
        #[cfg(feature = "jit")]
        if let NativeExecution::FusedScalar { evaluator, .. } = &mut self.execution {
            return match evaluator.evaluate(environment_values, published_scalars) {
                NativeRunOutcome::Completed => FusedTickOutcome::Completed,
                NativeRunOutcome::Fallback { replay_environment } => {
                    self.replay_environment = replay_environment;
                    FusedTickOutcome::CanonicalFallback
                }
            };
        }
        #[cfg(feature = "jit")]
        if let NativeExecution::FusedTemporal { evaluator, .. } = &mut self.execution {
            return match evaluator.evaluate(
                evaluators,
                environment_values,
                published_scalars,
                history_access,
            ) {
                NativeRunOutcome::Completed => FusedTickOutcome::CompletedAndCommitted,
                NativeRunOutcome::Fallback { replay_environment } => {
                    self.replay_environment = replay_environment;
                    FusedTickOutcome::CanonicalFallback
                }
            };
        }
        let _ = (
            evaluators,
            environment_values,
            published_scalars,
            history_access,
        );
        FusedTickOutcome::NotAvailable
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

    #[inline(always)]
    pub(in crate::dataflow) fn evaluate_graph(
        &mut self,
        evaluator: &mut Evaluator,
        environment_values: &[Value],
        published_scalars: &[Option<ScalarValue>],
        stream_slots: StreamSlots,
    ) -> GraphTickOutcome {
        #[cfg(feature = "jit")]
        if matches!(&self.execution, NativeExecution::PerStream)
            && let Some(native) = &mut evaluator.tier_states.native
        {
            let graph = &evaluator.program.graph;
            let environment_layout = &evaluator.program.environment_layout;
            let state = evaluator.tier_states.canonical.as_mut();
            return match native.evaluate(
                graph,
                state,
                environment_values,
                environment_layout,
                published_scalars,
                stream_slots,
            ) {
                Some(value) => GraphTickOutcome::Value(value),
                None if native.is_disabled() => GraphTickOutcome::CanonicalFallback,
                None => GraphTickOutcome::NotAvailable,
            };
        }
        let _ = (
            evaluator,
            environment_values,
            published_scalars,
            stream_slots,
        );
        GraphTickOutcome::NotAvailable
    }

    pub(in crate::dataflow) fn commit_graph(
        &mut self,
        evaluator: &mut Evaluator,
        environment_values: &[Value],
        retained_environment_values: Option<&[Value]>,
    ) -> bool {
        #[cfg(feature = "jit")]
        {
            if matches!(&self.execution, NativeExecution::PerStream)
                && let Some(native) = &mut evaluator.tier_states.native
            {
                return native.commit(
                    evaluator.tier_states.canonical.as_mut(),
                    environment_values,
                    &evaluator.program.environment_layout,
                    retained_environment_values,
                );
            }
            false
        }
        #[cfg(not(feature = "jit"))]
        {
            let _ = (evaluator, environment_values, retained_environment_values);
            false
        }
    }

    #[cfg(all(test, feature = "jit"))]
    pub(in crate::dataflow) fn compiled_artifact_count(&self, evaluators: &[Evaluator]) -> usize {
        match &self.execution {
            NativeExecution::None => 0,
            NativeExecution::FusedScalar { .. } | NativeExecution::FusedTemporal { .. } => 1,
            NativeExecution::PerStream => evaluators
                .iter()
                .filter(|evaluator| evaluator.tier_states.native.is_some())
                .count(),
        }
    }
}

#[cfg(all(test, feature = "jit"))]
mod tests {
    use super::*;
    use crate::dataflow::execution::evaluator::Evaluator;
    use crate::dataflow::execution::evaluator_state::{reset_state_clone_count, state_clone_count};
    use crate::dataflow::execution::scheduled_plan::PlanId;
    use crate::dataflow::execution_plan::StreamId;
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
        jit.configure(&plan, &mut evaluators, JitConfig::eager());

        let mut environment = vec![Value::NoVal; plan.environment_len];
        environment[0] = Value::Int(3);
        let mut published_scalars = vec![None; programs.len()];
        assert!(matches!(
            jit.evaluate_fused(
                &mut evaluators,
                &mut environment,
                &mut published_scalars,
                None,
            ),
            FusedTickOutcome::Completed
        ));

        reset_state_clone_count();
        jit.materialize_into(&mut evaluators, &plan);

        assert_eq!(state_clone_count(), 0);
        assert_eq!(
            evaluators[0].tier_states.canonical.node_values[0],
            Value::Int(4)
        );
        assert_eq!(
            evaluators[1].tier_states.canonical.node_values[0],
            Value::Int(8)
        );
    }
}
