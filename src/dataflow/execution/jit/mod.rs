//! Coordination boundary between canonical monitor execution and the optional native tier.
//!
//! The interpreter owns schedules, canonical state, and publication. This module owns JIT
//! activation and every compiled artifact. Its outcomes deliberately describe only what the
//! interpreter must do next, keeping Cranelift and native evaluator types out of the execution
//! loop.

use crate::dataflow::execution::quickening::ScalarValue;
use crate::dataflow::execution::scheduled_plan::ScheduledExecutionPlan;
use crate::dataflow::execution::stream_evaluator::StreamEvaluator;
use crate::dataflow::execution::stream_state::StreamState;
use crate::dataflow::execution_plan::StreamSlots;
use crate::dataflow::ir::BoundEvaluationGraph;
use crate::dataflow::*;

#[cfg(feature = "jit")]
mod backend;
#[cfg(feature = "jit")]
mod runtime;
#[cfg(feature = "jit")]
mod scheduled_state;

#[cfg(feature = "jit")]
use runtime::{
    JittedGraphEvaluator, JittedRunEvaluator, JittedRunOutcome, JittedTemporalRunEvaluator,
};

#[cfg_attr(not(feature = "jit"), allow(dead_code))]
pub(in crate::dataflow) enum FusedTickOutcome {
    NotHandled,
    Success,
    SuccessCommitted,
    Canonical,
}

#[cfg_attr(not(feature = "jit"), allow(dead_code))]
pub(in crate::dataflow) enum GraphTickOutcome {
    NotHandled,
    Value(Value),
    Canonical,
}

/// Canonical state made available to a per-stream native artifact for one tick.
#[cfg_attr(not(feature = "jit"), allow(dead_code))]
pub(in crate::dataflow) struct NativeGraphContext<'a> {
    pub(in crate::dataflow) graph: &'a BoundEvaluationGraph,
    pub(in crate::dataflow) state: &'a mut StreamState,
    pub(in crate::dataflow) environment_values: &'a [Value],
    pub(in crate::dataflow) environment_layout: &'a Rc<EnvironmentLayout>,
    pub(in crate::dataflow) published_scalars: &'a [Option<ScalarValue>],
    pub(in crate::dataflow) stream_slots: StreamSlots,
}

/// Canonical evaluator state made available to a scheduled native commit step.
#[cfg_attr(not(feature = "jit"), allow(dead_code))]
pub(in crate::dataflow) struct NativeCommitContext<'a> {
    pub(in crate::dataflow) state: &'a mut StreamState,
    pub(in crate::dataflow) environment_values: &'a [Value],
    pub(in crate::dataflow) environment_layout: &'a Rc<EnvironmentLayout>,
    pub(in crate::dataflow) retained_environment_values: Option<&'a [Value]>,
}

pub(in crate::dataflow) struct Jit {
    #[cfg(feature = "jit")]
    activation: Activation,
    #[cfg(feature = "jit")]
    plan: NativePlan,
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
enum NativePlan {
    None,
    FusedScalar {
        evaluator: JittedRunEvaluator,
        plan_id: u64,
    },
    FusedTemporal {
        evaluator: Box<JittedTemporalRunEvaluator>,
        plan_id: u64,
    },
    PerStream(Box<[Option<JittedGraphEvaluator>]>),
}

impl Jit {
    pub(in crate::dataflow) fn disabled() -> Self {
        Self {
            #[cfg(feature = "jit")]
            activation: Activation::Disabled,
            #[cfg(feature = "jit")]
            plan: NativePlan::None,
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
        config: JitConfig,
    ) {
        self.plan = NativePlan::None;
        self.replay_environment = None;
        if let Some(remaining_events) = config.hotness_threshold() {
            self.activation = Activation::Pending { remaining_events };
            self.report = Some(JitReport::pending());
        } else {
            self.activate(plan);
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
    pub(in crate::dataflow) fn activate_pending(&mut self, plan: &ScheduledExecutionPlan) {
        debug_assert!(matches!(self.activation, Activation::Pending { .. }));
        self.activate(plan);
    }

    /// Keeps a fused artifact tied to the schedule it was compiled from. Per-stream artifacts do
    /// not depend on order and remain valid across layout changes.
    pub(in crate::dataflow) fn schedule_changed(&mut self, plan: &ScheduledExecutionPlan) -> bool {
        #[cfg(feature = "jit")]
        {
            let recompile = matches!(
                &self.plan,
                NativePlan::FusedScalar { plan_id: compiled, .. }
                    | NativePlan::FusedTemporal { plan_id: compiled, .. }
                    if *compiled != plan.id.0
            );
            if recompile {
                self.activate(plan);
            }
            recompile
        }
        #[cfg(not(feature = "jit"))]
        {
            let _ = plan;
            false
        }
    }

    #[cfg(feature = "jit")]
    fn activate(&mut self, plan: &ScheduledExecutionPlan) {
        let fused_error = match JittedRunEvaluator::compile(plan) {
            Ok(Some(evaluator)) => {
                self.plan = NativePlan::FusedScalar {
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
                self.plan = NativePlan::FusedTemporal {
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
            Ok(evaluators) => {
                let unsupported_streams = evaluators
                    .iter()
                    .enumerate()
                    .filter_map(|(stream, evaluator)| evaluator.is_none().then_some(stream))
                    .collect::<Vec<_>>();
                let compiled_artifacts = evaluators.len() - unsupported_streams.len();
                let scheduled_temporal_streams = evaluators
                    .iter()
                    .enumerate()
                    .filter_map(|(stream, evaluator)| {
                        evaluator
                            .as_ref()
                            .is_some_and(JittedGraphEvaluator::has_scheduled_temporal_state)
                            .then_some(stream)
                    })
                    .collect::<Vec<_>>();
                let complete_temporal_kernel_streams = evaluators
                    .iter()
                    .enumerate()
                    .filter_map(|(stream, evaluator)| {
                        evaluator
                            .as_ref()
                            .is_some_and(JittedGraphEvaluator::has_complete_temporal_kernel)
                            .then_some(stream)
                    })
                    .collect::<Vec<_>>();
                let report_plan = if compiled_artifacts == 0 {
                    JitPlan::Unavailable
                } else {
                    JitPlan::PerStream
                };
                self.plan = NativePlan::PerStream(evaluators.into_boxed_slice());
                self.report = Some(JitReport::compiled(
                    report_plan,
                    compiled_artifacts,
                    unsupported_streams,
                    scheduled_temporal_streams,
                    complete_temporal_kernel_streams,
                    prior_error,
                ));
            }
            Err(error) => {
                self.plan = NativePlan::None;
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
        evaluators: &mut [StreamEvaluator],
        environment_values: &mut [Value],
        published_scalars: &mut [Option<ScalarValue>],
    ) -> FusedTickOutcome {
        #[cfg(feature = "jit")]
        if let NativePlan::FusedScalar { evaluator, .. } = &mut self.plan {
            return match evaluator.evaluate(environment_values, published_scalars) {
                JittedRunOutcome::Success => FusedTickOutcome::Success,
                JittedRunOutcome::SuccessCommitted => unreachable!(),
                JittedRunOutcome::Fallback { replay_environment } => {
                    self.replay_environment = replay_environment;
                    FusedTickOutcome::Canonical
                }
            };
        }
        #[cfg(feature = "jit")]
        if let NativePlan::FusedTemporal { evaluator, .. } = &mut self.plan {
            return match evaluator.evaluate(evaluators, environment_values, published_scalars) {
                JittedRunOutcome::SuccessCommitted => FusedTickOutcome::SuccessCommitted,
                JittedRunOutcome::Success => unreachable!(),
                JittedRunOutcome::Fallback { replay_environment } => {
                    self.replay_environment = replay_environment;
                    FusedTickOutcome::Canonical
                }
            };
        }
        let _ = (evaluators, environment_values, published_scalars);
        FusedTickOutcome::NotHandled
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
        stream: usize,
        context: NativeGraphContext<'_>,
    ) -> GraphTickOutcome {
        #[cfg(feature = "jit")]
        if let NativePlan::PerStream(evaluators) = &mut self.plan
            && let Some(evaluator) = &mut evaluators[stream]
        {
            return match evaluator.evaluate(
                context.graph,
                context.state,
                context.environment_values,
                context.environment_layout,
                context.published_scalars,
                context.stream_slots,
            ) {
                Some(value) => GraphTickOutcome::Value(value),
                None if evaluator.is_disabled() => GraphTickOutcome::Canonical,
                None => GraphTickOutcome::NotHandled,
            };
        }
        let _ = (stream, context);
        GraphTickOutcome::NotHandled
    }

    pub(in crate::dataflow) fn commit_graph(
        &mut self,
        stream: usize,
        context: NativeCommitContext<'_>,
    ) -> bool {
        #[cfg(feature = "jit")]
        {
            if let NativePlan::PerStream(evaluators) = &mut self.plan
                && let Some(evaluator) = evaluators.get_mut(stream).and_then(Option::as_mut)
            {
                return evaluator.commit(
                    context.state,
                    context.environment_values,
                    context.environment_layout,
                    context.retained_environment_values,
                );
            }
            false
        }
        #[cfg(not(feature = "jit"))]
        {
            let _ = (stream, context);
            false
        }
    }

    #[cfg(all(test, feature = "jit"))]
    pub(in crate::dataflow) fn compiled_artifact_count(&self) -> usize {
        match &self.plan {
            NativePlan::None => 0,
            NativePlan::FusedScalar { .. } | NativePlan::FusedTemporal { .. } => 1,
            NativePlan::PerStream(evaluators) => evaluators
                .iter()
                .filter(|evaluator| evaluator.is_some())
                .count(),
        }
    }
}
