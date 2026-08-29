use super::super::super::history::HistoryAccess;
use super::super::super::ir::{BoundRef, NodeId};
use super::super::interpreter::{
    evaluate_nodes_with_history, stage_recursive_delays, try_evaluate_nodes_with_history,
};
#[cfg(feature = "jit")]
use super::super::jit::JittedGraphEvaluator;
use super::super::quickening::{self, ScalarValue};
use super::{EvaluationEnvironment, Evaluator};
use crate::core::Value;
use crate::dataflow::DataflowEvaluationError;
use ecow::EcoVec;

impl Evaluator {
    pub(in crate::dataflow) fn reset(&mut self) {
        self.tier_states.reset();
    }

    pub(in crate::dataflow) fn evaluate_and_commit(
        &mut self,
        environment_values: &[Value],
        recursive_call: Option<&dyn Fn(EcoVec<Value>) -> Value>,
    ) -> Result<Value, DataflowEvaluationError> {
        self.evaluate_and_commit_with_history(environment_values, recursive_call, None)
    }

    pub(in crate::dataflow) fn evaluate_and_commit_with_history(
        &mut self,
        environment_values: &[Value],
        recursive_call: Option<&dyn Fn(EcoVec<Value>) -> Value>,
        history_access: Option<HistoryAccess<'_>>,
    ) -> Result<Value, DataflowEvaluationError> {
        let value = self.evaluate_and_stage_with_context(
            environment_values,
            None,
            recursive_call,
            history_access,
        )?;
        self.commit_temporal_state_with_history(environment_values, None, history_access);
        Ok(value)
    }

    pub(in crate::dataflow) fn evaluate_and_stage(
        &mut self,
        environment_values: &[Value],
    ) -> Result<Value, DataflowEvaluationError> {
        self.evaluate_and_stage_with_history(environment_values, None)
    }

    pub(in crate::dataflow) fn evaluate_and_stage_with_history(
        &mut self,
        environment_values: &[Value],
        history_access: Option<HistoryAccess<'_>>,
    ) -> Result<Value, DataflowEvaluationError> {
        self.evaluate_and_stage_with_context(environment_values, None, None, history_access)
    }

    pub(in crate::dataflow) fn evaluate_and_stage_with_retained_environment(
        &mut self,
        environment_values: &[Value],
        retained_environment_values: &[Value],
    ) -> Result<Value, DataflowEvaluationError> {
        self.evaluate_and_stage_with_retained_environment_and_history(
            environment_values,
            retained_environment_values,
            None,
        )
    }

    pub(in crate::dataflow) fn evaluate_and_stage_with_retained_environment_and_history(
        &mut self,
        environment_values: &[Value],
        retained_environment_values: &[Value],
        history_access: Option<HistoryAccess<'_>>,
    ) -> Result<Value, DataflowEvaluationError> {
        self.evaluate_and_stage_with_context(
            environment_values,
            Some(retained_environment_values),
            None,
            history_access,
        )
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn install_native_tier(
        &mut self,
        native: Option<JittedGraphEvaluator>,
    ) {
        self.tier_states.native = native;
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn materialize_native_tier(&mut self) {
        let Some(mut native) = self.tier_states.native.take() else {
            return;
        };
        native.snapshot_into(self);
        native.reset_after_context_transfer();
        self.tier_states.native = Some(native);
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn reset_native_tier(&mut self) {
        if let Some(native) = &mut self.tier_states.native {
            native.reset_after_context_transfer();
        }
    }

    #[cfg(all(test, feature = "jit"))]
    pub(in crate::dataflow) fn native_artifact_identity(&self) -> Option<*const ()> {
        self.tier_states
            .native
            .as_ref()
            .map(JittedGraphEvaluator::artifact_identity)
    }

    /// Top-level plans are schedule-owned. Nested and adaptively specialized evaluators retain
    /// their own plan because they execute outside a matching schedule step.
    pub(in crate::dataflow) fn detach_top_level_quick_plan(&mut self) {
        self.tier_states.detach_quick_plan();
    }

    pub(in crate::dataflow) fn materialize_quickening(&mut self) {
        self.tier_states.materialize_quickening();
    }

    #[cfg(test)]
    pub(in crate::dataflow) fn quickening_node_value(&self, node: NodeId) -> Option<ScalarValue> {
        self.tier_states
            .quickening
            .as_ref()
            .and_then(|state| quickening::node_value(state, node.index()))
    }

    #[inline]
    pub(in crate::dataflow) fn evaluate_infallible_and_stage_with_plan(
        &mut self,
        environment_values: &[Value],
        schedule_plan: Option<&quickening::Plan>,
        published_scalars: &[Option<ScalarValue>],
        adaptive_candidate: bool,
    ) -> Value {
        self.evaluate_infallible_and_stage_with_history(
            environment_values,
            schedule_plan,
            published_scalars,
            None,
            adaptive_candidate,
        )
    }

    #[inline]
    pub(in crate::dataflow) fn evaluate_infallible_and_stage_with_history(
        &mut self,
        environment_values: &[Value],
        schedule_plan: Option<&quickening::Plan>,
        published_scalars: &[Option<ScalarValue>],
        history_access: Option<HistoryAccess<'_>>,
        adaptive_candidate: bool,
    ) -> Value {
        debug_assert!(self.program.is_infallible());
        let body = &self.program.graph;
        let canonical_state = self.tier_states.canonical.as_mut();
        debug_assert_eq!(canonical_state.node_values.len(), body.nodes.len());
        debug_assert_eq!(canonical_state.node_states.len(), body.nodes.len());
        let context = EvaluationEnvironment {
            environment_values,
            environment_layout: &self.program.environment_layout,
            retained_environment_values: None,
            recursive_call: None,
        };

        let active_plan = schedule_plan.or(self.tier_states.quick_plan.as_deref());
        let can_adapt = adaptive_candidate
            && active_plan.is_none()
            && self.tier_states.quickening.is_none()
            && history_access.is_none();
        if let (Some(plan), Some(state)) = (active_plan, &mut self.tier_states.quickening) {
            quickening::execute_plan(
                state,
                plan,
                body,
                canonical_state,
                context,
                published_scalars,
                history_access,
            );
        } else {
            evaluate_nodes_with_history(&body.nodes, canonical_state, context, history_access);
        }
        let value = context.read_value(canonical_state, &body.output);
        stage_recursive_delays(&body.recursive_delays, canonical_state, &value);
        let adaptive_plan = can_adapt
            .then(|| quickening::plan_from_observed_single(body, canonical_state, context))
            .flatten();
        if let Some(plan) = adaptive_plan {
            self.tier_states.install_adaptive_plan(plan);
        }
        value
    }

    #[inline]
    pub(in crate::dataflow) fn evaluate_single_scalar_with_plan(
        &mut self,
        environment_values: &[Value],
        plan: &quickening::SingleScalarPlan,
        published_scalars: &[Option<ScalarValue>],
    ) -> quickening::DirectScalarResult {
        self.evaluate_single_scalar_with_history(environment_values, plan, published_scalars, None)
    }

    #[inline]
    pub(in crate::dataflow) fn evaluate_single_scalar_with_history(
        &mut self,
        environment_values: &[Value],
        plan: &quickening::SingleScalarPlan,
        published_scalars: &[Option<ScalarValue>],
        history_access: Option<HistoryAccess<'_>>,
    ) -> quickening::DirectScalarResult {
        let body = &self.program.graph;
        debug_assert_eq!(body.nodes.len(), 1);
        debug_assert_eq!(body.output, BoundRef::Node(NodeId::new(0)));
        debug_assert!(body.recursive_delays.is_empty());
        let canonical_state = self.tier_states.canonical.as_mut();
        let context = EvaluationEnvironment {
            environment_values,
            environment_layout: &self.program.environment_layout,
            retained_environment_values: None,
            recursive_call: None,
        };
        let Some(quickening_state) = self.tier_states.quickening.as_mut() else {
            evaluate_nodes_with_history(&body.nodes, canonical_state, context, history_access);
            let value = context.read_value(canonical_state, &body.output);
            return quickening::DirectScalarResult::Canonical(value);
        };
        quickening::execute_direct_scalar(
            quickening_state,
            plan,
            body,
            canonical_state,
            context,
            published_scalars,
        )
    }

    #[inline]
    pub(super) fn evaluate_and_stage_with_context(
        &mut self,
        environment_values: &[Value],
        retained_environment_values: Option<&[Value]>,
        recursive_call: Option<&dyn Fn(EcoVec<Value>) -> Value>,
        history_access: Option<HistoryAccess<'_>>,
    ) -> Result<Value, DataflowEvaluationError> {
        let body = &self.program.graph;
        let state = self.tier_states.canonical.as_mut();
        debug_assert_eq!(state.node_values.len(), body.nodes.len());
        debug_assert_eq!(state.node_states.len(), body.nodes.len());
        let context = EvaluationEnvironment {
            environment_values,
            environment_layout: &self.program.environment_layout,
            retained_environment_values,
            recursive_call,
        };

        if self.program.is_infallible() {
            if let (Some(plan), Some(quickening_state)) = (
                self.tier_states.quick_plan.as_deref(),
                &mut self.tier_states.quickening,
            ) {
                quickening::execute_plan(
                    quickening_state,
                    plan,
                    body,
                    state,
                    context,
                    &[],
                    history_access,
                );
            } else {
                evaluate_nodes_with_history(&body.nodes, state, context, history_access);
            }
        } else {
            try_evaluate_nodes_with_history(&body.nodes, state, context, history_access)?;
        }
        let value = context.read_value(state, &body.output);
        stage_recursive_delays(&body.recursive_delays, state, &value);
        Ok(value)
    }

    #[cold]
    #[inline(never)]
    pub(in crate::dataflow) fn rebuild_top_level_quickening(&mut self) {
        let Some(plan) = self
            .program
            .is_infallible()
            .then(|| quickening::Plan::new(&self.program.graph))
            .flatten()
        else {
            return;
        };
        let mut state = quickening::State::new(&plan);
        state.synchronize_from(self.tier_states.canonical.as_ref());
        self.tier_states.quickening = Some(state);
    }
}
