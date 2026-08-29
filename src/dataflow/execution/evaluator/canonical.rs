use super::super::super::history::HistoryAccess;
use super::super::interpreter::{
    commit_staged_temporal_state_with_history, discard_staged_temporal_state,
    evaluate_nodes_with_history, stage_recursive_delays,
};
use super::{EvaluationEnvironment, Evaluator};
use crate::core::Value;

impl Evaluator {
    pub(in crate::dataflow) fn evaluate_canonical_infallible(
        &mut self,
        environment_values: &[Value],
    ) -> Value {
        self.evaluate_canonical_infallible_with_history(environment_values, None)
    }

    pub(in crate::dataflow) fn evaluate_canonical_infallible_with_history(
        &mut self,
        environment_values: &[Value],
        history_access: Option<HistoryAccess<'_>>,
    ) -> Value {
        let body = &self.program.graph;
        let state = self.tier_states.canonical.as_mut();
        let context = EvaluationEnvironment {
            environment_values,
            environment_layout: &self.program.environment_layout,
            retained_environment_values: None,
            recursive_call: None,
        };
        evaluate_nodes_with_history(&body.nodes, state, context, history_access);
        let value = context.read_value(state, &body.output);
        stage_recursive_delays(&body.recursive_delays, state, &value);
        value
    }

    pub(in crate::dataflow) fn commit_temporal_state(&mut self, environment_values: &[Value]) {
        self.commit_temporal_state_with_history(environment_values, None, None);
    }

    pub(in crate::dataflow) fn commit_temporal_state_with_history(
        &mut self,
        environment_values: &[Value],
        retained_environment_values: Option<&[Value]>,
        history_access: Option<HistoryAccess<'_>>,
    ) {
        let state = self.tier_states.canonical.as_mut();
        let context = EvaluationEnvironment {
            environment_values,
            environment_layout: &self.program.environment_layout,
            retained_environment_values,
            recursive_call: None,
        };
        commit_staged_temporal_state_with_history(
            &self.program.graph,
            state,
            context,
            history_access,
        );
    }

    pub(in crate::dataflow) fn discard_staged_temporal_state(&mut self) {
        let state = self.tier_states.canonical.as_mut();
        discard_staged_temporal_state(&self.program.graph, state);
    }
}
