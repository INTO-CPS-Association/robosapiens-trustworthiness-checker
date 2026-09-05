//! Canonical evaluation of one stream program.
//!
//! Every tier ultimately reads and writes the canonical arena through these entry points: the
//! interpreter runs whole graphs here, a segmented graph runs its canonical node runs here, and the
//! optimized tiers replay their last row here when handing state back.

use std::ops::Range;

use super::super::super::history::HistoryAccess;
use super::super::node_evaluation::{
    evaluate_node_with_history, evaluate_nodes_with_history, try_evaluate_nodes_with_history,
};
use super::super::temporal_commit::{
    commit_staged_temporal_state_with_history, discard_staged_temporal_state,
    stage_recursive_delays,
};
use super::{EvaluationEnvironment, Evaluator};
use crate::core::Value;
use crate::dataflow::DataflowEvaluationError;
use crate::dataflow::ir::NodeId;
use ecow::EcoVec;

impl Evaluator {
    pub(in crate::dataflow) fn reset(&mut self) {
        self.canonical.reset();
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

    /// Evaluates a complete static graph canonically and stages its recursive delays.
    #[inline]
    pub(in crate::dataflow) fn evaluate_static_and_stage(
        &mut self,
        environment_values: &[Value],
        history_access: Option<HistoryAccess<'_>>,
    ) -> Value {
        debug_assert!(self.program.uses_static_evaluation());
        let body = &self.program.graph;
        let state = self.canonical.as_mut();
        debug_assert_eq!(state.node_values.len(), body.nodes.len());
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

    /// Evaluates one canonical node run of a segmented static graph.
    ///
    /// Nodes outside the run are evaluated by their own segment, so this reads and writes only the
    /// canonical node arena and leaves the graph output and recursive delays to
    /// [`Self::finish_static_graph`].
    #[inline]
    pub(in crate::dataflow) fn evaluate_canonical_nodes(
        &mut self,
        nodes: Range<usize>,
        environment_values: &[Value],
        history_access: Option<HistoryAccess<'_>>,
    ) {
        debug_assert!(self.program.uses_static_evaluation());
        let body = &self.program.graph;
        let state = self.canonical.as_mut();
        let context = EvaluationEnvironment {
            environment_values,
            environment_layout: &self.program.environment_layout,
            retained_environment_values: None,
            recursive_call: None,
        };
        for index in nodes {
            let value = evaluate_node_with_history(
                NodeId::new(index),
                &body.nodes[index],
                state,
                context,
                history_access,
            );
            state.node_values[index] = value;
        }
    }

    /// Reads the graph output of a segmented static graph and stages its recursive delays.
    #[inline]
    pub(in crate::dataflow) fn finish_static_graph(
        &mut self,
        environment_values: &[Value],
    ) -> Value {
        let body = &self.program.graph;
        let state = self.canonical.as_mut();
        let context = EvaluationEnvironment {
            environment_values,
            environment_layout: &self.program.environment_layout,
            retained_environment_values: None,
            recursive_call: None,
        };
        let value = context.read_value(state, &body.output);
        stage_recursive_delays(&body.recursive_delays, state, &value);
        value
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
        let state = self.canonical.as_mut();
        debug_assert_eq!(state.node_values.len(), body.nodes.len());
        debug_assert_eq!(state.node_states.len(), body.nodes.len());
        let context = EvaluationEnvironment {
            environment_values,
            environment_layout: &self.program.environment_layout,
            retained_environment_values,
            recursive_call,
        };

        if self.program.uses_static_evaluation() {
            evaluate_nodes_with_history(&body.nodes, state, context, history_access);
        } else {
            try_evaluate_nodes_with_history(&body.nodes, state, context, history_access)?;
        }
        let value = context.read_value(state, &body.output);
        stage_recursive_delays(&body.recursive_delays, state, &value);
        Ok(value)
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
        let state = self.canonical.as_mut();
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
        let state = self.canonical.as_mut();
        discard_staged_temporal_state(&self.program.graph, state);
    }
}
