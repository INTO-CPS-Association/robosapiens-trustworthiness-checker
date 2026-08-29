use super::super::super::environment::{EnvironmentLayout, EnvironmentSlot};
use super::super::super::execution_plan::{ReconfigurableExpressionId, StreamId};
use super::super::super::history_requirements::VariableHistoryRequirement;
use super::super::super::ir::NodeId;

use super::super::super::{ContextTransferPolicy, ReconfigurationMapping};
use super::super::dynamic_expressions::DynamicExpressionActivation;
use super::super::environment_projection::EnvironmentProjection;
use super::MonitorExecution;
use crate::VarName;
use crate::core::Value;
use crate::dataflow::DataflowEvaluationError;

impl MonitorExecution {
    /// Validate a target-indexed context mapping without materializing or changing either execution.
    pub(in crate::dataflow) fn validate_context_transfer(
        &self,
        source: &Self,
        mapping: &ReconfigurationMapping,
    ) -> bool {
        if self.tick_in_progress || source.tick_in_progress {
            return false;
        }
        if self.stream_slots.len() != self.evaluators.evaluators.len()
            || source.stream_slots.len() != source.evaluators.evaluators.len()
            || mapping.streams().len() != self.evaluators.evaluators.len()
            || !self.validate_environment_mapping(source, mapping)
        {
            return false;
        }

        let mut used_source_streams = vec![false; source.evaluators.evaluators.len()];
        mapping
            .streams()
            .iter()
            .enumerate()
            .all(|(target_index, stream_mapping)| {
                let target_stream = StreamId::new(target_index);
                let Some(target_evaluator) = self.evaluators.evaluators.get(target_index) else {
                    return false;
                };
                let Some(source_stream) = stream_mapping.source() else {
                    return true;
                };
                let source_index = source_stream.index();
                if source_index >= source.evaluators.evaluators.len()
                    || used_source_streams[source_index]
                    || self.stream_name(target_stream) != source.stream_name(source_stream)
                {
                    return false;
                }
                used_source_streams[source_index] = true;
                let source_evaluator = &source.evaluators.evaluators[source_index];
                target_evaluator.validate_context_mapping(source_evaluator, stream_mapping)
            })
    }

    /// Destructively apply a prepared and validated context mapping from a source execution.
    ///
    /// Root preparation performs the release-mode structural checks before this handoff. The
    /// remaining checks document that prepared-mapping contract and run only in debug builds.
    pub(in crate::dataflow) fn context_transfer_from(
        &mut self,
        source: &mut Self,
        mapping: &ReconfigurationMapping,
        policy: ContextTransferPolicy,
    ) {
        debug_assert!(
            self.validate_context_transfer(source, mapping),
            "context transfer requires a prepared and validated mapping"
        );
        if policy == ContextTransferPolicy::None {
            for evaluator in &mut self.evaluators.evaluators {
                evaluator.reset();
            }
            self.reset_after_context_transfer();
            return;
        }

        if mapping
            .streams()
            .iter()
            .any(|mapping| mapping.source().is_some())
        {
            source.engine.jit.materialize_into(
                &mut source.evaluators.evaluators,
                &source.engine.active_plan.semantic,
            );
            debug_assert!(
                self.validate_materialized_context_transfer(source, mapping),
                "materialized context transfer requires a prepared and validated mapping"
            );
        }

        for (target_index, stream_mapping) in mapping.streams().iter().enumerate() {
            let Some(source_stream) = stream_mapping.source() else {
                self.evaluators.evaluators[target_index].reset();
                continue;
            };
            self.evaluators.evaluators[target_index].rewrite_context_from(
                &mut source.evaluators.evaluators[source_stream.index()],
                stream_mapping,
                policy,
            );
        }
        self.reset_after_context_transfer();
    }

    fn validate_materialized_context_transfer(
        &self,
        source: &Self,
        mapping: &ReconfigurationMapping,
    ) -> bool {
        mapping
            .streams()
            .iter()
            .enumerate()
            .all(|(target_index, stream_mapping)| {
                let Some(source_stream) = stream_mapping.source() else {
                    return true;
                };
                let target_evaluator = &self.evaluators.evaluators[target_index];
                let source_evaluator = &source.evaluators.evaluators[source_stream.index()];
                target_evaluator.validate_context_rewrite(source_evaluator, stream_mapping)
            })
    }

    fn validate_environment_mapping(
        &self,
        source: &Self,
        mapping: &ReconfigurationMapping,
    ) -> bool {
        let (Some(target_evaluator), Some(source_evaluator)) = (
            self.evaluators.evaluators.first(),
            source.evaluators.evaluators.first(),
        ) else {
            return mapping.environments().is_empty();
        };
        let target_layout = &target_evaluator.program.environment_layout;
        let source_layout = &source_evaluator.program.environment_layout;
        if mapping.environments().len() != target_layout.len() {
            return false;
        }
        let mut used_source_slots = vec![false; source_layout.len()];
        mapping.environments().iter().copied().enumerate().all(
            |(target_index, environment_mapping)| {
                let Some(source_slot) = environment_mapping.source() else {
                    return true;
                };
                if source_slot.index() >= source_layout.len()
                    || used_source_slots[source_slot.index()]
                    || target_layout.variable(EnvironmentSlot::new(target_index))
                        != source_layout.variable(source_slot)
                {
                    return false;
                }
                used_source_slots[source_slot.index()] = true;
                true
            },
        )
    }

    fn stream_name(&self, stream: StreamId) -> Option<&VarName> {
        if stream.index() >= self.stream_slots.len()
            || stream.index() >= self.evaluators.evaluators.len()
        {
            return None;
        }
        self.evaluators.evaluators[stream.index()]
            .program
            .environment_layout
            .variable(self.stream_slots.slot(stream))
    }

    /// Validate the dense expression identity carried by a source barrier.
    ///
    /// The locations are generated in the same order as the expression plan, so this hook catches
    /// a stale or mismatched expression id before it can mutate an evaluator.
    pub(in crate::dataflow) fn validate_expression_location(
        &self,
        expression_id: ReconfigurableExpressionId,
        stream: StreamId,
        node: NodeId,
    ) -> bool {
        self.expression_locations
            .get(expression_id.index())
            .is_some_and(|location| location.stream == stream && location.node == node)
    }

    /// Reconfigure a nested expression atomically at its source barrier.
    #[inline]
    pub(in crate::dataflow) fn reconfigure_expression(
        &mut self,
        expression_id: ReconfigurableExpressionId,
        stream: StreamId,
        node: NodeId,
        source_value: Value,
        transfer: ContextTransferPolicy,
    ) -> Result<(DynamicExpressionActivation, bool), DataflowEvaluationError> {
        debug_assert!(
            self.validate_expression_location(expression_id, stream, node),
            "reconfigurable expression {expression_id:?} does not match planned stream {stream:?} and node {node:?}"
        );
        let (evaluators, shared_dynamic_expression_cache) = (
            &mut self.evaluators.evaluators,
            &mut self.shared_dynamic_expression_cache,
        );
        evaluators[stream.index()].reconfigure_expression(
            node,
            source_value,
            transfer,
            shared_dynamic_expression_cache,
        )
    }

    pub(in crate::dataflow) fn expression_dependency_slots(
        &self,
        stream: StreamId,
        node: NodeId,
    ) -> &[EnvironmentSlot] {
        self.evaluators.evaluators[stream.index()].expression_dependency_slots(node)
    }

    pub(in crate::dataflow) fn prepare_expression_environment_projection(
        &self,
        stream: StreamId,
        node: NodeId,
        outer_layout: &EnvironmentLayout,
        allowed_variables: &[VarName],
    ) -> Result<Option<EnvironmentProjection>, VarName> {
        self.evaluators.evaluators[stream.index()].prepare_expression_environment_projection(
            node,
            outer_layout,
            allowed_variables,
        )
    }

    pub(in crate::dataflow) fn install_expression_environment_projection(
        &mut self,
        stream: StreamId,
        node: NodeId,
        projection: EnvironmentProjection,
    ) {
        self.evaluators.evaluators[stream.index()]
            .install_expression_environment_projection(node, projection);
    }

    pub(in crate::dataflow) fn for_each_active_body_history_requirement(
        &self,
        visit: &mut impl FnMut(VariableHistoryRequirement),
    ) {
        for evaluator in &self.evaluators.evaluators {
            evaluator.for_each_active_body_history_requirement(&mut *visit);
        }
    }

    pub(in crate::dataflow) fn expression_requires_reconfiguration(
        &self,
        stream: StreamId,
        node: NodeId,
        source_value: &Value,
    ) -> bool {
        self.evaluators.evaluators[stream.index()]
            .expression_requires_reconfiguration(node, source_value)
    }
}
