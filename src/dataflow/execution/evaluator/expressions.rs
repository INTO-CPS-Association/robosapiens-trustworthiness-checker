use super::super::super::environment::{EnvironmentLayout, EnvironmentSlot};
use super::super::super::history_requirements::VariableHistoryRequirement;
use super::super::super::ir::{NodeId, ReconfigurableExpressionKind, StreamOp};
use super::super::environment_projection::EnvironmentProjection;
use super::super::evaluator_state::NodeState;
use super::Evaluator;
use crate::VarName;
use crate::core::Value;

impl Evaluator {
    pub(in crate::dataflow) fn expression_requires_reconfiguration(
        &self,
        node: NodeId,
        source_value: &Value,
    ) -> bool {
        let StreamOp::Dynamic(spec) = &self.program.graph.nodes[node.index()] else {
            unreachable!("reconfigurable expression referenced a non-dynamic node")
        };
        let NodeState::Dynamic(dynamic) =
            &self.tier_states.canonical.as_ref().node_states[node.index()]
        else {
            unreachable!("reconfigurable expression referenced incompatible runtime state")
        };
        match source_value {
            Value::Str(source_text) => match spec.kind {
                ReconfigurableExpressionKind::Deferred => dynamic.active_expression.is_none(),
                ReconfigurableExpressionKind::Dynamic => dynamic
                    .active_expression
                    .as_ref()
                    .is_none_or(|active| &active.source_text != source_text),
            },
            Value::Deferred | Value::NoVal => false,
            _ => true,
        }
    }

    pub(in crate::dataflow) fn expression_dependency_slots(
        &self,
        node: NodeId,
    ) -> &[EnvironmentSlot] {
        let StreamOp::Dynamic(_) = &self.program.graph.nodes[node.index()] else {
            unreachable!("reconfigurable expression referenced a non-dynamic node")
        };
        let NodeState::Dynamic(dynamic) =
            &self.tier_states.canonical.as_ref().node_states[node.index()]
        else {
            unreachable!("reconfigurable expression referenced incompatible runtime state")
        };
        dynamic.active_expression.as_ref().map_or(&[], |active| {
            active.environment_projection.outer_dependency_slots()
        })
    }

    pub(in crate::dataflow) fn prepare_expression_environment_projection(
        &self,
        node: NodeId,
        outer_layout: &EnvironmentLayout,
        allowed_variables: &[VarName],
    ) -> Result<Option<EnvironmentProjection>, VarName> {
        let StreamOp::Dynamic(_) = &self.program.graph.nodes[node.index()] else {
            unreachable!("reconfigurable expression referenced a non-dynamic node")
        };
        let NodeState::Dynamic(dynamic) =
            &self.tier_states.canonical.as_ref().node_states[node.index()]
        else {
            unreachable!("reconfigurable expression referenced incompatible runtime state")
        };
        dynamic
            .active_expression
            .as_ref()
            .map(|active| {
                let nested_layout = active.template.program.environment_layout.as_ref();
                if let Some(variable) =
                    active
                        .template
                        .nested_environment_slots
                        .iter()
                        .find_map(|&slot| {
                            nested_layout
                                .variable(slot)
                                .filter(|variable| !allowed_variables.contains(*variable))
                                .cloned()
                        })
                {
                    return Err(variable);
                }
                EnvironmentProjection::for_template(&active.template, outer_layout)
            })
            .transpose()
    }

    pub(in crate::dataflow) fn install_expression_environment_projection(
        &mut self,
        node: NodeId,
        projection: EnvironmentProjection,
    ) {
        let NodeState::Dynamic(dynamic) =
            &mut self.tier_states.canonical.as_mut().node_states[node.index()]
        else {
            unreachable!("reconfigurable expression referenced incompatible runtime state")
        };
        dynamic
            .active_expression
            .as_mut()
            .expect("prepared environment projection requires an active expression")
            .rebind_environment(projection);
    }

    pub(in crate::dataflow) fn for_each_active_body_history_requirement(
        &self,
        visit: &mut impl FnMut(VariableHistoryRequirement),
    ) {
        self.tier_states
            .canonical
            .as_ref()
            .for_each_active_body_history_requirement(visit);
    }
}
