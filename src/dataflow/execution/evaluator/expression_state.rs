//! Queries over the body currently installed at a reconfigurable node.
//!
//! Every method here destructures `StreamOp::Reconfigurable` / `NodeState::Reconfigurable` and asks
//! a question about the body installed at that node: does it need reconfiguring, which slots does
//! it read, what history does it require. It evaluates nothing and installs nothing.
//!
//! Its sibling [`super::reconfiguration`] performs the actual body swap. This file is the read side
//! of that subsystem.

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
        let StreamOp::Reconfigurable(spec) = &self.program.graph.nodes[node.index()] else {
            unreachable!("reconfigurable expression referenced a non-reconfigurable node")
        };
        let NodeState::Reconfigurable(expression) =
            &self.canonical.as_ref().node_states[node.index()]
        else {
            unreachable!("reconfigurable expression referenced incompatible runtime state")
        };
        match source_value {
            Value::Str(source_text) => match spec.kind {
                ReconfigurableExpressionKind::Deferred => expression.active_expression.is_none(),
                ReconfigurableExpressionKind::Dynamic => expression
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
        let StreamOp::Reconfigurable(_) = &self.program.graph.nodes[node.index()] else {
            unreachable!("reconfigurable expression referenced a non-reconfigurable node")
        };
        let NodeState::Reconfigurable(expression) =
            &self.canonical.as_ref().node_states[node.index()]
        else {
            unreachable!("reconfigurable expression referenced incompatible runtime state")
        };
        expression.active_expression.as_ref().map_or(&[], |active| {
            active.environment_projection.outer_dependency_slots()
        })
    }

    pub(in crate::dataflow) fn prepare_expression_environment_projection(
        &self,
        node: NodeId,
        outer_layout: &EnvironmentLayout,
        allowed_variables: &[VarName],
    ) -> Result<Option<EnvironmentProjection>, VarName> {
        let StreamOp::Reconfigurable(_) = &self.program.graph.nodes[node.index()] else {
            unreachable!("reconfigurable expression referenced a non-reconfigurable node")
        };
        let NodeState::Reconfigurable(expression) =
            &self.canonical.as_ref().node_states[node.index()]
        else {
            unreachable!("reconfigurable expression referenced incompatible runtime state")
        };
        expression
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
        let NodeState::Reconfigurable(expression) =
            &mut self.canonical.as_mut().node_states[node.index()]
        else {
            unreachable!("reconfigurable expression referenced incompatible runtime state")
        };
        expression
            .active_expression
            .as_mut()
            .expect("prepared environment projection requires an active expression")
            .rebind_environment(projection);
    }

    pub(in crate::dataflow) fn for_each_active_body_history_requirement(
        &self,
        visit: &mut impl FnMut(VariableHistoryRequirement),
    ) {
        self.canonical
            .as_ref()
            .for_each_active_body_history_requirement(visit);
    }
}
