use super::super::environment::{EnvironmentLayout, EnvironmentSlot};
use super::super::history_requirements::VariableHistoryRequirement;
use super::evaluator_state::ReconfigurableExpressionTemplate;
use crate::VarName;

#[derive(Clone, Copy)]
pub(in crate::dataflow) struct EnvironmentBinding {
    pub(in crate::dataflow) nested_slot: EnvironmentSlot,
    pub(in crate::dataflow) outer_slot: EnvironmentSlot,
}

#[derive(Clone)]
pub(in crate::dataflow) struct EnvironmentProjection {
    bindings: Box<[EnvironmentBinding]>,
    outer_dependency_slots: Box<[EnvironmentSlot]>,
    outer_history_requirements: Box<[VariableHistoryRequirement]>,
    nested_environment_size: usize,
}

impl EnvironmentProjection {
    #[cold]
    #[inline(never)]
    pub(in crate::dataflow) fn for_template(
        template: &ReconfigurableExpressionTemplate,
        outer_layout: &EnvironmentLayout,
    ) -> Result<Self, VarName> {
        let nested_layout = template.program.environment_layout.as_ref();
        let project = |nested| {
            let variable = nested_layout
                .variable(nested)
                .expect("dynamic template slot must belong to its compiled environment");
            outer_layout.slot(variable).ok_or_else(|| variable.clone())
        };

        let bindings = template
            .nested_environment_slots
            .iter()
            .copied()
            .map(|nested_slot| {
                project(nested_slot).map(|outer_slot| EnvironmentBinding {
                    nested_slot,
                    outer_slot,
                })
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_boxed_slice();
        let outer_dependency_slots = template
            .nested_dependency_slots
            .iter()
            .copied()
            .map(project)
            .collect::<Result<Vec<_>, _>>()?
            .into_boxed_slice();
        let outer_history_requirements = template
            .nested_history_requirements
            .iter()
            .map(|requirement| {
                project(requirement.slot()).map(|slot| VariableHistoryRequirement {
                    slot,
                    depth: requirement.depth(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_boxed_slice();

        Ok(Self {
            bindings,
            outer_dependency_slots,
            outer_history_requirements,
            nested_environment_size: nested_layout.len(),
        })
    }

    #[inline]
    pub(in crate::dataflow) fn bindings(&self) -> &[EnvironmentBinding] {
        &self.bindings
    }

    #[inline]
    pub(in crate::dataflow) fn outer_dependency_slots(&self) -> &[EnvironmentSlot] {
        &self.outer_dependency_slots
    }

    #[inline]
    pub(in crate::dataflow) fn outer_history_requirements(&self) -> &[VariableHistoryRequirement] {
        &self.outer_history_requirements
    }

    #[inline]
    pub(in crate::dataflow) fn nested_environment_size(&self) -> usize {
        self.nested_environment_size
    }
}
