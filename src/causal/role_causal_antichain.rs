use ecow::EcoVec;
use serde::Serialize;

use super::{
    CausalDomain, CausalRole, RoleCausalDomain, RoleCause, RoleExplanation, TimedAtom,
    explanation::minimise_explanations,
};

/// Inclusion-minimal alternative role-annotated cause explanations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct RoleCausalAntichain {
    alternatives: EcoVec<RoleExplanation>,
}

impl Default for RoleCausalAntichain {
    fn default() -> Self {
        Self::unit()
    }
}

impl RoleCausalAntichain {
    pub fn alternatives(&self) -> &[RoleExplanation] {
        &self.alternatives
    }

    pub fn causes(&self, alternative: usize) -> Option<&[RoleCause]> {
        self.alternatives
            .get(alternative)
            .map(RoleExplanation::causes)
    }
}

impl CausalDomain for RoleCausalAntichain {
    type Annotation = CausalRole;

    fn unit() -> Self {
        Self {
            alternatives: EcoVec::from([RoleExplanation::empty()]),
        }
    }

    fn atom(atom: TimedAtom) -> Self {
        Self {
            alternatives: EcoVec::from([RoleExplanation::direct(atom)]),
        }
    }

    fn joint(self, other: Self) -> Self {
        Self {
            alternatives: minimise_explanations(self.alternatives.iter().flat_map(|left| {
                other
                    .alternatives
                    .iter()
                    .map(move |right| left.clone().union(right.clone()))
            })),
        }
    }

    fn alternative(self, other: Self) -> Self {
        Self {
            alternatives: minimise_explanations(
                self.alternatives.into_iter().chain(other.alternatives),
            ),
        }
    }

    fn annotation_for(role: CausalRole) -> Self::Annotation {
        role
    }

    fn reannotate(self, role: Self::Annotation) -> Self {
        Self {
            alternatives: minimise_explanations(
                self.alternatives
                    .into_iter()
                    .map(|alternative| alternative.reannotate(role)),
            ),
        }
    }

    fn report_explanations(&self) -> Vec<RoleExplanation> {
        self.alternatives.to_vec()
    }
}

impl RoleCausalDomain for RoleCausalAntichain {}
