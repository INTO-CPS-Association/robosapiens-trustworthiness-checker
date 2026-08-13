use serde::Serialize;

use super::{CausalDomain, CausalRole, RoleCausalDomain, RoleCause, RoleExplanation, TimedAtom};

/// One compact role-annotated cause explanation.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct RoleCausalSet {
    explanation: RoleExplanation,
}

impl RoleCausalSet {
    pub fn explanation(&self) -> &RoleExplanation {
        &self.explanation
    }

    pub fn causes(&self) -> &[RoleCause] {
        self.explanation.causes()
    }
}

impl CausalDomain for RoleCausalSet {
    type Annotation = CausalRole;

    fn unit() -> Self {
        Self::default()
    }

    fn atom(atom: TimedAtom) -> Self {
        Self {
            explanation: RoleExplanation::direct(atom),
        }
    }

    fn joint(self, other: Self) -> Self {
        Self {
            explanation: self.explanation.union(other.explanation),
        }
    }

    fn alternative(self, other: Self) -> Self {
        self.joint(other)
    }

    fn annotation_for(role: CausalRole) -> Self::Annotation {
        role
    }

    fn reannotate(self, role: Self::Annotation) -> Self {
        Self {
            explanation: self.explanation.reannotate(role),
        }
    }

    fn report_explanations(&self) -> Vec<RoleExplanation> {
        vec![self.explanation.clone()]
    }
}

impl RoleCausalDomain for RoleCausalSet {}
