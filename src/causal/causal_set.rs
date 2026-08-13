use serde::Serialize;

use super::{AtomSet, CausalDomain, CausalRole, RoleExplanation, TimedAtom};

/// One unclassified set of external observations supporting a result.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct CausalSet {
    support: AtomSet,
}

impl CausalSet {
    pub fn support(&self) -> &AtomSet {
        &self.support
    }
}

impl CausalDomain for CausalSet {
    type Annotation = ();

    fn unit() -> Self {
        Self::default()
    }

    fn atom(atom: TimedAtom) -> Self {
        Self {
            support: AtomSet::singleton(atom),
        }
    }

    fn joint(self, other: Self) -> Self {
        Self {
            support: self.support.union(&other.support),
        }
    }

    fn alternative(self, other: Self) -> Self {
        self.joint(other)
    }

    fn annotation_for(_role: CausalRole) -> Self::Annotation {}

    fn reannotate(self, _annotation: Self::Annotation) -> Self {
        self
    }

    fn report_explanations(&self) -> Vec<RoleExplanation> {
        vec![RoleExplanation::unclassified(&self.support)]
    }
}
