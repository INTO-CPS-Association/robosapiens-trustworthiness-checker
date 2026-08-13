use std::fmt::Debug;

use serde::Serialize;

use super::{CausalRole, RoleExplanation, TimedAtom};

/// Algebra required by the causal DSRV evaluator.
pub trait CausalDomain: Clone + Debug + Default + PartialEq + Serialize + Sized + 'static {
    type Annotation: Clone + Debug + PartialEq + Eq + Serialize + 'static;

    /// An explanation requiring no external observation.
    fn unit() -> Self;

    /// A resolved external observation.
    fn atom(atom: TimedAtom) -> Self;

    /// Combine evidence that is jointly required.
    fn joint(self, other: Self) -> Self;

    /// Combine alternative sufficient explanations.
    fn alternative(self, other: Self) -> Self;

    /// Map an evaluator-level role to this domain's annotation type.
    fn annotation_for(role: CausalRole) -> Self::Annotation;

    /// Apply an annotation while preserving evidence already classified by a
    /// nested causal operator.
    fn reannotate(self, annotation: Self::Annotation) -> Self;

    /// Interpret this explanation according to its role in a containing result.
    fn used_as(self, role: CausalRole) -> Self {
        self.reannotate(Self::annotation_for(role))
    }

    /// Combine result-producing evidence with contextual evidence.
    fn with_context(self, evidence: Self, role: CausalRole) -> Self {
        self.joint(evidence.used_as(role))
    }

    /// Materialise normalized explanations for the shared reporting boundary.
    ///
    /// The reference domain uses empty role lists; role-aware domains retain
    /// their occurrence-level role annotations.
    fn report_explanations(&self) -> Vec<RoleExplanation>;

    fn joint_all(values: impl IntoIterator<Item = Self>) -> Self {
        values.into_iter().fold(Self::unit(), Self::joint)
    }

    fn alternative_all(values: impl IntoIterator<Item = Self>) -> Option<Self> {
        values.into_iter().reduce(Self::alternative)
    }
}

/// Additional algebra needed by role-aware causal monitoring.
pub trait RoleCausalDomain: CausalDomain<Annotation = super::CausalRole> {}
