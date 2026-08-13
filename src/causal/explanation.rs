use ecow::EcoVec;
use serde::Serialize;

use super::{AtomSet, CausalRole, CausalRoles, TimedAtom};

/// One normalized role-annotated cause occurrence.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct RoleCause {
    pub atom: TimedAtom,
    pub roles: CausalRoles,
}

impl RoleCause {
    pub fn new(atom: TimedAtom, roles: CausalRoles) -> Self {
        Self { atom, roles }
    }

    pub fn direct(atom: TimedAtom) -> Self {
        Self::new(atom, CausalRoles::direct())
    }

    pub(crate) fn unclassified(atom: TimedAtom) -> Self {
        Self::new(atom, CausalRoles::empty())
    }
}

/// One normalized, role-aware cause set.
///
/// Causes are sorted by [`TimedAtom`], and duplicate atoms are merged by
/// unioning their role sets.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct RoleExplanation {
    causes: EcoVec<RoleCause>,
}

impl RoleExplanation {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn direct(atom: TimedAtom) -> Self {
        Self {
            causes: EcoVec::from([RoleCause::direct(atom)]),
        }
    }

    pub(crate) fn unclassified(support: &AtomSet) -> Self {
        Self {
            causes: support
                .iter()
                .cloned()
                .map(RoleCause::unclassified)
                .collect::<Vec<_>>()
                .into(),
        }
    }

    pub fn causes(&self) -> &[RoleCause] {
        &self.causes
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &RoleCause> {
        self.causes.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.causes.is_empty()
    }

    /// Union two explanations, merging duplicate atoms and their roles.
    pub fn union(self, other: Self) -> Self {
        let mut causes = self
            .causes
            .into_iter()
            .chain(other.causes)
            .collect::<Vec<_>>();
        causes.sort_by(|left, right| left.atom.cmp(&right.atom));

        let mut merged: Vec<RoleCause> = Vec::with_capacity(causes.len());
        for cause in causes {
            if let Some(previous) = merged.last_mut()
                && previous.atom == cause.atom
            {
                previous.roles = previous.roles.union(cause.roles);
            } else {
                merged.push(cause);
            }
        }

        Self {
            causes: merged.into(),
        }
    }

    /// Reclassify direct causes while preserving existing contextual roles.
    pub fn reannotate(self, role: CausalRole) -> Self {
        Self {
            causes: self
                .causes
                .into_iter()
                .map(|cause| RoleCause::new(cause.atom, cause.roles.reannotate_direct(role)))
                .collect::<Vec<_>>()
                .into(),
        }
    }

    /// Return whether this explanation is no stronger than `other`.
    ///
    /// Every atom-role occurrence in `self` must also occur in `other`.
    pub fn dominates(&self, other: &Self) -> bool {
        self.causes.iter().all(|left| {
            other
                .causes
                .iter()
                .find(|right| right.atom == left.atom)
                .is_some_and(|right| left.roles.is_subset(right.roles))
        })
    }
}

pub(crate) fn minimise_explanations(
    explanations: impl IntoIterator<Item = RoleExplanation>,
) -> EcoVec<RoleExplanation> {
    let mut explanations = explanations.into_iter().collect::<Vec<_>>();
    explanations.sort_by(|left, right| left.causes.cmp(&right.causes));
    explanations.dedup();

    explanations
        .iter()
        .enumerate()
        .filter(|(index, explanation)| {
            !explanations
                .iter()
                .enumerate()
                .any(|(other_index, other)| other_index != *index && other.dominates(explanation))
        })
        .map(|(_, explanation)| explanation.clone())
        .collect::<Vec<_>>()
        .into()
}
