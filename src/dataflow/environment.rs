use std::collections::BTreeMap;

use super::plan::EnvironmentId;
use crate::VarName;

#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct EnvironmentLayout {
    slots: BTreeMap<VarName, EnvironmentId>,
}

impl EnvironmentLayout {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn from_vars(vars: impl IntoIterator<Item = VarName>) -> Self {
        let mut slots = BTreeMap::new();
        for var in vars {
            let id = EnvironmentId::new(slots.len());
            assert!(
                slots.insert(var.clone(), id).is_none(),
                "duplicate variable `{var}` in dataflow environment"
            );
        }
        let mut ids = slots.values().map(|id| id.index()).collect::<Vec<_>>();
        ids.sort_unstable();
        debug_assert!(ids.into_iter().eq(0..slots.len()));
        Self { slots }
    }

    pub(super) fn get(&self, var: &VarName) -> Option<EnvironmentId> {
        self.slots.get(var).copied()
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = &VarName> {
        self.slots.keys()
    }

    pub(super) fn len(&self) -> usize {
        self.slots.len()
    }
}
