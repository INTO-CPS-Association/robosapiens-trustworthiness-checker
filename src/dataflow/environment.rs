use std::collections::BTreeMap;

use crate::VarName;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct EnvironmentSlot(usize);

impl EnvironmentSlot {
    #[inline]
    pub(super) fn new(index: usize) -> Self {
        Self(index)
    }

    #[inline]
    pub(super) fn index(self) -> usize {
        self.0
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct EnvironmentLayout {
    slots: BTreeMap<VarName, EnvironmentSlot>,
}

impl EnvironmentLayout {
    pub(super) fn from_variables(variables: impl IntoIterator<Item = VarName>) -> Self {
        let mut slots = BTreeMap::new();
        for variable in variables {
            let slot = EnvironmentSlot::new(slots.len());
            assert!(
                slots.insert(variable.clone(), slot).is_none(),
                "duplicate variable `{variable}` in dataflow environment"
            );
        }
        let mut indices = slots.values().map(|slot| slot.index()).collect::<Vec<_>>();
        indices.sort_unstable();
        debug_assert!(indices.into_iter().eq(0..slots.len()));
        Self { slots }
    }

    pub(super) fn slot(&self, variable: &VarName) -> Option<EnvironmentSlot> {
        self.slots.get(variable).copied()
    }

    pub(super) fn variables(&self) -> impl Iterator<Item = &VarName> {
        self.slots.keys()
    }

    pub(super) fn len(&self) -> usize {
        self.slots.len()
    }
}
