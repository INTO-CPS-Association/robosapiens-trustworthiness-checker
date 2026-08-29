use std::collections::BTreeMap;

use crate::VarName;
use crate::core::StreamType;

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
    types: Vec<Option<StreamType>>,
}

impl EnvironmentLayout {
    pub(super) fn from_variables(variables: impl IntoIterator<Item = VarName>) -> Self {
        Self::from_variables_with_types(variables, &BTreeMap::new())
    }

    pub(super) fn from_variables_with_types(
        variables: impl IntoIterator<Item = VarName>,
        annotations: &BTreeMap<VarName, StreamType>,
    ) -> Self {
        let mut slots = BTreeMap::new();
        let mut types = Vec::new();
        for variable in variables {
            let slot = EnvironmentSlot::new(slots.len());
            assert!(
                slots.insert(variable.clone(), slot).is_none(),
                "duplicate variable `{variable}` in dataflow environment"
            );
            types.push(annotations.get(&variable).cloned());
        }
        let mut indices = slots.values().map(|slot| slot.index()).collect::<Vec<_>>();
        indices.sort_unstable();
        debug_assert!(indices.into_iter().eq(0..slots.len()));
        Self { slots, types }
    }

    pub(super) fn slot(&self, variable: &VarName) -> Option<EnvironmentSlot> {
        self.slots.get(variable).copied()
    }

    pub(super) fn variable(&self, slot: EnvironmentSlot) -> Option<&VarName> {
        self.slots
            .iter()
            .find_map(|(variable, &candidate)| (candidate == slot).then_some(variable))
    }

    pub(super) fn variables(&self) -> impl Iterator<Item = &VarName> {
        self.slots.keys()
    }

    pub(super) fn stream_type(&self, slot: EnvironmentSlot) -> Option<&StreamType> {
        self.types.get(slot.index()).and_then(Option::as_ref)
    }

    pub(super) fn len(&self) -> usize {
        self.slots.len()
    }
}
