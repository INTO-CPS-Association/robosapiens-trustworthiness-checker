use ecow::EcoVec;
use serde::Serialize;

use crate::VarName;

/// One externally observed DSRV input at a semisynchronous logical tick.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct TimedAtom {
    pub input: VarName,
    pub logical_tick: u64,
}

impl TimedAtom {
    pub fn new(input: VarName, logical_tick: u64) -> Self {
        Self {
            input,
            logical_tick,
        }
    }
}

/// Sorted, duplicate-free timed atoms with cheap copy-on-write cloning.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct AtomSet(EcoVec<TimedAtom>);

impl AtomSet {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn singleton(atom: TimedAtom) -> Self {
        Self(EcoVec::from([atom]))
    }

    pub fn from_atoms(atoms: impl IntoIterator<Item = TimedAtom>) -> Self {
        let mut atoms = atoms.into_iter().collect::<Vec<_>>();
        atoms.sort();
        atoms.dedup();
        Self(atoms.into())
    }

    pub fn union(&self, other: &Self) -> Self {
        Self::from_atoms(self.iter().cloned().chain(other.iter().cloned()))
    }

    pub fn is_subset(&self, other: &Self) -> bool {
        self.iter().all(|atom| other.0.binary_search(atom).is_ok())
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &TimedAtom> {
        self.0.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
