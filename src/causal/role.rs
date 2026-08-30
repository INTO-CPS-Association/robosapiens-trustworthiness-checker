use serde::{
    Serialize,
    ser::{SerializeSeq, Serializer},
};

/// The role played by an observed atom in one causal explanation.
///
/// Roles belong to a cause occurrence, not to [`crate::causal::TimedAtom`] itself. The same
/// external observation can therefore have different roles in different
/// explanations, or both roles in one explanation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CausalRole {
    /// Evidence that directly contributes to an emitted value or truth result.
    Direct,
    /// Evidence that chooses a branch, fallback, or active stream.
    Selection,
    /// Absence evidence that keeps an earlier value active.
    Retention,
    /// Evidence that supplies a value while the primary value is unavailable.
    Initialization,
    /// Evidence that installs or activates a runtime property.
    Activation,
}

/// The normalized set of roles attached to one cause occurrence.
///
/// This deliberately remains a compact bitset: the role vocabulary is closed
/// and small, while iteration still provides a deterministic serialized form.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CausalRoles(u8);

impl CausalRoles {
    const DIRECT: u8 = 1;
    const SELECTION: u8 = 2;
    const RETENTION: u8 = 4;
    const INITIALIZATION: u8 = 8;
    const ACTIVATION: u8 = 16;

    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn direct() -> Self {
        Self(Self::DIRECT)
    }

    pub const fn selection() -> Self {
        Self(Self::SELECTION)
    }

    pub const fn retention() -> Self {
        Self(Self::RETENTION)
    }

    pub const fn initialization() -> Self {
        Self(Self::INITIALIZATION)
    }

    pub const fn activation() -> Self {
        Self(Self::ACTIVATION)
    }

    pub const fn from_role(role: CausalRole) -> Self {
        match role {
            CausalRole::Direct => Self::direct(),
            CausalRole::Selection => Self::selection(),
            CausalRole::Retention => Self::retention(),
            CausalRole::Initialization => Self::initialization(),
            CausalRole::Activation => Self::activation(),
        }
    }

    pub const fn contains(self, role: CausalRole) -> bool {
        self.0 & Self::from_role(role).0 != 0
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub(crate) const fn reannotate_direct(self, role: CausalRole) -> Self {
        if self.contains(CausalRole::Direct) {
            Self((self.0 & !Self::DIRECT) | Self::from_role(role).0)
        } else {
            self.union(Self::from_role(role))
        }
    }

    pub const fn is_subset(self, other: Self) -> bool {
        self.0 & !other.0 == 0
    }

    pub fn iter(self) -> impl Iterator<Item = CausalRole> {
        [
            CausalRole::Direct,
            CausalRole::Selection,
            CausalRole::Retention,
            CausalRole::Initialization,
            CausalRole::Activation,
        ]
        .into_iter()
        .filter(move |role| self.contains(*role))
    }
}

impl From<CausalRole> for CausalRoles {
    fn from(role: CausalRole) -> Self {
        Self::from_role(role)
    }
}

impl Serialize for CausalRoles {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(None)?;
        for role in self.iter() {
            sequence.serialize_element(&role)?;
        }
        sequence.end()
    }
}
