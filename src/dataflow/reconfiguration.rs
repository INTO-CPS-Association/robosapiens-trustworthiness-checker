//! Semantic identities and the shared replacement contract for dataflow regions.
//!
//! This module deliberately contains portable, cold-path metadata.  Installed dataflow execution
//! continues to use dense `StreamId`, `EnvironmentSlot`, and `NodeId` values; these types are used
//! while compiling a replacement, validating a frontier, and constructing transfer reports.
//!
//! Replacement is serial and terminal.  There is no prepared transaction, no candidate generation,
//! and no rollback vocabulary: root and nested replacement share only the semantic target/frontier
//! validation performed by [`validate_replacement`], the normalized [`DefinitionKey`] identity, the
//! [`RevisionId`] history, and the error type reported when a replacement is refused.

use std::fmt;
use std::sync::Arc;

use crate::VarName;

/// A deterministic, source-independent owner address for a semantic region.
///
/// Addresses are intentionally strings rather than dense runtime handles.  They are suitable for
/// diagnostics, transfer reports, and revision construction, but must not be used for lookups on a
/// stable evaluation tick.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RegionAddress(Arc<str>);

impl RegionAddress {
    pub fn root() -> Self {
        Self(Arc::from("root"))
    }

    pub fn stream(stream: &VarName) -> Self {
        Self(Arc::from(format!("root/stream:{stream}")))
    }

    /// Construct a stable owner address from the declaration's source-owner binding and its
    /// occurrence among equal owners.  Active source text is intentionally absent.
    pub fn dynamic_body_owner(
        stream: &VarName,
        owner: impl fmt::Display,
        occurrence: usize,
    ) -> Self {
        Self(Arc::from(format!(
            "root/stream:{stream}/dynamic:{owner}:{occurrence}"
        )))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RegionAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A canonical semantic definition identity.
///
/// The canonical descriptor is retained alongside any digest by callers that want one.  Equality
/// therefore never relies on a collision-prone hash and never includes schedule or machine layout
/// data.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DefinitionKey(Arc<str>);

impl DefinitionKey {
    pub fn from_canonical(canonical: impl Into<String>) -> Self {
        Self(Arc::from(canonical.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DefinitionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Portable identity of a logical state owner.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StateKey {
    pub region: RegionAddress,
    pub owner: Arc<str>,
}

impl StateKey {
    pub fn new(region: RegionAddress, owner: impl Into<String>) -> Self {
        Self {
            region,
            owner: Arc::from(owner.into()),
        }
    }
}

/// The source of a replacement definition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DefinitionSource {
    Text(String),
}

impl DefinitionSource {
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text(text.into())
    }

    pub fn as_text(&self) -> &str {
        match self {
            Self::Text(text) => text,
        }
    }
}

/// A root or nested replacement target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplacementTarget {
    Root,
    Region(RegionAddress),
}

/// A semantic revision of the installed monitor.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RevisionId(pub u64);

impl fmt::Display for RevisionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl RevisionId {
    pub const INITIAL: Self = Self(0);

    /// Advance semantic history without ever reusing the maximum identity.
    pub fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

/// An input/output binding epoch.  It changes only when the root interface changes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InterfaceEpoch(pub u64);

impl fmt::Display for InterfaceEpoch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl InterfaceEpoch {
    pub const INITIAL: Self = Self(0);

    /// Advance interface history without silently reusing the maximum identity.
    pub fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

/// The two safe activation frontiers used by the shared replacement contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActivationFrontier {
    /// No stream has executed for the next tick, and the previous tick has committed.
    EmptyEvaluation {
        revision: RevisionId,
        interface_epoch: InterfaceEpoch,
    },
    /// A nested source barrier after its prerequisite closure and before its owner executes.
    SourceBarrier {
        revision: RevisionId,
        region: RegionAddress,
        owner_executed: bool,
    },
}

impl ActivationFrontier {
    pub fn revision(&self) -> RevisionId {
        match self {
            Self::EmptyEvaluation { revision, .. } | Self::SourceBarrier { revision, .. } => {
                *revision
            }
        }
    }
}

/// Reasons a replacement is refused before any state is changed.
///
/// Every variant is terminal for the runtime or monitor that reported it: root replacement
/// terminates the reconfigurable runtime and nested replacement poisons the monitor.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ReconfigurationError {
    #[error("replacement is based on revision {requested}, but the active revision is {active}")]
    StaleRevision {
        requested: RevisionId,
        active: RevisionId,
    },
    #[error("root replacement requires an empty evaluation frontier")]
    InvalidRootFrontier,
    #[error("nested replacement requires a source-barrier frontier")]
    InvalidNestedFrontier,
    #[error("replacement owner `{0}` has already executed")]
    OwnerAlreadyExecuted(RegionAddress),
    #[error("replacement definition is empty")]
    EmptyDefinition,
    #[error("replacement definition does not belong to the requested region")]
    TargetMismatch,
}

/// Validate the target, definition, and frontier shared by root and nested replacement.
///
/// This is the one place where both replacement paths agree on what a safe activation point is.
/// It performs no mutation, so a failure is reported before the caller changes anything; the
/// caller then applies its own terminal policy.
pub fn validate_replacement(
    target: &ReplacementTarget,
    definition: &DefinitionSource,
    frontier: &ActivationFrontier,
    base_revision: RevisionId,
) -> Result<(), ReconfigurationError> {
    if base_revision != frontier.revision() {
        return Err(ReconfigurationError::StaleRevision {
            requested: base_revision,
            active: frontier.revision(),
        });
    }
    if definition.as_text().trim().is_empty() {
        return Err(ReconfigurationError::EmptyDefinition);
    }
    match (target, frontier) {
        (ReplacementTarget::Root, ActivationFrontier::EmptyEvaluation { .. }) => Ok(()),
        (ReplacementTarget::Root, _) => Err(ReconfigurationError::InvalidRootFrontier),
        (
            ReplacementTarget::Region(target),
            ActivationFrontier::SourceBarrier {
                region,
                owner_executed,
                ..
            },
        ) if !owner_executed && target == region => Ok(()),
        (
            ReplacementTarget::Region(_),
            ActivationFrontier::SourceBarrier {
                owner_executed: false,
                ..
            },
        ) => Err(ReconfigurationError::TargetMismatch),
        (ReplacementTarget::Region(address), ActivationFrontier::SourceBarrier { .. }) => {
            Err(ReconfigurationError::OwnerAlreadyExecuted(address.clone()))
        }
        (ReplacementTarget::Region(_), _) => Err(ReconfigurationError::InvalidNestedFrontier),
    }
}

/// An individual state decision included in a transfer report.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferReportEntry {
    pub address: RegionAddress,
    pub state: StateKey,
    pub decision: TransferDecision,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferDecision {
    Transferred,
    Reset(Arc<str>),
    Rejected(Arc<str>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_frontier_must_name_the_requested_region() {
        let requested = RegionAddress::dynamic_body_owner(&VarName::new("a"), "test", 0);
        let barrier = RegionAddress::dynamic_body_owner(&VarName::new("b"), "test", 0);
        let error = validate_replacement(
            &ReplacementTarget::Region(requested),
            &DefinitionSource::text("x + 1"),
            &ActivationFrontier::SourceBarrier {
                revision: RevisionId::INITIAL,
                region: barrier,
                owner_executed: false,
            },
            RevisionId::INITIAL,
        )
        .unwrap_err();

        assert_eq!(error, ReconfigurationError::TargetMismatch);
    }

    #[test]
    fn root_replacement_requires_an_empty_evaluation_frontier() {
        let error = validate_replacement(
            &ReplacementTarget::Root,
            &DefinitionSource::text("in x\nout z\nz = x"),
            &ActivationFrontier::SourceBarrier {
                revision: RevisionId::INITIAL,
                region: RegionAddress::root(),
                owner_executed: false,
            },
            RevisionId::INITIAL,
        )
        .unwrap_err();

        assert_eq!(error, ReconfigurationError::InvalidRootFrontier);
    }

    #[test]
    fn an_empty_definition_is_refused_for_either_target() {
        let error = validate_replacement(
            &ReplacementTarget::Root,
            &DefinitionSource::text("   \n"),
            &ActivationFrontier::EmptyEvaluation {
                revision: RevisionId::INITIAL,
                interface_epoch: InterfaceEpoch::INITIAL,
            },
            RevisionId::INITIAL,
        )
        .unwrap_err();

        assert_eq!(error, ReconfigurationError::EmptyDefinition);
    }
}
