//! Stable semantic identities and cold-path results for dataflow reconfiguration.
//!
//! Execution uses dense local slots. These types describe the owners, revisions, and outcomes that
//! cross a compilation or source-barrier boundary.

use std::fmt;
use std::sync::Arc;

use crate::VarName;
use crate::fingerprint;

/// The stable identity of one reconfigurable expression occurrence.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ExpressionStateKey(Arc<str>);

impl ExpressionStateKey {
    pub(crate) fn new(stream: &VarName, owner: impl fmt::Display, occurrence: usize) -> Self {
        Self(Arc::from(format!("{stream}/{owner}/{occurrence}")))
    }
}

impl fmt::Display for ExpressionStateKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A precomputed semantic identity for a monitor definition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DefinitionKey(u128);

impl DefinitionKey {
    pub fn from_canonical(canonical: impl AsRef<[u8]>) -> Self {
        Self(fingerprint::fingerprint(
            "dataflow-definition-v1",
            canonical.as_ref(),
        ))
    }

    pub(crate) fn from_fingerprint(fingerprint: u128) -> Self {
        Self(fingerprint)
    }
}

impl fmt::Display for DefinitionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:032x}", self.0)
    }
}

/// A precomputed semantic identity for a complete stream evaluator state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamStateKey(u128);

impl StreamStateKey {
    pub(crate) fn from_canonical(canonical: &[u8]) -> Self {
        Self(fingerprint::fingerprint(
            "dataflow-stream-state-v1",
            canonical,
        ))
    }

    pub(crate) fn value(self) -> u128 {
        self.0
    }
}

impl fmt::Display for StreamStateKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:032x}", self.0)
    }
}

/// The semantic revision of the installed monitor definition.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MonitorRevision(pub u64);

impl MonitorRevision {
    pub const INITIAL: Self = Self(0);

    pub fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

impl fmt::Display for MonitorRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// The revision of the effective input/output interface.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InterfaceRevision(pub u64);

impl InterfaceRevision {
    pub const INITIAL: Self = Self(0);

    pub fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

impl fmt::Display for InterfaceRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Outcome for one top-level stream during context transfer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StreamStateTransferOutcome {
    /// The target stream matched by name and semantic state key.
    Transferred,
    /// The target stream starts with cold evaluator state.
    Initialized,
}

/// Context-transfer result for one top-level stream.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamStateTransfer {
    pub stream: VarName,
    pub outcome: StreamStateTransferOutcome,
}

impl StreamStateTransfer {
    pub fn new(stream: VarName, outcome: StreamStateTransferOutcome) -> Self {
        Self { stream, outcome }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revisions_advance_independently_and_checked() {
        assert_eq!(
            MonitorRevision::INITIAL.checked_next(),
            Some(MonitorRevision(1))
        );
        assert_eq!(
            InterfaceRevision::INITIAL.checked_next(),
            Some(InterfaceRevision(1))
        );
        assert_eq!(MonitorRevision(u64::MAX).checked_next(), None);
        assert_eq!(InterfaceRevision(u64::MAX).checked_next(), None);
    }

    #[test]
    fn semantic_keys_are_fixed_width_and_domain_separated() {
        let stream = StreamStateKey::from_canonical(b"x + 1");
        let definition = DefinitionKey::from_canonical(b"x + 1");

        assert_eq!(stream.to_string().len(), 32);
        assert_eq!(definition.to_string().len(), 32);
        assert_ne!(stream.to_string(), definition.to_string());
    }
}

/// Policy for carrying semantic state across a reconfiguration barrier.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ContextTransferPolicy {
    /// Start every candidate owner without prior state.
    None,
    /// Preserve state only for streams and nested bodies with an exact semantic match.
    #[default]
    MatchingStreamState,
}

/// Stream and variable-history outcomes from a root context transfer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ContextTransferReport {
    pub streams: Vec<StreamStateTransfer>,
    pub retained_history: Vec<VarName>,
}

impl ContextTransferReport {
    pub(crate) fn new(
        streams: impl IntoIterator<Item = StreamStateTransfer>,
        retained_history: impl IntoIterator<Item = VarName>,
    ) -> Self {
        Self {
            streams: streams.into_iter().collect(),
            retained_history: retained_history.into_iter().collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.streams.is_empty() && self.retained_history.is_empty()
    }
}

/// Result of a root or nested monitor reconfiguration.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReconfigurationReport {
    pub monitor_changed: bool,
    pub interface_changed: bool,
    pub monitor_revision: MonitorRevision,
    pub interface_revision: InterfaceRevision,
    pub context_transfer: ContextTransferReport,
}

impl ReconfigurationReport {
    pub fn new(
        monitor_changed: bool,
        interface_changed: bool,
        monitor_revision: MonitorRevision,
        interface_revision: InterfaceRevision,
        context_transfer: ContextTransferReport,
    ) -> Self {
        Self {
            monitor_changed,
            interface_changed,
            monitor_revision,
            interface_revision,
            context_transfer,
        }
    }

    pub fn changed(&self) -> bool {
        self.monitor_changed || self.interface_changed
    }
}

#[cfg(test)]
mod reconfiguration_contract_tests {
    use super::*;

    #[test]
    fn reconfiguration_report_changed_reflects_the_two_change_flags() {
        let unchanged = ReconfigurationReport::default();
        assert!(!unchanged.changed());

        let monitor_changed = ReconfigurationReport::new(
            true,
            false,
            MonitorRevision(1),
            InterfaceRevision::INITIAL,
            ContextTransferReport::default(),
        );
        assert!(monitor_changed.changed());

        let interface_changed = ReconfigurationReport::new(
            false,
            true,
            MonitorRevision::INITIAL,
            InterfaceRevision(1),
            ContextTransferReport::default(),
        );
        assert!(interface_changed.changed());
    }
}
