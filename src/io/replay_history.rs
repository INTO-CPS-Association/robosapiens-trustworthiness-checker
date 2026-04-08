use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use crate::{Value, VarName};

pub type ReplaySnapshot = BTreeMap<usize, BTreeMap<VarName, Value>>;

/// Storage backend used by input providers to optionally record deterministic replay history.
///
/// This allows providers (e.g. ROS, file) to share a single interface while deciding
/// whether history recording is enabled or disabled at construction time.
///
/// This functionality is destined to be replaced by context transfers or another better mechanism
/// once the distributed runtime is refactored to make use of Morten's work on the Reconfigurable
/// runtime, but for now we need it for the distributed runtime to be able to test potential new
/// plans against historical data.
#[derive(Debug, Clone)]
pub enum ReplayHistory {
    /// Store every replay row in-memory.
    StoreAll(Rc<RefCell<StoreAllReplayHistory>>),
    /// Disabled implementation: no-op recorder that never stores history.
    Disabled,
}

impl ReplayHistory {
    /// Create a StoreAll replay history recorder with an empty snapshot.
    pub fn store_all() -> Self {
        Self::StoreAll(Rc::new(RefCell::new(StoreAllReplayHistory::default())))
    }

    /// Create a StoreAll replay history recorder seeded with an existing snapshot.
    pub fn store_all_with_snapshot(snapshot: ReplaySnapshot) -> Self {
        Self::StoreAll(Rc::new(RefCell::new(StoreAllReplayHistory::from_snapshot(
            snapshot,
        ))))
    }

    /// Create a disabled replay history recorder.
    pub fn disabled() -> Self {
        Self::Disabled
    }

    /// Returns true if this recorder stores all replay rows.
    pub fn is_store_all(&self) -> bool {
        matches!(self, Self::StoreAll(_))
    }

    /// Return a snapshot clone for StoreAll history, or `None` for disabled history.
    pub fn snapshot(&self) -> Option<ReplaySnapshot> {
        match self {
            Self::StoreAll(inner) => Some(inner.borrow().snapshot.clone()),
            Self::Disabled => None,
        }
    }

    /// Record one deterministic event row.
    ///
    /// - StoreAll: inserts at current clock and advances clock.
    /// - Disabled: no-op.
    pub fn record_row(&self, row: BTreeMap<VarName, Value>) {
        if let Self::StoreAll(inner) = self {
            inner.borrow_mut().record_row(row);
        }
    }

    /// Convenience helper for providers whose semantics are:
    /// "one variable has a concrete value, all others are NoVal".
    ///
    /// - StoreAll: records constructed row and advances clock.
    /// - Disabled: no-op.
    pub fn record_sparse_event(
        &self,
        var_names: impl Iterator<Item = VarName>,
        active_var: &VarName,
        active_value: Value,
    ) {
        if let Self::StoreAll(inner) = self {
            let row = var_names
                .map(|name| {
                    if &name == active_var {
                        (name, active_value.clone())
                    } else {
                        (name, Value::NoVal)
                    }
                })
                .collect();
            inner.borrow_mut().record_row(row);
        }
    }

    /// Clears StoreAll history and resets clock to 0. Disabled is a no-op.
    pub fn clear(&self) {
        if let Self::StoreAll(inner) = self {
            inner.borrow_mut().clear();
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct StoreAllReplayHistory {
    snapshot: ReplaySnapshot,
    clock: usize,
}

impl StoreAllReplayHistory {
    pub fn from_snapshot(snapshot: ReplaySnapshot) -> Self {
        let clock = snapshot
            .keys()
            .max()
            .map(|k| k.saturating_add(1))
            .unwrap_or(0);

        Self { snapshot, clock }
    }

    pub fn record_row(&mut self, row: BTreeMap<VarName, Value>) {
        let t = self.clock;
        self.snapshot.insert(t, row);
        self.clock = t.saturating_add(1);
    }

    pub fn clear(&mut self) {
        self.snapshot.clear();
        self.clock = 0;
    }

    pub fn clock(&self) -> usize {
        self.clock
    }

    pub fn snapshot(&self) -> &ReplaySnapshot {
        &self.snapshot
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_all_records_and_advances_clock() {
        let history = ReplayHistory::store_all();

        let mut row0 = BTreeMap::new();
        row0.insert("x".into(), Value::Int(1));
        history.record_row(row0);

        let mut row1 = BTreeMap::new();
        row1.insert("x".into(), Value::Int(2));
        history.record_row(row1);

        let snap = history.snapshot().unwrap();
        assert_eq!(snap.len(), 2);
        assert_eq!(snap.get(&0).unwrap().get(&"x".into()), Some(&Value::Int(1)));
        assert_eq!(snap.get(&1).unwrap().get(&"x".into()), Some(&Value::Int(2)));
    }

    #[test]
    fn disabled_never_records() {
        let history = ReplayHistory::disabled();

        let mut row0 = BTreeMap::new();
        row0.insert("x".into(), Value::Int(1));
        history.record_row(row0);

        assert!(history.snapshot().is_none());
    }

    #[test]
    fn store_all_with_snapshot_sets_next_clock() {
        let mut seed: ReplaySnapshot = BTreeMap::new();
        let mut row = BTreeMap::new();
        row.insert("x".into(), Value::Int(10));
        seed.insert(4, row);

        let history = ReplayHistory::store_all_with_snapshot(seed);

        let mut row2 = BTreeMap::new();
        row2.insert("x".into(), Value::Int(11));
        history.record_row(row2);

        let snap = history.snapshot().unwrap();
        assert!(snap.contains_key(&4));
        assert!(snap.contains_key(&5));
    }
}
