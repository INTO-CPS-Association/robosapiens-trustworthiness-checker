use super::environment::EnvironmentSlot;
use crate::core::Value;
use std::ops::{Index, IndexMut};

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(in crate::dataflow) struct HistoryId(usize);

/// Read-only access to the monitor's named variable histories.
#[derive(Clone, Copy)]
pub(in crate::dataflow) struct HistoryAccess<'a> {
    store: &'a HistoryStore,
    bindings: &'a [Option<HistoryId>],
}

impl<'a> HistoryAccess<'a> {
    #[inline]
    pub(in crate::dataflow) fn new(
        store: &'a HistoryStore,
        bindings: &'a [Option<HistoryId>],
    ) -> Self {
        Self { store, bindings }
    }

    #[inline]
    pub(in crate::dataflow) fn read(self, slot: EnvironmentSlot, offset: usize) -> Value {
        self.bindings
            .get(slot.index())
            .copied()
            .flatten()
            .map_or(Value::Deferred, |history| self.store.read(history, offset))
    }

    #[inline]
    pub(in crate::dataflow) fn binding(self, slot: EnvironmentSlot) -> Option<HistoryId> {
        self.bindings.get(slot.index()).copied().flatten()
    }

    #[inline]
    pub(in crate::dataflow) fn has_binding(self, slot: EnvironmentSlot) -> bool {
        self.binding(slot).is_some()
    }

    #[inline]
    pub(in crate::dataflow) fn with_bindings<'b>(
        self,
        bindings: &'b [Option<HistoryId>],
    ) -> HistoryAccess<'b>
    where
        'a: 'b,
    {
        HistoryAccess {
            store: self.store,
            bindings,
        }
    }
}

impl HistoryId {
    #[inline]
    pub(in crate::dataflow) fn new(index: usize) -> Self {
        Self(index)
    }

    #[inline]
    pub(in crate::dataflow) fn index(self) -> usize {
        self.0
    }
}

#[derive(Debug)]
pub(in crate::dataflow) struct ValueHistory {
    slots: Vec<Value>,
    next_write: usize,
    len: usize,
    required_depth: usize,
    live: bool,
}

impl ValueHistory {
    pub(in crate::dataflow) fn new(required_depth: usize) -> Self {
        Self {
            slots: Self::empty_slots(required_depth),
            next_write: 0,
            len: 0,
            required_depth,
            live: true,
        }
    }

    fn tombstone() -> Self {
        Self {
            slots: Vec::new(),
            next_write: 0,
            len: 0,
            required_depth: 0,
            live: false,
        }
    }

    #[inline]
    pub(in crate::dataflow) fn required_depth(&self) -> usize {
        self.required_depth
    }

    #[cfg(test)]
    #[inline]
    pub(in crate::dataflow) fn capacity(&self) -> usize {
        self.slots.len()
    }

    #[cfg(test)]
    pub(in crate::dataflow) fn slots_ptr(&self) -> *const Value {
        self.slots.as_ptr()
    }

    #[cfg(test)]
    #[inline]
    pub(in crate::dataflow) fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    #[inline]
    pub(in crate::dataflow) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(in crate::dataflow) fn read(&self, offset: usize) -> Value {
        if offset == 0 || offset > self.required_depth || offset > self.len {
            return Value::Deferred;
        }

        let capacity = self.slots.len();
        debug_assert!(capacity > 0);
        let index = (self.next_write + capacity - offset) % capacity;
        self.slots[index].clone()
    }

    pub(in crate::dataflow) fn commit(&mut self, value: Value) {
        if self.required_depth == 0 {
            return;
        }

        let capacity = self.slots.len();
        debug_assert!(capacity > 0);
        self.slots[self.next_write] = value;
        self.next_write = Self::advance(self.next_write, capacity);
        self.len = (self.len + 1).min(self.required_depth);
    }

    pub(in crate::dataflow) fn set_required_depth(&mut self, required_depth: usize) {
        if required_depth == self.required_depth {
            return;
        }

        if required_depth <= self.slots.len() {
            self.required_depth = required_depth;
            self.len = self.len.min(required_depth);
            return;
        }

        self.rebuild(required_depth, required_depth);
        self.required_depth = required_depth;
    }

    pub(in crate::dataflow) fn reset(&mut self) {
        for slot in &mut self.slots {
            *slot = Value::NoVal;
        }
        self.next_write = 0;
        self.len = 0;
    }

    fn rebuild(&mut self, capacity: usize, required_depth: usize) {
        let old_capacity = self.slots.len();
        let retained = self.len.min(required_depth);
        let mut old_slots = std::mem::take(&mut self.slots);
        let mut slots = Self::empty_slots(capacity);

        if old_capacity != 0 {
            for index in 0..retained {
                let old_index = (self.next_write + old_capacity - retained + index) % old_capacity;
                slots[index] = std::mem::replace(&mut old_slots[old_index], Value::NoVal);
            }
        }

        self.slots = slots;
        self.next_write = retained;
        self.len = retained;
    }

    fn empty_slots(capacity: usize) -> Vec<Value> {
        let mut slots = Vec::with_capacity(capacity);
        slots.resize_with(capacity, || Value::NoVal);
        slots
    }

    #[inline]
    fn advance(index: usize, capacity: usize) -> usize {
        let next = index + 1;
        if next == capacity { 0 } else { next }
    }
}

#[derive(Debug, Default)]
pub(in crate::dataflow) struct HistoryStore {
    histories: Vec<ValueHistory>,
    live_count: usize,
}

impl HistoryStore {
    pub(in crate::dataflow) fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    #[inline]
    pub(in crate::dataflow) fn len(&self) -> usize {
        self.histories.len()
    }

    /// Tombstones do not count as live entries.
    #[inline]
    pub(in crate::dataflow) fn is_empty(&self) -> bool {
        self.live_count == 0
    }

    pub(in crate::dataflow) fn allocate(&mut self, required_depth: usize) -> HistoryId {
        let id = HistoryId::new(self.histories.len());
        self.histories.push(ValueHistory::new(required_depth));
        self.live_count += 1;
        id
    }

    #[inline]
    pub(in crate::dataflow) fn get(&self, id: HistoryId) -> Option<&ValueHistory> {
        self.histories
            .get(id.index())
            .filter(|history| history.live)
    }

    #[inline]
    pub(in crate::dataflow) fn get_mut(&mut self, id: HistoryId) -> Option<&mut ValueHistory> {
        self.histories
            .get_mut(id.index())
            .filter(|history| history.live)
    }

    #[inline]
    pub(in crate::dataflow) fn read(&self, id: HistoryId, offset: usize) -> Value {
        if self.is_empty() {
            return Value::Deferred;
        }
        self.get(id)
            .map_or(Value::Deferred, |history| history.read(offset))
    }

    pub(in crate::dataflow) fn commit(&mut self, id: HistoryId, value: Value) {
        self.live_history_mut(id).commit(value);
    }

    pub(in crate::dataflow) fn set_required_depth(&mut self, id: HistoryId, required_depth: usize) {
        self.live_history_mut(id).set_required_depth(required_depth);
    }

    pub(in crate::dataflow) fn take(&mut self, id: HistoryId) -> ValueHistory {
        let index = id.index();
        assert!(
            self.histories
                .get(index)
                .is_some_and(|history| history.live),
            "cannot take unavailable history {id:?}"
        );
        self.live_count -= 1;
        std::mem::replace(&mut self.histories[index], ValueHistory::tombstone())
    }

    #[cfg(test)]
    pub(in crate::dataflow) fn adopt(&mut self, history: ValueHistory) -> HistoryId {
        debug_assert!(history.live);
        let id = HistoryId::new(self.histories.len());
        self.histories.push(history);
        self.live_count += 1;
        id
    }

    #[cfg(test)]
    pub(in crate::dataflow) fn adopt_from(
        &mut self,
        source: &mut Self,
        id: HistoryId,
    ) -> HistoryId {
        self.adopt(source.take(id))
    }

    pub(in crate::dataflow) fn replace_from(
        &mut self,
        target_id: HistoryId,
        source: &mut Self,
        source_id: HistoryId,
    ) {
        let target_index = target_id.index();
        let target_depth = self
            .histories
            .get(target_index)
            .filter(|history| history.live)
            .map(ValueHistory::required_depth)
            .unwrap_or_else(|| panic!("cannot replace unavailable history {target_id:?}"));
        let mut history = source.take(source_id);
        history.set_required_depth(target_depth);
        let replaced = std::mem::replace(&mut self.histories[target_index], history);
        debug_assert!(replaced.live);
    }

    pub(in crate::dataflow) fn reset(&mut self) {
        if self.is_empty() {
            return;
        }
        for history in &mut self.histories {
            if history.live {
                history.reset();
            }
        }
    }

    fn live_history_mut(&mut self, id: HistoryId) -> &mut ValueHistory {
        self.get_mut(id)
            .unwrap_or_else(|| panic!("history {id:?} is unavailable"))
    }
}

impl Index<HistoryId> for HistoryStore {
    type Output = ValueHistory;

    fn index(&self, id: HistoryId) -> &Self::Output {
        self.get(id)
            .unwrap_or_else(|| panic!("history {id:?} is unavailable"))
    }
}

impl IndexMut<HistoryId> for HistoryStore {
    fn index_mut(&mut self, id: HistoryId) -> &mut Self::Output {
        self.live_history_mut(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn ordering_and_wraparound_are_newest_first() {
        let mut history = ValueHistory::new(3);
        history.commit(Value::Int(1));
        history.commit(Value::Int(2));
        history.commit(Value::Int(3));
        assert_eq!(history.read(1), Value::Int(3));
        assert_eq!(history.read(2), Value::Int(2));
        assert_eq!(history.read(3), Value::Int(1));

        history.commit(Value::NoVal);
        assert_eq!(history.read(1), Value::NoVal);
        assert_eq!(history.read(2), Value::Int(3));
        assert_eq!(history.read(3), Value::Int(2));
    }

    #[test]
    fn unavailable_offsets_return_deferred() {
        let mut history = ValueHistory::new(3);
        assert_eq!(history.read(0), Value::Deferred);
        assert_eq!(history.read(1), Value::Deferred);
        assert_eq!(history.read(usize::MAX), Value::Deferred);

        history.commit(Value::Int(7));
        assert_eq!(history.read(1), Value::Int(7));
        assert_eq!(history.read(2), Value::Deferred);
        assert_eq!(history.read(4), Value::Deferred);
    }

    #[test]
    fn growing_preserves_the_newest_suffix() {
        let mut history = ValueHistory::new(2);
        history.commit(Value::Int(1));
        history.commit(Value::Int(2));
        history.commit(Value::Int(3));
        history.set_required_depth(4);

        assert_eq!(history.read(1), Value::Int(3));
        assert_eq!(history.read(2), Value::Int(2));
        assert_eq!(history.read(3), Value::Deferred);

        history.commit(Value::Int(4));
        history.commit(Value::Int(5));
        assert_eq!(history.read(1), Value::Int(5));
        assert_eq!(history.read(2), Value::Int(4));
        assert_eq!(history.read(3), Value::Int(3));
        assert_eq!(history.read(4), Value::Int(2));
    }

    #[test]
    fn shrinking_does_not_expose_values_beyond_logical_depth() {
        let mut history = ValueHistory::new(4);
        for value in 1..=4 {
            history.commit(Value::Int(value));
        }
        let physical_capacity = history.capacity();
        history.set_required_depth(2);

        assert_eq!(history.capacity(), physical_capacity);
        assert_eq!(history.read(1), Value::Int(4));
        assert_eq!(history.read(2), Value::Int(3));
        assert_eq!(history.read(3), Value::Deferred);

        history.commit(Value::Int(5));
        assert_eq!(history.read(1), Value::Int(5));
        assert_eq!(history.read(2), Value::Int(4));
        assert_eq!(history.read(3), Value::Deferred);
    }

    #[test]
    fn depth_changes_within_capacity_do_not_move_slots() {
        let mut history = ValueHistory::new(4);
        for value in 1..=4 {
            history.commit(Value::Int(value));
        }

        let before_shrink = history.slots.as_ptr();
        history.set_required_depth(2);
        assert_eq!(history.slots.as_ptr(), before_shrink);
        assert_eq!(history.read(1), Value::Int(4));
        assert_eq!(history.read(2), Value::Int(3));

        history.commit(Value::Int(5));
        assert_eq!(history.read(1), Value::Int(5));
        assert_eq!(history.read(2), Value::Int(4));

        let before_regrowth = history.slots.as_ptr();
        history.set_required_depth(3);
        assert_eq!(history.slots.as_ptr(), before_regrowth);
        assert_eq!(history.read(3), Value::Deferred);

        history.commit(Value::Int(6));
        assert_eq!(history.read(1), Value::Int(6));
        assert_eq!(history.read(2), Value::Int(5));
        assert_eq!(history.read(3), Value::Int(4));
    }

    #[test]
    fn logical_wraparound_uses_depth_not_retained_physical_capacity() {
        let mut history = ValueHistory::new(8);
        for value in 1..=8 {
            history.commit(Value::Int(value));
        }
        history.set_required_depth(2);

        for value in 9..=12 {
            history.commit(Value::Int(value));
        }
        assert_eq!(history.capacity(), 8);
        assert_eq!(history.read(1), Value::Int(12));
        assert_eq!(history.read(2), Value::Int(11));
        assert_eq!(history.read(3), Value::Deferred);
    }

    #[test]
    fn taking_non_last_entry_keeps_later_ids_stable() {
        let mut store = HistoryStore::new();
        let first = store.allocate(1);
        let second = store.allocate(1);
        store.commit(second, Value::Int(2));

        let _taken = store.take(first);

        assert_eq!(store.len(), 2);
        assert!(!store.is_empty());
        assert!(store.get(first).is_none());
        assert_eq!(store.read(second, 1), Value::Int(2));
        assert_eq!(store[second].read(1), Value::Int(2));
    }

    #[test]
    fn entry_transfer_is_destructive_and_preserves_nontrivial_values() {
        let value = Value::Map(BTreeMap::from([(
            "payload".into(),
            Value::List(vec![Value::Int(9), Value::NoVal].into()),
        )]));
        let mut source = HistoryStore::new();
        let source_id = source.allocate(1);
        let unrelated_id = source.allocate(1);
        source.commit(source_id, value);
        source.commit(unrelated_id, Value::Int(22));

        let mut destination = HistoryStore::new();
        let destination_id = destination.adopt_from(&mut source, source_id);

        assert!(!source.is_empty());
        assert!(source.get(source_id).is_none());
        assert_eq!(source.read(unrelated_id, 1), Value::Int(22));
        assert_eq!(
            destination.read(destination_id, 1),
            Value::Map(BTreeMap::from([(
                "payload".into(),
                Value::List(vec![Value::Int(9), Value::NoVal].into()),
            )]))
        );
    }

    #[test]
    fn replacing_entry_keeps_target_ids_and_unrelated_values() {
        let mut source = HistoryStore::new();
        let source_id = source.allocate(3);
        source.commit(source_id, Value::Int(1));
        source.commit(source_id, Value::Int(2));
        source.commit(source_id, Value::Int(3));

        let mut target = HistoryStore::new();
        let target_id = target.allocate(2);
        let unrelated_id = target.allocate(1);
        target.commit(unrelated_id, Value::Int(22));

        target.replace_from(target_id, &mut source, source_id);

        assert_eq!(target_id.index(), 0);
        assert_eq!(unrelated_id.index(), 1);
        assert_eq!(target.read(target_id, 1), Value::Int(3));
        assert_eq!(target.read(target_id, 2), Value::Int(2));
        assert_eq!(target.read(unrelated_id, 1), Value::Int(22));
        assert!(source.get(source_id).is_none());
    }

    #[test]
    fn empty_store_reads_deferred_without_entries() {
        let mut store = HistoryStore::new();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
        assert_eq!(store.read(HistoryId::new(0), 1), Value::Deferred);
        store.reset();
        assert!(store.is_empty());
    }

    #[test]
    fn reset_clears_values_but_keeps_depth() {
        let mut history = ValueHistory::new(2);
        history.commit(Value::Int(1));
        history.reset();
        assert_eq!(history.required_depth(), 2);
        assert_eq!(history.read(1), Value::Deferred);
    }
}
