//! Counters for evaluator-owned collection operations.
//!
//! The canonical evaluator owns every list and map operation (they never run in the scalar
//! tiers). Its collection sites report here: list appends and concatenations, the elements handed
//! to `List.map`/`filter`/`fold`, and persistent map updates.
//!
//! # What the counters mean
//!
//! - A list append or concatenation is a *buffer replacement* when its result does not reuse the
//!   left operand's buffer, observed by comparing data pointers after the operation. It estimates
//!   copying of the left list: a replaced buffer usually means its entries were copied, but a
//!   unique buffer that merely reallocated to grow also shows as replaced, and nothing is
//!   measured with an allocator. Counts of replaced entries are therefore estimates, not exact
//!   copy counts.
//! - A concatenation with an empty left operand returns the right operand unchanged (and one
//!   with an empty right operand returns the left), so no entry is moved: it is counted in
//!   `list_concat_operand_reuses` and contributes nothing to the entry counts. Otherwise the
//!   right operand's entries are appended into the left buffer; they are counted in
//!   `list_concat_rhs_appended_entries` as transferred (cloned handle by handle), not as copied
//!   lists.
//! - `list_scan_entries` counts the elements of lists passed to `map`, `filter` and `fold`, counted
//!   before the scan. It is an upper bound on element callbacks: a callback error stops a scan
//!   early.
//! - Map updates count inserts and removals with the resulting map size only. A persistent map
//!   copies the path to the changed entry, never the whole map, and cloning a map handle is O(1);
//!   neither is reported as a copy.
//!
//! # Ownership
//!
//! The counters live in a thread-local cell owned by a stack of [`CollectionScope`]s. Every
//! monitor tick enters a scope: an observed monitor's scope counts, an unobserved monitor's scope
//! suspends counting, and leaving a scope — normally or by unwinding — restores the enclosing
//! scope's counters and armed state. A host function that evaluates another monitor during a tick
//! therefore attributes the inner monitor's operations to the inner recorder only, and the outer
//! recorder resumes with its own counts. Without the `diagnostics` feature every function here is
//! empty and no scope exists.

#[cfg(feature = "diagnostics")]
mod enabled {
    use std::cell::Cell;

    use serde::Serialize;

    use crate::core::Value;

    /// Collection operations counted over observed ticks.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
    pub(in crate::dataflow) struct CollectionCounters {
        pub(in crate::dataflow) list_appends: u64,
        /// Appends whose result did not reuse the source list's buffer.
        pub(in crate::dataflow) list_append_buffer_replacements: u64,
        /// Source entries of those appends: an estimate of entries copied.
        pub(in crate::dataflow) list_append_replaced_entries: u64,
        pub(in crate::dataflow) list_concats: u64,
        /// Concatenations whose result did not reuse the left operand's buffer.
        pub(in crate::dataflow) list_concat_lhs_buffer_replacements: u64,
        /// Left-operand entries of those concatenations: an estimate of entries copied.
        pub(in crate::dataflow) list_concat_lhs_replaced_entries: u64,
        /// Right-operand entries appended into the left buffer. Zero when either operand is empty.
        pub(in crate::dataflow) list_concat_rhs_appended_entries: u64,
        /// Concatenations with an empty operand, which return the other operand unchanged.
        pub(in crate::dataflow) list_concat_operand_reuses: u64,
        /// `List.map`, `List.filter` and `List.fold` applications.
        pub(in crate::dataflow) list_scans: u64,
        /// Elements of the scanned lists: an upper bound on element callbacks.
        pub(in crate::dataflow) list_scan_entries: u64,
        pub(in crate::dataflow) max_list_len: u64,
        /// Persistent map inserts and removals.
        pub(in crate::dataflow) map_updates: u64,
        pub(in crate::dataflow) max_map_len: u64,
    }

    impl CollectionCounters {
        pub(in crate::dataflow) fn merge(&mut self, other: &Self) {
            self.list_appends += other.list_appends;
            self.list_append_buffer_replacements += other.list_append_buffer_replacements;
            self.list_append_replaced_entries += other.list_append_replaced_entries;
            self.list_concats += other.list_concats;
            self.list_concat_lhs_buffer_replacements += other.list_concat_lhs_buffer_replacements;
            self.list_concat_lhs_replaced_entries += other.list_concat_lhs_replaced_entries;
            self.list_concat_rhs_appended_entries += other.list_concat_rhs_appended_entries;
            self.list_concat_operand_reuses += other.list_concat_operand_reuses;
            self.list_scans += other.list_scans;
            self.list_scan_entries += other.list_scan_entries;
            self.max_list_len = self.max_list_len.max(other.max_list_len);
            self.map_updates += other.map_updates;
            self.max_map_len = self.max_map_len.max(other.max_map_len);
        }
    }

    const ZERO: CollectionCounters = CollectionCounters {
        list_appends: 0,
        list_append_buffer_replacements: 0,
        list_append_replaced_entries: 0,
        list_concats: 0,
        list_concat_lhs_buffer_replacements: 0,
        list_concat_lhs_replaced_entries: 0,
        list_concat_rhs_appended_entries: 0,
        list_concat_operand_reuses: 0,
        list_scans: 0,
        list_scan_entries: 0,
        max_list_len: 0,
        map_updates: 0,
        max_map_len: 0,
    };

    thread_local! {
        static ARMED: Cell<bool> = const { Cell::new(false) };
        static COUNTERS: Cell<CollectionCounters> = const { Cell::new(ZERO) };
    }

    /// One monitor tick's counting scope. Dropping it, including during unwinding, restores the
    /// enclosing scope's counters and armed state.
    pub(in crate::dataflow) struct CollectionScope {
        saved_armed: bool,
        saved_counters: CollectionCounters,
        finished: bool,
    }

    impl CollectionScope {
        /// Enter a scope that counts (`observed`) or suspends counting.
        pub(in crate::dataflow) fn enter(observed: bool) -> Self {
            let saved_armed = ARMED.with(|armed| armed.replace(observed));
            let saved_counters = COUNTERS.with(|counters| counters.replace(ZERO));
            Self {
                saved_armed,
                saved_counters,
                finished: false,
            }
        }

        /// Leave the scope, returning what it counted.
        pub(in crate::dataflow) fn finish(mut self) -> CollectionCounters {
            self.finished = true;
            self.restore()
        }

        fn restore(&self) -> CollectionCounters {
            ARMED.with(|armed| armed.set(self.saved_armed));
            COUNTERS.with(|counters| counters.replace(self.saved_counters))
        }
    }

    impl Drop for CollectionScope {
        fn drop(&mut self) {
            if !self.finished {
                self.restore();
            }
        }
    }

    #[inline]
    fn update(change: impl FnOnce(&mut CollectionCounters)) {
        if ARMED.with(Cell::get) {
            COUNTERS.with(|counters| {
                let mut current = counters.get();
                change(&mut current);
                counters.set(current);
            });
        }
    }

    /// The buffer identity and length of a list operand, taken before the operation.
    #[derive(Clone, Copy)]
    pub(in crate::dataflow) struct ListIdentity(Option<(usize, usize)>);

    #[inline]
    pub(in crate::dataflow) fn list_identity(value: &Value) -> ListIdentity {
        ListIdentity(match value {
            Value::List(items) if !items.is_empty() => {
                Some((items.as_slice().as_ptr() as usize, items.len()))
            }
            _ => None,
        })
    }

    fn list_len(value: &Value) -> u64 {
        match value {
            Value::List(items) => items.len() as u64,
            _ => 0,
        }
    }

    /// Source entries whose buffer the result replaced, if it did.
    fn replaced(before: ListIdentity, after: &Value) -> Option<u64> {
        let (pointer, len) = before.0?;
        match after {
            Value::List(items) if items.as_slice().as_ptr() as usize != pointer => Some(len as u64),
            _ => None,
        }
    }

    #[inline]
    pub(in crate::dataflow) fn list_append(before: ListIdentity, after: &Value) {
        update(|counters| {
            counters.list_appends += 1;
            if let Some(entries) = replaced(before, after) {
                counters.list_append_buffer_replacements += 1;
                counters.list_append_replaced_entries += entries;
            }
            counters.max_list_len = counters.max_list_len.max(list_len(after));
        });
    }

    /// `rhs_len` is the right operand's length, read before the operation.
    #[inline]
    pub(in crate::dataflow) fn list_concat(before: ListIdentity, rhs_len: u64, after: &Value) {
        update(|counters| {
            counters.list_concats += 1;
            if before.0.is_none() || rhs_len == 0 {
                // An empty operand: the other one is returned unchanged.
                counters.list_concat_operand_reuses += 1;
            } else {
                counters.list_concat_rhs_appended_entries += rhs_len;
            }
            if let Some(entries) = replaced(before, after) {
                counters.list_concat_lhs_buffer_replacements += 1;
                counters.list_concat_lhs_replaced_entries += entries;
            }
            counters.max_list_len = counters.max_list_len.max(list_len(after));
        });
    }

    #[inline]
    pub(in crate::dataflow) fn list_operand_len(value: &Value) -> u64 {
        list_len(value)
    }

    #[inline]
    pub(in crate::dataflow) fn list_scan(list: &Value) {
        let Value::List(items) = list else {
            return;
        };
        let len = items.len() as u64;
        update(|counters| {
            counters.list_scans += 1;
            counters.list_scan_entries += len;
            counters.max_list_len = counters.max_list_len.max(len);
        });
    }

    #[inline]
    pub(in crate::dataflow) fn map_update(result: &Value) {
        let Value::Map(map) = result else {
            return;
        };
        let len = map.len() as u64;
        update(|counters| {
            counters.map_updates += 1;
            counters.max_map_len = counters.max_map_len.max(len);
        });
    }
}

#[cfg(not(feature = "diagnostics"))]
mod disabled {
    use crate::core::Value;

    #[derive(Clone, Copy)]
    pub(in crate::dataflow) struct ListIdentity;

    #[inline(always)]
    pub(in crate::dataflow) fn list_identity(_value: &Value) -> ListIdentity {
        ListIdentity
    }

    #[inline(always)]
    pub(in crate::dataflow) fn list_append(_before: ListIdentity, _after: &Value) {}

    #[inline(always)]
    pub(in crate::dataflow) fn list_operand_len(_value: &Value) {}

    #[inline(always)]
    pub(in crate::dataflow) fn list_concat(_before: ListIdentity, _rhs_len: (), _after: &Value) {}

    #[inline(always)]
    pub(in crate::dataflow) fn list_scan(_list: &Value) {}

    #[inline(always)]
    pub(in crate::dataflow) fn map_update(_result: &Value) {}
}

#[cfg(not(feature = "diagnostics"))]
pub(in crate::dataflow) use disabled::*;
#[cfg(feature = "diagnostics")]
pub(in crate::dataflow) use enabled::*;

#[cfg(all(test, feature = "diagnostics"))]
mod tests {
    use super::*;
    use crate::core::Value;

    fn appended(list: Value) -> Value {
        let before = list_identity(&list);
        let Value::List(mut items) = list else {
            unreachable!()
        };
        items.push(Value::Int(0));
        let result = Value::List(items);
        list_append(before, &result);
        result
    }

    #[test]
    fn nested_scopes_restore_the_enclosing_counts() {
        let outer = CollectionScope::enter(true);
        appended(Value::List([Value::Int(1)].into_iter().collect()));
        {
            // An unobserved inner monitor suspends counting.
            let inner = CollectionScope::enter(false);
            appended(Value::List([Value::Int(1)].into_iter().collect()));
            assert_eq!(inner.finish().list_appends, 0);
        }
        {
            // An observed inner monitor counts only its own operations.
            let inner = CollectionScope::enter(true);
            appended(Value::List([Value::Int(1)].into_iter().collect()));
            appended(Value::List([Value::Int(1)].into_iter().collect()));
            assert_eq!(inner.finish().list_appends, 2);
        }
        appended(Value::List([Value::Int(1)].into_iter().collect()));
        assert_eq!(outer.finish().list_appends, 2);
    }

    #[test]
    fn unwinding_restores_the_enclosing_scope() {
        let outer = CollectionScope::enter(true);
        appended(Value::List([Value::Int(1)].into_iter().collect()));
        let unwound = std::panic::catch_unwind(|| {
            let _inner = CollectionScope::enter(true);
            appended(Value::List([Value::Int(1)].into_iter().collect()));
            panic!("a host callback failed");
        });
        assert!(unwound.is_err());
        appended(Value::List([Value::Int(1)].into_iter().collect()));
        assert_eq!(
            outer.finish().list_appends,
            2,
            "the inner count is not merged"
        );
        // Nothing stays armed after the outermost scope.
        appended(Value::List([Value::Int(1)].into_iter().collect()));
        let probe = CollectionScope::enter(false);
        assert_eq!(probe.finish().list_appends, 0);
    }

    #[test]
    fn a_shared_buffer_is_replaced_and_a_unique_one_reused() {
        let scope = CollectionScope::enter(true);
        let mut spare = ecow::EcoVec::with_capacity(8);
        spare.extend((0..3).map(Value::Int));
        appended(Value::List(spare));
        let shared = Value::List((0..3).map(Value::Int).collect());
        let _held = shared.clone();
        appended(shared);
        let counters = scope.finish();
        assert_eq!(counters.list_appends, 2);
        assert_eq!(counters.list_append_buffer_replacements, 1);
        assert_eq!(counters.list_append_replaced_entries, 3);
    }

    fn concatenated(lhs: Value, rhs: Value) -> Value {
        use crate::core::values::operations as value_operations;
        let before = list_identity(&lhs);
        let rhs_len = list_operand_len(&rhs);
        let result = value_operations::list_concat(lhs, rhs).unwrap();
        list_concat(before, rhs_len, &result);
        result
    }

    fn list_of(len: i64) -> Value {
        Value::List((0..len).map(Value::Int).collect())
    }

    #[test]
    fn concatenation_with_an_empty_side_transfers_nothing() {
        // Empty left: the right operand is returned unchanged.
        let scope = CollectionScope::enter(true);
        let rhs = list_of(5);
        let Value::List(rhs_items) = &rhs else {
            unreachable!()
        };
        let rhs_address = rhs_items.as_slice().as_ptr();
        let result = concatenated(Value::List(ecow::EcoVec::new()), rhs.clone());
        let Value::List(result_items) = &result else {
            unreachable!()
        };
        assert_eq!(result_items.as_slice().as_ptr(), rhs_address);
        let counters = scope.finish();
        assert_eq!(counters.list_concats, 1);
        assert_eq!(counters.list_concat_operand_reuses, 1);
        assert_eq!(counters.list_concat_rhs_appended_entries, 0);
        assert_eq!(counters.list_concat_lhs_buffer_replacements, 0);
        assert_eq!(counters.list_concat_lhs_replaced_entries, 0);
        assert_eq!(counters.max_list_len, 5);

        // Empty right: the left operand is returned unchanged.
        let scope = CollectionScope::enter(true);
        concatenated(list_of(4), Value::List(ecow::EcoVec::new()));
        let counters = scope.finish();
        assert_eq!(counters.list_concat_operand_reuses, 1);
        assert_eq!(counters.list_concat_rhs_appended_entries, 0);
        assert_eq!(counters.list_concat_lhs_buffer_replacements, 0);
    }

    #[test]
    fn concatenation_of_two_lists_appends_the_right_entries() {
        let scope = CollectionScope::enter(true);
        // A shared left operand must be detached: its buffer is replaced.
        let lhs = list_of(3);
        let _held = lhs.clone();
        concatenated(lhs, list_of(2));
        let counters = scope.finish();
        assert_eq!(counters.list_concat_operand_reuses, 0);
        assert_eq!(counters.list_concat_rhs_appended_entries, 2);
        assert_eq!(counters.list_concat_lhs_buffer_replacements, 1);
        assert_eq!(counters.list_concat_lhs_replaced_entries, 3);
        assert_eq!(counters.max_list_len, 5);
    }

    #[test]
    fn scans_count_entries_before_the_scan() {
        let scope = CollectionScope::enter(true);
        list_scan(&list_of(7));
        list_scan(&Value::Int(1));
        let counters = scope.finish();
        assert_eq!(counters.list_scans, 1);
        assert_eq!(counters.list_scan_entries, 7);
    }
}
