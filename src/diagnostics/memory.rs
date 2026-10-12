//! Structured memory estimates.
//!
//! Owners report their *fixed* storage directly from layouts and capacities, and hand every
//! retained [`Value`] to a [`ValueSizer`], which estimates the heap payload reachable from it.
//!
//! The estimate follows the value representation rather than an allocator:
//!
//! - lists and tuples are reference-counted [`ecow::EcoVec`]s; a buffer is charged once per
//!   snapshot by its data pointer, with its capacity, and a later handle to the same buffer is
//!   reported as `shared_bytes` instead of being charged again;
//! - strings longer than the inline limit are charged once per heap buffer in the same way;
//! - union and newtype payloads are `Rc`s, charged once per allocation;
//! - maps are persistent B+trees whose nodes are reference counted. Cloning a map copies a handle,
//!   not its entries. A map root is charged once per snapshot only when its handle is matched, by
//!   pointer equality, against one of the [`MAP_IDENTITY_WINDOW`] most recent same-sized roots; a
//!   clone seen again outside that window can be charged again (counted in
//!   `unresolved_map_identities`). Distinct roots that share interior nodes after an update cannot be
//!   told apart through the public API, so map entry storage is reported separately as
//!   `map_logical_bytes`: a logical footprint, deduplicated per root handle, that may count shared
//!   interior nodes more than once;
//! - function values, and the mutable capture lists they are built from, are opaque: they are
//!   counted and a capture list contributes its slot capacity, but neither is traversed;
//! - a snapshot reports for every owner both the bytes it is *charged* (`total_bytes`: fixed
//!   storage, payload first reached here, logical map storage) and, beside it,
//!   `bytes_with_shared_roots`: the charge plus the `shared_bytes` of payload roots this owner
//!   holds that an earlier owner was already charged for. Charges add up to the snapshot total
//!   without double counting, except that this is not guaranteed while `unresolved_map_identities`
//!   is nonzero: a cloned map outside the comparison window may be charged again.
//!   `bytes_with_shared_roots` is a partial figure, not a reachable
//!   total: a shared list, map, union or newtype root is counted as its own shell only (its
//!   buffer header and capacity), and its children are not re-walked, so what hangs below a
//!   shared root is absent. It overlaps between owners and must never be summed;
//! - map roots are matched against only the most recent [`MAP_IDENTITY_WINDOW`] same-sized
//!   roots, so a cloned handle seen again outside that window is charged again. Every such charge
//!   is counted in `unresolved_map_identities`, and while it is nonzero the charges are not
//!   guaranteed globally unique;
//!
//! Traversal never clones a value, never mutates it and stops at the configured depth and work
//! budgets, marking the estimate truncated.

use std::collections::{HashMap, HashSet};
use std::mem::size_of;

use ecow::{EcoString, EcoVec};
use serde::Serialize;

use crate::core::{Value, ValueMap};

/// Bounds on one snapshot's traversal.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct MemoryBudget {
    /// Maximum nesting depth followed inside one value.
    pub max_depth: usize,
    /// Maximum units of work in one snapshot. A unit is charged for each value visited (including
    /// map values), each map key and each map-root identity comparison, so it bounds more than
    /// the number of values.
    pub max_work: usize,
}

impl Default for MemoryBudget {
    fn default() -> Self {
        Self {
            max_depth: 32,
            max_work: 2_000_000,
        }
    }
}

/// Estimated heap payload reachable from one or more values.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct PayloadEstimate {
    /// Estimated list, tuple, string and `Rc` payload bytes first reached here in this snapshot.
    pub unique_bytes: u64,
    /// Shells (buffer header and capacity) of list, tuple, string, union and newtype roots
    /// reached through a handle already charged elsewhere in this snapshot. Only the shell is
    /// counted: the children of a shared root are not re-walked.
    pub shared_bytes: u64,
    /// Logical per-root estimate of persistent [`ValueMap`] entry storage first reached here.
    /// Each distinct root handle is charged its entries once; cloning a handle is O(1), and a
    /// handle matched against an earlier root is not charged again. Matching covers only the most
    /// recent [`MAP_IDENTITY_WINDOW`] same-sized roots, so a clone outside that window is charged
    /// again (see `unresolved_map_identities`). Interior nodes shared by distinct roots after an
    /// update cannot be observed and may be counted under each root.
    pub map_logical_bytes: u64,
    pub list_entries: u64,
    pub map_entries: u64,
    pub max_list_len: u64,
    pub max_map_len: u64,
    /// Function values, whose captured state is opaque to the estimator.
    pub opaque_values: u64,
    /// Map roots charged without comparing against every earlier root of the same size, because
    /// the identity window was full. Such a root may be a clone of an earlier one and so charged
    /// twice; while this is nonzero the charges are not guaranteed globally unique.
    pub unresolved_map_identities: u64,
    /// The depth or work budget stopped traversal below this point.
    pub truncated: bool,
}

impl PayloadEstimate {
    pub fn merge(&mut self, other: &Self) {
        self.unique_bytes += other.unique_bytes;
        self.shared_bytes += other.shared_bytes;
        self.map_logical_bytes += other.map_logical_bytes;
        self.list_entries += other.list_entries;
        self.map_entries += other.map_entries;
        self.max_list_len = self.max_list_len.max(other.max_list_len);
        self.max_map_len = self.max_map_len.max(other.max_map_len);
        self.opaque_values += other.opaque_values;
        self.unresolved_map_identities += other.unresolved_map_identities;
        self.truncated |= other.truncated;
    }

    /// Unique payload plus logical map storage: the estimate an owner is charged.
    pub fn charged_bytes(&self) -> u64 {
        self.unique_bytes + self.map_logical_bytes
    }
}

/// Fixed storage plus estimated payload of one owner.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MemoryEstimate {
    /// Bytes counted directly from layout and capacity: slots, rings, scratch vectors and boxes.
    pub fixed_bytes: u64,
    pub payload: PayloadEstimate,
}

impl Serialize for MemoryEstimate {
    /// The stored fields plus the two derived figures, so a consumer need not recompute them.
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct View<'a> {
            fixed_bytes: u64,
            /// Fixed bytes plus payload first reached here: what the owner is charged.
            total_bytes: u64,
            /// The charge plus the shells of shared payload roots charged to an earlier owner.
            /// Children of a shared root are not re-walked, so this is a partial figure.
            bytes_with_shared_roots: u64,
            #[serde(flatten)]
            payload: &'a PayloadEstimate,
        }
        View {
            fixed_bytes: self.fixed_bytes,
            total_bytes: self.total_bytes(),
            bytes_with_shared_roots: self.bytes_with_shared_roots(),
            payload: &self.payload,
        }
        .serialize(serializer)
    }
}

impl MemoryEstimate {
    pub fn fixed(bytes: usize) -> Self {
        Self {
            fixed_bytes: bytes as u64,
            payload: PayloadEstimate::default(),
        }
    }

    pub fn merge(&mut self, other: &Self) {
        self.fixed_bytes += other.fixed_bytes;
        self.payload.merge(&other.payload);
    }

    /// Fixed plus charged payload bytes.
    pub fn total_bytes(&self) -> u64 {
        self.fixed_bytes + self.payload.charged_bytes()
    }

    /// The charge plus `shared_bytes`: the shells of payload roots this owner holds that an
    /// earlier owner was charged for. Shared roots are not re-walked, so what hangs below them is
    /// absent: a partial figure, not a recursive total, and one that overlaps between owners.
    pub fn bytes_with_shared_roots(&self) -> u64 {
        self.total_bytes() + self.payload.shared_bytes
    }
}

/// One owner's line in a snapshot. `path` names the owner hierarchically, for example
/// `streams/total/node 3 sindex/ring`.
#[derive(Clone, Debug, Serialize)]
pub struct MemoryComponent {
    pub path: String,
    pub category: &'static str,
    /// The stream this component belongs to, when it has one; used for dependency aggregation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<String>,
    #[serde(flatten)]
    pub estimate: MemoryEstimate,
    /// The owner exists but its contents cannot be inspected without changing it (for example
    /// native-compiled state). Its estimate covers only what is visible.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub opaque: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<&'static str>,
}

/// Why a requested snapshot was not retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotSkipped {
    #[error("no diagnostics recorder is installed")]
    NotRecording,
    #[error("the snapshot retention cap is full of mandatory snapshots")]
    RetentionFull,
}

/// Why a snapshot was taken, which decides how retention treats it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotKind {
    /// Taken on the configured tick cadence; the only kind that may be skipped or replaced.
    Periodic,
    /// Taken when a run finishes.
    Final,
    /// Taken before a root reconfiguration replaces the definition.
    BeforeReconfiguration,
    /// Requested by the caller.
    Explicit,
}

impl SnapshotKind {
    pub const fn is_periodic(self) -> bool {
        matches!(self, Self::Periodic)
    }
}

/// A structured memory snapshot taken at an explicit point.
#[derive(Clone, Debug, Serialize)]
pub struct MemorySnapshot {
    pub label: String,
    pub kind: SnapshotKind,
    /// Definition version of the monitor when the snapshot was taken, where the owner has one.
    /// Stream names are only comparable between snapshots of the same version.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub definition_version: Option<u32>,
    /// Successful monitor ticks completed before the snapshot.
    pub tick: u64,
    pub components: Vec<MemoryComponent>,
    pub total: MemoryEstimate,
    /// Work units spent by the traversal; never more than `budget.max_work`.
    pub work: usize,
    pub budget: MemoryBudget,
    /// The work budget stopped traversal: later owners and values were not inspected.
    pub budget_exhausted: bool,
    /// The depth budget stopped traversal inside at least one value.
    pub depth_truncated: bool,
}

impl MemorySnapshot {
    pub fn new(label: impl Into<String>, tick: u64, budget: MemoryBudget) -> Self {
        Self {
            label: label.into(),
            kind: SnapshotKind::Explicit,
            definition_version: None,
            tick,
            components: Vec::new(),
            total: MemoryEstimate::default(),
            work: 0,
            budget,
            budget_exhausted: false,
            depth_truncated: false,
        }
    }

    pub fn push(&mut self, component: MemoryComponent) {
        self.total.merge(&component.estimate);
        self.components.push(component);
    }

    /// Estimated bytes of each named stream. Stream names are only meaningful within this
    /// snapshot's `definition_version`.
    pub fn bytes_by_stream(&self) -> HashMap<&str, StreamBytes> {
        let mut totals = HashMap::<&str, StreamBytes>::new();
        for component in &self.components {
            if let Some(stream) = &component.stream {
                let entry = totals.entry(stream.as_str()).or_default();
                entry.charged += component.estimate.total_bytes();
                entry.with_shared_roots += component.estimate.bytes_with_shared_roots();
                entry.map_identity_uncertain |=
                    component.estimate.payload.unresolved_map_identities > 0;
                entry.opaque |= component.opaque;
                entry.truncated |= component.estimate.payload.truncated;
            }
        }
        totals
    }
}

/// What one snapshot attributes to one stream.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct StreamBytes {
    /// Bytes charged to the stream. Adds up across streams without double counting, except for
    /// map roots counted in `map_identity_uncertain`.
    pub charged: u64,
    /// The charge plus the shells of shared payload roots charged to an earlier owner. Partial:
    /// children of a shared root are not re-walked. Overlaps between streams; never sum it.
    pub with_shared_roots: u64,
    /// Some map root of the stream was charged without being compared against every earlier
    /// same-sized root, so a cloned handle may have been charged again.
    pub map_identity_uncertain: bool,
    /// Some component of the stream could not be inspected.
    pub opaque: bool,
    /// Some component of the stream was cut short by a budget.
    pub truncated: bool,
}

/// Estimates payload reachable from values without cloning or mutating them.
///
/// One sizer spans a snapshot so that buffers reached from several owners are charged once. The
/// lifetime ties remembered map handles to the borrowed owners being inspected.
///
/// # Work bound
///
/// Every unit of traversal is charged against [`MemoryBudget::max_work`] before it is done: one
/// unit per owner entered ([`Self::enter_owner`]), one
/// unit per value visited (including each map key and value) and one per map-root identity
/// comparison. Once the budget is spent no further value, child or comparison is examined, so a
/// snapshot never does more than `max_work` units however large the retained state is. Lengths,
/// capacities and string sizes are read in constant time; no string is scanned.
pub struct ValueSizer<'a> {
    budget: MemoryBudget,
    work: usize,
    exhausted: bool,
    depth_truncated: bool,
    seen_buffers: HashSet<usize>,
    seen_maps: HashMap<usize, Vec<&'a ValueMap>>,
}

/// Bytes of an `EcoVec` heap header: reference count and capacity.
const ECO_HEADER: usize = 2 * size_of::<usize>();
/// Longest string stored inline in an [`EcoString`].
const INLINE_STRING: usize = size_of::<EcoString>() - 1;
/// Crude per-entry overhead of a persistent B+tree map beyond the key and value themselves:
/// node headers, child pointers and slack amortized over leaf entries.
const MAP_ENTRY_OVERHEAD: usize = 16;
/// Most recent same-sized map roots compared when deciding whether a map handle was already
/// charged. A handle older than this is charged again and counted in
/// `unresolved_map_identities`.
pub const MAP_IDENTITY_WINDOW: usize = 32;

impl<'a> ValueSizer<'a> {
    pub fn new(budget: MemoryBudget) -> Self {
        Self {
            budget,
            work: 0,
            exhausted: false,
            depth_truncated: false,
            seen_buffers: HashSet::new(),
            seen_maps: HashMap::new(),
        }
    }

    /// Work units spent; never more than the budget's `max_work`.
    pub fn work(&self) -> usize {
        self.work
    }

    /// The work budget stopped traversal.
    pub fn exhausted(&self) -> bool {
        self.exhausted
    }

    /// The depth budget stopped traversal somewhere.
    pub fn depth_truncated(&self) -> bool {
        self.depth_truncated
    }

    /// Size in bytes of one inline [`Value`] slot.
    pub const fn value_slot_bytes() -> usize {
        size_of::<Value>()
    }

    /// Estimate the payload of `value`.
    pub fn value(&mut self, value: &'a Value) -> PayloadEstimate {
        let mut estimate = PayloadEstimate::default();
        self.visit(value, 0, &mut estimate);
        estimate
    }

    /// Estimate the payload of every value in `values`, stopping at the work budget.
    pub fn values(&mut self, values: impl IntoIterator<Item = &'a Value>) -> PayloadEstimate {
        let mut estimate = PayloadEstimate::default();
        for value in values {
            if !self.visit(value, 0, &mut estimate) {
                break;
            }
        }
        estimate
    }

    /// Spend one unit of work to enter an owner that holds no value of its own (an evaluator, a
    /// node state, a match arm), so walking a large structure of empty or scalar owners is bounded
    /// like walking values. `false`, with the budget marked exhausted, once none is left; the
    /// caller stops its loop and marks what it skipped.
    pub fn enter_owner(&mut self) -> bool {
        self.charge(1, &mut PayloadEstimate::default())
    }

    /// Spend `units` of work, or mark the budget exhausted and refuse.
    #[inline]
    fn charge(&mut self, units: usize, estimate: &mut PayloadEstimate) -> bool {
        if self.exhausted || self.work + units > self.budget.max_work {
            self.exhausted = true;
            estimate.truncated = true;
            return false;
        }
        self.work += units;
        true
    }

    /// Visit one value; `false` once the work budget is spent.
    fn visit(&mut self, value: &'a Value, depth: usize, estimate: &mut PayloadEstimate) -> bool {
        if !self.charge(1, estimate) {
            return false;
        }
        match value {
            Value::Int(_)
            | Value::Float(_)
            | Value::Bool(_)
            | Value::Unit
            | Value::Deferred
            | Value::NoVal => {}
            Value::Str(text) => self.string(text, estimate),
            Value::Function(_) => estimate.opaque_values += 1,
            Value::List(items) | Value::Tuple(items) => self.vector(items, depth, estimate),
            Value::Union(union) => {
                let bytes = size_of::<crate::core::UnionValue>() + ECO_HEADER;
                if self.first_sight(std::rc::Rc::as_ptr(union) as usize) {
                    estimate.unique_bytes += bytes as u64;
                    self.string(union.tag(), estimate);
                    if let Some(payload) = union.payload()
                        && self.may_descend(depth, estimate)
                    {
                        self.visit(payload, depth + 1, estimate);
                    }
                } else {
                    estimate.shared_bytes += bytes as u64;
                }
            }
            Value::Newtype(newtype) => {
                let bytes = size_of::<crate::core::NewtypeValue>() + ECO_HEADER;
                if self.first_sight(std::rc::Rc::as_ptr(newtype) as usize) {
                    estimate.unique_bytes += bytes as u64;
                    self.string(newtype.brand(), estimate);
                    if self.may_descend(depth, estimate) {
                        self.visit(newtype.value(), depth + 1, estimate);
                    }
                } else {
                    estimate.shared_bytes += bytes as u64;
                }
            }
            Value::Map(map) => self.map(map, depth, estimate),
        }
        !self.exhausted
    }

    /// Whether children one level below `depth` may be visited; marks depth truncation if not.
    #[inline]
    fn may_descend(&mut self, depth: usize, estimate: &mut PayloadEstimate) -> bool {
        if depth + 1 > self.budget.max_depth {
            estimate.truncated = true;
            self.depth_truncated = true;
            return false;
        }
        true
    }

    fn first_sight(&mut self, address: usize) -> bool {
        self.seen_buffers.insert(address)
    }

    /// Charge a heap string once per buffer. Constant time: the length is stored, and the text is
    /// never scanned.
    fn string(&mut self, text: &EcoString, estimate: &mut PayloadEstimate) {
        if text.len() <= INLINE_STRING {
            return;
        }
        let bytes = (ECO_HEADER + text.len()) as u64;
        if self.first_sight(text.as_ptr() as usize) {
            estimate.unique_bytes += bytes;
        } else {
            estimate.shared_bytes += bytes;
        }
    }

    fn vector(&mut self, items: &'a EcoVec<Value>, depth: usize, estimate: &mut PayloadEstimate) {
        let len = items.len() as u64;
        estimate.max_list_len = estimate.max_list_len.max(len);
        if items.capacity() == 0 {
            return;
        }
        let bytes = (ECO_HEADER + items.capacity() * size_of::<Value>()) as u64;
        if !self.first_sight(items.as_slice().as_ptr() as usize) {
            estimate.shared_bytes += bytes;
            return;
        }
        estimate.unique_bytes += bytes;
        estimate.list_entries += len;
        if !self.may_descend(depth, estimate) {
            return;
        }
        for item in items.iter() {
            if !self.visit(item, depth + 1, estimate) {
                return;
            }
        }
    }

    fn map(&mut self, map: &'a ValueMap, depth: usize, estimate: &mut PayloadEstimate) {
        let len = map.len();
        estimate.max_map_len = estimate.max_map_len.max(len as u64);
        if len == 0 {
            return;
        }
        let bytes =
            (len * (size_of::<EcoString>() + size_of::<Value>() + MAP_ENTRY_OVERHEAD)) as u64;
        // Compare with at most the most recent `MAP_IDENTITY_WINDOW` roots of the same size, one
        // work unit per comparison.
        let candidates = self
            .seen_maps
            .get(&len)
            .map_or(0, |seen| seen.len().min(MAP_IDENTITY_WINDOW));
        let mut seen_before = false;
        for offset in 0..candidates {
            if !self.charge(1, estimate) {
                return;
            }
            let seen = &self.seen_maps[&len];
            if seen[seen.len() - 1 - offset].ptr_eq(map) {
                seen_before = true;
                break;
            }
        }
        if seen_before {
            estimate.shared_bytes += bytes;
            return;
        }
        let same_size = self.seen_maps.entry(len).or_default();
        if same_size.len() > MAP_IDENTITY_WINDOW {
            estimate.unresolved_map_identities += 1;
        }
        same_size.push(map);
        estimate.map_logical_bytes += bytes;
        estimate.map_entries += len as u64;
        if !self.may_descend(depth, estimate) {
            return;
        }
        for (key, value) in map.iter() {
            // A key costs one unit, like a value.
            if !self.charge(1, estimate) {
                return;
            }
            self.string(key, estimate);
            if !self.visit(value, depth + 1, estimate) {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{UnionValue, ValueMap};

    fn sizer() -> ValueSizer<'static> {
        ValueSizer::new(MemoryBudget::default())
    }

    #[test]
    fn scalars_have_no_payload() {
        let values = [
            Value::Int(1),
            Value::Bool(true),
            Value::NoVal,
            Value::Deferred,
        ];
        let mut sizer = ValueSizer::new(MemoryBudget::default());
        assert_eq!(sizer.values(values.iter()), PayloadEstimate::default());
    }

    #[test]
    fn a_shared_list_is_charged_once() {
        let list = Value::List((0..10).map(Value::Int).collect());
        let copy = list.clone();
        let mut sizer = sizer();
        let first = sizer.value(Box::leak(Box::new(list)));
        let second = sizer.value(Box::leak(Box::new(copy)));
        assert!(first.unique_bytes >= (10 * size_of::<Value>()) as u64);
        assert_eq!(first.list_entries, 10);
        assert_eq!(first.max_list_len, 10);
        assert_eq!(second.unique_bytes, 0);
        assert_eq!(second.shared_bytes, first.unique_bytes);
    }

    #[test]
    fn map_handles_are_logical_and_deduplicated_per_root() {
        let map: ValueMap = (0..100)
            .map(|index| (EcoString::from(format!("k{index}")), Value::Int(index)))
            .collect::<std::collections::BTreeMap<_, _>>()
            .into();
        let first = Value::Map(map.clone());
        let second = Value::Map(map);
        let mut updated = first.clone();
        if let Value::Map(map) = &mut updated {
            map.insert("extra".into(), Value::Int(-1));
        }
        let (first, second, updated) = (
            Box::leak(Box::new(first)),
            Box::leak(Box::new(second)),
            Box::leak(Box::new(updated)),
        );
        let mut sizer = sizer();
        let a = sizer.value(first);
        let b = sizer.value(second);
        let c = sizer.value(updated);
        assert_eq!(a.map_entries, 100);
        assert_eq!(a.unique_bytes, 0, "short keys are inline");
        assert!(a.map_logical_bytes > 0);
        assert_eq!(
            b.map_logical_bytes, 0,
            "a cloned handle is not charged again"
        );
        assert_eq!(b.shared_bytes, a.map_logical_bytes);
        // The updated root shares most nodes, but that sharing is not observable: it is charged
        // logically in full.
        assert_eq!(c.map_entries, 101);
        assert_eq!(c.max_map_len, 101);
    }

    #[test]
    fn work_budget_truncates() {
        let list = Box::leak(Box::new(Value::List((0..100).map(Value::Int).collect())));
        let mut sizer = ValueSizer::new(MemoryBudget {
            max_depth: 32,
            max_work: 10,
        });
        let estimate = sizer.value(list);
        assert!(estimate.truncated);
        assert!(sizer.exhausted());
        assert_eq!(sizer.work(), 10);
    }

    #[test]
    fn a_huge_list_stops_at_the_work_bound() {
        let huge = Box::leak(Box::new(Value::List(
            (0..200_000)
                .map(|index| Value::List((0..4).map(|_| Value::Int(index)).collect()))
                .collect(),
        )));
        let started = std::time::Instant::now();
        let mut sizer = ValueSizer::new(MemoryBudget {
            max_depth: 32,
            max_work: 50,
        });
        let estimate = sizer.value(huge);
        assert_eq!(sizer.work(), 50);
        assert!(sizer.exhausted() && estimate.truncated);
        // The container is charged from its capacity; only the visited prefix is entered: the
        // outer list, then nine whole inner lists and the tenth with its four items.
        assert_eq!(estimate.list_entries, 200_000 + 4 * 10);
        // At a zero depth limit the children are not iterated at all.
        let mut shallow = ValueSizer::new(MemoryBudget {
            max_depth: 0,
            max_work: 1_000,
        });
        let estimate = shallow.value(huge);
        assert_eq!(shallow.work(), 1);
        assert!(estimate.truncated && shallow.depth_truncated() && !shallow.exhausted());
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
    }

    #[test]
    fn many_same_sized_maps_bound_identity_comparisons() {
        let maps: &'static [Value] = Box::leak(
            (0..2_000)
                .map(|index| {
                    Value::Map(ValueMap::from_iter([(
                        EcoString::from("k"),
                        Value::Int(index),
                    )]))
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        );
        let mut tiny = ValueSizer::new(MemoryBudget {
            max_depth: 32,
            max_work: 100,
        });
        let estimate = tiny.values(maps.iter());
        assert!(tiny.work() <= 100 && tiny.exhausted() && estimate.truncated);

        let mut ample = ValueSizer::new(MemoryBudget::default());
        let estimate = ample.values(maps.iter());
        // Each map costs one visit, at most `MAP_IDENTITY_WINDOW` comparisons, a key and a value.
        assert!(ample.work() <= maps.len() * (3 + MAP_IDENTITY_WINDOW));
        assert_eq!(estimate.map_entries, maps.len() as u64);
        assert!(estimate.unresolved_map_identities > 0);
        assert!(!estimate.truncated);
    }

    #[test]
    fn long_keys_cost_constant_work() {
        let key = "x".repeat(1 << 16);
        let map = Box::leak(Box::new(Value::Map(ValueMap::from_iter((0..64).map(
            |index| (EcoString::from(format!("{key}{index}")), Value::Int(index)),
        )))));
        let mut sizer = ValueSizer::new(MemoryBudget {
            max_depth: 32,
            max_work: 5,
        });
        let started = std::time::Instant::now();
        let estimate = sizer.value(map);
        assert!(sizer.work() <= 5 && estimate.truncated);
        // Five units: the map, then two (key, value) pairs. Each visited key is charged by its
        // length without being scanned; the keys beyond the bound are not entered.
        assert!(
            (2 << 16..3 << 16).contains(&(estimate.unique_bytes as usize)),
            "a visited key is charged by length: {}",
            estimate.unique_bytes
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
    }

    #[test]
    fn depth_budget_truncates_and_unions_are_followed() {
        let mut value = Value::Int(0);
        for _ in 0..5 {
            value = UnionValue::new("Some", Some(value)).into();
        }
        let value = Box::leak(Box::new(value));
        let estimate = ValueSizer::new(MemoryBudget {
            max_depth: 2,
            max_work: 100,
        })
        .value(value);
        assert!(estimate.truncated);
        let estimate = sizer().value(value);
        assert!(!estimate.truncated);
        assert!(estimate.unique_bytes > 0);
    }

    #[test]
    fn long_strings_are_charged_and_short_ones_are_inline() {
        let short = Box::leak(Box::new(Value::Str("short".into())));
        let long = Box::leak(Box::new(Value::Str(
            "a string well beyond the inline limit".into(),
        )));
        let mut sizer = sizer();
        assert_eq!(sizer.value(short).unique_bytes, 0);
        assert!(sizer.value(long).unique_bytes > 30);
    }

    #[test]
    fn a_huge_map_stops_at_the_work_bound() {
        let started = std::time::Instant::now();
        let huge = Box::leak(Box::new(Value::Map(ValueMap::from_iter(
            (0..100_000).map(|index| (EcoString::from(format!("k{index}")), Value::Int(index))),
        ))));
        let mut sizer = ValueSizer::new(MemoryBudget {
            max_depth: 32,
            max_work: 25,
        });
        let estimate = sizer.value(huge);
        // The map itself, then a key and a value per entry until the bound.
        assert_eq!(sizer.work(), 25);
        assert!(sizer.exhausted() && estimate.truncated);
        assert_eq!(
            estimate.map_entries, 100_000,
            "the size is read, not scanned"
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        // Once spent, further owners are not entered at all.
        let after = sizer.value(Box::leak(Box::new(Value::Int(1))));
        assert!(after.truncated);
        assert_eq!(sizer.work(), 25);
    }

    #[test]
    fn map_identity_comparisons_are_charged_to_the_work_bound() {
        // Distinct maps of one size are compared against the earlier ones: each comparison is a
        // unit of work, so a tiny budget stops inside the comparisons.
        let maps: &'static [Value] = Box::leak(
            (0..64)
                .map(|index| {
                    Value::Map(ValueMap::from_iter([(
                        EcoString::from("k"),
                        Value::Int(index),
                    )]))
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        );
        for max_work in [1, 7, 40, 200] {
            let mut sizer = ValueSizer::new(MemoryBudget {
                max_depth: 32,
                max_work,
            });
            sizer.values(maps.iter());
            assert!(sizer.work() <= max_work, "{} > {max_work}", sizer.work());
        }
    }

    #[test]
    fn bytes_with_shared_roots_add_the_shells_charged_elsewhere() {
        let list = Value::List((0..10).map(Value::Int).collect());
        let (first, second) = (Box::leak(Box::new(list.clone())), Box::leak(Box::new(list)));
        let mut sizer = sizer();
        let owner = MemoryEstimate {
            fixed_bytes: 8,
            payload: sizer.value(first),
        };
        let later = MemoryEstimate {
            fixed_bytes: 8,
            payload: sizer.value(second),
        };
        assert_eq!(
            later.total_bytes(),
            8,
            "charged nothing for the shared list"
        );
        assert_eq!(later.bytes_with_shared_roots(), owner.total_bytes());
        let json = serde_json::to_value(later).unwrap();
        assert_eq!(json["total_bytes"], 8);
        assert_eq!(json["bytes_with_shared_roots"], owner.total_bytes());
        assert_eq!(json["shared_bytes"], owner.payload.unique_bytes);
    }

    #[test]
    fn stream_bytes_carry_the_partial_and_uncertainty_flags() {
        let mut snapshot = MemorySnapshot::new("t", 0, MemoryBudget::default());
        let component = |stream: &str, unresolved: u64, shared: u64| MemoryComponent {
            path: format!("streams/{stream}"),
            category: "state",
            stream: Some(stream.to_owned()),
            estimate: MemoryEstimate {
                fixed_bytes: 10,
                payload: PayloadEstimate {
                    shared_bytes: shared,
                    unresolved_map_identities: unresolved,
                    ..PayloadEstimate::default()
                },
            },
            opaque: false,
            note: None,
        };
        snapshot.push(component("a", 0, 0));
        snapshot.push(component("b", 2, 7));
        let bytes = snapshot.bytes_by_stream();
        assert!(!bytes["a"].map_identity_uncertain);
        assert!(bytes["b"].map_identity_uncertain);
        assert_eq!((bytes["b"].charged, bytes["b"].with_shared_roots), (10, 17));
    }
}
