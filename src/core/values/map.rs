//! The shared, ordered payload of [`Value::Map`].
//!
//! A [`ValueMap`] is a persistent ordered map: a B+tree whose nodes are reference counted and
//! shared between copies, the map counterpart of the [`EcoVec`](ecow::EcoVec) behind lists and
//! tuples. Cloning a map, and so cloning any [`Value`] that contains one, copies a handle in O(1)
//! instead of rebuilding the tree. Mutating through a handle whose nodes are shared copies the
//! nodes on the path to the changed entry — for a struct-sized map, its one leaf of entries — and
//! leaves the rest shared. Copying an entry clones its key and value, which shares rather than
//! copies a nested map, list or string. Other handles never observe the change, so a map keeps
//! value semantics wherever it is retained — history, operand lifting, fanout and branch snapshots.
//!
//! Iteration is in sorted key order, which equality, display and serialization depend on.
//! Equality compares entries, never node identity: a map holding NaN is not equal to its own copy.
//!
//! The backing tree is not part of the API. A map converts to and from a [`BTreeMap`] with
//! [`From`] and offers the read and update operations of one; its iterators are its own types.

use std::{borrow::Borrow, collections::BTreeMap, fmt, iter::FusedIterator, ops::Index};

use ecow::EcoString;
use imbl::{GenericOrdMap, ordmap, shared_ptr::RcK};

use super::Value;

type Tree = GenericOrdMap<EcoString, Value, RcK>;

/// An ordered map from field names to values, shared until it is mutated.
#[derive(Clone, Default)]
pub struct ValueMap(Tree);

impl ValueMap {
    /// An empty map.
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of entries.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the map has no entries.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether both handles share the same root storage. This is storage identity, not equality:
    /// equal maps built separately are not `ptr_eq`, and it never inspects entries.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        self.0.ptr_eq(&other.0)
    }

    /// The value for a key.
    pub fn get<Q>(&self, key: &Q) -> Option<&Value>
    where
        EcoString: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.0.get(key)
    }

    /// Whether the map has an entry for a key.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        EcoString: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.0.contains_key(key)
    }

    /// Insert an entry, returning the value it replaced.
    pub fn insert(&mut self, key: EcoString, value: Value) -> Option<Value> {
        self.0.insert(key, value)
    }

    /// Remove an entry, returning its value. Removing an absent key copies nothing.
    pub fn remove<Q>(&mut self, key: &Q) -> Option<Value>
    where
        EcoString: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        if !self.0.contains_key(key) {
            return None;
        }
        self.0.remove(key)
    }

    /// A mutable reference to an entry's value. Looking up an absent key copies nothing.
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut Value>
    where
        EcoString: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        if !self.0.contains_key(key) {
            return None;
        }
        self.0.get_mut(key)
    }

    /// The entries in key order.
    pub fn iter(&self) -> Iter<'_> {
        Iter(self.0.iter())
    }

    /// The keys in order.
    pub fn keys(&self) -> Keys<'_> {
        Keys(self.0.keys())
    }

    /// The values in key order.
    pub fn values(&self) -> Values<'_> {
        Values(self.0.values())
    }

    /// The entries as an owned [`BTreeMap`].
    pub fn into_btree_map(self) -> BTreeMap<EcoString, Value> {
        self.into_iter().collect()
    }
}

/// The value for a key, as with a [`BTreeMap`].
///
/// # Panics
///
/// If the map has no entry for the key.
impl<Q> Index<&Q> for ValueMap
where
    EcoString: Borrow<Q>,
    Q: Ord + ?Sized,
{
    type Output = Value;

    fn index(&self, key: &Q) -> &Value {
        self.get(key).expect("no entry found for key")
    }
}

impl fmt::Debug for ValueMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

// Entry by entry, never by node identity, which the backing tree's own equality would shortcut.
impl PartialEq for ValueMap {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}

impl PartialEq<BTreeMap<EcoString, Value>> for ValueMap {
    fn eq(&self, other: &BTreeMap<EcoString, Value>) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}

impl From<BTreeMap<EcoString, Value>> for ValueMap {
    fn from(map: BTreeMap<EcoString, Value>) -> Self {
        map.into_iter().collect()
    }
}

impl From<ValueMap> for BTreeMap<EcoString, Value> {
    fn from(map: ValueMap) -> Self {
        map.into_btree_map()
    }
}

impl<const N: usize> From<[(EcoString, Value); N]> for ValueMap {
    fn from(entries: [(EcoString, Value); N]) -> Self {
        entries.into_iter().collect()
    }
}

impl FromIterator<(EcoString, Value)> for ValueMap {
    fn from_iter<I: IntoIterator<Item = (EcoString, Value)>>(entries: I) -> Self {
        Self(entries.into_iter().collect())
    }
}

impl Extend<(EcoString, Value)> for ValueMap {
    fn extend<I: IntoIterator<Item = (EcoString, Value)>>(&mut self, entries: I) {
        self.0.extend(entries);
    }
}

/// Owned iteration in key order. Entries in nodes this handle alone holds are moved; entries in
/// shared nodes are cloned, which shares rather than copies any nested map.
impl IntoIterator for ValueMap {
    type Item = (EcoString, Value);
    type IntoIter = IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter(self.0.into_iter())
    }
}

impl<'a> IntoIterator for &'a ValueMap {
    type Item = (&'a EcoString, &'a Value);
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

macro_rules! map_iterator {
    ($(#[$doc:meta])* $name:ident$(<$a:lifetime>)?, $inner:ty, $item:ty) => {
        $(#[$doc])*
        pub struct $name$(<$a>)?($inner);

        impl$(<$a>)? Iterator for $name$(<$a>)? {
            type Item = $item;

            fn next(&mut self) -> Option<Self::Item> {
                self.0.next()
            }

            fn size_hint(&self) -> (usize, Option<usize>) {
                self.0.size_hint()
            }
        }

        impl$(<$a>)? DoubleEndedIterator for $name$(<$a>)? {
            fn next_back(&mut self) -> Option<Self::Item> {
                self.0.next_back()
            }
        }

        impl$(<$a>)? ExactSizeIterator for $name$(<$a>)? {}
        impl$(<$a>)? FusedIterator for $name$(<$a>)? {}
    };
}

map_iterator!(
    /// The entries of a [`ValueMap`] in key order.
    Iter<'a>,
    ordmap::Iter<'a, EcoString, Value, RcK>,
    (&'a EcoString, &'a Value)
);
map_iterator!(
    /// The keys of a [`ValueMap`] in order.
    Keys<'a>,
    ordmap::Keys<'a, EcoString, Value, RcK>,
    &'a EcoString
);
map_iterator!(
    /// The values of a [`ValueMap`] in key order.
    Values<'a>,
    ordmap::Values<'a, EcoString, Value, RcK>,
    &'a Value
);
map_iterator!(
    /// The owned entries of a [`ValueMap`] in key order.
    IntoIter,
    ordmap::ConsumingIter<EcoString, Value, RcK>,
    (EcoString, Value)
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::JsonStreamValue;

    fn map(entries: &[(&str, Value)]) -> ValueMap {
        entries
            .iter()
            .map(|(key, value)| (EcoString::from(*key), value.clone()))
            .collect()
    }

    #[test]
    fn updating_a_copy_leaves_the_original_and_its_nested_maps_unchanged() {
        let inner = Value::Map(map(&[("x", Value::Int(1))]));
        let original = map(&[("inner", inner.clone()), ("n", Value::Int(0))]);
        let mut copy = original.clone();

        let Some(Value::Map(nested)) = copy.get_mut("inner") else {
            panic!("inner is a map");
        };
        nested.insert("x".into(), Value::Int(2));
        copy.insert("n".into(), Value::Int(1));
        copy.remove("missing");

        assert_eq!(
            original,
            map(&[("inner", inner.clone()), ("n", Value::Int(0))])
        );
        assert_eq!(inner, Value::Map(map(&[("x", Value::Int(1))])));
        assert_eq!(
            copy,
            map(&[
                ("inner", Value::Map(map(&[("x", Value::Int(2))]))),
                ("n", Value::Int(1)),
            ])
        );
    }

    // Large enough to span several tree nodes, so a copy shares most of them.
    fn numbered(len: i64) -> ValueMap {
        (0..len)
            .map(|index| (EcoString::from(format!("k{index:03}")), Value::Int(index)))
            .collect()
    }

    #[test]
    fn updating_a_copy_of_a_large_map_leaves_the_original_unchanged() {
        let original = numbered(200);
        let mut copy = original.clone();

        copy.insert("k050".into(), Value::Int(-1));
        copy.insert("k999".into(), Value::Int(999));
        copy.remove("k000");
        copy.remove("k199");

        assert_eq!(original, numbered(200));
        assert_eq!(copy.len(), 199);
        assert_eq!(copy.get("k050"), Some(&Value::Int(-1)));
        assert_eq!(copy.get("k000"), None);
        assert_eq!(original.get("k000"), Some(&Value::Int(0)));
        let keys = copy.keys().cloned().collect::<Vec<_>>();
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(keys.first().map(EcoString::as_str), Some("k001"));
        assert_eq!(keys.last().map(EcoString::as_str), Some("k999"));
    }

    #[test]
    fn a_large_map_holding_nan_is_not_equal_to_a_copy_sharing_its_nodes() {
        let mut original = numbered(200);
        original.insert("k100".into(), Value::Float(f64::NAN));
        let copy = original.clone();
        let mut updated = original.clone();
        updated.insert("k000".into(), Value::Int(0));

        assert_ne!(original, copy);
        assert_ne!(original, updated);
    }

    #[test]
    fn owned_iteration_yields_entries_in_key_order_whether_shared_or_not() {
        let unique = map(&[("b", Value::Int(2)), ("a", Value::Int(1))]);
        let shared = unique.clone();
        let expected = vec![
            (EcoString::from("a"), Value::Int(1)),
            (EcoString::from("b"), Value::Int(2)),
        ];

        assert_eq!(shared.clone().into_iter().collect::<Vec<_>>(), expected);
        assert_eq!(unique.into_iter().collect::<Vec<_>>(), expected);
        assert_eq!(
            shared.into_btree_map(),
            BTreeMap::from_iter(expected.clone())
        );
    }

    #[test]
    fn converts_to_and_from_a_btree_map() {
        let entries = BTreeMap::from([
            (EcoString::from("a"), Value::Int(1)),
            (EcoString::from("b"), Value::Bool(true)),
        ]);
        let map = ValueMap::from(entries.clone());

        assert_eq!(map, entries);
        assert_eq!(map.clone().into_btree_map(), entries);
        assert_eq!(Value::from(entries.clone()), Value::Map(map.clone()));
        assert_eq!(BTreeMap::from(map.clone()), entries);
        let mut extended = ValueMap::new();
        extended.extend(entries.clone());
        assert_eq!(extended, map);
    }

    #[test]
    fn equality_is_by_entries_and_ignores_insertion_order() {
        let mut forwards = ValueMap::new();
        forwards.insert("a".into(), Value::Int(1));
        forwards.insert("b".into(), Value::Int(2));
        let mut backwards = ValueMap::new();
        backwards.insert("b".into(), Value::Int(2));
        backwards.insert("a".into(), Value::Int(1));

        assert_eq!(Value::Map(forwards.clone()), Value::Map(backwards));
        assert_ne!(
            Value::Map(forwards),
            Value::Map(map(&[("a", Value::Int(1)), ("b", Value::Int(3))]))
        );
    }

    // As with a union payload, a shared handle is not a shortcut to equality.
    #[test]
    fn a_map_holding_nan_is_not_equal_to_its_copy() {
        let value = Value::Map(map(&[("x", Value::Float(f64::NAN))]));
        assert_ne!(value, value.clone());
    }

    #[test]
    fn serializes_and_reports_entries_in_key_order() {
        let mut entries = ValueMap::new();
        entries.insert("z".into(), Value::Int(1));
        entries.insert(
            "a".into(),
            Value::Map(map(&[("y", Value::Int(2)), ("b", Value::Int(3))])),
        );
        let value = Value::Map(entries);

        assert_eq!(value.encode_json().unwrap(), r#"{"a":{"b":3,"y":2},"z":1}"#);
        assert_eq!(
            value.dsrv_source(),
            r#"Map("a": Map("b": 3, "y": 2), "z": 1)"#
        );
        assert_eq!(
            format!("{value:?}"),
            r#"Map({"a": Map({"b": Int(3), "y": Int(2)}), "z": Int(1)})"#
        );
        assert_eq!(
            Value::decode_json(br#"{"z":1,"a":{"y":2,"b":3}}"#).unwrap(),
            value
        );
    }
}
