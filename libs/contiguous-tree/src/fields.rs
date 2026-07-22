//! Stored child collections resolved into borrowed tree cursors.

use std::{borrow::Borrow, fmt, marker::PhantomData};

use crate::TreeCursor;

/// Keyed values in source order, backed by a configurable collection.
#[derive(Clone, Debug, PartialEq)]
pub struct KeyedFields<Key, Value, Storage> {
    storage: Storage,
    marker: PhantomData<fn() -> (Key, Value)>,
}

impl<Key, Value, Storage> Default for KeyedFields<Key, Value, Storage>
where
    Storage: Default,
{
    fn default() -> Self {
        Self {
            storage: Storage::default(),
            marker: PhantomData,
        }
    }
}

impl<Key, Value, Storage> KeyedFields<Key, Value, Storage>
where
    Storage: AsRef<[(Key, Value)]>,
{
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (&Key, &Value)> + ExactSizeIterator {
        self.as_slice().iter().map(|(key, value)| (key, value))
    }

    pub fn keys(&self) -> impl DoubleEndedIterator<Item = &Key> + ExactSizeIterator {
        self.as_slice().iter().map(|(key, _)| key)
    }

    pub fn values(&self) -> impl DoubleEndedIterator<Item = &Value> + ExactSizeIterator {
        self.as_slice().iter().map(|(_, value)| value)
    }

    pub fn get<Q>(&self, key: &Q) -> Option<Value>
    where
        Key: Borrow<Q>,
        Value: Copy,
        Q: Eq + ?Sized,
    {
        self.as_slice()
            .iter()
            .rev()
            .find(|(found, _)| found.borrow() == key)
            .map(|(_, value)| *value)
    }

    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        Key: Borrow<Q>,
        Value: Copy,
        Q: Eq + ?Sized,
    {
        self.get(key).is_some()
    }

    pub fn duplicate_key(&self) -> Option<&Key>
    where
        Key: Eq,
    {
        duplicate_key(self.as_slice())
    }

    pub fn as_slice(&self) -> &[(Key, Value)] {
        self.storage.as_ref()
    }

    #[doc(hidden)]
    pub fn make_mut_with<'fields>(
        &'fields mut self,
        make_mut: impl FnOnce(&'fields mut Storage) -> &'fields mut [(Key, Value)],
    ) -> &'fields mut [(Key, Value)] {
        make_mut(&mut self.storage)
    }
}

impl<Key, Value, Storage> FromIterator<(Key, Value)> for KeyedFields<Key, Value, Storage>
where
    Storage: FromIterator<(Key, Value)>,
{
    fn from_iter<Items: IntoIterator<Item = (Key, Value)>>(items: Items) -> Self {
        Self {
            storage: items.into_iter().collect(),
            marker: PhantomData,
        }
    }
}

impl<Key, Value, Storage> From<Storage> for KeyedFields<Key, Value, Storage> {
    fn from(storage: Storage) -> Self {
        Self {
            storage,
            marker: PhantomData,
        }
    }
}

impl<Key, Value, Storage> IntoIterator for KeyedFields<Key, Value, Storage>
where
    Storage: IntoIterator<Item = (Key, Value)>,
{
    type Item = (Key, Value);
    type IntoIter = Storage::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.storage.into_iter()
    }
}

impl<'fields, Key, Value, Storage> IntoIterator for &'fields KeyedFields<Key, Value, Storage>
where
    Storage: AsRef<[(Key, Value)]>,
{
    type Item = (&'fields Key, &'fields Value);
    type IntoIter = std::iter::Map<
        std::slice::Iter<'fields, (Key, Value)>,
        fn(&(Key, Value)) -> (&Key, &Value),
    >;

    fn into_iter(self) -> Self::IntoIter {
        fn split<Key, Value>((key, value): &(Key, Value)) -> (&Key, &Value) {
            (key, value)
        }
        self.as_slice().iter().map(split)
    }
}

/// Return the first key that repeats an earlier key in source order.
pub fn duplicate_key<Key: Eq, Value>(fields: &[(Key, Value)]) -> Option<&Key> {
    fields.iter().enumerate().find_map(|(index, (key, _))| {
        fields[..index]
            .iter()
            .any(|(previous, _)| previous == key)
            .then_some(key)
    })
}

/// IDs from a stored collection resolved through their parent cursor.
#[derive(Clone)]
pub struct ResolvedIds<Cursor, IDs> {
    parent: Cursor,
    ids: IDs,
}

impl<Cursor, IDs> ResolvedIds<Cursor, IDs> {
    pub fn new(parent: Cursor, ids: IDs) -> Self {
        Self { parent, ids }
    }
}

impl<Cursor, IDs> fmt::Debug for ResolvedIds<Cursor, IDs>
where
    Cursor: TreeCursor + fmt::Debug,
    IDs: Iterator<Item = Cursor::Id> + Clone,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_list().entries(self.clone()).finish()
    }
}

impl<Cursor, IDs> Iterator for ResolvedIds<Cursor, IDs>
where
    Cursor: TreeCursor,
    IDs: Iterator<Item = Cursor::Id>,
{
    type Item = Cursor;

    fn next(&mut self) -> Option<Self::Item> {
        self.ids.next().map(|id| self.parent.child(id))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.ids.size_hint()
    }
}

impl<Cursor, IDs> DoubleEndedIterator for ResolvedIds<Cursor, IDs>
where
    Cursor: TreeCursor,
    IDs: DoubleEndedIterator<Item = Cursor::Id>,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.ids.next_back().map(|id| self.parent.child(id))
    }
}

impl<Cursor, IDs> ExactSizeIterator for ResolvedIds<Cursor, IDs>
where
    Cursor: TreeCursor,
    IDs: ExactSizeIterator<Item = Cursor::Id>,
{
}

/// Keyed child IDs resolved through their parent cursor.
#[derive(Clone, Copy)]
pub struct ResolvedFields<'fields, Cursor: TreeCursor, Key> {
    parent: Cursor,
    fields: &'fields [(Key, Cursor::Id)],
}

impl<Cursor, Key> fmt::Debug for ResolvedFields<'_, Cursor, Key>
where
    Cursor: TreeCursor + fmt::Debug,
    Key: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_map().entries(self.iter()).finish()
    }
}

impl<'fields, Cursor: TreeCursor, Key> ResolvedFields<'fields, Cursor, Key> {
    pub fn new(parent: Cursor, fields: &'fields [(Key, Cursor::Id)]) -> Self {
        Self { parent, fields }
    }

    pub fn iter(
        &self,
    ) -> impl DoubleEndedIterator<Item = (&'fields Key, Cursor)> + ExactSizeIterator {
        let parent = self.parent;
        self.fields
            .iter()
            .map(move |(key, id)| (key, parent.child(*id)))
    }

    pub fn get<Q>(&self, key: &Q) -> Option<Cursor>
    where
        Key: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        self.fields
            .iter()
            .rev()
            .find(|(found, _)| found.borrow() == key)
            .map(|(_, id)| self.parent.child(*id))
    }

    pub fn keys(&self) -> impl DoubleEndedIterator<Item = &'fields Key> + ExactSizeIterator {
        self.fields.iter().map(|(key, _)| key)
    }

    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        Key: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        self.get(key).is_some()
    }

    pub fn duplicate_key(&self) -> Option<&'fields Key>
    where
        Key: Eq,
    {
        duplicate_key(self.fields)
    }
}
