//! Stored child collections resolved into borrowed tree cursors.

use std::borrow::Borrow;

use crate::TreeCursor;

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
        self.fields
            .iter()
            .enumerate()
            .find_map(|(index, (key, _))| {
                self.fields[..index]
                    .iter()
                    .any(|(previous, _)| previous == key)
                    .then_some(key)
            })
    }
}
