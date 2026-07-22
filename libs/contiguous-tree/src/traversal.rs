//! Allocation-free traversal, structural comparison, and bottom-up folding.

use std::convert::Infallible;

use crate::{ArenaId, IdRange, TreeCursor, TreeNodeMut};

/// Convenience traversal operations shared by every tree cursor.
pub trait TreeCursorExt: TreeCursor {
    fn children(self) -> Children<Self> {
        Children::new(self)
    }

    fn postorder(self) -> Postorder<Self> {
        Postorder::new(self)
    }

    fn subtree_len(self) -> usize {
        self.subtree_ids().len()
    }

    fn fold<T>(self, operation: impl FnMut(FoldNode<'_, Self, T>) -> T) -> T {
        fold(self, operation)
    }

    fn try_fold<T, E>(
        self,
        operation: impl FnMut(FoldNode<'_, Self, T>) -> Result<T, E>,
    ) -> Result<T, E> {
        try_fold(self, operation)
    }

    fn try_zip_with<E, Other>(
        self,
        other: Other,
        zip: impl FnMut(Self, Other) -> Result<bool, E>,
    ) -> Result<bool, E>
    where
        Other: TreeCursor,
    {
        try_zip_with(self, other, zip)
    }
}

impl<Cursor: TreeCursor> TreeCursorExt for Cursor {}

pub struct Children<Cursor: TreeCursor> {
    parent: Cursor,
    ids: Cursor::ChildIds,
}

impl<Cursor: TreeCursor> Children<Cursor> {
    pub fn new(parent: Cursor) -> Self {
        Self {
            parent,
            ids: parent.child_ids(),
        }
    }
}

impl<Cursor: TreeCursor> Iterator for Children<Cursor> {
    type Item = Cursor;

    fn next(&mut self) -> Option<Self::Item> {
        self.ids.next().map(|id| self.parent.child(id))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.ids.size_hint()
    }
}

impl<Cursor: TreeCursor> DoubleEndedIterator for Children<Cursor> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.ids.next_back().map(|id| self.parent.child(id))
    }
}

impl<Cursor: TreeCursor> ExactSizeIterator for Children<Cursor> {}

pub struct Postorder<Cursor: TreeCursor> {
    root: Cursor,
    ids: IdRange<Cursor::Id>,
}

impl<Cursor: TreeCursor> Postorder<Cursor> {
    pub fn new(root: Cursor) -> Self {
        Self {
            root,
            ids: root.subtree_ids(),
        }
    }
}

impl<Cursor: TreeCursor> Iterator for Postorder<Cursor> {
    type Item = Cursor;

    fn next(&mut self) -> Option<Self::Item> {
        self.ids.next().map(|id| self.root.child(id))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.ids.size_hint()
    }
}

impl<Cursor: TreeCursor> ExactSizeIterator for Postorder<Cursor> {}

pub fn try_zip_with<Left, Right, E>(
    left: Left,
    right: Right,
    mut zip: impl FnMut(Left, Right) -> Result<bool, E>,
) -> Result<bool, E>
where
    Left: TreeCursor,
    Right: TreeCursor,
{
    let mut pending = vec![(left, right)];
    while let Some((left, right)) = pending.pop() {
        if !zip(left, right)? {
            return Ok(false);
        }
        let left_children = Children::new(left);
        let right_children = Children::new(right);
        if left_children.len() != right_children.len() {
            return Ok(false);
        }
        pending.extend(left_children.zip(right_children).rev());
    }
    Ok(true)
}

/// A node and the results previously computed for its direct children.
pub struct FoldNode<'results, Cursor: TreeCursor, T> {
    cursor: Cursor,
    results: &'results [T],
    start: usize,
}

impl<Cursor: TreeCursor, T> Copy for FoldNode<'_, Cursor, T> {}

impl<Cursor: TreeCursor, T> Clone for FoldNode<'_, Cursor, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'results, Cursor: TreeCursor, T> FoldNode<'results, Cursor, T> {
    pub fn cursor(self) -> Cursor {
        self.cursor
    }

    pub fn child(self, id: Cursor::Id) -> &'results T {
        debug_assert!(
            self.cursor
                .child_ids()
                .any(|child| child.index() == id.index()),
            "fold result must belong to a direct child"
        );
        let index = id
            .index()
            .checked_sub(self.start)
            .expect("child is outside the folded subtree");
        self.results
            .get(index)
            .expect("child result has not been computed")
    }

    pub fn children(self) -> impl DoubleEndedIterator<Item = &'results T> + ExactSizeIterator {
        self.cursor.child_ids().map(move |id| self.child(id))
    }

    pub fn fields<'fields, Key>(
        self,
        fields: &'fields [(Key, Cursor::Id)],
    ) -> impl DoubleEndedIterator<Item = (&'fields Key, &'results T)> + ExactSizeIterator {
        fields.iter().map(move |(key, id)| (key, self.child(*id)))
    }
}

impl<'results, Cursor: TreeCursor> FoldNode<'results, Cursor, Cursor::Id> {
    pub fn rebuild<Node>(self, mut node: Node) -> Node
    where
        Node: TreeNodeMut<Cursor::Id>,
    {
        node.for_each_child_id_mut(|child| *child = *self.child(*child));
        node
    }
}

pub fn try_fold<Cursor, T, E>(
    root: Cursor,
    mut fold: impl FnMut(FoldNode<'_, Cursor, T>) -> Result<T, E>,
) -> Result<T, E>
where
    Cursor: TreeCursor,
{
    let ids = root.subtree_ids();
    let start = ids
        .clone()
        .next()
        .expect("every subtree contains its root")
        .index();
    let mut results = Vec::with_capacity(ids.len());
    for id in ids {
        let cursor = root.child(id);
        results.push(fold(FoldNode {
            cursor,
            results: &results,
            start,
        })?);
    }
    Ok(results.pop().expect("every subtree contains its root"))
}

pub fn fold<Cursor, T>(root: Cursor, mut fold: impl FnMut(FoldNode<'_, Cursor, T>) -> T) -> T
where
    Cursor: TreeCursor,
{
    try_fold::<_, _, Infallible>(root, |node| Ok(fold(node))).unwrap_or_else(|never| match never {})
}
