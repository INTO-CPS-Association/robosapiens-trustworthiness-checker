//! Borrowed cursors and allocation-free traversal iterators.

use crate::{ArenaId, FoldNode, IdRange};

/// A copyable handle that resolves structural relationships within one tree.
pub trait TreeCursor: Copy {
    type Id: ArenaId;
    type ChildIds: DoubleEndedIterator<Item = Self::Id> + ExactSizeIterator;

    fn id(self) -> Self::Id;
    fn same_node(self, other: Self) -> bool;

    fn child_ids(self) -> Self::ChildIds;
    fn child(self, id: Self::Id) -> Self;
    fn subtree_ids(self) -> IdRange<Self::Id>;
}

/// Convenience traversal operations shared by every tree cursor.
pub trait TreeCursorExt: TreeCursor {
    fn children(self) -> Children<Self> {
        Children::new(self)
    }

    fn postorder(self) -> Postorder<Self> {
        Postorder::new(self)
    }

    fn fold<T>(self, fold: impl FnMut(FoldNode<'_, Self, T>) -> T) -> T {
        crate::fold(self, fold)
    }

    fn try_fold<T, E>(
        self,
        fold: impl FnMut(FoldNode<'_, Self, T>) -> Result<T, E>,
    ) -> Result<T, E> {
        crate::try_fold(self, fold)
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

/// A cursor paired with copyable context that is retained while traversing children.
#[derive(Clone, Copy)]
pub struct ContextCursor<Cursor, Context> {
    cursor: Cursor,
    context: Context,
}

impl<Cursor, Context> ContextCursor<Cursor, Context> {
    pub fn new(cursor: Cursor, context: Context) -> Self {
        Self { cursor, context }
    }

    pub fn cursor(self) -> Cursor
    where
        Cursor: Copy,
    {
        self.cursor
    }

    pub fn context(self) -> Context
    where
        Context: Copy,
    {
        self.context
    }
}

impl<Cursor, Context> TreeCursor for ContextCursor<Cursor, Context>
where
    Cursor: TreeCursor,
    Context: Copy,
{
    type Id = Cursor::Id;
    type ChildIds = Cursor::ChildIds;

    fn id(self) -> Self::Id {
        self.cursor.id()
    }

    fn same_node(self, other: Self) -> bool {
        self.cursor.same_node(other.cursor)
    }

    fn child_ids(self) -> Self::ChildIds {
        self.cursor.child_ids()
    }

    fn child(self, id: Self::Id) -> Self {
        Self::new(self.cursor.child(id), self.context)
    }

    fn subtree_ids(self) -> IdRange<Self::Id> {
        self.cursor.subtree_ids()
    }
}

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
