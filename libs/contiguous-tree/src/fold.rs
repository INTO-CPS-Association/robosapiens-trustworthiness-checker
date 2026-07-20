//! Bottom-up folding over contiguous subtrees.

use std::convert::Infallible;

use crate::{ArenaId, TreeCursor, TreeNodeMut};

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
