//! Copying and rebuilding subtrees into new storage.

use std::convert::Infallible;

use crate::{Arena, ArenaId, FoldNode, TreeCursor, TreeNodeMut, try_fold};

impl<Id, Node> Arena<Id, Node>
where
    Id: ArenaId,
    Node: Clone + TreeNodeMut<Id>,
{
    pub fn try_map_tree_from<Cursor, E>(
        &mut self,
        root: Cursor,
        mut map: impl FnMut(FoldNode<'_, Cursor, Id>) -> Result<Node, E>,
    ) -> Result<Id, E>
    where
        Cursor: TreeCursor<Id = Id>,
    {
        try_fold(root, |node| Ok(self.push_tree(map(node)?)))
    }

    pub fn map_tree_from<Cursor>(
        &mut self,
        root: Cursor,
        mut map: impl FnMut(FoldNode<'_, Cursor, Id>) -> Node,
    ) -> Id
    where
        Cursor: TreeCursor<Id = Id>,
    {
        self.try_map_tree_from::<_, Infallible>(root, |node| Ok(map(node)))
            .unwrap_or_else(|never| match never {})
    }

    /// Clone one complete occurrence subtree into this arena.
    pub fn clone_tree_from<Cursor>(
        &mut self,
        root: Cursor,
        mut clone_node: impl FnMut(Cursor) -> Node,
    ) -> Id
    where
        Cursor: TreeCursor<Id = Id>,
    {
        self.map_tree_from(root, |node| node.rebuild(clone_node(node.cursor())))
    }
}
