//! Copying and rebuilding subtrees into new storage.

use std::convert::Infallible;

use crate::{Arena, ArenaId, FoldNode, TreeCursor, TreeNodeMut, map_child_ids, try_fold};

/// A failure while cloning a tree with recursive subtree replacement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CloneTreeError<Cursor, PolicyError> {
    Policy(PolicyError),
    ReplacementCycle { cursors: Vec<Cursor> },
}

struct TreeCloner<'target, Id, Node, Cursor, Replace, CloneNode> {
    target: &'target mut Arena<Id, Node>,
    replace: Replace,
    clone_node: CloneNode,
    replacement_stack: Vec<Cursor>,
}

impl<Id, Node, Cursor, Replace, CloneNode> TreeCloner<'_, Id, Node, Cursor, Replace, CloneNode>
where
    Id: ArenaId,
    Node: Clone + TreeNodeMut<Id>,
    Cursor: TreeCursor<Id = Id>,
    CloneNode: FnMut(Cursor) -> Node,
{
    fn clone_with<PolicyError>(
        &mut self,
        source: Cursor,
    ) -> Result<Id, CloneTreeError<Cursor, PolicyError>>
    where
        Replace: FnMut(Cursor) -> Result<Option<Cursor>, PolicyError>,
    {
        if let Some(replacement) = (self.replace)(source).map_err(CloneTreeError::Policy)? {
            if let Some(cycle_start) = self
                .replacement_stack
                .iter()
                .position(|active| active.same_node(replacement))
            {
                let cursors = self.replacement_stack[cycle_start..]
                    .iter()
                    .copied()
                    .chain(std::iter::once(replacement))
                    .collect();
                return Err(CloneTreeError::ReplacementCycle { cursors });
            }

            self.replacement_stack.push(replacement);
            let cloned = self.clone_with(replacement);
            self.replacement_stack.pop();
            return cloned;
        }

        let mut error = None;
        let node = map_child_ids(&(self.clone_node)(source), |child| {
            if error.is_some() {
                return child;
            }
            match self.clone_with(source.child(child)) {
                Ok(cloned) => cloned,
                Err(clone_error) => {
                    error = Some(clone_error);
                    child
                }
            }
        });
        if let Some(error) = error {
            return Err(error);
        }
        Ok(self.target.push_tree(node))
    }
}

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

    /// Clone a tree while recursively replacing selected source subtrees.
    ///
    /// `replace` returns `Ok(None)` to clone the current node and its children, or
    /// `Ok(Some(cursor))` to clone and rewrite the replacement subtree instead. Replacement
    /// expansion is transitive and cycles are reported with the participating cursors.
    pub fn try_clone_tree_with<Cursor, PolicyError>(
        &mut self,
        root: Cursor,
        replace: impl FnMut(Cursor) -> Result<Option<Cursor>, PolicyError>,
        clone_node: impl FnMut(Cursor) -> Node,
    ) -> Result<Id, CloneTreeError<Cursor, PolicyError>>
    where
        Cursor: TreeCursor<Id = Id>,
    {
        TreeCloner {
            target: self,
            replace,
            clone_node,
            replacement_stack: Vec::new(),
        }
        .clone_with(root)
    }
}
