//! Copying and rebuilding subtrees into new storage.

use std::collections::HashMap;
use std::convert::Infallible;

use crate::node::map_child_ids;
use crate::{Arena, ArenaId, FoldNode, StorageIdentity, TreeCursor, TreeNodeMut, try_fold};

/// A failure while cloning a tree with transitive subtree replacement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CloneTreeError<Cursor, PolicyError> {
    Policy(PolicyError),
    ReplacementCycle { cursors: Vec<Cursor> },
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct NodeIdentity {
    storage: StorageIdentity,
    index: usize,
}

impl NodeIdentity {
    fn of<Cursor: TreeCursor>(cursor: Cursor) -> Self {
        Self {
            storage: cursor.storage_identity(),
            index: cursor.id().index(),
        }
    }
}

struct TreeCloner<Id, Node, Cursor, Replace, CloneNode, Push> {
    replace: Replace,
    clone_node: CloneNode,
    push: Push,
    replacement_stack: Vec<Cursor>,
    active_replacements: HashMap<NodeIdentity, usize>,
    id: std::marker::PhantomData<fn(Cursor, Node) -> Id>,
}

impl<Id, Node, Cursor, Replace, CloneNode, Push>
    TreeCloner<Id, Node, Cursor, Replace, CloneNode, Push>
where
    Id: ArenaId,
    Node: TreeNodeMut<Id>,
    Cursor: TreeCursor<Id = Id>,
    CloneNode: FnMut(Cursor) -> Node,
    Push: FnMut(Node) -> Id,
{
    fn clone_with<PolicyError>(
        &mut self,
        source: Cursor,
    ) -> Result<Id, CloneTreeError<Cursor, PolicyError>>
    where
        Replace: FnMut(Cursor) -> Result<Option<Cursor>, PolicyError>,
    {
        if let Some(replacement) = (self.replace)(source).map_err(CloneTreeError::Policy)? {
            let identity = NodeIdentity::of(replacement);
            if let Some(&cycle_start) = self.active_replacements.get(&identity) {
                let cursors = self.replacement_stack[cycle_start..]
                    .iter()
                    .copied()
                    .chain(std::iter::once(replacement))
                    .collect();
                return Err(CloneTreeError::ReplacementCycle { cursors });
            }

            self.active_replacements
                .insert(identity, self.replacement_stack.len());
            self.replacement_stack.push(replacement);
            let cloned = self.clone_with(replacement);
            let active = self
                .replacement_stack
                .pop()
                .expect("replacement recursion has a matching stack entry");
            debug_assert!(NodeIdentity::of(active) == identity);
            self.active_replacements.remove(&identity);
            return cloned;
        }

        let mut error = None;
        let node = map_child_ids((self.clone_node)(source), |child| {
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
        Ok((self.push)(node))
    }
}

/// Rebuild a source tree in postorder by emitting each mapped node through `push`.
pub(crate) fn try_map_tree_from<Cursor, Id, Node, Error>(
    root: Cursor,
    mut map: impl FnMut(FoldNode<'_, Cursor, Id>) -> Result<Node, Error>,
    mut push: impl FnMut(Node) -> Id,
) -> Result<Id, Error>
where
    Id: ArenaId,
    Node: TreeNodeMut<Id>,
    Cursor: TreeCursor<Id = Id>,
{
    try_fold(root, |node| Ok(push(map(node)?)))
}

/// Rebuild a source tree infallibly by emitting each mapped node through `push`.
pub(crate) fn map_tree_from<Cursor, Id, Node>(
    root: Cursor,
    mut map: impl FnMut(FoldNode<'_, Cursor, Id>) -> Node,
    push: impl FnMut(Node) -> Id,
) -> Id
where
    Id: ArenaId,
    Node: TreeNodeMut<Id>,
    Cursor: TreeCursor<Id = Id>,
{
    try_map_tree_from::<_, _, _, Infallible>(root, |node| Ok(map(node)), push)
        .unwrap_or_else(|never| match never {})
}

/// Clone one complete occurrence subtree by emitting cloned nodes through `push`.
pub(crate) fn clone_tree_from<Cursor, Id, Node>(
    root: Cursor,
    mut clone_node: impl FnMut(Cursor) -> Node,
    push: impl FnMut(Node) -> Id,
) -> Id
where
    Id: ArenaId,
    Node: TreeNodeMut<Id>,
    Cursor: TreeCursor<Id = Id>,
{
    map_tree_from(root, |node| node.rebuild(clone_node(node.cursor())), push)
}

/// Clone a tree while recursively replacing selected source subtrees.
pub(crate) fn try_clone_tree_with<Cursor, Id, Node, PolicyError>(
    root: Cursor,
    replace: impl FnMut(Cursor) -> Result<Option<Cursor>, PolicyError>,
    clone_node: impl FnMut(Cursor) -> Node,
    push: impl FnMut(Node) -> Id,
) -> Result<Id, CloneTreeError<Cursor, PolicyError>>
where
    Id: ArenaId,
    Node: TreeNodeMut<Id>,
    Cursor: TreeCursor<Id = Id>,
{
    TreeCloner {
        replace,
        clone_node,
        push,
        replacement_stack: Vec::new(),
        active_replacements: HashMap::new(),
        id: std::marker::PhantomData,
    }
    .clone_with(root)
}

impl<Id, Node> Arena<Id, Node>
where
    Id: ArenaId,
    Node: TreeNodeMut<Id>,
{
    pub fn try_map_tree_from<Cursor, E>(
        &mut self,
        root: Cursor,
        map: impl FnMut(FoldNode<'_, Cursor, Id>) -> Result<Node, E>,
    ) -> Result<Id, E>
    where
        Cursor: TreeCursor<Id = Id>,
    {
        let original_len = self.len();
        let result = try_map_tree_from(root, map, |node| self.push_tree(node));
        if result.is_err() {
            self.truncate(original_len);
        }
        result
    }

    pub fn map_tree_from<Cursor>(
        &mut self,
        root: Cursor,
        map: impl FnMut(FoldNode<'_, Cursor, Id>) -> Node,
    ) -> Id
    where
        Cursor: TreeCursor<Id = Id>,
    {
        map_tree_from(root, map, |node| self.push_tree(node))
    }

    /// Clone one complete occurrence subtree into this arena.
    pub fn clone_tree_from<Cursor>(
        &mut self,
        root: Cursor,
        clone_node: impl FnMut(Cursor) -> Node,
    ) -> Id
    where
        Cursor: TreeCursor<Id = Id>,
    {
        clone_tree_from(root, clone_node, |node| self.push_tree(node))
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
        let original_len = self.len();
        let result = try_clone_tree_with(root, replace, clone_node, |node| self.push_tree(node));
        if result.is_err() {
            self.truncate(original_len);
        }
        result
    }
}
