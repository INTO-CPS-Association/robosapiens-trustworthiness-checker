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

enum CloneWork<Cursor> {
    Enter(Cursor),
    Emit(Cursor),
    LeaveReplacement(NodeIdentity),
}

struct TreeCloner<Id, Node, Cursor, Replace, CloneNode, Push> {
    replace: Replace,
    clone_node: CloneNode,
    push: Push,
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
        let mut work = vec![CloneWork::Enter(source)];
        let mut results = Vec::new();
        let mut replacement_stack = Vec::new();
        let mut active_replacements = HashMap::new();

        while let Some(next) = work.pop() {
            match next {
                CloneWork::Enter(source) => {
                    if let Some(replacement) =
                        (self.replace)(source).map_err(CloneTreeError::Policy)?
                    {
                        let identity = NodeIdentity::of(replacement);
                        if let Some(&cycle_start) = active_replacements.get(&identity) {
                            let cursors = replacement_stack[cycle_start..]
                                .iter()
                                .copied()
                                .chain(std::iter::once(replacement))
                                .collect();
                            return Err(CloneTreeError::ReplacementCycle { cursors });
                        }

                        active_replacements.insert(identity, replacement_stack.len());
                        replacement_stack.push(replacement);
                        work.push(CloneWork::LeaveReplacement(identity));
                        work.push(CloneWork::Enter(replacement));
                        continue;
                    }

                    work.push(CloneWork::Emit(source));
                    work.extend(
                        source
                            .child_ids()
                            .rev()
                            .map(|child| CloneWork::Enter(source.child(child))),
                    );
                }
                CloneWork::Emit(source) => {
                    let child_count = source.child_ids().len();
                    let first_child = results
                        .len()
                        .checked_sub(child_count)
                        .expect("each cloned child produces one result");
                    let mut children = results[first_child..].iter().copied();
                    let node = map_child_ids((self.clone_node)(source), |_| {
                        children.next().expect("missing cloned child")
                    });
                    debug_assert!(children.next().is_none());
                    results.truncate(first_child);
                    results.push((self.push)(node));
                }
                CloneWork::LeaveReplacement(identity) => {
                    let replacement = replacement_stack
                        .pop()
                        .expect("replacement leave has a matching enter");
                    debug_assert!(NodeIdentity::of(replacement) == identity);
                    active_replacements.remove(&identity);
                }
            }
        }

        let [root] = results.as_slice() else {
            panic!("cloning one tree must produce exactly one root")
        };
        Ok(*root)
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
