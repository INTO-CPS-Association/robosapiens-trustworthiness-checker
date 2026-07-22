//! Invariant-preserving construction of contiguous postorder forests.

use std::error::Error;
use std::fmt;
use std::marker::PhantomData;

use crate::{
    ArenaId, CloneTreeError, Forest, ForestError, PostorderStorage, TreeCursor, TreeNode,
    TreeNodeMut,
};

/// A structural error while allocating a postorder node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BuildError<Id> {
    TooManyChildren {
        child_count: usize,
        root_count: usize,
    },
    ChildNotOnFrontier {
        position: usize,
        expected: Id,
        actual: Id,
    },
}

impl<Id: fmt::Debug> fmt::Display for BuildError<Id> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyChildren {
                child_count,
                root_count,
            } => write!(
                formatter,
                "tree node has {child_count} children but the builder contains only {root_count} roots"
            ),
            Self::ChildNotOnFrontier {
                position,
                expected,
                actual,
            } => write!(
                formatter,
                "tree child {position} is {actual:?}, expected trailing root {expected:?}"
            ),
        }
    }
}

impl<Id: fmt::Debug> Error for BuildError<Id> {}

/// Builds a contiguous postorder forest while retaining its current roots.
///
/// Each allocated node may consume only the trailing roots that it names as
/// children. Finishing the builder yields a validated [`Forest`] without repeating
/// validation when the requested roots match the constructed frontier.
pub struct ForestBuilder<Storage, Node>
where
    Storage: PostorderStorage<Node>,
    Node: TreeNode<Storage::Id>,
{
    storage: Storage,
    roots: Vec<Storage::Id>,
    node: PhantomData<fn(Node)>,
}

impl<Storage, Node> ForestBuilder<Storage, Node>
where
    Storage: PostorderStorage<Node>,
    Node: TreeNode<Storage::Id>,
{
    pub fn new(storage: Storage) -> Self {
        assert_eq!(
            storage.node_count(),
            0,
            "postorder builders require initially empty storage"
        );
        Self {
            storage,
            roots: Vec::new(),
            node: PhantomData,
        }
    }

    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    pub fn reserve(&mut self, additional: usize) {
        self.storage.reserve_nodes(additional);
        self.roots.reserve(additional);
    }

    pub fn try_alloc(&mut self, node: Node) -> Result<Storage::Id, BuildError<Storage::Id>> {
        let children = node.child_ids();
        let child_count = children.len();
        let Some(first_child) = self.roots.len().checked_sub(child_count) else {
            return Err(BuildError::TooManyChildren {
                child_count,
                root_count: self.roots.len(),
            });
        };
        for (position, (actual, expected)) in children
            .zip(self.roots[first_child..].iter().copied())
            .enumerate()
        {
            if actual.index() != expected.index() {
                return Err(BuildError::ChildNotOnFrontier {
                    position,
                    expected,
                    actual,
                });
            }
        }

        let expected_index = self.storage.node_count();
        let root = self.storage.push_node(node);
        assert_eq!(
            root.index(),
            expected_index,
            "postorder storage returned an ID other than the appended node"
        );
        assert_eq!(
            self.storage.node_count(),
            expected_index + 1,
            "postorder storage must append exactly one node"
        );
        self.roots.truncate(first_child);
        self.roots.push(root);
        Ok(root)
    }

    /// Clone one complete occurrence subtree directly into this builder.
    pub fn clone_tree_from<Cursor>(
        &mut self,
        root: Cursor,
        clone_node: impl FnMut(Cursor) -> Node,
    ) -> Storage::Id
    where
        Node: TreeNodeMut<Storage::Id>,
        Cursor: TreeCursor<Id = Storage::Id>,
    {
        let root = crate::transform::clone_tree_from(root, clone_node, |node| {
            self.storage.push_node(node)
        });
        self.roots.push(root);
        root
    }

    /// Clone a tree while recursively replacing selected source subtrees.
    ///
    /// Partial output is discarded if replacement fails or contains a cycle.
    pub fn try_clone_tree_with<Cursor, PolicyError>(
        &mut self,
        root: Cursor,
        replace: impl FnMut(Cursor) -> Result<Option<Cursor>, PolicyError>,
        clone_node: impl FnMut(Cursor) -> Node,
    ) -> Result<Storage::Id, CloneTreeError<Cursor, PolicyError>>
    where
        Node: TreeNodeMut<Storage::Id>,
        Cursor: TreeCursor<Id = Storage::Id>,
    {
        let original_len = self.storage.node_count();
        match crate::transform::try_clone_tree_with(root, replace, clone_node, |node| {
            self.storage.push_node(node)
        }) {
            Ok(root) => {
                self.roots.push(root);
                Ok(root)
            }
            Err(error) => {
                self.storage.truncate_nodes(original_len);
                Err(error)
            }
        }
    }

    pub fn finish(
        self,
        roots: impl IntoIterator<Item = Storage::Id>,
    ) -> Result<Forest<Storage>, ForestError> {
        let roots = roots.into_iter().collect::<Box<[_]>>();
        let same_roots = if roots.len() == self.roots.len() {
            let mut requested = roots.iter().map(|root| root.index()).collect::<Vec<_>>();
            let mut constructed = self
                .roots
                .iter()
                .map(|root| root.index())
                .collect::<Vec<_>>();
            requested.sort_unstable();
            constructed.sort_unstable();
            requested == constructed
        } else {
            false
        };

        if !same_roots {
            self.storage.validate_forest(&roots)?;
        }
        Ok(Forest::from_validated_parts(self.storage, roots))
    }
}
