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
        Self::with_capacity(storage, 0)
    }

    /// Create a builder with space for at least `root_capacity` frontier roots.
    pub fn with_capacity(storage: Storage, root_capacity: usize) -> Self {
        assert_eq!(
            storage.node_count(),
            0,
            "postorder builders require initially empty storage"
        );
        Self {
            storage,
            roots: Vec::with_capacity(root_capacity),
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

    pub(crate) fn root_count(&self) -> usize {
        self.roots.len()
    }

    /// Drop every node from `storage_len` on, keeping `root_count` roots.
    pub(crate) fn truncate_to(&mut self, storage_len: usize, root_count: usize) {
        self.storage.truncate_nodes(storage_len);
        self.roots.truncate(root_count);
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

    /// Fallibly rewrite an ordered forest from any cursor family.
    ///
    /// Each source node becomes exactly one destination *subtree*: a rewriter
    /// may allocate any number of nodes, but must leave exactly one new root on
    /// the frontier in place of its children's roots. Both storage and the root
    /// frontier are restored on any failure.
    pub fn try_rewrite_forest<Cursor, Rewrite>(
        &mut self,
        roots: impl IntoIterator<Item = Cursor>,
        rewriter: &mut Rewrite,
    ) -> Result<Vec<Storage::Id>, crate::RewriteError<Rewrite::Error, Storage::Id>>
    where
        Cursor: TreeCursor,
        Rewrite: crate::Rewriter<Cursor, Storage, Node> + ?Sized,
    {
        let original_len = self.storage.node_count();
        let original_roots = self.roots.clone();
        let result: Result<Vec<_>, _> = roots
            .into_iter()
            .map(|root| {
                let forest_start = self.storage.node_count();
                crate::try_fold(root, |folded: crate::FoldNode<'_, Cursor, Storage::Id>| {
                    let child_count = folded.cursor().child_ids().len();
                    let roots_before = self.roots.len().checked_sub(child_count).ok_or(
                        crate::RewriteError::NotOneRoot {
                            expected_roots: child_count,
                            actual_roots: self.roots.len(),
                        },
                    )?;
                    // Postorder storage places a node's children after every
                    // earlier frontier root, so they occupy the trailing range
                    // beginning here.
                    let children_start = match roots_before.checked_sub(1) {
                        Some(previous) => self.roots[previous].index() + 1,
                        None => forest_start,
                    };
                    let emitted = rewriter
                        .rewrite(crate::RewriteNode::new(
                            folded,
                            self,
                            roots_before,
                            children_start,
                        ))
                        .map_err(crate::RewriteError::Convert)?;
                    if self.roots.len() != roots_before + 1
                        || self.roots[roots_before].index() != emitted.index()
                    {
                        return Err(crate::RewriteError::NotOneRoot {
                            expected_roots: roots_before + 1,
                            actual_roots: self.roots.len(),
                        });
                    }
                    Ok(emitted)
                })
            })
            .collect();
        if result.is_err() {
            self.storage.truncate_nodes(original_len);
            self.roots = original_roots;
        }
        result
    }

    /// Rewrite one subtree, rolling back all emitted nodes on failure.
    pub fn try_rewrite<Cursor, Rewrite>(
        &mut self,
        root: Cursor,
        rewriter: &mut Rewrite,
    ) -> Result<Storage::Id, crate::RewriteError<Rewrite::Error, Storage::Id>>
    where
        Cursor: TreeCursor,
        Rewrite: crate::Rewriter<Cursor, Storage, Node> + ?Sized,
    {
        self.try_rewrite_forest([root], rewriter)
            .map(|roots| roots[0])
    }

    /// Fallibly transcode an ordered forest from any cursor family.
    ///
    /// The converter receives source cursors and already converted destination
    /// IDs; the ID types need not be the same. It must preserve the direct child
    /// order. Both storage and the root frontier are restored on any failure.
    /// This is the one-node-per-source-node case of [`Self::try_rewrite_forest`].
    pub fn try_transcode_forest<Cursor: TreeCursor, Error>(
        &mut self,
        roots: impl IntoIterator<Item = Cursor>,
        mut convert: impl FnMut(crate::FoldNode<'_, Cursor, Storage::Id>) -> Result<Node, Error>,
    ) -> Result<Vec<Storage::Id>, crate::TranscodeError<Error, Storage::Id>> {
        let mut transcode = |mut node: crate::RewriteNode<'_, '_, Cursor, Storage, Node>| {
            let folded = node.source_node();
            let converted = convert(folded).map_err(crate::TranscodeError::Convert)?;
            if !folded
                .children()
                .map(|id| id.index())
                .eq(converted.child_ids().map(|id| id.index()))
            {
                let expected = folded.children().copied().collect();
                let actual = converted.child_ids().collect();
                return Err(crate::TranscodeError::ChildrenChanged { expected, actual });
            }
            node.alloc(converted).map_err(crate::TranscodeError::Build)
        };
        self.try_rewrite_forest(roots, &mut transcode)
            .map_err(|error| match error {
                crate::RewriteError::Convert(error) => error,
                crate::RewriteError::Build(error) => crate::TranscodeError::Build(error),
                crate::RewriteError::NotOneRoot { .. } => {
                    unreachable!("transcoding allocates exactly one node per source node")
                }
            })
    }

    /// Transcode one subtree, rolling back all emitted nodes on failure.
    pub fn try_transcode<Cursor: TreeCursor, Error>(
        &mut self,
        root: Cursor,
        convert: impl FnMut(crate::FoldNode<'_, Cursor, Storage::Id>) -> Result<Node, Error>,
    ) -> Result<Storage::Id, crate::TranscodeError<Error, Storage::Id>> {
        self.try_transcode_forest([root], convert)
            .map(|roots| roots[0])
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
        let same_roots_in_order = roots.len() == self.roots.len()
            && roots
                .iter()
                .zip(&self.roots)
                .all(|(requested, constructed)| requested.index() == constructed.index());

        if !same_roots_in_order {
            let same_roots_unordered = if roots.len() == self.roots.len() {
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

            if !same_roots_unordered {
                self.storage.validate_forest(&roots)?;
            }
        }
        Ok(Forest::from_validated_parts(self.storage, roots))
    }
}
