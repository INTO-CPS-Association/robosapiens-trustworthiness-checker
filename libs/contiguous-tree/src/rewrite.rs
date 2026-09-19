//! Rewriting one tree family into another, one subtree per source node.
//!
//! A [`Rewriter`] converts each source node into exactly one destination
//! subtree. It may allocate several nodes, or none of its own, but it must
//! leave exactly one new root on the builder's frontier in place of the roots
//! its children produced. [`ForestBuilder::try_rewrite_forest`] checks that
//! after every call and restores storage and the frontier on any failure.
//!
//! Transcoding is the special case of allocating exactly one node per source
//! node, and [`ForestBuilder::try_transcode_forest`] is implemented on this.
//!
//! [`ForestBuilder::try_rewrite_forest`]: crate::ForestBuilder::try_rewrite_forest
//! [`ForestBuilder::try_transcode_forest`]: crate::ForestBuilder::try_transcode_forest

use std::error::Error;
use std::fmt;

use crate::{BuildError, FoldNode, ForestBuilder, PostorderStorage, TreeCursor, TreeNode};

/// A failure while rewriting a forest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RewriteError<Error, Id> {
    /// The rewriter reported its own failure.
    Convert(Error),
    /// A node named children that were not the trailing frontier roots.
    Build(BuildError<Id>),
    /// A rewriter left other than one new root for a source node.
    NotOneRoot {
        expected_roots: usize,
        actual_roots: usize,
    },
}

impl<E: fmt::Display, Id: fmt::Debug> fmt::Display for RewriteError<E, Id> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Convert(error) => write!(formatter, "tree rewrite failed: {error}"),
            Self::Build(error) => fmt::Display::fmt(error, formatter),
            Self::NotOneRoot {
                expected_roots,
                actual_roots,
            } => write!(
                formatter,
                "tree rewrite left {actual_roots} roots, expected {expected_roots}"
            ),
        }
    }
}

impl<E: fmt::Debug + fmt::Display, Id: fmt::Debug> Error for RewriteError<E, Id> {}

/// One source node, its converted children, and the builder they were built in.
pub struct RewriteNode<'fold, 'builder, Cursor, Storage, Node>
where
    Cursor: TreeCursor,
    Storage: PostorderStorage<Node>,
    Node: TreeNode<Storage::Id>,
{
    source: FoldNode<'fold, Cursor, Storage::Id>,
    builder: &'builder mut ForestBuilder<Storage, Node>,
    roots_before: usize,
    children_start: usize,
    allocated: bool,
}

impl<'fold, 'builder, Cursor, Storage, Node> RewriteNode<'fold, 'builder, Cursor, Storage, Node>
where
    Cursor: TreeCursor,
    Storage: PostorderStorage<Node>,
    Node: TreeNode<Storage::Id>,
{
    pub(crate) fn new(
        source: FoldNode<'fold, Cursor, Storage::Id>,
        builder: &'builder mut ForestBuilder<Storage, Node>,
        roots_before: usize,
        children_start: usize,
    ) -> Self {
        Self {
            source,
            builder,
            roots_before,
            children_start,
            allocated: false,
        }
    }

    /// The source node being rewritten.
    pub fn source(&self) -> Cursor {
        self.source.cursor()
    }

    /// The source node with its converted child IDs, as a fold node.
    pub fn source_node(&self) -> FoldNode<'fold, Cursor, Storage::Id> {
        self.source
    }

    /// The destination root of one direct source child.
    pub fn child(&self, source_child: Cursor::Id) -> Storage::Id {
        *self.source.child(source_child)
    }

    /// The destination roots of the direct source children, in order.
    pub fn children(
        &self,
    ) -> impl DoubleEndedIterator<Item = Storage::Id> + ExactSizeIterator + 'fold
    where
        Cursor: 'fold,
    {
        self.source.children().copied()
    }

    /// Allocate one destination node, consuming the trailing roots it names.
    pub fn alloc(&mut self, node: Node) -> Result<Storage::Id, BuildError<Storage::Id>> {
        let id = self.builder.try_alloc(node)?;
        self.allocated = true;
        Ok(id)
    }

    /// Drop the converted children, so a replacement subtree can be built.
    ///
    /// Only valid before allocating anything for this node, because the
    /// children occupy the trailing storage range.
    pub fn discard_children(&mut self) {
        assert!(
            !self.allocated,
            "children must be discarded before allocating a replacement"
        );
        self.builder
            .truncate_to(self.children_start, self.roots_before);
    }

    /// Whether this node has discarded its children without replacing them yet.
    pub fn children_discarded(&self) -> bool {
        !self.allocated && self.builder.root_count() == self.roots_before
    }
}

/// Converts each source node into one destination subtree.
pub trait Rewriter<Cursor, Storage, Node>
where
    Cursor: TreeCursor,
    Storage: PostorderStorage<Node>,
    Node: TreeNode<Storage::Id>,
{
    type Error;

    fn rewrite(
        &mut self,
        node: RewriteNode<'_, '_, Cursor, Storage, Node>,
    ) -> Result<Storage::Id, Self::Error>;
}

impl<Cursor, Storage, Node, Error, Convert> Rewriter<Cursor, Storage, Node> for Convert
where
    Cursor: TreeCursor,
    Storage: PostorderStorage<Node>,
    Node: TreeNode<Storage::Id>,
    Convert: FnMut(RewriteNode<'_, '_, Cursor, Storage, Node>) -> Result<Storage::Id, Error>,
{
    type Error = Error;

    fn rewrite(
        &mut self,
        node: RewriteNode<'_, '_, Cursor, Storage, Node>,
    ) -> Result<Storage::Id, Self::Error> {
        self(node)
    }
}
