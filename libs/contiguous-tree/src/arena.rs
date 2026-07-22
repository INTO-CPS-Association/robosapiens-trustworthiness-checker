//! Dense indexed storage and contiguous subtree ranges.

use std::{marker::PhantomData, ops::Range};

use crate::TreeNode;

/// An index type used by [`Arena`].
pub trait ArenaId: Copy {
    fn from_index(index: usize) -> Self;
    fn index(self) -> usize;
}

/// Contiguous indexed storage for tree nodes.
#[derive(Clone, Debug, PartialEq)]
pub struct Arena<Id, Node> {
    nodes: Vec<Node>,
    id: PhantomData<fn() -> Id>,
}

impl<Id, Node> Default for Arena<Id, Node> {
    fn default() -> Self {
        Self {
            nodes: Vec::new(),
            id: PhantomData,
        }
    }
}

impl<Id: ArenaId, Node> Arena<Id, Node> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            nodes: Vec::with_capacity(capacity),
            id: PhantomData,
        }
    }

    pub(crate) fn push_raw(&mut self, node: Node) -> Id {
        let id = Id::from_index(self.nodes.len());
        self.nodes.push(node);
        id
    }

    pub fn reserve(&mut self, additional: usize) {
        self.nodes.reserve(additional);
    }

    pub fn get(&self, id: Id) -> &Node {
        &self.nodes[id.index()]
    }

    pub fn get_mut(&mut self, id: Id) -> &mut Node {
        &mut self.nodes[id.index()]
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (Id, &Node)> + ExactSizeIterator {
        self.nodes
            .iter()
            .enumerate()
            .map(|(index, node)| (Id::from_index(index), node))
    }
}

impl<Id: ArenaId, Node: TreeNode<Id>> Arena<Id, Node> {
    /// Append a node to a contiguous postorder forest.
    ///
    /// Every child must already belong to this arena. Its child subtrees must
    /// immediately precede the new node, without gaps or sharing, and must occur
    /// in the same order as [`TreeNode::child_ids`]. These requirements make every
    /// completed subtree one contiguous ID range.
    ///
    /// The invariants are checked with debug assertions. Release builds trust the
    /// caller; violating them can make subtree iteration and folding include the
    /// wrong nodes or panic, but cannot by itself cause memory unsafety.
    pub fn push_tree(&mut self, node: Node) -> Id {
        #[cfg(debug_assertions)]
        {
            let id = Id::from_index(self.nodes.len());
            for child in node.child_ids() {
                debug_assert!(
                    child.index() < id.index(),
                    "tree children must be allocated before their parent"
                );
            }
            let subtree_len = node
                .child_ids()
                .map(|child| self.subtree_len(child))
                .sum::<usize>()
                + 1;
            let mut next = id.index() + 1 - subtree_len;
            for child in node.child_ids() {
                let child_len = self.subtree_len(child);
                debug_assert_eq!(
                    child.index() + 1 - child_len,
                    next,
                    "child subtrees must be contiguous and allocated in source order"
                );
                next += child_len;
            }
            debug_assert_eq!(next, id.index());
        }
        self.push_raw(node)
    }

    pub fn subtree_len(&self, id: Id) -> usize {
        let mut first = id;
        while let Some(child) = self.get(first).child_ids().next() {
            first = child;
        }
        id.index() + 1 - first.index()
    }

    pub fn subtree_ids(&self, root: Id) -> IdRange<Id> {
        let end = root.index() + 1;
        IdRange::new(end - self.subtree_len(root)..end)
    }

    pub fn assert_forest(&self, roots: impl IntoIterator<Item = Id>) {
        let mut parents = vec![0_u8; self.nodes.len()];
        for (_, node) in self.iter() {
            for child in node.child_ids() {
                parents[child.index()] = parents[child.index()].saturating_add(1);
            }
        }
        let mut is_root = vec![false; self.nodes.len()];
        for root in roots {
            is_root[root.index()] = true;
        }
        for (index, parent_count) in parents.into_iter().enumerate() {
            let expected = usize::from(!is_root[index]);
            debug_assert_eq!(
                usize::from(parent_count),
                expected,
                "each tree occurrence must have exactly one parent, except roots"
            );
        }
    }
}

/// Dense arena IDs covering a contiguous subtree in allocation order.
#[derive(Clone)]
pub struct IdRange<Id> {
    range: Range<usize>,
    id: PhantomData<fn() -> Id>,
}

impl<Id> IdRange<Id> {
    fn new(range: Range<usize>) -> Self {
        Self {
            range,
            id: PhantomData,
        }
    }
}

impl<Id: ArenaId> Iterator for IdRange<Id> {
    type Item = Id;

    fn next(&mut self) -> Option<Self::Item> {
        self.range.next().map(Id::from_index)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.range.size_hint()
    }
}

impl<Id: ArenaId> DoubleEndedIterator for IdRange<Id> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.range.next_back().map(Id::from_index)
    }
}

impl<Id: ArenaId> ExactSizeIterator for IdRange<Id> {}
