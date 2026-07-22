//! Dense indexed storage and contiguous subtree ranges.

use std::{error::Error, fmt, marker::PhantomData};

use crate::{ArenaId, IdRange, TreeNode};

/// A structural error in a contiguous postorder occurrence forest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ForestError {
    RootOutOfBounds {
        root: usize,
        len: usize,
    },
    DuplicateRoot {
        root: usize,
    },
    ChildOutOfBounds {
        parent: usize,
        child: usize,
        len: usize,
    },
    ChildNotPostorder {
        parent: usize,
        child: usize,
    },
    SharedOccurrence {
        index: usize,
        parent_count: usize,
    },
    RootHasParent {
        root: usize,
        parent_count: usize,
    },
    MissingRoot {
        index: usize,
    },
    ChildSubtreeGap {
        parent: usize,
        child: usize,
        expected_start: usize,
        actual_start: usize,
    },
    ChildrenNotAdjacentToParent {
        parent: usize,
        last_child: usize,
        child_end: usize,
    },
}

impl fmt::Display for ForestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::RootOutOfBounds { root, len } => {
                write!(
                    f,
                    "forest root {root} is out of bounds for storage of length {len}"
                )
            }
            Self::DuplicateRoot { root } => write!(f, "forest root {root} occurs more than once"),
            Self::ChildOutOfBounds { parent, child, len } => write!(
                f,
                "node {parent} has child {child}, which is out of bounds for storage of length {len}"
            ),
            Self::ChildNotPostorder { parent, child } => write!(
                f,
                "node {parent} has child {child}, which does not precede it in postorder"
            ),
            Self::SharedOccurrence {
                index,
                parent_count,
            } => write!(
                f,
                "node {index} is shared by {parent_count} parent occurrences"
            ),
            Self::RootHasParent { root, parent_count } => {
                write!(
                    f,
                    "forest root {root} has {parent_count} parent occurrences"
                )
            }
            Self::MissingRoot { index } => {
                write!(
                    f,
                    "parentless node {index} is missing from the forest roots"
                )
            }
            Self::ChildSubtreeGap {
                parent,
                child,
                expected_start,
                actual_start,
            } => write!(
                f,
                "child subtree {child} of node {parent} starts at {actual_start}, expected {expected_start} after the previous child"
            ),
            Self::ChildrenNotAdjacentToParent {
                parent,
                last_child,
                child_end,
            } => write!(
                f,
                "children of node {parent} end at {child_end} after child {last_child}, expected them to end at the parent"
            ),
        }
    }
}

impl Error for ForestError {}

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

    /// Discard nodes from `len` onward.
    pub(crate) fn truncate(&mut self, len: usize) {
        self.nodes.truncate(len);
    }

    pub fn get(&self, id: Id) -> &Node {
        &self.nodes[id.index()]
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
    /// This low-level operation exists for crate-internal transforms. Public
    /// construction goes through `ForestBuilder`, which checks the root frontier.
    pub(crate) fn push_tree(&mut self, node: Node) -> Id {
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

    /// Validate that `roots` designates every tree in this arena.
    ///
    /// Unlike the construction-time debug assertions, this checks all IDs before
    /// indexing and returns a concrete error for malformed external input.
    pub fn validate_forest(&self, roots: &[Id]) -> Result<(), ForestError> {
        #[derive(Clone, Copy, Default)]
        enum OccurrenceState {
            #[default]
            Unclaimed,
            Root,
            Child,
            RootAndChild,
        }

        let len = self.nodes.len();
        let mut states = vec![OccurrenceState::Unclaimed; len];
        let mut subtree_starts = vec![0_usize; len];
        for &root in roots {
            let root = root.index();
            if root >= len {
                return Err(ForestError::RootOutOfBounds { root, len });
            }
            states[root] = match states[root] {
                OccurrenceState::Unclaimed => OccurrenceState::Root,
                OccurrenceState::Child => OccurrenceState::RootAndChild,
                OccurrenceState::Root | OccurrenceState::RootAndChild => {
                    return Err(ForestError::DuplicateRoot { root });
                }
            };
        }

        for (parent, node) in self.nodes.iter().enumerate() {
            let mut first_start = parent;
            let mut expected_start = None;
            let mut last_child = None;
            for child in node.child_ids() {
                let child = child.index();
                if child >= len {
                    return Err(ForestError::ChildOutOfBounds { parent, child, len });
                }
                if child >= parent {
                    return Err(ForestError::ChildNotPostorder { parent, child });
                }

                states[child] = match states[child] {
                    OccurrenceState::Unclaimed => OccurrenceState::Child,
                    OccurrenceState::Root => OccurrenceState::RootAndChild,
                    OccurrenceState::Child | OccurrenceState::RootAndChild => {
                        return Err(ForestError::SharedOccurrence {
                            index: child,
                            parent_count: 2,
                        });
                    }
                };

                let actual_start = subtree_starts[child];
                if let Some(expected_start) = expected_start
                    && actual_start != expected_start
                {
                    return Err(ForestError::ChildSubtreeGap {
                        parent,
                        child,
                        expected_start,
                        actual_start,
                    });
                }
                if last_child.is_none() {
                    first_start = actual_start;
                }
                expected_start = Some(child + 1);
                last_child = Some(child);
            }
            if let Some(child) = last_child
                && expected_start != Some(parent)
            {
                return Err(ForestError::ChildrenNotAdjacentToParent {
                    parent,
                    last_child: child,
                    child_end: expected_start.expect("a last child has an end"),
                });
            }
            subtree_starts[parent] = first_start;
        }

        for (index, state) in states.into_iter().enumerate() {
            match state {
                OccurrenceState::Root | OccurrenceState::Child => {}
                OccurrenceState::RootAndChild => {
                    return Err(ForestError::RootHasParent {
                        root: index,
                        parent_count: 1,
                    });
                }
                OccurrenceState::Unclaimed => {
                    return Err(ForestError::MissingRoot { index });
                }
            }
        }

        Ok(())
    }

    /// Panic if `roots` does not designate every tree in this arena.
    pub fn assert_forest(&self, roots: impl IntoIterator<Item = Id>) {
        let roots = roots.into_iter().collect::<Vec<_>>();
        if let Err(error) = self.validate_forest(&roots) {
            panic!("invalid contiguous forest: {error}");
        }
    }
}
