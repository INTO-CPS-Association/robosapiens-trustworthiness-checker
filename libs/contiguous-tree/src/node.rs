//! Structural access to child IDs stored in a node.

use crate::ArenaId;

/// A stored node whose direct children are represented by arena IDs.
pub trait TreeNode<Id: ArenaId> {
    type ChildIds<'node>: DoubleEndedIterator<Item = Id> + ExactSizeIterator
    where
        Self: 'node;

    fn child_ids(&self) -> Self::ChildIds<'_>;
}

pub trait TreeNodeMut<Id: ArenaId>: TreeNode<Id> {
    fn for_each_child_id_mut(&mut self, visit: impl FnMut(&mut Id));
}

pub(crate) fn map_child_ids<Id, Node>(mut node: Node, mut map: impl FnMut(Id) -> Id) -> Node
where
    Id: ArenaId,
    Node: TreeNodeMut<Id>,
{
    node.for_each_child_id_mut(|child| *child = map(*child));
    node
}

/// Child IDs consisting of a small fixed prefix and an optional trailing collection.
pub struct ChildIds<'node, Id: Copy, Key> {
    fixed: [Option<Id>; 3],
    fixed_position: u8,
    fixed_len: u8,
    tail: Option<ChildIdTail<'node, Id, Key>>,
}

enum ChildIdTail<'node, Id, Key> {
    Slice(std::slice::Iter<'node, Id>),
    Keyed(std::slice::Iter<'node, (Key, Id)>),
}

impl<'node, Id: Copy, Key> ChildIds<'node, Id, Key> {
    pub fn empty() -> Self {
        Self {
            fixed: [None; 3],
            fixed_position: 0,
            fixed_len: 0,
            tail: None,
        }
    }

    pub fn push(&mut self, id: Id) {
        debug_assert!(
            self.tail.is_none(),
            "fixed children must precede child collections"
        );
        let slot = self
            .fixed
            .get_mut(usize::from(self.fixed_len))
            .expect("tree schema supports at most three fixed children");
        *slot = Some(id);
        self.fixed_len += 1;
    }

    pub fn extend_slice(&mut self, ids: &'node [Id]) {
        debug_assert!(
            self.tail.is_none(),
            "a node has at most one child collection"
        );
        self.tail = Some(ChildIdTail::Slice(ids.iter()));
    }

    pub fn extend_keyed(&mut self, fields: &'node [(Key, Id)]) {
        debug_assert!(
            self.tail.is_none(),
            "a node has at most one child collection"
        );
        self.tail = Some(ChildIdTail::Keyed(fields.iter()));
    }
}

impl<Id: Copy, Key> Iterator for ChildIds<'_, Id, Key> {
    type Item = Id;

    fn next(&mut self) -> Option<Self::Item> {
        if self.fixed_position < self.fixed_len {
            let id = self.fixed[usize::from(self.fixed_position)]
                .expect("fixed child slots are populated in order");
            self.fixed_position += 1;
            return Some(id);
        }
        match &mut self.tail {
            Some(ChildIdTail::Slice(ids)) => ids.next().copied(),
            Some(ChildIdTail::Keyed(fields)) => fields.next().map(|(_, id)| *id),
            None => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let tail_len = match &self.tail {
            Some(ChildIdTail::Slice(ids)) => ids.len(),
            Some(ChildIdTail::Keyed(fields)) => fields.len(),
            None => 0,
        };
        let len = usize::from(self.fixed_len - self.fixed_position) + tail_len;
        (len, Some(len))
    }
}

impl<Id: Copy, Key> ExactSizeIterator for ChildIds<'_, Id, Key> {}

impl<Id: Copy, Key> DoubleEndedIterator for ChildIds<'_, Id, Key> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let tail = match &mut self.tail {
            Some(ChildIdTail::Slice(ids)) => ids.next_back().copied(),
            Some(ChildIdTail::Keyed(fields)) => fields.next_back().map(|(_, id)| *id),
            None => None,
        };
        if tail.is_some() {
            return tail;
        }
        if self.fixed_position < self.fixed_len {
            self.fixed_len -= 1;
            self.fixed[usize::from(self.fixed_len)]
        } else {
            None
        }
    }
}
