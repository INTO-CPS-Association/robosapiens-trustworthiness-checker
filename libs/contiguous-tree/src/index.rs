//! Typed dense node indices and contiguous ID ranges.

use std::{marker::PhantomData, ops::Range};

/// An index type used by [`Arena`].
pub trait ArenaId: Copy {
    fn from_index(index: usize) -> Self;
    fn index(self) -> usize;
}

/// Dense arena IDs covering a contiguous subtree in allocation order.
#[derive(Clone)]
pub struct IdRange<Id> {
    range: Range<usize>,
    id: PhantomData<fn() -> Id>,
}

impl<Id> IdRange<Id> {
    pub(crate) fn new(range: Range<usize>) -> Self {
        Self {
            range,
            id: PhantomData,
        }
    }
}

impl<Id: ArenaId> IdRange<Id> {
    pub fn contains(&self, id: Id) -> bool {
        self.range.contains(&id.index())
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
