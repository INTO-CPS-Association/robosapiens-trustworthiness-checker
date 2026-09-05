//! Stable identities for computed streams.
//!
//! A [`StreamId`] names a computed stream for the whole life of a program, independently of the
//! order a schedule happens to run it in. [`StreamSlots`] is the affine map between those
//! identities and the environment row, and [`StreamSet`] is the sorted-set form used wherever
//! dependencies, sources, or commit sets are recorded.
//!
//! This module owns identity only: no dependency graph, no plan, and no mutable per-tick state.

use super::environment::EnvironmentSlot;

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct StreamId(usize);

impl StreamId {
    #[inline]
    pub(super) fn new(index: usize) -> Self {
        Self(index)
    }

    #[inline]
    pub(super) fn index(self) -> usize {
        self.0
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct StreamSlots {
    start: EnvironmentSlot,
    len: usize,
}

impl StreamSlots {
    pub(super) fn new(start: EnvironmentSlot, len: usize) -> Self {
        Self { start, len }
    }

    #[inline]
    pub(super) fn slot(self, stream: StreamId) -> EnvironmentSlot {
        debug_assert!(stream.index() < self.len);
        EnvironmentSlot::new(self.start.index() + stream.index())
    }

    #[inline]
    pub(super) fn stream(self, slot: EnvironmentSlot) -> Option<StreamId> {
        let index = slot.index().checked_sub(self.start.index())?;
        (index < self.len).then(|| StreamId::new(index))
    }

    #[inline]
    pub(super) fn start(self) -> EnvironmentSlot {
        self.start
    }

    pub(super) fn len(self) -> usize {
        self.len
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct StreamSet {
    pub(in crate::dataflow) streams: Vec<StreamId>,
}

impl StreamSet {
    pub(super) fn empty() -> Self {
        Self {
            streams: Vec::new(),
        }
    }

    pub(super) fn from_streams(streams: impl IntoIterator<Item = StreamId>) -> Self {
        let mut streams = streams.into_iter().collect::<Vec<_>>();
        streams.sort_unstable();
        streams.dedup();
        Self { streams }
    }

    #[inline]
    pub(super) fn contains(&self, stream: StreamId) -> bool {
        self.streams.binary_search(&stream).is_ok()
    }

    #[inline]
    pub(super) fn iter(&self) -> impl Iterator<Item = StreamId> + '_ {
        self.streams.iter().copied()
    }

    #[inline]
    pub(super) fn as_slice(&self) -> &[StreamId] {
        &self.streams
    }
}
