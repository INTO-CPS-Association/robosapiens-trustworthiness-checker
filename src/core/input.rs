//! Logical input updates, ticks, batches, and data-only streams.
//!
//! # Contract
//!
//! An input tick is one nonempty, duplicate-free set of variable updates evaluated
//! together. [`InputBatch`] stores an ordered sequence of ticks; a physical batch
//! boundary does not add model time, and packed storage does not change simultaneous
//! or sequential tick boundaries. [`InputStream`] carries only data batches.
//! Reconfiguration control uses a separate crate-private adapter.
//!
//! # Principal entities
//!
//! | Entity | Responsibility |
//! |---|---|
//! | [`InputUpdate`] | Names one variable and value inside a logical tick. |
//! | [`InputBatch`] | Owns zero or more ordered logical ticks while preserving efficient native storage. |
//! | [`InputTick`] and [`InputUpdateRef`] | Borrow one logical tick and its updates without expanding packed rows. |
//! | [`InputTicks`], [`InputUpdates`], and [`OwnedInputTicks`] | Traverse logical order independently of physical segment representation. |
//! | [`InputStream`] | Delivers successful batches or source errors to an ordinary runtime. |
//!
//! # Example
//!
//! ```
//! use trustworthiness_checker::{InputBatch, InputUpdate};
//!
//! # fn main() -> anyhow::Result<()> {
//! let batch = InputBatch::from_ticks(vec![
//!     vec![InputUpdate::new("x".into(), 1)],
//!     vec![
//!         InputUpdate::new("x".into(), 2),
//!         InputUpdate::new("y".into(), 3),
//!     ],
//! ])?;
//!
//! assert_eq!(batch.tick_count(), 2);
//! assert_eq!(batch.update_count(), 3);
//! assert_eq!(batch.ticks().nth(1).unwrap().len(), 2);
//! # Ok(())
//! # }
//! ```
//!
//! The first tick is an independent update. The second tick contains simultaneous
//! `x` and `y` updates. Delivering both in one [`InputBatch`] does not merge them.
//!
//! # Implementation mapping
//!
//! `InputSegment` represents singleton runs, simultaneous ticks, and fixed-layout
//! packed rows. `InputBatchStorage` combines one or several segments without exposing
//! that representation publicly. The iterator types project those segments onto the
//! logical tick contract; source resolution, opening, windowing, and live ownership
//! are implemented by the input pipeline modules under `crate::io`.

use std::{collections::HashSet, slice, vec};

use futures::StreamExt;

use super::{OutputStream, VarName};

/// One variable update in a logical input tick.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputUpdate<V> {
    pub variable: VarName,
    pub value: V,
}

impl<V> InputUpdate<V> {
    #[inline]
    pub fn new(variable: VarName, value: V) -> Self {
        Self { variable, value }
    }
}

/// A physical segment in an [`InputBatch`]. A segment is never itself a
/// batch: it is only one representation of an ordered range of logical ticks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum InputSegment<V> {
    /// Each update is an independent width-one logical tick.
    SingletonTicks(Vec<InputUpdate<V>>),
    /// Exactly one simultaneous logical tick.
    Tick(Vec<InputUpdate<V>>),
    /// Fixed-width row-major ticks sharing one variable layout.
    PackedRows {
        layout: Box<[VarName]>,
        values: Vec<V>,
    },
}

impl<V> InputSegment<V> {
    fn validate(&self) -> anyhow::Result<()> {
        match self {
            Self::SingletonTicks(_) => Ok(()),
            Self::Tick(updates) => validate_tick(updates),
            Self::PackedRows { layout, values } => validate_packed_layout(layout, values.len()),
        }
    }

    pub(crate) fn update_count(&self) -> usize {
        match self {
            Self::SingletonTicks(updates) | Self::Tick(updates) => updates.len(),
            Self::PackedRows { values, .. } => values.len(),
        }
    }

    pub(crate) fn tick_count(&self) -> usize {
        match self {
            Self::SingletonTicks(updates) => updates.len(),
            Self::Tick(updates) => usize::from(!updates.is_empty()),
            Self::PackedRows { layout, values } => values.len() / layout.len(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.tick_count() == 0
    }

    pub(crate) fn packed_rows(&self) -> Option<(&[VarName], &[V])> {
        match self {
            Self::PackedRows { layout, values } => Some((layout, values)),
            Self::SingletonTicks(_) | Self::Tick(_) => None,
        }
    }

    fn map_values<U, F>(self, map: &mut F) -> InputSegment<U>
    where
        F: FnMut(&VarName, V) -> U,
    {
        match self {
            Self::SingletonTicks(updates) => InputSegment::SingletonTicks(
                updates
                    .into_iter()
                    .map(|InputUpdate { variable, value }| {
                        let value = map(&variable, value);
                        InputUpdate { variable, value }
                    })
                    .collect(),
            ),
            Self::Tick(updates) => InputSegment::Tick(
                updates
                    .into_iter()
                    .map(|InputUpdate { variable, value }| {
                        let value = map(&variable, value);
                        InputUpdate { variable, value }
                    })
                    .collect(),
            ),
            Self::PackedRows { layout, values } => {
                let width = layout.len();
                InputSegment::PackedRows {
                    values: values
                        .into_iter()
                        .enumerate()
                        .map(|(index, value)| map(&layout[index % width], value))
                        .collect(),
                    layout,
                }
            }
        }
    }
}

/// The physical storage used by an [`InputBatch`]. `Single` deliberately
/// represents one segment; a batch may contain mixed segment kinds through
/// `Segments` without recursively nesting batches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum InputBatchStorage<V> {
    Single(InputSegment<V>),
    Segments(Vec<InputSegment<V>>),
}

/// An ordered sequence of logical input ticks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputBatch<V> {
    storage: InputBatchStorage<V>,
}

impl<V> InputBatch<V> {
    /// Construct one independent width-one tick.
    pub fn update(variable: impl Into<VarName>, value: V) -> Self {
        Self {
            storage: InputBatchStorage::Single(InputSegment::SingletonTicks(vec![
                InputUpdate::new(variable.into(), value),
            ])),
        }
    }

    /// Construct one simultaneous tick.
    pub fn tick(updates: Vec<InputUpdate<V>>) -> anyhow::Result<Self> {
        validate_tick(&updates)?;
        Ok(Self {
            storage: InputBatchStorage::Single(InputSegment::Tick(updates)),
        })
    }

    /// Construct a batch from ordered logical ticks. Consecutive width-one
    /// ticks are compacted into a singleton segment; wider ticks remain
    /// distinct `Tick` segments.
    pub fn from_ticks(ticks: Vec<Vec<InputUpdate<V>>>) -> anyhow::Result<Self> {
        let mut segments = Vec::new();
        let mut singleton_updates = Vec::new();
        for tick in ticks {
            validate_tick(&tick)?;
            if tick.len() == 1 {
                singleton_updates.push(tick.into_iter().next().expect("length checked"));
            } else {
                if !singleton_updates.is_empty() {
                    segments.push(InputSegment::SingletonTicks(std::mem::take(
                        &mut singleton_updates,
                    )));
                }
                segments.push(InputSegment::Tick(tick));
            }
        }
        if !singleton_updates.is_empty() {
            segments.push(InputSegment::SingletonTicks(singleton_updates));
        }
        Self::from_segments(segments)
    }

    /// Construct fixed-width row-major ticks. Empty values are allowed and
    /// represent an empty batch with a known layout.
    pub(crate) fn packed_rows(
        layout: impl Into<Box<[VarName]>>,
        values: Vec<V>,
    ) -> anyhow::Result<Self> {
        let segment = InputSegment::PackedRows {
            layout: layout.into(),
            values,
        };
        segment.validate()?;
        Ok(Self {
            storage: InputBatchStorage::Single(segment),
        })
    }

    pub fn empty() -> Self {
        Self {
            storage: InputBatchStorage::Single(InputSegment::SingletonTicks(Vec::new())),
        }
    }

    /// Construct a batch from physical segments without flattening them.
    pub(crate) fn from_segments(
        segments: impl IntoIterator<Item = InputSegment<V>>,
    ) -> anyhow::Result<Self> {
        let mut segments = segments.into_iter().collect::<Vec<_>>();
        for segment in &segments {
            segment.validate()?;
        }
        segments.retain(|segment| !segment.is_empty());
        let storage = match segments.len() {
            0 => InputBatchStorage::Single(InputSegment::SingletonTicks(Vec::new())),
            1 => InputBatchStorage::Single(segments.pop().expect("length checked")),
            _ => InputBatchStorage::Segments(segments),
        };
        Ok(Self { storage })
    }

    fn segment_cursor(&self) -> SegmentCursor<'_, V> {
        match &self.storage {
            InputBatchStorage::Single(segment) => SegmentCursor::Single(Some(segment)),
            InputBatchStorage::Segments(segments) => SegmentCursor::Slice(segments.iter()),
        }
    }

    /// Iterate physical segments in order without exposing their storage
    /// representation outside the crate.
    pub(crate) fn segments(&self) -> impl ExactSizeIterator<Item = &InputSegment<V>> + '_ {
        self.segment_cursor()
    }

    pub(crate) fn packed_rows_segment(&self) -> Option<(&[VarName], &[V])> {
        let mut segments = self.segments();
        let segment = segments.next()?;
        (segments.next().is_none())
            .then(|| segment.packed_rows())
            .flatten()
    }

    #[cfg(test)]
    pub(crate) fn segment_count(&self) -> usize {
        match &self.storage {
            InputBatchStorage::Single(segment) => usize::from(!segment.is_empty()),
            InputBatchStorage::Segments(segments) => segments.len(),
        }
    }

    pub fn tick_count(&self) -> usize {
        self.segments().map(InputSegment::tick_count).sum()
    }

    pub fn update_count(&self) -> usize {
        self.segments().map(InputSegment::update_count).sum()
    }

    pub fn len(&self) -> usize {
        self.update_count()
    }

    pub fn is_empty(&self) -> bool {
        self.tick_count() == 0
    }

    /// Borrow logical ticks without expanding packed storage.
    pub fn ticks(&self) -> InputTicks<'_, V> {
        InputTicks::new(self.segment_cursor(), self.tick_count())
    }

    /// Borrow updates in logical order without allocating.
    pub fn updates(&self) -> InputUpdates<'_, V> {
        InputUpdates::new(self.segment_cursor(), self.update_count())
    }

    /// Move physical segments into another batch. This is the preferred way
    /// for source composition and window stages to concatenate batches.
    pub fn concat(self, other: Self) -> anyhow::Result<Self> {
        let mut segments = self.into_segments();
        segments.extend(other.into_segments());
        Self::from_segments(segments)
    }

    pub(crate) fn into_segments(self) -> Vec<InputSegment<V>> {
        match self.storage {
            InputBatchStorage::Single(segment) if !segment.is_empty() => vec![segment],
            InputBatchStorage::Single(_) => Vec::new(),
            InputBatchStorage::Segments(segments) => segments,
        }
    }

    /// Move logical ticks out of the batch. Runtimes use this at fanout or
    /// evaluator boundaries; packed storage stays packed until that point.
    pub(crate) fn into_ticks(self) -> OwnedInputTicks<V> {
        let remaining = self.tick_count();
        OwnedInputTicks {
            segments: self.into_segments().into_iter(),
            current: None,
            remaining,
        }
    }

    /// Retain only selected variables while preserving packed row storage.
    pub fn select_variables(
        self,
        variables: &std::collections::BTreeSet<VarName>,
    ) -> anyhow::Result<Self> {
        let mut segments = Vec::new();
        for segment in self.into_segments() {
            match segment {
                InputSegment::SingletonTicks(updates) => {
                    let updates = updates
                        .into_iter()
                        .filter(|update| variables.contains(&update.variable))
                        .collect::<Vec<_>>();
                    if !updates.is_empty() {
                        segments.push(InputSegment::SingletonTicks(updates));
                    }
                }
                InputSegment::Tick(updates) => {
                    let updates = updates
                        .into_iter()
                        .filter(|update| variables.contains(&update.variable))
                        .collect::<Vec<_>>();
                    if !updates.is_empty() {
                        segments.push(InputSegment::Tick(updates));
                    }
                }
                InputSegment::PackedRows { layout, values } => {
                    let width = layout.len();
                    let selected = layout
                        .iter()
                        .map(|variable| variables.contains(variable))
                        .collect::<Vec<_>>();
                    let selected_width = selected.iter().filter(|&&keep| keep).count();
                    if selected_width == 0 {
                        continue;
                    }
                    if selected_width == width {
                        segments.push(InputSegment::PackedRows { layout, values });
                        continue;
                    }
                    let layout = layout
                        .into_vec()
                        .into_iter()
                        .zip(&selected)
                        .filter_map(|(variable, &keep)| keep.then_some(variable))
                        .collect::<Vec<_>>()
                        .into_boxed_slice();
                    let values = values
                        .into_iter()
                        .enumerate()
                        .filter_map(|(index, value)| selected[index % width].then_some(value))
                        .collect();
                    segments.push(InputSegment::PackedRows { layout, values });
                }
            }
        }
        Self::from_segments(segments)
    }

    pub fn map_values<U, F>(self, mut map: F) -> InputBatch<U>
    where
        F: FnMut(V) -> U,
    {
        self.map_update_values(|_, value| map(value))
    }

    pub fn map_update_values<U, F>(self, mut map: F) -> InputBatch<U>
    where
        F: FnMut(&VarName, V) -> U,
    {
        let segments = self
            .into_segments()
            .into_iter()
            .map(|segment| segment.map_values(&mut map))
            .collect::<Vec<_>>();
        InputBatch::from_segments(segments).expect("mapping preserves valid input shape")
    }

    pub fn try_map_values<U, E, F>(self, mut map: F) -> Result<InputBatch<U>, E>
    where
        F: FnMut(V) -> Result<U, E>,
    {
        self.try_map_update_values(|_, value| map(value))
    }

    pub fn try_map_update_values<U, E, F>(self, mut map: F) -> Result<InputBatch<U>, E>
    where
        F: FnMut(&VarName, V) -> Result<U, E>,
    {
        let mut mapped = Vec::new();
        for segment in self.into_segments() {
            let segment = match segment {
                InputSegment::SingletonTicks(updates) => InputSegment::SingletonTicks(
                    updates
                        .into_iter()
                        .map(|InputUpdate { variable, value }| {
                            let value = map(&variable, value)?;
                            Ok(InputUpdate { variable, value })
                        })
                        .collect::<Result<Vec<_>, E>>()?,
                ),
                InputSegment::Tick(updates) => InputSegment::Tick(
                    updates
                        .into_iter()
                        .map(|InputUpdate { variable, value }| {
                            let value = map(&variable, value)?;
                            Ok(InputUpdate { variable, value })
                        })
                        .collect::<Result<Vec<_>, E>>()?,
                ),
                InputSegment::PackedRows { layout, values } => {
                    let width = layout.len();
                    InputSegment::PackedRows {
                        values: values
                            .into_iter()
                            .enumerate()
                            .map(|(index, value)| map(&layout[index % width], value))
                            .collect::<Result<Vec<_>, E>>()?,
                        layout,
                    }
                }
            };
            mapped.push(segment);
        }
        Ok(InputBatch::from_segments(mapped).expect("mapping preserves valid input shape"))
    }
}

impl<V> From<InputUpdate<V>> for InputBatch<V> {
    fn from(update: InputUpdate<V>) -> Self {
        Self::update(update.variable, update.value)
    }
}

fn validate_tick<V>(updates: &[InputUpdate<V>]) -> anyhow::Result<()> {
    anyhow::ensure!(
        !updates.is_empty(),
        "input tick must contain at least one update"
    );
    let mut seen = HashSet::with_capacity(updates.len());
    for update in updates {
        anyhow::ensure!(
            seen.insert(&update.variable),
            "input tick contains duplicate variable `{}`",
            update.variable
        );
    }
    Ok(())
}

fn validate_packed_layout(layout: &[VarName], value_count: usize) -> anyhow::Result<()> {
    anyhow::ensure!(!layout.is_empty(), "packed input layout must not be empty");
    let mut seen = HashSet::with_capacity(layout.len());
    for variable in layout {
        anyhow::ensure!(
            seen.insert(variable),
            "packed input layout contains duplicate variable `{variable}`"
        );
    }
    anyhow::ensure!(
        value_count.is_multiple_of(layout.len()),
        "packed input contains {value_count} values, which is not divisible by layout width {}",
        layout.len()
    );
    Ok(())
}

enum SegmentCursor<'a, V> {
    Single(Option<&'a InputSegment<V>>),
    Slice(slice::Iter<'a, InputSegment<V>>),
}

impl<'a, V> Iterator for SegmentCursor<'a, V> {
    type Item = &'a InputSegment<V>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Single(segment) => segment.take(),
            Self::Slice(segments) => segments.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = match self {
            Self::Single(segment) => usize::from(segment.is_some()),
            Self::Slice(segments) => segments.len(),
        };
        (remaining, Some(remaining))
    }
}

impl<V> ExactSizeIterator for SegmentCursor<'_, V> {}

#[derive(Clone, Copy, Debug)]
enum TickRepresentation<'a, V> {
    Updates(&'a [InputUpdate<V>]),
    Packed {
        layout: &'a [VarName],
        values: &'a [V],
    },
}

/// A borrowed logical tick.
#[derive(Clone, Copy, Debug)]
pub struct InputTick<'a, V> {
    representation: TickRepresentation<'a, V>,
}

#[derive(Clone, Copy, Debug)]
pub struct InputUpdateRef<'a, V> {
    pub variable: &'a VarName,
    pub value: &'a V,
}

enum TickUpdates<'a, V> {
    Updates(slice::Iter<'a, InputUpdate<V>>),
    Packed(std::iter::Zip<slice::Iter<'a, VarName>, slice::Iter<'a, V>>),
}

impl<'a, V> Iterator for TickUpdates<'a, V> {
    type Item = InputUpdateRef<'a, V>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Updates(updates) => updates.next().map(|update| InputUpdateRef {
                variable: &update.variable,
                value: &update.value,
            }),
            Self::Packed(values) => values
                .next()
                .map(|(variable, value)| InputUpdateRef { variable, value }),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Updates(updates) => updates.size_hint(),
            Self::Packed(values) => values.size_hint(),
        }
    }
}

impl<V> ExactSizeIterator for TickUpdates<'_, V> {}

impl<'a, V> InputTick<'a, V> {
    pub fn len(&self) -> usize {
        match self.representation {
            TickRepresentation::Updates(updates) => updates.len(),
            TickRepresentation::Packed { values, .. } => values.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn updates(&self) -> impl ExactSizeIterator<Item = InputUpdateRef<'a, V>> + '_ {
        match self.representation {
            TickRepresentation::Updates(updates) => TickUpdates::Updates(updates.iter()),
            TickRepresentation::Packed { layout, values } => {
                TickUpdates::Packed(layout.iter().zip(values.iter()))
            }
        }
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = InputUpdateRef<'a, V>> + '_ {
        self.updates()
    }

    pub fn to_updates(&self) -> Vec<InputUpdate<V>>
    where
        V: Clone,
    {
        self.updates()
            .map(|update| InputUpdate::new(update.variable.clone(), update.value.clone()))
            .collect()
    }
}

enum SegmentTicks<'a, V> {
    Singleton(slice::Iter<'a, InputUpdate<V>>),
    Tick(Option<&'a [InputUpdate<V>]>),
    Packed {
        layout: &'a [VarName],
        rows: slice::ChunksExact<'a, V>,
    },
}

impl<'a, V> SegmentTicks<'a, V> {
    fn new(segment: &'a InputSegment<V>) -> Self {
        match segment {
            InputSegment::SingletonTicks(updates) => Self::Singleton(updates.iter()),
            InputSegment::Tick(updates) => Self::Tick(Some(updates)),
            InputSegment::PackedRows { layout, values } => Self::Packed {
                layout,
                rows: values.chunks_exact(layout.len()),
            },
        }
    }

    fn next(&mut self) -> Option<InputTick<'a, V>> {
        let representation = match self {
            Self::Singleton(updates) => {
                TickRepresentation::Updates(slice::from_ref(updates.next()?))
            }
            Self::Tick(updates) => TickRepresentation::Updates(updates.take()?),
            Self::Packed { layout, rows } => TickRepresentation::Packed {
                layout,
                values: rows.next()?,
            },
        };
        Some(InputTick { representation })
    }
}

pub struct InputTicks<'a, V> {
    segments: SegmentCursor<'a, V>,
    current: Option<SegmentTicks<'a, V>>,
    remaining: usize,
}

impl<'a, V> InputTicks<'a, V> {
    fn new(segments: SegmentCursor<'a, V>, remaining: usize) -> Self {
        Self {
            segments,
            current: None,
            remaining,
        }
    }
}

impl<'a, V> Iterator for InputTicks<'a, V> {
    type Item = InputTick<'a, V>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(tick) = self.current.as_mut().and_then(SegmentTicks::next) {
                self.remaining -= 1;
                return Some(tick);
            }
            self.current = Some(SegmentTicks::new(self.segments.next()?));
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<V> ExactSizeIterator for InputTicks<'_, V> {}

struct PackedUpdates<'a, V> {
    layout: &'a [VarName],
    values: slice::Iter<'a, V>,
    offset: usize,
}

enum SegmentUpdates<'a, V> {
    Updates(slice::Iter<'a, InputUpdate<V>>),
    Packed(PackedUpdates<'a, V>),
}

impl<'a, V> SegmentUpdates<'a, V> {
    fn new(segment: &'a InputSegment<V>) -> Self {
        match segment {
            InputSegment::SingletonTicks(updates) | InputSegment::Tick(updates) => {
                Self::Updates(updates.iter())
            }
            InputSegment::PackedRows { layout, values } => Self::Packed(PackedUpdates {
                layout,
                values: values.iter(),
                offset: 0,
            }),
        }
    }

    fn next(&mut self) -> Option<InputUpdateRef<'a, V>> {
        match self {
            Self::Updates(updates) => updates.next().map(|update| InputUpdateRef {
                variable: &update.variable,
                value: &update.value,
            }),
            Self::Packed(PackedUpdates {
                layout,
                values,
                offset,
            }) => {
                let value = values.next()?;
                let variable = &layout[*offset % layout.len()];
                *offset += 1;
                Some(InputUpdateRef { variable, value })
            }
        }
    }
}

pub struct InputUpdates<'a, V> {
    segments: SegmentCursor<'a, V>,
    current: Option<SegmentUpdates<'a, V>>,
    remaining: usize,
}

impl<'a, V> InputUpdates<'a, V> {
    fn new(segments: SegmentCursor<'a, V>, remaining: usize) -> Self {
        Self {
            segments,
            current: None,
            remaining,
        }
    }
}

impl<'a, V> Iterator for InputUpdates<'a, V> {
    type Item = InputUpdateRef<'a, V>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(update) = self.current.as_mut().and_then(SegmentUpdates::next) {
                self.remaining -= 1;
                return Some(update);
            }
            self.current = Some(SegmentUpdates::new(self.segments.next()?));
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<V> ExactSizeIterator for InputUpdates<'_, V> {}

enum OwnedSegment<V> {
    Singleton(vec::IntoIter<InputUpdate<V>>),
    Tick(Option<Vec<InputUpdate<V>>>),
    Packed {
        layout: Box<[VarName]>,
        values: vec::IntoIter<V>,
    },
}

pub struct OwnedInputTicks<V> {
    segments: vec::IntoIter<InputSegment<V>>,
    current: Option<OwnedSegment<V>>,
    remaining: usize,
}

impl<V> Iterator for OwnedInputTicks<V> {
    type Item = Vec<InputUpdate<V>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(current) = &mut self.current {
                let next = match current {
                    OwnedSegment::Singleton(updates) => updates.next().map(|update| vec![update]),
                    OwnedSegment::Tick(updates) => updates.take(),
                    OwnedSegment::Packed { layout, values } => {
                        let mut row = Vec::with_capacity(layout.len());
                        for index in 0..layout.len() {
                            let value = values.next()?;
                            row.push(InputUpdate::new(layout[index].clone(), value));
                        }
                        Some(row)
                    }
                };
                if next.is_some() {
                    self.remaining = self.remaining.saturating_sub(1);
                    return next;
                }
                self.current = None;
            }
            let segment = self.segments.next()?;
            self.current = Some(match segment {
                InputSegment::SingletonTicks(updates) => {
                    OwnedSegment::Singleton(updates.into_iter())
                }
                InputSegment::Tick(updates) => OwnedSegment::Tick(Some(updates)),
                InputSegment::PackedRows { layout, values } => OwnedSegment::Packed {
                    layout,
                    values: values.into_iter(),
                },
            });
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<V> ExactSizeIterator for OwnedInputTicks<V> {}

/// Ordinary runtimes receive data only. Reconfiguration is available through
/// the private `io::reconfigurable_input` adapter instead.
// ANCHOR: input_stream_alias
pub type InputStream<V> = OutputStream<anyhow::Result<InputBatch<V>>>;
// ANCHOR_END: input_stream_alias
pub(crate) type InputTickStream<V> = OutputStream<anyhow::Result<Vec<InputUpdate<V>>>>;

pub fn empty_input_stream<V: 'static>() -> InputStream<V> {
    Box::pin(futures::stream::empty())
}

pub fn once_input_batch<V: 'static>(batch: InputBatch<V>) -> InputStream<V> {
    Box::pin(futures::stream::iter([Ok(batch)]))
}

pub fn map_input_values<V, U, F>(mut stream: InputStream<V>, mut map: F) -> InputStream<U>
where
    V: 'static,
    U: 'static,
    F: FnMut(V) -> U + 'static,
{
    Box::pin(async_stream::try_stream! {
        while let Some(batch) = stream.next().await {
            yield batch?.map_values(&mut map);
        }
    })
}

pub fn try_map_input_values<V, U, E, F>(mut stream: InputStream<V>, mut map: F) -> InputStream<U>
where
    V: 'static,
    U: 'static,
    E: Into<anyhow::Error>,
    F: FnMut(V) -> Result<U, E> + 'static,
{
    Box::pin(async_stream::try_stream! {
        while let Some(batch) = stream.next().await {
            yield batch?.try_map_values(&mut map).map_err(Into::into)?;
        }
    })
}

/// Compose data-only child streams in observed completion order. A completed
/// child never terminates its siblings and child errors retain their source
/// context at the source boundary.
pub fn compose_input_streams<V>(mut streams: Vec<InputStream<V>>) -> InputStream<V>
where
    V: 'static,
{
    match streams.len() {
        0 => Box::pin(futures::stream::empty()),
        1 => streams.pop().expect("source count checked above"),
        _ => Box::pin(futures::stream::select_all(streams)),
    }
}

/// Expand a data-only batch into owned logical ticks at a runtime fanout or
/// evaluator boundary.
pub(crate) fn into_tick_stream<V: 'static>(mut input: InputStream<V>) -> InputTickStream<V> {
    Box::pin(async_stream::try_stream! {
        while let Some(batch) = input.next().await {
            for tick in batch?.into_ticks() {
                yield tick;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn update(variable: &str, value: i32) -> InputUpdate<i32> {
        InputUpdate::new(variable.into(), value)
    }

    #[test]
    fn logical_ticks_cover_singleton_atomic_packed_and_mixed_storage() {
        let packed =
            InputBatch::packed_rows(vec!["y".into(), "z".into()], vec![2, 3, 4, 5]).unwrap();
        let mixed = InputBatch::from_segments([
            InputSegment::SingletonTicks(vec![update("x", 1)]),
            InputSegment::Tick(vec![update("a", 6), update("b", 7)]),
            packed.into_segments().pop().unwrap(),
        ])
        .unwrap();
        let ticks = mixed
            .ticks()
            .map(|tick| tick.to_updates())
            .collect::<Vec<_>>();
        assert_eq!(
            ticks,
            vec![
                vec![update("x", 1)],
                vec![update("a", 6), update("b", 7)],
                vec![update("y", 2), update("z", 3)],
                vec![update("y", 4), update("z", 5)],
            ]
        );
        assert_eq!(mixed.segment_count(), 3);
    }

    #[test]
    fn constructors_reject_invalid_ticks_and_layouts() {
        assert!(InputBatch::tick(Vec::<InputUpdate<i32>>::new()).is_err());
        assert!(InputBatch::tick(vec![update("x", 1), update("x", 2)]).is_err());
        assert!(InputBatch::packed_rows(Vec::<VarName>::new(), vec![1]).is_err());
        assert!(InputBatch::packed_rows(vec!["x".into(), "x".into()], vec![1, 2]).is_err());
        assert!(InputBatch::packed_rows(vec!["x".into(), "y".into()], vec![1]).is_err());
    }

    #[test]
    fn mapping_and_concatenation_preserve_physical_storage_and_order() {
        let packed =
            InputBatch::packed_rows(vec!["x".into(), "y".into()], vec![1, 2, 3, 4]).unwrap();
        let singleton = InputBatch::update("z", 5);
        let mapped = packed.map_values(|value| value * 10);
        assert!(matches!(
            mapped.segments().next(),
            Some(InputSegment::PackedRows { .. })
        ));
        let combined = singleton.concat(mapped).unwrap();
        assert_eq!(
            combined
                .ticks()
                .map(|tick| tick.to_updates())
                .collect::<Vec<_>>(),
            vec![
                vec![update("z", 5)],
                vec![update("x", 10), update("y", 20)],
                vec![update("x", 30), update("y", 40)],
            ]
        );
        assert!(matches!(
            combined.segments().nth(1),
            Some(InputSegment::PackedRows { .. })
        ));
    }

    #[test]
    fn borrowed_iterators_are_exact_across_single_and_mixed_storage() {
        let single = InputBatch::update("x", 1);
        let mut ticks = single.ticks();
        assert_eq!(ticks.len(), 1);
        assert_eq!(ticks.next().unwrap().updates().len(), 1);
        assert_eq!(ticks.len(), 0);

        let mixed = InputBatch::from_segments([
            InputSegment::SingletonTicks(vec![update("x", 1), update("x", 2)]),
            InputSegment::PackedRows {
                layout: vec!["y".into(), "z".into()].into_boxed_slice(),
                values: vec![3, 4, 5, 6],
            },
        ])
        .unwrap();
        let mut updates = mixed.updates();
        assert_eq!(updates.len(), 6);
        assert_eq!(updates.by_ref().count(), 6);
        assert_eq!(updates.len(), 0);
        assert_eq!(mixed.ticks().len(), 4);
    }

    #[test]
    fn selecting_a_packed_subset_preserves_packed_rows() {
        let batch = InputBatch::packed_rows(
            vec!["x".into(), "y".into(), "z".into()],
            vec![1, 2, 3, 4, 5, 6],
        )
        .unwrap();
        let selected = batch
            .select_variables(&[VarName::new("x"), VarName::new("z")].into_iter().collect())
            .unwrap();

        let Some(InputSegment::PackedRows { layout, values }) = selected.segments().next() else {
            panic!("a nonempty packed subset must remain packed");
        };
        assert_eq!(layout.as_ref(), &[VarName::new("x"), VarName::new("z")]);
        assert_eq!(values, &[1, 3, 4, 6]);
        assert_eq!(selected.tick_count(), 2);
    }

    #[test]
    fn owned_iteration_preserves_tick_boundaries() {
        let batch = InputBatch::from_ticks(vec![
            vec![update("x", 1)],
            vec![update("x", 2), update("y", 3)],
        ])
        .unwrap();
        assert_eq!(
            batch.into_ticks().collect::<Vec<_>>(),
            vec![vec![update("x", 1)], vec![update("x", 2), update("y", 3)]]
        );
    }
}
