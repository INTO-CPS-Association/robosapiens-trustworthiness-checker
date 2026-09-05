//! Logical output batches, resolved interfaces, and the runtime-facing writer.
//!
//! # Contract
//!
//! A logical output tick is one nonempty, duplicate-free set of variable updates.
//! [`OutputBatch`] preserves an ordered sequence of ticks across singleton,
//! simultaneous, packed-row, and mixed physical storage. [`OutputWriter`] accepts
//! complete batches under sink readiness; acceptance is not proof that a remote
//! destination persisted or consumed them.
//!
//! # Principal entities
//!
//! | Entity | Responsibility |
//! |---|---|
//! | [`OutputUpdate`] | Names one variable and value inside a logical output tick. |
//! | [`OutputBatch`] | Owns ordered logical ticks while preserving their native physical segments. |
//! | [`OutputTick`], [`OutputTicks`], and [`OutputUpdates`] | Borrow the logical tick and update views without expanding packed rows. |
//! | [`OutputInterface`] and [`OutputRoute`] | Describe the resolved variables, roles, and transport routes supported by one opened writer. |
//! | [`OutputWriter`] | Applies readiness, send, flush, close, and sticky-error semantics to an output sink. |
//! | [`OutputError`] and [`OutputErrorKind`] | Classify closed, backend, source, and invalid-output failures. |
//!
//! # Example
//!
//! ```
//! use trustworthiness_checker::{OutputBatch, OutputUpdate};
//!
//! # fn main() -> Result<(), trustworthiness_checker::OutputError> {
//! let batch = OutputBatch::from_ticks(vec![
//!     vec![OutputUpdate::new("a".into(), true)],
//!     vec![
//!         OutputUpdate::new("a".into(), false),
//!         OutputUpdate::new("b".into(), true),
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
//! The batch contains two ordered ticks; its second tick publishes `a` and `b`
//! simultaneously. Mapping, concatenation, and variable selection preserve remaining
//! logical boundaries.
//!
//! # Writer lifecycle
//!
//! [`OutputWriter::send`] admits a batch and drives the wrapped sink back to readiness
//! without forcing a flush. [`OutputWriter::send_and_flush`] adds a downstream flush
//! barrier. The first operation failure is retained for later data operations, while
//! [`OutputWriter::close`] still attempts wrapped cleanup and combines a cleanup error
//! with the retained primary failure.
//!
//! # Implementation mapping
//!
//! `OutputSegment` and `OutputBatchStorage` preserve native physical forms behind the
//! logical iterators. [`OutputInterface`] owns immutable resolved route indexes.
//! [`OutputWriter`] wraps `DynOutputSink` and owns sticky failure and close state;
//! destination resolution, stage composition, routing, and live sessions are
//! implemented by the output pipeline under `crate::io`.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll},
};

use async_trait::async_trait;
use futures::{Sink, SinkExt, future::LocalBoxFuture};

use super::batch::{self, SegmentAccess, SegmentView};
use super::{StreamData, ValidatedLayout, VarName};

/// The kind of failure reported by an output operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputErrorKind {
    Closed,
    Backend,
    Source,
    Invalid,
}

/// An error from an output source, route, or sink.
///
/// The payloads are owned strings deliberately: output errors are cloneable so
/// [`OutputWriter`] can retain the first failure and return the same failure
/// from every subsequent operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutputError {
    /// The output writer or sink has already been closed.
    Closed,
    /// A backend could not accept or finish an output operation.
    Backend(String),
    /// An output source failed while producing output.
    Source(String),
    /// Output data or routing configuration is invalid.
    Invalid(String),
}

impl OutputError {
    pub fn backend(error: impl fmt::Display) -> Self {
        Self::Backend(error.to_string())
    }

    pub fn source(error: impl fmt::Display) -> Self {
        Self::Source(error.to_string())
    }

    pub fn invalid(error: impl fmt::Display) -> Self {
        Self::Invalid(error.to_string())
    }

    pub fn kind(&self) -> OutputErrorKind {
        match self {
            Self::Closed => OutputErrorKind::Closed,
            Self::Backend(_) => OutputErrorKind::Backend,
            Self::Source(_) => OutputErrorKind::Source,
            Self::Invalid(_) => OutputErrorKind::Invalid,
        }
    }

    pub fn is_closed(&self) -> bool {
        matches!(self, Self::Closed)
    }

    pub fn is_backend(&self) -> bool {
        matches!(self, Self::Backend(_))
    }

    pub fn is_source(&self) -> bool {
        matches!(self, Self::Source(_))
    }

    pub fn is_invalid(&self) -> bool {
        matches!(self, Self::Invalid(_))
    }

    pub fn message(&self) -> Option<&str> {
        match self {
            Self::Closed => None,
            Self::Backend(message) | Self::Source(message) | Self::Invalid(message) => {
                Some(message)
            }
        }
    }
}

impl fmt::Display for OutputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => formatter.write_str("output is closed"),
            Self::Backend(error) => write!(formatter, "output backend error: {error}"),
            Self::Source(error) => write!(formatter, "output source error: {error}"),
            Self::Invalid(error) => write!(formatter, "invalid output: {error}"),
        }
    }
}

impl std::error::Error for OutputError {}

impl From<anyhow::Error> for OutputError {
    fn from(error: anyhow::Error) -> Self {
        Self::invalid(error)
    }
}

impl From<String> for OutputError {
    fn from(error: String) -> Self {
        Self::invalid(error)
    }
}

impl From<&str> for OutputError {
    fn from(error: &str) -> Self {
        Self::invalid(error)
    }
}

/// One variable update in an output tick.
pub type OutputUpdate<V> = batch::Update<V>;

/// A borrowed output update yielded while iterating a tick or batch.
pub type OutputUpdateRef<'a, V> = batch::UpdateRef<'a, V>;

/// A physical output segment. A segment is never itself a batch: it is one
/// representation of an ordered range of logical output ticks.
#[derive(Clone, Debug, PartialEq)]
enum OutputSegment<V> {
    /// Each update is an independent width-one logical tick.
    SingletonTicks(Vec<OutputUpdate<V>>),
    /// Exactly one simultaneous logical tick.
    Tick(Vec<OutputUpdate<V>>),
    /// Fixed-width row-major ticks sharing one validated variable layout.
    /// Valid packed segments contain at least one complete row.
    PackedRows {
        layout: ValidatedLayout,
        values: Vec<V>,
    },
}

impl<V> SegmentAccess<V> for OutputSegment<V> {
    fn view(&self) -> SegmentView<'_, V> {
        match self {
            Self::SingletonTicks(updates) => SegmentView::Singleton(updates),
            Self::Tick(updates) => SegmentView::Tick(updates),
            Self::PackedRows { layout, values } => SegmentView::Packed {
                layout: layout.variables(),
                values,
            },
        }
    }
    fn into_owned(self) -> batch::OwnedSegment<V> {
        match self {
            Self::SingletonTicks(v) => batch::OwnedSegment::Singleton(v.into_iter()),
            Self::Tick(v) => batch::OwnedSegment::Tick(Some(v)),
            Self::PackedRows { layout, values } => {
                let width = layout.len();
                batch::OwnedSegment::Packed {
                    layout: layout.variables().to_vec(),
                    values: values.into_iter(),
                    width,
                }
            }
        }
    }
}

impl<V> OutputSegment<V> {
    fn validate(&self) -> Result<(), OutputError> {
        match self {
            Self::SingletonTicks(_) => Ok(()),
            Self::Tick(updates) => validate_tick(updates),
            Self::PackedRows { layout, values } => validate_packed_rows(layout, values.len()),
        }
    }

    fn update_count(&self) -> usize {
        match self {
            Self::SingletonTicks(updates) | Self::Tick(updates) => updates.len(),
            Self::PackedRows { values, .. } => values.len(),
        }
    }

    fn tick_count(&self) -> usize {
        match self {
            Self::SingletonTicks(updates) => updates.len(),
            Self::Tick(updates) => usize::from(!updates.is_empty()),
            Self::PackedRows { layout, values } => {
                if layout.is_empty() {
                    0
                } else {
                    values.len() / layout.len()
                }
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.tick_count() == 0
    }

    fn is_zero_row_packed(&self) -> bool {
        matches!(self, Self::PackedRows { values, .. } if values.is_empty())
    }

    fn map_values<U, F>(self, map: &mut F) -> OutputSegment<U>
    where
        F: FnMut(&VarName, V) -> U,
    {
        match self {
            Self::SingletonTicks(updates) => {
                OutputSegment::SingletonTicks(batch::map_updates(updates, map))
            }
            Self::Tick(updates) => OutputSegment::Tick(batch::map_updates(updates, map)),
            Self::PackedRows { layout, values } => {
                let values = batch::map_packed(layout.variables(), values, map);
                OutputSegment::PackedRows { layout, values }
            }
        }
    }
}

/// The physical storage used by an [`OutputBatch`]. `Single` deliberately
/// represents one segment; a batch may contain mixed segment kinds through
/// `Segments` without recursively nesting batches.
type OutputBatchStorage<V> = batch::Storage<OutputSegment<V>>;

/// An ordered sequence of logical output ticks.
#[derive(Clone, Debug, PartialEq)]
pub struct OutputBatch<V> {
    storage: OutputBatchStorage<V>,
}

impl<V> OutputBatch<V> {
    /// Construct one independent width-one tick.
    pub fn update(variable: impl Into<VarName>, value: V) -> Self {
        Self {
            storage: OutputBatchStorage::Single(OutputSegment::SingletonTicks(vec![
                OutputUpdate::new(variable.into(), value),
            ])),
        }
    }

    /// Construct one simultaneous tick.
    pub fn tick(updates: Vec<OutputUpdate<V>>) -> Result<Self, OutputError> {
        validate_tick(&updates)?;
        Ok(Self {
            storage: OutputBatchStorage::Single(OutputSegment::Tick(updates)),
        })
    }

    /// Construct a batch from ordered logical ticks. Consecutive width-one
    /// ticks are compacted into one singleton segment; wider ticks remain
    /// distinct `Tick` segments.
    pub fn from_ticks(ticks: Vec<Vec<OutputUpdate<V>>>) -> Result<Self, OutputError> {
        let mut segments = Vec::new();
        let mut singleton_updates = Vec::new();
        for tick in ticks {
            validate_tick(&tick)?;
            if tick.len() == 1 {
                singleton_updates.push(tick.into_iter().next().expect("length checked"));
            } else {
                if !singleton_updates.is_empty() {
                    segments.push(OutputSegment::SingletonTicks(std::mem::take(
                        &mut singleton_updates,
                    )));
                }
                segments.push(OutputSegment::Tick(tick));
            }
        }
        if !singleton_updates.is_empty() {
            segments.push(OutputSegment::SingletonTicks(singleton_updates));
        }
        Self::from_segments(segments)
    }

    /// Construct non-empty fixed-width row-major output ticks.
    ///
    /// `values` must contain at least one complete row for `layout`. Use
    /// [`Self::empty`] when there is no output to represent.
    pub fn packed_rows<L, I>(layout: L, values: I) -> Result<Self, OutputError>
    where
        L: Into<Arc<[VarName]>>,
        I: IntoIterator<Item = V>,
    {
        let layout = ValidatedLayout::from_arc(layout.into()).map_err(OutputError::from)?;
        Self::packed_rows_with_layout(layout, values.into_iter().collect())
    }

    /// Construct the canonical empty output batch.
    ///
    /// Empty output is not represented by a zero-row packed segment.
    pub fn empty() -> Self {
        Self {
            storage: OutputBatchStorage::Single(OutputSegment::SingletonTicks(Vec::new())),
        }
    }

    fn packed_rows_with_layout(
        layout: ValidatedLayout,
        values: Vec<V>,
    ) -> Result<Self, OutputError> {
        let segment = OutputSegment::PackedRows { layout, values };
        segment.validate()?;
        Ok(Self {
            storage: OutputBatchStorage::Single(segment),
        })
    }

    /// Construct a batch from physical segments without flattening them.
    fn from_segments(
        segments: impl IntoIterator<Item = OutputSegment<V>>,
    ) -> Result<Self, OutputError> {
        let storage = batch::normalize(
            segments,
            || OutputSegment::SingletonTicks(Vec::new()),
            |segment| {
                if segment.is_zero_row_packed() {
                    Ok(())
                } else {
                    segment.validate()
                }
            },
            OutputSegment::is_empty,
            |previous, segment| match (previous, segment) {
                (
                    OutputSegment::SingletonTicks(previous),
                    OutputSegment::SingletonTicks(mut next),
                ) => {
                    previous.append(&mut next);
                    Ok(None)
                }
                (
                    OutputSegment::PackedRows {
                        layout: previous_layout,
                        values: previous_values,
                    },
                    OutputSegment::PackedRows { layout, values },
                ) if previous_layout == &layout => {
                    previous_values.extend(values);
                    Ok(None)
                }
                (_, segment) => Ok(Some(segment)),
            },
        )?;
        Ok(Self { storage })
    }

    fn segment_cursor(&self) -> batch::SegmentCursor<'_, OutputSegment<V>> {
        self.storage.cursor()
    }

    #[cfg(test)]
    fn segment_count(&self) -> usize {
        match &self.storage {
            OutputBatchStorage::Single(segment) => usize::from(!segment.is_empty()),
            OutputBatchStorage::Segments(segments) => segments.len(),
        }
    }

    pub fn tick_count(&self) -> usize {
        self.segment_cursor().map(OutputSegment::tick_count).sum()
    }

    pub fn update_count(&self) -> usize {
        self.segment_cursor().map(OutputSegment::update_count).sum()
    }

    pub fn len(&self) -> usize {
        self.update_count()
    }

    pub fn is_empty(&self) -> bool {
        self.tick_count() == 0
    }

    /// Validate the shape of this batch without expanding packed storage.
    pub fn validate(&self) -> Result<(), OutputError> {
        for segment in self.segment_cursor() {
            segment.validate()?;
        }
        Ok(())
    }

    /// Borrow logical ticks without expanding packed storage.
    pub fn ticks(&self) -> OutputTicks<'_, V> {
        OutputTicks::new(self.segment_cursor(), self.tick_count())
    }

    /// Borrow updates in logical order without allocating.
    pub fn updates(&self) -> OutputUpdates<'_, V> {
        OutputUpdates::new(
            self.segment_cursor(),
            self.tick_count(),
            self.update_count(),
        )
    }

    /// Move physical segments into another batch. This preserves the producer's
    /// native representation rather than flattening rows into individual
    /// updates.
    pub fn concat(self, other: Self) -> Result<Self, OutputError> {
        let mut segments = self.into_segments();
        segments.extend(other.into_segments());
        Self::from_segments(segments)
    }

    /// Append another physical batch in place. This is used by coalescing
    /// stages so they do not repeatedly clone or expand logical ticks.
    pub(crate) fn append(&mut self, other: Self) -> Result<(), OutputError> {
        if other.is_empty() {
            return Ok(());
        }
        let current = std::mem::replace(
            &mut self.storage,
            OutputBatchStorage::Single(OutputSegment::SingletonTicks(Vec::new())),
        );
        let mut segments = match current {
            OutputBatchStorage::Single(segment) if !segment.is_empty() => vec![segment],
            OutputBatchStorage::Single(_) => Vec::new(),
            OutputBatchStorage::Segments(segments) => segments,
        };
        segments.extend(other.into_segments());
        self.storage = Self::storage_from_segments(&mut segments)?;
        Ok(())
    }

    fn storage_from_segments(
        segments: &mut Vec<OutputSegment<V>>,
    ) -> Result<OutputBatchStorage<V>, OutputError> {
        let mut merged = Vec::with_capacity(segments.len());
        for segment in segments.drain(..) {
            if segment.is_zero_row_packed() {
                continue;
            }
            segment.validate()?;
            if segment.is_empty() {
                continue;
            }
            match (merged.last_mut(), segment) {
                (
                    Some(OutputSegment::SingletonTicks(previous)),
                    OutputSegment::SingletonTicks(mut next),
                ) => previous.append(&mut next),
                (
                    Some(OutputSegment::PackedRows {
                        layout: previous_layout,
                        values: previous_values,
                    }),
                    OutputSegment::PackedRows { layout, values },
                ) if previous_layout == &layout => previous_values.extend(values),
                (_, segment) => merged.push(segment),
            }
        }
        Ok(match merged.len() {
            0 => OutputBatchStorage::Single(OutputSegment::SingletonTicks(Vec::new())),
            1 => OutputBatchStorage::Single(merged.pop().expect("length checked")),
            _ => OutputBatchStorage::Segments(merged),
        })
    }

    fn into_segments(self) -> Vec<OutputSegment<V>> {
        match self.storage {
            OutputBatchStorage::Single(segment) if !segment.is_empty() => vec![segment],
            OutputBatchStorage::Single(_) => Vec::new(),
            OutputBatchStorage::Segments(segments) => segments,
        }
    }

    /// Move logical ticks out of the batch. Packed rows are expanded only at
    /// this ownership boundary; borrowed iteration remains allocation-free.
    pub fn into_ticks(self) -> OwnedOutputTicks<V> {
        let remaining = self.tick_count();
        OwnedOutputTicks::new(self.into_segments(), remaining)
    }

    /// Clone only selected values while preserving each physical segment. This
    /// is used by a branching router; it avoids cloning unrelated values before
    /// selecting a destination.
    pub(crate) fn select_variables_cloned(
        &self,
        variables: &BTreeSet<VarName>,
    ) -> Result<Self, OutputError>
    where
        V: Clone,
    {
        let mut segments = Vec::new();
        for segment in self.segment_cursor() {
            match segment {
                OutputSegment::SingletonTicks(updates) => {
                    let updates = updates
                        .iter()
                        .filter(|update| variables.contains(&update.variable))
                        .cloned()
                        .collect::<Vec<_>>();
                    if !updates.is_empty() {
                        segments.push(OutputSegment::SingletonTicks(updates));
                    }
                }
                OutputSegment::Tick(updates) => {
                    let updates = updates
                        .iter()
                        .filter(|update| variables.contains(&update.variable))
                        .cloned()
                        .collect::<Vec<_>>();
                    if !updates.is_empty() {
                        segments.push(OutputSegment::Tick(updates));
                    }
                }
                OutputSegment::PackedRows { layout, values } => {
                    let selected = layout
                        .variables()
                        .iter()
                        .map(|variable| variables.contains(variable))
                        .collect::<Vec<_>>();
                    let selected_width = selected.iter().filter(|&&keep| keep).count();
                    if selected_width == 0 {
                        continue;
                    }
                    if selected_width == layout.len() {
                        segments.push(OutputSegment::PackedRows {
                            layout: layout.clone(),
                            values: values.clone(),
                        });
                        continue;
                    }
                    let selected_layout = ValidatedLayout::new(
                        layout
                            .variables()
                            .iter()
                            .cloned()
                            .zip(&selected)
                            .filter_map(|(variable, &keep)| keep.then_some(variable)),
                    )
                    .map_err(OutputError::from)?;
                    let width = layout.len();
                    let selected_values = values
                        .iter()
                        .enumerate()
                        .filter_map(|(index, value)| {
                            selected[index % width].then_some(value.clone())
                        })
                        .collect();
                    segments.push(OutputSegment::PackedRows {
                        layout: selected_layout,
                        values: selected_values,
                    });
                }
            }
        }
        Self::from_segments(segments)
    }

    /// Retain only selected variables while preserving packed row storage.
    pub fn select_variables(self, variables: &BTreeSet<VarName>) -> Result<Self, OutputError> {
        let mut segments = Vec::new();
        for segment in self.into_segments() {
            match segment {
                OutputSegment::SingletonTicks(updates) => {
                    let updates = batch::select_updates(updates, variables);
                    if !updates.is_empty() {
                        segments.push(OutputSegment::SingletonTicks(updates));
                    }
                }
                OutputSegment::Tick(updates) => {
                    let updates = batch::select_updates(updates, variables);
                    if !updates.is_empty() {
                        segments.push(OutputSegment::Tick(updates));
                    }
                }
                OutputSegment::PackedRows { layout, values } => {
                    let selected = batch::selected_columns(layout.variables(), variables);
                    let selected_width = selected.iter().filter(|&&keep| keep).count();
                    if selected_width == 0 {
                        continue;
                    }
                    if selected_width == layout.len() {
                        segments.push(OutputSegment::PackedRows { layout, values });
                        continue;
                    }

                    let selected_layout = layout
                        .variables()
                        .iter()
                        .cloned()
                        .zip(&selected)
                        .filter_map(|(variable, &keep)| keep.then_some(variable))
                        .collect::<Vec<_>>();
                    let selected_layout =
                        ValidatedLayout::new(selected_layout).map_err(OutputError::from)?;
                    let values = batch::select_packed_values(values, &selected);
                    segments.push(OutputSegment::PackedRows {
                        layout: selected_layout,
                        values,
                    });
                }
            }
        }
        Self::from_segments(segments)
    }

    pub fn map<U, F>(self, mut map: F) -> OutputBatch<U>
    where
        F: FnMut(V) -> U,
    {
        self.map_update_values(|_, value| map(value))
    }

    pub fn try_map<U, E, F>(self, mut map: F) -> Result<OutputBatch<U>, E>
    where
        F: FnMut(V) -> Result<U, E>,
    {
        self.try_map_update_values(|_, value| map(value))
    }

    pub fn map_values<U, F>(self, mut map: F) -> OutputBatch<U>
    where
        F: FnMut(V) -> U,
    {
        self.map_update_values(|_, value| map(value))
    }

    pub fn map_update_values<U, F>(self, mut map: F) -> OutputBatch<U>
    where
        F: FnMut(&VarName, V) -> U,
    {
        let segments = self
            .into_segments()
            .into_iter()
            .map(|segment| segment.map_values(&mut map))
            .collect::<Vec<_>>();
        OutputBatch::from_segments(segments).expect("mapping preserves valid output shape")
    }

    pub fn try_map_values<U, E, F>(self, mut map: F) -> Result<OutputBatch<U>, E>
    where
        F: FnMut(V) -> Result<U, E>,
    {
        self.try_map_update_values(|_, value| map(value))
    }

    pub fn try_map_update_values<U, E, F>(self, mut map: F) -> Result<OutputBatch<U>, E>
    where
        F: FnMut(&VarName, V) -> Result<U, E>,
    {
        let mut mapped = Vec::new();
        for segment in self.into_segments() {
            let segment = match segment {
                OutputSegment::SingletonTicks(updates) => OutputSegment::SingletonTicks(
                    updates
                        .into_iter()
                        .map(|OutputUpdate { variable, value }| {
                            let value = map(&variable, value)?;
                            Ok(OutputUpdate { variable, value })
                        })
                        .collect::<Result<Vec<_>, E>>()?,
                ),
                OutputSegment::Tick(updates) => OutputSegment::Tick(
                    updates
                        .into_iter()
                        .map(|OutputUpdate { variable, value }| {
                            let value = map(&variable, value)?;
                            Ok(OutputUpdate { variable, value })
                        })
                        .collect::<Result<Vec<_>, E>>()?,
                ),
                OutputSegment::PackedRows { layout, values } => {
                    let width = layout.len();
                    let values = values
                        .into_iter()
                        .enumerate()
                        .map(|(index, value)| map(&layout.variables()[index % width], value))
                        .collect::<Result<Vec<_>, E>>()?;
                    OutputSegment::PackedRows { layout, values }
                }
            };
            mapped.push(segment);
        }
        Ok(OutputBatch::from_segments(mapped).expect("mapping preserves valid output shape"))
    }
}

impl<V> From<OutputUpdate<V>> for OutputBatch<V> {
    fn from(update: OutputUpdate<V>) -> Self {
        Self::update(update.variable, update.value)
    }
}

fn validate_tick<V>(updates: &[OutputUpdate<V>]) -> Result<(), OutputError> {
    if updates.is_empty() {
        return Err(OutputError::invalid(
            "output tick must contain at least one update",
        ));
    }
    let mut seen = HashSet::with_capacity(updates.len());
    for update in updates {
        if !seen.insert(&update.variable) {
            return Err(OutputError::invalid(format!(
                "output tick contains duplicate variable `{}`",
                update.variable
            )));
        }
    }
    Ok(())
}

fn validate_packed_rows(layout: &ValidatedLayout, value_count: usize) -> Result<(), OutputError> {
    if layout.is_empty() {
        return Err(OutputError::invalid(
            "output packed layouts must contain at least one variable",
        ));
    }
    if value_count == 0 {
        return Err(OutputError::invalid(
            "packed output must contain at least one value; use OutputBatch::empty() for empty output",
        ));
    }
    if !value_count.is_multiple_of(layout.len()) {
        return Err(OutputError::invalid(format!(
            "packed output contains {value_count} values, which is not divisible by layout width {}",
            layout.len()
        )));
    }
    Ok(())
}

pub type OutputTick<'a, V> = batch::Tick<'a, V>;

pub struct OutputTicks<'a, V>(batch::Ticks<'a, OutputSegment<V>, V>);
impl<'a, V> OutputTicks<'a, V> {
    fn new(segments: batch::SegmentCursor<'a, OutputSegment<V>>, remaining: usize) -> Self {
        Self(batch::Ticks::new(segments, remaining))
    }
}
impl<'a, V> Iterator for OutputTicks<'a, V> {
    type Item = OutputTick<'a, V>;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}
impl<V> ExactSizeIterator for OutputTicks<'_, V> {}

pub struct OutputUpdates<'a, V>(batch::Updates<'a, OutputSegment<V>, V>);
impl<'a, V> OutputUpdates<'a, V> {
    fn new(
        segments: batch::SegmentCursor<'a, OutputSegment<V>>,
        tick_count: usize,
        update_count: usize,
    ) -> Self {
        Self(batch::Updates::new(segments, tick_count, update_count))
    }
}
impl<'a, V> Iterator for OutputUpdates<'a, V> {
    type Item = OutputUpdateRef<'a, V>;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}
impl<V> ExactSizeIterator for OutputUpdates<'_, V> {}

pub struct OwnedOutputTicks<V>(batch::OwnedTicks<OutputSegment<V>, V>);
impl<V> OwnedOutputTicks<V> {
    fn new(segments: Vec<OutputSegment<V>>, remaining: usize) -> Self {
        Self(batch::OwnedTicks::new(segments, remaining))
    }
}
impl<V> Iterator for OwnedOutputTicks<V> {
    type Item = Vec<OutputUpdate<V>>;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}
impl<V> ExactSizeIterator for OwnedOutputTicks<V> {}

impl<V> IntoIterator for OutputBatch<V> {
    type Item = Vec<OutputUpdate<V>>;
    type IntoIter = OwnedOutputTicks<V>;

    fn into_iter(self) -> Self::IntoIter {
        self.into_ticks()
    }
}

impl<'a, V> IntoIterator for &'a OutputBatch<V> {
    type Item = OutputTick<'a, V>;
    type IntoIter = OutputTicks<'a, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.ticks()
    }
}

/// The role of a routed output variable.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum OutputRole {
    #[default]
    Output,
    Auxiliary,
}

impl OutputRole {
    pub fn is_auxiliary(self) -> bool {
        matches!(self, Self::Auxiliary)
    }
}

impl fmt::Display for OutputRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Output => "output",
            Self::Auxiliary => "auxiliary",
        })
    }
}

/// A variable and its role in an output interface.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OutputRoute {
    pub variable: VarName,
    pub topic: Option<String>,
    pub message_type: Option<String>,
    pub role: OutputRole,
}

impl OutputRoute {
    pub fn new(
        variable: VarName,
        topic: Option<String>,
        message_type: Option<String>,
        role: OutputRole,
    ) -> Self {
        Self {
            variable,
            topic,
            message_type,
            role,
        }
    }

    pub fn output(variable: VarName) -> Self {
        Self::new(variable, None, None, OutputRole::Output)
    }

    pub fn auxiliary(variable: VarName) -> Self {
        Self::new(variable, None, None, OutputRole::Auxiliary)
    }

    pub fn from_role(role: OutputRole, variable: VarName) -> Self {
        Self::new(variable, None, None, role)
    }

    pub fn variable(&self) -> &VarName {
        &self.variable
    }

    pub fn validate(&self) -> Result<(), OutputError> {
        if self.variable.name().is_empty() {
            return Err(OutputError::invalid(
                "output route variable cannot be empty",
            ));
        }
        Ok(())
    }
}

/// The validated, shared set of variables available to an output backend.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OutputInterface {
    /// Resolved, fixed routes shared by every writer opened for this interface.
    routes: Arc<[OutputRoute]>,
    indexes: Arc<BTreeMap<VarName, usize>>,
}

impl OutputInterface {
    pub fn new(routes: impl Into<Arc<[OutputRoute]>>) -> Result<Self, OutputError> {
        let routes = routes.into();
        Self::validate_routes(&routes)?;
        let indexes = routes
            .iter()
            .enumerate()
            .map(|(index, route)| (route.variable.clone(), index))
            .collect();
        Ok(Self {
            routes,
            indexes: Arc::new(indexes),
        })
    }

    pub fn from_routes<R>(routes: R) -> Result<Self, OutputError>
    where
        R: IntoIterator<Item = OutputRoute>,
    {
        Self::new(routes.into_iter().collect::<Vec<_>>())
    }

    pub fn empty() -> Self {
        Self::default()
    }

    /// Build an interface in which every variable is a published output route.
    pub fn outputs<I>(variables: I) -> Result<Self, OutputError>
    where
        I: IntoIterator<Item = VarName>,
    {
        Self::from_routes(variables.into_iter().map(OutputRoute::output))
    }

    pub fn validate_routes(routes: &[OutputRoute]) -> Result<(), OutputError> {
        let mut seen = HashSet::with_capacity(routes.len());
        for route in routes {
            route.validate()?;
            if !seen.insert(&route.variable) {
                return Err(OutputError::invalid(format!(
                    "output route contains duplicate variable `{}`",
                    route.variable
                )));
            }
        }
        Ok(())
    }

    pub fn routes(&self) -> &[OutputRoute] {
        &self.routes
    }

    pub fn shared_routes(&self) -> Arc<[OutputRoute]> {
        Arc::clone(&self.routes)
    }

    pub fn len(&self) -> usize {
        self.routes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }

    pub fn route(&self, variable: &VarName) -> Option<&OutputRoute> {
        self.indexes
            .get(variable)
            .and_then(|index| self.routes.get(*index))
    }

    pub fn contains(&self, variable: &VarName) -> bool {
        self.route(variable).is_some()
    }

    pub fn routes_for(&self, role: OutputRole) -> impl Iterator<Item = &OutputRoute> {
        self.routes.iter().filter(move |route| route.role == role)
    }

    pub fn validate_batch<V>(&self, batch: &OutputBatch<V>) -> Result<(), OutputError> {
        batch.validate()?;
        for update in batch.updates() {
            if !self.contains(update.variable) {
                return Err(OutputError::invalid(format!(
                    "output update variable `{}` has no route",
                    update.variable
                )));
            }
        }
        Ok(())
    }
}

/// An optional asynchronous interface update operation owned by an opened writer.
///
/// The handle is deliberately separate from the data sink so wrappers can retain
/// it while the backend keeps ownership of its sender or transport resource.
#[derive(Clone)]
pub struct OutputInterfaceReconfigurationHandle {
    operation: Rc<dyn Fn(OutputInterface) -> LocalBoxFuture<'static, Result<(), OutputError>>>,
}

impl OutputInterfaceReconfigurationHandle {
    pub(crate) fn new<F>(operation: F) -> Self
    where
        F: Fn(OutputInterface) -> LocalBoxFuture<'static, Result<(), OutputError>> + 'static,
    {
        Self {
            operation: Rc::new(operation),
        }
    }

    pub async fn reconfigure(&self, interface: OutputInterface) -> Result<(), OutputError> {
        (self.operation)(interface).await
    }
}

/// A local, dynamically dispatched sink for output batches.
pub type DynOutputSink<V> = Pin<Box<dyn Sink<OutputBatch<V>, Error = OutputError>>>;

#[derive(Clone, Debug)]
enum OutputCloseState {
    Open,
    Closing,
    Closed(OutputError),
}

/// A sticky, local output sink wrapper.
///
/// The first operation error is retained for all later data operations. Close
/// is different: it is cleanup, so it is still forwarded to the wrapped sink
/// after an operation failure. The wrapped close is finalized only once; if it
/// also fails, its error is attached to the retained primary error.
pub struct OutputWriter<V> {
    sink: DynOutputSink<V>,
    interface_reconfiguration: Option<OutputInterfaceReconfigurationHandle>,
    primary_error: Option<OutputError>,
    close_state: OutputCloseState,
}

impl<V> Unpin for OutputWriter<V> {}

impl<V> OutputWriter<V> {
    pub fn new(sink: DynOutputSink<V>) -> Self {
        Self {
            sink,
            interface_reconfiguration: None,
            primary_error: None,
            close_state: OutputCloseState::Open,
        }
    }

    pub fn from_sink<S>(sink: S) -> Self
    where
        S: Sink<OutputBatch<V>, Error = OutputError> + 'static,
    {
        Self::new(Box::pin(sink))
    }

    pub(crate) fn from_sink_with_interface_reconfiguration<S>(
        sink: S,
        interface_reconfiguration: Option<OutputInterfaceReconfigurationHandle>,
    ) -> Self
    where
        V: 'static,
        S: Sink<OutputBatch<V>, Error = OutputError> + 'static,
    {
        Self {
            sink: Box::pin(sink),
            interface_reconfiguration,
            primary_error: None,
            close_state: OutputCloseState::Open,
        }
    }

    pub(crate) fn interface_reconfiguration(&self) -> Option<OutputInterfaceReconfigurationHandle> {
        self.interface_reconfiguration.clone()
    }

    pub fn is_closed(&self) -> bool {
        matches!(self.close_state, OutputCloseState::Closed(_))
    }

    pub fn is_failed(&self) -> bool {
        self.primary_error.is_some()
            || matches!(
                &self.close_state,
                OutputCloseState::Closed(error) if !error.is_closed()
            )
    }

    pub fn error(&self) -> Option<&OutputError> {
        if let Some(error) = &self.primary_error {
            return Some(error);
        }
        match &self.close_state {
            OutputCloseState::Closed(error) => Some(error),
            OutputCloseState::Open | OutputCloseState::Closing => None,
        }
    }

    pub fn into_sink(self) -> DynOutputSink<V> {
        self.sink
    }

    pub async fn feed(&mut self, batch: OutputBatch<V>) -> Result<(), OutputError> {
        SinkExt::feed(self, batch).await
    }

    /// Submit a batch without forcing a downstream flush. This is the
    /// runtime-facing operation so buffering and coalescing stages can observe
    /// multiple physical submissions. Call [`Self::flush`] for a completion
    /// barrier.
    pub async fn send(&mut self, batch: OutputBatch<V>) -> Result<(), OutputError> {
        SinkExt::feed(self, batch).await?;
        // `feed` transfers ownership without polling the operation started by
        // `start_send`. Drive the sink to its next ready point so direct async
        // backends (notably the manual receiver) can make progress, while
        // deliberately avoiding `poll_flush`, which would defeat coalescing.
        match futures::future::poll_fn(|context| self.sink.as_mut().poll_ready(context)).await {
            Ok(()) => Ok(()),
            Err(error) if error.is_closed() => Ok(()),
            Err(error) => Err(self.retain_error(error)),
        }
    }

    /// Submit a batch and wait for the downstream sink to flush it.
    pub async fn send_and_flush(&mut self, batch: OutputBatch<V>) -> Result<(), OutputError> {
        SinkExt::send(self, batch).await
    }

    pub async fn flush(&mut self) -> Result<(), OutputError> {
        SinkExt::flush(self).await
    }

    pub async fn close(&mut self) -> Result<(), OutputError> {
        SinkExt::close(self).await
    }

    fn operation_error(&self) -> Option<OutputError> {
        if let Some(error) = &self.primary_error {
            return Some(error.clone());
        }
        match &self.close_state {
            OutputCloseState::Open => None,
            OutputCloseState::Closing => Some(OutputError::Closed),
            OutputCloseState::Closed(error) => Some(error.clone()),
        }
    }

    fn retain_error(&mut self, error: OutputError) -> OutputError {
        if self.primary_error.is_none() {
            self.primary_error = Some(error);
        }
        self.primary_error
            .as_ref()
            .expect("a retained output error exists")
            .clone()
    }

    fn finish_close(&mut self, cleanup: Result<(), OutputError>) -> Result<(), OutputError> {
        let result = match cleanup {
            Ok(()) => self.primary_error.clone().map_or(Ok(()), Err),
            Err(cleanup_error) => Err(match self.primary_error.clone() {
                Some(primary) => combine_errors(primary, cleanup_error),
                None => cleanup_error,
            }),
        };
        let closed_error = result.clone().err().unwrap_or(OutputError::Closed);
        self.close_state = OutputCloseState::Closed(closed_error);
        result
    }
}

impl<V> Sink<OutputBatch<V>> for OutputWriter<V> {
    type Error = OutputError;

    fn poll_ready(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if let Some(error) = this.operation_error() {
            return Poll::Ready(Err(error));
        }

        match this.sink.as_mut().poll_ready(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(error)) => Poll::Ready(Err(this.retain_error(error))),
        }
    }

    fn start_send(self: Pin<&mut Self>, batch: OutputBatch<V>) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if let Some(error) = this.operation_error() {
            return Err(error);
        }
        if let Err(error) = batch.validate() {
            return Err(this.retain_error(error));
        }

        match this.sink.as_mut().start_send(batch) {
            Ok(()) => Ok(()),
            Err(error) => Err(this.retain_error(error)),
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if let Some(error) = this.operation_error() {
            return Poll::Ready(Err(error));
        }

        match this.sink.as_mut().poll_flush(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(error)) => Poll::Ready(Err(this.retain_error(error))),
        }
    }

    fn poll_close(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        match &this.close_state {
            OutputCloseState::Closed(error) => return Poll::Ready(Err(error.clone())),
            OutputCloseState::Open => this.close_state = OutputCloseState::Closing,
            OutputCloseState::Closing => {}
        }

        match this.sink.as_mut().poll_close(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => Poll::Ready(this.finish_close(result)),
        }
    }
}

fn combine_errors(primary: OutputError, cleanup: OutputError) -> OutputError {
    if primary == cleanup {
        return primary;
    }

    match primary {
        OutputError::Backend(message) => {
            OutputError::Backend(format!("{message}; additionally: {cleanup}"))
        }
        OutputError::Source(message) => {
            OutputError::Source(format!("{message}; additionally: {cleanup}"))
        }
        OutputError::Invalid(message) => {
            OutputError::Invalid(format!("{message}; additionally: {cleanup}"))
        }
        OutputError::Closed => OutputError::Backend(format!("{primary}; additionally: {cleanup}")),
    }
}

/// A local backend that opens one sink for a validated output interface.
#[async_trait(?Send)]
pub trait OutputBackend {
    type Val: StreamData;

    async fn open(
        &self,
        interface: OutputInterface,
    ) -> Result<OutputWriter<Self::Val>, OutputError>;
}

/// A cheaply clonable handle to a local output backend.
pub type SharedOutputBackend<V> = Rc<dyn OutputBackend<Val = V>>;

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        pin::Pin,
        rc::Rc,
        task::{Context, Poll},
    };

    use futures::{Sink, task::noop_waker_ref};

    use super::*;

    fn var(name: &str) -> VarName {
        VarName::new(name)
    }

    fn update(name: &str, value: i32) -> OutputUpdate<i32> {
        OutputUpdate::new(var(name), value)
    }

    #[test]
    fn logical_ticks_cover_singleton_atomic_packed_and_mixed_storage() {
        let packed =
            OutputBatch::packed_rows([var("output_y"), var("output_z")], [2, 3, 4, 5]).unwrap();
        let mixed = OutputBatch::from_segments([
            OutputSegment::SingletonTicks(vec![update("output_x", 1)]),
            OutputSegment::Tick(vec![update("output_a", 6), update("output_b", 7)]),
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
                vec![update("output_x", 1)],
                vec![update("output_a", 6), update("output_b", 7)],
                vec![update("output_y", 2), update("output_z", 3)],
                vec![update("output_y", 4), update("output_z", 5)],
            ]
        );
        assert_eq!(mixed.tick_count(), 4);
        assert_eq!(mixed.update_count(), 7);
        assert_eq!(mixed.segment_count(), 3);
    }

    #[test]
    fn constructors_reject_invalid_ticks_and_layouts() {
        assert!(OutputBatch::tick(Vec::<OutputUpdate<i32>>::new()).is_err());
        assert!(OutputBatch::tick(vec![update("output_x", 1), update("output_x", 2)]).is_err());
        assert!(OutputBatch::packed_rows([var("output_x"), var("output_y")], [1]).is_err());
        assert!(OutputBatch::packed_rows([var("output_x"), var("output_x")], [1, 2]).is_err());
        assert!(OutputBatch::packed_rows([], [1]).is_err());
        assert!(OutputBatch::<i32>::packed_rows([], []).is_err());

        let error = OutputBatch::<i32>::packed_rows([var("output_x")], [])
            .expect_err("zero-row packed output should be rejected");
        assert_eq!(
            error,
            OutputError::Invalid(
                "packed output must contain at least one value; use OutputBatch::empty() for empty output"
                    .to_owned()
            )
        );
    }

    #[test]
    fn empty_physical_segments_use_the_canonical_empty_batch() {
        let empty_packed = OutputSegment::PackedRows {
            layout: ValidatedLayout::new([var("output_x")]).unwrap(),
            values: Vec::<i32>::new(),
        };
        let empty = OutputBatch::from_segments([empty_packed]).unwrap();
        assert_eq!(empty, OutputBatch::empty());

        let selected = OutputBatch::packed_rows([var("output_x")], [1, 2])
            .unwrap()
            .select_variables(&BTreeSet::new())
            .unwrap();
        assert_eq!(selected, OutputBatch::empty());
    }

    #[test]
    fn borrowed_updates_and_owned_ticks_preserve_boundaries() {
        let batch = OutputBatch::from_ticks(vec![
            vec![update("output_x", 1)],
            vec![update("output_x", 2), update("output_y", 3)],
        ])
        .unwrap();

        let mut updates = batch.updates();
        assert_eq!(updates.len(), 3);
        assert_eq!(updates.by_ref().count(), 3);
        assert_eq!(updates.len(), 0);

        assert_eq!(
            batch.into_ticks().collect::<Vec<_>>(),
            vec![
                vec![update("output_x", 1)],
                vec![update("output_x", 2), update("output_y", 3)]
            ]
        );
    }

    #[test]
    fn mapping_selection_and_concatenation_preserve_packed_storage() {
        let packed = OutputBatch::packed_rows(
            [var("output_x"), var("output_y"), var("output_z")],
            [1, 2, 3, 4, 5, 6],
        )
        .unwrap();
        let selected = packed
            .select_variables(&[var("output_x"), var("output_z")].into_iter().collect())
            .unwrap();
        assert!(matches!(
            &selected.storage,
            OutputBatchStorage::Single(OutputSegment::PackedRows { .. })
        ));
        assert_eq!(
            selected
                .updates()
                .map(|update| (update.variable.name(), *update.value))
                .collect::<Vec<_>>(),
            [
                ("output_x".into(), 1),
                ("output_z".into(), 3),
                ("output_x".into(), 4),
                ("output_z".into(), 6),
            ]
        );

        let mapped =
            selected.map_update_values(|variable, value| format!("{}={value}", variable.name()));
        let combined = OutputBatch::update(var("output_before"), "before".to_owned())
            .concat(mapped)
            .unwrap();
        assert_eq!(combined.tick_count(), 3);
        assert_eq!(combined.update_count(), 5);
    }

    #[test]
    fn interface_validates_updates_without_expanding_storage() {
        let interface = OutputInterface::outputs([var("output_x")]).unwrap();
        let valid = OutputBatch::packed_rows([var("output_x")], [1, 2]).unwrap();
        interface.validate_batch(&valid).unwrap();

        let invalid = OutputBatch::update(var("output_missing"), 1);
        let error = interface.validate_batch(&invalid).unwrap_err();
        assert!(error.to_string().contains("output update variable"));
    }

    struct CountingSink {
        sends: usize,
        closes: Rc<Cell<usize>>,
        send_error: Option<OutputError>,
        close_error: Option<OutputError>,
    }

    impl Sink<OutputBatch<i32>> for CountingSink {
        type Error = OutputError;

        fn poll_ready(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, _batch: OutputBatch<i32>) -> Result<(), Self::Error> {
            let this = self.get_mut();
            this.sends += 1;
            match this.send_error.take() {
                Some(error) => Err(error),
                None => Ok(()),
            }
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            let this = self.get_mut();
            this.closes.set(this.closes.get() + 1);
            Poll::Ready(match this.close_error.take() {
                Some(error) => Err(error),
                None => Ok(()),
            })
        }
    }

    fn counting_writer(
        send_error: Option<OutputError>,
        close_error: Option<OutputError>,
    ) -> (OutputWriter<i32>, Rc<Cell<usize>>) {
        let closes = Rc::new(Cell::new(0));
        let sink = CountingSink {
            sends: 0,
            closes: Rc::clone(&closes),
            send_error,
            close_error,
        };
        (OutputWriter::from_sink(sink), closes)
    }

    #[test]
    fn writer_retains_primary_error_but_still_closes_once() {
        let (mut writer, closes) = counting_writer(Some(OutputError::backend("send failed")), None);
        let batch = OutputBatch::update(var("output_writer"), 1);

        smol::block_on(async {
            let primary = writer.send(batch.clone()).await.unwrap_err();
            assert_eq!(primary, OutputError::Backend("send failed".into()));
            assert_eq!(writer.send(batch.clone()).await.unwrap_err(), primary);
            assert_eq!(writer.close().await.unwrap_err(), primary);
            assert_eq!(writer.close().await.unwrap_err(), primary);
            assert_eq!(writer.send(batch).await.unwrap_err(), primary);
        });

        assert_eq!(closes.get(), 1);
        assert!(writer.is_closed());
        assert!(writer.is_failed());
    }

    #[test]
    fn writer_attaches_cleanup_error_without_replacing_primary() {
        let (mut writer, closes) = counting_writer(
            Some(OutputError::backend("send failed")),
            Some(OutputError::backend("close failed")),
        );
        let primary =
            smol::block_on(writer.send(OutputBatch::update(var("output_writer"), 1))).unwrap_err();
        let result = smol::block_on(writer.close()).unwrap_err();

        assert_eq!(primary, OutputError::Backend("send failed".into()));
        assert!(result.is_backend());
        assert!(result.to_string().contains("send failed"));
        assert!(result.to_string().contains("close failed"));
        assert_eq!(closes.get(), 1);
        assert_eq!(smol::block_on(writer.close()).unwrap_err(), result);
    }

    #[test]
    fn writer_sink_impl_accepts_ready_batches() {
        let (mut writer, _) = counting_writer(None, None);
        let mut writer = Pin::new(&mut writer);
        assert!(matches!(
            writer
                .as_mut()
                .poll_ready(&mut Context::from_waker(noop_waker_ref())),
            Poll::Ready(Ok(()))
        ));
    }
}
