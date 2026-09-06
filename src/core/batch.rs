use std::{slice, vec};

use super::VarName;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Update<V> {
    pub variable: VarName,
    pub value: V,
}
impl<V> Update<V> {
    pub fn new(variable: VarName, value: V) -> Self {
        Self { variable, value }
    }
}

pub(crate) fn map_updates<V, U>(
    updates: Vec<Update<V>>,
    map: &mut impl FnMut(&VarName, V) -> U,
) -> Vec<Update<U>> {
    updates
        .into_iter()
        .map(|Update { variable, value }| {
            let value = map(&variable, value);
            Update { variable, value }
        })
        .collect()
}

pub(crate) fn map_packed<V, U>(
    layout: &[VarName],
    values: Vec<V>,
    map: &mut impl FnMut(&VarName, V) -> U,
) -> Vec<U> {
    let width = layout.len();
    values
        .into_iter()
        .enumerate()
        .map(|(index, value)| map(&layout[index % width], value))
        .collect()
}

pub(crate) fn selected_columns(
    layout: &[VarName],
    variables: &std::collections::BTreeSet<VarName>,
) -> Vec<bool> {
    layout
        .iter()
        .map(|variable| variables.contains(variable))
        .collect()
}

pub(crate) fn select_updates<V>(
    updates: Vec<Update<V>>,
    variables: &std::collections::BTreeSet<VarName>,
) -> Vec<Update<V>> {
    updates
        .into_iter()
        .filter(|update| variables.contains(&update.variable))
        .collect()
}

pub(crate) fn select_packed_values<V>(values: Vec<V>, selected: &[bool]) -> Vec<V> {
    let width = selected.len();
    values
        .into_iter()
        .enumerate()
        .filter_map(|(index, value)| selected[index % width].then_some(value))
        .collect()
}

pub(crate) trait SegmentAccess<V>: Sized {
    fn view(&self) -> SegmentView<'_, V>;
    fn into_owned(self) -> OwnedSegment<V>;
}

pub(crate) enum SegmentView<'a, V> {
    Singleton(&'a [Update<V>]),
    Tick(&'a [Update<V>]),
    Packed {
        layout: &'a [VarName],
        values: &'a [V],
    },
}

pub(crate) enum OwnedSegment<V> {
    Singleton(vec::IntoIter<Update<V>>),
    Tick(Option<Vec<Update<V>>>),
    Packed {
        layout: Vec<VarName>,
        values: vec::IntoIter<V>,
        width: usize,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Storage<S> {
    Single(S),
    Segments(Vec<S>),
}

impl<S> Storage<S> {
    #[inline(always)]
    pub(crate) fn cursor(&self) -> SegmentCursor<'_, S> {
        match self {
            Self::Single(segment) => SegmentCursor::Single(Some(segment)),
            Self::Segments(segments) => SegmentCursor::Slice(segments.iter()),
        }
    }
}

pub(crate) fn normalize<S, E>(
    segments: impl IntoIterator<Item = S>,
    empty: impl FnOnce() -> S,
    mut validate: impl FnMut(&S) -> Result<(), E>,
    mut is_empty: impl FnMut(&S) -> bool,
    mut merge: impl FnMut(&mut S, S) -> Result<Option<S>, E>,
) -> Result<Storage<S>, E> {
    let mut first = None;
    let mut multiple = Vec::new();
    for segment in segments {
        validate(&segment)?;
        if is_empty(&segment) {
            continue;
        }
        if let Some(previous) = multiple.last_mut() {
            if let Some(segment) = merge(previous, segment)? {
                multiple.push(segment);
            }
        } else if let Some(previous) = first.as_mut() {
            if let Some(segment) = merge(previous, segment)? {
                multiple.push(first.take().expect("first segment is present"));
                multiple.push(segment);
            }
        } else {
            first = Some(segment);
        }
    }
    Ok(if multiple.is_empty() {
        Storage::Single(first.unwrap_or_else(empty))
    } else {
        Storage::Segments(multiple)
    })
}

pub(crate) enum SegmentCursor<'a, S> {
    Single(Option<&'a S>),
    Slice(slice::Iter<'a, S>),
}

impl<'a, S> Iterator for SegmentCursor<'a, S> {
    type Item = &'a S;
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Single(segment) => segment.take(),
            Self::Slice(segments) => segments.next(),
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = match self {
            Self::Single(segment) => usize::from(segment.is_some()),
            Self::Slice(segments) => segments.len(),
        };
        (len, Some(len))
    }
}
impl<S> ExactSizeIterator for SegmentCursor<'_, S> {}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct UpdateRef<'a, V> {
    pub variable: &'a VarName,
    pub value: &'a V,
}

#[derive(Clone, Copy, Debug)]
enum TickRepresentation<'a, V> {
    Slice(&'a [Update<V>]),
    Packed {
        layout: &'a [VarName],
        values: &'a [V],
    },
}

#[derive(Clone, Copy, Debug)]
pub struct Tick<'a, V> {
    representation: TickRepresentation<'a, V>,
}
impl<'a, V> Tick<'a, V> {
    pub fn len(&self) -> usize {
        match self.representation {
            TickRepresentation::Slice(v) => v.len(),
            TickRepresentation::Packed { layout, .. } => layout.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[inline(always)]
    pub fn updates(&self) -> TickUpdates<'a, V> {
        match self.representation {
            TickRepresentation::Slice(updates) => TickUpdates::Slice(updates.iter()),
            TickRepresentation::Packed { layout, values } => {
                TickUpdates::Packed(layout.iter().zip(values.iter()))
            }
        }
    }
    pub fn iter(&self) -> TickUpdates<'a, V> {
        self.updates()
    }
    pub fn to_updates(&self) -> Vec<Update<V>>
    where
        V: Clone,
    {
        self.updates()
            .map(|update| Update::new(update.variable.clone(), update.value.clone()))
            .collect()
    }
}

pub enum TickUpdates<'a, V> {
    Slice(slice::Iter<'a, Update<V>>),
    Packed(std::iter::Zip<slice::Iter<'a, VarName>, slice::Iter<'a, V>>),
}
impl<'a, V> Iterator for TickUpdates<'a, V> {
    type Item = UpdateRef<'a, V>;
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let (variable, value) = match self {
            Self::Slice(v) => v.next().map(|v| (&v.variable, &v.value))?,
            Self::Packed(v) => v.next()?,
        };
        Some(UpdateRef { variable, value })
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = match self {
            Self::Slice(v) => v.len(),
            Self::Packed(v) => v.len(),
        };
        (n, Some(n))
    }
}
impl<V> ExactSizeIterator for TickUpdates<'_, V> {}

enum SegmentTicks<'a, V> {
    Singleton(slice::Iter<'a, Update<V>>),
    Tick(Option<&'a [Update<V>]>),
    Packed {
        layout: &'a [VarName],
        rows: slice::ChunksExact<'a, V>,
    },
}
impl<'a, V> SegmentTicks<'a, V> {
    #[inline(always)]
    fn new<S: SegmentAccess<V>>(s: &'a S) -> Self {
        match s.view() {
            SegmentView::Singleton(v) => Self::Singleton(v.iter()),
            SegmentView::Tick(v) => Self::Tick(Some(v)),
            SegmentView::Packed { layout, values } => Self::Packed {
                layout,
                rows: values.chunks_exact(layout.len()),
            },
        }
    }
    #[inline(always)]
    fn next(&mut self) -> Option<Tick<'a, V>> {
        let representation = match self {
            Self::Singleton(v) => TickRepresentation::Slice(slice::from_ref(v.next()?)),
            Self::Tick(v) => TickRepresentation::Slice(v.take()?),
            Self::Packed { layout, rows } => TickRepresentation::Packed {
                layout,
                values: rows.next()?,
            },
        };
        Some(Tick { representation })
    }
}

pub struct Ticks<'a, S, V> {
    segments: SegmentCursor<'a, S>,
    current: Option<SegmentTicks<'a, V>>,
    remaining: usize,
}
impl<'a, S: SegmentAccess<V>, V> Ticks<'a, S, V> {
    #[inline(always)]
    pub(crate) fn new(segments: SegmentCursor<'a, S>, remaining: usize) -> Self {
        Self {
            segments,
            current: None,
            remaining,
        }
    }
}
impl<'a, S: SegmentAccess<V>, V> Iterator for Ticks<'a, S, V> {
    type Item = Tick<'a, V>;
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(t) = self.current.as_mut().and_then(SegmentTicks::next) {
                self.remaining -= 1;
                return Some(t);
            }
            self.current = Some(SegmentTicks::new(self.segments.next()?));
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}
impl<S: SegmentAccess<V>, V> ExactSizeIterator for Ticks<'_, S, V> {}

struct PackedUpdates<'a, V> {
    layout: &'a [VarName],
    values: slice::Iter<'a, V>,
    offset: usize,
}

enum SegmentUpdates<'a, V> {
    Slice(slice::Iter<'a, Update<V>>),
    Packed(PackedUpdates<'a, V>),
    Empty,
}

impl<'a, V> SegmentUpdates<'a, V> {
    #[inline(always)]
    fn new<S: SegmentAccess<V>>(segment: &'a S) -> Self {
        match segment.view() {
            SegmentView::Singleton(updates) | SegmentView::Tick(updates) => {
                Self::Slice(updates.iter())
            }
            SegmentView::Packed { layout, .. } if layout.is_empty() => Self::Empty,
            SegmentView::Packed { layout, values } => Self::Packed(PackedUpdates {
                layout,
                values: values.iter(),
                offset: 0,
            }),
        }
    }

    #[inline(always)]
    fn next(&mut self) -> Option<UpdateRef<'a, V>> {
        match self {
            Self::Slice(updates) => updates.next().map(|update| UpdateRef {
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
                Some(UpdateRef { variable, value })
            }
            Self::Empty => None,
        }
    }
}

pub struct Updates<'a, S, V> {
    segments: SegmentCursor<'a, S>,
    current: Option<SegmentUpdates<'a, V>>,
    remaining: usize,
}
impl<'a, S: SegmentAccess<V>, V> Updates<'a, S, V> {
    #[inline(always)]
    pub(crate) fn new(segments: SegmentCursor<'a, S>, remaining: usize) -> Self {
        Self {
            segments,
            current: None,
            remaining,
        }
    }
}
impl<'a, S: SegmentAccess<V>, V> Iterator for Updates<'a, S, V> {
    type Item = UpdateRef<'a, V>;
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(u) = self.current.as_mut().and_then(SegmentUpdates::next) {
                self.remaining -= 1;
                return Some(u);
            }
            self.current = Some(SegmentUpdates::new(self.segments.next()?));
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}
impl<S: SegmentAccess<V>, V> ExactSizeIterator for Updates<'_, S, V> {}

pub struct OwnedTicks<S, V> {
    segments: vec::IntoIter<S>,
    current: Option<OwnedSegment<V>>,
    remaining: usize,
}
impl<S: SegmentAccess<V>, V> OwnedTicks<S, V> {
    pub(crate) fn new(segments: Vec<S>, remaining: usize) -> Self {
        Self {
            segments: segments.into_iter(),
            current: None,
            remaining,
        }
    }
}
impl<S: SegmentAccess<V>, V> Iterator for OwnedTicks<S, V> {
    type Item = Vec<Update<V>>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let next = match self.current.as_mut() {
                Some(OwnedSegment::Singleton(v)) => v.next().map(|u| vec![u]),
                Some(OwnedSegment::Tick(v)) => v.take(),
                Some(OwnedSegment::Packed {
                    layout,
                    values,
                    width,
                }) => {
                    let vars = layout.iter().cloned();
                    let vals = values.by_ref().take(*width);
                    let row = vars
                        .zip(vals)
                        .map(|(variable, value)| Update { variable, value })
                        .collect::<Vec<_>>();
                    (!row.is_empty()).then_some(row)
                }
                None => None,
            };
            if let Some(t) = next {
                self.remaining -= 1;
                return Some(t);
            }
            self.current = Some(self.segments.next()?.into_owned());
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}
impl<S: SegmentAccess<V>, V> ExactSizeIterator for OwnedTicks<S, V> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_merges_nonempty_segments_across_empty_segments() {
        let storage = normalize(
            [1, 0, 2],
            || 0,
            |_| Ok::<_, ()>(()),
            |segment| *segment == 0,
            |previous, segment| {
                *previous += segment;
                Ok(None)
            },
        )
        .unwrap();

        assert_eq!(storage, Storage::Single(3));
    }

    #[test]
    fn normalize_validates_later_segments_after_empty_segments() {
        let result = normalize(
            [1, 0, -1],
            || 0,
            |segment| (*segment >= 0).then_some(()).ok_or("negative segment"),
            |segment| *segment == 0,
            |_, segment| Ok(Some(segment)),
        );

        assert_eq!(result, Err("negative segment"));
    }
}
