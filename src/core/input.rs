use std::{collections::HashSet, num::NonZeroUsize};

use super::{OutputStream, VarName};

#[derive(Debug, PartialEq)]
pub struct InputEvent<Val> {
    pub var: VarName,
    pub value: Val,
}

impl<Val> InputEvent<Val> {
    #[inline]
    pub fn new(var: VarName, value: Val) -> Self {
        Self { var, value }
    }
}

/// A stream of batched inputs
pub type InputStream<Val> = OutputStream<anyhow::Result<InputBatch<Val>>>;
pub(crate) type InputTickStream<Val> = OutputStream<anyhow::Result<Vec<InputEvent<Val>>>>;

/// A borrowed logical input tick, independent of its transport representation.
#[derive(Clone, Copy, Debug)]
pub struct InputTick<'a, V> {
    representation: InputTickRepresentation<'a, V>,
}

#[derive(Clone, Copy, Debug)]
enum InputTickRepresentation<'a, V> {
    Events(&'a [InputEvent<V>]),
    PackedRow {
        layout: &'a [VarName],
        values: &'a [V],
    },
}

/// A borrowed input event produced while iterating an [`InputTick`].
#[derive(Clone, Copy, Debug)]
pub struct InputEventRef<'a, V> {
    pub var: &'a VarName,
    pub value: &'a V,
}

enum InputTickIter<'a, V> {
    Events(std::slice::Iter<'a, InputEvent<V>>),
    Packed(std::iter::Zip<std::slice::Iter<'a, VarName>, std::slice::Iter<'a, V>>),
}

impl<'a, V> Iterator for InputTickIter<'a, V> {
    type Item = InputEventRef<'a, V>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Events(events) => events.next().map(|event| InputEventRef {
                var: &event.var,
                value: &event.value,
            }),
            Self::Packed(values) => values
                .next()
                .map(|(var, value)| InputEventRef { var, value }),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Events(events) => events.size_hint(),
            Self::Packed(values) => values.size_hint(),
        }
    }
}

impl<V> ExactSizeIterator for InputTickIter<'_, V> {}

impl<'a, V> InputTick<'a, V> {
    pub fn len(&self) -> usize {
        match self.representation {
            InputTickRepresentation::Events(events) => events.len(),
            InputTickRepresentation::PackedRow { values, .. } => values.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = InputEventRef<'a, V>> + '_ {
        match self.representation {
            InputTickRepresentation::Events(events) => InputTickIter::Events(events.iter()),
            InputTickRepresentation::PackedRow { layout, values } => {
                InputTickIter::Packed(layout.iter().zip(values))
            }
        }
    }

    pub fn to_events(&self) -> Vec<InputEvent<V>>
    where
        V: Clone,
    {
        self.iter()
            .map(|event| InputEvent::new(event.var.clone(), event.value.clone()))
            .collect()
    }
}

/// A transport batch containing zero or more logical input ticks.
#[derive(Debug, PartialEq)]
pub struct InputBatch<V> {
    representation: InputBatchRepresentation<V>,
}

#[derive(Debug, PartialEq)]
enum InputBatchRepresentation<V> {
    /// Independent events stored in one transport batch.
    ///
    /// Each event forms its own logical tick, so a batch containing `n` events
    /// advances the runtime `n` times.
    Events(Vec<InputEvent<V>>),

    /// One or more fixed-width logical ticks stored contiguously.
    ///
    /// Each consecutive `tick_width` events forms one simultaneous tick. For
    /// example, four events with `tick_width == 2` represent two logical ticks.
    /// Every tick must contain distinct variables, and the total event count must
    /// be divisible by `tick_width`.
    AtomicTicks {
        events: Vec<InputEvent<V>>,
        tick_width: NonZeroUsize,
    },

    /// Fixed-layout logical rows. Variable names are stored once for the
    /// entire transport batch, while values remain contiguous by row.
    PackedRows {
        layout: Box<[VarName]>,
        values: Vec<V>,
    },
}

impl<V> InputBatch<V> {
    /// Construct a transport batch of independent events.
    pub fn events(events: Vec<InputEvent<V>>) -> Self {
        Self {
            representation: InputBatchRepresentation::Events(events),
        }
    }

    /// Construct one non-empty simultaneous logical tick.
    pub fn step(events: Vec<InputEvent<V>>) -> anyhow::Result<Self> {
        let tick_width = NonZeroUsize::new(events.len())
            .ok_or_else(|| anyhow::anyhow!("input step must contain at least one event"))?;
        Self::validated_atomic_ticks(events, tick_width)
    }

    /// Construct several fixed-width simultaneous ticks in one transport batch.
    pub(crate) fn packed_steps(
        tick_width: NonZeroUsize,
        events: Vec<InputEvent<V>>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !events.is_empty(),
            "atomic input batch must contain at least one tick"
        );
        anyhow::ensure!(
            events.len().is_multiple_of(tick_width.get()),
            "input batch of {} events is not divisible into ticks of width {tick_width}",
            events.len()
        );
        Self::validated_atomic_ticks(events, tick_width)
    }

    /// Construct fixed-layout rows created by a trusted in-crate producer.
    ///
    /// The layout must be non-empty and contain distinct variables. Values
    /// must contain at least one complete row.
    pub(crate) fn trusted_packed_rows(layout: Box<[VarName]>, values: Vec<V>) -> Self {
        debug_assert!(!layout.is_empty());
        debug_assert!(!values.is_empty());
        debug_assert!(values.len().is_multiple_of(layout.len()));
        debug_assert_eq!(
            layout.iter().collect::<HashSet<_>>().len(),
            layout.len(),
            "packed input layout contains duplicate variables"
        );
        Self {
            representation: InputBatchRepresentation::PackedRows { layout, values },
        }
    }

    fn validated_atomic_ticks(
        events: Vec<InputEvent<V>>,
        tick_width: NonZeroUsize,
    ) -> anyhow::Result<Self> {
        let mut seen = HashSet::with_capacity(tick_width.get());
        for tick in events.chunks(tick_width.get()) {
            seen.clear();
            for event in tick {
                if !seen.insert(&event.var) {
                    anyhow::bail!(
                        "atomic input step contains duplicate variable `{}`",
                        event.var
                    );
                }
            }
        }
        Ok(Self {
            representation: InputBatchRepresentation::AtomicTicks { events, tick_width },
        })
    }

    pub fn ticks(&self) -> impl Iterator<Item = InputTick<'_, V>> {
        let mut offset = 0;
        std::iter::from_fn(move || match &self.representation {
            InputBatchRepresentation::Events(events) => {
                let event = events.get(offset)?;
                offset += 1;
                Some(InputTick {
                    representation: InputTickRepresentation::Events(std::slice::from_ref(event)),
                })
            }
            InputBatchRepresentation::AtomicTicks { events, tick_width } => {
                let end = offset.checked_add(tick_width.get())?;
                let tick = events.get(offset..end)?;
                offset = end;
                Some(InputTick {
                    representation: InputTickRepresentation::Events(tick),
                })
            }
            InputBatchRepresentation::PackedRows { layout, values } => {
                let end = offset.checked_add(layout.len())?;
                let row = values.get(offset..end)?;
                offset = end;
                Some(InputTick {
                    representation: InputTickRepresentation::PackedRow {
                        layout,
                        values: row,
                    },
                })
            }
        })
    }

    pub(crate) fn packed_rows(&self) -> Option<(&[VarName], &[V])> {
        match &self.representation {
            InputBatchRepresentation::PackedRows { layout, values } => Some((layout, values)),
            _ => None,
        }
    }

    /// Consume a batch of independent events, returning the atomic tick width
    /// when the batch instead contains `AtomicTicks`.
    pub(crate) fn into_independent_events(self) -> Result<Vec<InputEvent<V>>, NonZeroUsize> {
        match self.representation {
            InputBatchRepresentation::Events(events) => Ok(events),
            InputBatchRepresentation::AtomicTicks { tick_width, .. } => Err(tick_width),
            InputBatchRepresentation::PackedRows { layout, .. } => {
                Err(NonZeroUsize::new(layout.len()).unwrap())
            }
        }
    }

    pub(crate) fn into_ticks(self) -> impl Iterator<Item = Vec<InputEvent<V>>> {
        enum Storage<V> {
            Events {
                events: std::vec::IntoIter<InputEvent<V>>,
                tick_width: usize,
            },
            PackedRows {
                layout: Box<[VarName]>,
                values: std::vec::IntoIter<V>,
            },
        }

        let mut storage = match self.representation {
            InputBatchRepresentation::Events(events) => Storage::Events {
                events: events.into_iter(),
                tick_width: 1,
            },
            InputBatchRepresentation::AtomicTicks { events, tick_width } => Storage::Events {
                events: events.into_iter(),
                tick_width: tick_width.get(),
            },
            InputBatchRepresentation::PackedRows { layout, values } => Storage::PackedRows {
                layout,
                values: values.into_iter(),
            },
        };
        std::iter::from_fn(move || match &mut storage {
            Storage::Events { events, tick_width } => {
                (!events.as_slice().is_empty()).then(|| events.by_ref().take(*tick_width).collect())
            }
            Storage::PackedRows { layout, values } => {
                if values.as_slice().is_empty() {
                    return None;
                }
                Some(
                    values
                        .by_ref()
                        .take(layout.len())
                        .enumerate()
                        .map(|(index, value)| InputEvent::new(layout[index].clone(), value))
                        .collect(),
                )
            }
        })
    }
}

/// Flatten transport batches into a stream with one logical tick per item.
pub(crate) fn into_tick_stream<V: 'static>(mut input: InputStream<V>) -> InputTickStream<V> {
    Box::pin(async_stream::try_stream! {
        while let Some(batch) = futures::StreamExt::next(&mut input).await {
            for tick in batch?.into_ticks() {
                yield tick;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

    #[test]
    fn fixed_width_layout_iterates_complete_rows() {
        let batch = [
            InputEvent::new("x".into(), 1),
            InputEvent::new("y".into(), 2),
            InputEvent::new("x".into(), 3),
            InputEvent::new("y".into(), 4),
        ];
        let rows = InputBatch::packed_steps(NonZeroUsize::new(2).unwrap(), batch.into())
            .unwrap()
            .ticks()
            .map(|row| row.iter().map(|event| *event.value).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(rows, [vec![1, 2], vec![3, 4]]);
    }

    #[test]
    fn fixed_width_layout_rejects_partial_rows() {
        let batch = [
            InputEvent::new("x".into(), 1),
            InputEvent::new("y".into(), 2),
            InputEvent::new("x".into(), 3),
        ];
        assert!(InputBatch::packed_steps(NonZeroUsize::new(2).unwrap(), batch.into()).is_err());
    }

    #[test]
    fn packed_atomic_ticks_must_not_be_empty() {
        let error = InputBatch::<()>::packed_steps(NonZeroUsize::MIN, Vec::new()).unwrap_err();
        assert_eq!(
            error.to_string(),
            "atomic input batch must contain at least one tick"
        );
    }

    #[test]
    fn simultaneous_steps_must_not_be_empty() {
        let error = InputBatch::<()>::step(Vec::new()).unwrap_err();
        assert_eq!(
            error.to_string(),
            "input step must contain at least one event"
        );
    }

    #[test]
    fn empty_event_batches_contain_no_ticks() {
        let batch = InputBatch::<()>::events(Vec::new());
        assert_eq!(batch.ticks().count(), 0);
        assert_eq!(batch.into_ticks().count(), 0);
    }

    #[test]
    fn only_event_batches_can_be_consumed_as_independent_events() {
        let events = InputBatch::events(vec![InputEvent::new("x".into(), 1)])
            .into_independent_events()
            .unwrap();
        assert_eq!(events, [InputEvent::new("x".into(), 1)]);

        let tick_width = InputBatch::step(vec![InputEvent::new("x".into(), 1)])
            .unwrap()
            .into_independent_events()
            .unwrap_err();
        assert_eq!(tick_width, NonZeroUsize::MIN);
    }

    #[test]
    fn into_ticks_splits_events_and_packed_steps() {
        smol::block_on(async {
            let events = into_tick_stream(Box::pin(futures::stream::iter([Ok(
                InputBatch::events(vec![
                    InputEvent::new("x".into(), 1),
                    InputEvent::new("y".into(), 2),
                ]),
            )])))
            .map(Result::unwrap)
            .collect::<Vec<_>>()
            .await;
            assert_eq!(events.len(), 2);
            assert_eq!(events[0], [InputEvent::new("x".into(), 1)]);
            assert_eq!(events[1], [InputEvent::new("y".into(), 2)]);

            let steps = into_tick_stream(Box::pin(futures::stream::iter([Ok(
                InputBatch::packed_steps(
                    NonZeroUsize::new(2).unwrap(),
                    vec![
                        InputEvent::new("x".into(), 1),
                        InputEvent::new("y".into(), 2),
                        InputEvent::new("x".into(), 3),
                        InputEvent::new("y".into(), 4),
                    ],
                )
                .unwrap(),
            )])))
            .map(Result::unwrap)
            .collect::<Vec<_>>()
            .await;
            assert_eq!(steps.len(), 2);
            assert_eq!(steps[0].len(), 2);
            assert_eq!(steps[1][0].value, 3);
        });
    }

    #[test]
    fn atomic_step_rejects_duplicate_variables() {
        let batch = [
            InputEvent::new("x".into(), 1),
            InputEvent::new("x".into(), 2),
        ];
        let error = match InputBatch::step(batch.into()) {
            Ok(_) => panic!("duplicate variables should be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("duplicate variable `x`"));
    }

    #[test]
    fn packed_steps_allow_a_variable_once_per_step() {
        let batch = [
            InputEvent::new("x".into(), 1),
            InputEvent::new("x".into(), 2),
        ];
        let batch = InputBatch::packed_steps(NonZeroUsize::new(1).unwrap(), batch.into()).unwrap();
        assert_eq!(batch.ticks().count(), 2);
    }

    #[test]
    fn packed_rows_share_one_layout() {
        let batch = InputBatch::trusted_packed_rows(
            vec![VarName::new("x"), VarName::new("y")].into_boxed_slice(),
            vec![1, 2, 3, 4],
        );
        let rows = batch
            .ticks()
            .map(|tick| {
                tick.iter()
                    .map(|event| (event.var.clone(), *event.value))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            [
                vec![(VarName::new("x"), 1), (VarName::new("y"), 2)],
                vec![(VarName::new("x"), 3), (VarName::new("y"), 4)],
            ]
        );
    }

    #[test]
    fn packed_rows_expand_only_when_owned_ticks_are_requested() {
        let batch = InputBatch::trusted_packed_rows(
            vec![VarName::new("x"), VarName::new("y")].into_boxed_slice(),
            vec![1, 2, 3, 4],
        );
        assert_eq!(
            batch.into_ticks().collect::<Vec<_>>(),
            [
                vec![
                    InputEvent::new(VarName::new("x"), 1),
                    InputEvent::new(VarName::new("y"), 2),
                ],
                vec![
                    InputEvent::new(VarName::new("x"), 3),
                    InputEvent::new(VarName::new("y"), 4),
                ],
            ]
        );
    }

    #[test]
    fn fallible_source_delivers_batch_errors() {
        smol::block_on(async {
            let error = anyhow::anyhow!("input failed");
            let mut batches: InputStream<()> = Box::pin(futures::stream::iter([Err(error)]));
            assert_eq!(
                batches.next().await.unwrap().unwrap_err().to_string(),
                "input failed"
            );
        });
    }
}
