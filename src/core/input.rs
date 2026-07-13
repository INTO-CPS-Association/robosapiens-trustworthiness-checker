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

    pub fn ticks(&self) -> impl Iterator<Item = &[InputEvent<V>]> {
        let (events, tick_width) = match &self.representation {
            InputBatchRepresentation::Events(events) => (events.as_slice(), 1),
            InputBatchRepresentation::AtomicTicks { events, tick_width } => {
                (events.as_slice(), tick_width.get())
            }
        };
        events.chunks(tick_width)
    }

    pub(crate) fn into_ticks(self) -> impl Iterator<Item = Vec<InputEvent<V>>> {
        let (events, tick_width) = match self.representation {
            InputBatchRepresentation::Events(events) => (events, 1),
            InputBatchRepresentation::AtomicTicks { events, tick_width } => {
                (events, tick_width.get())
            }
        };
        let mut events = events.into_iter();
        std::iter::from_fn(move || {
            if events.as_slice().is_empty() {
                None
            } else {
                Some(events.by_ref().take(tick_width).collect())
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
            .map(|row| row.iter().map(|event| event.value).collect::<Vec<_>>())
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
