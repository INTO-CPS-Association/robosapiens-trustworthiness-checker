use std::{mem, num::NonZeroUsize, time::Duration};

use futures::{FutureExt, StreamExt, future::LocalBoxFuture};

use crate::{InputBatch, InputEvent, InputStream};

/// How aggregated event input affects logical time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregationSemantics {
    /// Preserve every event and its arrival order as independent logical ticks.
    PreserveTicks,
    /// Produce one simultaneous logical tick, retaining only the last value per variable.
    CoalesceToAtomicStep,
}

/// Optional bounded aggregation for event input sources.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InputAggregation {
    /// Maximum delay measured from the first event placed in an empty aggregator.
    pub max_delay: Duration,
    /// Optional raw-arrival limit; reaching it emits the aggregation before its deadline.
    pub event_limit: Option<NonZeroUsize>,
    /// Whether aggregated arrivals retain their logical ticks or form one atomic tick.
    pub semantics: AggregationSemantics,
}

impl InputAggregation {
    pub fn new(max_delay: Duration, semantics: AggregationSemantics) -> Self {
        Self {
            max_delay,
            event_limit: None,
            semantics,
        }
    }

    pub fn with_event_limit(mut self, event_limit: NonZeroUsize) -> Self {
        self.event_limit = Some(event_limit);
        self
    }

    pub fn is_passthrough(self) -> bool {
        self.max_delay.is_zero()
            && self.event_limit.is_none()
            && self.semantics == AggregationSemantics::PreserveTicks
    }
}

/// Injectable timer used by input aggregation. Implementations may use real or
/// simulated time; advancing a simulated timer must resolve elapsed sleeps.
pub(crate) trait InputTimer: 'static {
    fn sleep(&self, duration: Duration) -> LocalBoxFuture<'static, ()>;
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RealTimeInputTimer;

impl InputTimer for RealTimeInputTimer {
    fn sleep(&self, duration: Duration) -> LocalBoxFuture<'static, ()> {
        Box::pin(async move {
            smol::Timer::after(duration).await;
        })
    }
}

struct EventAggregator<V> {
    events: Vec<InputEvent<V>>,
    arrivals: usize,
    aggregation: InputAggregation,
}

impl<V> EventAggregator<V> {
    fn new(aggregation: InputAggregation) -> Self {
        Self {
            events: Vec::new(),
            arrivals: 0,
            aggregation,
        }
    }

    fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    fn push(&mut self, event: InputEvent<V>) -> Option<Vec<InputEvent<V>>> {
        self.arrivals += 1;
        match self.aggregation.semantics {
            AggregationSemantics::PreserveTicks => self.events.push(event),
            AggregationSemantics::CoalesceToAtomicStep => {
                if let Some(previous) = self
                    .events
                    .iter_mut()
                    .find(|previous| previous.var == event.var)
                {
                    previous.value = event.value;
                } else {
                    self.events.push(event);
                }
            }
        }
        self.aggregation
            .event_limit
            .is_some_and(|limit| self.arrivals >= limit.get())
            .then(|| self.take())
    }

    fn take(&mut self) -> Vec<InputEvent<V>> {
        self.arrivals = 0;
        mem::take(&mut self.events)
    }
}

fn independent_events<V>(batch: InputBatch<V>) -> anyhow::Result<Vec<InputEvent<V>>> {
    batch.into_independent_events().map_err(|tick_width| {
        anyhow::anyhow!(
            "input aggregation requires independent event ticks; received atomic ticks of width {tick_width}"
        )
    })
}

fn input_batch<V>(events: Vec<InputEvent<V>>, semantics: AggregationSemantics) -> InputBatch<V> {
    debug_assert!(!events.is_empty());
    match semantics {
        AggregationSemantics::PreserveTicks => InputBatch::events(events),
        AggregationSemantics::CoalesceToAtomicStep => {
            InputBatch::step(events).expect("the aggregator keeps at most one event per variable")
        }
    }
}

fn aggregate_without_delay<V: 'static>(
    mut source: InputStream<V>,
    aggregation: InputAggregation,
) -> InputStream<V> {
    if aggregation.semantics == AggregationSemantics::PreserveTicks
        && aggregation.event_limit.is_none()
    {
        return source;
    }
    Box::pin(async_stream::try_stream! {
        let mut aggregator = EventAggregator::new(aggregation);
        while let Some(batch) = source.next().await {
            for event in independent_events(batch?)? {
                if let Some(complete) = aggregator.push(event) {
                    yield input_batch(complete, aggregation.semantics);
                }
            }
            if !aggregator.is_empty() {
                yield input_batch(aggregator.take(), aggregation.semantics);
            }
        }
    })
}

fn aggregate_input<V, T>(
    source: InputStream<V>,
    aggregation: InputAggregation,
    timer: T,
) -> InputStream<V>
where
    V: 'static,
    T: InputTimer,
{
    if aggregation.max_delay.is_zero() {
        return aggregate_without_delay(source, aggregation);
    }

    Box::pin(async_stream::try_stream! {
        let mut source = source.fuse();
        let mut aggregator = EventAggregator::new(aggregation);

        loop {
            if aggregator.is_empty() {
                let Some(batch) = source.next().await else {
                    return;
                };
                for event in independent_events(batch?)? {
                    if let Some(complete) = aggregator.push(event) {
                        yield input_batch(complete, aggregation.semantics);
                    }
                }
                if aggregator.is_empty() {
                    continue;
                }
            }

            let mut deadline = timer.sleep(aggregation.max_delay).fuse();
            loop {
                let next = futures::select! {
                    batch = source.next().fuse() => Some(batch),
                    _ = deadline => None,
                };
                match next {
                    Some(batch) => {
                        let Some(batch) = batch else {
                            yield input_batch(aggregator.take(), aggregation.semantics);
                            return;
                        };
                        let batch = match batch {
                            Ok(batch) => batch,
                            Err(error) => {
                                yield input_batch(aggregator.take(), aggregation.semantics);
                                Err(error)?;
                                unreachable!();
                            }
                        };
                        let batch = match independent_events(batch) {
                            Ok(batch) => batch,
                            Err(error) => {
                                yield input_batch(aggregator.take(), aggregation.semantics);
                                Err(error)?;
                                unreachable!();
                            }
                        };
                        let mut completed = false;
                        for event in batch {
                            if let Some(batch) = aggregator.push(event) {
                                yield input_batch(batch, aggregation.semantics);
                                completed = true;
                            }
                        }
                        if completed {
                            break;
                        }
                    }
                    None => {
                        yield input_batch(aggregator.take(), aggregation.semantics);
                        break;
                    }
                }
            }
        }
    })
}

/// Aggregate an independent-event stream using an injectable timer.
pub(crate) fn aggregate_input_stream_with_timer<V, T>(
    input: InputStream<V>,
    aggregation: InputAggregation,
    timer: T,
) -> InputStream<V>
where
    V: 'static,
    T: InputTimer,
{
    aggregate_input(input, aggregation, timer)
}

/// Aggregate an independent-event stream using the real-time timer.
pub(crate) fn aggregate_input_stream<V: 'static>(
    input: InputStream<V>,
    aggregation: InputAggregation,
) -> InputStream<V> {
    aggregate_input_stream_with_timer(input, aggregation, RealTimeInputTimer)
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::BTreeMap, rc::Rc, time::Duration};

    use futures::{FutureExt, StreamExt, channel::mpsc, channel::oneshot};
    use proptest::prelude::*;

    use super::*;
    use crate::{InputEvent, OutputStream, Value, VarName};

    type EventBatchStream<V> = OutputStream<anyhow::Result<Vec<InputEvent<V>>>>;

    #[derive(Clone, Default)]
    struct ManualTimer {
        state: Rc<RefCell<ManualTimerState>>,
    }

    #[derive(Default)]
    struct ManualTimerState {
        now: Duration,
        sleepers: Vec<(Duration, oneshot::Sender<()>)>,
    }

    impl ManualTimer {
        fn advance(&self, duration: Duration) {
            let mut state = self.state.borrow_mut();
            state.now += duration;
            let now = state.now;
            let sleepers = mem::take(&mut state.sleepers);
            for (deadline, sender) in sleepers {
                if deadline <= now {
                    let _ = sender.send(());
                } else {
                    state.sleepers.push((deadline, sender));
                }
            }
        }

        fn sleeper_count(&self) -> usize {
            self.state.borrow().sleepers.len()
        }
    }

    impl InputTimer for ManualTimer {
        fn sleep(&self, duration: Duration) -> LocalBoxFuture<'static, ()> {
            let (sender, receiver) = oneshot::channel();
            let mut state = self.state.borrow_mut();
            let deadline = state.now + duration;
            state.sleepers.push((deadline, sender));
            Box::pin(async move {
                let _ = receiver.await;
            })
        }
    }

    fn sparse_channel<V: 'static>() -> (mpsc::UnboundedSender<Vec<InputEvent<V>>>, InputStream<V>) {
        let (sender, receiver) = mpsc::unbounded();
        (
            sender,
            Box::pin(receiver.map(|events| Ok(InputBatch::events(events)))),
        )
    }

    fn aggregated_stream<V: 'static>(
        input: InputStream<V>,
        aggregation: InputAggregation,
        timer: ManualTimer,
    ) -> EventBatchStream<V> {
        Box::pin(
            aggregate_input(input, aggregation, timer)
                .map(|batch| batch.map(|batch| batch.into_ticks().flatten().collect())),
        )
    }

    fn event(var: &str, value: i64) -> InputEvent<Value> {
        InputEvent::new(var.into(), Value::Int(value))
    }

    fn aggregated_source(semantics: AggregationSemantics) -> InputStream<Value> {
        smol::block_on(async move {
            let input = Box::pin(futures::stream::iter([Ok(InputBatch::events(vec![
                event("x", 1),
                event("y", 2),
            ]))]));
            aggregate_input_stream_with_timer(
                input,
                InputAggregation::new(Duration::ZERO, semantics),
                ManualTimer::default(),
            )
        })
    }

    #[test]
    fn aggregation_semantics_select_event_or_step_delivery() {
        smol::block_on(async {
            let mut events = aggregated_source(AggregationSemantics::PreserveTicks);
            let batch = events.next().await.unwrap().unwrap();
            assert_eq!(
                batch.ticks().collect::<Vec<_>>(),
                [[event("x", 1)].as_slice(), [event("y", 2)].as_slice(),]
            );

            let mut steps = aggregated_source(AggregationSemantics::CoalesceToAtomicStep);
            let batch = steps.next().await.unwrap().unwrap();
            assert_eq!(
                batch.ticks().collect::<Vec<_>>(),
                [[event("x", 1), event("y", 2)].as_slice()]
            );
        });
    }

    #[test]
    fn aggregation_ignores_empty_event_batches() {
        smol::block_on(async {
            let input = Box::pin(futures::stream::iter([Ok(InputBatch::<Value>::events(
                Vec::new(),
            ))]));
            let mut aggregated = aggregate_input_stream_with_timer(
                input,
                InputAggregation::new(Duration::ZERO, AggregationSemantics::CoalesceToAtomicStep),
                ManualTimer::default(),
            );
            assert!(aggregated.next().await.is_none());
        });
    }

    #[test]
    fn aggregation_rejects_atomic_input_ticks() {
        smol::block_on(async {
            let input = Box::pin(futures::stream::iter([Ok(InputBatch::step(vec![
                event("x", 1),
                event("y", 2),
            ])
            .unwrap())]));
            let mut aggregated = aggregate_input_stream_with_timer(
                input,
                InputAggregation::new(Duration::ZERO, AggregationSemantics::CoalesceToAtomicStep),
                ManualTimer::default(),
            );
            assert_eq!(
                aggregated.next().await.unwrap().unwrap_err().to_string(),
                "input aggregation requires independent event ticks; received atomic ticks of width 2"
            );

            let input = Box::pin(futures::stream::iter([Ok(InputBatch::step(vec![event(
                "x", 1,
            )])
            .unwrap())]));
            let mut aggregated = aggregate_input_stream_with_timer(
                input,
                InputAggregation::new(Duration::ZERO, AggregationSemantics::CoalesceToAtomicStep),
                ManualTimer::default(),
            );
            assert_eq!(
                aggregated.next().await.unwrap().unwrap_err().to_string(),
                "input aggregation requires independent event ticks; received atomic ticks of width 1"
            );
        });
    }

    #[test]
    fn aggregation_preserves_input_errors() {
        smol::block_on(async {
            let source: InputStream<Value> = Box::pin(futures::stream::iter([Err(
                anyhow::anyhow!("source failed"),
            )]));
            let mut aggregated = aggregate_input(
                source,
                InputAggregation::new(Duration::ZERO, AggregationSemantics::CoalesceToAtomicStep),
                ManualTimer::default(),
            );
            assert_eq!(
                aggregated.next().await.unwrap().unwrap_err().to_string(),
                "source failed"
            );
        });
    }

    #[test]
    fn aggregation_flushes_buffered_events_before_input_error() {
        smol::block_on(async {
            let expected = vec![event("x", 1), event("y", 2)];
            let source: InputStream<Value> = Box::pin(futures::stream::iter([
                Ok(InputBatch::events(vec![event("x", 1), event("y", 2)])),
                Err(anyhow::anyhow!("source failed")),
            ]));
            let mut aggregated = aggregate_input(
                source,
                InputAggregation::new(Duration::from_secs(1), AggregationSemantics::PreserveTicks),
                ManualTimer::default(),
            );

            assert_eq!(
                aggregated
                    .next()
                    .await
                    .unwrap()
                    .unwrap()
                    .into_ticks()
                    .flatten()
                    .collect::<Vec<_>>(),
                expected
            );
            assert_eq!(
                aggregated.next().await.unwrap().unwrap_err().to_string(),
                "source failed"
            );
        });
    }

    #[test]
    fn sparse_aggregation_uses_simulated_deadline_and_preserves_order() {
        let timer = ManualTimer::default();
        let (sender, input) = sparse_channel();
        let mut batches = aggregated_stream(
            input,
            InputAggregation::new(
                Duration::from_millis(10),
                AggregationSemantics::PreserveTicks,
            ),
            timer.clone(),
        );
        sender.unbounded_send(vec![event("x", 1)]).unwrap();
        assert!(batches.next().now_or_never().is_none());
        timer.advance(Duration::from_millis(5));
        sender.unbounded_send(vec![event("y", 2)]).unwrap();
        assert!(batches.next().now_or_never().is_none());
        timer.advance(Duration::from_millis(4));
        assert!(batches.next().now_or_never().is_none());
        timer.advance(Duration::from_millis(1));

        let batch = batches.next().now_or_never().flatten().unwrap().unwrap();
        assert_eq!(batch, vec![event("x", 1), event("y", 2)]);
    }

    #[test]
    fn atomic_aggregation_is_last_value_wins_per_variable() {
        let timer = ManualTimer::default();
        let (sender, input) = sparse_channel();
        let mut batches = aggregated_stream(
            input,
            InputAggregation::new(
                Duration::from_millis(10),
                AggregationSemantics::CoalesceToAtomicStep,
            ),
            timer.clone(),
        );
        sender
            .unbounded_send(vec![event("x", 1), event("y", 2)])
            .unwrap();
        assert!(batches.next().now_or_never().is_none());
        timer.advance(Duration::from_millis(5));
        sender.unbounded_send(vec![event("x", 3)]).unwrap();
        assert!(batches.next().now_or_never().is_none());
        timer.advance(Duration::from_millis(5));

        let batch = batches.next().now_or_never().flatten().unwrap().unwrap();
        assert_eq!(batch, vec![event("x", 3), event("y", 2)]);
    }

    #[test]
    fn event_limit_emits_aggregations_and_end_of_stream_flushes_tail() {
        let timer = ManualTimer::default();
        let (sender, input) = sparse_channel();
        let batches = aggregated_stream(
            input,
            InputAggregation::new(Duration::from_secs(1), AggregationSemantics::PreserveTicks)
                .with_event_limit(NonZeroUsize::new(2).unwrap()),
            timer,
        );
        sender
            .unbounded_send(vec![
                event("x", 1),
                event("x", 2),
                event("x", 3),
                event("x", 4),
                event("x", 5),
            ])
            .unwrap();
        sender.close_channel();

        let batches = smol::block_on(batches.map(Result::unwrap).collect::<Vec<_>>());
        assert_eq!(batches.iter().map(Vec::len).collect::<Vec<_>>(), [2, 2, 1]);
        assert_eq!(
            batches.into_iter().flatten().collect::<Vec<_>>(),
            (1..=5).map(|value| event("x", value)).collect::<Vec<_>>()
        );
    }

    #[test]
    fn dropping_aggregation_cancels_simulated_sleep_safely() {
        let timer = ManualTimer::default();
        let (sender, input) = sparse_channel();
        let mut batches = aggregated_stream(
            input,
            InputAggregation::new(Duration::from_secs(1), AggregationSemantics::PreserveTicks),
            timer.clone(),
        );
        sender.unbounded_send(vec![event("x", 1)]).unwrap();
        assert!(batches.next().now_or_never().is_none());
        assert_eq!(timer.sleeper_count(), 1);
        drop(batches);
        timer.advance(Duration::from_secs(1));
        assert_eq!(timer.sleeper_count(), 0);
    }

    proptest! {
        #[test]
        fn simulated_deadlines_match_sparse_aggregation_model(
            schedule in prop::collection::vec((0u8..10, 0u8..4, any::<i16>()), 0..40),
        ) {
            const AGGREGATION_MS: u64 = 5;
            let timer = ManualTimer::default();
            let (sender, input) = sparse_channel();
            let mut stream = aggregated_stream(
                input,
                InputAggregation::new(
                    Duration::from_millis(AGGREGATION_MS),
                    AggregationSemantics::PreserveTicks,
                ),
                timer.clone(),
            );

            let mut now = 0u64;
            let mut deadline = None;
            let mut expected_aggregation = Vec::new();
            let mut expected = Vec::new();
            let mut actual = Vec::new();

            for (gap, var, value) in schedule {
                now += u64::from(gap);
                timer.advance(Duration::from_millis(u64::from(gap)));
                if deadline.is_some_and(|deadline| deadline <= now) {
                    expected.push(mem::take(&mut expected_aggregation));
                    deadline = None;
                    actual.push(stream.next().now_or_never().flatten().unwrap().unwrap());
                }

                let event = InputEvent::new(VarName::new(&format!("v{var}")), value);
                if expected_aggregation.is_empty() {
                    deadline = Some(now + AGGREGATION_MS);
                }
                expected_aggregation.push(InputEvent::new(event.var.clone(), event.value));
                sender.unbounded_send(vec![event]).unwrap();
                prop_assert!(stream.next().now_or_never().is_none());
            }

            if !expected_aggregation.is_empty() {
                expected.push(expected_aggregation);
                timer.advance(Duration::from_millis(AGGREGATION_MS));
                actual.push(stream.next().now_or_never().flatten().unwrap().unwrap());
            }
            prop_assert_eq!(actual, expected);
        }

        #[test]
        fn sparse_aggregations_preserve_every_event_and_respect_event_limit(
            raw in prop::collection::vec((0u8..4, any::<i16>()), 0..100),
            limit in 1usize..16,
        ) {
            let aggregation = InputAggregation::new(
                Duration::from_secs(1),
                AggregationSemantics::PreserveTicks,
            )
            .with_event_limit(NonZeroUsize::new(limit).unwrap());
            let expected = raw
                .iter()
                .map(|(var, value)| (VarName::new(&format!("v{var}")), *value))
                .collect::<Vec<_>>();
            let events = expected
                .iter()
                .map(|(var, value)| InputEvent::new(var.clone(), *value))
                .collect::<Vec<_>>();
            let mut aggregator = EventAggregator::new(aggregation);
            let mut aggregations = events
                .into_iter()
                .filter_map(|event| aggregator.push(event))
                .collect::<Vec<_>>();
            if !aggregator.is_empty() {
                aggregations.push(aggregator.take());
            }

            prop_assert!(aggregations.iter().all(|batch| batch.len() <= limit));
            let actual = aggregations
                .into_iter()
                .flatten()
                .map(|event| (event.var, event.value))
                .collect::<Vec<_>>();
            prop_assert_eq!(actual, expected);
        }

        #[test]
        fn atomic_aggregations_keep_the_last_value_for_each_variable(
            raw in prop::collection::vec((0u8..8, any::<i16>()), 0..100),
        ) {
            let mut expected = BTreeMap::new();
            let events = raw
                .into_iter()
                .map(|(var, value)| {
                    let var = VarName::new(&format!("v{var}"));
                    expected.insert(var.clone(), value);
                    InputEvent::new(var, value)
                })
                .collect::<Vec<_>>();
            let mut aggregator = EventAggregator::new(InputAggregation::new(
                Duration::from_secs(1),
                AggregationSemantics::CoalesceToAtomicStep,
            ));
            for event in events {
                prop_assert!(aggregator.push(event).is_none());
            }
            let actual = aggregator
                .take()
                .into_iter()
                .map(|event| (event.var, event.value))
                .collect::<BTreeMap<_, _>>();
            prop_assert_eq!(actual, expected);
        }

        #[test]
        fn atomic_event_limits_match_independent_raw_arrival_windows(
            raw in prop::collection::vec((0u8..8, any::<i16>()), 0..100),
            limit in 1usize..16,
        ) {
            let aggregation = InputAggregation::new(
                Duration::from_secs(1),
                AggregationSemantics::CoalesceToAtomicStep,
            )
            .with_event_limit(NonZeroUsize::new(limit).unwrap());
            let mut aggregator = EventAggregator::new(aggregation);
            let mut actual = Vec::new();
            for (var, value) in &raw {
                if let Some(batch) = aggregator.push(InputEvent::new(
                    VarName::new(&format!("v{var}")),
                    *value,
                )) {
                    actual.push(batch);
                }
            }
            if !aggregator.is_empty() {
                actual.push(aggregator.take());
            }

            let expected = raw
                .chunks(limit)
                .map(|window| {
                    window.iter().fold(BTreeMap::new(), |mut values, (var, value)| {
                        values.insert(VarName::new(&format!("v{var}")), *value);
                        values
                    })
                })
                .collect::<Vec<_>>();
            let actual = actual
                .into_iter()
                .map(|batch| {
                    batch.into_iter().map(|event| (event.var, event.value)).collect()
                })
                .collect::<Vec<BTreeMap<_, _>>>();
            prop_assert_eq!(actual, expected);
        }
    }
}
