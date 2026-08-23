use std::{
    collections::BTreeMap, convert::Infallible, mem, num::NonZeroUsize, time::Duration, vec,
};

use futures::{FutureExt, StreamExt, future::LocalBoxFuture};

use crate::core::{
    InputBatch, InputSegment, InputStream, InputUpdate, OutputStream, OwnedInputTicks, VarName,
};
use crate::io::config::{InputReduction, InputStage};

/// Injectable timer used by input windows. Tests can provide a simulated timer;
/// production uses the real smol timer.
pub(super) trait InputTimer: 'static {
    fn sleep(&self, duration: Duration) -> LocalBoxFuture<'static, ()>;
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct RealTimeInputTimer;

impl InputTimer for RealTimeInputTimer {
    fn sleep(&self, duration: Duration) -> LocalBoxFuture<'static, ()> {
        Box::pin(async move {
            let _ = smol::Timer::after(duration).await;
        })
    }
}

pub(crate) fn apply_stage<V: 'static>(
    input: InputStream<V>,
    stage: InputStage,
) -> anyhow::Result<InputStream<V>> {
    apply_stage_with_timer(input, stage, RealTimeInputTimer)
}

pub(super) fn apply_stage_with_timer<V: 'static, T: InputTimer>(
    mut input: InputStream<V>,
    stage: InputStage,
    timer: T,
) -> anyhow::Result<InputStream<V>> {
    let events: WindowEventStream<V, Infallible> = Box::pin(async_stream::try_stream! {
        while let Some(batch) = input.next().await {
            yield WindowEvent::Data(batch?);
        }
    });
    let mut output = drive_window(events, stage, timer)?;
    Ok(Box::pin(async_stream::try_stream! {
        while let Some(event) = output.next().await {
            match event? {
                WindowEvent::Data(batch) => yield batch,
                WindowEvent::Control(never) => match never {},
            }
        }
    }))
}

pub(super) enum WindowEvent<V, C> {
    Data(InputBatch<V>),
    Control(C),
}

pub(super) type WindowEventStream<V, C> = OutputStream<anyhow::Result<WindowEvent<V, C>>>;

pub(super) fn drive_window<V: 'static, C: 'static, T: InputTimer>(
    mut source: WindowEventStream<V, C>,
    stage: InputStage,
    timer: T,
) -> anyhow::Result<WindowEventStream<V, C>> {
    let window = stage.window().clone();
    anyhow::ensure!(
        window.is_bounded(),
        "input window requires max_delay or update_limit"
    );
    let mut pending = WindowAccumulator::new(stage);

    Ok(Box::pin(async_stream::try_stream! {
        loop {
            while pending.is_empty() {
                match source.next().await {
                    None => return,
                    Some(Err(error)) => Err(error)?,
                    Some(Ok(WindowEvent::Control(control))) => {
                        yield WindowEvent::Control(control);
                        return;
                    }
                    Some(Ok(WindowEvent::Data(batch))) => {
                        for completed in pending.append(batch, window.update_limit) {
                            yield WindowEvent::Data(completed);
                        }
                    }
                }
            }

            if let Some(delay) = window.max_delay {
                let mut deadline = timer.sleep(delay).fuse();
                let mut source_error = None;
                loop {
                    futures::select_biased! {
                        _ = deadline => {
                            yield WindowEvent::Data(pending.take()?);
                            break;
                        }
                        item = source.next().fuse() => {
                            match item {
                                None => {
                                    yield WindowEvent::Data(pending.take()?);
                                    return;
                                }
                                Some(Err(error)) => {
                                    yield WindowEvent::Data(pending.take()?);
                                    source_error = Some(error);
                                    break;
                                }
                                Some(Ok(WindowEvent::Control(control))) => {
                                    yield WindowEvent::Data(pending.take()?);
                                    yield WindowEvent::Control(control);
                                    return;
                                }
                                Some(Ok(WindowEvent::Data(batch))) => {
                                    let mut flushed = false;
                                    for completed in pending.append(batch, window.update_limit) {
                                        flushed = true;
                                        yield WindowEvent::Data(completed);
                                    }
                                    if flushed {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                if let Some(error) = source_error {
                    Err(error)?;
                }
            } else {
                loop {
                    match source.next().await {
                        None => {
                            if !pending.is_empty() {
                                yield WindowEvent::Data(pending.take()?);
                            }
                            return;
                        }
                        Some(Err(error)) => {
                            if !pending.is_empty() {
                                yield WindowEvent::Data(pending.take()?);
                            }
                            Err(error)?;
                        }
                        Some(Ok(WindowEvent::Control(control))) => {
                            if !pending.is_empty() {
                                yield WindowEvent::Data(pending.take()?);
                            }
                            yield WindowEvent::Control(control);
                            return;
                        }
                        Some(Ok(WindowEvent::Data(batch))) => {
                            for completed in pending.append(batch, window.update_limit) {
                                yield WindowEvent::Data(completed);
                            }
                        }
                    }
                }
            }
        }
    }))
}

struct PendingBatch<V> {
    segments: Vec<InputSegment<V>>,
    updates: usize,
}

impl<V> PendingBatch<V> {
    fn new() -> Self {
        Self {
            segments: Vec::new(),
            updates: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.updates == 0
    }

    fn take(&mut self) -> anyhow::Result<InputBatch<V>> {
        self.updates = 0;
        InputBatch::from_segments(mem::take(&mut self.segments))
    }

    fn push(&mut self, segment: InputSegment<V>) {
        self.updates += segment.update_count();
        self.segments.push(segment);
    }

    fn append(&mut self, batch: InputBatch<V>, limit: Option<NonZeroUsize>) -> BatchAppend<'_, V> {
        BatchAppend {
            pending: self,
            segments: batch.into_segments().into_iter(),
            current: None,
            limit: limit.map(NonZeroUsize::get),
        }
    }
}

enum SegmentRemainder<V> {
    Singleton(vec::IntoIter<InputUpdate<V>>),
    Packed {
        layout: Box<[VarName]>,
        values: vec::IntoIter<V>,
    },
}

struct BatchAppend<'a, V> {
    pending: &'a mut PendingBatch<V>,
    segments: vec::IntoIter<InputSegment<V>>,
    current: Option<SegmentRemainder<V>>,
    limit: Option<usize>,
}

impl<V> Iterator for BatchAppend<'_, V> {
    type Item = InputBatch<V>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(remainder) = &mut self.current {
                let limit = self.limit.expect("remainders only exist with a limit");
                let room = limit.saturating_sub(self.pending.updates).max(1);
                let (segment, exhausted) = match remainder {
                    SegmentRemainder::Singleton(updates) => {
                        let take = room.min(updates.len());
                        let chunk = updates.by_ref().take(take).collect::<Vec<_>>();
                        (InputSegment::SingletonTicks(chunk), updates.len() == 0)
                    }
                    SegmentRemainder::Packed { layout, values } => {
                        let width = layout.len();
                        let rows = room.saturating_add(width - 1) / width;
                        let take = (rows.max(1) * width).min(values.len());
                        let chunk = values.by_ref().take(take).collect::<Vec<_>>();
                        (
                            InputSegment::PackedRows {
                                layout: layout.clone(),
                                values: chunk,
                            },
                            values.len() == 0,
                        )
                    }
                };
                self.pending.push(segment);
                if exhausted {
                    self.current = None;
                }
                if self.pending.updates >= limit {
                    return Some(self.pending.take().expect("window segments remain valid"));
                }
                continue;
            }

            let Some(segment) = self.segments.next() else {
                return None;
            };
            let Some(limit) = self.limit else {
                self.pending.push(segment);
                continue;
            };
            let room = limit.saturating_sub(self.pending.updates);
            if segment.update_count() <= room {
                self.pending.push(segment);
                if self.pending.updates >= limit {
                    return Some(self.pending.take().expect("window segments remain valid"));
                }
                continue;
            }

            match segment {
                InputSegment::Tick(updates) => {
                    self.pending.push(InputSegment::Tick(updates));
                    return Some(self.pending.take().expect("window segments remain valid"));
                }
                InputSegment::SingletonTicks(updates) => {
                    self.current = Some(SegmentRemainder::Singleton(updates.into_iter()));
                }
                InputSegment::PackedRows { layout, values } => {
                    self.current = Some(SegmentRemainder::Packed {
                        layout,
                        values: values.into_iter(),
                    });
                }
            }
        }
    }
}

struct PendingAtomic<V> {
    values: Vec<InputUpdate<V>>,
    indices: BTreeMap<VarName, usize>,
    updates: usize,
}

impl<V> PendingAtomic<V> {
    fn new() -> Self {
        Self {
            values: Vec::new(),
            indices: BTreeMap::new(),
            updates: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn push_tick(&mut self, tick: Vec<InputUpdate<V>>) {
        self.updates += tick.len();
        for update in tick {
            if let Some(index) = self.indices.get(&update.variable).copied() {
                self.values[index] = update;
            } else {
                let index = self.values.len();
                self.indices.insert(update.variable.clone(), index);
                self.values.push(update);
            }
        }
    }

    fn take(&mut self) -> anyhow::Result<InputBatch<V>> {
        self.updates = 0;
        self.indices.clear();
        InputBatch::tick(mem::take(&mut self.values))
    }

    fn append(&mut self, batch: InputBatch<V>, limit: Option<NonZeroUsize>) -> AtomicAppend<'_, V> {
        AtomicAppend {
            pending: self,
            ticks: batch.into_ticks(),
            limit: limit.map(NonZeroUsize::get),
        }
    }
}

struct AtomicAppend<'a, V> {
    pending: &'a mut PendingAtomic<V>,
    ticks: OwnedInputTicks<V>,
    limit: Option<usize>,
}

impl<V> Iterator for AtomicAppend<'_, V> {
    type Item = InputBatch<V>;

    fn next(&mut self) -> Option<Self::Item> {
        for tick in self.ticks.by_ref() {
            self.pending.push_tick(tick);
            if self
                .limit
                .is_some_and(|limit| self.pending.updates >= limit)
            {
                return Some(self.pending.take().expect("logical ticks remain valid"));
            }
        }
        None
    }
}

enum WindowAccumulator<V> {
    Batch(PendingBatch<V>),
    Atomic(PendingAtomic<V>),
}

impl<V> WindowAccumulator<V> {
    fn new(stage: InputStage) -> Self {
        match stage {
            InputStage::Batch(_) => Self::Batch(PendingBatch::new()),
            InputStage::WindowToStep {
                reduction: InputReduction::LastUpdateWins,
                ..
            } => Self::Atomic(PendingAtomic::new()),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Batch(pending) => pending.is_empty(),
            Self::Atomic(pending) => pending.is_empty(),
        }
    }

    fn take(&mut self) -> anyhow::Result<InputBatch<V>> {
        match self {
            Self::Batch(pending) => pending.take(),
            Self::Atomic(pending) => pending.take(),
        }
    }

    fn append(&mut self, batch: InputBatch<V>, limit: Option<NonZeroUsize>) -> WindowAppend<'_, V> {
        match self {
            Self::Batch(pending) => WindowAppend::Batch(pending.append(batch, limit)),
            Self::Atomic(pending) => WindowAppend::Atomic(pending.append(batch, limit)),
        }
    }
}

enum WindowAppend<'a, V> {
    Batch(BatchAppend<'a, V>),
    Atomic(AtomicAppend<'a, V>),
}

impl<V> Iterator for WindowAppend<'_, V> {
    type Item = InputBatch<V>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Batch(append) => append.next(),
            Self::Atomic(append) => append.next(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::config::InputWindow;
    use futures::{StreamExt, future};

    fn update(variable: &str, value: i32) -> InputUpdate<i32> {
        InputUpdate::new(variable.into(), value)
    }

    fn window(limit: usize) -> InputWindow {
        InputWindow::new(None, NonZeroUsize::new(limit)).unwrap()
    }

    fn stream(items: Vec<anyhow::Result<InputBatch<i32>>>) -> InputStream<i32> {
        Box::pin(futures::stream::iter(items))
    }

    #[derive(Clone, Copy)]
    struct ImmediateTimer;

    impl InputTimer for ImmediateTimer {
        fn sleep(&self, _duration: Duration) -> LocalBoxFuture<'static, ()> {
            Box::pin(future::ready(()))
        }
    }

    #[derive(Clone, Copy)]
    struct NeverTimer;

    impl InputTimer for NeverTimer {
        fn sleep(&self, _duration: Duration) -> LocalBoxFuture<'static, ()> {
            Box::pin(future::pending())
        }
    }

    #[test]
    fn deadline_flushes_pending_data_without_waiting_for_eof() {
        smol::block_on(async {
            let input: InputStream<i32> = Box::pin(
                futures::stream::iter([Ok(InputBatch::update("x", 1))])
                    .chain(futures::stream::pending()),
            );
            let stage =
                InputStage::Batch(InputWindow::new(Some(Duration::from_secs(1)), None).unwrap());
            let mut output = apply_stage_with_timer(input, stage, ImmediateTimer).unwrap();

            let batch = output.next().await.unwrap().unwrap();
            assert_eq!(batch.update_count(), 1);
        });
    }

    #[test]
    fn update_limit_flushes_before_a_pending_deadline() {
        smol::block_on(async {
            let stage = InputStage::Batch(
                InputWindow::new(Some(Duration::from_secs(1)), NonZeroUsize::new(2)).unwrap(),
            );
            let mut output = apply_stage_with_timer(
                stream(vec![
                    Ok(InputBatch::update("x", 1)),
                    Ok(InputBatch::update("x", 2)),
                ]),
                stage,
                NeverTimer,
            )
            .unwrap();

            let batch = output.next().await.unwrap().unwrap();
            assert_eq!(batch.update_count(), 2);
            assert!(output.next().await.is_none());
        });
    }

    #[test]
    fn batch_limit_flushes_at_tick_boundaries_and_eof_flushes_the_remainder() {
        smol::block_on(async {
            let batch = InputBatch::from_ticks(vec![
                vec![update("x", 1)],
                vec![update("x", 2)],
                vec![update("x", 3)],
            ])
            .unwrap();
            let mut output =
                apply_stage(stream(vec![Ok(batch)]), InputStage::Batch(window(2))).unwrap();

            assert_eq!(output.next().await.unwrap().unwrap().update_count(), 2);
            assert_eq!(output.next().await.unwrap().unwrap().update_count(), 1);
            assert!(output.next().await.is_none());
        });
    }

    #[test]
    fn source_error_flushes_pending_data_before_the_error() {
        smol::block_on(async {
            let mut output = apply_stage(
                stream(vec![
                    Ok(InputBatch::update("x", 1)),
                    Err(anyhow::anyhow!("boom")),
                ]),
                InputStage::Batch(window(2)),
            )
            .unwrap();

            assert_eq!(output.next().await.unwrap().unwrap().update_count(), 1);
            assert_eq!(
                output.next().await.unwrap().unwrap_err().to_string(),
                "boom"
            );
            assert!(output.next().await.is_none());
        });
    }

    #[test]
    fn batch_windows_preserve_packed_chunks() {
        smol::block_on(async {
            let packed = InputBatch::packed_rows(
                vec![VarName::new("x"), VarName::new("y")],
                vec![1, 2, 3, 4, 5, 6],
            )
            .unwrap();
            let mut output =
                apply_stage(stream(vec![Ok(packed)]), InputStage::Batch(window(3))).unwrap();

            let first = output.next().await.unwrap().unwrap();
            assert_eq!(first.update_count(), 4);
            assert!(matches!(
                first.segments().next(),
                Some(InputSegment::PackedRows { .. })
            ));
            let second = output.next().await.unwrap().unwrap();
            assert_eq!(second.update_count(), 2);
            assert!(matches!(
                second.segments().next(),
                Some(InputSegment::PackedRows { .. })
            ));
            assert!(output.next().await.is_none());
        });
    }

    #[test]
    fn atomic_windows_apply_last_update_wins_without_splitting_ticks() {
        smol::block_on(async {
            let batch = InputBatch::from_ticks(vec![
                vec![update("x", 1), update("y", 2)],
                vec![update("x", 3)],
            ])
            .unwrap();
            let stage = InputStage::WindowToStep {
                window: window(3),
                reduction: InputReduction::LastUpdateWins,
            };
            let mut output = apply_stage(stream(vec![Ok(batch)]), stage).unwrap();
            let result = output.next().await.unwrap().unwrap();

            assert_eq!(
                result.ticks().next().unwrap().to_updates(),
                vec![update("x", 3), update("y", 2)]
            );
            assert!(output.next().await.is_none());
        });
    }

    #[test]
    fn oversized_tick_is_atomic_and_flushes_as_one_window() {
        smol::block_on(async {
            let batch =
                InputBatch::tick(vec![update("x", 1), update("y", 2), update("z", 3)]).unwrap();
            let mut output =
                apply_stage(stream(vec![Ok(batch)]), InputStage::Batch(window(2))).unwrap();

            let result = output.next().await.unwrap().unwrap();
            assert_eq!(result.tick_count(), 1);
            assert_eq!(result.update_count(), 3);
            assert!(output.next().await.is_none());
        });
    }

    #[test]
    fn control_barrier_flushes_coalesced_data_before_terminating_the_window() {
        smol::block_on(async {
            let stage = InputStage::WindowToStep {
                window: InputWindow::new(None, NonZeroUsize::new(10)).unwrap(),
                reduction: InputReduction::LastUpdateWins,
            };
            let control = "control";
            let events = Box::pin(futures::stream::iter([
                Ok(WindowEvent::Data(InputBatch::update("x", 1))),
                Ok(WindowEvent::Control(control)),
                Ok(WindowEvent::Data(InputBatch::update("x", 2))),
            ]));
            let mut output = drive_window(events, stage, NeverTimer).unwrap();

            let Some(Ok(WindowEvent::Data(batch))) = output.next().await else {
                panic!("pending data must be flushed before control");
            };
            assert_eq!(
                batch.ticks().next().unwrap().to_updates(),
                vec![update("x", 1)]
            );
            assert!(matches!(
                output.next().await,
                Some(Ok(WindowEvent::Control("control")))
            ));
            assert!(output.next().await.is_none());
        });
    }
}
