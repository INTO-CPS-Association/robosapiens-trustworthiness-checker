use std::{
    cell::RefCell,
    collections::VecDeque,
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
    time::Duration,
};

use async_unsync::{bounded, oneshot};
use futures::{FutureExt, Sink, future::LocalBoxFuture};
use smol::{LocalExecutor, Task};

use crate::core::{OutputBatch, OutputError, OutputWriter};

/// A reliable bounded output queue.
///
/// `max_batches` bounds the number of complete physical batches retained by the
/// stage, including the batch currently admitted to the downstream sink.
/// `max_updates`, when present, is measured over that same queued-plus-in-flight
/// pressure. `Sink::poll_ready` cannot inspect the batch passed to the later
/// `start_send`, so readiness is an admission of one physical batch slot: one
/// indivisible batch may cross the update limit, but no further batch is
/// admitted until pressure drains. When no batch is retained, that same rule
/// permits one batch larger than `max_updates` as the sole oversized-batch
/// exception; this avoids deadlocking an indivisible physical batch.
///
/// The update bound is therefore a one-physical-batch overshoot contract rather
/// than a numeric hard bound on arbitrary batch sizes. A sink API that admits a
/// batch together with its size would be required for a numeric hard next-batch
/// bound without rejecting an already-admitted `start_send`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutputBuffer {
    pub max_batches: NonZeroUsize,
    pub max_updates: Option<NonZeroUsize>,
}

impl OutputBuffer {
    pub fn new(max_batches: usize) -> Result<Self, OutputError> {
        Self::with_limits(max_batches, None)
    }

    pub fn with_limits(
        max_batches: usize,
        max_updates: Option<usize>,
    ) -> Result<Self, OutputError> {
        let Some(max_batches) = NonZeroUsize::new(max_batches) else {
            return Err(OutputError::invalid(
                "output buffer max_batches must be greater than zero",
            ));
        };
        let max_updates = max_updates
            .map(|value| {
                NonZeroUsize::new(value).ok_or_else(|| {
                    OutputError::invalid("output buffer max_updates must be greater than zero")
                })
            })
            .transpose()?;
        Ok(Self {
            max_batches,
            max_updates,
        })
    }

    pub fn capacity(self) -> usize {
        self.max_batches.get()
    }
}

/// Tick-preserving physical coalescing configuration.
///
/// At least one bound must be present. Bounds apply only between physical
/// batches: a simultaneous tick is never split to satisfy a limit. Coalescing
/// never deduplicates updates or applies last-update-wins semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutputCoalescing {
    pub max_delay: Option<Duration>,
    pub tick_limit: Option<NonZeroUsize>,
    pub update_limit: Option<NonZeroUsize>,
}

impl OutputCoalescing {
    pub fn new(max_ticks: usize, max_delay: Option<Duration>) -> Result<Self, OutputError> {
        Self::with_limits(max_delay, Some(max_ticks), None)
    }

    pub fn with_limits(
        max_delay: Option<Duration>,
        tick_limit: Option<usize>,
        update_limit: Option<usize>,
    ) -> Result<Self, OutputError> {
        if max_delay.is_none() && tick_limit.is_none() && update_limit.is_none() {
            return Err(OutputError::invalid(
                "output coalescing requires a delay, tick_limit, or update_limit",
            ));
        }
        let tick_limit = tick_limit
            .map(|value| {
                NonZeroUsize::new(value).ok_or_else(|| {
                    OutputError::invalid("output coalescing tick_limit must be greater than zero")
                })
            })
            .transpose()?;
        let update_limit = update_limit
            .map(|value| {
                NonZeroUsize::new(value).ok_or_else(|| {
                    OutputError::invalid("output coalescing update_limit must be greater than zero")
                })
            })
            .transpose()?;
        Ok(Self {
            max_delay,
            tick_limit,
            update_limit,
        })
    }

    pub fn count(max_ticks: usize) -> Result<Self, OutputError> {
        Self::new(max_ticks, None)
    }

    fn validate(&self) -> Result<(), OutputError> {
        if self.max_delay.is_none() && self.tick_limit.is_none() && self.update_limit.is_none() {
            return Err(OutputError::invalid(
                "output coalescing requires a delay, tick_limit, or update_limit",
            ));
        }
        Ok(())
    }

    pub fn with_delay(mut self, max_delay: Option<Duration>) -> Result<Self, OutputError> {
        self.max_delay = max_delay;
        self.validate().map(|()| self)
    }

    pub fn max_ticks(self) -> Option<NonZeroUsize> {
        self.tick_limit
    }
}

/// An ordered output stage. Arrays are producer-to-consumer; opening wraps
/// sinks in reverse order so `[Coalesce, Buffer]` means coalesce then buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputStage {
    Coalesce(OutputCoalescing),
    Buffer(OutputBuffer),
}

impl OutputStage {
    pub fn buffer(max_batches: usize) -> Result<Self, OutputError> {
        Ok(Self::Buffer(OutputBuffer::new(max_batches)?))
    }

    pub fn buffer_with_limits(
        max_batches: usize,
        max_updates: Option<usize>,
    ) -> Result<Self, OutputError> {
        Ok(Self::Buffer(OutputBuffer::with_limits(
            max_batches,
            max_updates,
        )?))
    }

    pub fn coalesce(max_ticks: usize, max_delay: Option<Duration>) -> Result<Self, OutputError> {
        Ok(Self::Coalesce(OutputCoalescing::new(max_ticks, max_delay)?))
    }

    pub fn coalesce_with_limits(
        max_delay: Option<Duration>,
        tick_limit: Option<usize>,
        update_limit: Option<usize>,
    ) -> Result<Self, OutputError> {
        Ok(Self::Coalesce(OutputCoalescing::with_limits(
            max_delay,
            tick_limit,
            update_limit,
        )?))
    }

    pub(crate) fn apply<V: 'static>(
        self,
        writer: OutputWriter<V>,
        executor: Option<Rc<LocalExecutor<'static>>>,
    ) -> Result<OutputWriter<V>, OutputError> {
        match self {
            Self::Buffer(config) if config.max_updates.is_none() => {
                let executor = executor.ok_or_else(|| {
                    OutputError::invalid("output buffering requires a runtime local executor")
                })?;
                crate::io::output::OutputPump::new(writer, executor, config.max_batches.get())
            }
            Self::Buffer(config) => {
                let interface_reconfiguration = writer.interface_reconfiguration();
                Ok(OutputWriter::from_sink_with_interface_reconfiguration(
                    BufferSink::new(writer, config),
                    interface_reconfiguration,
                ))
            }
            Self::Coalesce(config) if config.max_delay.is_some() => {
                config.validate()?;
                let executor = executor.ok_or_else(|| {
                    OutputError::invalid(
                        "timed output coalescing requires a runtime local executor",
                    )
                })?;
                let interface_reconfiguration = writer.interface_reconfiguration();
                Ok(OutputWriter::from_sink_with_interface_reconfiguration(
                    TimedCoalescingSink::new(writer, config, executor),
                    interface_reconfiguration,
                ))
            }
            Self::Coalesce(config) => {
                config.validate()?;
                let interface_reconfiguration = writer.interface_reconfiguration();
                Ok(OutputWriter::from_sink_with_interface_reconfiguration(
                    CoalescingSink::new(writer, config),
                    interface_reconfiguration,
                ))
            }
        }
    }
}

struct BufferSink<V> {
    downstream: OutputWriter<V>,
    config: OutputBuffer,
    queue: VecDeque<OutputBatch<V>>,
    queued_updates: usize,
    in_flight_updates: usize,
    ready: bool,
    in_flight: bool,
    closing: bool,
    closed: bool,
    downstream_closed: bool,
    failure: Option<OutputError>,
    close_result: Option<Result<(), OutputError>>,
}

impl<V> Unpin for BufferSink<V> {}

impl<V> BufferSink<V> {
    fn new(downstream: OutputWriter<V>, config: OutputBuffer) -> Self {
        Self {
            downstream,
            config,
            queue: VecDeque::new(),
            queued_updates: 0,
            in_flight_updates: 0,
            ready: false,
            in_flight: false,
            closing: false,
            closed: false,
            downstream_closed: false,
            failure: None,
            close_result: None,
        }
    }

    fn state_error(&self) -> Option<OutputError> {
        if let Some(error) = &self.failure {
            return Some(error.clone());
        }
        if self.closing || self.closed {
            return Some(OutputError::Closed);
        }
        None
    }

    fn remember_failure(&mut self, error: OutputError) -> OutputError {
        if self.failure.is_none() {
            self.failure = Some(error);
        }
        self.failure
            .as_ref()
            .expect("buffer failure is retained")
            .clone()
    }

    fn poll_downstream_ready(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), OutputError>> {
        match Pin::new(&mut self.downstream).poll_ready(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                self.in_flight = false;
                self.in_flight_updates = 0;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(self.remember_failure(error))),
        }
    }

    /// Drain queued batches while preserving FIFO order. The downstream writer
    /// is polled as a barrier between each accepted batch, so a failure is
    /// visible before another queued batch is removed.
    fn poll_drain(
        &mut self,
        context: &mut Context<'_>,
        flush_downstream: bool,
    ) -> Poll<Result<(), OutputError>> {
        loop {
            if self.in_flight {
                match self.poll_downstream_ready(context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Ready(Ok(())) => {}
                }
            }

            let Some(batch) = self.queue.pop_front() else {
                if flush_downstream {
                    return match Pin::new(&mut self.downstream).poll_flush(context) {
                        Poll::Pending => Poll::Pending,
                        Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
                        Poll::Ready(Err(error)) => Poll::Ready(Err(self.remember_failure(error))),
                    };
                }
                return Poll::Ready(Ok(()));
            };
            let batch_updates = batch.update_count();
            self.queued_updates = self.queued_updates.saturating_sub(batch_updates);
            match Pin::new(&mut self.downstream).poll_ready(context) {
                Poll::Pending => {
                    self.queue.push_front(batch);
                    self.queued_updates = self.queued_updates.saturating_add(batch_updates);
                    return Poll::Pending;
                }
                Poll::Ready(Err(error)) => {
                    self.queue.push_front(batch);
                    self.queued_updates = self.queued_updates.saturating_add(batch_updates);
                    return Poll::Ready(Err(self.remember_failure(error)));
                }
                Poll::Ready(Ok(())) => {
                    if let Err(error) = Pin::new(&mut self.downstream).start_send(batch) {
                        return Poll::Ready(Err(self.remember_failure(error)));
                    }
                    self.in_flight = true;
                    self.in_flight_updates = batch_updates;
                }
            }
        }
    }

    fn retained_batches(&self) -> usize {
        self.queue.len() + usize::from(self.in_flight)
    }

    fn retained_updates(&self) -> usize {
        self.queued_updates.saturating_add(self.in_flight_updates)
    }

    fn can_accept(&self) -> bool {
        if self.retained_batches() >= self.config.max_batches.get() {
            return false;
        }
        self.config.max_updates.is_none_or(|limit| {
            self.retained_updates() < limit.get() || self.retained_batches() == 0
        })
    }

    fn finish_close(&mut self, cleanup: Result<(), OutputError>) -> Result<(), OutputError> {
        let result = match cleanup {
            Ok(()) => self.failure.clone().map_or(Ok(()), Err),
            Err(cleanup) => match self.failure.clone() {
                Some(primary) => Err(super::combine_errors(primary, cleanup)),
                None => Err(cleanup),
            },
        };
        self.closed = true;
        self.closing = false;
        self.close_result = Some(result.clone());
        result
    }
}

impl<V: 'static> Sink<OutputBatch<V>> for BufferSink<V> {
    type Error = OutputError;

    fn poll_ready(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if let Some(error) = this.state_error() {
            return Poll::Ready(Err(error));
        }
        while !this.can_accept() {
            match this.poll_drain(context, false) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {
                    if this.can_accept() {
                        break;
                    }
                }
            }
        }
        this.ready = true;
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, batch: OutputBatch<V>) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if let Some(error) = this.state_error() {
            return Err(error);
        }
        if !this.ready {
            return Err(this.remember_failure(OutputError::backend(
                "output buffer was not ready for start_send",
            )));
        }
        this.ready = false;
        if !batch.is_empty() {
            this.queued_updates = this.queued_updates.saturating_add(batch.update_count());
            this.queue.push_back(batch);
        }
        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if let Some(error) = this.failure.clone() {
            return Poll::Ready(Err(error));
        }
        this.poll_drain(context, true)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if let Some(result) = &this.close_result {
            return Poll::Ready(result.clone());
        }
        this.closing = true;

        if this.failure.is_none() && (!this.queue.is_empty() || this.in_flight) {
            match this.poll_drain(context, true) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => this.failure = Some(error),
                Poll::Ready(Ok(())) => {}
            }
        }

        if !this.downstream_closed {
            match Pin::new(&mut this.downstream).poll_close(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => this.downstream_closed = true,
                Poll::Ready(Err(error)) => {
                    this.downstream_closed = true;
                    return Poll::Ready(this.finish_close(Err(error)));
                }
            }
        }
        Poll::Ready(this.finish_close(Ok(())))
    }
}

struct CoalescingSink<V> {
    downstream: OutputWriter<V>,
    config: OutputCoalescing,
    pending: Option<OutputBatch<V>>,
    pending_ticks: usize,
    pending_updates: usize,
    timer: Option<LocalBoxFuture<'static, ()>>,
    ready: bool,
    in_flight: bool,
    closing: bool,
    closed: bool,
    downstream_closed: bool,
    failure: Option<OutputError>,
    close_result: Option<Result<(), OutputError>>,
}

impl<V> Unpin for CoalescingSink<V> {}

impl<V> CoalescingSink<V> {
    fn new(downstream: OutputWriter<V>, config: OutputCoalescing) -> Self {
        Self {
            downstream,
            config,
            pending: None,
            pending_ticks: 0,
            pending_updates: 0,
            timer: None,
            ready: false,
            in_flight: false,
            closing: false,
            closed: false,
            downstream_closed: false,
            failure: None,
            close_result: None,
        }
    }

    fn state_error(&self) -> Option<OutputError> {
        if let Some(error) = &self.failure {
            return Some(error.clone());
        }
        if self.closing || self.closed {
            return Some(OutputError::Closed);
        }
        None
    }

    fn remember_failure(&mut self, error: OutputError) -> OutputError {
        if self.failure.is_none() {
            self.failure = Some(error);
        }
        self.failure
            .as_ref()
            .expect("coalescing failure is retained")
            .clone()
    }

    fn timer_ready(&mut self, context: &mut Context<'_>) -> bool {
        let Some(timer) = self.timer.as_mut() else {
            return false;
        };
        match Pin::new(timer).poll(context) {
            Poll::Pending => false,
            Poll::Ready(()) => {
                self.timer = None;
                true
            }
        }
    }

    fn limit_reached(&self) -> bool {
        self.config
            .tick_limit
            .is_some_and(|limit| self.pending_ticks >= limit.get())
            || self
                .config
                .update_limit
                .is_some_and(|limit| self.pending_updates >= limit.get())
    }

    fn append(&mut self, batch: OutputBatch<V>) -> Result<(), OutputError> {
        if batch.is_empty() {
            return Ok(());
        }
        let ticks = batch.tick_count();
        let updates = batch.update_count();
        if let Some(pending) = self.pending.as_mut() {
            pending.append(batch)?;
        } else {
            if let Some(delay) = self.config.max_delay {
                self.timer = Some(Box::pin(async move {
                    smol::Timer::after(delay).await;
                }));
            }
            self.pending = Some(batch);
        }
        self.pending_ticks = self.pending_ticks.saturating_add(ticks);
        self.pending_updates = self.pending_updates.saturating_add(updates);
        Ok(())
    }

    fn poll_emit(
        &mut self,
        context: &mut Context<'_>,
        force: bool,
        timer_ready: bool,
    ) -> Poll<Result<(), OutputError>> {
        if self.in_flight {
            match Pin::new(&mut self.downstream).poll_ready(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(self.remember_failure(error))),
                Poll::Ready(Ok(())) => self.in_flight = false,
            }
        }
        let due = force || timer_ready || self.limit_reached();
        if !due {
            return Poll::Ready(Ok(()));
        }
        let Some(batch) = self.pending.take() else {
            return Poll::Ready(Ok(()));
        };
        let pending_ticks = self.pending_ticks;
        let pending_updates = self.pending_updates;
        let timer = self.timer.take();
        self.pending_ticks = 0;
        self.pending_updates = 0;
        self.timer = None;
        match Pin::new(&mut self.downstream).poll_ready(context) {
            Poll::Pending => {
                self.pending = Some(batch);
                self.pending_ticks = pending_ticks;
                self.pending_updates = pending_updates;
                self.timer = if timer_ready {
                    Some(Box::pin(async {}))
                } else {
                    timer
                };
                Poll::Pending
            }
            Poll::Ready(Err(error)) => {
                self.pending = Some(batch);
                self.pending_ticks = pending_ticks;
                self.pending_updates = pending_updates;
                self.timer = if timer_ready {
                    Some(Box::pin(async {}))
                } else {
                    timer
                };
                Poll::Ready(Err(self.remember_failure(error)))
            }
            Poll::Ready(Ok(())) => {
                if let Err(error) = Pin::new(&mut self.downstream).start_send(batch) {
                    return Poll::Ready(Err(self.remember_failure(error)));
                }
                self.in_flight = true;
                Poll::Ready(Ok(()))
            }
        }
    }

    fn finish_close(&mut self, cleanup: Result<(), OutputError>) -> Result<(), OutputError> {
        let result = match cleanup {
            Ok(()) => self.failure.clone().map_or(Ok(()), Err),
            Err(cleanup) => match self.failure.clone() {
                Some(primary) => Err(super::combine_errors(primary, cleanup)),
                None => Err(cleanup),
            },
        };
        self.closed = true;
        self.closing = false;
        self.close_result = Some(result.clone());
        result
    }
}

impl<V: 'static> Sink<OutputBatch<V>> for CoalescingSink<V> {
    type Error = OutputError;

    fn poll_ready(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if let Some(error) = this.state_error() {
            return Poll::Ready(Err(error));
        }
        let timer_ready = this.timer_ready(context);
        match this.poll_emit(context, false, timer_ready) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {
                this.ready = true;
                Poll::Ready(Ok(()))
            }
        }
    }

    fn start_send(self: Pin<&mut Self>, batch: OutputBatch<V>) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if let Some(error) = this.state_error() {
            return Err(error);
        }
        if !this.ready {
            return Err(this.remember_failure(OutputError::backend(
                "output coalescer was not ready for start_send",
            )));
        }
        this.ready = false;
        if let Err(error) = this.append(batch) {
            return Err(this.remember_failure(error));
        }
        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if let Some(error) = this.failure.clone() {
            return Poll::Ready(Err(error));
        }
        let timer_ready = this.timer_ready(context);
        match this.poll_emit(context, true, timer_ready) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => match Pin::new(&mut this.downstream).poll_flush(context) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
                Poll::Ready(Err(error)) => Poll::Ready(Err(this.remember_failure(error))),
            },
        }
    }

    fn poll_close(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if let Some(result) = &this.close_result {
            return Poll::Ready(result.clone());
        }
        this.closing = true;

        if this.failure.is_none() && (this.pending.is_some() || this.in_flight) {
            let timer_ready = this.timer_ready(context);
            match this.poll_emit(context, true, timer_ready) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => this.failure = Some(error),
                Poll::Ready(Ok(())) => {}
            }
            if this.failure.is_none() {
                match Pin::new(&mut this.downstream).poll_flush(context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => this.failure = Some(error),
                    Poll::Ready(Ok(())) => {}
                }
            }
        }

        if !this.downstream_closed {
            match Pin::new(&mut this.downstream).poll_close(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => this.downstream_closed = true,
                Poll::Ready(Err(error)) => {
                    this.downstream_closed = true;
                    return Poll::Ready(this.finish_close(Err(error)));
                }
            }
        }
        Poll::Ready(this.finish_close(Ok(())))
    }
}

type TimedCommandSender<V> = bounded::Sender<TimedCommand<V>>;
type TimedCommandPermit<V> = bounded::OwnedPermit<TimedCommand<V>>;
type TimedReservation<V> = LocalBoxFuture<'static, Result<TimedCommandPermit<V>, OutputError>>;

enum TimedCommand<V> {
    Batch(OutputBatch<V>),
    Flush(oneshot::Sender<Result<(), OutputError>>),
    Close(oneshot::Sender<Result<(), OutputError>>),
}

#[derive(Default)]
struct TimedWorkerState {
    error: Option<OutputError>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TimedBarrierKind {
    Flush,
    Close,
}

struct TimedBarrier<V> {
    kind: TimedBarrierKind,
    reply: Option<oneshot::Sender<Result<(), OutputError>>>,
    response: Option<oneshot::Receiver<Result<(), OutputError>>>,
    reservation: Option<TimedReservation<V>>,
    result: Option<Result<(), OutputError>>,
}

impl<V: 'static> TimedBarrier<V> {
    fn new(kind: TimedBarrierKind, sender: &TimedCommandSender<V>) -> Self {
        let (reply, response) = oneshot::channel().into_split();
        Self {
            kind,
            reply: Some(reply),
            response: Some(response),
            reservation: Some(timed_reserve(sender.clone())),
            result: None,
        }
    }
}

#[derive(Clone, Debug)]
enum TimedState {
    Open,
    Closing,
    Closed,
    Failed(OutputError),
}

/// Worker-backed coalescing used when a delay bound is configured. Keeping the
/// downstream writer in the worker is what lets the timer publish a pending
/// physical batch while the producer is idle; the producer-facing sink need
/// not be polled again to make the deadline effective.
struct TimedCoalescingSink<V> {
    worker: Option<Task<()>>,
    sender: Option<TimedCommandSender<V>>,
    worker_state: Rc<RefCell<TimedWorkerState>>,
    ready_permit: Option<TimedCommandPermit<V>>,
    reservation: Option<TimedReservation<V>>,
    barrier: Option<TimedBarrier<V>>,
    state: TimedState,
    failure_from_worker: bool,
}

impl<V: 'static> TimedCoalescingSink<V> {
    fn new(
        writer: OutputWriter<V>,
        config: OutputCoalescing,
        executor: Rc<LocalExecutor<'static>>,
    ) -> Self {
        let (sender, receiver) = bounded::channel(1).into_split();
        let worker_state = Rc::new(RefCell::new(TimedWorkerState::default()));
        let worker = executor.spawn(run_timed_worker(
            writer,
            receiver,
            config,
            Rc::clone(&worker_state),
        ));
        Self {
            worker: Some(worker),
            sender: Some(sender),
            worker_state,
            ready_permit: None,
            reservation: None,
            barrier: None,
            state: TimedState::Open,
            failure_from_worker: false,
        }
    }

    fn worker_error(&self) -> Option<OutputError> {
        self.worker_state.borrow().error.clone()
    }

    fn state_error(&self) -> Option<OutputError> {
        match &self.state {
            TimedState::Open => None,
            TimedState::Closing | TimedState::Closed => Some(OutputError::Closed),
            TimedState::Failed(error) => Some(error.clone()),
        }
    }

    fn fail(&mut self, error: OutputError) -> OutputError {
        self.failure_from_worker = false;
        self.state = TimedState::Failed(error.clone());
        error
    }

    fn fail_from_worker(&mut self, error: OutputError) -> OutputError {
        self.failure_from_worker = true;
        self.state = TimedState::Failed(error.clone());
        error
    }

    /// Drop producer-side channel handles so a failed worker can observe
    /// receiver termination even when no close barrier was sent.
    fn terminate_worker(&mut self) {
        self.ready_permit = None;
        self.reservation = None;
        self.barrier = None;
        self.sender = None;
    }

    fn poll_worker(&mut self, context: &mut Context<'_>) {
        let completed = self
            .worker
            .as_mut()
            .is_some_and(|worker| Pin::new(worker).poll(context).is_ready());
        if completed {
            self.worker = None;
        }
    }

    fn poll_open_worker(&mut self, context: &mut Context<'_>) -> Result<(), OutputError> {
        self.poll_worker(context);
        if let Some(error) = self.worker_error() {
            return Err(self.fail_from_worker(error));
        }
        if self.worker.is_none() {
            return Err(self.fail(OutputError::Closed));
        }
        Ok(())
    }

    fn reserve(&mut self) {
        if self.reservation.is_none() && self.ready_permit.is_none() {
            let sender = self
                .sender
                .as_ref()
                .expect("open timed coalescer has a command sender")
                .clone();
            self.reservation = Some(timed_reserve(sender));
        }
    }

    fn poll_reservation(&mut self, context: &mut Context<'_>) -> Poll<Result<(), OutputError>> {
        self.reserve();
        let reservation = self
            .reservation
            .as_mut()
            .expect("timed coalescing reservation exists");
        match reservation.as_mut().poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(permit)) => {
                self.reservation = None;
                self.ready_permit = Some(permit);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => {
                self.reservation = None;
                let worker_error = self.worker_error();
                Poll::Ready(Err(match worker_error {
                    Some(error) => self.fail_from_worker(error),
                    None => self.fail(error),
                }))
            }
        }
    }

    fn poll_barrier_reply(
        &mut self,
        barrier: &mut TimedBarrier<V>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), OutputError>> {
        if let Some(reservation) = barrier.reservation.as_mut() {
            match reservation.as_mut().poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(permit)) => {
                    barrier.reservation = None;
                    let reply = barrier.reply.take().expect("barrier reply is sent once");
                    let command = match barrier.kind {
                        TimedBarrierKind::Flush => TimedCommand::Flush(reply),
                        TimedBarrierKind::Close => TimedCommand::Close(reply),
                    };
                    let _permit = permit.send(command);
                }
                Poll::Ready(Err(error)) => {
                    barrier.reservation = None;
                    barrier.result = Some(Err(self.worker_error().unwrap_or(error)));
                }
            }
        }
        if barrier.result.is_none() {
            let Some(response) = barrier.response.as_mut() else {
                return Poll::Pending;
            };
            match Pin::new(response).poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(result)) => {
                    barrier.response = None;
                    barrier.result = Some(result);
                }
                Poll::Ready(Err(_)) => {
                    barrier.response = None;
                    barrier.result = Some(Err(self.worker_error().unwrap_or(OutputError::Closed)));
                }
            }
        }
        Poll::Ready(barrier.result.take().expect("barrier has a result"))
    }

    fn poll_flush_barrier(&mut self, context: &mut Context<'_>) -> Poll<Result<(), OutputError>> {
        let mut barrier = self
            .barrier
            .take()
            .expect("flush barrier is installed before polling");
        match self.poll_barrier_reply(&mut barrier, context) {
            Poll::Pending => {
                self.barrier = Some(barrier);
                Poll::Pending
            }
            result => result,
        }
    }

    fn poll_close_barrier(&mut self, context: &mut Context<'_>) -> Poll<Result<(), OutputError>> {
        let mut barrier = self
            .barrier
            .take()
            .expect("close barrier is installed before polling");
        let result = self.poll_barrier_reply(&mut barrier, context);
        if let Poll::Ready(result) = result {
            barrier.result = Some(result);
        } else {
            self.barrier = Some(barrier);
            return Poll::Pending;
        }
        self.poll_worker(context);
        if self.worker.is_some() {
            self.barrier = Some(barrier);
            return Poll::Pending;
        }
        let result = match (self.worker_error(), barrier.result.take()) {
            (Some(error), _) => Err(error),
            (None, Some(result)) => result,
            (None, None) => Err(OutputError::Closed),
        };
        match result {
            Ok(()) => {
                self.state = TimedState::Closed;
                Poll::Ready(Ok(()))
            }
            Err(error) => Poll::Ready(Err(self.fail(error))),
        }
    }
}

impl<V> Unpin for TimedCoalescingSink<V> {}

impl<V: 'static> Sink<OutputBatch<V>> for TimedCoalescingSink<V> {
    type Error = OutputError;

    fn poll_ready(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if let Some(error) = this.state_error() {
            return Poll::Ready(Err(error));
        }
        if let Err(error) = this.poll_open_worker(context) {
            return Poll::Ready(Err(error));
        }
        if this.barrier.is_some() {
            return Poll::Ready(Err(this.fail(OutputError::backend(
                "timed output coalescer has a barrier in progress",
            ))));
        }
        if this.ready_permit.is_some() {
            return Poll::Ready(Ok(()));
        }
        this.poll_reservation(context)
    }

    fn start_send(self: Pin<&mut Self>, batch: OutputBatch<V>) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if let Some(error) = this.state_error() {
            return Err(error);
        }
        // A readiness permit can outlive the worker's last poll. Check the
        // shared sticky state immediately before consuming the permit so a
        // batch is never reported as accepted after a timer-side failure.
        if let Some(error) = this.worker_error() {
            this.ready_permit = None;
            return Err(this.fail_from_worker(error));
        }
        if this.worker.is_none() {
            this.ready_permit = None;
            return Err(this.fail(OutputError::Closed));
        }
        let Some(permit) = this.ready_permit.take() else {
            return Err(this.fail(OutputError::backend(
                "timed output coalescer was not ready for start_send",
            )));
        };
        if let Some(error) = this.worker_error() {
            return Err(this.fail_from_worker(error));
        }
        if this.worker.is_none() {
            return Err(this.fail(OutputError::Closed));
        }
        let _permit = permit.send(TimedCommand::Batch(batch));
        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if let Some(error) = this.state_error() {
            if matches!(this.state, TimedState::Failed(_)) {
                this.poll_worker(context);
            }
            return Poll::Ready(Err(error));
        }
        if let Err(error) = this.poll_open_worker(context) {
            return Poll::Ready(Err(error));
        }
        if this.barrier.is_none() {
            this.ready_permit = None;
            this.reservation = None;
            let sender = this
                .sender
                .as_ref()
                .expect("open timed coalescer has a command sender");
            this.barrier = Some(TimedBarrier::new(TimedBarrierKind::Flush, sender));
        }
        this.poll_flush_barrier(context)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if matches!(this.state, TimedState::Closed) {
            return Poll::Ready(Err(OutputError::Closed));
        }
        if let TimedState::Failed(error) = &this.state {
            let state_error = error.clone();
            let failure_from_worker = this.failure_from_worker;
            // A local failure must close the command channel before waiting
            // for a worker that may be blocked in `receiver.recv()`.
            this.terminate_worker();
            this.poll_worker(context);
            if this.worker.is_some() {
                return Poll::Pending;
            }
            let error = match this.worker_error() {
                Some(worker_error) if failure_from_worker => worker_error,
                Some(cleanup) => super::combine_errors(state_error, cleanup),
                None => state_error,
            };
            return Poll::Ready(Err(error));
        }
        if matches!(this.state, TimedState::Open) {
            this.poll_worker(context);
            if this.worker.is_none() {
                let worker_error = this.worker_error();
                let error = worker_error.clone().unwrap_or(OutputError::Closed);
                return Poll::Ready(Err(match worker_error {
                    Some(error) => this.fail_from_worker(error),
                    None => this.fail(error),
                }));
            }
            this.ready_permit = None;
            this.reservation = None;
            this.state = TimedState::Closing;
        }
        if this
            .barrier
            .as_ref()
            .is_some_and(|barrier| barrier.kind == TimedBarrierKind::Flush)
        {
            match this.poll_flush_barrier(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    // A failed flush makes the worker perform its own final
                    // close; wait for that cleanup rather than returning early.
                    this.failure_from_worker = this.worker_error().is_some();
                    this.state = TimedState::Failed(error.clone());
                    this.poll_worker(context);
                    if this.worker.is_some() {
                        return Poll::Pending;
                    }
                    let error = this.worker_error().unwrap_or(error);
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(Ok(())) => {}
            }
        }
        if this.barrier.is_none() {
            let sender = this
                .sender
                .as_ref()
                .expect("closing timed coalescer has a command sender");
            this.barrier = Some(TimedBarrier::new(TimedBarrierKind::Close, sender));
        }
        this.poll_close_barrier(context)
    }
}

fn timed_reserve<V: 'static>(sender: TimedCommandSender<V>) -> TimedReservation<V> {
    Box::pin(async move {
        sender
            .reserve_owned()
            .await
            .map_err(|_| OutputError::Closed)
    })
}

fn record_timed_error(state: &Rc<RefCell<TimedWorkerState>>, error: OutputError) {
    let mut state = state.borrow_mut();
    state.error = Some(match state.error.take() {
        Some(primary) => super::combine_errors(primary, error),
        None => error,
    });
}

async fn close_timed_writer<V>(
    writer: &mut OutputWriter<V>,
    state: &Rc<RefCell<TimedWorkerState>>,
) {
    let writer_had_error = writer.error().is_some();
    if let Err(error) = writer.close().await {
        let mut state = state.borrow_mut();
        if writer_had_error {
            // `OutputWriter::close` already combines its retained operation
            // error with the downstream close error. Do not combine that
            // retained primary a second time.
            state.error = Some(error);
        } else {
            state.error = Some(match state.error.take() {
                Some(primary) => super::combine_errors(primary, error),
                None => error,
            });
        }
    }
}

async fn fail_timed_worker<V>(
    writer: &mut OutputWriter<V>,
    state: &Rc<RefCell<TimedWorkerState>>,
    error: OutputError,
) {
    let writer_had_error = writer.error().is_some();
    if !writer_had_error {
        record_timed_error(state, error.clone());
    }
    close_timed_writer(writer, state).await;
    if writer_had_error && state.borrow().error.is_none() {
        record_timed_error(state, error);
    }
}

async fn emit_timed_pending<V>(
    writer: &mut OutputWriter<V>,
    pending: &mut Option<OutputBatch<V>>,
    ticks: &mut usize,
    updates: &mut usize,
) -> Result<(), OutputError> {
    let Some(batch) = pending.take() else {
        return Ok(());
    };
    *ticks = 0;
    *updates = 0;
    // `send` drives the operation started by `feed` through the next
    // downstream readiness point without calling `flush`. This makes timer
    // and count emissions complete an asynchronous backend while preserving
    // coalescing across producer submissions.
    writer.send(batch).await
}

fn timed_result(state: &Rc<RefCell<TimedWorkerState>>) -> Result<(), OutputError> {
    state.borrow().error.clone().map_or(Ok(()), Err)
}

async fn handle_timed_command<V: 'static>(
    command: Option<TimedCommand<V>>,
    writer: &mut OutputWriter<V>,
    config: OutputCoalescing,
    state: &Rc<RefCell<TimedWorkerState>>,
    pending: &mut Option<OutputBatch<V>>,
    pending_ticks: &mut usize,
    pending_updates: &mut usize,
    timer: &mut Option<LocalBoxFuture<'static, ()>>,
) -> bool {
    let Some(command) = command else {
        close_timed_writer(writer, state).await;
        return false;
    };
    match command {
        TimedCommand::Batch(batch) => {
            if batch.is_empty() {
                return true;
            }
            if pending.is_none() {
                if let Some(delay) = config.max_delay {
                    *timer = Some(Box::pin(async move {
                        smol::Timer::after(delay).await;
                    }));
                }
                *pending = Some(batch);
            } else if let Some(current) = pending.as_mut() {
                if let Err(error) = current.append(batch) {
                    fail_timed_worker(writer, state, error).await;
                    return false;
                }
            }
            *pending_ticks = pending_ticks.saturating_add(
                pending
                    .as_ref()
                    .map_or(0, OutputBatch::tick_count)
                    .saturating_sub(*pending_ticks),
            );
            *pending_updates = pending_updates.saturating_add(
                pending
                    .as_ref()
                    .map_or(0, OutputBatch::update_count)
                    .saturating_sub(*pending_updates),
            );
            let limit_reached = config
                .tick_limit
                .is_some_and(|limit| *pending_ticks >= limit.get())
                || config
                    .update_limit
                    .is_some_and(|limit| *pending_updates >= limit.get());
            if limit_reached {
                *timer = None;
                if let Err(error) =
                    emit_timed_pending(writer, pending, pending_ticks, pending_updates).await
                {
                    fail_timed_worker(writer, state, error).await;
                    return false;
                }
            }
            true
        }
        TimedCommand::Flush(reply) => {
            let result = async {
                emit_timed_pending(writer, pending, pending_ticks, pending_updates).await?;
                writer.flush().await
            }
            .await;
            if let Err(error) = &result {
                fail_timed_worker(writer, state, error.clone()).await;
            }
            *timer = None;
            let keep_running = result.is_ok();
            let _ = reply.send(result);
            keep_running
        }
        TimedCommand::Close(reply) => {
            // Cleanup is best effort but ordered: even a pending-emission or
            // flush failure must not prevent the downstream close attempt.
            let emission =
                emit_timed_pending(writer, pending, pending_ticks, pending_updates).await;
            let flush = writer.flush().await;
            let close = writer.close().await;
            match close {
                Err(error) => {
                    // `OutputWriter::close` includes any retained emission or
                    // flush error in this result.
                    state.borrow_mut().error = Some(error);
                }
                Ok(()) => {
                    if let Err(error) = emission {
                        record_timed_error(state, error);
                    }
                    if let Err(error) = flush {
                        record_timed_error(state, error);
                    }
                }
            }
            *timer = None;
            let result = timed_result(state);
            let _ = reply.send(result);
            false
        }
    }
}

async fn run_timed_worker<V: 'static>(
    mut writer: OutputWriter<V>,
    mut receiver: bounded::Receiver<TimedCommand<V>>,
    config: OutputCoalescing,
    state: Rc<RefCell<TimedWorkerState>>,
) {
    let mut pending = None;
    let mut pending_ticks = 0;
    let mut pending_updates = 0;
    let mut timer: Option<LocalBoxFuture<'static, ()>> = None;

    loop {
        let keep_running = if let Some(mut deadline) = timer.take() {
            futures::select_biased! {
                () = deadline.as_mut().fuse() => {
                    match emit_timed_pending(
                        &mut writer,
                        &mut pending,
                        &mut pending_ticks,
                        &mut pending_updates,
                    ).await {
                        Ok(()) => true,
                        Err(error) => {
                            fail_timed_worker(&mut writer, &state, error).await;
                            false
                        }
                    }
                }
                command = receiver.recv().fuse() => {
                    timer = Some(deadline);
                    handle_timed_command(
                        command,
                        &mut writer,
                        config,
                        &state,
                        &mut pending,
                        &mut pending_ticks,
                        &mut pending_updates,
                        &mut timer,
                    ).await
                }
            }
        } else {
            handle_timed_command(
                receiver.recv().await,
                &mut writer,
                config,
                &state,
                &mut pending,
                &mut pending_ticks,
                &mut pending_updates,
                &mut timer,
            )
            .await
        };
        if !keep_running {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        pin::Pin,
        rc::Rc,
        task::{Context, Poll},
        time::Duration,
    };

    use futures::{
        Sink,
        future::{join, poll_fn},
        task::noop_waker_ref,
    };

    use super::*;
    use crate::core::{OutputUpdate, VarName};

    #[derive(Clone)]
    struct Recording {
        batches: Rc<RefCell<Vec<OutputBatch<i32>>>>,
        closes: Rc<RefCell<usize>>,
    }

    struct RecordingSink {
        recording: Recording,
        ready: bool,
        closed: bool,
    }

    impl Sink<OutputBatch<i32>> for RecordingSink {
        type Error = OutputError;

        fn poll_ready(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            if self.closed {
                return Poll::Ready(Err(OutputError::Closed));
            }
            self.ready = true;
            Poll::Ready(Ok(()))
        }

        fn start_send(
            mut self: Pin<&mut Self>,
            batch: OutputBatch<i32>,
        ) -> Result<(), Self::Error> {
            if !self.ready {
                return Err(OutputError::backend("recording sink was not ready"));
            }
            self.recording.batches.borrow_mut().push(batch);
            self.ready = false;
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            self.closed = true;
            *self.recording.closes.borrow_mut() += 1;
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Clone)]
    struct Controlled {
        pending: Rc<Cell<bool>>,
        fail_ready: Rc<Cell<bool>>,
        batches: Rc<RefCell<Vec<OutputBatch<i32>>>>,
        closes: Rc<Cell<usize>>,
    }

    struct ControlledSink {
        controlled: Controlled,
        ready: bool,
        closed: bool,
    }

    impl Sink<OutputBatch<i32>> for ControlledSink {
        type Error = OutputError;

        fn poll_ready(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            if self.closed {
                return Poll::Ready(Err(OutputError::Closed));
            }
            if self.controlled.fail_ready.replace(false) {
                return Poll::Ready(Err(OutputError::backend("controlled readiness failure")));
            }
            if self.controlled.pending.get() {
                return Poll::Pending;
            }
            self.ready = true;
            Poll::Ready(Ok(()))
        }

        fn start_send(
            mut self: Pin<&mut Self>,
            batch: OutputBatch<i32>,
        ) -> Result<(), Self::Error> {
            if !self.ready {
                return Err(OutputError::backend("controlled sink was not ready"));
            }
            self.controlled.batches.borrow_mut().push(batch);
            self.ready = false;
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            self.closed = true;
            self.controlled.closes.set(self.controlled.closes.get() + 1);
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Clone)]
    struct InFlightControl {
        block_after_send: Rc<Cell<bool>>,
        sent: Rc<Cell<usize>>,
    }

    #[derive(Clone, Default)]
    struct FailureGate {
        open: Rc<Cell<bool>>,
        started: Rc<Cell<bool>>,
        waker: Rc<RefCell<Option<std::task::Waker>>>,
    }

    impl FailureGate {
        fn release(&self) {
            self.open.set(true);
            if let Some(waker) = self.waker.borrow_mut().take() {
                waker.wake();
            }
        }
    }

    struct InFlightSink {
        control: InFlightControl,
        ready: bool,
        closed: bool,
    }

    impl Sink<OutputBatch<i32>> for InFlightSink {
        type Error = OutputError;

        fn poll_ready(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            if self.closed {
                return Poll::Ready(Err(OutputError::Closed));
            }
            if self.control.block_after_send.get() && self.control.sent.get() > 0 {
                return Poll::Pending;
            }
            self.ready = true;
            Poll::Ready(Ok(()))
        }

        fn start_send(
            mut self: Pin<&mut Self>,
            _batch: OutputBatch<i32>,
        ) -> Result<(), Self::Error> {
            if !self.ready {
                return Err(OutputError::backend("in-flight sink was not ready"));
            }
            self.ready = false;
            self.control.sent.set(self.control.sent.get() + 1);
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            self.closed = true;
            Poll::Ready(Ok(()))
        }
    }

    struct CleanupSink {
        fail_send: bool,
        fail_flush: bool,
        fail_close: bool,
        ready: bool,
        closed: bool,
        closes: Rc<Cell<usize>>,
    }

    impl Sink<OutputBatch<i32>> for CleanupSink {
        type Error = OutputError;

        fn poll_ready(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            if self.closed {
                return Poll::Ready(Err(OutputError::Closed));
            }
            self.ready = true;
            Poll::Ready(Ok(()))
        }

        fn start_send(
            mut self: Pin<&mut Self>,
            _batch: OutputBatch<i32>,
        ) -> Result<(), Self::Error> {
            if self.fail_send {
                return Err(OutputError::backend("timed worker failure"));
            }
            if !self.ready {
                return Err(OutputError::backend("cleanup sink was not ready"));
            }
            self.ready = false;
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            if self.fail_flush {
                Poll::Ready(Err(OutputError::backend("timed flush failure")))
            } else {
                Poll::Ready(Ok(()))
            }
        }

        fn poll_close(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            self.closed = true;
            self.closes.set(self.closes.get() + 1);
            if self.fail_close {
                Poll::Ready(Err(OutputError::backend("timed close failure")))
            } else {
                Poll::Ready(Ok(()))
            }
        }
    }

    fn batch(value: i32) -> OutputBatch<i32> {
        OutputBatch::update(VarName::new("x"), value)
    }

    fn wide_batch(width: usize) -> OutputBatch<i32> {
        const VARIABLES: [&str; 4] = ["x", "y", "z", "w"];
        OutputBatch::tick(
            (0..width)
                .map(|index| OutputUpdate::new(VarName::new(VARIABLES[index]), index as i32))
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn buffer_update_limit_documents_one_batch_crossing_and_drains_before_more() {
        let controlled = Controlled {
            pending: Rc::new(Cell::new(true)),
            fail_ready: Rc::new(Cell::new(false)),
            batches: Rc::new(RefCell::new(Vec::new())),
            closes: Rc::new(Cell::new(0)),
        };
        let downstream = OutputWriter::from_sink(ControlledSink {
            controlled: controlled.clone(),
            ready: false,
            closed: false,
        });
        let mut sink = BufferSink::new(downstream, OutputBuffer::with_limits(4, Some(3)).unwrap());
        let mut context = Context::from_waker(noop_waker_ref());

        assert!(Pin::new(&mut sink).poll_ready(&mut context).is_ready());
        Pin::new(&mut sink).start_send(wide_batch(2)).unwrap();
        assert!(Pin::new(&mut sink).poll_ready(&mut context).is_ready());
        Pin::new(&mut sink).start_send(wide_batch(2)).unwrap();
        assert_eq!(sink.queued_updates, 4);
        assert_eq!(sink.in_flight_updates, 0);

        // `poll_ready` cannot see the next batch's size. The single admitted
        // physical batch may cross the threshold, but another one waits for
        // all retained pressure to drain.
        assert!(Pin::new(&mut sink).poll_ready(&mut context).is_pending());
        assert_eq!(controlled.batches.borrow().len(), 0);
        controlled.pending.set(false);
        assert!(Pin::new(&mut sink).poll_ready(&mut context).is_ready());
        assert_eq!(controlled.batches.borrow().len(), 2);
        assert_eq!(sink.retained_updates(), 0);
    }

    #[test]
    fn buffer_allows_one_oversized_batch_only_when_empty() {
        let controlled = Controlled {
            pending: Rc::new(Cell::new(true)),
            fail_ready: Rc::new(Cell::new(false)),
            batches: Rc::new(RefCell::new(Vec::new())),
            closes: Rc::new(Cell::new(0)),
        };
        let downstream = OutputWriter::from_sink(ControlledSink {
            controlled: controlled.clone(),
            ready: false,
            closed: false,
        });
        let mut sink = BufferSink::new(downstream, OutputBuffer::with_limits(2, Some(2)).unwrap());
        let mut context = Context::from_waker(noop_waker_ref());

        assert!(Pin::new(&mut sink).poll_ready(&mut context).is_ready());
        Pin::new(&mut sink).start_send(wide_batch(3)).unwrap();
        assert_eq!(sink.queued_updates, 3);
        assert!(Pin::new(&mut sink).poll_ready(&mut context).is_pending());

        controlled.pending.set(false);
        assert!(Pin::new(&mut sink).poll_ready(&mut context).is_ready());
        assert_eq!(sink.retained_updates(), 0);
    }

    #[test]
    fn buffer_update_limit_includes_an_in_flight_batch() {
        let control = InFlightControl {
            block_after_send: Rc::new(Cell::new(false)),
            sent: Rc::new(Cell::new(0)),
        };
        let downstream = OutputWriter::from_sink(InFlightSink {
            control: control.clone(),
            ready: false,
            closed: false,
        });
        let mut sink = BufferSink::new(downstream, OutputBuffer::with_limits(2, Some(3)).unwrap());
        let mut context = Context::from_waker(noop_waker_ref());

        assert!(Pin::new(&mut sink).poll_ready(&mut context).is_ready());
        Pin::new(&mut sink).start_send(wide_batch(3)).unwrap();
        control.block_after_send.set(true);
        assert!(Pin::new(&mut sink).poll_ready(&mut context).is_pending());
        assert_eq!(sink.in_flight_updates, 3);
        assert_eq!(sink.retained_updates(), 3);

        control.block_after_send.set(false);
        assert!(Pin::new(&mut sink).poll_ready(&mut context).is_ready());
        assert_eq!(sink.in_flight_updates, 0);
        assert_eq!(sink.retained_updates(), 0);
    }

    #[test]
    fn coalescing_preserves_ticks_and_flushes_on_barrier() {
        smol::block_on(async {
            let recording = Recording {
                batches: Rc::new(RefCell::new(Vec::new())),
                closes: Rc::new(RefCell::new(0)),
            };
            let mut writer = OutputWriter::from_sink(CoalescingSink::new(
                OutputWriter::from_sink(RecordingSink {
                    recording: recording.clone(),
                    ready: false,
                    closed: false,
                }),
                OutputCoalescing::with_limits(None, Some(8), Some(8)).unwrap(),
            ));
            writer.send(batch(1)).await.unwrap();
            writer.send(batch(2)).await.unwrap();
            assert!(recording.batches.borrow().is_empty());
            writer.flush().await.unwrap();
            assert_eq!(recording.batches.borrow().len(), 1);
            assert_eq!(recording.batches.borrow()[0].tick_count(), 2);
            writer.close().await.unwrap();
            assert_eq!(*recording.closes.borrow(), 1);
        });
    }

    #[test]
    fn coalescing_restores_limits_and_timer_when_downstream_is_pending_or_fails() {
        smol::block_on(async {
            let controlled = Controlled {
                pending: Rc::new(Cell::new(true)),
                fail_ready: Rc::new(Cell::new(false)),
                batches: Rc::new(RefCell::new(Vec::new())),
                closes: Rc::new(Cell::new(0)),
            };
            let downstream = OutputWriter::from_sink(ControlledSink {
                controlled: controlled.clone(),
                ready: false,
                closed: false,
            });
            let mut sink = CoalescingSink::new(
                downstream,
                OutputCoalescing::with_limits(Some(Duration::from_secs(60)), Some(2), Some(2))
                    .unwrap(),
            );
            let mut context = Context::from_waker(noop_waker_ref());

            assert!(Pin::new(&mut sink).poll_ready(&mut context).is_ready());
            Pin::new(&mut sink).start_send(batch(1)).unwrap();
            assert!(Pin::new(&mut sink).poll_ready(&mut context).is_ready());
            Pin::new(&mut sink).start_send(batch(2)).unwrap();

            assert!(Pin::new(&mut sink).poll_ready(&mut context).is_pending());
            assert_eq!(sink.pending_ticks, 2);
            assert_eq!(sink.pending_updates, 2);
            assert!(sink.pending.is_some());
            assert!(sink.timer.is_some());

            controlled.pending.set(false);
            assert!(Pin::new(&mut sink).poll_ready(&mut context).is_ready());
            assert_eq!(sink.pending_ticks, 0);
            assert_eq!(sink.pending_updates, 0);
            assert_eq!(controlled.batches.borrow().len(), 1);

            let controlled = Controlled {
                pending: Rc::new(Cell::new(false)),
                fail_ready: Rc::new(Cell::new(false)),
                batches: Rc::new(RefCell::new(Vec::new())),
                closes: Rc::new(Cell::new(0)),
            };
            let downstream = OutputWriter::from_sink(ControlledSink {
                controlled: controlled.clone(),
                ready: false,
                closed: false,
            });
            let mut sink = CoalescingSink::new(
                downstream,
                OutputCoalescing::with_limits(None, Some(1), Some(1)).unwrap(),
            );
            assert!(Pin::new(&mut sink).poll_ready(&mut context).is_ready());
            Pin::new(&mut sink).start_send(batch(3)).unwrap();
            controlled.fail_ready.set(true);
            assert!(Pin::new(&mut sink).poll_ready(&mut context).is_ready());
            assert_eq!(sink.pending_ticks, 1);
            assert_eq!(sink.pending_updates, 1);
            assert!(sink.pending.is_some());
            assert!(Pin::new(&mut sink).poll_close(&mut context).is_ready());
            assert_eq!(controlled.closes.get(), 1);
        });
    }

    #[test]
    fn timed_deadline_preempts_sustained_ready_input() {
        smol::block_on(async {
            let recording = Recording {
                batches: Rc::new(RefCell::new(Vec::new())),
                closes: Rc::new(RefCell::new(0)),
            };
            let executor = Rc::new(LocalExecutor::new());
            let downstream = OutputWriter::from_sink(RecordingSink {
                recording: recording.clone(),
                ready: false,
                closed: false,
            });
            let config =
                OutputCoalescing::with_limits(Some(Duration::from_millis(2)), None, None).unwrap();
            let mut writer = OutputWriter::from_sink(TimedCoalescingSink::new(
                downstream,
                config,
                Rc::clone(&executor),
            ));
            let observed = Rc::new(Cell::new(false));
            let producer_observed = Rc::clone(&observed);
            let producer = async move {
                let mut value = 0;
                while !producer_observed.get() {
                    writer.send(batch(value)).await.unwrap();
                    value += 1;
                }
                writer.close().await.unwrap();
            };
            let observer = async {
                smol::Timer::after(Duration::from_millis(20)).await;
                assert!(
                    !recording.batches.borrow().is_empty(),
                    "an expired coalescing deadline was starved by ready commands"
                );
                observed.set(true);
            };
            executor.run(join(producer, observer)).await;
            assert_eq!(*recording.closes.borrow(), 1);
            assert!(executor.is_empty());
        });
    }

    #[test]
    fn timed_deadline_drives_an_async_downstream_without_a_follow_up_command() {
        smol::block_on(async {
            let started = Rc::new(Cell::new(0));
            let completed = Rc::new(Cell::new(0));
            let observed_ticks = Rc::new(Cell::new(0));
            let started_by_sink = Rc::clone(&started);
            let completed_by_sink = Rc::clone(&completed);
            let observed_by_sink = Rc::clone(&observed_ticks);
            let downstream = OutputWriter::from_sink(crate::io::output::AsyncFnSink::new(
                move |batch: OutputBatch<i32>| {
                    started_by_sink.set(started_by_sink.get() + 1);
                    observed_by_sink.set(batch.tick_count());
                    let completed = Rc::clone(&completed_by_sink);
                    async move {
                        smol::Timer::after(Duration::from_millis(10)).await;
                        completed.set(completed.get() + 1);
                        Ok(())
                    }
                },
            ));
            let executor = Rc::new(LocalExecutor::new());
            let config =
                OutputCoalescing::with_limits(Some(Duration::from_millis(2)), None, None).unwrap();
            let mut writer = OutputWriter::from_sink(TimedCoalescingSink::new(
                downstream,
                config,
                Rc::clone(&executor),
            ));

            executor
                .run(async {
                    writer.send(batch(1)).await.unwrap();
                    // No producer command, flush, or close is issued before
                    // this assertion. The deadline must drive completion.
                    smol::Timer::after(Duration::from_millis(35)).await;
                    assert_eq!(started.get(), 1);
                    assert_eq!(completed.get(), 1);
                    assert_eq!(observed_ticks.get(), 1);
                    writer.close().await.unwrap();
                })
                .await;
            assert!(executor.is_empty());
        });
    }

    #[test]
    fn timed_count_emission_drives_async_downstream_and_preserves_batching() {
        smol::block_on(async {
            let started = Rc::new(Cell::new(0));
            let completed = Rc::new(Cell::new(0));
            let observed_ticks = Rc::new(Cell::new(0));
            let started_by_sink = Rc::clone(&started);
            let completed_by_sink = Rc::clone(&completed);
            let observed_by_sink = Rc::clone(&observed_ticks);
            let downstream = OutputWriter::from_sink(crate::io::output::AsyncFnSink::new(
                move |batch: OutputBatch<i32>| {
                    started_by_sink.set(started_by_sink.get() + 1);
                    observed_by_sink.set(batch.tick_count());
                    let completed = Rc::clone(&completed_by_sink);
                    async move {
                        smol::Timer::after(Duration::from_millis(10)).await;
                        completed.set(completed.get() + 1);
                        Ok(())
                    }
                },
            ));
            let executor = Rc::new(LocalExecutor::new());
            let config =
                OutputCoalescing::with_limits(Some(Duration::from_secs(60)), Some(2), None)
                    .unwrap();
            let mut writer = OutputWriter::from_sink(TimedCoalescingSink::new(
                downstream,
                config,
                Rc::clone(&executor),
            ));

            executor
                .run(async {
                    writer.send(batch(1)).await.unwrap();
                    writer.send(batch(2)).await.unwrap();
                    // The count limit emitted one physical batch. Its async
                    // operation must finish without a third producer action.
                    smol::Timer::after(Duration::from_millis(35)).await;
                    assert_eq!(started.get(), 1);
                    assert_eq!(completed.get(), 1);
                    assert_eq!(observed_ticks.get(), 2);
                    writer.close().await.unwrap();
                })
                .await;
            assert!(executor.is_empty());
        });
    }

    #[test]
    fn timed_async_emission_error_reaches_sticky_worker_state() {
        smol::block_on(async {
            let executor = Rc::new(LocalExecutor::new());
            let downstream = OutputWriter::from_sink(crate::io::output::AsyncFnSink::new(
                move |_batch: OutputBatch<i32>| async move {
                    smol::Timer::after(Duration::from_millis(5)).await;
                    Err(OutputError::backend("timed async emission failure"))
                },
            ));
            let config =
                OutputCoalescing::with_limits(Some(Duration::from_millis(2)), None, None).unwrap();
            let mut writer = OutputWriter::from_sink(TimedCoalescingSink::new(
                downstream,
                config,
                Rc::clone(&executor),
            ));

            executor
                .run(async {
                    writer.send(batch(1)).await.unwrap();
                    smol::Timer::after(Duration::from_millis(25)).await;
                    let error = writer.flush().await.unwrap_err();
                    assert_eq!(error, OutputError::backend("timed async emission failure"));
                    assert_eq!(writer.flush().await.unwrap_err(), error);
                    assert_eq!(writer.close().await.unwrap_err(), error);
                })
                .await;
            assert!(executor.is_empty());
        });
    }

    #[test]
    fn timed_start_send_reports_worker_failure_after_ready_permit() {
        smol::block_on(async {
            let gate = FailureGate::default();
            let gate_for_sink = gate.clone();
            let downstream = OutputWriter::from_sink(crate::io::output::AsyncFnSink::new(
                move |_batch: OutputBatch<i32>| {
                    gate_for_sink.started.set(true);
                    let gate = gate_for_sink.clone();
                    async move {
                        poll_fn(move |context| {
                            if gate.open.get() {
                                Poll::Ready(Err(OutputError::backend("timed worker failure")))
                            } else {
                                *gate.waker.borrow_mut() = Some(context.waker().clone());
                                Poll::Pending
                            }
                        })
                        .await
                    }
                },
            ));
            let executor = Rc::new(LocalExecutor::new());
            let config =
                OutputCoalescing::with_limits(Some(Duration::from_secs(60)), Some(1), None)
                    .unwrap();
            let mut sink = TimedCoalescingSink::new(downstream, config, Rc::clone(&executor));

            // Drive the first reservation through the local executor instead
            // of assuming an immediately-ready channel state.
            executor
                .run(poll_fn(|context| {
                    match Pin::new(&mut sink).poll_ready(context) {
                        Poll::Ready(Ok(())) => Poll::Ready(()),
                        Poll::Ready(Err(error)) => {
                            panic!("initial timed readiness failed: {error}")
                        }
                        Poll::Pending => Poll::Pending,
                    }
                }))
                .await;
            assert!(sink.ready_permit.is_some());
            Pin::new(&mut sink).start_send(batch(1)).unwrap();

            // The count limit starts a genuinely asynchronous downstream
            // operation. Keep its gate closed while driving the worker and the
            // channel reservation until the stale permit is definitely held.
            executor
                .run(poll_fn(|context| {
                    match Pin::new(&mut sink).poll_ready(context) {
                        Poll::Ready(Err(error)) => {
                            panic!("readiness failed before the gate opened: {error}")
                        }
                        Poll::Ready(Ok(()))
                            if gate.started.get() && sink.ready_permit.is_some() =>
                        {
                            Poll::Ready(())
                        }
                        Poll::Ready(Ok(())) | Poll::Pending => {
                            context.waker().wake_by_ref();
                            Poll::Pending
                        }
                    }
                }))
                .await;
            assert!(gate.started.get());
            assert!(sink.ready_permit.is_some());

            // Now fail the worker while retaining the readiness permit. The
            // production start_send check must return that sticky failure
            // rather than enqueueing the batch into a closed worker channel.
            gate.release();
            executor
                .run(poll_fn(|context| {
                    sink.poll_worker(context);
                    if sink.worker_error().is_some() {
                        Poll::Ready(())
                    } else {
                        context.waker().wake_by_ref();
                        Poll::Pending
                    }
                }))
                .await;

            let error = Pin::new(&mut sink).start_send(batch(2)).unwrap_err();
            assert_eq!(error, OutputError::backend("timed worker failure"));

            let close = executor
                .run(poll_fn(|context| Pin::new(&mut sink).poll_close(context)))
                .await;
            assert_eq!(close, Err(error));
            assert!(executor.is_empty());
        });
    }

    #[test]
    fn timed_close_attempts_close_after_flush_failure_and_combines_errors() {
        smol::block_on(async {
            let closes = Rc::new(Cell::new(0));
            let executor = Rc::new(LocalExecutor::new());
            let downstream = OutputWriter::from_sink(CleanupSink {
                fail_send: false,
                fail_flush: true,
                fail_close: true,
                ready: false,
                closed: false,
                closes: Rc::clone(&closes),
            });
            let config =
                OutputCoalescing::with_limits(Some(Duration::from_secs(60)), None, None).unwrap();
            let mut writer = OutputWriter::from_sink(TimedCoalescingSink::new(
                downstream,
                config,
                Rc::clone(&executor),
            ));
            let error = executor
                .run(async {
                    writer.send(batch(1)).await.unwrap();
                    writer.close().await.unwrap_err()
                })
                .await;
            assert!(error.to_string().contains("timed flush failure"));
            assert!(error.to_string().contains("timed close failure"));
            assert_eq!(closes.get(), 1);
            assert!(executor.is_empty());
        });
    }

    #[test]
    fn timed_worker_failure_is_joined_before_close_returns() {
        smol::block_on(async {
            let closes = Rc::new(Cell::new(0));
            let executor = Rc::new(LocalExecutor::new());
            let downstream = OutputWriter::from_sink(CleanupSink {
                fail_send: true,
                fail_flush: false,
                fail_close: false,
                ready: false,
                closed: false,
                closes: Rc::clone(&closes),
            });
            let config =
                OutputCoalescing::with_limits(Some(Duration::from_secs(60)), None, None).unwrap();
            let mut writer = OutputWriter::from_sink(TimedCoalescingSink::new(
                downstream,
                config,
                Rc::clone(&executor),
            ));
            let error = executor
                .run(async {
                    let _ = writer.send(batch(1)).await;
                    writer.close().await.unwrap_err()
                })
                .await;
            assert!(error.to_string().contains("timed worker failure"));
            assert_eq!(closes.get(), 1);
            assert!(executor.is_empty());
        });
    }

    #[test]
    fn local_start_send_failure_closes_live_timed_worker() {
        smol::block_on(async {
            let closes = Rc::new(Cell::new(0));
            let executor = Rc::new(LocalExecutor::new());
            let downstream = OutputWriter::from_sink(CleanupSink {
                fail_send: false,
                fail_flush: false,
                fail_close: false,
                ready: false,
                closed: false,
                closes: Rc::clone(&closes),
            });
            let config =
                OutputCoalescing::with_limits(Some(Duration::from_secs(60)), None, None).unwrap();
            let mut sink = TimedCoalescingSink::new(downstream, config, Rc::clone(&executor));
            let primary = Pin::new(&mut sink).start_send(batch(1)).unwrap_err();
            let close = executor
                .run(poll_fn(|context| Pin::new(&mut sink).poll_close(context)))
                .await;
            assert_eq!(close, Err(primary));
            assert_eq!(closes.get(), 1);
            assert!(executor.is_empty());
        });
    }

    #[test]
    fn timed_coalescing_flushes_when_the_producer_is_idle() {
        smol::block_on(async {
            let recording = Recording {
                batches: Rc::new(RefCell::new(Vec::new())),
                closes: Rc::new(RefCell::new(0)),
            };
            let executor = Rc::new(LocalExecutor::new());
            let downstream = OutputWriter::from_sink(RecordingSink {
                recording: recording.clone(),
                ready: false,
                closed: false,
            });
            let config =
                OutputCoalescing::with_limits(Some(Duration::from_millis(5)), None, None).unwrap();
            let mut writer = OutputWriter::from_sink(TimedCoalescingSink::new(
                downstream,
                config,
                Rc::clone(&executor),
            ));

            executor
                .run(async {
                    writer.send(batch(1)).await.unwrap();
                    smol::Timer::after(Duration::from_millis(25)).await;
                    assert_eq!(recording.batches.borrow().len(), 1);
                    assert_eq!(recording.batches.borrow()[0].tick_count(), 1);
                    let close = writer.close().await;
                    assert!(
                        close.is_ok(),
                        "timed coalescer close failed: {close:?}; downstream closes: {}",
                        recording.closes.borrow()
                    );
                })
                .await;
            assert_eq!(*recording.closes.borrow(), 1);
            assert!(executor.is_empty());
        });
    }

    #[test]
    fn bounds_and_zero_values_are_validated() {
        assert!(OutputBuffer::new(0).is_err());
        assert!(OutputBuffer::with_limits(1, Some(0)).is_err());
        assert!(OutputCoalescing::with_limits(None, None, None).is_err());
        assert!(OutputCoalescing::with_limits(Some(Duration::ZERO), None, None).is_ok());

        let delay_only =
            OutputCoalescing::with_limits(Some(Duration::from_secs(1)), None, None).unwrap();
        assert!(delay_only.with_delay(None).is_err());
        assert!(OutputCoalescing::count(2).unwrap().with_delay(None).is_ok());
        let invalid = OutputCoalescing {
            max_delay: None,
            tick_limit: None,
            update_limit: None,
        };
        assert!(invalid.with_delay(None).is_err());
        assert!(invalid.validate().is_err());
        let recording = Recording {
            batches: Rc::new(RefCell::new(Vec::new())),
            closes: Rc::new(RefCell::new(0)),
        };
        let writer = OutputWriter::from_sink(RecordingSink {
            recording,
            ready: false,
            closed: false,
        });
        assert!(OutputStage::Coalesce(invalid).apply(writer, None).is_err());
        let _ = noop_waker_ref();
    }
}
