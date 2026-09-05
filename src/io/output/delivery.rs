//! Output delivery policy and the bounded local delivery owner.

use std::{
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
    time::Duration,
};

use async_unsync::{bounded, oneshot, unbounded};
use futures::{FutureExt, Sink, future::LocalBoxFuture};
use smol::{LocalExecutor, Task};

use crate::core::{OutputBatch, OutputError, OutputInterface, OutputSink, OutputWriter};
use crate::io::ShutdownDeadline;

const COALESCE_QUEUE_BATCHES: usize = 32;

/// Limits original admitted batches, including queued, coalesced, and in-flight work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueLimits {
    max_batches: NonZeroUsize,
    max_updates: Option<NonZeroUsize>,
}

impl QueueLimits {
    pub const fn new(max_batches: NonZeroUsize, max_updates: Option<NonZeroUsize>) -> Self {
        Self {
            max_batches,
            max_updates,
        }
    }

    pub const fn max_batches(self) -> NonZeroUsize {
        self.max_batches
    }
    pub const fn max_updates(self) -> Option<NonZeroUsize> {
        self.max_updates
    }
}

/// Thresholds for joining complete batches without changing their logical ticks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CoalescingLimits {
    max_ticks: Option<NonZeroUsize>,
    max_updates: Option<NonZeroUsize>,
    max_delay: Option<Duration>,
}

impl CoalescingLimits {
    pub fn new(
        max_ticks: Option<NonZeroUsize>,
        max_updates: Option<NonZeroUsize>,
        max_delay: Option<Duration>,
    ) -> Result<Self, OutputError> {
        if max_ticks.is_none() && max_updates.is_none() && max_delay.is_none() {
            return Err(OutputError::invalid(
                "output coalescing requires at least one threshold",
            ));
        }
        if max_delay.is_some_and(|delay| delay.is_zero()) {
            return Err(OutputError::invalid(
                "output coalescing delay must be greater than zero",
            ));
        }
        Ok(Self {
            max_ticks,
            max_updates,
            max_delay,
        })
    }

    pub const fn max_ticks(self) -> Option<NonZeroUsize> {
        self.max_ticks
    }
    pub const fn max_updates(self) -> Option<NonZeroUsize> {
        self.max_updates
    }
    pub const fn max_delay(self) -> Option<Duration> {
        self.max_delay
    }
}

/// Per-destination delivery behavior. The default owns no background task.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeliveryPolicy {
    pub queue: Option<QueueLimits>,
    pub coalesce: Option<CoalescingLimits>,
}

impl DeliveryPolicy {
    pub const fn direct() -> Self {
        Self {
            queue: None,
            coalesce: None,
        }
    }
    pub const fn queued(queue: QueueLimits) -> Self {
        Self {
            queue: Some(queue),
            coalesce: None,
        }
    }

    pub fn coalesce(limits: CoalescingLimits) -> Self {
        Self {
            queue: Some(QueueLimits::new(
                NonZeroUsize::new(COALESCE_QUEUE_BATCHES).expect("nonzero constant"),
                None,
            )),
            coalesce: Some(limits),
        }
    }

    pub fn with_queue(mut self, queue: QueueLimits) -> Self {
        self.queue = Some(queue);
        self
    }
}

/// Applies a delivery policy to an opened downstream owner.
pub struct Delivery<V> {
    writer: Option<OutputWriter<V>>,
    cancel: Option<unbounded::UnboundedSender<()>>,
    close_result: Option<Result<(), OutputError>>,
}

pub(crate) struct DeliveryCancellation(unbounded::UnboundedSender<()>);

impl DeliveryCancellation {
    pub(crate) fn cancel(&self) {
        let _ = self.0.send(());
    }
}

impl<V: 'static> Delivery<V> {
    pub fn new(
        writer: OutputWriter<V>,
        executor: Option<Rc<LocalExecutor<'static>>>,
        policy: DeliveryPolicy,
    ) -> Result<Self, OutputError> {
        if policy.queue.is_none() && policy.coalesce.is_some() {
            return Err(OutputError::invalid(
                "coalescing delivery requires queue limits",
            ));
        }
        if let Some(queue) = policy.queue {
            let executor = executor.ok_or_else(|| {
                OutputError::invalid("worker-backed output delivery requires a local executor")
            })?;
            let (sink, cancel) = WorkerSink::new(writer, executor, queue, policy.coalesce);
            Ok(Self {
                writer: Some(OutputWriter::from_output_sink(sink)),
                cancel: Some(cancel),
                close_result: None,
            })
        } else {
            Ok(Self {
                writer: Some(writer),
                cancel: None,
                close_result: None,
            })
        }
    }

    pub fn into_writer(self) -> OutputWriter<V> {
        self.writer.expect("delivery owns its writer")
    }

    pub(crate) fn into_parts(mut self) -> (OutputWriter<V>, Option<DeliveryCancellation>) {
        let writer = self.writer.take().expect("delivery owns its writer");
        let cancel = self.cancel.take().map(DeliveryCancellation);
        (writer, cancel)
    }

    pub fn writer_mut(&mut self) -> &mut OutputWriter<V> {
        self.writer.as_mut().expect("delivery owns its writer")
    }

    /// Closes within the session's absolute graceful-shutdown deadline.
    pub async fn close(&mut self, deadline: ShutdownDeadline) -> Result<(), OutputError> {
        if let Some(result) = &self.close_result {
            return result.clone();
        }
        let result = self
            .writer
            .as_mut()
            .expect("delivery owns its writer")
            .close_with_deadline(deadline)
            .await;
        self.close_result = Some(result.clone());
        result
    }
}

type CommandSender<V> = bounded::Sender<Command<V>>;
type Permit<V> = bounded::OwnedPermit<Command<V>>;
type Reservation<V> = LocalBoxFuture<'static, Result<Permit<V>, OutputError>>;
type Reply = oneshot::Sender<Result<(), OutputError>>;

enum Command<V> {
    Batch {
        batch: OutputBatch<V>,
        charge: Charge,
    },
    Flush(Reply),
    Rebind {
        interface: OutputInterface,
        reply: Reply,
    },
    Close(Reply),
}

#[derive(Clone, Copy, Debug, Default)]
struct Charge {
    batches: usize,
    updates: usize,
}

enum Completion {
    Delivered(Charge),
    Failed(OutputError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum BarrierKind {
    Flush,
    Close,
    Rebind(OutputInterface),
}

struct Barrier<V> {
    kind: BarrierKind,
    reservation: Option<Reservation<V>>,
    reply: Option<Reply>,
    response: oneshot::Receiver<Result<(), OutputError>>,
}

struct WorkerSink<V> {
    task: Option<Task<()>>,
    cancel: Option<unbounded::UnboundedSender<()>>,
    sender: Option<CommandSender<V>>,
    completions: unbounded::UnboundedReceiver<Completion>,
    limits: QueueLimits,
    charged: Charge,
    permit: Option<Permit<V>>,
    reservation: Option<Reservation<V>>,
    barrier: Option<Barrier<V>>,
    failure: Option<OutputError>,
    closing: bool,
    close_reply: Option<Result<(), OutputError>>,
    close_result: Option<Result<(), OutputError>>,
}

impl<V> Unpin for WorkerSink<V> {}

impl<V: 'static> WorkerSink<V> {
    fn new(
        writer: OutputWriter<V>,
        executor: Rc<LocalExecutor<'static>>,
        limits: QueueLimits,
        coalesce: Option<CoalescingLimits>,
    ) -> (Self, unbounded::UnboundedSender<()>) {
        let (sender, receiver) = bounded::channel(limits.max_batches.get()).into_split();
        let (completion_sender, completions) = unbounded::channel().into_split();
        let (cancel_sender, cancel_receiver) = unbounded::channel().into_split();
        let task = executor.spawn(run_worker_until_cancelled(
            writer,
            receiver,
            completion_sender,
            coalesce,
            limits,
            cancel_receiver,
        ));
        (
            Self {
                task: Some(task),
                cancel: Some(cancel_sender.clone()),
                sender: Some(sender),
                completions,
                limits,
                charged: Charge::default(),
                permit: None,
                reservation: None,
                barrier: None,
                failure: None,
                closing: false,
                close_reply: None,
                close_result: None,
            },
            cancel_sender,
        )
    }

    fn drain_completions(&mut self, cx: &mut Context<'_>) {
        loop {
            match self.completions.poll_recv(cx) {
                Poll::Ready(Some(Completion::Delivered(charge))) => {
                    self.charged.batches = self.charged.batches.saturating_sub(charge.batches);
                    self.charged.updates = self.charged.updates.saturating_sub(charge.updates);
                }
                Poll::Ready(Some(Completion::Failed(error))) => {
                    remember_worker_failure(&mut self.failure, error);
                }
                Poll::Ready(None) | Poll::Pending => break,
            }
        }
    }

    fn has_credit(&self) -> bool {
        self.charged.batches < self.limits.max_batches.get()
            && self
                .limits
                .max_updates
                .is_none_or(|limit| self.charged.updates < limit.get() || self.charged.batches == 0)
    }

    fn reserve(sender: CommandSender<V>) -> Reservation<V> {
        Box::pin(async move {
            sender
                .reserve_owned()
                .await
                .map_err(|_| OutputError::closed())
        })
    }

    fn begin_barrier(&mut self, kind: BarrierKind) {
        self.permit = None;
        self.reservation = None;
        let sender = self.sender.as_ref().expect("open worker sender").clone();
        let (reply, response) = oneshot::channel().into_split();
        self.barrier = Some(Barrier {
            kind,
            reservation: Some(Self::reserve(sender)),
            reply: Some(reply),
            response,
        });
    }

    fn poll_barrier(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), OutputError>> {
        let barrier = self.barrier.as_mut().expect("barrier installed");
        if let Some(reservation) = barrier.reservation.as_mut() {
            match reservation.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    self.barrier = None;
                    return Poll::Ready(Err(self.failure.clone().unwrap_or(error)));
                }
                Poll::Ready(Ok(permit)) => {
                    barrier.reservation = None;
                    let reply = barrier.reply.take().expect("barrier sent once");
                    let command = match &barrier.kind {
                        BarrierKind::Flush => Command::Flush(reply),
                        BarrierKind::Close => Command::Close(reply),
                        BarrierKind::Rebind(interface) => Command::Rebind {
                            interface: interface.clone(),
                            reply,
                        },
                    };
                    let _ = permit.send(command);
                }
            }
        }
        match Pin::new(&mut barrier.response).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(result)) => {
                self.barrier = None;
                Poll::Ready(result)
            }
            Poll::Ready(Err(_)) => {
                self.barrier = None;
                Poll::Ready(Err(self
                    .failure
                    .clone()
                    .unwrap_or_else(OutputError::closed)))
            }
        }
    }

    fn poll_existing_barrier(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), OutputError>> {
        if self.barrier.is_none() {
            Poll::Ready(Ok(()))
        } else {
            self.poll_barrier(cx)
        }
    }
}

fn remember_worker_failure(slot: &mut Option<OutputError>, error: OutputError) {
    *slot = Some(match slot.take() {
        Some(primary) => primary.with_cleanup(error),
        None => error,
    });
}

impl<V: 'static> Sink<OutputBatch<V>> for WorkerSink<V> {
    type Error = OutputError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        this.drain_completions(cx);
        if let Some(error) = &this.failure {
            return Poll::Ready(Err(error.clone()));
        }
        if this.closing {
            return Poll::Ready(Err(OutputError::closed()));
        }
        match this.poll_existing_barrier(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {}
        }
        if this.permit.is_some() {
            return Poll::Ready(Ok(()));
        }
        if !this.has_credit() {
            return Poll::Pending;
        }
        if this.reservation.is_none() {
            this.reservation = Some(Self::reserve(
                this.sender.as_ref().expect("open worker sender").clone(),
            ));
        }
        match this
            .reservation
            .as_mut()
            .expect("reservation exists")
            .as_mut()
            .poll(cx)
        {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(permit)) => {
                this.reservation = None;
                this.permit = Some(permit);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => {
                this.reservation = None;
                Poll::Ready(Err(this.failure.clone().unwrap_or(error)))
            }
        }
    }

    fn start_send(self: Pin<&mut Self>, batch: OutputBatch<V>) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if let Some(error) = &this.failure {
            return Err(error.clone());
        }
        let permit = this
            .permit
            .take()
            .ok_or_else(|| OutputError::backend("delivery worker was not ready for start_send"))?;
        let charge = Charge {
            batches: 1,
            updates: batch.update_count(),
        };
        this.charged.batches = this.charged.batches.saturating_add(1);
        this.charged.updates = this.charged.updates.saturating_add(charge.updates);
        let _ = permit.send(Command::Batch { batch, charge });
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        this.drain_completions(cx);
        if this.closing || this.close_result.is_some() {
            return Poll::Ready(Err(OutputError::closed()));
        }
        if let Some(error) = &this.failure {
            return Poll::Ready(Err(error.clone()));
        }
        let continuing_flush = this
            .barrier
            .as_ref()
            .is_some_and(|barrier| matches!(barrier.kind, BarrierKind::Flush));
        if this.barrier.is_some() && !continuing_flush {
            match this.poll_existing_barrier(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {}
            }
        }
        if this.barrier.is_none() {
            this.begin_barrier(BarrierKind::Flush);
        }
        this.poll_barrier(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if let Some(result) = &this.close_result {
            return Poll::Ready(result.clone());
        }
        this.drain_completions(cx);
        this.closing = true;
        if this.close_reply.is_none() {
            let completing_close = this
                .barrier
                .as_ref()
                .is_some_and(|barrier| matches!(barrier.kind, BarrierKind::Close));
            match this.poll_existing_barrier(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    this.close_reply = Some(Err(error));
                    this.sender = None;
                }
                Poll::Ready(Ok(())) if completing_close => {
                    this.close_reply = Some(Ok(()));
                    this.sender = None;
                }
                Poll::Ready(Ok(())) => {}
            }
        }
        if this.close_reply.is_none() && this.barrier.is_none() {
            this.begin_barrier(BarrierKind::Close);
        }
        if this.close_reply.is_none() {
            match this.poll_barrier(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => this.close_reply = Some(result),
            }
            this.sender = None;
        }
        if let Some(task) = this.task.as_mut() {
            if Pin::new(task).poll(cx).is_pending() {
                return Poll::Pending;
            }
        }
        this.task = None;
        let result = this
            .close_reply
            .take()
            .expect("a joined close worker has a reply result");
        this.close_result = Some(result.clone());
        Poll::Ready(result)
    }
}

impl<V: 'static> OutputSink<V> for WorkerSink<V> {
    fn abort(self: Pin<&mut Self>) {
        let this = self.get_mut();
        this.permit = None;
        this.reservation = None;
        this.barrier = None;
        this.sender = None;
        if let Some(cancel) = this.cancel.take() {
            let _ = cancel.send(());
        }
        // Dropping a smol task cancels it and drops the downstream writer.
        this.task.take();
    }

    fn poll_rebind(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        interface: &OutputInterface,
    ) -> Poll<Result<(), OutputError>> {
        let this = self.get_mut();
        this.drain_completions(cx);
        if let Some(error) = &this.failure {
            return Poll::Ready(Err(error.clone()));
        }
        if this.closing {
            return Poll::Ready(Err(OutputError::closed()));
        }
        let continuing_rebind = this.barrier.as_ref().is_some_and(
            |barrier| matches!(&barrier.kind, BarrierKind::Rebind(active) if active == interface),
        );
        if this.barrier.is_some() && !continuing_rebind {
            match this.poll_existing_barrier(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {}
            }
        }
        if this.barrier.is_none() {
            this.permit = None;
            this.reservation = None;
            let sender = this.sender.as_ref().expect("open worker sender").clone();
            let (reply, response) = oneshot::channel().into_split();
            this.barrier = Some(Barrier {
                kind: BarrierKind::Rebind(interface.clone()),
                reservation: Some(Self::reserve(sender)),
                reply: Some(reply),
                response,
            });
        }
        this.poll_barrier(cx)
    }
}

impl<V> Drop for WorkerSink<V> {
    fn drop(&mut self) {
        self.permit = None;
        self.reservation = None;
        self.barrier = None;
        self.sender = None;
        if let Some(task) = self.task.take() {
            task.detach();
        }
    }
}

async fn emit<V: 'static>(
    writer: &mut OutputWriter<V>,
    pending: &mut Option<(OutputBatch<V>, Charge)>,
    completions: &unbounded::UnboundedSender<Completion>,
) -> Result<(), OutputError> {
    let Some((batch, charge)) = pending.take() else {
        return Ok(());
    };
    match writer.send(batch).await {
        Ok(()) => {
            let _ = completions.send(Completion::Delivered(charge));
            Ok(())
        }
        Err(error) => Err(error),
    }
}

async fn run_worker_until_cancelled<V: 'static>(
    writer: OutputWriter<V>,
    receiver: bounded::Receiver<Command<V>>,
    completions: unbounded::UnboundedSender<Completion>,
    coalesce: Option<CoalescingLimits>,
    queue: QueueLimits,
    mut cancel: unbounded::UnboundedReceiver<()>,
) {
    let worker = run_worker(writer, receiver, completions, coalesce, queue).fuse();
    let cancelled = async move {
        if cancel.recv().await.is_none() {
            futures::future::pending::<()>().await;
        }
    }
    .fuse();
    futures::pin_mut!(worker, cancelled);
    let _ = futures::future::select(worker, cancelled).await;
}

async fn run_worker<V: 'static>(
    mut writer: OutputWriter<V>,
    mut receiver: bounded::Receiver<Command<V>>,
    completions: unbounded::UnboundedSender<Completion>,
    coalesce: Option<CoalescingLimits>,
    queue: QueueLimits,
) {
    let mut pending: Option<(OutputBatch<V>, Charge)> = None;
    let mut timer: Option<Pin<Box<smol::Timer>>> = None;
    let mut closed = false;
    loop {
        let command = if let Some(delay) = timer.as_mut() {
            let recv = receiver.recv().fuse();
            futures::pin_mut!(recv);
            match futures::future::select(recv, delay.as_mut()).await {
                futures::future::Either::Left((command, _)) => command,
                futures::future::Either::Right((_, _)) => {
                    timer = None;
                    if emit(&mut writer, &mut pending, &completions).await.is_err() {
                        break;
                    }
                    continue;
                }
            }
        } else {
            receiver.recv().await
        };

        let Some(command) = command else {
            let _ = emit(&mut writer, &mut pending, &completions).await;
            let _ = writer
                .close()
                .await
                .map_err(|error| completions.send(Completion::Failed(error)));
            closed = true;
            break;
        };
        match command {
            Command::Batch { batch, charge } => {
                if let Some(limits) = coalesce {
                    if let Some((joined, total)) = pending.as_mut() {
                        if let Err(error) = joined.append(batch) {
                            let _ = completions.send(Completion::Failed(error));
                            break;
                        }
                        total.batches += charge.batches;
                        total.updates = total.updates.saturating_add(charge.updates);
                    } else {
                        timer = limits
                            .max_delay
                            .map(|delay| Box::pin(smol::Timer::after(delay)));
                        pending = Some((batch, charge));
                    }
                    let (ticks, charge) = pending
                        .as_ref()
                        .map(|(batch, charge)| (batch.tick_count(), *charge))
                        .expect("pending batch");
                    let due = limits.max_ticks.is_some_and(|n| ticks >= n.get())
                        || limits
                            .max_updates
                            .is_some_and(|n| charge.updates >= n.get())
                        || charge.batches >= queue.max_batches.get()
                        || queue.max_updates.is_some_and(|n| charge.updates >= n.get());
                    if due {
                        timer = None;
                        if emit(&mut writer, &mut pending, &completions).await.is_err() {
                            break;
                        }
                    }
                } else {
                    pending = Some((batch, charge));
                    if emit(&mut writer, &mut pending, &completions).await.is_err() {
                        break;
                    }
                }
            }
            Command::Flush(reply) => {
                let result = match emit(&mut writer, &mut pending, &completions).await {
                    Ok(()) => writer.flush().await,
                    Err(error) => Err(error),
                };
                let failed = result.is_err();
                let _ = reply.send(result);
                if failed {
                    break;
                }
            }
            Command::Rebind { interface, reply } => {
                let result = emit(&mut writer, &mut pending, &completions).await;
                let result = match result {
                    Ok(()) => match writer.flush().await {
                        Ok(()) => writer.rebind(interface).await,
                        Err(error) => Err(error),
                    },
                    Err(error) => Err(error),
                };
                let failed = result.is_err();
                let _ = reply.send(result);
                if failed {
                    break;
                }
            }
            Command::Close(reply) => {
                let delivery = emit(&mut writer, &mut pending, &completions).await;
                let cleanup = writer.close().await;
                let result = match (delivery, cleanup) {
                    (Err(primary), Err(cleanup)) => Err(super::combine_errors(primary, cleanup)),
                    (Err(error), _) | (_, Err(error)) => Err(error),
                    _ => Ok(()),
                };
                let _ = reply.send(result);
                closed = true;
                break;
            }
        }
    }
    if !closed {
        if let Err(error) = writer.close().await {
            let _ = completions.send(Completion::Failed(error));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        rc::Rc,
        task::Poll,
    };

    use futures::{FutureExt, Sink, SinkExt, future::poll_fn};

    use super::*;

    struct RecordingSink {
        batches: Rc<RefCell<Vec<OutputBatch<i32>>>>,
    }

    struct CloseFailingSink {
        closes: Rc<Cell<usize>>,
    }

    struct SendAndCloseFailingSink {
        closes: Rc<Cell<usize>>,
    }

    struct RebindRecordingSink {
        events: Rc<RefCell<Vec<String>>>,
    }

    impl Sink<OutputBatch<i32>> for RebindRecordingSink {
        type Error = OutputError;
        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn start_send(self: Pin<&mut Self>, _: OutputBatch<i32>) -> Result<(), Self::Error> {
            self.events.borrow_mut().push("batch".to_owned());
            Ok(())
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    impl OutputSink<i32> for RebindRecordingSink {
        fn poll_rebind(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            interface: &OutputInterface,
        ) -> Poll<Result<(), OutputError>> {
            self.events
                .borrow_mut()
                .push(format!("rebind:{interface:?}"));
            Poll::Ready(Ok(()))
        }
    }

    impl Sink<OutputBatch<i32>> for CloseFailingSink {
        type Error = OutputError;

        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, _batch: OutputBatch<i32>) -> Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            self.closes.set(self.closes.get() + 1);
            Poll::Ready(Err(OutputError::backend("cleanup failed")))
        }
    }

    impl Sink<OutputBatch<i32>> for SendAndCloseFailingSink {
        type Error = OutputError;

        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, _: OutputBatch<i32>) -> Result<(), Self::Error> {
            Err(OutputError::backend("publish failed"))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.closes.set(self.closes.get() + 1);
            Poll::Ready(Err(OutputError::backend("cleanup failed")))
        }
    }

    impl Sink<OutputBatch<i32>> for RecordingSink {
        type Error = OutputError;

        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, batch: OutputBatch<i32>) -> Result<(), Self::Error> {
            self.get_mut().batches.borrow_mut().push(batch);
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn coalescing_keeps_original_batch_credits_until_delivery_completes() {
        let executor = Rc::new(LocalExecutor::new());
        let (started, observed) = async_channel::bounded(1);
        let (release, completion) = async_channel::bounded(1);
        let writer = OutputWriter::from_sink(crate::io::output::local_batch_sink(
            move |batch: OutputBatch<i32>| {
                let started = started.clone();
                let completion = completion.clone();
                async move {
                    started.send(batch.tick_count()).await.unwrap();
                    completion.recv().await.unwrap();
                    Ok(())
                }
            },
        ));
        let limits = QueueLimits::new(NonZeroUsize::new(2).unwrap(), None);
        let coalesce = CoalescingLimits::new(NonZeroUsize::new(2), None, None).unwrap();
        let (mut sink, _) = WorkerSink::new(writer, Rc::clone(&executor), limits, Some(coalesce));
        smol::block_on(executor.run(async {
            sink.feed(OutputBatch::update("x", 1)).await.unwrap();
            sink.feed(OutputBatch::update("x", 2)).await.unwrap();
            assert_eq!(observed.recv().await.unwrap(), 2);
            assert!(
                poll_fn(|cx| Pin::new(&mut sink).poll_ready(cx))
                    .now_or_never()
                    .is_none()
            );
            release.send(()).await.unwrap();
            poll_fn(|cx| Pin::new(&mut sink).poll_ready(cx))
                .await
                .unwrap();
            sink.close().await.unwrap();
        }));
    }

    #[test]
    fn update_pressure_allows_one_crossing_batch_and_an_oversized_single_batch() {
        for (updates_per_batch, admitted_batches) in [(2, 2), (5, 1)] {
            let executor = Rc::new(LocalExecutor::new());
            let (started, observed) = async_channel::bounded(2);
            let (release, completion) = async_channel::bounded(2);
            let writer = OutputWriter::from_sink(crate::io::output::local_batch_sink(
                move |batch: OutputBatch<i32>| {
                    let started = started.clone();
                    let completion = completion.clone();
                    async move {
                        started.send(batch.update_count()).await.unwrap();
                        completion.recv().await.unwrap();
                        Ok(())
                    }
                },
            ));
            let limits = QueueLimits::new(NonZeroUsize::new(4).unwrap(), NonZeroUsize::new(3));
            let (mut sink, _) = WorkerSink::new(writer, Rc::clone(&executor), limits, None);
            smol::block_on(executor.run(async {
                for _ in 0..admitted_batches {
                    let batch = OutputBatch::tick(
                        (0..updates_per_batch)
                            .map(|i| {
                                crate::core::OutputUpdate::new(format!("x{i}").into(), i as i32)
                            })
                            .collect(),
                    )
                    .unwrap();
                    sink.feed(batch).await.unwrap();
                }
                assert_eq!(observed.recv().await.unwrap(), updates_per_batch);
                assert!(
                    poll_fn(|cx| Pin::new(&mut sink).poll_ready(cx))
                        .now_or_never()
                        .is_none()
                );
                for _ in 0..admitted_batches {
                    release.send(()).await.unwrap();
                }
                poll_fn(|cx| Pin::new(&mut sink).poll_ready(cx))
                    .await
                    .unwrap();
                sink.close().await.unwrap();
            }));
        }
    }

    #[test]
    fn direct_is_the_default_and_coalescing_has_a_bounded_queue() {
        assert_eq!(DeliveryPolicy::default(), DeliveryPolicy::direct());
        let coalescing = CoalescingLimits::new(None, None, Some(Duration::from_millis(1))).unwrap();
        let policy = DeliveryPolicy::coalesce(coalescing);
        assert_eq!(policy.queue.unwrap().max_batches().get(), 32);
        assert_eq!(policy.queue.unwrap().max_updates(), None);
    }

    #[test]
    fn an_idle_producer_does_not_starve_the_coalescing_deadline() {
        let executor = Rc::new(LocalExecutor::new());
        let batches = Rc::new(RefCell::new(Vec::new()));
        let writer = OutputWriter::from_sink(RecordingSink {
            batches: Rc::clone(&batches),
        });
        let limits = CoalescingLimits::new(None, None, Some(Duration::from_millis(1))).unwrap();
        let mut writer = Delivery::new(
            writer,
            Some(Rc::clone(&executor)),
            DeliveryPolicy::coalesce(limits),
        )
        .unwrap()
        .into_writer();

        smol::block_on(executor.run(async {
            writer.feed(OutputBatch::update("x", 1)).await.unwrap();
            smol::Timer::after(Duration::from_millis(5)).await;
            writer.flush().await.unwrap();
            writer.close().await.unwrap();
        }));

        let batches = batches.borrow();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].update_count(), 1);
    }

    #[test]
    fn coalescing_preserves_original_ticks_until_a_completion_barrier() {
        let executor = Rc::new(LocalExecutor::new());
        let batches = Rc::new(RefCell::new(Vec::new()));
        let writer = OutputWriter::from_sink(RecordingSink {
            batches: Rc::clone(&batches),
        });
        let limits = CoalescingLimits::new(NonZeroUsize::new(2), None, None).unwrap();
        let queue = QueueLimits::new(NonZeroUsize::new(2).unwrap(), None);
        let mut writer = Delivery::new(
            writer,
            Some(Rc::clone(&executor)),
            DeliveryPolicy::coalesce(limits).with_queue(queue),
        )
        .unwrap()
        .into_writer();

        smol::block_on(executor.run(async {
            writer.feed(OutputBatch::update("x", 1)).await.unwrap();
            writer.feed(OutputBatch::update("x", 2)).await.unwrap();
            writer.flush().await.unwrap();
        }));

        let batches = batches.borrow();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].tick_count(), 2);
    }

    #[test]
    fn flush_cancels_a_held_ready_permit() {
        let executor = Rc::new(LocalExecutor::new());
        let batches = Rc::new(RefCell::new(Vec::new()));
        let writer = OutputWriter::from_sink(RecordingSink {
            batches: Rc::clone(&batches),
        });
        let limits = QueueLimits::new(NonZeroUsize::new(1).unwrap(), None);
        let (mut sink, _) = WorkerSink::new(writer, Rc::clone(&executor), limits, None);

        smol::block_on(executor.run(async {
            poll_fn(|cx| Pin::new(&mut sink).poll_ready(cx))
                .await
                .unwrap();
            sink.flush().await.unwrap();
            sink.close().await.unwrap();
        }));

        assert!(batches.borrow().is_empty());
    }

    #[test]
    fn cancelled_rebind_completes_before_admission_and_a_later_rebind() {
        let executor = Rc::new(LocalExecutor::new());
        let events = Rc::new(RefCell::new(Vec::new()));
        let writer = OutputWriter::from_output_sink(RebindRecordingSink {
            events: Rc::clone(&events),
        });
        let limits = QueueLimits::new(NonZeroUsize::new(1).unwrap(), None);
        let (mut sink, _) = WorkerSink::new(writer, Rc::clone(&executor), limits, None);
        let first = OutputInterface::outputs([crate::VarName::new("a")]).unwrap();
        let second = OutputInterface::outputs([crate::VarName::new("b")]).unwrap();

        assert!(
            poll_fn(|cx| Pin::new(&mut sink).poll_rebind(cx, &first))
                .now_or_never()
                .is_none()
        );
        smol::block_on(executor.run(async {
            sink.feed(OutputBatch::update("x", 1)).await.unwrap();
            poll_fn(|cx| Pin::new(&mut sink).poll_rebind(cx, &second))
                .await
                .unwrap();
            sink.close().await.unwrap();
        }));

        let events = events.borrow();
        assert_eq!(events.len(), 3);
        assert!(events[0].starts_with("rebind:"));
        assert_eq!(events[1], "batch");
        assert!(events[2].starts_with("rebind:"));
        assert_ne!(events[0], events[2]);
    }

    #[test]
    fn a_cancelled_flush_is_completed_before_later_admission() {
        let executor = Rc::new(LocalExecutor::new());
        let batches = Rc::new(RefCell::new(Vec::new()));
        let writer = OutputWriter::from_sink(RecordingSink {
            batches: Rc::clone(&batches),
        });
        let limits = QueueLimits::new(NonZeroUsize::new(2).unwrap(), None);
        let (mut sink, _) = WorkerSink::new(writer, Rc::clone(&executor), limits, None);

        assert!(sink.flush().now_or_never().is_none());
        smol::block_on(executor.run(async {
            sink.feed(OutputBatch::update("x", 1)).await.unwrap();
            sink.close().await.unwrap();
        }));

        assert_eq!(batches.borrow().len(), 1);
    }

    #[test]
    fn close_failure_is_retained_and_cleanup_runs_once() {
        let executor = Rc::new(LocalExecutor::new());
        let closes = Rc::new(Cell::new(0));
        let writer = OutputWriter::from_sink(CloseFailingSink {
            closes: Rc::clone(&closes),
        });
        let limits = QueueLimits::new(NonZeroUsize::new(1).unwrap(), None);
        let (mut sink, _) = WorkerSink::new(writer, Rc::clone(&executor), limits, None);

        smol::block_on(executor.run(async {
            let first = sink.close().await.unwrap_err();
            let second = sink.close().await.unwrap_err();
            assert_eq!(first, second);
        }));
        assert_eq!(closes.get(), 1);
    }

    #[test]
    fn send_failure_is_not_duplicated_when_worker_cleanup_finishes() {
        let executor = Rc::new(LocalExecutor::new());
        let closes = Rc::new(Cell::new(0));
        let writer = OutputWriter::from_sink(SendAndCloseFailingSink {
            closes: Rc::clone(&closes),
        });
        let limits = QueueLimits::new(NonZeroUsize::new(1).unwrap(), None);
        let (mut sink, _) = WorkerSink::new(writer, Rc::clone(&executor), limits, None);

        smol::block_on(executor.run(async {
            sink.feed(OutputBatch::update("x", 1)).await.unwrap();
            let primary = sink.flush().await.unwrap_err();
            assert_eq!(primary.message(), Some("publish failed"));
            assert_eq!(
                primary.cleanup_causes().collect::<Vec<_>>(),
                ["output backend error: cleanup failed"]
            );
            let final_error = sink.close().await.unwrap_err();
            assert_eq!(final_error, primary);
        }));
        assert_eq!(closes.get(), 1);
    }

    #[test]
    fn expired_close_is_retained_across_repeated_calls() {
        let batches = Rc::new(RefCell::new(Vec::new()));
        let writer = OutputWriter::from_sink(RecordingSink { batches });
        let mut delivery = Delivery::new(writer, None, DeliveryPolicy::direct()).unwrap();
        let deadline = ShutdownDeadline::after(Duration::ZERO);

        smol::block_on(async {
            let first = delivery.close(deadline).await.unwrap_err();
            let second = delivery.close(deadline).await.unwrap_err();
            assert_eq!(first, second);
            assert!(first.to_string().contains("deadline"));
        });
    }

    #[test]
    fn later_cleanup_failure_is_attached_to_the_primary_worker_failure() {
        let mut failure = None;
        remember_worker_failure(&mut failure, OutputError::backend("publish failed"));
        remember_worker_failure(&mut failure, OutputError::backend("cleanup failed"));
        let failure = failure.unwrap().to_string();
        assert!(failure.contains("publish failed"));
        assert!(failure.contains("cleanup failed"));
    }
}
