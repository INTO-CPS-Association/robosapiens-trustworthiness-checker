//! A bounded, local output pump.
//!
//! [`OutputPump`] puts complete output batches on one bounded
//! [`async_unsync::bounded`] queue. A local worker owns the downstream writer,
//! which keeps backend polling out of the producer-facing sink while retaining
//! explicit flush and close barriers.

use std::{
    cell::RefCell,
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

use async_unsync::{bounded, oneshot};
use futures::{Sink, future::LocalBoxFuture};
use smol::{LocalExecutor, Task};

use crate::core::{OutputBatch, OutputError, OutputWriter};

type BarrierResult = Result<(), OutputError>;
type CommandSender<V> = bounded::Sender<PumpCommand<V>>;
type CommandPermit<V> = bounded::OwnedPermit<PumpCommand<V>>;
type Reservation<V> = LocalBoxFuture<'static, Result<CommandPermit<V>, OutputError>>;

enum PumpCommand<V> {
    Batch(OutputBatch<V>),
    Flush(oneshot::Sender<BarrierResult>),
    Close(oneshot::Sender<BarrierResult>),
}

#[derive(Default)]
struct WorkerState {
    error: Option<OutputError>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BarrierKind {
    Flush,
    Close,
}

struct Barrier<V> {
    kind: BarrierKind,
    reply: Option<oneshot::Sender<BarrierResult>>,
    response: Option<oneshot::Receiver<BarrierResult>>,
    reservation: Option<Reservation<V>>,
    result: Option<BarrierResult>,
}

impl<V: 'static> Barrier<V> {
    fn new(kind: BarrierKind, sender: &CommandSender<V>) -> Self {
        let (reply, response) = oneshot::channel::<BarrierResult>().into_split();
        Self {
            kind,
            reply: Some(reply),
            response: Some(response),
            reservation: Some(reserve(sender.clone())),
            result: None,
        }
    }
}

/// A local sink that queues complete output batches for a downstream writer.
///
/// `OutputPump::new` is the usual fallible entry point: it returns the core
/// [`OutputWriter`] wrapper expected by output drivers. `from_writer` is
/// available when the pump sink itself is useful, for example when composing
/// another local sink. Both constructors reject zero capacity before spawning
/// a worker.
///
/// The pump is intentionally local. It uses `Rc`, `async_unsync` channels, and
/// a `smol::LocalExecutor`; it does not implement or require `Send` or `Sync`.
pub struct OutputPump<V> {
    sink: PumpSink<V>,
}

impl<V: 'static> OutputPump<V> {
    /// Creates a bounded pump and returns it as a sticky core output writer.
    ///
    /// `capacity` is the maximum number of complete commands held by the
    /// pump's bounded queue. Zero is rejected before the channel is created.
    pub fn new(
        writer: OutputWriter<V>,
        executor: Rc<LocalExecutor<'static>>,
        capacity: usize,
    ) -> Result<OutputWriter<V>, OutputError> {
        Ok(OutputWriter::from_sink(Self::from_writer(
            writer, executor, capacity,
        )?))
    }

    /// Creates the producer-facing pump sink without wrapping it in another
    /// [`OutputWriter`].
    pub fn from_writer(
        writer: OutputWriter<V>,
        executor: Rc<LocalExecutor<'static>>,
        capacity: usize,
    ) -> Result<Self, OutputError> {
        let capacity = NonZeroUsize::new(capacity).ok_or_else(|| {
            OutputError::invalid("output pump capacity must be greater than zero")
        })?;
        Ok(Self {
            sink: PumpSink::new(writer, executor, capacity),
        })
    }

    /// Wraps this pump sink in the core sticky output writer.
    pub fn into_writer(self) -> OutputWriter<V> {
        OutputWriter::from_sink(self)
    }
}

impl<V> Unpin for OutputPump<V> {}

impl<V: 'static> Sink<OutputBatch<V>> for OutputPump<V> {
    type Error = OutputError;

    fn poll_ready(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        Pin::new(&mut this.sink).poll_ready(context)
    }

    fn start_send(self: Pin<&mut Self>, batch: OutputBatch<V>) -> Result<(), Self::Error> {
        let this = self.get_mut();
        Pin::new(&mut this.sink).start_send(batch)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        Pin::new(&mut this.sink).poll_flush(context)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        Pin::new(&mut this.sink).poll_close(context)
    }
}

struct PumpSink<V> {
    worker: Option<Task<()>>,
    sender: Option<CommandSender<V>>,
    worker_state: Rc<RefCell<WorkerState>>,
    ready_permit: Option<CommandPermit<V>>,
    reservation: Option<Reservation<V>>,
    barrier: Option<Barrier<V>>,
    state: PumpState,
    failure_from_worker: bool,
}

#[derive(Clone, Debug)]
enum PumpState {
    Open,
    Closing,
    Closed,
    Failed(OutputError),
}

impl<V: 'static> PumpSink<V> {
    fn new(
        writer: OutputWriter<V>,
        executor: Rc<LocalExecutor<'static>>,
        capacity: NonZeroUsize,
    ) -> Self {
        let (sender, receiver) = bounded::channel(capacity.get()).into_split();
        let worker_state = Rc::new(RefCell::new(WorkerState::default()));
        let worker = executor.spawn(run_worker(writer, receiver, Rc::clone(&worker_state)));

        Self {
            worker: Some(worker),
            sender: Some(sender),
            worker_state,
            ready_permit: None,
            reservation: None,
            barrier: None,
            state: PumpState::Open,
            failure_from_worker: false,
        }
    }

    fn state_error(&self) -> Option<OutputError> {
        match &self.state {
            PumpState::Open => None,
            PumpState::Closing | PumpState::Closed => Some(OutputError::Closed),
            PumpState::Failed(error) => Some(error.clone()),
        }
    }

    fn worker_error(&self) -> Option<OutputError> {
        self.worker_state.borrow().error.clone()
    }

    fn fail(&mut self, error: OutputError) -> OutputError {
        self.failure_from_worker = false;
        self.state = PumpState::Failed(error.clone());
        error
    }

    fn fail_from_worker(&mut self, error: OutputError) -> OutputError {
        self.failure_from_worker = true;
        self.state = PumpState::Failed(error.clone());
        error
    }

    /// Drop every producer-side channel handle so a worker blocked in
    /// `receiver.recv()` can observe channel termination. A command already
    /// queued remains owned by the worker and follows its normal cleanup path.
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

    /// Observes the worker for operations that require an open pump. The
    /// closing path deliberately uses a separate observer so it can wait for
    /// the task after a downstream close error.
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

    fn reserve_batch(&mut self) {
        if self.reservation.is_none() && self.ready_permit.is_none() {
            let sender = self
                .sender
                .as_ref()
                .expect("open pump has a command sender")
                .clone();
            self.reservation = Some(reserve(sender));
        }
    }

    fn poll_batch_reservation(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), OutputError>> {
        self.reserve_batch();
        let reservation = self
            .reservation
            .as_mut()
            .expect("batch reservation is created before it is polled");
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
        barrier: &mut Barrier<V>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), OutputError>> {
        if let Some(reservation) = barrier.reservation.as_mut() {
            match reservation.as_mut().poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(permit)) => {
                    barrier.reservation = None;
                    let reply = barrier
                        .reply
                        .take()
                        .expect("a barrier permit is sent at most once");
                    let command = match barrier.kind {
                        BarrierKind::Flush => PumpCommand::Flush(reply),
                        BarrierKind::Close => PumpCommand::Close(reply),
                    };
                    let _sender = permit.send(command);
                }
                Poll::Ready(Err(error)) => {
                    barrier.reservation = None;
                    let error = self.worker_error().unwrap_or(error);
                    barrier.result = Some(Err(error));
                }
            }
        }

        if barrier.result.is_none() {
            if let Some(response) = barrier.response.as_mut() {
                match Pin::new(response).poll(context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(result)) => {
                        barrier.response = None;
                        barrier.result = Some(result);
                    }
                    Poll::Ready(Err(_)) => {
                        barrier.response = None;
                        barrier.result =
                            Some(Err(self.worker_error().unwrap_or(OutputError::Closed)));
                    }
                }
            } else {
                return Poll::Pending;
            }
        }

        Poll::Ready(
            barrier
                .result
                .take()
                .expect("a completed barrier has a result"),
        )
    }

    fn poll_flush_barrier(&mut self, context: &mut Context<'_>) -> Poll<Result<(), OutputError>> {
        let mut barrier = self
            .barrier
            .take()
            .expect("flush barrier is installed before it is polled");
        let result = self.poll_barrier_reply(&mut barrier, context);
        match result {
            Poll::Pending => {
                self.barrier = Some(barrier);
                Poll::Pending
            }
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(error)) => {
                let result = if self.worker_error().is_some() {
                    self.fail_from_worker(error)
                } else {
                    self.fail(error)
                };
                Poll::Ready(Err(result))
            }
        }
    }

    fn poll_close_barrier(&mut self, context: &mut Context<'_>) -> Poll<Result<(), OutputError>> {
        let mut barrier = self
            .barrier
            .take()
            .expect("close barrier is installed before it is polled");

        let reply_result = self.poll_barrier_reply(&mut barrier, context);
        if let Poll::Ready(result) = reply_result {
            barrier.result = Some(result);
        }

        // The close reply is sent immediately before the worker returns. Join
        // the task as well, rather than treating the reply as task completion.
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
                self.state = PumpState::Closed;
                Poll::Ready(Ok(()))
            }
            Err(error) => Poll::Ready(Err(self.fail(error))),
        }
    }
}

impl<V: 'static> Sink<OutputBatch<V>> for PumpSink<V> {
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
                "output pump has a flush barrier in progress",
            ))));
        }
        if this.ready_permit.is_some() {
            return Poll::Ready(Ok(()));
        }
        this.poll_batch_reservation(context)
    }

    fn start_send(self: Pin<&mut Self>, batch: OutputBatch<V>) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if let Some(error) = this.state_error() {
            return Err(error);
        }
        if let Some(error) = this.worker_error() {
            return Err(this.fail_from_worker(error));
        }
        if this.worker.is_none() {
            return Err(this.fail(OutputError::Closed));
        }
        if this.barrier.is_some() {
            return Err(this.fail(OutputError::backend(
                "output pump has a flush barrier in progress",
            )));
        }

        let Some(permit) = this.ready_permit.take() else {
            this.reservation = None;
            return Err(this.fail(OutputError::backend(
                "output pump was not ready for start_send",
            )));
        };
        let _sender = permit.send(PumpCommand::Batch(batch));
        Ok(())
    }

    fn poll_flush(
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
        if this.barrier.is_none() {
            // A readiness reservation not followed by start_send is canceled
            // before the barrier takes its place in the bounded queue.
            this.ready_permit = None;
            this.reservation = None;
            let sender = this
                .sender
                .as_ref()
                .expect("open pump has a command sender");
            this.barrier = Some(Barrier::new(BarrierKind::Flush, sender));
        }
        if this
            .barrier
            .as_ref()
            .is_some_and(|barrier| barrier.kind != BarrierKind::Flush)
        {
            return Poll::Ready(Err(this.fail(OutputError::Closed)));
        }
        this.poll_flush_barrier(context)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        match this.state {
            PumpState::Closed => return Poll::Ready(Err(OutputError::Closed)),
            PumpState::Failed(ref error) => {
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
            PumpState::Open | PumpState::Closing => {}
        }

        if matches!(this.state, PumpState::Open) {
            // Do not return a worker error before joining the task. A worker
            // can record its sticky error just before it exits, and close must
            // still wait for that termination path.
            this.poll_worker(context);
            if this.worker.is_none() && this.worker_error().is_none() {
                return Poll::Ready(Err(this.fail(OutputError::Closed)));
            }
            this.ready_permit = None;
            this.reservation = None;
            this.state = PumpState::Closing;
        }

        // If a caller canceled a pending flush and closes immediately, finish
        // that FIFO barrier before appending the close barrier.
        if this
            .barrier
            .as_ref()
            .is_some_and(|barrier| barrier.kind == BarrierKind::Flush)
        {
            match this.poll_flush_barrier(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    // The worker closes the downstream after a failed flush;
                    // wait for that cleanup before exposing the error.
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
                .expect("closing pump has a command sender");
            this.barrier = Some(Barrier::new(BarrierKind::Close, sender));
        }
        this.poll_close_barrier(context)
    }
}

fn reserve<V: 'static>(sender: CommandSender<V>) -> Reservation<V> {
    Box::pin(async move {
        sender
            .reserve_owned()
            .await
            .map_err(|_| OutputError::Closed)
    })
}

async fn run_worker<V: 'static>(
    mut writer: OutputWriter<V>,
    mut receiver: bounded::Receiver<PumpCommand<V>>,
    state: Rc<RefCell<WorkerState>>,
) {
    let mut downstream_closed = false;
    while let Some(command) = receiver.recv().await {
        match command {
            PumpCommand::Batch(batch) => {
                // Keep batches queued until an explicit flush/close barrier.
                // `feed` advances the previous accepted operation on the next
                // command and lets the bounded command queue overlap producer
                // evaluation with backend publication.
                if let Err(error) = writer.feed(batch).await {
                    state.borrow_mut().error = Some(error);
                    break;
                }
            }
            PumpCommand::Flush(reply) => {
                let result = writer.flush().await;
                if let Some(error) = result.as_ref().err() {
                    state.borrow_mut().error = Some(error.clone());
                }
                let failed = result.is_err();
                let _ = reply.send(result);
                if failed {
                    break;
                }
            }
            PumpCommand::Close(reply) => {
                // Cleanup is best effort but ordered: always attempt close even
                // when the flush barrier fails.
                let flush = writer.flush().await;
                let close = writer.close().await;
                let result = match close {
                    Err(error) => Err(error),
                    Ok(()) => flush,
                };
                if let Some(error) = result.as_ref().err() {
                    state.borrow_mut().error = Some(error.clone());
                }
                downstream_closed = true;
                let _ = reply.send(result);
                break;
            }
        }
    }

    // A producer can fail or disappear without sending a close barrier. The
    // worker still owns the downstream writer, so it must drive close exactly
    // once before terminating.
    if !downstream_closed {
        let writer_had_error = writer.error().is_some();
        if let Err(error) = writer.close().await {
            let mut state = state.borrow_mut();
            state.error = if writer_had_error {
                // `OutputWriter::close` already includes its retained primary
                // error in this result.
                Some(error)
            } else {
                Some(match state.error.take() {
                    Some(primary) => super::combine_errors(primary, error),
                    None => error,
                })
            };
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
    };

    use futures::{
        Sink,
        future::{join, poll_fn},
        task::noop_waker_ref,
    };

    use super::*;
    use crate::core::{OutputBatch, OutputError, OutputWriter, VarName};

    #[derive(Clone, Default)]
    struct Gate {
        open: Rc<Cell<bool>>,
        started: Rc<Cell<bool>>,
        waker: Rc<RefCell<Option<std::task::Waker>>>,
    }

    impl Gate {
        fn open(&self) {
            self.open.set(true);
            if let Some(waker) = self.waker.borrow_mut().take() {
                waker.wake();
            }
        }

        async fn open_when_started(self) {
            poll_fn(|context| {
                if self.started.get() {
                    self.open();
                    Poll::Ready(())
                } else {
                    context.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await;
        }
    }

    struct GatedSink {
        gate: Gate,
        batches: Rc<Cell<usize>>,
        flushes: Rc<Cell<usize>>,
        closes: Rc<Cell<usize>>,
    }

    impl GatedSink {
        fn poll_gate(&self, context: &mut Context<'_>) -> Poll<Result<(), OutputError>> {
            self.gate.started.set(true);
            if self.gate.open.get() {
                Poll::Ready(Ok(()))
            } else {
                *self.gate.waker.borrow_mut() = Some(context.waker().clone());
                Poll::Pending
            }
        }
    }

    impl Sink<OutputBatch<i32>> for GatedSink {
        type Error = OutputError;

        fn poll_ready(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            self.get_mut().poll_gate(context)
        }

        fn start_send(self: Pin<&mut Self>, _batch: OutputBatch<i32>) -> Result<(), Self::Error> {
            let this = self.get_mut();
            if !this.gate.open.get() {
                return Err(OutputError::backend("gated sink was not open"));
            }
            this.batches.set(this.batches.get() + 1);
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            let this = self.get_mut();
            match this.poll_gate(context) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(())) => {
                    this.flushes.set(this.flushes.get() + 1);
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            }
        }

        fn poll_close(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            let this = self.get_mut();
            match this.poll_gate(context) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(())) => {
                    this.closes.set(this.closes.get() + 1);
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            }
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
                return Err(OutputError::backend("pump worker failure"));
            }
            if !self.ready {
                return Err(OutputError::backend("pump cleanup sink was not ready"));
            }
            self.ready = false;
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            if self.fail_flush {
                Poll::Ready(Err(OutputError::backend("pump flush failure")))
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
                Poll::Ready(Err(OutputError::backend("pump close failure")))
            } else {
                Poll::Ready(Ok(()))
            }
        }
    }

    fn batch(value: i32) -> OutputBatch<i32> {
        OutputBatch::update(VarName::new("x"), value)
    }

    fn pump(
        gate: Gate,
        capacity: usize,
    ) -> (
        OutputWriter<i32>,
        Rc<Cell<usize>>,
        Rc<Cell<usize>>,
        Rc<Cell<usize>>,
        Rc<LocalExecutor<'static>>,
    ) {
        let batches = Rc::new(Cell::new(0));
        let flushes = Rc::new(Cell::new(0));
        let closes = Rc::new(Cell::new(0));
        let writer = OutputWriter::from_sink(GatedSink {
            gate,
            batches: Rc::clone(&batches),
            flushes: Rc::clone(&flushes),
            closes: Rc::clone(&closes),
        });
        let executor = Rc::new(LocalExecutor::new());
        let pump = OutputPump::new(writer, Rc::clone(&executor), capacity).unwrap();
        (pump, batches, flushes, closes, executor)
    }

    #[test]
    fn flush_is_a_downstream_flush_barrier() {
        smol::block_on(async {
            let gate = Gate::default();
            let opener = gate.clone();
            let (mut writer, batches, flushes, closes, executor) = pump(gate, 2);
            let operation = async move {
                writer.feed(batch(1)).await.unwrap();
                writer.flush().await.unwrap();
                writer
            };
            let opener = opener.open_when_started();
            let (writer, ()) = executor.run(join(operation, opener)).await;

            assert_eq!(batches.get(), 1);
            assert_eq!(flushes.get(), 1);
            assert_eq!(closes.get(), 0);
            drop(writer);
        });
    }

    #[test]
    fn close_waits_for_downstream_close_and_worker() {
        smol::block_on(async {
            let gate = Gate::default();
            let opener = gate.clone();
            let (mut writer, batches, flushes, closes, executor) = pump(gate, 2);
            let operation = async move {
                writer.feed(batch(1)).await.unwrap();
                writer.close().await.unwrap();
                assert!(writer.is_closed());
            };
            executor
                .run(join(operation, opener.open_when_started()))
                .await;

            assert_eq!(batches.get(), 1);
            assert_eq!(flushes.get(), 1);
            assert_eq!(closes.get(), 1);
            assert!(executor.is_empty());
        });
    }

    #[test]
    fn zero_capacity_is_rejected_without_creating_a_worker() {
        let executor = Rc::new(LocalExecutor::new());
        let result = OutputPump::new(
            OutputWriter::from_sink(CleanupSink {
                fail_send: false,
                fail_flush: false,
                fail_close: false,
                ready: false,
                closed: false,
                closes: Rc::new(Cell::new(0)),
            }),
            Rc::clone(&executor),
            0,
        );
        assert!(matches!(
            result,
            Err(OutputError::Invalid(message))
                if message == "output pump capacity must be greater than zero"
        ));
        assert!(executor.is_empty());

        let result = OutputPump::from_writer(
            OutputWriter::from_sink(CleanupSink {
                fail_send: false,
                fail_flush: false,
                fail_close: false,
                ready: false,
                closed: false,
                closes: Rc::new(Cell::new(0)),
            }),
            Rc::clone(&executor),
            0,
        );
        assert!(matches!(
            result,
            Err(OutputError::Invalid(message))
                if message == "output pump capacity must be greater than zero"
        ));
        assert!(executor.is_empty());
    }

    #[test]
    fn flush_failure_is_followed_by_close_and_error_combination() {
        smol::block_on(async {
            let closes = Rc::new(Cell::new(0));
            let downstream = OutputWriter::from_sink(CleanupSink {
                fail_send: false,
                fail_flush: true,
                fail_close: true,
                ready: false,
                closed: false,
                closes: Rc::clone(&closes),
            });
            let executor = Rc::new(LocalExecutor::new());
            let mut writer = OutputPump::new(downstream, Rc::clone(&executor), 1).unwrap();
            let error = executor
                .run(async {
                    writer.feed(batch(1)).await.unwrap();
                    writer.flush().await.unwrap_err();
                    writer.close().await.unwrap_err()
                })
                .await;
            assert!(error.to_string().contains("pump flush failure"));
            assert!(error.to_string().contains("pump close failure"));
            assert_eq!(closes.get(), 1);
            assert!(executor.is_empty());
        });
    }

    #[test]
    fn worker_failure_is_joined_before_close_returns() {
        smol::block_on(async {
            let closes = Rc::new(Cell::new(0));
            let downstream = OutputWriter::from_sink(CleanupSink {
                fail_send: true,
                fail_flush: false,
                fail_close: false,
                ready: false,
                closed: false,
                closes: Rc::clone(&closes),
            });
            let executor = Rc::new(LocalExecutor::new());
            let mut writer = OutputPump::new(downstream, Rc::clone(&executor), 1).unwrap();
            let error = executor
                .run(async {
                    writer.feed(batch(1)).await.unwrap();
                    writer.close().await.unwrap_err()
                })
                .await;
            assert!(error.to_string().contains("pump worker failure"));
            assert_eq!(closes.get(), 1);
            assert!(executor.is_empty());
        });
    }

    #[test]
    fn local_start_send_failure_closes_live_pump_worker() {
        smol::block_on(async {
            let closes = Rc::new(Cell::new(0));
            let downstream = OutputWriter::from_sink(CleanupSink {
                fail_send: false,
                fail_flush: false,
                fail_close: false,
                ready: false,
                closed: false,
                closes: Rc::clone(&closes),
            });
            let executor = Rc::new(LocalExecutor::new());
            let mut pump = OutputPump::from_writer(downstream, Rc::clone(&executor), 1).unwrap();
            let primary = Pin::new(&mut pump).start_send(batch(1)).unwrap_err();
            let close = executor
                .run(poll_fn(|context| Pin::new(&mut pump).poll_close(context)))
                .await;
            assert_eq!(close, Err(primary));
            assert_eq!(closes.get(), 1);
            assert!(executor.is_empty());
        });
    }

    #[test]
    fn local_barrier_failure_closes_live_pump_worker() {
        smol::block_on(async {
            let closes = Rc::new(Cell::new(0));
            let downstream = OutputWriter::from_sink(CleanupSink {
                fail_send: false,
                fail_flush: false,
                fail_close: false,
                ready: false,
                closed: false,
                closes: Rc::clone(&closes),
            });
            let executor = Rc::new(LocalExecutor::new());
            let mut pump = OutputPump::from_writer(downstream, Rc::clone(&executor), 1).unwrap();
            let mut context = Context::from_waker(noop_waker_ref());
            assert!(Pin::new(&mut pump).poll_flush(&mut context).is_pending());
            let primary = match Pin::new(&mut pump).poll_ready(&mut context) {
                Poll::Ready(Err(error)) => error,
                result => panic!("expected a local barrier failure, got {result:?}"),
            };
            let close = executor
                .run(poll_fn(|context| Pin::new(&mut pump).poll_close(context)))
                .await;
            assert_eq!(close, Err(primary));
            assert_eq!(closes.get(), 1);
            assert!(executor.is_empty());
        });
    }

    #[test]
    fn full_queue_applies_backpressure_until_worker_receives() {
        smol::block_on(async {
            let gate = Gate::default();
            gate.open();
            let (mut writer, _batches, _flushes, _closes, executor) = pump(gate, 1);
            let mut first = Box::pin(writer.feed(batch(1)));
            poll_fn(|context| {
                assert!(first.as_mut().poll(context).is_ready());
                Poll::Ready(())
            })
            .await;
            drop(first);

            let mut second = Box::pin(writer.feed(batch(2)));
            let pending = poll_fn(|context| {
                assert!(second.as_mut().poll(context).is_pending());
                Poll::Ready(())
            });
            pending.await;

            executor.run(async { second.await.unwrap() }).await;
            executor.run(async { writer.close().await.unwrap() }).await;
        });
    }

    #[test]
    fn worker_error_is_sticky_and_visible_to_producer() {
        struct FailingSink;

        impl Sink<OutputBatch<i32>> for FailingSink {
            type Error = OutputError;

            fn poll_ready(
                self: Pin<&mut Self>,
                _context: &mut Context<'_>,
            ) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn start_send(
                self: Pin<&mut Self>,
                _batch: OutputBatch<i32>,
            ) -> Result<(), Self::Error> {
                Err(OutputError::backend("sticky test failure"))
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
                Poll::Ready(Ok(()))
            }
        }

        smol::block_on(async {
            let executor = Rc::new(LocalExecutor::new());
            let writer = OutputWriter::from_sink(FailingSink);
            let mut writer = OutputPump::new(writer, Rc::clone(&executor), 1).unwrap();
            let error = executor
                .run(async {
                    writer.feed(batch(1)).await.unwrap();
                    writer.flush().await.unwrap_err()
                })
                .await;
            assert_eq!(error, OutputError::backend("sticky test failure"));

            assert_eq!(writer.flush().await.unwrap_err(), error);
            assert_eq!(writer.close().await.unwrap_err(), error);
        });
    }
}
