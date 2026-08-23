//! Local output backends for stdout, manual collection, and discard sinks.
//!
//! Every backend consumes the core logical [`OutputBatch`] representation. A
//! backend may filter a value by role, but it never changes the producer's tick
//! boundaries before consuming them.

use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet},
    future::Future,
    io::{self, Write},
    marker::PhantomData,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

use async_trait::async_trait;
use async_unsync::bounded;
use futures::{Sink, future::LocalBoxFuture};

use crate::core::{
    JsonStreamValue, OutputBackend, OutputBatch, OutputError, OutputInterface, OutputRole,
    OutputWriter, StreamData, VarName,
};

/// A local sink that runs one asynchronous operation for each accepted batch.
/// The operation is polled by the sink itself; it is never detached.
pub struct AsyncFnSink<T> {
    operation: Option<Box<dyn FnMut(T) -> LocalBoxFuture<'static, Result<(), OutputError>>>>,
    pending: Option<LocalBoxFuture<'static, Result<(), OutputError>>>,
    close_operation: Option<Box<dyn FnOnce() -> LocalBoxFuture<'static, Result<(), OutputError>>>>,
    close_pending: Option<LocalBoxFuture<'static, Result<(), OutputError>>>,
    state: AsyncFnSinkState,
    ready: bool,
}

#[derive(Clone, Debug)]
enum AsyncFnSinkState {
    Open,
    Closed,
    Failed(OutputError),
}

impl<T: 'static> AsyncFnSink<T> {
    pub fn new<F, Fut>(mut operation: F) -> Self
    where
        F: FnMut(T) -> Fut + 'static,
        Fut: Future<Output = Result<(), OutputError>> + 'static,
    {
        Self::from_local_fn(move |item| Box::pin(operation(item)))
    }

    pub fn with_close<F, Fut, C, CFut>(operation: F, close: C) -> Self
    where
        F: FnMut(T) -> Fut + 'static,
        Fut: Future<Output = Result<(), OutputError>> + 'static,
        C: FnOnce() -> CFut + 'static,
        CFut: Future<Output = Result<(), OutputError>> + 'static,
    {
        let mut sink = Self::new(operation);
        sink.close_operation = Some(Box::new(move || Box::pin(close())));
        sink
    }

    pub fn from_local_fn<F>(operation: F) -> Self
    where
        F: FnMut(T) -> LocalBoxFuture<'static, Result<(), OutputError>> + 'static,
    {
        Self {
            operation: Some(Box::new(operation)),
            pending: None,
            close_operation: None,
            close_pending: None,
            state: AsyncFnSinkState::Open,
            ready: false,
        }
    }

    pub fn is_closed(&self) -> bool {
        matches!(self.state, AsyncFnSinkState::Closed)
    }

    pub fn error(&self) -> Option<&OutputError> {
        match &self.state {
            AsyncFnSinkState::Failed(error) => Some(error),
            AsyncFnSinkState::Open | AsyncFnSinkState::Closed => None,
        }
    }

    fn state_error(&self) -> Option<OutputError> {
        match &self.state {
            AsyncFnSinkState::Open => None,
            AsyncFnSinkState::Closed => Some(OutputError::Closed),
            AsyncFnSinkState::Failed(error) => Some(error.clone()),
        }
    }

    fn fail(&mut self, error: OutputError) -> OutputError {
        self.state = AsyncFnSinkState::Failed(error.clone());
        error
    }

    fn poll_pending(&mut self, context: &mut Context<'_>) -> Poll<Result<(), OutputError>> {
        let Some(pending) = self.pending.as_mut() else {
            self.ready = true;
            return Poll::Ready(Ok(()));
        };
        match pending.as_mut().poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                self.pending = None;
                self.ready = true;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => {
                self.pending = None;
                self.ready = false;
                Poll::Ready(Err(self.fail(error)))
            }
        }
    }
}

impl<T: 'static> Sink<T> for AsyncFnSink<T> {
    type Error = OutputError;

    fn poll_ready(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if let Some(error) = this.state_error() {
            return Poll::Ready(Err(error));
        }
        if this.ready {
            return Poll::Ready(Ok(()));
        }
        this.poll_pending(context)
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if let Some(error) = this.state_error() {
            return Err(error);
        }
        if !this.ready || this.pending.is_some() {
            return Err(this.fail(OutputError::backend(
                "async output sink was not ready for start_send",
            )));
        }
        let operation = this
            .operation
            .as_mut()
            .expect("an open async sink retains its operation");
        this.pending = Some(operation(item));
        this.ready = false;
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
        this.poll_pending(context)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if matches!(this.state, AsyncFnSinkState::Closed) {
            return Poll::Ready(Err(OutputError::Closed));
        }
        if matches!(this.poll_pending(context), Poll::Pending) {
            return Poll::Pending;
        }
        if this.close_pending.is_none() {
            if let Some(close) = this.close_operation.take() {
                this.close_pending = Some(close());
            }
        }
        if let Some(close) = this.close_pending.as_mut() {
            match close.as_mut().poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => this.close_pending = None,
                Poll::Ready(Err(error)) => {
                    this.close_pending = None;
                    let error = match std::mem::replace(&mut this.state, AsyncFnSinkState::Closed) {
                        AsyncFnSinkState::Failed(primary) => super::combine_errors(primary, error),
                        AsyncFnSinkState::Open | AsyncFnSinkState::Closed => error,
                    };
                    return Poll::Ready(Err(error));
                }
            }
        }
        let primary = match std::mem::replace(&mut this.state, AsyncFnSinkState::Closed) {
            AsyncFnSinkState::Failed(error) => Some(error),
            AsyncFnSinkState::Open | AsyncFnSinkState::Closed => None,
        };
        this.operation = None;
        Poll::Ready(primary.map_or(Ok(()), Err))
    }
}

pub type LocalBatchSink<V> = AsyncFnSink<OutputBatch<V>>;

pub fn local_batch_sink<V, F, Fut>(operation: F) -> LocalBatchSink<V>
where
    V: 'static,
    F: FnMut(OutputBatch<V>) -> Fut + 'static,
    Fut: Future<Output = Result<(), OutputError>> + 'static,
{
    AsyncFnSink::new(operation)
}

pub type ManualOutputSender<V> = bounded::Sender<BTreeMap<VarName, V>>;
pub type ManualOutputReceiver<V> = bounded::Receiver<BTreeMap<VarName, V>>;

/// A bounded, consumer-driven backend useful for embedding and tests.
#[derive(Clone, Debug)]
pub struct ManualOutputBackend<V: StreamData> {
    sender: ManualOutputSender<V>,
}

impl<V: StreamData> ManualOutputBackend<V> {
    pub fn new(sender: ManualOutputSender<V>) -> Self {
        Self { sender }
    }

    pub fn channel(capacity: usize) -> (Self, ManualOutputReceiver<V>) {
        let (sender, receiver) = bounded::channel(capacity).into_split();
        (Self::new(sender), receiver)
    }

    pub fn sender(&self) -> &ManualOutputSender<V> {
        &self.sender
    }
}

#[async_trait(?Send)]
impl<V: StreamData> OutputBackend for ManualOutputBackend<V> {
    type Val = V;

    async fn open(
        &self,
        interface: OutputInterface,
    ) -> Result<OutputWriter<Self::Val>, OutputError> {
        let interface = Rc::new(interface);
        let sender = self.sender.clone();
        Ok(OutputWriter::from_sink(LocalBatchSink::new(
            move |batch: OutputBatch<V>| {
                let sender = sender.clone();
                let interface = Rc::clone(&interface);
                async move { send_manual_batch(sender, interface, batch).await }
            },
        )))
    }
}

async fn send_manual_batch<V: StreamData>(
    sender: ManualOutputSender<V>,
    interface: Rc<OutputInterface>,
    batch: OutputBatch<V>,
) -> Result<(), OutputError> {
    interface.validate_batch(&batch)?;
    for tick in batch.ticks() {
        let row = tick
            .updates()
            .map(|update| (update.variable.clone(), update.value.clone()))
            .collect::<BTreeMap<_, _>>();
        sender.send(row).await.map_err(|_| OutputError::Closed)?;
    }
    Ok(())
}

/// A backend that discards complete logical ticks.
#[derive(Clone, Copy, Debug, Default)]
pub struct NullOutputBackend<V: StreamData>(PhantomData<fn() -> V>);

impl<V: StreamData> NullOutputBackend<V> {
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

struct NullSink {
    ready: bool,
    closed: bool,
}

impl<V> Sink<OutputBatch<V>> for NullSink {
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

    fn start_send(mut self: Pin<&mut Self>, _batch: OutputBatch<V>) -> Result<(), Self::Error> {
        if self.closed {
            return Err(OutputError::Closed);
        }
        if !self.ready {
            return Err(OutputError::backend("null sink was not ready"));
        }
        self.ready = false;
        Ok(())
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        if self.closed {
            return Poll::Ready(Err(OutputError::Closed));
        }
        self.ready = true;
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        if self.closed {
            return Poll::Ready(Err(OutputError::Closed));
        }
        self.closed = true;
        self.ready = true;
        Poll::Ready(Ok(()))
    }
}

#[async_trait(?Send)]
impl<V: StreamData> OutputBackend for NullOutputBackend<V> {
    type Val = V;

    async fn open(
        &self,
        _interface: OutputInterface,
    ) -> Result<OutputWriter<Self::Val>, OutputError> {
        Ok(OutputWriter::from_sink(NullSink {
            ready: false,
            closed: false,
        }))
    }
}

/// A discard backend that intentionally closes each opened writer after a
/// logical tick limit. The reusable configuration contains only the limit;
/// operational counters belong to the individual sink created by `open`.
#[derive(Clone, Debug)]
pub struct LimitedNullOutputBackend<V: StreamData> {
    limit: usize,
    _value: PhantomData<fn() -> V>,
}

impl<V: StreamData> LimitedNullOutputBackend<V> {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            _value: PhantomData,
        }
    }

    pub fn limit(&self) -> usize {
        self.limit
    }
}

struct LimitedNullSink {
    limit: usize,
    ticks: usize,
    ready: bool,
    closed: bool,
}

impl<V> Sink<OutputBatch<V>> for LimitedNullSink {
    type Error = OutputError;

    fn poll_ready(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        if self.closed || self.ticks >= self.limit {
            return Poll::Ready(Err(OutputError::Closed));
        }
        self.ready = true;
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, batch: OutputBatch<V>) -> Result<(), Self::Error> {
        if self.closed || self.ticks >= self.limit {
            return Err(OutputError::Closed);
        }
        if !self.ready {
            return Err(OutputError::backend("limited null sink was not ready"));
        }
        let remaining = self.limit.saturating_sub(self.ticks);
        self.ticks = self.ticks.saturating_add(batch.tick_count().min(remaining));
        self.ready = false;
        Ok(())
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        if self.closed {
            return Poll::Ready(Err(OutputError::Closed));
        }
        self.ready = true;
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        if self.closed {
            return Poll::Ready(Err(OutputError::Closed));
        }
        self.closed = true;
        Poll::Ready(Ok(()))
    }
}

#[async_trait(?Send)]
impl<V: StreamData> OutputBackend for LimitedNullOutputBackend<V> {
    type Val = V;

    async fn open(
        &self,
        _interface: OutputInterface,
    ) -> Result<OutputWriter<Self::Val>, OutputError> {
        Ok(OutputWriter::from_sink(LimitedNullSink {
            limit: self.limit,
            ticks: 0,
            ready: false,
            closed: false,
        }))
    }
}

enum StdoutTarget {
    Stdout,
    Writer(Rc<RefCell<Box<dyn Write>>>),
}

impl Clone for StdoutTarget {
    fn clone(&self) -> Self {
        match self {
            Self::Stdout => Self::Stdout,
            Self::Writer(writer) => Self::Writer(Rc::clone(writer)),
        }
    }
}

/// A JSON-capable backend that writes one line per routed value.
#[derive(Clone)]
pub struct StdoutOutputBackend<V> {
    target: StdoutTarget,
    _value: PhantomData<fn() -> V>,
}

impl<V> StdoutOutputBackend<V> {
    pub fn new() -> Self {
        Self {
            target: StdoutTarget::Stdout,
            _value: PhantomData,
        }
    }

    pub fn with_writer<W>(writer: W) -> Self
    where
        W: Write + 'static,
    {
        Self {
            target: StdoutTarget::Writer(Rc::new(RefCell::new(Box::new(writer)))),
            _value: PhantomData,
        }
    }

    pub fn with_shared_writer<W>(writer: Rc<RefCell<W>>) -> Self
    where
        W: Write + 'static,
    {
        Self {
            target: StdoutTarget::Writer(Rc::new(RefCell::new(Box::new(SharedWriter { writer })))),
            _value: PhantomData,
        }
    }
}

impl<V> Default for StdoutOutputBackend<V> {
    fn default() -> Self {
        Self::new()
    }
}

struct SharedWriter<W> {
    writer: Rc<RefCell<W>>,
}

impl<W: Write> Write for SharedWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.writer.borrow_mut().write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.borrow_mut().flush()
    }
}

#[async_trait(?Send)]
impl<V: JsonStreamValue> OutputBackend for StdoutOutputBackend<V> {
    type Val = V;

    async fn open(
        &self,
        interface: OutputInterface,
    ) -> Result<OutputWriter<Self::Val>, OutputError> {
        let interface = Rc::new(interface);
        let auxiliary = Rc::new(
            interface
                .routes_for(OutputRole::Auxiliary)
                .map(|route| route.variable.clone())
                .collect::<BTreeSet<_>>(),
        );
        let target = self.target.clone();
        let row_number = Rc::new(Cell::new(0usize));
        Ok(OutputWriter::from_sink(LocalBatchSink::new(
            move |batch: OutputBatch<V>| {
                let interface = Rc::clone(&interface);
                let auxiliary = Rc::clone(&auxiliary);
                let target = target.clone();
                let row_number = Rc::clone(&row_number);
                async move {
                    interface.validate_batch(&batch)?;
                    write_stdout_batch(batch, &auxiliary, &target, &row_number)
                }
            },
        )))
    }
}

fn write_stdout_batch<V: JsonStreamValue>(
    batch: OutputBatch<V>,
    auxiliary: &BTreeSet<VarName>,
    target: &StdoutTarget,
    row_number: &Cell<usize>,
) -> Result<(), OutputError> {
    match target {
        StdoutTarget::Stdout => {
            let stdout = io::stdout();
            let mut writer = stdout.lock();
            write_stdout_ticks(batch, auxiliary, &mut writer, row_number)?;
            writer
                .flush()
                .map_err(|error| OutputError::backend(format!("failed to flush stdout: {error}")))
        }
        StdoutTarget::Writer(writer) => {
            let mut writer = writer.borrow_mut();
            write_stdout_ticks(batch, auxiliary, &mut **writer, row_number)?;
            writer.flush().map_err(|error| {
                OutputError::backend(format!("failed to flush stdout writer: {error}"))
            })
        }
    }
}

fn write_stdout_ticks<V: JsonStreamValue>(
    batch: OutputBatch<V>,
    auxiliary: &BTreeSet<VarName>,
    writer: &mut dyn Write,
    row_number: &Cell<usize>,
) -> Result<(), OutputError> {
    for tick in batch.ticks() {
        let number = row_number.get();
        row_number.set(number.saturating_add(1));
        for update in tick.updates() {
            if auxiliary.contains(update.variable) || update.value.is_no_val() {
                continue;
            }
            let encoded = update.value.encode_stdout().map_err(|error| {
                OutputError::backend(format!(
                    "failed to encode stdout value for `{}`: {error}",
                    update.variable
                ))
            })?;
            writeln!(writer, "{}[{}] = {}", update.variable, number, encoded).map_err(|error| {
                OutputError::backend(format!("failed to write stdout value: {error}"))
            })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        rc::Rc,
    };

    use super::*;
    use crate::{Value, core::OutputUpdate};

    fn var(name: &str) -> VarName {
        VarName::new(name)
    }

    fn interface(names: &[&str]) -> OutputInterface {
        OutputInterface::from_routes(
            names
                .iter()
                .map(|name| crate::core::OutputRoute::output(var(name))),
        )
        .unwrap()
    }

    #[test]
    fn manual_backend_preserves_logical_ticks() {
        smol::block_on(async {
            let (backend, mut receiver) = ManualOutputBackend::<i32>::channel(8);
            let mut writer = backend.open(interface(&["x", "y"])).await.unwrap();
            writer
                .send_and_flush(
                    OutputBatch::from_ticks(vec![
                        vec![
                            OutputUpdate::new(var("x"), 1),
                            OutputUpdate::new(var("y"), 10),
                        ],
                        vec![
                            OutputUpdate::new(var("x"), 2),
                            OutputUpdate::new(var("y"), 20),
                        ],
                    ])
                    .unwrap(),
                )
                .await
                .unwrap();
            let first = receiver.recv().await.unwrap();
            let second = receiver.recv().await.unwrap();
            assert_eq!(first[&var("x")], 1);
            assert_eq!(second[&var("y")], 20);
            writer.close().await.unwrap();
        });
    }

    #[test]
    fn limited_null_counts_ticks_and_closes_at_the_limit() {
        smol::block_on(async {
            let backend = LimitedNullOutputBackend::<i32>::new(2);
            let mut writer = backend.open(interface(&["x"])).await.unwrap();
            writer
                .send(
                    OutputBatch::from_ticks(vec![
                        vec![OutputUpdate::new(var("x"), 1)],
                        vec![OutputUpdate::new(var("x"), 2)],
                    ])
                    .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(
                writer.send(OutputBatch::update(var("x"), 3)).await,
                Err(OutputError::Closed)
            );
        });
    }

    #[test]
    fn stdout_filters_auxiliary_and_no_val_while_advancing_ticks() {
        smol::block_on(async {
            let output = Rc::new(RefCell::new(Vec::<u8>::new()));
            let backend = StdoutOutputBackend::<Value>::with_shared_writer(Rc::clone(&output));
            let interface = OutputInterface::from_routes([
                crate::core::OutputRoute::output(var("x")),
                crate::core::OutputRoute::auxiliary(var("debug")),
            ])
            .unwrap();
            let mut writer = backend.open(interface).await.unwrap();
            writer
                .send(
                    OutputBatch::from_ticks(vec![
                        vec![
                            OutputUpdate::new(var("x"), Value::Int(1)),
                            OutputUpdate::new(var("debug"), Value::Int(99)),
                        ],
                        vec![OutputUpdate::new(var("x"), Value::NoVal)],
                    ])
                    .unwrap(),
                )
                .await
                .unwrap();
            writer.close().await.unwrap();
            assert_eq!(
                String::from_utf8(output.borrow().clone()).unwrap(),
                "x[0] = Int(1)\n"
            );
        });
    }

    #[test]
    fn async_sink_runs_close_after_a_data_failure() {
        smol::block_on(async {
            let closes = Rc::new(Cell::new(0));
            let close_counter = Rc::clone(&closes);
            let sink = AsyncFnSink::with_close(
                |_batch: OutputBatch<i32>| async { Err(OutputError::backend("publish failed")) },
                move || async move {
                    close_counter.set(close_counter.get() + 1);
                    Ok(())
                },
            );
            let mut writer = OutputWriter::from_sink(sink);
            let first = writer
                .send_and_flush(OutputBatch::update(var("x"), 1))
                .await;
            assert_eq!(first, Err(OutputError::Backend("publish failed".into())));
            let _ = writer.close().await;
            assert_eq!(closes.get(), 1);
        });
    }
}
