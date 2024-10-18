use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;

use futures::future::join_all;
use futures::stream;
use futures::stream::BoxStream;
use futures::StreamExt;
use std::fmt::Debug;
use tokio::select;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tokio_util::sync::DropGuard;

use crate::core::ExpressionTyping;
use crate::core::InputProvider;
use crate::core::Monitor;
use crate::core::MonitoringSemantics;
use crate::core::Specification;
use crate::core::StreamExpr;
use crate::core::StreamSystem;
use crate::core::StreamTransformationFn;
use crate::core::TypeAnnotated;
use crate::core::TypeSystem;
use crate::core::{OutputStream, StreamContext, VarName};

enum WaitingStream<S> {
    Arrived(S),
    Waiting(oneshot::Receiver<S>),
}

// Convert a receiver of a stream to a stream which waits for the stream to
// arrive before supplying the first element
fn oneshot_to_stream<T: 'static>(receiver: oneshot::Receiver<OutputStream<T>>) -> OutputStream<T> {
    Box::pin(stream::unfold(
        WaitingStream::Waiting(receiver),
        |waiting_stream| async move {
            match waiting_stream {
                WaitingStream::Arrived(mut stream) => {
                    Some((stream.next().await?, WaitingStream::Arrived(stream)))
                }
                WaitingStream::Waiting(receiver) => match receiver.await {
                    Ok(mut stream) => Some((stream.next().await?, WaitingStream::Arrived(stream))),
                    Err(_) => None,
                },
            }
        },
    ))
}

// Wrap a stream in a drop guard to ensure that the associated cancellation
// token is not dropped before the stream has completed or been dropped
fn drop_guard_stream<T: 'static>(
    stream: OutputStream<T>,
    drop_guard: Arc<DropGuard>,
) -> OutputStream<T> {
    Box::pin(stream::unfold(
        (stream, drop_guard),
        |(mut stream, drop_guard)| async move {
            // let num_copies = Arc::strong_count(&drop_guard);
            // println!("Currently have {} copies of drop guard", num_copies);
            match stream.next().await {
                Some(val) => Some((val, (stream, drop_guard))),
                None => None,
            }
        },
    ))
}

struct DropGuardStream {
    drop_guard: Arc<DropGuard>,
}

impl StreamTransformationFn for DropGuardStream {
    fn transform<T: 'static>(&self, stream: OutputStream<T>) -> OutputStream<T> {
        drop_guard_stream(stream, self.drop_guard.clone())
    }
}

// An actor which manages a single variable by providing channels to get
// the output stream and publishing values to all subscribers
async fn manage_var<V: Clone + Debug + Send + 'static>(
    var: VarName,
    mut input_stream: OutputStream<V>,
    mut channel_request_rx: mpsc::Receiver<oneshot::Sender<OutputStream<V>>>,
    mut ready: watch::Receiver<bool>,
    cancel: CancellationToken,
) {
    let mut senders: Vec<mpsc::Sender<V>> = vec![];
    let mut send_requests = vec![];
    // Gathering senders
    loop {
        select! {
            biased;
            _ = cancel.cancelled() => {
                // println!("Ending manage_var for {} due to cancellation", var);
                return;
            }
            channel_sender = channel_request_rx.recv() => {
                if let Some(channel_sender) = channel_sender {
                    send_requests.push(channel_sender);
                }
                // We don't care if we stop receiving requests
            }
            _ = ready.changed() => {
                // println!("Moving to stage 2");
                break;
            }
        }
    }

    // Sending subscriptions
    if send_requests.len() == 1 {
        // Special case handling for a single request; just send the input stream
        let channel_sender = send_requests.pop().unwrap();
        if let Err(_) = channel_sender.send(input_stream) {
            panic!("Failed to send stream for {var} to requester");
        }
        // We directly re-forwarded the input stream, so we are done
        return;
    } else {
        for channel_sender in send_requests {
            let (tx, rx) = mpsc::channel(10);
            senders.push(tx);
            let stream = ReceiverStream::new(rx);
            // let typed_stream = SS::to_typed_stream(typ, Box::pin(stream));
            if let Err(_) = channel_sender.send(Box::pin(stream)) {
                panic!("Failed to send stream for {var} to requester");
            }
        }
    }

    // Distributing data
    loop {
        select! {
            biased;
            _ = cancel.cancelled() => {
                // println!("Ending manage_var for {} due to cancellation", var);
                return;
            }
            // Bad things will happen if this is called before everyone has subscribed
            data = input_stream.next() => {
                if let Some(data) = data {
                    assert!(!senders.is_empty());
                    let send_futs = senders.iter().map(|sender| sender.send(data.clone()));
                    for res in join_all(send_futs).await {
                        if let Err(_) = res {
                            println!("Failed to send data {:?} for {} due to no receivers", data, var);
                        }
                    }
                } else {
                    // println!("Ending manage_var for {} as out of input data", var);
                    return;
                }
            }
        }
    }
}

// Task for moving data from a channel to an broadcast channel
// with a clock used to control the rate of data distribution
async fn distribute<V: Clone + Debug + Send + 'static>(
    mut input_stream: OutputStream<V>,
    send: broadcast::Sender<V>,
    mut clock: watch::Receiver<usize>,
    cancellation_token: CancellationToken,
) {
    let mut clock_old = 0;
    loop {
        select! {
            biased;
            _ = cancellation_token.cancelled() => {
                return;
            }
            clock_upd = clock.changed() => {
                if clock_upd.is_err() {
                    // println!("Clock channel closed");
                    return;
                }
                let clock_new = *clock.borrow_and_update();
                // println!("Monitoring between {} and {}", clock_old, clock_new);
                for _ in clock_old+1..=clock_new {
                    // println!("Monitoring {}", i);
                    select! {
                        biased;
                        _ = cancellation_token.cancelled() => {
                            return;
                        }
                        data = input_stream.next() => {
                            if let Some(data) = data {
                                // println!("Distributing data {:?}", data);
                                // let data_copy = data.clone();
                                if let Err(_) = send.send(data) {
                                    // println!("sent")
                                    // println!("Failed to send data {:?} due to no receivers (err = {:?})", data_copy, e);
                                }
                            } else {
                                // println!("Ending distribute");
                                return;
                            }
                        }
                        // TODO: should we have a release lock here for deadlock prevention?
                    }
                    // tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
                clock_old = clock_new;
            }
        }
    }
}

// Task for moving data from an input stream to an output channel
async fn monitor<V: Clone + Debug + Send + 'static>(
    mut input_stream: OutputStream<V>,
    send: mpsc::Sender<V>,
    cancellation_token: CancellationToken,
) {
    loop {
        select! {
            biased;
            _ = cancellation_token.cancelled() => {
                return;
            }
            data = input_stream.next() => {
                match data {
                    Some(data) => {
                        // println!("Monitored data {:?}", data);
                        if let Err(_) = send.send(data).await {
                            return;
                        }
                    }
                    None => {
                        return;
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
struct VarData<SS: StreamSystem> {
    typ: <SS::TypeSystem as TypeSystem>::Type,
    // sender: mpsc::Sender<<SS::TypeSystem as TypeSystem>::TypedValue>,
    requester:
        mpsc::Sender<oneshot::Sender<OutputStream<<SS::TypeSystem as TypeSystem>::TypedValue>>>,
}

// A struct representing the top-level stream context for an async monitor
struct AsyncVarExchange<SS: StreamSystem> {
    var_data: BTreeMap<VarName, VarData<SS>>,
    cancellation_token: CancellationToken,
    #[allow(dead_code)]
    // This is used for RAII to cancel background tasks when the async var
    // exchange is dropped
    drop_guard: Arc<DropGuard>,
    vars_requested: (watch::Sender<usize>, watch::Receiver<usize>),
}

impl<SS: StreamSystem> AsyncVarExchange<SS> {
    fn new(
        var_data: BTreeMap<VarName, VarData<SS>>,
        cancellation_token: CancellationToken,
        drop_guard: Arc<DropGuard>,
    ) -> Self {
        let vars_requested = watch::channel(0);
        AsyncVarExchange {
            var_data,
            cancellation_token,
            drop_guard,
            vars_requested,
        }
    }
}

impl<SS: StreamSystem> StreamContext<SS> for Arc<AsyncVarExchange<SS>> {
    fn var(&self, var: &VarName) -> Option<SS::TypedStream> {
        // Retrieve the channel used to request the stream
        let var_data = self.var_data.get(var)?;
        let requester = var_data.requester.clone();

        // Request the stream
        let (tx, rx) = oneshot::channel();
        let var = var.clone();

        self.vars_requested.0.send_modify(|x| *x += 1);
        let var_sender = self.vars_requested.0.clone();

        tokio::spawn(async move {
            if let Err(e) = requester.send(tx).await {
                println!(
                    "Failed to request stream for {} due to no receivers (err = {:?})",
                    var, e
                );
            }
            var_sender.send_modify(|x| *x -= 1);
            // tx2.send(()).unwrap();
        });

        // Create a lazy typed stream from the request
        let stream = oneshot_to_stream(rx);
        // let stream = drop_guard_stream(stream, self.drop_guard.clone());
        let typed_stream = SS::to_typed_stream(var_data.typ.clone(), stream);
        Some(typed_stream)
    }

    fn advance(&self) {
        // Do nothing
    }

    fn subcontext(&self, history_length: usize) -> Box<dyn StreamContext<SS>> {
        Box::new(SubMonitor::new(self.clone(), history_length))
    }
}

// A subcontext which consumes data for a subset of the variables and makes
// it available when evaluating a deferred expression
struct SubMonitor<SS: StreamSystem> {
    parent: Arc<AsyncVarExchange<SS>>,
    senders: BTreeMap<
        VarName,
        (
            <SS::TypeSystem as TypeSystem>::Type,
            broadcast::Sender<<SS::TypeSystem as TypeSystem>::TypedValue>,
        ),
    >,
    buffer_size: usize,
    progress_sender: watch::Sender<usize>,
}

impl<SS: StreamSystem> SubMonitor<SS> {
    fn new(parent: Arc<AsyncVarExchange<SS>>, buffer_size: usize) -> Self {
        let mut senders = BTreeMap::new();

        for (var, var_data) in parent.var_data.iter() {
            senders.insert(
                var.clone(),
                (var_data.typ.clone(), broadcast::Sender::new(100)),
            );
        }

        let progress_sender = watch::channel(0).0;

        SubMonitor {
            parent,
            senders,
            buffer_size,
            progress_sender,
        }
        .start_monitors()
    }

    fn start_monitors(self) -> Self {
        for var in self.parent.var_data.keys() {
            let (send, recv) = mpsc::channel(self.buffer_size);
            let input_stream = self.parent.var(var).unwrap();
            let (_, child_sender) = self.senders.get(var).unwrap().clone();
            let clock = self.progress_sender.subscribe();
            tokio::spawn(distribute(
                Box::pin(ReceiverStream::new(recv)),
                child_sender,
                clock,
                self.parent.cancellation_token.clone(),
            ));
            tokio::spawn(monitor(
                Box::pin(input_stream),
                send.clone(),
                self.parent.cancellation_token.clone(),
            ));
        }

        self
    }
}

impl<SS: StreamSystem> StreamContext<SS> for SubMonitor<SS> {
    fn var(&self, var: &VarName) -> Option<SS::TypedStream> {
        let (typ, sender) = self.senders.get(var).unwrap();

        let recv: broadcast::Receiver<
            <<SS as StreamSystem>::TypeSystem as TypeSystem>::TypedValue,
        > = sender.subscribe();
        let stream: OutputStream<<<SS as StreamSystem>::TypeSystem as TypeSystem>::TypedValue> =
            Box::pin(BroadcastStream::new(recv).map(|x| x.unwrap()));

        Some(SS::to_typed_stream(typ.clone(), stream))
    }

    fn subcontext(&self, history_length: usize) -> Box<dyn StreamContext<SS>> {
        // TODO: consider if this is the right approach; creating a subcontext
        // is only used if eval is called within an eval, and it will require
        // careful thought to decide how much history should be passed down
        // (the current implementation passes down none)
        self.parent.subcontext(history_length)
    }

    fn advance(&self) {
        self.progress_sender.send_modify(|x| *x += 1)
    }
}

pub struct AsyncMonitorRunner<ET, SS, S, M>
where
    ET: ExpressionTyping,
    ET::TypedExpr: StreamExpr<<ET::TypeSystem as TypeSystem>::Type>,
    SS: StreamSystem<TypeSystem = ET::TypeSystem>,
    S: MonitoringSemantics<ET::TypedExpr, SS::TypedStream, StreamSystem = SS>,
    M: Specification<ET> + TypeAnnotated<ET::TypeSystem>,
{
    model: M,
    output_streams: BTreeMap<VarName, SS::TypedStream>,
    #[allow(dead_code)]
    // This is used for RAII to cancel background tasks when the async var
    // exchange is dropped
    cancellation_guard: Arc<DropGuard>,
    semantics_t: PhantomData<S>,
    expression_typing_t: PhantomData<ET>,
}

impl<
        ET: ExpressionTyping,
        SS: StreamSystem<TypeSystem = ET::TypeSystem>,
        S: MonitoringSemantics<ET::TypedExpr, SS::TypedStream, StreamSystem = SS>,
        M: Specification<ET> + TypeAnnotated<ET::TypeSystem>,
    > Monitor<ET, SS, S, M> for AsyncMonitorRunner<ET, SS, S, M>
where
    ET::TypedExpr: StreamExpr<<ET::TypeSystem as TypeSystem>::Type>,
{
    fn new(model: M, mut input_streams: impl InputProvider<SS>) -> Self {
        let cancellation_token = CancellationToken::new();
        let cancellation_guard = Arc::new(cancellation_token.clone().drop_guard());

        let mut var_data = BTreeMap::new();
        let mut to_launch_in = vec![];
        let (var_exchange_ready, watch_rx) = watch::channel(false);

        // Launch monitors for each input variable
        for var in model.input_vars().iter() {
            let typ = model.type_of_var(var).unwrap();
            let (tx1, rx1) = mpsc::channel(100000);
            let input_stream = input_streams.input_stream(var).unwrap();
            var_data.insert(
                var.clone(),
                VarData {
                    typ,
                    requester: tx1,
                },
            );
            to_launch_in.push((var.clone(), input_stream, rx1));
        }

        let mut to_launch_out = vec![];

        // Create tasks for each output variable
        for var in model.output_vars().iter() {
            let typ = model.type_of_var(var).unwrap();
            let (tx1, rx1) = mpsc::channel(100000);
            to_launch_out.push((var.clone(), rx1));
            var_data.insert(
                var.clone(),
                VarData {
                    typ,
                    requester: tx1,
                },
            );
        }

        let var_exchange = Arc::new(AsyncVarExchange::new(
            var_data,
            cancellation_token.clone(),
            cancellation_guard.clone(),
        ));

        // Launch monitors for each output variable as returned by the monitor
        let output_streams = model
            .output_vars()
            .iter()
            .map(|var| (var.clone(), var_exchange.var(var).unwrap()))
            .collect();
        let mut async_streams = vec![];
        for (var, _) in to_launch_out.iter() {
            let expr = model.var_expr(&var).unwrap();
            let stream = S::to_async_stream(expr, &var_exchange);
            let stream = SS::transform_stream(
                DropGuardStream {
                    drop_guard: cancellation_guard.clone(),
                },
                stream,
            );
            async_streams.push(stream);
        }

        for ((var, rx1), stream) in to_launch_out.into_iter().zip(async_streams) {
            tokio::spawn(manage_var(
                var,
                Box::pin(stream),
                rx1,
                watch_rx.clone(),
                cancellation_token.clone(),
            ));
        }
        for (var, input_stream, rx1) in to_launch_in {
            tokio::spawn(manage_var(
                var,
                Box::pin(input_stream),
                rx1,
                watch_rx.clone(),
                cancellation_token.clone(),
            ));
        }

        // Don't start producing until we have requested all subscriptions
        let mut num_requested = var_exchange.vars_requested.1.clone();
        tokio::spawn(async move {
            if let Err(e) = num_requested.wait_for(|x| *x == 0).await {
                panic!("Failed to wait for all vars to be requested: {:?}", e);
            }
            var_exchange_ready.send(true).unwrap();
        });

        Self {
            model,
            // var_exchange,
            output_streams,
            semantics_t: PhantomData,
            expression_typing_t: PhantomData,
            cancellation_guard,
        }
    }

    fn spec(&self) -> &M {
        &self.model
    }

    fn monitor_outputs(
        &mut self,
    ) -> BoxStream<'static, BTreeMap<VarName, <ET::TypeSystem as TypeSystem>::TypedValue>> {
        let output_streams = mem::take(&mut self.output_streams);
        let mut outputs = self.model.output_vars();
        outputs.sort();

        Box::pin(stream::unfold(
            (output_streams, outputs),
            |(mut output_streams, outputs)| async move {
                // println!("Monitoring outputs");
                let mut futures = vec![];
                for (_, stream) in output_streams.iter_mut() {
                    futures.push(stream.next());
                }

                let next_vals = join_all(futures).await;
                let mut res: BTreeMap<VarName, <ET::TypeSystem as TypeSystem>::TypedValue> =
                    BTreeMap::new();
                for (var, val) in outputs.clone().iter().zip(next_vals) {
                    res.insert(
                        var.clone(),
                        match val {
                            Some(val) => val,
                            None => return None,
                        },
                    );
                }
                return Some((res, (output_streams, outputs)));
            },
        ))
            as BoxStream<'static, BTreeMap<VarName, <ET::TypeSystem as TypeSystem>::TypedValue>>
    }
}
