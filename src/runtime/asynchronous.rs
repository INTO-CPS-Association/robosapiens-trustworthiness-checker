use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use futures::future::join_all;
use tokio::select;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tokio_util::sync::DropGuard;
use tracing::Level;
use tracing::debug;
use tracing::info;
use tracing::info_span;
use tracing::instrument;
use tracing::warn;

use crate::core::InputProvider;
use crate::core::Monitor;
use crate::core::MonitoringSemantics;
use crate::core::OutputHandler;
use crate::core::Specification;
use crate::core::{OutputStream, StreamContext, StreamData, VarName};
use crate::stream_utils::{drop_guard_stream, oneshot_to_stream};

/* An actor which manages access to a stream variable by tracking the
 * subscribers to the variable and creating independent output streams to
 * forwards new data to each subscribers.
 *
 * This actor goes through two stages:
 *  1. Gathering subscribers: In this stage, the actor waits for all subscribers
 *     to request output streams
 *  2. Distributing data: In this stage, the actor forwards data from the input
 *     stream to all subscribers.
 *
 * This has parameters:
 * - var: the name of the variable being managed
 * - input_stream: the stream of inputs which we are distributing
 * - channel_request_rx: a mpsc channel on which we receive requests for a
 *   new subscription to the variable. We are passed a oneshot channel which
 *   we can used to send the output stream to the requester.
 * - ready: a watch channel which is used to signal when all subscribers have
 *   requested the stream and determines when we move to stage 2 to start
 *   distributing data
 * - cancel: a cancellation token which is used to signal when the actor should
 *   terminate
 */
async fn manage_var<V: StreamData>(
    var: VarName,
    mut input_stream: OutputStream<V>,
    mut channel_request_rx: mpsc::Receiver<oneshot::Sender<OutputStream<V>>>,
    mut ready: watch::Receiver<bool>,
    cancel: CancellationToken,
) {
    let mut senders: Vec<mpsc::Sender<V>> = vec![];
    let mut send_requests = vec![];

    // Tracing for variable manager task
    let manage_var = info_span!("manage_var", var = format!("{:?}", var.clone()));
    let _ = manage_var.enter();

    /* Stage 1. Gathering subscribers */
    loop {
        select! {
            biased;
            _ = cancel.cancelled() => {
                info!(?var, "Ending manage_var due to cancellation");
                return;
            }
            _ = ready.wait_for(|x| *x) => {
                debug!(?var, "Moving to stage 2");
                break;
            }
            channel_sender = channel_request_rx.recv() => {
                if let Some(channel_sender) = channel_sender {
                    debug!(?var, "Received request for var");
                    send_requests.push(channel_sender);
                }
                // We don't care if we stop receiving requests
                debug!(?var, "Channel sender went away for var");
            }
        }
    }

    /* Send subscriptions to everyone who has requested one */
    if send_requests.len() == 1 {
        /* Special case handling for a single subscription request: just send
         * the input stream directly to the subscriber and skip stage 2 */
        let channel_sender = send_requests.pop().unwrap();
        if let Err(_) = channel_sender.send(input_stream) {
            panic!("Failed to send stream for {var} to requester");
        }
        // We directly re-forwarded the input stream, so we are done
        return;
    } else {
        /* Normal case: create a new channel for each subscriber and send
         * them a stream generated from the receiving end of this channel */
        for channel_sender in send_requests {
            let (tx, rx) = mpsc::channel(10);
            senders.push(tx);
            let stream = ReceiverStream::new(rx);
            if let Err(_) = channel_sender.send(Box::pin(stream)) {
                // panic!("Failed to send stream for {var} to requester");
                warn!(?var, "Failed to send stream for var to requester");
            }
        }
    }

    /* Stage 2. Distributing data */
    loop {
        select! {
            biased;
            _ = cancel.cancelled() => {
                info!(?var, "Ending manage_var due to cancellation");
                return;
            }
            // Bad things will happen if this is called before everyone has subscribed
            data = input_stream.next() => {
                if let Some(data) = data {
                    debug!(?var, ?data, "Sending var data");
                    // Senders can be empty if an input is not actually used
                    // assert!(!senders.is_empty());
                    let send_futs = senders.iter().map(|sender| sender.send(data.clone()));
                    for res in join_all(send_futs).await {
                        if let Err(err) = res {
                            warn!(name: "Failed to send var data due to no receivers",
                                ?data, ?var, ?err);
                        }
                    }
                } else {
                    debug!(?var, "manage_var out of input data");
                    return;
                }
            }
        }
    }
}

/* Task for moving data from a channel to an broadcast channel
 * with a clock used to control the rate of data distribution
 *
 * This is used alongside the monitor task to implement subcontexts
 * by buffering data from the input stream and distributing it to subscribers
 * when the clock advances (i.e. when subcontext.advance() is called)
 *
 * This has parameters:
 * - input_stream: the stream of inputs which we are distributing
 * - send: the broadcast channel to which we are sending data
 * - clock: a watch channel which is used to signal when the clock advances
 * - cancellation_token: a cancellation token which is used to signal when to
 *   terminate the task
 */
#[instrument(name="distribute", level=Level::INFO, skip(input_stream, send, clock, cancellation_token))]
async fn distribute<V: StreamData>(
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
                    warn!("Distribute clock channel closed");
                    return;
                }
                let clock_new = *clock.borrow_and_update();
                debug!(name: "Monitoring between clocks", clock_old, clock_new);
                for clock in clock_old+1..=clock_new {
                    debug!(name: "Monitoring", ?clock);
                    select! {
                        biased;
                        _ = cancellation_token.cancelled() => {
                            return;
                        }
                        data = input_stream.next() => {
                            if let Some(data) = data {
                                debug!(name: "Distributing data", ?data);
                                if let Err(_) = send.send(data) {
                                    warn!("Failed to distribute data due to no receivers");
                                }
                                debug!("Distributed data");
                            } else {
                                return;
                            }
                        }
                    }
                    // tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
                clock_old = clock_new;
            }
        }
    }
}

/* Task for moving data from an input stream to an output channel
 *
 * This is used in the implementation of subcontexts to buffer data before it
 * is sent to distribute
 *
 * This has parameters:
 * - input_stream: the stream of inputs which we are monitoring
 * - send: the channel to which we are sending data (corresponding to the
 *   other half of the channel held by distribute)
 * - cancellation_token: a cancellation token which is used to signal when to
 *   terminate the task
 */
async fn monitor<V: StreamData>(
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
                        debug!(name: "Monitored data", ?data);
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
struct VarData<Val> {
    requester: mpsc::Sender<oneshot::Sender<OutputStream<Val>>>,
}

/*
 * A StreamContext that manages subscriptions to each variable and provides
 * an asynchronous stream when requested by the monitoring semantics.
 *
 * This includes:
 * - var_data: a map from variable names to the data associated with each
 *   variable, including the requester channel used to request the stream
 * - cancellation_token: a cancellation token used to signal when any async
 *   actors we have launched should terminate
 * - drop_guard: a reference-counted drop guard associated with the
 *   cancellation token which signals to cancel all background tasks when there
 *   it is dropped (i.e. when the reference count goes to zero)
 * - vars_requested: a watch channel used to track the number of outstanding
 *   requests for subscriptions to each variable (this is used to determine
 *   when it is safe for managed_var to start distributing data)
 *
 * Most of the logic is in the manage_var actor which is launched for each
 * variable to manage subscriptions and distribute data.
 */
struct AsyncVarExchange<Val: StreamData> {
    var_data: BTreeMap<VarName, VarData<Val>>,
    cancellation_token: CancellationToken,
    #[allow(dead_code)]
    // This is used for RAII to cancel background tasks when the async var
    // exchange is dropped
    drop_guard: Arc<DropGuard>,
    vars_requested: (watch::Sender<usize>, watch::Receiver<usize>),
}

impl<Val: StreamData> AsyncVarExchange<Val> {
    fn new(
        var_data: BTreeMap<VarName, VarData<Val>>,
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

impl<Val: StreamData> StreamContext<Val> for Arc<AsyncVarExchange<Val>> {
    fn var(&self, var: &VarName) -> Option<OutputStream<Val>> {
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
                warn!(name: "Failed to request stream for var due to no receivers", ?var, err=?e);
            }
            var_sender.send_modify(|x| *x -= 1);
        });

        // Create a lazy typed stream from the request
        let stream = oneshot_to_stream(rx);
        // let stream = drop_guard_stream(stream, self.drop_guard.clone());
        Some(stream)
    }

    fn advance(&self) {
        // Do nothing
    }

    fn subcontext(&self, history_length: usize) -> Box<dyn StreamContext<Val>> {
        Box::new(SubMonitor::new(self.clone(), history_length))
    }
}

/* A subcontext which consumes data for a subset of the variables and makes
 * it available when evaluating a deferred expression
 *
 * This is implemented via multiple background monitor and distribute actor
 * pairs which buffer data from the input stream and distribute it to
 * each subscriber to the subcontext
 */
struct SubMonitor<Val: StreamData> {
    parent: Arc<AsyncVarExchange<Val>>,
    senders: BTreeMap<VarName, broadcast::Sender<Val>>,
    buffer_size: usize,
    progress_sender: watch::Sender<usize>,
}

impl<Val: StreamData> SubMonitor<Val> {
    fn new(parent: Arc<AsyncVarExchange<Val>>, buffer_size: usize) -> Self {
        let mut senders = BTreeMap::new();

        for (var, _var_data) in parent.var_data.iter() {
            senders.insert(var.clone(), broadcast::Sender::new(100));
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
            let child_sender = self.senders.get(var).unwrap().clone();
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

impl<Val: StreamData> StreamContext<Val> for SubMonitor<Val> {
    fn var(&self, var: &VarName) -> Option<OutputStream<Val>> {
        let sender = self.senders.get(var).unwrap();

        let recv: broadcast::Receiver<Val> = sender.subscribe();
        let stream: OutputStream<Val> = Box::pin(BroadcastStream::new(recv).map(|x| x.unwrap()));

        Some(stream)
    }

    fn subcontext(&self, history_length: usize) -> Box<dyn StreamContext<Val>> {
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

/*
 * A Monitor instance implementing the Async Runtime.
 *
 * This runtime uses async actors to keep track of dependencies between
 * channels and to distribute data between them, pass data around via async
 * streams, and automatically perform garbage collection of the data contained
 * in the streams.
 *
 * - The Expr type parameter is the type of the expressions in the model.
 * - The Val type parameter is the type of the values used in the channels.
 * - The S type parameter is the monitoring semantics used to evaluate the
 *   expressions as streams.
 * - The M type parameter is the model/specification being monitored.
 */
pub struct AsyncMonitorRunner<Expr, Val, S, M>
where
    Val: StreamData,
    S: MonitoringSemantics<Expr, Val>,
    M: Specification<Expr>,
    Expr: Sync + Send,
{
    model: M,
    output_handler: Box<dyn OutputHandler<Val>>,
    output_streams: BTreeMap<VarName, OutputStream<Val>>,
    #[allow(dead_code)]
    // This is used for RAII to cancel background tasks when the async var
    // exchange is dropped
    cancellation_guard: Arc<DropGuard>,
    expr_t: PhantomData<Expr>,
    semantics_t: PhantomData<S>,
}

#[async_trait]
impl<Expr: Sync + Send, Val, S, M> Monitor<M, Val> for AsyncMonitorRunner<Expr, Val, S, M>
where
    Val: StreamData,
    S: MonitoringSemantics<Expr, Val>,
    M: Specification<Expr>,
{
    fn new(
        model: M,
        input_streams: &mut dyn InputProvider<Val>,
        output: Box<dyn OutputHandler<Val>>,
    ) -> Self {
        let cancellation_token = CancellationToken::new();
        let cancellation_guard = Arc::new(cancellation_token.clone().drop_guard());

        let mut var_data = BTreeMap::new();
        let mut to_launch_in = vec![];
        let (var_exchange_ready, watch_rx) = watch::channel(false);

        // Launch monitors for each input variable
        for var in model.input_vars().iter() {
            // let typ = model.type_of_var(var).unwrap();
            let (tx1, rx1) = mpsc::channel(100000);
            let input_stream = input_streams.input_stream(var).unwrap();
            var_data.insert(var.clone(), VarData { requester: tx1 });
            to_launch_in.push((var.clone(), input_stream, rx1));
        }

        let mut to_launch_out = vec![];

        // Create tasks for each output variable
        for var in model.output_vars().iter() {
            let (tx1, rx1) = mpsc::channel(100000);
            to_launch_out.push((var.clone(), rx1));
            var_data.insert(var.clone(), VarData { requester: tx1 });
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
            let expr = model
                .var_expr(&var)
                .expect(format!("Failed to find expression for var {}", var.0.as_str()).as_str());
            let stream = Box::pin(S::to_async_stream(expr, &var_exchange));
            let stream = drop_guard_stream(stream, cancellation_guard.clone());
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
            output_streams,
            semantics_t: PhantomData,
            cancellation_guard,
            expr_t: PhantomData,
            output_handler: output,
        }
    }

    fn spec(&self) -> &M {
        &self.model
    }

    #[instrument(name="Running async Monitor", level=Level::INFO, skip(self))]
    async fn run(mut self) {
        self.output_handler.provide_streams(self.output_streams);
        self.output_handler.run().await;
    }
}
