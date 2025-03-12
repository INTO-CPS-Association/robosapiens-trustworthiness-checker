use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use futures::future::join_all;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tokio_util::sync::DropGuard;
use tracing::Level;
use tracing::debug;
use tracing::info;
use tracing::instrument;
use tracing::warn;

use crate::core::InputProvider;
use crate::core::Monitor;
use crate::core::MonitoringSemantics;
use crate::core::OutputHandler;
use crate::core::Specification;
use crate::core::SyncStreamContext;
use crate::core::{OutputStream, StreamContext, StreamData, VarName};
use crate::dependencies::interface::DependencyManager;
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
#[instrument(name="manage_var", level=Level::DEBUG, skip(input_stream, channel_request_rx, parent_clock, child_clock, cancel))]
async fn manage_var<V: StreamData>(
    var: VarName,
    mut input_stream: OutputStream<V>,
    mut channel_request_rx: mpsc::Receiver<(oneshot::Sender<()>, oneshot::Sender<OutputStream<V>>)>,
    mut parent_clock: watch::Receiver<usize>,
    child_clock: watch::Sender<usize>,
    // mut ready: watch::Receiver<bool>,
    cancel: CancellationToken,
) {
    let mut senders: Vec<mpsc::Sender<V>> = vec![];
    let mut receiving_requests = true;
    let mut clock_old = 0;

    // TODO: This could be optimized to readd an additional stage where
    // we gather up requests before sending out any streams. This would
    // mean that manage_var would go away if there is only one subscriber
    // since we would just send the input stream directly to the
    // subscriber. This adds complexity, but in previous versions of the
    // runtime could give a 2x speedup in some cases.

    loop {
        select! {
            biased;
            _ = cancel.cancelled() => {
                info!(?var, "Ending manage_var due to cancellation");
                return;
            }

            clock_upd = parent_clock.changed() => {
                if clock_upd.is_err() {
                    warn!("Distribute clock channel closed");
                    return;
                }
                let clock_new = *parent_clock.borrow_and_update();
                if clock_new == usize::MAX {
                    debug!("Closing to new subscribers since the clock is auto advancing");
                    receiving_requests = false;
                }
                debug!(clock_old, clock_new, "Monitoring between clocks");
                for clock in clock_old+1..=clock_new {
                    debug!(?clock, "Distributing single");
                    select! {
                        biased;
                        _ = cancel.cancelled() => {
                            debug!(?clock, "Ending distribute due to \
                            cancellation");
                            return;
                        }
                        data = input_stream.next() => {
                            if let Some(data) = data {
                                // Update the child clock to report our progress
                                let _ = child_clock.send(clock);
                                debug!(?data, "Distributing data");
                                let mut to_delete = vec![];
                                for (i, child_sender) in senders.iter().enumerate() {
                                    if let Err(_) = child_sender.send(data.clone()).await {
                                        debug!("Failed to distribute data due to no receivers");
                                        if clock_new == usize::MAX {
                                            debug!(
                                                "Stopping distributing since we currently have no subscribers \
                                                and the clock is auto advancing so no more subscriber can \
                                                join in the future"
                                            );
                                            to_delete.push(i);
                                            return;
                                        }
                                    }
                                }
                                for i in to_delete {
                                    senders.remove(i);
                                }
                                debug!("Distributed data");
                            } else {
                                debug!("Stopped distributing data due to end \
                                of input stream");
                                return;
                            }
                        }
                    }
                }
                debug!(clock_old, clock_new, "Finished monitoring between clocks");
                clock_old = clock_new;
            }

            channel_sender = channel_request_rx.recv() => {
                if !receiving_requests {
                    panic!("Received request after all subscribers have been gathered");
                }
                if let Some((done_sender, channel_sender)) = channel_sender {
                    debug!(?var, "Received request for var");
                    let (tx, rx) = mpsc::channel(10);
                    senders.push(tx);
                    let stream = ReceiverStream::new(rx);
                    if let Err(_) = channel_sender.send(Box::pin(stream)) {
                        // panic!("Failed to send stream for {var} to requester");
                        warn!(?var, "Failed to send stream for var to requester");
                    };
                    if let Err(_) = done_sender.send(()) {
                        warn!(?var, "Failed to send done signal for var to requester");
                    }
                    // send_requests.push(channel_sender);
                }
                // We don't care if we stop receiving requests
                debug!(?var, "Channel sender went away for var");
            }

        }
    }
}

/// Create a wrapper around an input stream which stores a history buffer of
/// data of length history_length for retrospective monitoring
fn store_history<V: StreamData>(
    var: VarName,
    history_length: usize,
    mut input_stream: OutputStream<V>,
) -> OutputStream<V> {
    if history_length == 0 {
        return input_stream;
    }

    let (send, recv) = mpsc::channel(history_length);

    tokio::spawn(async move {
        while let Some(data) = input_stream.next().await {
            debug!(
                ?var,
                ?data,
                ?history_length,
                "monitored history data for history"
            );
            if let Err(_) = send.send(data).await {
                debug!(
                    ?var,
                    ?history_length,
                    "Failed to send data due to no receivers; shutting down"
                );
                return;
            }
        }
        debug!("store_history out of input data");
    });

    Box::pin(ReceiverStream::new(recv))
}

/// A context which consumes data for a set of variables and makes
/// it available when evaluating a deferred expression
//
/// This is implemented in the background using a combination of
/// manage_var and store history actors
struct Context<Val: StreamData> {
    /// The variables which are available in the context
    vars: Vec<VarName>,
    /// Keeps track of the number of outstanding requests
    /// to provide streams for variables
    outstanding_var_requests: (watch::Sender<usize>, watch::Receiver<usize>),
    /// The channels to request streams for each variable
    /// (used by var)
    senders:
        BTreeMap<VarName, mpsc::Sender<(oneshot::Sender<()>, oneshot::Sender<OutputStream<Val>>)>>,
    /// The amount of history stored for retrospective monitoring
    /// of each variable (0 means no history)
    history_length: usize,
    /// Child clocks which are used to monitor the progress of
    /// consumption of each variable in the context
    child_clocks: BTreeMap<VarName, watch::Receiver<usize>>,
    /// The parent clock which is used to progress all streams
    /// in the context
    clock: watch::Sender<usize>,
    /// The cancellation token used to cancel all background tasks
    cancellation_token: CancellationToken,
}

impl<Val: StreamData> Context<Val> {
    fn new(
        input_streams: BTreeMap<VarName, OutputStream<Val>>,
        buffer_size: usize,
        cancellation_token: CancellationToken,
    ) -> Self {
        let mut senders = BTreeMap::new();
        let mut child_clock_recvs = BTreeMap::new();
        let mut child_clock_senders = BTreeMap::new();
        let mut receivers = BTreeMap::new();
        let mut vars = Vec::new();
        let outstanding_var_requests = watch::channel(0);

        for var in input_streams.keys() {
            let (watch_tx, watch_rx) = watch::channel(0);
            let (req_tx, req_rx) = mpsc::channel(10);
            vars.push(var.clone());
            receivers.insert(var.clone(), req_rx);
            senders.insert(var.clone(), req_tx);
            child_clock_recvs.insert(var.clone(), watch_rx);
            child_clock_senders.insert(var.clone(), watch_tx);
        }

        let clock = watch::channel(0).0;

        Context {
            vars,
            senders,
            history_length: buffer_size,
            outstanding_var_requests,
            child_clocks: child_clock_recvs,
            clock,
            cancellation_token,
        }
        .start_monitors(input_streams, receivers, child_clock_senders)
    }

    fn start_monitors(
        self,
        mut input_streams: BTreeMap<VarName, OutputStream<Val>>,
        mut receivers: BTreeMap<
            VarName,
            mpsc::Receiver<(oneshot::Sender<()>, oneshot::Sender<OutputStream<Val>>)>,
        >,
        mut child_progress_senders: BTreeMap<VarName, watch::Sender<usize>>,
    ) -> Self {
        for var in self.vars.iter() {
            let input_stream = input_streams.remove(var).unwrap();
            // let child_sender = self.senders.remove(var).unwrap();
            let receiver = receivers.remove(var).unwrap();
            let clock = self.clock.subscribe();
            tokio::spawn(manage_var(
                var.clone(),
                store_history(var.clone(), self.history_length, input_stream),
                receiver,
                clock,
                child_progress_senders.remove(var).unwrap(),
                self.cancellation_token.clone(),
            ));
        }

        self
    }

    /// Drop our internal references to senders, letting them close once all
    /// current subscribers have received all data
    fn finalize(&mut self) {
        self.senders = BTreeMap::new()
    }
}

impl<Val: StreamData> StreamContext<Val> for Context<Val> {
    fn var(&self, var: &VarName) -> Option<OutputStream<Val>> {
        let requester = self
            .senders
            .get(var)
            .expect(&format!("Requester message for var {} should exist", var))
            .clone();

        if self.is_clock_started() {
            panic!("Cannot request a stream after the clock has started");
        }

        // Request the stream
        let (tx, rx) = oneshot::channel();
        let var = var.clone();

        self.outstanding_var_requests.0.send_modify(|x| *x += 1);

        let out_standing = self.outstanding_var_requests.0.clone();

        tokio::spawn(async move {
            let (done_tx, done_rx) = oneshot::channel();

            if let Err(e) = requester.send((done_tx, tx)).await {
                warn!(name: "Failed to request stream for var due to no receivers", ?var, err=?e);
            }

            done_rx.await.unwrap();
            out_standing.send_modify(|x| *x -= 1);
        });

        // Create a lazy typed stream from the request
        let stream = oneshot_to_stream(rx);

        Some(stream)
    }

    fn subcontext(&self, history_length: usize) -> Box<dyn SyncStreamContext<Val>> {
        let input_streams = self
            .vars
            .iter()
            .map(|var| (var.clone(), self.var(var).unwrap()))
            .collect();

        // Recursively create a new context based on ourself
        Box::new(Context::new(
            input_streams,
            history_length,
            self.cancellation_token.clone(),
        ))
    }
}

#[async_trait]
impl<Val: StreamData> SyncStreamContext<Val> for Context<Val> {
    async fn advance_clock(&mut self) {
        self.outstanding_var_requests
            .1
            .wait_for(|x| *x == 0)
            .await
            .unwrap();
        self.clock.send_modify(|x| *x += 1);
    }

    fn clock(&self) -> usize {
        self.clock.borrow().clone()
    }

    async fn start_auto_clock(&mut self) {
        if !self.is_clock_started() {
            self.finalize();
            self.outstanding_var_requests
                .1
                .wait_for(|x| *x == 0)
                .await
                .unwrap();
            // Set the clock to the maximum value to allow all streams to
            // progress freely
            self.clock.send_modify(|x| *x = usize::MAX);
        }
    }

    fn is_clock_started(&self) -> bool {
        self.clock() == usize::MAX
    }

    async fn wait_till(&self, time: usize) {
        let futs = self.child_clocks.values().map(|x| {
            let mut x = x.clone();
            async move {
                x.wait_for(|y| *y >= time).await.unwrap();
            }
        });
        join_all(futs).await;
    }

    fn upcast(&self) -> &dyn StreamContext<Val> {
        self
    }
}

/// A Monitor instance implementing the Async Runtime.
///
/// This runtime uses async actors to keep track of dependencies between
/// channels and to distribute data between them, pass data around via async
/// streams, and automatically perform garbage collection of the data contained
/// in the streams.
///
///  - The Expr type parameter is the type of the expressions in the model.
///  - The Val type parameter is the type of the values used in the channels.
///  - The S type parameter is the monitoring semantics used to evaluate the
///    expressions as streams.
///  - The M type parameter is the model/specification being monitored.
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
        _dependencies: DependencyManager,
    ) -> Self {
        let cancellation_token = CancellationToken::new();
        let cancellation_guard = Arc::new(cancellation_token.clone().drop_guard());

        let input_vars = model.input_vars().clone();
        let output_vars = model.output_vars().clone();

        let input_streams = input_vars.iter().map(|var| {
            let stream = input_streams.input_stream(var).unwrap();
            (var.clone(), stream)
        });

        // Create deferred streams based on each of the output variables
        let output_oneshots: Vec<_> = output_vars
            .iter()
            .cloned()
            .map(|_| oneshot::channel::<OutputStream<Val>>())
            .collect();
        let (output_txs, output_rxs): (Vec<_>, Vec<_>) = output_oneshots.into_iter().unzip();
        let output_txs: BTreeMap<_, _> = output_vars
            .iter()
            .cloned()
            .zip(output_txs.into_iter())
            .collect();
        let output_streams = output_rxs.into_iter().map(oneshot_to_stream);
        let output_streams = output_vars.iter().cloned().zip(output_streams.into_iter());

        // Combine the input and output streams into a single map
        let streams = input_streams.chain(output_streams.into_iter()).collect();

        let mut context = Context::new(streams, 0, cancellation_token.clone());

        // Create a map of the output variables to their streams
        // based on using the context
        let output_streams = model
            .output_vars()
            .iter()
            .map(|var| {
                (
                    var.clone(),
                    // Add a guard to the stream to cancel background
                    // tasks whenever all the outputs are dropped
                    drop_guard_stream(
                        context.var(var).expect(
                            format!("Failed to find expression for var {}", var.0.as_str())
                                .as_str(),
                        ),
                        cancellation_guard.clone(),
                    ),
                )
            })
            .collect();

        // Send outputs computed based on the context to the
        // output handler
        for (var, tx) in output_txs {
            let expr = model
                .var_expr(&var)
                .expect(format!("Failed to find expression for var {}", var.0.as_str()).as_str());
            let stream = S::to_async_stream(expr, &context);
            if let Err(_) = tx.send(stream) {
                warn!(?var, "Failed to send stream for var to requester");
            }
        }

        tokio::spawn(async move {
            context.start_auto_clock().await;
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
