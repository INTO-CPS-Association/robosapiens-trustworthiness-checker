use async_trait::async_trait;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use tokio::sync::Mutex;

use futures::StreamExt;
use futures::stream;

use crate::core::InputProvider;
use crate::core::Monitor;
use crate::core::MonitoringSemantics;
use crate::core::OutputHandler;
use crate::core::Specification;
use crate::core::StreamData;
use crate::core::TimedStreamContext;
use crate::core::{OutputStream, StreamContext, VarName};
use crate::dependencies::traits::DependencyManager;

/*
 * A StreamContext that track the history of each of the variables as a queue
 * (Vec) of values, and provides this as an asynchronous stream when requested
 * by the monitoring semantics.
 *
 * This includes:
 *  - queues: a map from variable names to the underlying queues
 *  - input_streams: a map from variable names to the initial input streams
 *  - output_streams: a map from variable names to the streams that are lazily
 *    provided to the context
 *  - production_locks: a map from variable names to locks that are used to
 *    ensure that only one variable is updated at a time
 *
 * An QueuingVarContext is created with a list of variable names, the map of
 * input streams, and a map of (lazily provided) output streams.
 *
 * The Val type parameter is the type of the values contained in each
 * variable/stream.
 *
 * Note that currently this implementation does no garbage collection of
 * historical values, and so the queues will grow indefinitely.
 */
struct QueuingVarContext<Val: StreamData> {
    queues: BTreeMap<VarName, Arc<Mutex<Vec<Val>>>>,
    input_streams: BTreeMap<VarName, Arc<Mutex<OutputStream<Val>>>>,
    output_streams: BTreeMap<VarName, WaitingStream<OutputStream<Val>>>,
    production_locks: BTreeMap<VarName, Arc<Mutex<()>>>,
}

impl<Val: StreamData> QueuingVarContext<Val> {
    fn new(
        vars: Vec<VarName>,
        input_streams: BTreeMap<VarName, Arc<Mutex<OutputStream<Val>>>>,
        output_streams: BTreeMap<VarName, WaitingStream<OutputStream<Val>>>,
    ) -> Self {
        let mut queues = BTreeMap::new();
        let mut production_locks = BTreeMap::new();

        for var in vars {
            queues.insert(var.clone(), Arc::new(Mutex::new(Vec::new())));
            production_locks.insert(var.clone(), Arc::new(Mutex::new(())));
        }

        QueuingVarContext {
            queues,
            input_streams,
            output_streams,
            production_locks,
        }
    }
}

// A WaitingStream allows us to refer to streams which do not exist yet.
// Arrived means that the stream exists. Waiting means that we are waiting for the stream to arrive.
//
// The get_stream function is used to wait until the stream is available and
// then return it.
enum WaitingStream<S> {
    Arrived(Arc<Mutex<S>>),
    Waiting(tokio::sync::watch::Receiver<Option<Arc<Mutex<S>>>>),
}

impl<S> Clone for WaitingStream<S> {
    fn clone(&self) -> Self {
        match self {
            WaitingStream::Arrived(stream) => WaitingStream::Arrived(stream.clone()),
            WaitingStream::Waiting(receiver) => WaitingStream::Waiting(receiver.clone()),
        }
    }
}

impl<S> WaitingStream<S> {
    async fn get_stream(&mut self) -> Arc<Mutex<S>> {
        let ret_stream = match self {
            WaitingStream::Arrived(stream) => return stream.clone(),
            WaitingStream::Waiting(receiver) => {
                let stream = receiver.wait_for(|x| x.is_some()).await.unwrap();
                stream.as_ref().unwrap().clone()
            }
        };
        *self = WaitingStream::Arrived(ret_stream.clone());
        ret_stream
    }
}

/*
 * This function creates a buffered asynchronous stream based on an underlying
 * shared queue of known values xs and a (lazily provided) stream
 * waiting_stream.
 *
 * The returned stream will produce values from xs if they are available, or if
 * not, will produce values from waiting stream
 */
fn queue_buffered_stream<V: StreamData>(
    xs: Arc<Mutex<Vec<V>>>,
    waiting_stream: WaitingStream<OutputStream<V>>,
    lock: Arc<Mutex<()>>,
) -> OutputStream<V> {
    Box::pin(stream::unfold(
        (0, xs, waiting_stream, lock),
        // `i` is the latest index we have outputted.
        // `xs` is a vector of things we have already taken from `ws`.
        // `ws` is the waiting stream we are outputting from.
        // `lock` is required when two instances try to compute the same position of output channels
        |(i, xs, mut ws, lock)| async move {
            loop {
                // We have these three cases to ensure deadlock freedom

                // The scenario where xs contains one fewer element than the index we are currently at
                // so we must take the next value from the waiting stream
                if i == xs.lock().await.len() {
                    // Compute the next value, potentially using the previous one
                    let _ = lock.lock().await;
                    if i != xs.lock().await.len() {
                        continue;
                    }

                    let stream = ws.get_stream().await;
                    // We are guaranteed that this will not need to lock
                    // the production lock and hence should not deadlock
                    let mut stream_lock = stream.lock().await;
                    let x_next = stream_lock.next().await;
                    xs.lock().await.push(x_next?);
                }
                // If the index we are looking for is already inside `xs` then return it
                else if i < xs.lock().await.len() {
                    // We already have the value buffered, so return it
                    return Some((xs.lock().await[i].clone(), (i + 1, xs.clone(), ws, lock)));
                }
                // i > xs.len(). We can't compute the current value before previous values are computed
                // so we await the stream and ignore the result, causing us to repeat the loop
                else {
                    // Cause more previous values to be produced
                    let stream = ws.get_stream().await;
                    let mut stream_lock = stream.lock().await;
                    let _ = stream_lock.next().await;
                }
            }
        },
    ))
}

/*
 * Implement StreamContext for QueuingVarContext
 */
impl<Val: StreamData> StreamContext<Val> for Arc<QueuingVarContext<Val>> {
    fn var(&self, var: &VarName) -> Option<OutputStream<Val>> {
        let queue = self.queues.get(var)?;
        let production_lock = self.production_locks.get(var)?.clone();
        // Check if it is an input stream. In that case it has exists (has arrived).
        // Otherwise, it must be an output stream that either already exists (has arrived) or is waiting.
        let stream = self
            .input_streams
            .get(var)
            .cloned()
            .map(WaitingStream::Arrived)
            .or_else(|| self.output_streams.get(var).cloned())?;
        Some(queue_buffered_stream(
            queue.clone(),
            stream,
            production_lock,
        ))
    }

    fn subcontext(&self, history_length: usize) -> Box<dyn TimedStreamContext<Val>> {
        Box::new(SubMonitor::new(self.clone(), history_length))
    }
}

/*
 * A subcontext that provides a view of the parent context with a limited
 * history length.
 */
struct SubMonitor<Val: StreamData> {
    parent: Arc<QueuingVarContext<Val>>,
    #[allow(dead_code)]
    // Note that buffer_size is not used in this runtime -- it is unrestricted
    buffer_size: usize,
    index: Arc<StdMutex<usize>>,
}

impl<Val: StreamData> SubMonitor<Val> {
    fn new(parent: Arc<QueuingVarContext<Val>>, buffer_size: usize) -> Self {
        SubMonitor {
            parent,
            buffer_size,
            index: Arc::new(StdMutex::new(0)),
        }
    }
}

impl<Val: StreamData> StreamContext<Val> for SubMonitor<Val> {
    fn var(&self, var: &VarName) -> Option<OutputStream<Val>> {
        let parent_stream = self.parent.var(var)?;
        let index = *self.index.lock().unwrap();
        let substream = parent_stream.skip(index);

        Some(Box::pin(substream))
    }

    fn subcontext(&self, history_length: usize) -> Box<dyn TimedStreamContext<Val>> {
        // TODO: consider if this is the right approach; creating a subcontext
        // is only used if eval is called within an eval, and it will require
        // careful thought to decide how much history should be passed down
        // (the current implementation passes down none)
        self.parent.subcontext(history_length)
    }
}

#[async_trait]
impl<Val: StreamData> TimedStreamContext<Val> for SubMonitor<Val> {
    async fn clock(&self) -> usize {
        *self.index.lock().unwrap()
    }

    fn advance_clock(&self) {
        *self.index.lock().unwrap() += 1;
    }

    fn start_clock(&mut self) {
        // Do nothing
    }

    async fn wait_till(&self, time: usize) {
        while { self.index.lock().unwrap().clone() } < time {}
    }

    fn upcast(&self) -> &dyn StreamContext<Val> {
        self
    }
}

/*
 * A Monitor instance implementing the Queuing Runtime.
 *
 * This runtime uses a queue-based approach to track the history of each
 * variable (based on the QueuingVarContext var_exchange) and provide the
 * history as asynchronous streams to the monitoring semantics.
 *
 * - The Expr type parameter is the type of the expressions in the model.
 * - The Val type parameter is the type of the values used in the channels.
 * - The S type parameter is the monitoring semantics used to evaluate the
 *   expressions as streams.
 * - The M type parameter is the model/specification being monitored.
 */
pub struct QueuingMonitorRunner<Expr, Val, S, M>
where
    Val: StreamData,
    S: MonitoringSemantics<Expr, Val>,
    M: Specification<Expr>,
{
    model: M,
    var_exchange: Arc<QueuingVarContext<Val>>,
    semantics_t: PhantomData<S>,
    expr_t: PhantomData<Expr>,
    output_handler: Box<dyn OutputHandler<Val>>,
}

#[async_trait]
impl<Val: StreamData, Expr: Send, S: MonitoringSemantics<Expr, Val>, M: Specification<Expr>>
    Monitor<M, Val> for QueuingMonitorRunner<Expr, Val, S, M>
{
    fn new(
        model: M,
        input_streams: &mut dyn InputProvider<Val>,
        output: Box<dyn OutputHandler<Val>>,
        _dependencies: DependencyManager,
    ) -> Self {
        let var_names: Vec<VarName> = model
            .input_vars()
            .into_iter()
            .chain(model.output_vars().into_iter())
            .collect();

        let input_streams = model
            .input_vars()
            .iter()
            .map(|var| {
                let stream = input_streams.input_stream(var);
                (var.clone(), Arc::new(Mutex::new(stream.unwrap())))
            })
            .collect::<BTreeMap<_, _>>();

        let mut output_stream_senders = BTreeMap::new();
        let mut output_stream_waiting = BTreeMap::new();
        for var in model.output_vars() {
            let (tx, rx) = tokio::sync::watch::channel(None);
            output_stream_senders.insert(var.clone(), tx);
            output_stream_waiting.insert(var.clone(), WaitingStream::Waiting(rx));
        }

        let var_exchange = Arc::new(QueuingVarContext::<Val>::new(
            var_names,
            input_streams.clone(),
            output_stream_waiting,
        ));

        for var in model.output_vars() {
            let stream = S::to_async_stream(model.var_expr(&var).unwrap(), &var_exchange);
            // let stream: OutputStream<<SS::TypeSystem as TypeSystem>::TypedValue> = Box::pin(stream);
            output_stream_senders
                .get(&var)
                .unwrap()
                .send(Some(Arc::new(Mutex::new(stream))))
                .unwrap();
        }

        Self {
            model,
            var_exchange,
            semantics_t: PhantomData,
            expr_t: PhantomData,
            output_handler: output,
        }
    }

    fn spec(&self) -> &M {
        &self.model
    }

    async fn run(mut self) {
        let output_streams = self
            .model
            .output_vars()
            .into_iter()
            .map(|var| (var.clone(), self.output_stream(var)))
            .collect();
        self.output_handler.provide_streams(output_streams);
        self.output_handler.run().await;
    }
}

impl<Val: StreamData, Expr, S: MonitoringSemantics<Expr, Val>, M: Specification<Expr>>
    QueuingMonitorRunner<Expr, Val, S, M>
{
    fn output_stream(&self, var: VarName) -> OutputStream<Val> {
        self.var_exchange.var(&var).unwrap()
    }
}
