use std::collections::BTreeMap;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
// use std::sync::Mutex;
use futures::lock;
use std::sync::Mutex as StdMutex;
use std::sync::RwLock as StdRwLock;
use std::vec;
use tokio::sync::watch;
use tokio::sync::Mutex;

use futures::future::join_all;
use futures::stream;
use futures::stream::BoxStream;
use futures::StreamExt;
use tokio::sync::RwLock;

use crate::core::InputProvider;
use crate::core::Monitor;
use crate::core::MonitoringSemantics;
use crate::core::Specification;
use crate::core::StreamData;
use crate::core::StreamExpr;
use crate::core::{OutputStream, StreamContext, VarName};
use crate::ConcreteStreamData;

struct QueuingVarContext<T: StreamData> {
    queues: BTreeMap<VarName, Arc<Mutex<Vec<T>>>>,
    input_streams: BTreeMap<VarName, Arc<Mutex<OutputStream<T>>>>,
    output_streams: BTreeMap<VarName, WaitingStream<T>>,
    production_locks: BTreeMap<VarName, Arc<Mutex<()>>>,
}

impl<T: StreamData> QueuingVarContext<T> {
    fn new(
        vars: Vec<VarName>,
        input_streams: BTreeMap<VarName, Arc<Mutex<OutputStream<T>>>>,
        output_streams: BTreeMap<VarName, WaitingStream<T>>,
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

// A stream that is either already arrived or is waiting to be lazily supplied
#[derive(Clone)]
enum WaitingStream<T: StreamData> {
    Arrived(Arc<Mutex<OutputStream<T>>>),
    Waiting(tokio::sync::watch::Receiver<Option<Arc<Mutex<OutputStream<T>>>>>),
}

impl<T: StreamData> WaitingStream<T> {
    async fn get_stream(&mut self) -> Arc<Mutex<OutputStream<T>>> {
        let mut ret_stream = None;
        match self {
            WaitingStream::Arrived(stream) => return stream.clone(),
            WaitingStream::Waiting(receiver) => {
                let stream_lock = receiver.wait_for(|x| x.is_some()).await.unwrap();
                let stream = stream_lock.as_ref().unwrap().clone();
                ret_stream = Some(stream)
            }
        }
        *self = WaitingStream::Arrived(ret_stream.unwrap().clone());
        if let WaitingStream::Arrived(stream) = self {
            return stream.clone();
        } else {
            panic!("Stream should be arrived")
        }
    }
}

fn queue_buffered_stream<T: StreamData>(
    xs: Arc<Mutex<Vec<T>>>,
    waiting_stream: WaitingStream<T>,
    lock: Arc<Mutex<()>>,
) -> BoxStream<'static, T> {
    Box::pin(stream::unfold(
        (0, xs, waiting_stream, lock),
        |(i, xs, mut ws, lock)| async move {
            loop {
                // We have these three cases to ensure deadlock freedom
                // println!("producing value for i: {}", i);
                // println!("locking xs");
                // let mut xs_lock = xs.lock().await;
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
                } else if i < xs.lock().await.len() {
                    // We already have the value buffered, so return it
                    return Some((xs.lock().await[i].clone(), (i + 1, xs.clone(), ws, lock)));
                } else {
                    // Cause more previous values to be produced
                    let stream = ws.get_stream().await;
                    let mut stream_lock = stream.lock().await;
                    let _ = stream_lock.next().await;
                }
            }
        },
    ))
}

impl<T: StreamData> StreamContext<T> for Arc<QueuingVarContext<T>> {
    fn var(&self, var: &VarName) -> Option<OutputStream<T>> {
        let queue = self.queues.get(var)?;
        let production_lock = self.production_locks.get(var)?.clone();
        if let Some(stream) = self.input_streams.get(var).cloned() {
            return Some(queue_buffered_stream(
                queue.clone(),
                WaitingStream::Arrived(stream),
                production_lock,
            ));
        } else {
            let waiting_stream = self.output_streams.get(var)?.clone();
            return Some(queue_buffered_stream(
                queue.clone(),
                waiting_stream,
                production_lock,
            ));
        }
    }

    fn advance(&self) {
        // Do nothing
    }

    fn subcontext(&self, history_length: usize) -> Box<dyn StreamContext<T>> {
        Box::new(SubMonitor::new(self.clone(), history_length))
    }
}

struct SubMonitor<T: StreamData> {
    parent: Arc<QueuingVarContext<T>>,
    buffer_size: usize,
    index: Arc<StdMutex<usize>>,
}

impl<T: StreamData> SubMonitor<T> {
    fn new(parent: Arc<QueuingVarContext<T>>, buffer_size: usize) -> Self {
        SubMonitor {
            parent,
            buffer_size,
            index: Arc::new(StdMutex::new(0)),
        }
    }
}

impl<T: StreamData> StreamContext<T> for SubMonitor<T> {
    fn var(&self, var: &VarName) -> Option<OutputStream<T>> {
        let parent_stream = self.parent.var(var)?;

        Some(Box::pin(
            parent_stream.skip(self.index.lock().unwrap().clone()),
        ))
    }

    fn subcontext(&self, history_length: usize) -> Box<dyn StreamContext<T>> {
        // TODO: consider if this is the right approach; creating a subcontext
        // is only used if eval is called within an eval, and it will require
        // careful thought to decide how much history should be passed down
        // (the current implementation passes down none)
        self.parent.subcontext(history_length)
    }

    fn advance(&self) {
        *self.index.lock().unwrap() += 1;
    }
}

pub struct QueuingMonitorRunner<T, S, M, R>
where
    T: StreamExpr,
    S: MonitoringSemantics<T, R>,
    M: Specification<T>,
    R: StreamData,
{
    model: M,
    var_exchange: Arc<QueuingVarContext<R>>,
    phantom_t: PhantomData<T>,
    semantics_t: PhantomData<S>,
}

impl<T: StreamExpr, S: MonitoringSemantics<T, R>, M: Specification<T>, R: StreamData>
    Monitor<T, S, M, R> for QueuingMonitorRunner<T, S, M, R>
{
    fn new(model: M, mut input_streams: impl InputProvider<R>) -> Self {
        let var_names = model
            .input_vars()
            .into_iter()
            .chain(model.output_vars().into_iter())
            .collect();

        let input_streams = model
            .input_vars()
            .iter()
            .map(|var| {
                let stream = (&mut input_streams).input_stream(var);
                (var.clone(), Arc::new(Mutex::new(stream.unwrap())))
            })
            .collect::<BTreeMap<_, _>>();
        // let input_streams = Arc::new(Mutex::new(input_streams));

        let mut output_stream_senders = BTreeMap::new();
        let mut output_stream_waiting = BTreeMap::new();
        for var in model.output_vars() {
            let (tx, rx) = tokio::sync::watch::channel(None);
            output_stream_senders.insert(var.clone(), tx);
            output_stream_waiting.insert(var.clone(), WaitingStream::Waiting(rx));
        }

        let var_exchange = Arc::new(QueuingVarContext::new(
            var_names,
            input_streams.clone(),
            output_stream_waiting,
        ));

        for var in model.output_vars() {
            let stream = S::to_async_stream(model.var_expr(&var).unwrap(), &var_exchange);
            let stream = Arc::new(Mutex::new(stream));
            output_stream_senders
                .get(&var)
                .unwrap()
                .send(Some(stream))
                .unwrap();
        }

        Self {
            model,
            var_exchange,
            semantics_t: PhantomData,
            phantom_t: PhantomData,
        }
    }

    fn spec(&self) -> &M {
        &self.model
    }

    fn monitor_outputs(&mut self) -> BoxStream<'static, BTreeMap<VarName, R>> {
        let outputs = self.model.output_vars();
        let mut output_streams = vec![];
        for output in outputs.iter().cloned() {
            output_streams.push(self.output_stream(output));
        }

        Box::pin(stream::unfold(
            (output_streams, outputs),
            |(mut output_streams, outputs)| async move {
                let mut futures = vec![];
                for output_stream in output_streams.iter_mut() {
                    futures.push(output_stream.next());
                }

                let next_vals: Vec<Option<R>> = join_all(futures).await;
                let mut res: BTreeMap<VarName, R> = BTreeMap::new();
                for (var, val) in outputs.clone().iter().zip(next_vals) {
                    res.insert(
                        var.clone(),
                        match val {
                            Some(val) => val,
                            None => return None,
                        },
                    );
                }
                Some((res, (output_streams, outputs)))
                    as Option<(BTreeMap<VarName, R>, (Vec<OutputStream<R>>, Vec<VarName>))>
            },
        )) as BoxStream<'static, BTreeMap<VarName, R>>
    }
}

impl<T: StreamExpr, S: MonitoringSemantics<T, R>, M: Specification<T>, R: StreamData>
    QueuingMonitorRunner<T, S, M, R>
{
    fn output_stream(&self, var: VarName) -> OutputStream<R> {
        self.var_exchange.var(&var).unwrap()
    }
}
