use std::borrow::BorrowMut;
use std::collections::BTreeMap;
use std::future::Future;
use std::iter;
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::Mutex;
use std::u64::MAX;
use std::vec;

use futures::future::join_all;
use futures::stream;
use futures::stream::BoxStream;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use tokio_util::sync::DropGuard;

use crate::core::InputProvider;
use crate::core::Monitor;
use crate::core::MonitoringSemantics;
use crate::core::Specification;
use crate::core::StreamData;
use crate::core::StreamExpr;
use crate::core::{OutputStream, StreamContext, VarName};

struct QueuingVarExchange<T: StreamData> {
    queues: BTreeMap<VarName, Arc<Mutex<Vec<T>>>>,
    max_read: BTreeMap<VarName, Arc<Mutex<usize>>>,
}

impl<T: StreamData> QueuingVarExchange<T> {
    fn new(vars: Vec<VarName>) -> Self {
        let mut queues = BTreeMap::new();
        let mut max_read = BTreeMap::new();
        let max_read_lock = max_read.borrow_mut();

        for var in vars {
            queues.insert(var.clone(), Arc::new(Mutex::new(Vec::new())));
            max_read_lock.insert(var, Arc::new(Mutex::new(0)));
        }

        QueuingVarExchange { queues, max_read }
    }

    fn publish(&self, var: &VarName, data: T, max_queued: Option<usize>) -> Option<T> {
        // TODO: currently uses unbounded queues

        let queue = self.queues.get(var).unwrap();
        let mut queue = queue.lock().unwrap();
        let mut max_read = self.max_read.get(var).unwrap().lock().unwrap();
        if max_queued.is_none() || queue.len() - *max_read < max_queued.unwrap() {
            println!(
                "publishing data: {:?} with max_read: {:?} and queue length {:?}",
                data,
                *max_read,
                queue.len()
            );
            queue.push(data);
            None
        } else {
            Some(data)
        }
    }
}

fn queue_to_stream<T: StreamData>(xs: Vec<T>) -> BoxStream<'static, T> {
    Box::pin(stream::iter(xs.into_iter()))
}

fn count_read<T: StreamData>(
    stream: BoxStream<'static, T>,
    max_read: Arc<Mutex<usize>>,
) -> BoxStream<'static, T> {
    let max_reads = stream::iter(iter::repeat(max_read));
    Box::pin(stream.enumerate().zip(max_reads).map(|((i, x), max_read)| {
        println!("setting max_read = {:?}", i);
        *max_read.lock().unwrap().deref_mut() = i;
        x
    }))
}

fn queue_to_stream_incremental<T: StreamData>(xs: Arc<Mutex<Vec<T>>>) -> BoxStream<'static, T> {
    Box::pin(stream::unfold((0, xs), |(i, xs)| async move {
        loop {
            match {
                // Separate out the scope which requires the lock
                // to avoid holding it whilst waiting
                let xs = xs.lock().unwrap();
                if i < xs.len() {
                    // *max_read.lock().unwrap().deref_mut() = i;
                    Some(xs[i].clone())
                } else {
                    None
                }
            } {
                Some(next) => {
                    return Some((next, (i + 1, xs.clone())));
                }
                None => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
        }
    }))
}

impl<T: StreamData> StreamContext<T> for Arc<QueuingVarExchange<T>> {
    fn var(&self, var: &VarName) -> Option<OutputStream<T>> {
        let queue = self.queues.get(var)?;
        // let queue = queue.lock().unwrap();
        Some(queue_to_stream_incremental(queue.clone()))
    }

    fn advance(&self) {
        // Do nothing
    }

    fn subcontext(&self, history_length: usize) -> Box<dyn StreamContext<T>> {
        Box::new(SubMonitor::new(self.clone(), history_length))
    }
}

struct SubMonitor<T: StreamData> {
    parent: Arc<QueuingVarExchange<T>>,
    buffer_size: usize,
    index: Arc<Mutex<usize>>,
}

impl<T: StreamData> SubMonitor<T> {
    fn new(parent: Arc<QueuingVarExchange<T>>, buffer_size: usize) -> Self {
        SubMonitor {
            parent,
            buffer_size,
            index: Arc::new(Mutex::new(0)),
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
    input_streams: Arc<Mutex<BTreeMap<VarName, BoxStream<'static, R>>>>,
    model: M,
    var_exchange: Arc<QueuingVarExchange<R>>,
    // tasks: Option<Vec<Pin<Box<dyn Future<Output = ()> + Send>>>>,
    // output_streams: BTreeMap<VarName, OutputStream<R>>,
    cancellation_token: CancellationToken,
    cancellation_guard: DropGuard,
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

        let var_exchange = Arc::new(QueuingVarExchange::new(var_names));

        let input_streams = model
            .input_vars()
            .iter()
            .map(|var| {
                let stream = (&mut input_streams).input_stream(var);
                (var.clone(), stream.unwrap())
            })
            .collect::<BTreeMap<_, _>>();
        let input_streams = Arc::new(Mutex::new(input_streams));

        let cancellation_token = CancellationToken::new();
        let cancellation_guard = cancellation_token.clone().drop_guard();

        Self {
            model,
            input_streams,
            var_exchange,
            semantics_t: PhantomData,
            phantom_t: PhantomData,
            cancellation_token,
            cancellation_guard,
        }
        .spawn_tasks()
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
    fn spawn_tasks(self) -> Self {
        for var in self.model.input_vars() {
            let input = self.monitor_input(var.clone());
            tokio::spawn(input);
        }

        for var in self.model.output_vars() {
            let output = self.monitor_output(var.clone());
            tokio::spawn(output);
        }

        self
    }

    fn monitor_input(&self, var: VarName) -> impl Future<Output = ()> + Send + 'static {
        let var_exchange = self.var_exchange.clone();
        let mut input = self.input_streams.lock().unwrap().remove(&var).unwrap();
        let cancellation_token = self.cancellation_token.clone();

        async move {
            loop {
                tokio::select! {
                    biased;
                    _ = cancellation_token.cancelled() => {
                        return;
                    }
                    Some(mut data) = input.next() => {
                        // Try and send the data until the exchange is able to accept it
                        // Currently we can consume unbounded input
                        while let Some(unsent_data) = var_exchange.publish(&var, data, None) {
                            data = unsent_data;
                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        }
    }

    fn monitor_output(&self, var: VarName) -> impl Future<Output = ()> + Send + 'static {
        let var_exchange = self.var_exchange.clone();
        let mut var_stream = S::to_async_stream(self.model.var_expr(&var).unwrap(), &var_exchange);
        let cancellation_token = self.cancellation_token.clone();
        async move {
            loop {
                tokio::select! {
                    biased;
                    _ = cancellation_token.cancelled() => {
                        return;
                    }
                    Some(mut data) = var_stream.next() => {
                        // Try and send the data until the exchange is able to accept it
                        while let Some(unsent_data) = var_exchange.publish(&var, data, Some(60)) {
                            data = unsent_data;
                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        }
    }

    fn output_stream(&self, var: VarName) -> OutputStream<R> {
        count_read(
            self.var_exchange.var(&var).unwrap(),
            self.var_exchange.max_read.get(&var).unwrap().clone(),
        )
    }
}
