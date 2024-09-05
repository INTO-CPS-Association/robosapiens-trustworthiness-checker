use std::collections::BTreeMap;
use std::future::Future;
use std::marker::PhantomData;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;

use futures::future::join_all;
use futures::stream;
use futures::stream::BoxStream;
use futures::StreamExt;
use tokio::sync::broadcast::{channel, Sender};
use tokio_util::sync::CancellationToken;
use tokio_util::sync::DropGuard;

use crate::core::InputProvider;
use crate::core::Monitor;
use crate::core::MonitoringSemantics;
use crate::core::Specification;
use crate::core::StreamData;
use crate::core::StreamExpr;
use crate::core::{OutputStream, StreamContext, ConcreteStreamData, VarName};

struct AsyncVarExchange<T: StreamData> {
    senders: BTreeMap<VarName, Arc<Mutex<Sender<T>>>>,
}

impl<T: StreamData> AsyncVarExchange<T> {
    fn new(vars: Vec<VarName>) -> Self {
        let mut senders = BTreeMap::new();

        for var in vars {
            let (sender, _) = channel::<T>(100);
            senders.insert(var, Arc::new(Mutex::new(sender)));
        }

        AsyncVarExchange { senders }
    }

    fn publish(
        &self,
        var: &VarName,
        data: T,
        max_queued: Option<usize>,
    ) -> Option<T> {
        // Don't send if maxed_queued limit is set and reached
        // This check is integrated into publish so that len can
        // be checked within the same lock acquisition as sending the data
        // Return None if the data was not sent

        let sender = self.senders.get(&var).unwrap();
        {
            let sender = sender.lock().unwrap();

            if let Some(max_queued) = max_queued {
                if sender.len() < max_queued {
                    sender.send(data);
                    None
                } else {
                    Some(data)
                }
            } else {
                sender.send(data);
                None
            }
        }
    }
}

impl<T: StreamData> StreamContext<T> for Arc<AsyncVarExchange<T>> {
    fn var(&self, var: &VarName) -> Option<OutputStream<T>> {
        match self.senders.get(&var) {
            Some(sender) => {
                let receiver = sender.lock().unwrap().subscribe();
                println!("Subscribed to {}", var);
                Some(Box::pin(stream::unfold(
                    receiver,
                    |mut receiver| async move {
                        if let Ok(res) = receiver.recv().await {
                            Some((res, receiver))
                        } else {
                            None
                        }
                    },
                )))
            }
            None => None,
        }
    }
}

pub struct AsyncMonitorRunner<T, S, M, R>
where
    T: StreamExpr,
    S: MonitoringSemantics<T, R>,
    M: Specification<T>,
    R: StreamData,
{
    input_streams: Arc<Mutex<BTreeMap<VarName, BoxStream<'static, R>>>>,
    model: M,
    var_exchange: Arc<AsyncVarExchange<R>>,
    // tasks: Option<Vec<Pin<Box<dyn Future<Output = ()> + Send>>>>,
    output_streams: BTreeMap<VarName, OutputStream<R>>,
    cancellation_token: CancellationToken,
    cancellation_guard: DropGuard,
    phantom_t: PhantomData<T>,
    semantics_t: PhantomData<S>,
}

impl<T: StreamExpr, S: MonitoringSemantics<T, R>, M: Specification<T>, R: StreamData> Monitor<T, S, M, R>
    for AsyncMonitorRunner<T, S, M, R>
{
    fn new(model: M, mut input_streams: impl InputProvider<R>) -> Self {
        let var_names = model
            .input_vars()
            .into_iter()
            .chain(model.output_vars().into_iter())
            .collect();

        let var_exchange = Arc::new(AsyncVarExchange::new(var_names));

        let input_streams = model
            .input_vars()
            .iter()
            .map(|var| {
                let stream = (&mut input_streams).input_stream(var);
                (var.clone(), stream.unwrap())
            })
            .collect::<BTreeMap<_, _>>();
        let input_streams = Arc::new(Mutex::new(input_streams));

        let output_streams = BTreeMap::new();

        let cancellation_token = CancellationToken::new();
        let cancellation_guard = cancellation_token.clone().drop_guard();

        Self {
            model,
            input_streams,
            var_exchange,
            output_streams,
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
        let output_streams = mem::take(&mut self.output_streams);
        let mut outputs = self.model.output_vars();
        outputs.sort();

        Box::pin(stream::unfold(
            (output_streams, outputs),
            |(mut output_streams, outputs)| async move {
                let mut futures = vec![];
                for (_, stream) in output_streams.iter_mut() {
                    futures.push(stream.next());
                }

                let next_vals = join_all(futures).await;
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
                return Some((res, (output_streams, outputs)));
            },
        )) as BoxStream<'static, BTreeMap<VarName, R>>
    }
}

impl<T: StreamExpr, S: MonitoringSemantics<T, R>, M: Specification<T>, R: StreamData> AsyncMonitorRunner<T, S, M, R> {
    fn spawn_tasks(mut self) -> Self {
        let tasks = self.monitoring_tasks();

        for task in tasks.into_iter() {
            tokio::spawn(task);
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
                    _ = cancellation_token.cancelled() => {
                        return;
                    }
                    Some(mut data) = input.next() => {
                        // Try and send the data until the exchange is able to accept it
                        // We try and receive upto 80/100 input messages
                        // to avoid monitoring from blocking the input streams
                        while let Some(unsent_data) = var_exchange.publish(&var, data, Some(80)) {
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

        let sexpr = self.model.var_expr(&var).unwrap();
        let mut output = S::to_async_stream(sexpr, &var_exchange);

        let cancellation_token = self.cancellation_token.clone();

        async move {
            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        return;
                    }
                    Some(mut data) = output.next() => {
                        // Try and send the data until the exchange is able to accept it
                        while let Some(unsent_data) = var_exchange.publish(&var, data, Some(10)) {
                            data = unsent_data;
                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        }
    }

    // Define futures which monitors each of the input and output variables
    // as well as output streams which can be used to subscribe to the output
    // Note that the numbers of subscribers to each broadcast channel is
    // determined when this function is run
    fn monitoring_tasks(&mut self) -> Vec<Pin<Box<dyn Future<Output = ()> + Send>>> {
        let mut tasks: Vec<Pin<Box<dyn Future<Output = ()> + Send>>> = vec![];

        for var in self.model.input_vars().iter() {
            tasks.push(Box::pin(self.monitor_input(var.clone())));

            // Add output steams that just echo the inputs
            // self.output_streams
            // .insert(var.clone(), mc::var(&self.var_exchange, var.clone()));
        }

        for var in self.model.output_vars().iter() {
            tasks.push(Box::pin(self.monitor_output(var.clone())));

            // Add output steams that subscribe to the inputs
            let var_expr = T::var(var);
            let var_output_stream = S::to_async_stream(var_expr, &self.var_exchange);
            self.output_streams.insert(var.clone(), var_output_stream);
        }

        tasks
    }
}
