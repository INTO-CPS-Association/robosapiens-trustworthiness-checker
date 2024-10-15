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
use tokio::select;
use tokio::sync::broadcast;
use tokio::sync::broadcast::channel;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tokio_util::sync::DropGuard;

use crate::core::ExpressionTyping;
use crate::core::InputProvider;
use crate::core::Monitor;
use crate::core::MonitoringSemantics;
use crate::core::Specification;
use crate::core::StreamExpr;
use crate::core::StreamSystem;
use crate::core::TypeAnnotated;
use crate::core::TypeSystem;
use crate::core::{OutputStream, StreamContext, VarName};

struct AsyncVarExchange<SS: StreamSystem> {
    senders: BTreeMap<
        VarName,
        (
            <SS::TypeSystem as TypeSystem>::Type,
            Arc<Mutex<broadcast::Sender<Option<<SS::TypeSystem as TypeSystem>::TypedValue>>>>,
        ),
    >,
    cancellation_token: CancellationToken,
    drop_guard: Arc<DropGuard>,
}

impl<SS: StreamSystem> AsyncVarExchange<SS> {
    fn new(
        vars: Vec<(VarName, <SS::TypeSystem as TypeSystem>::Type)>,
        cancellation_token: CancellationToken,
        drop_guard: Arc<DropGuard>,
    ) -> Self {
        let mut senders = BTreeMap::new();

        for (var, typ) in vars {
            let (sender, _) = channel::<Option<<SS::TypeSystem as TypeSystem>::TypedValue>>(100);
            senders.insert(var, (typ, Arc::new(Mutex::new(sender))));
        }

        AsyncVarExchange {
            senders,
            cancellation_token,
            drop_guard,
        }
    }

    fn publish(
        &self,
        var: &VarName,
        data: Option<<SS::TypeSystem as TypeSystem>::TypedValue>,
        max_queued: Option<usize>,
    ) -> Option<Option<<SS::TypeSystem as TypeSystem>::TypedValue>> {
        // Don't send if maxed_queued limit is set and reached
        // This check is integrated into publish so that len can
        // be checked within the same lock acquisition as sending the data
        // Return None if the data was not sent

        let (typ, sender) = self.senders.get(&var).unwrap();
        {
            if let Some(data) = &data {
                assert_eq!(SS::TypeSystem::type_of_value(data), typ.clone())
            }

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

fn receiver_to_stream<SS: StreamSystem>(
    typ: <SS::TypeSystem as TypeSystem>::Type,
    recv: broadcast::Receiver<Option<<SS::TypeSystem as TypeSystem>::TypedValue>>,
) -> SS::TypedStream {
    SS::to_typed_stream(
        typ,
        Box::pin(
            stream::unfold(recv, |mut recv| async move {
                if let Ok(res) = recv.recv().await {
                    Some((res?, recv))
                } else {
                    None
                }
            })
            .fuse(),
        ) as OutputStream<<SS::TypeSystem as TypeSystem>::TypedValue>,
    )
}

impl<SS: StreamSystem> StreamContext<SS> for Arc<AsyncVarExchange<SS>> {
    fn var(&self, var: &VarName) -> Option<SS::TypedStream> {
        let (typ, sender) = self.senders.get(var)?;
        let receiver = sender.lock().unwrap().subscribe();
        println!("Subscribed to {}", var);
        Some(receiver_to_stream::<SS>(typ.clone(), receiver))
    }

    fn advance(&self) {
        // Do nothing
    }

    fn subcontext(&self, history_length: usize) -> Box<dyn StreamContext<SS>> {
        Box::new(SubMonitor::new(self.clone(), history_length))
    }
}

struct SubMonitor<SS: StreamSystem> {
    parent: Arc<AsyncVarExchange<SS>>,
    senders: BTreeMap<
        VarName,
        (
            <SS::TypeSystem as TypeSystem>::Type,
            broadcast::Sender<Option<<SS::TypeSystem as TypeSystem>::TypedValue>>,
        ),
    >,
    buffer_size: usize,
    progress_sender: watch::Sender<usize>,
}

impl<SS: StreamSystem> SubMonitor<SS> {
    fn new(parent: Arc<AsyncVarExchange<SS>>, buffer_size: usize) -> Self {
        let mut senders = BTreeMap::new();

        for (var, (typ, _)) in parent.senders.iter() {
            // buffers.insert(
            //     var.clone(),
            //     Arc::new(Mutex::new(RingBuffer::new(buffer_size))),
            // );
            senders.insert(var.clone(), (typ.clone(), broadcast::Sender::new(100)));
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
        for var in self.parent.senders.keys() {
            let (send, recv) = mpsc::channel(self.buffer_size);
            let parent_receiver = self
                .parent
                .senders
                .get(var)
                .unwrap()
                .1
                .lock()
                .unwrap()
                .subscribe();
            let (_, child_sender) = self.senders.get(var).unwrap().clone();
            let clock = self.progress_sender.subscribe();
            tokio::spawn(Self::distribute(
                recv,
                child_sender,
                clock,
                self.parent.cancellation_token.clone(),
            ));
            tokio::spawn(Self::monitor(
                parent_receiver,
                send.clone(),
                self.parent.cancellation_token.clone(),
            ));
        }

        self
    }

    async fn distribute(
        mut recv: mpsc::Receiver<Option<<SS::TypeSystem as TypeSystem>::TypedValue>>,
        send: broadcast::Sender<Option<<SS::TypeSystem as TypeSystem>::TypedValue>>,
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
                _ = clock.changed() => {
                    let clock_new = *clock.borrow_and_update();
                    for _ in clock_old..=clock_new {
                        select! {
                            biased;
                            _ = cancellation_token.cancelled() => {
                                return;
                            }
                            data = recv.recv() => {
                                if let Some(data) = data {
                                    // let data_copy = data.clone();
                                    if let Err(_) = send.send(data) {
                                        // println!("Failed to send data {:?} due to no receivers (err = {:?})", data_copy, e);
                                    }
                                } else {
                                    return;
                                }
                            }
                            // TODO: should we have a release lock here for deadlock prevention?
                        }
                    }
                    clock_old = clock_new;
                }
            }
        }
    }

    async fn monitor(
        mut recv: broadcast::Receiver<Option<<SS::TypeSystem as TypeSystem>::TypedValue>>,
        send: mpsc::Sender<Option<<SS::TypeSystem as TypeSystem>::TypedValue>>,
        cancellation_token: CancellationToken,
    ) {
        loop {
            select! {
                biased;
                _ = cancellation_token.cancelled() => {
                    return;
                }
                Ok(data) = recv.recv() => {
                    let finished = data.is_none();
                    send.send(data).await;
                    if finished {
                        return;
                    }
                }
            }
        }
    }
}

impl<SS: StreamSystem> StreamContext<SS> for SubMonitor<SS> {
    fn var(&self, var: &VarName) -> Option<SS::TypedStream> {
        // let buffer = mem::replace(
        // self.buffers.get(var)?.lock().unwrap().deref_mut(),
        // RingBuffer::new(self.buffer_size),
        // );
        // Don't clear the buffer since the evaled statement might be changed
        // in < history_length time steps, meaning that we need some of the
        // buffered data to evaluate the new statement

        let (typ, sender) = self.senders.get(var).unwrap();

        let recv = sender.subscribe();

        Some(receiver_to_stream::<SS>(typ.clone(), recv))
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
    S: MonitoringSemantics<ET::TypedExpr, StreamSystem = SS>,
    M: Specification<ET> + TypeAnnotated<ET::TypeSystem>,
{
    input_streams: Arc<Mutex<BTreeMap<VarName, SS::TypedStream>>>,
    model: M,
    var_exchange: Arc<AsyncVarExchange<SS>>,
    // tasks: Option<Vec<Pin<Box<dyn Future<Output = ()> + Send>>>>,
    output_streams: BTreeMap<VarName, SS::TypedStream>,
    cancellation_token: CancellationToken,
    cancellation_guard: Arc<DropGuard>,
    semantics_t: PhantomData<S>,
    expression_typing_t: PhantomData<ET>,
    // expr_t: PhantomData<E>,
}

impl<
        ET: ExpressionTyping,
        SS: StreamSystem<TypeSystem = ET::TypeSystem>,
        S: MonitoringSemantics<ET::TypedExpr, StreamSystem = SS>,
        M: Specification<ET> + TypeAnnotated<ET::TypeSystem>,
    > Monitor<ET, SS, S, M> for AsyncMonitorRunner<ET, SS, S, M>
where
    ET::TypedExpr: StreamExpr<<ET::TypeSystem as TypeSystem>::Type>,
{
    fn new(model: M, mut input_streams: impl InputProvider<SS>) -> Self {
        let var_names = model
            .input_vars()
            .into_iter()
            .chain(model.output_vars().into_iter())
            .map(|var| (var.clone(), model.type_of_var(&var)))
            .collect();

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
        let cancellation_guard = Arc::new(cancellation_token.clone().drop_guard());

        let var_exchange = Arc::new(AsyncVarExchange::new(
            var_names,
            cancellation_token.clone(),
            cancellation_guard.clone(),
        ));

        Self {
            model,
            input_streams,
            var_exchange,
            output_streams,
            semantics_t: PhantomData,
            expression_typing_t: PhantomData,
            cancellation_token,
            cancellation_guard,
        }
        .spawn_tasks()
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

impl<
        ET: ExpressionTyping,
        SS: StreamSystem<TypeSystem = ET::TypeSystem>,
        S: MonitoringSemantics<ET::TypedExpr, StreamSystem = SS>,
        M: Specification<ET> + TypeAnnotated<ET::TypeSystem>,
    > AsyncMonitorRunner<ET, SS, S, M>
where
    ET::TypedExpr: StreamExpr<<ET::TypeSystem as TypeSystem>::Type>,
{
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
                    biased;
                    _ = cancellation_token.cancelled() => {
                        return;
                    }
                    mut data = input.next() => {
                        // Try and send the data until the exchange is able to accept it
                        // We try and receive upto 80/100 input messages
                        // to avoid monitoring from blocking the input streams
                        let finished = data.is_none();
                        while let Some(unsent_data) = var_exchange.publish(&var, data, Some(80)) {
                            data = unsent_data;
                            // tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        }
                        if finished {
                            return;
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
                    biased;
                    _ = cancellation_token.cancelled() => {
                        return;
                    }
                    mut data = output.next() => {
                        // Try and send the data until the exchange is able to accept it
                        let finished = data.is_none();
                        while let Some(unsent_data) = var_exchange.publish(&var, data, Some(10)) {
                            data = unsent_data;
                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        }
                        if finished {
                            return;
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
            let var_expr = ET::TypedExpr::var(self.spec().type_of_var(var), var);
            let var_output_stream = S::to_async_stream(var_expr, &self.var_exchange);
            self.output_streams.insert(var.clone(), var_output_stream);
        }

        tasks
    }
}
