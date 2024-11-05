use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::vec;
use tokio::sync::Mutex;

use futures::future::join_all;
use futures::stream;
use futures::stream::BoxStream;
use futures::StreamExt;

use crate::core::ExpressionTyping;
use crate::core::InputProvider;
use crate::core::Monitor;
use crate::core::MonitoringSemantics;
use crate::core::Specification;
use crate::core::StreamData;
use crate::core::StreamSystem;
use crate::core::TypeAnnotated;
use crate::core::TypeSystem;
use crate::core::{OutputStream, StreamContext, VarName};
use crate::StreamExpr;

struct QueuingVarContext<SS: StreamSystem> {
    queues: BTreeMap<
        VarName,
        (
            <SS::TypeSystem as TypeSystem>::Type,
            Arc<Mutex<Vec<<SS::TypeSystem as TypeSystem>::TypedValue>>>,
        ),
    >,
    input_streams:
        BTreeMap<VarName, Arc<Mutex<OutputStream<<SS::TypeSystem as TypeSystem>::TypedValue>>>>,
    output_streams:
        BTreeMap<VarName, WaitingStream<OutputStream<<SS::TypeSystem as TypeSystem>::TypedValue>>>,
    production_locks: BTreeMap<VarName, Arc<Mutex<()>>>,
}

impl<SS: StreamSystem> QueuingVarContext<SS> {
    fn new(
        vars: Vec<(VarName, <SS::TypeSystem as TypeSystem>::Type)>,
        input_streams: BTreeMap<
            VarName,
            Arc<Mutex<OutputStream<<SS::TypeSystem as TypeSystem>::TypedValue>>>,
        >,
        output_streams: BTreeMap<
            VarName,
            WaitingStream<OutputStream<<SS::TypeSystem as TypeSystem>::TypedValue>>,
        >,
    ) -> Self {
        let mut queues = BTreeMap::new();
        let mut production_locks = BTreeMap::new();

        for (var, typ) in vars {
            queues.insert(var.clone(), (typ, Arc::new(Mutex::new(Vec::new()))));
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
        let ret_stream: Arc<Mutex<S>>;
        match self {
            WaitingStream::Arrived(stream) => return stream.clone(),
            WaitingStream::Waiting(receiver) => {
                let stream_lock = receiver.wait_for(|x| x.is_some()).await.unwrap();
                let stream = stream_lock.as_ref().unwrap().clone();
                ret_stream = stream
            }
        }
        *self = WaitingStream::Arrived(ret_stream);
        if let WaitingStream::Arrived(stream) = self {
            return stream.clone();
        } else {
            panic!("Stream should be arrived")
        }
    }
}

fn queue_buffered_stream<V: StreamData>(
    xs: Arc<Mutex<Vec<V>>>,
    waiting_stream: WaitingStream<OutputStream<V>>,
    lock: Arc<Mutex<()>>,
) -> OutputStream<V> {
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

impl<SS: StreamSystem> StreamContext<SS> for Arc<QueuingVarContext<SS>> {
    fn var(
        &self,
        var: &VarName,
    ) -> Option<OutputStream<<SS::TypeSystem as TypeSystem>::TypedValue>> {
        let (_, queue) = self.queues.get(var)?;
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

    fn subcontext(&self, history_length: usize) -> Box<dyn StreamContext<SS>> {
        Box::new(SubMonitor::new(self.clone(), history_length))
    }
}

struct SubMonitor<SS: StreamSystem> {
    parent: Arc<QueuingVarContext<SS>>,
    #[allow(dead_code)]
    // TODO: implement restricting subcontexts to a certain history length;
    // this is currently not implemented by the queuing runtime
    buffer_size: usize,
    index: Arc<StdMutex<usize>>,
}

impl<SS: StreamSystem> SubMonitor<SS> {
    fn new(parent: Arc<QueuingVarContext<SS>>, buffer_size: usize) -> Self {
        SubMonitor {
            parent,
            buffer_size,
            index: Arc::new(StdMutex::new(0)),
        }
    }
}

impl<SS: StreamSystem> StreamContext<SS> for SubMonitor<SS> {
    fn var(
        &self,
        var: &VarName,
    ) -> Option<OutputStream<<SS::TypeSystem as TypeSystem>::TypedValue>> {
        let parent_stream = self.parent.var(var)?;
        let index = *self.index.lock().unwrap();
        let substream = parent_stream.skip(index);

        Some(Box::pin(substream))
    }

    fn subcontext(&self, history_length: usize) -> Box<dyn StreamContext<SS>> {
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

pub struct QueuingMonitorRunner<ET, SS, S, M>
where
    ET: ExpressionTyping,
    ET::TypedExpr: StreamExpr<<ET::TypeSystem as TypeSystem>::Type>,
    SS: StreamSystem<TypeSystem = ET::TypeSystem>,
    S: MonitoringSemantics<ET::TypedExpr, SS::TypedStream, StreamSystem = SS>,
    M: Specification<ET> + TypeAnnotated<ET::TypeSystem>,
{
    model: M,
    var_exchange: Arc<QueuingVarContext<SS>>,
    // phantom_ts: PhantomData<TS>,
    semantics_t: PhantomData<S>,
    expression_typing_t: PhantomData<ET>,
}

impl<
        ET: ExpressionTyping,
        SS: StreamSystem<TypeSystem = ET::TypeSystem>,
        S: MonitoringSemantics<ET::TypedExpr, SS::TypedStream, StreamSystem = SS>,
        M: Specification<ET> + TypeAnnotated<ET::TypeSystem>,
    > Monitor<M, <SS::TypeSystem as TypeSystem>::TypedValue> for QueuingMonitorRunner<ET, SS, S, M>
where
    ET::TypedExpr: StreamExpr<<ET::TypeSystem as TypeSystem>::Type>,
{
    fn new(
        model: M,
        mut input_streams: impl InputProvider<<SS::TypeSystem as TypeSystem>::TypedValue>,
    ) -> Self {
        let var_names: Vec<(VarName, <SS::TypeSystem as TypeSystem>::Type)> = model
            .input_vars()
            .into_iter()
            .chain(model.output_vars().into_iter())
            .map(|var| (var.clone(), model.type_of_var(&var).unwrap()))
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

        let var_exchange = Arc::new(QueuingVarContext::<SS>::new(
            var_names,
            input_streams.clone(),
            output_stream_waiting,
        ));

        for var in model.output_vars() {
            let stream = S::to_async_stream(model.var_expr(&var).unwrap(), &var_exchange);
            let stream: OutputStream<<SS::TypeSystem as TypeSystem>::TypedValue> = Box::pin(stream);
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
            expression_typing_t: PhantomData,
        }
    }

    fn spec(&self) -> &M {
        &self.model
    }

    fn monitor_outputs(
        &mut self,
    ) -> BoxStream<'static, BTreeMap<VarName, <SS::TypeSystem as TypeSystem>::TypedValue>> {
        let outputs = self.model.output_vars();
        let mut output_streams: Vec<OutputStream<<SS::TypeSystem as TypeSystem>::TypedValue>> =
            vec![];
        for output in outputs.iter().cloned() {
            output_streams.push(Box::pin(self.output_stream(output)));
        }

        Box::pin(stream::unfold(
            (output_streams, outputs),
            |(mut output_streams, outputs)| async move {
                let mut futures = vec![];
                for output_stream in output_streams.iter_mut() {
                    futures.push(output_stream.next());
                }

                let next_vals: Vec<Option<<SS::TypeSystem as TypeSystem>::TypedValue>> =
                    join_all(futures).await;
                let mut res: BTreeMap<VarName, <SS::TypeSystem as TypeSystem>::TypedValue> =
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
                Some((res, (output_streams, outputs)))
                    as Option<(
                        BTreeMap<VarName, <SS::TypeSystem as TypeSystem>::TypedValue>,
                        (
                            Vec<OutputStream<<SS::TypeSystem as TypeSystem>::TypedValue>>,
                            Vec<VarName>,
                        ),
                    )>
            },
        ))
            as BoxStream<'static, BTreeMap<VarName, <SS::TypeSystem as TypeSystem>::TypedValue>>
    }
}

impl<
        ET: ExpressionTyping,
        SS: StreamSystem<TypeSystem = ET::TypeSystem>,
        S: MonitoringSemantics<ET::TypedExpr, SS::TypedStream, StreamSystem = SS>,
        M: Specification<ET> + TypeAnnotated<ET::TypeSystem>,
    > QueuingMonitorRunner<ET, SS, S, M>
where
    ET::TypedExpr: StreamExpr<<ET::TypeSystem as TypeSystem>::Type>,
{
    fn output_stream(&self, var: VarName) -> OutputStream<<SS::TypeSystem as TypeSystem>::TypedValue> {
        self.var_exchange.var(&var).unwrap()
    }
}
