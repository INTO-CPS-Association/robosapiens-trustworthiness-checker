use std::mem;
use std::{collections::BTreeMap, rc::Rc};

use crate::core::StreamData;
use crate::{InputProvider, OutputStream, VarName};

use async_cell::unsync::AsyncCell;
use async_stream::stream;
use async_unsync::oneshot;
use futures::future::{LocalBoxFuture, join_all};
use futures::{FutureExt, StreamExt, select};
use smol::LocalExecutor;
use tracing::info;
use unsync::broadcast::{Receiver as BroadcastReceiver, Sender as BroadcastSender};
use unsync::spsc::Sender as SpscSender;
use unsync::spsc::{self, Receiver as SpscReceiver};

/// An input provider that multiplexes a single inner input provider to multiple clients.
pub struct MultiplexedInputProvider<Val: Clone> {
    executor: Rc<LocalExecutor<'static>>,
    inner_input_provider: Box<dyn InputProvider<Val = Val>>,
    ready: Rc<AsyncCell<anyhow::Result<()>>>,
    run_result: Rc<AsyncCell<anyhow::Result<()>>>,
    should_run: Rc<AsyncCell<()>>,
    subscribers_req:
        Option<SpscReceiver<oneshot::Sender<BTreeMap<VarName, BroadcastReceiver<Val>>>>>,
    subscribers_req_tx: SpscSender<oneshot::Sender<BTreeMap<VarName, BroadcastReceiver<Val>>>>,
    senders: Option<BTreeMap<VarName, BroadcastSender<Val>>>,
    clock_rx: Option<SpscReceiver<()>>,
}

pub struct MultiplexedInputProviderClient<Val: Clone> {
    receivers: BTreeMap<VarName, BroadcastReceiver<Val>>,
    ready: Rc<AsyncCell<anyhow::Result<()>>>,
    run_result: Rc<AsyncCell<anyhow::Result<()>>>,
    should_run: Rc<AsyncCell<()>>,
}

impl<Val: StreamData> MultiplexedInputProvider<Val> {
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        inner_input_provider: Box<dyn InputProvider<Val = Val>>,
        clock_rx: SpscReceiver<()>,
    ) -> MultiplexedInputProvider<Val> {
        let ready = AsyncCell::shared();
        let run_result = AsyncCell::shared();
        let should_run = AsyncCell::shared();
        let senders = Some(
            inner_input_provider
                .vars()
                .iter()
                .cloned()
                .map(|v| (v, unsync::broadcast::channel(10)))
                .collect(),
        );
        let (subscribers_req_tx, subscribers_req_rx) = spsc::channel(10);
        let subscribers_req = Some(subscribers_req_rx);
        let clock_rx = Some(clock_rx);

        MultiplexedInputProvider {
            executor,
            ready,
            run_result,
            should_run,
            senders,
            inner_input_provider,
            clock_rx,
            subscribers_req,
            subscribers_req_tx,
        }
    }

    pub async fn subscribe(&mut self) -> MultiplexedInputProviderClient<Val> {
        let (receivers_tx, receivers_rx) = oneshot::channel().into_split();
        let _ = self.subscribers_req_tx.send(receivers_tx).await;
        info!("Waiting for receivers");
        let receivers = receivers_rx
            .await
            .expect("Requested and expected receivers");
        info!("Got receivers");
        let ready = self.ready.clone();
        let run_result = self.run_result.clone();
        let should_run = self.should_run.clone();

        MultiplexedInputProviderClient {
            receivers,
            ready,
            run_result,
            should_run,
        }
    }

    pub fn run(&mut self) -> LocalBoxFuture<'static, ()> {
        let run_result = self.run_result.clone();
        let mut inner_run = self.executor.spawn(self.inner_input_provider.run()).fuse();
        let mut clock_rx = mem::take(&mut self.clock_rx).expect("Clock receiver not available");
        let mut senders = mem::take(&mut self.senders).expect("Senders not available");
        let mut input_streams: Vec<_> = senders
            .keys()
            .map(|k| {
                self.inner_input_provider
                    .input_stream(k)
                    .expect("Expected input steam")
            })
            .collect();
        let subscribers_req_rx =
            mem::take(&mut self.subscribers_req).expect("Subscribers req channel is gone");
        info!("MultiplexedInputProvider run future created");

        Box::pin(async move {
            info!("MultiplexedInputProvider started");
            let mut next_subscribers_req_stream = Box::pin(StreamExt::fuse(stream! {
                let mut subscribers_req_rx = subscribers_req_rx;
                while let Some(res) = subscribers_req_rx.recv().await {
                    yield res
                }
            }));
            let mut all_inputs_stream = Box::pin(StreamExt::fuse(stream! {
                loop {
                    yield join_all(input_streams.iter_mut().map(|s| s.next())).await;
                }
            }));

            loop {
                select! {
                    res = inner_run => {
                        info!("Inner run returned");
                        run_result.set(res)
                    }
                    receiver_req_tx = next_subscribers_req_stream.next() => {
                        match receiver_req_tx {
                            Some(receiver_req_tx) => {
                                let receivers = senders.iter_mut()
                                    .map(|(k, tx)| (k.clone(), tx.subscribe()))
                                    .collect();
                                receiver_req_tx.send(receivers).expect("Tried to send ");
                            }
                            None => {
                                return
                            }
                        }
                    }
                    results = all_inputs_stream.next() => {
                        match results {
                            Some(mut results) => {
                                info!("Inputs returned, waiting for clock tick");
                                // Wait for clock tick before distributing any results
                                // (so new receivers have time to subscribe)
                                let mut clock_ticked = false;
                                let mut clock_tick = Box::pin(clock_rx.recv().fuse());
                                while !clock_ticked {
                                    select!{
                                        _ = clock_tick => {clock_ticked = true}
                                        // Also allow new requests whilst waiting to distribute the
                                        // input
                                        receiver_req_tx = next_subscribers_req_stream.next() => {
                                            match receiver_req_tx {
                                                Some(receiver_req_tx) => {
                                                    let receivers = senders.iter_mut()
                                                        .map(|(k, tx)| (k.clone(), tx.subscribe()))
                                                        .collect();
                                                    receiver_req_tx.send(receivers).expect("Tried to send ");
                                                }
                                                None => {
                                                    return
                                                }
                                            }
                                        }
                                    }
                                }

                                info!("Clocks ticked");

                                if results.iter().all(|x| x.is_none()) {
                                    return
                                }

                                for (res, sender) in results.iter_mut().zip(senders.values_mut()) {
                                    if let Some(res) = res {
                                        sender.send(res.clone()).await;
                                    }
                                }
                            }
                            None => {
                                return
                            }
                        }
                    }
                }
            }
        })
    }
}

impl<Val: Clone + 'static> InputProvider for MultiplexedInputProviderClient<Val> {
    type Val = Val;

    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<Self::Val>> {
        self.receivers.remove(var).map(|mut rx| {
            Box::pin(stream! {
                while let Some(val) = rx.recv().await {
                    yield val
                }
            }) as OutputStream<Val>
        })
    }

    fn ready(&self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let ready = self.ready.clone();

        Box::pin(async move { ready.take_shared().await })
    }

    fn run(&mut self) -> LocalBoxFuture<'static, Result<(), anyhow::Error>> {
        let run_result = self.run_result.clone();
        let should_run = self.should_run.clone();

        Box::pin(async move {
            should_run.set(());
            run_result.take_shared().await
        })
    }

    fn vars(&self) -> Vec<VarName> {
        self.receivers.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, rc::Rc};

    use futures::stream;
    use macro_rules_attribute::apply;
    use smol::{LocalExecutor, stream::StreamExt};
    use tracing::info;

    use super::MultiplexedInputProvider;
    use crate::{InputProvider, OutputStream, Value, async_test};

    #[apply(async_test)]
    async fn single_multiplexed_input_provider(ex: Rc<LocalExecutor<'static>>) {
        info!("In test");
        let mut in1 = BTreeMap::new();
        in1.insert(
            "x".into(),
            Box::pin(stream::iter(vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3),
            ])) as OutputStream<Value>,
        );
        let in1: Box<dyn InputProvider<Val = Value>> = Box::new(in1);

        let (mut clock_tx, clock_rx) = unsync::spsc::channel(10);

        let mut mux_in: MultiplexedInputProvider<Value> =
            MultiplexedInputProvider::new(ex.clone(), in1, clock_rx);

        info!("Detaching monitor");
        ex.spawn(mux_in.run()).detach();

        info!("Subscribing");
        let mut mux_in_client = mux_in.subscribe().await;

        // Tick the clock 3 times
        info!("Tick 0");
        clock_tx.send(()).await.unwrap();
        info!("Tick 1");
        clock_tx.send(()).await.unwrap();
        info!("Tick 2");
        clock_tx.send(()).await.unwrap();
        info!("Tick 3");
        // TODO: we need a fourth tick to detect that the stream has ended
        // is this correct
        clock_tx.send(()).await.unwrap();
        info!("Tick 4");

        let mut xs = mux_in_client
            .input_stream(&"x".into())
            .expect("Input stream should exist");
        // let xs: Vec<_> = xs.collect().await;
        let mut xs_expected = vec![Value::Int(1), Value::Int(2), Value::Int(3)].into_iter();

        // while let (Some(x), e_expected) =
        while let (Some(x), Some(x_exp)) = (xs.next().await, xs_expected.next()) {
            info!(?x, ?x_exp, "received output");
            assert_eq!(x, x_exp)
        }
    }

    #[apply(async_test)]
    async fn dual_multiplexed_input_provider(ex: Rc<LocalExecutor<'static>>) {
        info!("In test");
        let mut in1 = BTreeMap::new();
        in1.insert(
            "x".into(),
            Box::pin(stream::iter(vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3),
            ])) as OutputStream<Value>,
        );
        let in1: Box<dyn InputProvider<Val = Value>> = Box::new(in1);

        let (mut clock_tx, clock_rx) = unsync::spsc::channel(10);

        let mut mux_in: MultiplexedInputProvider<Value> =
            MultiplexedInputProvider::new(ex.clone(), in1, clock_rx);

        info!("Detaching monitor");
        ex.spawn(mux_in.run()).detach();

        info!("Subscribing");
        let mut mux_in_client1 = mux_in.subscribe().await;
        let mut mux_in_client2 = mux_in.subscribe().await;

        // Tick the clock 3 times
        info!("Tick 0");
        clock_tx.send(()).await.unwrap();
        info!("Tick 1");
        clock_tx.send(()).await.unwrap();
        info!("Tick 2");
        clock_tx.send(()).await.unwrap();
        info!("Tick 3");
        // TODO: we need a fourth tick to detect that the stream has ended
        // is this correct
        clock_tx.send(()).await.unwrap();
        info!("Tick 4");

        let mut xs1 = mux_in_client1
            .input_stream(&"x".into())
            .expect("Input stream should exist");
        let mut xs2 = mux_in_client2
            .input_stream(&"x".into())
            .expect("Input stream should exist");
        // let xs: Vec<_> = xs.collect().await;
        let mut xs_expected = vec![Value::Int(1), Value::Int(2), Value::Int(3)].into_iter();

        // while let (Some(x), e_expected) =
        while let (Some(x1), Some(x2), Some(x_exp)) =
            (xs1.next().await, xs2.next().await, xs_expected.next())
        {
            info!(?x1, ?x2, ?x_exp, "received output");
            assert_eq!(x1, x_exp);
            assert_eq!(x2, x_exp);
        }
    }

    #[apply(async_test)]
    async fn switching_multiplexed_input_provider(ex: Rc<LocalExecutor<'static>>) {
        info!("In test");
        let mut in1 = BTreeMap::new();
        in1.insert(
            "x".into(),
            Box::pin(stream::iter(vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3),
                Value::Int(4),
            ])) as OutputStream<Value>,
        );
        let in1: Box<dyn InputProvider<Val = Value>> = Box::new(in1);

        let (mut clock_tx, clock_rx) = unsync::spsc::channel(10);

        let mut mux_in: MultiplexedInputProvider<Value> =
            MultiplexedInputProvider::new(ex.clone(), in1, clock_rx);

        info!("Detaching monitor");
        ex.spawn(mux_in.run()).detach();

        {
            info!("Subscribing");
            let mut mux_in_client1 = mux_in.subscribe().await;
            // let mut mux_in_client2 = mux_in.subscribe().await;

            // Tick the clock 3 times
            info!("Tick 0");
            clock_tx.send(()).await.unwrap();
            info!("Tick 1");
            clock_tx.send(()).await.unwrap();
            info!("Tick 2");
            // clock_tx.send(()).await.unwrap();
            // info!("Tick 3");
            // TODO: we need a fourth tick to detect that the stream has ended
            // is this correct
            // clock_tx.send(()).await.unwrap();
            // info!("Tick 4");

            let mut xs1 = mux_in_client1
                .input_stream(&"x".into())
                .expect("Input stream should exist")
                .take(2);
            // let xs: Vec<_> = xs.collect().await;
            let mut xs_expected = vec![Value::Int(1), Value::Int(2)].into_iter();

            // while let (Some(x), e_expected) =
            while let (Some(x), Some(x_exp)) = (xs1.next().await, xs_expected.next()) {
                info!(?x, ?x_exp, "received output");
                assert_eq!(x, x_exp);
            }
        }

        {
            let mut mux_in_client2 = mux_in.subscribe().await;
            let mut xs2 = mux_in_client2
                .input_stream(&"x".into())
                .expect("Input stream should exist");

            clock_tx.send(()).await.unwrap();
            info!("Tick 3");
            clock_tx.send(()).await.unwrap();
            info!("Tick 4");
            clock_tx.send(()).await.unwrap();

            let mut xs_expected = vec![Value::Int(3), Value::Int(4)].into_iter();
            while let (Some(x), Some(x_exp)) = (xs2.next().await, xs_expected.next()) {
                info!(?x, ?x_exp, "received output");
                assert_eq!(x, x_exp);
            }
        }
    }
}
