use std::mem;
use std::{collections::BTreeMap, rc::Rc};

use crate::core::StreamData;
use crate::{InputProvider, OutputStream, VarName};

use async_cell::unsync::AsyncCell;
use async_stream::stream;
use async_unsync::oneshot::{self, Sender as OneshotSender};
use futures::future::{LocalBoxFuture, join_all};
use futures::{FutureExt, StreamExt, select};
use smol::LocalExecutor;
use tracing::{debug, info, warn};
use unsync::broadcast::{Receiver as BroadcastReceiver, Sender as BroadcastSender};
use unsync::spsc::Sender as SpscSender;
use unsync::spsc::{self, Receiver as SpscReceiver};

/// A multiplexer for a single inner input provider allowing multiple
/// MultiplexerClientInputProviders to be attached to receive independent copies
/// of the input streams. This supports dynamic attachment and removal of clients,
/// allowing for reconfigurable and overlapping access to a shared InputProvider.
pub struct InputProviderMultiplexer<Val: Clone> {
    executor: Rc<LocalExecutor<'static>>,
    inner_input_provider: Box<dyn InputProvider<Val = Val>>,
    ready: Rc<AsyncCell<anyhow::Result<()>>>,
    run_result: Rc<AsyncCell<anyhow::Result<()>>>,
    should_run: Rc<AsyncCell<()>>,
    subscribers_req:
        Option<SpscReceiver<oneshot::Sender<BTreeMap<VarName, BroadcastReceiver<Val>>>>>,
    subscribers_req_tx: SpscSender<oneshot::Sender<BTreeMap<VarName, BroadcastReceiver<Val>>>>,
    senders: Option<BTreeMap<VarName, BroadcastSender<Val>>>,
    clock_rx: Option<SpscReceiver<OneshotSender<()>>>,
}

pub struct MultiplexerClientInputProvider<Val: Clone> {
    receivers: BTreeMap<VarName, BroadcastReceiver<Val>>,
    ready: Rc<AsyncCell<anyhow::Result<()>>>,
    run_result: Rc<AsyncCell<anyhow::Result<()>>>,
    should_run: Rc<AsyncCell<()>>,
}

impl<Val: StreamData> InputProviderMultiplexer<Val> {
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        inner_input_provider: Box<dyn InputProvider<Val = Val>>,
        clock_rx: SpscReceiver<OneshotSender<()>>,
    ) -> InputProviderMultiplexer<Val> {
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

        InputProviderMultiplexer {
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

    pub async fn subscribe(&mut self) -> MultiplexerClientInputProvider<Val> {
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

        MultiplexerClientInputProvider {
            receivers,
            ready,
            run_result,
            should_run,
        }
    }

    pub fn run(&mut self) -> LocalBoxFuture<'static, ()> {
        info!("Starting InputProviderMultiplexer.run()");
        let run_result = self.run_result.clone();
        debug!("Spawning inner input provider run");
        let mut inner_run = self.executor.spawn(self.inner_input_provider.run()).fuse();

        debug!("Taking clock receiver and senders");
        let mut clock_rx = mem::take(&mut self.clock_rx).expect("Clock receiver not available");
        let mut senders = mem::take(&mut self.senders).expect("Senders not available");

        debug!("Creating input streams for {} variables", senders.len());
        let mut input_streams: Vec<_> = senders
            .keys()
            .map(|k| {
                debug!("Getting input stream for variable: {}", k);
                self.inner_input_provider
                    .input_stream(k)
                    .unwrap_or_else(|| panic!("Expected input stream for variable {}", k))
            })
            .collect();
        info!("Created {} input streams", input_streams.len());

        let subscribers_req_rx =
            mem::take(&mut self.subscribers_req).expect("Subscribers req channel is gone");
        info!(
            "MultiplexedInputProvider run future created with {} input streams",
            input_streams.len()
        );

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
                                receiver_req_tx.send(receivers)
                                    .unwrap_or_else(|_| panic!("Tried to send"));
                            }
                            None => {
                                return
                            }
                        }
                    }
                    results = all_inputs_stream.next() => {
                        match results {
                            Some(mut results) => {
                                info!("Received input values: {} values, need to wait for clock tick",
                                      results.iter().filter(|r| r.is_some()).count());
                                // Wait for clock tick before distributing any results
                                // (so new receivers have time to subscribe)
                                debug!("Preparing to receive clock tick");
                                let mut clock_tick = Box::pin(clock_rx.recv().fuse());
                                let mut clock_tocker = None;

                                debug!("Entering wait loop for clock tick");
                                while clock_tocker.is_none() {
                                    select!{
                                        new_clock_tocker = clock_tick => {
                                            info!("Received clock tick signal: {:?}", new_clock_tocker.is_some());
                                            clock_tocker = Some(new_clock_tocker)
                                        }
                                        // Also allow new requests whilst waiting to distribute the
                                        // input
                                        receiver_req_tx = next_subscribers_req_stream.next() => {
                                            match receiver_req_tx {
                                                Some(receiver_req_tx) => {
                                                    let receivers = senders.iter_mut()
                                                        .map(|(k, tx)| (k.clone(), tx.subscribe()))
                                                        .collect();
                                                    receiver_req_tx.send(receivers)
                                                        .unwrap_or_else(|_| panic!(
                                                            "Tried to send "));
                                                }
                                                None => {
                                                    return
                                                }
                                            }
                                        }
                                    }
                                }

                                info!("Clocks ticked, ready to distribute input values");

                                if results.iter().all(|x| x.is_none()) {
                                    info!("All inputs returned None, ending multiplexer");
                                    return
                                }

                                let present_count = results.iter().filter(|r| r.is_some()).count();
                                debug!("Distributing {} input values to subscribers", present_count);

                                let mut distributed_count = 0;
                                for (i, (res, sender)) in results.iter_mut().zip(senders.values_mut()).enumerate() {
                                    if let Some(res) = res {
                                        debug!("Distributing input value #{}: {:?}", i, res);
                                        sender.send(res.clone()).await;
                                        distributed_count += 1;
                                    }
                                }
                                info!("Distributed {} input values to subscribers", distributed_count);

                                info!("Sending clock tock to signal processing can continue");
                                let tocker = mem::take(&mut clock_tocker)
                                    .expect("Expected tocker to be set")
                                    .expect("Expected tocker to be Some");
                                match tocker.send(()) {
                                    Ok(_) => debug!("Successfully sent clock tock signal"),
                                    Err(e) => warn!("Failed to send clock tock: {:?}", e),
                                }
                                info!("Clock tock sent, input cycle complete");
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

impl<Val: Clone + 'static> InputProvider for MultiplexerClientInputProvider<Val> {
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

    use async_unsync::oneshot;
    use futures::stream;
    use macro_rules_attribute::apply;
    use smol::{LocalExecutor, stream::StreamExt};
    use tracing::info;

    use super::InputProviderMultiplexer;
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

        let mut mux_in: InputProviderMultiplexer<Value> =
            InputProviderMultiplexer::new(ex.clone(), in1, clock_rx);

        info!("Detaching monitor");
        ex.spawn(mux_in.run()).detach();

        info!("Subscribing");
        let mut mux_in_client = mux_in.subscribe().await;

        let mut xs = mux_in_client
            .input_stream(&"x".into())
            .expect("Input stream should exist");

        info!("Tick 1");
        let (tocker_tx, tocker_rx) = oneshot::channel().into_split();
        clock_tx.send(tocker_tx).await.unwrap();
        assert_eq!(xs.next().await, Some(Value::Int(1)));
        tocker_rx.await.unwrap();

        info!("Tick 2");
        let (tocker_tx, tocker_rx) = oneshot::channel().into_split();
        clock_tx.send(tocker_tx).await.unwrap();
        assert_eq!(xs.next().await, Some(Value::Int(2)));
        tocker_rx.await.unwrap();

        info!("Tick 3");
        let (tocker_tx, tocker_rx) = oneshot::channel().into_split();
        clock_tx.send(tocker_tx).await.unwrap();
        assert_eq!(xs.next().await, Some(Value::Int(3)));
        tocker_rx.await.unwrap();

        info!("Tick to close");
        let (tocker_tx, tocker_rx) = oneshot::channel().into_split();
        clock_tx.send(tocker_tx).await.unwrap();
        assert_eq!(xs.next().await, None);
        assert!(tocker_rx.await.is_err());
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

        let mut mux_in: InputProviderMultiplexer<Value> =
            InputProviderMultiplexer::new(ex.clone(), in1, clock_rx);

        info!("Detaching monitor");
        ex.spawn(mux_in.run()).detach();

        info!("Subscribing");
        let mut mux_in_client1 = mux_in.subscribe().await;
        let mut mux_in_client2 = mux_in.subscribe().await;

        let mut xs1 = mux_in_client1
            .input_stream(&"x".into())
            .expect("Input stream should exist");
        let mut xs2 = mux_in_client2
            .input_stream(&"x".into())
            .expect("Input stream should exist");

        info!("Tick 1");
        let (tocker_tx, tocker_rx) = oneshot::channel().into_split();
        clock_tx.send(tocker_tx).await.unwrap();
        assert_eq!(xs1.next().await, Some(Value::Int(1)));
        assert_eq!(xs2.next().await, Some(Value::Int(1)));
        tocker_rx.await.unwrap();

        info!("Tick 2");
        let (tocker_tx, tocker_rx) = oneshot::channel().into_split();
        clock_tx.send(tocker_tx).await.unwrap();
        assert_eq!(xs1.next().await, Some(Value::Int(2)));
        assert_eq!(xs2.next().await, Some(Value::Int(2)));
        tocker_rx.await.unwrap();

        info!("Tick 3");
        let (tocker_tx, tocker_rx) = oneshot::channel().into_split();
        clock_tx.send(tocker_tx).await.unwrap();
        assert_eq!(xs1.next().await, Some(Value::Int(3)));
        assert_eq!(xs2.next().await, Some(Value::Int(3)));
        tocker_rx.await.unwrap();

        info!("Tick to close");
        let (tocker_tx, tocker_rx) = oneshot::channel().into_split();
        clock_tx.send(tocker_tx).await.unwrap();
        assert_eq!(xs1.next().await, None);
        assert_eq!(xs2.next().await, None);
        assert!(tocker_rx.await.is_err());
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

        let mut mux_in: InputProviderMultiplexer<Value> =
            InputProviderMultiplexer::new(ex.clone(), in1, clock_rx);

        info!("Detaching monitor");
        ex.spawn(mux_in.run()).detach();

        {
            info!("Subscribing");
            let mut mux_in_client1 = mux_in.subscribe().await;
            let mut xs = mux_in_client1
                .input_stream(&"x".into())
                .expect("Input stream should exist");

            info!("Tick 1");
            let (tocker_tx, tocker_rx) = oneshot::channel().into_split();
            clock_tx.send(tocker_tx).await.unwrap();
            assert_eq!(xs.next().await, Some(Value::Int(1)));
            tocker_rx.await.unwrap();

            info!("Tick 2");
            let (tocker_tx, tocker_rx) = oneshot::channel().into_split();
            clock_tx.send(tocker_tx).await.unwrap();
            assert_eq!(xs.next().await, Some(Value::Int(2)));
            tocker_rx.await.unwrap();
        }

        {
            let mut mux_in_client2 = mux_in.subscribe().await;
            let mut xs = mux_in_client2
                .input_stream(&"x".into())
                .expect("Input stream should exist");

            info!("Tick 3");
            let (tocker_tx, tocker_rx) = oneshot::channel().into_split();
            clock_tx.send(tocker_tx).await.unwrap();
            assert_eq!(xs.next().await, Some(Value::Int(3)));
            tocker_rx.await.unwrap();

            info!("Tick 4");
            let (tocker_tx, tocker_rx) = oneshot::channel().into_split();
            clock_tx.send(tocker_tx).await.unwrap();
            assert_eq!(xs.next().await, Some(Value::Int(4)));
            tocker_rx.await.unwrap();

            info!("Tick to close");
            let (tocker_tx, tocker_rx) = oneshot::channel().into_split();
            clock_tx.send(tocker_tx).await.unwrap();
            assert_eq!(xs.next().await, None);
            assert!(tocker_rx.await.is_err());
        }
    }
}
