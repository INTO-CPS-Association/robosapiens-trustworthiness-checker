use crate::{
    InputProvider, OutputStream, VarName,
    semantics::AsyncConfig,
    stream_utils::{self, SenderWithAck},
};
use async_stream::stream;
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use std::collections::{BTreeMap, BTreeSet};
use tracing::debug;
use unsync::spsc::Sender as SpscSender;

const CHANNEL_SIZE: usize = 10;

struct Channel<AC: AsyncConfig> {
    sender: Option<SpscSender<AC::Val>>, // Data sent from user to receiver
    receiver: Option<OutputStream<AC::Val>>,
}

pub struct ManualInputProvider<AC: AsyncConfig> {
    map: BTreeMap<VarName, Channel<AC>>,
    senders: Option<BTreeMap<VarName, SenderWithAck<()>>>, // Control tick'er used within
                                                           // control_stream
}

impl<AC: AsyncConfig> ManualInputProvider<AC> {
    // This InputProvider is useful for cases like reconfiguration, where a parent RT needs to
    // instantiate and control when inputs are given to an inner RT.
    // In this case, the parent RT has a normal InputProvider, and the inner RT receives
    // ManualInputProvider.
    //
    // On top of the regular InputProvider interface, it needs to have a sender channel for pushing
    // values.
    pub fn new(input_vars: Vec<VarName>) -> Self {
        let mut map = BTreeMap::new();
        let mut senders = BTreeMap::new();
        for name in input_vars.into_iter() {
            let (tx, mut rx) = unsync::spsc::channel(CHANNEL_SIZE);
            let (ctrl_tx, mut ctrl_rx) = stream_utils::channel_with_ack(CHANNEL_SIZE);
            let var_stream: OutputStream<AC::Val> = Box::pin(stream! {
            while let Some(_) = ctrl_rx.next().await {
                if let Some(val) = rx.recv().await {
                    yield val;
                } else {
                    return;
                }
            }});
            map.insert(
                name.clone(),
                Channel {
                    sender: Some(tx),
                    receiver: Some(var_stream),
                },
            );
            senders.insert(name, ctrl_tx);
        }
        Self {
            map,
            senders: Some(senders),
        }
    }

    pub fn sender_channel(&mut self, var: &VarName) -> Option<SpscSender<AC::Val>> {
        self.map.get_mut(var)?.sender.take()
    }
}

#[async_trait(?Send)]
impl<AC: AsyncConfig> InputProvider for ManualInputProvider<AC> {
    type Val = AC::Val;
    fn var_stream(&mut self, var: &VarName) -> Option<OutputStream<Self::Val>> {
        self.map
            .get_mut(var)
            .and_then(|channel| channel.receiver.take())
    }

    async fn control_stream(&mut self) -> OutputStream<anyhow::Result<()>> {
        let mut ctrl_tx = self
            .senders
            .take()
            .expect("control stream can only be taken once");
        // Set of those streams that have been taken with var(),
        // otherwise we deadlock waiting for acks from those that are not taken.
        let taken = self
            .map
            .iter()
            .filter_map(|(name, stream)| stream.receiver.is_none().then(|| name.clone()))
            .collect::<BTreeSet<_>>();
        ctrl_tx.retain(|name, _| taken.contains(name));

        Box::pin(stream! {
            loop {
                // Must be in scope to ensure only one mutable borrow
                let dead = {
                    let mut futs = FuturesUnordered::new();

                    // Create sender tasks
                    for (name, sender) in &mut ctrl_tx {
                        let task = sender.send(()).map(|res| (name.clone(), res));
                        futs.push(task);
                    }

                    let mut dead = Vec::new();
                    // Futs returns None when empty - does not indicate tasks result
                    while let Some((name, res)) = futs.next().await {
                        if let Err(e) = res {
                            // Not an error, most likely because the channel is done
                            debug!("Failed to send tick to var stream {}: {}", name, e);
                            dead.push(name);
                        }
                    }
                    dead
                };

                for name in dead {
                    ctrl_tx.remove(&name);
                }

                yield Ok(());
                if ctrl_tx.is_empty() {
                    return;
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        InputProvider, Value, async_test, io::testing::ManualInputProvider,
        dsrv_fixtures::TestConfig,
    };
    use futures::{FutureExt, StreamExt};
    use macro_rules_attribute::apply;
    use smol::{LocalExecutor, Timer};
    use std::{rc::Rc, time::Duration};
    use tc_testutils::streams::with_timeout;

    #[apply(async_test)]
    async fn var_stream_availability(_ex: Rc<LocalExecutor<'static>>) {
        let data = vec!["x".into()];
        let mut provider = ManualInputProvider::<TestConfig>::new(data);

        // Available on first call
        assert!(provider.var_stream(&"x".into()).is_some());
        // Again - now unavailable
        assert!(provider.var_stream(&"x".into()).is_none());
    }

    #[apply(async_test)]
    async fn var_stream_no_progress(_ex: Rc<LocalExecutor<'static>>) {
        let xs = vec![Value::Int(1), Value::Int(2)];
        let ys = vec![Value::Int(3), Value::Int(4)];
        let mut x_iter = xs.into_iter();
        let mut y_iter = ys.into_iter();
        let mut provider = ManualInputProvider::<TestConfig>::new(vec!["x".into(), "y".into()]);

        let mut x_stream = provider
            .var_stream(&"x".into())
            .expect("x stream should be available");
        let mut y_stream = provider
            .var_stream(&"y".into())
            .expect("y stream should be available");

        let mut x_sender = provider
            .sender_channel(&"x".into())
            .expect("x sender should exist");
        let mut y_sender = provider
            .sender_channel(&"y".into())
            .expect("y sender should exist");
        let _ = x_sender
            .send(x_iter.next().unwrap())
            .await
            .expect("x_sender should be able to send");
        let _ = y_sender
            .send(y_iter.next().unwrap())
            .await
            .expect("y_sender should be able to send");

        // Wait 5 ms - to make sure that stream has progressed if it was capable of doing so:
        // (Because control_stream has not been ticked)
        Timer::after(Duration::from_millis(5)).await;
        assert_eq!(x_stream.next().now_or_never(), None);
        assert_eq!(y_stream.next().now_or_never(), None);

        // Taking the control stream should not cause the var streams to progress on their own:
        let _control_stream = provider.control_stream().await;
        // Wait 5 ms - to make sure that stream has progressed if it was capable of doing so:
        Timer::after(Duration::from_millis(5)).await;
        assert_eq!(x_stream.next().now_or_never(), None);
        assert_eq!(y_stream.next().now_or_never(), None);
    }

    #[apply(async_test)]
    async fn var_stream_progress(_ex: Rc<LocalExecutor<'static>>) {
        let xs = vec![Value::Int(1), Value::Int(2)];
        let ys = vec![Value::Int(3), Value::Int(4)];
        let mut x_iter = xs.into_iter();
        let mut y_iter = ys.into_iter();
        let mut provider = ManualInputProvider::<TestConfig>::new(vec!["x".into(), "y".into()]);

        let mut x_stream = provider
            .var_stream(&"x".into())
            .expect("x stream should be available");
        let mut y_stream = provider
            .var_stream(&"y".into())
            .expect("y stream should be available");

        let mut x_sender = provider
            .sender_channel(&"x".into())
            .expect("x sender should exist");
        let mut y_sender = provider
            .sender_channel(&"y".into())
            .expect("y sender should exist");

        let mut control_stream = provider.control_stream().await;

        let _ = x_sender
            .send(x_iter.next().unwrap())
            .await
            .expect("x_sender should be able to send");
        let _ = y_sender
            .send(y_iter.next().unwrap())
            .await
            .expect("y_sender should be able to send");
        let _ = control_stream
            .next()
            .await
            .expect("control stream should yield Ok");

        let x_res = with_timeout(x_stream.next(), 1, "x_stream.next()")
            .await
            .expect("x stream should yield a value");
        let y_res = with_timeout(y_stream.next(), 1, "y_stream.next()")
            .await
            .expect("y stream should yield a value");

        assert_eq!(x_res, Some(1.into()));
        assert_eq!(y_res, Some(3.into()));

        // Wait 5 ms - to make sure that stream has progressed if it was capable of doing so:
        Timer::after(Duration::from_millis(5)).await;
        assert_eq!(x_stream.next().now_or_never(), None);
        assert_eq!(y_stream.next().now_or_never(), None);

        // Release more data:
        let _ = x_sender
            .send(x_iter.next().unwrap())
            .await
            .expect("x_sender should be able to send");
        let _ = y_sender
            .send(y_iter.next().unwrap())
            .await
            .expect("y_sender should be able to send");
        let _ = control_stream
            .next()
            .await
            .expect("control stream should yield Ok");
        let x_res = with_timeout(x_stream.next(), 1, "x_stream.next()")
            .await
            .expect("x stream should yield a value");
        let y_res = with_timeout(y_stream.next(), 1, "y_stream.next()")
            .await
            .expect("y stream should yield a value");

        assert_eq!(x_res, Some(2.into()));
        assert_eq!(y_res, Some(4.into()));

        // Wait 5 ms - to make sure that stream has progressed if it was capable of doing so:
        Timer::after(Duration::from_millis(5)).await;
        assert_eq!(x_stream.next().now_or_never(), None);
        assert_eq!(y_stream.next().now_or_never(), None);

        // Drop senders as they are empty:
        std::mem::drop(x_sender);
        std::mem::drop(y_sender);

        // Release more data:
        let _ = control_stream
            .next()
            .await
            .expect("control stream should yield Ok");
        let x_res = with_timeout(x_stream.next(), 1, "x_stream.next()")
            .await
            .expect("x stream should yield a value");
        let y_res = with_timeout(y_stream.next(), 1, "y_stream.next()")
            .await
            .expect("y stream should yield a value");

        // Both are exhausted:
        assert_eq!(x_res, None);
        assert_eq!(y_res, None);
    }

    #[apply(async_test)]
    async fn var_stream_progress_different_len(_ex: Rc<LocalExecutor<'static>>) {
        let xs = vec![Value::Int(1), Value::Int(2)];
        let mut x_iter = xs.into_iter();
        let _ys: Vec<Value> = vec![]; // ys are empty..
        let mut provider = ManualInputProvider::<TestConfig>::new(vec!["x".into(), "y".into()]);

        let mut x_stream = provider
            .var_stream(&"x".into())
            .expect("x stream should be available");
        let mut y_stream = provider
            .var_stream(&"y".into())
            .expect("y stream should be available");

        let mut x_sender = provider
            .sender_channel(&"x".into())
            .expect("x sender should exist");
        let y_sender = provider
            .sender_channel(&"y".into())
            .expect("y sender should exist");

        let mut control_stream = provider.control_stream().await;

        std::mem::drop(y_sender); // Drop y to indicate it is done
        let _ = x_sender
            .send(x_iter.next().unwrap())
            .await
            .expect("x_sender should be able to send");
        let _ = control_stream
            .next()
            .await
            .expect("control stream should yield Ok");

        let x_res = with_timeout(x_stream.next(), 1, "x_stream.next()")
            .await
            .expect("x stream should yield a value");
        let y_res = with_timeout(y_stream.next(), 1, "y_stream.next()")
            .await
            .expect("y stream should yield a value");

        assert_eq!(x_res, Some(1.into()));
        assert_eq!(y_res, None);

        // Release more data:
        let _ = x_sender
            .send(x_iter.next().unwrap())
            .await
            .expect("x_sender should be able to send");
        let _ = control_stream
            .next()
            .await
            .expect("control stream should yield Ok");
        let x_res = with_timeout(x_stream.next(), 1, "x_stream.next()")
            .await
            .expect("x stream should yield a value");
        let y_res = with_timeout(y_stream.next(), 1, "y_stream.next()")
            .await
            .expect("y stream should yield a value");

        assert_eq!(x_res, Some(2.into()));
        assert_eq!(y_res, None);

        // Drop x as well:
        std::mem::drop(x_sender);

        // Release more data:
        let _ = control_stream
            .next()
            .await
            .expect("control stream should yield Ok");
        let x_res = with_timeout(x_stream.next(), 1, "x_stream.next()")
            .await
            .expect("x stream should yield a value");
        let y_res = with_timeout(y_stream.next(), 1, "y_stream.next()")
            .await
            .expect("y stream should yield a value");

        // Both are exhausted:
        assert_eq!(x_res, None);
        assert_eq!(y_res, None);
    }

    #[apply(async_test)]
    async fn var_stream_large_regression(_ex: Rc<LocalExecutor<'static>>) {
        // Test that checks that ManualInputProvider can handle a large number of ticks without
        // deadlocking or running out of memory.
        // Introduced after regression with runtime test

        const SIZE: usize = 3000;
        let xs: Vec<Value> = (0..SIZE).map(|x| Value::Int(x as i64)).collect();
        let ys: Vec<Value> = (0..SIZE).map(|x| Value::Int(x as i64)).collect();
        let add = if SIZE % 2 == 0 { 0 } else { 1 };
        let es: Vec<Value> = std::iter::repeat(Value::Deferred)
            .take((SIZE / 2) as usize)
            .chain((0..(SIZE / 2) + add).map(|_| Value::Str("x + y".into())))
            .collect();
        let mut x_iter = xs.into_iter();
        let mut y_iter = ys.into_iter();
        let mut e_iter = es.into_iter();
        let mut provider =
            ManualInputProvider::<TestConfig>::new(vec!["x".into(), "y".into(), "e".into()]);

        let mut x_sender = provider
            .sender_channel(&"x".into())
            .expect("x sender should exist");
        let mut y_sender = provider
            .sender_channel(&"y".into())
            .expect("y sender should exist");
        let mut e_sender = provider
            .sender_channel(&"e".into())
            .expect("e sender should exist");

        let mut x_stream = provider
            .var_stream(&"x".into())
            .expect("x stream should be available");
        let mut y_stream = provider
            .var_stream(&"y".into())
            .expect("y stream should be available");
        let mut e_stream = provider
            .var_stream(&"e".into())
            .expect("e stream should be available");

        let mut control_stream = provider.control_stream().await;

        for _ in 0..SIZE {
            let _ = with_timeout(control_stream.next(), 1, "control_stream.next()")
                .await
                .expect("control stream should yield a value");
            let _ = x_sender
                .send(x_iter.next().unwrap())
                .await
                .expect("x_sender should be able to send");
            let _ = with_timeout(x_stream.next(), 1, "x_stream.next()")
                .await
                .expect("x stream should yield a value");
            let _ = y_sender
                .send(y_iter.next().unwrap())
                .await
                .expect("y_sender should be able to send");
            let _ = with_timeout(y_stream.next(), 1, "y_stream.next()")
                .await
                .expect("y stream should yield a value");
            let _ = e_sender
                .send(e_iter.next().unwrap())
                .await
                .expect("e_sender should be able to send");
            let _ = with_timeout(e_stream.next(), 1, "e_stream.next()")
                .await
                .expect("e stream should yield a value");
        }

        let _ = control_stream
            .next()
            .await
            .expect("control stream should yield Ok");

        std::mem::drop(x_sender);
        std::mem::drop(y_sender);
        std::mem::drop(e_sender);
        let x_res = with_timeout(x_stream.next(), 1, "x_stream.next()")
            .await
            .expect("x stream should yield a value");
        let y_res = with_timeout(y_stream.next(), 1, "y_stream.next()")
            .await
            .expect("y stream should yield a value");
        let e_res = with_timeout(e_stream.next(), 1, "e_stream.next()")
            .await
            .expect("e stream should yield a value");

        // All are exhausted:
        assert_eq!(x_res, None);
        assert_eq!(y_res, None);
        assert_eq!(e_res, None);
    }
}
