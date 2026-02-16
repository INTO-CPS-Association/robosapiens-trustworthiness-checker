use crate::{InputProvider, OutputStream, VarName, semantics::AsyncConfig};
use async_stream::stream;
use async_trait::async_trait;
use std::collections::BTreeMap;
use tracing::debug;
use unsync::spsc::Sender as SpscSender;

const CHANNEL_SIZE: usize = 10;

struct Channel<AC: AsyncConfig> {
    sender: Option<SpscSender<AC::Val>>,
    control_sender: Option<SpscSender<()>>,
    receiver: Option<OutputStream<AC::Val>>,
}

pub struct ManualInputProvider<AC: AsyncConfig> {
    vars: BTreeMap<VarName, Channel<AC>>,
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
        let vars: BTreeMap<VarName, Channel<AC>> = input_vars
            .into_iter()
            .map(|v| {
                let (tx, mut rx) = unsync::spsc::channel(CHANNEL_SIZE);
                let (ctrl_tx, mut ctrl_rx) = unsync::spsc::channel(CHANNEL_SIZE);

                let rx: OutputStream<AC::Val> = Box::pin(stream! {
                    while let Some(_) = ctrl_rx.recv().await {
                        if let Some(val) = rx.recv().await {
                            yield val;
                        }
                        else {
                            return;
                        }
                    }
                });
                (
                    v,
                    Channel {
                        sender: Some(tx),
                        control_sender: Some(ctrl_tx),
                        receiver: Some(rx),
                    },
                )
            })
            .collect();

        Self { vars }
    }

    pub fn sender_channel(&mut self, var: &VarName) -> Option<SpscSender<AC::Val>> {
        self.vars.get_mut(var)?.sender.take()
    }
}

#[async_trait(?Send)]
impl<AC: AsyncConfig> InputProvider for ManualInputProvider<AC> {
    type Val = AC::Val;
    fn var_stream(&mut self, var: &VarName) -> Option<OutputStream<Self::Val>> {
        self.vars
            .get_mut(var)
            .and_then(|channel| channel.receiver.take())
    }

    async fn control_stream(&mut self) -> OutputStream<anyhow::Result<()>> {
        let mut ctrl_tx = BTreeMap::new();
        for (var, channel) in self.vars.iter_mut() {
            let sender = channel
                .control_sender
                .take()
                .expect("control stream can only be taken once");
            ctrl_tx.insert(var.clone(), sender);
        }

        Box::pin(stream! {
            loop {
                let mut dead = Vec::new();
                // Send to each sender:
                for (name, sender) in &mut ctrl_tx {
                    debug!("Sending tick to var stream {}", name);
                    if let Err(e) = sender.send(()).await {
                        // Not an error, most likely because the channel is done
                        debug!("Failed to send tick to var stream {}: {}", name, e);
                        dead.push(name.clone());
                    }
                }
                for name in dead {
                    ctrl_tx.remove(&name);
                }

                yield Ok(());
                if ctrl_tx.is_empty() {
                    return;
                }
                // Timer to avoid starvation - has been seen in tests.
                // (smool::future::yield_now() does not do the trick)
                // A better but more complex solution would be to add backpressure from the
                // tick receiver.
                smol::Timer::after(std::time::Duration::from_millis(1)).await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        InputProvider, Value, async_test, io::testing::ManualInputProvider,
        lola_fixtures::TestConfig,
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
    async fn control_stream_without_consuming(_ex: Rc<LocalExecutor<'static>>) {
        // Tests that the InputProvider does not hang if a Runtime does not consume all the
        // var_streams/sender_channels.
        let mut provider = ManualInputProvider::<TestConfig>::new(vec!["x".into(), "y".into()]);
        let mut control_stream = provider.control_stream().await;
        for _ in 1..15 {
            let _ = with_timeout(control_stream.next(), 1, "control_stream.next()")
                .await
                .expect("Control stream should not hang");
        }

        let mut provider = ManualInputProvider::<TestConfig>::new(vec!["x".into(), "y".into()]);
        let _x_stream = provider
            .var_stream(&"x".into())
            .expect("x stream should be available");
        let mut control_stream = provider.control_stream().await;
        for _ in 1..15 {
            let _ = with_timeout(control_stream.next(), 1, "control_stream.next()")
                .await
                .expect("Control stream should not hang");
        }
    }

    #[apply(async_test)]
    async fn var_stream_reverse_ticks(_ex: Rc<LocalExecutor<'static>>) {
        // Checks that if we first send the values through sender channels, and then tick the
        // control stream, the var_streams still yield the values.
        let xs = vec![Value::Int(1)];
        let mut x_iter = xs.into_iter();
        let mut provider = ManualInputProvider::<TestConfig>::new(vec!["x".into()]);

        let mut x_stream = provider
            .var_stream(&"x".into())
            .expect("x stream should be available");
        let mut x_sender = provider
            .sender_channel(&"x".into())
            .expect("x sender should exist");

        let mut control_stream = provider.control_stream().await;

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

        assert_eq!(x_res, Some(1.into()));

        std::mem::drop(x_sender); // Drop sender to indicate it is done
        let _ = control_stream
            .next()
            .await
            .expect("control stream should yield Ok");

        let x_res = with_timeout(x_stream.next(), 1, "x_stream.next()")
            .await
            .expect("x stream should yield a value");

        assert_eq!(x_res, None);
    }
}
