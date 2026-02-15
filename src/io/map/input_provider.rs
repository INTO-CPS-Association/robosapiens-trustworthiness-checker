use std::collections::BTreeMap;

use async_stream::stream;
use async_trait::async_trait;
use tracing::debug;
use unsync::spsc::Sender as SpscSender;

use crate::{InputProvider, OutputStream, Value, VarName};

type VarStream = OutputStream<Value>;

// TODO: Should only be enabled for testing and benches, but we have some bench code that is not
// conditionally compiled yet.

pub struct MapInputProvider {
    map: BTreeMap<VarName, Option<VarStream>>,
    senders: Option<BTreeMap<VarName, SpscSender<()>>>,
}

impl MapInputProvider {
    pub fn new(vars: BTreeMap<VarName, Vec<Value>>) -> Self {
        let mut map = BTreeMap::new();
        let mut senders = BTreeMap::new();
        for (name, data) in vars.into_iter() {
            let mut data_iter = data.into_iter();
            let (sender, mut receiver) = unsync::spsc::channel::<()>(1);
            let var_stream: OutputStream<Value> = Box::pin(stream! {
            while let Some(_) = receiver.recv().await {
                if let Some(val) = data_iter.next() {
                    yield val;
                } else {
                    return;
                }
            }});
            map.insert(name.clone(), Some(var_stream));
            senders.insert(name, sender);
        }
        Self {
            map,
            senders: Some(senders),
        }
    }
}

#[async_trait(?Send)]
impl InputProvider for MapInputProvider {
    type Val = Value;

    // We are consuming the input stream from the map when
    // we return it to ensure single ownership and static lifetime
    fn var_stream(&mut self, var: &VarName) -> Option<OutputStream<Self::Val>> {
        // Return the VarStream if it exists
        self.map.get_mut(var)?.take()
    }

    async fn control_stream(&mut self) -> OutputStream<anyhow::Result<()>> {
        let mut senders = self
            .senders
            .take()
            .expect("control stream can only be taken once");

        Box::pin(stream! {
            loop {
                let mut dead = Vec::new();
                // Send to each sender:
                for (name, sender) in &mut senders {
                    debug!("Sending tick to var stream {}", name);
                    if let Err(e) = sender.send(()).await {
                        debug!("Failed to send tick to var stream {}: {}", name, e);
                        dead.push(name.clone());
                    }
                }
                for name in dead {
                    senders.remove(&name);
                }

                yield Ok(());
                if senders.is_empty() {
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
    use std::{rc::Rc, time::Duration};

    use crate::async_test;

    use super::*;
    use futures::{FutureExt, StreamExt};
    use macro_rules_attribute::apply;
    use smol::{LocalExecutor, Timer};
    use tc_testutils::streams::with_timeout;

    #[apply(async_test)]
    async fn var_stream_availability(_ex: Rc<LocalExecutor<'static>>) {
        let data = BTreeMap::from([("x".into(), vec![1.into()])]);
        let mut provider = MapInputProvider::new(data);

        // Available on first call
        assert!(provider.var_stream(&"x".into()).is_some());
        // Again - now unavailable
        assert!(provider.var_stream(&"x".into()).is_none());
    }

    #[apply(async_test)]
    async fn var_stream_no_progress(_ex: Rc<LocalExecutor<'static>>) {
        let data = BTreeMap::from([
            ("x".into(), vec![1.into(), 2.into()]),
            ("y".into(), vec![3.into(), 4.into()]),
        ]);
        let mut provider = MapInputProvider::new(data);

        let mut x_stream = provider
            .var_stream(&"x".into())
            .expect("x stream should be available");
        let mut y_stream = provider
            .var_stream(&"y".into())
            .expect("y stream should be available");

        // Wait 5 ms - to make sure that stream has progressed if it was capable of doing so:
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
        let data = BTreeMap::from([
            ("x".into(), vec![1.into(), 2.into()]),
            ("y".into(), vec![3.into(), 4.into()]),
        ]);
        let mut provider = MapInputProvider::new(data);

        let mut x_stream = provider
            .var_stream(&"x".into())
            .expect("x stream should be available");
        let mut y_stream = provider
            .var_stream(&"y".into())
            .expect("y stream should be available");

        let mut control_stream = provider.control_stream().await;

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
        let data = BTreeMap::from([("x".into(), vec![1.into(), 2.into()]), ("y".into(), vec![])]);
        let mut provider = MapInputProvider::new(data);

        let mut x_stream = provider
            .var_stream(&"x".into())
            .expect("x stream should be available");
        let mut y_stream = provider
            .var_stream(&"y".into())
            .expect("y stream should be available");

        let mut control_stream = provider.control_stream().await;

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

        assert_eq!(x_res, None);
        assert_eq!(y_res, None);
    }

    #[apply(async_test)]
    async fn control_stream_without_consuming(_ex: Rc<LocalExecutor<'static>>) {
        // Tests that the InputProvider does not hang if a Runtime does not consume all the
        // var_streams.
        let data = BTreeMap::from([("x".into(), (1..15).map(Value::Int).collect())]);
        let mut provider = MapInputProvider::new(data.clone());
        let mut control_stream = provider.control_stream().await;
        for _ in 1..15 {
            let _ = with_timeout(control_stream.next(), 1, "control_stream.next()")
                .await
                .expect("Control stream should not hang");
        }

        let mut provider = MapInputProvider::new(data);
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
}
