use std::collections::{BTreeMap, BTreeSet};

use async_stream::stream;
use async_trait::async_trait;
use futures::FutureExt;
use futures::stream::FuturesUnordered;
use smol::stream::StreamExt;
use tracing::debug;

use crate::{
    InputProvider, OutputStream, Value, VarName,
    stream_utils::{self, SenderWithAck},
};

// TODO: Should only be enabled for testing and benches, but we have some bench code that is not
// conditionally compiled yet.

pub struct MapInputProvider {
    map: BTreeMap<VarName, Option<OutputStream<Value>>>,
    // Use ack channels to implement control_stream for legacy reasons.
    // This enables using `control_stream` in Runtimes with the old `run` behavior without starving
    // the system.
    senders: Option<BTreeMap<VarName, SenderWithAck<()>>>,
}

impl MapInputProvider {
    pub fn new(vars: BTreeMap<VarName, Vec<Value>>) -> Self {
        let mut map = BTreeMap::new();
        let mut senders = BTreeMap::new();
        for (name, data) in vars.into_iter() {
            let mut data_iter = data.into_iter();
            let (sender, mut receiver) = stream_utils::channel_with_ack(10);
            let var_stream: OutputStream<Value> = Box::pin(stream! {
            while let Some(_) = receiver.next().await {
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
        // Set of those streams that have been taken with var(),
        // otherwise we deadlock waiting for acks from those that are not taken.
        let taken = self
            .map
            .iter()
            .filter_map(|(name, stream)| stream.is_none().then(|| name.clone()))
            .collect::<BTreeSet<_>>();
        senders.retain(|name, _| taken.contains(name));

        Box::pin(stream! {
            loop {
                // Must be in scope to ensure only one mutable borrow
                let dead = {
                    let mut futs = FuturesUnordered::new();

                    // Create sender tasks
                    for (name, sender) in &mut senders {
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
                    senders.remove(&name);
                }

                yield Ok(());
                if senders.is_empty() {
                    return;
                }
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
}
