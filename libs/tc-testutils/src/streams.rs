use anyhow::anyhow;
use async_stream::stream;
use futures::{StreamExt, future::join_all};
use futures_timeout::TimeoutExt;
use smol::LocalExecutor;
use std::{rc::Rc, time::Duration};
use unsync::spsc::Sender as SpscSender;

use trustworthiness_checker::OutputStream;

pub type TickSender = SpscSender<()>;

// Helper stream that synchronizes other streams by only progressing when a tick is received
// We need these to control the flow of the OutputStream tests - essentially we are making them
// dependent on in InputStream (but without the dependencies)
pub fn tick_stream<T: 'static>(mut stream: OutputStream<T>) -> (TickSender, OutputStream<T>) {
    let (tick_sender, mut tick_receiver) = unsync::spsc::channel::<()>(10);
    let synced_stream = Box::pin(stream! {
    while let Some(_) = tick_receiver.recv().await {
        if let Some(vals) = stream.next().await {
            yield vals;
        } else {
            return;
        }
    }});
    (tick_sender, synced_stream)
}

// Helper stream that synchronizes multiple streams by only letting them progress
// when a master tick is received.
// Similar to tick_stream but for multiple streams controlled by a single master.
pub fn tick_streams<T: 'static>(
    ex: Rc<LocalExecutor<'static>>,
    streams: Vec<OutputStream<T>>,
) -> (TickSender, Vec<OutputStream<T>>) {
    // Create individually synched streams
    let (mut follower_senders, synced_streams): (Vec<_>, Vec<_>) =
        streams.into_iter().map(|s| tick_stream(s)).unzip();
    // Create basis for single sync stream
    let (leader_sender, mut leader_receiver) = unsync::spsc::channel::<()>(10);
    // Actual single sync stream:
    let synced_stream = Box::pin(stream! {
    while let Some(_) = leader_receiver.recv().await {
        let futs = follower_senders.iter_mut().map(|s: &mut TickSender | s.send(()));
        join_all(futs).await;
    }});
    // Make synced_stream run indefinitely - just waiting and forwarding ticks:
    ex.spawn(async move {
        let mut synced_stream = synced_stream;
        while let Some(_) = synced_stream.next().await {}
    })
    .detach();
    (leader_sender, synced_streams)
}

/// Wrapper to run a future with a timeout and provide a meaningful error message
pub async fn with_timeout<F, T>(fut: F, dur_sec: u64, name: &str) -> anyhow::Result<T>
where
    F: std::future::Future<Output = T>,
{
    let fut = fut.timeout(Duration::from_secs(dur_sec)).await;
    match fut {
        Ok(inner) => Ok(inner),
        Err(_) => Err(anyhow!("{} timed out after {} seconds", name, dur_sec)),
    }
}

/// Wrapper to run a future that returns a result with a timeout and provide a meaningful error message
pub async fn with_timeout_res<F, T, E>(fut: F, dur_sec: u64, name: &str) -> anyhow::Result<T>
where
    F: std::future::Future<Output = Result<T, E>>,
    E: Into<anyhow::Error>,
{
    let fut = fut.timeout(Duration::from_secs(dur_sec)).await;
    match fut {
        Ok(inner_res) => inner_res.map_err(|e| anyhow!("{} failed: {}", name, e.into())),
        Err(_) => Err(anyhow!("{} timed out after {} seconds", name, dur_sec)),
    }
}

// For each (x, y) pair, produces [x, NoVal, NoVal, y]
// Useful for testing e.g., InputProviders that cannot handle simultaneous inputs
pub fn interleave_with_constant<Iter, ValueType>(
    values: Iter,
    constant: ValueType,
) -> (Vec<ValueType>, Vec<ValueType>)
where
    Iter: IntoIterator<Item = (ValueType, ValueType)>,
    ValueType: Clone,
{
    values
        .into_iter()
        .fold((vec![], vec![]), |(mut x_vals, mut y_vals), (x, y)| {
            x_vals.push(x);
            y_vals.push(constant.clone());
            x_vals.push(constant.clone());
            y_vals.push(y);
            (x_vals, y_vals)
        })
}

/// Receives the expected number of values from the x and y subscription streams,
/// as if x and y was published simultaneously.
pub async fn receive_values_serially<ValueType>(
    x_tick: &mut TickSender,
    y_tick: &mut TickSender,
    mut x_sub_stream: OutputStream<ValueType>,
    mut y_sub_stream: OutputStream<ValueType>,
    stream_len: usize,
) -> anyhow::Result<(Vec<ValueType>, Vec<ValueType>)> {
    // Send one x_tick, wait for response. Send one y_tick, wait for response.
    let (mut x_vals, mut y_vals): (Vec<ValueType>, Vec<ValueType>) = (vec![], vec![]);
    for _ in 0..stream_len {
        x_tick.send(()).await?;
        let x_val = with_timeout(x_sub_stream.next(), 3, "x_sub_stream.next").await;
        let y_val = with_timeout(y_sub_stream.next(), 3, "y_sub_stream.next").await;
        x_vals.push(x_val?.expect("x_sub_stream ended"));
        y_vals.push(y_val?.expect("y_sub_stream ended"));

        y_tick.send(()).await?;
        let x_val = with_timeout(x_sub_stream.next(), 3, "x_sub_stream.next").await;
        let y_val = with_timeout(y_sub_stream.next(), 3, "y_sub_stream.next").await;
        x_vals.push(x_val?.expect("x_sub_stream ended"));
        y_vals.push(y_val?.expect("y_sub_stream ended"));
    }
    Ok((x_vals, y_vals))
}

#[cfg(test)]
mod tests {
    use super::{tick_stream, tick_streams};
    use futures::stream;
    use futures::{FutureExt, StreamExt};
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use smol::Timer;
    use std::rc::Rc;
    use std::time::Duration;

    use trustworthiness_checker::{OutputStream, Value, async_test, core::VarName};

    fn gen_data_streams(n: i64) -> (Vec<VarName>, Vec<OutputStream<Value>>, Vec<Vec<Value>>) {
        let x_stream: OutputStream<Value> = Box::pin(stream::iter((0..n).map(|x| (x * 2).into())));
        let y_stream: OutputStream<Value> =
            Box::pin(stream::iter((0..n).map(|x| (x * 2 + 1).into())));
        let stream_names = vec!["x".into(), "y".into()];
        let streams = vec![x_stream, y_stream];
        let expected = (0..n)
            .map(|x| vec![(x * 2).into(), (x * 2 + 1).into()])
            .collect::<Vec<Vec<Value>>>();
        (stream_names, streams, expected)
    }

    fn gen_default_streams() -> (Vec<VarName>, Vec<OutputStream<Value>>, Vec<Vec<Value>>) {
        gen_data_streams(10)
    }

    // Checks that tick_stream behaves as expected
    #[apply(async_test)]
    async fn tick_stream_test(_ex: Rc<LocalExecutor<'static>>) {
        let (_, mut streams, expected) = gen_default_streams();
        let stream = streams.pop().unwrap();
        let expected = expected.into_iter().map(|x| x.last().cloned().unwrap());
        // Converts them to a single tick-synchronized stream
        let (mut tick_sender, mut stream) = tick_stream(stream);
        let mut expected = expected.into_iter();

        // Wait 5 ms - to make sure that stream has progressed if it was capable of doing so
        // before sending a tick:
        Timer::after(Duration::from_millis(5)).await;
        assert_eq!(stream.next().now_or_never(), None);

        // Send ticks and check that the stream progresses
        tick_sender.send(()).await.expect("Failed to send tick");
        assert_eq!(stream.next().await, expected.next());

        // Wait 5 ms - to make sure that stream has progressed if it was capable of doing so
        // before sending a tick:
        Timer::after(Duration::from_millis(5)).await;
        assert_eq!(stream.next().now_or_never(), None);

        // Send ticks and check that the stream progresses
        tick_sender.send(()).await.expect("Failed to send tick");
        assert_eq!(stream.next().await, expected.next());

        // Let it finish:
        while let Some(d) = expected.next() {
            tick_sender.send(()).await.expect("Failed to send tick");
            assert_eq!(stream.next().await, Some(d));
        }

        // Check that the stream is done
        tick_sender.send(()).await.expect("Failed to send tick");
        assert_eq!(stream.next().await, None);
    }

    // Checks that tick_streams behaves as expected
    #[apply(async_test)]
    async fn tick_streams_test(ex: Rc<LocalExecutor<'static>>) {
        let (_, streams, expected) = gen_default_streams();
        // Converts them to multiple streams synchronized by a single tick stream
        let (mut tick_sender, mut streams) = tick_streams(ex, streams);
        let mut expected = expected.into_iter();

        // Wait 5 ms - to make sure that streams have progressed if they were capable of doing so
        // before sending a tick:
        Timer::after(Duration::from_millis(5)).await;
        streams.iter_mut().for_each(|s| {
            assert_eq!(s.next().now_or_never(), None);
        });

        // Send tick and check that the streams progress
        tick_sender.send(()).await.expect("Failed to send tick");
        for (s, d) in streams.iter_mut().zip(expected.next().unwrap()) {
            assert_eq!(s.next().await, Some(d));
        }

        // Wait 5 ms - to make sure that streams have progressed if they were capable of doing so
        // before sending a tick:
        Timer::after(Duration::from_millis(5)).await;
        streams.iter_mut().for_each(|s| {
            assert_eq!(s.next().now_or_never(), None);
        });

        // Send tick and check that the streams progress
        tick_sender.send(()).await.expect("Failed to send tick");
        for (s, d) in streams.iter_mut().zip(expected.next().unwrap()) {
            assert_eq!(s.next().await, Some(d));
        }

        // Let them finish:
        while let Some(ds) = expected.next() {
            tick_sender.send(()).await.expect("Failed to send tick");
            for (s, d) in streams.iter_mut().zip(ds) {
                assert_eq!(s.next().await, Some(d));
            }
        }

        // Check that the streams are done
        tick_sender.send(()).await.expect("Failed to send tick");
        streams.iter_mut().for_each(|s| {
            assert_eq!(s.next().now_or_never(), None);
        });
    }
}
