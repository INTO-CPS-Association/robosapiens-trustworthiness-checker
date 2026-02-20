use std::rc::Rc;

use crate::{OutputStream, utils::cancellation_token::DropGuard};
use anyhow::anyhow;
use async_stream::stream;
use async_unsync::oneshot;
use futures::{
    FutureExt, StreamExt,
    stream::{self, LocalBoxStream},
};

/* Converts a `oneshot::Receiver` of an `OutputStream` into an `OutputStream`.
 * Is done by first waiting for the oneshot to resolve to an OutputStream and
 * then continuously yielding the values from the stream. This is implemented
 * using the `flatten_stream` combinator from the `futures` crate, which
 * is essentially a general version of this function (except for handling the
 * case where the oneshot resolves to an error due to the sender going away).
 */
pub fn oneshot_to_stream<T: 'static>(
    receiver: oneshot::Receiver<LocalBoxStream<'static, T>>,
) -> LocalBoxStream<'static, T> {
    let empty_stream = Box::pin(stream::empty());
    Box::pin(
        receiver
            .map(|res| res.unwrap_or(empty_stream))
            .flatten_stream(),
    )
}

/* Wrap a stream in a drop guard to ensure that the associated cancellation
 * token is not dropped before the stream has completed or been dropped.
 * This is used for automatic cleanup of background tasks when all consumers
 * of an output stream have gone away. */
pub fn drop_guard_stream<T: 'static>(
    stream: LocalBoxStream<'static, T>,
    drop_guard: Rc<DropGuard>,
) -> LocalBoxStream<'static, T> {
    use tracing::debug;
    debug!("drop_guard_stream: Creating drop guard wrapper for stream");
    Box::pin(stream! {
        // Keep the shared reference to drop_guard alive until the stream
        // is done
        let _drop_guard = drop_guard.clone();
        debug!("drop_guard_stream: Drop guard acquired in stream, starting iteration");
        let mut stream = stream;
        while let Some(val) = stream.next().await {
            yield val;
        }
        debug!("drop_guard_stream: Stream ended naturally, drop guard will be released");
        // Explicit drop to show when it happens
        drop(_drop_guard);
        debug!("drop_guard_stream: Drop guard explicitly dropped");
    })
}

/// Convert a Receiver to an OutputStream
/// Similar to tokio::ReceiverStream
pub fn channel_to_output_stream<T: 'static>(
    mut receiver: unsync::spsc::Receiver<T>,
) -> OutputStream<T> {
    Box::pin(stream! {
        while let Some(val) = receiver.recv().await {
            yield val;
        }
    })
}

/// Sender that applies ack-based backpressure.
/// If two messages are sent, it will not send message 2 until message 1 has been acked by the
/// receiver.
pub struct SenderWithAck<T> {
    // A sender channel that sends the data and a oneshot sender for the ack back to the receiver.
    sender: unsync::spsc::Sender<(T, unsync::oneshot::Sender<()>)>,
    current_ack: Option<unsync::oneshot::Receiver<()>>,
}

impl<T> SenderWithAck<T> {
    pub fn new(sender: unsync::spsc::Sender<(T, unsync::oneshot::Sender<()>)>) -> Self {
        Self {
            sender,
            current_ack: None,
        }
    }

    pub async fn send(&mut self, data: T) -> anyhow::Result<()> {
        // Wait for the previous ack before sending the next value.
        if let Some(current_ack) = self.current_ack.take() {
            current_ack
                .await
                .ok_or_else(|| anyhow!("Failed to receive ack"))?;
        }

        let (ack_tx, ack_rx) = unsync::oneshot::channel();
        self.sender
            .send((data, ack_tx))
            .await
            .map_err(|e| anyhow!("Failed to send value: {e}"))?;
        self.current_ack = Some(ack_rx);
        Ok(())
    }
}

/// Use this for synchronous (handshaked) communication between async tasks
///
/// Creates an SPSC channel with ack-based backpressure.
///
/// Returns a `(SenderWithAck<T>, OutputStream<T>)` where the sender won’t send message (n+1)
/// until message \(n\) has been acked by the receiver.
///
/// Use this for synchronous (handshaked) communication between async tasks
///
/// Deadlock note: sending twice will block until the first message is acked.
pub fn channel_with_ack<T>(capacity: usize) -> (SenderWithAck<T>, OutputStream<T>)
where
    T: 'static,
{
    type SpscSender<T> = unsync::spsc::Sender<T>;
    type SpscReceiver<T> = unsync::spsc::Receiver<T>;
    type AckSender = unsync::oneshot::Sender<()>;

    let (sender, mut receiver): (SpscSender<(T, AckSender)>, SpscReceiver<(T, AckSender)>) =
        unsync::spsc::channel(capacity);

    let receiver_stream = Box::pin(stream! {
        while let Some(val) = receiver.recv().await {
            let (data, ack_sender) = val;
            // If ack receiver was dropped, stop stream gracefully.
            if ack_sender.send(()).is_err() {
                break;
            }
            yield data;
        }
    });

    let sender_with_ack = SenderWithAck::new(sender);
    (sender_with_ack, receiver_stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::async_test;
    use futures::join;
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::{rc::Rc, time::Duration};

    #[apply(async_test)]
    async fn channel_with_ack_sends_and_acks(_ex: Rc<LocalExecutor<'static>>) {
        // Tests that values sent through the channel are received correctly, and that the sender
        // receives an ack for each value.

        let values = [1, 2, 3, 4];
        let (mut sender, mut receiver) = channel_with_ack::<i32>(1);

        for val in values {
            let send_task = sender.send(val);
            let receive_task = receiver.next();

            // In this context we need join to avoid deadlock, but if we generally want to
            // communicate synchronously between two tasks then this won't be a problem.
            let (send_res, recv_res) = join!(send_task, receive_task);

            assert_eq!(send_res.expect("Send failed"), ());
            assert_eq!(recv_res, Some(val));
        }

        // Receiver ends correctly:
        drop(sender);
        assert_eq!(receiver.next().await, None);
    }

    #[apply(async_test)]
    async fn dropping_receiver_makes_send_error(_ex: Rc<LocalExecutor<'static>>) {
        // Tests that if the receiver is dropped, then the sender will get an error when trying to
        // send a value.

        let (mut sender, receiver) = channel_with_ack::<i32>(1);

        // Drop the stream side; no one will ever ack.
        drop(receiver);

        // Send should fail because the ack can't be received (ack sender will be dropped).
        let err = sender.send(42).await;
        assert!(err.is_err());
    }

    #[apply(async_test)]
    async fn dropping_sender_ends_stream_gracefully(_ex: Rc<LocalExecutor<'static>>) {
        // Tests that if the sender is dropped, then the receiver sees end-of-stream (None).

        let (sender, mut receiver) = channel_with_ack::<i32>(1);

        // Drop the only sender; this should eventually close the channel.
        drop(sender);

        // Receiver should observe end-of-stream (None).
        assert_eq!(receiver.next().await, None);
    }

    #[apply(async_test)]
    async fn channel_with_ack_backpressure_blocks_second_send(ex: Rc<LocalExecutor<'static>>) {
        let (sender, mut receiver) = channel_with_ack::<i32>(1);

        let send_twice_task = async {
            let mut sender = sender;
            sender.send(1).await?;
            sender.send(2).await
        };

        let send_twice = ex.spawn(send_twice_task);
        // Give it time to potentially finish:
        smol::Timer::after(Duration::from_millis(10)).await;
        assert!(
            !send_twice.is_finished(),
            "Second send should be blocked waiting for ack"
        );
        let res = receiver.next().await.expect("Should receive first value");
        assert_eq!(res, 1);
        send_twice
            .await
            .expect("Second send should complete after ack");
    }
}
