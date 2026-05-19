use std::cell::{Cell, RefCell};
use std::rc::Rc;

use crate::{OutputStream, utils::cancellation_token::DropGuard};
use anyhow::anyhow;
use async_stream::stream;
use async_unsync::{bounded, oneshot};
use event_listener::Event;
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

/// A fan-out that broadcasts every value pushed through a [`FanoutSender`]
/// to all live subscribers.  Dead subscribers (whose receivers have been
/// dropped) are pruned automatically.
///
/// Created via [`Fanout::new`], which returns a `(FanoutSender<T>, Rc<Fanout<T>>)`
/// pair.  Each call to [`subscribe`] on the `Fanout` returns a
/// `bounded::Receiver` that will receive a clone of every subsequent value
/// sent through the sender.
///
/// The fan-out is driven inline by [`FanoutSender::send`]. Dropping the
/// `FanoutSender` clears all subscribers, causing their receivers to return `None`.
pub struct Fanout<T> {
    subs: RefCell<Vec<bounded::Sender<T>>>,
    change_event: Event,
    sub_events: Cell<u64>,
    prune_events: Cell<u64>,
    any_events: Cell<u64>,
}

/// The sending half of a [`Fanout`].  Each call to [`send`] distributes a
/// value to every live subscriber.
///
/// Dropping the sender signals end-of-stream: all subscriber senders are
/// dropped and their receivers will return `None`.
pub struct FanoutSender<T> {
    fanout: Rc<Fanout<T>>,
}

impl<T: Clone + 'static> Fanout<T> {
    /// Capacity used for each subscriber's internal channel.
    const SUB_CAPACITY: usize = 1024;

    /// Create a new fan-out channel.  Returns a sender for pushing values
    /// and an `Rc<Fanout<T>>` used to create subscribers via [`subscribe`].
    pub fn new() -> (FanoutSender<T>, Rc<Self>) {
        let fanout = Rc::new(Self {
            subs: RefCell::new(Vec::new()),
            change_event: Event::new(),
            sub_events: Cell::new(0),
            prune_events: Cell::new(0),
            any_events: Cell::new(0),
        });
        let sender = FanoutSender {
            fanout: Rc::clone(&fanout),
        };
        (sender, fanout)
    }

    /// Register a new subscriber and send event signal.  Returns a `bounded::Receiver` that will
    /// receive a clone of every value sent through the [`FanoutSender`]
    /// after this call.
    pub fn subscribe(self: &Rc<Self>) -> bounded::Receiver<T> {
        let (tx, rx) = bounded::channel::<T>(Self::SUB_CAPACITY).into_split();
        self.subs.borrow_mut().push(tx);

        self.sub_events.set(self.sub_events.get().wrapping_add(1));
        self.any_events.set(self.any_events.get().wrapping_add(1));
        self.change_event.notify(usize::MAX);

        rx
    }

    /// Wait for at least one new subscriber to be observed
    pub async fn wait_for_sub_event(&self, seen: u64) -> u64 {
        self.wait_for_event(seen, |this| this.sub_events.get())
            .await
    }

    /// Wait until at least one dead subscriber has been observed and pruned
    pub async fn wait_for_prune_event(&self, seen: u64) -> u64 {
        self.wait_for_event(seen, |this| this.prune_events.get())
            .await
    }

    /// Wait until at least one event of any kind (new subscriber or prune) has been observed
    pub async fn wait_for_any_event(&self, seen: u64) -> u64 {
        self.wait_for_event(seen, |this| this.any_events.get())
            .await
    }

    /// Helper function to wait for an event counter to change
    async fn wait_for_event(&self, seen: u64, getter: impl Fn(&Self) -> u64) -> u64 {
        loop {
            let current = getter(self);
            if current != seen {
                return current;
            }

            let listener = self.change_event.listen();

            let current = getter(self);
            if current != seen {
                return current;
            }

            listener.await;
        }
    }

    pub fn sub_events(&self) -> u64 {
        self.sub_events.get()
    }

    pub fn prune_events(&self) -> u64 {
        self.prune_events.get()
    }

    pub fn any_events(&self) -> u64 {
        self.any_events.get()
    }
}

impl<T: Clone + 'static> FanoutSender<T> {
    /// Send a value to all current subscribers.  Dead subscribers are
    /// pruned automatically.  If no subscribers exist the value is
    /// silently dropped.
    pub async fn send(&self, msg: T) {
        let subs = self.fanout.subs.borrow();
        let snapshot: Vec<_> = subs.iter().cloned().collect();
        drop(subs);

        let snapshot_len = snapshot.len();
        let mut alive = Vec::with_capacity(snapshot_len);
        let mut removed_any = false;

        for sub in snapshot {
            if sub.send(msg.clone()).await.is_ok() {
                alive.push(sub);
            } else {
                removed_any = true;
            }
        }

        let mut subs = self.fanout.subs.borrow_mut();
        // Preserve subscribers that were added after our snapshot was taken
        // (e.g. via `subscribe` called from another task while we were yielding
        // at `sub.send().await`).  Without this, newly-added subscribers would be
        // silently lost.
        let drain_start = snapshot_len.min(subs.len());
        alive.extend(subs.drain(drain_start..));
        *subs = alive;
        drop(subs);

        if removed_any {
            self.fanout
                .prune_events
                .set(self.fanout.prune_events.get().wrapping_add(1));
            self.fanout
                .any_events
                .set(self.fanout.any_events.get().wrapping_add(1));
            self.fanout.change_event.notify(usize::MAX);
        }
    }

    pub fn fanout(&self) -> Rc<Fanout<T>> {
        Rc::clone(&self.fanout)
    }
}

impl<T> Drop for FanoutSender<T> {
    // Note: This implementation of Drop only works when FanoutSender is the only owner of the
    // fanout. I.e., FanoutSender is not Clone.
    fn drop(&mut self) {
        let had_subs = !self.fanout.subs.borrow().is_empty();
        self.fanout.subs.borrow_mut().clear();

        if had_subs {
            self.fanout
                .prune_events
                .set(self.fanout.prune_events.get().wrapping_add(1));
            self.fanout
                .any_events
                .set(self.fanout.any_events.get().wrapping_add(1));
            self.fanout.change_event.notify(usize::MAX);
        }
    }
}

impl<T> std::fmt::Debug for Fanout<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fanout")
            .field("sub_count", &self.subs.borrow().len())
            .finish()
    }
}

impl<T> std::fmt::Debug for FanoutSender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FanoutSender")
            .field("sub_count", &self.fanout.subs.borrow().len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::async_test;
    use futures::join;
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::{rc::Rc, time::Duration};
    use tc_testutils::streams::with_timeout;

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

    #[apply(async_test)]
    async fn fanout_single_subscriber(_ex: Rc<LocalExecutor<'static>>) {
        let (tx, fanout) = Fanout::new();
        let mut sub = fanout.subscribe();

        tx.send(1).await;
        assert_eq!(with_timeout(sub.recv(), 1, "r1").await.unwrap(), Some(1));

        tx.send(2).await;
        assert_eq!(with_timeout(sub.recv(), 1, "r2").await.unwrap(), Some(2));

        drop(tx);
        assert_eq!(sub.recv().await, None);
    }

    #[apply(async_test)]
    async fn fanout_multiple_subscribers(_ex: Rc<LocalExecutor<'static>>) {
        let (tx, fanout) = Fanout::new();
        let mut sub1 = fanout.subscribe();
        let mut sub2 = fanout.subscribe();

        tx.send(10).await;
        assert_eq!(with_timeout(sub1.recv(), 1, "s1").await.unwrap(), Some(10));
        assert_eq!(with_timeout(sub2.recv(), 1, "s2").await.unwrap(), Some(10));

        tx.send(20).await;
        assert_eq!(
            with_timeout(sub1.recv(), 1, "s1_2").await.unwrap(),
            Some(20)
        );
        assert_eq!(
            with_timeout(sub2.recv(), 1, "s2_2").await.unwrap(),
            Some(20)
        );
    }

    #[apply(async_test)]
    async fn fanout_late_subscriber(_ex: Rc<LocalExecutor<'static>>) {
        let (tx, fanout) = Fanout::new();
        let mut sub1 = fanout.subscribe();

        tx.send(1).await;
        assert_eq!(with_timeout(sub1.recv(), 1, "s1").await.unwrap(), Some(1));

        // Late subscriber only sees subsequent messages
        let mut sub2 = fanout.subscribe();
        tx.send(2).await;
        assert_eq!(with_timeout(sub1.recv(), 1, "s1_2").await.unwrap(), Some(2));
        assert_eq!(with_timeout(sub2.recv(), 1, "s2_2").await.unwrap(), Some(2));
    }

    #[apply(async_test)]
    async fn fanout_subscriber_drop_prunes_dead(_ex: Rc<LocalExecutor<'static>>) {
        let (tx, fanout) = Fanout::new();
        let mut sub1 = fanout.subscribe();
        let sub2 = fanout.subscribe();

        tx.send(1).await;
        with_timeout(sub1.recv(), 1, "s1").await.unwrap();

        // Drop sub2 — its sender should be pruned on next send
        drop(sub2);
        tx.send(2).await;

        // sub1 still receives; sub2 is gone so no panic or stall
        assert_eq!(with_timeout(sub1.recv(), 1, "s1_2").await.unwrap(), Some(2));
    }

    #[apply(async_test)]
    async fn fanout_source_exhausted_ends_all_subs(_ex: Rc<LocalExecutor<'static>>) {
        let (tx, fanout) = Fanout::new();
        let mut sub1 = fanout.subscribe();
        let mut sub2 = fanout.subscribe();

        tx.send(7).await;
        assert_eq!(with_timeout(sub1.recv(), 1, "s1").await.unwrap(), Some(7));
        assert_eq!(with_timeout(sub2.recv(), 1, "s2").await.unwrap(), Some(7));

        drop(tx);
        assert_eq!(sub1.recv().await, None);
        assert_eq!(sub2.recv().await, None);
    }

    #[apply(async_test)]
    async fn fanout_resubscribe_after_all_dropped(_ex: Rc<LocalExecutor<'static>>) {
        let (tx, fanout) = Fanout::new();

        let sub1 = fanout.subscribe();
        drop(sub1); // last subscriber gone, fan-out still alive

        let mut sub2 = fanout.subscribe();
        tx.send(42).await;
        assert_eq!(with_timeout(sub2.recv(), 1, "s2").await.unwrap(), Some(42));
    }

    /// Values sent before any subscriber exists are silently dropped.
    /// A late subscriber only sees values sent after it subscribed.
    #[apply(async_test)]
    async fn fanout_values_dropped_before_first_subscriber(_ex: Rc<LocalExecutor<'static>>) {
        let (tx, fanout) = Fanout::new();

        // Send values before any subscriber exists — they are silently dropped
        tx.send(0).await;
        tx.send(1).await;

        // Late subscriber joins, only sees subsequent values
        let mut sub = fanout.subscribe();
        tx.send(2).await;
        tx.send(3).await;

        assert_eq!(with_timeout(sub.recv(), 1, "r1").await.unwrap(), Some(2));
        assert_eq!(with_timeout(sub.recv(), 1, "r2").await.unwrap(), Some(3));

        drop(tx);
        assert_eq!(sub.recv().await, None);
    }

    /// Verifies that a subscriber added between two sends only receives
    /// values from that point forward.
    #[apply(async_test)]
    async fn fanout_subscriber_added_between_sends(_ex: Rc<LocalExecutor<'static>>) {
        let (tx, fanout) = Fanout::new();

        let mut sub1 = fanout.subscribe();
        tx.send(10).await;
        assert_eq!(
            with_timeout(sub1.recv(), 1, "sub1_a").await.unwrap(),
            Some(10)
        );

        // Add sub2 after the first value was already sent
        let mut sub2 = fanout.subscribe();
        tx.send(20).await;
        assert_eq!(
            with_timeout(sub1.recv(), 1, "sub1_b").await.unwrap(),
            Some(20)
        );
        assert_eq!(
            with_timeout(sub2.recv(), 1, "sub2_b").await.unwrap(),
            Some(20)
        );
    }

    // ─── fanout event emitter tests ──────────────────────────────────────

    #[apply(async_test)]
    async fn fanout_wait_for_sub_event_triggered_by_subscribe(_ex: Rc<LocalExecutor<'static>>) {
        let (_tx, fanout) = Fanout::<i32>::new();
        let seen = 0u64;

        // Spawn subscribe after a delay so the wait actually blocks
        let fan = fanout.clone();
        let subscribe_task = _ex.spawn(async move {
            smol::Timer::after(Duration::from_millis(10)).await;
            let _sub = fan.subscribe();
        });

        let new_count = fanout.wait_for_sub_event(seen).await;
        assert!(
            new_count > seen,
            "sub count should increase after subscribe"
        );
        subscribe_task.await;
    }

    #[apply(async_test)]
    async fn fanout_wait_for_sub_event_returns_immediately_when_already_changed(
        _ex: Rc<LocalExecutor<'static>>,
    ) {
        let (_tx, fanout) = Fanout::<i32>::new();

        // Subscribe first
        let _sub = fanout.subscribe();
        let seen = fanout.sub_events.get();

        // Second subscribe increases the counter
        let _sub2 = fanout.subscribe();

        // wait_for_sub_event should return immediately since seen is stale
        let deadline = smol::Timer::after(Duration::from_secs(1));
        let result = futures::future::select(
            Box::pin(fanout.wait_for_sub_event(seen)),
            Box::pin(deadline),
        )
        .await;
        match result {
            futures::future::Either::Left((count, _)) => {
                assert!(count > seen);
            }
            futures::future::Either::Right(_) => {
                panic!("wait_for_sub_event timed out even though event already occurred");
            }
        }
    }

    #[apply(async_test)]
    async fn fanout_wait_for_prune_event_triggered_by_dead_subscriber(
        _ex: Rc<LocalExecutor<'static>>,
    ) {
        let (tx, fanout) = Fanout::<i32>::new();
        let sub = fanout.subscribe();
        let seen = fanout.prune_events.get();

        // Drop the subscriber so it's dead, then send to trigger pruning
        drop(sub);

        let send_task = _ex.spawn(async move {
            tx.send(1).await;
        });

        let new_count = fanout.wait_for_prune_event(seen).await;
        assert!(
            new_count > seen,
            "prune count should increase after dead sub pruned"
        );
        send_task.await;
    }

    #[apply(async_test)]
    async fn fanout_wait_for_any_event_triggered_by_subscribe(_ex: Rc<LocalExecutor<'static>>) {
        let (_tx, fanout) = Fanout::<i32>::new();
        let seen = fanout.any_events.get();

        let _sub = fanout.subscribe();

        let new_count = fanout.wait_for_any_event(seen).await;
        assert!(
            new_count > seen,
            "any count should increase after subscribe"
        );
    }

    #[apply(async_test)]
    async fn fanout_wait_for_any_event_triggered_by_prune(_ex: Rc<LocalExecutor<'static>>) {
        let (tx, fanout) = Fanout::<i32>::new();
        let sub = fanout.subscribe();
        let seen = fanout.any_events.get();

        drop(sub);

        let send_task = _ex.spawn(async move {
            tx.send(1).await;
        });

        let new_count = fanout.wait_for_any_event(seen).await;
        assert!(new_count > seen, "any count should increase after prune");
        send_task.await;
    }

    #[apply(async_test)]
    async fn fanout_event_counters_increment_independently(_ex: Rc<LocalExecutor<'static>>) {
        let (tx, fanout) = Fanout::<i32>::new();

        assert_eq!(fanout.sub_events.get(), 0);
        assert_eq!(fanout.prune_events.get(), 0);
        assert_eq!(fanout.any_events.get(), 0);

        let sub1 = fanout.subscribe();
        assert_eq!(fanout.sub_events.get(), 1);
        assert_eq!(fanout.any_events.get(), 1);
        assert_eq!(fanout.prune_events.get(), 0, "prune unchanged by subscribe");

        // Drop and prune
        drop(sub1);
        tx.send(1).await;
        assert_eq!(fanout.prune_events.get(), 1);
        assert_eq!(fanout.any_events.get(), 2);

        let _sub2 = fanout.subscribe();
        assert_eq!(fanout.sub_events.get(), 2);
        assert_eq!(fanout.any_events.get(), 3);
    }

    #[apply(async_test)]
    async fn fanout_drop_sender_triggers_prune_event(_ex: Rc<LocalExecutor<'static>>) {
        let (tx, fanout) = Fanout::<i32>::new();
        let _sub = fanout.subscribe();
        let seen = fanout.prune_events.get();

        drop(tx);

        let new_count = fanout.wait_for_prune_event(seen).await;
        assert!(new_count > seen, "dropping sender should trigger prune");
    }

    /// Regression: subscriber added by another task during `send` should
    /// not be lost when the send loop finishes and replaces the subscriber
    /// list.
    #[apply(async_test)]
    async fn fanout_subscriber_not_lost_during_concurrent_send(_ex: Rc<LocalExecutor<'static>>) {
        let (tx, fanout) = Fanout::<i32>::new();
        let fan2 = fanout.clone();

        let mut sub1 = fanout.subscribe();

        // Spawn a task that subscribes a new sub while send is in-flight
        let late_sub_task = _ex.spawn(async move {
            // Yielding a bit to ensure send is already in its send loop
            smol::Timer::after(Duration::from_millis(5)).await;
            fan2.subscribe()
        });

        // Send a value (this yields internally, allowing late_sub_task to run)
        tx.send(99).await;
        let mut sub2 = late_sub_task.await;

        // Send another value — both subs should receive it
        tx.send(100).await;

        with_timeout(sub1.recv(), 1, "s1_a").await.unwrap();
        with_timeout(sub1.recv(), 1, "s1_b").await.unwrap();

        // sub2 should only receive the second value (added after first send)
        assert_eq!(
            with_timeout(sub2.recv(), 1, "s2_b").await.unwrap(),
            Some(100),
            "late subscriber should receive the value sent after it subscribed"
        );

        // Verify sub2 is still alive — send a third value
        tx.send(200).await;
        assert_eq!(
            with_timeout(sub1.recv(), 1, "s1_c").await.unwrap(),
            Some(200)
        );
        assert_eq!(
            with_timeout(sub2.recv(), 1, "s2_c").await.unwrap(),
            Some(200),
            "late subscriber should still be alive after first send completed"
        );
    }
}
