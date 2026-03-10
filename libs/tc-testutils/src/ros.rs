use std::rc::Rc;

use futures::{FutureExt, stream};
use r2r::{WrappedTypesupport, std_msgs::msg::Int32};
use smol::{LocalExecutor, stream::StreamExt};
use std::time::Duration;
use tracing::info;
use trustworthiness_checker::{OutputStream, utils::cancellation_token::CancellationToken};

use crate::streams::{TickSender, tick_stream, with_timeout_res};

/// Generate a unique name for a ROS topic based on the current test
pub fn qualified_ros_name<T>(_test: T, base: &str) -> String {
    let test_type_name = std::any::type_name::<T>().replace("::", "_");
    format!("{base}_{test_type_name}")
}

/* A simple ROS publisher node which publishes a sequence of values on a topic
 * This creates a ROS node node_name which runs in a background thread
 * until all the values have been published. */
async fn ros_stream_publisher<T: WrappedTypesupport + 'static>(
    ex: Rc<LocalExecutor<'static>>,
    node_name: String,
    topic: String,
    mut values: OutputStream<T>,
    values_len: usize,
) -> Result<(), anyhow::Error> {
    let cancellation_token = CancellationToken::new();

    info!(
        "Starting publisher {} for topic {} with {} values",
        node_name, topic, values_len
    );
    let ctx = r2r::Context::create().unwrap();
    let mut node = r2r::Node::create(ctx, node_name.as_str(), "").unwrap();
    let publisher = node
        .create_publisher::<T>(&topic, r2r::QosProfile::default())
        .unwrap();

    // Spawn a background async task to run the ROS node
    let child_cancellation_token = cancellation_token.clone();
    ex.spawn(async move {
        let mut cancelled = FutureExt::fuse(child_cancellation_token.cancelled());
        loop {
            futures::select_biased! {
                _ = cancelled => break,
                _ = FutureExt::fuse(smol::future::yield_now()) => {
                    node.spin_once(Duration::from_millis(0));
                }
            }
        }
    })
    .detach();

    // Handshake: wait until at least one inter-process subscriber is connected
    // before publishing the first message. This reduces flaky tests arising from message loss
    // at startup.
    let wait_for_subscribers = publisher
        .wait_for_inter_process_subscribers()
        .map_err(|e| anyhow::anyhow!("Failed to create subscriber wait future: {:?}", e))?;

    let mut waited = false;
    let mut wait_future = FutureExt::fuse(wait_for_subscribers);
    let mut wait_timeout = FutureExt::fuse(smol::Timer::after(Duration::from_secs(3)));
    while !waited {
        futures::select! {
            res = wait_future => {
                match res {
                    Ok(()) => {
                        waited = true;
                    }
                    Err(e) => {
                        return Err(anyhow::anyhow!(
                            "Failed while waiting for inter-process subscribers on topic {}: {:?}",
                            topic,
                            e
                        ));
                    }
                }
            }
            _ = wait_timeout => {
                return Err(anyhow::anyhow!(
                    "Timed out waiting for inter-process subscribers on topic {}",
                    topic
                ));
            }
            _ = FutureExt::fuse(smol::future::yield_now()) => {}
        }
    }

    let mut index = 0;
    while let Some(value) = values.next().await {
        info!(
            "Publishing message number {} with value: {:?} on topic: {}",
            index, value, topic
        );
        publisher.publish(&value).unwrap();
        index += 1;
    }

    info!(
        "Finished publishing all {} messages for topic {}",
        values_len, topic
    );

    // Ensure we wait a moment before disconnecting to allow for message delivery
    smol::Timer::after(Duration::from_millis(100)).await;

    cancellation_token.cancel();

    Ok(())
}

pub fn generate_xy_test_publisher_tasks_with_topics<T: Copy>(
    executor: Rc<LocalExecutor<'static>>,
    test: T, // Test for generating ros node names
    x_topic: &str,
    y_topic: &str,
    xs: Vec<Int32>,
    ys: Vec<Int32>,
) -> (
    (TickSender, smol::Task<anyhow::Result<()>>),
    (TickSender, smol::Task<anyhow::Result<()>>),
) {
    let (x_tick, x_pub_stream) = tick_stream(stream::iter(xs.clone()).boxed());
    let (y_tick, y_pub_stream) = tick_stream(stream::iter(ys.clone()).boxed());

    let x_publisher_task = executor.spawn(with_timeout_res(
        ros_stream_publisher(
            executor.clone(),
            qualified_ros_name(test, "x_publisher"),
            x_topic.to_string(),
            x_pub_stream,
            xs.len(),
        ),
        5,
        "x_publisher_task",
    ));

    let y_publisher_task = executor.spawn(with_timeout_res(
        ros_stream_publisher(
            executor.clone(),
            qualified_ros_name(test, "y_publisher"),
            y_topic.to_string(),
            y_pub_stream,
            ys.len(),
        ),
        5,
        "y_publisher_task",
    ));

    ((x_tick, x_publisher_task), (y_tick, y_publisher_task))
}

pub fn recv_ros_int_stream(
    ex: Rc<LocalExecutor<'static>>,
    node_name: String,
    topic: String,
    timeout_secs: u64,
) -> anyhow::Result<OutputStream<i32>> {
    let ctx = r2r::Context::create()
        .map_err(|e| anyhow::anyhow!("Failed to create ROS context: {:?}", e))?;
    let mut node = r2r::Node::create(ctx, node_name.as_str(), "")
        .map_err(|e| anyhow::anyhow!("Failed to create ROS node: {:?}", e))?;
    let stream = node
        .subscribe::<Int32>(topic.as_str(), r2r::QosProfile::default())
        .map_err(|e| anyhow::anyhow!("Failed to subscribe to topic {}: {:?}", topic, e))?;
    let cancellation_token = CancellationToken::new();

    // Spawn a background async task to spin the ROS node
    let child_cancellation_token = cancellation_token.clone();
    ex.spawn(async move {
        let mut cancelled = child_cancellation_token.cancelled().fuse();
        loop {
            futures::select_biased! {
                _ = cancelled => {
                    break;
                }
                _ = smol::future::yield_now().fuse() => {
                    node.spin_once(std::time::Duration::from_millis(0));
                }
            }
        }
    })
    .detach();

    // Yield Some(data) for each received message, or None on timeout between messages
    let timeout_dur = std::time::Duration::from_secs(timeout_secs);
    let stream = async_stream::stream! {
        let cancellation_token = cancellation_token.clone();
        futures::pin_mut!(stream);
        loop {
            let timeout = futures::FutureExt::fuse(smol::Timer::after(timeout_dur));
            futures::pin_mut!(timeout);
            futures::select! {
                msg = stream.next().fuse() => {
                    match msg {
                        Some(msg) => yield msg.data,
                        None => break,
                    }
                }
                _ = timeout => {
                    cancellation_token.clone().cancel();
                    break;
                }
            }
        }
    };

    Ok(Box::pin(stream))
}
