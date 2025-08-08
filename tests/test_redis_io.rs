//! Integration tests for Redis input provider functionality.
//!
//! These tests verify that the RedisInputProvider works correctly with:
//! - Redis pub/sub messaging
//! - Monitor runtime integration
//! - Multiple channel subscriptions
//! - Proper data type handling (integers, floats, strings)
//!
//! Tests require the `testcontainers` feature to be enabled and use Docker
//! to spin up Redis instances for testing.

use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Duration;

use approx::assert_abs_diff_eq;
use async_stream::stream;
use async_unsync::oneshot;
use futures::FutureExt;
use futures::StreamExt;
use futures::select;
use macro_rules_attribute::apply;
use redis::{self, AsyncTypedCommands};
use smol::LocalExecutor;

use smol_macros::test as smol_test;
use tc_testutils::redis::{dummy_redis_receiver, dummy_redis_sender, start_redis};
use tracing::{debug, info};
use trustworthiness_checker::{
    InputProvider, OutputStream, Value, VarName,
    core::REDIS_HOSTNAME,
    core::{OutputHandler, Runnable},
    dep_manage::interface::{DependencyKind, create_dependency_manager},
    io::{
        redis::{input_provider::RedisInputProvider, output_handler::RedisOutputHandler},
        testing::manual_output_handler::ManualOutputHandler,
    },
    lola_fixtures::{spec_simple_add_monitor, spec_simple_add_monitor_typed_float},
    lola_specification,
    runtime::asynchronous::AsyncMonitorRunner,
    semantics::UntimedLolaSemantics,
};
use winnow::Parser;

/// Tests basic Redis pub/sub functionality with dummy sender and receiver.
///
/// This test verifies that messages can be published to and received from
/// Redis channels using the helper functions.
#[cfg_attr(not(feature = "testcontainers"), ignore)]
#[test_log::test(apply(smol_test))]
async fn test_dummy_redis_sender_receiver(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let redis = start_redis().await;
    let port = redis.get_host_port_ipv4(6379).await.unwrap();
    let channel = "test_channel";
    let messages = vec![Value::Str("test_message".into())];

    // Create oneshot channel for coordination
    let ready_channel = oneshot::channel();
    let (ready_tx, ready_rx) = ready_channel.into_split();

    // Start receiver before sending to ensure we don't miss the message
    let mut outputs = dummy_redis_receiver(
        executor.clone(),
        REDIS_HOSTNAME,
        Some(port),
        vec![channel.to_string()],
        ready_tx,
    )
    .await?;

    // Wait for receiver to be ready
    ready_rx.await.unwrap();

    // Create oneshot channel for sender coordination
    let sender_ready_channel = oneshot::channel();
    let (sender_ready_tx, sender_ready_rx) = sender_ready_channel.into_split();
    let _ = sender_ready_tx.send(());

    // Re-send the message after receiver is set up
    dummy_redis_sender(
        REDIS_HOSTNAME,
        Some(port),
        channel.to_string(),
        messages.clone(),
        Box::pin(sender_ready_rx.map(|_| ())),
    )
    .await?;

    // Get the first output stream
    let mut output_stream = outputs.pop().unwrap();

    // Run executor and collect received message
    let received = output_stream.next().await;

    // Verify the message was received correctly
    assert_eq!(received, Some(Value::Str("test_message".into())));
    Ok(())
}

/// Tests RedisInputProvider integration with the monitor runtime using integer values.
///
/// This test creates a RedisInputProvider that subscribes to Redis channels for
/// variables 'x' and 'y', publishes integer values to those channels, and verifies
/// that the monitor correctly computes the sum (z = x + y) and produces the expected
/// output values.
#[cfg_attr(not(feature = "testcontainers"), ignore)]
#[test_log::test(apply(smol_test))]
async fn test_add_monitor_redis_input(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    let model = lola_specification
        .parse(spec_simple_add_monitor())
        .expect("Model could not be parsed");

    let xs = vec![Value::Int(1), Value::Int(2)];
    let ys = vec![Value::Int(3), Value::Int(4)];
    let zs = vec![Value::Int(4), Value::Int(6)];

    let redis = start_redis().await;
    let redis_port = redis
        .get_host_port_ipv4(6379)
        .await
        .expect("Failed to get host port for Redis server");

    let var_topics = [
        ("x".into(), "redis_input_x".to_string()),
        ("y".into(), "redis_input_y".to_string()),
    ]
    .into_iter()
    .collect::<BTreeMap<VarName, _>>();

    // Create the Redis input provider
    let input_provider = RedisInputProvider::new(
        executor.clone(),
        REDIS_HOSTNAME,
        Some(redis_port),
        var_topics,
    )
    .map_err(|e| anyhow::anyhow!("Failed to create Redis input provider: {}", e))?;

    input_provider.ready().await?;

    // Create oneshot channels for coordination
    let ready_channel_x = oneshot::channel();
    let (ready_tx_x, ready_rx_x) = ready_channel_x.into_split();
    let ready_channel_y = oneshot::channel();
    let (ready_tx_y, ready_rx_y) = ready_channel_y.into_split();

    // Run the monitor
    let mut output_handler = ManualOutputHandler::new(executor.clone(), vec!["z".into()]);
    let outputs = output_handler.get_output();
    let runner = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        model.clone(),
        Box::new(input_provider),
        Box::new(output_handler),
        create_dependency_manager(DependencyKind::Empty, model),
    );

    let res = executor.spawn(runner.run());

    // Spawn dummy Redis publisher tasks
    executor
        .spawn(dummy_redis_sender(
            REDIS_HOSTNAME,
            Some(redis_port),
            "redis_input_x".to_string(),
            xs,
            Box::pin(ready_rx_x.map(|_| ())),
        ))
        .detach();

    executor
        .spawn(dummy_redis_sender(
            REDIS_HOSTNAME,
            Some(redis_port),
            "redis_input_y".to_string(),
            ys,
            Box::pin(ready_rx_y.map(|_| ())),
        ))
        .detach();

    // Signal that senders are ready to start
    let _ = ready_tx_x.send(());
    let _ = ready_tx_y.send(());

    // Test we have the expected outputs
    info!("Waiting for {:?} outputs", zs.len());
    let outputs = outputs.take(zs.len()).collect::<Vec<_>>().await;
    info!("Outputs: {:?}", outputs);
    let expected_outputs = zs.into_iter().map(|val| vec![val]).collect::<Vec<_>>();
    assert_eq!(outputs, expected_outputs);

    info!("Output collection complete, output stream should now be dropped");

    info!("Waiting for monitor to complete after output stream drop...");
    let timeout_future = smol::Timer::after(Duration::from_secs(5));

    select! {
        result = res.fuse() => {
            info!("Monitor completed: {:?}", result);
            result?;
        }
        _ = futures::FutureExt::fuse(timeout_future) => {
            return Err(anyhow::anyhow!("Monitor did not complete within timeout after output stream was dropped"));
        }
    }

    Ok(())
}

/// Tests RedisInputProvider integration with the monitor runtime using floating-point values.
///
/// Similar to the integer test, but uses float values to verify that the RedisInputProvider
/// correctly handles different data types. Tests that floating-point arithmetic is performed
/// correctly and outputs are within expected precision bounds.
#[cfg_attr(not(feature = "testcontainers"), ignore)]
#[test_log::test(apply(smol_test))]
async fn test_add_monitor_redis_input_float(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let model = lola_specification
        .parse(spec_simple_add_monitor_typed_float())
        .expect("Model could not be parsed");

    let xs = vec![Value::Float(1.5), Value::Float(2.5)];
    let ys = vec![Value::Float(3.5), Value::Float(4.5)];
    let expected_zs = vec![5.0, 7.0];

    let redis = start_redis().await;
    let redis_port = redis
        .get_host_port_ipv4(6379)
        .await
        .expect("Failed to get host port for Redis server");

    let var_topics = [
        ("x".into(), "redis_input_x_float".to_string()),
        ("y".into(), "redis_input_y_float".to_string()),
    ]
    .into_iter()
    .collect::<BTreeMap<VarName, _>>();

    // Create the Redis input provider
    let input_provider = RedisInputProvider::new(
        executor.clone(),
        REDIS_HOSTNAME,
        Some(redis_port),
        var_topics,
    )
    .map_err(|e| anyhow::anyhow!("Failed to create Redis input provider: {}", e))?;

    input_provider.ready().await?;

    // Create oneshot channels for coordination
    let ready_channel_x = oneshot::channel();
    let (ready_tx_x, ready_rx_x) = ready_channel_x.into_split();
    let ready_channel_y = oneshot::channel();
    let (ready_tx_y, ready_rx_y) = ready_channel_y.into_split();

    // Run the monitor
    let mut output_handler = ManualOutputHandler::new(executor.clone(), vec!["z".into()]);
    let outputs = output_handler.get_output();
    let runner = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        model.clone(),
        Box::new(input_provider),
        Box::new(output_handler),
        create_dependency_manager(DependencyKind::Empty, model),
    );

    let res = executor.spawn(runner.run());

    // Spawn dummy Redis publisher tasks
    executor
        .spawn(dummy_redis_sender(
            REDIS_HOSTNAME,
            Some(redis_port),
            "redis_input_x_float".to_string(),
            xs,
            Box::pin(ready_rx_x.map(|_| ())),
        ))
        .detach();

    executor
        .spawn(dummy_redis_sender(
            REDIS_HOSTNAME,
            Some(redis_port),
            "redis_input_y_float".to_string(),
            ys,
            Box::pin(ready_rx_y.map(|_| ())),
        ))
        .detach();

    // Signal that senders are ready to start
    let _ = ready_tx_x.send(());
    let _ = ready_tx_y.send(());

    // Test we have the expected outputs
    info!("Waiting for {:?} outputs", expected_zs.len());
    let outputs = outputs.take(expected_zs.len()).collect::<Vec<_>>().await;
    info!("Outputs: {:?}", outputs);

    // Check that the outputs are approximately correct (for floating point)
    assert_eq!(outputs.len(), expected_zs.len());
    for (output, expected) in outputs.iter().zip(expected_zs.iter()) {
        assert_eq!(output.len(), 1);
        if let Value::Float(actual) = &output[0] {
            assert_abs_diff_eq!(actual, expected, epsilon = 1e-6);
        } else {
            panic!("Expected float output, got {:?}", output[0]);
        }
    }

    info!("Output collection complete, output stream should now be dropped");

    info!("Waiting for monitor to complete after output stream drop...");
    let timeout_future = smol::Timer::after(Duration::from_secs(5));

    select! {
        result = res.fuse() => {
            info!("Monitor completed: {:?}", result);
            result?;
        }
        _ = futures::FutureExt::fuse(timeout_future) => {
            return Err(anyhow::anyhow!("Monitor did not complete within timeout after output stream was dropped"));
        }
    }

    Ok(())
}

/// Tests RedisInputProvider with multiple channels and different data types.
///
/// This test verifies that a single RedisInputProvider can handle multiple
/// Redis channels simultaneously, each providing different types of data
/// (string, integer, float). It tests the provider's ability to route
/// messages from different channels to the correct variable streams.
#[cfg_attr(not(feature = "testcontainers"), ignore)]
#[test_log::test(apply(smol_test))]
async fn test_redis_input_provider_multiple_channels(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let redis = start_redis().await;
    let redis_port = redis
        .get_host_port_ipv4(6379)
        .await
        .expect("Failed to get host port for Redis server");

    let var_topics = [
        ("var1".into(), "channel1".to_string()),
        ("var2".into(), "channel2".to_string()),
        ("var3".into(), "channel3".to_string()),
    ]
    .into_iter()
    .collect::<BTreeMap<VarName, _>>();

    let mut input_provider = RedisInputProvider::new(
        executor.clone(),
        REDIS_HOSTNAME,
        Some(redis_port),
        var_topics,
    )
    .map_err(|e| anyhow::anyhow!("Failed to create Redis input provider: {}", e))?;

    // Create oneshot channels for coordination
    let ready_channel_1 = oneshot::channel();
    let (ready_tx_1, ready_rx_1) = ready_channel_1.into_split();
    let ready_channel_2 = oneshot::channel();
    let (ready_tx_2, ready_rx_2) = ready_channel_2.into_split();
    let ready_channel_3 = oneshot::channel();
    let (ready_tx_3, ready_rx_3) = ready_channel_3.into_split();

    // Get input streams for all variables
    let mut stream1 = input_provider
        .input_stream(&"var1".into())
        .expect("Failed to get stream for var1");
    let mut stream2 = input_provider
        .input_stream(&"var2".into())
        .expect("Failed to get stream for var2");
    let mut stream3 = input_provider
        .input_stream(&"var3".into())
        .expect("Failed to get stream for var3");

    // Start the input provider
    let provider_task = executor.spawn(input_provider.run());

    input_provider.ready().await?;

    // Send test messages to different channels
    executor
        .spawn(dummy_redis_sender(
            REDIS_HOSTNAME,
            Some(redis_port),
            "channel1".to_string(),
            vec![Value::Str("message1".into())],
            Box::pin(ready_rx_1.map(|_| ())),
        ))
        .detach();

    executor
        .spawn(dummy_redis_sender(
            REDIS_HOSTNAME,
            Some(redis_port),
            "channel2".to_string(),
            vec![Value::Int(42)],
            Box::pin(ready_rx_2.map(|_| ())),
        ))
        .detach();

    executor
        .spawn(dummy_redis_sender(
            REDIS_HOSTNAME,
            Some(redis_port),
            "channel3".to_string(),
            vec![Value::Float(3.14)],
            Box::pin(ready_rx_3.map(|_| ())),
        ))
        .detach();

    // Signal that senders are ready to start
    let _ = ready_tx_1.send(());
    let _ = ready_tx_2.send(());
    let _ = ready_tx_3.send(());

    // Collect messages from all streams
    let msg1 = executor.run(async { stream1.next().await }).await;
    let msg2 = executor
        .run(async { futures::StreamExt::next(&mut stream2).await })
        .await;
    let msg3 = executor
        .run(async { futures::StreamExt::next(&mut stream3).await })
        .await;

    // Verify messages
    assert_eq!(msg1, Some(Value::Str("message1".into())));
    assert_eq!(msg2, Some(Value::Int(42)));
    assert_eq!(msg3, Some(Value::Float(3.14)));

    // Clean up
    drop(stream1);
    drop(stream2);
    drop(stream3);

    let timeout_future = smol::Timer::after(std::time::Duration::from_secs(2));
    futures::select! {
        result = provider_task.fuse() => {
            info!("Provider completed: {:?}", result);
            result?;
        }
        _ = futures::FutureExt::fuse(timeout_future) => {
            info!("Provider task did not complete within timeout, this is expected");
        }
    }

    Ok(())
}

#[cfg_attr(not(feature = "testcontainers"), ignore)]
#[test_log::test(apply(smol_test))]
async fn test_pubsub_roundtrip(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    let redis = start_redis().await;
    let redis_port = redis
        .get_host_port_ipv4(6379)
        .await
        .expect("Failed to get host port for Redis server");

    // Test cases showing Value types and their JSON wire format
    let test_cases = vec![
        (Value::Int(42), "Integer 42"),
        (Value::Float(3.14), "Float 3.14"),
        (Value::Str("hello".into()), "String hello"),
        (Value::Bool(true), "Boolean true"),
        (Value::Unit, "Unit value"),
        (
            Value::List(vec![Value::Int(1), Value::Str("test".into())].into()),
            "Mixed list",
        ),
    ];

    for (value, description) in test_cases {
        let channel = format!("wire_format_test_{}", uuid::Uuid::new_v4());

        info!("Testing {}: {:?}", description, value);

        // Create receiver using our existing helper
        // Create oneshot channel for coordination
        let ready_channel = oneshot::channel();
        let (ready_tx, ready_rx) = ready_channel.into_split();

        let mut receiver_outputs = dummy_redis_receiver(
            executor.clone(),
            REDIS_HOSTNAME,
            Some(redis_port),
            vec![channel.clone()],
            ready_tx,
        )
        .await?;

        // Wait for receiver to be ready
        ready_rx.await.unwrap();

        // Create oneshot channel for sender coordination
        let sender_ready_channel = oneshot::channel();
        let (sender_ready_tx, sender_ready_rx) = sender_ready_channel.into_split();
        let _ = sender_ready_tx.send(());

        // Send the value using our existing helper
        dummy_redis_sender(
            REDIS_HOSTNAME,
            Some(redis_port),
            channel,
            vec![value.clone()],
            Box::pin(sender_ready_rx.map(|_| ())),
        )
        .await?;

        // Verify round-trip works
        if let Some(mut stream) = receiver_outputs.pop() {
            // Use a timeout to avoid hanging
            let timeout = smol::Timer::after(Duration::from_millis(500));
            futures::select! {
                received = stream.next().fuse() => {
                    match received {
                        Some(received_value) => {
                            info!("{} - Round-trip successful: {:?} -> {:?}",
                                    description, value, received_value);
                            assert_eq!(received_value, value);
                        }
                        None => {
                            return Err(anyhow::anyhow!("No message received for {}", description));
                        }
                    }
                }
                _ = futures::FutureExt::fuse(timeout) => {
                    return Err(anyhow::anyhow!("Timeout waiting for {} message", description));
                }
            }
        }
    }

    Ok(())
}

/// Tests edge cases in message serialization including special characters and complex types.
///
/// This test verifies that the Redis serialization handles edge cases correctly,
/// including strings with special characters, empty values, and complex nested structures.
#[cfg_attr(not(feature = "testcontainers"), ignore)]
#[test_log::test(apply(smol_test))]
async fn test_serialization_edge_cases(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    let redis = start_redis().await;
    let redis_port = redis
        .get_host_port_ipv4(6379)
        .await
        .expect("Failed to get host port for Redis server");

    // Test edge cases and special characters
    let edge_cases = vec![
        // Empty string
        Value::Str("".into()),
        // String with quotes
        Value::Str("He said \"Hello!\"".into()),
        // String with newlines and special chars
        Value::Str("Line 1\nLine 2\tTabbed\r\nWindows line ending".into()),
        // Unicode characters
        Value::Str("Héllo 世界 🚀".into()),
        // JSON-like string (should be escaped)
        Value::Str("{\"key\": \"value\"}".into()),
        // Very large integer
        Value::Int(i64::MAX),
        Value::Int(i64::MIN),
        // Special float values
        Value::Float(f64::INFINITY),
        Value::Float(f64::NEG_INFINITY),
        // Note: NaN cannot be tested as it doesn't equal itself
        // Complex list with mixed types
        Value::List(
            vec![
                Value::Int(1),
                Value::Str("nested".into()),
                Value::Bool(true),
                Value::Float(2.5),
            ]
            .into(),
        ),
        // Nested list
        Value::List(
            vec![
                Value::List(vec![Value::Int(1), Value::Int(2)].into()),
                Value::List(vec![Value::Str("a".into()), Value::Str("b".into())].into()),
            ]
            .into(),
        ),
    ];

    for (i, test_value) in edge_cases.into_iter().enumerate() {
        let channel = format!("edge_case_channel_{}", i);

        info!("Testing edge case {}: {:?}", i, test_value);

        // Create receiver first
        // Create oneshot channel for coordination
        let ready_channel = oneshot::channel();
        let (ready_tx, ready_rx) = ready_channel.into_split();

        let mut receiver_outputs = dummy_redis_receiver(
            executor.clone(),
            REDIS_HOSTNAME,
            Some(redis_port),
            vec![channel.clone()],
            ready_tx,
        )
        .await?;

        // Wait for receiver to be ready
        ready_rx.await.unwrap();

        // Create oneshot channel for sender coordination
        let sender_ready_channel = oneshot::channel();
        let (sender_ready_tx, sender_ready_rx) = sender_ready_channel.into_split();
        let _ = sender_ready_tx.send(());

        // Send the value
        dummy_redis_sender(
            REDIS_HOSTNAME,
            Some(redis_port),
            channel,
            vec![test_value.clone()],
            Box::pin(sender_ready_rx.map(|_| ())),
        )
        .await?;

        // Verify round-trip serialization
        if let Some(mut stream) = receiver_outputs.pop() {
            let received = futures::StreamExt::next(&mut stream).await;
            match received {
                Some(received_value) => {
                    info!(
                        "Round-trip successful for case {}: {:?} -> {:?}",
                        i, test_value, received_value
                    );

                    // Special handling for infinite floats which may have different representations
                    match (&test_value, &received_value) {
                        (Value::Float(sent), Value::Float(recv))
                            if sent.is_infinite() && recv.is_infinite() =>
                        {
                            assert_eq!(sent.is_sign_positive(), recv.is_sign_positive());
                        }
                        _ => {
                            assert_eq!(
                                received_value, test_value,
                                "Round-trip failed for edge case {}",
                                i
                            );
                        }
                    }
                }
                None => {
                    return Err(anyhow::anyhow!(
                        "No message received for edge case {}: {:?}",
                        i,
                        test_value
                    ));
                }
            }
        }
    }

    Ok(())
}

/// Tests raw Redis wire format by directly examining what gets published.
///
/// This test uses Redis's raw string operations to see exactly what bytes
/// are being sent over the wire when publishing Value types.
#[cfg_attr(not(feature = "testcontainers"), ignore)]
#[test_log::test(apply(smol_test))]
async fn test_raw_wire_format_inspection(
    _executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let redis = start_redis().await;
    let redis_port = redis
        .get_host_port_ipv4(6379)
        .await
        .expect("Failed to get host port for Redis server");
    let host_uri = format!("redis://127.0.0.1:{}", redis_port);

    let client = redis::Client::open(host_uri.as_str())?;
    let mut con = client.get_multiplexed_async_connection().await?;

    // Test values and their actual Redis wire format (Rust enum serialization)
    let test_cases = vec![
        (Value::Int(123), "{\"Int\":123}"),
        (Value::Float(45.67), "{\"Float\":45.67}"),
        (Value::Str("test".into()), "{\"Str\":\"test\"}"),
        (Value::Bool(true), "{\"Bool\":true}"),
        (Value::Bool(false), "{\"Bool\":false}"),
        (Value::Unit, "\"Unit\""),
    ];

    for (value, expected_json) in test_cases {
        let key = format!("wire_test_{}", uuid::Uuid::new_v4());

        // Use Redis SET command to store the value, then GET it back as a raw string
        con.set(&key, &value).await?;
        let raw_string: String = con.get(&key).await?.unwrap_or_default();

        info!("Value: {:?}", value);
        info!("Raw wire format: {}", raw_string);
        info!("Expected: {}", expected_json);

        // Verify the raw format matches expected Rust enum serialization
        assert_eq!(raw_string, expected_json);

        // Clean up
        let _: usize = con.del(&key).await?;
    }

    // Test complex structures
    let complex_value =
        Value::List(vec![Value::Int(1), Value::Str("hello".into()), Value::Bool(true)].into());

    let complex_key = format!("complex_wire_test_{}", uuid::Uuid::new_v4());
    con.set(&complex_key, &complex_value).await?;
    let complex_raw: String = con.get(&complex_key).await?.unwrap_or_default();

    info!("Complex value: {:?}", complex_value);
    info!("Complex raw format: {}", complex_raw);

    // Verify it's valid JSON by parsing it
    let parsed: serde_json::Value = serde_json::from_str(&complex_raw)?;
    info!("Parsed JSON: {:?}", parsed);

    // Clean up
    let _: usize = con.del(&complex_key).await?;

    Ok(())
}

/// Tests compatibility between manual enum format JSON strings and Value deserialization.
///
/// This test verifies that manually crafted enum format JSON strings can be successfully
/// deserialized into Value types, ensuring interoperability with external systems
/// that use the correct enum wire format.
#[cfg_attr(not(feature = "testcontainers"), ignore)]
#[test_log::test(apply(smol_test))]
async fn test_json_interoperability(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    let redis = start_redis().await;
    let redis_port = redis
        .get_host_port_ipv4(6379)
        .await
        .expect("Failed to get host port for Redis server");
    let host_uri = format!("redis://127.0.0.1:{}", redis_port);

    // Test cases: manually crafted enum format JSON and expected Value
    let json_test_cases = vec![
        ("{\"Int\":42}", Value::Int(42)),
        ("{\"Float\":3.14159}", Value::Float(3.14159)),
        (
            "{\"Str\":\"Hello, Redis!\"}",
            Value::Str("Hello, Redis!".into()),
        ),
        ("{\"Bool\":true}", Value::Bool(true)),
        ("{\"Bool\":false}", Value::Bool(false)),
        ("\"Unit\"", Value::Unit),
        (
            "{\"List\":[{\"Int\":1},{\"Str\":\"two\"},{\"Bool\":true},{\"Float\":4.5}]}",
            Value::List(
                vec![
                    Value::Int(1),
                    Value::Str("two".into()),
                    Value::Bool(true),
                    Value::Float(4.5),
                ]
                .into(),
            ),
        ),
        (
            "{\"List\":[{\"List\":[{\"Int\":1},{\"Int\":2}]},{\"List\":[{\"Str\":\"a\"},{\"Str\":\"b\"}]}]}",
            Value::List(
                vec![
                    Value::List(vec![Value::Int(1), Value::Int(2)].into()),
                    Value::List(vec![Value::Str("a".into()), Value::Str("b".into())].into()),
                ]
                .into(),
            ),
        ),
    ];

    for (json_str, expected_value) in json_test_cases {
        let channel = format!("json_test_{}", uuid::Uuid::new_v4());

        info!("Testing enum format JSON: {}", json_str);
        info!("Expected Value: {:?}", expected_value);

        // Create receiver first
        // Create oneshot channel for coordination
        let ready_channel = oneshot::channel();
        let (ready_tx, ready_rx) = ready_channel.into_split();

        let mut receiver_outputs = dummy_redis_receiver(
            executor.clone(),
            REDIS_HOSTNAME,
            Some(redis_port),
            vec![channel.clone()],
            ready_tx,
        )
        .await?;

        // Wait for receiver to be ready
        ready_rx.await.unwrap();

        // Manually publish the enum format JSON string
        let client = redis::Client::open(host_uri.as_str())?;
        let mut con = client.get_multiplexed_async_connection().await?;
        con.publish(&channel, json_str).await?;

        // Verify it deserializes to the expected Value
        if let Some(mut stream) = receiver_outputs.pop() {
            let received = stream.next().await;
            match received {
                Some(received_value) => {
                    info!("Enum format JSON deserialized to: {:?}", received_value);
                    assert_eq!(
                        received_value, expected_value,
                        "Enum format compatibility failed for: {}",
                        json_str
                    );
                }
                None => {
                    return Err(anyhow::anyhow!(
                        "No message received for enum format JSON: {}",
                        json_str
                    ));
                }
            }
        }
    }

    Ok(())
}

// Helper function to create a simple output stream for testing
fn create_test_output_stream(values: Vec<Value>) -> OutputStream<Value> {
    let stream = stream! {
        for value in values {
            yield value;
        }
    };
    Box::pin(stream)
}

// Helper function to consume messages from a Redis channel
async fn consume_redis_messages(
    host: String,
    channel: String,
    expected_count: usize,
    timeout_ms: u64,
    ready_tx: oneshot::Sender<()>,
) -> anyhow::Result<Vec<Value>> {
    let client = redis::Client::open(host)?;
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub.subscribe(&channel).await?;

    // Signal that we're ready to receive messages
    let _ = ready_tx.send(());

    let mut messages = Vec::new();
    let timeout_duration = Duration::from_millis(timeout_ms);

    for _ in 0..expected_count {
        let timeout_timer = smol::Timer::after(timeout_duration);
        let mut stream = pubsub.on_message();

        futures::select! {
            msg = stream.next().fuse() => {
                match msg {
                    Some(msg) => {
                        let payload: String = msg.get_payload()?;
                        let value: Value = serde_json::from_str(&payload)?;
                        messages.push(value);
                    }
                    None => break,
                }
            }
            _ = futures::FutureExt::fuse(timeout_timer) => {
                debug!("Timeout waiting for message");
                break;
            }
        }
    }

    Ok(messages)
}

#[apply(smol_test)]
async fn test_redis_output_handler_basic(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let redis = start_redis().await;
    let host = redis.get_host_port_ipv4(6379).await.unwrap();
    let host_uri = format!("redis://127.0.0.1:{}", host);

    // Create test variables and topics
    let var1 = VarName::new("test_var1");
    let var2 = VarName::new("test_var2");
    let var_names = vec![var1.clone(), var2.clone()];

    let mut var_topics = BTreeMap::new();
    var_topics.insert(var1.clone(), "topic1".to_string());
    var_topics.insert(var2.clone(), "topic2".to_string());

    // Create RedisOutputHandler
    let mut handler = RedisOutputHandler::new(
        executor.clone(),
        var_names,
        REDIS_HOSTNAME,
        Some(host),
        var_topics,
    )?;

    // Create test output streams
    let stream1 = create_test_output_stream(vec![Value::Int(42), Value::Str("hello".into())]);
    let stream2 = create_test_output_stream(vec![Value::Float(3.14), Value::Bool(true)]);

    // Provide streams to handler
    handler.provide_streams(vec![stream1, stream2]);

    // Create oneshot channels for coordination
    let ready_channel1 = oneshot::channel();
    let (ready_tx1, ready_rx1) = ready_channel1.into_split();
    let ready_channel2 = oneshot::channel();
    let (ready_tx2, ready_rx2) = ready_channel2.into_split();

    // Start consumers first
    let consumer1_task = executor.spawn(consume_redis_messages(
        host_uri.clone(),
        "topic1".to_string(),
        2,
        5000,
        ready_tx1,
    ));
    let consumer2_task = executor.spawn(consume_redis_messages(
        host_uri.clone(),
        "topic2".to_string(),
        2,
        5000,
        ready_tx2,
    ));

    // Wait for consumers to be ready
    ready_rx1.await.unwrap();
    ready_rx2.await.unwrap();

    // Run handler
    let handler_task = handler.run();

    // Wait for all tasks to complete
    let (handler_result, messages1, messages2) =
        futures::join!(handler_task, consumer1_task, consumer2_task);

    // Verify handler completed successfully
    handler_result?;

    // Verify messages were received correctly
    let messages1 = messages1?;
    let messages2 = messages2?;

    assert_eq!(messages1.len(), 2);
    assert_eq!(messages2.len(), 2);

    assert_eq!(messages1[0], Value::Int(42));
    assert_eq!(messages1[1], Value::Str("hello".into()));
    assert_eq!(messages2[0], Value::Float(3.14));
    assert_eq!(messages2[1], Value::Bool(true));

    Ok(())
}

#[apply(smol_test)]
async fn test_redis_output_handler_single_variable(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let redis = start_redis().await;
    let host = redis.get_host_port_ipv4(6379).await.unwrap();
    let host_uri = format!("redis://127.0.0.1:{}", host);

    // Create single test variable
    let var = VarName::new("single_var");
    let var_names = vec![var.clone()];

    let mut var_topics = BTreeMap::new();
    var_topics.insert(var.clone(), "single_topic".to_string());

    // Create RedisOutputHandler
    let mut handler = RedisOutputHandler::new(
        executor.clone(),
        var_names,
        REDIS_HOSTNAME,
        Some(host),
        var_topics,
    )?;

    // Create test output stream with various data types
    let stream = create_test_output_stream(vec![
        Value::Int(123),
        Value::Float(456.789),
        Value::Str("test_string".into()),
        Value::Bool(false),
        Value::Unit,
    ]);

    // Provide stream to handler
    handler.provide_streams(vec![stream]);

    // Create oneshot channel for coordination
    let ready_channel = oneshot::channel();
    let (ready_tx, ready_rx) = ready_channel.into_split();

    // Start consumer first
    let consumer_task = executor.spawn(consume_redis_messages(
        host_uri.clone(),
        "single_topic".to_string(),
        5,
        5000,
        ready_tx,
    ));

    // Wait for consumer to be ready
    ready_rx.await.unwrap();

    // Run handler
    let handler_task = handler.run();

    // Wait for completion
    let (handler_result, messages) = futures::join!(handler_task, consumer_task);

    // Verify results
    handler_result?;
    let messages = messages?;

    assert_eq!(messages.len(), 5);
    assert_eq!(messages[0], Value::Int(123));
    assert_eq!(messages[1], Value::Float(456.789));
    assert_eq!(messages[2], Value::Str("test_string".into()));
    assert_eq!(messages[3], Value::Bool(false));
    assert_eq!(messages[4], Value::Unit);

    Ok(())
}

#[apply(smol_test)]
async fn test_redis_output_handler_empty_stream(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let redis = start_redis().await;
    let host = redis.get_host_port_ipv4(6379).await.unwrap();
    let host_uri = format!("redis://127.0.0.1:{}", host);

    // Create test variable
    let var = VarName::new("empty_var");
    let var_names = vec![var.clone()];

    let mut var_topics = BTreeMap::new();
    var_topics.insert(var.clone(), "empty_topic".to_string());

    // Create RedisOutputHandler
    let mut handler = RedisOutputHandler::new(
        executor.clone(),
        var_names,
        REDIS_HOSTNAME,
        Some(host),
        var_topics,
    )?;

    // Create empty output stream
    let stream = create_test_output_stream(vec![]);

    // Provide stream to handler
    handler.provide_streams(vec![stream]);

    // Create oneshot channel for coordination
    let ready_channel = oneshot::channel();
    let (ready_tx, ready_rx) = ready_channel.into_split();

    // Start consuming messages (should timeout with no messages)
    let consumer_task = executor.spawn(consume_redis_messages(
        host_uri.clone(),
        "empty_topic".to_string(),
        1,
        1000,
        ready_tx,
    ));

    // Wait for consumer to be ready
    ready_rx.await.unwrap();

    // Run handler
    let handler_task = handler.run();

    // Wait for completion
    let (handler_result, messages) = futures::join!(handler_task, consumer_task);

    // Verify results
    handler_result?;
    let messages = messages?;

    // Should receive no messages
    assert_eq!(messages.len(), 0);

    Ok(())
}

#[apply(smol_test)]
async fn test_redis_output_handler_multiple_variables(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let redis = start_redis().await;
    let host = redis.get_host_port_ipv4(6379).await.unwrap();
    let host_uri = format!("redis://127.0.0.1:{}", host);

    // Create multiple test variables
    let var1 = VarName::new("multi_var1");
    let var2 = VarName::new("multi_var2");
    let var3 = VarName::new("multi_var3");
    let var_names = vec![var1.clone(), var2.clone(), var3.clone()];

    let mut var_topics = BTreeMap::new();
    var_topics.insert(var1.clone(), "multi_topic1".to_string());
    var_topics.insert(var2.clone(), "multi_topic2".to_string());
    var_topics.insert(var3.clone(), "multi_topic3".to_string());

    // Create RedisOutputHandler
    let mut handler = RedisOutputHandler::new(
        executor.clone(),
        var_names,
        REDIS_HOSTNAME,
        Some(host),
        var_topics,
    )?;

    // Create test output streams
    let stream1 = create_test_output_stream(vec![Value::Int(1), Value::Int(2)]);
    let stream2 = create_test_output_stream(vec![Value::Str("a".into()), Value::Str("b".into())]);
    let stream3 = create_test_output_stream(vec![Value::Bool(true), Value::Bool(false)]);

    // Provide streams to handler
    handler.provide_streams(vec![stream1, stream2, stream3]);

    // Create oneshot channels for coordination
    let ready_channel1 = oneshot::channel();
    let (ready_tx1, ready_rx1) = ready_channel1.into_split();
    let ready_channel2 = oneshot::channel();
    let (ready_tx2, ready_rx2) = ready_channel2.into_split();
    let ready_channel3 = oneshot::channel();
    let (ready_tx3, ready_rx3) = ready_channel3.into_split();

    // Start consumers first
    let consumer1_task = executor.spawn(consume_redis_messages(
        host_uri.clone(),
        "multi_topic1".to_string(),
        2,
        5000,
        ready_tx1,
    ));
    let consumer2_task = executor.spawn(consume_redis_messages(
        host_uri.clone(),
        "multi_topic2".to_string(),
        2,
        5000,
        ready_tx2,
    ));
    let consumer3_task = executor.spawn(consume_redis_messages(
        host_uri.clone(),
        "multi_topic3".to_string(),
        2,
        5000,
        ready_tx3,
    ));

    // Wait for consumers to be ready
    ready_rx1.await.unwrap();
    ready_rx2.await.unwrap();
    ready_rx3.await.unwrap();

    // Run handler
    let handler_task = handler.run();

    // Wait for completion
    let (handler_result, messages1, messages2, messages3) =
        futures::join!(handler_task, consumer1_task, consumer2_task, consumer3_task);

    // Verify results
    handler_result?;
    let messages1 = messages1?;
    let messages2 = messages2?;
    let messages3 = messages3?;

    assert_eq!(messages1.len(), 2);
    assert_eq!(messages2.len(), 2);
    assert_eq!(messages3.len(), 2);

    assert_eq!(messages1[0], Value::Int(1));
    assert_eq!(messages1[1], Value::Int(2));
    assert_eq!(messages2[0], Value::Str("a".into()));
    assert_eq!(messages2[1], Value::Str("b".into()));
    assert_eq!(messages3[0], Value::Bool(true));
    assert_eq!(messages3[1], Value::Bool(false));

    Ok(())
}

#[apply(smol_test)]
async fn test_redis_output_handler_var_names(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let redis = start_redis().await;
    let host = redis.get_host_port_ipv4(6379).await.unwrap();

    // Create test variables
    let var1 = VarName::new("var1");
    let var2 = VarName::new("var2");
    let var_names = vec![var1.clone(), var2.clone()];

    let mut var_topics = BTreeMap::new();
    var_topics.insert(var1.clone(), "topic1".to_string());
    var_topics.insert(var2.clone(), "topic2".to_string());

    // Create RedisOutputHandler
    let handler = RedisOutputHandler::new(
        executor.clone(),
        var_names.clone(),
        REDIS_HOSTNAME,
        Some(host),
        var_topics,
    )?;

    // Test var_names method
    let returned_var_names = handler.var_names();
    assert_eq!(returned_var_names, var_names);

    Ok(())
}

#[apply(smol_test)]
async fn test_redis_output_handler_json_serialization(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let redis = start_redis().await;
    let host = redis.get_host_port_ipv4(6379).await.unwrap();
    let host_uri = format!("redis://127.0.0.1:{}", host);

    // Create test variable
    let var = VarName::new("json_var");
    let var_names = vec![var.clone()];

    let mut var_topics = BTreeMap::new();
    var_topics.insert(var.clone(), "json_topic".to_string());

    // Create RedisOutputHandler
    let mut handler = RedisOutputHandler::new(
        executor.clone(),
        var_names,
        REDIS_HOSTNAME,
        Some(host),
        var_topics,
    )?;

    // Create test output stream with complex data
    let stream = create_test_output_stream(vec![
        Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)].into()),
        Value::Str("special chars: àáâãäåæçèéêë".into()),
    ]);

    // Provide stream to handler
    handler.provide_streams(vec![stream]);

    // Create oneshot channel for coordination
    let ready_channel = oneshot::channel();
    let (ready_tx, ready_rx) = ready_channel.into_split();

    // Start consuming messages using our helper function
    let consumer_task = executor.spawn(consume_redis_messages(
        host_uri.clone(),
        "json_topic".to_string(),
        2,
        5000,
        ready_tx,
    ));

    // Wait for consumer to be ready
    ready_rx.await.unwrap();

    // Run handler
    let handler_task = handler.run();

    // Wait for both tasks to complete
    let (handler_result, messages) = futures::join!(handler_task, consumer_task);
    handler_result?;
    let messages = messages?;

    // Convert messages back to JSON strings for verification
    let raw_messages: Vec<String> = messages
        .into_iter()
        .map(|msg| serde_json::to_string(&msg).unwrap())
        .collect();

    // Verify JSON serialization
    assert_eq!(raw_messages.len(), 2);

    // Verify first message (list)
    let value1: Value = serde_json::from_str(&raw_messages[0])?;
    assert_eq!(
        value1,
        Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)].into())
    );

    // Verify second message (string with special chars)
    let value2: Value = serde_json::from_str(&raw_messages[1])?;
    assert_eq!(value2, Value::Str("special chars: àáâãäåæçèéêë".into()));

    Ok(())
}

#[apply(smol_test)]
async fn test_redis_output_handler_concurrent_streams(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let redis = start_redis().await;
    let host = redis.get_host_port_ipv4(6379).await.unwrap();
    let host_uri = format!("redis://127.0.0.1:{}", host);

    // Create test variables
    let var1 = VarName::new("concurrent_var1");
    let var2 = VarName::new("concurrent_var2");
    let var_names = vec![var1.clone(), var2.clone()];

    let mut var_topics = BTreeMap::new();
    var_topics.insert(var1.clone(), "concurrent_topic1".to_string());
    var_topics.insert(var2.clone(), "concurrent_topic2".to_string());

    // Create RedisOutputHandler
    let mut handler = RedisOutputHandler::new(
        executor.clone(),
        var_names,
        REDIS_HOSTNAME,
        Some(host),
        var_topics,
    )?;

    // Create output streams with timing delays to test concurrency
    let stream1 = create_test_output_stream(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);

    let stream2 = create_test_output_stream(vec![
        Value::Str("a".into()),
        Value::Str("b".into()),
        Value::Str("c".into()),
    ]);

    // Provide streams to handler
    handler.provide_streams(vec![stream1, stream2]);

    // Create oneshot channels for coordination
    let ready_channel1 = oneshot::channel();
    let (ready_tx1, ready_rx1) = ready_channel1.into_split();
    let ready_channel2 = oneshot::channel();
    let (ready_tx2, ready_rx2) = ready_channel2.into_split();

    // Start consumers first
    let consumer1_task = executor.spawn(consume_redis_messages(
        host_uri.clone(),
        "concurrent_topic1".to_string(),
        3,
        5000,
        ready_tx1,
    ));
    let consumer2_task = executor.spawn(consume_redis_messages(
        host_uri.clone(),
        "concurrent_topic2".to_string(),
        3,
        5000,
        ready_tx2,
    ));

    // Wait for consumers to be ready
    ready_rx1.await.unwrap();
    ready_rx2.await.unwrap();

    // Run handler
    let handler_task = handler.run();

    // Wait for completion
    let (handler_result, messages1, messages2) =
        futures::join!(handler_task, consumer1_task, consumer2_task);

    // Verify results
    handler_result?;
    let messages1 = messages1?;
    let messages2 = messages2?;

    assert_eq!(messages1.len(), 3);
    assert_eq!(messages2.len(), 3);

    assert_eq!(messages1[0], Value::Int(1));
    assert_eq!(messages1[1], Value::Int(2));
    assert_eq!(messages1[2], Value::Int(3));
    assert_eq!(messages2[0], Value::Str("a".into()));
    assert_eq!(messages2[1], Value::Str("b".into()));
    assert_eq!(messages2[2], Value::Str("c".into()));

    // Verify concurrent execution completed successfully

    Ok(())
}

#[apply(smol_test)]
async fn test_redis_output_handler_error_handling(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    // Test with invalid Redis host
    let var = VarName::new("error_var");
    let var_names = vec![var.clone()];

    let mut var_topics = BTreeMap::new();
    var_topics.insert(var.clone(), "error_topic".to_string());

    // Creating the handler should succeed even with invalid host
    let result = RedisOutputHandler::new(
        executor.clone(),
        var_names,
        "invalid-host",
        Some(9999),
        var_topics,
    );
    assert!(result.is_ok());

    Ok(())
}
