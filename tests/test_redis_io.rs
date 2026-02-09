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

#[cfg(test)]
#[cfg(feature = "testcontainers")]
mod integration_tests {
    use std::collections::BTreeMap;
    use std::rc::Rc;
    use std::time::Duration;

    use async_stream::stream;
    use async_unsync::oneshot;
    use futures::FutureExt;
    use futures::StreamExt;
    use futures::stream;
    use macro_rules_attribute::apply;
    use redis::{self, AsyncTypedCommands};
    use smol::LocalExecutor;

    use smol_macros::test as smol_test;
    use tc_testutils::redis::dummy_redis_stream_sender;
    use tc_testutils::redis::{dummy_redis_receiver, dummy_redis_sender, start_redis};
    use tc_testutils::streams::TickSender;
    use tc_testutils::streams::interleave_with_constant;
    use tc_testutils::streams::receive_values_serially;
    use tc_testutils::streams::tick_stream;
    use tc_testutils::streams::with_timeout_res;
    use tracing::{debug, info};
    use trustworthiness_checker::async_test;
    use trustworthiness_checker::{
        InputProvider, OutputStream, Value, VarName,
        core::OutputHandler,
        core::REDIS_HOSTNAME,
        io::redis::{input_provider::RedisInputProvider, output_handler::RedisOutputHandler},
    };

    const X_TOPIC: &str = "x";
    const Y_TOPIC: &str = "y";

    fn generate_test_publisher_tasks(
        executor: Rc<LocalExecutor<'static>>,
        port: u16,
        xs: Vec<Value>,
        ys: Vec<Value>,
    ) -> (
        (TickSender, smol::Task<anyhow::Result<()>>),
        (TickSender, smol::Task<anyhow::Result<()>>),
    ) {
        let (x_tick, x_pub_stream) = tick_stream(stream::iter(xs.clone()).boxed());
        let (y_tick, y_pub_stream) = tick_stream(stream::iter(ys.clone()).boxed());

        // Spawn dummy MQTT publisher nodes and keep handles to wait for completion
        let x_publisher_task = executor.spawn(with_timeout_res(
            dummy_redis_stream_sender(
                REDIS_HOSTNAME,
                Some(port),
                X_TOPIC.to_string(),
                x_pub_stream,
            ),
            5,
            "x_publisher_task",
        ));

        let y_publisher_task = executor.spawn(with_timeout_res(
            dummy_redis_stream_sender(
                REDIS_HOSTNAME,
                Some(port),
                Y_TOPIC.to_string(),
                y_pub_stream,
            ),
            5,
            "y_publisher_task",
        ));

        ((x_tick, x_publisher_task), (y_tick, y_publisher_task))
    }

    /// Tests basic Redis pub/sub functionality with dummy sender and receiver.
    ///
    /// This test verifies that messages can be published to and received from
    /// Redis channels using the helper functions.
    #[apply(async_test)]
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

    /// Tests RedisInputProvider using integer values.
    #[apply(async_test)]
    async fn test_add_monitor_redis_input(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let xs = vec![Value::Int(1), Value::Int(2)];
        let ys = vec![Value::Int(3), Value::Int(4)];
        let stream_len = xs.len();

        let redis = start_redis().await;
        let redis_port = redis
            .get_host_port_ipv4(6379)
            .await
            .expect("Failed to get host port for Redis server");

        let var_topics = BTreeMap::from_iter([
            ("x".into(), X_TOPIC.to_string()),
            ("y".into(), Y_TOPIC.to_string()),
        ]);

        // Create the MQTT input provider
        let mut input_provider =
            RedisInputProvider::new(REDIS_HOSTNAME, Some(redis_port), var_topics)
                .map_err(|e| anyhow::anyhow!("Failed to create Redis input provider: {}", e))?;
        input_provider.connect().await?;

        let x_stream = input_provider
            .input_stream(&"x".into())
            .ok_or_else(|| anyhow::anyhow!("x stream unavailable"))?;
        let y_stream = input_provider
            .input_stream(&"y".into())
            .ok_or_else(|| anyhow::anyhow!("y stream unavailable"))?;

        let input_provider_ready = input_provider.ready();

        executor.spawn(input_provider.run()).detach();
        with_timeout_res(input_provider_ready, 5, "input_provider_ready").await?;

        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_test_publisher_tasks(executor.clone(), redis_port, xs.clone(), ys.clone());

        let (x_vals, y_vals) =
            receive_values_serially(&mut x_tick, &mut y_tick, x_stream, y_stream, stream_len)
                .await?;

        let exp_iter = xs.clone().into_iter().zip(ys.clone().into_iter());
        let (exp_x_vals, exp_y_vals) = interleave_with_constant(exp_iter, Value::NoVal);
        info!(?x_vals, ?y_vals, "Received values");
        assert_eq!(x_vals, exp_x_vals);
        assert_eq!(y_vals, exp_y_vals);

        // Final ticks to let them complete
        x_tick.send(()).await?;
        y_tick.send(()).await?;
        // Wait for publishers to complete and then shutdown MQTT server to terminate connections
        info!("Waiting for publishers to complete...");
        x_publisher_task.await?;
        y_publisher_task.await?;
        info!("All publishers completed, shutting down MQTT server");

        Ok(())
    }

    /// Tests RedisInputProvider using float values.
    /// Similar to the integer test, but uses float values to verify that the RedisInputProvider
    /// correctly handles different data types.
    #[apply(async_test)]
    async fn test_add_monitor_redis_input_float(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let xs = vec![Value::Float(1.5), Value::Float(2.5)];
        let ys = vec![Value::Float(3.5), Value::Float(4.5)];
        let stream_len = xs.len();

        let redis = start_redis().await;
        let redis_port = redis
            .get_host_port_ipv4(6379)
            .await
            .expect("Failed to get host port for Redis server");

        let var_topics = BTreeMap::from_iter([
            ("x".into(), X_TOPIC.to_string()),
            ("y".into(), Y_TOPIC.to_string()),
        ]);

        // Create the MQTT input provider
        let mut input_provider =
            RedisInputProvider::new(REDIS_HOSTNAME, Some(redis_port), var_topics)
                .map_err(|e| anyhow::anyhow!("Failed to create Redis input provider: {}", e))?;
        input_provider.connect().await?;

        let x_stream = input_provider
            .input_stream(&"x".into())
            .ok_or_else(|| anyhow::anyhow!("x stream unavailable"))?;
        let y_stream = input_provider
            .input_stream(&"y".into())
            .ok_or_else(|| anyhow::anyhow!("y stream unavailable"))?;

        let input_provider_ready = input_provider.ready();

        executor.spawn(input_provider.run()).detach();
        with_timeout_res(input_provider_ready, 5, "input_provider_ready").await?;

        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_test_publisher_tasks(executor.clone(), redis_port, xs.clone(), ys.clone());

        let (x_vals, y_vals) =
            receive_values_serially(&mut x_tick, &mut y_tick, x_stream, y_stream, stream_len)
                .await?;

        let exp_iter = xs.clone().into_iter().zip(ys.clone().into_iter());
        let (exp_x_vals, exp_y_vals) = interleave_with_constant(exp_iter, Value::NoVal);
        info!(?x_vals, ?y_vals, "Received values");
        assert_eq!(x_vals, exp_x_vals);
        assert_eq!(y_vals, exp_y_vals);

        // Final ticks to let them complete
        x_tick.send(()).await?;
        y_tick.send(()).await?;
        // Wait for publishers to complete and then shutdown MQTT server to terminate connections
        info!("Waiting for publishers to complete...");
        x_publisher_task.await?;
        y_publisher_task.await?;
        info!("All publishers completed, shutting down MQTT server");

        Ok(())
    }

    #[apply(async_test)]
    async fn test_add_monitor_redis_input_mixed(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let xs = vec![Value::Int(42), Value::Int(69)];
        let ys = vec![Value::Str("Hello".into()), Value::Str("World".into())];
        let stream_len = xs.len();

        let redis = start_redis().await;
        let redis_port = redis
            .get_host_port_ipv4(6379)
            .await
            .expect("Failed to get host port for Redis server");

        let var_topics = BTreeMap::from_iter([
            ("x".into(), X_TOPIC.to_string()),
            ("y".into(), Y_TOPIC.to_string()),
        ]);

        // Create the MQTT input provider
        let mut input_provider =
            RedisInputProvider::new(REDIS_HOSTNAME, Some(redis_port), var_topics)
                .map_err(|e| anyhow::anyhow!("Failed to create Redis input provider: {}", e))?;
        input_provider.connect().await?;

        let x_stream = input_provider
            .input_stream(&"x".into())
            .ok_or_else(|| anyhow::anyhow!("x stream unavailable"))?;
        let y_stream = input_provider
            .input_stream(&"y".into())
            .ok_or_else(|| anyhow::anyhow!("y stream unavailable"))?;

        let input_provider_ready = input_provider.ready();

        executor.spawn(input_provider.run()).detach();
        with_timeout_res(input_provider_ready, 5, "input_provider_ready").await?;

        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_test_publisher_tasks(executor.clone(), redis_port, xs.clone(), ys.clone());

        let (x_vals, y_vals) =
            receive_values_serially(&mut x_tick, &mut y_tick, x_stream, y_stream, stream_len)
                .await?;

        let exp_iter = xs.clone().into_iter().zip(ys.clone().into_iter());
        let (exp_x_vals, exp_y_vals) = interleave_with_constant(exp_iter, Value::NoVal);
        info!(?x_vals, ?y_vals, "Received values");
        assert_eq!(x_vals, exp_x_vals);
        assert_eq!(y_vals, exp_y_vals);

        // Final ticks to let them complete
        x_tick.send(()).await?;
        y_tick.send(()).await?;
        // Wait for publishers to complete and then shutdown MQTT server to terminate connections
        info!("Waiting for publishers to complete...");
        x_publisher_task.await?;
        y_publisher_task.await?;
        info!("All publishers completed, shutting down MQTT server");

        Ok(())
    }

    // TODO: TW - Where does the tests below belong? They seem misplaced to me.
    #[apply(async_test)]
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
    #[apply(async_test)]
    async fn test_serialization_edge_cases(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
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
    #[apply(async_test)]
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
            (Value::Int(123), "123"),
            (Value::Float(45.67), "45.67"),
            (Value::Str("test".into()), "\"test\""),
            (Value::Bool(true), "true"),
            (Value::Bool(false), "false"),
            (Value::Unit, "null"),
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
    #[apply(async_test)]
    async fn test_json_interoperability(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let redis = start_redis().await;
        let redis_port = redis
            .get_host_port_ipv4(6379)
            .await
            .expect("Failed to get host port for Redis server");
        let host_uri = format!("redis://127.0.0.1:{}", redis_port);

        // Test cases: manually crafted enum format JSON and expected Value
        let json_test_cases = vec![
            ("42", Value::Int(42)),
            ("3.14159", Value::Float(3.14159)),
            ("\"Hello, Redis!\"", Value::Str("Hello, Redis!".into())),
            ("true", Value::Bool(true)),
            ("false", Value::Bool(false)),
            ("null", Value::Unit),
            (
                "[1,\"two\",true,4.5]",
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
                "[[1,2],[\"a\",\"b\"]]",
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
            vec![],
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
            vec![],
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
            vec![],
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
            vec![],
        )?;

        // Create test output streams
        let stream1 = create_test_output_stream(vec![Value::Int(1), Value::Int(2)]);
        let stream2 =
            create_test_output_stream(vec![Value::Str("a".into()), Value::Str("b".into())]);
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
            vec![],
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
            vec![],
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
            vec![],
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
            vec![],
        );
        assert!(result.is_ok());

        Ok(())
    }
}
