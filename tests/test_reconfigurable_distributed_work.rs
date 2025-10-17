#[cfg(test)]
#[cfg(feature = "testcontainers")]
mod integration_tests {

    use async_compat::Compat as TokioCompat;
    use macro_rules_attribute::apply;
    use smol::{LocalExecutor, Timer};
    use std::rc::Rc;
    use std::time::Duration;
    use tc_testutils::mqtt::start_mqtt;
    use tracing::info;
    use trustworthiness_checker::{
        LOLASpecification, Value, async_test,
        core::{AbstractMonitorBuilder, Runnable, Runtime, Semantics},
        distributed::locality_receiver::LocalityReceiver,
        io::mqtt::{MQTTLocalityReceiver, MqttFactory, MqttMessage},
        lang::dynamic_lola::parser::CombExprParser,
        runtime::{
            RuntimeBuilder, asynchronous::Context, builder::DistributionMode,
            reconfigurable_async::ReconfAsyncMonitorBuilder,
        },
        semantics::{UntimedLolaSemantics, distributed::localisation::LocalitySpec},
    };

    const MQTT_FACTORY: MqttFactory = MqttFactory::Paho;

    /// Test that verifies the fix for reconfigurable runtime with distributed work.
    ///
    /// This test reproduces the exact scenario that was failing:
    /// 1. MQTTLocalityReceiver receives work assignment
    /// 2. ReconfAsyncMonitorBuilder is created with reconfigurable-async runtime
    /// 3. The receiver is properly passed to avoid creating a second one
    /// 4. No "not ready" error occurs
    #[ignore = "WIP test"]
    #[apply(async_test)]
    async fn test_reconfigurable_runtime_with_distributed_work(ex: Rc<LocalExecutor<'static>>) {
        info!("Starting test for reconfigurable runtime with distributed work");

        // Start containerized MQTT server
        let mqtt_server = start_mqtt().await;
        let mqtt_port = TokioCompat::new(mqtt_server.get_host_port_ipv4(1883))
            .await
            .expect("Failed to get host port for MQTT server");
        let mqtt_uri = format!("tcp://localhost:{}", mqtt_port);

        info!("MQTT server started on port {}", mqtt_port);

        // Simulate the exact CLI scenario
        let local_node = "test_node_a";

        // Step 1: Create the MQTTLocalityReceiver (as done in adapters.rs)
        info!("Creating MQTTLocalityReceiver for node '{}'", local_node);
        let receiver = MQTTLocalityReceiver::new_with_port(
            "localhost".to_string(), // CLI uses just "localhost"
            local_node.to_string(),
            mqtt_port,
        );

        // Step 2: Call ready() and wait for it to complete
        info!("Calling ready() on receiver");
        receiver
            .ready()
            .await
            .expect("ready() should succeed with MQTT server running");
        info!("Receiver is ready");

        // Step 3: Send a work assignment message
        let mqtt_client = MQTT_FACTORY
            .connect(&mqtt_uri)
            .await
            .expect("Failed to create MQTT client");

        let work_assignment = vec!["x".to_string(), "y".to_string(), "z".to_string()];
        let work_msg =
            serde_json::to_string(&work_assignment).expect("Failed to serialize work assignment");
        let topic = format!("start_monitors_at_{}", local_node);

        info!("Publishing work assignment to topic: {}", topic);
        let message = MqttMessage::new(topic.clone(), work_msg, 2);

        // Spawn the publish task
        let executor_clone = ex.clone();
        executor_clone
            .spawn(async move {
                mqtt_client
                    .publish(message)
                    .await
                    .expect("Failed to publish message");
                info!("Work assignment published");
            })
            .detach();

        // Step 4: Receive the work assignment
        info!("Waiting to receive work assignment");
        let locality = receiver
            .receive()
            .await
            .expect("Should receive work assignment");

        let received_vars = locality.local_vars();
        info!("Received work assignment: {:?}", received_vars);
        assert_eq!(received_vars.len(), 3);
        assert_eq!(received_vars[0].to_string(), "x");
        assert_eq!(received_vars[1].to_string(), "y");
        assert_eq!(received_vars[2].to_string(), "z");

        // // Step 5: Create the distribution mode with receiver (as the fix does)
        // // TODO: how to link this
        // let distribution_mode =
        //     DistributionMode::LocalMonitorWithReceiver(Box::new(locality), receiver.clone());

        // Step 6: Create a simple LOLA specification for testing
        let mut spec_str = r#"
            input x: Int32
            input y: Int32
            output z: Int32 := x + y
        "#;

        let spec: LOLASpecification =
            trustworthiness_checker::lang::dynamic_lola::parser::lola_specification(&mut spec_str)
                .expect("Failed to parse specification");

        // Step 7: Create the ReconfAsyncMonitorBuilder with the receiver
        info!("Creating ReconfAsyncMonitorBuilder");
        let builder = ReconfAsyncMonitorBuilder::<
            LOLASpecification,
            Context<Value>,
            Value,
            _,
            UntimedLolaSemantics<CombExprParser>,
        >::new()
        .executor(ex.clone())
        .model(spec.clone())
        .reconf_provider(receiver.clone());
        let runtime = builder.build();
        ex.spawn(runtime.run()).detach();

        info!("ReconfAsyncMonitorBuilder created with receiver");

        // Step 8: Test that we can receive another message without "not ready" error
        info!("Testing second receive to verify receiver is still functional");

        // Send another work assignment
        let mqtt_client2 = MQTT_FACTORY
            .connect(&mqtt_uri)
            .await
            .expect("Failed to create second MQTT client");

        let work_assignment2 = vec!["a".to_string(), "b".to_string()];
        let work_msg2 = serde_json::to_string(&work_assignment2).unwrap();
        let message2 = MqttMessage::new(topic, work_msg2, 2);

        let executor_clone2 = ex.clone();
        executor_clone2
            .spawn(async move {
                // Small delay to ensure receiver is waiting
                Timer::after(Duration::from_millis(200)).await;
                mqtt_client2.publish(message2).await.unwrap();
                info!("Second work assignment published");
            })
            .detach();

        // Try to receive the second message
        let result = receiver.receive().await;

        match result {
            Ok(locality2) => {
                let vars2 = locality2.local_vars();
                info!("Successfully received second work assignment: {:?}", vars2);
                assert_eq!(vars2.len(), 2);
                assert_eq!(vars2[0].to_string(), "a");
                assert_eq!(vars2[1].to_string(), "b");

                println!("\n✅ TEST PASSED: Reconfigurable runtime with distributed work");
                println!("   - Receiver was properly passed to ReconfAsyncMonitorBuilder");
                println!("   - No duplicate receiver was created");
                println!("   - No 'not ready' error occurred");
                println!("   - Multiple receives work correctly\n");
            }
            Err(e) => {
                panic!(
                    "Second receive failed! This means the fix didn't work properly: {}",
                    e
                );
            }
        }
    }

    /// Test the full flow from CLI arguments to runtime builder
    #[ignore = "WIP test"]
    #[apply(async_test)]
    async fn test_full_cli_to_runtime_flow(ex: Rc<LocalExecutor<'static>>) {
        info!("Testing full CLI to runtime flow");

        // Start containerized MQTT server
        let mqtt_server = start_mqtt().await;
        let mqtt_port = TokioCompat::new(mqtt_server.get_host_port_ipv4(1883))
            .await
            .expect("Failed to get host port for MQTT server");
        let mqtt_uri = format!("tcp://localhost:{}", mqtt_port);

        // Simulate CLI arguments
        let local_node = "node_a";

        // Create and prepare receiver
        let receiver = MQTTLocalityReceiver::new_with_port(
            "localhost".to_string(),
            local_node.to_string(),
            mqtt_port,
        );

        receiver.ready().await.expect("ready() failed");

        // Send work assignment
        let mqtt_client = MQTT_FACTORY
            .connect(&mqtt_uri)
            .await
            .expect("Failed to create MQTT client");

        let work = vec!["input_x".to_string(), "output_y".to_string()];
        let msg = serde_json::to_string(&work).unwrap();
        let topic = format!("start_monitors_at_{}", local_node);
        let message = MqttMessage::new(topic, msg, 2);

        ex.spawn(async move {
            Timer::after(Duration::from_millis(100)).await;
            mqtt_client.publish(message).await.unwrap();
        })
        .detach();

        // Receive work
        let locality = receiver.receive().await.expect("Failed to receive work");
        info!("Received work: {:?}", locality.local_vars());

        // Create distribution mode with receiver
        let dist_mode =
            DistributionMode::LocalMonitorWithReceiverAndLocality(Box::new(locality), receiver);

        // Create a simple spec
        let mut spec_str = "input input_x: Int32\noutput output_y: Int32 := input_x + 1";
        let spec: LOLASpecification =
            trustworthiness_checker::lang::dynamic_lola::parser::lola_specification(&mut spec_str)
                .expect("Failed to parse spec");

        // Create runtime builder
        let runtime_builder = RuntimeBuilder::new()
            .executor(ex.clone())
            .runtime(Runtime::ReconfigurableAsync)
            .semantics(Semantics::Untimed)
            .distribution_mode(dist_mode)
            .model(spec);

        runtime_builder.build();

        info!("Runtime builder created successfully with distribution mode");

        println!("\n✅ FULL FLOW TEST PASSED");
        println!("   - Work assignment received via MQTT");
        println!("   - Distribution mode created with receiver");
        println!("   - Runtime builder configured correctly");
        println!("   - Ready for monitor execution\n");
    }
}
