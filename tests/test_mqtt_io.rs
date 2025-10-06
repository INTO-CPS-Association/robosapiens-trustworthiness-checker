#[cfg(test)]
#[cfg(feature = "testcontainers")]
mod integration_tests {
    use std::vec;

    use async_compat::Compat as TokioCompat;
    use futures::{FutureExt, StreamExt};
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use tc_testutils::streams::with_timeout_res;
    use tracing::info;
    use trustworthiness_checker::InputProvider;
    use trustworthiness_checker::async_test;
    use trustworthiness_checker::io::mqtt::MqttFactory;
    use trustworthiness_checker::io::mqtt::MqttMessage;
    use trustworthiness_checker::lola_fixtures::spec_simple_add_monitor;
    use winnow::Parser;

    use approx::assert_abs_diff_eq;
    use tc_testutils::streams::with_timeout;

    use std::{collections::BTreeMap, rc::Rc};
    use tc_testutils::mqtt::{dummy_mqtt_publisher, get_mqtt_outputs, start_mqtt};
    use trustworthiness_checker::distributed::locality_receiver::LocalityReceiver;
    use trustworthiness_checker::io::mqtt::MQTTLocalityReceiver;
    use trustworthiness_checker::semantics::distributed::localisation::LocalitySpec;

    use trustworthiness_checker::lola_fixtures::{TestMonitorRunner, input_streams1};
    use trustworthiness_checker::{
        Value, VarName,
        core::Runnable,
        dep_manage::interface::{DependencyKind, create_dependency_manager},
        io::{
            mqtt::{MQTTInputProvider, MQTTOutputHandler},
            testing::manual_output_handler::ManualOutputHandler,
        },
        lola_fixtures::{input_streams_float, spec_simple_add_monitor_typed_float},
        lola_specification,
    };

    const MQTT_FACTORY: MqttFactory = MqttFactory::Paho;

    #[apply(async_test)]
    async fn test_add_monitor_mqtt_output(executor: Rc<LocalExecutor<'static>>) {
        let spec = lola_specification
            .parse(spec_simple_add_monitor())
            .expect("Model could not be parsed");

        let expected_outputs = vec![Value::Int(3), Value::Int(7)];

        let mqtt_server = start_mqtt().await;
        let mqtt_port = TokioCompat::new(mqtt_server.get_host_port_ipv4(1883))
            .await
            .expect("Failed to get host port for MQTT server");

        let input_streams = input_streams1();
        let mqtt_host = "localhost";
        let mqtt_topics = spec
            .output_vars
            .iter()
            .map(|v| (v.clone(), format!("mqtt_output_{}", v)))
            .collect::<BTreeMap<_, _>>();

        let outputs = with_timeout(
            get_mqtt_outputs(
                "mqtt_output_z".to_string(),
                "z_subscriber".to_string(),
                mqtt_port,
            ),
            10,
            "get_mqtt_outputs",
        )
        .await
        .unwrap();

        let output_handler = Box::new(
            MQTTOutputHandler::new(
                executor.clone(),
                MQTT_FACTORY,
                vec!["z".into()],
                mqtt_host,
                Some(mqtt_port),
                mqtt_topics,
                vec![],
            )
            .unwrap(),
        );
        let async_monitor = TestMonitorRunner::new(
            executor.clone(),
            spec.clone(),
            Box::new(input_streams),
            output_handler,
            create_dependency_manager(DependencyKind::Empty, spec),
        );
        executor.spawn(async_monitor.run()).detach();
        // Test the outputs
        let outputs = with_timeout(outputs.take(2).collect::<Vec<_>>(), 10, "outputs.take")
            .await
            .unwrap();
        assert_eq!(outputs, expected_outputs);
    }

    #[apply(async_test)]
    async fn test_add_monitor_mqtt_output_float(executor: Rc<LocalExecutor<'static>>) {
        let spec = lola_specification
            .parse(spec_simple_add_monitor_typed_float())
            .expect("Model could not be parsed");

        let mqtt_server = start_mqtt().await;
        let mqtt_port = TokioCompat::new(mqtt_server.get_host_port_ipv4(1883))
            .await
            .expect("Failed to get host port for MQTT server");

        let input_streams = input_streams_float();
        let mqtt_host = "localhost";
        let mqtt_topics = spec
            .output_vars
            .iter()
            .map(|v| (v.clone(), format!("mqtt_output_float_{}", v)))
            .collect::<BTreeMap<_, _>>();

        let outputs = with_timeout(
            get_mqtt_outputs(
                "mqtt_output_float_z".to_string(),
                "z_float_subscriber".to_string(),
                mqtt_port,
            ),
            10,
            "get_mqtt_outputs",
        )
        .await
        .unwrap();

        let output_handler = Box::new(
            MQTTOutputHandler::new(
                executor.clone(),
                MQTT_FACTORY,
                vec!["z".into()],
                mqtt_host,
                Some(mqtt_port),
                mqtt_topics,
                vec![],
            )
            .unwrap(),
        );
        let async_monitor = TestMonitorRunner::new(
            executor.clone(),
            spec.clone(),
            Box::new(input_streams),
            output_handler,
            create_dependency_manager(DependencyKind::Empty, spec),
        );
        executor.spawn(async_monitor.run()).detach();
        // Test the outputs
        let outputs = with_timeout(outputs.take(2).collect::<Vec<_>>(), 10, "outputs.take")
            .await
            .unwrap();
        match outputs[0] {
            Value::Float(f) => assert_abs_diff_eq!(f, 3.7, epsilon = 1e-4),
            _ => panic!("Expected float"),
        }
        match outputs[1] {
            Value::Float(f) => assert_abs_diff_eq!(f, 7.7, epsilon = 1e-4),
            _ => panic!("Expected float"),
        }
    }

    #[apply(async_test)]
    async fn test_add_monitor_mqtt_input(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let model = lola_specification
            .parse(spec_simple_add_monitor())
            .expect("Model could not be parsed");

        let xs = vec![Value::Int(1), Value::Int(2)];
        let ys = vec![Value::Int(3), Value::Int(4)];
        let zs = vec![Value::Int(4), Value::Int(6)];

        let mqtt_server = start_mqtt().await;

        let mqtt_port = TokioCompat::new(mqtt_server.get_host_port_ipv4(1883))
            .await
            .expect("Failed to get host port for MQTT server");

        let var_topics = [
            ("x".into(), "mqtt_input_x".to_string()),
            ("y".into(), "mqtt_input_y".to_string()),
        ]
        .into_iter()
        .collect::<BTreeMap<VarName, _>>();

        // Create the MQTT input provider
        let mut input_provider = MQTTInputProvider::new(
            executor.clone(),
            MQTT_FACTORY,
            "localhost",
            Some(mqtt_port),
            var_topics,
            0,
        );
        with_timeout_res(input_provider.connect(), 5, "input_provider_connect").await?;

        let input_provider_ready = input_provider.ready();

        // Run the monitor
        let mut output_handler = ManualOutputHandler::new(executor.clone(), vec!["z".into()]);
        let outputs = output_handler.get_output();
        let runner = TestMonitorRunner::new(
            executor.clone(),
            model.clone(),
            Box::new(input_provider),
            Box::new(output_handler),
            create_dependency_manager(DependencyKind::Empty, model),
        );

        let res = executor.spawn(runner.run());
        with_timeout_res(input_provider_ready, 5, "input_provider_ready").await?;

        // Spawn dummy MQTT publisher nodes and keep handles to wait for completion
        let x_publisher_task = executor.spawn(dummy_mqtt_publisher(
            "x_publisher".to_string(),
            "mqtt_input_x".to_string(),
            xs,
            mqtt_port,
        ));

        let y_publisher_task = executor.spawn(dummy_mqtt_publisher(
            "y_publisher".to_string(),
            "mqtt_input_y".to_string(),
            ys,
            mqtt_port,
        ));

        // Test we have the expected outputs
        // We have to specify how many outputs we want to take as the MQTT
        // topic is not assumed to tell us when it is done
        info!("Waiting for {:?} outputs", zs.len());
        let outputs = with_timeout(
            outputs.take(zs.len()).collect::<Vec<_>>(),
            10,
            "outputs.take",
        )
        .await?;
        info!("Outputs: {:?}", outputs);
        let expected_outputs = zs.into_iter().map(|val| vec![val]).collect::<Vec<_>>();
        assert_eq!(outputs, expected_outputs);

        info!("Output collection complete, output stream should now be dropped");

        // Wait for publishers to complete and then shutdown MQTT server to terminate connections
        info!("Waiting for publishers to complete...");
        with_timeout(x_publisher_task, 5, "x_publisher_task").await?;
        with_timeout(y_publisher_task, 5, "y_publisher_task").await?;
        info!("All publishers completed, shutting down MQTT server");

        info!("Waiting for monitor to complete after output stream drop...");
        let timeout_future = smol::Timer::after(std::time::Duration::from_secs(2));

        futures::select! {
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

    #[apply(async_test)]
    async fn test_add_monitor_mqtt_input_float(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let model = lola_specification
            .parse(spec_simple_add_monitor())
            .expect("Model could not be parsed");

        let xs = vec![Value::Float(1.3), Value::Float(3.4)];
        let ys = vec![Value::Float(2.4), Value::Float(4.3)];
        let zs = vec![Value::Float(3.7), Value::Float(7.7)];

        let mqtt_server = start_mqtt().await;

        let mqtt_port = TokioCompat::new(mqtt_server.get_host_port_ipv4(1883))
            .await
            .expect("Failed to get host port for MQTT server");

        let var_topics = [
            ("x".into(), "mqtt_input_float_x".to_string()),
            ("y".into(), "mqtt_input_float_y".to_string()),
        ]
        .into_iter()
        .collect::<BTreeMap<VarName, _>>();

        // Create the MQTT input provider
        let mut input_provider = MQTTInputProvider::new(
            executor.clone(),
            MQTT_FACTORY,
            "localhost",
            Some(mqtt_port),
            var_topics,
            0,
        );
        with_timeout_res(input_provider.connect(), 10, "input_provider_connect").await?;

        let input_provider_ready = input_provider.ready();
        // Run the monitor
        let mut output_handler = ManualOutputHandler::new(executor.clone(), vec!["z".into()]);
        let outputs = output_handler.get_output();
        let runner = TestMonitorRunner::new(
            executor.clone(),
            model.clone(),
            Box::new(input_provider),
            Box::new(output_handler),
            create_dependency_manager(DependencyKind::Empty, model),
        );

        let res = executor.spawn(runner.run());
        with_timeout_res(input_provider_ready, 5, "input_provider_ready").await?;

        // Spawn dummy MQTT publisher nodes and keep handles to wait for completion
        let x_publisher_task = executor.spawn(dummy_mqtt_publisher(
            "x_publisher_float".to_string(),
            "mqtt_input_float_x".to_string(),
            xs,
            mqtt_port,
        ));

        let y_publisher_task = executor.spawn(dummy_mqtt_publisher(
            "y_publisher_float".to_string(),
            "mqtt_input_float_y".to_string(),
            ys,
            mqtt_port,
        ));

        // Test we have the expected outputs
        // We have to specify how many outputs we want to take as the MQTT
        // topic is not assumed to tell us when it is done
        info!("Waiting for {:?} outputs", zs.len());
        let outputs = with_timeout(
            outputs.take(zs.len()).collect::<Vec<_>>(),
            10,
            "outputs.take",
        )
        .await?;
        info!("Outputs: {:?}", outputs);
        // Test the outputs
        assert_eq!(outputs.len(), zs.len());
        match outputs[0][0] {
            Value::Float(f) => assert_abs_diff_eq!(f, 3.7, epsilon = 1e-4),
            _ => panic!("Expected float"),
        }
        match outputs[1][0] {
            Value::Float(f) => assert_abs_diff_eq!(f, 7.7, epsilon = 1e-4),
            _ => panic!("Expected float"),
        }

        info!("Output collection complete, output stream should now be dropped");

        // Wait for publishers to complete and then shutdown MQTT server to terminate connections
        info!("Waiting for publishers to complete...");
        with_timeout(x_publisher_task, 5, "x_publisher_task").await?;
        with_timeout(y_publisher_task, 5, "y_publisher_task").await?;
        info!("All publishers completed, shutting down MQTT server");

        info!("Waiting for monitor to complete after output stream drop...");
        let timeout_future = smol::Timer::after(std::time::Duration::from_secs(2));

        futures::select! {
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

    #[apply(async_test)]
    async fn test_mqtt_locality_receiver(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        println!("Starting test");
        let mqtt_server = start_mqtt().await;
        println!("Got MQTT server");
        let mqtt_port = TokioCompat::new(mqtt_server.get_host_port_ipv4(1883))
            .await
            .expect("Failed to get host port for MQTT server");
        let mqtt_uri = format!("tcp://localhost:{}", mqtt_port);
        let node_name = "test_node".to_string();

        // Create locality receiver and wait for it to be ready
        let locality_receiver = MQTTLocalityReceiver::new(mqtt_uri.clone(), node_name);
        let _ = with_timeout(locality_receiver.ready(), 5, "locality_receiver.ready()").await?;

        executor
            .spawn(async move {
                // Receiver is already ready, publish immediately
                println!("Receiver is ready, publishing message");

                let mqtt_client = MQTT_FACTORY
                    .connect(&mqtt_uri)
                    .await
                    .expect("Failed to create MQTT client");
                let topic = "start_monitors_at_test_node".to_string();
                let message = serde_json::to_string(&vec!["x", "y"]).unwrap();
                let message = MqttMessage::new(topic, message, 1);
                mqtt_client.publish(message).await.unwrap();
                println!("Published message");
            })
            .detach();

        // Wait for the result
        let locality_spec = with_timeout_res(
            locality_receiver.receive(),
            5,
            "locality_receiver.receive()",
        )
        .await?;
        println!("Received locality spec");

        assert_eq!(locality_spec.local_vars(), vec!["x".into(), "y".into()]);

        Ok(())
    }
}
