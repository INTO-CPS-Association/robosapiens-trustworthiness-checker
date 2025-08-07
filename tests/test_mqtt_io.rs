use std::vec;

use async_compat::Compat as TokioCompat;
use futures::{FutureExt, StreamExt};
use macro_rules_attribute::apply;
use paho_mqtt as mqtt;
use smol::LocalExecutor;
use tracing::info;
use trustworthiness_checker::async_test;
use trustworthiness_checker::io::mqtt::client::provide_mqtt_client;
use trustworthiness_checker::lola_fixtures::spec_simple_add_monitor;
use trustworthiness_checker::{InputProvider, OutputStream};
use winnow::Parser;

#[cfg(test)]
mod tests {
    use approx::assert_abs_diff_eq;

    use std::{collections::BTreeMap, rc::Rc};
    use tc_testutils::mqtt::{dummy_mqtt_publisher, get_mqtt_outputs, start_mqtt};
    use trustworthiness_checker::distributed::locality_receiver::LocalityReceiver;
    use trustworthiness_checker::io::mqtt::MQTTLocalityReceiver;
    use trustworthiness_checker::semantics::distributed::localisation::LocalitySpec;

    use trustworthiness_checker::lola_fixtures::input_streams1;
    use trustworthiness_checker::{
        Value, VarName,
        core::{OutputHandler, Runnable},
        dep_manage::interface::{DependencyKind, create_dependency_manager},
        io::{
            mqtt::{MQTTInputProvider, MQTTOutputHandler},
            testing::manual_output_handler::ManualOutputHandler,
        },
        lola_fixtures::{input_streams_float, spec_simple_add_monitor_typed_float},
        lola_specification,
        runtime::asynchronous::AsyncMonitorRunner,
        semantics::UntimedLolaSemantics,
    };

    use super::*;

    #[cfg_attr(not(feature = "testcontainers"), ignore)]
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

        let outputs = get_mqtt_outputs(
            "mqtt_output_z".to_string(),
            "z_subscriber".to_string(),
            mqtt_port,
        )
        .await;

        let output_handler = Box::new(
            MQTTOutputHandler::new(
                executor.clone(),
                vec!["z".into()],
                mqtt_host,
                Some(mqtt_port),
                mqtt_topics,
            )
            .unwrap(),
        );
        let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
            executor.clone(),
            spec.clone(),
            Box::new(input_streams),
            output_handler,
            create_dependency_manager(DependencyKind::Empty, spec),
        );
        executor.spawn(async_monitor.run()).detach();
        // Test the outputs
        let outputs = outputs.take(2).collect::<Vec<_>>().await;
        assert_eq!(outputs, expected_outputs);
    }

    #[cfg_attr(not(feature = "testcontainers"), ignore)]
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

        let outputs = get_mqtt_outputs(
            "mqtt_output_float_z".to_string(),
            "z_float_subscriber".to_string(),
            mqtt_port,
        )
        .await;

        let output_handler = Box::new(
            MQTTOutputHandler::new(
                executor.clone(),
                vec!["z".into()],
                mqtt_host,
                Some(mqtt_port),
                mqtt_topics,
            )
            .unwrap(),
        );
        let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
            executor.clone(),
            spec.clone(),
            Box::new(input_streams),
            output_handler,
            create_dependency_manager(DependencyKind::Empty, spec),
        );
        executor.spawn(async_monitor.run()).detach();
        // Test the outputs
        let outputs = outputs.take(2).collect::<Vec<_>>().await;
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
    async fn test_manual_output_handler_completion(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        use futures::stream;

        info!("Creating ManualOutputHandler with infinite input stream");
        let mut handler = ManualOutputHandler::new(executor.clone(), vec!["test".into()]);

        // Create an infinite stream
        let infinite_stream: OutputStream<Value> =
            Box::pin(stream::iter((0..).map(|x| Value::Int(x))));
        handler.provide_streams(vec![infinite_stream]);

        let output_stream = handler.get_output();
        let handler_task = executor.spawn(handler.run());

        info!("Taking only 2 items from infinite stream");
        let outputs = output_stream.take(2).collect::<Vec<_>>().await;
        info!("Collected outputs: {:?}", outputs);

        info!("Output stream dropped, waiting for handler to complete");
        let timeout_future = smol::Timer::after(std::time::Duration::from_secs(2));

        futures::select! {
            result = handler_task.fuse() => {
                info!("Handler completed: {:?}", result);
                result?;
            }
            _ = futures::FutureExt::fuse(timeout_future) => {
                return Err(anyhow::anyhow!("ManualOutputHandler did not complete after output stream was dropped"));
            }
        }

        Ok(())
    }

    #[cfg_attr(not(feature = "testcontainers"), ignore)]
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
        let input_provider = MQTTInputProvider::new(
            executor.clone(),
            "localhost",
            Some(mqtt_port),
            var_topics,
            0,
        )
        .unwrap();
        input_provider.ready().await?;

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
        let outputs = outputs.take(zs.len()).collect::<Vec<_>>().await;
        info!("Outputs: {:?}", outputs);
        let expected_outputs = zs.into_iter().map(|val| vec![val]).collect::<Vec<_>>();
        assert_eq!(outputs, expected_outputs);

        info!("Output collection complete, output stream should now be dropped");

        // Wait for publishers to complete and then shutdown MQTT server to terminate connections
        info!("Waiting for publishers to complete...");
        x_publisher_task.await;
        y_publisher_task.await;
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

    #[cfg_attr(not(feature = "testcontainers"), ignore)]
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
        let input_provider = MQTTInputProvider::new(
            executor.clone(),
            "localhost",
            Some(mqtt_port),
            var_topics,
            0,
        )
        .unwrap();
        input_provider.ready().await?;

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
        let outputs = outputs.take(zs.len()).collect::<Vec<_>>().await;
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
        x_publisher_task.await;
        y_publisher_task.await;
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

    #[cfg_attr(not(feature = "testcontainers"), ignore)]
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

        // Create oneshot channel for synchronization
        let locality_receiver = MQTTLocalityReceiver::new(mqtt_uri.clone(), node_name);
        let ready = locality_receiver.ready();

        executor
            .spawn(async move {
                // Wait for receiver to signal it's actually subscribed
                ready.await;
                println!("Receiver is subscribed, publishing message");

                let mqtt_client = provide_mqtt_client(mqtt_uri)
                    .await
                    .expect("Failed to create MQTT client");
                let topic = "start_monitors_at_test_node".to_string();
                let message = serde_json::to_string(&vec!["x", "y"]).unwrap();
                let message = mqtt::Message::new(topic, message, 1);
                mqtt_client.publish(message).await.unwrap();
                println!("Published message");
            })
            .detach();

        // Wait for the result
        let locality_spec = locality_receiver.receive().await.unwrap();
        println!("Received locality spec");

        assert_eq!(locality_spec.local_vars(), vec!["x".into(), "y".into()]);

        Ok(())
    }
}
