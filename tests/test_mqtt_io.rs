use std::vec;

use futures::{FutureExt, StreamExt};
use macro_rules_attribute::apply;
use paho_mqtt as mqtt;
use smol::LocalExecutor;
use smol_macros::test as smol_test;
use std::mem;
use tracing::{debug, info, instrument};
use trustworthiness_checker::io::mqtt::client::{
    provide_mqtt_client, provide_mqtt_client_with_subscription,
};
use trustworthiness_checker::lola_fixtures::spec_simple_add_monitor;
use trustworthiness_checker::{InputProvider, OutputStream, Value};
use winnow::Parser;

use async_compat::Compat as TokioCompat;
use testcontainers_modules::mosquitto::{self, Mosquitto};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync as ContainerAsyncTokio, Image};

struct ContainerAsync<T: Image> {
    inner: Option<ContainerAsyncTokio<T>>,
}

impl<T: Image> ContainerAsync<T> {
    fn new(inner: ContainerAsyncTokio<T>) -> Self {
        Self { inner: Some(inner) }
    }

    async fn get_host_port_ipv4(
        &self,
        port: u16,
    ) -> Result<u16, testcontainers_modules::testcontainers::TestcontainersError> {
        TokioCompat::new(self.inner.as_ref().unwrap().get_host_port_ipv4(port)).await
    }
}

impl<T: Image> Drop for ContainerAsync<T> {
    fn drop(&mut self) {
        let inner = mem::take(&mut self.inner);
        TokioCompat::new(async move { mem::drop(inner) });
    }
}

#[instrument(level = tracing::Level::INFO)]
async fn start_mqtt() -> ContainerAsync<Mosquitto> {
    let image = mosquitto::Mosquitto::default();

    ContainerAsync::new(
        TokioCompat::new(image.start())
            .await
            .expect("Failed to start EMQX test container"),
    )
}

#[instrument(level = tracing::Level::INFO)]
async fn get_outputs(topic: String, client_name: String, port: u16) -> OutputStream<Value> {
    // Create a new client
    let (mqtt_client, stream) =
        provide_mqtt_client_with_subscription(format!("tcp://localhost:{}", port))
            .await
            .expect("Failed to create MQTT client");
    info!(
        "Received client for Z with client_id: {:?}",
        mqtt_client.client_id()
    );

    // Try to get the messages
    //let mut stream = mqtt_client.clone().get_stream(10);
    mqtt_client.subscribe(topic, 1).await.unwrap();
    info!("Subscribed to Z outputs");
    return Box::pin(stream.map(|msg| {
        let binding = msg;
        let payload = binding.payload_str();
        let res = serde_json::from_str(&payload).unwrap();
        debug!(name:"Received message", ?res, topic=?binding.topic());
        res
    }));
}

#[instrument(level = tracing::Level::INFO)]
async fn dummy_publisher(client_name: String, topic: String, values: Vec<Value>, port: u16) {
    // Create a new client
    let mqtt_client = provide_mqtt_client(format!("tcp://localhost:{}", port))
        .await
        .expect("Failed to create MQTT client");

    // Try to send the messages
    let mut output_strs = values.iter().map(|val| serde_json::to_string(val).unwrap());
    while let Some(output_str) = output_strs.next() {
        let message = mqtt::Message::new(topic.clone(), output_str.clone(), 1);
        match mqtt_client.publish(message.clone()).await {
            Ok(_) => {
                debug!(name: "Published message", ?message, topic=?message.topic());
            }
            Err(e) => {
                panic!("Lost MQTT connection with error {:?}.", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_abs_diff_eq;
    use std::{collections::BTreeMap, rc::Rc};
    use test_log::test;
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
    #[test(apply(smol_test))]
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
        let mqtt_host = format!("tcp://localhost:{}", mqtt_port);
        let mqtt_topics = spec
            .output_vars
            .iter()
            .map(|v| (v.clone(), format!("mqtt_output_{}", v)))
            .collect::<BTreeMap<_, _>>();

        let outputs = get_outputs(
            "mqtt_output_z".to_string(),
            "z_subscriber".to_string(),
            mqtt_port,
        )
        .await;
        // sleep(Duration::from_secs(2)).await;

        let output_handler = Box::new(
            MQTTOutputHandler::new(
                executor.clone(),
                vec!["z".into()],
                mqtt_host.as_str(),
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
    #[test(apply(smol_test))]
    async fn test_add_monitor_mqtt_output_float(executor: Rc<LocalExecutor<'static>>) {
        let spec = lola_specification
            .parse(spec_simple_add_monitor_typed_float())
            .expect("Model could not be parsed");

        let mqtt_server = start_mqtt().await;
        let mqtt_port = TokioCompat::new(mqtt_server.get_host_port_ipv4(1883))
            .await
            .expect("Failed to get host port for MQTT server");

        let input_streams = input_streams_float();
        let mqtt_host = format!("tcp://localhost:{}", mqtt_port);
        let mqtt_topics = spec
            .output_vars
            .iter()
            .map(|v| (v.clone(), format!("mqtt_output_float_{}", v)))
            .collect::<BTreeMap<_, _>>();

        let outputs = get_outputs(
            "mqtt_output_float_z".to_string(),
            "z_float_subscriber".to_string(),
            mqtt_port,
        )
        .await;
        // sleep(Duration::from_secs(2)).await;

        let output_handler = Box::new(
            MQTTOutputHandler::new(
                executor.clone(),
                vec!["z".into()],
                mqtt_host.as_str(),
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

    #[test(apply(smol_test))]
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
    #[test(apply(smol_test))]
    async fn test_add_monitor_mqtt_input(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let model = lola_specification
            .parse(spec_simple_add_monitor())
            .expect("Model could not be parsed");

        // let pool = tokio::task::LocalSet::new();

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

        // Create the ROS input provider
        let input_provider = MQTTInputProvider::new(
            executor.clone(),
            format!("tcp://localhost:{}", mqtt_port).as_str(),
            var_topics,
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

        // Spawn dummy MQTT publisher nodes
        executor
            .spawn(dummy_publisher(
                "x_publisher".to_string(),
                "mqtt_input_x".to_string(),
                xs,
                mqtt_port,
            ))
            .detach();

        executor
            .spawn(dummy_publisher(
                "y_publisher".to_string(),
                "mqtt_input_y".to_string(),
                ys,
                mqtt_port,
            ))
            .detach();

        // Test we have the expected outputs
        // We have to specify how many outputs we want to take as the ROS
        // topic is not assumed to tell us when it is done
        info!("Waiting for {:?} outputs", zs.len());
        let outputs = outputs.take(zs.len()).collect::<Vec<_>>().await;
        info!("Outputs: {:?}", outputs);
        let expected_outputs = zs.into_iter().map(|val| vec![val]).collect::<Vec<_>>();
        assert_eq!(outputs, expected_outputs);

        info!("Output collection complete, output stream should now be dropped");

        info!("Waiting for monitor to complete after output stream drop...");
        let timeout_future = smol::Timer::after(std::time::Duration::from_secs(5));

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
    #[test(apply(smol_test))]
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

        // Create the ROS input provider
        let input_provider = MQTTInputProvider::new(
            executor.clone(),
            format!("tcp://localhost:{}", mqtt_port).as_str(),
            var_topics,
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

        // Spawn dummy MQTT publisher nodes
        executor
            .spawn(dummy_publisher(
                "x_publisher_float".to_string(),
                "mqtt_input_float_x".to_string(),
                xs,
                mqtt_port,
            ))
            .detach();

        executor
            .spawn(dummy_publisher(
                "y_publisher_float".to_string(),
                "mqtt_input_float_y".to_string(),
                ys,
                mqtt_port,
            ))
            .detach();

        // Test we have the expected outputs
        // We have to specify how many outputs we want to take as the ROS
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

        info!("Waiting for monitor to complete after output stream drop...");
        let timeout_future = smol::Timer::after(std::time::Duration::from_secs(5));

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
    #[test(apply(smol_test))]
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
