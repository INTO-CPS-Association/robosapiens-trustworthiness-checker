#![allow(warnings)]
use std::thread::spawn;
use std::time::Duration;
use std::{future::Future, vec};

use futures::{StreamExt, stream};
use macro_rules_attribute::apply;
use paho_mqtt as mqtt;
use smol::LocalExecutor;
use smol_macros::test as smol_test;
use std::pin::Pin;
use std::{mem, task};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument};
use trustworthiness_checker::io::mqtt::client::{
    provide_mqtt_client, provide_mqtt_client_with_subscription,
};
use trustworthiness_checker::lola_fixtures::spec_simple_add_monitor;
use trustworthiness_checker::{OutputStream, Value};
use winnow::Parser;

use async_compat::Compat as TokioCompat;
use async_compat::CompatExt;
use async_once_cell::Lazy;
use testcontainers_modules::mosquitto::{self, Mosquitto};
use testcontainers_modules::testcontainers::core::WaitFor;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, Mount};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{
    ContainerAsync as ContainerAsyncTokio, GenericImage, Image, ImageExt,
};
use tokio::sync::oneshot;

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
    let (mqtt_client, mut stream) =
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
    use futures::executor;
    use std::{collections::BTreeMap, pin::Pin, rc::Rc};
    use test_log::test;

    use testcontainers_modules::testcontainers::{
        ContainerAsync,
        runners::{self, AsyncRunner},
    };
    use tokio;
    use tokio::time::sleep;
    use tracing::info_span;
    use trustworthiness_checker::{
        Monitor, Value, VarName,
        core::Runnable,
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
    use trustworthiness_checker::{
        distributed::locality_receiver::LocalityReceiver,
        io::mqtt::MQTTLocalityReceiver,
        lola_fixtures::{
            input_streams1, spec_simple_add_decomposed_1, spec_simple_add_decomposed_2,
        },
        semantics::distributed::localisation::LocalitySpec,
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

        let mut input_streams = input_streams1();
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

        let mut output_handler = Box::new(
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

        let mut input_streams = input_streams_float();
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

        let mut output_handler = Box::new(
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

    #[cfg_attr(not(feature = "testcontainers"), ignore)]
    #[test(apply(smol_test))]
    async fn test_add_monitor_mqtt_input(executor: Rc<LocalExecutor<'static>>) {
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
        let mut input_provider = MQTTInputProvider::new(
            executor.clone(),
            format!("tcp://localhost:{}", mqtt_port).as_str(),
            var_topics,
        )
        .unwrap();
        input_provider
            .started
            .wait_for(|x| info_span!("Waited for input provider started").in_scope(|| *x))
            .await;

        // Run the monitor
        let mut output_handler = ManualOutputHandler::new(executor.clone(), vec!["z".into()]);
        let outputs = output_handler.get_output();
        let mut runner = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
            executor.clone(),
            model.clone(),
            Box::new(input_provider),
            Box::new(output_handler),
            create_dependency_manager(DependencyKind::Empty, model),
        );

        executor.spawn(runner.run()).detach();

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
    }

    #[cfg_attr(not(feature = "testcontainers"), ignore)]
    #[test(apply(smol_test))]
    async fn test_add_monitor_mqtt_input_float(executor: Rc<LocalExecutor<'static>>) {
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
        let mut input_provider = MQTTInputProvider::new(
            executor.clone(),
            format!("tcp://localhost:{}", mqtt_port).as_str(),
            var_topics,
        )
        .unwrap();
        input_provider
            .started
            .wait_for(|x| info_span!("Waited for input provider started").in_scope(|| *x))
            .await;

        // Run the monitor
        let mut output_handler = ManualOutputHandler::new(executor.clone(), vec!["z".into()]);
        let outputs = output_handler.get_output();
        let mut runner = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
            executor.clone(),
            model.clone(),
            Box::new(input_provider),
            Box::new(output_handler),
            create_dependency_manager(DependencyKind::Empty, model),
        );

        executor.spawn(runner.run()).detach();

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
    }

    #[cfg_attr(not(feature = "testcontainers"), ignore)]
    #[test(apply(smol_test))]
    async fn test_mqtt_locality_receiver(executor: Rc<LocalExecutor<'static>>) {
        println!("Starting test");
        let mqtt_server = start_mqtt().await;
        println!("Got MQTT server");
        let mqtt_port = TokioCompat::new(mqtt_server.get_host_port_ipv4(1883))
            .await
            .expect("Failed to get host port for MQTT server");
        let mqtt_uri = format!("tcp://localhost:{}", mqtt_port);

        let receiver = MQTTLocalityReceiver::new(mqtt_uri.clone(), "test_node".to_string());

        println!("Created receiver");

        let handle = executor.spawn(async move {
            // Wait for the receiver to be ready
            smol::Timer::after(std::time::Duration::from_millis(300)).await;
            let mqtt_client = provide_mqtt_client(mqtt_uri)
                .await
                .expect("Failed to create MQTT client");
            let topic = "start_monitors_at_test_node".to_string();
            let message = serde_json::to_string(&vec!["x", "y"]).unwrap();
            let message = mqtt::Message::new(topic, message, 1);
            mqtt_client.publish(message).await.unwrap();
            println!("Published message");
        });

        println!("Awaiting locality spec");

        let locality_spec = receiver.receive().await.unwrap();
        println!("Received locality spec");

        let local_vars = locality_spec.local_vars();

        assert_eq!(local_vars, vec!["x".into(), "y".into()]);
    }
}
