#![allow(warnings)]
use std::time::Duration;
use std::{future::Future, vec};

use futures::{StreamExt, stream};
use paho_mqtt as mqtt;
use std::pin::Pin;
use std::task;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument};
use trustworthiness_checker::io::mqtt::client::{
    provide_mqtt_client, provide_mqtt_client_with_subscription,
};
use trustworthiness_checker::lola_fixtures::spec_simple_add_monitor;
use trustworthiness_checker::{OutputStream, Value};
use winnow::Parser;

use async_once_cell::Lazy;
use testcontainers_modules::testcontainers::core::WaitFor;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, Mount};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

#[instrument(level = tracing::Level::INFO)]
async fn start_emqx() -> ContainerAsync<GenericImage> {
    let image = GenericImage::new("emqx/emqx", "5.8.3")
        .with_wait_for(WaitFor::message_on_stdout("EMQX 5.8.3 is running now!"))
        .with_exposed_port(1883_u16.tcp())
        .with_startup_timeout(Duration::from_secs(30));
    // .with_mount(Mount::bind_mount("/tmp/emqx_logs", "/opt/emqx/log"));

    image
        .start()
        .await
        .expect("Failed to start EMQX test container")
}

pub struct StartEmqx(Option<Pin<Box<dyn Future<Output = ContainerAsync<GenericImage>> + Send>>>);

impl Future for StartEmqx {
    type Output = ContainerAsync<GenericImage>;
    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> task::Poll<ContainerAsync<GenericImage>> {
        Pin::new(self.0.get_or_insert_with(|| Box::pin(start_emqx()))).poll(cx)
    }
}

pub static MQTT_TEST_CONTAINER: Lazy<ContainerAsync<GenericImage>, StartEmqx> =
    Lazy::new(StartEmqx(None));

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
    use std::{collections::BTreeMap, pin::Pin};
    use test_log::test;

    use testcontainers_modules::testcontainers::{
        ContainerAsync,
        runners::{self, AsyncRunner},
    };
    use tokio::time::sleep;
    use tracing::info_span;
    use trustworthiness_checker::{
        Monitor, Value, VarName,
        dependencies::traits::{DependencyKind, create_dependency_manager},
        io::{
            mqtt::{MQTTInputProvider, MQTTOutputHandler},
            testing::manual_output_handler::ManualOutputHandler,
        },
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
    #[test(tokio::test)]
    async fn test_add_monitor_mqtt_output() {
        let model = lola_specification
            .parse(spec_simple_add_monitor())
            .expect("Model could not be parsed");

        let expected_outputs = vec![Value::Int(3), Value::Int(7)];

        let emqx_server = Pin::new(&MQTT_TEST_CONTAINER).get().await;
        let mqtt_port = emqx_server
            .get_host_port_ipv4(1883)
            .await
            .expect("Failed to get host port for EMQX server");

        let mut input_streams = input_streams1();
        let spec = lola_specification(&mut spec_simple_add_monitor()).unwrap();
        let mqtt_host = format!("tcp://localhost:{}", mqtt_port);
        let mqtt_topics = spec
            .output_vars
            .iter()
            .map(|v| (v.clone(), format!("mqtt_output_{}", v.0.clone())))
            .collect::<BTreeMap<_, _>>();

        let outputs = get_outputs(
            "mqtt_output_z".to_string(),
            "z_subscriber".to_string(),
            mqtt_port,
        )
        .await;
        // sleep(Duration::from_secs(2)).await;

        let mut output_handler =
            Box::new(MQTTOutputHandler::new(mqtt_host.as_str(), mqtt_topics).unwrap());
        let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
            spec.clone(),
            &mut input_streams,
            output_handler,
            create_dependency_manager(DependencyKind::Empty, Box::new(spec)),
        );
        tokio::spawn(async_monitor.run());
        // Test the outputs
        let outputs = outputs.take(2).collect::<Vec<_>>().await;
        assert_eq!(outputs, expected_outputs);
    }

    #[cfg_attr(not(feature = "testcontainers"), ignore)]
    #[test(tokio::test)]
    async fn test_add_monitor_mqtt_input() {
        let model = lola_specification
            .parse(spec_simple_add_monitor())
            .expect("Model could not be parsed");

        // let pool = tokio::task::LocalSet::new();

        let xs = vec![Value::Int(1), Value::Int(2)];
        let ys = vec![Value::Int(3), Value::Int(4)];
        let zs = vec![Value::Int(4), Value::Int(6)];

        let emqx_server = MQTT_TEST_CONTAINER.get_unpin().await;

        let mqtt_port = emqx_server
            .get_host_port_ipv4(1883)
            .await
            .expect("Failed to get host port for EMQX server");

        let var_topics = [
            ("x".into(), "mqtt_input_x".to_string()),
            ("y".into(), "mqtt_input_y".to_string()),
        ]
        .into_iter()
        .collect::<BTreeMap<VarName, _>>();

        // Create the ROS input provider
        let mut input_provider = MQTTInputProvider::new(
            format!("tcp://localhost:{}", mqtt_port).as_str(),
            var_topics,
        )
        .unwrap();
        input_provider
            .started
            .wait_for(|x| info_span!("Waited for input provider started").in_scope(|| *x))
            .await;

        // Run the monitor
        let mut output_handler = ManualOutputHandler::new(vec!["z".into()]);
        let outputs = output_handler.get_output();
        let mut runner = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
            model.clone(),
            &mut input_provider,
            Box::new(output_handler),
            create_dependency_manager(DependencyKind::Empty, Box::new(model)),
        );

        tokio::spawn(runner.run());

        // Spawn dummy ROS publisher nodes
        tokio::spawn(dummy_publisher(
            "x_publisher".to_string(),
            "mqtt_input_x".to_string(),
            xs,
            mqtt_port,
        ));

        tokio::spawn(dummy_publisher(
            "y_publisher".to_string(),
            "mqtt_input_y".to_string(),
            ys,
            mqtt_port,
        ));

        // Test we have the expected outputs
        // We have to specify how many outputs we want to take as the ROS
        // topic is not assumed to tell us when it is done
        info!("Waiting for {:?} outputs", zs.len());
        let outputs = outputs.take(zs.len()).collect::<Vec<_>>().await;
        info!("Outputs: {:?}", outputs);
        let expected_outputs = zs
            .into_iter()
            .map(|val| vec![(VarName("z".into()), val)].into_iter().collect())
            .collect::<Vec<_>>();
        assert_eq!(outputs, expected_outputs);
    }

    #[cfg_attr(not(feature = "testcontainers"), ignore)]
    #[test(tokio::test)]
    async fn test_mqtt_locality_receiver() {
        println!("Starting test");
        let emqx_server = MQTT_TEST_CONTAINER.get_unpin().await;
        println!("Got EMQX server");
        let mqtt_port = emqx_server
            .get_host_port_ipv4(1883)
            .await
            .expect("Failed to get host port for EMQX server");
        let mqtt_uri = format!("tcp://localhost:{}", mqtt_port);

        let receiver = MQTTLocalityReceiver::new(mqtt_uri.clone(), "test_node".to_string());

        println!("Created receiver");

        let handle = tokio::spawn(async move {
            // Wait for the receiver to be ready
            tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
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

        handle.abort();
    }
}
