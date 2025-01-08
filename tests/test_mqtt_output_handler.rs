#![allow(warnings)]
use std::time::Duration;
use std::{future::Future, vec};

use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use trustworthiness_checker::mqtt_client::provide_mqtt_client;
use winnow::Parser;
mod lola_fixtures;
use lola_fixtures::spec_simple_add_monitor;
use paho_mqtt as mqtt;
use trustworthiness_checker::{OutputStream, Value};
mod mqtt_testcontainer;
use crate::mqtt_testcontainer::EmqxImage;

async fn get_z_outputs(client_name: String, port: u16) -> OutputStream<Value> {
    // Create a new client
    let mqtt_client = provide_mqtt_client(format!("tcp://localhost:{}", port))
        .await
        .expect("Failed to create MQTT client");

    // Try to get the messages
    let mut stream = mqtt_client.clone().get_stream(10);
    mqtt_client.subscribe("z", 1).await.unwrap();
    return Box::pin(stream.map(|msg| {
        let binding = msg.unwrap();
        let payload = binding.payload_str();
        serde_json::from_str(&payload).unwrap()
    }));
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use lola_fixtures::input_streams1;
    use testcontainers_modules::testcontainers::{
        runners::{self, AsyncRunner},
        ContainerAsync,
    };
    use trustworthiness_checker::{
        lola_specification, manual_output_handler::ManualOutputHandler,
        mqtt_input_provider::MQTTInputProvider, mqtt_output_handler::MQTTOutputHandler,
        AsyncMonitorRunner, Monitor, UntimedLolaSemantics, Value, VarName,
    };

    use super::*;

    async fn start_emqx() -> ContainerAsync<EmqxImage> {
        EmqxImage::default()
            .start()
            .await
            .expect("Failed to start EMQX test container")
    }

    #[tokio::test]
    async fn test_add_monitor_mqtt() {
        let model = lola_specification
            .parse(spec_simple_add_monitor())
            .expect("Model could not be parsed");

        // let pool = tokio::task::LocalSet::new();

        let expected_outputs = vec![Value::Int(3), Value::Int(7)];

        println!("Starting EMQX server");

        let emqx_server = start_emqx().await;
        let mqtt_port = emqx_server.get_host_port_ipv4(1883).await.unwrap();
        let outputs = get_z_outputs("z_subscriber".to_string(), mqtt_port).await;

        println!("EMQX server started");

        let mut input_streams = input_streams1();
        let spec = lola_specification(&mut spec_simple_add_monitor()).unwrap();
        let mqtt_host = format!("tcp://localhost:{}", mqtt_port);
        let mqtt_topics = spec
            .output_vars
            .iter()
            .map(|v| (v.clone(), v.0.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut output_handler =
            Box::new(MQTTOutputHandler::new(mqtt_host.as_str(), mqtt_topics).unwrap());
        let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
            spec,
            &mut input_streams,
            output_handler,
        );
        tokio::spawn(async_monitor.run());

        // Test the outputs
        let outputs = outputs.take(2).collect::<Vec<_>>().await;
        println!("Outputs: {:?}", outputs);
        assert_eq!(outputs, expected_outputs);
    }
}

/* A simple ROS publisher node which publishes a sequence of values on a topic
 * This creates a ROS node node_name which runs in a background thread
 * until all the values have been published. */
// fn dummy_publisher<T: WrappedTypesupport + 'static>(
//     node_name: String,
//     topic: String,
//     values: Vec<T>,
// ) -> impl Future<Output = ()> {
//     // Create a ROS node and publisher

//     use tokio::select;
//     let ctx = r2r::Context::create().unwrap();
//     let mut node = r2r::Node::create(ctx, &*node_name, "").unwrap();
//     let publisher = node
//         .create_publisher::<T>(&topic, r2r::QosProfile::default())
//         .unwrap();

//     async move {
//         // Cancellation token for managing the lifetime of the background task
//         let cancellation_token = CancellationToken::new();

//         // Create a drop guard to ensure the background task is not dropped
//         // until we are done
//         let _drop_guard = cancellation_token.clone().drop_guard();

//         // Spawn a background async task to run the ROS node
//         // and spin until cancelled
//         tokio::task::spawn(async move {
//             loop {
//                 select! {
//                     biased;
//                     _ = cancellation_token.cancelled() => {
//                         return;
//                     },
//                     _ = tokio::task::yield_now() => {
//                         node.spin_once(std::time::Duration::from_millis(0));
//                     },
//                 }
//             }
//         });

//         // Publish the values on the topic
//         for val in values {
//             println!("Publishing value: {:?} on topic: {}", val, topic);
//             publisher.publish(&val).unwrap();
//         }
//     }
// }

#[cfg(feature = "ros")]
#[tokio::test]
async fn test_add_monitor_ros() {
    let var_topics = json_to_mapping(
        r#"
        {
            "x": {
                "topic": "/x",
                "msg_type": "Int32"
            },
            "y": {
                "topic": "/y",
                "msg_type": "Int32"
            }
        }
        "#,
    )
    .unwrap();

    let model = lola_specification
        .parse(spec_simple_add_monitor())
        .expect("Model could not be parsed");

    // let pool = tokio::task::LocalSet::new();

    let xs = vec![Int32 { data: 1 }, Int32 { data: 2 }];
    let ys = vec![Int32 { data: 3 }, Int32 { data: 4 }];
    let zs = vec![Value::Int(4), Value::Int(6)];

    // Spawn dummy ROS publisher nodes
    tokio::spawn(dummy_publisher(
        "x_publisher".to_string(),
        "/x".to_string(),
        xs,
    ));

    tokio::spawn(dummy_publisher(
        "y_publisher".to_string(),
        "/y".to_string(),
        ys,
    ));

    // Create the ROS input provider
    let input_provider = ROSInputProvider::new(var_topics).unwrap();

    // Run the monitor
    let mut runner =
        AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(model, input_provider);

    // Test we have the expected outputs
    let outputs = runner.monitor_outputs();
    // We have to specify how many outputs we want to take as the ROS
    // topic is not assumed to tell us when it is done
    let outputs = outputs.take(zs.len()).collect::<Vec<_>>().await;
    println!("Outputs: {:?}", outputs);
    let expected_outputs = zs
        .into_iter()
        .map(|val| vec![(VarName("z".into()), val)].into_iter().collect())
        .collect::<Vec<_>>();
    assert_eq!(outputs, expected_outputs);
}
