use std::fmt::Debug;

use crate::testcontainers::ContainerAsync;
use async_compat::Compat as TokioCompat;
use futures::StreamExt;
use futures_timeout::TimeoutExt;
use paho_mqtt as mqtt;
use serde::ser::Serialize;
use testcontainers_modules::{
    mosquitto::{self, Mosquitto},
    testcontainers::runners::AsyncRunner,
};
use tracing::{debug, info, instrument};
use trustworthiness_checker::{
    OutputStream, Value,
    io::mqtt::{provide_mqtt_client, provide_mqtt_client_with_subscription},
};

#[instrument(level = tracing::Level::INFO)]
pub async fn start_mqtt() -> ContainerAsync<Mosquitto> {
    let image = mosquitto::Mosquitto::default();

    ContainerAsync::new(
        TokioCompat::new(image.start())
            .timeout(std::time::Duration::from_secs(10))
            .await
            .expect("Timed out starting EMQX test container")
            .expect("Failed to start EMQX test container"),
    )
}

#[instrument(level = tracing::Level::INFO)]
pub async fn get_mqtt_outputs(
    topic: String,
    client_name: String,
    port: u16,
) -> OutputStream<Value> {
    // Create a new client
    let (mqtt_client, stream) =
        provide_mqtt_client_with_subscription(format!("tcp://localhost:{}", port), 0)
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
        let res: Value = serde_json::from_str(&payload).unwrap();
        debug!(?res, topic=?binding.topic(), "Received message");

        // Handle wrapped format {"value": actual_value} from output handler
        match &res {
            Value::Map(map) => {
                if let Some(actual_value) = map.get("value") {
                    actual_value.clone()
                } else {
                    res
                }
            }
            _ => res,
        }
    }));
}

/// Publishes all values from a Vec<Value>.
#[instrument(level = tracing::Level::INFO)]
pub async fn dummy_mqtt_publisher<T: Debug + Sized + Send + Serialize + 'static>(
    client_name: String,
    topic: String,
    values: Vec<T>,
    port: u16,
) {
    let len = values.len();
    publish_values(
        &client_name,
        &topic,
        futures::stream::iter(values).boxed(),
        len,
        port,
    )
    .await;
}

/// Publishes all values from an OutputStream<Value>.
#[instrument(level = tracing::Level::INFO, skip(values))]
pub async fn dummy_stream_mqtt_publisher<T: Debug + Sized + Send + Serialize + 'static>(
    client_name: String,
    topic: String,
    values: OutputStream<T>,
    values_len: usize,
    port: u16,
) {
    publish_values(&client_name, &topic, values, values_len, port).await;
}

/// Generic logic for the dummy publishers
async fn publish_values<T: Debug + Sized + Send + Serialize + 'static>(
    client_name: &str,
    topic: &str,
    mut values: OutputStream<T>,
    values_len: usize,
    port: u16,
) {
    info!(
        "Starting publisher {} for topic {} with {} values",
        client_name, topic, values_len
    );

    let mqtt_client = provide_mqtt_client(format!("tcp://localhost:{}", port))
        .await
        .expect("Failed to create MQTT client");

    let mut index = 0;
    while let Some(value) = values.next().await {
        let output_str = serde_json::to_string(&value)
            .unwrap_or_else(|e| panic!("Failed to serialize value {:?}: {:?}", value, e));

        let message = mqtt::Message::new(topic.to_string(), output_str.clone(), 1);

        info!(
            "Publishing message {}/{} on topic {}: {}",
            index + 1,
            values_len,
            topic,
            output_str
        );

        match mqtt_client.publish(message).await {
            Ok(_) => {
                info!(
                    "Successfully published message {}/{} on topic {}",
                    index + 1,
                    values_len,
                    topic
                );
            }
            Err(e) => {
                panic!(
                    "Lost MQTT connection with error {:?} on topic {}.",
                    e, topic
                );
            }
        }
        index += 1;
    }

    info!(
        "Finished publishing all {} messages for topic {}",
        values_len, topic
    );

    if let Err(e) = mqtt_client.disconnect(None).await {
        debug!("Failed to disconnect MQTT client {}: {:?}", client_name, e);
    } else {
        debug!("Successfully disconnected MQTT client {}", client_name);
    }
}
