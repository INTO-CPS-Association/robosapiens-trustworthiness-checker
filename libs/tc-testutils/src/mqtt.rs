use crate::testcontainers::ContainerAsync;
use async_compat::Compat as TokioCompat;
use futures::StreamExt;
use paho_mqtt as mqtt;
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
            .await
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
        let res = serde_json::from_str(&payload).unwrap();
        debug!(name:"Received message", ?res, topic=?binding.topic());
        res
    }));
}

#[instrument(level = tracing::Level::INFO)]
pub async fn dummy_mqtt_publisher(
    client_name: String,
    topic: String,
    values: Vec<Value>,
    port: u16,
) {
    info!(
        "Starting publisher {} for topic {} with {} values",
        client_name,
        topic,
        values.len()
    );

    // Create a new client
    let mqtt_client = provide_mqtt_client(format!("tcp://localhost:{}", port))
        .await
        .expect("Failed to create MQTT client");

    // Try to send the messages
    for (index, value) in values.iter().enumerate() {
        let output_str = match serde_json::to_string(value) {
            Ok(s) => s,
            Err(e) => {
                panic!("Failed to serialize value {:?}: {:?}", value, e);
            }
        };

        let message = mqtt::Message::new(topic.clone(), output_str.clone(), 1);
        info!(
            "Publishing message {}/{} on topic {}: {}",
            index + 1,
            values.len(),
            topic,
            output_str
        );

        match mqtt_client.publish(message.clone()).await {
            Ok(_) => {
                info!(
                    "Successfully published message {}/{} on topic {}",
                    index + 1,
                    values.len(),
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
    }

    info!(
        "Finished publishing all {} messages for topic {}",
        values.len(),
        topic
    );

    // Clean up the MQTT client to prevent resource contention
    if let Err(e) = mqtt_client.disconnect(None).await {
        debug!("Failed to disconnect MQTT client {}: {:?}", client_name, e);
    } else {
        debug!("Successfully disconnected MQTT client {}", client_name);
    }
}
