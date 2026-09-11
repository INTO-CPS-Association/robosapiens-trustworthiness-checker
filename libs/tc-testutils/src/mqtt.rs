use crate::testcontainers::ContainerAsync;
use async_compat::Compat as TokioCompat;
use futures::StreamExt;
use futures_timeout::TimeoutFutureExt;
use serde::ser::Serialize;
use std::fmt::Debug;
use testcontainers_modules::{
    mosquitto::{self, Mosquitto},
    testcontainers::runners::AsyncRunner,
    testcontainers::{ImageExt, core::IntoContainerPort},
};
use tracing::{debug, info, instrument};
use trustworthiness_checker::{
    LocalStream, Value,
    core::JsonStreamValue,
    io::mqtt::{self, MqttMessage},
};

#[instrument(level = tracing::Level::INFO)]
pub async fn start_mqtt() -> ContainerAsync<Mosquitto> {
    let image = mosquitto::Mosquitto::default();

    ContainerAsync::new(
        TokioCompat::new(image.start())
            .timeout(std::time::Duration::from_secs(10))
            .await
            .expect("Timed out starting Mosquitto test container")
            .expect("Failed to start Mosquitto test container"),
    )
}

/// Start Mosquitto with an explicit, currently available host port mapping.
///
/// Use this when a test stops and restarts the same container: Docker may
/// allocate a different port for an automatically published port on restart.
/// Port selection and container creation cannot be atomic, so a collision
/// during that handoff selects another port and retries.
pub async fn start_mqtt_on_available_port() -> anyhow::Result<(ContainerAsync<Mosquitto>, u16)> {
    const START_ATTEMPTS: usize = 10;
    let mut last_start_error = None;

    for _ in 0..START_ATTEMPTS {
        let reservation = std::net::TcpListener::bind(("127.0.0.1", 0))?;
        let host_port = reservation.local_addr()?.port();
        drop(reservation);

        let image = mosquitto::Mosquitto::default().with_mapped_port(host_port, 1883.tcp());
        match TokioCompat::new(image.start())
            .timeout(std::time::Duration::from_secs(10))
            .await
        {
            Ok(Ok(container)) => return Ok((ContainerAsync::new(container), host_port)),
            Ok(Err(error)) => last_start_error = Some(error),
            Err(error) => return Err(anyhow::anyhow!("Timed out starting Mosquitto: {error}")),
        }
    }

    Err(anyhow::anyhow!(
        "Failed to start Mosquitto with an available fixed port after {START_ATTEMPTS} attempts: {}",
        last_start_error.expect("every start attempt records its error")
    ))
}

#[instrument(level = tracing::Level::INFO)]
pub async fn get_mqtt_outputs(topic: String, client_name: String, port: u16) -> LocalStream<Value> {
    // Create a new client
    let (mqtt_client, stream) = mqtt::connect_and_receive(&format!("tcp://localhost:{}", port))
        .await
        .expect("Failed to create MQTT client");
    info!("Received client for Z",);

    // Try to get the messages
    //let mut stream = mqtt_client.clone().get_stream(10);
    mqtt_client.subscribe(&topic, 1).await.unwrap();
    info!("Subscribed to Z outputs");
    Box::pin(stream.map(move |msg| {
        let _keep_client_alive = &mqtt_client;
        let binding = msg.expect("MQTT output receive failed");
        let payload = binding.payload;
        let res: Value = serde_json::from_str(&payload).unwrap();
        debug!(?res, topic=?binding.topic, "Received message");

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
    }))
}

/// Publishes all values from a Vec<Value>.
#[instrument(level = tracing::Level::INFO)]
pub async fn dummy_mqtt_publisher<T: Debug + Sized + Serialize + 'static>(
    client_name: String,
    topic: String,
    values: Vec<T>,
    port: u16,
) -> Result<(), anyhow::Error> {
    let len = values.len();
    publish_values(
        &client_name,
        &topic,
        futures::stream::iter(values).boxed_local(),
        len,
        port,
    )
    .await
}

/// Publishes all serializable values from an output stream.
#[instrument(level = tracing::Level::INFO, skip(values))]
pub async fn dummy_stream_mqtt_publisher<T: Debug + Sized + Serialize + 'static>(
    client_name: String,
    topic: String,
    values: LocalStream<T>,
    values_len: usize,
    port: u16,
) -> Result<(), anyhow::Error> {
    publish_values(&client_name, &topic, values, values_len, port).await
}

/// Publishes already-encoded JSON payloads without serializing the string again.
///
/// This is useful for protocol messages whose wire representation is a JSON
/// object rather than a JSON string containing an object.
pub async fn dummy_stream_mqtt_payload_publisher(
    client_name: String,
    topic: String,
    mut payloads: LocalStream<String>,
    payloads_len: usize,
    port: u16,
) -> Result<(), anyhow::Error> {
    info!(
        "Starting raw payload publisher {} for topic {} with {} payloads",
        client_name, topic, payloads_len
    );

    let mqtt_client = mqtt::connect(&format!("tcp://localhost:{port}"))
        .await
        .map_err(|error| anyhow::anyhow!("Failed to create MQTT client: {error}"))?;

    let mut index = 0;
    while let Some(payload) = payloads.next().await {
        let message = MqttMessage::new(topic.clone(), payload.clone(), 1);
        mqtt_client
            .publish(message)
            .await
            .map_err(|error| anyhow::anyhow!("Lost MQTT connection with error {error:?}"))?;
        info!(
            "Published raw payload {}/{} on topic {}: {}",
            index + 1,
            payloads_len,
            topic,
            payload
        );
        index += 1;
        smol::Timer::after(std::time::Duration::from_millis(50)).await;
    }

    mqtt_client
        .disconnect()
        .await
        .map_err(|error| anyhow::anyhow!("Failed to disconnect MQTT client: {error}"))?;
    Ok(())
}

/// Publishes values through their JSON stream codec.
#[instrument(level = tracing::Level::INFO, skip(values))]
pub async fn dummy_stream_mqtt_json_publisher<T: Debug + JsonStreamValue + 'static>(
    _client_name: String,
    topic: String,
    values: LocalStream<T>,
    values_len: usize,
    port: u16,
) -> Result<(), anyhow::Error> {
    let uri = format!("tcp://localhost:{port}");
    let mqtt_client = mqtt::connect(&uri)
        .await
        .map_err(|error| anyhow::anyhow!("Failed to create MQTT client: {error}"))?;

    let mut index = 0;
    let mut values = values;
    while let Some(value) = values.next().await {
        let payload = value
            .encode_json()
            .map_err(|error| anyhow::anyhow!("Failed to serialize value {value:?}: {error}"))?;
        mqtt_client
            .publish(MqttMessage::new(topic.clone(), payload, 1))
            .await
            .map_err(|error| anyhow::anyhow!("Lost MQTT connection with error {error:?}"))?;
        index += 1;
        smol::Timer::after(std::time::Duration::from_millis(50)).await;
    }
    info!("Finished publishing {index}/{values_len} JSON messages on topic {topic}");
    mqtt_client
        .disconnect()
        .await
        .map_err(|error| anyhow::anyhow!("Failed to disconnect MQTT client: {error}"))?;
    Ok(())
}

/// Subscribes to typed JSON stream values.
pub async fn get_mqtt_json_outputs<V: JsonStreamValue + 'static>(
    topic: String,
    _client_name: String,
    port: u16,
) -> LocalStream<V> {
    let (mqtt_client, stream) = mqtt::connect_and_receive(&format!("tcp://localhost:{port}"))
        .await
        .expect("Failed to create MQTT client");
    mqtt_client.subscribe(&topic, 1).await.unwrap();

    Box::pin(stream.map(move |message| {
        let _keep_client_alive = &mqtt_client;
        let message = message.expect("MQTT typed output receive failed");
        V::decode_mqtt_payload(message.payload.as_bytes()).expect("MQTT typed output should decode")
    }))
}

/// Generic logic for the dummy publishers
async fn publish_values<T: Debug + Sized + Serialize + 'static>(
    client_name: &str,
    topic: &str,
    mut values: LocalStream<T>,
    values_len: usize,
    port: u16,
) -> Result<(), anyhow::Error> {
    info!(
        "Starting publisher {} for topic {} with {} values",
        client_name, topic, values_len
    );

    let mqtt_client = mqtt::connect(&format!("tcp://localhost:{}", port))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create MQTT client: {}", e))?;

    let mut index = 0;
    while let Some(value) = values.next().await {
        let output_str = serde_json::to_string(&value)
            .map_err(|e| anyhow::anyhow!("Failed to serialize value {:?}: {:?}", value, e))?;

        let message = MqttMessage::new(topic.to_string(), output_str.clone(), 1);

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
                return Err(anyhow::anyhow!(
                    "Lost MQTT connection with error {:?} on topic {}.",
                    e,
                    topic
                ));
            }
        }

        // Add a small delay between publishing messages to avoid overwhelming the broker
        smol::Timer::after(std::time::Duration::from_millis(50)).await;

        index += 1;
    }

    info!(
        "Finished publishing all {} messages for topic {}",
        values_len, topic
    );

    mqtt_client
        .disconnect()
        .await
        .map_err(|error| anyhow::anyhow!("Failed to disconnect MQTT client: {error}"))?;
    Ok(())
}
