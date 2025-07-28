use std::time::Duration;

use async_stream::stream;
use futures::{FutureExt, StreamExt, stream::BoxStream};
use paho_mqtt::{self as mqtt, Message};
use tracing::{Level, debug, info, instrument, warn};
use uuid::Uuid;

/* An interface for creating the MQTT client lazily and sharing a single
 * instance of the client across all whole application (i.e. sharing
 * it between the input provider and the output handler). */

#[instrument(level=Level::INFO, skip(client))]
fn message_stream(
    mut client: mqtt::AsyncClient,
    max_reconnect_attempts: u32,
) -> BoxStream<'static, Message> {
    Box::pin(stream! {
        let mut reconnect_attempts = 0;

        loop {
            let mut stream = client.get_stream(10);

            // Inner loop to read from current stream
            loop {
                match stream.next().await {
                    Some(msg) => {
                        match msg {
                            Some(message) => {
                                debug!(name: "Received MQTT message", ?message, topic = message.topic());
                                yield message;
                                reconnect_attempts = 0; // Reset counter on successful message
                            }
                            None => {
                                debug!(name = "MQTT connection lost, will attempt reconnect");
                                break; // Break inner loop, try reconnect
                            }
                        }
                    }
                    None => {
                        break; // Break inner loop, try reconnect
                    }
                }
            }

            // Stream exhausted, try to reconnect
            warn!("Connection lost. Attempting reconnect...");
            reconnect_attempts += 1;

            if reconnect_attempts > max_reconnect_attempts {
                if max_reconnect_attempts == 0 {
                    warn!("Reconnection disabled (max_reconnect_attempts=0), stopping MQTT stream immediately");
                } else {
                    warn!("Max reconnection attempts ({}) reached, stopping MQTT stream", max_reconnect_attempts);
                }
                break;
            }

            // Add timeout to reconnection attempt (shorter timeout to prevent hanging)
            let reconnect_future = client.reconnect();
            let timeout_future = smol::Timer::after(Duration::from_millis(500));

            futures::select! {
                result = FutureExt::fuse(reconnect_future) => {
                    match result {
                        Ok(_) => {
                            info!("MQTT client reconnected successfully after {} attempts", reconnect_attempts);
                            continue; // Continue outer loop with new connection
                        }
                        Err(err) => {
                            warn!(name: "MQTT client reconnection failed", ?err, attempt = reconnect_attempts);
                            // Add small delay before next attempt or termination
                            smol::Timer::after(Duration::from_millis(100)).await;
                            break; // Break outer loop, terminate stream
                        }
                    }
                }
                _ = FutureExt::fuse(timeout_future) => {
                    warn!("MQTT reconnection timeout after 500ms, attempt {}/{}", reconnect_attempts, max_reconnect_attempts);
                    // Add small delay before next attempt or termination
                    smol::Timer::after(Duration::from_millis(100)).await;
                    break; // Break outer loop, terminate stream
                }
            }
        }
    })
}

pub async fn provide_mqtt_client_with_subscription(
    uri: String,
    max_reconnect_attempts: u32,
) -> Result<(mqtt::AsyncClient, BoxStream<'static, Message>), mqtt::Error> {
    let create_opts = mqtt::CreateOptionsBuilder::new_v3()
        .server_uri(uri.clone())
        .client_id(format!(
            "robosapiens_trustworthiness_checker_{}",
            Uuid::new_v4()
        ))
        .finalize();

    let connect_opts = mqtt::ConnectOptionsBuilder::new_v3()
        .keep_alive_interval(Duration::from_secs(30))
        .clean_session(false)
        .finalize();

    let mqtt_client = match mqtt::AsyncClient::new(create_opts) {
        Ok(client) => client,
        Err(e) => {
            return Err(e);
        }
    };

    debug!(
        name = "Created MQTT client",
        ?uri,
        client_id = mqtt_client.client_id()
    );

    let stream = message_stream(mqtt_client.clone(), max_reconnect_attempts);
    debug!(
        name = "Started consuming MQTT messages",
        ?uri,
        client_id = mqtt_client.client_id()
    );

    // Try to connect to the broker
    mqtt_client
        .clone()
        .connect(connect_opts)
        .await
        .map(|_| (mqtt_client, stream))
}

pub async fn provide_mqtt_client(uri: String) -> Result<mqtt::AsyncClient, mqtt::Error> {
    let create_opts = mqtt::CreateOptionsBuilder::new_v3()
        .server_uri(uri.clone())
        .client_id(format!(
            "robosapiens_trustworthiness_checker_{}",
            Uuid::new_v4()
        ))
        .finalize();

    let connect_opts = mqtt::ConnectOptionsBuilder::new_v3()
        .keep_alive_interval(Duration::from_secs(30))
        .clean_session(false)
        .finalize();

    let mqtt_client = match mqtt::AsyncClient::new(create_opts) {
        Ok(client) => client,
        Err(e) => {
            // (we don't care if the requester has gone away)
            return Err(e);
        }
    };

    debug!(
        ?uri,
        client_id = mqtt_client.client_id(),
        "Created MQTT client",
    );

    // Try to connect to the broker
    mqtt_client
        .clone()
        .connect(connect_opts)
        .await
        .map(|_| mqtt_client)
}
