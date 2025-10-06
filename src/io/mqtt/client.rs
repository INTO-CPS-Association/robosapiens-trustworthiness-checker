use std::time::Duration;

use async_stream::stream;
use futures::{FutureExt, StreamExt, stream::BoxStream};
use paho_mqtt::{self as mqtt, Message};
use tracing::{Level, debug, info, instrument, warn};
use uuid::Uuid;

/* An interface for creating the MQTT client that can be used
 * across all whole application (i.e. sharing it between the input
 * provider and the output handler). */

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
                                debug!(?message, topic = message.topic(), "Received MQTT message");
                                yield message;
                                reconnect_attempts = 0; // Reset counter on successful message
                            }
                            None => {
                                debug!("MQTT connection lost, will attempt reconnect");
                                break; // Break inner loop, try reconnect
                            }
                        }
                    }
                    None => {
                        break; // Break inner loop, try reconnect
                    }
                }
            }

            // Stream exhausted, check if we should reconnect
            if max_reconnect_attempts == 0 {
                warn!("Connection lost. Reconnection disabled (max_reconnect_attempts=0), stopping MQTT stream");
                break;
            }

            warn!("Connection lost. Attempting reconnect...");
            reconnect_attempts += 1;

            if reconnect_attempts > max_reconnect_attempts {
                warn!("Max reconnection attempts ({}) reached, stopping MQTT stream", max_reconnect_attempts);
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
                            warn!(?err, attempt = reconnect_attempts, "MQTT client reconnection failed");
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
    uri: &str,
    max_reconnect_attempts: u32,
) -> Result<(mqtt::AsyncClient, BoxStream<'static, Message>), mqtt::Error> {
    let (mqtt_client, opts) = new_client(uri)?;
    let stream = message_stream(mqtt_client.clone(), max_reconnect_attempts);
    debug!(
        ?uri,
        client_id = mqtt_client.client_id(),
        "Started consuming MQTT messages",
    );
    let client = connect_impl(mqtt_client, opts).await?;
    Ok((client, stream))
}

pub async fn provide_mqtt_client(uri: &str) -> Result<mqtt::AsyncClient, mqtt::Error> {
    let (mqtt_client, opts) = new_client(uri)?;
    connect_impl(mqtt_client, opts).await
}

fn new_client(uri: &str) -> Result<(mqtt::AsyncClient, mqtt::ConnectOptions), mqtt::Error> {
    let create_opts = mqtt::CreateOptionsBuilder::new_v3()
        .server_uri(uri)
        .client_id(format!(
            "robosapiens_trustworthiness_checker_{}",
            Uuid::new_v4()
        ))
        .finalize();

    let opts = mqtt::ConnectOptionsBuilder::new_v3()
        .keep_alive_interval(Duration::from_secs(30))
        .clean_session(true)
        .finalize();

    // Error means requester has gone away - we don't care in that case
    let (client, opts) = mqtt::AsyncClient::new(create_opts).map(|client| (client, opts))?;
    debug!(?uri, client_id = client.client_id(), "Created MQTT client",);

    Ok((client, opts))
}

async fn connect_impl(
    mqtt_client: mqtt::AsyncClient,
    opts: mqtt::ConnectOptions,
) -> Result<mqtt::AsyncClient, mqtt::Error> {
    // Try to connect to the broker
    mqtt_client.connect(opts).await.map(|_| mqtt_client)
}

// FnMut(mqtt::AsyncClient, u32) -> BoxStream<'static, Message>>

// use async_trait::async_trait;
// use futures::stream::BoxStream;
//
// #[async_trait]
// pub trait MqttClient: Send + Sync + Clone + 'static {
//     async fn connect(&self) -> Result<(), mqtt::Error>;
//     async fn reconnect(&self) -> Result<(), mqtt::Error>;
//     fn get_stream(&self, capacity: usize) -> BoxStream<'static, Option<mqtt::Message>>;
// }
//
// use futures::StreamExt;
// use paho_mqtt as mqtt;
//
// #[async_trait::async_trait]
// impl MqttClient for mqtt::AsyncClient {
//     async fn connect(&self) -> Result<(), mqtt::Error> {
//         let connect_opts = mqtt::ConnectOptionsBuilder::new_v3()
//             .keep_alive_interval(std::time::Duration::from_secs(30))
//             .clean_session(true)
//             .finalize();
//         self.connect(connect_opts).await.map(|_| ())
//     }
//
//     async fn reconnect(&self) -> Result<(), mqtt::Error> {
//         self.reconnect().await.map(|_| ())
//     }
//
//     fn get_stream(&self, capacity: usize) -> BoxStream<'static, Option<mqtt::Message>> {
//         Box::pin(self.get_stream(capacity))
//     }
// }
