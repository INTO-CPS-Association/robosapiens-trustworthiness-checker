use std::time::Duration;

use anyhow::anyhow;
use async_stream::stream;
use async_trait::async_trait;
use futures::{FutureExt, StreamExt, stream::BoxStream};
use paho_mqtt::{self as mqtt};
use std::fmt::Debug;
use tracing::{Level, debug, info, instrument, warn};
use uuid::Uuid;

/* An interface for creating the MQTT client that can be used
 * across all whole application (i.e. sharing it between the input
 * provider and the output handler). */

#[derive(Clone, Debug)]
pub enum MqttFactory {
    Paho,
    // TODO: Impl Mock
    Mock,
}

impl MqttFactory {
    /// Connect to the MQTT broker at the given URI and return the connected client
    pub async fn connect(&self, uri: &str) -> anyhow::Result<Box<dyn MqttClient>> {
        match self {
            MqttFactory::Paho => paho::connect(uri).await,
            // TODO: Impl Mock
            MqttFactory::Mock => paho::connect(uri).await,
        }
    }

    /// Connect to the MQTT broker at the given URI and return the connected client and a stream
    /// for receiving data
    pub async fn connect_and_receive(
        &self,
        uri: &str,
        max_reconnect_attempts: u32,
    ) -> anyhow::Result<(Box<dyn MqttClient>, BoxStream<'static, MqttMessage>)> {
        match self {
            MqttFactory::Paho => paho::connect_and_receive(uri, max_reconnect_attempts).await,
            // TODO: Impl Mock
            MqttFactory::Mock => paho::connect_and_receive(uri, max_reconnect_attempts).await,
        }
    }
}

#[async_trait]
pub trait MqttClient: Send + Sync {
    async fn publish(&self, message: MqttMessage) -> anyhow::Result<()>;

    async fn reconnect(&self) -> anyhow::Result<()>;

    async fn disconnect(&self) -> anyhow::Result<()>;

    // TODO: Rewrite into taking str and iterators
    async fn subscribe(&self, topic: &String, qos: i32) -> anyhow::Result<()>;
    async fn subscribe_many(&self, topics: &Vec<String>, qos: &[i32]) -> anyhow::Result<()>;
    async fn subscribe_many_same_qos(&self, topics: &Vec<String>, qos: i32) -> anyhow::Result<()> {
        let qos_vec = vec![qos; topics.len()];
        self.subscribe_many(topics, &qos_vec).await
    }
    async fn unsubscribe_many(&self, topics: &Vec<String>) -> anyhow::Result<()>;

    fn clone_box(&self) -> Box<dyn MqttClient>;
}

impl Clone for Box<dyn MqttClient> {
    fn clone(&self) -> Box<dyn MqttClient> {
        self.clone_box()
    }
}

#[derive(Clone, Debug)]
pub struct MqttMessage {
    pub topic: String,
    pub payload: String,
    pub qos: i32,
}

impl MqttMessage {
    pub fn new(topic: String, payload: String, qos: i32) -> Self {
        Self {
            topic,
            payload,
            qos,
        }
    }
}

pub struct PahoClient {
    client: mqtt::AsyncClient,
}

mod paho {
    use super::*;

    async fn connect_impl(
        mqtt_client: mqtt::AsyncClient,
        opts: mqtt::ConnectOptions,
    ) -> Result<mqtt::AsyncClient, mqtt::Error> {
        // Try to connect to the broker
        mqtt_client.connect(opts).await.map(|_| mqtt_client)
    }

    fn new_client_impl(
        uri: &str,
    ) -> Result<(mqtt::AsyncClient, mqtt::ConnectOptions), mqtt::Error> {
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

    #[instrument(level=Level::INFO, skip(client))]
    fn message_stream(
        mut client: mqtt::AsyncClient,
        max_reconnect_attempts: u32,
    ) -> BoxStream<'static, MqttMessage> {
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
                                    let message = MqttMessage::new(
                                        message.topic().to_string(),
                                        message.payload_str().to_string(),
                                        message.qos() as i32,
                                    );
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

    pub(crate) async fn connect(uri: &str) -> anyhow::Result<Box<dyn MqttClient>> {
        let (client, opts) = new_client_impl(uri)?;
        let client = connect_impl(client, opts).await?;
        Ok(Box::new(PahoClient { client }) as Box<dyn MqttClient>)
    }

    pub(crate) async fn connect_and_receive(
        uri: &str,
        max_reconnect_attempts: u32,
    ) -> anyhow::Result<(Box<dyn MqttClient>, BoxStream<'static, MqttMessage>)> {
        let (client, opts) = new_client_impl(uri)?;
        let stream = message_stream(client.clone(), max_reconnect_attempts);
        let client = connect_impl(client, opts).await?;
        Ok((Box::new(PahoClient { client }), stream))
    }
}

#[async_trait]
impl MqttClient for PahoClient {
    async fn publish(&self, message: MqttMessage) -> anyhow::Result<()> {
        self.client
            .publish(mqtt::Message::new(
                message.topic,
                message.payload,
                message.qos,
            ))
            .await?;
        Ok(())
    }

    async fn reconnect(&self) -> anyhow::Result<()> {
        match self.client.reconnect().await {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow!("{}", e)),
        }
    }

    async fn disconnect(&self) -> anyhow::Result<()> {
        match self.client.disconnect(None).await {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow!("{}", e)),
        }
    }

    async fn subscribe(&self, topic: &String, qos: i32) -> anyhow::Result<()> {
        match self.client.subscribe(topic, qos).await {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow!("{}", e)),
        }
    }
    async fn subscribe_many(&self, topics: &Vec<String>, qos: &[i32]) -> anyhow::Result<()> {
        match self.client.subscribe_many(topics, qos).await {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow!("{}", e)),
        }
    }

    async fn unsubscribe_many(&self, topics: &Vec<String>) -> anyhow::Result<()> {
        match self.client.unsubscribe_many(topics).await {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow!("{}", e)),
        }
    }

    fn clone_box(&self) -> Box<dyn MqttClient> {
        Box::new(PahoClient {
            client: self.client.clone(),
        })
    }
}
