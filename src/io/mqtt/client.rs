use anyhow::anyhow;
use async_stream::stream;
use async_trait::async_trait;
use futures::{StreamExt, stream::BoxStream};
use paho_mqtt::{self as mqtt};
use std::fmt::Debug;
use std::time::Duration;
use tracing::{Level, debug, info, instrument};
use uuid::Uuid;

/* An interface for creating the MQTT client that can be used
 * across all whole application (i.e. sharing it between the input
 * provider and the output handler). */

#[derive(Clone, Debug)]
pub enum MqttFactory {
    Paho,
}

impl MqttFactory {
    /// Connect to the MQTT broker at the given URI and return the connected client
    pub async fn connect(&self, uri: &str) -> anyhow::Result<Box<dyn MqttClient>> {
        info!(?uri, "Connecting to MQTT broker with factory {:?}", self);
        match self {
            MqttFactory::Paho => paho::connect(uri).await,
        }
    }

    /// Connect to the MQTT broker at the given URI and return the connected client and a stream
    /// for receiving data
    pub async fn connect_and_receive(
        &self,
        uri: &str,
        max_reconnect_attempts: u32,
    ) -> anyhow::Result<(Box<dyn MqttClient>, BoxStream<'static, MqttMessage>)> {
        info!(
            ?uri,
            "Connecting and receiving to MQTT broker with factory {:?}", self
        );
        match self {
            MqttFactory::Paho => paho::connect_and_receive(uri, max_reconnect_attempts).await,
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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
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
            .clean_session(false)
            .automatic_reconnect(Duration::from_millis(500), Duration::from_secs(60))
            .finalize();

        // Error means requester has gone away - we don't care in that case
        let (client, opts) = mqtt::AsyncClient::new(create_opts).map(|client| (client, opts))?;
        debug!(?uri, client_id = client.client_id(), "Created MQTT client",);

        Ok((client, opts))
    }

    #[instrument(level=Level::INFO, skip(client))]
    fn message_stream(
        mut client: mqtt::AsyncClient,
        _max_reconnect_attempts: u32,
    ) -> BoxStream<'static, MqttMessage> {
        // Note: Important that we call get_stream before the async block, otherwise
        // we risk MQTT client receiving messages before stream is registered
        let stream = client.get_stream(10);
        Box::pin(stream! {
            let mut stream = stream;
            while let Some(msg) = stream.next().await {
                match msg {
                    Some(message) => {
                        debug!(?message, topic = message.topic(), "Received MQTT message");
                        let message = MqttMessage::new(
                            message.topic().to_string(),
                            message.payload_str().to_string(),
                            message.qos() as i32,
                        );
                        yield message;
                    }
                    None => {
                        debug!("MQTT connection lost, waiting for auto-reconnect");
                        smol::Timer::after(Duration::from_millis(200)).await;
                        stream = client.get_stream(10);
                    }
                }
            }
            debug!("MQTT stream ended permanently. Disconnecting.");
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
