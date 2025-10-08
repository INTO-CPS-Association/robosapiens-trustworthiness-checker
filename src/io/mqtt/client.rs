use anyhow::anyhow;
use async_stream::stream;
use async_trait::async_trait;
use futures::{FutureExt, StreamExt, stream::BoxStream};
use paho_mqtt::{self as mqtt};
use std::fmt::Debug;
use std::time::Duration;
use tracing::{Level, debug, info, instrument, warn};
use uuid::Uuid;

/* An interface for creating the MQTT client that can be used
 * across all whole application (i.e. sharing it between the input
 * provider and the output handler). */

#[derive(Clone, Debug)]
pub enum MqttFactory {
    Paho,
    Mock,
}

impl MqttFactory {
    /// Connect to the MQTT broker at the given URI and return the connected client
    pub async fn connect(&self, uri: &str) -> anyhow::Result<Box<dyn MqttClient>> {
        info!(?uri, "Connecting to MQTT broker with factory {:?}", self);
        match self {
            MqttFactory::Paho => paho::connect(uri).await,
            MqttFactory::Mock => mock::connect(uri).await,
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
            MqttFactory::Mock => mock::connect_and_receive(uri, max_reconnect_attempts).await,
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

mod mock {
    use super::*;
    use async_stream::stream;
    use futures::{SinkExt, channel::mpsc, lock::Mutex};
    use std::{
        collections::BTreeMap,
        sync::{Arc, OnceLock},
    };

    use crate::io::mqtt::{MqttClient, MqttMessage, client::MockClient};

    /// Mock broker instance
    #[derive(Default)]
    pub struct MockBroker {
        /// Map of uri -> client_id -> map of senders that forward (data, subscribed topics, unsubscribed topics)
        clients: BTreeMap<
            String,
            BTreeMap<
                u32,
                (
                    mpsc::Sender<MqttMessage>,
                    mpsc::Sender<String>,
                    mpsc::Sender<String>,
                ),
            >,
        >,
        client_id_counter: u32,
    }

    pub type SharedBroker = Arc<Mutex<MockBroker>>;

    impl MockBroker {
        pub async fn publish(&mut self, uri: String, msg: MqttMessage) -> anyhow::Result<()> {
            let data_map = self.clients.entry(uri.clone()).or_default();

            // Drain the map to take ownership of the senders
            let old_data = std::mem::take(data_map);
            let mut new_data = BTreeMap::new();
            let x = old_data.keys().cloned().collect::<Vec<u32>>();
            info!(?x, ?msg, ?uri, "Sending to ids");

            for (id, (mut data_tx, sub_tx, unsub_tx)) in old_data {
                if data_tx.send(msg.clone()).await.is_ok() {
                    // Keep only alive channels
                    new_data.insert(id, (data_tx, sub_tx, unsub_tx));
                } else {
                    debug!("Dropping closed client in MockBroker with id {}", id);
                }
            }
            *data_map = new_data;

            Ok(())
        }

        pub(crate) async fn connect(&mut self, uri: &str) -> anyhow::Result<Box<dyn MqttClient>> {
            let id = self.client_id_counter;
            self.client_id_counter += 1;
            Ok(Box::new(MockClient {
                uri: uri.to_string(),
                id,
            }) as Box<dyn MqttClient>)
        }

        pub(crate) async fn connect_and_receive(
            &mut self,
            uri: &str,
            _max_reconnect_attempts: u32,
        ) -> anyhow::Result<(Box<dyn MqttClient>, BoxStream<'static, MqttMessage>)> {
            let id = self.client_id_counter;
            self.client_id_counter += 1;
            let client = Box::new(MockClient {
                uri: uri.to_string(),
                id,
            }) as Box<dyn MqttClient>;
            let (sub_tx, sub_rx) = mpsc::channel(512);
            let (unsub_tx, unsub_rx) = mpsc::channel(512);
            let (data_tx, data_rx) = mpsc::channel::<MqttMessage>(512);
            let res = self
                .clients
                .entry(uri.to_string())
                .or_default()
                .insert(id, (data_tx, sub_tx, unsub_tx));
            info!(?self.clients, "Clients after insertion");

            assert!(res.is_none(), "Client ID collision in mock broker");

            let stream = Box::pin(stream! {
                let mut data_rx = data_rx;
                let mut sub_rx = sub_rx;
                let mut unsub_rx = unsub_rx;
                let mut subscriptions = Vec::new();
                let id = id.clone();
                loop {
                    futures::select! {
                        msg = data_rx.next().fuse() => {
                            if let Some(msg) = msg {
                                info!(?id, ?msg, "MockBroker received message");
                                let topic = msg.topic.clone();
                                if subscriptions.iter().any(|t| t == &topic) {
                                    yield msg;
                                }
                            }
                            else {
                                info!(?id, "Closing channel in MockBrocker");
                                break; // Channel closed, exit loop
                            }
                        }
                        topic = sub_rx.next().fuse() => {
                            if let Some(topic) = topic {
                                info!(?id, ?topic, "MockBroker subscribing to topic");
                                subscriptions.push(topic);
                            }
                        }
                        topic = unsub_rx.next().fuse() => {
                            if let Some(topic) = topic {
                                info!(?id, ?topic, "MockBroker unsubscribing to topic");
                                subscriptions.retain(|t| t != &topic);
                            }
                        }
                    }
                }
            }) as BoxStream<'static, MqttMessage>;

            Ok((client, stream))
        }

        pub async fn subscribe(
            &mut self,
            uri: &String,
            topic: &String,
            id: u32,
        ) -> anyhow::Result<()> {
            let (_, sub_tx, _) = self
                .clients
                .get_mut(uri)
                .and_then(|m| m.get_mut(&id))
                .expect("Client not found in mock broker");
            sub_tx
                .send(topic.clone())
                .await
                .map_err(|e| anyhow!("{}", e))
        }

        pub async fn unsubscribe(
            &mut self,
            uri: &String,
            topic: &String,
            id: u32,
        ) -> anyhow::Result<()> {
            let (_, _, unsub_tx) = self
                .clients
                .get_mut(uri)
                .and_then(|m| m.get_mut(&id))
                .expect("Client not found in mock broker");
            unsub_tx
                .send(topic.clone())
                .await
                .map_err(|e| anyhow!("{}", e))
        }
    }

    pub fn global_broker() -> &'static SharedBroker {
        static BROKER: OnceLock<SharedBroker> = OnceLock::new();
        BROKER.get_or_init(|| Arc::new(Mutex::new(MockBroker::default())))
    }

    pub async fn connect(uri: &str) -> anyhow::Result<Box<dyn MqttClient>> {
        let mut broker = global_broker().lock().await;
        broker.connect(uri).await
    }

    pub async fn connect_and_receive(
        uri: &str,
        max_reconnect_attempts: u32,
    ) -> anyhow::Result<(Box<dyn MqttClient>, BoxStream<'static, MqttMessage>)> {
        let mut broker = global_broker().lock().await;
        broker
            .connect_and_receive(uri, max_reconnect_attempts)
            .await
    }
}

pub struct MockClient {
    uri: String,
    id: u32,
}

#[async_trait]
impl MqttClient for MockClient {
    async fn publish(&self, message: MqttMessage) -> anyhow::Result<()> {
        let mut broker = mock::global_broker().lock().await;
        broker.publish(self.uri.clone(), message).await
    }
    async fn reconnect(&self) -> anyhow::Result<()> {
        Ok(())
    }
    async fn disconnect(&self) -> anyhow::Result<()> {
        Ok(())
    }
    async fn subscribe(&self, topic: &String, _qos: i32) -> anyhow::Result<()> {
        let mut broker = mock::global_broker().lock().await;
        broker.subscribe(&self.uri, &topic, self.id).await
    }
    async fn subscribe_many(&self, topics: &Vec<String>, _qos: &[i32]) -> anyhow::Result<()> {
        let mut broker = mock::global_broker().lock().await;
        for topic in topics {
            broker.subscribe(&self.uri, topic, self.id).await?;
        }
        Ok(())
    }
    async fn unsubscribe_many(&self, topics: &Vec<String>) -> anyhow::Result<()> {
        let mut broker = mock::global_broker().lock().await;
        for topic in topics {
            broker.unsubscribe(&self.uri, topic, self.id).await?;
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn MqttClient> {
        Box::new(MockClient {
            uri: self.uri.clone(),
            id: self.id,
        })
    }
}
