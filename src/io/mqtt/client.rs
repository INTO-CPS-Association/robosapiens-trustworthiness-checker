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
        path::PathBuf,
        sync::{Arc, OnceLock},
    };
    use std::{
        fs::{File, OpenOptions},
        io::{BufRead, BufReader, Seek, SeekFrom, Write},
        path::Path,
    };
    use tracing::error;

    use crate::io::mqtt::{MqttClient, MqttMessage, client::MockClient};

    /// Mock broker instance
    /// NOTE: There is an inherent starvation issue with this design.
    /// The more clients a broker has, the larger the chance of starvation
    #[derive(Default)]
    pub struct MockBroker {
        /// Map of uri -> client_id -> map of senders that forward (subscribed topics, unsubscribed topics)
        clients: BTreeMap<String, BTreeMap<u32, (mpsc::Sender<String>, mpsc::Sender<String>)>>,
        client_id_counter: u32,
    }

    pub type SharedBroker = Arc<Mutex<MockBroker>>;

    // We need to use a file to store the published messages because
    // we are running from multiple crates which means a static global variable is insufficient.
    // This also allows us to use the mock broker across multiple processes, such as with CLI tests.
    const DATA_FILE: &str = "/tmp/tc_mock_mqtt.json";

    /// Repeatedly tries to open and lock a file.
    async fn file_exclusive(
        max_attempts: u32,
        path: &Path,
        open_options: &OpenOptions,
    ) -> anyhow::Result<File> {
        debug!(
            ?path,
            ?max_attempts,
            ?open_options,
            "Reading exclusive from file"
        );
        let mut attempts = 0;
        while attempts <= max_attempts {
            match open_options.open(path) {
                Ok(f) => {
                    if let Err(_) = fs2::FileExt::try_lock_exclusive(&f) {
                        attempts += 1;
                        debug!(?path, ?attempts, "Attempted to acquire file lock");
                        smol::Timer::after(Duration::from_millis(10)).await;
                    } else {
                        return Ok(f);
                    }
                }
                Err(e) => return Err(anyhow!(e).context("Failed to open file")),
            }
        }

        Err(anyhow!(
            "Failed to acquire file lock for {:?} after {} attempts",
            path,
            max_attempts
        ))
    }

    /// Opens a file in read-only mode with a **shared lock**.
    pub async fn read_file_exclusive(max_attempts: u32, path: &Path) -> anyhow::Result<File> {
        debug!(?path, ?max_attempts, "Reading from file");
        let mut opts = OpenOptions::new();
        // Not using create(true) here because some systems allow concurrent read access
        // while having write permissions (which create requires) is one process at a time
        if !path.exists() {
            File::create(path)?;
        }
        opts.read(true);
        file_exclusive(max_attempts, path, &opts).await
    }

    /// Opens a file for reading and writing with an **exclusive lock**.
    pub async fn append_file_exclusive(max_attempts: u32, path: &Path) -> anyhow::Result<File> {
        let mut opts = OpenOptions::new();
        opts.create(true).append(true);
        file_exclusive(max_attempts, path, &opts).await
    }

    /// Tails a file for new lines, yielding each new line as it is added.
    pub fn tail_new_lines_exclusive(
        path: String,
        max_attempts: u32,
    ) -> BoxStream<'static, anyhow::Result<String>> {
        Box::pin(stream! {
            let path = PathBuf::from(path);
            // Start by getting an initial position to discard old data
            let mut pos = {
                let file = read_file_exclusive(max_attempts, &path).await;
                match file {
                    Ok(mut file) => {
                        match file.seek(SeekFrom::End(0)) {
                            Ok(p) => p,
                            Err(e) => {
                                yield Err(anyhow!(e).context("Failed to seek to end of file"));
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        yield Err(anyhow!(e).context("Failed to open file for reading"));
                        return;
                    }
                }
            };

            // Actual tail loop
            loop {
                // Wait without keeping the lock to prevent starvation
                smol::Timer::after(Duration::from_millis(100)).await;
                let file = read_file_exclusive(max_attempts, &path).await;
                match file {
                    // Read and yield all the lines since pos and update pos
                    Ok(file) => {
                        let mut reader = BufReader::new(&file);
                        // Seek to the last known position
                        if let Err(e) = reader.seek(SeekFrom::Start(pos)) {
                            yield Err(anyhow!(e).context("Failed to seek to end of file"));
                            return;
                        }

                        let mut line = String::new();
                        loop {
                            match reader.read_line(&mut line) {
                                Ok(0) => break, // EOF, stop inner loop
                                Ok(bytes_read) => {
                                    pos += bytes_read as u64;
                                    yield Ok(line.trim_end().to_string());
                                    line.clear();
                                }
                                Err(e) => {
                                    yield Err(anyhow!(e).context("Error reading line from mock MQTT file"));
                                    return;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        yield Err(anyhow!(e).context("Error reading line from mock MQTT file"));
                        return;
                    }
                }
            }
        })
    }

    impl MockBroker {
        pub async fn publish(&mut self, uri: String, msg: MqttMessage) -> anyhow::Result<()> {
            let path = Path::new(DATA_FILE);
            let mut file = append_file_exclusive(5, path).await?;
            let msg = (uri, msg);
            let json_line = serde_json::to_string(&msg)?;
            writeln!(file, "{}", json_line)?;
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
            let res = self
                .clients
                .entry(uri.to_string())
                .or_default()
                .insert(id, (sub_tx, unsub_tx));
            info!(?self.clients, "Clients after insertion");
            assert!(res.is_none(), "Client ID collision in mock broker");
            let data_reader = tail_new_lines_exclusive(DATA_FILE.to_string(), 5);
            let uri = uri.to_string();

            let stream = Box::pin(stream! {
                let mut sub_rx = sub_rx;
                let mut unsub_rx = unsub_rx;
                let mut subscriptions = Vec::new();
                let id = id.clone();
                let mut data_reader = data_reader;
                loop {
                    futures::select! {
                        line = data_reader.next().fuse() => {
                            if let Some(line) = line {
                                info!(?id, ?line, "MockBroker received line");
                                match line {
                                    Ok(line) => {
                                        match serde_json::from_str::<(String, MqttMessage)>(&line) {
                                            Ok((msg_uri, msg)) => {
                                                if msg_uri != uri {
                                                    continue; // Ignore messages for other URIs
                                                }
                                                info!(?id, ?msg, "MockBroker received message");
                                                let topic = msg.topic.clone();
                                                if subscriptions.iter().any(|t| t == &topic) {
                                                    yield msg;
                                                }
                                            }
                                            Err(e) => {
                                                error!(?e, ?line, "Failed to parse line as JSON");
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!(?e, "Failed to read line from mock MQTT file");
                                    }
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
            let (sub_tx, _) = self
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
            let (_, unsub_tx) = self
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
