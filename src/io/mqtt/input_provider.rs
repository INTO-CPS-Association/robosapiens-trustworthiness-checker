use std::{collections::BTreeMap, rc::Rc};

use async_cell::unsync::AsyncCell;
use futures::{FutureExt, StreamExt, future::LocalBoxFuture};
use paho_mqtt as mqtt;
use smol::LocalExecutor;
use tracing::{Level, debug, info, info_span, instrument, warn};

use unsync::oneshot::Receiver as OSReceiver;
use unsync::oneshot::Sender as OSSender;
use unsync::spsc::Sender as SpscSender;

use super::client::provide_mqtt_client_with_subscription;
use crate::stream_utils::channel_to_output_stream;
use crate::{
    InputProvider, OutputStream, Value,
    core::VarName,
    io::mqtt::input_provider::mqtt::AsyncClient,
    utils::cancellation_token::{CancellationToken, DropGuard},
};
use anyhow::anyhow;

const QOS: i32 = 1;
const CHANNEL_SIZE: usize = 10;

type Topic = String;
// A map between channel names and the MQTT channels they
// correspond to
pub type VarTopicMap = BTreeMap<VarName, Topic>;

pub struct MQTTInputProvider {
    var_topics: VarTopicMap,
    uri: String,
    max_reconnect_attempts: u32,

    // Streams that can be taken ownership of by calling `input_stream`
    available_streams: BTreeMap<VarName, OutputStream<Value>>,
    // Channels used to send to the `available_streams`
    senders: Option<BTreeMap<VarName, SpscSender<Value>>>,

    // Oneshot used to pass the MQTT client and stream from connect() to run()
    client_streams_rx: Option<OSReceiver<(AsyncClient, OutputStream<mqtt::Message>)>>,
    client_streams_tx: Option<OSSender<(AsyncClient, OutputStream<mqtt::Message>)>>,

    drop_guard: DropGuard,
    // Mainly used for debugging purposes
    pub started: Rc<AsyncCell<bool>>,
}

impl MQTTInputProvider {
    #[instrument(level = Level::INFO, skip(var_topics))]
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        host: &str,
        port: Option<u16>,
        var_topics: VarTopicMap,
        max_reconnect_attempts: u32,
    ) -> Self {
        let host: String = host.to_string();

        let (senders, available_streams): (
            BTreeMap<_, SpscSender<Value>>,
            BTreeMap<_, OutputStream<Value>>,
        ) = var_topics
            .iter()
            .map(|(v, _)| {
                let (tx, rx) = unsync::spsc::channel(CHANNEL_SIZE);
                let rx = channel_to_output_stream(rx);
                ((v.clone(), tx), (v.clone(), rx))
            })
            .unzip();
        let senders = Some(senders);

        let started = AsyncCell::new_with(false).into_shared();
        let drop_guard = CancellationToken::new().drop_guard();

        let uri = match port {
            Some(port) => format!("tcp://{}:{}", host, port),
            None => format!("tcp://{}", host),
        };

        let (client_streams_tx, client_streams_rx) = unsync::oneshot::channel();
        let client_streams_tx = Some(client_streams_tx);
        let client_streams_rx = Some(client_streams_rx);

        MQTTInputProvider {
            var_topics,
            started,
            uri,
            max_reconnect_attempts,
            available_streams,
            senders,
            client_streams_tx,
            client_streams_rx,
            drop_guard,
        }
    }

    pub async fn connect(&mut self) -> anyhow::Result<()> {
        let client_streams_tx =
            std::mem::take(&mut self.client_streams_tx).expect("Client stream tx already taken");

        // Create and connect to the MQTT client
        info!("Getting client with subscription");
        let (client, mqtt_stream) =
            provide_mqtt_client_with_subscription(self.uri.clone(), self.max_reconnect_attempts)
                .await?;
        info!(?self.uri, "InputProvider MQTT client connected to broker");

        let topics = self.var_topics.values().collect::<Vec<_>>();
        let qos = vec![QOS; topics.len()];
        loop {
            match client.subscribe_many(&topics, &qos).await {
                Ok(_) => break,
                Err(e) => {
                    warn!(?topics, err=?e, "Failed to subscribe to topics");
                    smol::Timer::after(std::time::Duration::from_millis(100)).await;
                    info!("Retrying subscribing to MQTT topics");
                    let _e = client.reconnect().await;
                }
            }
        }
        info!(?self.uri, ?topics, "Connected and subscribed to MQTT broker");

        client_streams_tx
            .send((client, mqtt_stream))
            .map_err(|_| anyhow::anyhow!("Failed to send client streams"))?;

        info!("Sent client and mqtt_stream to external task");
        Ok(())
    }

    async fn run_logic(
        var_topics: BTreeMap<VarName, String>,
        mut senders: BTreeMap<VarName, SpscSender<Value>>,
        started: Rc<AsyncCell<bool>>,
        cancellation_token: CancellationToken,
        client_streams_rx: OSReceiver<(AsyncClient, OutputStream<paho_mqtt::Message>)>,
    ) -> anyhow::Result<()> {
        let mqtt_input_span = info_span!("MQTTInputProvider run_logic");
        let _enter = mqtt_input_span.enter();
        info!("MQTTInputProvider run_logic started");

        let var_topics_inverse = var_topics
            .iter()
            .map(|(var, top)| (top.clone(), var.clone()))
            .collect::<BTreeMap<_, _>>();

        let (client, mut mqtt_stream) = client_streams_rx
            .await
            .ok_or_else(|| anyhow::anyhow!("Failed to receive MQTT client and stream"))?;

        debug!("Set started");
        started.set(true);

        let result = async {
            loop {
                futures::select! {
                    msg = mqtt_stream.next().fuse() => {
                        match msg {
                            Some(msg) => {
                                // Process the message
                                debug!(topic = msg.topic(), "Received MQTT message on topic:");
                                let mut value = serde_json5::from_str(&msg.payload_str()).map_err(|e| {
                                    anyhow!(e).context(format!(
                                        "Failed to parse value {:?} sent from MQTT",
                                        msg.payload_str(),
                                    ))
                                })?;

                                // Unwrap maps wrapped in "value" (done by MQTTOutputHandler to make MQTT clients happy)
                                if let Value::Map(map) = &value {
                                    if let Some(inner) = map.get("value") {
                                        value = inner.clone();
                                    }
                                }
                                debug!(?value, "MQTT message value:");

                                let var = var_topics_inverse.get(msg.topic()).ok_or_else(|| anyhow::anyhow!(
                                    "No variable found for topic {}",
                                    msg.topic()
                                ))?;
                                let sender = senders.get_mut(var).ok_or_else(|| anyhow::anyhow!(
                                    "No sender found for variable {}",
                                    var
                                ))?;
                                sender.send(value).await.map_err(|_| anyhow::anyhow!("Failed to send value"))?;
                            }
                            None => {
                                debug!("MQTT stream ended");
                                return Ok(());
                            }
                        }
                    }
                    _ = cancellation_token.cancelled().fuse() => {
                        debug!("MQTTInputProvider: Input monitor task cancelled");
                        return Ok(());
                    }
                }
            }
        }.await;

        // Always disconnect the client when we're done, regardless of success or error
        debug!("Disconnecting MQTT client");
        let _ = client.disconnect(None).await;

        result
    }
}

impl InputProvider for MQTTInputProvider {
    type Val = Value;

    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<Value>> {
        // Take ownership of the stream for the variable, if it exists
        self.available_streams.remove(var)
    }

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let senders = std::mem::take(&mut self.senders).expect("Senders already taken");
        let client_streams_rx = self
            .client_streams_rx
            .take()
            .expect("Client streams rx already taken");

        Box::pin(Self::run_logic(
            self.var_topics.clone(),
            senders,
            self.started.clone(),
            self.drop_guard.clone_tok(),
            client_streams_rx,
        ))
    }

    fn ready(&self) -> LocalBoxFuture<'static, Result<(), anyhow::Error>> {
        let started = self.started.clone();
        Box::pin(async move {
            debug!("In ready");
            while !started.get().await {
                debug!("Checking if ready");
                smol::Timer::after(std::time::Duration::from_millis(100)).await;
            }
            Ok(())
        })
    }

    fn vars(&self) -> Vec<VarName> {
        self.var_topics.keys().cloned().collect()
    }
}
