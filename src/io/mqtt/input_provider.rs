use std::{collections::BTreeMap, rc::Rc};

use async_cell::unsync::AsyncCell;
use async_stream::stream;
use async_unsync::{bounded, oneshot};
use futures::{FutureExt, StreamExt, future::LocalBoxFuture};
use paho_mqtt as mqtt;
use smol::LocalExecutor;
use tracing::{Level, debug, info, info_span, instrument, warn};

use super::client::provide_mqtt_client_with_subscription;
use crate::{
    InputProvider, OutputStream, Value,
    core::VarName,
    io::mqtt::input_provider::mqtt::AsyncClient,
    utils::cancellation_token::{CancellationToken, DropGuard},
};
use anyhow::anyhow;

const QOS: i32 = 1;

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
    senders: Option<BTreeMap<VarName, bounded::Sender<Value>>>,

    // Oneshot used to pass the MQTT client and stream from connect() to run()
    client_streams_rx: Option<oneshot::Receiver<(AsyncClient, OutputStream<mqtt::Message>)>>,
    client_streams_tx: Option<oneshot::Sender<(AsyncClient, OutputStream<mqtt::Message>)>>,

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

        let (senders, receivers): (
            BTreeMap<_, bounded::Sender<Value>>,
            BTreeMap<_, bounded::Receiver<Value>>,
        ) = var_topics
            .iter()
            .map(|(v, _)| {
                let (tx, rx) = bounded::channel(10).into_split();
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

        let (client_streams_tx, client_streams_rx) = oneshot::channel().into_split();
        let client_streams_tx = Some(client_streams_tx);
        let client_streams_rx = Some(client_streams_rx);

        let available_streams = var_topics
            .keys()
            .zip(receivers.into_values())
            .map(|(v, mut rx)| {
                let stream: OutputStream<Value> = Box::pin(stream! {
                    while let Some(value) =  rx.recv().await {
                        yield value;
                    }
                });
                (v.clone(), stream)
            })
            .collect();

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
                    warn!(name: "Failed to subscribe to topics", ?topics, err=?e);
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

        Ok(())
    }

    async fn run_logic(
        var_topics: BTreeMap<VarName, String>,
        uri: String,
        senders: BTreeMap<VarName, bounded::Sender<Value>>,
        started: Rc<AsyncCell<bool>>,
        cancellation_token: CancellationToken,
        client_streams_rx: oneshot::Receiver<(AsyncClient, OutputStream<paho_mqtt::Message>)>,
    ) -> anyhow::Result<()> {
        info!("MQTTInputProvider run_logic started");
        let mqtt_input_span = info_span!("MQTTInputProvider run_logic", ?uri, ?var_topics);
        let _enter = mqtt_input_span.enter();

        let var_topics_inverse = var_topics
            .iter()
            .map(|(var, top)| (top.clone(), var.clone()))
            .collect::<BTreeMap<_, _>>();

        let (client, mut mqtt_stream) = client_streams_rx
            .await
            .map_err(|_| anyhow::anyhow!("Failed to receive MQTT client and stream"))?;

        debug!("Set started");
        started.set(true);

        let result = async {
            loop {
                futures::select! {
                    msg = mqtt_stream.next().fuse() => {
                        match msg {
                            Some(msg) => {
                                // Process the message
                                debug!(name: "Received MQTT message", ?msg, topic = msg.topic());
                                let value = serde_json5::from_str(&msg.payload_str()).map_err(|e| {
                                    anyhow!(e).context(format!(
                                        "Failed to parse value {:?} sent from MQTT",
                                        msg.payload_str(),
                                    ))
                                })?;

                                // Unwrap maps wrapped in "value" (done by MQTTOutputHandler to make MQTT clients happy):
                                let value = if let Value::Map(map) = &value {
                                    map.get("value").cloned().unwrap_or(value)
                                } else {
                                    value
                                };

                                let var = var_topics_inverse.get(msg.topic()).ok_or_else(|| anyhow::anyhow!(
                                    "No variable found for topic {}",
                                    msg.topic()
                                ))?;
                                let sender = senders.get(var).ok_or_else(|| anyhow::anyhow!(
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
            self.uri.clone(),
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
                smol::future::yield_now().await;
            }
            Ok(())
        })
    }

    fn vars(&self) -> Vec<VarName> {
        self.var_topics.keys().cloned().collect()
    }
}
