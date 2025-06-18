use std::{collections::BTreeMap, future::pending, rc::Rc};

use futures::{StreamExt, future::LocalBoxFuture};
use paho_mqtt as mqtt;
use smol::LocalExecutor;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{Level, debug, error, info, info_span, instrument, warn};
// TODO: should we use a cancellation token to cleanup the background task
// or does it go away when anyway the receivers of our outputs go away?
// use tokio_util::sync::CancellationToken;

// use crate::stream_utils::drop_guard_stream;
use super::client::provide_mqtt_client_with_subscription;
use crate::{InputProvider, OutputStream, Value, core::VarName};
// use async_stream::stream;

const QOS: i32 = 1;

pub struct VarData {
    pub variable: VarName,
    pub channel_name: String,
    stream: Option<OutputStream<Value>>,
}

// A map between channel names and the MQTT channels they
// correspond to
pub type InputChannelMap = BTreeMap<VarName, String>;

pub struct MQTTInputProvider {
    #[allow(dead_code)]
    executor: Rc<LocalExecutor<'static>>,
    pub var_map: BTreeMap<VarName, VarData>,
    // node: Arc<Mutex<r2r::Node>>,
    pub started: watch::Receiver<bool>,
}

// #[Error]
// enum MQTTInputProviderError {
// MQTTClientError(mqtt::Error)
// }

impl MQTTInputProvider {
    // TODO: should we have dependency injection for the MQTT client?
    #[instrument(level = Level::INFO, skip(var_topics))]
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        host: &str,
        var_topics: InputChannelMap,
    ) -> Result<Self, mqtt::Error> {
        let host: String = host.to_string();

        let (senders, receivers): (
            BTreeMap<_, mpsc::Sender<Value>>,
            BTreeMap<_, mpsc::Receiver<Value>>,
        ) = var_topics
            .iter()
            .map(|(v, _)| {
                let (tx, rx) = mpsc::channel(10);
                ((v.clone(), tx), (v.clone(), rx))
            })
            .unzip();

        let topics = var_topics.values().cloned().collect::<Vec<_>>();
        let topic_vars = var_topics
            .iter()
            .map(|(k, v)| (v.clone(), k.clone()))
            .collect::<BTreeMap<_, _>>();

        let (started_tx, started_rx) = watch::channel(false);

        executor
            .spawn(MQTTInputProvider::input_monitor(
                var_topics.clone(),
                topic_vars,
                host,
                topics,
                senders,
                started_tx,
            ))
            .detach();

        let var_data = var_topics
            .into_iter()
            .zip(receivers.into_values())
            .map(|((v, topic), rx)| {
                let stream = ReceiverStream::new(rx);
                (
                    v.clone(),
                    VarData {
                        variable: v,
                        channel_name: topic,
                        stream: Some(Box::pin(stream)),
                    },
                )
            })
            .collect();

        Ok(MQTTInputProvider {
            executor,
            var_map: var_data,
            started: started_rx,
        })
    }
    async fn input_monitor(
        var_topics: BTreeMap<VarName, String>,
        topic_vars: BTreeMap<String, VarName>,
        host: String,
        topics: Vec<String>,
        senders: BTreeMap<VarName, mpsc::Sender<Value>>,
        started_tx: watch::Sender<bool>,
    ) {
        let mqtt_input_span = info_span!("InputProvider MQTT startup task", ?host, ?var_topics);
        let _enter = mqtt_input_span.enter();
        // Create and connect to the MQTT client
        let (client, mut stream) = provide_mqtt_client_with_subscription(host.clone())
            .await
            .unwrap();
        info_span!("InputProvider MQTT client connected", ?host, ?var_topics);
        let qos = topics.iter().map(|_| QOS).collect::<Vec<_>>();
        loop {
            match client.subscribe_many(&topics, &qos).await {
                Ok(_) => break,
                Err(e) => {
                    warn!(name: "Failed to subscribe to topics", ?topics, err=?e);
                    info!("Retrying in 100ms");
                    let _e = client.reconnect().await;
                }
            }
        }
        info!(name: "Connected to MQTT broker", ?host, ?var_topics);
        started_tx
            .send(true)
            .expect("Failed to send started signal");

        while let Some(msg) = stream.next().await {
            // Process the message
            debug!(name: "Received MQTT message", ?msg, topic = msg.topic());
            let value = serde_json::from_str(&msg.payload_str()).unwrap_or_else(|_| {
                panic!(
                    "Failed to parse value {:?} sent from MQTT",
                    msg.payload_str()
                )
            });
            if let Some(sender) = senders.get(topic_vars.get(msg.topic()).unwrap()) {
                sender
                    .send(value)
                    .await
                    .expect("Failed to send value to channel");
            } else {
                error!(name: "Channel not found for topic", topic=?msg.topic());
            }
        }
    }
}

impl InputProvider for MQTTInputProvider {
    type Val = Value;

    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<Value>> {
        let var_data = self.var_map.get_mut(var)?;
        let stream = var_data.stream.take()?;
        Some(stream)
    }

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(pending())
    }
}
