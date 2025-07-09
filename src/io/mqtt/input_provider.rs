use std::{collections::BTreeMap, rc::Rc};

use async_cell::unsync::AsyncCell;
use futures::{StreamExt, future::LocalBoxFuture};
use paho_mqtt as mqtt;
use smol::LocalExecutor;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{Level, debug, info, info_span, instrument, warn};

use super::client::provide_mqtt_client_with_subscription;
use crate::{InputProvider, OutputStream, Value, core::VarName};
use anyhow::anyhow;

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
    pub result: Rc<AsyncCell<anyhow::Result<()>>>,
    pub started: watch::Receiver<bool>,
}

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
        let result = AsyncCell::shared();

        executor
            .spawn(MQTTInputProvider::input_monitor(
                result.clone(),
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
            result,
            var_map: var_data,
            started: started_rx,
        })
    }

    async fn input_monitor(
        result: Rc<AsyncCell<anyhow::Result<()>>>,
        var_topics: BTreeMap<VarName, String>,
        topic_vars: BTreeMap<String, VarName>,
        host: String,
        topics: Vec<String>,
        senders: BTreeMap<VarName, mpsc::Sender<Value>>,
        started_tx: watch::Sender<bool>,
    ) {
        let result = result.guard_shared(Err(anyhow::anyhow!("InputProvider crashed")));
        result.set(
            Self::input_monitor_with_result(
                var_topics, topic_vars, host, topics, senders, started_tx,
            )
            .await,
        )
    }

    async fn input_monitor_with_result(
        var_topics: BTreeMap<VarName, String>,
        topic_vars: BTreeMap<String, VarName>,
        host: String,
        topics: Vec<String>,
        senders: BTreeMap<VarName, mpsc::Sender<Value>>,
        started_tx: watch::Sender<bool>,
    ) -> anyhow::Result<()> {
        let mqtt_input_span = info_span!("InputProvider MQTT startup task", ?host, ?var_topics);
        let _enter = mqtt_input_span.enter();
        // Create and connect to the MQTT client
        let (client, mut stream) = provide_mqtt_client_with_subscription(host.clone()).await?;
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
            let value = match serde_json5::from_str(&msg.payload_str()) {
                Ok(value) => value,
                Err(e) => {
                    return Err(anyhow!(e).context(format!(
                        "Failed to parse value {:?} sent from MQTT",
                        msg.payload_str(),
                    )));
                }
            };
            if let Some(sender) = senders.get(topic_vars.get(msg.topic()).unwrap()) {
                sender.send(value).await?;
            } else {
                return Err(anyhow::anyhow!(
                    "Channel not found for topic {}",
                    msg.topic()
                ));
            }
        }

        Ok(())
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
        Box::pin(self.result.take_shared())
    }

    fn ready(&self) -> LocalBoxFuture<'static, Result<(), anyhow::Error>> {
        let mut started = self.started.clone();
        Box::pin(async move {
            started.wait_for(|x| *x).await?;
            Ok(())
        })
    }
}
