use std::{collections::BTreeMap, rc::Rc};

use async_cell::unsync::AsyncCell;
use async_stream::stream;
use async_unsync::bounded;
use futures::{StreamExt, future::LocalBoxFuture};
use paho_mqtt as mqtt;
use smol::LocalExecutor;
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
    pub started: Rc<AsyncCell<bool>>,
}

impl MQTTInputProvider {
    // TODO: should we have dependency injection for the MQTT client?
    #[instrument(level = Level::INFO, skip(var_topics))]
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        host: &str,
        port: Option<u16>,
        var_topics: InputChannelMap,
        max_reconnect_attempts: u32,
    ) -> Result<Self, mqtt::Error> {
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

        let topics = var_topics.values().cloned().collect::<Vec<_>>();
        let topic_vars = var_topics
            .iter()
            .map(|(k, v)| (v.clone(), k.clone()))
            .collect::<BTreeMap<_, _>>();

        let started = AsyncCell::new_with(false).into_shared();
        let result = AsyncCell::shared();

        executor
            .spawn(MQTTInputProvider::input_monitor(
                result.clone(),
                var_topics.clone(),
                topic_vars,
                host,
                port,
                topics,
                senders,
                started.clone(),
                max_reconnect_attempts,
            ))
            .detach();

        let var_data = var_topics
            .into_iter()
            .zip(receivers.into_values())
            .map(|((v, topic), mut rx)| {
                let stream = Box::pin(stream! {
                    loop {
                        match rx.recv().await {
                            Some(value) => yield value,
                            None => break,
                        }
                    }
                });
                (
                    v.clone(),
                    VarData {
                        variable: v,
                        channel_name: topic,
                        stream: Some(stream),
                    },
                )
            })
            .collect();

        Ok(MQTTInputProvider {
            executor,
            result,
            var_map: var_data,
            started,
        })
    }

    async fn input_monitor(
        result: Rc<AsyncCell<anyhow::Result<()>>>,
        var_topics: BTreeMap<VarName, String>,
        topic_vars: BTreeMap<String, VarName>,
        host: String,
        port: Option<u16>,
        topics: Vec<String>,
        senders: BTreeMap<VarName, bounded::Sender<Value>>,
        started: Rc<AsyncCell<bool>>,
        max_reconnect_attempts: u32,
    ) {
        let result = result.guard_shared(Err(anyhow::anyhow!("InputProvider crashed")));
        result.set(
            Self::input_monitor_with_result(
                var_topics,
                topic_vars,
                host,
                port,
                topics,
                senders,
                started,
                max_reconnect_attempts,
            )
            .await,
        )
    }

    async fn input_monitor_with_result(
        var_topics: BTreeMap<VarName, String>,
        topic_vars: BTreeMap<String, VarName>,
        host: String,
        port: Option<u16>,
        topics: Vec<String>,
        senders: BTreeMap<VarName, bounded::Sender<Value>>,
        started: Rc<AsyncCell<bool>>,
        max_reconnect_attempts: u32,
    ) -> anyhow::Result<()> {
        let mqtt_input_span = info_span!("InputProvider MQTT startup task", ?host, ?var_topics);
        let _enter = mqtt_input_span.enter();
        let uri = match port {
            Some(port) => format!("tcp://{}:{}", host, port),
            None => format!("tcp://{}", host),
        };
        // Create and connect to the MQTT client
        let (client, mut stream) =
            provide_mqtt_client_with_subscription(uri.clone(), max_reconnect_attempts).await?;
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
        started.set(true);

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
                sender
                    .send(value)
                    .await
                    .map_err(|_| anyhow::anyhow!("Failed to send value"))?;
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
        let started = self.started.clone();
        Box::pin(async move {
            while !started.get().await {
                smol::future::yield_now().await;
            }
            Ok(())
        })
    }
}
