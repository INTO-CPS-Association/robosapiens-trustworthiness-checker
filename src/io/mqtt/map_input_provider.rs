use std::{collections::BTreeMap, rc::Rc};

// Fork of the MQTTInputProvider that accepts Values in a key-value format

use anyhow::anyhow;
use async_cell::unsync::AsyncCell;
use ecow::eco_vec;
use futures::{StreamExt, future::LocalBoxFuture};
use paho_mqtt as mqtt;
use serde_json::Value as JValue;
use smol::LocalExecutor;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{Level, debug, debug_span, info, info_span, instrument, warn};

use super::client::provide_mqtt_client_with_subscription;
use crate::{InputProvider, OutputStream, Value, core::VarName};

const QOS: i32 = 1;

trait ToValue {
    fn to_value(self) -> Value;
}

impl ToValue for JValue {
    fn to_value(self) -> Value {
        match self {
            JValue::Null => Value::Unit,
            JValue::Bool(val) => Value::Bool(val),
            JValue::Number(num) => {
                if num.is_i64() {
                    Value::Int(num.as_i64().unwrap())
                } else if num.is_u64() {
                    panic!("Number too large")
                } else {
                    // Guaranteed to be f64 at this point
                    Value::Float(num.as_f64().unwrap())
                }
            }
            JValue::String(val) => Value::Str(val.into()),
            JValue::Array(vals) => Value::List(vals.iter().map(|v| v.clone().to_value()).collect()),
            // Objects currently represented of list of key-value pairs. Since we don't have pairs
            // it becomes Lists of 2-value Lists
            JValue::Object(vals) => Value::List(
                vals.iter()
                    .map(|(k, v)| Value::List(eco_vec![k.clone().into(), v.clone().to_value()]))
                    .collect(),
            ),
        }
    }
}

pub struct VarData {
    pub variable: VarName,
    pub channel_name: String,
    stream: Option<OutputStream<Value>>,
}

// A map between channel names and the MQTT channels they
// correspond to
pub type InputChannelMap = BTreeMap<VarName, String>;

pub struct MapMQTTInputProvider {
    #[allow(dead_code)]
    executor: Rc<LocalExecutor<'static>>,
    pub var_map: BTreeMap<VarName, VarData>,
    pub result: Rc<AsyncCell<anyhow::Result<()>>>,
    // node: Arc<Mutex<r2r::Node>>,
    pub started: watch::Receiver<bool>,
}

impl MapMQTTInputProvider {
    // TODO: should we have dependency injection for the MQTT client?
    #[instrument(level = Level::INFO, skip(var_topics))]
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        host: &str,
        var_topics: InputChannelMap,
    ) -> Result<Self, mqtt::Error> {
        // Client options
        let host = host.to_string();

        // Create a pair of mpsc channels for each topic which is used to put
        // messages received on that topic into an appropriate stream of
        // typed values
        let mut senders = BTreeMap::new();
        let mut receivers = BTreeMap::new();
        for (v, _) in var_topics.iter() {
            let (tx, rx) = mpsc::channel(10);
            senders.insert(v.clone(), tx);
            receivers.insert(v.clone(), rx);
        }

        let topics = var_topics.values().cloned().collect::<Vec<_>>();
        let topic_vars = var_topics
            .iter()
            .map(|(k, v)| (v.clone(), k.clone()))
            .collect::<BTreeMap<_, _>>();
        info!(name: "InputProvider connecting to MQTT broker",
            ?host, ?var_topics, ?topic_vars);

        let (started_tx, started_rx) = watch::channel(false);
        let result = AsyncCell::shared();

        // Spawn a background task to receive messages from the MQTT broker and
        // send them to the appropriate channel based on which topic they were
        // received on
        // Should go away when the sender goes away by sender.send throwing
        // due to no senders
        executor
            .spawn(MapMQTTInputProvider::input_monitor(
                result.clone(),
                host,
                var_topics.clone(),
                topic_vars,
                topics,
                senders,
                started_tx,
            ))
            .detach();

        // Build the variable map from the input monitor streams
        let var_data = var_topics
            .iter()
            .map(|(v, topic)| {
                let rx = receivers.remove(v).expect("Channel not found for topic");
                let stream = ReceiverStream::new(rx);
                (
                    v.clone(),
                    VarData {
                        variable: v.clone(),
                        channel_name: topic.clone(),
                        stream: Some(Box::pin(stream)),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();

        Ok(MapMQTTInputProvider {
            executor,
            result,
            var_map: var_data,
            started: started_rx,
        })
    }

    async fn input_monitor(
        result: Rc<AsyncCell<anyhow::Result<()>>>,
        host: String,
        var_topics: BTreeMap<VarName, String>,
        topic_vars: BTreeMap<String, VarName>,
        topics: Vec<String>,
        senders: BTreeMap<VarName, mpsc::Sender<Value>>,
        started_tx: watch::Sender<bool>,
    ) {
        let result = result.guard_shared(Err(anyhow::anyhow!("InputProvider crashed")));
        result.set(
            Self::input_monitor_with_result(
                host, var_topics, topic_vars, topics, senders, started_tx,
            )
            .await,
        )
    }

    async fn input_monitor_with_result(
        host: String,
        var_topics: BTreeMap<VarName, String>,
        topic_vars: BTreeMap<String, VarName>,
        topics: Vec<String>,
        senders: BTreeMap<VarName, mpsc::Sender<Value>>,
        started_tx: watch::Sender<bool>,
    ) -> anyhow::Result<()> {
        let mqtt_input_span = debug_span!("InputProvider MQTT startup task", ?host, ?var_topics);
        let _enter = mqtt_input_span.enter();
        // Create and connect to the MQTT client
        let (client, mut stream) = provide_mqtt_client_with_subscription(host.clone())
            .await
            .unwrap();
        info_span!("InputProvider MQTT client connected", ?host, ?var_topics);
        loop {
            match client.subscribe_many_same_qos(&topics, QOS).await {
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
            let jvalue = match serde_json5::from_str::<JValue>(&msg.payload_str()) {
                Ok(value) => value,
                Err(e) => {
                    return Err(anyhow!(e).context(format!(
                        "Failed to parse value {:?} sent from MQTT",
                        msg.payload_str()
                    )));
                }
            };
            debug!("JValue: {:?}", jvalue);
            let value = jvalue.to_value();
            if let Some(sender) = senders.get(topic_vars.get(msg.topic()).unwrap()) {
                sender
                    .send(value)
                    .await
                    .expect("Failed to send value to channel");
            } else {
                return Err(anyhow!("Channel not found for topic {}", msg.topic()));
            }
        }

        Ok(())
    }
}

impl InputProvider for MapMQTTInputProvider {
    type Val = Value;

    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<Value>> {
        let var_data = self.var_map.get_mut(var)?;
        let stream = var_data.stream.take()?;
        Some(stream)
    }

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let res = self.result.clone();

        Box::pin(async move { res.take().await })
    }

    fn ready(&self) -> LocalBoxFuture<'static, Result<(), anyhow::Error>> {
        let mut started = self.started.clone();
        Box::pin(async move {
            started.wait_for(|x| *x).await?;
            Ok(())
        })
    }
}
