use std::collections::BTreeMap;
use std::mem;
use std::rc::Rc;

use async_trait::async_trait;
use futures::StreamExt;
use futures::future::LocalBoxFuture;
use paho_mqtt::{self as mqtt};
use smol::LocalExecutor;
use tracing::{Level, debug, info, info_span, instrument, warn};
// TODO: should we use a cancellation token to cleanup the background task
// or does it go away when anyway the receivers of our outputs go away?
// use tokio_util::sync::CancellationToken;

use super::client::provide_mqtt_client;
use crate::core::OutputHandler;
// use crate::stream_utils::drop_guard_stream;
use crate::{OutputStream, Value, core::VarName};

// const QOS: &[i32] = &[1, 1];

pub struct VarData {
    pub variable: VarName,
    pub topic_name: String,
    stream: Option<OutputStream<Value>>,
}

// A map between channel names and the MQTT topics they
// correspond to
pub type OutputChannelMap = BTreeMap<VarName, String>;

pub struct MQTTOutputHandler {
    #[allow(dead_code)]
    executor: Rc<LocalExecutor<'static>>,
    pub var_map: BTreeMap<VarName, VarData>,
    // node: Arc<Mutex<r2r::Node>>,
    hostname: String,
}

#[instrument(level = Level::INFO, skip(stream, client))]
async fn publish_stream(
    topic_name: String,
    mut stream: OutputStream<Value>,
    client: mqtt::AsyncClient,
) {
    while let Some(data) = stream.next().await {
        let data = serde_json::to_string(&data).unwrap();
        let message = mqtt::Message::new(topic_name.clone(), data, 1);
        loop {
            debug!(
                name="OutputHandler publishing MQTT message",
                ?message,
                topic=?message.topic()
            );
            match client.publish(message.clone()).await {
                Ok(_) => break,
                Err(_e) => {
                    warn!(name: "Lost connection. Attempting reconnect...",
                        topic=?message.topic());
                    client.reconnect().await.unwrap();
                }
            }
        }
    }
}

#[async_trait(?Send)]
impl OutputHandler for MQTTOutputHandler {
    type Val = Value;

    fn provide_streams(&mut self, streams: BTreeMap<VarName, OutputStream<Value>>) {
        for (var, stream) in streams.into_iter() {
            let var_data = self.var_map.get_mut(&var).expect("Variable not found");
            var_data.stream = Some(stream);
        }
    }

    #[instrument(level = Level::INFO, skip(self))]
    fn run(&mut self) -> LocalBoxFuture<'static, ()> {
        let streams = self
            .var_map
            .iter_mut()
            .map(|(_, var_data)| {
                let channel_name = var_data.topic_name.clone();
                let stream = mem::take(&mut var_data.stream).expect("Stream not found");
                (channel_name, stream)
            })
            .collect::<Vec<_>>();
        let hostname = self.hostname.clone();
        info!(name: "OutputProvider MQTT startup task launched",
            ?hostname, num_streams = ?streams.len());

        Box::pin(async move {
            let mqtt_output_span = info_span!("OutputProvider MQTT startup task");
            let _enter = mqtt_output_span.enter();

            debug!("Awaiting client creation");
            let client = provide_mqtt_client(hostname).await.unwrap();
            debug!("Client created");

            futures::future::join_all(
                streams
                    .into_iter()
                    .map(|(channel_name, stream)| {
                        let client = client.clone();
                        publish_stream(channel_name, stream, client)
                    })
                    .collect::<Vec<_>>(),
            )
            .await;
        })
    }
}

impl MQTTOutputHandler {
    // TODO: should we have dependency injection for the MQTT client?
    #[instrument(level = Level::INFO)]
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        host: &str,
        var_topics: OutputChannelMap,
    ) -> Result<Self, mqtt::Error> {
        let hostname = host.to_string();

        let var_map = var_topics
            .into_iter()
            .map(|(var, topic_name)| {
                (
                    var.clone(),
                    VarData {
                        variable: var,
                        topic_name,
                        stream: None,
                    },
                )
            })
            .collect();

        Ok(MQTTOutputHandler {
            executor,
            var_map,
            hostname,
        })
    }
}
