use std::collections::BTreeMap;
use std::mem;
use std::rc::Rc;

use futures::StreamExt;
use futures::future::LocalBoxFuture;
use paho_mqtt::{self as mqtt};
use smol::LocalExecutor;
use tracing::{Level, debug, info, instrument, warn};
// TODO: should we use a cancellation token to cleanup the background task
// or does it go away when anyway the receivers of our outputs go away?
// use crate::utils::cancellation_token::CancellationToken;

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
    pub var_names: Vec<VarName>,
    pub var_map: BTreeMap<VarName, VarData>,
    pub hostname: String,
    pub port: Option<u16>,
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

impl OutputHandler for MQTTOutputHandler {
    type Val = Value;

    fn var_names(&self) -> Vec<VarName> {
        self.var_names.clone()
    }

    fn provide_streams(&mut self, streams: Vec<OutputStream<Value>>) {
        for (var, stream) in self.var_names().iter().zip(streams.into_iter()) {
            let var_data = self.var_map.get_mut(var).expect("Variable not found");
            var_data.stream = Some(stream);
        }
    }

    #[instrument(level = Level::INFO, skip(self))]
    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
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
        let port = self.port.clone();
        info!(name: "OutputProvider MQTT startup task launched",
            ?hostname, num_streams = ?streams.len());

        Box::pin(MQTTOutputHandler::inner_handler(hostname, port, streams))
    }
}

impl MQTTOutputHandler {
    // TODO: should we have dependency injection for the MQTT client?
    #[instrument(level = Level::INFO)]
    pub fn new(
        _executor: Rc<LocalExecutor<'static>>,
        var_names: Vec<VarName>,
        host: &str,
        port: Option<u16>,
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
            var_names,
            var_map,
            hostname,
            port,
        })
    }

    async fn inner_handler(
        host: String,
        port: Option<u16>,
        streams: Vec<(String, OutputStream<Value>)>,
    ) -> anyhow::Result<()> {
        debug!("Awaiting client creation");
        let uri = match port {
            Some(port) => format!("tcp://{}:{}", host, port),
            None => format!("tcp://{}", host),
        };
        let client = provide_mqtt_client(uri).await?;
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

        Ok(())
    }
}
