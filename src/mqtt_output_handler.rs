use std::collections::BTreeMap;
use std::future::Future;
use std::mem;
use std::pin::Pin;

use async_trait::async_trait;
use futures::StreamExt;
use paho_mqtt::{self as mqtt};
// TODO: should we use a cancellation token to cleanup the background task
// or does it go away when anyway the receivers of our outputs go away?
// use tokio_util::sync::CancellationToken;

use crate::core::OutputHandler;
use crate::mqtt_client::provide_mqtt_client;
// use crate::stream_utils::drop_guard_stream;
use crate::{core::VarName, OutputStream, Value};

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
    pub var_map: BTreeMap<VarName, VarData>,
    // node: Arc<Mutex<r2r::Node>>,
    hostname: String,
}

async fn publish_stream(
    topic_name: String,
    mut stream: OutputStream<Value>,
    client: mqtt::AsyncClient,
) {
    println!(
        "[Output Handler] Publishing stream on topic: {:?}",
        topic_name
    );
    while let Some(data) = stream.next().await {
        let data = serde_json::to_string(&data).unwrap();
        let message = mqtt::Message::new(topic_name.clone(), data, 1);
        loop {
            println!(
                "[Output Handler] Publishing message: {:?} on topic: {:?}",
                message,
                message.topic()
            );
            match client.publish(message.clone()).await {
                Ok(_) => break,
                Err(_e) => {
                    println!("Lost connection. Attempting reconnect...");
                    client.reconnect().await.unwrap();
                }
            }
        }
    }
}

#[async_trait]
impl OutputHandler<Value> for MQTTOutputHandler {
    fn provide_streams(&mut self, streams: BTreeMap<VarName, OutputStream<Value>>) {
        for (var, stream) in streams.into_iter() {
            let var_data = self.var_map.get_mut(&var).expect("Variable not found");
            var_data.stream = Some(stream);
        }
    }

    fn run(&mut self) -> Pin<Box<dyn Future<Output = ()> + 'static + Send>> {
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
        println!("[Output Handler] Running on hostname: {:?}", hostname);
        println!("[Output Handler] Number of streams: {}", streams.len());

        Box::pin(async move {
            println!("[Output Handler] Starting MQTT output handler future");
            println!("[Output Handler] Awaiting client creation");
            let client = provide_mqtt_client(hostname).await.unwrap();
            println!("[Output Handler] Client created");

            futures::future::join_all(
                streams
                    .into_iter()
                    .map(|(channel_name, stream)| {
                        let client = client.clone();
                        println!(
                            "[Output Handler] Task to publish stream on topic: {:?}",
                            channel_name
                        );
                        publish_stream(channel_name, stream, client)
                    })
                    .collect::<Vec<_>>(),
            )
            .await;
        })
    }
}

// #[Error]
// enum MQTTInputProviderError {
// MQTTClientError(mqtt::Error)
// }

impl MQTTOutputHandler {
    // TODO: should we have dependency injection for the MQTT client?
    pub fn new(host: &str, var_topics: OutputChannelMap) -> Result<Self, mqtt::Error> {
        let hostname = host.to_string();
        println!("[Output Handler] Var topics: {:?}", var_topics.clone());

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

        Ok(MQTTOutputHandler { var_map, hostname })
    }
}
