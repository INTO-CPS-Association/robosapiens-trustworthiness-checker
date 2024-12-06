use std::collections::BTreeMap;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use paho_mqtt::{self as mqtt};
// TODO: should we use a cancellation token to cleanup the background task
// or does it go away when anyway the receivers of our outputs go away?
// use tokio_util::sync::CancellationToken;

use crate::core::OutputHandler;
// use crate::stream_utils::drop_guard_stream;
use crate::{core::VarName, InputProvider, OutputStream, Value};

// const QOS: &[i32] = &[1, 1];

pub struct VarData {
    pub variable: VarName,
    pub channel_name: String,
    stream: Option<OutputStream<Value>>,
}

// A map between channel names and the MQTT channels they
// correspond to
pub type OutputChannelMap = BTreeMap<VarName, String>;

pub struct MQTTOutputHandler {
    pub var_map: BTreeMap<VarName, VarData>,
    // node: Arc<Mutex<r2r::Node>>,
    client: mqtt::AsyncClient,
    connect_options: mqtt::ConnectOptions,
}

async fn publish_stream(
    channel_name: String,
    mut stream: OutputStream<Value>,
    client: mqtt::AsyncClient,
) {
    while let Some(data) = stream.next().await {
        let data = serde_json::to_string(&data).unwrap();
        let message = mqtt::Message::new(channel_name.clone(), data, 1);
        client.publish(message).await.unwrap();
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
        let client = self.client.clone();
        let streams = self
            .var_map
            .iter_mut()
            .map(|(_, var_data)| {
                let channel_name = var_data.channel_name.clone();
                let stream = mem::take(&mut var_data.stream).expect("Stream not found");
                (channel_name, stream)
            })
            .collect::<Vec<_>>();
        let connect_options = self.connect_options.clone();

        Box::pin(async move {
            client
                .connect(connect_options)
                .await
                .expect("Failed to establish MQTT connection");

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

// #[Error]
// enum MQTTInputProviderError {
// MQTTClientError(mqtt::Error)
// }

impl MQTTOutputHandler {
    // TODO: should we have dependency injection for the MQTT client?
    pub fn new(host: &str, var_topics: OutputChannelMap) -> Result<Self, mqtt::Error> {
        // Client options
        let create_opts = mqtt::CreateOptionsBuilder::new_v3()
            .server_uri(host)
            .client_id("robosapiens_trustworthiness_checker")
            .finalize();

        let client = mqtt::AsyncClient::new(create_opts)?;

        let connect_options = mqtt::ConnectOptionsBuilder::new_v3()
            .keep_alive_interval(Duration::from_secs(30))
            .clean_session(false)
            .finalize();

        let var_map = var_topics
            .into_iter()
            .map(|(var, channel_name)| {
                (
                    var.clone(),
                    VarData {
                        variable: var,
                        channel_name,
                        stream: None,
                    },
                )
            })
            .collect();

        Ok(MQTTOutputHandler {
            var_map,
            client,
            connect_options,
        })
    }
}

impl InputProvider<Value> for MQTTOutputHandler {
    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<Value>> {
        let var_data = self.var_map.get_mut(var)?;
        let stream = var_data.stream.take()?;
        Some(stream)
    }
}
