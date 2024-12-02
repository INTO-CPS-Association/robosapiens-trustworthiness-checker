use std::collections::BTreeMap;
use std::time::Duration;

use futures::StreamExt;
use paho_mqtt as mqtt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
// TODO: should we use a cancellation token to cleanup the background task
// or does it go away when anyway the receivers of our outputs go away?
// use tokio_util::sync::CancellationToken;

// use crate::stream_utils::drop_guard_stream;
use crate::{core::VarName, InputProvider, OutputStream, Value};
// use async_stream::stream;

const QOS: &[i32] = &[1, 1];

pub struct VarData {
    pub variable: VarName,
    pub channel_name: String,
    stream: Option<OutputStream<Value>>,
}

pub struct MQTTStreamMapping {
    pub var_map: BTreeMap<VarName, VarData>,
}

// A map between channel names and the MQTT channels they
// correspond to
pub type InputChannelMap = BTreeMap<VarName, String>;

pub struct MQTTInputProvider {
    pub var_map: BTreeMap<VarName, VarData>,
    // node: Arc<Mutex<r2r::Node>>,
}

// #[Error]
// enum MQTTInputProviderError {
// MQTTClientError(mqtt::Error)
// }

impl MQTTInputProvider {
    // TODO: should we have dependency injection for the MQTT client?
    pub fn new(host: &str, var_topics: InputChannelMap) -> Result<Self, mqtt::Error> {
        // Client options
        let create_opts = mqtt::CreateOptionsBuilder::new_v3()
            .server_uri(host)
            .client_id("robosapiens_trustworthiness_checker")
            .finalize();

        let mut client = mqtt::AsyncClient::new(create_opts)?;

        let connect_options = mqtt::ConnectOptionsBuilder::new_v3()
            .keep_alive_interval(Duration::from_secs(30))
            .clean_session(false)
            .finalize();

        let mut reconnection_attempts: usize = 0;

        // let (tx, rx) = tokio::sync::watch::channel(false);
        // let notify = Arc::new(Notify::new());

        let mut stream = client.get_stream(10);
        // let cancellation_token = CancellationToken::new();

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

        let topics = var_topics.values().map(|t| t.clone()).collect::<Vec<_>>();

        // Spawn a background task to receive messages from the MQTT broker and
        // send them to the appropriate channel based on which topic they were
        // received on
        // Should go away when the sender goes away by sender.send throwing
        // due to no senders
        tokio::spawn(async move {
            // let drop_guard = cancellation_token.clone().drop_guard();

            // println!("Spawning MQTT receiver task");

            client.connect(connect_options).await.unwrap();
            client.subscribe_many(&topics, QOS).await.unwrap();

            while let Some(msg) = stream.next().await {
                match msg {
                    Some(msg) => {
                        // Process the message
                        // println!("Received message: {:?}", msg);
                        let value = serde_json::from_str(&msg.payload_str()).expect(
                            format!(
                                "Failed to parse value {:?} sent from MQTT",
                                msg.payload_str()
                            )
                            .as_str(),
                        );
                        let sender = senders
                            .get(&VarName(msg.topic().to_string()))
                            .expect("Channel not found for topic");
                        sender
                            .send(value)
                            .await
                            .expect("Failed to send value to channel");
                    }
                    None => {
                        // Connection lost, try to reconnect
                        while let Err(e) = client.reconnect().await {
                            println!("Reconnection attempt failed: {:?}", e);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            reconnection_attempts += 1;
                        }
                        println!("Reconnected after {} attempts", reconnection_attempts);
                    }
                }
            }
        });

        // Build the variable map from the input monitor streams
        let var_data = var_topics
            .iter()
            .map(|(v, topic)| {
                let rx = receivers.remove(&v).expect(&"Channel not found for topic");
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

        Ok(MQTTInputProvider { var_map: var_data })
    }
}

impl InputProvider<Value> for MQTTInputProvider {
    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<Value>> {
        let var_data = self.var_map.get_mut(var)?;
        let stream = var_data.stream.take()?;
        Some(stream)
    }
}
