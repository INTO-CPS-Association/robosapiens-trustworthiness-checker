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
use crate::{core::VarName, mqtt_client::provide_mqtt_client, InputProvider, OutputStream, Value};
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
        let mut reconnection_attempts: usize = 0;
        let host = host.to_string();

        // let (tx, rx) = tokio::sync::watch::channel(false);
        // let notify = Arc::new(Notify::new());

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

        let topics = var_topics.values().cloned().collect::<Vec<_>>();

        // Spawn a background task to receive messages from the MQTT broker and
        // send them to the appropriate channel based on which topic they were
        // received on
        // Should go away when the sender goes away by sender.send throwing
        // due to no senders
        tokio::spawn(async move {
            // let drop_guard = cancellation_token.clone().drop_guard();

            // println!("Spawning MQTT receiver task");

            // Create and connect to the MQTT client
            let client = provide_mqtt_client(host.clone()).await.unwrap();
            let mut stream = client.clone().get_stream(10);
            println!("Connected to MQTT broker with topics {:?}", topics);
            let qos = topics.iter().map(|_| QOS).collect::<Vec<_>>();
            loop {
                match client.subscribe_many(&topics, &qos).await {
                    Ok(_) => break,
                    Err(e) => {
                        println!(
                            "Failed to subscribe to topics {:?} with error {:?}, retrying in 100ms",
                            topics, e
                        );
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        println!("Reconnecting");
                        let _e = client.reconnect().await;
                    }
                }
            }
            // println!("Connected to MQTT broker");

            while let Some(msg) = stream.next().await {
                match msg {
                    Some(msg) => {
                        // Process the message
                        // println!("Received message: {:?} on {:?}", msg, msg.topic());
                        let value = serde_json::from_str(&msg.payload_str()).expect(
                            format!(
                                "Failed to parse value {:?} sent from MQTT",
                                msg.payload_str()
                            )
                            .as_str(),
                        );
                        if let Some(sender) = senders.get(&VarName(msg.topic().to_string())) {
                            sender
                                .send(value)
                                .await
                                .expect("Failed to send value to channel");
                        } else {
                            println!("Channel not found for topic {:?}", msg.topic());
                        }
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
