use std::rc::Rc;

use async_cell::unsync::AsyncCell;
use async_channel::Receiver;
use async_trait::async_trait;
use futures::StreamExt;
use futures::future::LocalBoxFuture;
use paho_mqtt::Message;
use tracing::{debug, info, warn};

use crate::{
    VarName, distributed::locality_receiver::LocalityReceiver,
    semantics::distributed::localisation::LocalitySpec,
};

use super::provide_mqtt_client_with_subscription;

const MQTT_QOS: i32 = 1;

#[derive(Clone)]
pub struct MQTTLocalityReceiver {
    mqtt_host: String,
    local_node: String,
    mqtt_port: Option<u16>,
    ready: Rc<AsyncCell<bool>>,
    message_receiver: Rc<AsyncCell<Option<Receiver<Message>>>>,
    mqtt_client: Rc<AsyncCell<Option<paho_mqtt::AsyncClient>>>,
}

impl MQTTLocalityReceiver {
    fn new_base(mqtt_host: String, local_node: String, mqtt_port: Option<u16>) -> Self {
        let ready = AsyncCell::new_with(false).into_shared();
        let message_receiver = AsyncCell::new_with(None).into_shared();
        let mqtt_client = AsyncCell::new_with(None).into_shared();

        Self {
            mqtt_host,
            local_node,
            mqtt_port,
            ready,
            message_receiver,
            mqtt_client,
        }
    }

    pub fn new(mqtt_host: String, local_node: String) -> Self {
        Self::new_base(mqtt_host, local_node, None)
    }

    pub fn new_with_port(mqtt_host: String, local_node: String, port: u16) -> Self {
        Self::new_base(mqtt_host, local_node, Some(port))
    }

    fn topic(&self) -> String {
        format!("start_monitors_at_{}", self.local_node)
    }

    pub fn ready(&self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let ready = self.ready.clone();
        let message_receiver_cell = self.message_receiver.clone();
        let mqtt_client_cell = self.mqtt_client.clone();
        let mqtt_host = self.mqtt_host.clone();
        let mqtt_port = self.mqtt_port;
        let topic = self.topic();

        Box::pin(async move {
            // Check if already ready
            if ready.get().await {
                return Ok(());
            }

            debug!("Setting up MQTT connection and subscription for locality receiver");

            // Format the MQTT host into a proper URI if needed
            let mqtt_uri = if mqtt_host.starts_with("tcp://") || mqtt_host.starts_with("ssl://") {
                // Already a proper URI
                debug!(?mqtt_host, ?mqtt_port, "mqtt_host is proper uri");
                mqtt_host
            } else if mqtt_host.contains(':') {
                // Has port but no protocol, add tcp://
                debug!(?mqtt_host, ?mqtt_port, "mqtt_host already has port");
                format!("tcp://{}", mqtt_host)
            } else {
                // Just hostname, add protocol and port (use configured port or default to 1883)
                debug!(?mqtt_host, ?mqtt_port, "mqtt_host is plain hostname");
                let port = mqtt_port.unwrap_or(1883);
                format!("tcp://{}:{}", mqtt_host, port)
            };

            debug!("Connecting to MQTT broker at: {}", mqtt_uri);

            // Create channel for message passing
            let (sender, receiver) = async_channel::bounded::<Message>(10);

            // Create MQTT connection and establish subscription
            match provide_mqtt_client_with_subscription(mqtt_uri, u32::MAX).await {
                Ok((client, mut stream)) => {
                    debug!("Subscribing to topic: {}", topic);

                    // Subscribe and wait for acknowledgment
                    match client.subscribe(&topic, MQTT_QOS).await {
                        Ok(_) => {
                            debug!("Subscription confirmed for topic: {}", topic);

                            // Store the receiver for receive() to use
                            message_receiver_cell.set(Some(receiver));

                            // Store the client for cleanup
                            mqtt_client_cell.set(Some(client.clone()));

                            // Spawn background task to forward MQTT messages to channel
                            smol::spawn(async move {
                                while let Some(msg) = stream.next().await {
                                    debug!("Forwarding MQTT message to channel");
                                    if sender.send(msg).await.is_err() {
                                        debug!("Channel closed, stopping MQTT message forwarding");
                                        break;
                                    }
                                }
                                // Clean up connection when done
                                if let Err(e) = client.disconnect(None).await {
                                    debug!("Failed to disconnect MQTT client: {}", e);
                                }
                            })
                            .detach();

                            // Mark as ready only after subscription is confirmed
                            ready.set(true);
                            debug!("MQTT locality receiver is now ready and subscribed");
                            Ok(())
                        }
                        Err(e) => {
                            warn!("Failed to subscribe to topic {}: {}", topic, e);
                            Err(anyhow::anyhow!(
                                "Failed to subscribe to topic {}: {}",
                                topic,
                                e
                            ))
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to create MQTT client: {}", e);
                    Err(anyhow::anyhow!("Failed to create MQTT client: {}", e))
                }
            }
        })
    }

    /// Explicitly cleanup the receiver when done using it
    /// This clears the ready state and message receiver channel
    pub async fn cleanup(&self) {
        debug!("Cleaning up MQTT locality receiver");

        // Disconnect MQTT client if it exists
        if let Some(client) = self.mqtt_client.get().await {
            debug!("Disconnecting MQTT client");
            if let Err(e) = client.disconnect(None).await {
                debug!("Failed to disconnect MQTT client during cleanup: {}", e);
            }
        }

        self.ready.set(false);
        self.message_receiver.set(None);
        self.mqtt_client.set(None);
        debug!("MQTT locality receiver cleaned up");
    }
}

#[async_trait(?Send)]
impl LocalityReceiver for MQTTLocalityReceiver {
    async fn receive(&self) -> anyhow::Result<impl LocalitySpec + 'static> {
        debug!("Waiting for work assignment message");

        // Ensure we're ready (connection and subscription are established)
        if !self.ready.get().await {
            return Err(anyhow::anyhow!(
                "MQTT locality receiver not ready - call ready() first"
            ));
        }

        // Get the message receiver channel
        let receiver = match self.message_receiver.get().await {
            Some(r) => r,
            None => {
                return Err(anyhow::anyhow!(
                    "No message receiver available - ready() may not have been called"
                ));
            }
        };

        debug!("Waiting for message on topic: {}", self.topic());

        // Wait for the next message via channel - subscription is already established
        let result = match receiver.recv().await {
            Ok(msg) => {
                debug!("Received message: {}", msg.payload_str());
                let msg_content = msg.payload_str().to_string();

                let local_topics: Vec<String> = serde_json::from_str(&msg_content)
                    .map_err(|e| anyhow::anyhow!("Failed to parse work assignment JSON: {}", e))?;

                let var_names: Vec<VarName> = local_topics.into_iter().map(|s| s.into()).collect();

                info!(
                    "Received work assignment for {}: {:?}",
                    self.local_node, var_names
                );
                Ok(var_names)
            }
            Err(_) => Err(anyhow::anyhow!("Channel closed - no message received")),
        };

        // Note: We do NOT clear the ready state or message receiver here
        // The receiver should remain ready to receive multiple messages
        // without needing to call ready() again
        // If cleanup is needed, call cleanup() explicitly when done

        result
    }
}
