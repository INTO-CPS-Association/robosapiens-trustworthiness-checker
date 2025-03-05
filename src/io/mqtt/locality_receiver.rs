use async_trait::async_trait;
use tokio_stream::StreamExt;

use crate::{
    VarName, distributed::locality_receiver::LocalityReceiver,
    semantics::distributed::localisation::LocalitySpec,
};

use super::provide_mqtt_client_with_subscription;

const MQTT_QOS: i32 = 1;

pub struct MQTTLocalityReceiver {
    mqtt_host: String,
    local_node: String,
}

impl MQTTLocalityReceiver {
    pub fn new(mqtt_host: String, local_node: String) -> Self {
        Self {
            mqtt_host,
            local_node,
        }
    }

    fn topic(&self) -> String {
        format!("start_monitors_at_{}", self.local_node)
    }
}

#[async_trait]
impl LocalityReceiver for MQTTLocalityReceiver {
    async fn receive(&self) -> Result<impl LocalitySpec + 'static, Box<dyn std::error::Error>> {
        let (client, mut stream) =
            provide_mqtt_client_with_subscription(self.mqtt_host.clone()).await?;
        client.subscribe(self.topic(), MQTT_QOS).await?;
        match stream.next().await {
            Some(msg) => {
                let msg_content = msg.payload_str().to_string();
                let local_topics: Vec<String> = serde_json::from_str(&msg_content)?;
                let local_topics: Vec<VarName> =
                    local_topics.into_iter().map(|s| s.into()).collect();
                Ok(local_topics)
            }
            None => Err("No message received".into()),
        }
    }
}
