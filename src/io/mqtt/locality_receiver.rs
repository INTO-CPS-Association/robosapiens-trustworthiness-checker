use std::rc::Rc;

use async_cell::unsync::AsyncCell;
use async_trait::async_trait;
use futures::StreamExt;
use futures::future::LocalBoxFuture;

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
    ready: Rc<AsyncCell<bool>>,
}

impl MQTTLocalityReceiver {
    pub fn new(mqtt_host: String, local_node: String) -> Self {
        let ready = AsyncCell::new_with(false).into_shared();
        Self {
            mqtt_host,
            local_node,
            ready,
        }
    }

    fn topic(&self) -> String {
        format!("start_monitors_at_{}", self.local_node)
    }

    pub fn ready(&self) -> LocalBoxFuture<'static, ()> {
        let ready = self.ready.clone();
        Box::pin(async move {
            while !ready.get().await {
                smol::future::yield_now().await;
            }
        })
    }
}

#[async_trait(?Send)]
impl LocalityReceiver for MQTTLocalityReceiver {
    async fn receive(&self) -> anyhow::Result<impl LocalitySpec + 'static> {
        let (client, mut stream) =
            provide_mqtt_client_with_subscription(self.mqtt_host.clone(), u32::MAX).await?;
        client.subscribe(self.topic(), MQTT_QOS).await?;
        self.ready.set(true);
        match stream.next().await {
            Some(msg) => {
                let msg_content = msg.payload_str().to_string();
                let local_topics: Vec<String> = serde_json::from_str(&msg_content)?;
                let local_topics: Vec<VarName> =
                    local_topics.into_iter().map(|s| s.into()).collect();
                Ok(local_topics)
            }
            None => Err(anyhow::anyhow!("No message received")),
        }
    }
}
