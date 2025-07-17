use async_trait::async_trait;
use paho_mqtt::{self as mqtt};
use tracing::debug;

use crate::{
    VarName,
    distributed::{
        distribution_graphs::NodeName, scheduling::communication::SchedulerCommunicator,
    },
};

use super::provide_mqtt_client;

pub struct MQTTSchedulerCommunicator {
    mqtt_uri: String,
}

impl MQTTSchedulerCommunicator {
    pub fn new(mqtt_uri: String) -> Self {
        Self { mqtt_uri }
    }
}

#[async_trait(?Send)]
impl SchedulerCommunicator for MQTTSchedulerCommunicator {
    async fn schedule_work(
        &mut self,
        node: NodeName,
        work: Vec<VarName>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mqtt_client = provide_mqtt_client(self.mqtt_uri.clone()).await?;
        let work_msg = serde_json::to_string(&work)?;
        let work_topic = format!("start_monitors_at_{}", node);
        debug!("Scheduler sending work on topic {:?}", work_topic);
        let work_msg = mqtt::Message::new(work_topic, work_msg, 2);
        mqtt_client.publish(work_msg).await?;

        Ok(())
    }
}
