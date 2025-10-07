use async_trait::async_trait;
use tracing::debug;

use crate::{
    VarName,
    distributed::{
        distribution_graphs::NodeName, scheduling::communication::SchedulerCommunicator,
    },
    io::mqtt::{MqttFactory, MqttMessage},
};

const MQTT_FACTORY: MqttFactory = MqttFactory::Paho;

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
        let mqtt_client = MQTT_FACTORY.connect(&self.mqtt_uri).await?;
        let work_msg = serde_json::to_string(&work)?;
        let work_topic = format!("start_monitors_at_{}", node);
        debug!("Scheduler sending work on topic {:?}", work_topic);
        let work_msg = MqttMessage::new(work_topic.clone(), work_msg, 2);
        mqtt_client.publish(work_msg).await?;

        Ok(())
    }
}
