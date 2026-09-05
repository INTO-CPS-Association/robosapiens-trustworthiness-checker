use crate::{core::InputError, io::RetryPolicy};
use futures::stream::BoxStream;
use tracing::info;

pub(crate) mod mqtt311;
pub use mqtt311::MqttClient;

pub async fn connect(uri: &str) -> anyhow::Result<MqttClient> {
    info!(?uri, "Connecting to MQTT broker");
    connect_with_retry(uri, RetryPolicy::output_default()).await
}
pub async fn connect_with_retry(uri: &str, retry: RetryPolicy) -> anyhow::Result<MqttClient> {
    mqtt311::connect(uri, retry).await
}
pub async fn connect_and_receive(
    uri: &str,
) -> anyhow::Result<(
    MqttClient,
    BoxStream<'static, Result<MqttMessage, InputError>>,
)> {
    info!(?uri, "Connecting to MQTT broker and opening receive stream");
    connect_and_receive_with_retry(uri, RetryPolicy::input_default()).await
}
pub async fn connect_and_receive_with_retry(
    uri: &str,
    retry: RetryPolicy,
) -> anyhow::Result<(
    MqttClient,
    BoxStream<'static, Result<MqttMessage, InputError>>,
)> {
    mqtt311::connect_and_receive(uri, retry).await
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MqttMessage {
    pub topic: String,
    pub payload: String,
    pub qos: i32,
}
impl MqttMessage {
    pub fn new(topic: String, payload: String, qos: i32) -> Self {
        Self {
            topic,
            payload,
            qos,
        }
    }
}
