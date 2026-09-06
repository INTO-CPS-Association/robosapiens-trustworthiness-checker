use crate::{
    core::InputError,
    io::{RetryPolicy, mqtt::MqttProtocol},
};
use futures::stream::BoxStream;
use tracing::info;

pub(crate) mod driver;
pub use driver::MqttClient;

pub async fn connect(uri: &str) -> anyhow::Result<MqttClient> {
    info!(?uri, "Connecting to MQTT broker");
    connect_with_retry(uri, RetryPolicy::output_default()).await
}
pub async fn connect_with_protocol(
    uri: &str,
    protocol: MqttProtocol,
) -> anyhow::Result<MqttClient> {
    connect_with_protocol_and_retry(uri, protocol, RetryPolicy::output_default()).await
}
pub async fn connect_with_retry(uri: &str, retry: RetryPolicy) -> anyhow::Result<MqttClient> {
    connect_with_protocol_and_retry(uri, MqttProtocol::default(), retry).await
}
pub async fn connect_with_protocol_and_retry(
    uri: &str,
    protocol: MqttProtocol,
    retry: RetryPolicy,
) -> anyhow::Result<MqttClient> {
    driver::connect(uri, protocol, retry).await
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
    connect_and_receive_with_protocol_and_retry(uri, MqttProtocol::default(), retry).await
}
pub async fn connect_and_receive_with_protocol_and_retry(
    uri: &str,
    protocol: MqttProtocol,
    retry: RetryPolicy,
) -> anyhow::Result<(
    MqttClient,
    BoxStream<'static, Result<MqttMessage, InputError>>,
)> {
    driver::connect_and_receive(uri, protocol, retry).await
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
