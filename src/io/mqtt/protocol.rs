use std::collections::BTreeMap;

use crate::VarName;
use crate::core::{InputStream, JsonStreamValue, LocalStream};

use crate::io::ReconfigurationRequest;
use crate::io::RetryPolicy;

#[derive(Debug)]
pub(crate) enum MqttInputItem<V> {
    Data(crate::InputBatch<V>),
    Control(ReconfigurationRequest),
    Boundary(u64),
}

pub(super) type VarTopicMap = BTreeMap<VarName, String>;
pub(super) type InverseVarTopicMap = BTreeMap<String, VarName>;

pub(crate) fn validate_input_format(format: &crate::core::FormatId) -> anyhow::Result<()> {
    anyhow::ensure!(
        matches!(format.as_str(), "json" | "json5"),
        "MQTT input format `{format}` is unsupported; expected `json` or `json5`"
    );
    Ok(())
}

/// MQTT wire protocol used for broker connections.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum MqttProtocol {
    #[serde(rename = "3.1.1")]
    #[default]
    V311,
    #[serde(rename = "5")]
    V5,
}

impl std::fmt::Display for MqttProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::V311 => "3.1.1",
            Self::V5 => "5",
        })
    }
}

impl std::str::FromStr for MqttProtocol {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "3.1.1" => Ok(Self::V311),
            "5" => Ok(Self::V5),
            _ => Err(format!(
                "unsupported MQTT protocol `{value}`; expected `3.1.1` or `5`"
            )),
        }
    }
}

impl MqttProtocol {
    pub(crate) async fn open_owned_items<V: JsonStreamValue>(
        self,
        host: &str,
        port: Option<u16>,
        var_topics: VarTopicMap,
        retry: RetryPolicy,
    ) -> anyhow::Result<(
        LocalStream<anyhow::Result<MqttInputItem<V>>>,
        super::rumqttc_input_stream::RumqttcInputControl,
    )> {
        super::rumqttc_input_stream::owned_input_stream_items(self, host, port, var_topics, retry)
            .await
    }

    pub(crate) async fn open_owned_data<V: JsonStreamValue>(
        self,
        host: &str,
        port: Option<u16>,
        var_topics: VarTopicMap,
        retry: RetryPolicy,
    ) -> anyhow::Result<(
        InputStream<V>,
        super::rumqttc_input_stream::RumqttcInputControl,
    )> {
        let (items, owner) = super::rumqttc_input_stream::owned_input_stream_items(
            self, host, port, var_topics, retry,
        )
        .await?;
        let stream = Box::pin(
            async_stream::try_stream! { let mut items=items; while let Some(item)=futures::StreamExt::next(&mut items).await { match item? { MqttInputItem::Data(batch)=>yield batch, MqttInputItem::Control(_)=>unreachable!("data-only MQTT stream cannot receive control"), MqttInputItem::Boundary(_)=>unreachable!("data-only MQTT stream cannot receive boundary") } } },
        );
        Ok((stream, owner))
    }
    pub(crate) async fn open_reconfigurable<V: JsonStreamValue>(
        self,
        host: &str,
        port: Option<u16>,
        var_topics: VarTopicMap,
        retry: RetryPolicy,
        control_topic: String,
    ) -> anyhow::Result<(
        LocalStream<anyhow::Result<MqttInputItem<V>>>,
        super::rumqttc_input_stream::RumqttcInputControl,
    )> {
        super::rumqttc_input_stream::reconfigurable_input_stream_items(
            self,
            host,
            port,
            var_topics,
            retry,
            control_topic,
        )
        .await
    }

    pub(crate) async fn open_data<V: JsonStreamValue>(
        self,
        host: &str,
        port: Option<u16>,
        var_topics: VarTopicMap,
        retry: RetryPolicy,
    ) -> anyhow::Result<InputStream<V>> {
        let (items, mut owner) = super::rumqttc_input_stream::owned_input_stream_items(
            self, host, port, var_topics, retry,
        )
        .await?;
        Ok(Box::pin(async_stream::try_stream! {
            let mut items = items;
            while let Some(item) = futures::StreamExt::next(&mut items).await {
                match item? {
                    MqttInputItem::Data(batch) => yield batch,
                    MqttInputItem::Control(_) => unreachable!("data-only MQTT stream cannot receive control"),
                    MqttInputItem::Boundary(_) => unreachable!("data-only MQTT stream cannot receive boundary"),
                }
            }
            owner.shutdown().await?;
        }))
    }
}

pub async fn input_stream<V: JsonStreamValue>(
    protocol: MqttProtocol,
    host: &str,
    port: Option<u16>,
    var_topics: VarTopicMap,
    retry: RetryPolicy,
) -> anyhow::Result<InputStream<V>> {
    protocol.open_data(host, port, var_topics, retry).await
}

pub(super) fn invert_topic_mapping(var_topics: &VarTopicMap) -> InverseVarTopicMap {
    var_topics
        .iter()
        .map(|(variable, topic)| (topic.clone(), variable.clone()))
        .collect()
}

pub(super) fn validate_topic_mapping(
    var_topics: &VarTopicMap,
    control_topic: Option<&str>,
) -> anyhow::Result<()> {
    if let Some(control_topic) = control_topic {
        anyhow::ensure!(
            !control_topic.trim().is_empty(),
            "MQTT control topic cannot be empty"
        );
    }
    let mut mapped_topics = BTreeMap::new();
    for (var, topic) in var_topics {
        anyhow::ensure!(
            !var.name().trim().is_empty(),
            "MQTT input variable cannot be empty"
        );
        anyhow::ensure!(
            !topic.trim().is_empty(),
            "MQTT input route for `{var}` cannot be empty"
        );
        if let Some(control_topic) = control_topic
            && topic == control_topic
        {
            anyhow::bail!(
                "MQTT control topic `{control_topic}` collides with input topic `{topic}` for variable `{var}`"
            );
        }
        if let Some(previous) = mapped_topics.insert(topic, var) {
            anyhow::bail!("MQTT input topic `{topic}` is mapped to both `{previous}` and `{var}`");
        }
    }
    Ok(())
}

pub(super) fn decode_payload<V: JsonStreamValue>(payload: &[u8]) -> anyhow::Result<V> {
    V::decode_mqtt_payload(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;

    #[test]
    fn mqtt_311_is_the_default_protocol() {
        assert_eq!(MqttProtocol::default(), MqttProtocol::V311);
    }

    #[test]
    fn protocol_serde_uses_wire_version_spellings() {
        assert_eq!(
            serde_json::to_string(&MqttProtocol::V311).unwrap(),
            "\"3.1.1\""
        );
        assert_eq!(serde_json::to_string(&MqttProtocol::V5).unwrap(), "\"5\"");
        assert_eq!(
            serde_json::from_str::<MqttProtocol>("\"3.1.1\"").unwrap(),
            MqttProtocol::V311
        );
        assert_eq!(
            serde_json::from_str::<MqttProtocol>("\"5\"").unwrap(),
            MqttProtocol::V5
        );
    }

    #[test]
    fn mqtt_payload_decoder_accepts_json5() {
        let value = decode_payload::<Value>(br#"{value: [1, "two",],}"#).unwrap();
        assert_eq!(
            value,
            Value::List(vec![Value::Int(1), Value::Str("two".into())].into())
        );
    }
}
