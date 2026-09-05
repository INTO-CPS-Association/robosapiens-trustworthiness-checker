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

/// MQTT client implementation used for input subscriptions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MqttInputBackend {
    #[default]
    Rumqttc,
}

impl MqttInputBackend {
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
        let (items, owner) =
            super::rumqttc_input_stream::owned_input_stream_items(host, port, var_topics, retry)
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
        let (items, mut owner) =
            super::rumqttc_input_stream::owned_input_stream_items(host, port, var_topics, retry)
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
    backend: MqttInputBackend,
    host: &str,
    port: Option<u16>,
    var_topics: VarTopicMap,
    retry: RetryPolicy,
) -> anyhow::Result<InputStream<V>> {
    backend.open_data(host, port, var_topics, retry).await
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
    fn rumqttc_is_the_default_input_backend() {
        assert_eq!(MqttInputBackend::default(), MqttInputBackend::Rumqttc);
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
