use std::collections::BTreeMap;

use crate::VarName;
use crate::core::{InputStream, JsonStreamValue, OutputStream};
use ::core::cfg_select;

use crate::io::ReconfigurationRequest;

#[derive(Debug)]
pub(crate) enum MqttInputItem<V> {
    Data(crate::InputBatch<V>),
    Control(ReconfigurationRequest),
}

pub(super) type VarTopicMap = BTreeMap<VarName, String>;
pub(super) type InverseVarTopicMap = BTreeMap<String, VarName>;

/// MQTT client implementation used for input subscriptions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MqttInputBackend {
    #[default]
    Rumqttc,
    Paho,
}

impl MqttInputBackend {
    pub(crate) async fn open_items<V: JsonStreamValue>(
        self,
        host: &str,
        port: Option<u16>,
        var_topics: VarTopicMap,
        max_reconnect_attempts: u32,
        control_topic: Option<String>,
    ) -> anyhow::Result<OutputStream<anyhow::Result<MqttInputItem<V>>>> {
        validate_topic_mapping(&var_topics, control_topic.as_deref())?;
        match self {
            Self::Rumqttc => {
                super::rumqttc_input_stream::input_stream_items(
                    host,
                    port,
                    var_topics,
                    max_reconnect_attempts,
                    control_topic,
                )
                .await
            }
            Self::Paho => {
                cfg_select! {
                    feature = "mqtt" => {
                    let items = super::input_stream::input_stream_items(
                        host,
                        port,
                        var_topics,
                        max_reconnect_attempts,
                        control_topic,
                    )
                    .await?;
                    Ok(items)
                    },
                    _ => {
                        let _ = (
                            host,
                            port,
                            var_topics,
                            max_reconnect_attempts,
                            control_topic,
                        );
                        anyhow::bail!("Paho MQTT support not enabled")
                    },
                }
            }
        }
    }

    pub(crate) async fn open_data<V: JsonStreamValue>(
        self,
        host: &str,
        port: Option<u16>,
        var_topics: VarTopicMap,
        max_reconnect_attempts: u32,
    ) -> anyhow::Result<InputStream<V>> {
        let items = self
            .open_items(host, port, var_topics, max_reconnect_attempts, None)
            .await?;
        Ok(Box::pin(async_stream::try_stream! {
            let mut items = items;
            while let Some(item) = futures::StreamExt::next(&mut items).await {
                match item? {
                    MqttInputItem::Data(batch) => yield batch,
                    MqttInputItem::Control(_) => unreachable!("data-only MQTT stream cannot receive control"),
                }
            }
        }))
    }
}

pub async fn input_stream<V: JsonStreamValue>(
    backend: MqttInputBackend,
    host: &str,
    port: Option<u16>,
    var_topics: VarTopicMap,
    max_reconnect_attempts: u32,
) -> anyhow::Result<InputStream<V>> {
    backend
        .open_data(host, port, var_topics, max_reconnect_attempts)
        .await
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
