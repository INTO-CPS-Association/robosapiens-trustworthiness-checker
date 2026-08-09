use std::collections::BTreeMap;

use crate::VarName;
use crate::core::{InputStream, JsonStreamValue, OutputStream};

use crate::io::MonitorConfig;

#[derive(Debug)]
pub(crate) enum MqttInputItem<V> {
    Data(crate::InputBatch<V>),
    Control(MonitorConfig),
}

type VarTopicMap = BTreeMap<VarName, String>;

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
                #[cfg(feature = "mqtt")]
                {
                    let items = super::input_stream::input_stream_items(
                        host,
                        port,
                        var_topics,
                        max_reconnect_attempts,
                        control_topic,
                    )
                    .await?;
                    Ok(items)
                }
                #[cfg(not(feature = "mqtt"))]
                {
                    let _ = (
                        host,
                        port,
                        var_topics,
                        max_reconnect_attempts,
                        control_topic,
                    );
                    anyhow::bail!("Paho MQTT support not enabled")
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

fn validate_topic_mapping(
    var_topics: &VarTopicMap,
    control_topic: Option<&str>,
) -> anyhow::Result<()> {
    let mut mapped_topics = BTreeMap::new();
    for (var, topic) in var_topics {
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
    fn duplicate_topics_are_rejected_before_connecting() {
        smol::block_on(async {
            let result = MqttInputBackend::Rumqttc
                .open_items::<Value>(
                    "unreachable.invalid",
                    None,
                    BTreeMap::from([
                        (VarName::new("x"), "shared".to_owned()),
                        (VarName::new("y"), "shared".to_owned()),
                    ]),
                    0,
                    None,
                )
                .await;
            let error = match result {
                Ok(_) => panic!("duplicate MQTT topics should be rejected"),
                Err(error) => error,
            };
            assert!(error.to_string().contains("mapped to both"));
        });
    }
}
