use std::collections::BTreeMap;

use crate::{InputStream, Value, VarName};

type VarTopicMap = BTreeMap<VarName, String>;

/// MQTT client implementation used for input subscriptions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MqttInputBackend {
    /// The pure-Rust input client used by default.
    #[default]
    Rumqttc,
    /// The legacy Paho input client.
    Paho,
}

impl MqttInputBackend {
    async fn open(
        self,
        host: &str,
        port: Option<u16>,
        var_topics: VarTopicMap,
        max_reconnect_attempts: u32,
    ) -> anyhow::Result<InputStream<Value>> {
        validate_topic_mapping(&var_topics)?;
        match self {
            Self::Rumqttc => {
                super::rumqttc_input_stream::input_stream(
                    host,
                    port,
                    var_topics,
                    max_reconnect_attempts,
                )
                .await
            }
            Self::Paho => {
                super::input_stream::input_stream(host, port, var_topics, max_reconnect_attempts)
                    .await
            }
        }
    }
}

/// Open an MQTT input stream with the selected backend.
pub async fn input_stream(
    backend: MqttInputBackend,
    host: &str,
    port: Option<u16>,
    var_topics: VarTopicMap,
    max_reconnect_attempts: u32,
) -> anyhow::Result<InputStream<Value>> {
    backend
        .open(host, port, var_topics, max_reconnect_attempts)
        .await
}

fn validate_topic_mapping(var_topics: &VarTopicMap) -> anyhow::Result<()> {
    let mut mapped_topics = BTreeMap::new();
    for (var, topic) in var_topics {
        if let Some(previous) = mapped_topics.insert(topic, var) {
            anyhow::bail!("MQTT input topic `{topic}` is mapped to both `{previous}` and `{var}`");
        }
    }
    Ok(())
}

pub(super) fn parse_value(payload: &[u8]) -> anyhow::Result<Value> {
    let mut value: Value = match serde_json::from_slice(payload) {
        Ok(value) => value,
        Err(json_error) => {
            let payload = std::str::from_utf8(payload)?;
            serde_json5::from_str(payload).map_err(|json5_error| {
                anyhow::anyhow!(json5_error).context(format!(
                    "Failed to parse value {payload:?} sent from MQTT; JSON error was {json_error}"
                ))
            })?
        }
    };
    if let Value::Map(map) = &mut value
        && let Some(inner) = map.remove("value")
    {
        value = inner;
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rumqttc_is_the_default_input_backend() {
        assert_eq!(MqttInputBackend::default(), MqttInputBackend::Rumqttc);
    }

    #[test]
    fn duplicate_topics_are_rejected_before_connecting() {
        smol::block_on(async {
            let result = MqttInputBackend::Rumqttc
                .open(
                    "unreachable.invalid",
                    None,
                    BTreeMap::from([
                        (VarName::new("x"), "shared".to_owned()),
                        (VarName::new("y"), "shared".to_owned()),
                    ]),
                    0,
                )
                .await;
            let error = match result {
                Ok(_) => panic!("duplicate topic mapping should be rejected"),
                Err(error) => error,
            };
            assert_eq!(
                error.to_string(),
                "MQTT input topic `shared` is mapped to both `x` and `y`"
            );
        });
    }

    #[test]
    fn wrapped_values_are_decoded_consistently() {
        assert_eq!(parse_value(br#"{"value": 42}"#).unwrap(), Value::Int(42));
    }

    #[test]
    fn empty_inputs_need_no_connection_for_either_backend() {
        smol::block_on(async {
            for backend in [MqttInputBackend::Rumqttc, MqttInputBackend::Paho] {
                let mut input = backend
                    .open("unreachable.invalid", None, BTreeMap::new(), 0)
                    .await
                    .unwrap();
                assert!(futures::StreamExt::next(&mut input).await.is_none());
            }
        });
    }
}
