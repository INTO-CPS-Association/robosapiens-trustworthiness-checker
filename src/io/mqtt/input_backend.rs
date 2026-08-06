use std::collections::BTreeMap;

use crate::core::JsonStreamValue;
use crate::{InputStream, VarName};

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
    async fn open<V: JsonStreamValue>(
        self,
        host: &str,
        port: Option<u16>,
        var_topics: VarTopicMap,
        max_reconnect_attempts: u32,
    ) -> anyhow::Result<InputStream<V>> {
        validate_topic_mapping(&var_topics)?;
        match self {
            Self::Rumqttc => {
                super::rumqttc_input_stream::input_stream::<V>(
                    host,
                    port,
                    var_topics,
                    max_reconnect_attempts,
                )
                .await
            }
            Self::Paho => {
                #[cfg(feature = "mqtt")]
                {
                    super::input_stream::input_stream::<V>(
                        host,
                        port,
                        var_topics,
                        max_reconnect_attempts,
                    )
                    .await
                }
                #[cfg(not(feature = "mqtt"))]
                {
                    let _ = (host, port, var_topics, max_reconnect_attempts);
                    anyhow::bail!("Paho MQTT support not enabled")
                }
            }
        }
    }
}

/// Open an MQTT input stream with the selected backend.
pub async fn input_stream<V: JsonStreamValue>(
    backend: MqttInputBackend,
    host: &str,
    port: Option<u16>,
    var_topics: VarTopicMap,
    max_reconnect_attempts: u32,
) -> anyhow::Result<InputStream<V>> {
    backend
        .open::<V>(host, port, var_topics, max_reconnect_attempts)
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
                .open::<Value>(
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
        assert_eq!(
            decode_payload::<Value>(br#"{"value": 42}"#).unwrap(),
            Value::Int(42)
        );
        assert_eq!(
            decode_payload::<Value>(br#"{"value":42,"source":"sensor-a"}"#).unwrap(),
            Value::Int(42)
        );
    }

    #[test]
    fn natural_timed_objects_are_not_unwrapped() {
        use crate::runtime::mstlo::{MstloTimedValue, MstloValue};
        use std::time::Duration;

        let natural = decode_payload::<MstloTimedValue>(br#"{"time":10,"value":2.5}"#).unwrap();
        assert_eq!(
            natural,
            MstloTimedValue::new(Duration::from_millis(10), MstloValue::Float(2.5))
        );

        let wrapped =
            decode_payload::<MstloTimedValue>(br#"{"value":{"time":10,"value":2.5}}"#).unwrap();
        assert_eq!(wrapped, natural);

        let wrapped_with_metadata = decode_payload::<MstloTimedValue>(
            br#"{"value":{"time":10,"value":2.5},"source":"sensor-a"}"#,
        )
        .unwrap();
        assert_eq!(wrapped_with_metadata, natural);
    }

    #[test]
    fn wrapped_timed_values_preserve_non_finite_numbers() {
        use crate::runtime::mstlo::{MstloTimedValue, MstloValue};
        use std::time::Duration;

        for (text, expected) in [
            ("Infinity", f64::INFINITY),
            ("-Infinity", f64::NEG_INFINITY),
        ] {
            let payload = format!(r#"{{"value":{{"time":10,"value":{text}}}}}"#);
            assert_eq!(
                decode_payload::<MstloTimedValue>(payload.as_bytes()).unwrap(),
                MstloTimedValue::new(Duration::from_millis(10), MstloValue::Float(expected))
            );
        }

        let value =
            decode_payload::<MstloTimedValue>(br#"{"value":{"time":10,"value":NaN}}"#).unwrap();
        assert!(matches!(value.value, MstloValue::Float(value) if value.is_nan()));
    }

    #[test]
    fn empty_rumqttc_input_needs_no_connection() {
        smol::block_on(async {
            let mut input = MqttInputBackend::Rumqttc
                .open::<Value>("unreachable.invalid", None, BTreeMap::new(), 0)
                .await
                .unwrap();
            assert!(futures::StreamExt::next(&mut input).await.is_none());
        });
    }

    #[cfg(feature = "mqtt")]
    #[test]
    fn empty_paho_input_needs_no_connection() {
        smol::block_on(async {
            let mut input = MqttInputBackend::Paho
                .open::<Value>("unreachable.invalid", None, BTreeMap::new(), 0)
                .await
                .unwrap();
            assert!(futures::StreamExt::next(&mut input).await.is_none());
        });
    }
}
