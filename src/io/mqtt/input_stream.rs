use anyhow::Context;
use futures::{FutureExt, StreamExt};
use std::collections::BTreeMap;
use tracing::{Level, debug, info, info_span, instrument, warn};

use super::input_backend::MqttInputItem;
use crate::core::{InputBatch, JsonStreamValue, OutputStream, VarName};
use crate::io::MonitorConfig;
use crate::io::mqtt::{MqttFactory, MqttMessage};
use crate::utils::cancellation_token::CancellationToken;

type VarTopicMap = BTreeMap<VarName, String>;
type InverseVarTopicMap = BTreeMap<String, VarName>;
const QOS: i32 = 1;

#[instrument(level = Level::INFO, skip(var_topics))]
pub(crate) async fn input_stream_items<V: JsonStreamValue>(
    host: &str,
    port: Option<u16>,
    var_topics: VarTopicMap,
    max_attempts: u32,
    control_topic: Option<String>,
) -> anyhow::Result<OutputStream<anyhow::Result<MqttInputItem<V>>>> {
    if var_topics.is_empty() && control_topic.is_none() {
        return Ok(Box::pin(futures::stream::empty()));
    }
    let uri = match port {
        Some(port) => format!("tcp://{host}:{port}"),
        None => format!("tcp://{host}"),
    };
    info!(%uri, topics = var_topics.len(), "Connecting MQTT input stream");
    let (client, stream) = MqttFactory::Paho
        .connect_and_receive(&uri, max_attempts)
        .await?;
    let stream: OutputStream<MqttMessage> = stream;
    let mut topics = var_topics.values().cloned().collect::<Vec<_>>();
    if let Some(control_topic) = &control_topic {
        topics.push(control_topic.clone());
    }
    let qos = vec![QOS; topics.len()];
    let mut retries = 0;
    loop {
        match client.subscribe_many(&topics, &qos).await {
            Ok(_) => break,
            Err(error) if retries < max_attempts => {
                retries += 1;
                warn!(?error, retries, "Failed to subscribe to MQTT input topics");
                smol::Timer::after(std::time::Duration::from_millis(100)).await;
                client.reconnect().await?;
            }
            Err(error) => return Err(error),
        }
    }
    let drop_guard = CancellationToken::new().drop_guard();
    let cancellation_token = drop_guard.clone_tok();
    Ok(Box::pin(async_stream::try_stream! {
        let _drop_guard = drop_guard;
        let mqtt_input_span = info_span!("Paho MQTT input stream");
        let _enter = mqtt_input_span.enter();
        let mut mqtt_stream = stream;
        let var_topics_inverse = invert_topics(var_topics);
        let mut terminal_error = None;

        loop {
            let msg = futures::select! {
                msg = mqtt_stream.next().fuse() => msg,
                _ = cancellation_token.cancelled().fuse() => {
                    debug!("Paho MQTT input stream cancelled");
                    break;
                }
            };
            let Some(msg) = msg else {
                debug!("MQTT stream ended");
                break;
            };
            if control_topic.as_deref() == Some(msg.topic.as_str()) {
                let request = MonitorConfig::from_json(&msg.payload)
                    .context("invalid MQTT monitor configuration")?;
                yield MqttInputItem::Control(request);
                break;
            }
            match parse_event::<V>(msg, &var_topics_inverse) {
                Ok(Some(batch)) => yield MqttInputItem::Data(batch),
                Ok(None) => {}
                Err(error) => {
                    terminal_error = Some(error);
                    break;
                }
            }
        }

        debug!("Disconnecting MQTT client");
        let _ = client.disconnect().await;
        if let Some(error) = terminal_error {
            Err(error)?;
        }
    }))
}

fn invert_topics(var_topics: VarTopicMap) -> InverseVarTopicMap {
    var_topics
        .into_iter()
        .map(|(var, topic)| (topic, var))
        .collect()
}

fn parse_event<V: JsonStreamValue>(
    msg: MqttMessage,
    var_topics_inverse: &InverseVarTopicMap,
) -> anyhow::Result<Option<InputBatch<V>>> {
    let Some(var) = var_topics_inverse.get(&msg.topic).cloned() else {
        return Ok(None);
    };
    let value = super::input_backend::decode_payload::<V>(msg.payload.as_bytes())
        .with_context(|| format!("failed to parse value for MQTT variable `{var}`"))?;
    Ok(Some(InputBatch::update(var, value)))
}
