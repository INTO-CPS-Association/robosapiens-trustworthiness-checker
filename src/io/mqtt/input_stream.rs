use futures::{FutureExt, StreamExt};
use std::collections::BTreeMap;
use tracing::{Level, debug, info, info_span, instrument, warn};

use crate::{
    InputBatch, InputEvent, InputStream, OutputStream, Value, VarName,
    io::mqtt::{MqttFactory, MqttMessage},
    utils::cancellation_token::CancellationToken,
};

type VarTopicMap = BTreeMap<VarName, String>;
type InverseVarTopicMap = BTreeMap<String, VarName>;
const QOS: i32 = 1;

/// Connect and subscribe to MQTT topics, returning the resulting input stream.
#[instrument(level = Level::INFO, skip(var_topics))]
pub async fn input_stream(
    host: &str,
    port: Option<u16>,
    var_topics: VarTopicMap,
    max_attempts: u32,
) -> anyhow::Result<InputStream<Value>> {
    if var_topics.is_empty() {
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
    let topics = var_topics.values().cloned().collect::<Vec<_>>();
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
            match parse_event(msg, &var_topics_inverse) {
                Ok(Some(event)) => yield InputBatch::events(vec![event]),
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

fn parse_event(
    msg: MqttMessage,
    var_topics_inverse: &InverseVarTopicMap,
) -> anyhow::Result<Option<InputEvent<Value>>> {
    let Some(var) = var_topics_inverse.get(&msg.topic).cloned() else {
        return Ok(None);
    };
    let value = super::input_backend::parse_value(msg.payload.as_bytes())?;
    Ok(Some(InputEvent::new(var, value)))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::input_stream;

    #[test]
    fn empty_input_needs_no_connection() {
        smol::block_on(async {
            assert!(
                input_stream("localhost", None, BTreeMap::new(), 0)
                    .await
                    .is_ok()
            );
        });
    }
}
