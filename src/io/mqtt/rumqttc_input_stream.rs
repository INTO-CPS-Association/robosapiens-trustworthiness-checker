use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::Context;
use async_compat::Compat as TokioCompat;
use futures::FutureExt;
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, MqttOptions, QoS, SubscribeFilter};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::core::{InputBatch, JsonStreamValue, OutputStream, VarName};
use crate::io::MonitorConfig;
use crate::utils::cancellation_token::CancellationToken;

use super::input_backend::MqttInputItem;

type VarTopicMap = BTreeMap<VarName, String>;
type InverseVarTopicMap = BTreeMap<String, VarName>;
const RUMQTTC_CHANNEL_SIZE: usize = 1024;

async fn poll_next_item<V: JsonStreamValue>(
    eventloop: &mut EventLoop,
    topics: &InverseVarTopicMap,
    control_topic: Option<&str>,
    max_reconnect_attempts: u32,
    reconnect_attempts: &mut u32,
) -> anyhow::Result<MqttInputItem<V>> {
    loop {
        match TokioCompat::new(eventloop.poll()).await {
            Ok(Event::Incoming(Incoming::Publish(publish))) => {
                *reconnect_attempts = 0;
                if control_topic == Some(publish.topic.as_str()) {
                    let payload = std::str::from_utf8(&publish.payload)
                        .context("MQTT monitor configuration is not UTF-8")?;
                    return Ok(MqttInputItem::Control(MonitorConfig::from_json(payload)?));
                }
                let Some(variable) = topics.get(&publish.topic).cloned() else {
                    continue;
                };
                let value = super::input_backend::decode_payload::<V>(&publish.payload)
                    .with_context(|| {
                        format!("failed to parse value for MQTT variable `{variable}`")
                    })?;
                return Ok(MqttInputItem::Data(InputBatch::update(variable, value)));
            }
            Ok(_) => *reconnect_attempts = 0,
            Err(error) => {
                *reconnect_attempts += 1;
                if max_reconnect_attempts != u32::MAX
                    && *reconnect_attempts > max_reconnect_attempts
                {
                    return Err(error.into());
                }
                warn!(
                    ?error,
                    reconnect_attempts, "rumqttc poll failed; waiting for reconnection"
                );
                smol::Timer::after(Duration::from_millis(100)).await;
            }
        }
    }
}

pub(super) async fn input_stream_items<V: JsonStreamValue>(
    host: &str,
    port: Option<u16>,
    var_topics: VarTopicMap,
    max_reconnect_attempts: u32,
    control_topic: Option<String>,
) -> anyhow::Result<OutputStream<anyhow::Result<MqttInputItem<V>>>> {
    if var_topics.is_empty() && control_topic.is_none() {
        return Ok(Box::pin(futures::stream::empty()));
    }

    let mut options = MqttOptions::new(
        format!("robosapiens_trustworthiness_checker_{}", Uuid::new_v4()),
        host,
        port.unwrap_or(1883),
    );
    options.set_keep_alive(Duration::from_secs(30));
    options.set_clean_session(false);
    options.set_request_channel_capacity(RUMQTTC_CHANNEL_SIZE);
    options.set_inflight(RUMQTTC_CHANNEL_SIZE as u16);

    let (client, mut eventloop) = AsyncClient::new(options, RUMQTTC_CHANNEL_SIZE);
    let mut filters = var_topics
        .values()
        .map(|topic| SubscribeFilter::new(topic.clone(), QoS::AtLeastOnce))
        .collect::<Vec<_>>();
    if let Some(topic) = &control_topic {
        filters.push(SubscribeFilter::new(topic.clone(), QoS::AtLeastOnce));
    }
    info!(%host, ?port, topics = var_topics.len(), "Connecting rumqttc input stream");
    TokioCompat::new(client.subscribe_many(filters)).await?;
    loop {
        if matches!(
            TokioCompat::new(eventloop.poll()).await?,
            Event::Incoming(Incoming::SubAck(_))
        ) {
            break;
        }
    }

    let topics = var_topics
        .into_iter()
        .map(|(variable, topic)| (topic, variable))
        .collect();
    let drop_guard = CancellationToken::new().drop_guard();
    let cancellation_token = drop_guard.clone_tok();
    Ok(Box::pin(async_stream::try_stream! {
        let _drop_guard = drop_guard;
        let mut reconnect_attempts = 0;
        let mut terminal_error = None;
        loop {
            let item = futures::select! {
                item = poll_next_item::<V>(
                    &mut eventloop,
                    &topics,
                    control_topic.as_deref(),
                    max_reconnect_attempts,
                    &mut reconnect_attempts,
                ).fuse() => Some(item),
                _ = cancellation_token.cancelled().fuse() => {
                    debug!("rumqttc input stream cancelled");
                    None
                }
            };
            let Some(item) = item else { break; };
            match item {
                Ok(item @ MqttInputItem::Control(_)) => {
                    yield item;
                    break;
                }
                Ok(item) => yield item,
                Err(error) => {
                    terminal_error = Some(error);
                    break;
                }
            }
        }
        let _ = TokioCompat::new(client.disconnect()).await;
        if let Some(error) = terminal_error {
            Err(error)?;
        }
    }))
}
