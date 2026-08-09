use anyhow::Context;
use futures::StreamExt;
use std::collections::{BTreeMap, btree_map::Entry};

use crate::core::{InputBatch, InputStream, JsonStreamValue, OutputStream, VarName};
use crate::io::MonitorConfig;

#[derive(Debug)]
pub(crate) enum RedisInputItem<V> {
    Data(InputBatch<V>),
    Control(MonitorConfig),
}

pub async fn input_stream_items<V: JsonStreamValue>(
    hostname: &str,
    port: Option<u16>,
    var_topics: BTreeMap<VarName, String>,
    control_topic: Option<String>,
) -> anyhow::Result<OutputStream<anyhow::Result<RedisInputItem<V>>>> {
    let topic_vars = validate_topics(&var_topics, control_topic.as_deref())?;
    if var_topics.is_empty() && control_topic.is_none() {
        return Ok(Box::pin(futures::stream::empty()));
    }
    let url = match port {
        Some(port) => format!("redis://{hostname}:{port}"),
        None => format!("redis://{hostname}"),
    };

    let client = redis::Client::open(url)?;
    let mut pubsub = client.get_async_pubsub().await?;
    let mut channel_names = var_topics.values().cloned().collect::<Vec<_>>();
    if let Some(control_topic) = &control_topic {
        channel_names.push(control_topic.clone());
    }
    pubsub.subscribe(channel_names).await?;
    let mut redis_stream = pubsub.into_on_message();
    Ok(Box::pin(async_stream::try_stream! {
        while let Some(message) = redis_stream.next().await {
            if control_topic.as_deref() == Some(message.get_channel_name()) {
                let payload = message
                    .get_payload::<String>()
                    .map_err(anyhow::Error::from)
                    .context("Redis monitor configuration is not valid UTF-8")?;
                yield RedisInputItem::Control(MonitorConfig::from_json(&payload)?);
                return;
            }
            let Some(variable) = topic_vars.get(message.get_channel_name()).cloned() else {
                continue;
            };
            let payload = message
                .get_payload::<String>()
                .map_err(anyhow::Error::from)
                .context("Redis message payload is not valid UTF-8")?;
            let value = V::decode_json(payload.as_bytes())
                .with_context(|| format!("invalid Redis JSON5 payload for variable `{variable}`"))?;
            yield RedisInputItem::Data(InputBatch::update(variable, value));
        }
    }))
}

pub async fn input_stream<V: JsonStreamValue>(
    hostname: &str,
    port: Option<u16>,
    var_topics: BTreeMap<VarName, String>,
) -> anyhow::Result<InputStream<V>> {
    let items = input_stream_items(hostname, port, var_topics, None).await?;
    Ok(Box::pin(async_stream::try_stream! {
        let mut items = items;
        while let Some(item) = items.next().await {
            match item? {
                RedisInputItem::Data(batch) => yield batch,
                RedisInputItem::Control(_) => unreachable!("data-only Redis stream cannot receive control"),
            }
        }
    }))
}

fn validate_topics(
    var_topics: &BTreeMap<VarName, String>,
    control_topic: Option<&str>,
) -> anyhow::Result<BTreeMap<String, VarName>> {
    let mut topic_vars = BTreeMap::new();
    for (variable, topic) in var_topics {
        match topic_vars.entry(topic.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(variable.clone());
            }
            Entry::Occupied(entry) => {
                anyhow::bail!(
                    "duplicate Redis data route `{topic}` is mapped to variables `{}` and `{variable}`",
                    entry.get()
                );
            }
        }
    }
    if let Some(control_topic) = control_topic {
        if let Some(variable) = topic_vars.get(control_topic) {
            anyhow::bail!(
                "Redis control route `{control_topic}` collides with data route for variable `{variable}`"
            );
        }
    }
    Ok(topic_vars)
}
