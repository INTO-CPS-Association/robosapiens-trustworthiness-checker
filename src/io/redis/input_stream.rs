use std::collections::BTreeMap;

use futures::StreamExt;
use tracing::info;

use crate::{InputBatch, InputStream, Value, VarName};

/// Connect and subscribe to Redis channels, returning the resulting input stream.
pub async fn input_stream(
    hostname: &str,
    port: Option<u16>,
    var_topics: BTreeMap<VarName, String>,
) -> anyhow::Result<InputStream<Value>> {
    if var_topics.is_empty() {
        return Ok(Box::pin(futures::stream::empty()));
    }
    let url = match port {
        Some(p) => format!("redis://{}:{}", hostname, p),
        None => format!("redis://{}", hostname),
    };

    let client = redis::Client::open(url)?;
    let mut pubsub = client.get_async_pubsub().await?;
    let channel_names = var_topics.values().collect::<Vec<_>>();
    info!("Subscribing to Redis channel_names: {:?}", channel_names);
    pubsub.subscribe(channel_names).await?;
    let mut redis_stream = pubsub.into_on_message();
    let topic_vars = var_topics
        .into_iter()
        .map(|(var, topic)| (topic, var))
        .collect::<BTreeMap<_, _>>();

    Ok(Box::pin(async_stream::try_stream! {
        while let Some(msg) = redis_stream.next().await {
            let Some(var) = topic_vars.get(msg.get_channel_name()).cloned() else {
                continue;
            };
            let value = msg.get_payload::<Value>().map_err(anyhow::Error::from)?;
            yield InputBatch::events(vec![crate::InputEvent::new(var, value)]);
        }
    }))
}
