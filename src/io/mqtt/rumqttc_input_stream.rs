use std::collections::BTreeMap;
use std::time::Duration;

use async_compat::Compat as TokioCompat;
use futures::FutureExt;
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, MqttOptions, QoS, SubscribeFilter};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::utils::cancellation_token::CancellationToken;
use crate::{InputBatch, InputEvent, InputStream, Value, VarName};

type VarTopicMap = BTreeMap<VarName, String>;
type InverseVarTopicMap = BTreeMap<String, VarName>;

const RUMQTTC_CHANNEL_SIZE: usize = 1024;

async fn poll_next_event(
    eventloop: &mut EventLoop,
    topics: &InverseVarTopicMap,
    max_reconnect_attempts: u32,
    reconnect_attempts: &mut u32,
) -> anyhow::Result<InputEvent<Value>> {
    loop {
        match TokioCompat::new(eventloop.poll()).await {
            Ok(Event::Incoming(Incoming::Publish(publish))) => {
                *reconnect_attempts = 0;
                let Some(var) = topics.get(&publish.topic).cloned() else {
                    continue;
                };
                return Ok(InputEvent::new(
                    var,
                    super::input_backend::parse_value(&publish.payload)?,
                ));
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

/// Connect and subscribe using rumqttc, returning an independent-event stream.
pub(super) async fn input_stream(
    host: &str,
    port: Option<u16>,
    var_topics: VarTopicMap,
    max_reconnect_attempts: u32,
) -> anyhow::Result<InputStream<Value>> {
    if var_topics.is_empty() {
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
    let filters = var_topics
        .values()
        .map(|topic| SubscribeFilter::new(topic.clone(), QoS::AtLeastOnce))
        .collect::<Vec<_>>();
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
        .map(|(var, topic)| (topic, var))
        .collect();
    let drop_guard = CancellationToken::new().drop_guard();
    let cancellation_token = drop_guard.clone_tok();
    Ok(Box::pin(async_stream::try_stream! {
        let _drop_guard = drop_guard;
        let mut reconnect_attempts = 0;
        let mut terminal_error = None;
        loop {
            let event = futures::select! {
                event = poll_next_event(
                    &mut eventloop,
                    &topics,
                    max_reconnect_attempts,
                    &mut reconnect_attempts,
                ).fuse() => Some(event),
                _ = cancellation_token.cancelled().fuse() => {
                    debug!("rumqttc input stream cancelled");
                    None
                }
            };
            let Some(event) = event else {
                break;
            };
            match event {
                Ok(event) => yield InputBatch::events(vec![event]),
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

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

    #[test]
    fn empty_input_needs_no_connection() {
        smol::block_on(async {
            let mut input = input_stream("localhost", None, BTreeMap::new(), 0)
                .await
                .unwrap();
            assert!(input.next().await.is_none());
        });
    }
}
