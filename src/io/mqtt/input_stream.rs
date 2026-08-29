use std::rc::Rc;

use anyhow::Context;
use futures::{FutureExt, StreamExt, stream::BoxStream};
use tracing::{Level, debug, info, info_span, instrument, warn};

use super::input_backend::{InverseVarTopicMap, MqttInputItem, VarTopicMap, invert_topic_mapping};
use crate::core::{InputBatch, JsonStreamValue, OutputStream, VarName};
use crate::io::ReconfigurationRequest;
use crate::io::mqtt::{MqttClient, MqttFactory, MqttMessage};
use crate::utils::cancellation_token::CancellationToken;

const QOS: i32 = 1;

enum PahoInputEvent {
    Data(MqttMessage),
    Control(ReconfigurationRequest),
}

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
    let (client, stream) = open_transport(
        host,
        port,
        &var_topics,
        max_attempts,
        control_topic.as_deref(),
    )
    .await?;
    let client: Rc<dyn MqttClient> = Rc::from(client);
    let events = paho_event_stream(stream, control_topic);
    Ok(map_legacy_items(
        events,
        invert_topic_mapping(&var_topics),
        client,
    ))
}

async fn open_transport(
    host: &str,
    port: Option<u16>,
    var_topics: &VarTopicMap,
    max_attempts: u32,
    control_topic: Option<&str>,
) -> anyhow::Result<(Box<dyn MqttClient>, BoxStream<'static, MqttMessage>)> {
    super::input_backend::validate_topic_mapping(var_topics, control_topic)?;
    let uri = match port {
        Some(port) => format!("tcp://{host}:{port}"),
        None => format!("tcp://{host}"),
    };
    info!(%uri, topics = var_topics.len(), "Connecting MQTT input stream");
    let (client, stream) = MqttFactory::Paho
        .connect_and_receive(&uri, max_attempts)
        .await?;
    let mut topics = var_topics.values().cloned().collect::<Vec<_>>();
    if let Some(control_topic) = control_topic {
        topics.push(control_topic.to_owned());
    }
    let qos = vec![QOS; topics.len()];
    subscribe_many_with_retries(&*client, &topics, &qos, max_attempts).await?;
    Ok((client, stream))
}

async fn subscribe_many_with_retries(
    client: &dyn MqttClient,
    topics: &Vec<String>,
    qos: &[i32],
    max_attempts: u32,
) -> anyhow::Result<()> {
    let mut retries = 0;
    loop {
        match client.subscribe_many(topics, qos).await {
            Ok(()) => return Ok(()),
            Err(error) if retries < max_attempts => {
                retries += 1;
                warn!(?error, retries, "Failed to subscribe to MQTT input topics");
                smol::Timer::after(std::time::Duration::from_millis(100)).await;
                client.reconnect().await?;
            }
            Err(error) => return Err(error),
        }
    }
}

fn paho_event_stream(
    mut mqtt_stream: BoxStream<'static, MqttMessage>,
    control_topic: Option<String>,
) -> OutputStream<anyhow::Result<PahoInputEvent>> {
    let drop_guard = CancellationToken::new().drop_guard();
    let cancellation_token = drop_guard.clone_tok();
    Box::pin(async_stream::try_stream! {
        let _drop_guard = drop_guard;
        let mqtt_input_span = info_span!("Paho MQTT input stream");
        let _enter = mqtt_input_span.enter();
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
                match ReconfigurationRequest::from_json(&msg.payload)
                    .context("invalid MQTT monitor configuration")
                {
                    Ok(request) => yield PahoInputEvent::Control(request),
                    Err(error) => {
                        terminal_error = Some(error);
                        break;
                    }
                }
            } else {
                yield PahoInputEvent::Data(msg);
            }
        }

        if let Some(error) = terminal_error {
            Err(error)?;
        }
    })
}

fn map_legacy_items<V: JsonStreamValue + 'static>(
    mut events: OutputStream<anyhow::Result<PahoInputEvent>>,
    topics: InverseVarTopicMap,
    client: Rc<dyn MqttClient>,
) -> OutputStream<anyhow::Result<MqttInputItem<V>>> {
    Box::pin(async_stream::try_stream! {
        let mut terminal_error = None;
        let mut transport_closed = false;
        while let Some(event) = events.next().await {
            match event {
                Ok(PahoInputEvent::Control(request)) => {
                    // The legacy consumer drops this stream after receiving the
                    // control item, so cleanup after this yield is not polled.
                    let _ = client.disconnect().await;
                    transport_closed = true;
                    // Preserve the historical convenience-stream behavior:
                    // the first control is yielded as a replacement barrier,
                    // then the ordinary stream closes.
                    yield MqttInputItem::Control(request);
                    break;
                }
                Ok(PahoInputEvent::Data(message)) => {
                    if let Some(variable) = topics.get(&message.topic).cloned() {
                        match parse_message::<V>(message, variable) {
                            Ok(batch) => yield MqttInputItem::Data(batch),
                            Err(error) => {
                                terminal_error = Some(error);
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    terminal_error = Some(error);
                    break;
                }
            }
        }

        if !transport_closed {
            let _ = client.disconnect().await;
        }
        if let Some(error) = terminal_error {
            Err(error)?;
        }
    })
}

fn parse_message<V: JsonStreamValue>(
    msg: MqttMessage,
    var: VarName,
) -> anyhow::Result<InputBatch<V>> {
    let value = super::input_backend::decode_payload::<V>(msg.payload.as_bytes())
        .with_context(|| format!("failed to parse value for MQTT variable `{var}`"))?;
    Ok(InputBatch::update(var, value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::stream;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    struct DisconnectTrackingClient {
        disconnected: Arc<AtomicBool>,
        subscribe_many_ok: bool,
    }

    #[async_trait]
    impl MqttClient for DisconnectTrackingClient {
        async fn publish(&self, _message: MqttMessage) -> anyhow::Result<()> {
            Ok(())
        }

        async fn reconnect(&self) -> anyhow::Result<()> {
            Ok(())
        }

        async fn disconnect(&self) -> anyhow::Result<()> {
            self.disconnected.store(true, Ordering::SeqCst);
            Ok(())
        }

        fn set_raw_message_callback(&self, _callback: Box<dyn FnMut(&str, &[u8]) + Send>) {}

        fn remove_raw_message_callback(&self) {}

        async fn subscribe(&self, _topic: &String, _qos: i32) -> anyhow::Result<()> {
            Ok(())
        }

        async fn subscribe_many(&self, _topics: &Vec<String>, _qos: &[i32]) -> anyhow::Result<()> {
            if self.subscribe_many_ok {
                Ok(())
            } else {
                anyhow::bail!("subscription refused")
            }
        }

        async fn unsubscribe_many(&self, _topics: &Vec<String>) -> anyhow::Result<()> {
            Ok(())
        }

        fn clone_box(&self) -> Box<dyn MqttClient> {
            Box::new(Self {
                disconnected: Arc::clone(&self.disconnected),
                subscribe_many_ok: self.subscribe_many_ok,
            })
        }
    }

    #[test]
    fn legacy_paho_stream_disconnects_before_yielding_control() {
        smol::block_on(async {
            let disconnected = Arc::new(AtomicBool::new(false));
            let client = Rc::new(DisconnectTrackingClient {
                disconnected: Arc::clone(&disconnected),
                subscribe_many_ok: true,
            }) as Rc<dyn MqttClient>;
            let events = Box::pin(stream::iter([Ok(PahoInputEvent::Control(
                ReconfigurationRequest::new("next"),
            ))]));
            let mut items =
                map_legacy_items::<crate::Value>(events, InverseVarTopicMap::new(), client);

            assert!(matches!(
                items.next().await,
                Some(Ok(MqttInputItem::Control(_)))
            ));
            assert!(disconnected.load(Ordering::SeqCst));
        });
    }
}
