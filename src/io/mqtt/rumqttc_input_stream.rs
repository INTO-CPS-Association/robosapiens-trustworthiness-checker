use anyhow::Context;
use async_channel::{Receiver, Sender};
use futures::FutureExt;
use std::collections::{BTreeMap, VecDeque};
use tracing::debug;

use crate::core::{InputBatch, JsonStreamValue, LocalStream, VarName};
use crate::io::{ReconfigurationRequest, RetryPolicy};

use super::MqttProtocol;
use super::client::driver::{self, RawMqttMessage};
use super::protocol::{InverseVarTopicMap, MqttInputItem, VarTopicMap, invert_topic_mapping};

const INPUT_CAPACITY: usize = 1024;

enum InputEvent {
    Publish(RawPublish),
    Boundary(u64),
    Error(String),
}
struct RawPublish {
    topic: String,
    payload: Vec<u8>,
    variable: Option<VarName>,
}
struct InputCommand {
    candidate: Option<InverseVarTopicMap>,
    additions: Vec<String>,
    removals: Vec<String>,
    response: Sender<Result<u64, String>>,
    boundary_id: Option<u64>,
    resume: bool,
}

pub(crate) struct RumqttcInputControl {
    commands: Sender<InputCommand>,
    active_topics: VarTopicMap,
    control_topic: Option<String>,
    cancel: Sender<()>,
    worker: Option<smol::Task<anyhow::Result<()>>>,
}

impl RumqttcInputControl {
    pub(crate) async fn rebind(&mut self, candidate: VarTopicMap) -> anyhow::Result<()> {
        super::protocol::validate_topic_mapping(&candidate, self.control_topic.as_deref())?;
        let additions = candidate
            .values()
            .filter(|topic| !self.active_topics.values().any(|old| old == *topic))
            .cloned()
            .collect();
        let removals = self
            .active_topics
            .values()
            .filter(|topic| !candidate.values().any(|new| new == *topic))
            .cloned()
            .collect();
        let (response, result) = async_channel::bounded(1);
        self.commands
            .send(InputCommand {
                candidate: Some(invert_topic_mapping(&candidate)),
                additions,
                removals,
                response,
                boundary_id: None,
                resume: false,
            })
            .await
            .map_err(|_| anyhow::anyhow!("rumqttc input owner stopped before rebind"))?;
        result
            .recv()
            .await
            .map_err(|_| anyhow::anyhow!("rumqttc input owner stopped during rebind"))?
            .map_err(anyhow::Error::msg)?;
        self.active_topics = candidate;
        Ok(())
    }

    /// Stops admission at an engine-ordered generation boundary. All raw
    /// observations before the boundary precede its same-channel marker.
    /// This call returns after command admission; `rebind` resumes admission.
    pub(crate) async fn pause(&mut self, boundary_id: u64) -> anyhow::Result<()> {
        let (response, _result) = async_channel::bounded(1);
        self.commands
            .send(InputCommand {
                candidate: None,
                additions: Vec::new(),
                removals: Vec::new(),
                response,
                boundary_id: Some(boundary_id),
                resume: false,
            })
            .await
            .map_err(|_| anyhow::anyhow!("rumqttc input owner stopped before pause"))?;
        Ok(())
    }

    /// Resumes an unchanged source after a global boundary. The command is
    /// admitted asynchronously; the owner reuses the paused generation and
    /// installs the existing map without changing broker subscriptions.
    pub(crate) async fn resume(&mut self) -> anyhow::Result<()> {
        let (response, _result) = async_channel::bounded(1);
        self.commands
            .send(InputCommand {
                candidate: Some(invert_topic_mapping(&self.active_topics)),
                additions: Vec::new(),
                removals: Vec::new(),
                response,
                boundary_id: None,
                resume: true,
            })
            .await
            .map_err(|_| anyhow::anyhow!("rumqttc input owner stopped before resume"))?;
        Ok(())
    }

    pub(crate) async fn shutdown(&mut self) -> anyhow::Result<()> {
        let _ = self.cancel.send(()).await;
        if let Some(worker) = self.worker.take() {
            worker.await?;
        }
        Ok(())
    }
}

pub(crate) async fn reconfigurable_input_stream_items<V: JsonStreamValue>(
    protocol: MqttProtocol,
    host: &str,
    port: Option<u16>,
    topics: VarTopicMap,
    retry: RetryPolicy,
    control: String,
) -> anyhow::Result<(
    LocalStream<anyhow::Result<MqttInputItem<V>>>,
    RumqttcInputControl,
)> {
    open_items(protocol, host, port, topics, retry, Some(control)).await
}

pub(crate) async fn owned_input_stream_items<V: JsonStreamValue>(
    protocol: MqttProtocol,
    host: &str,
    port: Option<u16>,
    topics: VarTopicMap,
    retry: RetryPolicy,
) -> anyhow::Result<(
    LocalStream<anyhow::Result<MqttInputItem<V>>>,
    RumqttcInputControl,
)> {
    open_items(protocol, host, port, topics, retry, None).await
}

async fn open_items<V: JsonStreamValue>(
    protocol: MqttProtocol,
    host: &str,
    port: Option<u16>,
    topics: VarTopicMap,
    retry: RetryPolicy,
    control: Option<String>,
) -> anyhow::Result<(
    LocalStream<anyhow::Result<MqttInputItem<V>>>,
    RumqttcInputControl,
)> {
    super::protocol::validate_topic_mapping(&topics, control.as_deref())?;
    let uri = format!("tcp://{host}:{}", port.unwrap_or(1883));
    let (client, raw, driver) = driver::connect_raw_with_protocol(&uri, protocol, retry).await?;
    let mut subscriptions = topics.values().cloned().collect::<Vec<_>>();
    if let Some(topic) = &control {
        subscriptions.push(topic.clone());
    }
    if !subscriptions.is_empty() {
        client.subscribe_many_same_qos(&subscriptions, 1).await?;
    }

    let (events, event_rx) = async_channel::bounded(INPUT_CAPACITY);
    let (commands, command_rx) = async_channel::bounded(1);
    let (cancel, cancel_rx) = async_channel::bounded(1);
    let worker = smol::spawn(run_owner(
        client,
        driver,
        raw,
        invert_topic_mapping(&topics),
        control.clone(),
        events,
        command_rx,
        cancel_rx,
    ));
    Ok((
        map_items(event_rx, control.clone()),
        RumqttcInputControl {
            commands,
            active_topics: topics,
            control_topic: control,
            cancel,
            worker: Some(worker),
        },
    ))
}

async fn run_owner(
    client: super::MqttClient,
    driver: smol::Task<()>,
    raw: Receiver<Result<RawMqttMessage, crate::core::InputError>>,
    topics: InverseVarTopicMap,
    control: Option<String>,
    events: Sender<InputEvent>,
    commands: Receiver<InputCommand>,
    cancel: Receiver<()>,
) -> anyhow::Result<()> {
    let mut awaiting_rebind = false;
    let mut cancelled = false;
    let mut topic_versions = BTreeMap::from([(0_u64, topics)]);
    let mut deferred = VecDeque::new();
    let mut pause_generation = None;
    'owner: loop {
        let next = if awaiting_rebind {
            futures::select! {
                _ = cancel.recv().fuse() => { cancelled = true; None },
                command = commands.recv().fuse() => command.ok().map(OwnerInput::Command),
            }
        } else {
            futures::select! {
                _ = cancel.recv().fuse() => { cancelled = true; None },
                command = commands.recv().fuse() => command.ok().map(OwnerInput::Command),
                message = async {
                    if let Some(message) = deferred.pop_front() { Some(message) } else { raw.recv().await.ok() }
                }.fuse() => message.map(OwnerInput::Message),
            }
        };
        let Some(next) = next else { break };
        match next {
            OwnerInput::Message(Ok(message)) => {
                let generation = message.generation;
                let is_control_message = control.as_deref() == Some(message.topic.as_str());
                if is_control_message {
                    let boundary = client.subscribe_boundary(Vec::new()).await?;
                    while let Ok(queued) = raw.try_recv() {
                        match queued {
                            Ok(queued)
                                if queued.generation <= generation
                                    && control.as_deref() != Some(queued.topic.as_str()) =>
                            {
                                if let Some(publish) =
                                    route(queued, &topic_versions, control.as_deref())
                                {
                                    if !send_event(&events, &cancel, InputEvent::Publish(publish))
                                        .await?
                                    {
                                        cancelled = true;
                                        break 'owner;
                                    }
                                }
                            }
                            queued => {
                                deferred.push_back(queued);
                                break;
                            }
                        }
                    }
                    pause_generation = Some(boundary);
                }
                if let Some(publish) = route(message, &topic_versions, control.as_deref()) {
                    let is_control = control.as_deref() == Some(publish.topic.as_str());
                    if !send_event(&events, &cancel, InputEvent::Publish(publish)).await? {
                        cancelled = true;
                        break;
                    }
                    awaiting_rebind = is_control;
                }
                topic_versions.retain(|version, _| *version >= generation);
            }
            OwnerInput::Message(Err(error)) => {
                let text = error.to_string();
                queue_terminal(&events, text.clone());
                anyhow::bail!(text);
            }
            OwnerInput::Command(command) => {
                if command.resume && pause_generation.is_none() {
                    let generation = *topic_versions.keys().next_back().unwrap_or(&0);
                    let _ = command.response.send(Ok(generation)).await;
                    continue;
                }
                if command.candidate.is_none() {
                    if let Some(boundary) = pause_generation {
                        if !send_event(
                            &events,
                            &cancel,
                            InputEvent::Boundary(
                                command.boundary_id.expect("pause command has boundary id"),
                            ),
                        )
                        .await?
                        {
                            cancelled = true;
                            break;
                        }
                        let _ = command.response.send(Ok(boundary)).await;
                        continue;
                    }
                    let old_generation = *topic_versions.keys().next_back().unwrap_or(&0);
                    let boundary = client.subscribe_boundary(Vec::new()).await?;
                    if !drain_old_generation(
                        &raw,
                        &mut deferred,
                        old_generation,
                        &topic_versions,
                        control.as_deref(),
                        &events,
                        &cancel,
                    )
                    .await?
                    {
                        cancelled = true;
                        break;
                    }
                    if !send_event(
                        &events,
                        &cancel,
                        InputEvent::Boundary(
                            command.boundary_id.expect("pause command has boundary id"),
                        ),
                    )
                    .await?
                    {
                        cancelled = true;
                        break;
                    }
                    pause_generation = Some(boundary);
                    awaiting_rebind = true;
                    let _ = command.response.send(Ok(boundary)).await;
                    continue;
                }
                let paused_generation = pause_generation;
                let result = {
                    let operation = apply_command(&client, &command, paused_generation).fuse();
                    futures::pin_mut!(operation);
                    futures::select! {
                        result = operation => result,
                        _ = cancel.recv().fuse() => {
                            cancelled = true;
                            let _ = command.response.try_send(Err("rumqttc input command cancelled".into()));
                            break;
                        }
                    }
                };
                if let Ok(generation) = result.as_ref() {
                    if let Some(paused) = pause_generation.take() {
                        topic_versions.insert(paused, command.candidate.as_ref().unwrap().clone());
                    }
                    topic_versions.insert(*generation, command.candidate.as_ref().unwrap().clone());
                    if raw.is_empty() {
                        topic_versions.retain(|version, _| version == generation);
                    }
                }
                let reply = result.map_err(|error| error.to_string());
                let failed = reply.is_err();
                let _ = command.response.send(reply).await;
                if failed {
                    break;
                }
                awaiting_rebind = false;
            }
        }
    }
    if cancelled {
        driver.cancel().await;
        return Ok(());
    }
    debug!("Disconnecting shared MQTT input client");
    let disconnect = client.disconnect().await;
    driver.await;
    disconnect
}

enum OwnerInput {
    Message(Result<RawMqttMessage, crate::core::InputError>),
    Command(InputCommand),
}

fn queue_terminal(events: &Sender<InputEvent>, error: String) {
    let events = events.clone();
    smol::spawn(async move {
        let _ = events.send(InputEvent::Error(error)).await;
    })
    .detach();
}

async fn send_event(
    events: &Sender<InputEvent>,
    cancel: &Receiver<()>,
    event: InputEvent,
) -> anyhow::Result<bool> {
    futures::select! {
        result = events.send(event).fuse() => result
            .map(|_| true)
            .map_err(|_| anyhow::anyhow!("MQTT input consumer dropped")),
        _ = cancel.recv().fuse() => Ok(false),
    }
}

async fn drain_old_generation(
    raw: &Receiver<Result<RawMqttMessage, crate::core::InputError>>,
    deferred: &mut VecDeque<Result<RawMqttMessage, crate::core::InputError>>,
    generation: u64,
    versions: &BTreeMap<u64, InverseVarTopicMap>,
    control: Option<&str>,
    events: &Sender<InputEvent>,
    cancel: &Receiver<()>,
) -> anyhow::Result<bool> {
    while let Ok(queued) = raw.try_recv() {
        match queued {
            Ok(queued)
                if queued.generation <= generation && control != Some(queued.topic.as_str()) =>
            {
                if let Some(publish) = route(queued, versions, control) {
                    if !send_event(events, cancel, InputEvent::Publish(publish)).await? {
                        return Ok(false);
                    }
                }
            }
            queued => {
                deferred.push_back(queued);
                break;
            }
        }
    }
    Ok(true)
}

async fn apply_command(
    client: &super::MqttClient,
    command: &InputCommand,
    paused: Option<u64>,
) -> anyhow::Result<u64> {
    let generation = if command.additions.is_empty() {
        match paused {
            Some(generation) => generation,
            None => client.subscribe_boundary(Vec::new()).await?,
        }
    } else {
        client.subscribe_boundary(command.additions.clone()).await?
    };
    if !command.removals.is_empty() {
        client.unsubscribe_many(&command.removals).await?;
    }
    Ok(generation)
}

fn route(
    message: RawMqttMessage,
    versions: &BTreeMap<u64, InverseVarTopicMap>,
    control: Option<&str>,
) -> Option<RawPublish> {
    let is_control = control == Some(message.topic.as_str());
    let variable = if is_control {
        None
    } else {
        versions
            .get(&message.generation)
            .and_then(|topics| topics.get(&message.topic))
            .cloned()
    };
    if !is_control && variable.is_none() {
        return None;
    }
    Some(RawPublish {
        topic: message.topic,
        payload: message.payload,
        variable,
    })
}

fn map_items<V: JsonStreamValue + 'static>(
    events: Receiver<InputEvent>,
    control: Option<String>,
) -> LocalStream<anyhow::Result<MqttInputItem<V>>> {
    Box::pin(async_stream::try_stream! {
        while let Ok(event) = events.recv().await {
            match event {
                InputEvent::Publish(publish) if control.as_deref() == Some(publish.topic.as_str()) => {
                    let payload = std::str::from_utf8(&publish.payload).context("MQTT monitor configuration is not UTF-8")?;
                    yield MqttInputItem::Control(ReconfigurationRequest::from_json(payload)?);
                }
                InputEvent::Publish(publish) => {
                    let Some(variable) = publish.variable else { continue };
                    let value = super::protocol::decode_payload::<V>(&publish.payload)
                        .with_context(|| format!("failed to parse value for MQTT variable `{variable}`"))?;
                    yield MqttInputItem::Data(InputBatch::update(variable, value));
                }
                InputEvent::Error(error) => Err(anyhow::anyhow!(error))?,
                InputEvent::Boundary(id) => yield MqttInputItem::Boundary(id),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(generation: u64, topic: &str) -> RawMqttMessage {
        RawMqttMessage {
            topic: topic.into(),
            payload: vec![0xff],
            qos: 1,
            generation,
        }
    }

    #[test]
    fn queued_messages_keep_the_map_from_their_engine_generation() {
        let versions = BTreeMap::from([
            (0, BTreeMap::from([("old".into(), VarName::new("x"))])),
            (1, BTreeMap::from([("new".into(), VarName::new("x"))])),
        ]);
        assert_eq!(
            route(message(0, "old"), &versions, None).unwrap().variable,
            Some(VarName::new("x"))
        );
        assert!(route(message(0, "new"), &versions, None).is_none());
        assert_eq!(
            route(message(1, "new"), &versions, None).unwrap().variable,
            Some(VarName::new("x"))
        );
    }

    #[test]
    fn routing_preserves_non_utf8_payload_bytes() {
        let versions = BTreeMap::from([(0, BTreeMap::from([("data".into(), VarName::new("x"))]))]);
        assert_eq!(
            route(message(0, "data"), &versions, None).unwrap().payload,
            vec![0xff]
        );
    }

    #[test]
    fn native_pause_uses_a_distinct_owner_command() {
        smol::block_on(async {
            let (commands, receiver) = async_channel::bounded(1);
            let (cancel, _cancel_receiver) = async_channel::bounded(1);
            let mut control = RumqttcInputControl {
                commands,
                active_topics: BTreeMap::new(),
                control_topic: None,
                cancel,
                worker: Some(smol::spawn(async { Ok(()) })),
            };
            let operation = control.pause(42);
            let responder = async {
                let command = receiver.recv().await.unwrap();
                assert!(command.candidate.is_none());
                assert_eq!(command.boundary_id, Some(42));
                assert!(command.response.send(Ok(7)).await.is_err());
            };
            let (result, ()) = futures::join!(operation, responder);
            result.unwrap();
        });
    }

    #[test]
    fn terminal_error_remains_ordered_behind_full_event_queue() {
        smol::block_on(async {
            let (events, receiver) = async_channel::bounded(1);
            events.send(InputEvent::Boundary(9)).await.unwrap();
            queue_terminal(&events, "overflow".into());
            drop(events);

            assert!(matches!(
                receiver.recv().await.unwrap(),
                InputEvent::Boundary(9)
            ));
            assert!(
                matches!(receiver.recv().await.unwrap(), InputEvent::Error(error) if error == "overflow")
            );
            assert!(receiver.recv().await.is_err());
        });
    }

    #[test]
    fn blocked_event_send_observes_owner_cancellation() {
        smol::block_on(async {
            let (events, _receiver) = async_channel::bounded(1);
            events.send(InputEvent::Boundary(1)).await.unwrap();
            let (cancel, cancelled) = async_channel::bounded(1);
            cancel.send(()).await.unwrap();
            assert!(
                !send_event(&events, &cancelled, InputEvent::Boundary(2))
                    .await
                    .unwrap()
            );
        });
    }
}
