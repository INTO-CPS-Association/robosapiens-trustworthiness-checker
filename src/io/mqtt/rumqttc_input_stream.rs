use std::{collections::VecDeque, time::Duration};

use anyhow::Context;
use async_channel::{Receiver, Sender};
use async_compat::Compat as TokioCompat;
use futures::{FutureExt, StreamExt};
use rumqttc::{
    AsyncClient, Event, EventLoop, Incoming, MqttOptions, QoS, SubscribeFilter, SubscribeReasonCode,
};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::core::{InputBatch, JsonStreamValue, OutputStream, VarName};
use crate::io::ReconfigurationRequest;

use super::input_backend::{InverseVarTopicMap, MqttInputItem, VarTopicMap, invert_topic_mapping};

const RUMQTTC_CHANNEL_SIZE: usize = 1024;

enum RumqttcEvent {
    Publish(RawPublish),
    Error(String),
}

struct RawPublish {
    topic: String,
    payload: Vec<u8>,
    /// Filled by the event-loop owner immediately before emission. A publish
    /// kept in the reconfiguration queue remains unrouted until the candidate
    /// map is active.
    variable: Option<VarName>,
}

struct RumqttcCommand {
    candidate_topics: InverseVarTopicMap,
    additions: Vec<String>,
    removals: Vec<String>,
    response: Sender<Result<(), String>>,
}

struct RumqttcInputTransport {
    events: Receiver<RumqttcEvent>,
    commands: Sender<RumqttcCommand>,
    cancel: Sender<()>,
    worker: smol::Task<()>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Acknowledgement {
    Subscribe,
    Unsubscribe,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PendingDispatch {
    Drained,
    Barrier,
    Closed,
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

    let inverse_topics = invert_topic_mapping(&var_topics);
    let transport = open_transport(
        host,
        port,
        &var_topics,
        max_reconnect_attempts,
        control_topic.as_deref(),
    )
    .await?;
    let events = rumqttc_event_stream(transport, control_topic.clone());
    Ok(map_legacy_items(events, inverse_topics, control_topic))
}

async fn open_transport(
    host: &str,
    port: Option<u16>,
    var_topics: &VarTopicMap,
    max_reconnect_attempts: u32,
    control_topic: Option<&str>,
) -> anyhow::Result<RumqttcInputTransport> {
    super::input_backend::validate_topic_mapping(var_topics, control_topic)?;

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
    let inverse_topics = invert_topic_mapping(var_topics);
    let mut filters = var_topics
        .values()
        .map(|topic| SubscribeFilter::new(topic.clone(), QoS::AtLeastOnce))
        .collect::<Vec<_>>();
    if let Some(topic) = control_topic {
        filters.push(SubscribeFilter::new(topic.to_owned(), QoS::AtLeastOnce));
    }
    info!(%host, ?port, topics = var_topics.len(), "Connecting rumqttc input stream");
    client.subscribe_many(filters).await?;

    // The initial subscription is also a barrier. Retained or immediately
    // published messages observed before its SubAck are preserved in order.
    let mut pending = VecDeque::new();
    let mut reconnect_attempts = 0;
    wait_for_ack(
        &mut eventloop,
        Acknowledgement::Subscribe,
        var_topics.len() + usize::from(control_topic.is_some()),
        &inverse_topics,
        control_topic,
        max_reconnect_attempts,
        &mut reconnect_attempts,
        &mut pending,
    )
    .await?;

    let (events, event_receiver) = async_channel::unbounded();
    let (commands, command_receiver) = async_channel::bounded(1);
    let (cancel, cancel_receiver) = async_channel::bounded(1);
    let worker = smol::spawn(run_event_loop(
        client,
        eventloop,
        inverse_topics,
        control_topic.map(str::to_owned),
        max_reconnect_attempts,
        pending,
        events,
        command_receiver,
        cancel_receiver,
    ));
    Ok(RumqttcInputTransport {
        events: event_receiver,
        commands,
        cancel,
        worker,
    })
}

// rumqttc 0.25's AsyncClient subscribe/unsubscribe APIs return enqueue status,
// not packet IDs. The event-loop owner therefore keeps these commands
// single-flight, making the next matching acknowledgement unambiguous.
async fn wait_for_ack(
    eventloop: &mut EventLoop,
    expected: Acknowledgement,
    expected_topics: usize,
    topics: &InverseVarTopicMap,
    control_topic: Option<&str>,
    max_reconnect_attempts: u32,
    reconnect_attempts: &mut u32,
    pending: &mut VecDeque<RawPublish>,
) -> anyhow::Result<()> {
    loop {
        match TokioCompat::new(eventloop.poll()).await {
            Ok(Event::Incoming(Incoming::Publish(publish))) => {
                *reconnect_attempts = 0;
                let is_control = control_topic == Some(publish.topic.as_str());
                if is_control || topics.contains_key(&publish.topic) {
                    pending.push_back(raw_publish(publish));
                }
            }
            Ok(Event::Incoming(Incoming::SubAck(ack)))
                if expected == Acknowledgement::Subscribe =>
            {
                anyhow::ensure!(
                    ack.return_codes.len() == expected_topics,
                    "rumqttc MQTT SubAck acknowledged {} topics, expected {}",
                    ack.return_codes.len(),
                    expected_topics
                );
                if let Some((index, code)) = ack
                    .return_codes
                    .iter()
                    .enumerate()
                    .find(|(_, code)| !matches!(code, SubscribeReasonCode::Success(_)))
                {
                    anyhow::bail!(
                        "rumqttc MQTT subscription for topic index {index} was rejected: {code:?}"
                    );
                }
                *reconnect_attempts = 0;
                return Ok(());
            }
            Ok(Event::Incoming(Incoming::UnsubAck(_)))
                if expected == Acknowledgement::Unsubscribe =>
            {
                *reconnect_attempts = 0;
                return Ok(());
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

async fn run_event_loop(
    client: AsyncClient,
    mut eventloop: EventLoop,
    mut active_topics: InverseVarTopicMap,
    control_topic: Option<String>,
    max_reconnect_attempts: u32,
    mut pending: VecDeque<RawPublish>,
    events: Sender<RumqttcEvent>,
    commands: Receiver<RumqttcCommand>,
    cancel: Receiver<()>,
) {
    let mut awaiting_command = false;
    let mut command_in_flight = false;
    let mut reconnect_attempts = 0;
    loop {
        if awaiting_command {
            debug_assert!(
                !command_in_flight,
                "rumqttc input command overlap would make acknowledgements ambiguous"
            );
            if command_in_flight {
                let _ = events
                    .send(RumqttcEvent::Error(
                        "rumqttc input command overlap".to_owned(),
                    ))
                    .await;
                break;
            }
            let command = futures::select! {
                command = commands.recv().fuse() => command.ok(),
                _ = cancel.recv().fuse() => None,
            };
            let Some(command) = command else {
                break;
            };
            let RumqttcCommand {
                candidate_topics,
                additions,
                removals,
                response,
            } = command;
            command_in_flight = true;
            debug_assert!(command_in_flight);
            let result = apply_command(
                &client,
                &mut eventloop,
                &mut active_topics,
                candidate_topics,
                additions,
                removals,
                &control_topic,
                max_reconnect_attempts,
                &mut reconnect_attempts,
                &mut pending,
            )
            .await;
            command_in_flight = false;
            let error = result.as_ref().err().cloned();
            let mut next_awaiting_command = false;
            if result.is_ok() {
                match dispatch_pending(
                    &events,
                    &mut pending,
                    &active_topics,
                    control_topic.as_deref(),
                )
                .await
                {
                    PendingDispatch::Drained => {}
                    PendingDispatch::Barrier => next_awaiting_command = true,
                    PendingDispatch::Closed => break,
                }
            }
            if response.send(result).await.is_err() {
                break;
            }
            if let Some(error) = error {
                let _ = events.send(RumqttcEvent::Error(error)).await;
                break;
            }
            awaiting_command = next_awaiting_command;
            continue;
        }

        match dispatch_pending(
            &events,
            &mut pending,
            &active_topics,
            control_topic.as_deref(),
        )
        .await
        {
            PendingDispatch::Drained => {}
            PendingDispatch::Barrier => {
                awaiting_command = true;
                continue;
            }
            PendingDispatch::Closed => break,
        }

        let next = futures::select! {
            event = TokioCompat::new(eventloop.poll()).fuse() => {
                Some(WorkerInput::Event(event))
            }
            _ = cancel.recv().fuse() => None,
        };
        let Some(next) = next else {
            break;
        };

        match next {
            WorkerInput::Event(Ok(Event::Incoming(Incoming::Publish(publish)))) => {
                reconnect_attempts = 0;
                if let Some(event) = route_publish(
                    raw_publish(publish),
                    &active_topics,
                    control_topic.as_deref(),
                ) {
                    let is_control = matches!(
                        &event,
                        RumqttcEvent::Publish(publish)
                            if control_topic.as_deref() == Some(publish.topic.as_str())
                    );
                    if events.send(event).await.is_err() {
                        break;
                    }
                    if is_control {
                        // The owner deliberately stops polling here. Data
                        // after this control cannot be tagged with the old
                        // subscription set while a reconfiguration is applying.
                        awaiting_command = true;
                    }
                }
            }
            WorkerInput::Event(Ok(_)) => reconnect_attempts = 0,
            WorkerInput::Event(Err(error)) => {
                reconnect_attempts += 1;
                if max_reconnect_attempts != u32::MAX && reconnect_attempts > max_reconnect_attempts
                {
                    let _ = events.send(RumqttcEvent::Error(error.to_string())).await;
                    break;
                }
                warn!(
                    ?error,
                    reconnect_attempts, "rumqttc poll failed; waiting for reconnection"
                );
                smol::Timer::after(Duration::from_millis(100)).await;
            }
        }
    }

    debug!("Disconnecting rumqttc MQTT input client");
    let _ = client.disconnect().await;
}

enum WorkerInput {
    Event(Result<Event, rumqttc::ConnectionError>),
}

async fn apply_command(
    client: &AsyncClient,
    eventloop: &mut EventLoop,
    active_topics: &mut InverseVarTopicMap,
    candidate_topics: InverseVarTopicMap,
    additions: Vec<String>,
    removals: Vec<String>,
    control_topic: &Option<String>,
    max_reconnect_attempts: u32,
    reconnect_attempts: &mut u32,
    pending: &mut VecDeque<RawPublish>,
) -> Result<(), String> {
    if !additions.is_empty() {
        let filters = additions
            .iter()
            .map(|topic| SubscribeFilter::new(topic.clone(), QoS::AtLeastOnce))
            .collect::<Vec<_>>();
        client
            .subscribe_many(filters)
            .await
            .map_err(|error| error.to_string())?;
        wait_for_ack(
            eventloop,
            Acknowledgement::Subscribe,
            additions.len(),
            &candidate_topics,
            control_topic.as_deref(),
            max_reconnect_attempts,
            reconnect_attempts,
            pending,
        )
        .await
        .map_err(|error| error.to_string())?;
    }

    // Install the candidate decoding view before waiting for removal acks. Raw
    // publishes observed by those waits are routed only after the complete
    // command succeeds, so they are all decoded under this candidate map.
    *active_topics = candidate_topics.clone();

    for topic in removals {
        client
            .unsubscribe(topic.as_str())
            .await
            .map_err(|error| error.to_string())?;
        wait_for_ack(
            eventloop,
            Acknowledgement::Unsubscribe,
            1,
            &candidate_topics,
            control_topic.as_deref(),
            max_reconnect_attempts,
            reconnect_attempts,
            pending,
        )
        .await
        .map_err(|error| error.to_string())?;
    }

    Ok(())
}

fn raw_publish(publish: rumqttc::Publish) -> RawPublish {
    RawPublish {
        topic: publish.topic,
        payload: publish.payload.to_vec(),
        variable: None,
    }
}

fn route_publish(
    mut publish: RawPublish,
    topics: &InverseVarTopicMap,
    control_topic: Option<&str>,
) -> Option<RumqttcEvent> {
    let is_control = control_topic == Some(publish.topic.as_str());
    publish.variable = if is_control {
        None
    } else {
        topics.get(&publish.topic).cloned()
    };
    if !is_control && publish.variable.is_none() {
        return None;
    }
    Some(RumqttcEvent::Publish(publish))
}

async fn dispatch_pending(
    events: &Sender<RumqttcEvent>,
    pending: &mut VecDeque<RawPublish>,
    topics: &InverseVarTopicMap,
    control_topic: Option<&str>,
) -> PendingDispatch {
    while let Some(publish) = pending.pop_front() {
        let is_control = control_topic == Some(publish.topic.as_str());
        let Some(event) = route_publish(publish, topics, control_topic) else {
            continue;
        };
        if events.send(event).await.is_err() {
            return PendingDispatch::Closed;
        }
        if is_control {
            return PendingDispatch::Barrier;
        }
    }
    PendingDispatch::Drained
}

fn rumqttc_event_stream(
    transport: RumqttcInputTransport,
    terminal_control_topic: Option<String>,
) -> OutputStream<anyhow::Result<RumqttcEvent>> {
    let RumqttcInputTransport {
        events,
        commands,
        cancel,
        worker,
    } = transport;
    Box::pin(async_stream::try_stream! {
        let _commands = commands;
        let cancel = cancel;
        let mut worker = Some(worker);
        while let Ok(event) = events.recv().await {
            match event {
                RumqttcEvent::Publish(publish)
                    if terminal_control_topic.as_deref() == Some(publish.topic.as_str()) =>
                {
                    // The legacy consumer drops its generator after this
                    // yield, so the worker must be stopped before yielding.
                    let _ = cancel.send(()).await;
                    if let Some(worker) = worker.take() {
                        worker.await;
                    }
                    yield RumqttcEvent::Publish(publish);
                    break;
                }
                RumqttcEvent::Publish(publish) => yield RumqttcEvent::Publish(publish),
                RumqttcEvent::Error(error) => Err(anyhow::anyhow!(error))?,
            }
        }
    })
}

fn map_legacy_items<V: JsonStreamValue + 'static>(
    mut events: OutputStream<anyhow::Result<RumqttcEvent>>,
    topics: InverseVarTopicMap,
    control_topic: Option<String>,
) -> OutputStream<anyhow::Result<MqttInputItem<V>>> {
    Box::pin(async_stream::try_stream! {
        while let Some(event) = events.next().await {
            match event? {
                RumqttcEvent::Publish(publish)
                    if control_topic.as_deref() == Some(publish.topic.as_str()) =>
                {
                    let payload = std::str::from_utf8(&publish.payload)
                        .context("MQTT monitor configuration is not UTF-8")?;
                    yield MqttInputItem::Control(ReconfigurationRequest::from_json(payload)?);
                    break;
                }
                RumqttcEvent::Publish(publish) => {
                    let Some(variable) = publish
                        .variable
                        .or_else(|| topics.get(&publish.topic).cloned())
                    else {
                        continue;
                    };
                    let value = super::input_backend::decode_payload::<V>(&publish.payload)
                        .with_context(|| {
                            format!("failed to parse value for MQTT variable `{variable}`")
                        })?;
                    yield MqttInputItem::Data(InputBatch::update(variable, value));
                }
                RumqttcEvent::Error(_) => unreachable!("event stream converts errors before yielding"),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    fn raw(topic: &str) -> RawPublish {
        RawPublish {
            topic: topic.to_owned(),
            payload: Vec::new(),
            variable: None,
        }
    }

    #[test]
    fn pending_data_after_a_control_uses_the_candidate_route_map() {
        let active = InverseVarTopicMap::from([("old".to_owned(), VarName::new("x"))]);
        let candidate = InverseVarTopicMap::from([("new".to_owned(), VarName::new("x"))]);
        let mut pending = VecDeque::from([raw("control"), raw("new")]);
        let (events, receiver) = async_channel::unbounded();

        let result = smol::block_on(dispatch_pending(
            &events,
            &mut pending,
            &active,
            Some("control"),
        ));
        assert_eq!(result, PendingDispatch::Barrier);
        assert_eq!(pending.len(), 1);
        assert!(matches!(
            receiver.try_recv().unwrap(),
            RumqttcEvent::Publish(_)
        ));

        let result = smol::block_on(dispatch_pending(
            &events,
            &mut pending,
            &candidate,
            Some("control"),
        ));
        assert_eq!(result, PendingDispatch::Drained);
        let RumqttcEvent::Publish(publish) = receiver.try_recv().unwrap() else {
            panic!("expected a routed data publish");
        };
        assert_eq!(publish.variable, Some(VarName::new("x")));
    }

    #[test]
    fn legacy_rumqttc_stream_stops_worker_before_yielding_control() {
        smol::block_on(async {
            let (event_sender, event_receiver) = async_channel::unbounded();
            let (commands, _command_receiver) = async_channel::bounded(1);
            let (cancel, cancel_receiver) = async_channel::bounded(1);
            let stopped = Arc::new(AtomicBool::new(false));
            let worker_stopped = Arc::clone(&stopped);
            let worker = smol::spawn(async move {
                let _ = cancel_receiver.recv().await;
                worker_stopped.store(true, Ordering::SeqCst);
            });
            let transport = RumqttcInputTransport {
                events: event_receiver,
                commands,
                cancel,
                worker,
            };
            let mut stream = rumqttc_event_stream(transport, Some("control".to_owned()));
            event_sender
                .send(RumqttcEvent::Publish(raw("control")))
                .await
                .unwrap();

            assert!(matches!(
                stream.next().await,
                Some(Ok(RumqttcEvent::Publish(_)))
            ));
            assert!(stopped.load(Ordering::SeqCst));
        });
    }
}
