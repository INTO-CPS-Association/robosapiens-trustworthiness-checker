use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, anyhow};
use async_compat::Compat as TokioCompat;
use futures::{FutureExt, StreamExt, future, stream::BoxStream};
use rumqttc::{
    AsyncClient, Event, EventLoop, Incoming, MqttOptions, Outgoing, QoS, SubscribeFilter,
    SubscribeReasonCode,
};
use tracing::warn;
use uuid::Uuid;

use super::MqttMessage;
use crate::{
    core::InputError,
    io::mqtt::MqttProtocol,
    io::retry::{RetryPolicy, RetryTracker},
};

#[derive(Clone)]
enum TransportClient {
    V311(AsyncClient),
    V5(rumqttc::v5::AsyncClient),
}
enum TransportLoop {
    V311(EventLoop),
    V5(rumqttc::v5::EventLoop),
}

#[derive(Debug)]
struct ProtocolAckError(String);
impl std::fmt::Display for ProtocolAckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for ProtocolAckError {}
fn rejected(message: impl Into<String>) -> anyhow::Error {
    ProtocolAckError(message.into()).into()
}

fn is_terminal_protocol_error(error: &anyhow::Error) -> bool {
    error.downcast_ref::<ProtocolAckError>().is_some()
}

impl TransportClient {
    fn rejects_outbound_qos2(&self) -> bool {
        matches!(self, Self::V5(_))
    }

    async fn publish(
        &self,
        topic: String,
        qos: QoS,
        retain: bool,
        payload: String,
    ) -> anyhow::Result<()> {
        match self {
            Self::V311(c) => c.publish(topic, qos, retain, payload).await?,
            Self::V5(c) => c.publish(topic, qos_v5(qos), retain, payload).await?,
        }
        Ok(())
    }
    async fn subscribe_many(&self, filters: Vec<(String, QoS)>) -> anyhow::Result<()> {
        match self {
            Self::V311(c) => {
                c.subscribe_many(filters.into_iter().map(|(t, q)| SubscribeFilter::new(t, q)))
                    .await?
            }
            Self::V5(c) => {
                c.subscribe_many(filters.into_iter().map(|(path, qos)| {
                    rumqttc::v5::mqttbytes::v5::Filter {
                        path,
                        qos: qos_v5(qos),
                        nolocal: false,
                        preserve_retain: false,
                        retain_forward_rule:
                            rumqttc::v5::mqttbytes::v5::RetainForwardRule::OnEverySubscribe,
                    }
                }))
                .await?
            }
        }
        Ok(())
    }
    async fn unsubscribe(&self, topic: String) -> anyhow::Result<()> {
        match self {
            Self::V311(c) => c.unsubscribe(topic).await?,
            Self::V5(c) => c.unsubscribe(topic).await?,
        };
        Ok(())
    }
    async fn disconnect(&self) -> anyhow::Result<()> {
        match self {
            Self::V311(c) => c.disconnect().await?,
            Self::V5(c) => c.disconnect().await?,
        };
        Ok(())
    }
}

impl TransportLoop {
    async fn poll(&mut self) -> anyhow::Result<Event> {
        match self {
            Self::V311(e) => Ok(TokioCompat::new(e.poll()).await?),
            Self::V5(e) => loop {
                let event = TokioCompat::new(e.poll())
                    .await
                    .map_err(|error| match error {
                        rumqttc::v5::ConnectionError::ConnectionRefused(reason) => {
                            rejected(format!("MQTT 5 connection rejected: {reason:?}"))
                        }
                        rumqttc::v5::ConnectionError::MqttState(
                            rumqttc::v5::StateError::ConnFail { reason },
                        ) => rejected(format!("MQTT 5 connection rejected: {reason:?}")),
                        other => anyhow::Error::new(other),
                    })?;
                if let Some(event) = normalize_v5(event)? {
                    break Ok(event);
                }
            },
        }
    }
}

fn normalize_v5(event: rumqttc::v5::Event) -> anyhow::Result<Option<Event>> {
    use rumqttc::v5::mqttbytes::v5::{
        Packet, PubAckReason, PubCompReason, SubscribeReasonCode as S, UnsubAckReason,
    };
    Ok(Some(match event {
        rumqttc::v5::Event::Outgoing(out) => Event::Outgoing(out),
        rumqttc::v5::Event::Incoming(packet) => Event::Incoming(match packet {
            Packet::ConnAck(a) => {
                if a.code != rumqttc::v5::mqttbytes::v5::ConnectReturnCode::Success {
                    return Err(rejected(format!(
                        "MQTT 5 connection rejected: {:?}",
                        a.code
                    )));
                }
                Incoming::ConnAck(rumqttc::ConnAck::new(
                    rumqttc::ConnectReturnCode::Success,
                    a.session_present,
                ))
            }
            Packet::Publish(p) => {
                let topic =
                    String::from_utf8(p.topic.to_vec()).context("MQTT 5 topic is not UTF-8")?;
                let mut out = rumqttc::Publish::new(topic, qos_v311(p.qos), p.payload.to_vec());
                out.pkid = p.pkid;
                out.retain = p.retain;
                out.dup = p.dup;
                Incoming::Publish(out)
            }
            Packet::PubAck(a) => {
                if !matches!(
                    a.reason,
                    PubAckReason::Success | PubAckReason::NoMatchingSubscribers
                ) {
                    return Err(rejected(format!(
                        "MQTT 5 PubAck rejected publish: {:?}",
                        a.reason
                    )));
                }
                Incoming::PubAck(rumqttc::PubAck::new(a.pkid))
            }
            Packet::PubRec(a) => {
                use rumqttc::v5::mqttbytes::v5::PubRecReason;
                if !matches!(
                    a.reason,
                    PubRecReason::Success | PubRecReason::NoMatchingSubscribers
                ) {
                    return Err(rejected(format!(
                        "MQTT 5 PubRec rejected publish: {:?}",
                        a.reason
                    )));
                }
                return Ok(None);
            }
            Packet::PubComp(a) => {
                if a.reason != PubCompReason::Success {
                    return Err(rejected(format!(
                        "MQTT 5 PubComp rejected publish: {:?}",
                        a.reason
                    )));
                }
                Incoming::PubComp(rumqttc::PubComp::new(a.pkid))
            }
            Packet::SubAck(a) => {
                let codes = a
                    .return_codes
                    .into_iter()
                    .map(|c| match c {
                        S::Success(q) => Ok(SubscribeReasonCode::Success(qos_v311(q))),
                        other => Err(rejected(format!(
                            "MQTT 5 SubAck rejected subscription: {other:?}"
                        ))),
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                Incoming::SubAck(rumqttc::SubAck::new(a.pkid, codes))
            }
            Packet::UnsubAck(a) => {
                if !a.reasons.iter().all(|r| {
                    matches!(
                        r,
                        UnsubAckReason::Success | UnsubAckReason::NoSubscriptionExisted
                    )
                }) {
                    return Err(rejected(format!(
                        "MQTT 5 UnsubAck rejected unsubscribe: {:?}",
                        a.reasons
                    )));
                }
                Incoming::UnsubAck(rumqttc::UnsubAck::new(a.pkid))
            }
            _ => return Ok(None),
        }),
    }))
}

fn qos_v5(qos: QoS) -> rumqttc::v5::mqttbytes::QoS {
    match qos {
        QoS::AtMostOnce => rumqttc::v5::mqttbytes::QoS::AtMostOnce,
        QoS::AtLeastOnce => rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
        QoS::ExactlyOnce => rumqttc::v5::mqttbytes::QoS::ExactlyOnce,
    }
}
fn qos_v311(qos: rumqttc::v5::mqttbytes::QoS) -> QoS {
    match qos {
        rumqttc::v5::mqttbytes::QoS::AtMostOnce => QoS::AtMostOnce,
        rumqttc::v5::mqttbytes::QoS::AtLeastOnce => QoS::AtLeastOnce,
        rumqttc::v5::mqttbytes::QoS::ExactlyOnce => QoS::ExactlyOnce,
    }
}

const COMMAND_CAPACITY: usize = 64;
const REQUEST_CAPACITY: usize = 128;
const MAX_INFLIGHT: u16 = 32;
const RECEIVE_CAPACITY: usize = 1024;
type Reply = async_channel::Sender<Result<(), String>>;

#[derive(Debug)]
pub(crate) struct RawMqttMessage {
    pub(crate) topic: String,
    pub(crate) payload: Vec<u8>,
    pub(crate) qos: i32,
    pub(crate) generation: u64,
}

enum Command {
    Publish(MqttMessage, Reply),
    Subscribe(Vec<(String, QoS)>, Reply),
    Unsubscribe(Vec<String>, Reply),
    SubscribeBoundary(
        Vec<(String, QoS)>,
        async_channel::Sender<Result<u64, String>>,
    ),
    AdvanceBoundary(async_channel::Sender<Result<u64, String>>),
    Disconnect(Option<Reply>),
}
enum Submitted {
    Publish(QoS, Reply),
    Subscribe(usize, Reply),
    Unsubscribe(Arc<UnsubscribeGroup>),
    SubscribeBoundary(usize, async_channel::Sender<Result<u64, String>>),
    Restore(usize),
    Disconnect(Option<Reply>),
}
enum Pending {
    Publish(QoS, Reply),
    Subscribe(usize, Reply),
    Unsubscribe(Arc<UnsubscribeGroup>),
    SubscribeBoundary(usize, async_channel::Sender<Result<u64, String>>),
    Restore(usize),
}
struct UnsubscribeGroup {
    remaining: AtomicUsize,
    reply: Reply,
}

struct Shared {
    commands: async_channel::Sender<Command>,
}
impl Drop for Shared {
    fn drop(&mut self) {
        let _ = self.commands.try_send(Command::Disconnect(None));
    }
}
#[derive(Clone)]
pub struct MqttClient {
    shared: Arc<Shared>,
}

impl MqttClient {
    async fn request(&self, command: impl FnOnce(Reply) -> Command) -> anyhow::Result<()> {
        let (tx, rx) = async_channel::bounded(1);
        self.shared
            .commands
            .send(command(tx))
            .await
            .map_err(|_| anyhow!("MQTT driver has stopped"))?;
        rx.recv()
            .await
            .map_err(|_| anyhow!("MQTT driver stopped before completing the operation"))?
            .map_err(anyhow::Error::msg)
    }
}

impl MqttClient {
    pub async fn publish(&self, message: MqttMessage) -> anyhow::Result<()> {
        self.request(|r| Command::Publish(message, r)).await
    }
    pub async fn disconnect(&self) -> anyhow::Result<()> {
        self.request(|r| Command::Disconnect(Some(r))).await
    }
    pub async fn subscribe(&self, topic: &String, qos: i32) -> anyhow::Result<()> {
        let qos = parse_qos(qos)?;
        self.request(|r| Command::Subscribe(vec![(topic.clone(), qos)], r))
            .await
    }
    pub async fn subscribe_many(&self, topics: &Vec<String>, qos: &[i32]) -> anyhow::Result<()> {
        anyhow::ensure!(
            topics.len() == qos.len(),
            "MQTT topics and QoS lists differ in length"
        );
        let filters = topics
            .iter()
            .zip(qos)
            .map(|(t, q)| Ok((t.clone(), parse_qos(*q)?)))
            .collect::<anyhow::Result<Vec<_>>>()?;
        self.request(|r| Command::Subscribe(filters, r)).await
    }
    pub async fn subscribe_many_same_qos(
        &self,
        topics: &Vec<String>,
        qos: i32,
    ) -> anyhow::Result<()> {
        self.subscribe_many(topics, &vec![qos; topics.len()]).await
    }
    pub async fn unsubscribe_many(&self, topics: &Vec<String>) -> anyhow::Result<()> {
        self.request(|r| Command::Unsubscribe(topics.clone(), r))
            .await
    }

    pub(crate) async fn subscribe_boundary(&self, topics: Vec<String>) -> anyhow::Result<u64> {
        let (reply, result) = async_channel::bounded(1);
        let command = if topics.is_empty() {
            Command::AdvanceBoundary(reply)
        } else {
            Command::SubscribeBoundary(
                topics
                    .into_iter()
                    .map(|topic| (topic, QoS::AtLeastOnce))
                    .collect(),
                reply,
            )
        };
        self.shared
            .commands
            .send(command)
            .await
            .map_err(|_| anyhow!("MQTT driver has stopped"))?;
        result
            .recv()
            .await
            .map_err(|_| anyhow!("MQTT driver stopped during input rebind"))?
            .map_err(anyhow::Error::msg)
    }
}

pub(super) async fn connect(
    uri: &str,
    protocol: MqttProtocol,
    retry: RetryPolicy,
) -> anyhow::Result<MqttClient> {
    let (client, _, worker) = open(uri, protocol, retry, false).await?;
    worker.detach();
    Ok(client)
}
pub(super) async fn connect_and_receive(
    uri: &str,
    protocol: MqttProtocol,
    retry: RetryPolicy,
) -> anyhow::Result<(
    MqttClient,
    BoxStream<'static, Result<MqttMessage, InputError>>,
)> {
    let (client, rx, worker) = open(uri, protocol, retry, true).await?;
    worker.detach();
    let rx = rx.map(|item| {
        item.and_then(|message| {
            String::from_utf8(message.payload)
                .map(|payload| MqttMessage::new(message.topic, payload, message.qos))
                .map_err(|error| InputError::source(format!("MQTT payload is not UTF-8: {error}")))
        })
    });
    Ok((
        client,
        Box::pin(rx) as BoxStream<'static, Result<MqttMessage, InputError>>,
    ))
}

pub(crate) async fn connect_raw_with_protocol(
    uri: &str,
    protocol: MqttProtocol,
    retry: RetryPolicy,
) -> anyhow::Result<(
    MqttClient,
    async_channel::Receiver<Result<RawMqttMessage, InputError>>,
    smol::Task<()>,
)> {
    open(uri, protocol, retry, true).await
}

async fn open(
    uri: &str,
    protocol: MqttProtocol,
    retry: RetryPolicy,
    receive: bool,
) -> anyhow::Result<(
    MqttClient,
    async_channel::Receiver<Result<RawMqttMessage, InputError>>,
    smol::Task<()>,
)> {
    let mut options = options_from_uri(uri)?;
    options
        .set_keep_alive(Duration::from_secs(30))
        .set_clean_session(false)
        .set_request_channel_capacity(REQUEST_CAPACITY)
        .set_inflight(MAX_INFLIGHT);
    let (network, eventloop) = match protocol {
        MqttProtocol::V311 => {
            let (client, eventloop) = AsyncClient::new(options, REQUEST_CAPACITY);
            (
                TransportClient::V311(client),
                TransportLoop::V311(eventloop),
            )
        }
        MqttProtocol::V5 => {
            let v5 = v5_options(&options);
            let (client, eventloop) = rumqttc::v5::AsyncClient::new(v5, REQUEST_CAPACITY);
            (TransportClient::V5(client), TransportLoop::V5(eventloop))
        }
    };
    let (command_tx, command_rx) = async_channel::bounded(COMMAND_CAPACITY);
    let (message_tx, message_rx) = async_channel::bounded(RECEIVE_CAPACITY);
    let (ready_tx, ready_rx) = async_channel::bounded(1);
    let shared = Arc::new(Shared {
        commands: command_tx,
    });
    let worker = smol::spawn(run_driver(
        network, eventloop, command_rx, message_tx, retry, ready_tx, receive,
    ));
    ready_rx
        .recv()
        .await
        .map_err(|_| anyhow!("MQTT driver stopped while connecting"))?
        .map_err(anyhow::Error::msg)?;
    Ok((MqttClient { shared }, message_rx, worker))
}

#[allow(clippy::too_many_arguments)]
async fn run_driver(
    client: TransportClient,
    mut eventloop: TransportLoop,
    commands: async_channel::Receiver<Command>,
    messages: async_channel::Sender<Result<RawMqttMessage, InputError>>,
    retry: RetryPolicy,
    ready: async_channel::Sender<Result<(), String>>,
    receive: bool,
) {
    let mut submitted = VecDeque::new();
    let mut pending = HashMap::new();
    let mut tracker = retry.tracker();
    let mut ready = Some(ready);
    let mut closing = false;
    let mut disconnect_waiting = None;
    let mut generation = 0_u64;
    let mut subscriptions = HashMap::<String, QoS>::new();
    loop {
        let event = eventloop.poll().fuse();
        let command = if !closing && submitted.len() + pending.len() < usize::from(MAX_INFLIGHT) {
            commands.recv().left_future()
        } else {
            future::pending().right_future()
        }
        .fuse();
        futures::pin_mut!(event, command);
        futures::select_biased! {
            result = event => match result {
                Ok(event) => {
                    if handle_event(
                        event, &messages, &mut submitted, &mut pending,
                        &client, &mut ready, &mut tracker, receive, &mut generation, &subscriptions,
                    ).await {
                        break;
                    }
                },
                Err(error) => {
                    if is_terminal_protocol_error(&error) {
                        let cause = error.to_string();
                        fail_all(cause.clone(), &mut submitted, &mut pending, &mut ready).await;
                        queue_terminal(&messages, None, cause);
                        break;
                    }
                    warn!(?error, "MQTT connection failed; rumqttc retains protocol retransmission state");
                    let Some(delay) = tracker.record_failure() else {
                        let cause = format!("MQTT retry limit exhausted: {error}");
                        fail_all(cause.clone(), &mut submitted, &mut pending, &mut ready).await;
                        queue_terminal(&messages, None, cause);
                        break;
                    };
                    RetryTracker::backoff(delay).await;
                }
            },
            result = command => match result {
                Ok(command) => {
                    submit(&client, command, &mut submitted, &mut disconnect_waiting, closing, &mut generation, &mut subscriptions).await;
                    closing = disconnect_waiting.is_some()
                        || submitted.iter().any(|s| matches!(s, Submitted::Disconnect(_)));
                },
                Err(_) => {
                    disconnect_waiting.get_or_insert(None);
                    closing = true;
                },
            }
        }
        if submitted.is_empty() && pending.is_empty() {
            if let Some(reply) = disconnect_waiting.take() {
                match client.disconnect().await {
                    Ok(()) => submitted.push_back(Submitted::Disconnect(reply)),
                    Err(e) => respond(reply, Err(e.to_string())).await,
                }
            }
        }
    }
}

async fn submit(
    client: &TransportClient,
    command: Command,
    submitted: &mut VecDeque<Submitted>,
    disconnect_waiting: &mut Option<Option<Reply>>,
    closing: bool,
    generation: &mut u64,
    subscriptions: &mut HashMap<String, QoS>,
) {
    if closing {
        let reply = match command {
            Command::Publish(_, r) | Command::Subscribe(_, r) | Command::Unsubscribe(_, r) => {
                Some(r)
            }
            Command::SubscribeBoundary(_, r) | Command::AdvanceBoundary(r) => {
                let _ = r.send(Err("MQTT client is closing".into())).await;
                None
            }
            Command::Disconnect(r) => r,
        };
        respond(reply, Err("MQTT client is closing".into())).await;
        return;
    }
    match command {
        Command::Publish(m, r) => match parse_qos(m.qos) {
            Ok(QoS::ExactlyOnce) if client.rejects_outbound_qos2() => {
                respond(
                    Some(r),
                    Err("MQTT 5 QoS 2 publishing is unsupported because the transport cannot report rejected PubRec acknowledgements".into()),
                )
                .await;
            }
            Ok(q) => match client.publish(m.topic, q, false, m.payload).await {
                Ok(()) => submitted.push_back(Submitted::Publish(q, r)),
                Err(e) => respond(Some(r), Err(e.to_string())).await,
            },
            Err(e) => respond(Some(r), Err(e.to_string())).await,
        },
        Command::Subscribe(fs, r) => {
            let count = fs.len();
            match client.subscribe_many(fs.clone()).await {
                Ok(()) => {
                    subscriptions.extend(fs);
                    submitted.push_back(Submitted::Subscribe(count, r));
                }
                Err(e) => respond(Some(r), Err(e.to_string())).await,
            }
        }
        Command::Unsubscribe(ts, r) => {
            if ts.is_empty() {
                respond(Some(r), Ok(())).await;
            } else {
                let group = Arc::new(UnsubscribeGroup {
                    remaining: AtomicUsize::new(ts.len()),
                    reply: r,
                });
                for topic in ts {
                    match client.unsubscribe(topic.clone()).await {
                        Ok(()) => {
                            subscriptions.remove(&topic);
                            submitted.push_back(Submitted::Unsubscribe(Arc::clone(&group)));
                        }
                        Err(e) => {
                            respond(Some(group.reply.clone()), Err(e.to_string())).await;
                            break;
                        }
                    }
                }
            }
        }
        Command::SubscribeBoundary(fs, r) => {
            let count = fs.len();
            match client.subscribe_many(fs.clone()).await {
                Ok(()) => {
                    subscriptions.extend(fs);
                    submitted.push_back(Submitted::SubscribeBoundary(count, r));
                }
                Err(e) => {
                    let _ = r.send(Err(e.to_string())).await;
                }
            }
        }
        Command::AdvanceBoundary(r) => {
            *generation = generation.wrapping_add(1);
            let _ = r.send(Ok(*generation)).await;
        }
        Command::Disconnect(r) => *disconnect_waiting = Some(r),
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_event(
    event: Event,
    messages: &async_channel::Sender<Result<RawMqttMessage, InputError>>,
    submitted: &mut VecDeque<Submitted>,
    pending: &mut HashMap<u16, Pending>,
    client: &TransportClient,
    ready: &mut Option<async_channel::Sender<Result<(), String>>>,
    tracker: &mut RetryTracker,
    receive: bool,
    generation: &mut u64,
    subscriptions: &HashMap<String, QoS>,
) -> bool {
    match event {
        Event::Outgoing(Outgoing::Publish(id)) => {
            if !pending.contains_key(&id) {
                match submitted.pop_front() {
                    Some(Submitted::Publish(QoS::AtMostOnce, r)) => {
                        tracker.record_success();
                        respond(Some(r), Ok(())).await
                    }
                    Some(Submitted::Publish(qos, r)) => {
                        pending.insert(id, Pending::Publish(qos, r));
                    }
                    _ => {
                        return protocol_failure(
                            messages,
                            "unexpected outgoing Publish",
                            submitted,
                            pending,
                        )
                        .await;
                    }
                }
            }
        }
        Event::Outgoing(Outgoing::Subscribe(id)) if pending.contains_key(&id) => {}
        Event::Outgoing(Outgoing::Subscribe(id)) => match submitted.pop_front() {
            Some(Submitted::Subscribe(n, r)) => {
                pending.insert(id, Pending::Subscribe(n, r));
            }
            Some(Submitted::SubscribeBoundary(n, r)) => {
                pending.insert(id, Pending::SubscribeBoundary(n, r));
            }
            Some(Submitted::Restore(n)) => {
                pending.insert(id, Pending::Restore(n));
            }
            _ => {
                return protocol_failure(
                    messages,
                    "unexpected outgoing Subscribe",
                    submitted,
                    pending,
                )
                .await;
            }
        },
        Event::Outgoing(Outgoing::Unsubscribe(id)) if pending.contains_key(&id) => {}
        Event::Outgoing(Outgoing::Unsubscribe(id)) => match submitted.pop_front() {
            Some(Submitted::Unsubscribe(group)) => {
                pending.insert(id, Pending::Unsubscribe(group));
            }
            _ => {
                return protocol_failure(
                    messages,
                    "unexpected outgoing Unsubscribe",
                    submitted,
                    pending,
                )
                .await;
            }
        },
        Event::Outgoing(Outgoing::Disconnect) => match submitted.pop_front() {
            Some(Submitted::Disconnect(r)) => {
                respond(r, Ok(())).await;
                return true;
            }
            _ => {
                return protocol_failure(
                    messages,
                    "unexpected outgoing Disconnect",
                    submitted,
                    pending,
                )
                .await;
            }
        },
        Event::Incoming(Incoming::ConnAck(ack)) => {
            if let Some(tx) = ready.take() {
                tracker.record_success();
                let _ = tx.send(Ok(())).await;
            } else if !ack.session_present && !subscriptions.is_empty() {
                let filters = subscriptions
                    .iter()
                    .map(|(topic, qos)| (topic.clone(), *qos))
                    .collect();
                match client.subscribe_many(filters).await {
                    Ok(()) => submitted.push_back(Submitted::Restore(subscriptions.len())),
                    Err(error) => {
                        return protocol_failure(
                            messages,
                            &format!("failed to restore MQTT subscriptions: {error}"),
                            submitted,
                            pending,
                        )
                        .await;
                    }
                }
            }
        }
        Event::Incoming(Incoming::PubAck(a)) => match pending.remove(&a.pkid) {
            Some(Pending::Publish(QoS::AtLeastOnce, r)) => {
                tracker.record_success();
                respond(Some(r), Ok(())).await
            }
            _ => return protocol_failure(messages, "unmatched PubAck", submitted, pending).await,
        },
        Event::Incoming(Incoming::PubComp(a)) => match pending.remove(&a.pkid) {
            Some(Pending::Publish(QoS::ExactlyOnce, r)) => {
                tracker.record_success();
                respond(Some(r), Ok(())).await
            }
            _ => return protocol_failure(messages, "unmatched PubComp", submitted, pending).await,
        },
        Event::Incoming(Incoming::SubAck(a)) => match pending.remove(&a.pkid) {
            Some(Pending::Subscribe(n, r)) => {
                tracker.record_success();
                respond(
                    Some(r),
                    validate_suback(&a.return_codes, n).map_err(|e| e.to_string()),
                )
                .await
            }
            Some(Pending::SubscribeBoundary(n, r)) => {
                tracker.record_success();
                match validate_suback(&a.return_codes, n) {
                    Ok(()) => {
                        *generation = generation.wrapping_add(1);
                        let _ = r.send(Ok(*generation)).await;
                    }
                    Err(error) => {
                        let text = error.to_string();
                        let _ = r.send(Err(text.clone())).await;
                        return protocol_failure(messages, &text, submitted, pending).await;
                    }
                }
            }
            Some(Pending::Restore(n)) => {
                tracker.record_success();
                if validate_suback(&a.return_codes, n).is_err() {
                    return protocol_failure(
                        messages,
                        "MQTT subscription restore was rejected",
                        submitted,
                        pending,
                    )
                    .await;
                }
            }
            _ => return protocol_failure(messages, "unmatched SubAck", submitted, pending).await,
        },
        Event::Incoming(Incoming::UnsubAck(a)) => match pending.remove(&a.pkid) {
            Some(Pending::Unsubscribe(group)) => {
                tracker.record_success();
                if group.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
                    respond(Some(group.reply.clone()), Ok(())).await;
                }
            }
            _ => return protocol_failure(messages, "unmatched UnsubAck", submitted, pending).await,
        },
        Event::Incoming(Incoming::Publish(p)) => {
            let m = RawMqttMessage {
                topic: p.topic,
                payload: p.payload.to_vec(),
                qos: qos_number(p.qos),
                generation: *generation,
            };
            if receive {
                if let Err(error) = messages.try_send(Ok(m)) {
                    let observation = match error.into_inner() {
                        Ok(message) => Some(message),
                        Err(_) => None,
                    };
                    queue_terminal(
                        messages,
                        observation,
                        "MQTT receive backlog exhausted or its consumer was dropped".into(),
                    );
                    fail_protocol(
                        "MQTT receive backlog exhausted or its consumer was dropped",
                        submitted,
                        pending,
                    )
                    .await;
                    return true;
                }
            }
        }
        _ => {}
    }
    false
}

async fn protocol_failure(
    messages: &async_channel::Sender<Result<RawMqttMessage, InputError>>,
    error: &str,
    submitted: &mut VecDeque<Submitted>,
    pending: &mut HashMap<u16, Pending>,
) -> bool {
    queue_terminal(messages, None, error.into());
    fail_protocol(error, submitted, pending).await;
    true
}
async fn fail_protocol(
    error: &str,
    submitted: &mut VecDeque<Submitted>,
    pending: &mut HashMap<u16, Pending>,
) {
    while let Some(s) = submitted.pop_front() {
        let r = match s {
            Submitted::Publish(_, r) | Submitted::Subscribe(_, r) => Some(r),
            Submitted::Unsubscribe(group) => Some(group.reply.clone()),
            Submitted::Disconnect(r) => r,
            Submitted::SubscribeBoundary(_, r) => {
                let _ = r.send(Err(error.into())).await;
                None
            }
            Submitted::Restore(_) => None,
        };
        respond(r, Err(error.into())).await;
    }
    for (_, p) in pending.drain() {
        let r = match p {
            Pending::Publish(_, r) | Pending::Subscribe(_, r) => r,
            Pending::Unsubscribe(group) => group.reply.clone(),
            Pending::SubscribeBoundary(_, r) => {
                let _ = r.send(Err(error.into())).await;
                continue;
            }
            Pending::Restore(_) => continue,
        };
        respond(Some(r), Err(error.into())).await;
    }
}
async fn fail_all(
    error: String,
    submitted: &mut VecDeque<Submitted>,
    pending: &mut HashMap<u16, Pending>,
    ready: &mut Option<async_channel::Sender<Result<(), String>>>,
) {
    fail_protocol(&error, submitted, pending).await;
    if let Some(r) = ready.take() {
        let _ = r.send(Err(error)).await;
    }
}
async fn respond(reply: Option<Reply>, result: Result<(), String>) {
    if let Some(r) = reply {
        let _ = r.send(result).await;
    }
}

fn queue_terminal(
    messages: &async_channel::Sender<Result<RawMqttMessage, InputError>>,
    observation: Option<RawMqttMessage>,
    error: String,
) {
    let messages = messages.clone();
    smol::spawn(async move {
        if let Some(message) = observation {
            if messages.send(Ok(message)).await.is_err() {
                return;
            }
        }
        let _ = messages.send(Err(InputError::source(error))).await;
    })
    .detach();
}

fn validate_suback(codes: &[SubscribeReasonCode], topics: usize) -> anyhow::Result<()> {
    anyhow::ensure!(
        codes.len() == topics,
        "MQTT SubAck returned {} result codes for {topics} topics",
        codes.len()
    );
    if let Some((i, c)) = codes
        .iter()
        .enumerate()
        .find(|(_, c)| !matches!(c, SubscribeReasonCode::Success(_)))
    {
        anyhow::bail!("MQTT subscription at topic index {i} was rejected: {c:?}")
    }
    Ok(())
}
fn parse_qos(q: i32) -> anyhow::Result<QoS> {
    match q {
        0 => Ok(QoS::AtMostOnce),
        1 => Ok(QoS::AtLeastOnce),
        2 => Ok(QoS::ExactlyOnce),
        _ => Err(anyhow!("unsupported MQTT QoS {q}")),
    }
}
fn qos_number(q: QoS) -> i32 {
    match q {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    }
}
fn options_from_uri(uri: &str) -> anyhow::Result<MqttOptions> {
    let a = uri
        .strip_prefix("tcp://")
        .or_else(|| uri.strip_prefix("mqtt://"))
        .unwrap_or(uri);
    anyhow::ensure!(!a.contains('/'), "unsupported MQTT URI `{uri}`");
    let (h, p) = match a.rsplit_once(':') {
        Some((h, p)) if !h.is_empty() => (h, p.parse().context("invalid MQTT port")?),
        _ => (a, 1883),
    };
    anyhow::ensure!(!h.is_empty(), "MQTT URI has no host");
    let mut options = MqttOptions::new(
        format!("robosapiens_trustworthiness_checker_{}", Uuid::new_v4()),
        h,
        p,
    );
    options
        .set_keep_alive(Duration::from_secs(30))
        .set_clean_session(false);
    Ok(options)
}

fn v5_options(options: &MqttOptions) -> rumqttc::v5::MqttOptions {
    let (host, port) = options.broker_address();
    let mut v5 = rumqttc::v5::MqttOptions::new(options.client_id(), host, port);
    v5.set_keep_alive(Duration::from_secs(30))
        .set_clean_start(false)
        .set_session_expiry_interval(Some(u32::MAX))
        .set_request_channel_capacity(REQUEST_CAPACITY)
        .set_outgoing_inflight_upper_limit(MAX_INFLIGHT);
    v5
}
#[cfg(test)]
mod tests {
    use super::*;
    use rumqttc::PubAck;

    #[test]
    fn mqtt5_negative_acknowledgements_are_terminal_protocol_errors() {
        use rumqttc::v5::mqttbytes::v5::{Packet, PubAck, PubAckReason, PubRec, PubRecReason};
        let mut ack = PubAck::new(3, None);
        ack.reason = PubAckReason::NotAuthorized;
        let error = normalize_v5(rumqttc::v5::Event::Incoming(Packet::PubAck(ack))).unwrap_err();
        assert!(error.downcast_ref::<ProtocolAckError>().is_some());

        let mut rec = PubRec::new(4, None);
        rec.reason = PubRecReason::QuotaExceeded;
        let error = normalize_v5(rumqttc::v5::Event::Incoming(Packet::PubRec(rec))).unwrap_err();
        assert!(error.downcast_ref::<ProtocolAckError>().is_some());
    }

    #[test]
    fn mqtt5_no_matching_subscribers_completes_qos1_publish() {
        use rumqttc::v5::mqttbytes::v5::{Packet, PubAck, PubAckReason};
        let mut ack = PubAck::new(3, None);
        ack.reason = PubAckReason::NoMatchingSubscribers;
        assert!(
            normalize_v5(rumqttc::v5::Event::Incoming(Packet::PubAck(ack)))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn mqtt5_negative_subscription_acknowledgements_are_terminal() {
        use rumqttc::v5::mqttbytes::v5::{
            Packet, SubAck, SubscribeReasonCode, UnsubAck, UnsubAckReason,
        };
        let suback = SubAck {
            pkid: 8,
            return_codes: vec![SubscribeReasonCode::NotAuthorized],
            properties: None,
        };
        let error = normalize_v5(rumqttc::v5::Event::Incoming(Packet::SubAck(suback))).unwrap_err();
        assert!(is_terminal_protocol_error(&error));

        let unsuback = UnsubAck {
            pkid: 9,
            reasons: vec![UnsubAckReason::NotAuthorized],
            properties: None,
        };
        let error =
            normalize_v5(rumqttc::v5::Event::Incoming(Packet::UnsubAck(unsuback))).unwrap_err();
        assert!(is_terminal_protocol_error(&error));
    }

    #[test]
    fn mqtt5_session_options_preserve_broker_session() {
        let base = options_from_uri("tcp://broker:2883").unwrap();
        let options = v5_options(&base);
        assert_eq!(options.broker_address(), ("broker".into(), 2883));
        assert!(!options.clean_start());
        assert_eq!(options.session_expiry_interval(), Some(u32::MAX));
        assert_eq!(options.keep_alive(), Duration::from_secs(30));
    }

    #[test]
    fn mqtt5_qos2_publish_is_rejected_before_transport_submission() {
        smol::block_on(async {
            let (network, _eventloop) = rumqttc::v5::AsyncClient::new(
                rumqttc::v5::MqttOptions::new("qos2-test", "localhost", 1883),
                2,
            );
            let (reply, result) = async_channel::bounded(1);
            let mut submitted = VecDeque::new();
            let mut disconnect = None;
            submit(
                &TransportClient::V5(network),
                Command::Publish(MqttMessage::new("topic".into(), "value".into(), 2), reply),
                &mut submitted,
                &mut disconnect,
                false,
                &mut 0,
                &mut HashMap::new(),
            )
            .await;

            assert!(submitted.is_empty());
            let error = result.recv().await.unwrap().unwrap_err();
            assert!(error.contains("MQTT 5 QoS 2 publishing is unsupported"));
        });
    }

    fn raw(topic: &str, payload: &str) -> RawMqttMessage {
        RawMqttMessage {
            topic: topic.into(),
            payload: payload.as_bytes().to_vec(),
            qos: 1,
            generation: 0,
        }
    }

    async fn drive(
        event: Event,
        submitted: &mut VecDeque<Submitted>,
        pending: &mut HashMap<u16, Pending>,
        tracker: &mut RetryTracker,
    ) -> bool {
        let (messages, _rx) = async_channel::bounded(1);
        let mut ready = None;
        let mut generation = 0;
        handle_event(
            event,
            &messages,
            submitted,
            pending,
            &TransportClient::V311(
                AsyncClient::new(MqttOptions::new("test-drive", "localhost", 1883), 2).0,
            ),
            &mut ready,
            tracker,
            false,
            &mut generation,
            &HashMap::new(),
        )
        .await
    }

    #[test]
    fn qos1_completion_waits_for_matching_puback() {
        smol::block_on(async {
            let (reply, completed) = async_channel::bounded(1);
            let mut submitted = VecDeque::from([Submitted::Publish(QoS::AtLeastOnce, reply)]);
            let mut pending = HashMap::new();
            let mut tracker = RetryPolicy::output_default().tracker();

            assert!(
                !drive(
                    Event::Outgoing(Outgoing::Publish(7)),
                    &mut submitted,
                    &mut pending,
                    &mut tracker
                )
                .await
            );
            assert!(completed.try_recv().is_err());
            assert!(
                !drive(
                    Event::Incoming(Incoming::PubAck(PubAck::new(7))),
                    &mut submitted,
                    &mut pending,
                    &mut tracker
                )
                .await
            );
            assert_eq!(completed.recv().await.unwrap(), Ok(()));
        });
    }

    #[test]
    fn reconnect_retransmission_does_not_consume_next_submission() {
        smol::block_on(async {
            let (first, _first_rx) = async_channel::bounded(1);
            let (second, _second_rx) = async_channel::bounded(1);
            let mut submitted = VecDeque::from([Submitted::Publish(QoS::AtLeastOnce, second)]);
            let mut pending = HashMap::from([(9, Pending::Publish(QoS::AtLeastOnce, first))]);
            let mut tracker = RetryPolicy::output_default().tracker();

            assert!(
                !drive(
                    Event::Outgoing(Outgoing::Publish(9)),
                    &mut submitted,
                    &mut pending,
                    &mut tracker
                )
                .await
            );
            assert_eq!(submitted.len(), 1);
            assert!(pending.contains_key(&9));
        });
    }

    #[test]
    fn subscribe_retransmission_does_not_consume_restore_submission() {
        smol::block_on(async {
            let (reply, _reply_rx) = async_channel::bounded(1);
            let mut submitted =
                VecDeque::from([Submitted::Restore(2), Submitted::Subscribe(1, reply)]);
            let mut pending = HashMap::new();
            let mut tracker = RetryPolicy::output_default().tracker();
            assert!(
                !drive(
                    Event::Outgoing(Outgoing::Subscribe(11)),
                    &mut submitted,
                    &mut pending,
                    &mut tracker
                )
                .await
            );
            assert!(matches!(pending.get(&11), Some(Pending::Restore(2))));
            assert_eq!(submitted.len(), 1);
            assert!(
                !drive(
                    Event::Outgoing(Outgoing::Subscribe(11)),
                    &mut submitted,
                    &mut pending,
                    &mut tracker
                )
                .await
            );
            assert_eq!(submitted.len(), 1);
        });
    }

    #[test]
    fn unsubscribe_retransmission_keeps_the_next_submission_queued() {
        smol::block_on(async {
            let (first, _first_rx) = async_channel::bounded(1);
            let (second, _second_rx) = async_channel::bounded(1);
            let group = Arc::new(UnsubscribeGroup {
                remaining: AtomicUsize::new(1),
                reply: first,
            });
            let mut submitted = VecDeque::from([Submitted::Subscribe(1, second)]);
            let mut pending = HashMap::from([(12, Pending::Unsubscribe(Arc::clone(&group)))]);
            let mut tracker = RetryPolicy::output_default().tracker();
            assert!(
                !drive(
                    Event::Outgoing(Outgoing::Unsubscribe(12)),
                    &mut submitted,
                    &mut pending,
                    &mut tracker
                )
                .await
            );
            assert_eq!(submitted.len(), 1);
            assert!(matches!(pending.get(&12), Some(Pending::Unsubscribe(_))));
        });
    }

    #[test]
    fn disconnect_is_held_behind_accepted_work() {
        smol::block_on(async {
            let options = MqttOptions::new("test", "localhost", 1883);
            let (client, _eventloop) = AsyncClient::new(options, 2);
            let (publish_reply, _rx) = async_channel::bounded(1);
            let (close_reply, _close_rx) = async_channel::bounded(1);
            let mut submitted =
                VecDeque::from([Submitted::Publish(QoS::AtLeastOnce, publish_reply)]);
            let mut disconnect = None;
            submit(
                &TransportClient::V311(client),
                Command::Disconnect(Some(close_reply)),
                &mut submitted,
                &mut disconnect,
                false,
                &mut 0,
                &mut HashMap::new(),
            )
            .await;
            assert!(disconnect.is_some());
            assert_eq!(submitted.len(), 1);
        });
    }

    #[test]
    fn last_handle_drop_closes_a_full_command_channel() {
        let (commands, receiver) = async_channel::bounded(1);
        let (reply, _reply_rx) = async_channel::bounded(1);
        assert!(
            commands
                .try_send(Command::Publish(
                    MqttMessage::new("x".into(), "1".into(), 1),
                    reply
                ))
                .is_ok()
        );
        let shared = Shared { commands };
        drop(shared);
        assert!(matches!(receiver.try_recv(), Ok(Command::Publish(_, _))));
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn connection_success_does_not_reset_pending_operation_retry_budget() {
        smol::block_on(async {
            let mut submitted = VecDeque::new();
            let mut pending = HashMap::new();
            let mut tracker = RetryPolicy::output_default().tracker();
            tracker.record_failure();
            let before = tracker.consecutive_failures();
            // With initial readiness already reported, this is a reconnect.
            let (messages, _rx) = async_channel::bounded(1);
            let (client, _eventloop) =
                AsyncClient::new(MqttOptions::new("connack-test", "localhost", 1883), 2);
            let mut ready = None;
            let connack = rumqttc::ConnAck::new(rumqttc::ConnectReturnCode::Success, false);
            handle_event(
                Event::Incoming(Incoming::ConnAck(connack)),
                &messages,
                &mut submitted,
                &mut pending,
                &TransportClient::V311(client),
                &mut ready,
                &mut tracker,
                false,
                &mut 0,
                &HashMap::new(),
            )
            .await;
            assert_eq!(tracker.consecutive_failures(), before);
        });
    }

    #[test]
    fn terminal_receive_error_follows_admitted_observations() {
        smol::block_on(async {
            let (messages, receiver) = async_channel::bounded(1);
            let admitted = raw("first", "1");
            messages.send(Ok(admitted)).await.unwrap();
            queue_terminal(
                &messages,
                Some(raw("overflow", "2")),
                "receive backlog exhausted".into(),
            );
            drop(messages);

            assert_eq!(receiver.recv().await.unwrap().unwrap().topic, "first");
            assert_eq!(receiver.recv().await.unwrap().unwrap().topic, "overflow");
            let error = receiver.recv().await.unwrap().unwrap_err();
            assert!(error.to_string().contains("receive backlog exhausted"));
            assert!(receiver.recv().await.is_err());
        });
    }

    #[test]
    fn clean_receive_close_has_no_synthetic_error() {
        smol::block_on(async {
            let (messages, receiver) =
                async_channel::bounded::<Result<RawMqttMessage, InputError>>(1);
            messages.send(Ok(raw("last", "1"))).await.unwrap();
            drop(messages);
            assert_eq!(receiver.recv().await.unwrap().unwrap().topic, "last");
            assert!(receiver.recv().await.is_err());
        });
    }
    #[test]
    fn uri_preserves_identity_and_session() {
        let o = options_from_uri("tcp://broker:2883").unwrap();
        assert_eq!(o.broker_address(), ("broker".into(), 2883));
        assert!(!o.clean_session());
        assert!(
            o.client_id()
                .starts_with("robosapiens_trustworthiness_checker_")
        );
    }
    #[test]
    fn rejects_negative_suback() {
        assert!(
            validate_suback(&[SubscribeReasonCode::Failure], 1)
                .unwrap_err()
                .to_string()
                .contains("topic index 0")
        );
    }
}
