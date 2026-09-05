use anyhow::Context;
use futures::{FutureExt, StreamExt};
use std::collections::{BTreeMap, btree_map::Entry};

use crate::core::{InputBatch, InputStream, JsonStreamValue, LocalStream, VarName};
use crate::io::{ReconfigurationRequest, RetryPolicy};

#[derive(Debug)]
pub(crate) enum RedisInputItem<V> {
    Data(InputBatch<V>),
    Control(ReconfigurationRequest),
    Boundary(u64),
}

type RedisChannelMap = BTreeMap<String, VarName>;

/// Owns the input worker and its Pub/Sub connection. Cancellation joins the
/// worker, dropping both connection halves and releasing subscriptions.
pub(crate) struct RedisInputControl {
    commands: async_channel::Sender<RedisInputCommand>,
    worker: Option<smol::Task<anyhow::Result<()>>>,
    control_topic: Option<String>,
}

enum RedisInputCommand {
    Pause(u64),
    Rebind(
        BTreeMap<VarName, String>,
        async_channel::Sender<Result<(), String>>,
    ),
    Resume,
}

impl RedisInputControl {
    pub(crate) async fn pause(&self, boundary: u64) -> anyhow::Result<()> {
        self.commands
            .send(RedisInputCommand::Pause(boundary))
            .await
            .map_err(|_| anyhow::anyhow!("Redis input worker stopped before pause"))
    }

    pub(crate) async fn resume(&self) -> anyhow::Result<()> {
        self.commands
            .send(RedisInputCommand::Resume)
            .await
            .map_err(|_| anyhow::anyhow!("Redis input worker stopped before resume"))
    }

    pub(crate) async fn rebind(
        &mut self,
        data_topics: BTreeMap<VarName, String>,
    ) -> anyhow::Result<()> {
        validate_topics(&data_topics, self.control_topic.as_deref())?;
        let (tx, rx) = async_channel::bounded(1);
        self.commands
            .send(RedisInputCommand::Rebind(data_topics, tx))
            .await
            .map_err(|_| anyhow::anyhow!("Redis input worker stopped before rebind"))?;
        rx.recv()
            .await
            .map_err(|_| anyhow::anyhow!("Redis input worker stopped during rebind"))?
            .map_err(anyhow::Error::msg)
    }

    pub(crate) async fn shutdown(&mut self) -> anyhow::Result<()> {
        self.commands.close();
        if let Some(worker) = self.worker.take() {
            if let Some(result) = worker.cancel().await {
                result?;
            }
        }
        Ok(())
    }
}

impl Drop for RedisInputControl {
    fn drop(&mut self) {
        // Dropping the sink closes the Pub/Sub connection. Explicit shutdown
        // is used by into_drain so its EOF waits for UNSUBSCRIBE completion.
        self.commands.close();
    }
}

fn retryable_connection_error(error: &redis::RedisError) -> bool {
    match error.kind() {
        redis::ErrorKind::Io => true,
        redis::ErrorKind::Server(kind) => matches!(
            kind,
            redis::ServerErrorKind::BusyLoading
                | redis::ServerErrorKind::TryAgain
                | redis::ServerErrorKind::ClusterDown
                | redis::ServerErrorKind::MasterDown
        ),
        _ => false,
    }
}

async fn connect_once(
    url: &str,
    data_topics: &BTreeMap<VarName, String>,
    control_topic: Option<&str>,
) -> Result<(redis::aio::PubSubSink, redis::aio::PubSubStream), redis::RedisError> {
    let client = redis::Client::open(url)?;
    let pubsub = client.get_async_pubsub().await?;
    let (mut sink, messages) = pubsub.split();
    let mut channels = data_topics.values().cloned().collect::<Vec<_>>();
    if let Some(control) = control_topic {
        channels.push(control.to_owned());
    }
    if !channels.is_empty() {
        sink.subscribe(channels).await?;
    }
    Ok((sink, messages))
}

async fn connect_with_retry(
    url: &str,
    data_topics: &BTreeMap<VarName, String>,
    control_topic: Option<&str>,
    retry: RetryPolicy,
) -> anyhow::Result<(redis::aio::PubSubSink, redis::aio::PubSubStream)> {
    let mut tracker = retry.tracker();
    loop {
        match connect_once(url, data_topics, control_topic).await {
            Ok(connection) => return Ok(connection),
            Err(error) if retryable_connection_error(&error) => {
                let Some(delay) = tracker.record_failure() else {
                    return Err(anyhow::Error::new(error));
                };
                crate::io::RetryTracker::backoff(delay).await;
            }
            Err(error) => return Err(anyhow::Error::new(error)),
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_input_worker(
    url: String,
    retry: RetryPolicy,
    mut data_topics: BTreeMap<VarName, String>,
    mut topic_vars: RedisChannelMap,
    control_topic: Option<String>,
    mut sink: redis::aio::PubSubSink,
    mut messages: redis::aio::PubSubStream,
    events: async_channel::Sender<anyhow::Result<RedisIngressItem>>,
    commands: async_channel::Receiver<RedisInputCommand>,
) -> anyhow::Result<()> {
    enum Next {
        Command(Result<RedisInputCommand, async_channel::RecvError>),
        Message(Option<redis::Msg>),
    }
    let mut paused = false;
    loop {
        let next = {
            let command = commands.recv().fuse();
            let message = if paused {
                futures::future::pending().boxed()
            } else {
                messages.next().boxed()
            }
            .fuse();
            futures::pin_mut!(command, message);
            futures::select_biased! {
                command = command => Next::Command(command),
                message = message => Next::Message(message),
            }
        };
        match next {
            Next::Command(command) => match command {
                Ok(RedisInputCommand::Pause(id)) => {
                    // This sender is the same FIFO as data delivery. Awaiting
                    // capacity preserves every admitted observation before the marker.
                    if emit_boundary(&events, id).await.is_err() {
                        break;
                    }
                    paused = true;
                }
                Ok(RedisInputCommand::Resume) => paused = false,
                Ok(RedisInputCommand::Rebind(candidate, response)) => {
                    let result = async {
                        let candidate_vars = validate_topics(&candidate, control_topic.as_deref())?;
                        let removed = data_topics
                            .values()
                            .filter(|t| !candidate.values().any(|n| n == *t))
                            .cloned()
                            .collect::<Vec<_>>();
                        let added = candidate
                            .values()
                            .filter(|t| !data_topics.values().any(|a| a == *t))
                            .cloned()
                            .collect::<Vec<_>>();
                        if !removed.is_empty() {
                            sink.unsubscribe(removed).await?;
                        }
                        if !added.is_empty() {
                            sink.subscribe(added).await?;
                        }
                        data_topics = candidate;
                        topic_vars = candidate_vars;
                        paused = false;
                        Ok::<_, anyhow::Error>(())
                    }
                    .await
                    .map_err(|e| e.to_string());
                    let _ = response.send(result).await;
                }
                Err(_) => break,
            },
            Next::Message(message) => match message {
                Some(message) => {
                    match route_message(message, &topic_vars, control_topic.as_deref()) {
                        Ok(Some(item)) => {
                            if events.send(Ok(item)).await.is_err() {
                                break;
                            }
                        }
                        Ok(None) => {}
                        Err(error) => {
                            let _ = events.send(Err(error)).await;
                            break;
                        }
                    }
                }
                None => {
                    let recovered =
                        connect_with_retry(&url, &data_topics, control_topic.as_deref(), retry)
                            .await
                            .context("Redis input connection recovery failed")?;
                    sink = recovered.0;
                    messages = recovered.1;
                }
            },
        }
    }
    Ok(())
}

async fn emit_boundary(
    events: &async_channel::Sender<anyhow::Result<RedisIngressItem>>,
    id: u64,
) -> Result<(), async_channel::SendError<anyhow::Result<RedisIngressItem>>> {
    events.send(Ok(RedisIngressItem::Boundary(id))).await
}

pub(crate) async fn open_owned_input_stream_items<V: JsonStreamValue>(
    hostname: &str,
    port: Option<u16>,
    var_topics: BTreeMap<VarName, String>,
    control_topic: Option<String>,
    retry: RetryPolicy,
) -> anyhow::Result<(
    LocalStream<anyhow::Result<RedisInputItem<V>>>,
    RedisInputControl,
)> {
    let topic_vars = validate_topics(&var_topics, control_topic.as_deref())?;
    let url = match port {
        Some(port) => format!("redis://{hostname}:{port}"),
        None => format!("redis://{hostname}"),
    };
    let (sink, messages) =
        connect_with_retry(&url, &var_topics, control_topic.as_deref(), retry).await?;
    let (events, receiver) = async_channel::bounded(1024);
    let (commands, command_receiver) = async_channel::bounded(8);
    let worker_control_topic = control_topic.clone();
    let worker = smol::spawn(run_input_worker(
        url,
        retry,
        var_topics,
        topic_vars,
        worker_control_topic,
        sink,
        messages,
        events,
        command_receiver,
    ));
    let stream = Box::pin(async_stream::try_stream! {
        while let Ok(item) = receiver.recv().await {
            yield match item? {
                RedisIngressItem::Data(variable, payload) => {
                    let value = V::decode_json(&payload)
                        .with_context(|| format!("invalid Redis JSON5 payload for variable `{variable}`"))?;
                    RedisInputItem::Data(InputBatch::update(variable, value))
                }
                RedisIngressItem::Control(payload) => RedisInputItem::Control(
                    ReconfigurationRequest::from_json(std::str::from_utf8(&payload)
                        .context("Redis monitor configuration is not valid UTF-8")?)?
                ),
                RedisIngressItem::Boundary(id) => RedisInputItem::Boundary(id),
            };
        }
    });
    Ok((
        stream,
        RedisInputControl {
            commands,
            worker: Some(worker),
            control_topic,
        },
    ))
}

pub async fn input_stream_items<V: JsonStreamValue>(
    hostname: &str,
    port: Option<u16>,
    var_topics: BTreeMap<VarName, String>,
    control_topic: Option<String>,
) -> anyhow::Result<LocalStream<anyhow::Result<RedisInputItem<V>>>> {
    if var_topics.is_empty() && control_topic.is_none() {
        return Ok(Box::pin(futures::stream::empty()));
    }
    let (mut stream, mut owner) = open_owned_input_stream_items(
        hostname,
        port,
        var_topics,
        control_topic,
        RetryPolicy::input_default(),
    )
    .await?;
    Ok(Box::pin(async_stream::try_stream! {
        while let Some(item) = stream.next().await {
            yield item?;
        }
        owner.shutdown().await?;
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
                RedisInputItem::Boundary(_) => unreachable!("data-only Redis stream is never paused"),
            }
        }
    }))
}

#[derive(Debug)]
enum RedisIngressItem {
    Data(VarName, Vec<u8>),
    Control(Vec<u8>),
    Boundary(u64),
}

fn route_message(
    message: redis::Msg,
    topic_vars: &RedisChannelMap,
    control_topic: Option<&str>,
) -> anyhow::Result<Option<RedisIngressItem>> {
    if control_topic == Some(message.get_channel_name()) {
        return Ok(Some(RedisIngressItem::Control(message.get_payload()?)));
    }
    let Some(variable) = topic_vars.get(message.get_channel_name()).cloned() else {
        return Ok(None);
    };
    Ok(Some(RedisIngressItem::Data(
        variable,
        message.get_payload()?,
    )))
}

fn validate_topics(
    var_topics: &BTreeMap<VarName, String>,
    control_topic: Option<&str>,
) -> anyhow::Result<RedisChannelMap> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boundary_waits_for_capacity_and_follows_admitted_data() {
        smol::block_on(async {
            let (tx, rx) = async_channel::bounded(1);
            tx.send(Ok(RedisIngressItem::Data(VarName::new("x"), b"1".to_vec())))
                .await
                .unwrap();

            let marker = emit_boundary(&tx, 17);
            let drain = async {
                assert!(matches!(
                    rx.recv().await.unwrap().unwrap(),
                    RedisIngressItem::Data(_, _)
                ));
                marker.await.unwrap();
                assert!(matches!(
                    rx.recv().await.unwrap().unwrap(),
                    RedisIngressItem::Boundary(17)
                ));
            };
            drain.await;
        });
    }

    #[test]
    fn pause_only_enqueues_the_request() {
        smol::block_on(async {
            let (commands, receiver) = async_channel::bounded(1);
            let owner = RedisInputControl {
                commands,
                worker: None,
                control_topic: None,
            };
            owner.pause(9).await.unwrap();
            assert!(matches!(
                receiver.recv().await.unwrap(),
                RedisInputCommand::Pause(9)
            ));
            owner.commands.close();
        });
    }
}
