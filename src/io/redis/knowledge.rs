#[cfg(test)]
use crate::io::RetryLimit;
use crate::io::RetryPolicy;
#[cfg(test)]
use std::num::NonZeroU32;
use std::{cell::RefCell, collections::BTreeMap, rc::Rc, time::Duration};

use anyhow::{Context, anyhow};
use futures::{FutureExt, StreamExt, future::LocalBoxFuture, stream::FuturesUnordered};

use crate::core::{InputBatch, InputStream, InputUpdate, Value, VarName};
use crate::io::redis_config::{
    RedisKnowledgeConfig, decode_redis_knowledge_value, redis_keyspace_channel,
};

/// The Redis knowledge provider is intentionally Value-specific. Generic input
/// construction calls this opener only after validating that its value domain is
/// the ordinary project [`Value`].
pub(crate) fn open_value_redis_knowledge(
    config: RedisKnowledgeConfig,
    bindings: BTreeMap<VarName, String>,
) -> LocalBoxFuture<'static, anyhow::Result<(InputStream<Value>, RedisKnowledgeInputControl)>> {
    Box::pin(async move {
        let tasks = Rc::new(RefCell::new(Vec::new()));
        let stream = open_value_source(config, bindings, Rc::clone(&tasks)).await?;
        Ok((stream, RedisKnowledgeInputControl { tasks }))
    })
}

type NotificationTasks = Rc<RefCell<Vec<Rc<NotificationTask>>>>;

pub(crate) struct RedisKnowledgeInputControl {
    tasks: NotificationTasks,
}

impl RedisKnowledgeInputControl {
    pub(crate) async fn shutdown(&mut self) -> anyhow::Result<()> {
        let tasks = self.tasks.borrow().clone();
        for task in &tasks {
            task.cancel();
        }
        for task in tasks {
            task.join().await;
        }
        Ok(())
    }
}

impl Drop for RedisKnowledgeInputControl {
    fn drop(&mut self) {
        for task in self.tasks.borrow().iter() {
            task.cancel();
        }
    }
}

#[derive(Clone, Debug)]
struct SelectedKey {
    variable: VarName,
    key: String,
    channel: String,
}

fn selected_keys(
    config: &RedisKnowledgeConfig,
    bindings: BTreeMap<VarName, String>,
) -> anyhow::Result<Vec<SelectedKey>> {
    let mut seen_keys = BTreeMap::<String, VarName>::new();
    let mut selected = Vec::with_capacity(bindings.len());
    for (variable, key) in bindings {
        anyhow::ensure!(
            !variable.name().trim().is_empty(),
            "Redis knowledge model variable name cannot be empty"
        );
        anyhow::ensure!(
            !key.trim().is_empty(),
            "Redis knowledge key for `{variable}` cannot be empty"
        );
        if let Some(previous) = seen_keys.insert(key.clone(), variable.clone()) {
            anyhow::bail!(
                "active Redis knowledge key `{key}` is mapped to both `{previous}` and `{variable}`"
            );
        }
        selected.push(SelectedKey {
            variable,
            channel: redis_keyspace_channel(config.database, &key),
            key,
        });
    }
    Ok(selected)
}

#[derive(Debug)]
struct NotificationTracker {
    pending: Vec<bool>,
    closed: bool,
}

impl NotificationTracker {
    fn new(key_count: usize) -> Self {
        Self {
            pending: vec![false; key_count],
            closed: false,
        }
    }

    fn prepare_connection(&mut self) {
        self.closed = false;
    }

    fn mark(&mut self, index: usize) {
        if let Some(pending) = self.pending.get_mut(index) {
            *pending = true;
        }
    }

    fn close(&mut self) {
        self.closed = true;
    }

    fn take_pending(&mut self) -> Vec<usize> {
        let mut pending = Vec::new();
        for (index, dirty) in self.pending.iter_mut().enumerate() {
            if *dirty {
                *dirty = false;
                pending.push(index);
            }
        }
        pending
    }

    #[cfg(test)]
    fn pending_count(&self) -> usize {
        self.pending.iter().filter(|dirty| **dirty).count()
    }
}

type SharedNotificationTracker = std::sync::Arc<std::sync::Mutex<NotificationTracker>>;

struct NotificationDrain {
    tracker: SharedNotificationTracker,
    wake: async_channel::Receiver<()>,
    task: Rc<NotificationTask>,
    tasks: NotificationTasks,
}

struct NotificationTask {
    cancel: async_channel::Sender<()>,
    task: RefCell<Option<smol::Task<()>>>,
}

impl NotificationTask {
    fn cancel(&self) {
        let _ = self.cancel.try_send(());
    }

    async fn join(&self) {
        let task = self.task.borrow_mut().take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

impl NotificationDrain {
    fn start(
        messages: redis::aio::PubSubStream,
        channel_indices: BTreeMap<String, usize>,
        tracker: SharedNotificationTracker,
        tasks: &NotificationTasks,
    ) -> Self {
        tracker
            .lock()
            .expect("notification tracker poisoned")
            .prepare_connection();
        let (cancel, cancel_receiver) = async_channel::bounded(1);
        let (wake, wake_receiver) = async_channel::bounded(1);
        let task_tracker = tracker.clone();
        let task_wake = wake.clone();
        let task = smol::spawn(async move {
            drain_notifications(
                messages,
                channel_indices,
                task_tracker,
                task_wake,
                cancel_receiver,
            )
            .await;
        });
        let task = Rc::new(NotificationTask {
            cancel,
            task: RefCell::new(Some(task)),
        });
        tasks.borrow_mut().push(Rc::clone(&task));
        Self {
            tracker,
            wake: wake_receiver,
            task,
            tasks: Rc::clone(tasks),
        }
    }

    fn take_pending(&self) -> Vec<usize> {
        self.tracker
            .lock()
            .expect("notification tracker poisoned")
            .take_pending()
    }

    fn is_closed(&self) -> bool {
        self.tracker
            .lock()
            .expect("notification tracker poisoned")
            .closed
    }

    fn wait(&self) -> LocalBoxFuture<'_, ()> {
        Box::pin(async move {
            let _ = self.wake.recv().await;
        })
    }

    async fn stop(&mut self) {
        self.task.cancel();
        self.task.join().await;
        self.tasks
            .borrow_mut()
            .retain(|task| !Rc::ptr_eq(task, &self.task));
    }
}

impl Drop for NotificationDrain {
    fn drop(&mut self) {
        self.task.cancel();
    }
}

async fn drain_notifications(
    messages: redis::aio::PubSubStream,
    channel_indices: BTreeMap<String, usize>,
    tracker: SharedNotificationTracker,
    wake: async_channel::Sender<()>,
    cancellation: async_channel::Receiver<()>,
) {
    drain_notification_stream(
        messages,
        move |message: &redis::Msg| channel_indices.get(message.get_channel_name()).copied(),
        tracker,
        wake,
        cancellation,
    )
    .await;
}

async fn drain_notification_stream<S, F>(
    messages: S,
    index_of: F,
    tracker: SharedNotificationTracker,
    wake: async_channel::Sender<()>,
    cancellation: async_channel::Receiver<()>,
) where
    S: futures::Stream + Send + 'static,
    S::Item: Send + 'static,
    F: Fn(&S::Item) -> Option<usize> + Send + 'static,
{
    let mut messages = Box::pin(messages);
    loop {
        let mut message_stream = messages.as_mut();
        let next_message = message_stream.next().fuse();
        let cancelled = cancellation.recv().fuse();
        futures::pin_mut!(next_message, cancelled);
        futures::select_biased! {
            _ = cancelled => return,
            message = next_message => {
                let Some(message) = message else {
                    tracker
                        .lock()
                        .expect("notification tracker poisoned")
                        .close();
                    let _ = wake.try_send(());
                    return;
                };
                if let Some(index) = index_of(&message) {
                    tracker
                        .lock()
                        .expect("notification tracker poisoned")
                        .mark(index);
                    if wake.try_send(()).is_err() && wake.is_closed() {
                        return;
                    }
                }
            }
        }
    }
}

struct RedisKnowledgeConnection {
    command: redis::aio::MultiplexedConnection,
    _pubsub_sink: redis::aio::PubSubSink,
    drain: NotificationDrain,
}

impl RedisKnowledgeConnection {
    async fn stop_drain(&mut self) {
        self.drain.stop().await;
    }
}

fn redis_url(config: &RedisKnowledgeConfig) -> String {
    match config.port {
        Some(port) => format!("redis://{}:{}/{}", config.host, port, config.database),
        None => format!("redis://{}/{}", config.host, config.database),
    }
}

fn retryable_redis_error(error: &redis::RedisError) -> bool {
    match error.kind() {
        // Connection refusal, reset, broken pipes, and other transport
        // failures are represented by the Redis client as I/O errors.
        redis::ErrorKind::Io => true,
        // Redis explicitly identifies these server states as transient. The
        // remaining server errors, including NOPERM, WRONGTYPE, invalid DB,
        // and invalid command arguments, are deterministic and must not be
        // retried as if the connection were broken.
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

#[derive(Debug)]
struct AttemptError {
    error: anyhow::Error,
    retryable: bool,
}

impl AttemptError {
    fn redis(operation: impl Into<String>, error: redis::RedisError) -> Self {
        let retryable = retryable_redis_error(&error);
        Self {
            error: anyhow::Error::new(error).context(operation.into()),
            retryable,
        }
    }

    fn permanent(error: anyhow::Error) -> Self {
        Self {
            error,
            retryable: false,
        }
    }
}

trait KnowledgeTimer: 'static {
    fn sleep(&self, duration: Duration) -> LocalBoxFuture<'static, ()>;
}

#[derive(Clone, Copy, Debug)]
struct RealKnowledgeTimer;

impl KnowledgeTimer for RealKnowledgeTimer {
    fn sleep(&self, duration: Duration) -> LocalBoxFuture<'static, ()> {
        Box::pin(async move {
            let _ = smol::Timer::after(duration).await;
        })
    }
}

async fn retry_operation<T, F>(
    retry: &RetryPolicy,
    timer: &impl KnowledgeTimer,
    mut operation: F,
) -> Result<T, AttemptError>
where
    T: 'static,
    F: FnMut() -> LocalBoxFuture<'static, Result<T, AttemptError>>,
{
    let mut tracker = retry.tracker();
    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) if !error.retryable => return Err(error),
            Err(error) => {
                let Some(delay) = tracker.record_failure() else {
                    return Err(error);
                };
                timer.sleep(delay).await;
            }
        }
    }
}

async fn connect_once(
    config: &RedisKnowledgeConfig,
    channels: &[String],
    tracker: SharedNotificationTracker,
    tasks: NotificationTasks,
) -> Result<RedisKnowledgeConnection, AttemptError> {
    let url = redis_url(config);
    let client = redis::Client::open(url).map_err(|error| {
        AttemptError::redis(
            format!(
                "opening Redis knowledge client for database {}",
                config.database
            ),
            error,
        )
    })?;
    let pubsub = client.get_async_pubsub().await.map_err(|error| {
        AttemptError::redis(
            format!(
                "opening Redis knowledge Pub/Sub connection for database {}",
                config.database
            ),
            error,
        )
    })?;
    let (mut pubsub_sink, messages) = pubsub.split();
    pubsub_sink
        .subscribe(channels.to_vec())
        .await
        .map_err(|error| {
            AttemptError::redis(
                format!(
                    "subscribing to Redis knowledge keyspace channels for database {}",
                    config.database
                ),
                error,
            )
        })?;

    // The drain starts immediately after exact subscriptions are established,
    // before the command connection or any snapshot command is acquired. This
    // keeps Redis's unbounded PubSubStream queue short-lived and retains
    // notifications that arrive during MGET in the bounded tracker.
    let channel_indices = channels
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, channel)| (channel, index))
        .collect();
    let mut drain = NotificationDrain::start(messages, channel_indices, tracker, &tasks);
    let command = match client.get_multiplexed_async_connection().await {
        Ok(command) => command,
        Err(error) => {
            drain.stop().await;
            return Err(AttemptError::redis(
                format!(
                    "opening Redis knowledge command connection and selecting database {}",
                    config.database
                ),
                error,
            ));
        }
    };
    Ok(RedisKnowledgeConnection {
        command,
        _pubsub_sink: pubsub_sink,
        drain,
    })
}

async fn connect_with_retry(
    config: &RedisKnowledgeConfig,
    channels: Vec<String>,
    tracker: SharedNotificationTracker,
    tasks: NotificationTasks,
) -> anyhow::Result<RedisKnowledgeConnection> {
    let retry = config.retry.clone();
    let config = config.clone();
    retry_operation(&retry, &RealKnowledgeTimer, move || {
        let config = config.clone();
        let channels = channels.clone();
        let tracker = tracker.clone();
        let tasks = tasks.clone();
        Box::pin(async move { connect_once(&config, &channels, tracker, tasks).await })
    })
    .await
    .map_err(|error| error.error)
}

async fn read_snapshot_once(
    config: &RedisKnowledgeConfig,
    command: &mut redis::aio::MultiplexedConnection,
    selected: &[SelectedKey],
) -> Result<Vec<Value>, AttemptError> {
    let keys = selected
        .iter()
        .map(|selected| selected.key.clone())
        .collect::<Vec<_>>();
    let raw: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
        .arg(keys)
        .query_async(command)
        .await
        .map_err(|error| {
            AttemptError::redis(
                format!(
                    "reading Redis knowledge initial/reconnect MGET from database {}",
                    config.database
                ),
                error,
            )
        })?;
    if raw.len() != selected.len() {
        return Err(AttemptError::permanent(anyhow!(
            "Redis knowledge MGET returned {} values for {} selected keys",
            raw.len(),
            selected.len()
        )));
    }

    raw.into_iter()
        .zip(selected)
        .map(|(payload, selected)| {
            decode_redis_knowledge_value(&selected.key, payload.as_deref()).map_err(|error| {
                AttemptError::permanent(error.context(format!(
                    "decoding Redis knowledge snapshot for key `{}`",
                    selected.key
                )))
            })
        })
        .collect()
}

async fn connect_with_snapshot(
    config: &RedisKnowledgeConfig,
    channels: Vec<String>,
    selected: Vec<SelectedKey>,
    tracker: SharedNotificationTracker,
    tasks: NotificationTasks,
) -> anyhow::Result<(RedisKnowledgeConnection, Vec<Value>)> {
    let retry = config.retry.clone();
    let config = config.clone();
    retry_operation(&retry, &RealKnowledgeTimer, move || {
        let config = config.clone();
        let channels = channels.clone();
        let selected = selected.clone();
        let tracker = tracker.clone();
        let tasks = tasks.clone();
        Box::pin(async move {
            let mut connection = connect_once(&config, &channels, tracker, tasks).await?;
            match read_snapshot_once(&config, &mut connection.command, &selected).await {
                Ok(values) => Ok((connection, values)),
                Err(error) => {
                    // A failed snapshot must not leave its drain consuming the
                    // old connection while the retry loop creates a new one.
                    connection.stop_drain().await;
                    Err(error)
                }
            }
        })
    })
    .await
    .map_err(|error| error.error)
}

#[derive(Debug)]
struct KeyState {
    last_emitted: Option<Value>,
    dirty: bool,
    read_in_flight: bool,
    dirtied_during_read: bool,
}

#[derive(Debug)]
struct KnowledgeStateMachine {
    states: Vec<KeyState>,
}

impl KnowledgeStateMachine {
    fn new(key_count: usize) -> Self {
        Self {
            states: (0..key_count)
                .map(|_| KeyState {
                    last_emitted: None,
                    dirty: false,
                    read_in_flight: false,
                    dirtied_during_read: false,
                })
                .collect(),
        }
    }

    fn seed_initial(&mut self, index: usize, value: Value) {
        let state = &mut self.states[index];
        state.last_emitted = Some(value);
        state.dirty = false;
        state.read_in_flight = false;
        state.dirtied_during_read = false;
    }

    fn mark_dirty(&mut self, index: usize) {
        let state = &mut self.states[index];
        if state.read_in_flight {
            state.dirtied_during_read = true;
        } else {
            state.dirty = true;
        }
    }

    fn take_dirty(&mut self) -> Option<usize> {
        let index = self
            .states
            .iter()
            .position(|state| state.dirty && !state.read_in_flight)?;
        let state = &mut self.states[index];
        state.dirty = false;
        state.read_in_flight = true;
        Some(index)
    }

    fn finish_read(
        &mut self,
        index: usize,
        result: Result<Value, ReadFailure>,
    ) -> ReadCompletionResult {
        let state = &mut self.states[index];
        state.read_in_flight = false;
        if state.dirtied_during_read {
            state.dirty = true;
            state.dirtied_during_read = false;
        }

        match result {
            Ok(value) => {
                if state.last_emitted.as_ref() == Some(&value) {
                    ReadCompletionResult::Unchanged
                } else {
                    state.last_emitted = Some(value.clone());
                    ReadCompletionResult::Changed(value)
                }
            }
            Err(error) => ReadCompletionResult::Failed(error),
        }
    }

    fn reset_reads_for_reconnect(&mut self) {
        for state in &mut self.states {
            if state.read_in_flight && state.dirtied_during_read {
                state.dirty = true;
            }
            state.read_in_flight = false;
            state.dirtied_during_read = false;
        }
    }

    fn observe_snapshot(&mut self, index: usize, value: Value) -> Option<Value> {
        let state = &mut self.states[index];
        state.read_in_flight = false;
        state.dirtied_during_read = false;
        state.dirty = false;
        if state.last_emitted.as_ref() == Some(&value) {
            None
        } else {
            state.last_emitted = Some(value.clone());
            Some(value)
        }
    }
}

enum ReadFailure {
    Redis(redis::RedisError),
    Decode(anyhow::Error),
}

struct ReadCompletion {
    index: usize,
    result: Result<Value, ReadFailure>,
}

enum ReadCompletionResult {
    Changed(Value),
    Unchanged,
    Failed(ReadFailure),
}

async fn read_key(
    index: usize,
    key: String,
    mut command: redis::aio::MultiplexedConnection,
) -> ReadCompletion {
    let raw: Result<Option<Vec<u8>>, redis::RedisError> =
        redis::cmd("GET").arg(&key).query_async(&mut command).await;
    let result = match raw {
        Ok(payload) => {
            decode_redis_knowledge_value(&key, payload.as_deref()).map_err(ReadFailure::Decode)
        }
        Err(error) => Err(ReadFailure::Redis(error)),
    };
    ReadCompletion { index, result }
}

fn schedule_reads(
    state: &mut KnowledgeStateMachine,
    command: &redis::aio::MultiplexedConnection,
    selected: &[SelectedKey],
    reads: &mut FuturesUnordered<LocalBoxFuture<'static, ReadCompletion>>,
) {
    while let Some(index) = state.take_dirty() {
        let key = selected[index].key.clone();
        let command = command.clone();
        reads.push(Box::pin(read_key(index, key, command)));
    }
}

fn initial_batch(
    state: &mut KnowledgeStateMachine,
    selected: &[SelectedKey],
    values: Vec<Value>,
) -> anyhow::Result<InputBatch<Value>> {
    anyhow::ensure!(
        selected.len() == values.len(),
        "Redis knowledge initial snapshot size does not match selected key count"
    );
    let ticks = values
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            state.seed_initial(index, value.clone());
            vec![InputUpdate::new(selected[index].variable.clone(), value)]
        })
        .collect::<Vec<_>>();
    InputBatch::from_ticks(ticks)
}

fn reconnect_updates(
    state: &mut KnowledgeStateMachine,
    selected: &[SelectedKey],
    values: Vec<Value>,
) -> anyhow::Result<Option<InputBatch<Value>>> {
    anyhow::ensure!(
        selected.len() == values.len(),
        "Redis knowledge reconnect snapshot size does not match selected key count"
    );
    let ticks = values
        .into_iter()
        .enumerate()
        .filter_map(|(index, value)| {
            state
                .observe_snapshot(index, value)
                .map(|value| vec![InputUpdate::new(selected[index].variable.clone(), value)])
        })
        .collect::<Vec<_>>();
    if ticks.is_empty() {
        Ok(None)
    } else {
        InputBatch::from_ticks(ticks).map(Some)
    }
}

fn apply_pending_notifications(tracker: &NotificationDrain, state: &mut KnowledgeStateMachine) {
    for index in tracker.take_pending() {
        state.mark_dirty(index);
    }
}

async fn recover_connection(
    connection: &mut RedisKnowledgeConnection,
    config: &RedisKnowledgeConfig,
    channels: &[String],
    selected: &[SelectedKey],
    tracker: SharedNotificationTracker,
    tasks: NotificationTasks,
    state: &mut KnowledgeStateMachine,
) -> anyhow::Result<(RedisKnowledgeConnection, Option<InputBatch<Value>>)> {
    // Stop and await the old drain before opening a replacement. This makes
    // connection and reconfiguration barriers explicit: an old notification cannot
    // mark the new connection after the fresh snapshot has begun.
    connection.stop_drain().await;
    state.reset_reads_for_reconnect();
    let (new_connection, values) =
        connect_with_snapshot(config, channels.to_vec(), selected.to_vec(), tracker, tasks).await?;
    let updates = reconnect_updates(state, selected, values)?;
    Ok((new_connection, updates))
}

async fn open_value_source(
    config: RedisKnowledgeConfig,
    bindings: BTreeMap<VarName, String>,
    tasks: NotificationTasks,
) -> anyhow::Result<InputStream<Value>> {
    config.validate()?;
    let selected = selected_keys(&config, bindings)?;
    if selected.is_empty() {
        return Ok(Box::pin(futures::stream::empty()));
    }
    let channels = selected
        .iter()
        .map(|selected| selected.channel.clone())
        .collect::<Vec<_>>();
    let tracker = std::sync::Arc::new(std::sync::Mutex::new(NotificationTracker::new(
        selected.len(),
    )));
    let mut state = KnowledgeStateMachine::new(selected.len());
    let (mut connection, initial) = if config.publish_initial {
        connect_with_snapshot(
            &config,
            channels.clone(),
            selected.clone(),
            tracker.clone(),
            tasks.clone(),
        )
        .await
        .context("Redis knowledge source could not acquire its initial snapshot")?
    } else {
        (
            connect_with_retry(&config, channels.clone(), tracker.clone(), tasks.clone())
                .await
                .context("Redis knowledge source could not connect")?,
            Vec::new(),
        )
    };

    let initial_batch = if config.publish_initial {
        Some(initial_batch(&mut state, &selected, initial)?)
    } else {
        None
    };
    let stream = async_stream::stream! {
        if let Some(batch) = initial_batch {
            yield Ok(batch);
        }

        let mut reads: FuturesUnordered<LocalBoxFuture<'static, ReadCompletion>> =
            FuturesUnordered::new();

        loop {
            // Notifications are reduced by the independent drain even while
            // this stream is suspended at the yield above. Applying the
            // bounded bitset here turns them into dirty-key state transitions.
            apply_pending_notifications(&connection.drain, &mut state);
            schedule_reads(&mut state, &connection.command, &selected, &mut reads);

            if connection.drain.is_closed() {
                reads = FuturesUnordered::new();
                match recover_connection(
                    &mut connection,
                    &config,
                    &channels,
                    &selected,
                    tracker.clone(),
                    tasks.clone(),
                    &mut state,
                )
                .await {
                    Ok((new_connection, updates)) => {
                        connection = new_connection;
                        if let Some(batch) = updates {
                            yield Ok(batch);
                        }
                        continue;
                    }
                    Err(error) => {
                        yield Err(error.context("Redis knowledge connection recovery failed"));
                        return;
                    }
                }
            }

            // Drain already-completed GETs before waiting on notification
            // wakeups. This prevents a ready read from being starved by a
            // continuously-ready notification path.
            if let Some(completion) = reads.next().now_or_never().flatten() {
                match state.finish_read(completion.index, completion.result) {
                    ReadCompletionResult::Changed(value) => {
                        yield Ok(InputBatch::update(
                            selected[completion.index].variable.clone(),
                            value,
                        ));
                    }
                    ReadCompletionResult::Unchanged => {}
                    ReadCompletionResult::Failed(ReadFailure::Decode(error)) => {
                        yield Err(error.context(format!(
                            "decoding Redis knowledge notification state for key `{}`",
                            selected[completion.index].key
                        )));
                    }
                    ReadCompletionResult::Failed(ReadFailure::Redis(error))
                        if !retryable_redis_error(&error) =>
                    {
                        // Permanent GET/command errors leave suppression
                        // history untouched and are surfaced through the input
                        // stream. The public InputSource adapter treats that
                        // error as terminal for the configured source.
                        yield Err(anyhow::Error::new(error).context(format!(
                            "Redis knowledge GET for key `{}` in database {} failed",
                            selected[completion.index].key,
                            config.database
                        )));
                    }
                    ReadCompletionResult::Failed(ReadFailure::Redis(error)) => {
                        reads = FuturesUnordered::new();
                        match recover_connection(
                            &mut connection,
                            &config,
                            &channels,
                            &selected,
                            tracker.clone(),
                            tasks.clone(),
                            &mut state,
                        )
                        .await {
                            Ok((new_connection, updates)) => {
                                connection = new_connection;
                                if let Some(batch) = updates {
                                    yield Ok(batch);
                                }
                            }
                            Err(recovery) => {
                                yield Err(anyhow::Error::new(error).context(format!(
                                    "Redis knowledge GET for key `{}` in database {} failed and connection recovery failed: {recovery}",
                                    selected[completion.index].key,
                                    config.database
                                )));
                                return;
                            }
                        }
                    }
                }
                continue;
            }

            if reads.is_empty() {
                connection.drain.wait().await;
                continue;
            }

            // Both sources are polled when work is pending. A ready read is
            // given the explicit fast path above; this select remains fair
            // between new dirty indications and reads that complete later.
            enum NextEvent {
                Wake,
                Read(Option<ReadCompletion>),
            }
            let event = {
                let read_next = reads.next().fuse();
                let wake_next = connection.drain.wait().fuse();
                futures::pin_mut!(read_next, wake_next);
                futures::select! {
                    completion = read_next => NextEvent::Read(completion),
                    _ = wake_next => NextEvent::Wake,
                }
            };
            match event {
                NextEvent::Wake => continue,
                NextEvent::Read(None) => continue,
                NextEvent::Read(Some(completion)) => {
                    match state.finish_read(completion.index, completion.result) {
                        ReadCompletionResult::Changed(value) => {
                            yield Ok(InputBatch::update(
                                selected[completion.index].variable.clone(),
                                value,
                            ));
                        }
                        ReadCompletionResult::Unchanged => {}
                        ReadCompletionResult::Failed(ReadFailure::Decode(error)) => {
                            yield Err(error.context(format!(
                                "decoding Redis knowledge notification state for key `{}`",
                                selected[completion.index].key
                            )));
                        }
                        ReadCompletionResult::Failed(ReadFailure::Redis(error))
                            if !retryable_redis_error(&error) =>
                        {
                            yield Err(anyhow::Error::new(error).context(format!(
                                "Redis knowledge GET for key `{}` in database {} failed",
                                selected[completion.index].key,
                                config.database
                            )));
                        }
                        ReadCompletionResult::Failed(ReadFailure::Redis(error)) => {
                            reads = FuturesUnordered::new();
                            match recover_connection(
                                &mut connection,
                                &config,
                                &channels,
                                &selected,
                                tracker.clone(),
                                tasks.clone(),
                                &mut state,
                            )
                            .await {
                                Ok((new_connection, updates)) => {
                                    connection = new_connection;
                                    if let Some(batch) = updates {
                                        yield Ok(batch);
                                    }
                                }
                                Err(recovery) => {
                                    yield Err(anyhow::Error::new(error).context(format!(
                                        "Redis knowledge GET for key `{}` in database {} failed and connection recovery failed: {recovery}",
                                        selected[completion.index].key,
                                        config.database
                                    )));
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }
    };
    Ok(Box::pin(
        stream.map(|item| item.map_err(crate::InputError::from)),
    ))
}

#[cfg(test)]
mod redis_knowledge_tests {
    use super::*;
    use std::{
        cell::{Cell, RefCell},
        future::Future,
        pin::Pin,
        rc::Rc,
        task::{Context, Poll},
    };

    use futures::future;

    #[test]
    fn owner_shutdown_joins_registered_notification_tasks() {
        smol::block_on(async {
            let (cancel, cancelled) = async_channel::bounded(1);
            let completed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let task_completed = std::sync::Arc::clone(&completed);
            let task = smol::spawn(async move {
                let _ = cancelled.recv().await;
                smol::Timer::after(Duration::from_millis(10)).await;
                task_completed.store(true, std::sync::atomic::Ordering::SeqCst);
            });
            let notification = Rc::new(NotificationTask {
                cancel,
                task: RefCell::new(Some(task)),
            });
            let mut owner = RedisKnowledgeInputControl {
                tasks: Rc::new(RefCell::new(vec![notification])),
            };

            owner.shutdown().await.unwrap();

            assert!(
                completed.load(std::sync::atomic::Ordering::SeqCst),
                "shutdown returned before notification join"
            );
        });
    }

    fn retry_config(max_attempts: Option<NonZeroU32>) -> RetryPolicy {
        RetryPolicy::new(
            max_attempts.map_or(RetryLimit::Unlimited, RetryLimit::Attempts),
            Duration::from_millis(2),
            Duration::from_millis(5),
        )
        .unwrap()
    }

    #[derive(Clone)]
    struct ImmediateTimer {
        delays: Rc<RefCell<Vec<Duration>>>,
    }

    impl KnowledgeTimer for ImmediateTimer {
        fn sleep(&self, duration: Duration) -> LocalBoxFuture<'static, ()> {
            self.delays.borrow_mut().push(duration);
            Box::pin(future::ready(()))
        }
    }

    #[derive(Clone)]
    struct PendingTimer {
        started: Rc<Cell<bool>>,
        dropped: Rc<Cell<bool>>,
    }

    struct PendingSleep {
        started: Rc<Cell<bool>>,
        dropped: Rc<Cell<bool>>,
    }

    impl Future for PendingSleep {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.started.set(true);
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    impl Drop for PendingSleep {
        fn drop(&mut self) {
            self.dropped.set(true);
        }
    }

    impl KnowledgeTimer for PendingTimer {
        fn sleep(&self, _duration: Duration) -> LocalBoxFuture<'static, ()> {
            Box::pin(PendingSleep {
                started: self.started.clone(),
                dropped: self.dropped.clone(),
            })
        }
    }

    fn attempts_error(retryable: bool) -> AttemptError {
        AttemptError {
            error: anyhow!("attempt failed"),
            retryable,
        }
    }

    #[test]
    fn retry_defaults_are_long_lived_and_serde_uses_milliseconds() {
        let retry = RetryPolicy::input_default();
        assert_eq!(retry.limit(), RetryLimit::Unlimited);
        assert_eq!(retry.initial_backoff(), Duration::from_millis(250));
        assert_eq!(retry.max_backoff(), Duration::from_secs(5));

        let decoded: RetryPolicy =
            json5::from_str(r#"{max_attempts:null, initial_delay_ms:250, max_delay_ms:5000}"#)
                .unwrap();
        assert_eq!(decoded, retry);
        let serialized = serde_json::to_value(&retry).unwrap();
        assert_eq!(serialized["initial_delay_ms"], 250);
        assert_eq!(serialized["max_delay_ms"], 5000);
    }

    #[test]
    fn retry_rejects_invalid_delays() {
        for json in [
            r#"{initial_delay_ms:0, max_delay_ms:1}"#,
            r#"{initial_delay_ms:5, max_delay_ms:4}"#,
            r#"{initial_delay_ms:1, max_delay_ms:0}"#,
            r#"{initial_delay_ms:1, max_delay_ms:1, unknown:2}"#,
        ] {
            assert!(json5::from_str::<RetryPolicy>(json).is_err());
        }
    }

    #[test]
    fn config_rejects_empty_and_duplicate_keys() {
        let config = RedisKnowledgeConfig {
            host: "redis".to_owned(),
            port: None,
            database: 2,
            publish_initial: true,
            keys: BTreeMap::from([
                (VarName::new("x"), "same".to_owned()),
                (VarName::new("y"), "same".to_owned()),
            ]),
            retry: RetryPolicy::input_default(),
        };
        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("mapped to both"));

        let config = RedisKnowledgeConfig {
            keys: BTreeMap::from([(VarName::new("x"), "  ".to_owned())]),
            ..config
        };
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("cannot be empty")
        );
    }

    #[test]
    fn decoding_preserves_missing_null_and_plain_text_semantics() {
        assert_eq!(
            decode_redis_knowledge_value("missing", None).unwrap(),
            Value::NoVal
        );
        assert_eq!(
            decode_redis_knowledge_value("null", Some(b"null")).unwrap(),
            Value::Unit
        );
        assert_eq!(
            decode_redis_knowledge_value("json", Some(br#"{"a": 1}"#)).unwrap(),
            Value::Map(BTreeMap::from([("a".into(), Value::Int(1))]))
        );
        assert_eq!(
            decode_redis_knowledge_value("json5", Some(b"{a: 1}")).unwrap(),
            Value::Map(BTreeMap::from([("a".into(), Value::Int(1))]))
        );
        assert_eq!(
            decode_redis_knowledge_value("text", Some(b"plain text")).unwrap(),
            Value::Str("plain text".into())
        );
        let error = decode_redis_knowledge_value("binary:key", Some(&[0xff, 0xfe])).unwrap_err();
        assert!(error.to_string().contains("binary:key"));
    }

    #[test]
    fn equivalent_decoded_values_are_equal() {
        let json = decode_redis_knowledge_value("x", Some(b"{\"a\":1}")).unwrap();
        let json5 = decode_redis_knowledge_value("x", Some(b"{a: 1}")).unwrap();
        assert_eq!(json, json5);
    }

    #[test]
    fn dirty_state_coalesces_notifications_and_emits_only_changes() {
        let mut state = KnowledgeStateMachine::new(2);
        state.mark_dirty(0);
        state.mark_dirty(0);
        assert_eq!(state.take_dirty(), Some(0));
        assert_eq!(state.take_dirty(), None);
        state.mark_dirty(0);
        assert!(matches!(
            state.finish_read(0, Ok(Value::Int(1))),
            ReadCompletionResult::Changed(Value::Int(1))
        ));
        assert_eq!(state.take_dirty(), Some(0));
        assert!(matches!(
            state.finish_read(0, Ok(Value::Int(1))),
            ReadCompletionResult::Unchanged
        ));

        state.mark_dirty(1);
        assert_eq!(state.take_dirty(), Some(1));
        state.mark_dirty(1);
        assert!(matches!(
            state.finish_read(1, Ok(Value::NoVal)),
            ReadCompletionResult::Changed(Value::NoVal)
        ));
        assert_eq!(state.take_dirty(), Some(1));
        assert!(matches!(
            state.finish_read(1, Ok(Value::NoVal)),
            ReadCompletionResult::Unchanged
        ));
    }

    #[test]
    fn notification_during_read_schedules_one_follow_up_and_keys_are_independent() {
        let mut state = KnowledgeStateMachine::new(2);
        state.mark_dirty(0);
        assert_eq!(state.take_dirty(), Some(0));
        state.mark_dirty(0);
        state.mark_dirty(0);
        state.mark_dirty(1);
        assert!(matches!(
            state.finish_read(0, Ok(Value::Int(1))),
            ReadCompletionResult::Changed(_)
        ));
        assert_eq!(state.take_dirty(), Some(0));
        assert_eq!(state.take_dirty(), Some(1));
        assert!(matches!(
            state.finish_read(0, Err(ReadFailure::Decode(anyhow!("bad value")))),
            ReadCompletionResult::Failed(ReadFailure::Decode(_))
        ));
        assert!(matches!(
            state.finish_read(1, Ok(Value::Int(2))),
            ReadCompletionResult::Changed(_)
        ));
    }

    #[test]
    fn failed_reads_do_not_change_suppression_history() {
        let mut state = KnowledgeStateMachine::new(1);
        state.mark_dirty(0);
        assert_eq!(state.take_dirty(), Some(0));
        assert!(matches!(
            state.finish_read(
                0,
                Err(ReadFailure::Redis(redis::RedisError::from((
                    redis::ErrorKind::Io,
                    "read failed"
                )),))
            ),
            ReadCompletionResult::Failed(_)
        ));
        state.mark_dirty(0);
        assert_eq!(state.take_dirty(), Some(0));
        assert!(matches!(
            state.finish_read(0, Ok(Value::Int(3))),
            ReadCompletionResult::Changed(_)
        ));
    }

    #[test]
    fn retry_uses_capped_exponential_backoff_and_finite_attempts() {
        smol::block_on(async {
            let delays = Rc::new(RefCell::new(Vec::new()));
            let timer = ImmediateTimer {
                delays: delays.clone(),
            };
            let mut count = 0;
            let error = retry_operation(&retry_config(NonZeroU32::new(6)), &timer, || {
                count += 1;
                Box::pin(async { Err::<(), _>(attempts_error(true)) })
            })
            .await
            .unwrap_err();
            assert_eq!(count, 6);
            assert_eq!(
                &*delays.borrow(),
                &[
                    Duration::from_millis(2),
                    Duration::from_millis(4),
                    Duration::from_millis(5),
                    Duration::from_millis(5),
                    Duration::from_millis(5),
                ]
            );
            assert_eq!(error.error.to_string(), "attempt failed");
        });
    }

    #[test]
    fn retry_forever_stops_on_success_and_permanent_errors() {
        smol::block_on(async {
            let timer = ImmediateTimer {
                delays: Rc::new(RefCell::new(Vec::new())),
            };
            let mut count = 0;
            let result = retry_operation(&retry_config(None), &timer, || {
                count += 1;
                Box::pin(async move {
                    if count == 3 {
                        Ok::<_, AttemptError>(7)
                    } else {
                        Err(attempts_error(true))
                    }
                })
            })
            .await
            .unwrap();
            assert_eq!(result, 7);
            assert_eq!(count, 3);

            let mut count = 0;
            let result = retry_operation(&retry_config(None), &timer, || {
                count += 1;
                Box::pin(async { Err::<(), _>(attempts_error(false)) })
            })
            .await;
            assert!(result.is_err());
            assert_eq!(count, 1);
        });
    }

    #[test]
    fn notification_tracker_coalesces_bursts_to_one_bit_per_selected_key() {
        let mut tracker = NotificationTracker::new(3);
        for _ in 0..100_000 {
            tracker.mark(0);
        }
        tracker.mark(1);
        tracker.mark(2);
        assert_eq!(tracker.pending_count(), 3);
        assert_eq!(tracker.take_pending(), vec![0, 1, 2]);
        assert_eq!(tracker.pending_count(), 0);
        tracker.mark(1);
        assert_eq!(tracker.take_pending(), vec![1]);
    }

    #[test]
    fn notification_drain_reduces_bursts_without_downstream_polling() {
        smol::block_on(async {
            let tracker = std::sync::Arc::new(std::sync::Mutex::new(NotificationTracker::new(3)));
            let (messages, receiver) = async_channel::bounded(1);
            let (wake, _wake_receiver) = async_channel::bounded(1);
            let (cancel, cancellation) = async_channel::bounded(1);
            let task = smol::spawn(drain_notification_stream(
                receiver,
                |index: &usize| Some(*index),
                tracker.clone(),
                wake,
                cancellation,
            ));

            for _ in 0..100_000 {
                messages.send(0).await.unwrap();
            }
            messages.send(1).await.unwrap();
            messages.send(2).await.unwrap();
            messages.close();
            task.await;

            let mut tracker = tracker.lock().unwrap();
            assert_eq!(tracker.pending_count(), 3);
            assert_eq!(tracker.take_pending(), vec![0, 1, 2]);
            assert!(tracker.closed);
            drop(cancel);
        });
    }

    #[test]
    fn notification_drain_cancellation_stops_cancelled_source_processing() {
        smol::block_on(async {
            let tracker = std::sync::Arc::new(std::sync::Mutex::new(NotificationTracker::new(2)));
            let (messages, receiver) = async_channel::bounded(1);
            let retained_receiver = receiver.clone();
            let (wake, wake_receiver) = async_channel::bounded(1);
            let (cancel, cancellation) = async_channel::bounded(1);
            let task = smol::spawn(drain_notification_stream(
                receiver,
                |index: &usize| Some(*index),
                tracker.clone(),
                wake,
                cancellation,
            ));

            messages.send(0).await.unwrap();
            wake_receiver.recv().await.unwrap();
            assert_eq!(tracker.lock().unwrap().take_pending(), vec![0]);

            cancel.send(()).await.unwrap();
            task.await;

            messages.send(1).await.unwrap();
            smol::future::yield_now().await;
            assert_eq!(tracker.lock().unwrap().pending_count(), 0);
            drop(retained_receiver);
        });
    }

    #[test]
    fn completed_read_is_processed_when_notification_wakeup_is_ready() {
        smol::block_on(async {
            let mut state = KnowledgeStateMachine::new(2);
            state.mark_dirty(0);
            assert_eq!(state.take_dirty(), Some(0));

            let mut reads: FuturesUnordered<LocalBoxFuture<'static, ReadCompletion>> =
                FuturesUnordered::new();
            reads.push(Box::pin(async {
                ReadCompletion {
                    index: 0,
                    result: Ok(Value::Int(7)),
                }
            }));
            let tracker = std::sync::Arc::new(std::sync::Mutex::new(NotificationTracker::new(2)));
            tracker.lock().unwrap().mark(1);

            // This is the same ready-read fast path used by the transport
            // loop. A ready wakeup cannot prevent the completed GET from
            // being consumed.
            let completion = reads.next().now_or_never().flatten().unwrap();
            assert!(matches!(
                state.finish_read(completion.index, completion.result),
                ReadCompletionResult::Changed(Value::Int(7))
            ));
            for index in tracker.lock().unwrap().take_pending() {
                state.mark_dirty(index);
            }
            assert_eq!(state.take_dirty(), Some(1));
        });
    }

    #[test]
    fn redis_error_retry_classifier_allows_only_transient_transport_states() {
        let io = redis::RedisError::from((redis::ErrorKind::Io, "connection reset"));
        assert!(retryable_redis_error(&io));

        for kind in [
            redis::ServerErrorKind::TryAgain,
            redis::ServerErrorKind::BusyLoading,
            redis::ServerErrorKind::ClusterDown,
            redis::ServerErrorKind::MasterDown,
        ] {
            let error = redis::RedisError::from((redis::ErrorKind::Server(kind), "transient"));
            assert!(retryable_redis_error(&error));
        }

        for error in [
            redis::RedisError::from((
                redis::ErrorKind::AuthenticationFailed,
                "authentication rejected",
            )),
            redis::RedisError::from((
                redis::ErrorKind::Server(redis::ServerErrorKind::NoPerm),
                "NOPERM",
            )),
            redis::RedisError::from((
                redis::ErrorKind::Server(redis::ServerErrorKind::ResponseError),
                "ERR invalid DB index",
            )),
            redis::RedisError::from((
                redis::ErrorKind::Server(redis::ServerErrorKind::ResponseError),
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
        ] {
            assert!(!retryable_redis_error(&error));
        }
    }

    #[test]
    fn permanent_errors_stop_retrying_even_when_retry_is_forever() {
        smol::block_on(async {
            let timer = ImmediateTimer {
                delays: Rc::new(RefCell::new(Vec::new())),
            };
            let mut attempts = 0;
            let error = retry_operation(&retry_config(None), &timer, || {
                attempts += 1;
                Box::pin(async {
                    Err::<(), _>(AttemptError::redis(
                        "GET for key `wrong:type`",
                        redis::RedisError::from((
                            redis::ErrorKind::Server(redis::ServerErrorKind::ResponseError),
                            "WRONGTYPE",
                        )),
                    ))
                })
            })
            .await
            .unwrap_err();
            assert_eq!(attempts, 1);
            assert!(!error.retryable);
            assert!(error.error.to_string().contains("wrong:type"));
        });
    }

    #[test]
    fn reconnect_snapshots_preserve_history_and_emit_missing_transitions() {
        let selected = vec![
            SelectedKey {
                variable: VarName::new("mode"),
                key: "mode".to_owned(),
                channel: redis_keyspace_channel(2, "mode"),
            },
            SelectedKey {
                variable: VarName::new("plan"),
                key: "plan".to_owned(),
                channel: redis_keyspace_channel(2, "plan"),
            },
        ];
        let mut state = KnowledgeStateMachine::new(2);
        state.seed_initial(0, Value::Int(1));
        state.seed_initial(1, Value::NoVal);

        assert!(
            reconnect_updates(&mut state, &selected, vec![Value::Int(1), Value::NoVal])
                .unwrap()
                .is_none()
        );
        let changed = reconnect_updates(&mut state, &selected, vec![Value::NoVal, Value::Int(2)])
            .unwrap()
            .unwrap();
        assert_eq!(changed.tick_count(), 2);
        assert!(changed.ticks().all(|tick| tick.len() == 1));
        let values = changed
            .updates()
            .map(|update| update.value.clone())
            .collect::<Vec<_>>();
        assert_eq!(values, vec![Value::NoVal, Value::Int(2)]);
        assert!(
            reconnect_updates(&mut state, &selected, vec![Value::NoVal, Value::Int(2)])
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn retry_cancellation_drops_a_pending_backoff() {
        smol::block_on(async {
            let started = Rc::new(Cell::new(false));
            let dropped = Rc::new(Cell::new(false));
            let timer = PendingTimer {
                started: started.clone(),
                dropped: dropped.clone(),
            };
            let retry_settings = retry_config(None);
            {
                let retry = retry_operation(&retry_settings, &timer, || {
                    Box::pin(async { Err::<(), _>(attempts_error(true)) })
                });
                let cancelled = future::poll_fn(|cx| {
                    if started.get() {
                        Poll::Ready(())
                    } else {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                });
                let retry = retry.fuse();
                let cancelled = cancelled.fuse();
                futures::pin_mut!(retry, cancelled);
                futures::select_biased! {
                    _ = cancelled => {}
                    _ = retry => panic!("retry should be cancelled during backoff")
                }
            }
            assert!(started.get());
            assert!(dropped.get());
        });
    }

    #[test]
    fn notifications_during_reconnect_snapshot_remain_dirty_for_follow_up_get() {
        let tracker = std::sync::Arc::new(std::sync::Mutex::new(NotificationTracker::new(1)));
        tracker.lock().unwrap().mark(0);
        let selected = [SelectedKey {
            variable: VarName::new("x"),
            key: "x".to_owned(),
            channel: redis_keyspace_channel(2, "x"),
        }];
        let mut state = KnowledgeStateMachine::new(1);
        state.seed_initial(0, Value::Int(1));
        let _ = reconnect_updates(&mut state, &selected, vec![Value::Int(1)]).unwrap();
        for index in tracker.lock().unwrap().take_pending() {
            state.mark_dirty(index);
        }
        assert_eq!(state.take_dirty(), Some(0));
    }

    #[test]
    fn keyspace_channels_are_exact_and_database_scoped() {
        assert_eq!(
            redis_keyspace_channel(2, "robot:mode"),
            "__keyspace@2__:robot:mode"
        );
        assert_ne!(
            redis_keyspace_channel(1, "robot:mode"),
            redis_keyspace_channel(2, "robot:mode")
        );
    }
}
