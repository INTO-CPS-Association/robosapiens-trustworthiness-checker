use std::{
    cell::{Cell, RefCell},
    collections::BTreeSet,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

use futures::{FutureExt, Stream, StreamExt};
use smol::{LocalExecutor, Task};

use crate::core::InputBatch;
use crate::io::aggregation::{
    InputTimer, RealTimeInputTimer, WindowEvent, WindowEventStream, drive_window,
};
use crate::io::builders::{InputPipeline, InputPipelineReconfigurationPlan};
use crate::io::config::{
    InputPolicy, ReconfigurationRequest, ResolvedInput, ResolvedSource, SourceId,
};
use crate::io::{SessionId, SessionRevision, ShutdownDeadline};

/// A typed item at the reconfigurable orchestration boundary. Ordinary input
/// streams contain only `InputBatch` values; reconfigurable streams also carry
/// reusable `Reconfigure(ReconfigurationRequest)` control barriers.
#[derive(Debug)]
// ANCHOR: reconfigurable_input_item
pub(crate) enum ReconfigurableInputItem<V> {
    Data(InputBatch<V>),
    Reconfigure(ReconfigurationRequest),
    Boundary(u64),
}

#[derive(Clone, Copy)]
enum RelayCommand {
    Boundary(u64),
    Resume,
}
// ANCHOR_END: reconfigurable_input_item

pub(crate) type ReconfigurableInputStream<V> =
    crate::LocalStream<anyhow::Result<ReconfigurableInputItem<V>>>;

pub(crate) struct OpenedInputSource<V> {
    pub(crate) id: SourceId,
    stream: ReconfigurableInputStream<V>,
    ingress: SourceIngress,
    control: Option<InputSourceControl>,
}

pub(crate) enum InputSourceControl {
    Rumqttc(crate::io::mqtt::RumqttcInputControl),
    #[cfg(feature = "redis")]
    Redis(crate::io::redis::RedisInputControl),
    #[cfg(feature = "redis")]
    RedisKnowledge(crate::io::redis::RedisKnowledgeInputControl),
    #[cfg(feature = "ros")]
    Ros {
        controls: Vec<crate::io::ros::RosInputControl>,
        active_topics: std::collections::BTreeMap<crate::VarName, String>,
    },
    Channel(crate::io::channel::ReconfigurableChannelControl),
    #[cfg(test)]
    DelayedTest {
        delay: std::time::Duration,
        completed: Rc<std::cell::Cell<bool>>,
    },
}

impl InputSourceControl {
    async fn pause(&mut self, boundary: u64) -> anyhow::Result<bool> {
        match self {
            Self::Rumqttc(control) => {
                control.pause(boundary).await?;
                Ok(true)
            }
            #[cfg(feature = "redis")]
            Self::Redis(control) => {
                control.pause(boundary).await?;
                Ok(true)
            }
            #[cfg(feature = "redis")]
            Self::RedisKnowledge(_) => Ok(false),
            #[cfg(feature = "ros")]
            Self::Ros { controls, .. } => {
                let mut native = false;
                for control in controls {
                    native |= control.pause(boundary).await?;
                }
                Ok(native)
            }
            Self::Channel(control) => {
                control.pause(boundary).await?;
                Ok(true)
            }
            #[cfg(test)]
            Self::DelayedTest { .. } => Ok(false),
        }
    }

    async fn resume(&mut self) -> anyhow::Result<()> {
        match self {
            Self::Rumqttc(control) => control.resume().await,
            #[cfg(feature = "redis")]
            Self::Redis(control) => control.resume().await,
            #[cfg(feature = "redis")]
            Self::RedisKnowledge(_) => Ok(()),
            #[cfg(feature = "ros")]
            Self::Ros { controls, .. } => {
                for control in controls {
                    control.resume().await?;
                }
                Ok(())
            }
            Self::Channel(control) => control.resume(),
            #[cfg(test)]
            Self::DelayedTest { .. } => Ok(()),
        }
    }

    async fn rebind(&mut self, candidate: &ResolvedSource) -> anyhow::Result<()> {
        let topics = candidate
            .bindings()
            .iter()
            .map(|binding| {
                (
                    binding.variable().clone(),
                    binding.route().address().to_owned(),
                )
            })
            .collect();
        match self {
            Self::Rumqttc(control) => control.rebind(topics).await,
            #[cfg(feature = "redis")]
            Self::Redis(control) => control.rebind(topics).await,
            #[cfg(feature = "redis")]
            Self::RedisKnowledge(_) => {
                anyhow::bail!("Redis knowledge input does not support in-place rebind")
            }
            #[cfg(feature = "ros")]
            Self::Ros {
                controls,
                active_topics,
            } => {
                let mapping: std::collections::BTreeMap<String, (String, String)> = candidate
                    .bindings()
                    .iter()
                    .map(|binding| {
                        let format = binding.route().format().ok_or_else(|| {
                            anyhow::anyhow!(
                                "ROS route for `{}` requires a route format",
                                binding.variable()
                            )
                        })?;
                        Ok((
                            binding.variable().to_string(),
                            (binding.route().address().to_owned(), format.to_string()),
                        ))
                    })
                    .collect::<anyhow::Result<_>>()?;
                let mut rebound = false;
                for control in controls {
                    if control.supports_reconfiguration() {
                        control.rebind(mapping.clone()).await?;
                        rebound = true;
                    }
                }
                anyhow::ensure!(rebound, "ROS input owner does not support in-place rebind");
                *active_topics = topics;
                Ok(())
            }
            Self::Channel(control) => {
                control
                    .rebind(
                        candidate
                            .bindings()
                            .iter()
                            .map(|binding| binding.variable().clone())
                            .collect(),
                    )
                    .await
            }
            #[cfg(test)]
            Self::DelayedTest { .. } => {
                anyhow::bail!("delayed test input owner does not support rebind")
            }
        }
    }

    async fn shutdown(&mut self) -> anyhow::Result<()> {
        match self {
            Self::Rumqttc(control) => control.shutdown().await,
            #[cfg(feature = "redis")]
            Self::Redis(control) => control.shutdown().await,
            #[cfg(feature = "redis")]
            Self::RedisKnowledge(control) => control.shutdown().await,
            #[cfg(feature = "ros")]
            Self::Ros { controls, .. } => {
                let mut errors = Vec::new();
                for control in controls {
                    if let Err(error) = control.shutdown().await {
                        errors.push(error);
                    }
                }
                match errors.len() {
                    0 => Ok(()),
                    1 => Err(errors.pop().unwrap()),
                    _ => Err(errors.remove(0).context(format!(
                        "{} additional ROS input cleanup operation(s) failed",
                        errors.len()
                    ))),
                }
            }
            Self::Channel(control) => {
                control.shutdown().await;
                Ok(())
            }
            #[cfg(test)]
            Self::DelayedTest { delay, completed } => {
                smol::Timer::after(*delay).await;
                completed.set(true);
                Ok(())
            }
        }
    }
}

struct SourceIngress {
    cancellation: crate::utils::cancellation_token::CancellationToken,
    task: Option<Task<()>>,
    cleanup: Option<Task<anyhow::Result<()>>>,
    executor: Option<Rc<LocalExecutor<'static>>>,
    commands: Option<async_unsync::bounded::Sender<RelayCommand>>,
}

impl SourceIngress {
    fn stop(&mut self, mut control: Option<InputSourceControl>) {
        if self.task.is_none() && self.executor.is_none() {
            return;
        }
        self.cancellation.cancel();
        if self.cleanup.is_some() {
            return;
        }
        let task = self.task.take();
        let Some(executor) = self.executor.as_ref().map(Rc::clone) else {
            return;
        };
        self.cleanup = Some(executor.spawn(async move {
            if let Some(task) = task {
                task.await;
            }
            if let Some(control) = control.as_mut() {
                control.shutdown().await?;
            }
            Ok(())
        }));
    }
}

impl Drop for SourceIngress {
    fn drop(&mut self) {
        self.cancellation.cancel();
        drop(self.task.take());
    }
}

impl<V: 'static> OpenedInputSource<V> {
    /// Keep a finite, poll-driven source on the direct path. Such a source has
    /// no ingress task to stop; graceful drain simply polls it to EOF.
    pub(crate) fn direct(id: SourceId, stream: ReconfigurableInputStream<V>) -> Self {
        Self {
            id,
            stream: Box::pin(stream.fuse()),
            ingress: SourceIngress {
                cancellation: crate::utils::cancellation_token::CancellationToken::new(),
                task: None,
                cleanup: None,
                executor: None,
                commands: None,
            },
            control: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn relay(
        id: SourceId,
        source: ReconfigurableInputStream<V>,
        executor: Rc<LocalExecutor<'static>>,
    ) -> Self {
        Self::relay_with_control(id, source, executor, None)
    }

    pub(crate) fn relay_with_control(
        id: SourceId,
        mut source: ReconfigurableInputStream<V>,
        executor: Rc<LocalExecutor<'static>>,
        control: Option<InputSourceControl>,
    ) -> Self {
        // One queued item plus one item held by the relay preserves normal
        // backpressure while giving source removal a concrete local boundary.
        let (sender, receiver) = async_unsync::bounded::channel(1).into_split();
        let (commands, mut command_receiver) = async_unsync::bounded::channel(1).into_split();
        let cancellation = crate::utils::cancellation_token::CancellationToken::new();
        let worker_cancellation = cancellation.clone();
        let task = executor.spawn(async move {
            'outer: loop {
                enum Next<T> {
                    Command(RelayCommand),
                    Item(Option<T>),
                    Stop,
                }
                let next = futures::select_biased! {
                    _ = worker_cancellation.cancelled().fuse() => break,
                    command = command_receiver.recv().fuse() => match command {
                        Some(command) => Next::Command(command),
                        None => Next::Stop,
                    },
                    item = source.next().fuse() => Next::Item(item),
                };
                match next {
                    Next::Command(RelayCommand::Resume) => {}
                    Next::Command(RelayCommand::Boundary(boundary)) => {
                        if sender
                            .send(Ok(ReconfigurableInputItem::Boundary(boundary)))
                            .await
                            .is_err()
                        {
                            break;
                        }
                        loop {
                            let command = futures::select_biased! {
                                _ = worker_cancellation.cancelled().fuse() => break 'outer,
                                command = command_receiver.recv().fuse() => command,
                            };
                            match command {
                                Some(RelayCommand::Resume) => break,
                                Some(RelayCommand::Boundary(_)) => continue,
                                None => break 'outer,
                            }
                        }
                    }
                    Next::Item(Some(item)) => {
                        let native_boundary =
                            matches!(&item, Ok(ReconfigurableInputItem::Boundary(_)));
                        if sender.send(item).await.is_err() {
                            break;
                        }
                        if native_boundary {
                            loop {
                                let command = futures::select_biased! {
                                    _ = worker_cancellation.cancelled().fuse() => break 'outer,
                                    command = command_receiver.recv().fuse() => command,
                                };
                                match command {
                                    Some(RelayCommand::Resume) => break,
                                    Some(RelayCommand::Boundary(_)) => continue,
                                    None => break 'outer,
                                }
                            }
                        }
                    }
                    Next::Item(None) | Next::Stop => break,
                }
            }
        });
        let stream = futures::stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        });
        Self {
            id,
            stream: Box::pin(stream.fuse()),
            ingress: SourceIngress {
                cancellation,
                task: Some(task),
                cleanup: None,
                executor: Some(executor),
                commands: Some(commands),
            },
            control,
        }
    }

    fn stop_ingress(&mut self) {
        self.ingress.stop(self.control.take());
    }

    pub(crate) async fn close(mut self) -> Vec<anyhow::Error> {
        self.stop_ingress();
        drain_source(&mut self).await
    }

    async fn rebind(&mut self, candidate: &ResolvedSource) -> anyhow::Result<()> {
        let control = self.control.as_mut().ok_or_else(|| {
            anyhow::anyhow!(
                "input source `{}` does not support in-place rebind",
                self.id
            )
        })?;
        control.rebind(candidate).await
    }
}

impl<V: 'static> Stream for OpenedInputSource<V> {
    type Item = anyhow::Result<ReconfigurableInputItem<V>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.ingress.task.is_none()
            && self.ingress.cleanup.is_none()
            && self.ingress.cancellation.is_cancelled_now()
        {
            return Poll::Ready(None);
        }
        match self.stream.as_mut().poll_next(cx) {
            // Direct finite sources have no ingress task or cleanup owner.
            // Their EOF is final; scheduling worker cleanup would wake forever.
            Poll::Ready(None) if self.ingress.executor.is_none() => Poll::Ready(None),
            Poll::Ready(None) if self.ingress.cleanup.is_some() => {
                let cleanup = self.ingress.cleanup.as_mut().unwrap();
                match Pin::new(cleanup).poll(cx) {
                    Poll::Ready(Ok(())) => {
                        self.ingress.cleanup = None;
                        Poll::Ready(None)
                    }
                    Poll::Ready(Err(error)) => {
                        self.ingress.cleanup = None;
                        Poll::Ready(Some(Err(error)))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
            Poll::Ready(None)
                if self.ingress.task.is_none() && self.ingress.cancellation.is_cancelled_now() =>
            {
                Poll::Ready(None)
            }
            Poll::Ready(None) => {
                self.stop_ingress();
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            other => other,
        }
    }
}

pub(crate) struct InputSourceSet<V> {
    sources: Vec<OpenedInputSource<V>>,
    next: usize,
    boundary: Option<(u64, BTreeSet<SourceId>)>,
}

impl<V: 'static> InputSourceSet<V> {
    pub(crate) fn new(sources: Vec<OpenedInputSource<V>>) -> Self {
        Self {
            sources,
            next: 0,
            boundary: None,
        }
    }

    async fn begin_boundary(
        &mut self,
        id: u64,
    ) -> anyhow::Result<Vec<async_unsync::bounded::Sender<RelayCommand>>> {
        anyhow::ensure!(
            self.boundary.is_none(),
            "an input boundary is already active"
        );
        let mut pending = BTreeSet::new();
        let mut commands = Vec::with_capacity(self.sources.len());
        for source in &mut self.sources {
            if let Some(sender) = source.ingress.commands.clone() {
                pending.insert(source.id.clone());
                let native = match source.control.as_mut() {
                    Some(control) => control.pause(id).await?,
                    None => false,
                };
                if !native {
                    commands.push(sender);
                }
            }
        }
        self.boundary = Some((id, pending));
        Ok(commands)
    }

    async fn resume_all(
        &mut self,
    ) -> anyhow::Result<Vec<async_unsync::bounded::Sender<RelayCommand>>> {
        let mut commands = Vec::with_capacity(self.sources.len());
        for source in &mut self.sources {
            if let Some(control) = source.control.as_mut() {
                control.resume().await?;
            }
            if let Some(command) = source.ingress.commands.clone() {
                commands.push(command);
            }
        }
        Ok(commands)
    }

    fn insert(&mut self, source: OpenedInputSource<V>) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.sources.iter().all(|active| active.id != source.id),
            "input source `{}` is already active",
            source.id
        );
        self.sources.push(source);
        Ok(())
    }

    fn detach(&mut self, removed: &BTreeSet<SourceId>) -> Vec<OpenedInputSource<V>> {
        let mut retained = Vec::with_capacity(self.sources.len());
        let mut draining = Vec::new();
        for source in self.sources.drain(..) {
            if removed.contains(&source.id) {
                draining.push(source);
            } else {
                retained.push(source);
            }
        }
        self.sources = retained;
        self.next = self.next.min(self.sources.len().saturating_sub(1));
        draining
    }

    fn contains(&self, source: &SourceId) -> bool {
        self.sources.iter().any(|active| &active.id == source)
    }

    pub(crate) fn stop_all(&mut self) {
        for source in &mut self.sources {
            source.stop_ingress();
        }
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<anyhow::Result<ReconfigurableInputItem<V>>>> {
        if self
            .boundary
            .as_ref()
            .is_some_and(|(_, pending)| pending.is_empty())
        {
            let (id, _) = self.boundary.take().unwrap();
            return Poll::Ready(Some(Ok(ReconfigurableInputItem::Boundary(id))));
        }
        if self.sources.is_empty() {
            return Poll::Ready(None);
        }
        let mut checked = 0;
        while checked < self.sources.len() {
            let index = self.next % self.sources.len();
            self.next = (index + 1) % self.sources.len();
            match Pin::new(&mut self.sources[index]).poll_next(cx) {
                Poll::Ready(Some(Ok(ReconfigurableInputItem::Boundary(id)))) => {
                    let source = self.sources[index].id.clone();
                    let Some((active, pending)) = &mut self.boundary else {
                        return Poll::Ready(Some(Err(anyhow::anyhow!(
                            "unexpected input boundary"
                        ))));
                    };
                    if *active != id || !pending.remove(&source) {
                        return Poll::Ready(Some(Err(anyhow::anyhow!(
                            "invalid input boundary marker"
                        ))));
                    }
                    if pending.is_empty() {
                        self.boundary = None;
                        return Poll::Ready(Some(Ok(ReconfigurableInputItem::Boundary(id))));
                    }
                    checked += 1;
                }
                Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
                Poll::Ready(None) => {
                    let source = self.sources[index].id.clone();
                    let completed_boundary = if let Some((id, pending)) = &mut self.boundary {
                        pending.remove(&source);
                        pending.is_empty().then_some(*id)
                    } else {
                        None
                    };
                    self.sources.remove(index);
                    if let Some(id) = completed_boundary {
                        self.boundary = None;
                        return Poll::Ready(Some(Ok(ReconfigurableInputItem::Boundary(id))));
                    }
                    if self.sources.is_empty() {
                        return Poll::Ready(None);
                    }
                    self.next %= self.sources.len();
                }
                Poll::Pending => checked += 1,
            }
        }
        Poll::Pending
    }
}

impl<V: 'static> Stream for InputSourceSet<V> {
    type Item = anyhow::Result<ReconfigurableInputItem<V>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        InputSourceSet::poll_next(&mut self, cx)
    }
}

pub(crate) struct SharedInputSourceSet<V>(pub(crate) Rc<RefCell<InputSourceSet<V>>>);

impl<V: 'static> Stream for SharedInputSourceSet<V> {
    type Item = anyhow::Result<ReconfigurableInputItem<V>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.borrow_mut().poll_next(cx)
    }
}

/// Consuming graceful-shutdown stream for an input session.
pub(crate) struct InputSessionDrain<V> {
    stream: ReconfigurableInputStream<V>,
}

impl<V: 'static> Stream for InputSessionDrain<V> {
    type Item = anyhow::Result<ReconfigurableInputItem<V>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}

async fn drain_source<V: 'static>(source: &mut OpenedInputSource<V>) -> Vec<anyhow::Error> {
    let mut errors = Vec::new();
    while let Some(item) = source.next().await {
        if let Err(error) = item {
            errors.push(error);
        }
    }
    errors
}

async fn drain_sources<V: 'static>(sources: &mut [OpenedInputSource<V>]) -> Vec<anyhow::Error> {
    let mut errors = Vec::new();
    for source in sources {
        errors.extend(drain_source(source).await);
    }
    errors
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReconfigurationControl {
    pub source: SourceId,
    pub route: Box<str>,
}

impl ReconfigurationControl {
    pub fn new(source: impl Into<SourceId>, route: impl Into<Box<str>>) -> anyhow::Result<Self> {
        let source = source.into();
        let route = route.into();
        anyhow::ensure!(
            !source.trim().is_empty(),
            "reconfiguration source cannot be empty"
        );
        anyhow::ensure!(
            !route.trim().is_empty(),
            "reconfiguration route cannot be empty"
        );
        Ok(Self { source, route })
    }
}

/// Input adapter shared by the reconfigurable runtimes.
/// It owns the reusable pipeline and validated control binding, but opens no
/// source resources until an open method is called. Each opened stream yields
/// typed data batches or a parsed `ReconfigurationRequest` control item.
#[derive(Clone, Debug)]
pub(crate) struct ReconfigurableInput<V = crate::Value> {
    pipeline: InputPipeline<V>,
    control: ReconfigurationControl,
    executor: Rc<LocalExecutor<'static>>,
}

impl<V: Clone> ReconfigurableInput<V> {
    pub fn new(
        pipeline: InputPipeline<V>,
        requested_route: Option<String>,
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<Self> {
        pipeline.ensure_reconfigurable(requested_route.as_deref())?;
        let resolved = pipeline
            .sources()
            .resolve_reconfiguration_source(requested_route.as_deref())?;
        anyhow::ensure!(
            resolved.source().supports_reconfiguration(),
            "input source `{}` does not support reconfiguration",
            resolved.source_id()
        );
        let control = ReconfigurationControl::new(
            resolved.source_id().clone(),
            resolved.route().to_owned().into_boxed_str(),
        )?;
        Ok(Self {
            pipeline,
            control,
            executor,
        })
    }

    pub fn pipeline(&self) -> &InputPipeline<V> {
        &self.pipeline
    }

    pub async fn open_session(
        &self,
        resolved: ResolvedInput,
    ) -> anyhow::Result<InputPipelineSession<V>>
    where
        V: crate::core::FileInputValue + crate::core::RosStreamValue,
    {
        let opened = self
            .pipeline
            .open_reconfigurable_sources(&resolved, &self.control, Rc::clone(&self.executor))
            .await?;
        let sources = Rc::new(RefCell::new(InputSourceSet::new(opened)));
        let raw: ReconfigurableInputStream<V> = Box::pin(SharedInputSourceSet(Rc::clone(&sources)));
        let stream = apply_barrier_policy(raw, self.pipeline.policy())?;
        Ok(InputPipelineSession {
            pipeline: self.pipeline.clone(),
            control: self.control.clone(),
            active: resolved,
            sources,
            stream,
            executor: Rc::clone(&self.executor),
            session: SessionId::new(),
            revision: SessionRevision::initial(),
            pending_revision: None,
        })
    }
}

/// A live input boundary owns the active plan and control-aware stream.
pub(crate) struct InputPipelineSession<V = crate::Value> {
    pipeline: InputPipeline<V>,
    control: ReconfigurationControl,
    active: ResolvedInput,
    sources: Rc<RefCell<InputSourceSet<V>>>,
    stream: ReconfigurableInputStream<V>,
    executor: Rc<LocalExecutor<'static>>,
    session: SessionId,
    revision: SessionRevision,
    pending_revision: Option<SessionRevision>,
}

impl<V> Unpin for InputPipelineSession<V> {}

impl<V> InputPipelineSession<V>
where
    V: crate::core::FileInputValue + crate::core::RosStreamValue,
{
    #[cfg(test)]
    pub(crate) fn add_delayed_cleanup_for_test(
        &mut self,
        delay: std::time::Duration,
        completed: Rc<Cell<bool>>,
    ) {
        let source = OpenedInputSource::relay_with_control(
            SourceId::new(),
            Box::pin(futures::stream::pending()),
            Rc::clone(&self.executor),
            Some(InputSourceControl::DelayedTest { delay, completed }),
        );
        self.sources
            .borrow_mut()
            .insert(source)
            .expect("test cleanup source ID must be unique");
    }

    pub(crate) fn active(&self) -> &ResolvedInput {
        &self.active
    }

    pub(crate) fn session_id(&self) -> SessionId {
        self.session
    }

    pub(crate) fn revision(&self) -> SessionRevision {
        self.revision
    }

    pub(crate) fn commit_revision(&mut self, expected: SessionRevision) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.revision == expected && self.pending_revision == Some(expected),
            "input session revision changed before coordinated commit"
        );
        self.revision = self.revision.next();
        self.pending_revision = None;
        Ok(())
    }

    fn validate_plan(&self, plan: &InputPipelineReconfigurationPlan) -> anyhow::Result<()> {
        anyhow::ensure!(
            plan.session() == self.session
                && plan.expected_revision() == self.revision
                && self.pending_revision.is_none(),
            "stale input pipeline reconfiguration plan does not match the active session revision"
        );
        Ok(())
    }

    async fn stop_and_drain(&mut self, deadline: ShutdownDeadline) -> Vec<anyhow::Error> {
        self.sources.borrow_mut().stop_all();
        let mut cleanup = Vec::new();
        loop {
            match deadline.timeout(self.stream.next()).await {
                Ok(Some(Err(error))) => cleanup.push(error),
                Ok(Some(Ok(_))) => {}
                Ok(None) => break,
                Err(timeout) => {
                    cleanup.push(anyhow::Error::new(timeout));
                    break;
                }
            }
        }
        cleanup
    }

    async fn fail_after_cleanup(
        &mut self,
        primary: anyhow::Error,
        shutdown_timeout: Option<std::time::Duration>,
        failure_deadline: &Rc<Cell<Option<ShutdownDeadline>>>,
    ) -> anyhow::Error {
        let deadline = failure_deadline.get().unwrap_or_else(|| {
            let deadline =
                shutdown_timeout.map_or_else(ShutdownDeadline::none, ShutdownDeadline::after);
            failure_deadline.set(Some(deadline));
            deadline
        });
        self.stop_and_drain(deadline)
            .await
            .into_iter()
            .fold(primary, |error, cleanup| {
                error.context(format!("input cleanup also failed: {cleanup:#}"))
            })
    }

    /// Consume the owner and drain every locally admitted item through the
    /// active input window. EOF is observed only after all source relays have
    /// stopped and their admitted items have been emitted.
    #[cfg(test)]
    pub(crate) fn into_drain(self) -> InputSessionDrain<V> {
        self.into_drain_with_deadline(ShutdownDeadline::none())
    }

    pub(crate) fn into_drain_with_deadline(
        self,
        deadline: ShutdownDeadline,
    ) -> InputSessionDrain<V> {
        self.sources.borrow_mut().stop_all();
        let mut stream = self.stream;
        InputSessionDrain {
            stream: Box::pin(async_stream::stream! {
                loop {
                    match deadline.timeout(stream.next()).await {
                        Ok(Some(item)) => yield item,
                        Ok(None) => return,
                        Err(error) => {
                            yield Err(anyhow::Error::new(error));
                            return;
                        }
                    }
                }
            }),
        }
    }

    /// Apply a prepared transition while the caller processes old-binding
    /// observations sequentially.
    ///
    /// The session is consumed so failure cannot return a partially changed
    /// owner. Unchanged relays remain live with bounded backpressure. A rumqttc
    /// control source changes subscriptions in place at its protocol barrier;
    /// transports without an in-place owner are replaced only when their
    /// source ID is removed and a distinct source is added.
    pub(crate) async fn rebind<F>(
        mut self,
        plan: InputPipelineReconfigurationPlan,
        shutdown_timeout: Option<std::time::Duration>,
        failure_deadline: Rc<Cell<Option<ShutdownDeadline>>>,
        mut process_old: F,
    ) -> anyhow::Result<Self>
    where
        F: std::ops::AsyncFnMut(InputBatch<V>) -> anyhow::Result<()>,
    {
        if let Err(error) = self.validate_plan(&plan) {
            return Err(self
                .fail_after_cleanup(error, shutdown_timeout, &failure_deadline)
                .await);
        }
        let boundary = self.revision.next().get();
        let boundary_result = {
            let mut sources = self.sources.borrow_mut();
            sources.begin_boundary(boundary).await
        };
        let commands = match boundary_result {
            Ok(commands) => commands,
            Err(error) => {
                return Err(self
                    .fail_after_cleanup(
                        error.context("input source pause failed; session stopped"),
                        shutdown_timeout,
                        &failure_deadline,
                    )
                    .await);
            }
        };
        for command in commands {
            // A failed command send means the relay has already reached its
            // local admission boundary. Keep it in the pending set: polling
            // the shared data channel drains any item admitted before relay
            // termination, then EOF completes this source's boundary.
            let _ = command.send(RelayCommand::Boundary(boundary)).await;
        }

        loop {
            match self.stream.next().await {
                Some(Ok(ReconfigurableInputItem::Data(batch))) => {
                    if let Err(error) = process_old(batch).await {
                        let error =
                            error.context("old-binding callback failed; input session stopped");
                        return Err(self
                            .fail_after_cleanup(error, shutdown_timeout, &failure_deadline)
                            .await);
                    }
                }
                Some(Ok(ReconfigurableInputItem::Boundary(id))) if id == boundary => break,
                Some(Ok(ReconfigurableInputItem::Boundary(_))) => {
                    let error = anyhow::anyhow!("unexpected input boundary during rebind");
                    return Err(self
                        .fail_after_cleanup(error, shutdown_timeout, &failure_deadline)
                        .await);
                }
                Some(Ok(ReconfigurableInputItem::Reconfigure(_))) => {
                    let error = anyhow::anyhow!(
                        "a second reconfiguration command arrived during input rebind"
                    );
                    return Err(self
                        .fail_after_cleanup(error, shutdown_timeout, &failure_deadline)
                        .await);
                }
                Some(Err(error)) => {
                    return Err(self
                        .fail_after_cleanup(error, shutdown_timeout, &failure_deadline)
                        .await);
                }
                None => {
                    return Err(anyhow::anyhow!(
                        "input ended while establishing rebind boundary"
                    ));
                }
            }
        }

        let candidate = plan.candidate().clone();
        let mut rebound = plan
            .rebound_sources()
            .iter()
            .map(|source| (source.source().clone(), source.clone()))
            .collect::<std::collections::BTreeMap<_, _>>();
        if plan
            .removed_sources()
            .iter()
            .any(|source| source == &self.control.source)
        {
            rebound.insert(
                self.control.source.clone(),
                ResolvedSource::new(self.control.source.clone(), []),
            );
        }
        for (source_id, source_candidate) in &rebound {
            // Detach only the owner being changed. If it fails, every owner
            // not yet processed remains in the source set and participates in
            // fail-stop cleanup.
            let mut detached = self
                .sources
                .borrow_mut()
                .detach(&BTreeSet::from([source_id.clone()]));
            let Some(mut source) = detached.pop() else {
                let error = anyhow::anyhow!("input source `{source_id}` is not active");
                return Err(self
                    .fail_after_cleanup(error, shutdown_timeout, &failure_deadline)
                    .await);
            };
            if source.control.is_none() {
                source.stop_ingress();
                let cleanup = drain_source(&mut source).await;
                let mut error = anyhow::anyhow!(
                    "input source `{source_id}` does not support in-place rebind; session stopped"
                );
                for cleanup in cleanup {
                    error = error.context(format!("input cleanup also failed: {cleanup:#}"));
                }
                return Err(self
                    .fail_after_cleanup(error, shutdown_timeout, &failure_deadline)
                    .await);
            }
            if let Err(error) = source.rebind(source_candidate).await {
                source.stop_ingress();
                let cleanup = drain_source(&mut source).await;
                let mut error = error.context(format!(
                    "input source `{source_id}` rebind failed; session stopped"
                ));
                for cleanup in cleanup {
                    error = error.context(format!("input cleanup also failed: {cleanup:#}"));
                }
                return Err(self
                    .fail_after_cleanup(error, shutdown_timeout, &failure_deadline)
                    .await);
            }
            self.sources.borrow_mut().insert(source)?;
        }

        let removed = plan
            .removed_sources()
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut draining = self.sources.borrow_mut().detach(&removed);

        for source in &mut draining {
            source.stop_ingress();
        }
        let cleanup = drain_sources(&mut draining).await;
        if !cleanup.is_empty() {
            let mut error = anyhow::anyhow!("removed input source cleanup failed");
            for cause in cleanup {
                error = error.context(format!("input cleanup also failed: {cause:#}"));
            }
            return Err(self
                .fail_after_cleanup(error, shutdown_timeout, &failure_deadline)
                .await);
        }

        let additions = plan.added_sources().iter().cloned().collect::<Vec<_>>();
        let additions = match self
            .pipeline
            .open_reconfigurable_source_plans(&additions, &self.control, Rc::clone(&self.executor))
            .await
        {
            Ok(additions) => additions,
            Err(error) => {
                return Err(self
                    .fail_after_cleanup(
                        error.context("input additions failed; session stopped"),
                        shutdown_timeout,
                        &failure_deadline,
                    )
                    .await);
            }
        };
        for source in additions {
            self.sources.borrow_mut().insert(source)?;
        }
        anyhow::ensure!(
            self.sources.borrow().contains(&self.control.source),
            "input rebind lost control source `{}`",
            self.control.source
        );

        let resume_result = {
            let mut sources = self.sources.borrow_mut();
            sources.resume_all().await
        };
        let resumes = match resume_result {
            Ok(resumes) => resumes,
            Err(error) => {
                return Err(self
                    .fail_after_cleanup(
                        error.context("input source resume failed; session stopped"),
                        shutdown_timeout,
                        &failure_deadline,
                    )
                    .await);
            }
        };
        for command in resumes {
            if command.send(RelayCommand::Resume).await.is_err() {
                let error = anyhow::anyhow!("input source stopped before rebind resumed");
                return Err(self
                    .fail_after_cleanup(error, shutdown_timeout, &failure_deadline)
                    .await);
            }
        }
        self.active = candidate;
        self.pending_revision = Some(plan.expected_revision());
        Ok(self)
    }
}

impl<V> Stream for InputPipelineSession<V> {
    type Item = anyhow::Result<ReconfigurableInputItem<V>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.stream.as_mut().poll_next(cx)
    }
}

fn apply_barrier_policy<V: 'static>(
    input: ReconfigurableInputStream<V>,
    policy: Option<&InputPolicy>,
) -> anyhow::Result<ReconfigurableInputStream<V>> {
    apply_barrier_policy_with_timer(input, policy, RealTimeInputTimer)
}

fn apply_barrier_policy_with_timer<V: 'static, T: InputTimer>(
    mut input: ReconfigurableInputStream<V>,
    policy: Option<&InputPolicy>,
    timer: T,
) -> anyhow::Result<ReconfigurableInputStream<V>> {
    let Some(policy) = policy.cloned() else {
        return Ok(input);
    };
    let events: WindowEventStream<V, ReconfigurationRequest> =
        Box::pin(async_stream::try_stream! {
            while let Some(item) = input.next().await {
                yield match item? {
                    ReconfigurableInputItem::Data(batch) => WindowEvent::Data(batch),
                    ReconfigurableInputItem::Reconfigure(control) => WindowEvent::Control(control),
                    ReconfigurableInputItem::Boundary(boundary) => WindowEvent::Boundary(boundary),
                };
            }
        });
    let mut output = drive_window(events, policy, timer)?;
    Ok(Box::pin(async_stream::try_stream! {
        while let Some(event) = output.next().await {
            yield match event? {
                WindowEvent::Data(batch) => ReconfigurableInputItem::Data(batch),
                WindowEvent::Control(control) => {
                    ReconfigurableInputItem::Reconfigure(control)
                }
                WindowEvent::Boundary(boundary) => ReconfigurableInputItem::Boundary(boundary),
            };
        }
    }))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, num::NonZeroUsize};

    use super::*;
    use crate::core::InputUpdate;
    use crate::io::config::InputWindow;
    use crate::io::{InputPipeline, InputSource};
    use crate::stream_utils::Fanout;
    use crate::{Value, VarName};

    #[test]
    fn direct_source_eof_completes_without_scheduling_cleanup() {
        let mut source = OpenedInputSource::<Value>::direct(
            "finite".to_owned(),
            Box::pin(futures::stream::empty()),
        );
        assert!(matches!(source.next().now_or_never(), Some(None)));
        assert!(matches!(source.next().now_or_never(), Some(None)));
    }

    #[test]
    fn stopped_source_relay_drains_admitted_items_to_eof() {
        let executor = Rc::new(LocalExecutor::new());
        let task_executor = Rc::clone(&executor);
        smol::block_on(executor.run(async move {
            let (sender, receiver) = async_channel::bounded(2);
            sender
                .send(Ok(ReconfigurableInputItem::Data(InputBatch::update(
                    "x", 1,
                ))))
                .await
                .unwrap();
            sender
                .send(Ok(ReconfigurableInputItem::Data(InputBatch::update(
                    "x", 2,
                ))))
                .await
                .unwrap();
            let raw: ReconfigurableInputStream<i32> = Box::pin(receiver);
            let mut source = OpenedInputSource::relay("source".into(), raw, task_executor);

            assert!(matches!(
                source.next().await.unwrap().unwrap(),
                ReconfigurableInputItem::Data(_)
            ));
            smol::future::yield_now().await;
            source.stop_ingress();

            let ReconfigurableInputItem::Data(batch) = source.next().await.unwrap().unwrap() else {
                panic!("the item admitted before source shutdown must be drained");
            };
            assert_eq!(*batch.updates().next().unwrap().value, 2);
            assert!(source.next().await.is_none());
        }));
    }

    #[test]
    fn source_eof_waits_for_delayed_transport_cleanup() {
        let executor = Rc::new(LocalExecutor::new());
        let task_executor = Rc::clone(&executor);
        smol::block_on(executor.run(async move {
            let completed = Rc::new(std::cell::Cell::new(false));
            let raw: ReconfigurableInputStream<i32> = Box::pin(futures::stream::pending());
            let control = InputSourceControl::DelayedTest {
                delay: std::time::Duration::from_millis(10),
                completed: Rc::clone(&completed),
            };
            let mut source = OpenedInputSource::relay_with_control(
                "owned".into(),
                raw,
                task_executor,
                Some(control),
            );

            source.stop_ingress();
            assert!(!completed.get());
            assert!(source.next().await.is_none());
            assert!(completed.get(), "EOF must follow transport cleanup");
        }));
    }

    #[test]
    fn ended_relay_drains_its_locally_admitted_item_before_boundary() {
        let executor = Rc::new(LocalExecutor::new());
        let task_executor = Rc::clone(&executor);
        smol::block_on(executor.run(async move {
            let raw: ReconfigurableInputStream<i32> = Box::pin(futures::stream::iter([Ok(
                ReconfigurableInputItem::Data(InputBatch::update("x", 1)),
            )]));
            let source = OpenedInputSource::relay("finite".into(), raw, task_executor);
            while !source.ingress.task.as_ref().unwrap().is_finished() {
                smol::future::yield_now().await;
            }
            let mut sources = InputSourceSet::new(vec![source]);

            // Let the relay admit the ready item and observe transport EOF. The
            // boundary partitions locally admitted relay data; it does not
            // claim that every future-ready transport message was admitted.
            let commands = sources.begin_boundary(1).await.unwrap();
            for command in commands {
                assert!(command.send(RelayCommand::Boundary(1)).await.is_err());
            }

            assert!(matches!(
                sources.next().await.unwrap().unwrap(),
                ReconfigurableInputItem::Data(_)
            ));
            assert!(matches!(
                sources.next().await.unwrap().unwrap(),
                ReconfigurableInputItem::Boundary(1)
            ));
        }));
    }

    #[test]
    fn reusable_barrier_flushes_data_before_control_and_preserves_post_control_data() {
        smol::block_on(async {
            let control = ReconfigurationRequest::from_json(r#"{"specification":"in x"}"#).unwrap();
            let before = InputBatch::update("x", 1);
            let after = InputBatch::update("x", 2);
            let input: ReconfigurableInputStream<i32> = Box::pin(futures::stream::iter([
                Ok(ReconfigurableInputItem::Data(before)),
                Ok(ReconfigurableInputItem::Reconfigure(control)),
                Ok(ReconfigurableInputItem::Data(after)),
            ]));
            let policy = InputPolicy::Batch(InputWindow::new(None, NonZeroUsize::new(10)).unwrap());
            let mut output = apply_barrier_policy(input, Some(&policy)).unwrap();

            let ReconfigurableInputItem::Data(batch) = output.next().await.unwrap().unwrap() else {
                panic!("pending data must be flushed before control");
            };
            assert_eq!(
                batch.ticks().next().unwrap().to_updates(),
                vec![InputUpdate::new("x".into(), 1)]
            );
            assert!(matches!(
                output.next().await.unwrap().unwrap(),
                ReconfigurableInputItem::Reconfigure(_)
            ));
            let ReconfigurableInputItem::Data(batch) = output.next().await.unwrap().unwrap() else {
                panic!("post-control data must remain live");
            };
            assert_eq!(
                batch.ticks().next().unwrap().to_updates(),
                vec![InputUpdate::new("x".into(), 2)]
            );
            assert!(output.next().await.is_none());
        });
    }

    #[test]
    fn session_drain_emits_admitted_data_and_then_eof() {
        let executor = Rc::new(LocalExecutor::new());
        let task_executor = Rc::clone(&executor);
        smol::block_on(executor.run(async move {
            let (data_sender, data_fanout) = Fanout::<Value>::new();
            let (_control_sender, control_fanout) = Fanout::<Value>::new();
            let source = InputSource::channel_with_control(
                BTreeMap::from([(VarName::new("x"), data_fanout)]),
                Some(control_fanout),
            )
            .with_reconfiguration_route("control")
            .unwrap();
            let pipeline = InputPipeline::new(source);
            let resolved = pipeline
                .resolve(&BTreeSet::from([VarName::new("x")]), None)
                .unwrap();
            let input =
                ReconfigurableInput::new(pipeline, Some("control".to_owned()), task_executor)
                    .unwrap();
            let session = input.open_session(resolved).await.unwrap();

            data_sender.send(Value::Int(7)).await;
            smol::future::yield_now().await;
            let mut drain = session.into_drain();
            let ReconfigurableInputItem::Data(batch) = drain.next().await.unwrap().unwrap() else {
                panic!("admitted input must be drained as data")
            };
            assert_eq!(*batch.updates().next().unwrap().value, Value::Int(7));
            assert!(drain.next().await.is_none());
        }));
    }

    #[test]
    fn programmatic_rebind_establishes_its_own_stream_boundary() {
        let executor = Rc::new(LocalExecutor::new());
        let task_executor = Rc::clone(&executor);
        smol::block_on(executor.run(async move {
            let (_data_sender, data_fanout) = Fanout::<Value>::new();
            let (_control_sender, control_fanout) = Fanout::<Value>::new();
            let source = InputSource::channel_with_control(
                BTreeMap::from([(VarName::new("x"), data_fanout)]),
                Some(control_fanout),
            )
            .with_reconfiguration_route("control")
            .unwrap();
            let pipeline = InputPipeline::new(source);
            let resolved = pipeline
                .resolve(&BTreeSet::from([VarName::new("x")]), None)
                .unwrap();
            let input = ReconfigurableInput::new(
                pipeline.clone(),
                Some("control".to_owned()),
                task_executor,
            )
            .unwrap();
            let session = input.open_session(resolved).await.unwrap();
            let candidate = pipeline
                .resolve(&BTreeSet::from([VarName::new("x")]), None)
                .unwrap();
            let plan = pipeline
                .plan_reconfiguration(
                    session.active(),
                    candidate,
                    session.session_id(),
                    session.revision(),
                )
                .unwrap();

            let session = session
                .rebind(plan, None, Rc::new(Cell::new(None)), async |_| Ok(()))
                .await
                .unwrap();
            assert_eq!(session.revision(), SessionRevision::initial());
        }));
    }

    #[test]
    fn repeated_noop_control_barriers_advance_one_revision_each() {
        let executor = Rc::new(LocalExecutor::new());
        let task_executor = Rc::clone(&executor);
        smol::block_on(executor.run(async move {
            let (_data_sender, data_fanout) = Fanout::<Value>::new();
            let (control_sender, control_fanout) = Fanout::<Value>::new();
            let source = InputSource::channel_with_control(
                BTreeMap::from([(VarName::new("x"), data_fanout)]),
                Some(control_fanout),
            )
            .with_reconfiguration_route("control")
            .unwrap();
            let pipeline = InputPipeline::new(source);
            let variables = BTreeSet::from([VarName::new("x")]);
            let resolved = pipeline.resolve(&variables, None).unwrap();
            let input = ReconfigurableInput::new(
                pipeline.clone(),
                Some("control".to_owned()),
                task_executor,
            )
            .unwrap();
            let mut session = input.open_session(resolved).await.unwrap();

            for expected in [
                SessionRevision::initial(),
                SessionRevision::initial().next(),
            ] {
                control_sender
                    .send(Value::Str(r#"{"specification":"in x"}"#.into()))
                    .await;
                assert!(matches!(
                    session.next().await.unwrap().unwrap(),
                    ReconfigurableInputItem::Reconfigure(_)
                ));
                let candidate = pipeline.resolve(&variables, None).unwrap();
                let plan = pipeline
                    .plan_reconfiguration(
                        session.active(),
                        candidate,
                        session.session_id(),
                        expected,
                    )
                    .unwrap();
                session = session
                    .rebind(plan, None, Rc::new(Cell::new(None)), async |_| Ok(()))
                    .await
                    .unwrap();
                assert_eq!(session.revision(), expected);
                session.commit_revision(expected).unwrap();
                assert_eq!(session.revision(), expected.next());
            }
        }));
    }
}
