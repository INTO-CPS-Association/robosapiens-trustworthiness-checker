use std::{
    cell::RefCell,
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
    InputStage, ReconfigurationRequest, ResolvedInput, ResolvedSource, SourceId,
};

/// A typed item at the reconfigurable orchestration boundary. Ordinary input
/// streams contain only `InputBatch` values; reconfigurable streams also carry
/// reusable `Reconfigure(ReconfigurationRequest)` control barriers.
#[derive(Debug)]
pub(crate) enum ReconfigurableInputItem<V> {
    Data(InputBatch<V>),
    Reconfigure(ReconfigurationRequest),
}

pub(crate) type ReconfigurableInputStream<V> =
    crate::OutputStream<anyhow::Result<ReconfigurableInputItem<V>>>;

pub(crate) struct OpenedInputSource<V> {
    pub(crate) id: SourceId,
    stream: ReconfigurableInputStream<V>,
    ingress: SourceIngress,
}

struct SourceIngress {
    cancellation: crate::utils::cancellation_token::CancellationToken,
    task: Option<Task<()>>,
}

impl SourceIngress {
    fn stop(&self) {
        self.cancellation.cancel();
    }
}

impl Drop for SourceIngress {
    fn drop(&mut self) {
        self.cancellation.cancel();
        drop(self.task.take());
    }
}

impl<V: 'static> OpenedInputSource<V> {
    pub(crate) fn direct(id: SourceId, source: ReconfigurableInputStream<V>) -> Self {
        let cancellation = crate::utils::cancellation_token::CancellationToken::new();
        Self {
            id,
            stream: source,
            ingress: SourceIngress {
                cancellation,
                task: None,
            },
        }
    }

    pub(crate) fn relay(
        id: SourceId,
        mut source: ReconfigurableInputStream<V>,
        executor: Rc<LocalExecutor<'static>>,
    ) -> Self {
        // One queued item plus one item held by the relay preserves normal
        // backpressure while giving source removal a concrete local boundary.
        let (sender, receiver) = async_unsync::bounded::channel(1).into_split();
        let cancellation = crate::utils::cancellation_token::CancellationToken::new();
        let worker_cancellation = cancellation.clone();
        let task = executor.spawn(async move {
            loop {
                let item = futures::select_biased! {
                    _ = worker_cancellation.cancelled().fuse() => break,
                    item = source.next().fuse() => item,
                };
                let Some(item) = item else {
                    break;
                };
                if sender.send(item).await.is_err() {
                    break;
                }
            }
        });
        let stream = futures::stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        });
        Self {
            id,
            stream: Box::pin(stream),
            ingress: SourceIngress {
                cancellation,
                task: Some(task),
            },
        }
    }

    fn stop_ingress(&self) {
        self.ingress.stop();
    }
}

impl<V> Stream for OpenedInputSource<V> {
    type Item = anyhow::Result<ReconfigurableInputItem<V>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.ingress.task.is_none() && self.ingress.cancellation.is_cancelled_now() {
            Poll::Ready(None)
        } else {
            self.stream.as_mut().poll_next(cx)
        }
    }
}

struct InputSourceSet<V> {
    sources: Vec<OpenedInputSource<V>>,
    next: usize,
}

impl<V> InputSourceSet<V> {
    fn new(sources: Vec<OpenedInputSource<V>>) -> Self {
        Self { sources, next: 0 }
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

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<anyhow::Result<ReconfigurableInputItem<V>>>> {
        if self.sources.is_empty() {
            return Poll::Ready(None);
        }
        let mut checked = 0;
        while checked < self.sources.len() {
            let index = self.next % self.sources.len();
            self.next = (index + 1) % self.sources.len();
            match Pin::new(&mut self.sources[index]).poll_next(cx) {
                Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
                Poll::Ready(None) => {
                    self.sources.remove(index);
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

impl<V> Stream for InputSourceSet<V> {
    type Item = anyhow::Result<ReconfigurableInputItem<V>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        InputSourceSet::poll_next(&mut self, cx)
    }
}

struct SharedInputSourceSet<V>(Rc<RefCell<InputSourceSet<V>>>);

impl<V> Stream for SharedInputSourceSet<V> {
    type Item = anyhow::Result<ReconfigurableInputItem<V>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.borrow_mut().poll_next(cx)
    }
}

pub(crate) struct RemovedInputDrain<V> {
    stream: ReconfigurableInputStream<V>,
}

impl<V> Stream for RemovedInputDrain<V> {
    type Item = anyhow::Result<ReconfigurableInputItem<V>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
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

    pub async fn open_stream(
        &self,
        resolved: ResolvedInput,
    ) -> anyhow::Result<ReconfigurableInputStream<V>>
    where
        V: crate::core::FileInputValue + crate::core::RosStreamValue,
    {
        let raw = self
            .pipeline
            .open_reconfigurable(resolved, &self.control)
            .await?;
        apply_barrier_stage(raw, self.pipeline.stages())
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
        let stream = apply_barrier_stage(raw, self.pipeline.stages())?;
        Ok(InputPipelineSession {
            pipeline: self.pipeline.clone(),
            control: self.control.clone(),
            active: resolved,
            sources,
            stream,
            executor: Rc::clone(&self.executor),
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
}

impl<V> Unpin for InputPipelineSession<V> {}

impl<V> InputPipelineSession<V>
where
    V: crate::core::FileInputValue + crate::core::RosStreamValue,
{
    pub(crate) fn active(&self) -> &ResolvedInput {
        &self.active
    }

    fn validate_plan(&self, plan: &InputPipelineReconfigurationPlan) -> anyhow::Result<()> {
        anyhow::ensure!(
            plan.active_fingerprint() == self.active.fingerprint(),
            "stale input pipeline reconfiguration plan does not match the active resolution"
        );
        Ok(())
    }

    /// Stop and detach removed sources. Their bounded local relays close after
    /// forwarding already admitted items, so this stream reaches genuine EOF.
    pub(crate) fn remove_sources(
        &mut self,
        plan: &InputPipelineReconfigurationPlan,
    ) -> anyhow::Result<RemovedInputDrain<V>> {
        self.validate_plan(plan)?;
        let removed = plan
            .removed_sources()
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        // A finite source may already have reached EOF and removed itself from
        // the composed set; in that case its removal is already complete.
        let draining = self.sources.borrow_mut().detach(&removed);
        for source in &draining {
            source.stop_ingress();
        }
        let raw: ReconfigurableInputStream<V> = Box::pin(InputSourceSet::new(draining));
        Ok(RemovedInputDrain {
            stream: apply_barrier_stage(raw, self.pipeline.stages())?,
        })
    }

    /// Open additions after all removals have reached EOF, then commit the
    /// candidate resolution. Unchanged sources remain live throughout.
    pub(crate) async fn add_sources_and_commit(
        &mut self,
        plan: InputPipelineReconfigurationPlan,
    ) -> anyhow::Result<()> {
        self.validate_plan(&plan)?;
        let candidate = plan.candidate().clone();
        let mut additions = self
            .pipeline
            .open_reconfigurable_source_plans(
                plan.added_sources(),
                &self.control,
                Rc::clone(&self.executor),
            )
            .await?;
        if plan
            .removed_sources()
            .iter()
            .any(|source| source == &self.control.source)
            && additions
                .iter()
                .all(|source| source.id != self.control.source)
        {
            let control_plan = ResolvedSource::new(self.control.source.clone(), []);
            additions.extend(
                self.pipeline
                    .open_reconfigurable_source_plans(
                        &[control_plan],
                        &self.control,
                        Rc::clone(&self.executor),
                    )
                    .await?,
            );
        }
        {
            let mut sources = self.sources.borrow_mut();
            let control_was_data_source = self
                .active
                .sources()
                .iter()
                .any(|source| source.source() == &self.control.source);
            if !control_was_data_source
                && additions
                    .iter()
                    .any(|source| source.id == self.control.source)
            {
                sources.detach(&BTreeSet::from([self.control.source.clone()]));
            }
            for source in additions {
                sources.insert(source)?;
            }
            if !sources.contains(&self.control.source) {
                anyhow::bail!(
                    "input reconfiguration removed control source `{}` without replacing it",
                    self.control.source
                );
            }
        }
        self.active = candidate;
        Ok(())
    }
}

impl<V> Stream for InputPipelineSession<V> {
    type Item = anyhow::Result<ReconfigurableInputItem<V>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().stream.as_mut().poll_next(cx)
    }
}

fn apply_barrier_stage<V: 'static>(
    input: ReconfigurableInputStream<V>,
    stages: &[InputStage],
) -> anyhow::Result<ReconfigurableInputStream<V>> {
    apply_barrier_stage_with_timer(input, stages, RealTimeInputTimer)
}

fn apply_barrier_stage_with_timer<V: 'static, T: InputTimer>(
    mut input: ReconfigurableInputStream<V>,
    stages: &[InputStage],
    timer: T,
) -> anyhow::Result<ReconfigurableInputStream<V>> {
    let Some(stage) = stages.first().cloned() else {
        return Ok(input);
    };
    let events: WindowEventStream<V, ReconfigurationRequest> =
        Box::pin(async_stream::try_stream! {
            while let Some(item) = input.next().await {
                yield match item? {
                    ReconfigurableInputItem::Data(batch) => WindowEvent::Data(batch),
                    ReconfigurableInputItem::Reconfigure(control) => WindowEvent::Control(control),
                };
            }
        });
    let mut output = drive_window(events, stage, timer)?;
    Ok(Box::pin(async_stream::try_stream! {
        while let Some(event) = output.next().await {
            yield match event? {
                WindowEvent::Data(batch) => ReconfigurableInputItem::Data(batch),
                WindowEvent::Control(control) => {
                    ReconfigurableInputItem::Reconfigure(control)
                }
            };
        }
    }))
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;
    use crate::core::InputUpdate;
    use crate::io::config::InputWindow;

    #[test]
    fn stopped_direct_source_reaches_eof_without_polling_the_transport() {
        smol::block_on(async {
            let (sender, receiver) = async_channel::bounded(1);
            sender
                .send(Ok(ReconfigurableInputItem::Data(InputBatch::update(
                    "x", 1,
                ))))
                .await
                .unwrap();
            let raw: ReconfigurableInputStream<i32> = Box::pin(receiver);
            let mut source = OpenedInputSource::direct("source".into(), raw);

            source.stop_ingress();

            assert!(source.next().await.is_none());
        });
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
            let stage = InputStage::Batch(InputWindow::new(None, NonZeroUsize::new(10)).unwrap());
            let mut output = apply_barrier_stage(input, &[stage]).unwrap();

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
}
