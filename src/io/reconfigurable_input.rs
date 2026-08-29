use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{Stream, StreamExt};

use crate::core::InputBatch;
use crate::io::aggregation::{
    InputTimer, RealTimeInputTimer, WindowEvent, WindowEventStream, drive_window,
};
use crate::io::builders::InputPipeline;
use crate::io::config::{InputStage, ReconfigurationRequest, ResolvedInput, SourceId};

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
}

impl<V: Clone> ReconfigurableInput<V> {
    pub fn new(
        pipeline: InputPipeline<V>,
        requested_route: Option<String>,
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
        Ok(Self { pipeline, control })
    }

    pub fn pipeline(&self) -> &InputPipeline<V> {
        &self.pipeline
    }

    pub async fn open_resolved(
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
        let raw = self
            .pipeline
            .open_reconfigurable(resolved.clone(), &self.control)
            .await?;
        let stream = apply_barrier_stage(raw, self.pipeline.stages())?;
        Ok(InputPipelineSession {
            active: resolved,
            stream,
        })
    }
}

/// A live input boundary owns the active plan and control-aware stream.
pub(crate) struct InputPipelineSession<V = crate::Value> {
    active: ResolvedInput,
    stream: ReconfigurableInputStream<V>,
}

impl<V> Unpin for InputPipelineSession<V> {}

impl<V> InputPipelineSession<V> {
    pub(crate) fn active(&self) -> &ResolvedInput {
        &self.active
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
