use anyhow::Context;
use futures::select;
use futures::{FutureExt, StreamExt};
use r2r;
use smol::{LocalExecutor, Task};
use std::rc::Rc;
use std::{
    collections::BTreeMap,
    pin::Pin,
    task::{Context as TaskContext, Poll},
};
use tracing::{Level, instrument};
use uuid::Uuid;

use super::{
    ROS_SPIN_INTERVAL, ROS_SPIN_TIMEOUT,
    ros_topic_stream_mapping::{RosMsgType, RosStreamMapping},
};

use crate::core::empty_input_stream;
use crate::io::ReconfigurationRequest;
use crate::utils::cancellation_token::CancellationToken;
use crate::{InputBatch, InputStream, LocalStream, Value, VarName};

pub struct RosInputControl {
    cancellation: CancellationToken,
    spinner: Option<Task<()>>,
    commands: Option<async_channel::Sender<RosInputCommand>>,
}

pub(super) type RawMapping = BTreeMap<String, (String, String)>;

enum RosInputCommand {
    Pause(u64),
    Resume(async_channel::Sender<anyhow::Result<()>>),
    Rebind(RawMapping, async_channel::Sender<anyhow::Result<()>>),
}

#[doc(hidden)]
pub enum RosInputItem<V> {
    Data(InputBatch<V>),
    Boundary(u64),
}

#[doc(hidden)]
pub type RosInputStream<V> = LocalStream<anyhow::Result<RosInputItem<V>>>;

impl RosInputControl {
    pub(crate) fn supports_reconfiguration(&self) -> bool {
        self.commands.is_some()
    }

    pub(crate) fn inactive() -> Self {
        Self {
            cancellation: CancellationToken::new(),
            spinner: None,
            commands: None,
        }
    }

    pub(crate) fn new(cancellation: CancellationToken, spinner: Task<()>) -> Self {
        Self {
            cancellation,
            spinner: Some(spinner),
            commands: None,
        }
    }

    fn managed(
        cancellation: CancellationToken,
        spinner: Task<()>,
        commands: async_channel::Sender<RosInputCommand>,
    ) -> Self {
        Self {
            cancellation,
            spinner: Some(spinner),
            commands: Some(commands),
        }
    }

    pub(crate) async fn pause(&self, boundary: u64) -> anyhow::Result<bool> {
        let Some(commands) = &self.commands else {
            return Ok(false);
        };
        commands
            .send(RosInputCommand::Pause(boundary))
            .await
            .map_err(|_| anyhow::anyhow!("ROS input owner has stopped"))?;
        Ok(true)
    }

    pub(crate) async fn resume(&self) -> anyhow::Result<()> {
        if self.commands.is_none() {
            return Ok(());
        }
        self.request(|reply| RosInputCommand::Resume(reply)).await
    }

    pub(crate) async fn rebind(&self, mapping: RawMapping) -> anyhow::Result<()> {
        self.request(|reply| RosInputCommand::Rebind(mapping, reply))
            .await
    }

    async fn request(
        &self,
        command: impl FnOnce(async_channel::Sender<anyhow::Result<()>>) -> RosInputCommand,
    ) -> anyhow::Result<()> {
        let Some(commands) = &self.commands else {
            anyhow::bail!("ROS input owner does not support reconfiguration")
        };
        let (reply, result) = async_channel::bounded(1);
        commands
            .send(command(reply))
            .await
            .map_err(|_| anyhow::anyhow!("ROS input owner has stopped"))?;
        result
            .recv()
            .await
            .map_err(|_| anyhow::anyhow!("ROS input owner stopped during reconfiguration"))?
    }

    pub async fn shutdown(&mut self) -> anyhow::Result<()> {
        self.cancellation.cancel();
        if let Some(spinner) = self.spinner.take() {
            spinner.await;
        }
        Ok(())
    }
}

pub(super) type RawStreams<V> = BTreeMap<VarName, LocalStream<anyhow::Result<V>>>;
type StreamFactory<V> = fn(&mut r2r::Node, &RawMapping) -> anyhow::Result<RawStreams<V>>;

struct RosStreams<V> {
    entries: Vec<(String, (String, String), LocalStream<anyhow::Result<V>>)>,
    next: usize,
}

impl<V> RosStreams<V> {
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn new(mapping: &RawMapping, mut streams: RawStreams<V>) -> Self {
        let entries = mapping
            .iter()
            .filter_map(|(variable, route)| {
                streams
                    .remove(&VarName::new(variable))
                    .map(|stream| (variable.clone(), route.clone(), stream))
            })
            .collect();
        Self { entries, next: 0 }
    }

    fn rebind(
        &mut self,
        node: &mut r2r::Node,
        mapping: &RawMapping,
        create: StreamFactory<V>,
    ) -> anyhow::Result<()> {
        let mut old = std::mem::take(&mut self.entries)
            .into_iter()
            .map(|entry| (entry.0.clone(), entry))
            .collect::<BTreeMap<_, _>>();
        let mut entries = Vec::with_capacity(mapping.len());
        for (variable, route) in mapping {
            if let Some(entry) = old.remove(variable).filter(|entry| entry.1 == *route) {
                entries.push(entry);
                continue;
            }
            let singleton = BTreeMap::from([(variable.clone(), route.clone())]);
            let mut created = create(node, &singleton)?;
            let stream = created.remove(&VarName::new(variable)).ok_or_else(|| {
                anyhow::anyhow!("ROS input factory did not create variable `{variable}`")
            })?;
            entries.push((variable.clone(), route.clone(), stream));
        }
        self.entries = entries;
        self.next = 0;
        Ok(())
    }
}

impl<V> futures::Stream for RosStreams<V> {
    type Item = anyhow::Result<(VarName, V)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        if self.entries.is_empty() {
            return Poll::Ready(None);
        }
        let mut remaining = self.entries.len();
        let mut index = self.next % self.entries.len();
        while remaining > 0 {
            let polled = self.entries[index].2.as_mut().poll_next(cx);
            match polled {
                Poll::Ready(Some(item)) => {
                    self.next = (index + 1) % self.entries.len();
                    let variable = VarName::new(&self.entries[index].0);
                    return Poll::Ready(Some(item.map(|value| (variable, value))));
                }
                Poll::Ready(None) => {
                    drop(self.entries.remove(index));
                    remaining -= 1;
                    if self.entries.is_empty() {
                        self.next = 0;
                        return Poll::Ready(None);
                    }
                    if index == self.entries.len() {
                        index = 0;
                    }
                }
                Poll::Pending => {
                    remaining -= 1;
                    index = (index + 1) % self.entries.len();
                }
            }
        }
        self.next = index;
        Poll::Pending
    }
}

pub(crate) fn open_managed_ros_input<V: 'static>(
    executor: Rc<LocalExecutor<'static>>,
    mapping: RawMapping,
    create_streams: StreamFactory<V>,
) -> anyhow::Result<(RosInputStream<V>, RosInputControl)> {
    let context = r2r::Context::create()?;
    let node_name = format!("input_monitor_{}", Uuid::new_v4().simple());
    let mut node = r2r::Node::create(context, &node_name, "")?;
    let streams = RosStreams::new(&mapping, create_streams(&mut node, &mapping)?);
    let (commands, command_rx) = async_channel::bounded(1);
    let (items, item_rx) = async_channel::bounded(1);
    let cancellation = CancellationToken::new();
    let worker_cancel = cancellation.clone();
    let spinner = executor.spawn(async move {
        let worker = run_managed_ros_input(node, streams, create_streams, command_rx, items);
        futures::pin_mut!(worker);
        futures::select_biased! {
            _ = worker_cancel.cancelled().fuse() => {}
            _ = worker.fuse() => {}
        }
    });
    let stream = Box::pin(item_rx.map(|item| item)) as RosInputStream<V>;
    Ok((
        stream,
        RosInputControl::managed(cancellation, spinner, commands),
    ))
}

async fn run_managed_ros_input<V: 'static>(
    mut node: r2r::Node,
    mut streams: RosStreams<V>,
    create_streams: StreamFactory<V>,
    commands: async_channel::Receiver<RosInputCommand>,
    items: async_channel::Sender<anyhow::Result<RosInputItem<V>>>,
) {
    enum Next<V> {
        Command(Result<RosInputCommand, async_channel::RecvError>),
        Spin,
        Item(Option<anyhow::Result<(VarName, V)>>),
    }
    let mut spin_ticks = smol::Timer::interval(ROS_SPIN_INTERVAL);
    loop {
        let next = {
            let event = if streams.is_empty() {
                futures::future::pending().right_future()
            } else {
                streams.next().left_future()
            }
            .fuse();
            futures::pin_mut!(event);
            futures::select_biased! {
                command = commands.recv().fuse() => Next::Command(command),
                _ = spin_ticks.next().fuse() => Next::Spin,
                event = event => Next::Item(event),
            }
        };
        match next {
            Next::Spin => node.spin_once(ROS_SPIN_TIMEOUT),
            Next::Command(command) => match command {
                Ok(RosInputCommand::Pause(boundary)) => {
                    if items
                        .send(Ok(RosInputItem::Boundary(boundary)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    loop {
                        match commands.recv().await {
                            Ok(RosInputCommand::Resume(reply)) => {
                                let _ = reply.send(Ok(())).await;
                                break;
                            }
                            Ok(RosInputCommand::Rebind(candidate, reply)) => {
                                let result = streams.rebind(&mut node, &candidate, create_streams);
                                match result {
                                    Ok(()) => {
                                        let _ = reply.send(Ok(())).await;
                                        break;
                                    }
                                    Err(error) => {
                                        let _ = reply.send(Err(error)).await;
                                        return;
                                    }
                                }
                            }
                            Ok(RosInputCommand::Pause(_)) => {}
                            Err(_) => return,
                        }
                    }
                }
                Ok(RosInputCommand::Resume(reply)) => {
                    let _ = reply.send(Ok(())).await;
                }
                Ok(RosInputCommand::Rebind(candidate, reply)) => {
                    let result = streams.rebind(&mut node, &candidate, create_streams);
                    match result {
                        Ok(()) => {
                            let _ = reply.send(Ok(())).await;
                        }
                        Err(error) => {
                            let _ = reply.send(Err(error)).await;
                            return;
                        }
                    }
                }
                Err(_) => return,
            },
            Next::Item(event) => match event {
                Some(Ok((variable, value))) => {
                    if items
                        .send(Ok(RosInputItem::Data(InputBatch::update(variable, value))))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Some(Err(error)) => {
                    if items.send(Err(error)).await.is_err() {
                        return;
                    }
                }
                None => return,
            },
        }
    }
}

impl Drop for RosInputControl {
    fn drop(&mut self) {
        self.cancellation.cancel();
        let _ = self.spinner.take();
    }
}

impl RosMsgType {
    /* Create a stream of values received on a ROS topic */
    fn node_output_stream(
        &self,
        node: &mut r2r::Node,
        topic: &str,
        qos: r2r::QosProfile,
    ) -> anyhow::Result<LocalStream<Value>> {
        Ok(match self {
            RosMsgType::Bool => Box::pin(
                node.subscribe::<r2r::std_msgs::msg::Bool>(topic, qos)?
                    .map(|val| Value::Bool(val.data)),
            ),
            RosMsgType::String => Box::pin(
                node.subscribe::<r2r::std_msgs::msg::String>(topic, qos)?
                    .map(|val| Value::Str(val.data.into())),
            ),
            RosMsgType::Int64 => Box::pin(
                node.subscribe::<r2r::std_msgs::msg::Int64>(topic, qos)?
                    .map(|val| Value::Int(val.data)),
            ),
            RosMsgType::Int32 => Box::pin(
                node.subscribe::<r2r::std_msgs::msg::Int32>(topic, qos)?
                    .map(|val| Value::Int(val.data.into())),
            ),
            RosMsgType::Int32List => Box::pin(
                node.subscribe::<r2r::std_msgs::msg::Int32MultiArray>(topic, qos)?
                    .map(|val| {
                        serde_json::to_value(val.data)
                            .expect("Failed to serialize ROS2 Int32MultiArray msg to JSON")
                            .try_into()
                            .expect("Failed to serialize ROS2 Int32MultiArray msg to internal representation")
                    }),
            ),
            RosMsgType::Int16 => Box::pin(
                node.subscribe::<r2r::std_msgs::msg::Int16>(topic, qos)?
                    .map(|val| Value::Int(val.data.into())),
            ),
            RosMsgType::Int8 => Box::pin(
                node.subscribe::<r2r::std_msgs::msg::Int8>(topic, qos)?
                    .map(|val| Value::Int(val.data.into())),
            ),
            RosMsgType::Float64 => Box::pin(
                node.subscribe::<r2r::std_msgs::msg::Float64>(topic, qos)?
                    .map(|val| Value::Float(val.data)),
            ),
            RosMsgType::Float32 => Box::pin(
                node.subscribe::<r2r::std_msgs::msg::Float32>(topic, qos)?
                    .map(|val| Value::Float(val.data.into())),
            ),
            RosMsgType::HumanModelPart => Box::pin(
                node.subscribe::<r2r::robo_sapiens_interfaces::msg::HumanModelPart>(topic, qos)?
                    .map(|val| {
                        serde_json::to_value(val)
                            .expect("Failed to serialize ROS2 HumanModelPart msg to JSON")
                            .try_into()
                            .expect("Failed to serialize ROS2 HumanModelPart msg to internal representation")
                    }),
            ),
            RosMsgType::HumanModel => Box::pin(
                node.subscribe::<r2r::robo_sapiens_interfaces::msg::HumanModel>(topic, qos)?
                    .map(|val| {
                        serde_json::to_value(val)
                            .expect("Failed to serialize ROS2 HumanModel msg to JSON")
                            .try_into()
                            .expect("Failed to serialize ROS2 HumanModel msg to internal representation")
                    }),
            ),
            RosMsgType::HumanModelList => Box::pin(
                node.subscribe::<r2r::robo_sapiens_interfaces::msg::HumanModelList>(topic, qos)?
                    .map(|val| {
                        serde_json::to_value(val)
                            .expect("Failed to serialize ROS2 HumanModelList msg to JSON")
                            .try_into()
                            .expect("Failed to serialize ROS2 HumanModelList msg to internal representation")
                    }),
            ),
            RosMsgType::RVData => Box::pin(
                node.subscribe::<r2r::id_pose_msgs::msg::RVData>(topic, qos)?
                    .map(|val| {
                        serde_json::to_value(val)
                            .expect("Failed to serialize ROS2 RVData msg to JSON")
                            .try_into()
                            .expect("Failed to serialize ROS2 RVData msg to internal representation")
                    }),
            ),
            RosMsgType::RVDataArray => Box::pin(
                node.subscribe::<r2r::id_pose_msgs::msg::RVDataArray>(topic, qos)?
                    .map(|val| {
                        serde_json::to_value(val)
                            .expect("Failed to serialize ROS2 RVDataArray msg to JSON")
                            .try_into()
                            .expect("Failed to serialize ROS2 RVDataArray msg to internal representation")
                    }),
            ),
            RosMsgType::Pose2D => Box::pin(
                node.subscribe::<r2r::geometry_msgs::msg::Pose2D>(topic, qos)?
                    .map(|val| {
                        Value::Map(BTreeMap::from([
                            ("x".into(), Value::Float(val.x)),
                            ("y".into(), Value::Float(val.y)),
                            ("theta".into(), Value::Float(val.theta)),
                        ]))
                    }),
            ),
            RosMsgType::Odom => Box::pin(
                node.subscribe::<r2r::nav_msgs::msg::Odometry>(topic, qos)?
                    .map(|val| {
                        serde_json::to_value(val)
                            .expect("Failed to serialize ROS2 Odometry msg to JSON")
                            .try_into()
                            .expect(
                                "Failed to serialize ROS2 Odometry msg to internal representation",
                            )
                    }),
            ),
            RosMsgType::MstloTimedValue => {
                anyhow::bail!(
                    "MstloTimedValue ROS input requires an MstloTimedValue stream, not Value"
                )
            }
        })
    }
}

/// Subscribe to a ROS `std_msgs/String` control topic.
pub(crate) fn control_stream(
    executor: Rc<LocalExecutor<'static>>,
    topic: String,
) -> anyhow::Result<(
    LocalStream<anyhow::Result<ReconfigurationRequest>>,
    RosInputControl,
)> {
    let context = r2r::Context::create()?;
    let node_name = format!("input_control_{}", Uuid::new_v4().simple());
    let mut node = r2r::Node::create(context, &node_name, "")?;
    let subscription =
        node.subscribe::<r2r::std_msgs::msg::String>(&topic, r2r::QosProfile::default())?;

    let cancellation_token = CancellationToken::new();
    let cancellation_for_spin = cancellation_token.clone();
    let spinner = executor.spawn(async move {
        let mut spin_ticks = smol::Timer::interval(ROS_SPIN_INTERVAL);
        loop {
            select! {
                _ = cancellation_for_spin.cancelled().fuse() => return,
                _ = spin_ticks.next().fuse() => node.spin_once(ROS_SPIN_TIMEOUT),
            }
        }
    });

    let stream = Box::pin(async_stream::try_stream! {
        let mut subscription = subscription;
        while let Some(message) = subscription.next().await {
            let request = ReconfigurationRequest::from_json(&message.data)
                .with_context(|| format!("invalid ROS monitor configuration on `{topic}`"))?;
            yield request;
        }
    });
    Ok((stream, RosInputControl::new(cancellation_token, spinner)))
}

/// Subscribe to ROS topics and return a stream that owns the subscriber lifetime.
#[instrument(level = Level::INFO, skip(var_topics))]
pub fn open_ros_input(
    executor: Rc<LocalExecutor<'static>>,
    var_topics: RosStreamMapping,
) -> anyhow::Result<(InputStream<Value>, RosInputControl)> {
    if var_topics.is_empty() {
        return Ok((empty_input_stream(), RosInputControl::inactive()));
    }
    let mapping = var_topics
        .into_iter()
        .map(|(variable, data)| {
            Ok((
                variable,
                (
                    data.topic,
                    super::ros_topic_stream_mapping::ros_msg_type_to_string(data.msg_type)?,
                ),
            ))
        })
        .collect::<anyhow::Result<_>>()?;
    let (stream, owner) = open_reconfigurable_ros_input(executor, mapping)?;
    let stream = Box::pin(stream.filter_map(|item| async move {
        match item {
            Ok(RosInputItem::Data(batch)) => Some(Ok(batch)),
            Ok(RosInputItem::Boundary(_)) => None,
            Err(error) => Some(Err(crate::InputError::source(error))),
        }
    }));
    Ok((stream, owner))
}

pub(crate) fn open_reconfigurable_ros_input(
    executor: Rc<LocalExecutor<'static>>,
    mapping: RawMapping,
) -> anyhow::Result<(RosInputStream<Value>, RosInputControl)> {
    open_managed_ros_input(executor, mapping, create_value_streams)
}

fn create_value_streams(
    node: &mut r2r::Node,
    mapping: &RawMapping,
) -> anyhow::Result<RawStreams<Value>> {
    let var_topics = super::raw_mapping_to_ros(mapping.clone())?;
    var_topics
        .into_iter()
        .map(|(variable, data)| {
            let stream =
                data.msg_type
                    .node_output_stream(node, &data.topic, r2r::QosProfile::default())?;
            Ok((
                VarName::new(&variable),
                Box::pin(stream.map(Ok)) as LocalStream<anyhow::Result<Value>>,
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::time::Duration;

    use super::*;

    fn empty_raw_streams(
        _node: &mut r2r::Node,
        _mapping: &RawMapping,
    ) -> anyhow::Result<RawStreams<Value>> {
        Ok(BTreeMap::new())
    }

    fn queued_raw_streams(
        _node: &mut r2r::Node,
        mapping: &RawMapping,
    ) -> anyhow::Result<RawStreams<Value>> {
        Ok(mapping
            .keys()
            .map(|variable| {
                let stream = futures::stream::iter([
                    Ok(Value::Int(1)),
                    Ok(Value::Int(2)),
                    Ok(Value::Int(3)),
                ]);
                (
                    VarName::new(variable),
                    Box::pin(stream) as LocalStream<anyhow::Result<Value>>,
                )
            })
            .collect())
    }

    fn reject_nonempty_mapping(
        _node: &mut r2r::Node,
        mapping: &RawMapping,
    ) -> anyhow::Result<RawStreams<Value>> {
        anyhow::ensure!(mapping.is_empty(), "deliberate stream creation failure");
        Ok(BTreeMap::new())
    }

    struct EndsOnce {
        polls: Rc<Cell<usize>>,
    }

    impl futures::Stream for EndsOnce {
        type Item = anyhow::Result<Value>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
            let polls = self.polls.get();
            assert_eq!(polls, 0, "completed ROS stream was polled again");
            self.polls.set(polls + 1);
            Poll::Ready(None)
        }
    }

    async fn assert_shutdown_completes(owner: &mut RosInputControl) {
        let shutdown = owner.shutdown();
        let timeout = smol::Timer::after(Duration::from_millis(250));
        futures::pin_mut!(shutdown, timeout);
        assert!(
            matches!(
                futures::future::select(shutdown, timeout).await,
                futures::future::Either::Left((Ok(()), _))
            ),
            "ROS input shutdown timed out"
        );
    }

    #[test]
    fn shutdown_joins_spinner_before_completing() {
        let executor = Rc::new(LocalExecutor::new());
        let task_executor = Rc::clone(&executor);
        smol::block_on(executor.run(async move {
            let cancellation = CancellationToken::new();
            let worker_cancellation = cancellation.clone();
            let stopped = Rc::new(Cell::new(false));
            let worker_stopped = Rc::clone(&stopped);
            let spinner = task_executor.spawn(async move {
                worker_cancellation.cancelled().await;
                smol::Timer::after(std::time::Duration::from_millis(10)).await;
                worker_stopped.set(true);
            });
            let mut owner = RosInputControl::new(cancellation, spinner);

            owner.shutdown().await.unwrap();

            assert!(stopped.get(), "shutdown returned before the spinner joined");
        }));
    }

    #[test]
    fn managed_shutdown_completes_while_paused() {
        let executor = Rc::new(LocalExecutor::new());
        let task_executor = Rc::clone(&executor);
        smol::block_on(executor.run(async move {
            let (mut stream, mut owner) =
                open_managed_ros_input(task_executor, BTreeMap::new(), empty_raw_streams).unwrap();

            assert!(owner.pause(17).await.unwrap());
            assert!(matches!(
                stream.next().await.unwrap().unwrap(),
                RosInputItem::Boundary(17)
            ));

            assert_shutdown_completes(&mut owner).await;
        }));
    }

    #[test]
    fn managed_shutdown_completes_with_a_full_output_queue() {
        let executor = Rc::new(LocalExecutor::new());
        let task_executor = Rc::clone(&executor);
        smol::block_on(executor.run(async move {
            let mapping =
                BTreeMap::from([("x".to_owned(), ("/unused".to_owned(), "Int32".to_owned()))]);
            let (_stream, mut owner) =
                open_managed_ros_input(task_executor, mapping, queued_raw_streams).unwrap();

            smol::future::yield_now().await;
            assert_shutdown_completes(&mut owner).await;
        }));
    }

    #[test]
    fn completed_raw_streams_are_removed_before_the_next_poll() {
        smol::block_on(async {
            let polls = Rc::new(Cell::new(0));
            let mapping =
                BTreeMap::from([("x".to_owned(), ("/unused".to_owned(), "Int32".to_owned()))]);
            let streams = BTreeMap::from([(
                VarName::new("x"),
                Box::pin(EndsOnce {
                    polls: Rc::clone(&polls),
                }) as LocalStream<anyhow::Result<Value>>,
            )]);
            let mut streams = RosStreams::new(&mapping, streams);

            assert!(streams.next().await.is_none());
            assert!(streams.next().await.is_none());
            assert_eq!(polls.get(), 1);
        });
    }

    #[test]
    fn removing_a_completed_raw_stream_preserves_round_robin_order() {
        smol::block_on(async {
            let ended_polls = Rc::new(Cell::new(0));
            let mapping = BTreeMap::from([
                ("a".to_owned(), ("/a".to_owned(), "Int32".to_owned())),
                ("b".to_owned(), ("/b".to_owned(), "Int32".to_owned())),
                ("c".to_owned(), ("/c".to_owned(), "Int32".to_owned())),
            ]);
            let streams = BTreeMap::from([
                (
                    VarName::new("a"),
                    Box::pin(futures::stream::iter([
                        Ok(Value::Int(1)),
                        Ok(Value::Int(3)),
                    ])) as LocalStream<anyhow::Result<Value>>,
                ),
                (
                    VarName::new("b"),
                    Box::pin(EndsOnce {
                        polls: Rc::clone(&ended_polls),
                    }) as LocalStream<anyhow::Result<Value>>,
                ),
                (
                    VarName::new("c"),
                    Box::pin(futures::stream::iter([
                        Ok(Value::Int(2)),
                        Ok(Value::Int(4)),
                    ])) as LocalStream<anyhow::Result<Value>>,
                ),
            ]);
            let mut streams = RosStreams::new(&mapping, streams);

            let mut observed = Vec::new();
            for _ in 0..4 {
                let (variable, value) = streams.next().await.unwrap().unwrap();
                observed.push((variable, value));
            }
            assert_eq!(
                observed,
                [
                    (VarName::new("a"), Value::Int(1)),
                    (VarName::new("c"), Value::Int(2)),
                    (VarName::new("a"), Value::Int(3)),
                    (VarName::new("c"), Value::Int(4)),
                ]
            );
            assert_eq!(ended_polls.get(), 1);
        });
    }

    #[test]
    fn failed_active_rebind_stops_the_managed_stream() {
        let executor = Rc::new(LocalExecutor::new());
        let task_executor = Rc::clone(&executor);
        smol::block_on(executor.run(async move {
            let (mut stream, mut owner) =
                open_managed_ros_input(task_executor, BTreeMap::new(), reject_nonempty_mapping)
                    .unwrap();
            let mapping =
                BTreeMap::from([("x".to_owned(), ("/unused".to_owned(), "Int32".to_owned()))]);

            assert!(owner.rebind(mapping).await.is_err());
            assert!(stream.next().await.is_none());
            assert_shutdown_completes(&mut owner).await;
        }));
    }

    #[test]
    fn failed_paused_rebind_stops_the_managed_stream() {
        let executor = Rc::new(LocalExecutor::new());
        let task_executor = Rc::clone(&executor);
        smol::block_on(executor.run(async move {
            let (mut stream, mut owner) =
                open_managed_ros_input(task_executor, BTreeMap::new(), reject_nonempty_mapping)
                    .unwrap();
            assert!(owner.pause(23).await.unwrap());
            assert!(matches!(
                stream.next().await.unwrap().unwrap(),
                RosInputItem::Boundary(23)
            ));
            let mapping =
                BTreeMap::from([("x".to_owned(), ("/unused".to_owned(), "Int32".to_owned()))]);

            assert!(owner.rebind(mapping).await.is_err());
            assert!(stream.next().await.is_none());
            assert_shutdown_completes(&mut owner).await;
        }));
    }
}
