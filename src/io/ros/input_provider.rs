use async_cell::unsync::AsyncCell;
use async_stream::stream;
use async_trait::async_trait;
use futures::FutureExt;
use futures::future;
use futures::select;
use futures::stream;
use futures::{StreamExt, future::LocalBoxFuture};
use r2r;
use smol::LocalExecutor;
use std::collections::BTreeMap;
use std::rc::Rc;
use tracing::debug;
use tracing::info_span;
use tracing::{Level, instrument};
use unsync::spsc;

use super::ros_topic_stream_mapping::{ROSMsgType, ROSStreamMapping, VariableMappingData};

use crate::core::InputProviderNew;
use crate::stream_utils::channel_to_output_stream;
use crate::stream_utils::drop_guard_stream;
use crate::utils::cancellation_token::CancellationToken;
use crate::{InputProvider, OutputStream, Value, core::VarName};

pub type Topic = String;
// A map between channel names and the MQTT channels they
// correspond to
pub type VarTopicMap = BTreeMap<VarName, Topic>;
pub type InverseVarTopicMap = BTreeMap<Topic, VarName>;

pub const QOS: i32 = 1;
pub const CHANNEL_SIZE: usize = 10;

pub struct ROSInputProvider {
    pub var_map: BTreeMap<VarName, VariableMappingData>,
    pub result: Rc<AsyncCell<anyhow::Result<()>>>,
    pub started: Rc<AsyncCell<bool>>,

    // Streams that can be taken ownership of by calling `input_stream`
    available_streams: BTreeMap<VarName, OutputStream<Value>>,
    // Channels used to send to the `available_streams`
    senders: Option<BTreeMap<VarName, spsc::Sender<Value>>>,
    ros_streams: BTreeMap<VarName, OutputStream<Value>>,
    cancellation_token: CancellationToken,
}

impl ROSMsgType {
    /* Create a stream of values received on a ROS topic */
    fn node_output_stream(
        &self,
        node: &mut r2r::Node,
        topic: &str,
        qos: r2r::QosProfile,
    ) -> Result<OutputStream<Value>, r2r::Error> {
        Ok(match self {
            ROSMsgType::Bool => Box::pin(
                        node.subscribe::<r2r::std_msgs::msg::Bool>(topic, qos)?
                            .map(|val| Value::Bool(val.data)),
                    ),
            ROSMsgType::String => Box::pin(
                        node.subscribe::<r2r::std_msgs::msg::String>(topic, qos)?
                            .map(|val| Value::Str(val.data.into())),
                    ),
            ROSMsgType::Int64 => Box::pin(
                        node.subscribe::<r2r::std_msgs::msg::Int64>(topic, qos)?
                            .map(|val| Value::Int(val.data)),
                    ),
            ROSMsgType::Int32 => Box::pin(
                        node.subscribe::<r2r::std_msgs::msg::Int32>(topic, qos)?
                            .map(|val| Value::Int(val.data.into())),
                    ),
            ROSMsgType::Int32List => Box::pin(
                node.subscribe::<r2r::std_msgs::msg::Int32MultiArray>(topic, qos)?
                    .map(|val| {
                        serde_json::to_value(val.data)
                            .expect("Failed to serialize ROS2 Int32MultiArray msg to JSON")
                            .try_into()
                            .expect("Failed to serialize ROS2 Int32MultiArray msg to internal representation")
                    }),
                    ),
            ROSMsgType::Int16 => Box::pin(
                        node.subscribe::<r2r::std_msgs::msg::Int16>(topic, qos)?
                            .map(|val| Value::Int(val.data.into())),
                    ),
            ROSMsgType::Int8 => Box::pin(
                        node.subscribe::<r2r::std_msgs::msg::Int8>(topic, qos)?
                            .map(|val| Value::Int(val.data.into())),
                    ),
            ROSMsgType::Float64 => Box::pin(
                        node.subscribe::<r2r::std_msgs::msg::Float64>(topic, qos)?
                            .map(|val| Value::Float(val.data)),
                    ),
            ROSMsgType::Float32 => Box::pin(
                        node.subscribe::<r2r::std_msgs::msg::Float32>(topic, qos)?
                            .map(|val| Value::Float(val.data.into())),
                    ),
            ROSMsgType::HumanModelPart => Box::pin(
                        node.subscribe::<r2r::robo_sapiens_interfaces::msg::HumanModelPart>(topic, qos)?
                            .map(|val| {
                                serde_json::to_value(val)
                                    .expect("Failed to serialize ROS2 HumanModelPart msg to JSON")
                                    .try_into()
                                    .expect("Failed to serialize ROS2 HumanModelPart msg to internal representation")
                            }),
                    ),
            ROSMsgType::HumanModel => Box::pin(
                        node.subscribe::<r2r::robo_sapiens_interfaces::msg::HumanModel>(topic, qos)?
                            .map(|val| {
                                serde_json::to_value(val)
                                    .expect("Failed to serialize ROS2 HumanModel msg to JSON")
                                    .try_into()
                                    .expect("Failed to serialize ROS2 HumanModel msg to internal representation")
                            }),
                    ),
            ROSMsgType::HumanModelList => Box::pin(
                        node.subscribe::<r2r::robo_sapiens_interfaces::msg::HumanModelList >(topic, qos)?
                            .map(|val| {
                                serde_json::to_value(val)
                                    .expect("Failed to serialize ROS2 HumanModelList msg to JSON")
                                    .try_into()
                                    .expect("Failed to serialize ROS2 HumanModelList msg to internal representation")
                            }),
                    ),
            ROSMsgType::RVData => Box::pin(
                        node.subscribe::<r2r::id_pose_msgs::msg::RVData>(topic, qos)?
                            .map(|val| {
                                serde_json::to_value(val)
                                    .expect("Failed to serialize ROS2 RVData msg to JSON")
                                    .try_into()
                                    .expect("Failed to serialize ROS2 RVData msg to internal representation")
                            }),
                    ),
            ROSMsgType::RVDataArray => Box::pin(
                        node.subscribe::<r2r::id_pose_msgs::msg::RVDataArray>(topic, qos)?
                            .map(|val| {
                                serde_json::to_value(val)
                                    .expect("Failed to serialize ROS2 RVDataArray msg to JSON")
                                    .try_into()
                                    .expect("Failed to serialize ROS2 RVDataArray msg to internal representation")
                            }),
                    ),
        })
    }
}

impl ROSInputProvider {
    #[instrument(level = Level::INFO, skip(var_topics))]
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        var_topics: ROSStreamMapping,
    ) -> Result<Self, r2r::Error> {
        // Create a ROS node to subscribe to all of the input topics
        let ctx = r2r::Context::create()?;
        let mut node = r2r::Node::create(ctx, "input_monitor", "")?;

        // Cancellation token to stop the subscriber node
        // if all consumers of the output streams have
        // gone away
        let cancellation_token = CancellationToken::new();
        let drop_guard = Rc::new(cancellation_token.clone().drop_guard());
        let cancellation_token_task = cancellation_token.clone();
        let var_topics_shallow: BTreeMap<VarName, Topic> = var_topics
            .iter()
            .map(|(k, v)| (k.clone().into(), v.topic.clone()))
            .collect();
        let (senders, available_streams) = Self::create_senders_receiver(var_topics_shallow.iter());
        let senders = Some(senders);

        // Provide streams for all input variables
        let mut var_map = BTreeMap::new();
        let mut ros_streams = BTreeMap::new();
        for (var_name, var_data) in var_topics.into_iter() {
            let qos = r2r::QosProfile::default();
            let stream = var_data
                .msg_type
                .node_output_stream(&mut node, &var_data.topic, qos)?;
            // Apply a drop guard to the stream to ensure that the
            // subscriber ROS node does not go away whilst the stream
            // is still being consumed
            let stream = drop_guard_stream(stream, drop_guard.clone());
            var_map.insert(VarName::from(var_name.clone()), var_data);
            ros_streams.insert(VarName::from(var_name), stream);
        }

        // TODO: Should not be spawning a task inside new. Should fix the potential race conditions
        // instead of circumventing them like this.
        // Launch the ROS subscriber node in background async task
        executor
            .spawn(async move {
                loop {
                    select! {
                        _ = cancellation_token_task.cancelled().fuse() => {
                            return;
                        },
                        _ = smol::future::yield_now().fuse() => {
                            node.spin_once(std::time::Duration::from_millis(0));
                        },
                    }
                }
            })
            .detach();

        let started = AsyncCell::new_with(false).into_shared();
        let result = AsyncCell::shared();

        Ok(Self {
            var_map,
            result,
            started,
            ros_streams,
            available_streams,
            senders,
            cancellation_token,
        })
    }

    fn create_senders_receiver<'a, I>(
        var_topics: I,
    ) -> (
        BTreeMap<VarName, spsc::Sender<Value>>,
        BTreeMap<VarName, OutputStream<Value>>,
    )
    where
        I: IntoIterator<Item = (&'a VarName, &'a Topic)>,
    {
        let (senders, receivers): (
            BTreeMap<_, spsc::Sender<Value>>,
            BTreeMap<_, OutputStream<Value>>,
        ) = var_topics
            .into_iter()
            .map(|(v, _)| {
                let (tx, rx) = unsync::spsc::channel(CHANNEL_SIZE);
                let rx = channel_to_output_stream(rx);
                ((v.clone(), tx), (v.clone(), rx))
            })
            .unzip();
        (senders, receivers)
    }

    async fn receive_from_any_stream(
        ros_streams: &mut BTreeMap<VarName, OutputStream<Value>>,
    ) -> (VarName, Option<Value>) {
        ros_streams
            .iter_mut()
            .map(|(var_name, stream)| stream.next().map(|val| (var_name.clone(), val)))
            .collect::<stream::FuturesUnordered<_>>()
            .next()
            .await
            .expect("No streams available")
    }

    async fn handle_received_value(
        var: VarName,
        value: Value,
        senders: &mut BTreeMap<VarName, spsc::Sender<Value>>,
    ) -> anyhow::Result<()> {
        // Get the sender for this variable
        let sender = senders
            .get_mut(&var)
            .ok_or_else(|| anyhow::anyhow!("No sender found for variable {}", var))?;

        debug!("ROSInputProvider: Got sender");
        // Forward the value to sender
        sender
            .send(value)
            .await
            .map_err(|_| anyhow::anyhow!("Failed to send value"))?;

        debug!("ROSInputProvider: Sent to sender");
        // Send `NoVal` to all other senders concurrently
        let futs =
            senders
                .iter_mut()
                .filter(|(name, _)| **name != var)
                .map(|(name, s)| async move {
                    debug!("ROSInputProvider: Sending NoVal to variable {}", name);
                    let r = s.send(Value::NoVal).await;
                    debug!("ROSInputProvider: Sent NoVal to variable {}", name);
                    r
                });

        // Run them all concurrently
        debug!("ROSInputProvider: Awaiting results");
        let results = future::join_all(futs).await;
        debug!("ROSInputProvider: Awaited results");

        // Check for errors
        if results.iter().any(|r| r.is_err()) {
            anyhow::bail!("Failed to send NoVal");
        }

        Ok(())
    }

    async fn run_logic(
        ros_streams: BTreeMap<VarName, OutputStream<Value>>,
        senders: BTreeMap<VarName, spsc::Sender<Value>>,
        cancellation_token: CancellationToken,
    ) -> anyhow::Result<()> {
        let mut stream = Self::create_run_stream(ros_streams, senders, cancellation_token).await;
        while let Some(res) = stream.next().await {
            match res {
                Ok(()) => continue,
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    async fn create_run_stream(
        mut ros_streams: BTreeMap<VarName, OutputStream<Value>>,
        mut senders: BTreeMap<VarName, spsc::Sender<Value>>,
        cancellation_token: CancellationToken,
    ) -> OutputStream<anyhow::Result<()>> {
        Box::pin(stream! {
            let mqtt_input_span = info_span!("ROSInputProvider run_logic");
            let _enter = mqtt_input_span.enter();
            loop {
                futures::select! {
                    (var, val) = Self::receive_from_any_stream(&mut ros_streams).fuse() => {
                        match val {
                            Some(value) => {
                                debug!("ROSInputProvider: Received value for variable {}", var);
                                if let Err(e) = Self::handle_received_value(var, value, &mut senders).await {
                                    debug!("ROSInputProvider: Error handling received value: {:?}", e);
                                    yield Err(e);
                                    return;
                                }
                            }
                            None => {
                                debug!("ROSInputProvider: Stream for variable {} ended", var);
                                // Remove the stream from the map
                                ros_streams.remove(&var);
                                // If all streams have ended, exit the loop
                                if ros_streams.is_empty() {
                                    debug!("ROSInputProvider: All streams ended, exiting");
                                    return;
                                }
                            }
                        }
                    }
                    _ = cancellation_token.cancelled().fuse() => {
                        debug!("ROSInputProvider: Input monitor task cancelled");
                        return;
                    }
                }
            }
        })
    }
}

impl InputProvider for ROSInputProvider {
    type Val = Value;

    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<Value>> {
        let stream = self.available_streams.remove(var)?;
        Some(stream)
    }

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let senders = std::mem::take(&mut self.senders).expect("Senders already taken");
        let cancellation_token = self.cancellation_token.clone();
        let ros_streams = std::mem::take(&mut self.ros_streams);
        Box::pin(Self::run_logic(ros_streams, senders, cancellation_token))
    }

    fn ready(&self) -> LocalBoxFuture<'static, Result<(), anyhow::Error>> {
        Box::pin(futures::future::ready(Ok(())))
    }

    fn vars(&self) -> Vec<VarName> {
        self.var_map.keys().cloned().collect()
    }
}

#[async_trait(?Send)]
impl InputProviderNew for ROSInputProvider {
    type Val = Value;
    fn var_stream(&mut self, var: &VarName) -> Option<OutputStream<Value>> {
        let stream = self.available_streams.remove(var)?;
        Some(stream)
    }

    async fn control_stream(&mut self) -> OutputStream<anyhow::Result<()>> {
        let senders = std::mem::take(&mut self.senders).expect("Senders already taken");
        let cancellation_token = self.cancellation_token.clone();
        let ros_streams = std::mem::take(&mut self.ros_streams);
        Self::create_run_stream(ros_streams, senders, cancellation_token).await
    }

    fn vars_new(&self) -> Vec<VarName> {
        self.var_map.keys().cloned().collect()
    }
}
