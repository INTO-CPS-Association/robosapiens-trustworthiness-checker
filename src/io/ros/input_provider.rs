use async_stream::stream;
use async_trait::async_trait;
use futures::FutureExt;
use futures::StreamExt;
use futures::future;
use futures::select;
use r2r;
use smol::LocalExecutor;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use tracing::debug;
use tracing::info;
use tracing::info_span;
use tracing::{Level, instrument};
use unsync::spsc;
use uuid::Uuid;

use super::{
    ROS_SPIN_INTERVAL, ROS_SPIN_TIMEOUT,
    ros_topic_stream_mapping::{RosMsgType, RosStreamMapping, VariableMappingData},
};

use crate::stream_utils::channel_to_output_stream;
use crate::stream_utils::drop_guard_stream;
use crate::utils::cancellation_token::CancellationToken;
use crate::{InputProvider, OutputStream, Value, core::VarName};

pub type Topic = String;
// A map between channel names and the ROS topics they correspond to
pub type VarTopicMap = BTreeMap<VarName, Topic>;
pub type InverseVarTopicMap = BTreeMap<Topic, VarName>;

pub const QOS: i32 = 1;
pub const CHANNEL_SIZE: usize = 10;

/// ROS input provider.
pub struct RosInputProvider {
    pub var_map: BTreeMap<VarName, VariableMappingData>,

    // Streams that can be taken ownership of by calling `var_stream`.
    available_streams: BTreeMap<VarName, OutputStream<Value>>,
    // Channels used to send to the `available_streams`.
    senders: Option<BTreeMap<VarName, spsc::Sender<Value>>>,
    ros_streams: BTreeMap<VarName, OutputStream<Value>>,
    cancellation_token: CancellationToken,
}

impl RosMsgType {
    /* Create a stream of values received on a ROS topic */
    fn node_output_stream(
        &self,
        node: &mut r2r::Node,
        topic: &str,
        qos: r2r::QosProfile,
    ) -> Result<OutputStream<Value>, r2r::Error> {
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
        })
    }
}

impl RosInputProvider {
    #[instrument(level = Level::INFO, skip(var_topics))]
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        var_topics: RosStreamMapping,
    ) -> Result<Self, r2r::Error> {
        // Create a ROS node to subscribe to all of the input topics
        let ctx = r2r::Context::create()?;
        let uuid = Uuid::new_v4().simple().to_string();
        let mut node = r2r::Node::create(ctx, format!("input_monitor_{}", uuid).as_str(), "")?;

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
        info!(
            "ROSInputProvider: Got variable-topic mapping: {:?}",
            var_topics_shallow
        );
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
                let mut spin_ticks = smol::Timer::interval(ROS_SPIN_INTERVAL);
                loop {
                    select! {
                        _ = cancellation_token_task.cancelled().fuse() => {
                            return;
                        },
                        _ = spin_ticks.next().fuse() => {
                            node.spin_once(ROS_SPIN_TIMEOUT);
                        },
                    }
                }
            })
            .detach();

        Ok(Self {
            var_map,
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

    fn merge_ros_streams(
        ros_streams: BTreeMap<VarName, OutputStream<Value>>,
    ) -> OutputStream<(VarName, Value)> {
        Box::pin(futures::stream::select_all(ros_streams.into_iter().map(
            |(var_name, stream)| {
                Box::pin(stream.map(move |value| (var_name.clone(), value)))
                    as OutputStream<(VarName, Value)>
            },
        )))
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

    async fn create_run_stream(
        ros_streams: BTreeMap<VarName, OutputStream<Value>>,
        mut senders: BTreeMap<VarName, spsc::Sender<Value>>,
        cancellation_token: CancellationToken,
    ) -> OutputStream<anyhow::Result<()>> {
        Box::pin(stream! {
            let ros_input_span = info_span!("ROSInputProvider run_logic");
            let _enter = ros_input_span.enter();
            let mut ros_events = Self::merge_ros_streams(ros_streams);

            loop {
                futures::select! {
                    event = ros_events.next().fuse() => {
                        match event {
                            Some((var, value)) => {
                                info!(
                                    "ROSInputProvider: Received value from stream for variable {:?}: {:?}",
                                    var, value
                                );

                                if let Err(e) = Self::handle_received_value(var, value, &mut senders).await {
                                    debug!("ROSInputProvider: Error handling received value: {:?}", e);
                                    yield Err(e);
                                    return;
                                } else {
                                    yield Ok(());
                                }
                            }
                            None => {
                                debug!("ROSInputProvider: All streams ended, exiting");
                                return;
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

#[async_trait(?Send)]
impl InputProvider for RosInputProvider {
    type Val = Value;

    fn var_stream(&mut self, var: &VarName) -> Option<OutputStream<Value>> {
        let stream = self.available_streams.remove(var)?;
        Some(stream)
    }

    fn event_stream(&mut self, vars: &BTreeSet<VarName>) -> Option<OutputStream<(VarName, Value)>> {
        let selected = vars
            .iter()
            .filter_map(|var| self.ros_streams.remove_entry(var))
            .collect::<BTreeMap<_, _>>();

        (!selected.is_empty()).then(|| Self::merge_ros_streams(selected))
    }

    fn event_stream_needs_control(&self) -> bool {
        false
    }

    async fn control_stream(&mut self) -> OutputStream<anyhow::Result<()>> {
        let senders = std::mem::take(&mut self.senders).expect("Senders already taken");
        let cancellation_token = self.cancellation_token.clone();
        let ros_streams = std::mem::take(&mut self.ros_streams);

        Self::create_run_stream(ros_streams, senders, cancellation_token).await
    }
}
