use futures::FutureExt;
use futures::StreamExt;
use futures::select;
use r2r;
use smol::LocalExecutor;
use std::collections::BTreeMap;
use std::rc::Rc;
use tracing::info;
use tracing::{Level, instrument};
use uuid::Uuid;

use super::{
    ROS_SPIN_INTERVAL, ROS_SPIN_TIMEOUT,
    ros_topic_stream_mapping::{RosMsgType, RosStreamMapping},
};

use crate::stream_utils::drop_guard_stream;
use crate::utils::cancellation_token::CancellationToken;
use crate::{InputBatch, InputStream, OutputStream, Value, core::VarName};

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

/// Subscribe to ROS topics and return a stream that owns the subscriber lifetime.
#[instrument(level = Level::INFO, skip(var_topics))]
pub fn input_stream(
    executor: Rc<LocalExecutor<'static>>,
    var_topics: RosStreamMapping,
) -> anyhow::Result<InputStream<Value>> {
    if var_topics.is_empty() {
        return Ok(Box::pin(futures::stream::empty()));
    }
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

    let var_topics_shallow: BTreeMap<VarName, String> = var_topics
        .iter()
        .map(|(k, v)| (k.clone().into(), v.topic.clone()))
        .collect();
    info!(
        "ROS input stream: got variable-topic mapping: {:?}",
        var_topics_shallow
    );
    // Provide streams for all input variables
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
        ros_streams.insert(VarName::from(var_name), stream);
    }

    // TODO: Should not be spawning a task during stream construction. Fix the potential race conditions
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

    Ok(Box::pin(merge_ros_streams(ros_streams).map(
        |(var, value)| Ok(InputBatch::events(vec![crate::InputEvent::new(var, value)])),
    )))
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
