use std::collections::BTreeMap;
use std::rc::Rc;

use async_cell::unsync::AsyncCell;
use futures::FutureExt;
use futures::select;
use futures::{StreamExt, future::LocalBoxFuture};
use r2r;
use smol::LocalExecutor;
use tracing::{Level, instrument};

use super::ros_topic_stream_mapping::{ROSMsgType, ROSStreamMapping, VariableMappingData};

use crate::stream_utils::drop_guard_stream;
use crate::utils::cancellation_token::CancellationToken;
use crate::{InputProvider, OutputStream, Value, core::VarName};

pub struct VarData {
    pub mapping_data: VariableMappingData,
    stream: Option<OutputStream<Value>>,
}

pub struct ROSInputProvider {
    #[allow(dead_code)]
    executor: Rc<LocalExecutor<'static>>,
    pub var_map: BTreeMap<VarName, VarData>,
    pub result: Rc<AsyncCell<anyhow::Result<()>>>,
    pub started: Rc<AsyncCell<bool>>,
    // node: Arc<Mutex<r2r::Node>>,
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
            ROSMsgType::Human => Box::pin(
                        node.subscribe::<r2r::robo_sapiens_interfaces::msg::Human>(topic, qos)?
                            .map(|val| {
                                serde_json::to_value(val)
                                    .expect("Failed to serialize ROS2 Human msg to JSON")
                                    .try_into()
                                    .expect("Failed to serialize ROS2 Human msg to internal representation")
                            }),
                    ),
            ROSMsgType::HumanList => Box::pin(
                        node.subscribe::<r2r::robo_sapiens_interfaces::msg::HumanList>(topic, qos)?
                            .map(|val| {
                                serde_json::to_value(val)
                                    .expect("Failed to serialize ROS2 HumanList msg to JSON")
                                    .try_into()
                                    .expect("Failed to serialize ROS2 HumanList msg to internal representation")
                            }),
                    ),
            ROSMsgType::HumanBodyPart => Box::pin(
                        node.subscribe::<r2r::robo_sapiens_interfaces::msg::HumanBodyPart>(topic, qos)?
                            .map(|val| {
                                serde_json::to_value(val)
                                    .expect("Failed to serialize ROS2 HumanBodyPart msg to JSON")
                                    .try_into()
                                    .expect("Failed to serialize ROS2 HumanBodyPart msg to internal representation")
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

        // Provide streams for all input variables
        let mut var_map = BTreeMap::new();
        for (var_name, var_data) in var_topics.into_iter() {
            let qos = r2r::QosProfile::default();
            let stream = var_data
                .msg_type
                .node_output_stream(&mut node, &var_data.topic, qos)?;
            // Apply a drop guard to the stream to ensure that the
            // subscriber ROS node does not go away whilst the stream
            // is still being consumed
            let stream = drop_guard_stream(stream, drop_guard.clone());
            var_map.insert(
                VarName::from(var_name),
                VarData {
                    mapping_data: var_data,
                    stream: Some(stream),
                },
            );
        }

        // Launch the ROS subscriber node in background async task
        executor
            .spawn(async move {
                loop {
                    select! {
                        _ = cancellation_token.cancelled().fuse() => {
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
            executor,
            var_map,
            result,
            started,
        })
    }
}

impl InputProvider for ROSInputProvider {
    type Val = Value;

    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<Value>> {
        let var_data = self.var_map.get_mut(var)?;
        let stream = var_data.stream.take()?;
        Some(stream)
    }

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.result.take_shared())
    }

    fn ready(&self) -> LocalBoxFuture<'static, Result<(), anyhow::Error>> {
        Box::pin(futures::future::ready(Ok(())))
    }
}
