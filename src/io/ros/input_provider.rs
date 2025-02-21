use std::collections::BTreeMap;
use std::sync::Arc;

use futures::StreamExt;
use r2r;
use tokio::select;
use tokio_util::sync::CancellationToken;

use super::ros_topic_stream_mapping::{ROSMsgType, ROSStreamMapping, VariableMappingData};

use crate::stream_utils::drop_guard_stream;
use crate::{core::VarName, InputProvider, OutputStream, Value};

pub struct VarData {
    pub mapping_data: VariableMappingData,
    stream: Option<OutputStream<Value>>,
}

pub struct ROSInputProvider {
    pub var_map: BTreeMap<VarName, VarData>,
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
                    .map(|val| Value::Str(val.data)),
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
        })
    }
}

impl ROSInputProvider {
    pub fn new(var_topics: ROSStreamMapping) -> Result<Self, r2r::Error> {
        // Create a ROS node to subscribe to all of the input topics
        let ctx = r2r::Context::create()?;
        let mut node = r2r::Node::create(ctx, "input_monitor", "")?;

        // Cancellation token to stop the subscriber node
        // if all consumers of the output streams have
        // gone away
        let cancellation_token = CancellationToken::new();
        let drop_guard = Arc::new(cancellation_token.clone().drop_guard());

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
                VarName(var_name),
                VarData {
                    mapping_data: var_data,
                    stream: Some(stream),
                },
            );
        }

        // Launch the ROS subscriber node in background async task
        tokio::task::spawn(async move {
            loop {
                select! {
                    biased;
                    _ = cancellation_token.cancelled() => {
                        return;
                    },
                    _ = tokio::task::yield_now() => {
                        node.spin_once(std::time::Duration::from_millis(0));
                    },
                }
            }
        });

        Ok(Self { var_map })
    }
}

impl InputProvider<Value> for ROSInputProvider {
    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<Value>> {
        let var_data = self.var_map.get_mut(var)?;
        let stream = var_data.stream.take()?;
        Some(stream)
    }
}
