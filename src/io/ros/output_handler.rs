use std::collections::BTreeMap;
use std::mem;
use std::rc::Rc;

use futures::future::{Either, LocalBoxFuture};
use futures::{FutureExt, StreamExt};
use smol::LocalExecutor;
use tracing::{Level, debug, error, info, instrument, warn};

use super::ros_topic_stream_mapping::{ROSMsgType, ROSStreamMapping};
use crate::core::OutputHandler;
use crate::utils::cancellation_token::CancellationToken;
use crate::{OutputStream, Value, core::VarName};

pub struct VarData {
    pub variable: VarName,
    /// None Option can be used if topic name is not set due to being an auxiliary variable
    pub topic_name: Option<String>,
    pub msg_type: Option<ROSMsgType>,
    stream: Option<OutputStream<Value>>,
}

pub struct ROSOutputHandler {
    executor: Rc<LocalExecutor<'static>>,
    pub node_name: String,
    pub var_names: Vec<VarName>,
    pub var_map: BTreeMap<VarName, VarData>,
    pub aux_info: Vec<VarName>,
}

/// A generic publisher that converts a dynamically typed `Value` and publishes it on a ROS topic.
trait ValuePublisher {
    fn publish_value(&self, value: &Value) -> anyhow::Result<()>;
}

struct TypedValuePublisher<T: r2r::WrappedTypesupport> {
    publisher: r2r::Publisher<T>,
    convert_and_publish: fn(&r2r::Publisher<T>, &Value) -> anyhow::Result<()>,
}

impl<T: r2r::WrappedTypesupport> ValuePublisher for TypedValuePublisher<T> {
    fn publish_value(&self, value: &Value) -> anyhow::Result<()> {
        (self.convert_and_publish)(&self.publisher, value)
    }
}

/// Create a ValuePublisher for the given message type on the given topic.
fn create_value_publisher(
    node: &mut r2r::Node,
    topic: &str,
    msg_type: &ROSMsgType,
) -> Result<Box<dyn ValuePublisher>, r2r::Error> {
    let qos = r2r::QosProfile::default();
    Ok(match msg_type {
        ROSMsgType::Bool => Box::new(TypedValuePublisher {
            publisher: node.create_publisher::<r2r::std_msgs::msg::Bool>(topic, qos)?,
            convert_and_publish: |pub_handle, value| match value {
                Value::Bool(v) => {
                    let msg = r2r::std_msgs::msg::Bool { data: *v };
                    pub_handle
                        .publish(&msg)
                        .map_err(|e| anyhow::anyhow!("Failed to publish Bool: {:?}", e))
                }
                _ => Err(anyhow::anyhow!(
                    "Expected Bool value for Bool publisher, got {:?}",
                    value
                )),
            },
        }),
        ROSMsgType::String => Box::new(TypedValuePublisher {
            publisher: node.create_publisher::<r2r::std_msgs::msg::String>(topic, qos)?,
            convert_and_publish: |pub_handle, value| match value {
                Value::Str(v) => {
                    let msg = r2r::std_msgs::msg::String {
                        data: v.to_string(),
                    };
                    pub_handle
                        .publish(&msg)
                        .map_err(|e| anyhow::anyhow!("Failed to publish String: {:?}", e))
                }
                _ => Err(anyhow::anyhow!(
                    "Expected Str value for String publisher, got {:?}",
                    value
                )),
            },
        }),
        ROSMsgType::Int64 => Box::new(TypedValuePublisher {
            publisher: node.create_publisher::<r2r::std_msgs::msg::Int64>(topic, qos)?,
            convert_and_publish: |pub_handle, value| match value {
                Value::Int(v) => {
                    let msg = r2r::std_msgs::msg::Int64 { data: *v };
                    pub_handle
                        .publish(&msg)
                        .map_err(|e| anyhow::anyhow!("Failed to publish Int64: {:?}", e))
                }
                _ => Err(anyhow::anyhow!(
                    "Expected Int value for Int64 publisher, got {:?}",
                    value
                )),
            },
        }),
        ROSMsgType::Int32 => Box::new(TypedValuePublisher {
            publisher: node.create_publisher::<r2r::std_msgs::msg::Int32>(topic, qos)?,
            convert_and_publish: |pub_handle, value| match value {
                Value::Int(v) => {
                    let msg = r2r::std_msgs::msg::Int32 { data: *v as i32 };
                    pub_handle
                        .publish(&msg)
                        .map_err(|e| anyhow::anyhow!("Failed to publish Int32: {:?}", e))
                }
                _ => Err(anyhow::anyhow!(
                    "Expected Int value for Int32 publisher, got {:?}",
                    value
                )),
            },
        }),
        ROSMsgType::Int16 => Box::new(TypedValuePublisher {
            publisher: node.create_publisher::<r2r::std_msgs::msg::Int16>(topic, qos)?,
            convert_and_publish: |pub_handle, value| match value {
                Value::Int(v) => {
                    let msg = r2r::std_msgs::msg::Int16 { data: *v as i16 };
                    pub_handle
                        .publish(&msg)
                        .map_err(|e| anyhow::anyhow!("Failed to publish Int16: {:?}", e))
                }
                _ => Err(anyhow::anyhow!(
                    "Expected Int value for Int16 publisher, got {:?}",
                    value
                )),
            },
        }),
        ROSMsgType::Int8 => Box::new(TypedValuePublisher {
            publisher: node.create_publisher::<r2r::std_msgs::msg::Int8>(topic, qos)?,
            convert_and_publish: |pub_handle, value| match value {
                Value::Int(v) => {
                    let msg = r2r::std_msgs::msg::Int8 { data: *v as i8 };
                    pub_handle
                        .publish(&msg)
                        .map_err(|e| anyhow::anyhow!("Failed to publish Int8: {:?}", e))
                }
                _ => Err(anyhow::anyhow!(
                    "Expected Int value for Int8 publisher, got {:?}",
                    value
                )),
            },
        }),
        ROSMsgType::Float64 => Box::new(TypedValuePublisher {
            publisher: node.create_publisher::<r2r::std_msgs::msg::Float64>(topic, qos)?,
            convert_and_publish: |pub_handle, value| match value {
                Value::Float(v) => {
                    let msg = r2r::std_msgs::msg::Float64 { data: *v };
                    pub_handle
                        .publish(&msg)
                        .map_err(|e| anyhow::anyhow!("Failed to publish Float64: {:?}", e))
                }
                _ => Err(anyhow::anyhow!(
                    "Expected Float value for Float64 publisher, got {:?}",
                    value
                )),
            },
        }),
        ROSMsgType::Float32 => Box::new(TypedValuePublisher {
            publisher: node.create_publisher::<r2r::std_msgs::msg::Float32>(topic, qos)?,
            convert_and_publish: |pub_handle, value| match value {
                Value::Float(v) => {
                    let msg = r2r::std_msgs::msg::Float32 { data: *v as f32 };
                    pub_handle
                        .publish(&msg)
                        .map_err(|e| anyhow::anyhow!("Failed to publish Float32: {:?}", e))
                }
                _ => Err(anyhow::anyhow!(
                    "Expected Float value for Float32 publisher, got {:?}",
                    value
                )),
            },
        }),
        // For complex types (Int32List, HumanModelPart, etc.), fall back to JSON serialization
        // via r2r::std_msgs::msg::String
        _ => {
            warn!(
                "Complex ROS msg type {:?} not directly supported for output; \
                 falling back to JSON-encoded std_msgs/String on topic {}",
                msg_type, topic
            );
            Box::new(TypedValuePublisher {
                publisher: node.create_publisher::<r2r::std_msgs::msg::String>(topic, qos)?,
                convert_and_publish: |pub_handle, value| {
                    let json_str = serde_json::to_string(value)
                        .map_err(|e| anyhow::anyhow!("Failed to serialize value to JSON: {}", e))?;
                    let msg = r2r::std_msgs::msg::String { data: json_str };
                    pub_handle
                        .publish(&msg)
                        .map_err(|e| anyhow::anyhow!("Failed to publish JSON String: {:?}", e))
                },
            })
        }
    })
}

/// Publish a stream of values via a ROS ValuePublisher; this assumes that the type of the
/// publisher is compatible with the stream type
#[instrument(level = Level::INFO, skip(stream, publisher))]
async fn publish_ros_stream(
    topic_name: String,
    mut stream: OutputStream<Value>,
    publisher: Box<dyn ValuePublisher>,
) {
    info!(
        "Starting ROS output stream publisher for topic: {}",
        topic_name
    );
    let mut message_count = 0;

    while let Some(data) = stream.next().await {
        message_count += 1;
        debug!(
            "Received value #{} from stream for topic {}: {:?}",
            message_count, topic_name, data
        );

        if data == Value::NoVal {
            continue;
        }

        match publisher.publish_value(&data) {
            Ok(_) => {
                debug!(
                    "Successfully published message #{} to topic {}",
                    message_count, topic_name
                );
            }
            Err(e) => {
                error!(
                    "Failed to publish message #{} to topic {}: {:?}",
                    message_count, topic_name, e
                );
            }
        }
    }

    debug!(
        "Exiting ROS publish stream for topic {} after processing {} messages",
        topic_name, message_count
    );
}

async fn await_stream(mut stream: OutputStream<Value>) {
    debug!("Starting to monitor auxiliary stream");
    let mut count = 0;
    while let Some(value) = stream.next().await {
        count += 1;
        debug!("Received auxiliary value #{}: {:?}", count, value);
    }
    debug!("Auxiliary stream ended after {} values", count);
}

impl ROSOutputHandler {
    #[instrument(level = Level::INFO, skip(var_topics))]
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        node_name: String,
        var_names: Vec<VarName>,
        var_topics: ROSStreamMapping,
        aux_info: Vec<VarName>,
    ) -> anyhow::Result<Self> {
        debug!(
            "Creating new ROS output handler for {} variables: {:?}",
            var_names.len(),
            var_names
        );

        let var_map = var_topics
            .into_iter()
            .map(|(var_name, mapping_data)| {
                let var = VarName::from(var_name);
                info!(
                    "Mapping variable {} to ROS topic {} with msg_type {:?}",
                    var, mapping_data.topic, mapping_data.msg_type
                );
                (
                    var.clone(),
                    VarData {
                        variable: var,
                        topic_name: Some(mapping_data.topic),
                        msg_type: Some(mapping_data.msg_type),
                        stream: None,
                    },
                )
            })
            .collect();

        debug!(
            "Created ROS output handler with {} variables",
            var_names.len()
        );

        Ok(ROSOutputHandler {
            executor,
            node_name,
            var_names,
            var_map,
            aux_info,
        })
    }

    async fn inner_handler(
        executor: Rc<LocalExecutor<'static>>,
        node_name: String,
        streams: Vec<(
            VarName,
            Option<String>,
            Option<ROSMsgType>,
            OutputStream<Value>,
        )>,
        aux_info: Vec<VarName>,
    ) -> anyhow::Result<()> {
        debug!("Starting ROS inner handler for {} streams", streams.len());

        // Create a ROS node for publishing
        let ctx = r2r::Context::create()
            .map_err(|e| anyhow::anyhow!("Failed to create ROS context: {:?}", e))?;
        let mut node = r2r::Node::create(ctx, node_name.as_str(), "")
            .map_err(|e| anyhow::anyhow!("Failed to create ROS node: {:?}", e))?;
        debug!("Created ROS publisher node 'tc_ros_output'");

        // Cancellation token to stop spinning the node
        let cancellation_token = CancellationToken::new();

        // Create publishers and build per-stream futures
        let stream_futures = streams
            .into_iter()
            .map(|(var_name, topic_name, msg_type, stream)| {
                if aux_info.contains(&var_name) {
                    debug!(
                        "Variable '{}' is auxiliary, not publishing to ROS",
                        var_name
                    );
                    Either::Left(await_stream(stream))
                } else {
                    let topic_name = topic_name
                        .expect("topic_name must be provided for non-auxiliary variables");
                    let msg_type =
                        msg_type.expect("msg_type must be provided for non-auxiliary variables");
                    let publisher = create_value_publisher(&mut node, &topic_name, &msg_type)
                        .unwrap_or_else(|e| {
                            panic!(
                                "Failed to create ROS publisher for topic '{}': {:?}",
                                topic_name, e
                            )
                        });
                    debug!(
                        "Setting up ROS stream publisher for variable '{}' to topic '{}'",
                        var_name, topic_name
                    );
                    Either::Right(publish_ros_stream(topic_name, stream, publisher))
                }
            })
            .collect::<Vec<_>>();

        // Spawn a background async task to spin the ROS node
        let child_cancellation_token = cancellation_token.clone();
        executor
            .spawn(async move {
                let mut cancelled = child_cancellation_token.cancelled().fuse();
                loop {
                    futures::select_biased! {
                        _ = cancelled => {
                            break;
                        }
                        _ = smol::future::yield_now().fuse() => {
                            node.spin_once(std::time::Duration::from_millis(0));
                        }
                    }
                }
            })
            .detach();

        debug!(
            "Awaiting completion of {} stream handlers ({} aux, {} publishing)",
            stream_futures.len(),
            aux_info.len(),
            stream_futures.len() - aux_info.len()
        );

        futures::future::join_all(stream_futures).await;

        debug!("All ROS output stream processors have completed");
        Ok(())
    }
}

impl OutputHandler for ROSOutputHandler {
    type Val = Value;

    fn var_names(&self) -> Vec<VarName> {
        self.var_names.clone()
    }

    fn provide_streams(&mut self, streams: Vec<OutputStream<Value>>) {
        debug!("Providing {} streams to ROS output handler", streams.len());
        debug!("Expected var_names: {:?}", self.var_names());

        for (var, stream) in self.var_names().iter().zip(streams.into_iter()) {
            debug!("Assigning stream for output variable: {}", var);
            match self.var_map.get_mut(var) {
                Some(var_data) => {
                    var_data.stream = Some(stream);
                }
                None => {
                    let var_data = VarData {
                        variable: var.clone(),
                        topic_name: None,
                        msg_type: None,
                        stream: Some(stream),
                    };
                    self.var_map.insert(var.clone(), var_data);
                }
            };

            debug!("Successfully assigned stream for output variable: {}", var);
        }
        debug!("All output streams provided to ROS output handler");
    }

    #[instrument(level = Level::INFO, skip(self))]
    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        debug!(
            "Starting ROS output handler run with {} variables",
            self.var_map.len()
        );

        let streams = self
            .var_map
            .iter_mut()
            .filter_map(|(var_name, var_data)| {
                let topic_name = var_data.topic_name.clone();
                let msg_type = var_data.msg_type.clone();
                debug!(
                    "Taking stream for variable '{}' to publish to ROS topic '{:?}'",
                    var_name, topic_name
                );

                match mem::take(&mut var_data.stream) {
                    Some(s) => {
                        debug!("Successfully got stream for variable '{}'", var_name);
                        Some((var_name.clone(), topic_name, msg_type, s))
                    }
                    None => {
                        warn!("Stream not found for variable '{}'", var_name);
                        None
                    }
                }
            })
            .collect::<Vec<_>>();

        debug!("Collected {} streams for ROS publishing", streams.len());
        for (var, topic, msg_type, _) in &streams {
            debug!(
                "Will publish variable '{}' to ROS topic '{:?}' as {:?}",
                var, topic, msg_type
            );
        }

        info!(
            num_streams = ?streams.len(),
            "ROS OutputHandler startup task launched"
        );

        let executor = self.executor.clone();
        let aux_info = self.aux_info.clone();

        Box::pin(ROSOutputHandler::inner_handler(
            executor,
            self.node_name.clone(),
            streams,
            aux_info,
        ))
    }
}
