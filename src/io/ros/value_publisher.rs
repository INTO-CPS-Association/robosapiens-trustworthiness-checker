use tracing::warn;

use super::ros_topic_stream_mapping::RosMsgType;
use crate::{Value, core::JsonStreamValue};

/// Type-erased conversion used by the dynamic `Value` ROS output backend.
pub(crate) trait ValuePublisher: 'static {
    fn publish_value(&self, value: &Value) -> anyhow::Result<()>;
}

struct TypedValuePublisher<T: r2r::WrappedTypesupport + 'static> {
    publisher: r2r::Publisher<T>,
    convert_and_publish: fn(&r2r::Publisher<T>, &Value) -> anyhow::Result<()>,
}

impl<T: r2r::WrappedTypesupport + 'static> ValuePublisher for TypedValuePublisher<T> {
    fn publish_value(&self, value: &Value) -> anyhow::Result<()> {
        (self.convert_and_publish)(&self.publisher, value)
    }
}

/// Create a dynamic publisher for a resolved ROS message type.
///
/// Message types without a project-specific direct conversion use a
/// `std_msgs/String` JSON fallback, while MSTLO values are rejected here and
/// handled by the typed MSTLO backend.
pub(crate) fn create_value_publisher(
    node: &mut r2r::Node,
    topic: &str,
    msg_type: &RosMsgType,
) -> anyhow::Result<Box<dyn ValuePublisher>> {
    let qos = r2r::QosProfile::default();
    Ok(match msg_type {
        RosMsgType::Bool => Box::new(TypedValuePublisher {
            publisher: node.create_publisher::<r2r::std_msgs::msg::Bool>(topic, qos)?,
            convert_and_publish: |publisher, value| match value {
                Value::Bool(value) => publisher
                    .publish(&r2r::std_msgs::msg::Bool { data: *value })
                    .map_err(|error| anyhow::anyhow!("failed to publish Bool: {error:?}")),
                _ => Err(anyhow::anyhow!("expected Bool value, got {value:?}")),
            },
        }),
        RosMsgType::String => Box::new(TypedValuePublisher {
            publisher: node.create_publisher::<r2r::std_msgs::msg::String>(topic, qos)?,
            convert_and_publish: |publisher, value| match value {
                Value::Str(value) => publisher
                    .publish(&r2r::std_msgs::msg::String {
                        data: value.to_string(),
                    })
                    .map_err(|error| anyhow::anyhow!("failed to publish String: {error:?}")),
                _ => Err(anyhow::anyhow!("expected Str value, got {value:?}")),
            },
        }),
        RosMsgType::Int64 => Box::new(TypedValuePublisher {
            publisher: node.create_publisher::<r2r::std_msgs::msg::Int64>(topic, qos)?,
            convert_and_publish: |publisher, value| match value {
                Value::Int(value) => publisher
                    .publish(&r2r::std_msgs::msg::Int64 { data: *value })
                    .map_err(|error| anyhow::anyhow!("failed to publish Int64: {error:?}")),
                _ => Err(anyhow::anyhow!("expected Int value, got {value:?}")),
            },
        }),
        RosMsgType::Int32 => Box::new(TypedValuePublisher {
            publisher: node.create_publisher::<r2r::std_msgs::msg::Int32>(topic, qos)?,
            convert_and_publish: |publisher, value| match value {
                Value::Int(value) => publisher
                    .publish(&r2r::std_msgs::msg::Int32 {
                        data: *value as i32,
                    })
                    .map_err(|error| anyhow::anyhow!("failed to publish Int32: {error:?}")),
                _ => Err(anyhow::anyhow!("expected Int value, got {value:?}")),
            },
        }),
        RosMsgType::Int16 => Box::new(TypedValuePublisher {
            publisher: node.create_publisher::<r2r::std_msgs::msg::Int16>(topic, qos)?,
            convert_and_publish: |publisher, value| match value {
                Value::Int(value) => publisher
                    .publish(&r2r::std_msgs::msg::Int16 {
                        data: *value as i16,
                    })
                    .map_err(|error| anyhow::anyhow!("failed to publish Int16: {error:?}")),
                _ => Err(anyhow::anyhow!("expected Int value, got {value:?}")),
            },
        }),
        RosMsgType::Int8 => Box::new(TypedValuePublisher {
            publisher: node.create_publisher::<r2r::std_msgs::msg::Int8>(topic, qos)?,
            convert_and_publish: |publisher, value| match value {
                Value::Int(value) => publisher
                    .publish(&r2r::std_msgs::msg::Int8 { data: *value as i8 })
                    .map_err(|error| anyhow::anyhow!("failed to publish Int8: {error:?}")),
                _ => Err(anyhow::anyhow!("expected Int value, got {value:?}")),
            },
        }),
        RosMsgType::Float64 => Box::new(TypedValuePublisher {
            publisher: node.create_publisher::<r2r::std_msgs::msg::Float64>(topic, qos)?,
            convert_and_publish: |publisher, value| match value {
                Value::Float(value) => publisher
                    .publish(&r2r::std_msgs::msg::Float64 { data: *value })
                    .map_err(|error| anyhow::anyhow!("failed to publish Float64: {error:?}")),
                _ => Err(anyhow::anyhow!("expected Float value, got {value:?}")),
            },
        }),
        RosMsgType::Float32 => Box::new(TypedValuePublisher {
            publisher: node.create_publisher::<r2r::std_msgs::msg::Float32>(topic, qos)?,
            convert_and_publish: |publisher, value| match value {
                Value::Float(value) => publisher
                    .publish(&r2r::std_msgs::msg::Float32 {
                        data: *value as f32,
                    })
                    .map_err(|error| anyhow::anyhow!("failed to publish Float32: {error:?}")),
                _ => Err(anyhow::anyhow!("expected Float value, got {value:?}")),
            },
        }),
        RosMsgType::MstloTimedValue => {
            return Err(anyhow::anyhow!(
                "MstloTimedValue ROS output requires the typed MSTLO output backend"
            ));
        }
        _ => {
            warn!(?msg_type, %topic, "using JSON String fallback for ROS output");
            Box::new(TypedValuePublisher {
                publisher: node.create_publisher::<r2r::std_msgs::msg::String>(topic, qos)?,
                convert_and_publish: |publisher, value| {
                    let data = value.encode_json()?;
                    publisher
                        .publish(&r2r::std_msgs::msg::String { data })
                        .map_err(|error| {
                            anyhow::anyhow!("failed to publish JSON String: {error:?}")
                        })
                },
            })
        }
    })
}
