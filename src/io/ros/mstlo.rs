use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Duration;

use anyhow::Context;
use futures::{FutureExt, StreamExt};
use smol::LocalExecutor;

use uuid::Uuid;

use crate::core::{
    InputBatch, InputStream, LocalStream, OutputBinding, OutputError, OutputInterface, VarName,
    empty_input_stream,
};
use crate::runtime::mstlo::{MstloTimedValue, MstloValue};
use crate::utils::cancellation_token::CancellationToken;

use crate::io::output::{RosPublisher, validate_ros_interface};

use super::{
    ROS_SPIN_INTERVAL, ROS_SPIN_TIMEOUT,
    ros_topic_stream_mapping::{RosMsgType, ros_output_route_mapping},
};

pub type RosMstloTimedValue = r2r::robo_sapiens_interfaces::msg::MstloTimedValue;

const FLOAT_KIND: u8 = 0;
const BOOL_KIND: u8 = 1;
const ROBUSTNESS_INTERVAL_KIND: u8 = 2;

/// Convert a Rust duration to a ROS duration.
pub fn duration_to_ros(
    duration: Duration,
) -> anyhow::Result<r2r::builtin_interfaces::msg::Duration> {
    let seconds = duration.as_secs();
    let seconds = i32::try_from(seconds)
        .context("MSTLO ROS timestamp does not fit in builtin_interfaces/Duration.sec")?;
    Ok(r2r::builtin_interfaces::msg::Duration {
        sec: seconds,
        nanosec: duration.subsec_nanos(),
    })
}

/// Convert a ROS duration to a non-negative Rust duration.
pub fn duration_from_ros(
    duration: &r2r::builtin_interfaces::msg::Duration,
) -> anyhow::Result<Duration> {
    anyhow::ensure!(
        duration.sec >= 0,
        "MSTLO ROS timestamp has negative seconds"
    );
    anyhow::ensure!(
        duration.nanosec < 1_000_000_000,
        "MSTLO ROS timestamp nanoseconds must be less than 1,000,000,000"
    );
    Ok(Duration::new(duration.sec as u64, duration.nanosec))
}

/// Convert a ROS message to a native MSTLO value.
pub fn mstlo_value_from_ros(message: &RosMstloTimedValue) -> anyhow::Result<MstloTimedValue> {
    let value = match message.kind {
        FLOAT_KIND => MstloValue::Float(message.float_value),
        BOOL_KIND => MstloValue::Bool(message.bool_value),
        ROBUSTNESS_INTERVAL_KIND => {
            MstloValue::RobustnessInterval(message.interval_lower, message.interval_upper)
        }
        kind => anyhow::bail!("unknown MstloTimedValue payload kind {kind}"),
    };
    Ok(MstloTimedValue::new(
        duration_from_ros(&message.time)?,
        value,
    ))
}

/// Convert a native MSTLO value to a ROS message.
pub fn mstlo_value_to_ros(value: &MstloTimedValue) -> anyhow::Result<RosMstloTimedValue> {
    let time = duration_to_ros(value.timestamp)?;
    let mut message = RosMstloTimedValue {
        time,
        kind: FLOAT_KIND,
        float_value: 0.0,
        bool_value: false,
        interval_lower: 0.0,
        interval_upper: 0.0,
    };
    match value.value {
        MstloValue::Float(float_value) => {
            message.kind = FLOAT_KIND;
            message.float_value = float_value;
        }
        MstloValue::Bool(bool_value) => {
            message.kind = BOOL_KIND;
            message.bool_value = bool_value;
        }
        MstloValue::RobustnessInterval(lower, upper) => {
            message.kind = ROBUSTNESS_INTERVAL_KIND;
            message.interval_lower = lower;
            message.interval_upper = upper;
        }
        MstloValue::NoVal => anyhow::bail!("MstloTimedValue::NoVal must not be published to ROS"),
    }
    Ok(message)
}

/// Validate the fixed interface used by the typed MSTLO sink.
pub(crate) fn validate_output_interface(interface: &OutputInterface) -> Result<(), OutputError> {
    validate_ros_interface(interface, |message_type| {
        if *message_type == RosMsgType::MstloTimedValue {
            Ok(())
        } else {
            Err(OutputError::invalid(format!(
                "MSTLO ROS output requires message type `MstloTimedValue`, got `{message_type:?}`"
            )))
        }
    })
}

struct MstloOutputPublisher {
    topic: String,
    publisher: r2r::Publisher<RosMstloTimedValue>,
}

impl RosPublisher<MstloTimedValue> for MstloOutputPublisher {
    fn publish(&self, value: &MstloTimedValue) -> Result<(), OutputError> {
        let message = mstlo_value_to_ros(value).map_err(|error| {
            OutputError::backend(format!(
                "failed to encode MSTLO ROS output on `{}`: {error}",
                self.topic
            ))
        })?;
        self.publisher.publish(&message).map_err(|error| {
            OutputError::backend(format!(
                "failed to publish MSTLO ROS output on `{}`: {error:?}",
                self.topic
            ))
        })
    }
}

/// Create the typed publisher used by the opened ROS output owner.
pub(crate) fn create_mstlo_output_publisher(
    node: &mut r2r::Node,
    route: &OutputBinding,
) -> Result<Box<dyn RosPublisher<MstloTimedValue>>, OutputError> {
    let (topic, message_type) = ros_output_route_mapping(route)?;
    if message_type != RosMsgType::MstloTimedValue {
        return Err(OutputError::invalid(format!(
            "MSTLO ROS output route `{}` has message type `{message_type:?}`, expected `MstloTimedValue`",
            route.variable()
        )));
    }

    let publisher = node
        .create_publisher::<RosMstloTimedValue>(topic, r2r::QosProfile::default())
        .map_err(|error| {
            OutputError::backend(format!(
                "failed to create MSTLO ROS publisher for `{topic}`: {error:?}"
            ))
        })?;
    Ok(Box::new(MstloOutputPublisher {
        topic: topic.to_owned(),
        publisher,
    }))
}

fn validate_mapping(mapping: &BTreeMap<String, (String, String)>) -> anyhow::Result<()> {
    for (variable, (_topic, message_type)) in mapping {
        anyhow::ensure!(
            message_type == "MstloTimedValue",
            "MSTLO ROS variable `{variable}` must use message type `MstloTimedValue`, got `{message_type}`"
        );
    }
    Ok(())
}

/// Subscribe to native MSTLO ROS messages.
pub fn open_ros_input(
    executor: Rc<LocalExecutor<'static>>,
    mapping: BTreeMap<String, (String, String)>,
) -> anyhow::Result<(InputStream<MstloTimedValue>, super::RosInputControl)> {
    validate_mapping(&mapping)?;
    if mapping.is_empty() {
        return Ok((empty_input_stream(), super::RosInputControl::inactive()));
    }

    let context = r2r::Context::create()?;
    let node_name = format!("input_monitor_{}", Uuid::new_v4().simple());
    let mut node = r2r::Node::create(context, &node_name, "")?;
    let cancellation_token = CancellationToken::new();
    let cancellation_for_spin = cancellation_token.clone();

    let mut streams: Vec<LocalStream<anyhow::Result<(VarName, MstloTimedValue)>>> = Vec::new();
    for (variable, (topic, _)) in mapping {
        let variable = VarName::new(&variable);
        let subscription =
            node.subscribe::<RosMstloTimedValue>(&topic, r2r::QosProfile::default())?;
        let stream = Box::pin(subscription.map(move |message| {
            let value = mstlo_value_from_ros(&message).with_context(|| {
                format!("invalid MstloTimedValue received for variable `{variable}`")
            })?;
            anyhow::ensure!(
                matches!(value.value, MstloValue::Float(_)),
                "MSTLO ROS input for variable `{variable}` must have FLOAT kind"
            );
            Ok((variable.clone(), value))
        })) as LocalStream<anyhow::Result<(VarName, MstloTimedValue)>>;
        streams.push(stream);
    }

    let spinner = executor.spawn(async move {
        let mut spin_ticks = smol::Timer::interval(ROS_SPIN_INTERVAL);
        loop {
            futures::select_biased! {
                _ = cancellation_for_spin.cancelled().fuse() => break,
                _ = spin_ticks.next().fuse() => node.spin_once(ROS_SPIN_TIMEOUT),
            }
        }
    });

    let merged = futures::stream::select_all(streams);
    let stream = Box::pin(async_stream::try_stream! {
        futures::pin_mut!(merged);
        while let Some(event) = merged.next().await {
            let (variable, value) = event?;
            yield InputBatch::update(variable, value);
        }
    });
    Ok((
        stream,
        super::RosInputControl::new(cancellation_token, spinner),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ros_duration_round_trip_and_validation() {
        let value = Duration::new(12, 345_678_901);
        assert_eq!(
            duration_from_ros(&duration_to_ros(value).unwrap()).unwrap(),
            value
        );
        assert!(
            duration_from_ros(&r2r::builtin_interfaces::msg::Duration {
                sec: -1,
                nanosec: 0,
            })
            .is_err()
        );
        assert!(
            duration_from_ros(&r2r::builtin_interfaces::msg::Duration {
                sec: 0,
                nanosec: 1_000_000_000,
            })
            .is_err()
        );
        assert!(duration_to_ros(Duration::new(i32::MAX as u64 + 1, 0)).is_err());
    }

    #[test]
    fn ros_message_conversion_covers_all_payload_variants() {
        let values = [
            MstloTimedValue::new(Duration::from_millis(1), MstloValue::Float(2.5)),
            MstloTimedValue::new(Duration::from_millis(2), MstloValue::Bool(true)),
            MstloTimedValue::new(
                Duration::from_millis(3),
                MstloValue::RobustnessInterval(-1.0, 2.0),
            ),
        ];
        for value in values {
            assert_eq!(
                mstlo_value_from_ros(&mstlo_value_to_ros(&value).unwrap()).unwrap(),
                value
            );
        }
        assert!(
            mstlo_value_to_ros(&MstloTimedValue::new(Duration::ZERO, MstloValue::NoVal,)).is_err()
        );
        let mut unknown = mstlo_value_to_ros(&values[0]).unwrap();
        unknown.kind = 99;
        assert!(mstlo_value_from_ros(&unknown).is_err());
    }
}
