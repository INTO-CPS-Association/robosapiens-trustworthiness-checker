use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Duration;

use anyhow::Context;
use futures::future::LocalBoxFuture;
use futures::{FutureExt, StreamExt};
use smol::LocalExecutor;

use uuid::Uuid;

use crate::core::{
    InputBatch, InputEvent, InputStream, OutputHandler, OutputStream, StreamData, VarName,
};
use crate::runtime::mstlo::{MstloTimedValue, MstloValue};
use crate::stream_utils::drop_guard_stream;
use crate::utils::cancellation_token::CancellationToken;

use super::{ROS_SPIN_INTERVAL, ROS_SPIN_TIMEOUT};

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
pub fn input_stream(
    executor: Rc<LocalExecutor<'static>>,
    mapping: BTreeMap<String, (String, String)>,
) -> anyhow::Result<InputStream<MstloTimedValue>> {
    validate_mapping(&mapping)?;
    if mapping.is_empty() {
        return Ok(Box::pin(futures::stream::empty()));
    }

    let context = r2r::Context::create()?;
    let node_name = format!("input_monitor_{}", Uuid::new_v4().simple());
    let mut node = r2r::Node::create(context, &node_name, "")?;
    let cancellation_token = CancellationToken::new();
    let drop_guard = Rc::new(cancellation_token.clone().drop_guard());
    let cancellation_for_spin = cancellation_token.clone();

    let mut streams: Vec<OutputStream<anyhow::Result<(VarName, MstloTimedValue)>>> = Vec::new();
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
        })) as OutputStream<anyhow::Result<(VarName, MstloTimedValue)>>;
        streams.push(drop_guard_stream(stream, drop_guard.clone()));
    }

    executor
        .spawn(async move {
            let mut spin_ticks = smol::Timer::interval(ROS_SPIN_INTERVAL);
            loop {
                futures::select_biased! {
                    _ = cancellation_for_spin.cancelled().fuse() => break,
                    _ = spin_ticks.next().fuse() => node.spin_once(ROS_SPIN_TIMEOUT),
                }
            }
        })
        .detach();

    let merged = futures::stream::select_all(streams);
    Ok(Box::pin(async_stream::try_stream! {
        futures::pin_mut!(merged);
        while let Some(event) = merged.next().await {
            let (variable, value) = event?;
            yield InputBatch::events(vec![InputEvent::new(variable, value)]);
        }
    }))
}

struct OutputVarData {
    topic: Option<String>,
    stream: Option<OutputStream<MstloTimedValue>>,
}

/// MSTLO ROS output handler.
pub struct MstloRosOutputHandler {
    executor: Rc<LocalExecutor<'static>>,
    node_name: String,
    variables: BTreeMap<VarName, OutputVarData>,
    aux_info: Vec<VarName>,
}

impl MstloRosOutputHandler {
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        node_name: String,
        mapping: BTreeMap<String, (String, String)>,
        aux_info: Vec<VarName>,
    ) -> anyhow::Result<Self> {
        validate_mapping(&mapping)?;
        let variables = mapping
            .into_iter()
            .map(|(variable, (topic, _))| {
                (
                    VarName::new(&variable),
                    OutputVarData {
                        topic: Some(topic),
                        stream: None,
                    },
                )
            })
            .collect();
        Ok(Self {
            executor,
            node_name,
            variables,
            aux_info,
        })
    }

    async fn publish_stream(
        topic: String,
        mut stream: OutputStream<MstloTimedValue>,
        publisher: r2r::Publisher<RosMstloTimedValue>,
    ) -> anyhow::Result<()> {
        while let Some(value) = stream.next().await {
            if value.is_no_val() {
                continue;
            }
            let message = mstlo_value_to_ros(&value)
                .with_context(|| format!("failed to encode MSTLO ROS output for `{topic}`"))?;
            publisher
                .publish(&message)
                .map_err(|error| anyhow::anyhow!(error))
                .with_context(|| format!("failed to publish MSTLO ROS output on `{topic}`"))?;
        }
        Ok(())
    }

    async fn drain_stream(mut stream: OutputStream<MstloTimedValue>) -> anyhow::Result<()> {
        while stream.next().await.is_some() {}
        Ok(())
    }

    async fn inner_handler(
        executor: Rc<LocalExecutor<'static>>,
        node_name: String,
        streams: Vec<(VarName, Option<String>, OutputStream<MstloTimedValue>)>,
        aux_info: Vec<VarName>,
    ) -> anyhow::Result<()> {
        let context = r2r::Context::create()?;
        let node_name = format!("{}_{}", node_name, Uuid::new_v4().simple());
        let mut node = r2r::Node::create(context, &node_name, "")?;
        let cancellation_token = CancellationToken::new();
        let cancellation_guard = cancellation_token.drop_guard();
        let cancellation_for_spin = cancellation_guard.clone_tok();

        let mut tasks: Vec<LocalBoxFuture<'static, anyhow::Result<()>>> = Vec::new();
        for (variable, topic, stream) in streams {
            if aux_info.contains(&variable) {
                tasks.push(Box::pin(Self::drain_stream(stream)));
                continue;
            }
            let topic = topic.ok_or_else(|| {
                anyhow::anyhow!("ROS output mapping is missing topic for `{variable}`")
            })?;
            let publisher = node
                .create_publisher::<RosMstloTimedValue>(&topic, r2r::QosProfile::default())
                .map_err(|error| anyhow::anyhow!(error))
                .with_context(|| format!("failed to create MSTLO ROS publisher for `{topic}`"))?;
            tasks.push(Box::pin(Self::publish_stream(topic, stream, publisher)));
        }

        executor
            .spawn(async move {
                let mut spin_ticks = smol::Timer::interval(ROS_SPIN_INTERVAL);
                loop {
                    futures::select_biased! {
                        _ = cancellation_for_spin.cancelled().fuse() => break,
                        _ = spin_ticks.next().fuse() => node.spin_once(ROS_SPIN_TIMEOUT),
                    }
                }
            })
            .detach();

        futures::future::try_join_all(tasks).await.map(|_| ())
    }
}

impl OutputHandler for MstloRosOutputHandler {
    type Val = MstloTimedValue;

    fn provide_streams(&mut self, streams: BTreeMap<VarName, OutputStream<Self::Val>>) {
        for (variable, stream) in streams {
            self.variables
                .entry(variable)
                .or_insert(OutputVarData {
                    topic: None,
                    stream: None,
                })
                .stream = Some(stream);
        }
    }

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let streams = self
            .variables
            .iter_mut()
            .filter_map(|(variable, data)| {
                data.stream
                    .take()
                    .map(|stream| (variable.clone(), data.topic.clone(), stream))
            })
            .collect();
        Self::inner_handler(
            self.executor.clone(),
            self.node_name.clone(),
            streams,
            self.aux_info.clone(),
        )
        .boxed_local()
    }
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
