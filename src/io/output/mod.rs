//! Output destinations, deterministic resolution, and the local output data
//! plane.
//!
//! The output boundary is a single local [`OutputWriter`]. Configuration is
//! resolved into fixed destination interfaces before any backend is opened;
//! multiple opened writers are hidden behind a tick-preserving router.

mod backend;
mod pipeline;
mod sinks;
mod stages;

#[cfg(feature = "mqtt")]
mod mqtt;
#[cfg(feature = "redis")]
mod redis;
#[cfg(feature = "ros")]
mod ros;

mod pump;

pub use backend::{MqttOutputBackendKind, OutputBackendConfig, OutputBackendKind};
pub(crate) use pipeline::OutputPipelineReconfigurationPlan;
pub use pipeline::{
    OutputDestination, OutputDestinationSelection, OutputDestinations, OutputPipeline,
    OutputPipelineSession, ResolvedDestination, ResolvedOutput, ResolvedOutputBinding,
};
pub use sinks::{
    AsyncFnSink, LimitedNullOutputBackend, LocalBatchSink, ManualOutputBackend,
    ManualOutputReceiver, ManualOutputSender, NullOutputBackend, StdoutOutputBackend,
    local_batch_sink,
};
pub use stages::{OutputBuffer, OutputCoalescing, OutputStage};

#[cfg(feature = "mqtt")]
pub use mqtt::{MQTT_MAX_RETRIES, MqttOutputBackend};
pub use pump::OutputPump;
#[cfg(feature = "redis")]
pub use redis::RedisOutputBackend;
#[cfg(feature = "ros")]
pub use ros::RosOutputBackend;
#[cfg(feature = "ros")]
pub(crate) use ros::{
    RosPublisher, create_value_ros_publisher, validate_ros_interface, validate_value_interface,
};

pub use crate::io::config::{
    DestinationConfig, DestinationId, DestinationKind, OutputConfigFile, OutputStageConfig,
};

pub(crate) fn remember_error(
    slot: &mut Option<crate::core::OutputError>,
    error: crate::core::OutputError,
) {
    let Some(previous) = slot.take() else {
        *slot = Some(error);
        return;
    };
    *slot = Some(combine_errors(previous, error));
}

pub(crate) fn combine_errors(
    primary: crate::core::OutputError,
    cleanup: crate::core::OutputError,
) -> crate::core::OutputError {
    if primary == cleanup {
        return primary;
    }
    match primary {
        crate::core::OutputError::Backend(message) => {
            crate::core::OutputError::Backend(format!("{message}; additionally: {cleanup}"))
        }
        crate::core::OutputError::Source(message) => {
            crate::core::OutputError::Source(format!("{message}; additionally: {cleanup}"))
        }
        crate::core::OutputError::Invalid(message) => {
            crate::core::OutputError::Invalid(format!("{message}; additionally: {cleanup}"))
        }
        crate::core::OutputError::Closed => {
            crate::core::OutputError::Backend(format!("{primary}; additionally: {cleanup}"))
        }
    }
}
