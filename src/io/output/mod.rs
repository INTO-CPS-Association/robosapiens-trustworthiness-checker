//! Output destination planning, opened ownership, stages, and routing.
//!
//! # Contract
//!
//! [`OutputPipeline`] resolves model outputs and request-local bindings against a
//! stable [`OutputDestinations`] registry before opening resources. The resulting
//! [`ResolvedOutput`] fixes variable ownership, mirroring, routes, codecs, stages,
//! and backend interfaces. Opening exposes one runtime-facing
//! [`crate::core::OutputWriter`]; one non-session destination uses a direct writer,
//! while sessions and multi-destination outputs use a tick-preserving router.
//!
//! # Principal entities
//!
//! | Entity | Responsibility |
//! |---|---|
//! | [`OutputDestination`] | Holds one unopened backend, selection policy, route catalog, and destination-local stages. |
//! | [`OutputDestinations`] | Owns the deterministic destination registry and optional default owner. |
//! | [`OutputPipeline`] | Resolves complete plans and opens the backend owners and stage wrappers named by them. |
//! | [`ResolvedOutput`] | Stores one immutable, resource-free output plan. |
//! | [`OutputStage`] | Applies bounded buffering or logical-tick-preserving coalescing before or after routing according to ownership. |
//! | [`OutputPipelineSession`] | Retains fixed destination owners and mutable selected-variable/interface state for live reconfiguration. |
//!
//! # Ownership and ordering
//!
//! Shared stages wrap the complete delivery path before routing. Destination-local
//! stages wrap one opened backend owner after routing. Router readiness waits for
//! every active destination writer; with several owners, admitted batches are
//! selected by each destination's resolved variable set and empty selections are
//! skipped. This preserves per-destination logical tick order but provides neither
//! pressure isolation nor atomic commit across destinations.
//!
//! # Implementation mapping
//!
//! `pipeline` implements resolution, opening, routing, session ownership, and live
//! interface handoff. `stages` and `pump` implement buffer/coalescing wrappers and
//! their worker/barrier lifecycle. `backend` and the transport modules implement
//! unopened backend configuration and opened destination writers.

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
