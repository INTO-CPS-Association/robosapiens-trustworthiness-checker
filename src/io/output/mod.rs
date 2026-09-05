//! Output destination planning, opened ownership, stages, and routing.
//!
//! # Contract
//!
//! [`OutputPipeline`] resolves model outputs and request-local bindings against a
//! stable [`OutputDestinations`] registry before opening resources. The resulting
//! [`ResolvedOutput`] fixes variable ownership, mirroring, routes, codecs, and delivery,
//! and backend interfaces. Opening exposes one runtime-facing
//! [`crate::core::OutputWriter`]; one non-session destination uses a direct writer,
//! while sessions and multi-destination outputs use a tick-preserving router.
//!
//! # Principal entities
//!
//! | Entity | Responsibility |
//! |---|---|
//! | [`OutputDestination`] | Holds one unopened backend, selection policy, route catalog, and delivery policy. |
//! | [`OutputDestinations`] | Owns the deterministic destination registry and optional default owner. |
//! | [`OutputPipeline`] | Resolves complete plans and opens backend and delivery owners. |
//! | [`ResolvedOutput`] | Stores one immutable, resource-free output plan. |
//! | [`DeliveryPolicy`] | Selects direct delivery or bounded queued/coalesced delivery per destination. |
//! | [`OutputPipelineSession`] | Retains fixed destination owners and mutable selected-variable/interface state for live reconfiguration. |
//!
//! # Ownership and ordering
//!
//! Each destination applies its policy before entering the router. Router readiness waits for
//! every active destination writer; with several owners, admitted batches are
//! selected by each destination's resolved variable set and empty selections are
//! skipped. This preserves per-destination logical tick order but provides neither
//! pressure isolation nor atomic commit across destinations.
//!
//! # Implementation mapping
//!
//! `pipeline` implements resolution, opening, routing, session ownership, and live
//! interface handoff. `delivery` implements bounded worker and barrier lifecycle.
//! `backend` and the transport modules implement
//! unopened backend configuration and opened destination writers.

mod backend;
mod configuration;
mod delivery;
mod pipeline;
mod sinks;

mod mqtt;
#[cfg(feature = "redis")]
mod redis;
#[cfg(feature = "ros")]
mod ros;

#[cfg(any(test, feature = "test-support"))]
pub use backend::TestOutputOpener;
pub use backend::{OutputBackendConfig, OutputBackendKind};
pub use delivery::{CoalescingLimits, Delivery, DeliveryPolicy, QueueLimits};
pub(crate) use pipeline::OutputPipelineReconfigurationPlan;
pub use pipeline::{
    OutputDestination, OutputDestinationSelection, OutputDestinations, OutputPipeline,
    OutputPipelineSession, ResolvedDestination, ResolvedOutput,
};
pub use sinks::{AsyncFnSink, LocalBatchSink, local_batch_sink};
pub(crate) use sinks::{InterfaceSink, open_limited_null, open_null, open_stdout};

#[cfg(feature = "ros")]
pub(crate) use ros::{
    RosPublisher, create_value_ros_publisher, open as open_ros_output, validate_ros_interface,
    validate_value_interface,
};

pub use crate::io::config::{
    DestinationConfig, DestinationId, DestinationKind, OutputCoalescingConfig, OutputConfigFile,
    OutputDeliveryConfig, OutputQueueConfig,
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
    primary.with_cleanup(cleanup)
}
