//! Engine-independent building blocks for opt-in profiling, tracing and memory estimates.
//!
//! This module is compiled only with the `diagnostics` cargo feature. It owns the parts of the
//! diagnostics facility that do not know about any particular engine:
//!
//! | piece | responsibility |
//! |:------|:---------------|
//! | [`DiagnosticsConfig`] | What to capture: step timing, trace selection and bounds, memory snapshot cadence and traversal budgets. |
//! | [`DurationStats`] | Count, total and maximum of elapsed wall-clock durations. |
//! | [`PipelineProfile`] | Aggregate elapsed time per [`PipelinePhase`], each labelled synchronous work or elapsed wait, plus counters and maxima. |
//! | [`Timeline`] | A bounded buffer of spans and instants that labels what it dropped. |
//! | [`ValueSizer`] | A budgeted, non-cloning estimator of the heap payload reachable from a [`crate::Value`]. |
//! | [`DiagnosticsHub`] | The run-wide owner of the configuration and of input-source recorders. |
//! | [`chrome`] | Export of timelines in the Chrome trace event format, which Perfetto reads. |
//!
//! # Dependency direction
//!
//! Engine-specific recording sits next to its owner and depends on this module, never the other
//! way round: [`crate::dataflow`] records execution steps, ticks, its model map and structured
//! memory; the RPIO input owner records decode, projection and conformance work; and
//! [`crate::runtime::diagnostics`] assembles one run's report from those parts and writes the
//! artifacts. [`crate::Value`] does not depend on any recorder; the estimator here only reads
//! values through their public, non-mutating API.
//!
//! # What is measured and what is estimated
//!
//! Durations are wall-clock elapsed time from [`std::time::Instant`]. A synchronous span is the
//! elapsed time of code that did not yield, which on a loaded machine still includes preemption;
//! it is not CPU time. An *elapsed wait* is the time an `await` took to complete, which includes
//! any synchronous work the awaited future performed while it was polled, such as decoding a
//! replayed record. Memory figures are estimates assembled from explicit owners and capacities;
//! they are not allocator measurements and are unrelated to process RSS.

// Diagnostics only read state; none of this code may need `unsafe`.
#![forbid(unsafe_code)]

pub mod chrome;
mod config;
mod hub;
mod memory;
mod pipeline;
mod stats;
mod timeline;

pub use config::{DiagnosticsConfig, MemoryConfig, RecorderLimits, TickRange, TraceConfig};
pub use hub::{DiagnosticsHub, SourceRecord, SourceRecorder};
pub use memory::{
    MemoryBudget, MemoryComponent, MemoryEstimate, MemorySnapshot, PayloadEstimate, SnapshotKind,
    SnapshotSkipped, StreamBytes, ValueSizer,
};
pub use pipeline::{Counters, PhaseKind, PipelinePhase, PipelineProfile, PipelineStamp};
pub use stats::DurationStats;
pub use timeline::{Timeline, TimelineEvent, TimelineTrack};

/// Schema identifier written into every summary so consumers can detect format changes.
pub const REPORT_SCHEMA: &str = "tc-diagnostics/1";

/// Nanoseconds from `epoch` to `instant`, saturating at zero for instants before the epoch.
#[inline]
pub fn nanos_since(epoch: std::time::Instant, instant: std::time::Instant) -> u64 {
    u64::try_from(instant.saturating_duration_since(epoch).as_nanos()).unwrap_or(u64::MAX)
}
