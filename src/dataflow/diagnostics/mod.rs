//! Dataflow-specific profiling, tracing, attribution and structured memory.
//!
//! This module is the only place the monitor's diagnostics are recorded and reported. The
//! execution owners contain one-line hooks into it:
//!
//! | owner | hook |
//! |:------|:-----|
//! | `DataflowMonitor` | tick begin/end, history commit, selected value summaries, dynamic activation, schedule and revision changes, reconfiguration |
//! | `MonitorExecution` | plan observation, per-step timing with the executor actually used, commit, plan switches and native tier activation |
//! | lowering and binding | the [`NodeSites`] sidecar carrying frontend spans and origins on each graph node |
//! | canonical node evaluation | [`collections`] counters at list append, concatenation, scan and map update sites |
//!
//! Without the `diagnostics` feature, [`ExecutionProbe`] and [`NodeSites`] are zero-sized and every
//! hook is an empty inlined function. With it, a monitor records nothing until
//! [`DataflowMonitor::enable_diagnostics`](crate::dataflow::DataflowMonitor) installs a recorder.
//!
//! # Semantic invariants
//!
//! Observation reads state only. It does not advance ticks, change schedules, disable
//! quickening, select or deselect native tiers, or materialize native state: memory snapshots of
//! natively owned regions report the canonical arena as possibly stale instead. Timing a fused
//! scalar region or a whole-schedule native artifact yields one aggregate with its member streams,
//! never fabricated per-stream figures.

// Diagnostics only read state; none of this code may need `unsafe`.
#![forbid(unsafe_code)]

pub(in crate::dataflow) mod collections;
mod probe;
mod sites;

#[cfg(feature = "diagnostics")]
mod dependencies;
#[cfg(feature = "diagnostics")]
mod memory;
#[cfg(feature = "diagnostics")]
mod model;
#[cfg(feature = "diagnostics")]
mod recorder;
#[cfg(feature = "diagnostics")]
mod report;

pub(in crate::dataflow) use probe::{ExecutionProbe, Stamp, StepPath, StepPhase};
pub(in crate::dataflow) use sites::{DebugSources, NodeSites};

#[cfg(feature = "diagnostics")]
pub(in crate::dataflow) use memory::{MemoryVisitor, SnapshotPoint};
#[cfg(feature = "diagnostics")]
pub(in crate::dataflow) use model::DefinitionRecord;
#[cfg(feature = "diagnostics")]
pub(in crate::dataflow) use recorder::{DataflowRecorder, PlanDescriptor, StepDescriptor};
#[cfg(feature = "diagnostics")]
pub use report::DataflowDiagnosticsReport;

#[cfg(feature = "diagnostics")]
pub(in crate::dataflow) fn report_inputs<'a>(
    program: &'a crate::dataflow::DataflowProgram,
    dynamic_slots: &'a dyn Fn(usize) -> Vec<crate::dataflow::environment::EnvironmentSlot>,
    jit: Option<serde_json::Value>,
) -> report::ReportInputs<'a> {
    report::ReportInputs {
        program,
        dynamic_slots,
        jit,
    }
}
