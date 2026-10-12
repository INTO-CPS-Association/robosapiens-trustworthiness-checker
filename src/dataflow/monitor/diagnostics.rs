//! The monitor's diagnostics entry points and tick-level hooks.
//!
//! [`DataflowMonitor::enable_diagnostics`] installs a recorder in the monitor's execution owner.
//! From then on each tick records its duration and outcome, commit and history-commit time,
//! dynamic activations, schedule and revision changes, and, for ticks in the configured trace
//! range, step spans and summaries of selected stream values. The recorder survives
//! [`DataflowMonitor::reset`] and root reconfiguration, which install a new definition version.
//!
//! Without the `diagnostics` feature the hooks below are empty.

// Diagnostics only read state; none of this code may need `unsafe`.
#![forbid(unsafe_code)]

use super::DataflowMonitor;
#[cfg(not(feature = "diagnostics"))]
use crate::dataflow::DataflowEvaluationError;
use crate::dataflow::diagnostics::Stamp;

#[cfg(feature = "diagnostics")]
mod enabled {
    use std::mem::size_of;

    use super::super::DataflowMonitor;
    use super::TickScope;
    use crate::core::Value;
    use crate::dataflow::DataflowEvaluationError;
    use crate::dataflow::diagnostics::{
        DataflowDiagnosticsReport, DataflowRecorder, DefinitionRecord, MemoryVisitor,
        SnapshotPoint, Stamp,
    };
    use crate::dataflow::environment::EnvironmentSlot;
    use crate::diagnostics::{
        DiagnosticsConfig, MemoryEstimate, MemorySnapshot, SnapshotSkipped, Timeline,
    };

    impl DataflowMonitor {
        /// Start recording diagnostics for this monitor, replacing any earlier recorder.
        ///
        /// Recording observes evaluation without changing it: outputs, errors, commit
        /// boundaries, schedules and tier selection are the same with or without it.
        pub fn enable_diagnostics(&mut self, config: &DiagnosticsConfig) {
            let mut recorder = Box::new(DataflowRecorder::new(config));
            recorder.install_definition(DefinitionRecord::capture(&self.program, &config.limits));
            self.execution.install_recorder(recorder);
        }

        /// Whether a recorder is installed.
        pub fn diagnostics_enabled(&self) -> bool {
            self.execution.recorder().is_some()
        }

        /// The report of everything recorded so far, or `None` without a recorder.
        pub fn diagnostics_report(&self) -> Option<DataflowDiagnosticsReport> {
            let recorder = self.execution.recorder()?;
            let dynamic_slots = |stream: usize| self.execution.active_dependency_slots(stream);
            Some(DataflowDiagnosticsReport::build(
                recorder,
                crate::dataflow::diagnostics::report_inputs(
                    &self.program,
                    &dynamic_slots,
                    self.execution.jit_summary(),
                ),
            ))
        }

        /// The recorded monitor and step timeline, for trace export.
        pub fn diagnostics_timeline(&self) -> Option<&Timeline> {
            self.execution.recorder().map(|recorder| &recorder.timeline)
        }

        /// Take a structured memory snapshot now and retain it in the report.
        ///
        /// The snapshot only reads monitor state. An explicit snapshot is mandatory: when the
        /// retention cap is full it replaces the most recent periodic snapshot. It is skipped, and
        /// the skip returned, only without a recorder or when every retained snapshot is
        /// mandatory; the skip is also listed in the report's `mandatory_skipped`. A refused
        /// request never returns a snapshot taken earlier.
        pub fn record_memory_snapshot(
            &mut self,
            label: &str,
        ) -> Result<&MemorySnapshot, SnapshotSkipped> {
            let index = self.record_snapshot(SnapshotPoint::Explicit(label.to_owned()))?;
            self.execution
                .recorder()
                .and_then(|recorder| recorder.memory.snapshots.get(index))
                .ok_or(SnapshotSkipped::NotRecording)
        }

        /// Take the run's final snapshot; mandatory like an explicit one. A runtime calls this when
        /// it finishes.
        pub fn record_final_memory_snapshot(&mut self) -> Result<&MemorySnapshot, SnapshotSkipped> {
            let index = self.record_snapshot(SnapshotPoint::Final)?;
            self.execution
                .recorder()
                .and_then(|recorder| recorder.memory.snapshots.get(index))
                .ok_or(SnapshotSkipped::NotRecording)
        }

        /// Take and retain a snapshot at `point`, returning its index in the store. A snapshot
        /// that retention would refuse is not built: no value is traversed for it.
        pub(in crate::dataflow) fn record_snapshot(
            &mut self,
            point: SnapshotPoint,
        ) -> Result<usize, SnapshotSkipped> {
            let recorder = self
                .execution
                .recorder_mut()
                .ok_or(SnapshotSkipped::NotRecording)?;
            if !point.kind().is_periodic() && !recorder.mandatory_snapshot_fits() {
                recorder.skip_mandatory_snapshot(point.label());
                return Err(SnapshotSkipped::RetentionFull);
            }
            let snapshot = self
                .memory_snapshot(point)
                .ok_or(SnapshotSkipped::NotRecording)?;
            let recorder = self
                .execution
                .recorder_mut()
                .ok_or(SnapshotSkipped::NotRecording)?;
            recorder.memory.built += 1;
            recorder.push_snapshot(snapshot)
        }

        fn stream_names(&self) -> Vec<String> {
            self.program
                .stream_vars()
                .iter()
                .map(|var| var.name().to_string())
                .collect()
        }

        fn slot_name(&self, slot: usize) -> String {
            self.program
                .environment_layout()
                .variable(EnvironmentSlot::new(slot))
                .map_or_else(|| format!("slot {slot}"), |var| var.name().to_string())
        }

        /// Build a snapshot of this monitor's owners without recording it.
        pub(in crate::dataflow) fn memory_snapshot(
            &self,
            point: SnapshotPoint,
        ) -> Option<MemorySnapshot> {
            let recorder = self.execution.recorder()?;
            let names = self.stream_names();
            let input_count = self.program.input_vars().len();
            let mut visitor = MemoryVisitor::new(
                &point,
                recorder.ticks.succeeded,
                recorder.definition_version(),
                recorder.config().memory.budget,
            );

            // Owners are visited from the most specific to the most general, so a buffer reached
            // from several places is charged to the stream state that retains it and reported as
            // shared by the environment rows that merely reference it.
            self.execution.visit_memory(&mut visitor, &names);

            let histories = self.history_store.diagnostic_histories();
            for (slot, binding) in self.history_bindings.iter().enumerate() {
                // One unit per binding, so the walk is bounded by the snapshot's work budget.
                if !visitor.enter_owner() {
                    break;
                }
                let Some(id) = binding else {
                    continue;
                };
                let Some(history) = histories.get(id.index()) else {
                    continue;
                };
                let (slots, depth, live) = history.diagnostic_slots();
                if !live {
                    continue;
                }
                let name = self.slot_name(slot);
                let estimate = visitor.slots(slots.len(), slots.iter());
                visitor.push(
                    format!("history/{name} (depth {depth})"),
                    "history",
                    (slot >= input_count).then_some(name.as_str()),
                    estimate,
                );
            }
            visitor.push(
                "monitor/history_bindings",
                "environment",
                None,
                MemoryEstimate::fixed(
                    self.history_bindings.len()
                        * (size_of::<Option<crate::dataflow::history::HistoryId>>()
                            + size_of::<usize>()),
                ),
            );

            let environment = visitor.slots(
                self.environment_values.capacity(),
                self.environment_values.iter(),
            );
            visitor.push("monitor/environment", "environment", None, environment);
            if let Some(retained) = &self.retained_environment_values {
                let estimate = visitor.slots(retained.capacity(), retained.iter());
                visitor.push(
                    "monitor/retained_environment",
                    "environment",
                    None,
                    estimate,
                );
            }

            // The nodes the bounded model-map capture visited when the definition was installed;
            // the program is not walked again. When the capture's node budget stopped it, this is
            // a lower bound and the component is marked truncated.
            let (program_nodes, lower_bound) = recorder
                .definitions
                .last()
                .map_or((0, false), |d| (d.node_count, d.node_count_is_lower_bound));
            let mut graphs = MemoryEstimate::fixed(
                program_nodes
                    * (size_of::<crate::dataflow::ir::BoundOp>()
                        + size_of::<Option<crate::dataflow::ir::ScalarSignature>>()),
            );
            graphs.payload.truncated = lower_bound;
            visitor.push(
                if lower_bound {
                    format!("program/graphs (at least {program_nodes} nodes visited, shared)")
                } else {
                    format!("program/graphs ({program_nodes} nodes, shared)")
                },
                "program",
                None,
                graphs,
            );
            Some(visitor.finish())
        }

        /// Start a tick: a timestamp and an observed collection scope when recording, and a
        /// suspended collection scope otherwise, so this tick's collection operations are never
        /// attributed to an enclosing monitor's recorder.
        #[inline]
        pub(in crate::dataflow) fn diag_tick_begin(&mut self) -> TickScope {
            let observed = self.execution.recorder().is_some();
            TickScope {
                collections: crate::dataflow::diagnostics::collections::CollectionScope::enter(
                    observed,
                ),
                start: observed.then(std::time::Instant::now),
            }
        }

        #[inline]
        pub(in crate::dataflow) fn diag_tick_end(
            &mut self,
            scope: TickScope,
            result: &Result<(), DataflowEvaluationError>,
        ) {
            let TickScope { start, collections } = scope;
            let collections = collections.finish();
            let Some(start) = start else {
                return;
            };
            let revision = self.revision.0;
            let periodic = {
                let Some(recorder) = self.execution.recorder_mut() else {
                    return;
                };
                recorder.tick_end(
                    start,
                    collections,
                    result
                        .as_ref()
                        .err()
                        .map(|error| error as &dyn std::fmt::Display),
                    revision,
                );
                result.is_ok() && recorder.periodic_snapshot_due()
            };
            if periodic {
                let _ = self.record_snapshot(SnapshotPoint::Periodic);
            }
        }

        /// Summarize selected streams of the tick in progress, before history commit moves them.
        #[inline]
        pub(in crate::dataflow) fn diag_record_values(&mut self) {
            let Some(recorder) = self.execution.recorder() else {
                return;
            };
            if !recorder.traces_values() {
                return;
            }
            let layout = self.program.environment_layout();
            let selected = recorder
                .selected_streams()
                .iter()
                .filter_map(|name| {
                    let slot = layout.slot(&crate::VarName::new(name))?;
                    Some((name.clone(), slot.index()))
                })
                .collect::<Vec<_>>();
            let environment = &self.environment_values;
            let values = selected
                .iter()
                .map(|(name, slot)| (name.as_str(), &environment[*slot]))
                .collect::<Vec<(&str, &Value)>>();
            if let Some(recorder) = self.execution.recorder_mut() {
                recorder.record_values(values.into_iter());
            }
        }

        #[inline]
        pub(in crate::dataflow) fn diag_history_commit(&mut self, stamp: Stamp) {
            if let (Some(start), Some(recorder)) = (stamp, self.execution.recorder_mut()) {
                recorder.record_history_commit(start);
            }
        }

        pub(in crate::dataflow) fn diag_schedule_update(&mut self, stamp: Stamp, changed: bool) {
            if let (Some(start), Some(recorder)) = (stamp, self.execution.recorder_mut()) {
                recorder.record_schedule_update(start, changed);
            }
        }

        pub(in crate::dataflow) fn diag_event(
            &mut self,
            name: &'static str,
            args: impl FnOnce() -> serde_json::Value,
        ) {
            if let Some(recorder) = self.execution.recorder_mut() {
                recorder.event(name, args);
            }
        }

        /// Record a definition change after `reset` or root reconfiguration.
        pub(in crate::dataflow) fn diag_definition_installed(&mut self, cause: &'static str) {
            let Some(recorder) = self.execution.recorder() else {
                return;
            };
            let record = DefinitionRecord::capture(&self.program, &recorder.config().limits);
            let revision = self.revision.0;
            if let Some(recorder) = self.execution.recorder_mut() {
                recorder.event(cause, || serde_json::json!({ "revision": revision }));
                recorder.install_definition(record);
            }
        }
    }
}

/// Record one `dynamic`/`defer` activation at the source barrier. `activation` is `None` when it
/// failed; the error itself is recorded when the tick fails.
#[inline]
pub(super) fn record_dynamic_activation(
    execution: &mut crate::dataflow::execution::monitor_execution::MonitorExecution,
    program: &crate::dataflow::DataflowProgram,
    stamp: Stamp,
    expression: usize,
    node: usize,
    stream: crate::dataflow::stream_id::StreamId,
    activation: Option<
        crate::dataflow::execution::reconfigurable_expressions::ReconfigurableExpressionActivation,
    >,
) {
    #[cfg(feature = "diagnostics")]
    {
        use crate::dataflow::execution::reconfigurable_expressions::ReconfigurableExpressionActivation as Activation;
        // Name the plan in effect, which the step loop has not necessarily observed yet.
        execution.observe_active_plan();
        let (Some(start), Some(recorder)) = (stamp, execution.recorder_mut()) else {
            return;
        };
        let outcome = match activation {
            None => "error",
            Some(Activation::Unchanged) => "unchanged",
            Some(Activation::Activated { .. }) => "activated",
            Some(Activation::Replaced { .. }) => "replaced",
        };
        let name = program
            .stream_vars()
            .get(stream.index())
            .map_or_else(String::new, |var| var.name().to_string());
        recorder.record_dynamic(start, expression, node, &name, outcome);
    }
    #[cfg(not(feature = "diagnostics"))]
    {
        let _ = (
            execution, program, stamp, expression, node, stream, activation,
        );
    }
}

/// What a tick's hooks carry from its beginning to its end.
#[cfg(feature = "diagnostics")]
pub(in crate::dataflow) struct TickScope {
    start: Stamp,
    collections: crate::dataflow::diagnostics::collections::CollectionScope,
}

#[cfg(not(feature = "diagnostics"))]
pub(in crate::dataflow) struct TickScope;

impl DataflowMonitor {
    #[inline(always)]
    pub(in crate::dataflow) fn diag_stamp(&self) -> Stamp {
        self.execution.probe().stamp()
    }
}

#[cfg(not(feature = "diagnostics"))]
impl DataflowMonitor {
    #[inline(always)]
    pub(in crate::dataflow) fn diag_tick_begin(&mut self) -> TickScope {
        TickScope
    }

    #[inline(always)]
    pub(in crate::dataflow) fn diag_tick_end(
        &mut self,
        _scope: TickScope,
        _result: &Result<(), DataflowEvaluationError>,
    ) {
    }

    #[inline(always)]
    pub(in crate::dataflow) fn diag_record_values(&mut self) {}

    #[inline(always)]
    pub(in crate::dataflow) fn diag_history_commit(&mut self, _stamp: Stamp) {}

    #[inline(always)]
    pub(in crate::dataflow) fn diag_schedule_update(&mut self, _stamp: Stamp, _changed: bool) {}

    #[inline(always)]
    pub(in crate::dataflow) fn diag_definition_installed(&mut self, _cause: &'static str) {}
}
