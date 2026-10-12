//! Run-level diagnostics: pipeline phase timing in the dataflow runtime adapter, and assembly of
//! one run's report and artifacts.
//!
//! The runtime adapter owns a [`RuntimeProbe`]. Without the `diagnostics` feature it is zero-sized
//! and its methods are empty. With the feature, a runtime built with a
//! [`DiagnosticsHub`](crate::diagnostics::DiagnosticsHub) times its phases — input waits, row
//! preparation, monitor evaluation, output packing, output admission and shutdown — and, when it
//! finishes, combines them with the monitor's [`DataflowDiagnosticsReport`] and every input-source
//! record registered with the hub into a [`RunReport`]. When the hub's configuration names an
//! output directory the runtime writes `summary.json` and `trace.json` there.
//!
//! Writing artifacts never changes a run's result: a write failure is logged as a warning.
//!
//! Library users driving a [`DataflowMonitor`] directly can build the same artifacts with
//! [`RunArtifacts::for_monitor`].

// Diagnostics only read state; none of this code may need `unsafe`.
#![forbid(unsafe_code)]

#[cfg(feature = "diagnostics")]
mod enabled {
    use std::path::{Path, PathBuf};
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

    use serde::Serialize;
    use serde_json::json;

    use crate::dataflow::{DataflowDiagnosticsReport, DataflowMonitor};
    use crate::diagnostics::{
        DiagnosticsConfig, DiagnosticsHub, PipelinePhase, PipelineProfile, REPORT_SCHEMA, Timeline,
        TimelineEvent, TimelineTrack, chrome, nanos_since,
    };

    pub(crate) type Stamp = Option<Instant>;

    struct RunRecorder {
        hub: DiagnosticsHub,
        profile: PipelineProfile,
        timeline: Timeline,
        ticks: u64,
        started: Instant,
    }

    /// Pipeline hooks of one runtime adapter.
    #[derive(Default)]
    pub(crate) struct RuntimeProbe(Option<Box<RunRecorder>>);

    impl RuntimeProbe {
        pub(crate) fn new(hub: Option<DiagnosticsHub>) -> Self {
            Self(hub.map(|hub| {
                let capacity = hub.config().trace.capacity;
                let epoch = hub.config().epoch;
                Box::new(RunRecorder {
                    hub,
                    profile: PipelineProfile::default(),
                    timeline: Timeline::new(capacity),
                    ticks: 0,
                    // The run starts at the session epoch, the origin of every timeline.
                    started: epoch,
                })
            }))
        }

        #[inline]
        pub(crate) fn stamp(&self) -> Stamp {
            self.0.as_ref().map(|_| Instant::now())
        }

        #[inline]
        pub(crate) fn phase(&mut self, stamp: Stamp, phase: PipelinePhase) {
            let (Some(start), Some(recorder)) = (stamp, self.0.as_deref_mut()) else {
                return;
            };
            let nanos = nanos_since(start, Instant::now());
            recorder.profile.record(phase, nanos);
            let config = recorder.hub.config();
            if config.trace.ticks.contains(recorder.ticks) {
                let start_ns = nanos_since(config.epoch, start);
                let tick = recorder.ticks;
                recorder.timeline.push_with(|| TimelineEvent {
                    track: TimelineTrack::Pipeline,
                    name: phase.name().into(),
                    category: "pipeline",
                    start_ns,
                    duration_ns: Some(nanos),
                    args: Some(json!({ "tick": tick, "kind": phase.kind() })),
                });
            }
        }

        #[inline]
        pub(crate) fn count(&mut self, name: &'static str, amount: u64) {
            if let Some(recorder) = self.0.as_deref_mut() {
                recorder.profile.counters.add(name, amount);
            }
        }

        #[inline]
        pub(crate) fn max(&mut self, name: &'static str, value: u64) {
            if let Some(recorder) = self.0.as_deref_mut() {
                recorder.profile.counters.max(name, value);
            }
        }

        /// One logical tick was handed to the monitor.
        #[inline]
        pub(crate) fn tick(&mut self) {
            if let Some(recorder) = self.0.as_deref_mut() {
                recorder.ticks += 1;
                recorder.profile.counters.add("logical_ticks", 1);
            }
        }

        /// Record the final memory snapshot, build the run report and write artifacts if the
        /// configuration names an output directory. Never alters `outcome`.
        pub(crate) fn finish(
            &mut self,
            monitor: &mut DataflowMonitor,
            outcome: &anyhow::Result<()>,
        ) {
            let Some(recorder) = self.0.take() else {
                return;
            };
            // A skipped final snapshot is recorded in the report's mandatory skips.
            let _ = monitor.record_final_memory_snapshot();
            let RunRecorder {
                hub,
                profile,
                timeline,
                started,
                ..
            } = *recorder;
            let outcome = outcome.as_ref().err().map(|error| format!("{error:#}"));
            let artifacts =
                RunArtifacts::build(&hub, Some(&*monitor), profile, timeline, started, outcome);
            if let Some(directory) = &hub.config().output_dir {
                match artifacts.write(directory) {
                    Ok((summary, trace)) => tracing::info!(
                        summary = %summary.display(),
                        trace = %trace.display(),
                        "wrote diagnostics artifacts"
                    ),
                    Err(error) => tracing::warn!(
                        directory = %directory.display(),
                        "could not write diagnostics artifacts: {error:#}"
                    ),
                }
            }
        }
    }

    /// Identity of one run, for reproducing it.
    #[derive(Clone, Debug, Serialize)]
    pub struct RunIdentity {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub label: Option<String>,
        pub started_unix_ms: u128,
        pub elapsed_ns: u64,
        pub crate_version: &'static str,
        pub features: Vec<&'static str>,
        /// Whether the binary was built with `debug_assertions`. That is a build setting, not
        /// proof that the code ran unoptimized: a profile can enable it with optimizations on, or
        /// disable it without.
        pub debug_assertions: bool,
        pub metadata: std::collections::BTreeMap<String, String>,
        pub step_timing: bool,
        pub trace: crate::diagnostics::TraceConfig,
        pub memory: crate::diagnostics::MemoryConfig,
    }

    #[derive(Clone, Debug, Serialize)]
    pub struct SourceSummary {
        pub name: String,
        pub profile: PipelineProfile,
        pub timeline_dropped: u64,
    }

    /// The machine-readable summary of one run (`summary.json`).
    #[derive(Clone, Debug, Serialize)]
    pub struct RunReport {
        pub schema: &'static str,
        pub run: RunIdentity,
        /// The run's error, or `None` when it succeeded.
        pub error: Option<String>,
        pub pipeline: PipelineProfile,
        pub sources: Vec<SourceSummary>,
        /// Sources opened beyond the hub's registry bound, whose phases are not reported.
        pub sources_dropped: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub dataflow: Option<DataflowDiagnosticsReport>,
        pub notes: Vec<&'static str>,
    }

    /// A run report and its Chrome/Perfetto trace, ready to write.
    pub struct RunArtifacts {
        pub report: RunReport,
        pub trace: serde_json::Value,
    }

    fn features() -> Vec<&'static str> {
        let mut features = vec!["diagnostics"];
        if cfg!(feature = "jit") {
            features.push("jit");
        }
        if cfg!(feature = "jemalloc") {
            features.push("jemalloc");
        }
        if cfg!(feature = "redis") {
            features.push("redis");
        }
        features
    }

    impl RunArtifacts {
        /// Artifacts for a monitor driven directly, without a runtime adapter. `pipeline` may carry
        /// phases the caller timed itself.
        pub fn for_monitor(
            hub: &DiagnosticsHub,
            monitor: &DataflowMonitor,
            pipeline: Option<PipelineProfile>,
        ) -> Self {
            Self::build(
                hub,
                Some(monitor),
                pipeline.unwrap_or_default(),
                Timeline::new(0),
                hub.config().epoch,
                None,
            )
        }

        fn build(
            hub: &DiagnosticsHub,
            monitor: Option<&DataflowMonitor>,
            pipeline: PipelineProfile,
            pipeline_timeline: Timeline,
            started: Instant,
            error: Option<String>,
        ) -> Self {
            let config: &DiagnosticsConfig = hub.config();
            let mut sources = Vec::new();
            let mut source_timelines = Vec::new();
            hub.for_each_source(|record| {
                sources.push(SourceSummary {
                    name: record.name.clone(),
                    profile: record.profile.clone(),
                    timeline_dropped: record.timeline.dropped(),
                });
                source_timelines.push(record.timeline.clone());
            });
            let dataflow = monitor.and_then(DataflowMonitor::diagnostics_report);
            let started_unix_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|now| now.as_millis())
                .unwrap_or_default()
                .saturating_sub(started.elapsed().as_millis());
            let run = RunIdentity {
                label: config.label.clone(),
                started_unix_ms,
                elapsed_ns: nanos_since(started, Instant::now()),
                crate_version: env!("CARGO_PKG_VERSION"),
                features: features(),
                debug_assertions: cfg!(debug_assertions),
                metadata: config.metadata.clone(),
                step_timing: config.step_timing,
                trace: config.trace.clone(),
                memory: config.memory.clone(),
            };
            let mut timelines = vec![&pipeline_timeline];
            if let Some(timeline) = monitor.and_then(DataflowMonitor::diagnostics_timeline) {
                timelines.push(timeline);
            }
            timelines.extend(source_timelines.iter());
            let trace = chrome::export(
                timelines,
                json!({ "schema": REPORT_SCHEMA, "label": config.label }),
            );
            Self {
                report: RunReport {
                    schema: REPORT_SCHEMA,
                    run,
                    error,
                    pipeline,
                    sources,
                    sources_dropped: hub.dropped_sources(),
                    dataflow,
                    notes: vec![
                        "Durations are wall-clock elapsed time, not CPU time.",
                        "elapsed_wait phases include any synchronous work done by the awaited \
                         future, such as replay decoding, and time spent on other tasks.",
                        "Memory figures are structured estimates, not allocator measurements \
                         or RSS.",
                    ],
                },
                trace,
            }
        }

        /// Write `summary.json` and `trace.json` into `directory`, creating it if needed.
        pub fn write(&self, directory: &Path) -> std::io::Result<(PathBuf, PathBuf)> {
            std::fs::create_dir_all(directory)?;
            let summary = directory.join("summary.json");
            let trace = directory.join("trace.json");
            std::fs::write(&summary, serde_json::to_vec_pretty(&self.report)?)?;
            std::fs::write(&trace, serde_json::to_vec(&self.trace)?)?;
            Ok((summary, trace))
        }
    }
}

#[cfg(not(feature = "diagnostics"))]
mod disabled {
    use crate::dataflow::DataflowMonitor;

    pub(crate) type Stamp = ();

    /// Phases the runtime records; only names without the `diagnostics` feature.
    #[derive(Clone, Copy)]
    pub(crate) enum PipelinePhase {
        InputWait,
        RowPreparation,
        MonitorEvaluation,
        OutputPacking,
        OutputSend,
        OutputFinish,
        InputDrain,
        Reconfiguration,
        Compile,
    }

    #[derive(Default)]
    pub(crate) struct RuntimeProbe;

    impl RuntimeProbe {
        #[inline(always)]
        pub(crate) fn stamp(&self) -> Stamp {}

        #[inline(always)]
        pub(crate) fn phase(&mut self, _stamp: Stamp, _phase: PipelinePhase) {}

        #[inline(always)]
        pub(crate) fn count(&mut self, _name: &'static str, _amount: u64) {}

        #[inline(always)]
        pub(crate) fn max(&mut self, _name: &'static str, _value: u64) {}

        #[inline(always)]
        pub(crate) fn tick(&mut self) {}

        #[inline(always)]
        pub(crate) fn finish(
            &mut self,
            _monitor: &mut DataflowMonitor,
            _outcome: &anyhow::Result<()>,
        ) {
        }
    }
}

#[cfg(feature = "diagnostics")]
pub(crate) use crate::diagnostics::PipelinePhase;
#[cfg(not(feature = "diagnostics"))]
pub(crate) use disabled::{PipelinePhase, RuntimeProbe};
#[cfg(feature = "diagnostics")]
pub(crate) use enabled::RuntimeProbe;
#[cfg(feature = "diagnostics")]
pub use enabled::{RunArtifacts, RunIdentity, RunReport, SourceSummary};
