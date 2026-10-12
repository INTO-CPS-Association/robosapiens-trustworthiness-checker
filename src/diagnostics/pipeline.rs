use std::collections::BTreeMap;
use std::time::Instant;

use serde::Serialize;

use super::DurationStats;

/// Whether a phase's elapsed time is synchronous work or an awaited wait.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PhaseKind {
    /// Elapsed time of code that ran without yielding. Wall-clock, not CPU time.
    SynchronousWork,
    /// Elapsed time until an awaited future completed. It includes any synchronous work that
    /// future performed while polled, and time the executor spent elsewhere.
    ElapsedWait,
}

/// One stage of the runtime pipeline.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PipelinePhase {
    /// Specification compilation into a dataflow program, at runtime construction.
    Compile,
    /// Awaiting the next input batch from the input stream.
    InputWait,
    /// Reading and decoding one source record (source owned).
    InputDecode,
    /// Projecting decoded records and clock samples into the input variable's value (source owned).
    InputProjection,
    /// Checking a projected value against the declared input type (source owned).
    InputConformance,
    /// Writing input provenance (source owned).
    InputProvenance,
    /// Writing one logical tick's values into the reusable monitor input row.
    RowPreparation,
    /// One call of the monitor's synchronous evaluation.
    MonitorEvaluation,
    /// Appending a completed output row and packing rows into an output batch.
    OutputPacking,
    /// Awaiting admission of one output batch by the writer.
    OutputSend,
    /// Awaiting the final writer flush and close.
    OutputFinish,
    /// Draining input at shutdown.
    InputDrain,
    /// Planning and applying a root reconfiguration, including its I/O cutover.
    Reconfiguration,
}

impl PipelinePhase {
    pub const fn kind(self) -> PhaseKind {
        match self {
            Self::InputWait
            | Self::OutputSend
            | Self::OutputFinish
            | Self::InputDrain
            | Self::Reconfiguration => PhaseKind::ElapsedWait,
            Self::Compile
            | Self::InputDecode
            | Self::InputProjection
            | Self::InputConformance
            | Self::InputProvenance
            | Self::RowPreparation
            | Self::MonitorEvaluation
            | Self::OutputPacking => PhaseKind::SynchronousWork,
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Compile => "compile",
            Self::InputWait => "input_wait",
            Self::InputDecode => "input_decode",
            Self::InputProjection => "input_projection",
            Self::InputConformance => "input_conformance",
            Self::InputProvenance => "input_provenance",
            Self::RowPreparation => "row_preparation",
            Self::MonitorEvaluation => "monitor_evaluation",
            Self::OutputPacking => "output_packing",
            Self::OutputSend => "output_send",
            Self::OutputFinish => "output_finish",
            Self::InputDrain => "input_drain",
            Self::Reconfiguration => "reconfiguration",
        }
    }
}

/// Start of a timed pipeline phase.
pub type PipelineStamp = Instant;

/// Named monotone counters and running maxima.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Counters {
    pub counts: BTreeMap<&'static str, u64>,
    pub maxima: BTreeMap<&'static str, u64>,
}

impl Counters {
    #[inline]
    pub fn add(&mut self, name: &'static str, amount: u64) {
        *self.counts.entry(name).or_default() += amount;
    }

    #[inline]
    pub fn max(&mut self, name: &'static str, value: u64) {
        let entry = self.maxima.entry(name).or_default();
        *entry = (*entry).max(value);
    }

    pub fn count(&self, name: &str) -> u64 {
        self.counts.get(name).copied().unwrap_or(0)
    }

    pub fn merge(&mut self, other: &Self) {
        for (name, value) in &other.counts {
            self.add(name, *value);
        }
        for (name, value) in &other.maxima {
            self.max(name, *value);
        }
    }
}

/// Aggregate elapsed time per pipeline phase, plus counters.
#[derive(Clone, Debug, Default)]
pub struct PipelineProfile {
    phases: BTreeMap<PipelinePhase, DurationStats>,
    pub counters: Counters,
}

#[derive(Serialize)]
struct PhaseEntry {
    phase: PipelinePhase,
    kind: PhaseKind,
    #[serde(flatten)]
    stats: DurationStats,
}

impl PipelineProfile {
    #[inline]
    pub fn record(&mut self, phase: PipelinePhase, nanos: u64) {
        self.phases.entry(phase).or_default().record(nanos);
    }

    pub fn phase(&self, phase: PipelinePhase) -> DurationStats {
        self.phases.get(&phase).copied().unwrap_or_default()
    }

    pub fn phases(&self) -> impl Iterator<Item = (PipelinePhase, DurationStats)> + '_ {
        self.phases.iter().map(|(phase, stats)| (*phase, *stats))
    }

    pub fn merge(&mut self, other: &Self) {
        for (phase, stats) in &other.phases {
            self.phases.entry(*phase).or_default().merge(stats);
        }
        self.counters.merge(&other.counters);
    }
}

impl Serialize for PipelineProfile {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Profile<'a> {
            phases: Vec<PhaseEntry>,
            counters: &'a Counters,
        }
        Profile {
            phases: self
                .phases
                .iter()
                .map(|(phase, stats)| PhaseEntry {
                    phase: *phase,
                    kind: phase.kind(),
                    stats: *stats,
                })
                .collect(),
            counters: &self.counters,
        }
        .serialize(serializer)
    }
}
