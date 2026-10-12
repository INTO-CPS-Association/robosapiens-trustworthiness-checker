use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;

use serde::Serialize;

/// What one diagnostics session captures.
///
/// Every bound is explicit: the trace buffer, the detailed tick range, the number of memory
/// snapshots and the work each snapshot may do. Observation never changes evaluation; a smaller
/// bound only means less detail, labelled as dropped or truncated in the report.
#[derive(Clone, Debug)]
pub struct DiagnosticsConfig {
    /// Directory receiving `summary.json` and `trace.json` when a runtime finishes.
    pub output_dir: Option<PathBuf>,
    /// Time each physical execution step. Tick, commit and phase totals are kept either way.
    pub step_timing: bool,
    pub trace: TraceConfig,
    pub memory: MemoryConfig,
    pub limits: RecorderLimits,
    /// Common origin of every timeline in the session, so pipeline, monitor and input tracks line
    /// up in one trace.
    pub epoch: Instant,
    /// Free-form label copied into the run identity.
    pub label: Option<String>,
    /// Further run identity, such as the specification path and command line.
    pub metadata: std::collections::BTreeMap<String, String>,
}

impl Default for DiagnosticsConfig {
    fn default() -> Self {
        Self {
            output_dir: None,
            step_timing: true,
            trace: TraceConfig::default(),
            memory: MemoryConfig::default(),
            limits: RecorderLimits::default(),
            epoch: Instant::now(),
            label: None,
            metadata: Default::default(),
        }
    }
}

impl DiagnosticsConfig {
    /// A default configuration writing artifacts into `output_dir`.
    pub fn with_output_dir(output_dir: impl Into<PathBuf>) -> Self {
        Self {
            output_dir: Some(output_dir.into()),
            ..Self::default()
        }
    }
}

/// Selection and bounds of the event trace.
#[derive(Clone, Debug, Serialize)]
pub struct TraceConfig {
    /// Maximum number of retained timeline entries per recorder. Later entries are dropped and
    /// counted rather than evicting earlier ones, so the start of a run is always complete.
    pub capacity: usize,
    /// Logical ticks (zero-based, per monitor) receiving tick and step spans and value summaries.
    /// Structural events such as schedule, revision, tier and reconfiguration changes are
    /// recorded at any tick, subject only to `capacity`.
    pub ticks: TickRange,
    /// Streams whose values are summarized for ticks in `ticks`. Empty selects none.
    pub streams: Vec<String>,
    /// Maximum characters of one value summary.
    pub value_chars: usize,
}

impl Default for TraceConfig {
    fn default() -> Self {
        Self {
            capacity: 100_000,
            ticks: TickRange::new(0, Some(256)),
            streams: Vec::new(),
            value_chars: 96,
        }
    }
}

/// A half-open range of zero-based tick indices. `end: None` is unbounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct TickRange {
    pub start: u64,
    pub end: Option<u64>,
}

impl TickRange {
    pub const ALL: Self = Self {
        start: 0,
        end: None,
    };
    pub const NONE: Self = Self {
        start: 0,
        end: Some(0),
    };

    pub const fn new(start: u64, end: Option<u64>) -> Self {
        Self { start, end }
    }

    #[inline]
    pub fn contains(&self, tick: u64) -> bool {
        tick >= self.start && self.end.is_none_or(|end| tick < end)
    }
}

impl FromStr for TickRange {
    type Err = String;

    /// Parses `START..END`, `START..`, `..END`, `all` or `none`.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text.trim() {
            "all" => return Ok(Self::ALL),
            "none" => return Ok(Self::NONE),
            _ => {}
        }
        let (start, end) = text.split_once("..").ok_or_else(|| {
            format!("expected START..END, START.., ..END, all or none; got `{text}`")
        })?;
        let parse = |part: &str| -> Result<Option<u64>, String> {
            let part = part.trim();
            if part.is_empty() {
                Ok(None)
            } else {
                part.parse::<u64>()
                    .map(Some)
                    .map_err(|error| format!("invalid tick `{part}`: {error}"))
            }
        };
        let start = parse(start)?.unwrap_or(0);
        let end = parse(end)?;
        if end.is_some_and(|end| end < start) {
            return Err(format!("tick range `{text}` ends before it starts"));
        }
        Ok(Self { start, end })
    }
}

/// Bounds on the registries a monitor recorder retains, so a long or frequently reconfigured run
/// cannot grow its report without limit. What exceeds a bound is counted and reported.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct RecorderLimits {
    /// Definition records (model maps) retained. The first definition and the most recent ones are
    /// kept; an evicted definition's plans are evicted with it.
    pub max_definitions: usize,
    /// Physical plans with step aggregates retained. Steps of plans beyond the bound are not
    /// recorded; their ticks still count.
    pub max_plans: usize,
    /// Graph nodes listed per definition in the model map.
    pub max_model_nodes: usize,
    /// Distinct `dynamic`/`defer` expressions with their own activation aggregates.
    pub max_dynamic_expressions: usize,
    /// Outputs receiving a dependency-closure aggregate, and streams listed per closure.
    pub max_dependency_outputs: usize,
    pub max_listed_closure_streams: usize,
    /// Input-source recorders retained by a hub. A source opened beyond the bound still records
    /// into a private record, but it is not reported; the hub counts it as dropped.
    pub max_sources: usize,
    /// Labels kept of mandatory memory snapshots that retention refused; the rest are counted.
    pub max_skipped_snapshot_labels: usize,
    /// Bytes of source one definition capture may scan for line and column positions, in total
    /// across files (indexing line starts plus counting columns). Past it, located nodes keep
    /// their file, span and snippet and report no line or column, flagged `position_truncated`.
    pub max_source_scan_bytes: usize,
}

impl Default for RecorderLimits {
    fn default() -> Self {
        Self {
            max_definitions: 16,
            max_plans: 256,
            max_model_nodes: 20_000,
            max_dynamic_expressions: 1_024,
            max_dependency_outputs: 256,
            max_listed_closure_streams: 64,
            max_sources: 64,
            max_skipped_snapshot_labels: 32,
            max_source_scan_bytes: 16 << 20,
        }
    }
}

/// When structured memory snapshots are taken and how much each may traverse.
#[derive(Clone, Debug, Serialize)]
pub struct MemoryConfig {
    /// Snapshot after every this many successful ticks. A runtime also requests a final snapshot
    /// when it finishes and the monitor requests one before each root reconfiguration.
    pub every_ticks: Option<u64>,
    /// Maximum number of retained snapshots. Periodic snapshots are not taken once it is reached.
    /// A final, pre-reconfiguration or explicit snapshot then replaces the most recent periodic
    /// one; if every retained snapshot is of those kinds, it is skipped and its label recorded.
    pub max_snapshots: usize,
    pub budget: super::MemoryBudget,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            every_ticks: None,
            max_snapshots: 32,
            budget: super::MemoryBudget::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_ranges_parse_and_bound() {
        let range: TickRange = "10..20".parse().unwrap();
        assert!(!range.contains(9));
        assert!(range.contains(10));
        assert!(range.contains(19));
        assert!(!range.contains(20));
        assert_eq!("5..".parse::<TickRange>().unwrap(), TickRange::new(5, None));
        assert_eq!(
            "..3".parse::<TickRange>().unwrap(),
            TickRange::new(0, Some(3))
        );
        assert_eq!("all".parse::<TickRange>().unwrap(), TickRange::ALL);
        assert!(!"none".parse::<TickRange>().unwrap().contains(0));
        assert!("4..2".parse::<TickRange>().is_err());
        assert!("x".parse::<TickRange>().is_err());
    }
}
