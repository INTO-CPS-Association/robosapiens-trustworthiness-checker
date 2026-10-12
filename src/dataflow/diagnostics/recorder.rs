//! Bounded aggregation of one monitor's execution.
//!
//! The recorder keeps aggregates keyed by compact, versioned identities — definition version,
//! plan identity, step index, expression — plus one bounded [`Timeline`]. Every registry has an
//! explicit bound from [`crate::diagnostics::RecorderLimits`]; what exceeds it is counted. Labels
//! are prepared once when a plan is first observed, never on the step path. Reports are built from
//! the recorder on demand and never written from here.
//!
//! # Tick identity
//!
//! `tick` is the zero-based index of the monitor tick in progress (or the next one between ticks).
//! It counts every evaluation that reached the monitor's tick hooks, successful or failed, and is
//! the `tick` argument of every monitor, step and value entry. The runtime adapter uses the same
//! zero-based index for the pipeline spans of the tick it is serving.

use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Instant;

use serde_json::json;

use super::collections::CollectionCounters;
use super::model::DefinitionRecord;
use super::probe::{StepPath, StepPhase};
use crate::core::Value;
use crate::diagnostics::{
    DiagnosticsConfig, DurationStats, MemorySnapshot, SnapshotSkipped, Timeline, TimelineEvent,
    TimelineTrack, nanos_since,
};

/// Identity of one physical plan, unique across reconfigurations of the same monitor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::dataflow) struct PlanKey {
    pub(in crate::dataflow) definition_version: u32,
    pub(in crate::dataflow) id: u64,
    pub(in crate::dataflow) generation: u64,
}

/// What a physical step covers, in stream indices of its definition.
#[derive(Clone, Debug)]
pub(in crate::dataflow) enum StepDescriptor {
    /// One stream's canonical graph, with `islands` quickened scalar islands inside it.
    Graph { stream: usize, islands: usize },
    /// A fused scalar region over whole streams. Its time is not divisible among members.
    Region { region: usize, members: Vec<usize> },
}

#[derive(Clone, Debug, Default)]
pub(in crate::dataflow) struct PlanDescriptor {
    pub(in crate::dataflow) quickening: bool,
    pub(in crate::dataflow) source: Vec<StepDescriptor>,
    pub(in crate::dataflow) main: Vec<StepDescriptor>,
    /// Every stream of the schedule, in plan order: the members of a whole-schedule artifact.
    pub(in crate::dataflow) streams: Vec<usize>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::dataflow) struct StepStats {
    pub(in crate::dataflow) all: DurationStats,
    pub(in crate::dataflow) paths: [DurationStats; StepPath::COUNT],
}

impl StepStats {
    #[inline]
    fn record(&mut self, path: StepPath, nanos: u64) {
        self.all.record(nanos);
        self.paths[path.index()].record(nanos);
    }
}

pub(in crate::dataflow) struct PlanProfile {
    pub(in crate::dataflow) key: PlanKey,
    pub(in crate::dataflow) descriptor: PlanDescriptor,
    pub(in crate::dataflow) source: Vec<StepStats>,
    pub(in crate::dataflow) main: Vec<StepStats>,
    pub(in crate::dataflow) whole: StepStats,
    source_labels: Vec<Rc<str>>,
    main_labels: Vec<Rc<str>>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::dataflow) struct TickTotals {
    pub(in crate::dataflow) succeeded: u64,
    pub(in crate::dataflow) failed: u64,
    pub(in crate::dataflow) duration: DurationStats,
    pub(in crate::dataflow) commit: DurationStats,
    pub(in crate::dataflow) committed_streams: u64,
    pub(in crate::dataflow) history_commit: DurationStats,
}

/// A `dynamic`/`defer` expression, identified by the definition version, the stream that owns it
/// and its graph node in that stream. The expression index alone is not an identity: it is only
/// meaningful within one definition version, and node indices are reused by every definition.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::dataflow) struct ExpressionKey {
    pub(in crate::dataflow) definition_version: u32,
    pub(in crate::dataflow) stream: String,
    /// Top-level graph node of the expression inside its stream; the model map lists it under
    /// this path.
    pub(in crate::dataflow) node: usize,
    pub(in crate::dataflow) expression: usize,
}

#[derive(Clone, Debug, Default)]
pub(in crate::dataflow) struct DynamicTotals {
    pub(in crate::dataflow) by_outcome: BTreeMap<&'static str, DurationStats>,
    pub(in crate::dataflow) by_expression: BTreeMap<ExpressionKey, DurationStats>,
    /// Activations of expressions beyond `max_dynamic_expressions`, aggregated together.
    pub(in crate::dataflow) overflow: DurationStats,
}

/// Retained snapshots and what retention could not keep.
#[derive(Clone, Debug, Default)]
pub(in crate::dataflow) struct SnapshotStore {
    pub(in crate::dataflow) snapshots: Vec<MemorySnapshot>,
    /// Snapshots whose owners were traversed. A snapshot retention would refuse is never built,
    /// so this counts the traversal work done, not the snapshots requested.
    pub(in crate::dataflow) built: u64,
    /// Periodic snapshots not taken because the cap was full.
    pub(in crate::dataflow) periodic_skipped: u64,
    /// Periodic snapshots replaced by mandatory ones.
    pub(in crate::dataflow) periodic_replaced: u64,
    /// Labels of mandatory snapshots skipped because every retained snapshot was mandatory; at
    /// most `limits.max_skipped_snapshot_labels` are kept, the rest only counted.
    pub(in crate::dataflow) mandatory_skipped: Vec<String>,
    /// All mandatory snapshots skipped, including those beyond the kept labels.
    pub(in crate::dataflow) mandatory_skipped_count: u64,
}

/// Registries whose bounds were reached.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub(in crate::dataflow) struct Dropped {
    pub(in crate::dataflow) definitions: u64,
    pub(in crate::dataflow) plans: u64,
    /// Ticks whose steps ran on a plan the recorder could not retain.
    pub(in crate::dataflow) ticks_on_unrecorded_plans: u64,
    pub(in crate::dataflow) dynamic_expressions: u64,
}

pub(in crate::dataflow) struct DataflowRecorder {
    config: DiagnosticsConfig,
    tick: u64,
    tick_in_range: bool,
    pub(in crate::dataflow) ticks: TickTotals,
    pub(in crate::dataflow) plans: Vec<PlanProfile>,
    current_plan: Option<usize>,
    /// The plan in use is not retained (the plan registry is full).
    unrecorded_plan: Option<PlanKey>,
    pub(in crate::dataflow) definitions: Vec<DefinitionRecord>,
    next_definition_version: u32,
    pub(in crate::dataflow) dynamic: DynamicTotals,
    pub(in crate::dataflow) schedule_updates: DurationStats,
    pub(in crate::dataflow) schedule_changes: u64,
    pub(in crate::dataflow) plan_switches: BTreeMap<&'static str, DurationStats>,
    pub(in crate::dataflow) tier_activations: DurationStats,
    pub(in crate::dataflow) event_counts: BTreeMap<&'static str, u64>,
    pub(in crate::dataflow) timeline: Timeline,
    pub(in crate::dataflow) memory: SnapshotStore,
    pub(in crate::dataflow) collections: CollectionCounters,
    pub(in crate::dataflow) dropped: Dropped,
}

impl DataflowRecorder {
    pub(in crate::dataflow) fn new(config: &DiagnosticsConfig) -> Self {
        Self {
            config: config.clone(),
            tick: 0,
            tick_in_range: config.trace.ticks.contains(0),
            ticks: TickTotals::default(),
            plans: Vec::new(),
            current_plan: None,
            unrecorded_plan: None,
            definitions: Vec::new(),
            next_definition_version: 0,
            dynamic: DynamicTotals::default(),
            schedule_updates: DurationStats::default(),
            schedule_changes: 0,
            plan_switches: BTreeMap::new(),
            tier_activations: DurationStats::default(),
            event_counts: BTreeMap::new(),
            timeline: Timeline::new(config.trace.capacity),
            memory: SnapshotStore::default(),
            collections: CollectionCounters::default(),
            dropped: Dropped::default(),
        }
    }

    pub(in crate::dataflow) fn config(&self) -> &DiagnosticsConfig {
        &self.config
    }

    #[inline]
    pub(in crate::dataflow) fn step_timing(&self) -> bool {
        self.config.step_timing
    }

    /// The version of the definition currently running.
    pub(in crate::dataflow) fn definition_version(&self) -> u32 {
        self.next_definition_version.saturating_sub(1)
    }

    fn ns(&self, instant: Instant) -> u64 {
        nanos_since(self.config.epoch, instant)
    }

    fn identity(&self) -> EventIdentity {
        EventIdentity {
            tick: self.tick,
            version: self.definition_version(),
            plan: self.current_plan_key(),
        }
    }

    fn current_plan_key(&self) -> Option<PlanKey> {
        self.current_plan
            .map(|index| self.plans[index].key)
            .or(self.unrecorded_plan)
    }

    // Definitions and plans.

    /// Install the next definition version. The registry keeps the first definition and the most
    /// recent ones; an evicted definition takes its plans with it.
    pub(in crate::dataflow) fn install_definition(&mut self, mut definition: DefinitionRecord) {
        let version = self.next_definition_version;
        self.next_definition_version += 1;
        definition.version = version;
        definition.installed_at_tick = self.tick;
        let key = definition.definition_key.clone();
        let limit = self.config.limits.max_definitions.max(2);
        if self.definitions.len() >= limit {
            let evicted = self.definitions.remove(1).version;
            let before = self.plans.len();
            self.plans
                .retain(|plan| plan.key.definition_version != evicted);
            self.dropped.plans += (before - self.plans.len()) as u64;
            self.dropped.definitions += 1;
        }
        self.definitions.push(definition);
        self.current_plan = None;
        self.unrecorded_plan = None;
        self.event(
            "definition_installed",
            || json!({ "definition_version": version, "definition_key": key }),
        );
    }

    /// Make `key` the plan receiving step records, describing it on first sight.
    #[inline]
    pub(in crate::dataflow) fn observe_plan(
        &mut self,
        id: u64,
        generation: u64,
        describe: impl FnOnce() -> PlanDescriptor,
    ) {
        let key = PlanKey {
            definition_version: self.definition_version(),
            id,
            generation,
        };
        if self.current_plan_key() == Some(key) {
            return;
        }
        if let Some(index) = self.plans.iter().position(|plan| plan.key == key) {
            self.current_plan = Some(index);
            self.unrecorded_plan = None;
            return;
        }
        if self.plans.len() >= self.config.limits.max_plans {
            self.current_plan = None;
            self.unrecorded_plan = Some(key);
            self.dropped.plans += 1;
            return;
        }
        let descriptor = describe();
        let label = |step: &StepDescriptor| -> Rc<str> { self.step_label(step).into() };
        let source_labels = descriptor.source.iter().map(label).collect();
        let main_labels = descriptor.main.iter().map(label).collect();
        let streams = descriptor.streams.len();
        self.plans.push(PlanProfile {
            key,
            source: vec![StepStats::default(); descriptor.source.len()],
            main: vec![StepStats::default(); descriptor.main.len()],
            whole: StepStats::default(),
            descriptor,
            source_labels,
            main_labels,
        });
        self.current_plan = Some(self.plans.len() - 1);
        self.unrecorded_plan = None;
        let version = key.definition_version;
        self.event("plan_described", || {
            json!({
                "definition_version": version,
                "plan": id,
                "generation": generation,
                "streams": streams,
            })
        });
    }

    pub(in crate::dataflow) fn stream_name(&self, version: u32, stream: usize) -> &str {
        self.definitions
            .iter()
            .find(|definition| definition.version == version)
            .and_then(|definition| definition.streams.get(stream))
            .map_or("?", |stream| stream.name.as_str())
    }

    fn step_label(&self, step: &StepDescriptor) -> String {
        let version = self.definition_version();
        match step {
            StepDescriptor::Graph { stream, .. } => self.stream_name(version, *stream).to_owned(),
            StepDescriptor::Region { members, .. } => {
                let mut label = String::from("region[");
                for (index, member) in members.iter().enumerate() {
                    if index > 0 {
                        label.push_str(", ");
                    }
                    if index == 4 {
                        label.push_str(&format!("+{}", members.len() - 4));
                        break;
                    }
                    label.push_str(self.stream_name(version, *member));
                }
                label.push(']');
                label
            }
        }
    }

    // Ticks.

    pub(in crate::dataflow) fn tick_end(
        &mut self,
        start: Instant,
        collections: CollectionCounters,
        error: Option<&dyn std::fmt::Display>,
        revision: u64,
    ) {
        let end = Instant::now();
        self.collections.merge(&collections);
        let nanos = nanos_since(start, end);
        self.ticks.duration.record(nanos);
        if self.current_plan.is_none() && self.unrecorded_plan.is_some() {
            self.dropped.ticks_on_unrecorded_plans += 1;
        }
        let version = self.definition_version();
        let plan = self.current_plan_key();
        match error {
            None => self.ticks.succeeded += 1,
            Some(error) => {
                self.ticks.failed += 1;
                let tick = self.tick;
                let message = error.to_string();
                self.event("tick_failed", || {
                    json!({
                        "tick": tick,
                        "definition_version": version,
                        "revision": revision,
                        "error": message,
                    })
                });
            }
        }
        if self.tick_in_range {
            let start_ns = self.ns(start);
            let tick = self.tick;
            let failed = error.is_some();
            self.timeline.push_with(|| TimelineEvent {
                track: TimelineTrack::Monitor,
                name: "tick".into(),
                category: "tick",
                start_ns,
                duration_ns: Some(nanos),
                args: Some(json!({
                    "tick": tick,
                    "definition_version": version,
                    "plan": plan.map(|plan| plan.id),
                    "generation": plan.map(|plan| plan.generation),
                    "revision": revision,
                    "failed": failed,
                })),
            });
        }
        self.tick += 1;
        self.tick_in_range = self.config.trace.ticks.contains(self.tick);
    }

    #[inline]
    pub(in crate::dataflow) fn record_step(
        &mut self,
        start: Instant,
        phase: StepPhase,
        index: usize,
        path: StepPath,
    ) {
        let end = Instant::now();
        let nanos = nanos_since(start, end);
        let Some(plan) = self.current_plan else {
            return;
        };
        let profile = &mut self.plans[plan];
        let key = profile.key;
        let (stats, label) = match phase {
            StepPhase::Source => (
                profile.source.get_mut(index),
                profile.source_labels.get(index),
            ),
            StepPhase::Main => (profile.main.get_mut(index), profile.main_labels.get(index)),
            StepPhase::Whole => (Some(&mut profile.whole), None),
        };
        if let Some(stats) = stats {
            stats.record(path, nanos);
        }
        let name: std::borrow::Cow<'static, str> = match (phase, label) {
            (StepPhase::Whole, _) => "whole-schedule native".into(),
            (_, Some(label)) => label.to_string().into(),
            (_, None) => "step".into(),
        };
        if path == StepPath::Failed {
            let step = name.to_string();
            let tick = self.tick;
            self.event("step_failed", || {
                json!({
                    "tick": tick,
                    "definition_version": key.definition_version,
                    "plan": key.id,
                    "generation": key.generation,
                    "phase": phase,
                    "index": index,
                    "step": step,
                    "elapsed_ns": nanos,
                })
            });
        }
        if self.tick_in_range {
            let start_ns = nanos_since(self.config.epoch, start);
            let tick = self.tick;
            self.timeline.push_with(|| TimelineEvent {
                track: TimelineTrack::Steps,
                name,
                category: "step",
                start_ns,
                duration_ns: Some(nanos),
                args: Some(json!({
                    "tick": tick,
                    "definition_version": key.definition_version,
                    "plan": key.id,
                    "generation": key.generation,
                    "index": index,
                    "phase": phase,
                    "path": path,
                })),
            });
        }
    }

    pub(in crate::dataflow) fn record_commit(&mut self, start: Instant, streams: usize) {
        self.ticks.commit.record(nanos_since(start, Instant::now()));
        self.ticks.committed_streams += streams as u64;
    }

    pub(in crate::dataflow) fn record_history_commit(&mut self, start: Instant) {
        self.ticks
            .history_commit
            .record(nanos_since(start, Instant::now()));
    }

    // Structural events.

    /// Count a structural event and add it to the timeline when there is room. Structural events
    /// are recorded at any tick, unlike tick and step spans. The tick and definition version are
    /// added to object arguments that do not carry them.
    pub(in crate::dataflow) fn event(
        &mut self,
        name: &'static str,
        args: impl FnOnce() -> serde_json::Value,
    ) {
        *self.event_counts.entry(name).or_default() += 1;
        let start_ns = self.ns(Instant::now());
        let identity = self.identity();
        self.timeline.push_with(|| TimelineEvent {
            track: TimelineTrack::Monitor,
            name: name.into(),
            category: "event",
            start_ns,
            duration_ns: None,
            args: Some(with_identity(args(), identity)),
        });
    }

    /// A structural span, such as a native compilation, recorded at any tick.
    pub(in crate::dataflow) fn span(
        &mut self,
        name: &'static str,
        start: Instant,
        args: impl FnOnce() -> serde_json::Value,
    ) -> u64 {
        let nanos = nanos_since(start, Instant::now());
        *self.event_counts.entry(name).or_default() += 1;
        let start_ns = self.ns(start);
        let identity = self.identity();
        self.timeline.push_with(|| TimelineEvent {
            track: TimelineTrack::Monitor,
            name: name.into(),
            category: "event",
            start_ns,
            duration_ns: Some(nanos),
            args: Some(with_identity(args(), identity)),
        });
        nanos
    }

    pub(in crate::dataflow) fn record_dynamic(
        &mut self,
        start: Instant,
        expression: usize,
        node: usize,
        stream_name: &str,
        outcome: &'static str,
    ) {
        let stream = stream_name.to_owned();
        let event_stream = stream.clone();
        let nanos = self.span("dynamic_activation", start, || {
            json!({
                "expression": expression,
                "node": node,
                "stream": event_stream,
                "outcome": outcome,
            })
        });
        self.dynamic
            .by_outcome
            .entry(outcome)
            .or_default()
            .record(nanos);
        let key = ExpressionKey {
            definition_version: self.definition_version(),
            stream,
            node,
            expression,
        };
        if let Some(stats) = self.dynamic.by_expression.get_mut(&key) {
            stats.record(nanos);
        } else if self.dynamic.by_expression.len() < self.config.limits.max_dynamic_expressions {
            self.dynamic
                .by_expression
                .entry(key)
                .or_default()
                .record(nanos);
        } else {
            self.dropped.dynamic_expressions += 1;
            self.dynamic.overflow.record(nanos);
        }
    }

    pub(in crate::dataflow) fn record_schedule_update(&mut self, start: Instant, changed: bool) {
        self.schedule_updates
            .record(nanos_since(start, Instant::now()));
        if changed {
            self.schedule_changes += 1;
            self.event("schedule_changed", || json!({}));
        }
    }

    pub(in crate::dataflow) fn record_plan_switch(
        &mut self,
        start: Instant,
        outcome: &'static str,
        id: u64,
        generation: u64,
    ) {
        // `plan` and `generation` name the plan switched to; the plan left, when one had been
        // observed, is `from_plan` and `from_generation`.
        let from = self.current_plan_key();
        let nanos = self.span("plan_switch", start, || {
            json!({
                "outcome": outcome,
                "plan": id,
                "generation": generation,
                "from_plan": from.map(|plan| plan.id),
                "from_generation": from.map(|plan| plan.generation),
            })
        });
        self.plan_switches.entry(outcome).or_default().record(nanos);
    }

    pub(in crate::dataflow) fn record_tier_activation(
        &mut self,
        start: Instant,
        outcome: impl FnOnce() -> serde_json::Value,
    ) {
        let nanos = self.span("tier_activation", start, outcome);
        self.tier_activations.record(nanos);
    }

    // Selective value trace.

    /// Whether selected stream values should be summarized for the tick in progress.
    pub(in crate::dataflow) fn traces_values(&self) -> bool {
        self.tick_in_range && !self.config.trace.streams.is_empty()
    }

    pub(in crate::dataflow) fn selected_streams(&self) -> &[String] {
        &self.config.trace.streams
    }

    pub(in crate::dataflow) fn record_values<'v>(
        &mut self,
        values: impl Iterator<Item = (&'v str, &'v Value)>,
    ) {
        let limit = self.config.trace.value_chars;
        let (tick, version) = (self.tick, self.definition_version());
        let start_ns = self.ns(Instant::now());
        self.timeline.push_with(|| {
            let summaries = values
                .map(|(name, value)| (name.to_owned(), summarize(value, limit).into()))
                .collect::<serde_json::Map<_, _>>();
            TimelineEvent {
                track: TimelineTrack::Monitor,
                name: "values".into(),
                category: "values",
                start_ns,
                duration_ns: None,
                args: Some(json!({
                    "tick": tick,
                    "definition_version": version,
                    "values": summaries,
                })),
            }
        });
    }

    // Memory.

    /// Whether the tick that just succeeded completes a periodic interval of successful ticks and
    /// the retention cap leaves room. A full cap counts the skipped snapshot instead, so no
    /// traversal is done for a snapshot that could not be kept.
    pub(in crate::dataflow) fn periodic_snapshot_due(&mut self) -> bool {
        let due = self.config.memory.every_ticks.is_some_and(|every| {
            every > 0 && self.ticks.succeeded > 0 && self.ticks.succeeded.is_multiple_of(every)
        });
        if !due {
            return false;
        }
        if self.memory.snapshots.len() >= self.config.memory.max_snapshots {
            self.memory.periodic_skipped += 1;
            return false;
        }
        true
    }

    /// Whether a mandatory (final, pre-reconfiguration or explicit) snapshot could be retained:
    /// there is room, or a periodic snapshot to replace. Checked before traversal so a snapshot
    /// that cannot be kept is never built.
    pub(in crate::dataflow) fn mandatory_snapshot_fits(&self) -> bool {
        self.memory.snapshots.len() < self.config.memory.max_snapshots
            || self
                .memory
                .snapshots
                .iter()
                .any(|snapshot| snapshot.kind.is_periodic())
    }

    /// Record that a mandatory snapshot labelled `label` could not be retained.
    pub(in crate::dataflow) fn skip_mandatory_snapshot(&mut self, label: String) {
        self.memory.mandatory_skipped_count += 1;
        if self.memory.mandatory_skipped.len() < self.config.limits.max_skipped_snapshot_labels {
            self.memory.mandatory_skipped.push(label);
        }
    }

    /// Retain `snapshot` under the cap, returning its index in the store, or why it was not kept.
    ///
    /// With room, every kind is kept. At the cap a periodic snapshot is counted as skipped, and a
    /// mandatory one replaces the most recent periodic snapshot (the earliest ones stay as a
    /// baseline); when every retained snapshot is mandatory it is skipped and its label recorded.
    /// A refused snapshot never alters the store.
    pub(in crate::dataflow) fn push_snapshot(
        &mut self,
        snapshot: MemorySnapshot,
    ) -> Result<usize, SnapshotSkipped> {
        let cap = self.config.memory.max_snapshots;
        if self.memory.snapshots.len() < cap {
            self.memory.snapshots.push(snapshot);
            return Ok(self.memory.snapshots.len() - 1);
        }
        if snapshot.kind.is_periodic() {
            self.memory.periodic_skipped += 1;
            return Err(SnapshotSkipped::RetentionFull);
        }
        match self
            .memory
            .snapshots
            .iter()
            .rposition(|retained| retained.kind.is_periodic())
        {
            Some(index) => {
                self.memory.snapshots.remove(index);
                self.memory.snapshots.push(snapshot);
                self.memory.periodic_replaced += 1;
                Ok(self.memory.snapshots.len() - 1)
            }
            None => {
                self.skip_mandatory_snapshot(snapshot.label);
                Err(SnapshotSkipped::RetentionFull)
            }
        }
    }
}

/// Where an event happened: the tick in progress, the definition version and, when a plan has
/// been observed, the plan identity and generation then in effect.
#[derive(Clone, Copy)]
struct EventIdentity {
    tick: u64,
    version: u32,
    plan: Option<PlanKey>,
}

/// Add the tick, definition version and plan identity to an object argument that lacks them. An
/// event naming a plan of its own (a plan switch names the plan it switched to) keeps it.
fn with_identity(mut args: serde_json::Value, identity: EventIdentity) -> serde_json::Value {
    if let serde_json::Value::Object(fields) = &mut args {
        fields.entry("tick").or_insert(identity.tick.into());
        fields
            .entry("definition_version")
            .or_insert(identity.version.into());
        if let Some(plan) = identity.plan {
            fields.entry("plan").or_insert(plan.id.into());
            fields.entry("generation").or_insert(plan.generation.into());
        }
    }
    args
}

/// A bounded rendering of `value`: its variant, sizes and a prefix of its display text. Reads the
/// value only; nothing is cloned, and formatting stops once the limit is reached.
pub(in crate::dataflow) fn summarize(value: &Value, limit: usize) -> String {
    use std::fmt::Write as _;

    struct Bounded {
        text: String,
        chars: usize,
        limit: usize,
        truncated: bool,
    }
    impl std::fmt::Write for Bounded {
        fn write_str(&mut self, part: &str) -> std::fmt::Result {
            let room = self.limit.saturating_sub(self.chars);
            let mut taken = 0;
            for (offset, character) in part.char_indices() {
                if taken == room {
                    self.text.push_str(&part[..offset]);
                    self.chars += taken;
                    self.truncated = true;
                    return Err(std::fmt::Error);
                }
                let _ = character;
                taken += 1;
            }
            self.text.push_str(part);
            self.chars += taken;
            Ok(())
        }
    }

    let kind = match value {
        Value::List(items) => format!("list[{}] ", items.len()),
        Value::Tuple(items) => format!("tuple[{}] ", items.len()),
        Value::Map(map) => format!("map[{}] ", map.len()),
        _ => String::new(),
    };
    let chars = kind.chars().count();
    let mut bounded = Bounded {
        text: kind,
        chars,
        limit: limit.max(8),
        truncated: false,
    };
    let _ = write!(bounded, "{value}");
    if bounded.truncated {
        bounded.text.push('…');
    }
    bounded.text
}
