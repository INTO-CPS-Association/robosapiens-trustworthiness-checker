//! The machine-readable report of one monitor's diagnostics.

use std::collections::BTreeMap;

use serde::Serialize;

use super::dependencies::{self, Cost, DependencyReport};
use super::model::DefinitionRecord;
use super::probe::{StepPath, StepPhase};
use super::recorder::{DataflowRecorder, StepDescriptor, StepStats};
use super::sites::SourcePoint;
use crate::dataflow::DataflowProgram;
use crate::dataflow::environment::EnvironmentSlot;
use crate::diagnostics::{DurationStats, MemorySnapshot};

/// What a monitor recorded, ready to serialize as JSON.
///
/// Stream indices are scoped by `definition_version`: each definition the monitor ran is listed
/// in `definitions`, and every plan, step and event names the version it ran under.
#[derive(Clone, Debug, Serialize)]
pub struct DataflowDiagnosticsReport {
    definitions: Vec<DefinitionRecord>,
    ticks: TicksReport,
    plans: Vec<PlanReport>,
    streams: Vec<StreamTimeReport>,
    regions: Vec<RegionTimeReport>,
    definition_sites: Vec<DefinitionSiteReport>,
    dynamic: DynamicReport,
    collections: CollectionsReport,
    scheduler: SchedulerReport,
    tiers: TierReport,
    events: BTreeMap<&'static str, u64>,
    memory: MemoryReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    dependencies: Option<DependencyReport>,
    /// Registry entries beyond the recorder's limits.
    dropped: super::recorder::Dropped,
    timeline: TimelineSummary,
    coverage: Vec<&'static str>,
}

impl DataflowDiagnosticsReport {
    /// The report as a JSON value.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("diagnostics reports serialize to JSON")
    }

    pub fn ticks_succeeded(&self) -> u64 {
        self.ticks.succeeded
    }

    pub fn ticks_failed(&self) -> u64 {
        self.ticks.failed
    }

    pub fn memory_snapshots(&self) -> &[MemorySnapshot] {
        &self.memory.snapshots
    }
}

#[derive(Clone, Debug, Serialize)]
struct TicksReport {
    attempted: u64,
    succeeded: u64,
    failed: u64,
    duration: DurationStats,
    commit: DurationStats,
    committed_streams: u64,
    history_commit: DurationStats,
}

#[derive(Clone, Debug, Serialize)]
struct PlanReport {
    definition_version: u32,
    plan: u64,
    generation: u64,
    quickening: bool,
    steps: Vec<StepReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    whole_schedule_native: Option<StepReport>,
}

#[derive(Clone, Debug, Serialize)]
struct StepReport {
    phase: StepPhase,
    index: usize,
    kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    members: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    islands: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    region: Option<usize>,
    total: DurationStats,
    paths: BTreeMap<String, DurationStats>,
}

/// Elapsed time of graph steps that evaluated exactly one stream, summed over plans.
#[derive(Clone, Debug, Serialize)]
struct StreamTimeReport {
    definition_version: u32,
    stream: String,
    total: DurationStats,
    paths: BTreeMap<String, DurationStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    site: Option<SourcePoint>,
}

/// A fused region or whole-schedule artifact: one aggregate over its members.
#[derive(Clone, Debug, Serialize)]
struct RegionTimeReport {
    definition_version: u32,
    kind: &'static str,
    members: Vec<String>,
    total: DurationStats,
    paths: BTreeMap<String, DurationStats>,
}

/// Where inlined definitions were written and which call sites inlined them.
#[derive(Clone, Debug, Serialize)]
struct DefinitionSiteReport {
    definition: SourcePoint,
    call_sites: Vec<CallSiteReport>,
}

#[derive(Clone, Debug, Serialize)]
struct CallSiteReport {
    stream: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    call_site: Option<SourcePoint>,
    nodes: usize,
}

#[derive(Clone, Debug, Serialize)]
struct DynamicReport {
    by_outcome: BTreeMap<&'static str, DurationStats>,
    by_expression: Vec<ExpressionReport>,
    overflow: DurationStats,
    note: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct ExpressionReport {
    definition_version: u32,
    stream: String,
    /// Graph node of the expression in its stream; a path in the definition's model map.
    node: usize,
    /// Expression index within the definition version.
    expression: usize,
    activations: DurationStats,
}

#[derive(Clone, Debug, Serialize)]
struct CollectionsReport {
    #[serde(flatten)]
    counters: super::collections::CollectionCounters,
    note: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct SchedulerReport {
    updates: DurationStats,
    changes: u64,
    plan_switches: BTreeMap<&'static str, DurationStats>,
}

#[derive(Clone, Debug, Serialize)]
struct TierReport {
    activations: DurationStats,
    #[serde(skip_serializing_if = "Option::is_none")]
    jit: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize)]
struct MemoryReport {
    snapshots: Vec<MemorySnapshot>,
    max_snapshots: usize,
    /// Snapshots whose owners were traversed; refused snapshots are never built.
    snapshots_built: u64,
    /// Periodic snapshots not taken because the cap was full.
    periodic_skipped: u64,
    /// Periodic snapshots replaced by mandatory ones.
    periodic_replaced: u64,
    /// Labels of mandatory snapshots skipped because every retained snapshot was mandatory,
    /// up to a bound; `mandatory_skipped_count` counts them all.
    mandatory_skipped: Vec<String>,
    mandatory_skipped_count: u64,
    note: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct TimelineSummary {
    retained: usize,
    dropped: u64,
    capacity: usize,
}

fn paths(stats: &StepStats) -> BTreeMap<String, DurationStats> {
    StepPath::ALL
        .iter()
        .filter(|path| !stats.paths[path.index()].is_empty())
        .map(|path| {
            (
                serde_json::to_value(path)
                    .ok()
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .unwrap_or_default(),
                stats.paths[path.index()],
            )
        })
        .collect()
}

fn merge_paths(into: &mut BTreeMap<String, DurationStats>, from: BTreeMap<String, DurationStats>) {
    for (path, stats) in from {
        into.entry(path).or_default().merge(&stats);
    }
}

pub(in crate::dataflow) struct ReportInputs<'a> {
    pub(in crate::dataflow) program: &'a DataflowProgram,
    pub(in crate::dataflow) dynamic_slots: &'a dyn Fn(usize) -> Vec<EnvironmentSlot>,
    pub(in crate::dataflow) jit: Option<serde_json::Value>,
}

impl DataflowDiagnosticsReport {
    pub(in crate::dataflow) fn build(
        recorder: &DataflowRecorder,
        inputs: ReportInputs<'_>,
    ) -> Self {
        let name = |version: u32, stream: usize| recorder.stream_name(version, stream).to_owned();
        let mut plans = Vec::new();
        let mut stream_totals =
            BTreeMap::<(u32, usize), (DurationStats, BTreeMap<String, DurationStats>)>::new();
        let mut regions = BTreeMap::<
            (u32, Vec<usize>, &'static str),
            (DurationStats, BTreeMap<String, DurationStats>),
        >::new();
        for plan in &recorder.plans {
            let version = plan.key.definition_version;
            let mut steps = Vec::new();
            for (phase, descriptors, stats) in [
                (StepPhase::Source, &plan.descriptor.source, &plan.source),
                (StepPhase::Main, &plan.descriptor.main, &plan.main),
            ] {
                for (index, (descriptor, stats)) in descriptors.iter().zip(stats).enumerate() {
                    let step_paths = paths(stats);
                    let report = match descriptor {
                        StepDescriptor::Graph { stream, islands } => {
                            let entry = stream_totals.entry((version, *stream)).or_default();
                            entry.0.merge(&stats.all);
                            merge_paths(&mut entry.1, step_paths.clone());
                            StepReport {
                                phase,
                                index,
                                kind: "graph",
                                stream: Some(name(version, *stream)),
                                members: Vec::new(),
                                islands: Some(*islands),
                                region: None,
                                total: stats.all,
                                paths: step_paths,
                            }
                        }
                        StepDescriptor::Region { region, members } => {
                            let entry = regions
                                .entry((version, members.clone(), "scalar_region"))
                                .or_default();
                            entry.0.merge(&stats.all);
                            merge_paths(&mut entry.1, step_paths.clone());
                            StepReport {
                                phase,
                                index,
                                kind: "scalar_region",
                                stream: None,
                                members: members
                                    .iter()
                                    .map(|member| name(version, *member))
                                    .collect(),
                                islands: None,
                                region: Some(*region),
                                total: stats.all,
                                paths: step_paths,
                            }
                        }
                    };
                    steps.push(report);
                }
            }
            let whole_schedule_native = (!plan.whole.all.is_empty()).then(|| {
                let entry = regions
                    .entry((
                        version,
                        plan.descriptor.streams.clone(),
                        "whole_schedule_native",
                    ))
                    .or_default();
                entry.0.merge(&plan.whole.all);
                merge_paths(&mut entry.1, paths(&plan.whole));
                StepReport {
                    phase: StepPhase::Whole,
                    index: 0,
                    kind: "whole_schedule_native",
                    stream: None,
                    members: plan
                        .descriptor
                        .streams
                        .iter()
                        .map(|member| name(version, *member))
                        .collect(),
                    islands: None,
                    region: None,
                    total: plan.whole.all,
                    paths: paths(&plan.whole),
                }
            });
            if steps.iter().all(|step| step.total.is_empty()) && whole_schedule_native.is_none() {
                // Described when first observed but never executed, for example a plan replaced at
                // the source barrier of its first tick.
                continue;
            }
            plans.push(PlanReport {
                definition_version: version,
                plan: plan.key.id,
                generation: plan.key.generation,
                quickening: plan.descriptor.quickening,
                steps,
                whole_schedule_native,
            });
        }

        let site_of = |version: u32, stream: usize| {
            recorder
                .definitions
                .iter()
                .find(|definition| definition.version == version)
                .and_then(|definition| definition.streams.get(stream))
                .and_then(|stream| stream.sites.site.clone())
        };
        let streams = stream_totals
            .iter()
            .map(|((version, stream), (total, paths))| StreamTimeReport {
                definition_version: *version,
                stream: name(*version, *stream),
                total: *total,
                paths: paths.clone(),
                site: site_of(*version, *stream),
            })
            .collect();
        let region_reports = regions
            .iter()
            .map(
                |((version, members, kind), (total, paths))| RegionTimeReport {
                    definition_version: *version,
                    kind,
                    members: members
                        .iter()
                        .map(|member| name(*version, *member))
                        .collect(),
                    total: *total,
                    paths: paths.clone(),
                },
            )
            .collect();

        let current_version = recorder.definition_version();
        // Only a snapshot of the running definition may supply memory costs: a stream of an
        // earlier definition may share a name but not a meaning.
        let current_snapshot = recorder
            .memory
            .snapshots
            .iter()
            .rev()
            .find(|snapshot| snapshot.definition_version == Some(current_version));
        let snapshot_bytes = current_snapshot.map(MemorySnapshot::bytes_by_stream);
        let mut costs = BTreeMap::<usize, Cost>::new();
        for ((version, stream), (total, _)) in &stream_totals {
            if *version == current_version {
                costs.entry(*stream).or_default().step_ns += total.total_ns;
            }
        }
        for (index, var) in inputs.program.stream_vars().iter().enumerate() {
            let cost = costs.entry(index).or_default();
            *cost = match &snapshot_bytes {
                None => Cost::without_memory(cost.step_ns),
                Some(bytes) => {
                    let found = bytes.get(var.name().as_str()).copied().unwrap_or_default();
                    Cost {
                        step_ns: cost.step_ns,
                        bytes: Some(found.charged),
                        bytes_with_shared_roots: Some(found.with_shared_roots),
                        opaque: found.opaque,
                        truncated: found.truncated,
                        map_identity_uncertain: found.map_identity_uncertain,
                    }
                }
            };
        }
        let region_costs = regions
            .iter()
            .filter(|((version, _, _), _)| *version == current_version)
            .map(|((_, members, _), (total, _))| (members.clone(), total.total_ns))
            .collect::<Vec<_>>();
        let dependencies = Some(dependencies::build(
            inputs.program,
            current_version,
            current_snapshot.map(|snapshot| (snapshot.label.clone(), snapshot.tick)),
            inputs.dynamic_slots,
            &costs,
            &region_costs,
            &recorder.config().limits,
        ));

        Self {
            definitions: recorder.definitions.clone(),
            ticks: TicksReport {
                attempted: recorder.ticks.succeeded + recorder.ticks.failed,
                succeeded: recorder.ticks.succeeded,
                failed: recorder.ticks.failed,
                duration: recorder.ticks.duration,
                commit: recorder.ticks.commit,
                committed_streams: recorder.ticks.committed_streams,
                history_commit: recorder.ticks.history_commit,
            },
            plans,
            streams,
            regions: region_reports,
            definition_sites: definition_sites(recorder.definitions.last()),
            dynamic: DynamicReport {
                by_outcome: recorder.dynamic.by_outcome.clone(),
                by_expression: recorder
                    .dynamic
                    .by_expression
                    .iter()
                    .map(|(key, stats)| ExpressionReport {
                        definition_version: key.definition_version,
                        stream: key.stream.clone(),
                        node: key.node,
                        expression: key.expression,
                        activations: *stats,
                    })
                    .collect(),
                overflow: recorder.dynamic.overflow,
                note: "Each activation covers source resolution, body lookup or compilation, \
                       environment projection and state transfer at the source barrier. \
                       Expressions are keyed by definition version, so a reused expression index \
                       after a root change is a different entry.",
            },
            collections: CollectionsReport {
                counters: recorder.collections,
                note: "Buffer replacements estimate copying: a list append or concatenation whose \
                       result does not reuse the left operand's buffer, observed by pointer. A \
                       unique buffer that reallocates to grow also shows as replaced, so the \
                       entry counts are estimates. A concatenation with an empty operand returns \
                       the other operand unchanged and is counted in list_concat_operand_reuses; \
                       otherwise the right operand's entries are appended into the left buffer \
                       (list_concat_rhs_appended_entries), transferred rather than copied as a \
                       list. list_scan_entries is counted before each scan and bounds the \
                       callbacks from above. Map updates count operations only: a persistent map \
                       copies the path to the changed entry, and cloning a map handle is O(1) \
                       and never counted.",
            },
            scheduler: SchedulerReport {
                updates: recorder.schedule_updates,
                changes: recorder.schedule_changes,
                plan_switches: recorder.plan_switches.clone(),
            },
            tiers: TierReport {
                activations: recorder.tier_activations,
                jit: inputs.jit,
            },
            events: recorder.event_counts.clone(),
            memory: MemoryReport {
                snapshots: recorder.memory.snapshots.clone(),
                max_snapshots: recorder.config().memory.max_snapshots,
                snapshots_built: recorder.memory.built,
                periodic_skipped: recorder.memory.periodic_skipped,
                periodic_replaced: recorder.memory.periodic_replaced,
                mandatory_skipped: recorder.memory.mandatory_skipped.clone(),
                mandatory_skipped_count: recorder.memory.mandatory_skipped_count,
                note: "Estimates from owners, capacities and value payloads; not allocator \
                       measurements or RSS. Each payload buffer is charged once per snapshot \
                       (unique_bytes) to the first owner reaching it; a later owner holding the \
                       same root reports only its shell as shared_bytes. \
                       bytes_with_shared_roots (total_bytes plus shared_bytes) is a partial \
                       figure, not a reachable total: the children of a shared root are not \
                       re-walked. It overlaps between owners and is reported per stream only, \
                       never aggregated. map_logical_bytes is a logical per-root estimate for \
                       persistent maps: a cloned handle is O(1) and is not charged again when it \
                       is matched, but matching covers only the most recent same-sized roots, so \
                       a clone outside that window is charged again and counted in \
                       unresolved_map_identities (charges are then not guaranteed globally \
                       unique). Interior nodes shared by distinct roots cannot be observed and \
                       may be counted under each. Function values and their mutable capture \
                       lists, and native-owned state, are opaque: counted by handle or slot, not \
                       traversed. Snapshots are scoped to a definition_version; stream names are \
                       only comparable within one.",
            },
            dropped: recorder.dropped,
            dependencies,
            timeline: TimelineSummary {
                retained: recorder.timeline.events().len(),
                dropped: recorder.timeline.dropped(),
                capacity: recorder.timeline.capacity(),
            },
            coverage: vec![
                "Step times are wall-clock elapsed time per physical step; per-stream times come \
                 only from graph steps that evaluate one stream.",
                "Fused scalar regions and whole-schedule native artifacts are reported as one \
                 aggregate with their members; no per-stream split is inferred.",
                "Inlined def bodies have no timed call boundary: definition_sites attributes \
                 nodes, not time.",
                "Nodes of runtime-compiled dynamic/defer bodies are not listed in the model map.",
                "A TypedDataflowMonitor's direct native entry bypasses the recorded monitor once \
                 promoted.",
                "Tick numbers are zero-based counts of monitor evaluations; pipeline spans of \
                 the tick being admitted carry the same index, and wait or flush spans carry the \
                 index of the next tick to be admitted.",
                "Collection counters cover only the canonical evaluator's list append, \
                 concatenation, scan and map update sites.",
                "The model map is bounded by a node budget shared across a definition's \
                 streams; node_count_is_lower_bound means the budget was reached and completeness is not established (it can also hold when the count equals the graph size). Registries \
                 beyond their limits are counted in dropped.",
            ],
        }
    }
}

/// Group inlined nodes by call site, then call sites by the definition they inlined.
///
/// Each inlined node records the definition span of its own subexpression. The widest of those
/// spans within one call site is the inlined body, which stands for the definition.
fn definition_sites(definition: Option<&DefinitionRecord>) -> Vec<DefinitionSiteReport> {
    let Some(definition) = definition else {
        return Vec::new();
    };
    type CallKey = (String, Option<(u32, u32)>);
    let mut calls = BTreeMap::<CallKey, (CallSiteReport, SourcePoint)>::new();
    for stream in &definition.streams {
        for node in &stream.sites.nodes {
            let Some(site) = &node.definition_site else {
                continue;
            };
            let key = (
                stream.name.clone(),
                node.call_site.as_ref().map(|call| (call.start, call.end)),
            );
            let (call, body) = calls.entry(key).or_insert_with(|| {
                (
                    CallSiteReport {
                        stream: stream.name.clone(),
                        call_site: node.call_site.clone(),
                        nodes: 0,
                    },
                    site.clone(),
                )
            });
            call.nodes += 1;
            let width = |point: &SourcePoint| point.end.saturating_sub(point.start);
            if site.file == body.file && width(site) > width(body) {
                *body = site.clone();
            }
        }
    }
    let mut groups = BTreeMap::<(String, u32, u32), DefinitionSiteReport>::new();
    for (call, body) in calls.into_values() {
        groups
            .entry((body.file.clone(), body.start, body.end))
            .or_insert_with(|| DefinitionSiteReport {
                definition: body,
                call_sites: Vec::new(),
            })
            .call_sites
            .push(call);
    }
    groups.into_values().collect()
}
