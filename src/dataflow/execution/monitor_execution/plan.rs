//! The physical partition of one scheduled plan.
//!
//! [`ExecutionPlan`] takes a semantically ordered [`ScheduledExecutionPlan`] and decides *how* to
//! run it: which contiguous runs of streams become scalar regions, and which streams stay canonical
//! graph steps with scalar islands interleaved between canonical node runs.
//!
//! A plan is a route, never state. [`PlanIdentity`] names the route so a cached plan can be
//! reselected, but evicting or replacing a plan cannot reset a delay ring, a function frame, or a
//! nested evaluator — those hang off stable stream identities in the arena instead.

use std::ops::Range;
use std::rc::Rc;

use super::super::super::environment::EnvironmentSlot;
use super::super::super::stream_id::{StreamId, StreamSlots};
use super::super::quickening::{QuickenedRegionPlan, supports_program};
use super::super::scalar_region::{GraphSegment, ScalarRegion, segment_stream_graph};
use super::super::scheduled_plan::{PlanId, PlannedStream, ScheduledExecutionPlan};
use super::{EXECUTION_LAYOUT_CACHE_SIZE, MonitorExecution};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct PlanIdentity {
    pub(super) id: PlanId,
    pub(super) generation: u64,
}

/// The physical partition of one scheduled plan.
///
/// `regions` and `quickened_regions` are index-aligned: every region the plan owns has exactly one
/// executable quickened plan, and the native tier compiles the subset it supports at the same
/// indexes. Steps reference regions by that shared index.
pub(super) struct ExecutionPlan {
    pub(super) identity: PlanIdentity,
    pub(super) semantic: Box<ScheduledExecutionPlan>,
    pub(super) regions: Box<[ScalarRegion]>,
    pub(super) quickened_regions: Box<[QuickenedRegionPlan]>,
    pub(super) source_steps: Box<[ExecutionStep]>,
    pub(super) main_steps: Box<[ExecutionStep]>,
}

/// One unit of the physical order: an accelerable region, or a canonical graph.
pub(super) enum ExecutionStep {
    ScalarRegion(usize),
    Graph(GraphStep),
}

/// One stream evaluated through its canonical graph.
///
/// `segments` is empty when the whole graph runs canonically; otherwise `region` names the one
/// scalar region holding this graph's islands, and `segments` alternates its members with canonical
/// node runs in graph order.
pub(super) struct GraphStep {
    pub(super) stream: StreamId,
    pub(super) slot: EnvironmentSlot,
    pub(super) region: Option<usize>,
    pub(super) segments: Box<[ExecutableSegment]>,
}

/// A graph step's execution order: island members interleaved with canonical node runs.
///
/// This is the physical counterpart to [`super::super::scalar_region::GraphSegment`]. The extra
/// `nodes` range is denormalised from the region's `GraphIsland` so the decline path can fall back
/// to canonical evaluation without indirecting through the region arena.
pub(super) enum ExecutableSegment {
    /// One member of the graph step's region. `nodes` is the canonical node run it replaces, used
    /// when the row's types reject the member and it falls back to canonical evaluation.
    Island {
        member: usize,
        nodes: Range<usize>,
    },
    Canonical(Range<usize>),
}

struct RegionArena<'a> {
    regions: &'a mut Vec<ScalarRegion>,
    quickened: &'a mut Vec<QuickenedRegionPlan>,
}

impl RegionArena<'_> {
    fn push(&mut self, semantic: &ScheduledExecutionPlan, region: ScalarRegion) -> Option<usize> {
        let quickened = QuickenedRegionPlan::from_region(semantic, &region)?;
        Some(self.install(region, quickened))
    }

    fn install(&mut self, region: ScalarRegion, quickened: QuickenedRegionPlan) -> usize {
        let index = self.regions.len();
        self.regions.push(region);
        self.quickened.push(quickened);
        index
    }
}

impl ExecutionPlan {
    pub(super) fn new(semantic: ScheduledExecutionPlan, quickening: bool, generation: u64) -> Self {
        let mut regions = Vec::new();
        let mut quickened_regions = Vec::new();
        let source_steps = {
            let mut arena = RegionArena {
                regions: &mut regions,
                quickened: &mut quickened_regions,
            };
            Self::build_steps(&semantic, semantic.source_streams(), &mut arena, quickening)
        };
        let main_steps = {
            let mut arena = RegionArena {
                regions: &mut regions,
                quickened: &mut quickened_regions,
            };
            Self::build_steps(&semantic, semantic.main_streams(), &mut arena, quickening)
        };

        let identity = PlanIdentity {
            id: semantic.id,
            generation,
        };
        Self {
            identity,
            semantic: Box::new(semantic),
            regions: regions.into_boxed_slice(),
            quickened_regions: quickened_regions.into_boxed_slice(),
            source_steps,
            main_steps,
        }
    }

    fn build_steps(
        semantic: &ScheduledExecutionPlan,
        planned_streams: &[PlannedStream],
        arena: &mut RegionArena<'_>,
        quickening: bool,
    ) -> Box<[ExecutionStep]> {
        let mut steps = Vec::with_capacity(planned_streams.len());
        let mut index = 0;

        while index < planned_streams.len() {
            if let Some((end, region, quickened)) =
                Self::grow_stream_region(semantic, planned_streams, index)
            {
                steps.push(ExecutionStep::ScalarRegion(
                    arena.install(region, quickened),
                ));
                index = end;
                continue;
            }

            let planned = &planned_streams[index];
            let (region, segments) = quickening
                .then(|| Self::build_graph_region(semantic, planned, arena))
                .unwrap_or_default();
            steps.push(ExecutionStep::Graph(GraphStep {
                stream: planned.stream,
                slot: planned.output.environment(),
                region,
                segments,
            }));
            index += 1;
        }
        steps.into_boxed_slice()
    }

    /// Finds the longest run of whole streams starting at `index` that forms one scalar region.
    fn grow_stream_region(
        semantic: &ScheduledExecutionPlan,
        planned_streams: &[PlannedStream],
        index: usize,
    ) -> Option<(usize, ScalarRegion, QuickenedRegionPlan)> {
        let (len, region) = ScalarRegion::from_stream_prefix(
            semantic,
            &planned_streams[index..],
            supports_program,
        )?;
        let quickened = QuickenedRegionPlan::from_region(semantic, &region)?;
        Some((index + len, region, quickened))
    }

    /// Splits one canonical stream graph into one scalar region of islands and canonical node runs.
    ///
    /// The whole graph stays canonical unless the region is executable, so a graph step never holds
    /// a region index without executable region state behind it.
    fn build_graph_region(
        semantic: &ScheduledExecutionPlan,
        planned: &PlannedStream,
        arena: &mut RegionArena<'_>,
    ) -> (Option<usize>, Box<[ExecutableSegment]>) {
        let Some((graph, semantic_segments)) =
            segment_stream_graph(planned.stream, planned.program.as_ref(), supports_program)
        else {
            return (None, Box::default());
        };
        let Some(region) = arena.push(semantic, ScalarRegion::Graph(graph.clone())) else {
            return (None, Box::default());
        };
        let segments = semantic_segments
            .iter()
            .map(|segment| match segment {
                GraphSegment::Canonical(nodes) => ExecutableSegment::Canonical(nodes.clone()),
                GraphSegment::Island(member) => ExecutableSegment::Island {
                    member: *member,
                    nodes: graph.islands[*member].nodes.clone(),
                },
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        (Some(region), segments)
    }
}

impl MonitorExecution {
    pub(in crate::dataflow) fn select_schedule_ranges(
        &mut self,
        source_order: &[StreamId],
        main_order: &[StreamId],
        stream_slots: StreamSlots,
    ) {
        let matches = |plan: &ExecutionPlan| {
            plan.semantic.source_stream_count == source_order.len()
                && plan
                    .semantic
                    .source_order()
                    .eq(source_order.iter().copied())
                && plan.semantic.main_order().eq(main_order.iter().copied())
        };
        if matches(&self.engine.active_plan) {
            return;
        }
        self.materialize_authoritative_state();
        if let Some(cached) = self.engine.cached_plans.iter().position(matches) {
            std::mem::swap(
                &mut self.engine.active_plan,
                &mut self.engine.cached_plans[cached],
            );
        } else {
            let semantic = ScheduledExecutionPlan::from_metadata(
                PlanId(self.engine.next_plan_id),
                Rc::clone(&self.engine.active_plan.semantic.metadata),
                stream_slots,
                source_order,
                main_order,
                &self.temporal_streams,
            );
            self.engine.next_plan_id += 1;
            let generation = self.engine.next_plan_generation;
            self.engine.next_plan_generation = self
                .engine
                .next_plan_generation
                .checked_add(1)
                .expect("execution plan generation overflow");
            let new_plan = ExecutionPlan::new(semantic, self.engine.quickening, generation);
            let previous = std::mem::replace(&mut self.engine.active_plan, new_plan);
            if self.engine.cached_plans.len() == EXECUTION_LAYOUT_CACHE_SIZE {
                self.engine.cached_plans.remove(0);
            }
            self.engine.cached_plans.push(previous);
        }
        self.engine.jit.schedule_changed(
            &self.engine.active_plan.semantic,
            &self.engine.active_plan.regions,
        );
        self.configure_active_region_states();
    }
}
