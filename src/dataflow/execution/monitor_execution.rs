//! Monitor-level ownership and replaceable, cacheable scheduled plans.
//!
//! A fixed arena owns one persistent evaluator per logical stream, holding only canonical state.
//! An [`ExecutionPlan`] owns the semantic schedule and partitions its physical order into scalar
//! regions and graph steps, giving each graph step one region whose members are its scalar islands,
//! interleaved with canonical node runs. Quickening and native code are alternative executors for the same regions. Region
//! outputs are published explicitly across source, graph, and region boundaries. Tier and schedule
//! transitions materialize the authoritative state before replacing its executor.

mod plan;
mod reconfiguration;
#[cfg(test)]
mod tests;
mod tick;
mod tiers;

use super::super::history::HistoryId;
use super::super::ir::{NodeId, StreamProgram};
use super::super::stream_id::{StreamId, StreamSlots};
use super::evaluator::Evaluator;
use super::jit::Jit;
use super::quickening::{QuickenedRegionPlan, QuickenedRegionState, ScalarValue};
use super::reconfigurable_expressions::SharedReconfigurableExpressionCache;
use super::scheduled_plan::{PlanId, ScheduledExecutionPlan};
use plan::{ExecutionPlan, PlanIdentity};
use std::rc::Rc;

const EXECUTION_LAYOUT_CACHE_SIZE: usize = 4;

#[derive(Clone)]
/// One persistent evaluator per logical stream, keyed by stable stream identity.
///
/// `published_scalars` is the explicit scalar boundary between regions: a later region reads an
/// earlier region's result from here rather than round-tripping through a `Value` environment slot.
/// `region_states` is present only while a plan owns regions, and is rebuilt when the plan changes.
struct EvaluatorArena {
    evaluators: Box<[Evaluator]>,
    published_scalars: Box<[Option<ScalarValue>]>,
    region_states: Option<RegionStates>,
}

#[derive(Clone)]
struct RegionStates {
    identity: PlanIdentity,
    states: Box<[QuickenedRegionState]>,
    authority: Box<[RegionAuthority]>,
}

/// Which executor currently owns the semantic state of one region.
#[derive(Clone, Copy)]
/// Which executor currently owns the semantic state of one region.
///
/// Exactly one at a time. Entering an executor synchronizes state into it; leaving materializes
/// state back out. Never let two believe they own the same nodes.
enum RegionAuthority {
    Canonical,
    Quickened,
    Native,
}

impl RegionStates {
    /// Takes quickened authority for one region, refreshing its state from the canonical arena
    /// whenever another tier owned it last.
    #[inline]
    /// Takes a region over from whichever tier last owned it.
    ///
    /// `false` leaves the canonical arena authoritative: synchronization declined because a
    /// temporal node the region covers holds state outside the scalar domain.
    #[must_use]
    fn enter_quickened(
        &mut self,
        region: usize,
        plan: &QuickenedRegionPlan,
        evaluators: &mut [Evaluator],
    ) -> bool {
        if matches!(self.authority[region], RegionAuthority::Quickened) {
            return true;
        }
        if !plan.synchronize(&mut self.states[region], evaluators) {
            return false;
        }
        self.authority[region] = RegionAuthority::Quickened;
        true
    }
}

#[derive(Clone, Copy)]
struct ExpressionLocation {
    stream: StreamId,
    node: NodeId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Which tier last mutated semantic state for the monitor as a whole.
///
/// Read by `materialize_authoritative_state` to decide what must be written back to the canonical
/// arena before a route changes.
enum AuthoritativeTier {
    Canonical,
    Regions(PlanIdentity),
    WholeNative,
}

pub(in crate::dataflow) struct MonitorExecution {
    evaluators: EvaluatorArena,
    stream_slots: StreamSlots,
    temporal_streams: Box<[StreamId]>,
    expression_locations: Box<[ExpressionLocation]>,
    engine: ExecutionEngine,
    authoritative_tier: AuthoritativeTier,
    tick_in_progress: bool,
    shared_reconfigurable_expression_cache: SharedReconfigurableExpressionCache,
}

/// The single tier-selection and plan-cache boundary for a monitor.
struct ExecutionEngine {
    active_plan: ExecutionPlan,
    cached_plans: Vec<ExecutionPlan>,
    next_plan_id: u64,
    next_plan_generation: u64,
    quickening: bool,
    jit: Jit,
}

impl MonitorExecution {
    #[cfg(test)]
    pub(in crate::dataflow) fn new_with_source_prelude(
        programs: Vec<Rc<StreamProgram>>,
        stream_slots: StreamSlots,
        source_order: &[StreamId],
        main_order: &[StreamId],
        temporal_streams: &[StreamId],
    ) -> Self {
        Self::new_with_source_prelude_and_history(
            programs,
            stream_slots,
            source_order,
            main_order,
            temporal_streams,
            &[],
        )
    }

    pub(in crate::dataflow) fn new_with_source_prelude_and_history(
        programs: Vec<Rc<StreamProgram>>,
        stream_slots: StreamSlots,
        source_order: &[StreamId],
        main_order: &[StreamId],
        temporal_streams: &[StreamId],
        history_bindings: &[Option<HistoryId>],
    ) -> Self {
        let expression_locations = programs
            .iter()
            .enumerate()
            .flat_map(|(stream, program)| {
                program
                    .reconfigurable_expressions()
                    .map(move |(node, _, _)| ExpressionLocation {
                        stream: StreamId::new(stream),
                        node,
                    })
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let semantic = ScheduledExecutionPlan::new(
            PlanId(0),
            &programs,
            stream_slots,
            source_order,
            main_order,
            temporal_streams,
        );
        let active_plan = ExecutionPlan::new(semantic, true, 0);
        let active_plan_identity = active_plan.identity;
        let authoritative_tier = (!active_plan.regions.is_empty())
            .then_some(AuthoritativeTier::Regions(active_plan_identity))
            .unwrap_or(AuthoritativeTier::Canonical);
        let mut evaluators = EvaluatorArena::new_with_history(programs, history_bindings);
        evaluators.configure_regions(active_plan_identity, &active_plan.quickened_regions, true);
        Self {
            evaluators,
            stream_slots,
            temporal_streams: temporal_streams.to_vec().into_boxed_slice(),
            expression_locations,
            engine: ExecutionEngine {
                active_plan,
                cached_plans: Vec::new(),
                next_plan_id: 1,
                next_plan_generation: 1,
                quickening: true,
                jit: Jit::disabled(),
            },
            authoritative_tier,
            tick_in_progress: false,
            shared_reconfigurable_expression_cache: SharedReconfigurableExpressionCache::default(),
        }
    }
}
