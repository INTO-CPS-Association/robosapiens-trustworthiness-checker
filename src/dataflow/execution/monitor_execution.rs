//! Monitor-level ownership and replaceable, cacheable scheduled plans.
//!
//! A fixed arena owns one persistent evaluator per logical stream. An execution
//! plan only chooses an evaluation order and replaces eligible environment
//! reads with compact values published by earlier planned streams. Layout rebuilds
//! therefore reorder stream IDs without moving temporal, function,
//! dynamic-expression, or deoptimization state.
//!
//! Logical results are published after every planned stream. Canonical instructions and
//! nested evaluators consequently observe the canonical environment and do not
//! form fusion barriers.
//!
//! Planning is backend independent. A [`PlanBundle`] owns the semantic schedule and a
//! schedule-wide quickened artifact. The optional native tier is coordinated by
//! [`ExecutionEngine`], but it consumes the same ordered programs and never changes the plan.
//! Canonical state remains in the evaluator arena. A native tier may temporarily use a packed
//! physical layout, but that layout maps to stable plan state slots and is materialized before
//! canonical replay.

mod plan;
mod reconfiguration;
#[cfg(test)]
mod tests;
mod tick;
mod tiers;

use super::super::execution_plan::{StreamId, StreamSlots};
use super::super::history::HistoryId;
use super::super::ir::{NodeId, StreamProgram};
use super::dynamic_expressions::SharedDynamicExpressionCache;
use super::evaluator::Evaluator;
use super::jit::Jit;
use super::quickening::ScalarValue;
use super::scheduled_plan::{PlanId, ScheduledExecutionPlan};
use plan::PlanBundle;
use std::rc::Rc;

const EXECUTION_LAYOUT_CACHE_SIZE: usize = 4;

#[derive(Clone)]
struct EvaluatorArena {
    evaluators: Box<[Evaluator]>,
    published_scalars: Box<[Option<ScalarValue>]>,
}

#[derive(Clone, Copy)]
struct ExpressionLocation {
    stream: StreamId,
    node: NodeId,
}

pub(in crate::dataflow) struct MonitorExecution {
    evaluators: EvaluatorArena,
    stream_slots: StreamSlots,
    temporal_streams: Box<[StreamId]>,
    expression_locations: Box<[ExpressionLocation]>,
    engine: ExecutionEngine,
    tick_in_progress: bool,
    shared_dynamic_expression_cache: SharedDynamicExpressionCache,
}

/// The single tier-selection and plan-cache boundary for a monitor.
struct ExecutionEngine {
    active_plan: PlanBundle,
    cached_plans: Vec<PlanBundle>,
    next_plan_id: u64,
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
        let active_plan = PlanBundle::new(semantic, true);
        let mut evaluators = EvaluatorArena::new_with_history(programs, history_bindings);
        evaluators.detach_top_level_quick_plans();
        Self {
            evaluators,
            stream_slots,
            temporal_streams: temporal_streams.to_vec().into_boxed_slice(),
            expression_locations,
            engine: ExecutionEngine {
                active_plan,
                cached_plans: Vec::new(),
                next_plan_id: 1,
                quickening: true,
                jit: Jit::disabled(),
            },
            tick_in_progress: false,
            shared_dynamic_expression_cache: SharedDynamicExpressionCache::default(),
        }
    }
}
