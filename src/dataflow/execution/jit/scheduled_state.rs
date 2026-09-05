//! Scheduled scalar temporal operations over evaluator-owned canonical state.
//!
//! Evaluation steps read the previous temporal state and materialize typed scalar boundary values
//! for a native fragment. Commit steps remain explicit in the scheduler's semantic plan and run only
//! at its logical end-of-tick barrier. This interpreter is the fallback physical view of those
//! steps; a schedule-wide native artifact may use a packed state layout mapped to the same stable
//! plan slots.

use crate::dataflow::execution::evaluator_state::{EvaluatorState, NodeState};
use crate::dataflow::execution::scheduled_plan::{TemporalOperation, TemporalPlan};
use crate::dataflow::history::HistoryAccess;
use crate::dataflow::ir::{BoundRef, NodeId};

#[derive(Clone)]
pub(super) struct ScheduledTemporalPlan {
    plan: TemporalPlan,
}

impl ScheduledTemporalPlan {
    pub(super) fn build(plan: &TemporalPlan, boundary_nodes: &[NodeId]) -> Option<Self> {
        if boundary_nodes.is_empty() {
            return plan.operations.is_empty().then(Self::empty);
        }
        boundary_nodes
            .iter()
            .copied()
            .eq(plan.nodes())
            .then(|| Self { plan: plan.clone() })
    }

    fn empty() -> Self {
        Self {
            plan: TemporalPlan::default(),
        }
    }

    pub(super) fn promote(
        &self,
        state: &mut EvaluatorState,
        history_access: Option<HistoryAccess<'_>>,
    ) -> bool {
        // Promotion is the representation half of activation and the quick tier performs the same
        // one; hydration below is the storage half, which only a native kernel needs. Promoting
        // first leaves hydration a single typed implementation instead of one per representation.
        for step in self.plan.operations.iter() {
            if !state.node_states[step.node().index()].promote_scalar_temporal() {
                self.deopt(state);
                return false;
            }
        }
        let Some(history_access) = history_access else {
            return true;
        };
        for step in self.plan.operations.iter() {
            let TemporalOperation::Delay {
                state: slot,
                input: BoundRef::External(input),
                offset,
            } = step
            else {
                continue;
            };
            let NodeState::ScalarDelay(delay) = &mut state.node_states[slot.node.index()] else {
                unreachable!("a promoted delay is a scalar delay");
            };
            let hydrated = usize::try_from(*offset).is_ok_and(|depth| {
                delay.hydrate_shared_history(depth, history_access.recent_values(*input, depth))
            });
            if !hydrated {
                self.deopt(state);
                return false;
            }
        }
        true
    }

    pub(super) fn deopt(&self, state: &mut EvaluatorState) {
        for step in self.plan.operations.iter() {
            state.node_states[step.node().index()].demote_scalar_temporal();
        }
    }
}
