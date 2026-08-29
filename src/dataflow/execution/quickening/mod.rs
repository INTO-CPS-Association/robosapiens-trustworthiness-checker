mod adaptive;
mod interpreter;
mod plan;
mod scalar;
mod state;

pub(in crate::dataflow) use adaptive::{is_adaptive_candidate, plan_from_observed_single};
pub(in crate::dataflow) use interpreter::{
    DirectScalarResult, execute_direct_scalar, execute_plan,
};
pub(in crate::dataflow) use plan::{Plan, SingleScalarPlan};
pub(in crate::dataflow) use scalar::ScalarValue;
pub(in crate::dataflow) use state::State;
#[cfg(test)]
pub(in crate::dataflow) use state::node_value;
