use super::super::plan::*;
use super::super::*;
use super::interpreter::*;
use super::state::*;
use std::cell::RefCell;

#[derive(Clone, Copy)]
pub(in crate::dataflow) struct EvalContext<'a> {
    pub(in crate::dataflow) inputs: &'a [Value],
    pub(in crate::dataflow) environment: &'a Rc<EnvironmentLayout>,
    pub(in crate::dataflow) recursive_call: Option<&'a dyn Fn(EcoVec<Value>) -> Value>,
    pub(in crate::dataflow) dynamic_dependencies: Option<&'a RefCell<BTreeSet<VarName>>>,
}

impl EvalContext<'_> {
    pub(in crate::dataflow) fn read(
        self,
        state: &DataflowState,
        operand: &BoundRef,
        _tick: usize,
    ) -> Value {
        match operand {
            BoundRef::Const(value) => value.clone(),
            BoundRef::External(id) => self.inputs[id.index()].clone(),
            BoundRef::Node(id) => state
                .nodes
                .get(id.index())
                .unwrap_or_else(|| panic!("dataflow node {:?} was not evaluated", id))
                .clone(),
        }
    }
}

/// Owns one executable plan and its persistent state.
#[derive(Clone)]
pub(in crate::dataflow) struct PlanExecutor {
    plan: Rc<ExecutablePlan>,
    state: DataflowState,
}

impl PlanExecutor {
    pub(in crate::dataflow) fn new(plan: Rc<ExecutablePlan>) -> Self {
        plan.body.debug_assert_valid(plan.environment.len());
        let state = DataflowState::new(&plan.body);
        debug_assert_eq!(state.nodes.len(), plan.body.nodes.len());
        debug_assert_eq!(state.states.len(), plan.body.nodes.len());
        Self { plan, state }
    }

    pub(in crate::dataflow) fn reset_state(&mut self) {
        self.state.reset_for_reuse();
    }

    pub(in crate::dataflow) fn may_reconfigure_dependencies(&self) -> bool {
        self.plan.evaluation == EvaluationKind::Fallible
    }

    pub(in crate::dataflow) fn evaluate(
        &mut self,
        inputs: &[Value],
        recursive_call: Option<&dyn Fn(EcoVec<Value>) -> Value>,
    ) -> Result<Value, DataflowEvalError> {
        self.evaluate_observing(inputs, recursive_call, None)
    }

    pub(in crate::dataflow) fn evaluate_observing(
        &mut self,
        inputs: &[Value],
        recursive_call: Option<&dyn Fn(EcoVec<Value>) -> Value>,
        dynamic_dependencies: Option<&RefCell<BTreeSet<VarName>>>,
    ) -> Result<Value, DataflowEvalError> {
        let body = &self.plan.body;
        debug_assert_eq!(self.state.nodes.len(), body.nodes.len());
        debug_assert_eq!(self.state.states.len(), body.nodes.len());
        let evaluation = self.plan.evaluation;
        let context = EvalContext {
            inputs,
            environment: &self.plan.environment,
            recursive_call,
            dynamic_dependencies,
        };

        match evaluation {
            EvaluationKind::Static => eval_nodes_at(&body.nodes, &mut self.state, 0, context),
            EvaluationKind::Fallible => {
                try_eval_nodes_at(&body.nodes, &mut self.state, 0, context)?
            }
        }
        let value = context.read(&self.state, &body.output, 0);
        commit_recursive_delays(&body.recursive_delays, &mut self.state, &value);
        Ok(value)
    }
}
