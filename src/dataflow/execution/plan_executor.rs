use super::super::plan::*;
use super::super::*;
use super::interpreter::*;
use super::state::*;
use std::cell::RefCell;

#[derive(Clone, Copy)]
pub(in crate::dataflow) struct PlanEvalContext<'a> {
    pub(in crate::dataflow) inputs: &'a [Value],
    pub(in crate::dataflow) environment: &'a Rc<EnvironmentLayout>,
    pub(in crate::dataflow) recursive_call: Option<&'a dyn Fn(EcoVec<Value>) -> Value>,
    pub(in crate::dataflow) dynamic_dependencies: Option<&'a RefCell<Vec<EnvironmentId>>>,
}

impl PlanEvalContext<'_> {
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
        self.plan.evaluation == PlanMode::Dynamic
    }

    pub(in crate::dataflow) fn evaluate(
        &mut self,
        inputs: &[Value],
        recursive_call: Option<&dyn Fn(EcoVec<Value>) -> Value>,
    ) -> Result<Value, DataflowEvalError> {
        let value = self.evaluate_deferred(inputs, recursive_call, None)?;
        self.commit_delays(inputs);
        Ok(value)
    }

    pub(in crate::dataflow) fn evaluate_observing_deferred(
        &mut self,
        inputs: &[Value],
        dynamic_dependencies: Option<&RefCell<Vec<EnvironmentId>>>,
    ) -> Result<Value, DataflowEvalError> {
        self.evaluate_deferred(inputs, None, dynamic_dependencies)
    }

    pub(in crate::dataflow) fn commit_delays(&mut self, inputs: &[Value]) {
        let context = PlanEvalContext {
            inputs,
            environment: &self.plan.environment,
            recursive_call: None,
            dynamic_dependencies: None,
        };
        commit_staged_delays(&self.plan.body, &mut self.state, context);
    }

    fn evaluate_deferred(
        &mut self,
        inputs: &[Value],
        recursive_call: Option<&dyn Fn(EcoVec<Value>) -> Value>,
        dynamic_dependencies: Option<&RefCell<Vec<EnvironmentId>>>,
    ) -> Result<Value, DataflowEvalError> {
        let body = &self.plan.body;
        debug_assert_eq!(self.state.nodes.len(), body.nodes.len());
        debug_assert_eq!(self.state.states.len(), body.nodes.len());
        let evaluation = self.plan.evaluation;
        let context = PlanEvalContext {
            inputs,
            environment: &self.plan.environment,
            recursive_call,
            dynamic_dependencies,
        };

        match evaluation {
            PlanMode::Static => eval_nodes_at(&body.nodes, &mut self.state, 0, context),
            PlanMode::Dynamic => try_eval_nodes_at(&body.nodes, &mut self.state, 0, context)?,
        }
        let value = context.read(&self.state, &body.output, 0);
        commit_recursive_delays(&body.recursive_delays, &mut self.state, &value);
        Ok(value)
    }
}
