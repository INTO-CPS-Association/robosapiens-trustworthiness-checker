use super::super::ir::*;
use super::super::*;
use super::dynamic_expressions::update_active_expression;
use super::interpreter::*;
use super::specialization::{self, ScalarValue};
use super::stream_state::*;

#[derive(Clone, Copy)]
pub(in crate::dataflow) struct EvaluationContext<'a> {
    pub(in crate::dataflow) environment_values: &'a [Value],
    pub(in crate::dataflow) environment_layout: &'a Rc<EnvironmentLayout>,
    pub(in crate::dataflow) retained_environment_values: Option<&'a [Value]>,
    pub(in crate::dataflow) recursive_call: Option<&'a dyn Fn(EcoVec<Value>) -> Value>,
}

impl EvaluationContext<'_> {
    pub(in crate::dataflow) fn read_value(self, state: &StreamState, operand: &BoundRef) -> Value {
        match operand {
            BoundRef::Const(value) => value.clone(),
            BoundRef::External(slot) => self.environment_values[slot.index()].clone(),
            BoundRef::Node(node) => state
                .node_values
                .get(node.index())
                .unwrap_or_else(|| panic!("dataflow node {:?} was not evaluated", node))
                .clone(),
        }
    }
}

/// Owns one stream program and its persistent evaluation state.
#[derive(Clone)]
pub(in crate::dataflow) struct StreamEvaluator {
    pub(in crate::dataflow) program: Rc<StreamProgram>,
    pub(in crate::dataflow) state: StreamState,
    specialization_state: Option<specialization::State>,
}

impl StreamEvaluator {
    pub(in crate::dataflow) fn new(program: Rc<StreamProgram>) -> Self {
        program
            .graph
            .debug_assert_valid(program.environment_layout.len());
        let state = StreamState::new(&program.graph);
        let specialization_state = program
            .specialization_plan
            .as_deref()
            .map(specialization::State::new);
        debug_assert_eq!(state.node_values.len(), program.graph.nodes.len());
        debug_assert_eq!(state.node_states.len(), program.graph.nodes.len());
        Self {
            program,
            state,
            specialization_state,
        }
    }

    pub(in crate::dataflow) fn reset(&mut self) {
        self.state.reset();
        if let Some(state) = &mut self.specialization_state {
            state.reset();
        }
    }

    #[inline]
    pub(in crate::dataflow) fn resolve_reconfiguration_point(
        &mut self,
        node: NodeId,
        source_value: Value,
    ) -> Result<&[EnvironmentSlot], DataflowEvaluationError> {
        let (program, state) = (&self.program, &mut self.state);
        let StreamOp::Dynamic(spec) = &program.graph.nodes[node.index()] else {
            unreachable!("reconfiguration point referenced a non-dynamic node")
        };
        let NodeState::Dynamic(dynamic) = &mut state.node_states[node.index()] else {
            unreachable!("reconfiguration point referenced incompatible runtime state")
        };
        let source_value = match source_value {
            Value::NoVal => dynamic.last_source_value.clone().unwrap_or(Value::NoVal),
            value => value,
        };
        match source_value {
            Value::Str(source_text) => {
                update_active_expression(source_text, spec, dynamic, &program.environment_layout)?
            }
            Value::Deferred | Value::NoVal => {}
            other => {
                return Err(DataflowEvaluationError::InvalidExpressionSource(
                    other.to_string(),
                ));
            }
        }
        Ok(dynamic
            .active_expression
            .as_ref()
            .map_or(&[], |active| active.dependency_slots.as_slice()))
    }

    pub(in crate::dataflow) fn evaluate_and_commit(
        &mut self,
        environment_values: &[Value],
        recursive_call: Option<&dyn Fn(EcoVec<Value>) -> Value>,
    ) -> Result<Value, DataflowEvaluationError> {
        let value =
            self.evaluate_and_stage_with_context(environment_values, None, recursive_call)?;
        self.commit_temporal_state(environment_values);
        Ok(value)
    }

    pub(in crate::dataflow) fn evaluate_and_stage(
        &mut self,
        environment_values: &[Value],
    ) -> Result<Value, DataflowEvaluationError> {
        self.evaluate_and_stage_with_context(environment_values, None, None)
    }

    pub(in crate::dataflow) fn evaluate_and_stage_with_retained_environment(
        &mut self,
        environment_values: &[Value],
        retained_environment_values: &[Value],
    ) -> Result<Value, DataflowEvaluationError> {
        self.evaluate_and_stage_with_context(
            environment_values,
            Some(retained_environment_values),
            None,
        )
    }

    #[inline]
    pub(in crate::dataflow) fn evaluate_infallible_and_stage_with_plan(
        &mut self,
        environment_values: &[Value],
        specialization_plan_override: Option<&specialization::Plan>,
        published_scalars: &[Option<ScalarValue>],
    ) -> Value {
        debug_assert!(self.program.is_infallible());
        let body = &self.program.graph;
        debug_assert_eq!(self.state.node_values.len(), body.nodes.len());
        debug_assert_eq!(self.state.node_states.len(), body.nodes.len());
        let context = EvaluationContext {
            environment_values,
            environment_layout: &self.program.environment_layout,
            retained_environment_values: None,
            recursive_call: None,
        };

        let specialization_plan =
            specialization_plan_override.or(self.program.specialization_plan.as_deref());
        if let (Some(plan), Some(state)) = (specialization_plan, &mut self.specialization_state) {
            specialization::execute(
                state,
                plan,
                body,
                &mut self.state,
                context,
                published_scalars,
            );
        } else {
            debug_assert!(specialization_plan.is_none() && self.specialization_state.is_none());
            evaluate_nodes(&body.nodes, &mut self.state, context);
        }
        let value = context.read_value(&self.state, &body.output);
        stage_recursive_delays(&body.recursive_delays, &mut self.state, &value);
        value
    }

    #[inline]
    pub(in crate::dataflow) fn evaluate_single_scalar_with_plan(
        &mut self,
        environment_values: &[Value],
        plan: &specialization::SingleScalarPlan,
        published_scalars: &[Option<ScalarValue>],
    ) -> specialization::DirectResult {
        let body = &self.program.graph;
        debug_assert_eq!(body.nodes.len(), 1);
        debug_assert_eq!(body.output, BoundRef::Node(NodeId::new(0)));
        debug_assert!(body.recursive_delays.is_empty());
        let context = EvaluationContext {
            environment_values,
            environment_layout: &self.program.environment_layout,
            retained_environment_values: None,
            recursive_call: None,
        };
        specialization::execute_single(
            self.specialization_state
                .as_mut()
                .expect("single scalar plan requires specialization state"),
            plan,
            body,
            &mut self.state,
            context,
            published_scalars,
        )
    }

    pub(in crate::dataflow) fn commit_temporal_state(&mut self, environment_values: &[Value]) {
        self.commit_temporal_state_with_retained_environment(environment_values, None);
    }

    pub(in crate::dataflow) fn commit_temporal_state_with_retained_environment(
        &mut self,
        environment_values: &[Value],
        retained_environment_values: Option<&[Value]>,
    ) {
        let context = EvaluationContext {
            environment_values,
            environment_layout: &self.program.environment_layout,
            retained_environment_values,
            recursive_call: None,
        };
        commit_staged_temporal_state(&self.program.graph, &mut self.state, context);
    }

    fn evaluate_and_stage_with_context(
        &mut self,
        environment_values: &[Value],
        retained_environment_values: Option<&[Value]>,
        recursive_call: Option<&dyn Fn(EcoVec<Value>) -> Value>,
    ) -> Result<Value, DataflowEvaluationError> {
        let body = &self.program.graph;
        debug_assert_eq!(self.state.node_values.len(), body.nodes.len());
        debug_assert_eq!(self.state.node_states.len(), body.nodes.len());
        let context = EvaluationContext {
            environment_values,
            environment_layout: &self.program.environment_layout,
            retained_environment_values,
            recursive_call,
        };

        if self.program.is_infallible() {
            if let Some(state) = &mut self.specialization_state {
                specialization::execute(
                    state,
                    self.program
                        .specialization_plan
                        .as_deref()
                        .expect("specialization state requires a plan"),
                    body,
                    &mut self.state,
                    context,
                    &[],
                );
            } else {
                evaluate_nodes(&body.nodes, &mut self.state, context);
            }
        } else {
            try_evaluate_nodes(&body.nodes, &mut self.state, context)?;
        }
        let value = context.read_value(&self.state, &body.output);
        stage_recursive_delays(&body.recursive_delays, &mut self.state, &value);
        Ok(value)
    }
}
