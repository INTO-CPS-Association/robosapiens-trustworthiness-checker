use super::super::ir::*;
use super::super::*;
use super::dynamic_expressions::{
    DynamicExpressionActivation, prepare_active_expression_with_change,
};
use super::interpreter::*;
use super::quickening::{self, ScalarValue};
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

struct PreparedRegionBody {
    active_expression: ActiveExpression,
    environment_values: Vec<Value>,
    last_source_value: Option<Value>,
}

/// The outcome of compiling and transferring one nested replacement body.  The body remains local
/// until the monitor has accepted the transfer; installation is a separate move performed at the
/// source barrier.
pub(in crate::dataflow) struct RegionReplacement {
    pub(in crate::dataflow) activation: DynamicExpressionActivation,
    /// `false` only when a changed body could not continue the previous body's state. Under
    /// `Compatible` the replacement is still installed cold; under `Strict` the caller poisons.
    pub(in crate::dataflow) state_transferred: bool,
    replacement: Option<PreparedRegionBody>,
}

impl RegionReplacement {
    fn unchanged() -> Self {
        Self {
            activation: DynamicExpressionActivation::Unchanged,
            state_transferred: true,
            replacement: None,
        }
    }
}

struct DynamicBodyDonor {
    active_expression: ActiveExpression,
    environment_values: Vec<Value>,
    last_source_value: Option<Value>,
}

fn transfer_prepared_body(
    target: &mut ActiveExpression,
    source: &ActiveExpression,
    target_environment_values: &mut Vec<Value>,
    source_environment_values: &[Value],
    layout: &Rc<EnvironmentLayout>,
    require_exact: bool,
) -> bool {
    let transferred = if require_exact {
        target.evaluator.transfer_from(&source.evaluator)
    } else {
        target.evaluator.transfer_compatible_from(&source.evaluator)
    };
    if !transferred {
        return false;
    }

    target_environment_values.resize(layout.len(), Value::NoVal);
    for target_slot in target.environment_slots.iter().copied() {
        let Some(variable) = layout.variable(target_slot) else {
            return false;
        };
        let Some(source_slot) = source
            .environment_slots
            .iter()
            .copied()
            .find(|slot| layout.variable(*slot) == Some(variable))
        else {
            return false;
        };
        let Some(source_value) = source_environment_values.get(source_slot.index()) else {
            return false;
        };
        target_environment_values[target_slot.index()] = source_value.clone();
    }
    true
}

/// Owns one stream program and its persistent evaluation state.
#[derive(Clone)]
#[repr(C)]
pub(in crate::dataflow) struct StreamEvaluator {
    pub(in crate::dataflow) program: Rc<StreamProgram>,
    pub(in crate::dataflow) state: StreamState,
    quickening_state: Option<quickening::State>,
    // Code is deliberately last: growing a quick plan must not perturb the offsets of the hot
    // semantic state fields used by canonical and native temporal execution.
    quick_plan: Option<Rc<quickening::Plan>>,
}

impl StreamEvaluator {
    pub(in crate::dataflow) fn new(program: Rc<StreamProgram>) -> Self {
        program
            .graph
            .debug_assert_valid(program.environment_layout.len());
        let state = StreamState::new(&program.graph);
        let quick_plan = program
            .is_infallible()
            .then(|| quickening::Plan::new(&program.graph))
            .flatten();
        let quickening_state = quick_plan.as_ref().map(quickening::State::new);
        let quick_plan = quick_plan.map(Rc::new);
        debug_assert_eq!(state.node_values.len(), program.graph.nodes.len());
        debug_assert_eq!(state.node_states.len(), program.graph.nodes.len());
        Self {
            program,
            state,
            quickening_state,
            quick_plan,
        }
    }

    pub(in crate::dataflow) fn evaluate_canonical_infallible(
        &mut self,
        environment_values: &[Value],
    ) -> Value {
        let body = &self.program.graph;
        let context = EvaluationContext {
            environment_values,
            environment_layout: &self.program.environment_layout,
            retained_environment_values: None,
            recursive_call: None,
        };
        evaluate_nodes(&body.nodes, &mut self.state, context);
        let value = context.read_value(&self.state, &body.output);
        stage_recursive_delays(&body.recursive_delays, &mut self.state, &value);
        value
    }

    pub(in crate::dataflow) fn reset(&mut self) {
        self.state.reset();
        if let Some(state) = &mut self.quickening_state {
            state.reset();
        }
    }

    /// Transfer canonical state from an evaluator in the previous monitor.
    ///
    /// Environment slots are replacement-local, so compatibility compares external references by
    /// variable name. A failed transfer leaves the local destination freshly initialized.
    pub(in crate::dataflow) fn transfer_from(&mut self, source: &Self) -> bool {
        if !graphs_semantically_equal(
            &self.program.graph,
            &self.program.environment_layout,
            &source.program.graph,
            &source.program.environment_layout,
        ) {
            return false;
        }
        let transferred = self.state.transfer_from(
            &source.state,
            &self.program.graph,
            &source.program.graph,
            &self.program.environment_layout,
            &source.program.environment_layout,
        );
        if transferred {
            self.invalidate_derived_state();
        }
        transferred
    }

    /// Transfer unchanged semantic state owners into a changed local body. The target evaluator
    /// owns a fresh graph; only matching state cells are copied, so stateless edits and new owners
    /// remain cold while compatible delay/operator history continues.
    pub(in crate::dataflow) fn transfer_compatible_from(&mut self, source: &Self) -> bool {
        let transferred = self.state.transfer_compatible_from(
            &source.state,
            &self.program.graph,
            &source.program.graph,
            &self.program.environment_layout,
            &source.program.environment_layout,
        );
        if transferred {
            self.invalidate_derived_state();
        }
        transferred
    }

    /// Native and quickening state are derived from canonical state and cannot be copied as part of
    /// a root transfer. Leave the canonical evaluator available as the safe fallback until a new
    /// native/quickened tier has rebuilt its own state. Immutable quickening code remains reusable.
    pub(in crate::dataflow) fn invalidate_derived_state(&mut self) {
        self.quickening_state = None;
    }

    /// Top-level schedule plans own their quick code. Nested evaluators retain a local plan because
    /// they are entered outside the monitor schedule and therefore have no `PlanBundle` step.
    pub(in crate::dataflow) fn detach_top_level_quick_plan(&mut self) {
        self.quick_plan = None;
    }

    /// Compile a nested replacement body, transfer from the old donor, and return a local body that
    /// has not yet been installed.  This ordering is intentional: a strict transfer failure never
    /// leaves the new body visible, while a compatible failure can install the freshly compiled
    /// body with incompatible cells reset.
    #[inline]
    pub(in crate::dataflow) fn replace_reconfiguration_point(
        &mut self,
        node: NodeId,
        source_value: Value,
        transfer: ContextTransferPolicy,
    ) -> Result<RegionReplacement, DataflowEvaluationError> {
        let (program, state) = (&self.program, &mut self.state);
        let StreamOp::Dynamic(spec) = &program.graph.nodes[node.index()] else {
            unreachable!("reconfiguration point referenced a non-dynamic node")
        };
        let NodeState::Dynamic(dynamic) = &mut state.node_states[node.index()] else {
            unreachable!("reconfiguration point referenced incompatible runtime state")
        };
        let source_text = match source_value {
            Value::Str(source_text) => source_text,
            Value::Deferred | Value::NoVal => {
                return Ok(RegionReplacement::unchanged());
            }
            other => {
                return Err(DataflowEvaluationError::InvalidExpressionSource(
                    other.to_string(),
                ));
            }
        };

        let had_active_expression = dynamic.active_expression.is_some();
        let donor = if spec.mode == DynamicExpressionMode::Dynamic && had_active_expression {
            Some(DynamicBodyDonor {
                active_expression: dynamic
                    .active_expression
                    .take()
                    .expect("active dynamic body was just observed"),
                environment_values: std::mem::take(&mut dynamic.environment_values),
                last_source_value: dynamic.last_source_value.take(),
            })
        } else {
            None
        };
        let previous_dependency_slots = donor.as_ref().map_or(&[][..], |donor| {
            donor.active_expression.dependency_slots.as_slice()
        });
        let prepared = prepare_active_expression_with_change(
            source_text,
            spec,
            dynamic,
            &program.environment_layout,
            had_active_expression,
            previous_dependency_slots,
        )?
        .expect("the source barrier requested a changed nested body");
        let mut active_expression = prepared.active_expression;
        let mut environment_values = Vec::new();
        let mut state_transferred = true;
        let mut last_source_value = None;

        if let Some(donor) = donor {
            last_source_value = donor.last_source_value;
            if transfer != ContextTransferPolicy::None {
                state_transferred = transfer_prepared_body(
                    &mut active_expression,
                    &donor.active_expression,
                    &mut environment_values,
                    &donor.environment_values,
                    &program.environment_layout,
                    transfer == ContextTransferPolicy::Strict,
                );
            }
        }

        Ok(RegionReplacement {
            activation: prepared.activation,
            state_transferred,
            replacement: Some(PreparedRegionBody {
                active_expression,
                environment_values,
                last_source_value,
            }),
        })
    }

    /// Install a previously transferred local body exactly once.
    pub(in crate::dataflow) fn install_reconfiguration_point(
        &mut self,
        node: NodeId,
        replacement: RegionReplacement,
    ) -> Result<(), DataflowEvaluationError> {
        let Some(replacement) = replacement.replacement else {
            return Ok(());
        };
        let NodeState::Dynamic(dynamic) = &mut self.state.node_states[node.index()] else {
            unreachable!("reconfiguration point referenced incompatible runtime state")
        };
        dynamic.active_expression = Some(replacement.active_expression);
        dynamic.environment_values = replacement.environment_values;
        dynamic.last_source_value = replacement.last_source_value;
        Ok(())
    }

    pub(in crate::dataflow) fn reconfiguration_point_requires_update(
        &self,
        node: NodeId,
        source_value: &Value,
    ) -> bool {
        let StreamOp::Dynamic(spec) = &self.program.graph.nodes[node.index()] else {
            unreachable!("reconfiguration point referenced a non-dynamic node")
        };
        let NodeState::Dynamic(dynamic) = &self.state.node_states[node.index()] else {
            unreachable!("reconfiguration point referenced incompatible runtime state")
        };
        match source_value {
            Value::Str(source_text) => match spec.mode {
                DynamicExpressionMode::Defer => dynamic.active_expression.is_none(),
                DynamicExpressionMode::Dynamic => dynamic
                    .active_expression
                    .as_ref()
                    .is_none_or(|active| &active.source_text != source_text),
            },
            Value::Deferred | Value::NoVal => false,
            _ => true,
        }
    }

    pub(in crate::dataflow) fn reconfiguration_point_dependency_slots(
        &self,
        node: NodeId,
    ) -> &[EnvironmentSlot] {
        let StreamOp::Dynamic(_) = &self.program.graph.nodes[node.index()] else {
            unreachable!("reconfiguration point referenced a non-dynamic node")
        };
        let NodeState::Dynamic(dynamic) = &self.state.node_states[node.index()] else {
            unreachable!("reconfiguration point referenced incompatible runtime state")
        };
        dynamic
            .active_expression
            .as_ref()
            .map_or(&[], |active| active.dependency_slots.as_slice())
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
        quickening_plan_override: Option<&quickening::Plan>,
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

        let quickening_plan = quickening_plan_override.or(self.quick_plan.as_deref());
        if let (Some(plan), Some(state)) = (quickening_plan, &mut self.quickening_state) {
            quickening::execute(
                state,
                plan,
                body,
                &mut self.state,
                context,
                published_scalars,
            );
        } else {
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
        plan: &quickening::SingleScalarPlan,
        published_scalars: &[Option<ScalarValue>],
    ) -> quickening::DirectResult {
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
        let Some(state) = self.quickening_state.as_mut() else {
            evaluate_nodes(&body.nodes, &mut self.state, context);
            let value = context.read_value(&self.state, &body.output);
            return quickening::DirectResult::Canonical(value);
        };
        quickening::execute_single(
            state,
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

    pub(in crate::dataflow) fn discard_staged_temporal_state(&mut self) {
        super::interpreter::discard_staged_temporal_state(&self.program.graph, &mut self.state);
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
            if let Some(state) = &mut self.quickening_state {
                quickening::execute(
                    state,
                    self.quick_plan
                        .as_deref()
                        .expect("quickening state requires a plan"),
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
