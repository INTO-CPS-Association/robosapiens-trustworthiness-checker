use super::super::history::HistoryId;
use super::super::history_requirements::VariableHistoryRequirement;
use super::super::ir::*;
use super::super::*;

use super::environment_projection::EnvironmentProjection;
use super::evaluator::Evaluator;
use super::quickening::ScalarValue;
use crate::core::{RuntimeFunction, RuntimeFunctionValueCallable};
use std::{cell::RefCell, ops::Deref, rc::Rc};

#[cfg(test)]
use std::cell::Cell;

pub(in crate::dataflow) struct EvaluatorState {
    pub(in crate::dataflow) node_values: Vec<Value>,
    pub(in crate::dataflow) node_states: Vec<NodeState>,
}

impl Clone for EvaluatorState {
    fn clone(&self) -> Self {
        #[cfg(test)]
        STATE_CLONES.with(|count| count.set(count.get() + 1));
        Self {
            node_values: self.node_values.clone(),
            node_states: self.node_states.clone(),
        }
    }
}

#[cfg(test)]
thread_local! {
    static STATE_CLONES: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
pub(in crate::dataflow) fn reset_state_clone_count() {
    STATE_CLONES.with(|count| count.set(0));
}

#[cfg(test)]
pub(in crate::dataflow) fn state_clone_count() -> usize {
    STATE_CLONES.with(Cell::get)
}

#[derive(Clone)]
pub(in crate::dataflow) enum NodeState {
    UnaryLift {
        last_input: Option<Value>,
    },
    BinaryLift {
        last_left: Option<Value>,
        last_right: Option<Value>,
    },
    OperandLift {
        last_operands: Vec<Option<Value>>,
    },
    Delay(DelayState),
    #[cfg_attr(not(feature = "jit"), allow(dead_code))]
    ScalarDelay(ScalarDelayState),
    Default {
        last_input: Option<Value>,
    },
    #[cfg_attr(not(feature = "jit"), allow(dead_code))]
    ScalarDefault {
        last_input: Option<ScalarValue>,
    },
    Init {
        started: bool,
    },
    IsDefined {
        last_input: Option<Value>,
    },
    When {
        last_input: Option<Value>,
        started: bool,
    },
    Update {
        switched: bool,
        last_base: Option<Value>,
        last_update: Option<Value>,
    },
    Latch {
        last_value: Option<Value>,
    },
    CallLift {
        last_function: Option<Value>,
        last_arguments: Vec<Option<Value>>,
        active_function: Option<RuntimeFunction>,
        callable: Option<RuntimeFunctionValueCallable>,
    },
    Function {
        function: Option<RuntimeFunction>,
        captures: Rc<RefCell<Vec<Value>>>,
    },
    PersistentCall {
        evaluator: Evaluator,
        environment_values: Vec<Value>,
        last_arguments: Vec<Option<Value>>,
    },
    Dynamic(Box<DynamicExpressionState>),
    LazyIf(LazyIfState),
}

#[derive(Clone)]
pub(in crate::dataflow) struct LazyIfState {
    pub(in crate::dataflow) then_state: Box<EvaluatorState>,
    pub(in crate::dataflow) else_state: Box<EvaluatorState>,
    pub(in crate::dataflow) last_condition: Option<Value>,
    pub(in crate::dataflow) last_then_value: Option<Value>,
    pub(in crate::dataflow) last_else_value: Option<Value>,
}

impl LazyIfState {
    fn reset(&mut self) {
        self.then_state.reset();
        self.else_state.reset();
        self.last_condition = None;
        self.last_then_value = None;
        self.last_else_value = None;
    }
}

pub(in crate::dataflow) const DYNAMIC_EXPRESSION_CACHE_CAPACITY: usize = 4;

#[derive(Clone, Default)]
pub(in crate::dataflow) struct DynamicExpressionState {
    pub(in crate::dataflow) active_expression: Option<ActiveExpression>,
    /// Immutable program templates in least-recently-used order.
    pub(in crate::dataflow) template_cache: Vec<Rc<DynamicExpressionTemplate>>,
    pub(in crate::dataflow) last_source_value: Option<Value>,
    /// `defer`'s retained published result: the last non-`NoVal` body result (`Deferred` counts as
    /// a value). This is independent of retained source input, outer-environment retention, and
    /// the evaluator's temporal state.
    pub(in crate::dataflow) last_defer_result: Option<Value>,
    pub(in crate::dataflow) environment_values: Vec<Value>,
}

impl DynamicExpressionState {
    pub(in crate::dataflow) fn cache_template(&mut self, template: Rc<DynamicExpressionTemplate>) {
        if let Some(index) = self
            .template_cache
            .iter()
            .position(|cached| cached.source_text == template.source_text)
        {
            let cached = self.template_cache.remove(index);
            if Rc::ptr_eq(
                &cached.program.environment_layout,
                &template.program.environment_layout,
            ) {
                self.template_cache.push(cached);
                return;
            }
        }
        if self.template_cache.len() == DYNAMIC_EXPRESSION_CACHE_CAPACITY {
            self.template_cache.remove(0);
        }
        self.template_cache.push(template);
    }

    pub(in crate::dataflow) fn update_environment(
        &mut self,
        environment_values: &[Value],
        retained_environment_values: Option<&[Value]>,
    ) {
        let Some(active) = &self.active_expression else {
            return;
        };
        let projection = &active.environment_projection;
        if self.environment_values.len() != projection.nested_environment_size() {
            self.environment_values
                .resize(projection.nested_environment_size(), Value::NoVal);
        }

        if let Some(retained) = retained_environment_values {
            debug_assert_eq!(environment_values.len(), retained.len());
            for binding in projection.bindings() {
                let current = &environment_values[binding.outer_slot.index()];
                self.environment_values[binding.nested_slot.index()] = if current == &Value::NoVal {
                    retained[binding.outer_slot.index()].clone()
                } else {
                    current.clone()
                };
            }
        } else {
            for binding in projection.bindings() {
                self.environment_values[binding.nested_slot.index()] =
                    environment_values[binding.outer_slot.index()].clone();
            }
        }
    }

    pub(in crate::dataflow) fn for_each_active_body_history_requirement(
        &self,
        visit: &mut impl FnMut(VariableHistoryRequirement),
    ) {
        let Some(active) = &self.active_expression else {
            return;
        };
        for &requirement in active.environment_projection.outer_history_requirements() {
            visit(requirement);
        }
    }
}

#[derive(Clone)]
pub(in crate::dataflow) struct DynamicExpressionTemplate {
    pub(in crate::dataflow) source_text: EcoString,
    pub(in crate::dataflow) program: Rc<StreamProgram>,
    pub(in crate::dataflow) nested_dependency_slots: Vec<EnvironmentSlot>,
    pub(in crate::dataflow) nested_environment_slots: Vec<EnvironmentSlot>,
    pub(in crate::dataflow) nested_history_requirements:
        super::super::history_requirements::HistoryRequirements,
}

#[derive(Clone)]
pub(in crate::dataflow) struct ActiveExpression {
    pub(in crate::dataflow) template: Rc<DynamicExpressionTemplate>,
    pub(in crate::dataflow) evaluator: Evaluator,
    pub(in crate::dataflow) environment_projection: EnvironmentProjection,
}

impl ActiveExpression {
    pub(in crate::dataflow) fn rebind_environment(&mut self, projection: EnvironmentProjection) {
        self.environment_projection = projection;
    }
}

impl Deref for ActiveExpression {
    type Target = DynamicExpressionTemplate;

    fn deref(&self) -> &Self::Target {
        &self.template
    }
}

#[derive(Clone)]
pub(in crate::dataflow) struct DelayState {
    values: Vec<Value>,
    next_write: usize,
    filled_slots: usize,
    last_output: Option<Value>,
    write_pending: bool,
    shared_read_pending: bool,
    staged_recursive_value: Option<Value>,
}

/// Compact evaluator-owned state used by scheduled scalar temporal operations.
#[derive(Clone)]
pub(in crate::dataflow) struct ScalarDelayState {
    values: Vec<ScalarValue>,
    next_write: usize,
    filled_slots: usize,
    last_output: Option<ScalarValue>,
    write_pending: bool,
    shared_read_pending: bool,
    staged_recursive_value: Option<ScalarValue>,
}

impl DelayState {
    pub(in crate::dataflow) fn new(offset: usize) -> Self {
        Self {
            values: vec![Value::NoVal; offset],
            next_write: 0,
            filled_slots: 0,
            last_output: None,
            write_pending: false,
            shared_read_pending: false,
            staged_recursive_value: None,
        }
    }

    pub(in crate::dataflow) fn new_shared() -> Self {
        Self {
            values: Vec::new(),
            next_write: 0,
            filled_slots: 0,
            last_output: None,
            write_pending: false,
            shared_read_pending: false,
            staged_recursive_value: None,
        }
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn hydrate_shared_history(
        &mut self,
        depth: usize,
        values: impl IntoIterator<Item = Value>,
    ) {
        if !self.values.is_empty() {
            return;
        }
        self.values = vec![Value::NoVal; depth];
        self.next_write = 0;
        self.filled_slots = 0;
        for value in values {
            self.push_value(value);
        }
        self.shared_read_pending = false;
    }

    pub(in crate::dataflow) fn read_delayed_value(&self) -> Value {
        if self.values.is_empty() || self.filled_slots < self.values.len() {
            Value::Deferred
        } else {
            self.values[self.next_write].clone()
        }
    }

    pub(in crate::dataflow) fn push_value(&mut self, value: Value) {
        if self.values.is_empty() {
            return;
        }
        self.values[self.next_write] = value;
        self.next_write = (self.next_write + 1) % self.values.len();
        self.filled_slots = self.filled_slots.saturating_add(1).min(self.values.len());
    }

    pub(in crate::dataflow) fn read_and_stage_write(&mut self) -> Value {
        debug_assert!(
            !self.write_pending && !self.shared_read_pending,
            "delay was evaluated more than once before commit"
        );
        self.write_pending = true;
        let previous = self.read_delayed_value();
        super::lifting::retain_last_value(previous, &mut self.last_output)
    }

    pub(in crate::dataflow) fn read_shared_value(&mut self, value: Value) -> Value {
        debug_assert!(
            !self.write_pending && !self.shared_read_pending,
            "delay was evaluated more than once before commit"
        );
        self.shared_read_pending = true;
        self.values.clear();
        self.next_write = 0;
        self.filled_slots = 0;
        super::lifting::retain_last_value(value, &mut self.last_output)
    }

    pub(in crate::dataflow) fn commit_staged_write(&mut self, value: Value) {
        if self.shared_read_pending {
            self.shared_read_pending = false;
            return;
        }
        if self.write_pending {
            self.write_pending = false;
            self.push_value(value);
        }
    }

    pub(in crate::dataflow) fn discard_staged_write(&mut self) {
        self.write_pending = false;
        self.shared_read_pending = false;
    }

    pub(in crate::dataflow) fn stage_recursive_value(&mut self, value: Value) {
        debug_assert!(
            self.staged_recursive_value.is_none(),
            "recursive delay was evaluated more than once before commit"
        );
        self.staged_recursive_value = Some(value);
    }

    pub(in crate::dataflow) fn commit_recursive_value(&mut self) {
        if let Some(value) = self.staged_recursive_value.take() {
            self.push_value(value);
        }
    }

    pub(in crate::dataflow) fn discard_recursive_value(&mut self) {
        self.staged_recursive_value = None;
    }

    pub(in crate::dataflow) fn retain_current_value(&mut self, value: Value) -> Value {
        super::lifting::retain_last_value(value, &mut self.last_output)
    }

    pub(in crate::dataflow) fn reset(&mut self) {
        self.next_write = 0;
        self.filled_slots = 0;
        self.last_output = None;
        self.write_pending = false;
        self.shared_read_pending = false;
        self.staged_recursive_value = None;
    }

    #[cfg_attr(not(feature = "jit"), allow(dead_code))]
    pub(in crate::dataflow) fn to_scalar(&self) -> Option<ScalarDelayState> {
        let optional_scalar = |value: &Option<Value>| match value {
            Some(value) => ScalarValue::from_untyped_value(value).map(Some),
            None => Some(None),
        };
        Some(ScalarDelayState {
            values: self
                .values
                .iter()
                .map(ScalarValue::from_untyped_value)
                .collect::<Option<Vec<_>>>()?,
            next_write: self.next_write,
            filled_slots: self.filled_slots,
            last_output: optional_scalar(&self.last_output)?,
            write_pending: self.write_pending,
            shared_read_pending: self.shared_read_pending,
            staged_recursive_value: optional_scalar(&self.staged_recursive_value)?,
        })
    }
}

impl ScalarDelayState {
    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn native_parts(
        &self,
    ) -> (&[ScalarValue], usize, usize, Option<ScalarValue>) {
        (
            &self.values,
            self.next_write,
            self.filled_slots,
            self.last_output,
        )
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn restore_native_parts(
        &mut self,
        values: &[ScalarValue],
        next_write: usize,
        filled_slots: usize,
        last_output: Option<ScalarValue>,
    ) {
        debug_assert_eq!(self.values.len(), values.len());
        self.values.copy_from_slice(values);
        self.next_write = next_write;
        self.filled_slots = filled_slots.min(self.values.len());
        self.last_output = last_output;
        self.write_pending = false;
        self.shared_read_pending = false;
        self.staged_recursive_value = None;
    }

    #[inline]
    pub(in crate::dataflow) fn read_delayed_value(&self) -> ScalarValue {
        if self.values.is_empty() || self.filled_slots < self.values.len() {
            ScalarValue::Deferred
        } else {
            self.values[self.next_write]
        }
    }

    #[inline]
    pub(in crate::dataflow) fn read_and_stage_write(&mut self) -> ScalarValue {
        debug_assert!(!self.write_pending && !self.shared_read_pending);
        self.write_pending = true;
        let previous = self.read_delayed_value();
        match previous {
            ScalarValue::NoVal => self.last_output.unwrap_or(ScalarValue::NoVal),
            value => {
                self.last_output = Some(value);
                value
            }
        }
    }

    #[inline]
    pub(in crate::dataflow) fn read_shared_value(&mut self, value: ScalarValue) -> ScalarValue {
        debug_assert!(!self.write_pending && !self.shared_read_pending);
        self.shared_read_pending = true;
        self.values.clear();
        self.next_write = 0;
        self.filled_slots = 0;
        match value {
            ScalarValue::NoVal => self.last_output.unwrap_or(ScalarValue::NoVal),
            value => {
                self.last_output = Some(value);
                value
            }
        }
    }

    #[inline]
    pub(in crate::dataflow) fn retain_current_value(&mut self, value: ScalarValue) -> ScalarValue {
        match value {
            ScalarValue::NoVal => self.last_output.unwrap_or(ScalarValue::NoVal),
            value => {
                self.last_output = Some(value);
                value
            }
        }
    }

    #[inline]
    pub(in crate::dataflow) fn commit_staged_write(&mut self, value: ScalarValue) {
        if self.shared_read_pending {
            self.shared_read_pending = false;
            return;
        }
        if !self.write_pending {
            return;
        }
        self.write_pending = false;
        if self.values.is_empty() {
            return;
        }
        self.values[self.next_write] = value;
        self.next_write = (self.next_write + 1) % self.values.len();
        self.filled_slots = self.filled_slots.saturating_add(1).min(self.values.len());
    }

    pub(in crate::dataflow) fn discard_staged_write(&mut self) {
        self.write_pending = false;
        self.shared_read_pending = false;
    }

    pub(in crate::dataflow) fn stage_recursive_value(&mut self, value: ScalarValue) {
        debug_assert!(self.staged_recursive_value.is_none());
        self.staged_recursive_value = Some(value);
    }

    pub(in crate::dataflow) fn commit_recursive_value(&mut self) {
        if let Some(value) = self.staged_recursive_value.take() {
            if self.values.is_empty() {
                return;
            }
            self.values[self.next_write] = value;
            self.next_write = (self.next_write + 1) % self.values.len();
            self.filled_slots = self.filled_slots.saturating_add(1).min(self.values.len());
        }
    }

    pub(in crate::dataflow) fn discard_recursive_value(&mut self) {
        self.staged_recursive_value = None;
    }

    #[cfg_attr(not(feature = "jit"), allow(dead_code))]
    pub(in crate::dataflow) fn to_canonical(&self) -> DelayState {
        DelayState {
            values: self
                .values
                .iter()
                .copied()
                .map(ScalarValue::into_value)
                .collect(),
            next_write: self.next_write,
            filled_slots: self.filled_slots,
            last_output: self.last_output.map(ScalarValue::into_value),
            write_pending: self.write_pending,
            shared_read_pending: self.shared_read_pending,
            staged_recursive_value: self.staged_recursive_value.map(ScalarValue::into_value),
        }
    }

    fn reset(&mut self) {
        self.next_write = 0;
        self.filled_slots = 0;
        self.last_output = None;
        self.write_pending = false;
        self.shared_read_pending = false;
        self.staged_recursive_value = None;
    }
}

impl EvaluatorState {
    #[cfg(test)]
    pub(in crate::dataflow) fn new(body: &BoundEvaluationGraph) -> Self {
        Self::new_for_nodes(&body.nodes, &[])
    }

    pub(in crate::dataflow) fn new_with_history(
        body: &BoundEvaluationGraph,
        history_bindings: &[Option<HistoryId>],
    ) -> Self {
        Self::new_for_nodes(&body.nodes, history_bindings)
    }

    /// Perform the read-only structural part of an ownership-moving context rewrite.
    ///
    /// Native execution may temporarily represent a canonical owner with a compact state variant,
    /// so this preflight checks the graph and mapping shape but leaves representation checks to
    /// [`Self::validate_context_rewrite`] after native state has been materialized.
    pub(in crate::dataflow) fn validate_context_mapping(
        &self,
        source: &Self,
        target_body: &BoundEvaluationGraph,
        source_body: &BoundEvaluationGraph,
        mapping: &StreamMapping,
    ) -> bool {
        if matches!(mapping, StreamMapping::Unmapped) {
            return true;
        }
        if !state_dimensions_compatible(self, source, target_body, source_body) {
            return false;
        }

        match mapping {
            StreamMapping::Exact(_) | StreamMapping::Unmapped => true,
        }
    }

    /// Validate an exact ownership-moving rewrite without changing either state arena.
    pub(in crate::dataflow) fn validate_exact_rewrite(
        &self,
        source: &Self,
        target_body: &BoundEvaluationGraph,
        source_body: &BoundEvaluationGraph,
        target_layout: &Rc<EnvironmentLayout>,
        source_layout: &Rc<EnvironmentLayout>,
    ) -> bool {
        state_shape_compatible(
            self,
            source,
            target_body,
            source_body,
            target_layout,
            source_layout,
        )
    }

    /// Validate an ownership-moving rewrite without changing either state arena.
    pub(in crate::dataflow) fn validate_context_rewrite(
        &self,
        source: &Self,
        target_body: &BoundEvaluationGraph,
        source_body: &BoundEvaluationGraph,
        target_layout: &Rc<EnvironmentLayout>,
        source_layout: &Rc<EnvironmentLayout>,
        mapping: &StreamMapping,
    ) -> bool {
        if !self.validate_context_mapping(source, target_body, source_body, mapping) {
            return false;
        }
        match mapping {
            StreamMapping::Exact(_) => state_shape_compatible(
                self,
                source,
                target_body,
                source_body,
                target_layout,
                source_layout,
            ),
            StreamMapping::Unmapped => true,
        }
    }

    pub(in crate::dataflow) fn dynamic_expression_state(
        &self,
        node: NodeId,
    ) -> &DynamicExpressionState {
        let NodeState::Dynamic(dynamic) = &self.node_states[node.index()] else {
            unreachable!("reconfigurable expression referenced incompatible runtime state")
        };
        dynamic
    }

    pub(in crate::dataflow) fn dynamic_expression_state_mut(
        &mut self,
        node: NodeId,
    ) -> &mut DynamicExpressionState {
        let NodeState::Dynamic(dynamic) = &mut self.node_states[node.index()] else {
            unreachable!("reconfigurable expression referenced incompatible runtime state")
        };
        dynamic
    }

    pub(in crate::dataflow) fn for_each_active_body_history_requirement(
        &self,
        visit: &mut impl FnMut(VariableHistoryRequirement),
    ) {
        for state in &self.node_states {
            match state {
                NodeState::Dynamic(dynamic) => {
                    dynamic.for_each_active_body_history_requirement(&mut *visit);
                }
                NodeState::PersistentCall { evaluator, .. } => {
                    evaluator.for_each_active_body_history_requirement(&mut *visit);
                }
                NodeState::LazyIf(lazy_if) => {
                    lazy_if
                        .then_state
                        .for_each_active_body_history_requirement(&mut *visit);
                    lazy_if
                        .else_state
                        .for_each_active_body_history_requirement(&mut *visit);
                }
                _ => {}
            }
        }
    }

    fn new_for_nodes(nodes: &[BoundOp], history_bindings: &[Option<HistoryId>]) -> Self {
        Self {
            node_values: vec![Value::NoVal; nodes.len()],
            node_states: nodes
                .iter()
                .map(|op| NodeState::for_op_with_history(op, history_bindings))
                .collect(),
        }
    }

    pub(in crate::dataflow) fn reset(&mut self) {
        for node in &mut self.node_values {
            *node = Value::NoVal;
        }
        for state in &mut self.node_states {
            state.reset();
        }
    }

    #[cfg(test)]
    pub(in crate::dataflow) fn delay_ring_lengths(&self) -> Vec<usize> {
        self.node_states
            .iter()
            .filter_map(|state| match state {
                NodeState::Delay(history) => Some(history.values.len()),
                NodeState::ScalarDelay(history) => Some(history.values.len()),
                _ => None,
            })
            .collect()
    }
}

fn state_dimensions_compatible(
    target: &EvaluatorState,
    source: &EvaluatorState,
    target_body: &BoundEvaluationGraph,
    source_body: &BoundEvaluationGraph,
) -> bool {
    target.node_values.len() == target_body.nodes.len()
        && target.node_states.len() == target_body.nodes.len()
        && source.node_values.len() == source_body.nodes.len()
        && source.node_states.len() == source_body.nodes.len()
}

fn state_shape_compatible(
    target: &EvaluatorState,
    source: &EvaluatorState,
    target_body: &BoundEvaluationGraph,
    source_body: &BoundEvaluationGraph,
    target_layout: &Rc<EnvironmentLayout>,
    source_layout: &Rc<EnvironmentLayout>,
) -> bool {
    target.node_values.len() == target_body.nodes.len()
        && target.node_states.len() == target_body.nodes.len()
        && source.node_values.len() == source_body.nodes.len()
        && source.node_states.len() == source_body.nodes.len()
        && target
            .node_states
            .iter()
            .zip(&source.node_states)
            .enumerate()
            .all(|(index, (target_state, source_state))| {
                node_state_can_rewrite(
                    target_state,
                    source_state,
                    &target_body.nodes[index],
                    &source_body.nodes[index],
                    target_layout,
                    source_layout,
                )
            })
}

fn node_state_can_rewrite(
    target: &NodeState,
    source: &NodeState,
    target_op: &BoundOp,
    source_op: &BoundOp,
    target_layout: &Rc<EnvironmentLayout>,
    source_layout: &Rc<EnvironmentLayout>,
) -> bool {
    match (target, source) {
        (NodeState::UnaryLift { .. }, NodeState::UnaryLift { .. })
        | (NodeState::Default { .. }, NodeState::Default { .. })
        | (NodeState::ScalarDefault { .. }, NodeState::ScalarDefault { .. })
        | (NodeState::Init { .. }, NodeState::Init { .. })
        | (NodeState::IsDefined { .. }, NodeState::IsDefined { .. })
        | (NodeState::When { .. }, NodeState::When { .. })
        | (NodeState::Latch { .. }, NodeState::Latch { .. }) => true,
        (NodeState::BinaryLift { .. }, NodeState::BinaryLift { .. })
        | (NodeState::Update { .. }, NodeState::Update { .. }) => true,
        (
            NodeState::OperandLift {
                last_operands: target,
            },
            NodeState::OperandLift {
                last_operands: source,
            },
        ) => target.len() == source.len(),
        (NodeState::Delay(target), NodeState::Delay(source)) => {
            target.values.len() == source.values.len()
        }
        (NodeState::ScalarDelay(target), NodeState::ScalarDelay(source)) => {
            target.values.len() == source.values.len()
        }
        (
            NodeState::CallLift {
                last_arguments: target_arguments,
                ..
            },
            NodeState::CallLift {
                last_arguments: source_arguments,
                ..
            },
        ) => target_arguments.len() == source_arguments.len(),
        (
            NodeState::Function {
                captures: target_captures,
                ..
            },
            NodeState::Function {
                captures: source_captures,
                ..
            },
        ) => {
            let (
                StreamOp::Function { func: target_func },
                StreamOp::Function { func: source_func },
            ) = (target_op, source_op)
            else {
                return false;
            };
            target_captures.borrow().len() == target_func.capture_slots.len()
                && source_captures.borrow().len() == source_func.capture_slots.len()
        }
        (
            NodeState::PersistentCall {
                evaluator: target_evaluator,
                environment_values: target_environment,
                last_arguments: target_arguments,
            },
            NodeState::PersistentCall {
                evaluator: source_evaluator,
                environment_values: source_environment,
                last_arguments: source_arguments,
            },
        ) => {
            target_environment.len() == source_environment.len()
                && target_arguments.len() == source_arguments.len()
                && target_evaluator.program.state_key() == source_evaluator.program.state_key()
                && state_shape_compatible(
                    &target_evaluator.tier_states.canonical,
                    &source_evaluator.tier_states.canonical,
                    &target_evaluator.program.graph,
                    &source_evaluator.program.graph,
                    &target_evaluator.program.environment_layout,
                    &source_evaluator.program.environment_layout,
                )
        }
        (NodeState::Dynamic(_), NodeState::Dynamic(_)) => {
            matches!(
                (target_op, source_op),
                (StreamOp::Dynamic(_), StreamOp::Dynamic(_))
            )
        }
        (NodeState::LazyIf(target), NodeState::LazyIf(source)) => {
            let (
                StreamOp::If {
                    then_branch: target_then,
                    else_branch: target_else,
                    ..
                },
                StreamOp::If {
                    then_branch: source_then,
                    else_branch: source_else,
                    ..
                },
            ) = (target_op, source_op)
            else {
                return false;
            };
            state_shape_compatible(
                target.then_state.as_ref(),
                source.then_state.as_ref(),
                target_then,
                source_then,
                target_layout,
                source_layout,
            ) && state_shape_compatible(
                target.else_state.as_ref(),
                source.else_state.as_ref(),
                target_else,
                source_else,
                target_layout,
                source_layout,
            )
        }
        _ => false,
    }
}

impl NodeState {
    fn for_op_with_history(op: &BoundOp, history_bindings: &[Option<HistoryId>]) -> Self {
        match op {
            StreamOp::Unary { .. } => Self::UnaryLift { last_input: None },
            StreamOp::Binary { .. } => Self::BinaryLift {
                last_left: None,
                last_right: None,
            },
            StreamOp::List(items) | StreamOp::Tuple(items) => Self::OperandLift {
                last_operands: vec![None; items.len()],
            },
            StreamOp::Map(items) => Self::OperandLift {
                last_operands: vec![None; items.len()],
            },
            StreamOp::LIndex { .. }
            | StreamOp::LAppend { .. }
            | StreamOp::LConcat { .. }
            | StreamOp::MInsert { .. }
            | StreamOp::ListMap { .. }
            | StreamOp::ListFilter { .. } => Self::OperandLift {
                last_operands: vec![None; 2],
            },
            StreamOp::LHead { .. }
            | StreamOp::LTail { .. }
            | StreamOp::LLen { .. }
            | StreamOp::MGet { .. }
            | StreamOp::MRemove { .. }
            | StreamOp::MHasKey { .. }
            | StreamOp::TGet { .. }
            | StreamOp::Fix { .. } => Self::OperandLift {
                last_operands: vec![None; 1],
            },
            StreamOp::ListFold { .. } => Self::OperandLift {
                last_operands: vec![None; 3],
            },
            StreamOp::Delay { input, offset } => {
                let shared = *offset > 0
                    && matches!(input, BoundRef::External(slot)
                        if history_bindings
                            .get(slot.index())
                            .is_some_and(Option::is_some));
                if shared {
                    Self::Delay(DelayState::new_shared())
                } else {
                    Self::Delay(DelayState::new(
                        usize::try_from(*offset).expect("sindex offset does not fit usize"),
                    ))
                }
            }
            StreamOp::RecursiveDelay { offset } => Self::Delay(DelayState::new(
                usize::try_from(offset.get()).expect("sindex offset does not fit usize"),
            )),
            StreamOp::Default { .. } => Self::Default { last_input: None },
            StreamOp::Init { .. } => Self::Init { started: false },
            StreamOp::IsDefined { .. } => Self::IsDefined { last_input: None },
            StreamOp::When { .. } => Self::When {
                last_input: None,
                started: false,
            },
            StreamOp::Update { .. } => Self::Update {
                switched: false,
                last_base: None,
                last_update: None,
            },
            StreamOp::Latch { .. } => Self::Latch { last_value: None },
            StreamOp::Apply { args, .. } | StreamOp::Partial { args, .. } => Self::CallLift {
                last_function: None,
                last_arguments: vec![None; args.len()],
                active_function: None,
                callable: None,
            },
            StreamOp::Function { func } => Self::Function {
                function: None,
                captures: Rc::new(RefCell::new(vec![Value::NoVal; func.capture_slots.len()])),
            },
            StreamOp::DirectApply { func, args } => Self::PersistentCall {
                evaluator: Evaluator::new(Rc::clone(&func.program)),
                environment_values: vec![
                    Value::NoVal;
                    func.capture_slots.len() + func.parameters.len()
                ],
                last_arguments: vec![None; args.len()],
            },
            StreamOp::RecursiveApply { args, .. } | StreamOp::RecursiveCall { args } => {
                Self::CallLift {
                    last_function: None,
                    last_arguments: vec![None; args.len()],
                    active_function: None,
                    callable: None,
                }
            }
            StreamOp::Dynamic(_) => Self::Dynamic(Box::default()),
            StreamOp::If {
                then_branch,
                else_branch,
                ..
            } => Self::LazyIf(LazyIfState {
                then_state: Box::new(EvaluatorState::new_for_nodes(
                    &then_branch.nodes,
                    history_bindings,
                )),
                else_state: Box::new(EvaluatorState::new_for_nodes(
                    &else_branch.nodes,
                    history_bindings,
                )),
                last_condition: None,
                last_then_value: None,
                last_else_value: None,
            }),
        }
    }

    fn reset(&mut self) {
        match self {
            Self::UnaryLift { last_input }
            | Self::Default { last_input }
            | Self::IsDefined { last_input } => *last_input = None,
            Self::BinaryLift {
                last_left,
                last_right,
            } => {
                *last_left = None;
                *last_right = None;
            }
            Self::OperandLift { last_operands } => last_operands.fill(None),
            Self::Delay(history) => history.reset(),
            Self::ScalarDelay(history) => history.reset(),
            Self::ScalarDefault { last_input } => *last_input = None,
            Self::Init { started } => *started = false,
            Self::When {
                last_input,
                started,
            } => {
                *last_input = None;
                *started = false;
            }
            Self::Update {
                switched,
                last_base,
                last_update,
            } => {
                *switched = false;
                *last_base = None;
                *last_update = None;
            }
            Self::Latch { last_value } => *last_value = None,
            Self::CallLift {
                last_function,
                last_arguments,
                active_function,
                callable,
            } => {
                *last_function = None;
                last_arguments.fill(None);
                *active_function = None;
                *callable = None;
            }
            Self::Function { function, captures } => {
                *function = None;
                captures.borrow_mut().fill(Value::NoVal);
            }
            Self::PersistentCall {
                evaluator,
                environment_values,
                last_arguments,
            } => {
                evaluator.reset();
                environment_values.fill(Value::NoVal);
                last_arguments.fill(None);
            }
            Self::Dynamic(dynamic) => **dynamic = DynamicExpressionState::default(),
            Self::LazyIf(lazy_if) => lazy_if.reset(),
        }
    }
}
