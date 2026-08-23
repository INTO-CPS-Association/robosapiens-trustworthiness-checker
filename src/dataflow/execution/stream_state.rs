use super::super::ir::*;
use super::super::*;
use super::dynamic_expressions::update_active_expression_with_change;
use super::quickening::ScalarValue;
use super::stream_evaluator::StreamEvaluator;
use crate::core::{RuntimeFunction, RuntimeFunctionValueCallable};
use std::{cell::RefCell, ops::Deref, rc::Rc};

#[derive(Clone)]
pub(in crate::dataflow) struct StreamState {
    pub(in crate::dataflow) node_values: Vec<Value>,
    pub(in crate::dataflow) node_states: Vec<NodeState>,
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
        evaluator: StreamEvaluator,
        environment_values: Vec<Value>,
        last_arguments: Vec<Option<Value>>,
    },
    Dynamic(Box<DynamicExpressionState>),
    LazyIf(LazyIfState),
}

#[derive(Clone)]
pub(in crate::dataflow) struct LazyIfState {
    pub(in crate::dataflow) then_state: Box<StreamState>,
    pub(in crate::dataflow) else_state: Box<StreamState>,
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
    /// Immutable program templates in most-recently-used order.
    pub(in crate::dataflow) template_cache: Vec<Rc<DynamicExpressionTemplate>>,
    pub(in crate::dataflow) last_source_value: Option<Value>,
    /// `defer`'s retained published result: the last non-`NoVal` body result (`Deferred` counts as
    /// a value). This is independent of retained source input, outer-environment retention, and
    /// the evaluator's temporal state.
    pub(in crate::dataflow) last_defer_result: Option<Value>,
    pub(in crate::dataflow) environment_values: Vec<Value>,
}

impl DynamicExpressionState {
    pub(in crate::dataflow) fn cached_template(
        &mut self,
        source_text: &EcoString,
    ) -> Option<Rc<DynamicExpressionTemplate>> {
        let index = self
            .template_cache
            .iter()
            .position(|template| &template.source_text == source_text)?;
        let template = self.template_cache.remove(index);
        self.template_cache.insert(0, Rc::clone(&template));
        Some(template)
    }

    pub(in crate::dataflow) fn cache_template(&mut self, template: Rc<DynamicExpressionTemplate>) {
        debug_assert!(
            self.template_cache
                .iter()
                .all(|cached| cached.source_text != template.source_text),
            "dynamic expression template cache contains duplicate source text"
        );
        self.template_cache.insert(0, template);
        self.template_cache
            .truncate(DYNAMIC_EXPRESSION_CACHE_CAPACITY);
    }

    pub(in crate::dataflow) fn update_environment(
        &mut self,
        environment_values: &[Value],
        retained_environment_values: Option<&[Value]>,
    ) {
        let Some(active) = &self.active_expression else {
            return;
        };
        if self.environment_values.len() != environment_values.len() {
            self.environment_values
                .resize(environment_values.len(), Value::NoVal);
        }

        if let Some(retained) = retained_environment_values {
            debug_assert_eq!(environment_values.len(), retained.len());
            for &slot in &active.environment_slots {
                let current = &environment_values[slot.index()];
                self.environment_values[slot.index()] = if current == &Value::NoVal {
                    retained[slot.index()].clone()
                } else {
                    current.clone()
                };
            }
        } else {
            for &slot in &active.environment_slots {
                self.environment_values[slot.index()] = environment_values[slot.index()].clone();
            }
        }
    }
}

#[derive(Clone)]
pub(in crate::dataflow) struct DynamicExpressionTemplate {
    pub(in crate::dataflow) source_text: EcoString,
    pub(in crate::dataflow) program: Rc<StreamProgram>,
    pub(in crate::dataflow) dependency_slots: Vec<EnvironmentSlot>,
    pub(in crate::dataflow) environment_slots: Vec<EnvironmentSlot>,
}

#[derive(Clone)]
pub(in crate::dataflow) struct ActiveExpression {
    pub(in crate::dataflow) template: Rc<DynamicExpressionTemplate>,
    pub(in crate::dataflow) evaluator: StreamEvaluator,
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
            staged_recursive_value: None,
        }
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
            !self.write_pending,
            "delay was evaluated more than once before commit"
        );
        self.write_pending = true;
        let previous = self.read_delayed_value();
        super::lifting::retain_last_value(previous, &mut self.last_output)
    }

    pub(in crate::dataflow) fn commit_staged_write(&mut self, value: Value) {
        if self.write_pending {
            self.write_pending = false;
            self.push_value(value);
        }
    }

    pub(in crate::dataflow) fn discard_staged_write(&mut self) {
        self.write_pending = false;
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
        debug_assert!(!self.write_pending);
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
            staged_recursive_value: self.staged_recursive_value.map(ScalarValue::into_value),
        }
    }

    fn reset(&mut self) {
        self.next_write = 0;
        self.filled_slots = 0;
        self.last_output = None;
        self.write_pending = false;
        self.staged_recursive_value = None;
    }
}

impl StreamState {
    pub(in crate::dataflow) fn new(body: &BoundEvaluationGraph) -> Self {
        Self::new_for_nodes(&body.nodes)
    }

    /// Copy state after the caller has established semantic program compatibility.
    ///
    /// The recursive walk is deliberately aware of the corresponding bound programs.  This lets
    /// nested evaluators and capture/environment vectors can be remapped by variable name instead of
    /// by replacement-local slot position.
    pub(in crate::dataflow) fn transfer_from(
        &mut self,
        source: &Self,
        target_body: &BoundEvaluationGraph,
        source_body: &BoundEvaluationGraph,
        target_layout: &Rc<EnvironmentLayout>,
        source_layout: &Rc<EnvironmentLayout>,
    ) -> bool {
        let mut candidate = self.clone();
        if !transfer_stream_state(
            &mut candidate,
            source,
            target_body,
            source_body,
            target_layout,
            source_layout,
        ) {
            return false;
        }
        *self = candidate;
        true
    }

    pub(in crate::dataflow) fn transfer_compatible_from(
        &mut self,
        source: &Self,
        target_body: &BoundEvaluationGraph,
        source_body: &BoundEvaluationGraph,
        target_layout: &Rc<EnvironmentLayout>,
        source_layout: &Rc<EnvironmentLayout>,
    ) -> bool {
        let mut candidate = self.clone();
        if !transfer_compatible_state(
            &mut candidate,
            source,
            target_body,
            source_body,
            target_layout,
            source_layout,
        ) {
            return false;
        }
        *self = candidate;
        true
    }

    fn new_for_nodes(nodes: &[BoundOp]) -> Self {
        Self {
            node_values: vec![Value::NoVal; nodes.len()],
            node_states: nodes.iter().map(NodeState::for_op).collect(),
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
}

fn transfer_stream_state(
    target: &mut StreamState,
    source: &StreamState,
    target_body: &BoundEvaluationGraph,
    source_body: &BoundEvaluationGraph,
    target_layout: &Rc<EnvironmentLayout>,
    source_layout: &Rc<EnvironmentLayout>,
) -> bool {
    if target.node_values.len() != source.node_values.len()
        || target.node_states.len() != source.node_states.len()
        || target_body.nodes.len() != source_body.nodes.len()
    {
        return false;
    }

    for index in 0..target.node_states.len() {
        if !transfer_node_state(
            &mut target.node_states[index],
            &source.node_states[index],
            &target_body.nodes[index],
            &source_body.nodes[index],
            target_layout,
            source_layout,
        ) {
            return false;
        }
    }
    target.node_values.clone_from(&source.node_values);
    true
}

fn transfer_compatible_state(
    target: &mut StreamState,
    source: &StreamState,
    target_body: &BoundEvaluationGraph,
    source_body: &BoundEvaluationGraph,
    target_layout: &Rc<EnvironmentLayout>,
    source_layout: &Rc<EnvironmentLayout>,
) -> bool {
    if target.node_states.len() != target_body.nodes.len()
        || source.node_states.len() != source_body.nodes.len()
    {
        return false;
    }
    let source_descriptors = source_body
        .nodes
        .iter()
        .enumerate()
        .map(|(index, _)| {
            node_semantic_descriptor(source_body, NodeId::new(index), source_layout.as_ref())
        })
        .collect::<Vec<_>>();
    let mut used_source = vec![false; source_body.nodes.len()];
    let mut transferred_any = false;
    for (target_index, target_op) in target_body.nodes.iter().enumerate() {
        let target_descriptor = node_semantic_descriptor(
            target_body,
            NodeId::new(target_index),
            target_layout.as_ref(),
        );
        let mut candidates = source_descriptors
            .iter()
            .enumerate()
            .filter(|(_, source_descriptor)| **source_descriptor == target_descriptor)
            .map(|(source_index, _)| source_index);
        let Some(source_index) = candidates.next() else {
            continue;
        };
        if candidates.next().is_some() || used_source[source_index] {
            continue;
        }
        if transfer_node_state(
            &mut target.node_states[target_index],
            &source.node_states[source_index],
            target_op,
            &source_body.nodes[source_index],
            target_layout,
            source_layout,
        ) {
            used_source[source_index] = true;
            transferred_any = true;
        }
    }
    transferred_any || target_body.nodes.is_empty()
}

fn transfer_node_state(
    target: &mut NodeState,
    source: &NodeState,
    target_op: &BoundOp,
    source_op: &BoundOp,
    target_layout: &Rc<EnvironmentLayout>,
    source_layout: &Rc<EnvironmentLayout>,
) -> bool {
    match (target, source) {
        (
            NodeState::UnaryLift { last_input: target },
            NodeState::UnaryLift { last_input: source },
        )
        | (NodeState::Default { last_input: target }, NodeState::Default { last_input: source })
        | (
            NodeState::IsDefined { last_input: target },
            NodeState::IsDefined { last_input: source },
        ) => {
            *target = source.clone();
            true
        }
        (
            NodeState::BinaryLift {
                last_left: target_left,
                last_right: target_right,
            },
            NodeState::BinaryLift {
                last_left: source_left,
                last_right: source_right,
            },
        ) => {
            *target_left = source_left.clone();
            *target_right = source_right.clone();
            true
        }
        (
            NodeState::OperandLift {
                last_operands: target,
            },
            NodeState::OperandLift {
                last_operands: source,
            },
        ) if target.len() == source.len() => {
            target.clone_from(source);
            true
        }
        (NodeState::Delay(target), NodeState::Delay(source))
            if target.values.len() == source.values.len() =>
        {
            *target = source.clone();
            true
        }
        (NodeState::ScalarDelay(target), NodeState::ScalarDelay(source))
            if target.values.len() == source.values.len() =>
        {
            *target = source.clone();
            true
        }
        (NodeState::Init { started: target }, NodeState::Init { started: source }) => {
            *target = *source;
            true
        }
        (
            NodeState::When {
                last_input: target_input,
                started: target_started,
            },
            NodeState::When {
                last_input: source_input,
                started: source_started,
            },
        ) => {
            *target_input = source_input.clone();
            *target_started = *source_started;
            true
        }
        (
            NodeState::Update {
                switched: target_switched,
                last_base: target_base,
                last_update: target_update,
            },
            NodeState::Update {
                switched: source_switched,
                last_base: source_base,
                last_update: source_update,
            },
        ) => {
            *target_switched = *source_switched;
            *target_base = source_base.clone();
            *target_update = source_update.clone();
            true
        }
        (NodeState::Latch { last_value: target }, NodeState::Latch { last_value: source }) => {
            *target = source.clone();
            true
        }
        (
            NodeState::CallLift {
                last_function: target_function,
                last_arguments: target_arguments,
                active_function: target_active,
                callable: target_callable,
            },
            NodeState::CallLift {
                last_function: source_function,
                last_arguments: source_arguments,
                active_function: source_active,
                callable: source_callable,
            },
        ) if target_arguments.len() == source_arguments.len() => {
            *target_function = source_function.clone();
            target_arguments.clone_from(source_arguments);
            *target_active = source_active.clone();
            *target_callable = source_callable.clone();
            true
        }
        (
            NodeState::Function {
                function: target_function,
                captures: target_captures,
            },
            NodeState::Function {
                function: source_function,
                captures: source_captures,
            },
        ) => {
            let (
                StreamOp::Function { func: target_func },
                StreamOp::Function { func: source_func },
            ) = (target_op, source_op)
            else {
                return false;
            };
            let mut captures = target_captures.borrow_mut();
            if !remap_slots(
                &mut captures,
                &source_captures.borrow(),
                &target_func.capture_slots,
                &source_func.capture_slots,
                target_layout,
                source_layout,
            ) {
                return false;
            }
            *target_function = source_function.clone();
            true
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
        ) if target_environment.len() == source_environment.len()
            && target_arguments.len() == source_arguments.len() =>
        {
            if !target_evaluator.transfer_from(source_evaluator) {
                return false;
            }
            if !remap_environment_values(
                target_environment,
                source_environment,
                &target_evaluator.program.environment_layout,
                &source_evaluator.program.environment_layout,
            ) {
                return false;
            }
            target_arguments.clone_from(source_arguments);
            true
        }
        (NodeState::Dynamic(target), NodeState::Dynamic(source)) => {
            let (StreamOp::Dynamic(target_spec), StreamOp::Dynamic(source_spec)) =
                (target_op, source_op)
            else {
                return false;
            };
            transfer_dynamic_state(
                target,
                source,
                target_spec,
                source_spec,
                target_layout,
                source_layout,
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
            if !transfer_stream_state(
                &mut target.then_state,
                &source.then_state,
                target_then,
                source_then,
                target_layout,
                source_layout,
            ) || !transfer_stream_state(
                &mut target.else_state,
                &source.else_state,
                target_else,
                source_else,
                target_layout,
                source_layout,
            ) {
                return false;
            }
            target.last_condition = source.last_condition.clone();
            target.last_then_value = source.last_then_value.clone();
            target.last_else_value = source.last_else_value.clone();
            true
        }
        _ => false,
    }
}

fn remap_environment_values(
    target: &mut Vec<Value>,
    source: &[Value],
    target_layout: &Rc<EnvironmentLayout>,
    source_layout: &Rc<EnvironmentLayout>,
) -> bool {
    target.resize(target_layout.len(), Value::NoVal);
    for target_slot in 0..target_layout.len() {
        let target_slot = EnvironmentSlot::new(target_slot);
        let Some(variable) = target_layout.variable(target_slot) else {
            return false;
        };
        let Some(source_slot) = source_layout.slot(variable) else {
            continue;
        };
        let Some(value) = source.get(source_slot.index()) else {
            return false;
        };
        target[target_slot.index()] = value.clone();
    }
    true
}

fn remap_slots(
    target: &mut Vec<Value>,
    source: &[Value],
    target_slots: &[EnvironmentSlot],
    source_slots: &[EnvironmentSlot],
    target_layout: &Rc<EnvironmentLayout>,
    source_layout: &Rc<EnvironmentLayout>,
) -> bool {
    if target_slots.len() != source_slots.len() || target.len() != target_slots.len() {
        return false;
    }
    let mut remapped = vec![Value::NoVal; target.len()];
    for (index, target_slot) in target_slots.iter().copied().enumerate() {
        let Some(variable) = target_layout.variable(target_slot) else {
            return false;
        };
        let Some(source_slot) = source_slots
            .iter()
            .copied()
            .find(|slot| source_layout.variable(*slot) == Some(variable))
        else {
            return false;
        };
        let Some(source_index) = source_slots.iter().position(|slot| *slot == source_slot) else {
            return false;
        };
        remapped[index] = source.get(source_index).cloned().unwrap_or(Value::NoVal);
    }
    target.clone_from(&remapped);
    true
}

fn transfer_dynamic_state(
    target: &mut DynamicExpressionState,
    source: &DynamicExpressionState,
    target_spec: &BoundDynamicExpressionSpec,
    source_spec: &BoundDynamicExpressionSpec,
    target_layout: &Rc<EnvironmentLayout>,
    source_layout: &Rc<EnvironmentLayout>,
) -> bool {
    if target_spec.mode != source_spec.mode {
        return false;
    }
    let Some(source_active) = source.active_expression.as_ref() else {
        return true;
    };

    let mut candidate = DynamicExpressionState::default();
    if update_active_expression_with_change(
        source_active.source_text.clone(),
        target_spec,
        &mut candidate,
        target_layout,
    )
    .is_err()
    {
        return false;
    }
    let Some(target_active) = candidate.active_expression.as_mut() else {
        return false;
    };
    if !graphs_semantically_equal(
        &target_active.template.program.graph,
        &target_active.template.program.environment_layout,
        &source_active.template.program.graph,
        &source_active.template.program.environment_layout,
    ) || !target_active
        .evaluator
        .transfer_from(&source_active.evaluator)
    {
        return false;
    }

    candidate
        .environment_values
        .resize(target_layout.len(), Value::NoVal);
    for target_slot in target_active.environment_slots.iter().copied() {
        let Some(variable) = target_layout.variable(target_slot) else {
            return false;
        };
        let Some(source_slot) = source_layout.slot(variable) else {
            return false;
        };
        let Some(value) = source.environment_values.get(source_slot.index()) else {
            return false;
        };
        candidate.environment_values[target_slot.index()] = value.clone();
    }
    candidate.last_source_value = source.last_source_value.clone();
    candidate.last_defer_result = source.last_defer_result.clone();
    *target = candidate;
    true
}

impl NodeState {
    fn for_op(op: &BoundOp) -> Self {
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
            StreamOp::Delay { offset, .. } => Self::Delay(DelayState::new(
                usize::try_from(*offset).expect("sindex offset does not fit usize"),
            )),
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
                evaluator: StreamEvaluator::new(Rc::clone(&func.program)),
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
                then_state: Box::new(StreamState::new_for_nodes(&then_branch.nodes)),
                else_state: Box::new(StreamState::new_for_nodes(&else_branch.nodes)),
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
