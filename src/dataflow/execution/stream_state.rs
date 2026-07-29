use super::super::ir::*;
use super::super::*;
use super::stream_evaluator::StreamEvaluator;
use crate::core::{RuntimeFunction, RuntimeFunctionValueCallable};
use std::{cell::RefCell, rc::Rc};

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
    Default {
        last_input: Option<Value>,
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

#[derive(Clone, Default)]
pub(in crate::dataflow) struct DynamicExpressionState {
    pub(in crate::dataflow) active_expression: Option<ActiveExpression>,
    pub(in crate::dataflow) last_source_value: Option<Value>,
    pub(in crate::dataflow) last_result: Option<Value>,
    pub(in crate::dataflow) environment_values: Vec<Value>,
}

impl DynamicExpressionState {
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
pub(in crate::dataflow) struct ActiveExpression {
    pub(in crate::dataflow) source_text: EcoString,
    pub(in crate::dataflow) evaluator: StreamEvaluator,
    pub(in crate::dataflow) dependency_slots: Vec<EnvironmentSlot>,
    pub(in crate::dataflow) environment_slots: Vec<EnvironmentSlot>,
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
}

impl StreamState {
    pub(in crate::dataflow) fn new(body: &BoundEvaluationGraph) -> Self {
        Self::new_for_nodes(&body.nodes)
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
