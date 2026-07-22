use super::super::plan::*;
use super::super::*;
use super::plan_executor::PlanExecutor;
use crate::core::{RuntimeFunction, RuntimeFunctionValueCallable};
use std::{cell::RefCell, rc::Rc};

#[derive(Clone)]
pub(in crate::dataflow) struct DataflowState {
    pub(in crate::dataflow) nodes: Vec<Value>,
    pub(in crate::dataflow) states: Vec<NodeState>,
}

#[derive(Clone)]
pub(in crate::dataflow) enum NodeState {
    UnaryLift {
        last: Option<Value>,
    },
    BinaryLift {
        lhs_last: Option<Value>,
        rhs_last: Option<Value>,
    },
    OperandLift {
        last: Vec<Option<Value>>,
    },
    Delay(SIndexValueHistory),
    Default {
        last: Option<Value>,
    },
    Init {
        started: bool,
    },
    IsDefined {
        last: Option<Value>,
    },
    When {
        last: Option<Value>,
        started: bool,
    },
    Update {
        switched: bool,
        base_last: Option<Value>,
        update_last: Option<Value>,
    },
    Latch {
        value_last: Option<Value>,
    },
    CallLift {
        func_last: Option<Value>,
        arg_last: Vec<Option<Value>>,
        active_function: Option<RuntimeFunction>,
        callable: Option<RuntimeFunctionValueCallable>,
    },
    Function {
        function: Option<RuntimeFunction>,
        captures: Rc<RefCell<Vec<Value>>>,
    },
    PersistentCall {
        executor: PlanExecutor,
        values: Vec<Value>,
        arg_last: Vec<Option<Value>>,
    },
    Dynamic(DynamicState),
    LazyIf(LazyIfState),
}

#[derive(Clone)]
pub(in crate::dataflow) struct LazyIfState {
    pub(in crate::dataflow) then_state: Box<DataflowState>,
    pub(in crate::dataflow) else_state: Box<DataflowState>,
    pub(in crate::dataflow) condition_last: Option<Value>,
    pub(in crate::dataflow) then_last: Option<Value>,
    pub(in crate::dataflow) else_last: Option<Value>,
}

impl LazyIfState {
    fn reset(&mut self) {
        self.then_state.reset_for_reuse();
        self.else_state.reset_for_reuse();
        self.condition_last = None;
        self.then_last = None;
        self.else_last = None;
    }
}

#[derive(Clone, Default)]
pub(in crate::dataflow) struct DynamicState {
    pub(in crate::dataflow) active: Option<ActiveDynamic>,
    pub(in crate::dataflow) source_last: Option<Value>,
    pub(in crate::dataflow) result_last: Option<Value>,
    pub(in crate::dataflow) environment_last: Vec<Option<Value>>,
    pub(in crate::dataflow) environment_values: Vec<Value>,
}

#[derive(Clone)]
pub(in crate::dataflow) struct ActiveDynamic {
    pub(in crate::dataflow) source: EcoString,
    pub(in crate::dataflow) executor: PlanExecutor,
    pub(in crate::dataflow) dependencies: Vec<EnvironmentId>,
}

#[derive(Clone)]
pub(in crate::dataflow) struct SIndexValueHistory {
    values: Vec<Value>,
    next: usize,
    filled: usize,
    output_last: Option<Value>,
    pending: bool,
}

impl SIndexValueHistory {
    pub(in crate::dataflow) fn new(offset: usize) -> Self {
        Self {
            values: vec![Value::NoVal; offset],
            next: 0,
            filled: 0,
            output_last: None,
            pending: false,
        }
    }

    pub(in crate::dataflow) fn read(&self) -> Value {
        if self.values.is_empty() || self.filled < self.values.len() {
            Value::Deferred
        } else {
            self.values[self.next].clone()
        }
    }

    pub(in crate::dataflow) fn push(&mut self, value: Value) {
        if self.values.is_empty() {
            return;
        }
        self.values[self.next] = value;
        self.next = (self.next + 1) % self.values.len();
        self.filled = self.filled.saturating_add(1).min(self.values.len());
    }

    pub(in crate::dataflow) fn read_and_stage(&mut self) -> Value {
        debug_assert!(
            !self.pending,
            "delay was evaluated more than once before commit"
        );
        self.pending = true;
        let previous = self.read();
        super::value_evaluation::stream_lift_value(previous, &mut self.output_last)
    }

    pub(in crate::dataflow) fn commit_pending(&mut self, value: Value) {
        if self.pending {
            self.pending = false;
            self.push(value);
        }
    }

    pub(in crate::dataflow) fn lift_current(&mut self, value: Value) -> Value {
        super::value_evaluation::stream_lift_value(value, &mut self.output_last)
    }

    pub(in crate::dataflow) fn reset(&mut self) {
        self.next = 0;
        self.filled = 0;
        self.output_last = None;
        self.pending = false;
    }
}

impl DataflowState {
    pub(in crate::dataflow) fn new(body: &BoundPlanBody) -> Self {
        Self::new_for_nodes(&body.nodes)
    }

    fn new_for_nodes(nodes: &[BoundOp]) -> Self {
        Self {
            nodes: vec![Value::NoVal; nodes.len()],
            states: nodes.iter().map(NodeState::for_op).collect(),
        }
    }

    pub(in crate::dataflow) fn reset_for_reuse(&mut self) {
        for node in &mut self.nodes {
            *node = Value::NoVal;
        }
        for state in &mut self.states {
            state.reset();
        }
    }
}

impl NodeState {
    fn for_op(op: &BoundOp) -> Self {
        match op {
            BoundOp::Unary { .. } => Self::UnaryLift { last: None },
            BoundOp::Binary { .. } => Self::BinaryLift {
                lhs_last: None,
                rhs_last: None,
            },
            BoundOp::List(items) | BoundOp::Tuple(items) => Self::OperandLift {
                last: vec![None; items.len()],
            },
            BoundOp::Map(items) => Self::OperandLift {
                last: vec![None; items.len()],
            },
            BoundOp::LIndex { .. }
            | BoundOp::LAppend { .. }
            | BoundOp::LConcat { .. }
            | BoundOp::MInsert { .. }
            | BoundOp::ListMap { .. }
            | BoundOp::ListFilter { .. } => Self::OperandLift {
                last: vec![None; 2],
            },
            BoundOp::LHead { .. }
            | BoundOp::LTail { .. }
            | BoundOp::LLen { .. }
            | BoundOp::MGet { .. }
            | BoundOp::MRemove { .. }
            | BoundOp::MHasKey { .. }
            | BoundOp::TGet { .. }
            | BoundOp::Fix { .. } => Self::OperandLift {
                last: vec![None; 1],
            },
            BoundOp::ListFold { .. } => Self::OperandLift {
                last: vec![None; 3],
            },
            BoundOp::SIndex { offset, .. } => Self::Delay(SIndexValueHistory::new(
                usize::try_from(*offset).expect("sindex offset does not fit usize"),
            )),
            BoundOp::RecursiveSIndex { offset } => Self::Delay(SIndexValueHistory::new(
                usize::try_from(offset.get()).expect("sindex offset does not fit usize"),
            )),
            BoundOp::Default { .. } => Self::Default { last: None },
            BoundOp::Init { .. } => Self::Init { started: false },
            BoundOp::IsDefined { .. } => Self::IsDefined { last: None },
            BoundOp::When { .. } => Self::When {
                last: None,
                started: false,
            },
            BoundOp::Update { .. } => Self::Update {
                switched: false,
                base_last: None,
                update_last: None,
            },
            BoundOp::Latch { .. } => Self::Latch { value_last: None },
            BoundOp::Apply { args, .. } | BoundOp::Partial { args, .. } => Self::CallLift {
                func_last: None,
                arg_last: vec![None; args.len()],
                active_function: None,
                callable: None,
            },
            BoundOp::Function { func } => Self::Function {
                function: None,
                captures: Rc::new(RefCell::new(vec![Value::NoVal; func.capture_sources.len()])),
            },
            BoundOp::DirectApply { func, args } => Self::PersistentCall {
                executor: PlanExecutor::new(Rc::clone(&func.plan)),
                values: vec![Value::NoVal; func.capture_sources.len() + func.params.len()],
                arg_last: vec![None; args.len()],
            },
            BoundOp::DirectFixApply { args, .. } | BoundOp::RecursiveCall { args } => {
                Self::CallLift {
                    func_last: None,
                    arg_last: vec![None; args.len()],
                    active_function: None,
                    callable: None,
                }
            }
            BoundOp::Dynamic(_) => Self::Dynamic(DynamicState::default()),
            BoundOp::If {
                then_branch,
                else_branch,
                ..
            } => Self::LazyIf(LazyIfState {
                then_state: Box::new(DataflowState::new_for_nodes(&then_branch.nodes)),
                else_state: Box::new(DataflowState::new_for_nodes(&else_branch.nodes)),
                condition_last: None,
                then_last: None,
                else_last: None,
            }),
        }
    }

    fn reset(&mut self) {
        match self {
            Self::UnaryLift { last } | Self::Default { last } | Self::IsDefined { last } => {
                *last = None
            }
            Self::BinaryLift { lhs_last, rhs_last } => {
                *lhs_last = None;
                *rhs_last = None;
            }
            Self::OperandLift { last } => last.fill(None),
            Self::Delay(history) => history.reset(),
            Self::Init { started } => *started = false,
            Self::When { last, started } => {
                *last = None;
                *started = false;
            }
            Self::Update {
                switched,
                base_last,
                update_last,
            } => {
                *switched = false;
                *base_last = None;
                *update_last = None;
            }
            Self::Latch { value_last } => *value_last = None,
            Self::CallLift {
                func_last,
                arg_last,
                active_function,
                callable,
            } => {
                *func_last = None;
                arg_last.fill(None);
                *active_function = None;
                *callable = None;
            }
            Self::Function { function, captures } => {
                *function = None;
                captures.borrow_mut().fill(Value::NoVal);
            }
            Self::PersistentCall {
                executor,
                values,
                arg_last,
            } => {
                executor.reset_state();
                values.fill(Value::NoVal);
                arg_last.fill(None);
            }
            Self::Dynamic(dynamic) => *dynamic = DynamicState::default(),
            Self::LazyIf(lazy_if) => lazy_if.reset(),
        }
    }
}
