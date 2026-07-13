use super::super::plan::*;
use super::super::*;
use super::dynamic::*;
use super::functions::*;
use super::plan_executor::*;
use super::state::*;
use super::value_ops::*;

/// Evaluates one node at one logical tick and is shared by every execution mode.
pub(in crate::dataflow) fn eval_node_at(
    node_id: NodeId,
    op: &BoundOp,
    state: &mut DataflowState,
    tick: usize,
    context: EvalContext<'_>,
) -> Value {
    match op {
        BoundOp::Unary { op, arg } => {
            let arg = context.read(state, arg, tick);
            let NodeState::UnaryLift { last } = &mut state.states[node_id.index()] else {
                unreachable!("unary node has incompatible runtime state")
            };
            lift_unary_with_state(*op, arg, last)
        }
        BoundOp::Binary { op, lhs, rhs } => {
            let lhs = context.read(state, lhs, tick);
            let rhs = context.read(state, rhs, tick);
            let NodeState::BinaryLift { lhs_last, rhs_last } = &mut state.states[node_id.index()]
            else {
                unreachable!("binary node has incompatible runtime state")
            };
            lift_binary_with_state(*op, lhs, rhs, lhs_last, rhs_last)
        }
        BoundOp::If { .. } => eval_lazy_if(node_id, op, state, tick, context),
        BoundOp::SIndex { input, offset } => {
            let current = context.read(state, input, tick);
            let NodeState::Delay(history) = &mut state.states[node_id.index()] else {
                unreachable!("delay node has incompatible runtime state")
            };
            if *offset == 0 {
                history.lift_current(current)
            } else {
                history.read_and_push(current)
            }
        }
        BoundOp::RecursiveSIndex { .. } => {
            let NodeState::Delay(history) = &mut state.states[node_id.index()] else {
                unreachable!("recursive delay node has incompatible runtime state")
            };
            history.read()
        }
        BoundOp::Default { input, fallback } => {
            let input = context.read(state, input, tick);
            let NodeState::Default { last } = &mut state.states[node_id.index()] else {
                unreachable!("default node has incompatible runtime state")
            };
            let input = stream_lift_value(input, last);
            if input == Value::Deferred {
                context.read(state, fallback, tick)
            } else {
                input
            }
        }
        BoundOp::Init { input, initial } => {
            let input = context.read(state, input, tick);
            let NodeState::Init { started } = &mut state.states[node_id.index()] else {
                unreachable!("init node has incompatible runtime state")
            };
            if *started {
                input
            } else if input == Value::NoVal {
                context.read(state, initial, tick)
            } else {
                *started = true;
                input
            }
        }
        BoundOp::IsDefined { input } => {
            let input = context.read(state, input, tick);
            let NodeState::IsDefined { last } = &mut state.states[node_id.index()] else {
                unreachable!("is_defined node has incompatible runtime state")
            };
            Value::Bool(stream_lift_value(input, last) != Value::Deferred)
        }
        BoundOp::When { input } => {
            let input = context.read(state, input, tick);
            let NodeState::When { last, started } = &mut state.states[node_id.index()] else {
                unreachable!("when node has incompatible runtime state")
            };
            let input = stream_lift_value(input, last);
            if *started {
                Value::Bool(true)
            } else if input == Value::Deferred || input == Value::NoVal {
                Value::Bool(false)
            } else {
                *started = true;
                Value::Bool(true)
            }
        }
        BoundOp::Update { base, update } => {
            let base = context.read(state, base, tick);
            let update = context.read(state, update, tick);
            let NodeState::Update {
                switched,
                base_last,
                update_last,
            } = &mut state.states[node_id.index()]
            else {
                unreachable!("update node has incompatible runtime state")
            };
            let base = stream_lift_value(base, base_last);
            let update = stream_lift_value(update, update_last);
            if *switched {
                update
            } else if update == Value::Deferred || update == Value::NoVal {
                base
            } else {
                *switched = true;
                update
            }
        }
        BoundOp::Latch { value, trigger } => {
            let value = context.read(state, value, tick);
            let trigger = context.read(state, trigger, tick);
            let NodeState::Latch { value_last } = &mut state.states[node_id.index()] else {
                unreachable!("latch node has incompatible runtime state")
            };
            let value = stream_lift_value(value, value_last);
            if trigger == Value::NoVal {
                Value::NoVal
            } else {
                value
            }
        }
        // Collection and aggregate values.
        BoundOp::List(items) => {
            let values = items
                .iter()
                .map(|item| context.read(state, item, tick))
                .collect::<Vec<_>>();
            let values = lift_value_operands(node_id, state, values);
            lift_many(values, |values| Value::List(EcoVec::from(values)))
        }
        BoundOp::Tuple(items) => {
            let values = items
                .iter()
                .map(|item| context.read(state, item, tick))
                .collect::<Vec<_>>();
            let values = lift_value_operands(node_id, state, values);
            lift_many(values, |values| Value::Tuple(EcoVec::from(values)))
        }
        BoundOp::Map(items) => {
            let keys = items.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
            let values = items
                .iter()
                .map(|(_, value)| context.read(state, value, tick))
                .collect::<Vec<_>>();
            let values = lift_value_operands(node_id, state, values);
            lift_map_values(keys.into_iter().zip(values).collect(), Value::Map)
        }
        BoundOp::LIndex { list, index: idx } => {
            let values = vec![
                context.read(state, list, tick),
                context.read(state, idx, tick),
            ];
            let mut values = lift_value_operands(node_id, state, values).into_iter();
            lift_two(values.next().unwrap(), values.next().unwrap(), eval_lindex)
        }
        BoundOp::LAppend { list, value } => {
            let values = vec![
                context.read(state, list, tick),
                context.read(state, value, tick),
            ];
            let mut values = lift_value_operands(node_id, state, values).into_iter();
            lift_two(values.next().unwrap(), values.next().unwrap(), eval_lappend)
        }
        BoundOp::LConcat { lhs, rhs } => {
            let values = vec![
                context.read(state, lhs, tick),
                context.read(state, rhs, tick),
            ];
            let mut values = lift_value_operands(node_id, state, values).into_iter();
            lift_two(values.next().unwrap(), values.next().unwrap(), eval_lconcat)
        }
        BoundOp::LHead { list } => {
            let values = vec![context.read(state, list, tick)];
            lift_one(
                lift_value_operands(node_id, state, values).remove(0),
                eval_lhead,
            )
        }
        BoundOp::LTail { list } => {
            let values = vec![context.read(state, list, tick)];
            lift_one(
                lift_value_operands(node_id, state, values).remove(0),
                eval_ltail,
            )
        }
        BoundOp::LLen { list } => {
            let values = vec![context.read(state, list, tick)];
            lift_one(
                lift_value_operands(node_id, state, values).remove(0),
                eval_llen,
            )
        }
        BoundOp::MGet { map, key } => {
            let values = vec![context.read(state, map, tick)];
            lift_one(
                lift_value_operands(node_id, state, values).remove(0),
                |map| eval_mget(map, key),
            )
        }
        BoundOp::MRemove { map, key } => {
            let values = vec![context.read(state, map, tick)];
            lift_one(
                lift_value_operands(node_id, state, values).remove(0),
                |map| eval_mremove(map, key),
            )
        }
        BoundOp::MInsert { map, key, value } => {
            let values = vec![
                context.read(state, map, tick),
                context.read(state, value, tick),
            ];
            let mut values = lift_value_operands(node_id, state, values).into_iter();
            lift_two(
                values.next().unwrap(),
                values.next().unwrap(),
                |map, value| eval_minsert(map, key, value),
            )
        }
        BoundOp::MHasKey { map, key } => {
            let values = vec![context.read(state, map, tick)];
            lift_one(
                lift_value_operands(node_id, state, values).remove(0),
                |map| eval_mhas_key(map, key),
            )
        }
        BoundOp::TGet { tuple, index: idx } => {
            let values = vec![context.read(state, tuple, tick)];
            lift_one(
                lift_value_operands(node_id, state, values).remove(0),
                |tuple| eval_tget(tuple, *idx),
            )
        }

        // Function construction and application.
        BoundOp::Function { func } => eval_function_op(func, tick, context),
        BoundOp::Apply { func, args } => {
            let func = context.read(state, func, tick);
            let args = args
                .iter()
                .map(|arg| context.read(state, arg, tick))
                .collect::<Vec<_>>();
            let NodeState::CallLift {
                func_last,
                arg_last,
            } = &mut state.states[node_id.index()]
            else {
                unreachable!("apply node has incompatible runtime state")
            };
            let func = stream_lift_value(func, func_last);
            let args = lift_call_args(args, arg_last);
            eval_apply_op(func, args)
        }
        BoundOp::DirectFixApply { func, args } => {
            let args = args
                .iter()
                .map(|arg| context.read(state, arg, tick))
                .collect::<Vec<_>>();
            let NodeState::CallLift { arg_last, .. } = &mut state.states[node_id.index()] else {
                unreachable!("direct fix application node has incompatible runtime state")
            };
            let args = lift_call_args(args, arg_last);
            eval_direct_fix_apply_op(func, args, tick, context)
        }
        BoundOp::RecursiveCall { args } => {
            let args = args
                .iter()
                .map(|arg| context.read(state, arg, tick))
                .collect::<Vec<_>>();
            let NodeState::CallLift { arg_last, .. } = &mut state.states[node_id.index()] else {
                unreachable!("recursive call node has incompatible runtime state")
            };
            let args = lift_call_args(args, arg_last);
            eval_recursive_call_op(args, context)
        }
        BoundOp::Partial {
            func,
            args,
            display,
        } => {
            let func = context.read(state, func, tick);
            let args = args
                .iter()
                .map(|arg| context.read(state, arg, tick))
                .collect::<Vec<_>>();
            let NodeState::CallLift {
                func_last,
                arg_last,
            } = &mut state.states[node_id.index()]
            else {
                unreachable!("partial application node has incompatible runtime state")
            };
            let func = stream_lift_value(func, func_last);
            let args = lift_call_args(args, arg_last);
            eval_partial_op(func, args, display.clone())
        }
        BoundOp::Fix { func, display } => {
            let values = vec![context.read(state, func, tick)];
            eval_fix_op(
                lift_value_operands(node_id, state, values).remove(0),
                display.clone(),
            )
        }
        BoundOp::ListMap { func, list } => {
            let values = vec![
                context.read(state, func, tick),
                context.read(state, list, tick),
            ];
            let mut values = lift_value_operands(node_id, state, values).into_iter();
            eval_list_map_op(values.next().unwrap(), values.next().unwrap())
        }
        BoundOp::ListFilter { func, list } => {
            let values = vec![
                context.read(state, func, tick),
                context.read(state, list, tick),
            ];
            let mut values = lift_value_operands(node_id, state, values).into_iter();
            eval_list_filter_op(values.next().unwrap(), values.next().unwrap())
        }
        BoundOp::ListFold { func, init, list } => {
            let values = vec![
                context.read(state, func, tick),
                context.read(state, init, tick),
                context.read(state, list, tick),
            ];
            let mut values = lift_value_operands(node_id, state, values).into_iter();
            eval_list_fold_op(
                values.next().unwrap(),
                values.next().unwrap(),
                values.next().unwrap(),
            )
        }

        // Runtime-compiled expressions use the fallible traversal.
        BoundOp::Dynamic(_) => unreachable!("dynamic node reached infallible evaluator"),
    }
}

pub(in crate::dataflow) fn eval_nodes_at(
    nodes: &[BoundOp],
    state: &mut DataflowState,
    tick: usize,
    context: EvalContext<'_>,
) {
    for (index, op) in nodes.iter().enumerate() {
        let node_id = NodeId::new(index);
        let value = eval_node_at(node_id, op, state, tick, context);
        state.nodes[index] = value;
    }
}

fn eval_lazy_if(
    node_id: NodeId,
    op: &BoundOp,
    state: &mut DataflowState,
    tick: usize,
    context: EvalContext<'_>,
) -> Value {
    let BoundOp::If {
        cond,
        then_branch,
        else_branch,
    } = op
    else {
        unreachable!("non-if node evaluated as lazy branch")
    };
    let condition = context.read(state, cond, tick);
    let NodeState::LazyIf(lazy_if) = &mut state.states[node_id.index()] else {
        unreachable!("if node has incompatible runtime state")
    };
    let condition = stream_lift_value(condition, &mut lazy_if.condition_last);
    if context.recursive_call.is_some() {
        return match condition {
            Value::Bool(true) => eval_branch(then_branch, &mut lazy_if.then_state, tick, context),
            Value::Bool(false) => eval_branch(else_branch, &mut lazy_if.else_state, tick, context),
            Value::Deferred => Value::Deferred,
            Value::NoVal => Value::NoVal,
            other => panic!("if condition must be bool, got {:?}", other),
        };
    }
    let then_value = eval_branch(then_branch, &mut lazy_if.then_state, tick, context);
    let then_value = stream_lift_value(then_value, &mut lazy_if.then_last);
    let else_value = eval_branch(else_branch, &mut lazy_if.else_state, tick, context);
    let else_value = stream_lift_value(else_value, &mut lazy_if.else_last);

    if then_value == Value::NoVal || else_value == Value::NoVal {
        return Value::NoVal;
    }

    match condition {
        Value::Bool(true) => then_value,
        Value::Bool(false) => else_value,
        Value::Deferred => Value::Deferred,
        Value::NoVal => Value::NoVal,
        other => panic!("if condition must be bool, got {:?}", other),
    }
}

fn eval_branch(
    branch: &BoundPlanBody,
    state: &mut DataflowState,
    tick: usize,
    context: EvalContext<'_>,
) -> Value {
    eval_nodes_at(&branch.nodes, state, tick, context);
    let output = context.read(state, &branch.output, tick);
    commit_recursive_delays(&branch.recursive_delays, state, &output);
    output
}

pub(in crate::dataflow) fn try_eval_nodes_at(
    nodes: &[BoundOp],
    state: &mut DataflowState,
    tick: usize,
    context: EvalContext<'_>,
) -> Result<(), DataflowEvalError> {
    for (index, op) in nodes.iter().enumerate() {
        let node_id = NodeId::new(index);
        let value = match op {
            BoundOp::Dynamic(spec) => {
                let current = context.read(state, &spec.input, tick);
                let NodeState::Dynamic(dynamic) = &mut state.states[index] else {
                    unreachable!("dynamic node has incompatible runtime state")
                };
                if dynamic.environment_last.len() != context.inputs.len() {
                    dynamic.environment_last = vec![None; context.inputs.len()];
                }
                dynamic.environment_values = context
                    .inputs
                    .iter()
                    .cloned()
                    .zip(&mut dynamic.environment_last)
                    .map(|(value, last)| stream_lift_value(value, last))
                    .collect();
                let current = stream_lift_value(current, &mut dynamic.source_last);
                let result = eval_dynamic_value(current, spec, dynamic, tick, context)?;
                stream_lift_value(result, &mut dynamic.result_last)
            }
            BoundOp::If { .. } => try_eval_lazy_if(node_id, op, state, tick, context)?,
            _ => eval_node_at(node_id, op, state, tick, context),
        };
        state.nodes[index] = value;
    }
    Ok(())
}

fn try_eval_lazy_if(
    node_id: NodeId,
    op: &BoundOp,
    state: &mut DataflowState,
    tick: usize,
    context: EvalContext<'_>,
) -> Result<Value, DataflowEvalError> {
    let BoundOp::If {
        cond,
        then_branch,
        else_branch,
    } = op
    else {
        unreachable!("non-if node evaluated as lazy branch")
    };
    let condition = context.read(state, cond, tick);
    let NodeState::LazyIf(lazy_if) = &mut state.states[node_id.index()] else {
        unreachable!("if node has incompatible runtime state")
    };
    let condition = stream_lift_value(condition, &mut lazy_if.condition_last);

    match condition {
        Value::Bool(true) => {
            let selected = try_eval_branch(then_branch, &mut lazy_if.then_state, tick, context);
            let unselected = try_eval_branch(else_branch, &mut lazy_if.else_state, tick, context);
            let selected = selected.map(|value| stream_lift_value(value, &mut lazy_if.then_last));
            let unselected =
                unselected.map(|value| stream_lift_value(value, &mut lazy_if.else_last));
            let selected = selected?;
            if selected == Value::NoVal || matches!(unselected, Ok(Value::NoVal)) {
                Ok(Value::NoVal)
            } else {
                Ok(selected)
            }
        }
        Value::Bool(false) => {
            let unselected = try_eval_branch(then_branch, &mut lazy_if.then_state, tick, context);
            let selected = try_eval_branch(else_branch, &mut lazy_if.else_state, tick, context);
            let unselected =
                unselected.map(|value| stream_lift_value(value, &mut lazy_if.then_last));
            let selected =
                selected.map(|value| stream_lift_value(value, &mut lazy_if.else_last))?;
            if selected == Value::NoVal || matches!(unselected, Ok(Value::NoVal)) {
                Ok(Value::NoVal)
            } else {
                Ok(selected)
            }
        }
        Value::Deferred => {
            let then_value = try_eval_branch(then_branch, &mut lazy_if.then_state, tick, context);
            let else_value = try_eval_branch(else_branch, &mut lazy_if.else_state, tick, context);
            let then_value =
                then_value.map(|value| stream_lift_value(value, &mut lazy_if.then_last));
            let else_value =
                else_value.map(|value| stream_lift_value(value, &mut lazy_if.else_last));
            if matches!(then_value, Ok(Value::NoVal)) || matches!(else_value, Ok(Value::NoVal)) {
                Ok(Value::NoVal)
            } else {
                Ok(Value::Deferred)
            }
        }
        Value::NoVal => {
            let then_value = try_eval_branch(then_branch, &mut lazy_if.then_state, tick, context);
            let else_value = try_eval_branch(else_branch, &mut lazy_if.else_state, tick, context);
            if let Ok(value) = then_value {
                stream_lift_value(value, &mut lazy_if.then_last);
            }
            if let Ok(value) = else_value {
                stream_lift_value(value, &mut lazy_if.else_last);
            }
            Ok(Value::NoVal)
        }
        other => panic!("if condition must be bool, got {:?}", other),
    }
}

fn try_eval_branch(
    branch: &BoundPlanBody,
    state: &mut DataflowState,
    tick: usize,
    context: EvalContext<'_>,
) -> Result<Value, DataflowEvalError> {
    try_eval_nodes_at(&branch.nodes, state, tick, context)?;
    let output = context.read(state, &branch.output, tick);
    commit_recursive_delays(&branch.recursive_delays, state, &output);
    Ok(output)
}

pub(in crate::dataflow) fn commit_recursive_delays(
    delays: &[NodeId],
    state: &mut DataflowState,
    output: &Value,
) {
    for delay in delays {
        let NodeState::Delay(history) = &mut state.states[delay.index()] else {
            unreachable!("recursive delay node has incompatible runtime state")
        };
        history.push(output.clone());
    }
}

fn lift_call_args(mut args: Vec<Value>, last: &mut [Option<Value>]) -> EcoVec<Value> {
    debug_assert_eq!(args.len(), last.len());
    for (arg, last) in args.iter_mut().zip(last) {
        *arg = stream_lift_value(arg.clone(), last);
    }
    args.into()
}

fn lift_value_operands(
    node_id: NodeId,
    state: &mut DataflowState,
    mut values: Vec<Value>,
) -> Vec<Value> {
    let NodeState::OperandLift { last } = &mut state.states[node_id.index()] else {
        unreachable!("lifted value node has incompatible runtime state")
    };
    debug_assert_eq!(values.len(), last.len());
    for (value, last) in values.iter_mut().zip(last) {
        *value = stream_lift_value(value.clone(), last);
    }
    values
}
