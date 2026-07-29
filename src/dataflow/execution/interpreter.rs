use super::super::ir::*;
use super::super::*;
use super::dynamic_expressions::*;
use super::functions::*;
use super::lifting::*;
use super::stream_evaluator::*;
use super::stream_state::*;
use crate::core::values::operations as value_operations;

/// Evaluates one node and is shared by every execution mode.
pub(in crate::dataflow) fn evaluate_node(
    node_id: NodeId,
    op: &BoundOp,
    state: &mut StreamState,
    context: EvaluationContext<'_>,
) -> Value {
    match op {
        StreamOp::Unary { op, arg } => {
            let arg = context.read_value(state, arg);
            let NodeState::UnaryLift { last_input } = &mut state.node_states[node_id.index()]
            else {
                unreachable!("unary node has incompatible runtime state")
            };
            lift_unary_with_state(*op, arg, last_input)
        }
        StreamOp::Binary { op, lhs, rhs } => {
            let lhs = context.read_value(state, lhs);
            let rhs = context.read_value(state, rhs);
            let NodeState::BinaryLift {
                last_left,
                last_right,
            } = &mut state.node_states[node_id.index()]
            else {
                unreachable!("binary node has incompatible runtime state")
            };
            lift_binary_with_state(*op, lhs, rhs, last_left, last_right)
        }
        StreamOp::If { .. } => evaluate_lazy_if(node_id, op, state, context),
        StreamOp::Delay { input, offset } => {
            if *offset == 0 {
                let current = context.read_value(state, input);
                let NodeState::Delay(history) = &mut state.node_states[node_id.index()] else {
                    unreachable!("delay node has incompatible runtime state")
                };
                history.retain_current_value(current)
            } else {
                let NodeState::Delay(history) = &mut state.node_states[node_id.index()] else {
                    unreachable!("delay node has incompatible runtime state")
                };
                history.read_and_stage_write()
            }
        }
        StreamOp::RecursiveDelay { .. } => {
            let NodeState::Delay(history) = &mut state.node_states[node_id.index()] else {
                unreachable!("recursive delay node has incompatible runtime state")
            };
            history.read_delayed_value()
        }
        StreamOp::Default { input, fallback } => {
            let input = context.read_value(state, input);
            let NodeState::Default { last_input } = &mut state.node_states[node_id.index()] else {
                unreachable!("default node has incompatible runtime state")
            };
            let input = retain_last_value(input, last_input);
            if input == Value::Deferred {
                context.read_value(state, fallback)
            } else {
                input
            }
        }
        StreamOp::Init { input, initial } => {
            let input = context.read_value(state, input);
            let NodeState::Init { started } = &mut state.node_states[node_id.index()] else {
                unreachable!("init node has incompatible runtime state")
            };
            if *started {
                input
            } else if input == Value::NoVal {
                context.read_value(state, initial)
            } else {
                *started = true;
                input
            }
        }
        StreamOp::IsDefined { input } => {
            let input = context.read_value(state, input);
            let NodeState::IsDefined { last_input } = &mut state.node_states[node_id.index()]
            else {
                unreachable!("is_defined node has incompatible runtime state")
            };
            Value::Bool(retain_last_value(input, last_input) != Value::Deferred)
        }
        StreamOp::When { input } => {
            let input = context.read_value(state, input);
            let NodeState::When {
                last_input,
                started,
            } = &mut state.node_states[node_id.index()]
            else {
                unreachable!("when node has incompatible runtime state")
            };
            let input = retain_last_value(input, last_input);
            if *started {
                Value::Bool(true)
            } else if input == Value::Deferred || input == Value::NoVal {
                Value::Bool(false)
            } else {
                *started = true;
                Value::Bool(true)
            }
        }
        StreamOp::Update { base, update } => {
            let base = context.read_value(state, base);
            let update = context.read_value(state, update);
            let NodeState::Update {
                switched,
                last_base,
                last_update,
            } = &mut state.node_states[node_id.index()]
            else {
                unreachable!("update node has incompatible runtime state")
            };
            let base = retain_last_value(base, last_base);
            let update = retain_last_value(update, last_update);
            if *switched {
                update
            } else if update == Value::Deferred || update == Value::NoVal {
                base
            } else {
                *switched = true;
                update
            }
        }
        StreamOp::Latch { value, trigger } => {
            let value = context.read_value(state, value);
            let trigger = context.read_value(state, trigger);
            let NodeState::Latch { last_value } = &mut state.node_states[node_id.index()] else {
                unreachable!("latch node has incompatible runtime state")
            };
            let value = retain_last_value(value, last_value);
            if trigger == Value::NoVal {
                Value::NoVal
            } else {
                value
            }
        }
        // Collection and aggregate values.
        StreamOp::List(items) => {
            let values = items
                .iter()
                .map(|item| context.read_value(state, item))
                .collect::<Vec<_>>();
            let values = lift_value_operands(node_id, state, values);
            lift_many(values, |values| Value::List(EcoVec::from(values)))
        }
        StreamOp::Tuple(items) => {
            let values = items
                .iter()
                .map(|item| context.read_value(state, item))
                .collect::<Vec<_>>();
            let values = lift_value_operands(node_id, state, values);
            lift_many(values, |values| Value::Tuple(EcoVec::from(values)))
        }
        StreamOp::Map(items) => {
            let keys = items.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
            let values = items
                .iter()
                .map(|(_, value)| context.read_value(state, value))
                .collect::<Vec<_>>();
            let values = lift_value_operands(node_id, state, values);
            lift_map_values(keys.into_iter().zip(values).collect(), Value::Map)
        }
        StreamOp::LIndex { list, index: idx } => {
            let values = vec![
                context.read_value(state, list),
                context.read_value(state, idx),
            ];
            let mut values = lift_value_operands(node_id, state, values).into_iter();
            lift_two(
                values.next().unwrap(),
                values.next().unwrap(),
                |list, index| expect_value(value_operations::list_index(list, index)),
            )
        }
        StreamOp::LAppend { list, value } => {
            let values = vec![
                context.read_value(state, list),
                context.read_value(state, value),
            ];
            let mut values = lift_value_operands(node_id, state, values).into_iter();
            lift_two(
                values.next().unwrap(),
                values.next().unwrap(),
                |list, value| expect_value(value_operations::list_append(list, value)),
            )
        }
        StreamOp::LConcat { lhs, rhs } => {
            let values = vec![
                context.read_value(state, lhs),
                context.read_value(state, rhs),
            ];
            let mut values = lift_value_operands(node_id, state, values).into_iter();
            lift_two(
                values.next().unwrap(),
                values.next().unwrap(),
                |lhs, rhs| expect_value(value_operations::list_concat(lhs, rhs)),
            )
        }
        StreamOp::LHead { list } => {
            let values = vec![context.read_value(state, list)];
            lift_one(
                lift_value_operands(node_id, state, values).remove(0),
                |list| expect_value(value_operations::list_head(list)),
            )
        }
        StreamOp::LTail { list } => {
            let values = vec![context.read_value(state, list)];
            lift_one(
                lift_value_operands(node_id, state, values).remove(0),
                |list| expect_value(value_operations::list_tail(list)),
            )
        }
        StreamOp::LLen { list } => {
            let values = vec![context.read_value(state, list)];
            lift_one(
                lift_value_operands(node_id, state, values).remove(0),
                |list| expect_value(value_operations::list_len(list)),
            )
        }
        StreamOp::MGet { map, key } => {
            let values = vec![context.read_value(state, map)];
            lift_one(
                lift_value_operands(node_id, state, values).remove(0),
                |map| expect_value(value_operations::map_get(map, key)),
            )
        }
        StreamOp::MRemove { map, key } => {
            let values = vec![context.read_value(state, map)];
            lift_one(
                lift_value_operands(node_id, state, values).remove(0),
                |map| expect_value(value_operations::map_remove(map, key)),
            )
        }
        StreamOp::MInsert { map, key, value } => {
            let values = vec![
                context.read_value(state, map),
                context.read_value(state, value),
            ];
            let mut values = lift_value_operands(node_id, state, values).into_iter();
            lift_two(
                values.next().unwrap(),
                values.next().unwrap(),
                |map, value| expect_value(value_operations::map_insert(map, key, value)),
            )
        }
        StreamOp::MHasKey { map, key } => {
            let values = vec![context.read_value(state, map)];
            lift_one(
                lift_value_operands(node_id, state, values).remove(0),
                |map| expect_value(value_operations::map_has_key(map, key)),
            )
        }
        StreamOp::TGet { tuple, index: idx } => {
            let values = vec![context.read_value(state, tuple)];
            lift_one(
                lift_value_operands(node_id, state, values).remove(0),
                |tuple| expect_value(value_operations::tuple_get(tuple, *idx)),
            )
        }

        // Function construction and application.
        StreamOp::Function { func } => {
            let NodeState::Function { function, captures } =
                &mut state.node_states[node_id.index()]
            else {
                unreachable!("function node has incompatible runtime state")
            };
            evaluate_function(func, context, function, captures)
        }
        StreamOp::Apply { func, args } => {
            let func = context.read_value(state, func);
            let args = args
                .iter()
                .map(|arg| context.read_value(state, arg))
                .collect::<Vec<_>>();
            let NodeState::CallLift {
                last_function,
                last_arguments,
                active_function,
                callable,
            } = &mut state.node_states[node_id.index()]
            else {
                unreachable!("apply node has incompatible runtime state")
            };
            let func = retain_last_value(func, last_function);
            let args = lift_call_args(args, last_arguments);
            evaluate_apply(func, args, active_function, callable)
        }
        StreamOp::DirectApply { func, args } => {
            let args = args
                .iter()
                .map(|arg| context.read_value(state, arg))
                .collect::<Vec<_>>();
            let NodeState::PersistentCall {
                evaluator,
                environment_values,
                last_arguments,
            } = &mut state.node_states[node_id.index()]
            else {
                unreachable!("direct apply node has incompatible runtime state")
            };
            let args = lift_call_args(args, last_arguments);
            evaluate_direct_apply(func, args, context, evaluator, environment_values)
        }
        StreamOp::RecursiveApply { func, args } => {
            let args = args
                .iter()
                .map(|arg| context.read_value(state, arg))
                .collect::<Vec<_>>();
            let NodeState::CallLift { last_arguments, .. } =
                &mut state.node_states[node_id.index()]
            else {
                unreachable!("direct fix application node has incompatible runtime state")
            };
            let args = lift_call_args(args, last_arguments);
            evaluate_recursive_apply(func, args, context)
        }
        StreamOp::RecursiveCall { args } => {
            let args = args
                .iter()
                .map(|arg| context.read_value(state, arg))
                .collect::<Vec<_>>();
            let NodeState::CallLift { last_arguments, .. } =
                &mut state.node_states[node_id.index()]
            else {
                unreachable!("recursive call node has incompatible runtime state")
            };
            let args = lift_call_args(args, last_arguments);
            evaluate_recursive_call(args, context)
        }
        StreamOp::Partial {
            func,
            args,
            display,
        } => {
            let func = context.read_value(state, func);
            let args = args
                .iter()
                .map(|arg| context.read_value(state, arg))
                .collect::<Vec<_>>();
            let NodeState::CallLift {
                last_function,
                last_arguments,
                ..
            } = &mut state.node_states[node_id.index()]
            else {
                unreachable!("partial application node has incompatible runtime state")
            };
            let func = retain_last_value(func, last_function);
            let args = lift_call_args(args, last_arguments);
            evaluate_partial(func, args, display.clone())
        }
        StreamOp::Fix { func, display } => {
            let values = vec![context.read_value(state, func)];
            evaluate_fix(
                lift_value_operands(node_id, state, values).remove(0),
                display.clone(),
            )
        }
        StreamOp::ListMap { func, list } => {
            let values = vec![
                context.read_value(state, func),
                context.read_value(state, list),
            ];
            let mut values = lift_value_operands(node_id, state, values).into_iter();
            evaluate_list_map(values.next().unwrap(), values.next().unwrap())
        }
        StreamOp::ListFilter { func, list } => {
            let values = vec![
                context.read_value(state, func),
                context.read_value(state, list),
            ];
            let mut values = lift_value_operands(node_id, state, values).into_iter();
            evaluate_list_filter(values.next().unwrap(), values.next().unwrap())
        }
        StreamOp::ListFold { func, init, list } => {
            let values = vec![
                context.read_value(state, func),
                context.read_value(state, init),
                context.read_value(state, list),
            ];
            let mut values = lift_value_operands(node_id, state, values).into_iter();
            evaluate_list_fold(
                values.next().unwrap(),
                values.next().unwrap(),
                values.next().unwrap(),
            )
        }

        // Runtime-compiled expressions use the fallible traversal.
        StreamOp::Dynamic(_) => unreachable!("dynamic node reached infallible evaluator"),
    }
}

pub(in crate::dataflow) fn evaluate_nodes(
    nodes: &[BoundOp],
    state: &mut StreamState,
    context: EvaluationContext<'_>,
) {
    for (index, op) in nodes.iter().enumerate() {
        let node_id = NodeId::new(index);
        let value = evaluate_node(node_id, op, state, context);
        state.node_values[index] = value;
    }
}

fn evaluate_lazy_if(
    node_id: NodeId,
    op: &BoundOp,
    state: &mut StreamState,
    context: EvaluationContext<'_>,
) -> Value {
    let StreamOp::If {
        cond,
        then_branch,
        else_branch,
    } = op
    else {
        unreachable!("non-if node evaluated as lazy branch")
    };
    let condition = context.read_value(state, cond);
    let NodeState::LazyIf(lazy_if) = &mut state.node_states[node_id.index()] else {
        unreachable!("if node has incompatible runtime state")
    };
    let condition = retain_last_value(condition, &mut lazy_if.last_condition);
    if context.recursive_call.is_some() {
        return match condition {
            Value::Bool(true) => evaluate_branch(then_branch, &mut lazy_if.then_state, context),
            Value::Bool(false) => evaluate_branch(else_branch, &mut lazy_if.else_state, context),
            Value::Deferred => Value::Deferred,
            Value::NoVal => Value::NoVal,
            other => panic!("if condition must be bool, got {:?}", other),
        };
    }
    let then_value = evaluate_branch(then_branch, &mut lazy_if.then_state, context);
    let then_value = retain_last_value(then_value, &mut lazy_if.last_then_value);
    let else_value = evaluate_branch(else_branch, &mut lazy_if.else_state, context);
    let else_value = retain_last_value(else_value, &mut lazy_if.last_else_value);

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

fn evaluate_branch(
    branch: &BoundEvaluationGraph,
    state: &mut StreamState,
    context: EvaluationContext<'_>,
) -> Value {
    evaluate_nodes(&branch.nodes, state, context);
    let output = context.read_value(state, &branch.output);
    stage_recursive_delays(&branch.recursive_delays, state, &output);
    output
}

pub(in crate::dataflow) fn try_evaluate_nodes(
    nodes: &[BoundOp],
    state: &mut StreamState,
    context: EvaluationContext<'_>,
) -> Result<(), DataflowEvaluationError> {
    for (index, op) in nodes.iter().enumerate() {
        let node_id = NodeId::new(index);
        let value = match op {
            StreamOp::Dynamic(spec) => {
                let current = context.read_value(state, &spec.input);
                let NodeState::Dynamic(dynamic) = &mut state.node_states[index] else {
                    unreachable!("dynamic node has incompatible runtime state")
                };
                let current = retain_last_value(current, &mut dynamic.last_source_value);
                let result = evaluate_dynamic_expression(current, spec, dynamic, context)?;
                retain_last_value(result, &mut dynamic.last_result)
            }
            StreamOp::If { .. } => try_evaluate_lazy_if(node_id, op, state, context)?,
            _ => evaluate_node(node_id, op, state, context),
        };
        state.node_values[index] = value;
    }
    Ok(())
}

fn try_evaluate_lazy_if(
    node_id: NodeId,
    op: &BoundOp,
    state: &mut StreamState,
    context: EvaluationContext<'_>,
) -> Result<Value, DataflowEvaluationError> {
    let StreamOp::If {
        cond,
        then_branch,
        else_branch,
    } = op
    else {
        unreachable!("non-if node evaluated as lazy branch")
    };
    let condition = context.read_value(state, cond);
    let NodeState::LazyIf(lazy_if) = &mut state.node_states[node_id.index()] else {
        unreachable!("if node has incompatible runtime state")
    };
    let condition = retain_last_value(condition, &mut lazy_if.last_condition);

    match condition {
        Value::Bool(true) => {
            let selected = try_evaluate_branch(then_branch, &mut lazy_if.then_state, context);
            let unselected = try_evaluate_branch(else_branch, &mut lazy_if.else_state, context);
            let selected =
                selected.map(|value| retain_last_value(value, &mut lazy_if.last_then_value));
            let unselected =
                unselected.map(|value| retain_last_value(value, &mut lazy_if.last_else_value));
            let selected = selected?;
            if selected == Value::NoVal || matches!(unselected, Ok(Value::NoVal)) {
                Ok(Value::NoVal)
            } else {
                Ok(selected)
            }
        }
        Value::Bool(false) => {
            let unselected = try_evaluate_branch(then_branch, &mut lazy_if.then_state, context);
            let selected = try_evaluate_branch(else_branch, &mut lazy_if.else_state, context);
            let unselected =
                unselected.map(|value| retain_last_value(value, &mut lazy_if.last_then_value));
            let selected =
                selected.map(|value| retain_last_value(value, &mut lazy_if.last_else_value))?;
            if selected == Value::NoVal || matches!(unselected, Ok(Value::NoVal)) {
                Ok(Value::NoVal)
            } else {
                Ok(selected)
            }
        }
        Value::Deferred => {
            let then_value = try_evaluate_branch(then_branch, &mut lazy_if.then_state, context);
            let else_value = try_evaluate_branch(else_branch, &mut lazy_if.else_state, context);
            let then_value =
                then_value.map(|value| retain_last_value(value, &mut lazy_if.last_then_value));
            let else_value =
                else_value.map(|value| retain_last_value(value, &mut lazy_if.last_else_value));
            if matches!(then_value, Ok(Value::NoVal)) || matches!(else_value, Ok(Value::NoVal)) {
                Ok(Value::NoVal)
            } else {
                Ok(Value::Deferred)
            }
        }
        Value::NoVal => {
            let then_value = try_evaluate_branch(then_branch, &mut lazy_if.then_state, context);
            let else_value = try_evaluate_branch(else_branch, &mut lazy_if.else_state, context);
            if let Ok(value) = then_value {
                retain_last_value(value, &mut lazy_if.last_then_value);
            }
            if let Ok(value) = else_value {
                retain_last_value(value, &mut lazy_if.last_else_value);
            }
            Ok(Value::NoVal)
        }
        other => panic!("if condition must be bool, got {:?}", other),
    }
}

fn try_evaluate_branch(
    branch: &BoundEvaluationGraph,
    state: &mut StreamState,
    context: EvaluationContext<'_>,
) -> Result<Value, DataflowEvaluationError> {
    let snapshot = state.clone();
    if let Err(error) = try_evaluate_nodes(&branch.nodes, state, context) {
        *state = snapshot;
        return Err(error);
    }
    let output = context.read_value(state, &branch.output);
    stage_recursive_delays(&branch.recursive_delays, state, &output);
    Ok(output)
}

pub(in crate::dataflow) fn stage_recursive_delays(
    delays: &[NodeId],
    state: &mut StreamState,
    output: &Value,
) {
    for delay in delays {
        let NodeState::Delay(history) = &mut state.node_states[delay.index()] else {
            unreachable!("recursive delay node has incompatible runtime state")
        };
        history.stage_recursive_value(output.clone());
    }
}

pub(in crate::dataflow) fn commit_staged_temporal_state(
    body: &BoundEvaluationGraph,
    state: &mut StreamState,
    context: EvaluationContext<'_>,
) {
    for (index, op) in body.nodes.iter().enumerate() {
        match op {
            StreamOp::Delay { input, offset } if *offset > 0 => {
                let current = context.read_value(state, input);
                let NodeState::Delay(history) = &mut state.node_states[index] else {
                    unreachable!("delay node has incompatible runtime state")
                };
                history.commit_staged_write(current);
            }
            StreamOp::RecursiveDelay { .. } => {
                let NodeState::Delay(history) = &mut state.node_states[index] else {
                    unreachable!("recursive delay node has incompatible runtime state")
                };
                history.commit_recursive_value();
            }
            StreamOp::If {
                then_branch,
                else_branch,
                ..
            } => {
                let NodeState::LazyIf(lazy_if) = &mut state.node_states[index] else {
                    unreachable!("if node has incompatible runtime state")
                };
                commit_staged_temporal_state(then_branch, &mut lazy_if.then_state, context);
                commit_staged_temporal_state(else_branch, &mut lazy_if.else_state, context);
            }
            StreamOp::DirectApply { func, .. } => {
                let NodeState::PersistentCall {
                    evaluator,
                    environment_values,
                    ..
                } = &mut state.node_states[index]
                else {
                    unreachable!("direct application node has incompatible runtime state")
                };
                let capture_count = func.capture_slots.len();
                for (slot, source) in environment_values[..capture_count]
                    .iter_mut()
                    .zip(&func.capture_slots)
                {
                    *slot = context.environment_values[source.index()].clone();
                }
                evaluator.commit_temporal_state(environment_values);
            }
            StreamOp::Dynamic(_) => {
                let NodeState::Dynamic(dynamic) = &mut state.node_states[index] else {
                    unreachable!("dynamic node has incompatible runtime state")
                };
                if dynamic
                    .active_expression
                    .as_ref()
                    .is_some_and(|active| active.evaluator.program.requires_temporal_commit())
                {
                    dynamic.update_environment(
                        context.environment_values,
                        context.retained_environment_values,
                    );
                    let active = dynamic
                        .active_expression
                        .as_mut()
                        .expect("active dynamic expression disappeared before commit");
                    active
                        .evaluator
                        .commit_temporal_state(&dynamic.environment_values);
                }
            }
            _ => {}
        }
    }
}

fn lift_call_args(mut args: Vec<Value>, last: &mut [Option<Value>]) -> EcoVec<Value> {
    debug_assert_eq!(args.len(), last.len());
    for (arg, last) in args.iter_mut().zip(last) {
        *arg = retain_last_value(arg.clone(), last);
    }
    args.into()
}

fn lift_value_operands(
    node_id: NodeId,
    state: &mut StreamState,
    mut values: Vec<Value>,
) -> Vec<Value> {
    let NodeState::OperandLift { last_operands } = &mut state.node_states[node_id.index()] else {
        unreachable!("lifted value node has incompatible runtime state")
    };
    debug_assert_eq!(values.len(), last_operands.len());
    for (value, last) in values.iter_mut().zip(last_operands) {
        *value = retain_last_value(value.clone(), last);
    }
    values
}
