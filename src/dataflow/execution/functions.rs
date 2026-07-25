use super::super::ir::*;
use super::super::*;
use super::lifting::propagated_special;
use super::stream_evaluator::{EvaluationContext, StreamEvaluator};
use futures::StreamExt;
use std::{cell::RefCell, rc::Rc};

pub(in crate::dataflow) fn evaluate_function(
    func: &StreamFunction,
    context: EvaluationContext<'_>,
    function: &mut Option<RuntimeFunction>,
    captures: &Rc<RefCell<Vec<Value>>>,
) -> Value {
    for (value, source) in captures.borrow_mut().iter_mut().zip(&func.capture_slots) {
        *value = context.environment_values[source.index()].clone();
    }
    let function = function.get_or_insert_with(|| {
        let display = func.display.clone();
        let program = Rc::clone(&func.program);
        let captures = Rc::clone(captures);
        let parameter_count = func.parameters.len();
        let temporal = func.program.requires_temporal_commit();
        RuntimeFunction::value_factory(display, temporal, move || {
            let evaluator = Rc::new(RefCell::new(StreamEvaluator::new(Rc::clone(&program))));
            let captures = Rc::clone(&captures);
            Rc::new(move |args| {
                if args.len() != parameter_count {
                    return Err(anyhow::anyhow!(
                        "Function expected {} arguments, got {}",
                        parameter_count,
                        args.len()
                    ));
                }
                let mut values = captures.borrow().clone();
                values.extend(args);
                evaluator
                    .borrow_mut()
                    .evaluate_and_commit(&values, None)
                    .map_err(anyhow::Error::from)
            })
        })
    });
    Value::Function(function.clone())
}

struct RecursiveCall {
    program: Rc<StreamProgram>,
    environment_template: Vec<Value>,
    parameter_count: usize,
    available_frames: RefCell<Vec<CallFrame>>,
}

struct CallFrame {
    evaluator: StreamEvaluator,
    environment_values: Vec<Value>,
}

impl RecursiveCall {
    fn new(func: &StreamFunction, context: EvaluationContext<'_>) -> Self {
        let mut values = func
            .capture_slots
            .iter()
            .map(|source| context.environment_values[source.index()].clone())
            .collect::<Vec<_>>();
        values.resize(
            func.capture_slots.len() + func.parameters.len(),
            Value::NoVal,
        );
        Self {
            program: Rc::clone(&func.program),
            environment_template: values,
            parameter_count: func.parameters.len(),
            available_frames: RefCell::new(Vec::new()),
        }
    }

    fn new_frame(&self) -> CallFrame {
        CallFrame {
            evaluator: StreamEvaluator::new(Rc::clone(&self.program)),
            environment_values: self.environment_template.clone(),
        }
    }
    fn call_recursive(&self, args: EcoVec<Value>) -> anyhow::Result<Value> {
        let recursive_call = |args| {
            self.call_recursive(args)
                .expect("direct recursive function application failed")
        };
        self.call_with_context(args, Some(&recursive_call))
    }

    fn call_with_context(
        &self,
        args: EcoVec<Value>,
        recursive_call: Option<&dyn Fn(EcoVec<Value>) -> Value>,
    ) -> anyhow::Result<Value> {
        if self.parameter_count != args.len() {
            return Err(anyhow::anyhow!(
                "Function expected {} arguments, got {}",
                self.parameter_count,
                args.len()
            ));
        }

        let mut frame = self
            .available_frames
            .borrow_mut()
            .pop()
            .unwrap_or_else(|| self.new_frame());

        let parameter_start = frame.environment_values.len() - self.parameter_count;
        for (slot, value) in frame.environment_values[parameter_start..]
            .iter_mut()
            .zip(args)
        {
            *slot = value;
        }

        frame.evaluator.reset();
        let value = frame
            .evaluator
            .evaluate_and_commit(&frame.environment_values, recursive_call)
            .expect("function programs cannot contain fallible dynamic operators");
        self.available_frames.borrow_mut().push(frame);
        Ok(value)
    }
}

pub(in crate::dataflow) fn evaluate_apply(
    func: Value,
    args: EcoVec<Value>,
    active_function: &mut Option<RuntimeFunction>,
    callable: &mut Option<crate::core::RuntimeFunctionValueCallable>,
) -> Value {
    if let Some(value) = propagated_special(std::iter::once(&func).chain(args.iter())) {
        return value;
    }
    let Value::Function(function) = func else {
        panic!("Function application requires a function, got {}", func);
    };
    let changed = active_function
        .as_ref()
        .is_none_or(|active| !active.same_definition(&function));
    if changed {
        *callable = function.instantiate_value();
        *active_function = Some(function.clone());
    }
    if let Some(callable) = callable {
        return callable(args).expect("Function application failed");
    }
    call_runtime_function_once(function, args)
}

pub(in crate::dataflow) fn evaluate_direct_apply(
    func: &StreamFunction,
    args: EcoVec<Value>,
    context: EvaluationContext<'_>,
    evaluator: &mut StreamEvaluator,
    environment_values: &mut [Value],
) -> Value {
    let capture_count = func.capture_slots.len();
    debug_assert_eq!(
        environment_values.len(),
        capture_count + func.parameters.len()
    );
    debug_assert_eq!(args.len(), func.parameters.len());

    for (slot, source) in environment_values[..capture_count]
        .iter_mut()
        .zip(&func.capture_slots)
    {
        *slot = context.environment_values[source.index()].clone();
    }
    for (slot, value) in environment_values[capture_count..].iter_mut().zip(args) {
        *slot = value;
    }

    evaluator
        .evaluate_and_stage(environment_values)
        .expect("direct function programs cannot contain fallible dynamic operators")
}

pub(in crate::dataflow) fn evaluate_recursive_apply(
    func: &StreamFunction,
    args: EcoVec<Value>,
    context: EvaluationContext<'_>,
) -> Value {
    if let Some(value) = propagated_special(args.iter()) {
        return value;
    }

    let call = RecursiveCall::new(func, context);
    call.call_recursive(args)
        .expect("direct recursive function application failed")
}

pub(in crate::dataflow) fn evaluate_recursive_call(
    args: EcoVec<Value>,
    context: EvaluationContext<'_>,
) -> Value {
    if let Some(value) = propagated_special(args.iter()) {
        return value;
    }
    let recursive_call = context
        .recursive_call
        .expect("recursive self-call evaluated outside direct fix context");
    recursive_call(args)
}

pub(in crate::dataflow) fn evaluate_partial(
    func: Value,
    applied: EcoVec<Value>,
    display: EcoString,
) -> Value {
    if let Some(value) = propagated_special(std::iter::once(&func).chain(applied.iter())) {
        return value;
    }
    let Value::Function(function) = func else {
        panic!("partial requires a function, got {}", func);
    };
    if function.requires_call_site_instance() {
        panic!("temporal functions are not supported by partial application");
    }
    partial_function(function, applied, display)
}

pub(in crate::dataflow) fn evaluate_fix(func: Value, display: EcoString) -> Value {
    match func {
        Value::NoVal => Value::NoVal,
        Value::Deferred => Value::Deferred,
        Value::Function(function) => fix_function(function, display),
        other => panic!("fix requires a function, got {}", other),
    }
}

pub(in crate::dataflow) fn evaluate_list_map(func: Value, list: Value) -> Value {
    if let Some(value) = propagated_special([&func, &list]) {
        return value;
    }
    match (func, list) {
        (Value::Function(function), _) if function.requires_call_site_instance() => {
            panic!("temporal functions are not supported by List.map")
        }
        (Value::Function(function), Value::List(values)) => Value::List(
            values
                .into_iter()
                .map(|value| {
                    call_runtime_function_once(function.clone(), EcoVec::from(vec![value]))
                })
                .collect(),
        ),
        (func, list) => panic!(
            "List.map requires a function and list, got {} and {}",
            func, list
        ),
    }
}

pub(in crate::dataflow) fn evaluate_list_filter(func: Value, list: Value) -> Value {
    if let Some(value) = propagated_special([&func, &list]) {
        return value;
    }
    match (func, list) {
        (Value::Function(function), _) if function.requires_call_site_instance() => {
            panic!("temporal functions are not supported by List.filter")
        }
        (Value::Function(function), Value::List(values)) => {
            let mut filtered = EcoVec::new();
            for value in values {
                match call_runtime_function_once(
                    function.clone(),
                    EcoVec::from(vec![value.clone()]),
                ) {
                    Value::Bool(true) => filtered.push(value),
                    Value::Bool(false) => {}
                    other => panic!("List.filter returned non-bool value {}", other),
                }
            }
            Value::List(filtered)
        }
        (func, list) => panic!(
            "List.filter requires a function and list, got {} and {}",
            func, list
        ),
    }
}

pub(in crate::dataflow) fn evaluate_list_fold(func: Value, init: Value, list: Value) -> Value {
    if let Some(value) = propagated_special([&func, &init, &list]) {
        return value;
    }
    match (func, init, list) {
        (Value::Function(function), _, _) if function.requires_call_site_instance() => {
            panic!("temporal functions are not supported by List.fold")
        }
        (Value::Function(function), mut acc, Value::List(values)) => {
            for value in values {
                acc = call_runtime_function_once(function.clone(), EcoVec::from(vec![acc, value]));
            }
            acc
        }
        (func, _, list) => panic!(
            "List.fold requires a function and list, got {} and {}",
            func, list
        ),
    }
}

pub(in crate::dataflow) fn call_runtime_function_once(
    function: RuntimeFunction,
    args: EcoVec<Value>,
) -> Value {
    if let Some(callable) = function.instantiate_value() {
        return callable(args).expect("Function application failed");
    }
    if function.has_value_callable() {
        return function
            .call_value(args)
            .expect("Function application failed");
    }
    let mut stream = function.call(args).expect("Function application failed");
    futures::executor::block_on(stream.next()).unwrap_or(Value::NoVal)
}

fn partial_function(
    function: RuntimeFunction,
    applied: EcoVec<Value>,
    display: EcoString,
) -> Value {
    let stream_function = function.clone();
    let applied_for_stream = applied.clone();
    let runtime_function = RuntimeFunction::native_value(display, move |args| {
        let mut all_args = applied.clone();
        all_args.extend(args);
        Ok(call_runtime_function_once(function.clone(), all_args))
    });
    if !stream_function.supports_value_calls() {
        return Value::Function(RuntimeFunction::native(
            runtime_function.display_source().clone(),
            move |args| {
                let mut all_args = applied_for_stream.clone();
                all_args.extend(args);
                stream_function.call(all_args)
            },
        ));
    }
    Value::Function(runtime_function)
}

fn fix_function(function: RuntimeFunction, display: EcoString) -> Value {
    let slot: Rc<RefCell<Option<RuntimeFunction>>> = Rc::new(RefCell::new(None));
    let slot_for_call = slot.clone();
    let function_for_stream = function.clone();
    let slot_for_stream = slot.clone();
    let runtime_function = RuntimeFunction::native_value(display, move |args| {
        let self_function = slot_for_call
            .borrow()
            .as_ref()
            .expect("recursive function initialized")
            .clone();
        let mut all_args = EcoVec::new();
        all_args.push(Value::Function(self_function));
        all_args.extend(args);
        Ok(call_runtime_function_once(function.clone(), all_args))
    });
    let runtime_function = if function_for_stream.supports_value_calls() {
        runtime_function
    } else {
        RuntimeFunction::native(runtime_function.display_source().clone(), move |args| {
            let self_function = slot_for_stream
                .borrow()
                .as_ref()
                .expect("recursive function initialized")
                .clone();
            let mut all_args = EcoVec::new();
            all_args.push(Value::Function(self_function));
            all_args.extend(args);
            function_for_stream.call(all_args)
        })
    };
    *slot.borrow_mut() = Some(runtime_function.clone());
    Value::Function(runtime_function)
}
