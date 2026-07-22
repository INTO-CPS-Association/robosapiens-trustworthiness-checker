use std::{cell::RefCell, rc::Rc};

use super::combinators as mc;
use super::semantics::evaluate_scope;
use super::shared_output::SharedOutput;
use crate::VarName;
use crate::core::OutputStream;
use crate::core::RuntimeFunction;
use crate::core::StreamType;
use crate::core::Value;
use crate::lang::dsrv::ast::{CheckedExpr, Expr, ExprRef, ExprView};
use crate::semantics::{AsyncConfig, StreamContext};
use async_stream::stream;
use contiguous_tree::TreeCursorExt;
use ecow::{EcoString, EcoVec};
use futures::StreamExt;

#[derive(Clone)]
pub(super) struct ScopedExpr {
    pub(super) expr: Expr,
    phase: ExprPhase,
    pub(super) environment: Option<Rc<EvalBindingFrame>>,
    owner: Option<VarName>,
}

/// AST-owned checking state retained while an expression moves through lexical scopes.
#[derive(Clone)]
enum ExprPhase {
    Unchecked,
    Checked(CheckedExpr),
}

/// An immutable lexical frame. Arguments retain the scope in which they were created.
pub(super) struct EvalBindingFrame {
    parent: Option<Rc<EvalBindingFrame>>,
    bindings: EcoVec<(VarName, EvalBinding)>,
}

#[derive(Clone)]
enum EvalBinding {
    Expression(ScopedExpr),
    Stream(SharedOutput<Value>),
}

struct UntimedFunctionDef<AC>
where
    AC: AsyncConfig<Val = Value>,
{
    params: EcoVec<(VarName, StreamType)>,
    body: ScopedExpr,
    captures: EcoVec<(VarName, SharedOutput<Value>)>,
    temporal: bool,
    context: Rc<AC::Ctx>,
}

struct UntimedFunctionInstance {
    output: OutputStream<Value>,
    capture_drivers: Vec<OutputStream<Value>>,
}

impl<AC> UntimedFunctionDef<AC>
where
    AC: AsyncConfig<Val = Value>,
{
    fn instantiate(
        &self,
        arguments: &[SharedOutput<Value>],
    ) -> anyhow::Result<UntimedFunctionInstance> {
        if self.params.len() != arguments.len() {
            return Err(anyhow::anyhow!(
                "Function expected {} arguments, got {}",
                self.params.len(),
                arguments.len()
            ));
        }
        let body = self
            .body
            .clone()
            .bind_streams(self.captures.iter().cloned())
            .bind_streams(
                self.params
                    .iter()
                    .zip(arguments)
                    .map(|((name, _), stream)| (name.clone(), stream.clone())),
            );
        Ok(UntimedFunctionInstance {
            output: evaluate_scope::<AC>(body, &self.context),
            capture_drivers: self
                .captures
                .iter()
                .map(|(_, stream)| stream.subscribe())
                .collect(),
        })
    }

    fn partially_apply(&self, arguments: &[SharedOutput<Value>]) -> anyhow::Result<Rc<Self>> {
        if arguments.len() > self.params.len() {
            return Err(anyhow::anyhow!(
                "Function expected at most {} partial arguments, got {}",
                self.params.len(),
                arguments.len()
            ));
        }
        let mut captures = self.captures.clone();
        captures.extend(
            self.params
                .iter()
                .zip(arguments)
                .map(|((name, _), stream)| (name.clone(), stream.clone())),
        );
        Ok(Rc::new(Self {
            params: self.params.iter().skip(arguments.len()).cloned().collect(),
            body: self.body.clone(),
            captures,
            temporal: self.temporal,
            context: Rc::clone(&self.context),
        }))
    }
}

impl ScopedExpr {
    pub(super) fn unchecked(expr: Expr) -> Self {
        Self {
            expr,
            phase: ExprPhase::Unchecked,
            environment: None,
            owner: None,
        }
    }

    pub(super) fn checked(checked: CheckedExpr) -> Self {
        Self {
            expr: checked.expr().clone(),
            phase: ExprPhase::Checked(checked),
            environment: None,
            owner: None,
        }
    }

    pub(super) fn with_owner(mut self, owner: VarName) -> Self {
        self.owner = Some(owner);
        self
    }

    pub(super) fn owner(&self) -> Option<&VarName> {
        self.owner.as_ref()
    }

    pub(super) fn as_ref(&self) -> ExprRef<'_> {
        self.expr.as_ref()
    }

    pub(super) fn typ<'a>(
        &'a self,
        expr: ExprRef<'a>,
    ) -> Option<&'a crate::lang::dsrv::type_checker::TCType> {
        match &self.phase {
            ExprPhase::Unchecked => None,
            ExprPhase::Checked(checked) => Some(checked.cursor(expr).typ()),
        }
    }

    pub(super) fn shared_type_info(
        &self,
    ) -> Option<&Rc<crate::lang::dsrv::type_checker::TypeInfo>> {
        match &self.phase {
            ExprPhase::Unchecked => None,
            ExprPhase::Checked(checked) => Some(checked.as_ref().shared_type_info()),
        }
    }

    /// Attach this expression's phase and lexical environment to borrowed syntax.
    pub(super) fn scope(&self, expr: ExprRef<'_>) -> Self {
        Self {
            expr: self.expr.subtree(expr),
            phase: self.phase.clone(),
            environment: self.environment.clone(),
            owner: self.owner.clone(),
        }
    }

    pub(super) fn bind(
        self,
        params: &EcoVec<(VarName, StreamType)>,
        args: EcoVec<ScopedExpr>,
    ) -> anyhow::Result<Self> {
        if params.len() != args.len() {
            return Err(anyhow::anyhow!(
                "Function expected {} arguments, got {}",
                params.len(),
                args.len()
            ));
        }
        let bindings = params
            .iter()
            .zip(args)
            .map(|((name, _), argument)| (name.clone(), EvalBinding::Expression(argument)))
            .collect();
        Ok(Self {
            expr: self.expr,
            phase: self.phase,
            environment: Some(Rc::new(EvalBindingFrame {
                parent: self.environment,
                bindings,
            })),
            owner: self.owner,
        })
    }

    pub(super) fn resolve(&self, name: &VarName) -> Option<Self> {
        let mut frame = self.environment.as_deref();
        while let Some(current) = frame {
            if let Some((_, value)) = current
                .bindings
                .iter()
                .rev()
                .find(|(bound, _)| bound == name)
            {
                if let EvalBinding::Expression(value) = value {
                    return Some(value.clone());
                }
            }
            frame = current.parent.as_deref();
        }
        None
    }

    pub(super) fn resolve_stream(&self, name: &VarName) -> Option<OutputStream<Value>> {
        let mut frame = self.environment.as_deref();
        while let Some(current) = frame {
            if let Some((_, EvalBinding::Stream(value))) = current
                .bindings
                .iter()
                .rev()
                .find(|(bound, _)| bound == name)
            {
                return Some(value.subscribe());
            }
            frame = current.parent.as_deref();
        }
        None
    }

    fn bind_streams(
        self,
        bindings: impl IntoIterator<Item = (VarName, SharedOutput<Value>)>,
    ) -> Self {
        Self {
            expr: self.expr,
            phase: self.phase,
            environment: Some(Rc::new(EvalBindingFrame {
                parent: self.environment,
                bindings: bindings
                    .into_iter()
                    .map(|(name, stream)| (name, EvalBinding::Stream(stream)))
                    .collect(),
            })),
            owner: self.owner,
        }
    }

    /// Follow lexical aliases while the head expression is a bound variable.
    fn resolve_head(mut self) -> Self {
        while let ExprView::Var(name) = self.as_ref().view() {
            let Some(bound) = self.resolve(name) else {
                break;
            };
            self = bound;
        }
        self
    }
}

pub(crate) fn bind_expression_for_benchmark(
    body: Expr,
    params: &EcoVec<(VarName, StreamType)>,
    args: EcoVec<Expr>,
) -> usize {
    let framed = ScopedExpr::unchecked(body)
        .bind(
            params,
            args.into_iter().map(ScopedExpr::unchecked).collect(),
        )
        .expect("benchmark fixture has matching function arity");
    std::hint::black_box(&framed);
    framed.environment.as_ref().map_or(0, Rc::strong_count)
}

fn value_expression(value: Value, template: &ScopedExpr) -> ScopedExpr {
    ScopedExpr {
        expr: Expr::value_with_span(value, template.expr.as_ref().span()),
        phase: ExprPhase::Unchecked,
        environment: template.environment.clone(),
        owner: template.owner.clone(),
    }
}

fn eval_function_once(
    function: RuntimeFunction,
    args: EcoVec<Value>,
) -> impl std::future::Future<Output = Value> {
    async move {
        let mut stream = function.call(args).expect("Function application failed");
        stream.next().await.unwrap_or(Value::NoVal)
    }
}

pub(super) fn make_function<AC>(
    display: EcoString,
    params: EcoVec<(VarName, StreamType)>,
    body: ScopedExpr,
    ctx: &AC::Ctx,
) -> Value
where
    AC: AsyncConfig<Val = Value>,
{
    use ExprView::*;

    let callable_ctx = Rc::new(ctx.subcontext(0));
    let param_names = params
        .iter()
        .map(|(name, _)| name)
        .collect::<std::collections::BTreeSet<_>>();
    let captures = body
        .expr
        .free_variables()
        .into_iter()
        .filter(|name| !param_names.contains(name))
        .filter(|name| body.resolve(name).is_none() && body.resolve_stream(name).is_none())
        .filter_map(|name| {
            ctx.var(&name)
                .map(|stream| (name, SharedOutput::new(stream)))
        })
        .collect();
    let definition = Rc::new(UntimedFunctionDef::<AC> {
        params: params.clone(),
        body: body.clone(),
        captures,
        temporal: body.as_ref().postorder().any(|node| {
            matches!(
                node.view(),
                SIndex(_, _)
                    | Init(_, _)
                    | When(_)
                    | Update(_, _)
                    | Latch(_, _)
                    | Dynamic(_, _, _)
                    | Defer(_, _, _)
            )
        }),
        context: Rc::clone(&callable_ctx),
    });
    let runtime_function = RuntimeFunction::native(display, move |args| {
        let arguments = args
            .into_iter()
            .map(|value| value_expression(value, &body))
            .collect();
        let body = body.clone().bind(&params, arguments)?;
        Ok(evaluate_scope::<AC>(body, &callable_ctx))
    })
    .with_language_payload(definition);
    Value::Function(runtime_function)
}

fn partial_function(
    function: RuntimeFunction,
    applied: EcoVec<Value>,
    display: EcoString,
) -> Value {
    let runtime_function = RuntimeFunction::native(display, move |args| {
        let mut all_args = applied.clone();
        all_args.extend(args);
        function.call(all_args)
    });
    Value::Function(runtime_function)
}

fn partial_tree_function<AC>(
    function: RuntimeFunction,
    definition: Rc<UntimedFunctionDef<AC>>,
    applied: &[SharedOutput<Value>],
    display: EcoString,
) -> anyhow::Result<Value>
where
    AC: AsyncConfig<Val = Value>,
{
    let partial_definition = definition.partially_apply(applied)?;
    let Value::Function(function) = partial_function(function, EcoVec::new(), display) else {
        unreachable!();
    };
    Ok(Value::Function(
        function.with_language_payload(partial_definition),
    ))
}

fn reject_temporal_collection_function<AC>(function: &RuntimeFunction, operation: &str)
where
    AC: AsyncConfig<Val = Value>,
{
    if function
        .language_payload::<UntimedFunctionDef<AC>>()
        .is_some_and(|definition| definition.temporal)
    {
        panic!("temporal functions are not supported by {operation}");
    }
}

fn fix_function(function: RuntimeFunction, display: EcoString) -> Value {
    let slot: Rc<RefCell<Option<RuntimeFunction>>> = Rc::new(RefCell::new(None));
    let slot_for_call = slot.clone();
    let runtime_function = RuntimeFunction::native(display, move |args| {
        let self_function = slot_for_call
            .borrow()
            .as_ref()
            .expect("recursive function initialized")
            .clone();
        let mut all_args = EcoVec::new();
        all_args.push(Value::Function(self_function));
        all_args.extend(args);
        function.call(all_args)
    });
    *slot.borrow_mut() = Some(runtime_function.clone());
    Value::Function(runtime_function)
}

pub(super) fn eval_apply<AC>(
    func_expr: ScopedExpr,
    args: EcoVec<ScopedExpr>,
    ctx: &AC::Ctx,
) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value>,
{
    // A syntactic lambda keeps tree semantics: its arguments retain their
    // lexical scope and are evaluated when the bound variable is used. Once a
    // function is carried as a runtime Value, application is necessarily
    // pointwise because only argument values cross that boundary.
    let func_expr = func_expr.resolve_head();
    if let ExprView::Lambda(params, body) = func_expr.as_ref().view() {
        let body = func_expr
            .scope(body)
            .bind(params, args)
            .expect("Function application failed");
        return evaluate_scope::<AC>(body, ctx);
    }

    let mut func_stream = evaluate_scope::<AC>(func_expr, ctx);
    let arg_sources = args
        .into_iter()
        .map(|arg| SharedOutput::new(evaluate_scope::<AC>(arg, ctx)))
        .collect::<Vec<_>>();
    let mut arg_streams = arg_sources
        .iter()
        .map(SharedOutput::subscribe)
        .collect::<Vec<_>>();
    Box::pin(stream! {
        let mut active_tree_function: Option<(RuntimeFunction, UntimedFunctionInstance)> = None;
        loop {
            let Some(func_value) = func_stream.next().await else {
                return;
            };
            if let Value::Function(function) = &func_value
                && let Some(definition) =
                    function.language_payload::<UntimedFunctionDef<AC>>()
            {
                let changed = active_tree_function
                    .as_ref()
                    .is_none_or(|(active, _)| !active.same_definition(function));
                if changed {
                    let output = definition
                        .instantiate(&arg_sources)
                        .expect("Function application failed");
                    active_tree_function = Some((function.clone(), output));
                }
                // Function inputs are stream ports: advance each port once per
                // application tick even when the body can produce an initial
                // value without polling it (for example, a stream delay).
                for arg_stream in &mut arg_streams {
                    if arg_stream.next().await.is_none() {
                        return;
                    }
                }
                let Some((_, instance)) = &mut active_tree_function else {
                    unreachable!();
                };
                for capture in &mut instance.capture_drivers {
                    if capture.next().await.is_none() {
                        return;
                    }
                }
                let Some(value) = instance.output.next().await else {
                    return;
                };
                yield value;
                continue;
            }
            active_tree_function = None;
            let mut args = EcoVec::new();
            for arg_stream in &mut arg_streams {
                let Some(arg) = arg_stream.next().await else {
                    return;
                };
                args.push(arg);
            }

            if func_value == Value::NoVal || args.iter().any(|arg| *arg == Value::NoVal) {
                yield Value::NoVal;
                continue;
            }
            if func_value == Value::Deferred || args.iter().any(|arg| *arg == Value::Deferred) {
                yield Value::Deferred;
                continue;
            }

            let Value::Function(function) = func_value else {
                panic!("Function application requires a function, got {}", func_value);
            };
            yield eval_function_once(function, args).await;
        }
    })
}

pub(super) fn eval_partial<AC>(
    func_expr: ScopedExpr,
    args: EcoVec<ScopedExpr>,
    ctx: &AC::Ctx,
) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value>,
{
    let display: EcoString = format!("partial({}, ...)", func_expr.expr).into();
    let mut func_stream = evaluate_scope::<AC>(func_expr, ctx);
    let arg_sources = args
        .into_iter()
        .map(|arg| SharedOutput::new(evaluate_scope::<AC>(arg, ctx)))
        .collect::<Vec<_>>();
    let mut arg_streams = arg_sources
        .iter()
        .map(SharedOutput::subscribe)
        .collect::<Vec<_>>();
    Box::pin(stream! {
        let mut active_tree_partial: Option<(RuntimeFunction, Value)> = None;
        loop {
            let Some(func_value) = func_stream.next().await else {
                return;
            };
            if let Value::Function(function) = &func_value
                && let Some(definition) =
                    function.language_payload::<UntimedFunctionDef<AC>>()
            {
                let changed = active_tree_partial
                    .as_ref()
                    .is_none_or(|(active, _)| !active.same_definition(function));
                if changed {
                    let partial = partial_tree_function(
                        function.clone(),
                        definition,
                        &arg_sources,
                        display.clone(),
                    )
                    .expect("partial application failed");
                    active_tree_partial = Some((function.clone(), partial));
                }
                let Some((_, partial)) = &active_tree_partial else {
                    unreachable!();
                };
                yield partial.clone();
                continue;
            }
            active_tree_partial = None;
            let mut applied = EcoVec::new();
            for arg_stream in &mut arg_streams {
                let Some(arg) = arg_stream.next().await else {
                    return;
                };
                applied.push(arg);
            }

            if func_value == Value::NoVal || applied.iter().any(|arg| *arg == Value::NoVal) {
                yield Value::NoVal;
                continue;
            }
            if func_value == Value::Deferred || applied.iter().any(|arg| *arg == Value::Deferred) {
                yield Value::Deferred;
                continue;
            }

            let Value::Function(function) = func_value else {
                panic!("partial requires a function, got {}", func_value);
            };
            yield partial_function(function, applied, display.clone());
        }
    })
}

pub(super) fn eval_fix<AC>(func_expr: ScopedExpr, ctx: &AC::Ctx) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value>,
{
    let display: EcoString = format!("fix({})", func_expr.expr).into();
    let mut func_stream = evaluate_scope::<AC>(func_expr, ctx);
    Box::pin(stream! {
        while let Some(func_value) = func_stream.next().await {
            match func_value {
                Value::NoVal => yield Value::NoVal,
                Value::Deferred => yield Value::Deferred,
                Value::Function(function) => yield fix_function(function, display.clone()),
                other => panic!("fix requires a function, got {}", other),
            }
        }
    })
}

pub(super) fn eval_list_map<AC>(
    func_expr: ScopedExpr,
    list_expr: ScopedExpr,
    ctx: &AC::Ctx,
) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value>,
{
    let mut func_stream = evaluate_scope::<AC>(func_expr, ctx);
    let mut list_stream = evaluate_scope::<AC>(list_expr, ctx);
    Box::pin(stream! {
        while let (Some(func_value), Some(list_value)) = (func_stream.next().await, list_stream.next().await) {
            match (func_value, list_value) {
                (Value::NoVal, _) | (_, Value::NoVal) => yield Value::NoVal,
                (Value::Deferred, _) | (_, Value::Deferred) => yield Value::Deferred,
                (Value::Function(function), Value::List(values)) => {
                    reject_temporal_collection_function::<AC>(&function, "List.map");
                    let mut mapped = EcoVec::new();
                    for value in values {
                        mapped.push(eval_function_once(function.clone(), EcoVec::from(vec![value])).await);
                    }
                    yield Value::List(mapped);
                }
                (func, list) => panic!("List.map requires a function and list, got {} and {}", func, list),
            }
        }
    })
}

pub(super) fn eval_list_filter<AC>(
    func_expr: ScopedExpr,
    list_expr: ScopedExpr,
    ctx: &AC::Ctx,
) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value>,
{
    let mut func_stream = evaluate_scope::<AC>(func_expr, ctx);
    let mut list_stream = evaluate_scope::<AC>(list_expr, ctx);
    Box::pin(stream! {
        while let (Some(func_value), Some(list_value)) = (func_stream.next().await, list_stream.next().await) {
            match (func_value, list_value) {
                (Value::NoVal, _) | (_, Value::NoVal) => yield Value::NoVal,
                (Value::Deferred, _) | (_, Value::Deferred) => yield Value::Deferred,
                (Value::Function(function), Value::List(values)) => {
                    reject_temporal_collection_function::<AC>(&function, "List.filter");
                    let mut filtered = EcoVec::new();
                    for value in values {
                        match eval_function_once(function.clone(), EcoVec::from(vec![value.clone()])).await {
                            Value::Bool(true) => filtered.push(value),
                            Value::Bool(false) => {}
                            other => panic!("List.filter returned non-bool value {}", other),
                        }
                    }
                    yield Value::List(filtered);
                }
                (func, list) => panic!("List.filter requires a function and list, got {} and {}", func, list),
            }
        }
    })
}

pub(super) fn eval_list_fold<AC>(
    func_expr: ScopedExpr,
    init_expr: ScopedExpr,
    list_expr: ScopedExpr,
    ctx: &AC::Ctx,
) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value>,
{
    let mut func_stream = mc::stream_lift_base(evaluate_scope::<AC>(func_expr, ctx));
    let mut init_stream = mc::stream_lift_base(evaluate_scope::<AC>(init_expr, ctx));
    let mut list_stream = mc::stream_lift_base(evaluate_scope::<AC>(list_expr, ctx));
    Box::pin(stream! {
        while let (Some(func_value), Some(init), Some(list_value)) = (func_stream.next().await, init_stream.next().await, list_stream.next().await) {
            match (func_value, init, list_value) {
                (Value::NoVal, _, _) | (_, Value::NoVal, _) | (_, _, Value::NoVal) => yield Value::NoVal,
                (Value::Deferred, _, _) | (_, Value::Deferred, _) | (_, _, Value::Deferred) => yield Value::Deferred,
                (Value::Function(function), mut acc, Value::List(values)) => {
                    reject_temporal_collection_function::<AC>(&function, "List.fold");
                    for value in values {
                        acc = eval_function_once(function.clone(), EcoVec::from(vec![acc, value])).await;
                    }
                    yield acc;
                }
                (func, _, list) => panic!("List.fold requires a function and list, got {} and {}", func, list),
            }
        }
    })
}
