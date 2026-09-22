use std::collections::BTreeMap;
use std::{cell::RefCell, rc::Rc};

use super::atemporal::{Scope, eval_atemporal};
use super::combinators as mc;
use super::semantics::evaluate_scope;
use super::shared_output::SharedOutput;
use crate::VarName;
use crate::core::LocalStream;
use crate::core::RuntimeFunction;
use crate::core::{PartialMarker, Value, propagated_special};
use crate::lang::dsrv::ast::{AstShared, CheckedExpr, Expr, ExprRef, ExprView};
use crate::lang::dsrv::runtime_expression::{
    RuntimeExpressionSite, RuntimeExpressionSites, UntypedExpr,
};
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
    /// Bare syntax built directly by a program or test, never prepared.
    Unchecked,
    /// A prepared expression evaluated without consulting its types.
    Untyped(AstShared<RuntimeExpressionSites>),
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
    params: EcoVec<(VarName, crate::core::StreamTypeAscription)>,
    body: ScopedExpr,
    captures: EcoVec<(VarName, SharedOutput<Value>)>,
    temporal: bool,
    context: Rc<AC::Ctx>,
}

struct UntimedFunctionInstance {
    output: LocalStream<Value>,
    capture_drivers: Vec<LocalStream<Value>>,
}

/// One tree-backed callback invocation context for one collection tick.
///
/// Reusing the instance keeps captures fixed at this tick for every element.
/// Unbounded argument ports also make list length independent of channel capacity.
struct CollectionFunctionInstance {
    arguments: Vec<futures::channel::mpsc::UnboundedSender<Value>>,
    output: LocalStream<Value>,
}

impl CollectionFunctionInstance {
    async fn new<AC>(definition: &UntimedFunctionDef<AC>) -> Self
    where
        AC: AsyncConfig<Val = Value>,
    {
        let mut arguments = Vec::with_capacity(definition.params.len());
        let mut inputs = Vec::with_capacity(definition.params.len());
        for _ in &definition.params {
            let (sender, receiver) = futures::channel::mpsc::unbounded();
            arguments.push(sender);
            inputs.push(SharedOutput::new(Box::pin(receiver)));
        }
        let mut captures = Vec::with_capacity(definition.captures.len());
        for (name, output) in &definition.captures {
            let value = output.subscribe().next().await.unwrap_or(Value::NoVal);
            captures.push((name.clone(), SharedOutput::new(mc::val(value))));
        }
        let body = definition.body.clone().bind_streams(captures).bind_streams(
            definition
                .params
                .iter()
                .zip(&inputs)
                .map(|((name, _), stream)| (name.clone(), stream.clone())),
        );
        Self {
            arguments,
            output: evaluate_scope::<AC>(body, &definition.context),
        }
    }

    async fn apply(&mut self, arguments: impl IntoIterator<Item = Value>) -> Value {
        for (port, value) in self.arguments.iter_mut().zip(arguments) {
            port.unbounded_send(value)
                .expect("collection callback argument port closed");
        }
        self.output.next().await.unwrap_or(Value::NoVal)
    }
}

enum CollectionCallback {
    Atemporal {
        callback: AtemporalCallback,
        captures: BTreeMap<VarName, Value>,
    },
    Tree(CollectionFunctionInstance),
    Stream(RuntimeFunction),
}

impl CollectionCallback {
    async fn new<AC>(function: RuntimeFunction, operation: &'static str) -> Self
    where
        AC: AsyncConfig<Val = Value>,
    {
        if let Some(mut callback) = AtemporalCallback::of::<AC>(&function) {
            let captures = callback.read_captures().await;
            return Self::Atemporal { callback, captures };
        }
        reject_temporal_collection_function::<AC>(&function, operation);
        if let Some(definition) = function.language_payload::<UntimedFunctionDef<AC>>() {
            return Self::Tree(CollectionFunctionInstance::new(&definition).await);
        }
        Self::Stream(function)
    }

    async fn apply(&mut self, arguments: impl IntoIterator<Item = Value>) -> Value {
        let arguments = arguments.into_iter().collect::<Vec<_>>();
        match self {
            Self::Atemporal { callback, captures } => callback.apply(captures, arguments),
            Self::Tree(callback) => callback.apply(arguments).await,
            Self::Stream(function) => {
                eval_function_once(function.clone(), EcoVec::from(arguments)).await
            }
        }
    }
}

/// Advance callback captures even when a collection operand makes this tick
/// produce only a marker.
async fn advance_collection_captures<AC>(function: &RuntimeFunction)
where
    AC: AsyncConfig<Val = Value>,
{
    let Some(definition) = function.language_payload::<UntimedFunctionDef<AC>>() else {
        return;
    };
    for (_, capture) in &definition.captures {
        let _ = capture.subscribe().next().await;
    }
}

fn collection_marker(values: impl IntoIterator<Item = Value>) -> Option<Value> {
    propagated_special(values.into_iter().map(|value| PartialMarker::of(&value)))
        .map(PartialMarker::into_value)
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

    pub(super) fn untyped(expr: UntypedExpr) -> Self {
        Self {
            phase: ExprPhase::Untyped(AstShared::clone(expr.sites())),
            expr: expr.expr().clone(),
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
            ExprPhase::Unchecked | ExprPhase::Untyped(_) => None,
            ExprPhase::Checked(checked) => Some(checked.cursor(expr).typ()),
        }
    }

    #[cfg(test)]
    pub(super) fn shared_type_environment(
        &self,
    ) -> Option<&AstShared<crate::lang::dsrv::type_checker::StreamTypeEnvironment>> {
        match &self.phase {
            ExprPhase::Unchecked | ExprPhase::Untyped(_) => None,
            ExprPhase::Checked(checked) => Some(checked.as_ref().shared_type_environment()),
        }
    }

    /// The site of `expr`, a `dynamic` or `defer` occurrence of this expression.
    pub(super) fn runtime_expression(&self, expr: ExprRef<'_>) -> RuntimeExpressionSite {
        match &self.phase {
            ExprPhase::Unchecked => RuntimeExpressionSite::unlocated(expr),
            ExprPhase::Untyped(sites) => sites.site(expr).untyped(),
            ExprPhase::Checked(checked) => checked.cursor(expr).runtime_expression().clone(),
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
        params: &EcoVec<(VarName, crate::core::StreamTypeAscription)>,
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

    pub(super) fn bind_values(
        self,
        params: &EcoVec<(VarName, crate::core::StreamTypeAscription)>,
        values: EcoVec<Value>,
    ) -> anyhow::Result<Self> {
        if params.len() != values.len() {
            return Err(anyhow::anyhow!(
                "Function expected {} arguments, got {}",
                params.len(),
                values.len()
            ));
        }
        let bindings = params
            .iter()
            .zip(values)
            .map(|((name, _), value)| {
                (
                    name.clone(),
                    EvalBinding::Stream(SharedOutput::new(mc::val(value))),
                )
            })
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

    pub(super) fn resolve_stream(&self, name: &VarName) -> Option<LocalStream<Value>> {
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
    params: &EcoVec<(VarName, crate::core::StreamTypeAscription)>,
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

/// An expression that decides within a tick, such as a `match`.
///
/// The names it could read are read once per tick, whichever arm turns out
/// to be selected, and the expression is then evaluated over values. Reading
/// every arm's names rather than the selected arm's is what keeps the stream
/// advancing at the same rate as the rest of the specification, and is the
/// rule the dependency graph already states.
pub(super) fn eval_within_tick<AC>(expression: ScopedExpr, ctx: &AC::Ctx) -> LocalStream<Value>
where
    AC: AsyncConfig<Val = Value>,
{
    let mut names = Vec::new();
    let mut streams = Vec::new();
    for name in expression.expr.as_ref().free_variables() {
        // A name a pattern binds is not a stream; it comes from the match.
        let stream = expression
            .resolve_stream(&name)
            .or_else(|| ctx.var(&name))
            .map(mc::stream_lift_base);
        if let Some(stream) = stream {
            names.push(name);
            streams.push(stream);
        }
    }
    let expr = expression.expr.clone();
    let evaluate = move |bound: &BTreeMap<VarName, Value>| {
        eval_atemporal(expr.as_ref(), bound)
            .unwrap_or_else(|error| panic!("expression failed within its tick: {error}"))
    };
    // With nothing to read, the value is the same every tick.
    if streams.is_empty() {
        return mc::val(evaluate(&BTreeMap::new()));
    }
    Box::pin(stream! {
        loop {
            let mut bound = BTreeMap::new();
            for (name, stream) in names.iter().zip(streams.iter_mut()) {
                match stream.next().await {
                    Some(value) => {
                        bound.insert(name.clone(), value);
                    }
                    None => return,
                }
            }
            yield evaluate(&bound);
        }
    })
}

/// A collection callback evaluated within the tick that calls it.
///
/// The callback runs once per element, so its body cannot be a stream: there
/// is nothing to advance it per element, and the names it reads from around
/// it must hold what they hold this tick. Those names are read once per
/// tick, from subscriptions this holds, and the body is then evaluated for
/// each element over values alone.
struct AtemporalCallback {
    parameters: EcoVec<(VarName, crate::core::StreamTypeAscription)>,
    body: Expr,
    captures: Vec<(VarName, LocalStream<Value>)>,
}

impl AtemporalCallback {
    /// Whether this function can be run within a tick, and the means to do
    /// it. A body that reads history cannot; lexical bindings are resolved to
    /// one captured value for the surrounding tick.
    fn of<AC>(function: &RuntimeFunction) -> Option<Self>
    where
        AC: AsyncConfig<Val = Value>,
    {
        let definition = function.language_payload::<UntimedFunctionDef<AC>>()?;
        if definition.body.expr.as_ref().postorder().any(|node| {
            matches!(
                node.view(),
                ExprView::SIndex(..)
                    | ExprView::Init(..)
                    | ExprView::Update(..)
                    | ExprView::Latch(..)
                    | ExprView::When(..)
                    | ExprView::Dynamic(..)
                    | ExprView::Defer(..)
                    | ExprView::Fix(..)
                    | ExprView::Partial(..)
                    | ExprView::MonitoredAt(..)
                    | ExprView::Dist(..)
            )
        }) {
            return None;
        }
        Some(Self {
            parameters: definition.params.clone(),
            body: definition.body.expr.clone(),
            captures: definition
                .captures
                .iter()
                .map(|(name, stream)| (name.clone(), stream.subscribe()))
                .collect(),
        })
    }

    /// Read every captured name once, for the tick about to be evaluated.
    async fn read_captures(&mut self) -> BTreeMap<VarName, Value> {
        let mut values = BTreeMap::new();
        for (name, stream) in &mut self.captures {
            let value = stream.next().await.unwrap_or(Value::NoVal);
            values.insert(name.clone(), value);
        }
        values
    }

    /// Apply the callback to one element, with the tick's captured values.
    fn apply(&self, captures: &BTreeMap<VarName, Value>, arguments: Vec<Value>) -> Value {
        let mut bound = captures.clone();
        for ((name, _), value) in self.parameters.iter().zip(arguments) {
            bound.insert(name.clone(), value);
        }
        let scope = Scope { bound, outer: None };
        eval_atemporal(self.body.as_ref(), &scope)
            .unwrap_or_else(|error| panic!("collection callback failed: {error}"))
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
    params: EcoVec<(VarName, crate::core::StreamTypeAscription)>,
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
        .as_ref()
        .free_variables()
        .into_iter()
        .filter(|name| !param_names.contains(name))
        .filter_map(|name| {
            body.resolve_stream(&name)
                .or_else(|| {
                    body.resolve(&name)
                        .map(|expression| evaluate_scope::<AC>(expression, ctx))
                })
                .or_else(|| ctx.var(&name))
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
        let body = body.clone().bind_values(&params, args)?;
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
) -> LocalStream<Value>
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
) -> LocalStream<Value>
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

pub(super) fn eval_fix<AC>(func_expr: ScopedExpr, ctx: &AC::Ctx) -> LocalStream<Value>
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
) -> LocalStream<Value>
where
    AC: AsyncConfig<Val = Value>,
{
    let mut func_stream = mc::stream_lift_base(evaluate_scope::<AC>(func_expr, ctx));
    let mut list_stream = mc::stream_lift_base(evaluate_scope::<AC>(list_expr, ctx));
    Box::pin(stream! {
        while let (Some(func_value), Some(list_value)) = (func_stream.next().await, list_stream.next().await) {
            if let Some(marker) = collection_marker([func_value.clone(), list_value.clone()]) {
                if let Value::Function(function) = &func_value {
                    advance_collection_captures::<AC>(function).await;
                }
                yield marker;
                continue;
            }
            match func_value {
                Value::NoVal => yield Value::NoVal,
                Value::Deferred => yield Value::Deferred,
                Value::Function(function) => {
                    let Value::List(values) = list_value else {
                        panic!("List.map requires a list, got {list_value}");
                    };
                    let mut callback = CollectionCallback::new::<AC>(function, "List.map").await;
                    let mut mapped = EcoVec::new();
                    for value in values {
                        mapped.push(callback.apply([value]).await);
                    }
                    yield Value::List(mapped);
                }
                func => panic!("List.map requires a function, got {func}"),
            }
        }
    })
}

pub(super) fn eval_list_filter<AC>(
    func_expr: ScopedExpr,
    list_expr: ScopedExpr,
    ctx: &AC::Ctx,
) -> LocalStream<Value>
where
    AC: AsyncConfig<Val = Value>,
{
    let mut func_stream = mc::stream_lift_base(evaluate_scope::<AC>(func_expr, ctx));
    let mut list_stream = mc::stream_lift_base(evaluate_scope::<AC>(list_expr, ctx));
    Box::pin(stream! {
        while let (Some(func_value), Some(list_value)) = (func_stream.next().await, list_stream.next().await) {
            if let Some(marker) = collection_marker([func_value.clone(), list_value.clone()]) {
                if let Value::Function(function) = &func_value {
                    advance_collection_captures::<AC>(function).await;
                }
                yield marker;
                continue;
            }
            match func_value {
                Value::NoVal => yield Value::NoVal,
                Value::Deferred => yield Value::Deferred,
                Value::Function(function) => {
                    let Value::List(values) = list_value else {
                        panic!("List.filter requires a list, got {list_value}");
                    };
                    let mut callback = CollectionCallback::new::<AC>(function, "List.filter").await;
                    let mut filtered = EcoVec::new();
                    let keep = |kept: Value, value: Value, filtered: &mut EcoVec<Value>| match kept {
                        Value::Bool(true) => filtered.push(value),
                        Value::Bool(false) => {}
                        other => panic!("List.filter returned non-bool value {}", other),
                    };
                    for value in values {
                        let kept = callback.apply([value.clone()]).await;
                        keep(kept, value, &mut filtered);
                    }
                    yield Value::List(filtered);
                }
                func => panic!("List.filter requires a function, got {func}"),
            }
        }
    })
}

pub(super) fn eval_list_fold<AC>(
    func_expr: ScopedExpr,
    init_expr: ScopedExpr,
    list_expr: ScopedExpr,
    ctx: &AC::Ctx,
) -> LocalStream<Value>
where
    AC: AsyncConfig<Val = Value>,
{
    let mut func_stream = mc::stream_lift_base(evaluate_scope::<AC>(func_expr, ctx));
    let mut init_stream = mc::stream_lift_base(evaluate_scope::<AC>(init_expr, ctx));
    let mut list_stream = mc::stream_lift_base(evaluate_scope::<AC>(list_expr, ctx));
    Box::pin(stream! {
        while let (Some(func_value), Some(init), Some(list_value)) = (func_stream.next().await, init_stream.next().await, list_stream.next().await) {
            if let Some(marker) = collection_marker([
                func_value.clone(),
                init.clone(),
                list_value.clone(),
            ]) {
                if let Value::Function(function) = &func_value {
                    advance_collection_captures::<AC>(function).await;
                }
                yield marker;
                continue;
            }
            match func_value {
                Value::NoVal => yield Value::NoVal,
                Value::Deferred => yield Value::Deferred,
                Value::Function(function) => {
                    if let Some(marker) = collection_marker([init.clone(), list_value.clone()]) {
                        advance_collection_captures::<AC>(&function).await;
                        yield marker;
                        continue;
                    }
                    let (mut acc, values) = match (init, list_value) {
                        (acc, Value::List(values)) => (acc, values),
                        (_, list) => panic!("List.fold requires a list, got {list}"),
                    };
                    let mut callback = CollectionCallback::new::<AC>(function, "List.fold").await;
                    for value in values {
                        acc = callback.apply([acc, value]).await;
                    }
                    yield acc;
                }
                func => panic!("List.fold requires a function, got {func}"),
            }
        }
    })
}
