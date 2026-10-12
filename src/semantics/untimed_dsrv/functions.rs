use std::collections::BTreeMap;
use std::{cell::RefCell, rc::Rc};

use super::atemporal::{RecursiveCall, Scope, eval_atemporal};
use super::combinators as mc;
use super::semantics::evaluate_scope;
use super::shared_output::SharedOutput;
use crate::VarName;
use crate::core::LocalStream;
use crate::core::RuntimeFunction;
use crate::core::values::operations::ListFilter;
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

/// What every function value a lambda yields has in common.
///
/// A lambda yields one function value per tick. The values share this
/// definition, which is how an application recognises that the function it
/// is applying has not changed, and differ only in what their captured names
/// hold at the tick they were yielded for.
struct UntimedFunctionDef<AC>
where
    AC: AsyncConfig<Val = Value>,
{
    display: EcoString,
    params: EcoVec<(VarName, crate::core::StreamTypeAscription)>,
    body: ScopedExpr,
    /// The names the body reads from around it, in the order of
    /// [`UntimedClosure::captures`].
    captures: EcoVec<VarName>,
    temporal: bool,
    context: Rc<AC::Ctx>,
}

/// One function value: a definition, and what each of its captured names
/// holds at the tick the value was yielded for.
///
/// Carrying values rather than streams is what keeps a function value that
/// is used in several places consistent. Whoever yields the value reads each
/// captured stream exactly once per tick, so a function value applied by
/// two streams, or by one stream each tick, sees the tick it belongs to and
/// advances nothing itself.
struct UntimedClosure<AC>
where
    AC: AsyncConfig<Val = Value>,
{
    definition: Rc<UntimedFunctionDef<AC>>,
    captures: EcoVec<Value>,
}

/// A function applied as a stream, for a body that may read history.
///
/// Its captured names are ports that the application fills once per tick
/// from the function value of that tick, so the body sees them as streams.
struct UntimedFunctionInstance {
    output: LocalStream<Value>,
    captures: Vec<futures::channel::mpsc::UnboundedSender<Value>>,
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
    fn new<AC>(closure: &UntimedClosure<AC>) -> Self
    where
        AC: AsyncConfig<Val = Value>,
    {
        let definition = &closure.definition;
        let mut arguments = Vec::with_capacity(definition.params.len());
        let mut inputs = Vec::with_capacity(definition.params.len());
        for _ in &definition.params {
            let (sender, receiver) = futures::channel::mpsc::unbounded();
            arguments.push(sender);
            inputs.push(SharedOutput::new(Box::pin(receiver)));
        }
        let body = closure.bind_captures().bind_streams(
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
    fn new<AC>(function: RuntimeFunction, operation: &'static str) -> Self
    where
        AC: AsyncConfig<Val = Value>,
    {
        if let Some((callback, captures)) = AtemporalCallback::of::<AC>(&function) {
            return Self::Atemporal { callback, captures };
        }
        reject_temporal_collection_function::<AC>(&function, operation);
        if let Some(closure) = function.language_payload::<UntimedClosure<AC>>() {
            return Self::Tree(CollectionFunctionInstance::new(&closure));
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

fn collection_marker<'a>(values: impl IntoIterator<Item = &'a Value>) -> Option<Value> {
    propagated_special(values.into_iter().map(PartialMarker::of)).map(PartialMarker::into_value)
}

impl<AC> UntimedFunctionDef<AC>
where
    AC: AsyncConfig<Val = Value>,
{
    /// The function value of one tick, whose captured names hold `captures`.
    fn function(self: &Rc<Self>, captures: EcoVec<Value>) -> Value {
        let closure = Rc::new(UntimedClosure {
            definition: Rc::clone(self),
            captures,
        });
        let callable = Rc::clone(&closure);
        // Called with values, as `fix` and a function from outside the tree
        // call it, the body has nothing to read its captures from but the
        // values this function value holds; its context subscribes to
        // nothing.
        let runtime_function = if self.evaluates_within_a_tick() {
            RuntimeFunction::native_value(self.display.clone(), move |args| {
                callable.call_with_values(args, false)
            })
        } else {
            RuntimeFunction::native(self.display.clone(), move |args| {
                let definition = &callable.definition;
                let body = callable
                    .bind_captures()
                    .bind_values(&definition.params, args)?;
                Ok(evaluate_scope::<AC>(body, &definition.context))
            })
        }
        .with_language_payload(closure);
        Value::Function(runtime_function)
    }

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
        let mut ports = Vec::with_capacity(self.captures.len());
        let mut captures = Vec::with_capacity(self.captures.len());
        for name in &self.captures {
            let (sender, receiver) = futures::channel::mpsc::unbounded();
            ports.push(sender);
            captures.push((name.clone(), SharedOutput::new(Box::pin(receiver))));
        }
        let body = self.body.clone().bind_streams(captures).bind_streams(
            self.params
                .iter()
                .zip(arguments)
                .map(|((name, _), stream)| (name.clone(), stream.clone())),
        );
        Ok(UntimedFunctionInstance {
            output: evaluate_scope::<AC>(body, &self.context),
            captures: ports,
        })
    }

    /// Whether the body can be evaluated over values within one tick: it
    /// reads no history and builds no function that could.
    fn evaluates_within_a_tick(&self) -> bool {
        !self.body.expr.as_ref().postorder().any(|node| {
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
        })
    }

    /// The definition left once the first `count` parameters are captured.
    fn partially_apply(&self, count: usize, display: EcoString) -> anyhow::Result<Rc<Self>> {
        if count > self.params.len() {
            return Err(anyhow::anyhow!(
                "Function expected at most {} partial arguments, got {}",
                self.params.len(),
                count
            ));
        }
        let mut captures = self.captures.clone();
        captures.extend(self.params.iter().take(count).map(|(name, _)| name.clone()));
        Ok(Rc::new(Self {
            display,
            params: self.params.iter().skip(count).cloned().collect(),
            body: self.body.clone(),
            captures,
            temporal: self.temporal,
            context: Rc::clone(&self.context),
        }))
    }
}

impl<AC> UntimedClosure<AC>
where
    AC: AsyncConfig<Val = Value>,
{
    /// The captured names and the values they hold for this function value.
    fn captured(&self) -> impl Iterator<Item = (&VarName, &Value)> {
        self.definition.captures.iter().zip(&self.captures)
    }

    /// Call the function as the body of a recursive one, over values.
    fn call_recursively(&self, arguments: EcoVec<Value>) -> anyhow::Result<Value> {
        self.call_with_values(arguments, true)
    }

    fn call_with_values(&self, arguments: EcoVec<Value>, recursive: bool) -> anyhow::Result<Value> {
        let definition = &self.definition;
        anyhow::ensure!(
            definition.params.len() == arguments.len(),
            "Function expected {} arguments, got {}",
            definition.params.len(),
            arguments.len()
        );
        let mut bound = self
            .captured()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        bound.extend(
            definition
                .params
                .iter()
                .map(|(name, _)| name.clone())
                .zip(arguments),
        );
        let scope = Scope {
            bound,
            outer: recursive.then_some(&RecursiveCall as &dyn super::atemporal::Environment),
        };
        eval_atemporal(definition.body.expr.as_ref(), &scope).map_err(anyhow::Error::new)
    }

    /// The body, with each captured name bound to its value for this tick.
    fn bind_captures(&self) -> ScopedExpr {
        self.definition.body.clone().bind_streams(
            self.captured()
                .map(|(name, value)| (name.clone(), SharedOutput::new(mc::val(value.clone())))),
        )
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
}

impl AtemporalCallback {
    /// Whether this function can be run within a tick, and the means to do
    /// it with the values its captured names hold this tick. A body that
    /// reads history cannot.
    fn of<AC>(function: &RuntimeFunction) -> Option<(Self, BTreeMap<VarName, Value>)>
    where
        AC: AsyncConfig<Val = Value>,
    {
        let closure = function.language_payload::<UntimedClosure<AC>>()?;
        let definition = &closure.definition;
        if !definition.evaluates_within_a_tick() {
            return None;
        }
        let callback = Self {
            parameters: definition.params.clone(),
            body: definition.body.expr.clone(),
        };
        let captures = closure
            .captured()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        Some((callback, captures))
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

/// The function values of a lambda, one per tick.
///
/// Each value carries what the names its body reads from around it hold at
/// that tick. This stream reads each of them exactly once per tick, through
/// one subscription it owns, so however many places apply the function, and
/// however late they first do, every application sees the tick it belongs
/// to and none of them advances a captured stream.
pub(super) fn eval_lambda<AC>(
    display: EcoString,
    params: EcoVec<(VarName, crate::core::StreamTypeAscription)>,
    body: ScopedExpr,
    ctx: &AC::Ctx,
) -> LocalStream<Value>
where
    AC: AsyncConfig<Val = Value>,
{
    use ExprView::*;

    // The body reads the specification's streams only through its captures,
    // which this stream advances once per tick. The context it is evaluated
    // in therefore subscribes to nothing. A full subcontext would subscribe
    // to every stream and never be ticked, so each publisher's bounded
    // channel to it would fill and block the runtime: after 8 ticks under
    // semi-sync (its channel capacity), after 1000 under async.
    let callable_ctx = Rc::new(ctx.restricted_subcontext(EcoVec::new(), 0));
    let param_names = params
        .iter()
        .map(|(name, _)| name)
        .collect::<std::collections::BTreeSet<_>>();
    let (captures, capture_streams): (EcoVec<_>, Vec<_>) = body
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
                .map(|stream| (name, stream))
        })
        .unzip();
    let definition = Rc::new(UntimedFunctionDef::<AC> {
        display,
        params,
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
        body,
        captures,
        context: callable_ctx,
    });
    // With nothing captured, every tick's function value is the same.
    if capture_streams.is_empty() {
        return mc::val(definition.function(EcoVec::new()));
    }
    // A lambda without captures repeats its value for as long as it is
    // read, and so does one with captures: a captured stream that has ended
    // holds no value, as a list callback has always seen it, and the
    // operands the function is applied to decide when the result ends.
    let mut capture_streams = capture_streams
        .into_iter()
        .map(|stream| stream.chain(futures::stream::repeat(Value::NoVal)))
        .collect::<Vec<_>>();
    Box::pin(stream! {
        loop {
            let mut captures = EcoVec::with_capacity(capture_streams.len());
            for stream in &mut capture_streams {
                captures.push(stream.next().await.unwrap_or(Value::NoVal));
            }
            yield definition.function(captures);
        }
    })
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

fn reject_temporal_collection_function<AC>(function: &RuntimeFunction, operation: &str)
where
    AC: AsyncConfig<Val = Value>,
{
    if function
        .language_payload::<UntimedClosure<AC>>()
        .is_some_and(|closure| closure.definition.temporal)
    {
        panic!("temporal functions are not supported by {operation}");
    }
}

fn fix_function<AC>(function: RuntimeFunction, display: EcoString) -> Value
where
    AC: AsyncConfig<Val = Value>,
{
    let slot: Rc<RefCell<Option<RuntimeFunction>>> = Rc::new(RefCell::new(None));
    let slot_for_call = slot.clone();
    // A recursive body is evaluated over values, so that its `if` can leave
    // the recursive branch unevaluated once the base case is selected. The
    // stream path cannot: it advances both branches every tick, so every
    // call would make another without end.
    if let Some(closure) = function.language_payload::<UntimedClosure<AC>>()
        && closure.definition.evaluates_within_a_tick()
    {
        let runtime_function = RuntimeFunction::native_value(display, move |args| {
            let self_function = slot_for_call
                .borrow()
                .as_ref()
                .expect("recursive function initialized")
                .clone();
            let mut all_args = EcoVec::new();
            all_args.push(Value::Function(self_function));
            all_args.extend(args);
            closure.call_recursively(all_args)
        });
        *slot.borrow_mut() = Some(runtime_function.clone());
        return Value::Function(runtime_function);
    }
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
        // An argument elaboration checks against a newtype is checked on
        // every application tick whether or not the body uses it, as the
        // dataflow graph, which evaluates every node, does: a callee that
        // ignores its argument does not bypass the check.
        let checks = args
            .iter()
            .filter(|arg| {
                matches!(arg.as_ref().view(), ExprView::Ascribe(_, target) if target.mentions_newtype())
            })
            .map(|arg| evaluate_scope::<AC>(arg.clone(), ctx))
            .collect::<Vec<_>>();
        let body = func_expr
            .scope(body)
            .bind(params, args)
            .expect("Function application failed");
        let result = evaluate_scope::<AC>(body, ctx);
        if checks.is_empty() {
            return result;
        }
        return Box::pin(stream! {
            let mut result = result;
            let mut checks = checks;
            while let Some(value) = result.next().await {
                for check in &mut checks {
                    // Its value is the body's to use; only its failure matters.
                    let _ = check.next().await;
                }
                yield value;
            }
        });
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
        let mut active_tree_function: Option<(Rc<UntimedFunctionDef<AC>>, UntimedFunctionInstance)> =
            None;
        loop {
            let Some(func_value) = func_stream.next().await else {
                return;
            };
            if let Value::Function(function) = &func_value
                && let Some(closure) = function.language_payload::<UntimedClosure<AC>>()
            {
                // Each tick brings a new function value; it is the same
                // function while its definition is.
                let changed = active_tree_function
                    .as_ref()
                    .is_none_or(|(active, _)| !Rc::ptr_eq(active, &closure.definition));
                if changed {
                    let output = closure
                        .definition
                        .instantiate(&arg_sources)
                        .expect("Function application failed");
                    active_tree_function = Some((Rc::clone(&closure.definition), output));
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
                for (port, value) in instance.captures.iter_mut().zip(&closure.captures) {
                    port.unbounded_send(value.clone())
                        .expect("function capture port closed");
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
    let mut arg_streams = args
        .into_iter()
        .map(|arg| evaluate_scope::<AC>(arg, ctx))
        .collect::<Vec<_>>();
    Box::pin(stream! {
        // The function partially applied, and what it becomes with its
        // leading parameters captured.
        let mut active_tree_partial: Option<(Rc<UntimedFunctionDef<AC>>, Rc<UntimedFunctionDef<AC>>)> =
            None;
        loop {
            let Some(func_value) = func_stream.next().await else {
                return;
            };
            if let Value::Function(function) = &func_value
                && let Some(closure) = function.language_payload::<UntimedClosure<AC>>()
            {
                let changed = active_tree_partial
                    .as_ref()
                    .is_none_or(|(active, _)| !Rc::ptr_eq(active, &closure.definition));
                if changed {
                    let partial = closure
                        .definition
                        .partially_apply(arg_streams.len(), display.clone())
                        .expect("partial application failed");
                    active_tree_partial = Some((Rc::clone(&closure.definition), partial));
                }
                // The applied arguments become captures, read this tick as
                // the function's own captures were.
                let mut captures = closure.captures.clone();
                for arg_stream in &mut arg_streams {
                    let Some(arg) = arg_stream.next().await else {
                        return;
                    };
                    captures.push(arg);
                }
                let Some((_, partial)) = &active_tree_partial else {
                    unreachable!();
                };
                yield partial.function(captures);
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
                Value::Function(function) => yield fix_function::<AC>(function, display.clone()),
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
            if let Some(marker) = collection_marker([&func_value, &list_value]) {
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
                    let mut callback = CollectionCallback::new::<AC>(function, "List.map");
                    let mut mapped = EcoVec::with_capacity(values.len());
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
            if let Some(marker) = collection_marker([&func_value, &list_value]) {
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
                    let mut callback = CollectionCallback::new::<AC>(function, "List.filter");
                    let mut filter = ListFilter::new(values);
                    while let Some(value) = filter.pending() {
                        let argument = value.clone();
                        match callback.apply([argument]).await {
                            Value::Bool(keep) => filter.decide(keep),
                            other => panic!("List.filter returned non-bool value {}", other),
                        }
                    }
                    yield Value::List(filter.finish());
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
            if let Some(marker) = collection_marker([&func_value, &init, &list_value]) {
                yield marker;
                continue;
            }
            match func_value {
                Value::NoVal => yield Value::NoVal,
                Value::Deferred => yield Value::Deferred,
                Value::Function(function) => {
                    if let Some(marker) = collection_marker([&init, &list_value]) {
                        yield marker;
                        continue;
                    }
                    let (mut acc, values) = match (init, list_value) {
                        (acc, Value::List(values)) => (acc, values),
                        (_, list) => panic!("List.fold requires a list, got {list}"),
                    };
                    let mut callback = CollectionCallback::new::<AC>(function, "List.fold");
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
