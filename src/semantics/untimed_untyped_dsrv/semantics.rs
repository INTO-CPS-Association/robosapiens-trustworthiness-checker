use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use super::combinators as mc;
use crate::core::OutputStream;
use crate::core::RuntimeFunction;
use crate::core::StreamType;
use crate::core::Value;
use crate::lang::core::parser::ExprParser;
use crate::lang::dsrv::ast::{
    BoolBinOp, CompBinOp, NumericalBinOp, SBinOp, SExpr, SpannedExpr, StrBinOp,
};
use crate::lang::dsrv::span::Spanned;
use crate::semantics::{AsyncConfig, MonitoringSemantics, StreamContext};
use async_stream::stream;
use ecow::{EcoString, EcoVec};
use futures::StreamExt;
use tracing::debug;

type UntypedFunctionEnv = BTreeMap<crate::VarName, SpannedExpr>;

fn value_expr(value: Value, span: crate::lang::dsrv::span::Span) -> SpannedExpr {
    Spanned {
        node: SExpr::Val(value),
        span,
    }
}

fn subst_expr(expr: SpannedExpr, env: &UntypedFunctionEnv) -> SpannedExpr {
    let span = expr.span;
    let node = match expr.node {
        SExpr::Val(_) => return expr,
        SExpr::Var(v) => {
            return env.get(&v).cloned().unwrap_or(Spanned {
                node: SExpr::Var(v),
                span,
            });
        }
        SExpr::BinOp(a, b, op) => SExpr::BinOp(
            Box::new(subst_expr(*a, env)),
            Box::new(subst_expr(*b, env)),
            op,
        ),
        SExpr::If(c, a, b) => SExpr::If(
            Box::new(subst_expr(*c, env)),
            Box::new(subst_expr(*a, env)),
            Box::new(subst_expr(*b, env)),
        ),
        SExpr::SIndex(e, i) => SExpr::SIndex(Box::new(subst_expr(*e, env)), i),
        SExpr::Default(a, b) => {
            SExpr::Default(Box::new(subst_expr(*a, env)), Box::new(subst_expr(*b, env)))
        }
        SExpr::Update(a, b) => {
            SExpr::Update(Box::new(subst_expr(*a, env)), Box::new(subst_expr(*b, env)))
        }
        SExpr::IsDefined(e) => SExpr::IsDefined(Box::new(subst_expr(*e, env))),
        SExpr::When(e) => SExpr::When(Box::new(subst_expr(*e, env))),
        SExpr::Latch(a, b) => {
            SExpr::Latch(Box::new(subst_expr(*a, env)), Box::new(subst_expr(*b, env)))
        }
        SExpr::Init(a, b) => {
            SExpr::Init(Box::new(subst_expr(*a, env)), Box::new(subst_expr(*b, env)))
        }
        SExpr::Not(e) => SExpr::Not(Box::new(subst_expr(*e, env))),
        SExpr::Lambda(params, body) => {
            let mut nested_env = env.clone();
            for (name, _) in &params {
                nested_env.remove(name);
            }
            SExpr::Lambda(params, Box::new(subst_expr(*body, &nested_env)))
        }
        SExpr::Apply(func, args) => SExpr::Apply(
            Box::new(subst_expr(*func, env)),
            args.into_iter().map(|arg| subst_expr(arg, env)).collect(),
        ),
        SExpr::Fix(func) => SExpr::Fix(Box::new(subst_expr(*func, env))),
        SExpr::Partial(func, args) => SExpr::Partial(
            Box::new(subst_expr(*func, env)),
            args.into_iter().map(|arg| subst_expr(arg, env)).collect(),
        ),
        SExpr::List(items) => SExpr::List(items.into_iter().map(|e| subst_expr(e, env)).collect()),
        SExpr::Tuple(items) => {
            SExpr::Tuple(items.into_iter().map(|e| subst_expr(e, env)).collect())
        }
        SExpr::LIndex(a, b) => {
            SExpr::LIndex(Box::new(subst_expr(*a, env)), Box::new(subst_expr(*b, env)))
        }
        SExpr::LAppend(a, b) => {
            SExpr::LAppend(Box::new(subst_expr(*a, env)), Box::new(subst_expr(*b, env)))
        }
        SExpr::LConcat(a, b) => {
            SExpr::LConcat(Box::new(subst_expr(*a, env)), Box::new(subst_expr(*b, env)))
        }
        SExpr::LHead(e) => SExpr::LHead(Box::new(subst_expr(*e, env))),
        SExpr::LTail(e) => SExpr::LTail(Box::new(subst_expr(*e, env))),
        SExpr::LLen(e) => SExpr::LLen(Box::new(subst_expr(*e, env))),
        SExpr::LMap(f, l) => {
            SExpr::LMap(Box::new(subst_expr(*f, env)), Box::new(subst_expr(*l, env)))
        }
        SExpr::LFilter(f, l) => {
            SExpr::LFilter(Box::new(subst_expr(*f, env)), Box::new(subst_expr(*l, env)))
        }
        SExpr::LFold(f, init, l) => SExpr::LFold(
            Box::new(subst_expr(*f, env)),
            Box::new(subst_expr(*init, env)),
            Box::new(subst_expr(*l, env)),
        ),
        SExpr::Map(map) => SExpr::Map(
            map.into_iter()
                .map(|(k, v)| (k, subst_expr(v, env)))
                .collect(),
        ),
        SExpr::Struct(map) => SExpr::Struct(
            map.into_iter()
                .map(|(k, v)| (k, subst_expr(v, env)))
                .collect(),
        ),
        SExpr::ObjectLiteral(map) => SExpr::ObjectLiteral(
            map.into_iter()
                .map(|(k, v)| (k, subst_expr(v, env)))
                .collect(),
        ),
        SExpr::MGet(map, k) => SExpr::MGet(Box::new(subst_expr(*map, env)), k),
        SExpr::SGet(st, k) => SExpr::SGet(Box::new(subst_expr(*st, env)), k),
        SExpr::MInsert(map, k, v) => SExpr::MInsert(
            Box::new(subst_expr(*map, env)),
            k,
            Box::new(subst_expr(*v, env)),
        ),
        SExpr::MRemove(map, k) => SExpr::MRemove(Box::new(subst_expr(*map, env)), k),
        SExpr::MHasKey(map, k) => SExpr::MHasKey(Box::new(subst_expr(*map, env)), k),
        SExpr::Sin(e) => SExpr::Sin(Box::new(subst_expr(*e, env))),
        SExpr::Cos(e) => SExpr::Cos(Box::new(subst_expr(*e, env))),
        SExpr::Tan(e) => SExpr::Tan(Box::new(subst_expr(*e, env))),
        SExpr::Abs(e) => SExpr::Abs(Box::new(subst_expr(*e, env))),
        SExpr::Dynamic(mut runtime) => {
            runtime.source = Box::new(subst_expr(*runtime.source, env));
            SExpr::Dynamic(runtime)
        }
        SExpr::Defer(mut runtime) => {
            runtime.source = Box::new(subst_expr(*runtime.source, env));
            SExpr::Defer(runtime)
        }
        SExpr::MonitoredAt(v, n) => SExpr::MonitoredAt(v, n),
        SExpr::Dist(a, b) => SExpr::Dist(a, b),
    };
    Spanned { node, span }
}

fn function_body_with_args(
    params: &EcoVec<crate::VarName>,
    body: &SpannedExpr,
    args: EcoVec<Value>,
) -> anyhow::Result<SpannedExpr> {
    if params.len() != args.len() {
        return Err(anyhow::anyhow!(
            "Function expected {} arguments, got {}",
            params.len(),
            args.len()
        ));
    }

    let mut env = UntypedFunctionEnv::new();
    for (name, value) in params.iter().zip(args) {
        env.insert(name.clone(), value_expr(value, body.span));
    }
    Ok(subst_expr(body.clone(), &env))
}

fn function_body_with_arg_exprs(
    params: &EcoVec<(crate::VarName, StreamType)>,
    body: SpannedExpr,
    args: EcoVec<SpannedExpr>,
) -> anyhow::Result<SpannedExpr> {
    if params.len() != args.len() {
        return Err(anyhow::anyhow!(
            "Function expected {} arguments, got {}",
            params.len(),
            args.len()
        ));
    }

    let mut env = UntypedFunctionEnv::new();
    for ((name, _), arg) in params.iter().zip(args) {
        env.insert(name.clone(), arg);
    }
    Ok(subst_expr(body, &env))
}

fn eval_untyped_function_values_once<AC, Parser>(
    function: RuntimeFunction,
    args: EcoVec<Value>,
) -> impl std::future::Future<Output = Value>
where
    Parser: ExprParser<SpannedExpr> + 'static,
    AC: AsyncConfig<Val = Value, Expr = SpannedExpr>,
{
    async move {
        let mut stream = function.call(args).expect("Function application failed");
        stream.next().await.unwrap_or(Value::NoVal)
    }
}

fn make_untyped_function<AC, Parser>(
    display: EcoString,
    params: EcoVec<crate::VarName>,
    body: SpannedExpr,
    ctx: &AC::Ctx,
) -> Value
where
    Parser: ExprParser<SpannedExpr> + 'static,
    AC: AsyncConfig<Val = Value, Expr = SpannedExpr>,
{
    let callable_ctx = ctx.subcontext(0);
    let runtime_function = RuntimeFunction::native(display, move |args| {
        let body = function_body_with_args(&params, &body, args)?;
        Ok(
            <UntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                body,
                &callable_ctx,
            ),
        )
    });
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

fn eval_apply<AC, Parser>(
    func_expr: SpannedExpr,
    args: EcoVec<SpannedExpr>,
    ctx: &AC::Ctx,
) -> OutputStream<Value>
where
    Parser: ExprParser<SpannedExpr> + 'static,
    AC: AsyncConfig<Val = Value, Expr = SpannedExpr>,
{
    if let SExpr::Lambda(params, body) = func_expr.node {
        let body = function_body_with_arg_exprs(&params, *body, args)
            .expect("Function application failed");
        return <UntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
            body, ctx,
        );
    }

    let mut func_stream =
        <UntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(func_expr, ctx);
    let mut arg_streams = args
        .into_iter()
        .map(|arg| {
            <UntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(arg, ctx)
        })
        .collect::<Vec<_>>();
    Box::pin(stream! {
        loop {
            let Some(func_value) = func_stream.next().await else {
                return;
            };
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
            yield eval_untyped_function_values_once::<AC, Parser>(function, args).await;
        }
    })
}

fn eval_partial<AC, Parser>(
    func_expr: SpannedExpr,
    args: EcoVec<SpannedExpr>,
    ctx: &AC::Ctx,
) -> OutputStream<Value>
where
    Parser: ExprParser<SpannedExpr> + 'static,
    AC: AsyncConfig<Val = Value, Expr = SpannedExpr>,
{
    let display: EcoString = format!("partial({}, ...)", func_expr).into();
    let mut func_stream =
        <UntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(func_expr, ctx);
    let mut arg_streams = args
        .into_iter()
        .map(|arg| {
            <UntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(arg, ctx)
        })
        .collect::<Vec<_>>();
    Box::pin(stream! {
        loop {
            let Some(func_value) = func_stream.next().await else {
                return;
            };
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

fn eval_fix<AC, Parser>(func_expr: SpannedExpr, ctx: &AC::Ctx) -> OutputStream<Value>
where
    Parser: ExprParser<SpannedExpr> + 'static,
    AC: AsyncConfig<Val = Value, Expr = SpannedExpr>,
{
    let display: EcoString = format!("fix({})", func_expr).into();
    let mut func_stream =
        <UntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(func_expr, ctx);
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

fn eval_list_map<AC, Parser>(
    func_expr: SpannedExpr,
    list_expr: SpannedExpr,
    ctx: &AC::Ctx,
) -> OutputStream<Value>
where
    Parser: ExprParser<SpannedExpr> + 'static,
    AC: AsyncConfig<Val = Value, Expr = SpannedExpr>,
{
    let mut func_stream =
        <UntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(func_expr, ctx);
    let mut list_stream =
        <UntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(list_expr, ctx);
    Box::pin(stream! {
        while let (Some(func_value), Some(list_value)) = (func_stream.next().await, list_stream.next().await) {
            match (func_value, list_value) {
                (Value::NoVal, _) | (_, Value::NoVal) => yield Value::NoVal,
                (Value::Deferred, _) | (_, Value::Deferred) => yield Value::Deferred,
                (Value::Function(function), Value::List(values)) => {
                    let mut mapped = EcoVec::new();
                    for value in values {
                        mapped.push(eval_untyped_function_values_once::<AC, Parser>(function.clone(), EcoVec::from(vec![value])).await);
                    }
                    yield Value::List(mapped);
                }
                (func, list) => panic!("List.map requires a function and list, got {} and {}", func, list),
            }
        }
    })
}

fn eval_list_filter<AC, Parser>(
    func_expr: SpannedExpr,
    list_expr: SpannedExpr,
    ctx: &AC::Ctx,
) -> OutputStream<Value>
where
    Parser: ExprParser<SpannedExpr> + 'static,
    AC: AsyncConfig<Val = Value, Expr = SpannedExpr>,
{
    let mut func_stream =
        <UntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(func_expr, ctx);
    let mut list_stream =
        <UntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(list_expr, ctx);
    Box::pin(stream! {
        while let (Some(func_value), Some(list_value)) = (func_stream.next().await, list_stream.next().await) {
            match (func_value, list_value) {
                (Value::NoVal, _) | (_, Value::NoVal) => yield Value::NoVal,
                (Value::Deferred, _) | (_, Value::Deferred) => yield Value::Deferred,
                (Value::Function(function), Value::List(values)) => {
                    let mut filtered = EcoVec::new();
                    for value in values {
                        match eval_untyped_function_values_once::<AC, Parser>(function.clone(), EcoVec::from(vec![value.clone()])).await {
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

fn eval_list_fold<AC, Parser>(
    func_expr: SpannedExpr,
    init_expr: SpannedExpr,
    list_expr: SpannedExpr,
    ctx: &AC::Ctx,
) -> OutputStream<Value>
where
    Parser: ExprParser<SpannedExpr> + 'static,
    AC: AsyncConfig<Val = Value, Expr = SpannedExpr>,
{
    let mut func_stream =
        <UntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(func_expr, ctx);
    let mut init_stream =
        <UntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(init_expr, ctx);
    let mut list_stream =
        <UntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(list_expr, ctx);
    Box::pin(stream! {
        while let (Some(func_value), Some(init), Some(list_value)) = (func_stream.next().await, init_stream.next().await, list_stream.next().await) {
            match (func_value, init, list_value) {
                (Value::NoVal, _, _) | (_, Value::NoVal, _) | (_, _, Value::NoVal) => yield Value::NoVal,
                (Value::Deferred, _, _) | (_, Value::Deferred, _) | (_, _, Value::Deferred) => yield Value::Deferred,
                (Value::Function(function), mut acc, Value::List(values)) => {
                    for value in values {
                        acc = eval_untyped_function_values_once::<AC, Parser>(function.clone(), EcoVec::from(vec![acc, value])).await;
                    }
                    yield acc;
                }
                (func, _, list) => panic!("List.fold requires a function and list, got {} and {}", func, list),
            }
        }
    })
}

#[derive(Clone)]
pub struct UntimedDsrvSemantics<Parser>
where
    Parser: ExprParser<SpannedExpr> + 'static,
{
    _parser: std::marker::PhantomData<Parser>,
}

impl<Parser, AC> MonitoringSemantics<AC> for UntimedDsrvSemantics<Parser>
where
    Parser: ExprParser<SpannedExpr> + 'static,
    AC: AsyncConfig<Val = Value, Expr = SpannedExpr>,
{
    fn to_async_stream(expr: SpannedExpr, ctx: &AC::Ctx) -> OutputStream<Value> {
        debug!("Creating async stream for expression: {:?}", expr);
        match expr.node {
            SExpr::Val(v) => {
                debug!("Constant value: {:?}", v);
                mc::val(v)
            }
            SExpr::BinOp(e1, e2, op) => {
                debug!("Binary operation: {:?} {:?} {:?}", e1, op, e2);
                let e1 = <Self as MonitoringSemantics<AC>>::to_async_stream(*e1, ctx);
                let e2 = <Self as MonitoringSemantics<AC>>::to_async_stream(*e2, ctx);
                match op {
                    SBinOp::NOp(NumericalBinOp::Add) => {
                        debug!("Performing addition operation");
                        mc::plus(e1, e2)
                    }
                    SBinOp::NOp(NumericalBinOp::Sub) => {
                        debug!("Performing subtraction operation");
                        mc::minus(e1, e2)
                    }
                    SBinOp::NOp(NumericalBinOp::Mul) => {
                        debug!("Performing multiplication operation");
                        mc::mult(e1, e2)
                    }
                    SBinOp::NOp(NumericalBinOp::Div) => {
                        debug!("Performing division operation");
                        mc::div(e1, e2)
                    }
                    SBinOp::NOp(NumericalBinOp::Mod) => {
                        debug!("Performing modulo operation");
                        mc::modulo(e1, e2)
                    }
                    SBinOp::BOp(BoolBinOp::Or) => {
                        debug!("Performing logical OR operation");
                        mc::or(e1, e2)
                    }
                    SBinOp::BOp(BoolBinOp::And) => {
                        debug!("Performing logical AND operation");
                        mc::and(e1, e2)
                    }
                    SBinOp::BOp(BoolBinOp::Impl) => {
                        debug!("Performing logical IMPLICATION operation");
                        mc::implication(e1, e2)
                    }
                    SBinOp::SOp(StrBinOp::Concat) => {
                        debug!("Performing string concatenation");
                        mc::concat(e1, e2)
                    }
                    SBinOp::COp(CompBinOp::Eq) => {
                        debug!("Performing equality comparison");
                        mc::eq(e1, e2)
                    }
                    SBinOp::COp(CompBinOp::Le) => {
                        debug!("Performing less than or equal comparison");
                        mc::le(e1, e2)
                    }
                    SBinOp::COp(CompBinOp::Lt) => {
                        debug!("Performing less than comparison");
                        mc::lt(e1, e2)
                    }
                    SBinOp::COp(CompBinOp::Ge) => {
                        debug!("Performing greater than or equal comparison");
                        mc::ge(e1, e2)
                    }
                    SBinOp::COp(CompBinOp::Gt) => {
                        debug!("Performing greater than comparison");
                        mc::gt(e1, e2)
                    }
                }
            }
            SExpr::Not(x) => {
                debug!("Performing logical NOT operation");
                let x = <Self as MonitoringSemantics<AC>>::to_async_stream(*x, ctx);
                mc::not(x)
            }
            SExpr::Var(v) => {
                debug!("Accessing variable: {:?}", v);
                mc::var::<AC>(ctx, v)
            }
            SExpr::Dynamic(runtime) => {
                let e = <Self as MonitoringSemantics<AC>>::to_async_stream(*runtime.source, ctx);
                mc::dynamic::<AC, Parser>(ctx, e, runtime.scope, 1)
            }
            SExpr::Defer(runtime) => {
                let e = <Self as MonitoringSemantics<AC>>::to_async_stream(*runtime.source, ctx);
                mc::defer::<AC, Parser>(ctx, e, runtime.scope, 1)
            }
            SExpr::Update(e1, e2) => {
                let e1 = <Self as MonitoringSemantics<AC>>::to_async_stream(*e1, ctx);
                let e2 = <Self as MonitoringSemantics<AC>>::to_async_stream(*e2, ctx);
                mc::update(e1, e2)
            }
            SExpr::Default(e, d) => {
                let e = <Self as MonitoringSemantics<AC>>::to_async_stream(*e, ctx);
                let d = <Self as MonitoringSemantics<AC>>::to_async_stream(*d, ctx);
                mc::default(e, d)
            }
            SExpr::IsDefined(e) => {
                let e = <Self as MonitoringSemantics<AC>>::to_async_stream(*e, ctx);
                mc::is_defined(e)
            }
            SExpr::When(e) => {
                let e = <Self as MonitoringSemantics<AC>>::to_async_stream(*e, ctx);
                mc::when(e)
            }
            SExpr::Latch(e1, e2) => {
                let e1 = <Self as MonitoringSemantics<AC>>::to_async_stream(*e1, ctx);
                let e2 = <Self as MonitoringSemantics<AC>>::to_async_stream(*e2, ctx);
                mc::latch(e1, e2)
            }
            SExpr::Init(e1, e2) => {
                let e1 = <Self as MonitoringSemantics<AC>>::to_async_stream(*e1, ctx);
                let e2 = <Self as MonitoringSemantics<AC>>::to_async_stream(*e2, ctx);
                mc::init(e1, e2)
            }
            SExpr::SIndex(e, i) => {
                let e = <Self as MonitoringSemantics<AC>>::to_async_stream(*e, ctx);
                mc::sindex(e, i)
            }
            SExpr::If(b, e1, e2) => {
                let b = <Self as MonitoringSemantics<AC>>::to_async_stream(*b, ctx);
                let e1 = <Self as MonitoringSemantics<AC>>::to_async_stream(*e1, ctx);
                let e2 = <Self as MonitoringSemantics<AC>>::to_async_stream(*e2, ctx);
                mc::if_stm(b, e1, e2)
            }
            SExpr::List(exprs) => {
                let exprs: Vec<_> = exprs
                    .into_iter()
                    .map(|e| <Self as MonitoringSemantics<AC>>::to_async_stream(e, ctx))
                    .collect();
                mc::list(exprs)
            }
            SExpr::Tuple(exprs) => {
                let exprs: Vec<_> = exprs
                    .into_iter()
                    .map(|e| <Self as MonitoringSemantics<AC>>::to_async_stream(e, ctx))
                    .collect();
                mc::tuple(exprs)
            }
            SExpr::LIndex(e, i) => {
                let e = <Self as MonitoringSemantics<AC>>::to_async_stream(*e, ctx);
                let i = <Self as MonitoringSemantics<AC>>::to_async_stream(*i, ctx);
                mc::lindex(e, i)
            }
            SExpr::LAppend(lst, el) => {
                let lst = <Self as MonitoringSemantics<AC>>::to_async_stream(*lst, ctx);
                let el = <Self as MonitoringSemantics<AC>>::to_async_stream(*el, ctx);
                mc::lappend(lst, el)
            }
            SExpr::LConcat(lst1, lst2) => {
                let lst1 = <Self as MonitoringSemantics<AC>>::to_async_stream(*lst1, ctx);
                let lst2 = <Self as MonitoringSemantics<AC>>::to_async_stream(*lst2, ctx);
                mc::lconcat(lst1, lst2)
            }
            SExpr::LHead(lst) => {
                let lst = <Self as MonitoringSemantics<AC>>::to_async_stream(*lst, ctx);
                mc::lhead(lst)
            }
            SExpr::LTail(lst) => {
                let lst = <Self as MonitoringSemantics<AC>>::to_async_stream(*lst, ctx);
                mc::ltail(lst)
            }
            SExpr::LLen(lst) => {
                let lst = <Self as MonitoringSemantics<AC>>::to_async_stream(*lst, ctx);
                mc::llen(lst)
            }
            SExpr::Lambda(params, body) => {
                let param_names = params.iter().map(|(name, _)| name.clone()).collect();
                let params_display = params
                    .iter()
                    .map(|(name, typ)| format!("{}: {}", name, typ))
                    .collect::<Vec<_>>()
                    .join(", ");
                let display = format!("\\{} -> {}", params_display, body).into();
                mc::val(make_untyped_function::<AC, Parser>(
                    display,
                    param_names,
                    *body,
                    ctx,
                ))
            }
            SExpr::Apply(func, args) => eval_apply::<AC, Parser>(*func, args, ctx),
            SExpr::Fix(func) => eval_fix::<AC, Parser>(*func, ctx),
            SExpr::Partial(func, args) => eval_partial::<AC, Parser>(*func, args, ctx),
            SExpr::LMap(func, list) => eval_list_map::<AC, Parser>(*func, *list, ctx),
            SExpr::LFilter(func, list) => eval_list_filter::<AC, Parser>(*func, *list, ctx),
            SExpr::LFold(func, init, list) => {
                eval_list_fold::<AC, Parser>(*func, *init, *list, ctx)
            }
            SExpr::Map(map) | SExpr::Struct(map) | SExpr::ObjectLiteral(map) => {
                let map: BTreeMap<_, _> = map
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k,
                            <Self as MonitoringSemantics<AC>>::to_async_stream(v, ctx),
                        )
                    })
                    .collect();
                mc::map(map)
            }
            SExpr::MGet(map, k) => {
                let map = <Self as MonitoringSemantics<AC>>::to_async_stream(*map, ctx);
                mc::mget(map, k)
            }
            SExpr::SGet(_, _) => {
                panic!("dot field access is only supported for structs in typed semantics")
            }
            SExpr::MRemove(map, k) => {
                let map = <Self as MonitoringSemantics<AC>>::to_async_stream(*map, ctx);
                mc::mremove(map, k)
            }
            SExpr::MInsert(map, k, v) => {
                let map = <Self as MonitoringSemantics<AC>>::to_async_stream(*map, ctx);
                let v = <Self as MonitoringSemantics<AC>>::to_async_stream(*v, ctx);
                mc::minsert(map, k, v)
            }
            SExpr::MHasKey(map, k) => {
                let map = <Self as MonitoringSemantics<AC>>::to_async_stream(*map, ctx);
                mc::mhas_key(map, k)
            }
            SExpr::MonitoredAt(_, _) => {
                unimplemented!("Function monitored_at only supported in distributed semantics")
            }
            SExpr::Dist(_, _) => {
                unimplemented!("Function dist only supported in distributed semantics")
            }
            SExpr::Sin(v) => {
                let v = <Self as MonitoringSemantics<AC>>::to_async_stream(*v, ctx);
                mc::sin(v)
            }
            SExpr::Cos(v) => {
                let v = <Self as MonitoringSemantics<AC>>::to_async_stream(*v, ctx);
                mc::cos(v)
            }
            SExpr::Tan(v) => {
                let v = <Self as MonitoringSemantics<AC>>::to_async_stream(*v, ctx);
                mc::tan(v)
            }
            SExpr::Abs(v) => {
                let v = <Self as MonitoringSemantics<AC>>::to_async_stream(*v, ctx);
                mc::abs(v)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::async_test;
    use crate::core::StreamTypeAscription;
    use crate::dsrv_fixtures::TestConfig;
    use crate::lang::dsrv::ast::SpannedExpr;
    use crate::lang::dsrv::lalr_parser::LALRParser;
    use crate::runtime::asynchronous::Context;
    use crate::semantics::StreamContext;
    use ecow::eco_vec;
    use futures::stream::{self, StreamExt};
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::rc::Rc;

    type SExpr = SpannedExpr;
    type Semantics = UntimedDsrvSemantics<LALRParser>;
    type TestCtx = Context<TestConfig>;

    fn to_stream(expr: SExpr, ctx: &TestCtx) -> OutputStream<Value> {
        <Semantics as MonitoringSemantics<TestConfig>>::to_async_stream(expr, ctx)
    }
    // ============================================================================
    // DEFER TESTS
    // ============================================================================

    #[apply(async_test)]
    async fn test_defer_int(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExpr::Defer(
            Box::new(SExpr::Val("x + 1")),
            StreamTypeAscription::Unascribed,
            eco_vec!["x".into()],
        );

        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![2.into(), 3.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_int_x_squared(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExpr::Defer(
            Box::new(SExpr::Val("x * x")),
            StreamTypeAscription::Unascribed,
            eco_vec!["x".into()],
        );

        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![4.into(), 9.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_bool(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExpr::Defer(
            Box::new(SExpr::Val("x && y")),
            StreamTypeAscription::Unascribed,
            eco_vec!["x".into(), "y".into()],
        );

        let x = Box::pin(stream::iter(vec![Value::Bool(true), Value::Bool(false)]));
        let y = Box::pin(stream::iter(vec![Value::Bool(true), Value::Bool(true)]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![Value::Bool(true), Value::Bool(false)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_with_deferred_value(executor: Rc<LocalExecutor<'static>>) {
        // Use a variable stream carrying Deferred instead of Val(Deferred),
        // because Val produces an infinite repeating stream via stream::repeat.
        let expr = SExpr::Defer(
            Box::new(SExpr::Var("e".into())),
            StreamTypeAscription::Unascribed,
            eco_vec!["e".into(), "x".into()],
        );

        let e = Box::pin(stream::iter(vec![Value::Deferred]));
        let x = Box::pin(stream::iter(vec![2.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![Value::Deferred];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_float(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExpr::Defer(
            Box::new(SExpr::Val("x + 1.5")),
            StreamTypeAscription::Unascribed,
            eco_vec!["x".into()],
        );

        let x = Box::pin(stream::iter(vec![Value::Float(1.0), Value::Float(2.0)]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![Value::Float(2.5), Value::Float(3.5)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_str(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExpr::Defer(
            Box::new(SExpr::Val("x ++ y")),
            StreamTypeAscription::Unascribed,
            eco_vec!["x".into(), "y".into()],
        );

        let x = Box::pin(stream::iter(vec![
            Value::Str("hello".into()),
            Value::Str("hi".into()),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Str(" world".into()),
            Value::Str(" there".into()),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![
            Value::Str("hello world".into()),
            Value::Str("hi there".into()),
        ];
        assert_eq!(res, exp);
    }

    // ============================================================================
    // DYNAMIC TESTS
    // ============================================================================

    #[apply(async_test)]
    async fn test_dynamic_int(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExpr::Dynamic(
            Box::new(SExpr::Var("e".into())),
            StreamTypeAscription::Unascribed,
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x + 1".into()),
            Value::Str("x + 2".into()),
        ]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![2.into(), 4.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_int_x_squared(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExpr::Dynamic(
            Box::new(SExpr::Var("e".into())),
            StreamTypeAscription::Unascribed,
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x * x".into()),
            Value::Str("x * x".into()),
        ]));
        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![4.into(), 9.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_with_start_deferred(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExpr::Dynamic(
            Box::new(SExpr::Var("e".into())),
            StreamTypeAscription::Unascribed,
        );

        let e = Box::pin(stream::iter(vec![
            Value::Deferred,
            Value::Str("x + 1".into()),
        ]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![Value::Deferred, 3.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_with_mid_deferred(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExpr::Dynamic(
            Box::new(SExpr::Var("e".into())),
            StreamTypeAscription::Unascribed,
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x + 1".into()),
            Value::Deferred,
            Value::Str("x + 2".into()),
        ]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![2.into(), Value::Deferred, 5.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_bool(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExpr::Dynamic(
            Box::new(SExpr::Var("e".into())),
            StreamTypeAscription::Unascribed,
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x && y".into()),
            Value::Str("x || y".into()),
        ]));
        let x = Box::pin(stream::iter(vec![Value::Bool(true), Value::Bool(false)]));
        let y = Box::pin(stream::iter(vec![Value::Bool(false), Value::Bool(true)]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into(), "y".into()],
            vec![e, x, y],
            10,
        );

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![Value::Bool(false), Value::Bool(true)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_float(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExpr::Dynamic(
            Box::new(SExpr::Var("e".into())),
            StreamTypeAscription::Unascribed,
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x + 1.5".into()),
            Value::Str("x * 2.0".into()),
        ]));
        let x = Box::pin(stream::iter(vec![Value::Float(1.0), Value::Float(2.0)]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![Value::Float(2.5), Value::Float(4.0)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_str(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExpr::Dynamic(
            Box::new(SExpr::Var("e".into())),
            StreamTypeAscription::Unascribed,
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x ++ y".into()),
            Value::Str("y ++ x".into()),
        ]));
        let x = Box::pin(stream::iter(vec![
            Value::Str("hello ".into()),
            Value::Str("hi ".into()),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Str("world".into()),
            Value::Str("there".into()),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into(), "y".into()],
            vec![e, x, y],
            10,
        );

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![
            Value::Str("hello world".into()),
            Value::Str("therehi ".into()),
        ];
        assert_eq!(res, exp);
    }
}
