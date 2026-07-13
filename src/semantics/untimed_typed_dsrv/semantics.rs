use std::collections::BTreeMap;

use super::combinators as mc;
use crate::VarName;
use crate::core::OutputStream;
use crate::core::PartialStreamValue;
use crate::core::RuntimeFunction;
use crate::core::Value;
use crate::core::stream_casting::{from_typed_stream, to_typed_partial_stream, to_typed_stream};
use crate::core::values::StreamType;
use crate::lang::core::parser::ExprParser;
use crate::lang::dsrv::ast::{
    BoolBinOp, CompBinOp, FloatBinOp, IntBinOp, NumericalBinOp, SBinOp, StrBinOp,
};
use crate::lang::dsrv::ast::{SExpr, SpannedExpr};
use crate::lang::dsrv::type_checker::{
    SExprAny, SExprBool, SExprFloat, SExprInt, SExprStr, SExprTE, SExprUnit, TypedApplyExpr,
    TypedFoldExpr, TypedFunctionExpr, TypedListExpr, TypedListExprKind, TypedMapExpr,
    TypedMapExprKind, TypedStructExpr, TypedStructExprKind, TypedTupleExpr, TypedTupleExprKind,
};
use crate::semantics::untimed_untyped_dsrv::combinators as uc;
use crate::semantics::{AsyncConfig, MonitoringSemantics, StreamContext};
use async_stream::stream;
use ecow::EcoVec;
use futures::StreamExt;

#[derive(Clone)]
pub struct TypedUntimedDsrvSemantics<Parser>
where
    Parser: ExprParser<SpannedExpr> + 'static,
{
    _parser: std::marker::PhantomData<Parser>,
}

impl<Parser, AC> MonitoringSemantics<AC> for TypedUntimedDsrvSemantics<Parser>
where
    Parser: ExprParser<SpannedExpr> + 'static,
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
{
    fn to_async_stream(expr: AC::Expr, ctx: &AC::Ctx) -> OutputStream<Value> {
        match expr {
            SExprTE::Int(e) => from_typed_stream::<PartialStreamValue<i64>>(to_async_stream_int::<
                AC,
                Parser,
            >(e, ctx)),
            SExprTE::Float(e) => from_typed_stream::<PartialStreamValue<f64>>(
                to_async_stream_float::<AC, Parser>(e, ctx),
            ),
            SExprTE::Str(e) => from_typed_stream::<PartialStreamValue<String>>(
                to_async_stream_str::<AC, Parser>(e, ctx),
            ),
            SExprTE::Bool(e) => from_typed_stream::<PartialStreamValue<bool>>(
                to_async_stream_bool::<AC, Parser>(e, ctx),
            ),
            SExprTE::Unit(e) => from_typed_stream::<PartialStreamValue<()>>(
                to_async_stream_unit::<AC, Parser>(e, ctx),
            ),
            SExprTE::List(tl) => from_typed_stream::<PartialStreamValue<EcoVec<Value>>>(
                eval_typed_list::<AC, Parser>(tl, ctx),
            ),
            SExprTE::Map(tm) => eval_typed_map::<AC, Parser>(tm, ctx),
            SExprTE::Struct(st) => eval_typed_struct::<AC, Parser>(st, ctx),
            SExprTE::Tuple(tuple) => eval_typed_tuple::<AC, Parser>(tuple, ctx),
            SExprTE::Function(func) => eval_typed_function::<AC, Parser>(func, ctx),
            SExprTE::Fold(fold) => eval_typed_fold::<AC, Parser>(fold, ctx),
            SExprTE::Apply(apply) => eval_typed_apply::<AC, Parser>(apply, ctx),
            SExprTE::Any(e) => eval_dyn::<AC, Parser>(e, ctx),
        }
    }
}

fn eval_dyn<AC, Parser>(expr: SExprAny, ctx: &AC::Ctx) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SpannedExpr> + 'static,
{
    match expr {
        SExprAny::Var(v) => ctx.var(&v).unwrap(),
        SExprAny::Val(v) => uc::val(v),
        SExprAny::Expr(e) => eval_untyped_expr::<AC, Parser>(e, ctx),
    }
}

fn eval_untyped_expr<AC, Parser>(expr: impl Into<SpannedExpr>, ctx: &AC::Ctx) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SpannedExpr> + 'static,
{
    let expr = expr.into().node;
    match expr {
        SExpr::Val(v) => uc::val(v),
        SExpr::Var(v) => uc::var::<AC>(ctx, v),
        SExpr::BinOp(e1, e2, op) => {
            let e1 = eval_untyped_expr::<AC, Parser>(*e1, ctx);
            let e2 = eval_untyped_expr::<AC, Parser>(*e2, ctx);
            match op {
                SBinOp::NOp(NumericalBinOp::Add) => uc::plus(e1, e2),
                SBinOp::NOp(NumericalBinOp::Sub) => uc::minus(e1, e2),
                SBinOp::NOp(NumericalBinOp::Mul) => uc::mult(e1, e2),
                SBinOp::NOp(NumericalBinOp::Div) => uc::div(e1, e2),
                SBinOp::NOp(NumericalBinOp::Mod) => uc::modulo(e1, e2),
                SBinOp::BOp(BoolBinOp::Or) => uc::or(e1, e2),
                SBinOp::BOp(BoolBinOp::And) => uc::and(e1, e2),
                SBinOp::BOp(BoolBinOp::Impl) => uc::implication(e1, e2),
                SBinOp::SOp(StrBinOp::Concat) => uc::concat(e1, e2),
                SBinOp::COp(CompBinOp::Eq) => uc::eq(e1, e2),
                SBinOp::COp(CompBinOp::Le) => uc::le(e1, e2),
                SBinOp::COp(CompBinOp::Lt) => uc::lt(e1, e2),
                SBinOp::COp(CompBinOp::Ge) => uc::ge(e1, e2),
                SBinOp::COp(CompBinOp::Gt) => uc::gt(e1, e2),
            }
        }
        SExpr::Not(e) => uc::not(eval_untyped_expr::<AC, Parser>(*e, ctx)),
        SExpr::Update(e1, e2) => uc::update(
            eval_untyped_expr::<AC, Parser>(*e1, ctx),
            eval_untyped_expr::<AC, Parser>(*e2, ctx),
        ),
        SExpr::Default(e, d) => uc::default(
            eval_untyped_expr::<AC, Parser>(*e, ctx),
            eval_untyped_expr::<AC, Parser>(*d, ctx),
        ),
        SExpr::IsDefined(e) => uc::is_defined(eval_untyped_expr::<AC, Parser>(*e, ctx)),
        SExpr::When(e) => uc::when(eval_untyped_expr::<AC, Parser>(*e, ctx)),
        SExpr::Latch(e1, e2) => uc::latch(
            eval_untyped_expr::<AC, Parser>(*e1, ctx),
            eval_untyped_expr::<AC, Parser>(*e2, ctx),
        ),
        SExpr::Init(e1, e2) => uc::init(
            eval_untyped_expr::<AC, Parser>(*e1, ctx),
            eval_untyped_expr::<AC, Parser>(*e2, ctx),
        ),
        SExpr::SIndex(e, i) => uc::sindex(eval_untyped_expr::<AC, Parser>(*e, ctx), i),
        SExpr::If(b, e1, e2) => uc::if_stm(
            eval_untyped_expr::<AC, Parser>(*b, ctx),
            eval_untyped_expr::<AC, Parser>(*e1, ctx),
            eval_untyped_expr::<AC, Parser>(*e2, ctx),
        ),
        SExpr::List(exprs) => uc::list(
            exprs
                .into_iter()
                .map(|e| eval_untyped_expr::<AC, Parser>(e, ctx))
                .collect(),
        ),
        SExpr::Tuple(exprs) => uc::tuple(
            exprs
                .into_iter()
                .map(|e| eval_untyped_expr::<AC, Parser>(e, ctx))
                .collect(),
        ),
        SExpr::LIndex(e, i) => uc::lindex(
            eval_untyped_expr::<AC, Parser>(*e, ctx),
            eval_untyped_expr::<AC, Parser>(*i, ctx),
        ),
        SExpr::LAppend(lst, el) => uc::lappend(
            eval_untyped_expr::<AC, Parser>(*lst, ctx),
            eval_untyped_expr::<AC, Parser>(*el, ctx),
        ),
        SExpr::LConcat(lst1, lst2) => uc::lconcat(
            eval_untyped_expr::<AC, Parser>(*lst1, ctx),
            eval_untyped_expr::<AC, Parser>(*lst2, ctx),
        ),
        SExpr::LHead(lst) => uc::lhead(eval_untyped_expr::<AC, Parser>(*lst, ctx)),
        SExpr::LTail(lst) => uc::ltail(eval_untyped_expr::<AC, Parser>(*lst, ctx)),
        SExpr::LLen(lst) => uc::llen(eval_untyped_expr::<AC, Parser>(*lst, ctx)),
        SExpr::Lambda(_, _) | SExpr::Apply(_, _) | SExpr::Fix(_) | SExpr::Partial(_, _) => {
            panic!("function values require typed DSRV semantics")
        }
        SExpr::LMap(_, _) | SExpr::LFilter(_, _) | SExpr::LFold(_, _, _) => {
            panic!("higher-order list operations require typed DSRV semantics")
        }
        SExpr::Map(map) | SExpr::Struct(map) | SExpr::ObjectLiteral(map) => uc::map(
            map.into_iter()
                .map(|(k, v)| (k, eval_untyped_expr::<AC, Parser>(v, ctx)))
                .collect::<BTreeMap<_, _>>(),
        ),
        SExpr::MGet(map, k) => uc::mget(eval_untyped_expr::<AC, Parser>(*map, ctx), k),
        SExpr::MRemove(map, k) => uc::mremove(eval_untyped_expr::<AC, Parser>(*map, ctx), k),
        SExpr::MInsert(map, k, v) => uc::minsert(
            eval_untyped_expr::<AC, Parser>(*map, ctx),
            k,
            eval_untyped_expr::<AC, Parser>(*v, ctx),
        ),
        SExpr::MHasKey(map, k) => uc::mhas_key(eval_untyped_expr::<AC, Parser>(*map, ctx), k),
        SExpr::Sin(v) => uc::sin(eval_untyped_expr::<AC, Parser>(*v, ctx)),
        SExpr::Cos(v) => uc::cos(eval_untyped_expr::<AC, Parser>(*v, ctx)),
        SExpr::Tan(v) => uc::tan(eval_untyped_expr::<AC, Parser>(*v, ctx)),
        SExpr::Abs(v) => uc::abs(eval_untyped_expr::<AC, Parser>(*v, ctx)),
        SExpr::Dynamic(_) | SExpr::Defer(_) => {
            panic!("dynamic/defer inside gradual Any fallback is not supported yet")
        }
        SExpr::SGet(_, _) => {
            panic!("dot field access is only supported for structs in typed semantics")
        }
        SExpr::MonitoredAt(_, _) => {
            unimplemented!("Function monitored_at only supported in distributed semantics")
        }
        SExpr::Dist(_, _) => {
            unimplemented!("Function dist only supported in distributed semantics")
        }
    }
}

fn function_source(func: &TypedFunctionExpr) -> String {
    match func.body.as_ref() {
        SExprTE::Any(SExprAny::Val(Value::Function(function))) => {
            function.display_source().to_string()
        }
        _ => format!("{}", func),
    }
}

fn function_body_with_value_args(
    func: &TypedFunctionExpr,
    args: EcoVec<Value>,
) -> anyhow::Result<SExprTE> {
    if func.params.len() != args.len() {
        return Err(anyhow::anyhow!(
            "Function expected {} arguments, got {}",
            func.params.len(),
            args.len()
        ));
    }

    let mut env = FunctionEnv::new();
    if let Some(name) = &func.recursive_name {
        env.insert(name.clone(), SExprTE::Function(func.clone()));
    }
    for ((name, typ), value) in func.params.iter().zip(args) {
        env.insert(name.clone(), typed_value_expr(value, typ));
    }
    Ok(subst_te((*func.body).clone(), &env))
}

fn eval_typed_function<AC, Parser>(func: TypedFunctionExpr, ctx: &AC::Ctx) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SpannedExpr> + 'static,
{
    if let Some(var) = typed_function_var(&func) {
        return ctx.var(var).unwrap();
    }

    let source = function_source(&func);
    let callable_func = func.clone();
    let callable_ctx = ctx.subcontext(0);
    let runtime_function = RuntimeFunction::native(source, move |args| {
        let body = function_body_with_value_args(&callable_func, args)?;
        Ok(<TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<
            AC,
        >>::to_async_stream(body, &callable_ctx))
    });
    uc::val(Value::Function(runtime_function))
}

fn typed_function_var(func: &TypedFunctionExpr) -> Option<&VarName> {
    if func.recursive_name.is_some() {
        return None;
    }
    let SExprTE::Any(SExprAny::Var(var)) = func.body.as_ref() else {
        return None;
    };
    if func
        .params
        .iter()
        .all(|(name, _)| name.name().starts_with('$') || name.name().starts_with("_f"))
    {
        Some(var)
    } else {
        None
    }
}

type FunctionEnv = BTreeMap<crate::VarName, SExprTE>;

fn typed_value_expr(value: Value, typ: &crate::lang::dsrv::type_checker::TCType) -> SExprTE {
    use crate::lang::dsrv::type_checker::TCType;
    match typ {
        TCType::Int => match value {
            Value::Int(v) => SExprTE::Int(SExprInt::Val(PartialStreamValue::Known(v))),
            Value::Deferred => SExprTE::Int(SExprInt::Val(PartialStreamValue::Deferred)),
            Value::NoVal => SExprTE::Int(SExprInt::Val(PartialStreamValue::NoVal)),
            other => panic!("expected Int function argument, got {}", other),
        },
        TCType::Float => match value {
            Value::Float(v) => SExprTE::Float(SExprFloat::Val(PartialStreamValue::Known(v))),
            Value::Deferred => SExprTE::Float(SExprFloat::Val(PartialStreamValue::Deferred)),
            Value::NoVal => SExprTE::Float(SExprFloat::Val(PartialStreamValue::NoVal)),
            other => panic!("expected Float function argument, got {}", other),
        },
        TCType::Str => match value {
            Value::Str(v) => SExprTE::Str(SExprStr::Val(PartialStreamValue::Known(v.into()))),
            Value::Deferred => SExprTE::Str(SExprStr::Val(PartialStreamValue::Deferred)),
            Value::NoVal => SExprTE::Str(SExprStr::Val(PartialStreamValue::NoVal)),
            other => panic!("expected Str function argument, got {}", other),
        },
        TCType::Bool => match value {
            Value::Bool(v) => SExprTE::Bool(SExprBool::Val(PartialStreamValue::Known(v))),
            Value::Deferred => SExprTE::Bool(SExprBool::Val(PartialStreamValue::Deferred)),
            Value::NoVal => SExprTE::Bool(SExprBool::Val(PartialStreamValue::NoVal)),
            other => panic!("expected Bool function argument, got {}", other),
        },
        TCType::Unit => match value {
            Value::Unit => SExprTE::Unit(SExprUnit::Val(PartialStreamValue::Known(()))),
            Value::Deferred => SExprTE::Unit(SExprUnit::Val(PartialStreamValue::Deferred)),
            Value::NoVal => SExprTE::Unit(SExprUnit::Val(PartialStreamValue::NoVal)),
            other => panic!("expected Unit function argument, got {}", other),
        },
        TCType::List(inner) => match value {
            Value::List(values) => SExprTE::List(TypedListExpr {
                element_type: *inner.clone(),
                kind: TypedListExprKind::Literal(
                    values
                        .into_iter()
                        .map(|value| typed_value_expr(value, inner))
                        .collect(),
                ),
            }),
            Value::Deferred => SExprTE::List(TypedListExpr {
                element_type: *inner.clone(),
                kind: TypedListExprKind::Literal(Vec::new()),
            }),
            other => panic!("expected List function argument, got {}", other),
        },
        TCType::Map(inner) => match value {
            Value::Map(values) => SExprTE::Map(TypedMapExpr {
                value_type: *inner.clone(),
                kind: TypedMapExprKind::Literal(
                    values
                        .into_iter()
                        .map(|(key, value)| (key, typed_value_expr(value, inner)))
                        .collect(),
                ),
            }),
            other => panic!("expected Map function argument, got {}", other),
        },
        TCType::Struct(fields, allow_extra_fields) => match value {
            Value::Map(values) => SExprTE::Struct(TypedStructExpr {
                typ_map: fields.clone(),
                allow_extra_fields: *allow_extra_fields,
                kind: TypedStructExprKind::Literal(
                    fields
                        .iter()
                        .map(|(key, typ)| {
                            let value = values.get(key).cloned().unwrap_or(Value::Unit);
                            (key.clone(), typed_value_expr(value, typ))
                        })
                        .collect(),
                ),
            }),
            other => panic!("expected Struct function argument, got {}", other),
        },
        TCType::Tuple(types) => match value {
            Value::Tuple(values) | Value::List(values) => SExprTE::Tuple(TypedTupleExpr {
                element_types: types.clone(),
                kind: TypedTupleExprKind::Literal(
                    values
                        .into_iter()
                        .zip(types.iter())
                        .map(|(value, typ)| typed_value_expr(value, typ))
                        .collect(),
                ),
            }),
            other => panic!("expected Tuple function argument, got {}", other),
        },
        TCType::Function(_, _) => match value {
            Value::Function(function) => SExprTE::Any(SExprAny::Val(Value::Function(function))),
            other => panic!("expected Function function argument, got {}", other),
        },
        TCType::Any | TCType::EmptyList | TCType::EmptyMap | TCType::Unknown => {
            SExprTE::Any(SExprAny::Val(value))
        }
    }
}

fn env_expr<'a>(env: &'a FunctionEnv, name: &crate::VarName) -> Option<&'a SExprTE> {
    env.get(name)
}

fn subst_te(expr: SExprTE, env: &FunctionEnv) -> SExprTE {
    match expr {
        SExprTE::Int(e) => SExprTE::Int(subst_int(e, env)),
        SExprTE::Float(e) => SExprTE::Float(subst_float(e, env)),
        SExprTE::Str(e) => SExprTE::Str(subst_str(e, env)),
        SExprTE::Bool(e) => SExprTE::Bool(subst_bool(e, env)),
        SExprTE::Unit(e) => SExprTE::Unit(subst_unit(e, env)),
        SExprTE::List(e) => SExprTE::List(subst_list(e, env)),
        SExprTE::Map(e) => SExprTE::Map(subst_map(e, env)),
        SExprTE::Struct(e) => SExprTE::Struct(subst_struct(e, env)),
        SExprTE::Tuple(e) => SExprTE::Tuple(subst_tuple(e, env)),
        SExprTE::Function(e) => {
            if let Some(var) = typed_function_var(&e) {
                if let Some(replacement) = env_expr(env, var) {
                    return replacement.clone();
                }
            }
            SExprTE::Function(subst_function(e, env))
        }
        SExprTE::Fold(e) => SExprTE::Fold(TypedFoldExpr {
            func: Box::new(subst_function(*e.func, env)),
            init: Box::new(subst_te(*e.init, env)),
            list: Box::new(subst_list(*e.list, env)),
            result_type: e.result_type,
        }),
        SExprTE::Apply(e) => {
            let func = match subst_te(SExprTE::Function(*e.func), env) {
                SExprTE::Function(func) => func,
                other => panic!("function substitution produced non-function {}", other),
            };
            SExprTE::Apply(TypedApplyExpr {
                func: Box::new(func),
                args: e.args.into_iter().map(|arg| subst_te(arg, env)).collect(),
                result_type: e.result_type,
            })
        }
        SExprTE::Any(SExprAny::Var(v)) => env_expr(env, &v)
            .cloned()
            .unwrap_or(SExprTE::Any(SExprAny::Var(v))),
        SExprTE::Any(e) => SExprTE::Any(e),
    }
}

fn subst_function(func: TypedFunctionExpr, env: &FunctionEnv) -> TypedFunctionExpr {
    let mut nested_env = env.clone();
    for (name, _) in &func.params {
        nested_env.remove(name);
    }
    if let Some(name) = &func.recursive_name {
        nested_env.remove(name);
    }
    TypedFunctionExpr {
        params: func.params,
        body: Box::new(subst_te(*func.body, &nested_env)),
        return_type: func.return_type,
        recursive_name: func.recursive_name,
    }
}

fn subst_int(expr: SExprInt, env: &FunctionEnv) -> SExprInt {
    use SExprInt::*;
    match expr {
        Var(v) => match env_expr(env, &v) {
            Some(SExprTE::Int(e)) => e.clone(),
            Some(other) => panic!("expected Int substitution for {}, got {}", v, other),
            None => Var(v),
        },
        Cast(e) => Cast(Box::new(subst_te(*e, env))),
        If(c, a, b) => If(
            Box::new(subst_bool(*c, env)),
            Box::new(subst_int(*a, env)),
            Box::new(subst_int(*b, env)),
        ),
        SIndex(e, i) => SIndex(Box::new(subst_int(*e, env)), i),
        BinOp(a, b, op) => BinOp(
            Box::new(subst_int(*a, env)),
            Box::new(subst_int(*b, env)),
            op,
        ),
        Default(a, b) => Default(Box::new(subst_int(*a, env)), Box::new(subst_int(*b, env))),
        Update(a, b) => Update(Box::new(subst_int(*a, env)), Box::new(subst_int(*b, env))),
        Latch(a, b) => Latch(Box::new(subst_int(*a, env)), Box::new(subst_int(*b, env))),
        Abs(e) => Abs(Box::new(subst_int(*e, env))),
        Init(a, b) => Init(Box::new(subst_int(*a, env)), Box::new(subst_int(*b, env))),
        Defer(e, t, vs) => Defer(Box::new(subst_str(*e, env)), t, vs),
        Dynamic(e, t) => Dynamic(Box::new(subst_str(*e, env)), t),
        RestrictedDynamic(e, vs, t) => RestrictedDynamic(Box::new(subst_str(*e, env)), vs, t),
        LLen(e) => LLen(subst_list(e, env)),
        LHeadList(e) => LHeadList(subst_list(e, env)),
        LIndexList(e, idx) => LIndexList(subst_list(e, env), Box::new(subst_int(*idx, env))),
        MGetMap(e, k) => MGetMap(subst_map(e, env), k),
        SGetStruct(e, k) => SGetStruct(subst_struct(e, env), k),
        SGetTuple(e, i) => SGetTuple(subst_tuple(e, env), i),
        Val(v) => Val(v),
    }
}

fn subst_float(expr: SExprFloat, env: &FunctionEnv) -> SExprFloat {
    use SExprFloat::*;
    match expr {
        Var(v) => match env_expr(env, &v) {
            Some(SExprTE::Float(e)) => e.clone(),
            Some(other) => panic!("expected Float substitution for {}, got {}", v, other),
            None => Var(v),
        },
        Cast(e) => Cast(Box::new(subst_te(*e, env))),
        If(c, a, b) => If(
            Box::new(subst_bool(*c, env)),
            Box::new(subst_float(*a, env)),
            Box::new(subst_float(*b, env)),
        ),
        SIndex(e, i) => SIndex(Box::new(subst_float(*e, env)), i),
        BinOp(a, b, op) => BinOp(
            Box::new(subst_float(*a, env)),
            Box::new(subst_float(*b, env)),
            op,
        ),
        Default(a, b) => Default(
            Box::new(subst_float(*a, env)),
            Box::new(subst_float(*b, env)),
        ),
        Update(a, b) => Update(
            Box::new(subst_float(*a, env)),
            Box::new(subst_float(*b, env)),
        ),
        Latch(a, b) => Latch(
            Box::new(subst_float(*a, env)),
            Box::new(subst_float(*b, env)),
        ),
        Sin(e) => Sin(Box::new(subst_float(*e, env))),
        Cos(e) => Cos(Box::new(subst_float(*e, env))),
        Tan(e) => Tan(Box::new(subst_float(*e, env))),
        Abs(e) => Abs(Box::new(subst_float(*e, env))),
        Init(a, b) => Init(
            Box::new(subst_float(*a, env)),
            Box::new(subst_float(*b, env)),
        ),
        Defer(e, t, vs) => Defer(Box::new(subst_str(*e, env)), t, vs),
        Dynamic(e, t) => Dynamic(Box::new(subst_str(*e, env)), t),
        RestrictedDynamic(e, vs, t) => RestrictedDynamic(Box::new(subst_str(*e, env)), vs, t),
        LHeadList(e) => LHeadList(subst_list(e, env)),
        LIndexList(e, idx) => LIndexList(subst_list(e, env), Box::new(subst_int(*idx, env))),
        MGetMap(e, k) => MGetMap(subst_map(e, env), k),
        SGetStruct(e, k) => SGetStruct(subst_struct(e, env), k),
        SGetTuple(e, i) => SGetTuple(subst_tuple(e, env), i),
        Val(v) => Val(v),
    }
}

fn subst_str(expr: SExprStr, env: &FunctionEnv) -> SExprStr {
    use SExprStr::*;
    match expr {
        Var(v) => match env_expr(env, &v) {
            Some(SExprTE::Str(e)) => e.clone(),
            Some(other) => panic!("expected Str substitution for {}, got {}", v, other),
            None => Var(v),
        },
        Cast(e) => Cast(Box::new(subst_te(*e, env))),
        If(c, a, b) => If(
            Box::new(subst_bool(*c, env)),
            Box::new(subst_str(*a, env)),
            Box::new(subst_str(*b, env)),
        ),
        SIndex(e, i) => SIndex(Box::new(subst_str(*e, env)), i),
        BinOp(a, b, op) => BinOp(
            Box::new(subst_str(*a, env)),
            Box::new(subst_str(*b, env)),
            op,
        ),
        Default(a, b) => Default(Box::new(subst_str(*a, env)), Box::new(subst_str(*b, env))),
        Update(a, b) => Update(Box::new(subst_str(*a, env)), Box::new(subst_str(*b, env))),
        Latch(a, b) => Latch(Box::new(subst_str(*a, env)), Box::new(subst_str(*b, env))),
        Init(a, b) => Init(Box::new(subst_str(*a, env)), Box::new(subst_str(*b, env))),
        Defer(e, t, vs) => Defer(Box::new(subst_str(*e, env)), t, vs),
        Dynamic(e, t) => Dynamic(Box::new(subst_str(*e, env)), t),
        RestrictedDynamic(e, vs, t) => RestrictedDynamic(Box::new(subst_str(*e, env)), vs, t),
        LHeadList(e) => LHeadList(subst_list(e, env)),
        LIndexList(e, idx) => LIndexList(subst_list(e, env), Box::new(subst_int(*idx, env))),
        MGetMap(e, k) => MGetMap(subst_map(e, env), k),
        SGetStruct(e, k) => SGetStruct(subst_struct(e, env), k),
        SGetTuple(e, i) => SGetTuple(subst_tuple(e, env), i),
        Val(v) => Val(v),
    }
}

fn subst_bool(expr: SExprBool, env: &FunctionEnv) -> SExprBool {
    use SExprBool::*;
    match expr {
        Var(v) => match env_expr(env, &v) {
            Some(SExprTE::Bool(e)) => e.clone(),
            Some(other) => panic!("expected Bool substitution for {}, got {}", v, other),
            None => Var(v),
        },
        Cast(e) => Cast(Box::new(subst_te(*e, env))),
        Cmp(op, a, b) => Cmp(op, Box::new(subst_te(*a, env)), Box::new(subst_te(*b, env))),
        BinOp(a, b, op) => BinOp(
            Box::new(subst_bool(*a, env)),
            Box::new(subst_bool(*b, env)),
            op,
        ),
        Not(e) => Not(Box::new(subst_bool(*e, env))),
        If(c, a, b) => If(
            Box::new(subst_bool(*c, env)),
            Box::new(subst_bool(*a, env)),
            Box::new(subst_bool(*b, env)),
        ),
        SIndex(e, i) => SIndex(Box::new(subst_bool(*e, env)), i),
        Default(a, b) => Default(Box::new(subst_bool(*a, env)), Box::new(subst_bool(*b, env))),
        Update(a, b) => Update(Box::new(subst_bool(*a, env)), Box::new(subst_bool(*b, env))),
        Latch(a, b) => Latch(Box::new(subst_bool(*a, env)), Box::new(subst_bool(*b, env))),
        Init(a, b) => Init(Box::new(subst_bool(*a, env)), Box::new(subst_bool(*b, env))),
        IsDefined(e) => IsDefined(Box::new(subst_te(*e, env))),
        When(e) => When(Box::new(subst_te(*e, env))),
        LHeadList(e) => LHeadList(subst_list(e, env)),
        LIndexList(e, idx) => LIndexList(subst_list(e, env), Box::new(subst_int(*idx, env))),
        MGetMap(e, k) => MGetMap(subst_map(e, env), k),
        SGetStruct(e, k) => SGetStruct(subst_struct(e, env), k),
        SGetTuple(e, i) => SGetTuple(subst_tuple(e, env), i),
        MHasKeyMap(e, k) => MHasKeyMap(subst_map(e, env), k),
        Defer(e, t, vs) => Defer(Box::new(subst_str(*e, env)), t, vs),
        Dynamic(e, t) => Dynamic(Box::new(subst_str(*e, env)), t),
        RestrictedDynamic(e, vs, t) => RestrictedDynamic(Box::new(subst_str(*e, env)), vs, t),
        Val(v) => Val(v),
    }
}

fn subst_unit(expr: SExprUnit, env: &FunctionEnv) -> SExprUnit {
    use SExprUnit::*;
    match expr {
        Var(v) => match env_expr(env, &v) {
            Some(SExprTE::Unit(e)) => e.clone(),
            Some(other) => panic!("expected Unit substitution for {}, got {}", v, other),
            None => Var(v),
        },
        Cast(e) => Cast(Box::new(subst_te(*e, env))),
        If(c, a, b) => If(
            Box::new(subst_bool(*c, env)),
            Box::new(subst_unit(*a, env)),
            Box::new(subst_unit(*b, env)),
        ),
        SIndex(e, i) => SIndex(Box::new(subst_unit(*e, env)), i),
        Default(a, b) => Default(Box::new(subst_unit(*a, env)), Box::new(subst_unit(*b, env))),
        Update(a, b) => Update(Box::new(subst_unit(*a, env)), Box::new(subst_unit(*b, env))),
        Latch(a, b) => Latch(Box::new(subst_unit(*a, env)), Box::new(subst_unit(*b, env))),
        Init(a, b) => Init(Box::new(subst_unit(*a, env)), Box::new(subst_unit(*b, env))),
        Defer(e, t, vs) => Defer(Box::new(subst_str(*e, env)), t, vs),
        Dynamic(e, t) => Dynamic(Box::new(subst_str(*e, env)), t),
        RestrictedDynamic(e, vs, t) => RestrictedDynamic(Box::new(subst_str(*e, env)), vs, t),
        LHeadList(e) => LHeadList(subst_list(e, env)),
        LIndexList(e, idx) => LIndexList(subst_list(e, env), Box::new(subst_int(*idx, env))),
        MGetMap(e, k) => MGetMap(subst_map(e, env), k),
        SGetStruct(e, k) => SGetStruct(subst_struct(e, env), k),
        SGetTuple(e, i) => SGetTuple(subst_tuple(e, env), i),
        Val(v) => Val(v),
    }
}

fn subst_list(expr: TypedListExpr, env: &FunctionEnv) -> TypedListExpr {
    let element_type = expr.element_type;
    let kind = match expr.kind {
        TypedListExprKind::Var(v) => match env_expr(env, &v) {
            Some(SExprTE::List(e)) => return e.clone(),
            Some(other) => panic!("expected List substitution for {}, got {}", v, other),
            None => TypedListExprKind::Var(v),
        },
        TypedListExprKind::If(c, a, b) => TypedListExprKind::If(
            Box::new(subst_bool(*c, env)),
            Box::new(subst_list(*a, env)),
            Box::new(subst_list(*b, env)),
        ),
        TypedListExprKind::SIndex(e, i) => {
            TypedListExprKind::SIndex(Box::new(subst_list(*e, env)), i)
        }
        TypedListExprKind::Default(a, b) => {
            TypedListExprKind::Default(Box::new(subst_list(*a, env)), Box::new(subst_list(*b, env)))
        }
        TypedListExprKind::Update(a, b) => {
            TypedListExprKind::Update(Box::new(subst_list(*a, env)), Box::new(subst_list(*b, env)))
        }
        TypedListExprKind::Latch(a, b) => {
            TypedListExprKind::Latch(Box::new(subst_list(*a, env)), Box::new(subst_list(*b, env)))
        }
        TypedListExprKind::Init(a, b) => {
            TypedListExprKind::Init(Box::new(subst_list(*a, env)), Box::new(subst_list(*b, env)))
        }
        TypedListExprKind::Defer(e, t, vs) => {
            TypedListExprKind::Defer(Box::new(subst_str(*e, env)), t, vs)
        }
        TypedListExprKind::Dynamic(e, t) => {
            TypedListExprKind::Dynamic(Box::new(subst_str(*e, env)), t)
        }
        TypedListExprKind::RestrictedDynamic(e, vs, t) => {
            TypedListExprKind::RestrictedDynamic(Box::new(subst_str(*e, env)), vs, t)
        }
        TypedListExprKind::Literal(items) => {
            TypedListExprKind::Literal(items.into_iter().map(|e| subst_te(e, env)).collect())
        }
        TypedListExprKind::LTail(e) => TypedListExprKind::LTail(Box::new(subst_list(*e, env))),
        TypedListExprKind::LConcat(a, b) => {
            TypedListExprKind::LConcat(Box::new(subst_list(*a, env)), Box::new(subst_list(*b, env)))
        }
        TypedListExprKind::LAppend(list, elem) => TypedListExprKind::LAppend(
            Box::new(subst_list(*list, env)),
            Box::new(subst_te(*elem, env)),
        ),
        TypedListExprKind::LMap(func, list) => TypedListExprKind::LMap(
            Box::new(subst_function(*func, env)),
            Box::new(subst_list(*list, env)),
        ),
        TypedListExprKind::LFilter(func, list) => TypedListExprKind::LFilter(
            Box::new(subst_function(*func, env)),
            Box::new(subst_list(*list, env)),
        ),
        TypedListExprKind::LHeadList(e) => {
            TypedListExprKind::LHeadList(Box::new(subst_list(*e, env)))
        }
        TypedListExprKind::LIndexList(e, idx) => TypedListExprKind::LIndexList(
            Box::new(subst_list(*e, env)),
            Box::new(subst_int(*idx, env)),
        ),
        TypedListExprKind::MGetMap(e, k) => {
            TypedListExprKind::MGetMap(Box::new(subst_map(*e, env)), k)
        }
        TypedListExprKind::SGetStruct(e, k) => {
            TypedListExprKind::SGetStruct(Box::new(subst_struct(*e, env)), k)
        }
        TypedListExprKind::SGetTuple(e, i) => {
            TypedListExprKind::SGetTuple(Box::new(subst_tuple(*e, env)), i)
        }
    };
    TypedListExpr { element_type, kind }
}

fn subst_map(expr: TypedMapExpr, env: &FunctionEnv) -> TypedMapExpr {
    let value_type = expr.value_type;
    let kind = match expr.kind {
        TypedMapExprKind::Var(v) => match env_expr(env, &v) {
            Some(SExprTE::Map(e)) => return e.clone(),
            Some(other) => panic!("expected Map substitution for {}, got {}", v, other),
            None => TypedMapExprKind::Var(v),
        },
        TypedMapExprKind::Literal(entries) => TypedMapExprKind::Literal(
            entries
                .into_iter()
                .map(|(k, v)| (k, subst_te(v, env)))
                .collect(),
        ),
        TypedMapExprKind::Default(a, b) => {
            TypedMapExprKind::Default(Box::new(subst_map(*a, env)), Box::new(subst_map(*b, env)))
        }
        TypedMapExprKind::If(c, a, b) => TypedMapExprKind::If(
            Box::new(subst_bool(*c, env)),
            Box::new(subst_map(*a, env)),
            Box::new(subst_map(*b, env)),
        ),
        TypedMapExprKind::Update(a, b) => {
            TypedMapExprKind::Update(Box::new(subst_map(*a, env)), Box::new(subst_map(*b, env)))
        }
        TypedMapExprKind::Latch(a, b) => {
            TypedMapExprKind::Latch(Box::new(subst_map(*a, env)), Box::new(subst_map(*b, env)))
        }
        TypedMapExprKind::Init(a, b) => {
            TypedMapExprKind::Init(Box::new(subst_map(*a, env)), Box::new(subst_map(*b, env)))
        }
        TypedMapExprKind::SIndex(e, i) => TypedMapExprKind::SIndex(Box::new(subst_map(*e, env)), i),
        TypedMapExprKind::Defer(e, t, vs) => {
            TypedMapExprKind::Defer(Box::new(subst_str(*e, env)), t, vs)
        }
        TypedMapExprKind::Dynamic(e, t) => {
            TypedMapExprKind::Dynamic(Box::new(subst_str(*e, env)), t)
        }
        TypedMapExprKind::RestrictedDynamic(e, vs, t) => {
            TypedMapExprKind::RestrictedDynamic(Box::new(subst_str(*e, env)), vs, t)
        }
        TypedMapExprKind::MInsert(map, k, v) => TypedMapExprKind::MInsert(
            Box::new(subst_map(*map, env)),
            k,
            Box::new(subst_te(*v, env)),
        ),
        TypedMapExprKind::MRemove(map, k) => {
            TypedMapExprKind::MRemove(Box::new(subst_map(*map, env)), k)
        }
        TypedMapExprKind::MGetMap(map, k) => {
            TypedMapExprKind::MGetMap(Box::new(subst_map(*map, env)), k)
        }
        TypedMapExprKind::SGetStruct(st, k) => {
            TypedMapExprKind::SGetStruct(Box::new(subst_struct(*st, env)), k)
        }
        TypedMapExprKind::SGetTuple(tuple, i) => {
            TypedMapExprKind::SGetTuple(Box::new(subst_tuple(*tuple, env)), i)
        }
        TypedMapExprKind::LHeadList(list) => {
            TypedMapExprKind::LHeadList(Box::new(subst_list(*list, env)))
        }
        TypedMapExprKind::LIndexList(list, idx) => TypedMapExprKind::LIndexList(
            Box::new(subst_list(*list, env)),
            Box::new(subst_int(*idx, env)),
        ),
    };
    TypedMapExpr { value_type, kind }
}

fn subst_struct(expr: TypedStructExpr, env: &FunctionEnv) -> TypedStructExpr {
    let typ_map = expr.typ_map;
    let allow_extra_fields = expr.allow_extra_fields;
    let kind = match expr.kind {
        TypedStructExprKind::Var(v) => match env_expr(env, &v) {
            Some(SExprTE::Struct(e)) => return e.clone(),
            Some(other) => panic!("expected Struct substitution for {}, got {}", v, other),
            None => TypedStructExprKind::Var(v),
        },
        TypedStructExprKind::Literal(entries) => TypedStructExprKind::Literal(
            entries
                .into_iter()
                .map(|(k, v)| (k, subst_te(v, env)))
                .collect(),
        ),
        TypedStructExprKind::Default(a, b) => TypedStructExprKind::Default(
            Box::new(subst_struct(*a, env)),
            Box::new(subst_struct(*b, env)),
        ),
        TypedStructExprKind::If(c, a, b) => TypedStructExprKind::If(
            Box::new(subst_bool(*c, env)),
            Box::new(subst_struct(*a, env)),
            Box::new(subst_struct(*b, env)),
        ),
        TypedStructExprKind::Update(a, b) => TypedStructExprKind::Update(
            Box::new(subst_struct(*a, env)),
            Box::new(subst_struct(*b, env)),
        ),
        TypedStructExprKind::Latch(a, b) => TypedStructExprKind::Latch(
            Box::new(subst_struct(*a, env)),
            Box::new(subst_struct(*b, env)),
        ),
        TypedStructExprKind::Init(a, b) => TypedStructExprKind::Init(
            Box::new(subst_struct(*a, env)),
            Box::new(subst_struct(*b, env)),
        ),
        TypedStructExprKind::SIndex(e, i) => {
            TypedStructExprKind::SIndex(Box::new(subst_struct(*e, env)), i)
        }
        TypedStructExprKind::Defer(e, t, vs) => {
            TypedStructExprKind::Defer(Box::new(subst_str(*e, env)), t, vs)
        }
        TypedStructExprKind::Dynamic(e, t) => {
            TypedStructExprKind::Dynamic(Box::new(subst_str(*e, env)), t)
        }
        TypedStructExprKind::RestrictedDynamic(e, vs, t) => {
            TypedStructExprKind::RestrictedDynamic(Box::new(subst_str(*e, env)), vs, t)
        }
        TypedStructExprKind::SUpdate(st, k, v) => TypedStructExprKind::SUpdate(
            Box::new(subst_struct(*st, env)),
            k,
            Box::new(subst_te(*v, env)),
        ),
        TypedStructExprKind::SGet(st, k) => {
            TypedStructExprKind::SGet(Box::new(subst_struct(*st, env)), k)
        }
        TypedStructExprKind::MGetMap(map, k) => {
            TypedStructExprKind::MGetMap(Box::new(subst_map(*map, env)), k)
        }
        TypedStructExprKind::LHeadList(list) => {
            TypedStructExprKind::LHeadList(Box::new(subst_list(*list, env)))
        }
        TypedStructExprKind::LIndexList(list, idx) => TypedStructExprKind::LIndexList(
            Box::new(subst_list(*list, env)),
            Box::new(subst_int(*idx, env)),
        ),
    };
    TypedStructExpr {
        typ_map,
        allow_extra_fields,
        kind,
    }
}

fn subst_tuple(expr: TypedTupleExpr, env: &FunctionEnv) -> TypedTupleExpr {
    let element_types = expr.element_types;
    let kind = match expr.kind {
        TypedTupleExprKind::Var(v) => match env_expr(env, &v) {
            Some(SExprTE::Tuple(e)) => return e.clone(),
            Some(other) => panic!("expected Tuple substitution for {}, got {}", v, other),
            None => TypedTupleExprKind::Var(v),
        },
        TypedTupleExprKind::Literal(items) => {
            TypedTupleExprKind::Literal(items.into_iter().map(|e| subst_te(e, env)).collect())
        }
        TypedTupleExprKind::Default(a, b) => TypedTupleExprKind::Default(
            Box::new(subst_tuple(*a, env)),
            Box::new(subst_tuple(*b, env)),
        ),
        TypedTupleExprKind::If(c, a, b) => TypedTupleExprKind::If(
            Box::new(subst_bool(*c, env)),
            Box::new(subst_tuple(*a, env)),
            Box::new(subst_tuple(*b, env)),
        ),
        TypedTupleExprKind::Update(a, b) => TypedTupleExprKind::Update(
            Box::new(subst_tuple(*a, env)),
            Box::new(subst_tuple(*b, env)),
        ),
        TypedTupleExprKind::Latch(a, b) => TypedTupleExprKind::Latch(
            Box::new(subst_tuple(*a, env)),
            Box::new(subst_tuple(*b, env)),
        ),
        TypedTupleExprKind::Init(a, b) => TypedTupleExprKind::Init(
            Box::new(subst_tuple(*a, env)),
            Box::new(subst_tuple(*b, env)),
        ),
        TypedTupleExprKind::SIndex(e, i) => {
            TypedTupleExprKind::SIndex(Box::new(subst_tuple(*e, env)), i)
        }
        TypedTupleExprKind::TGetTuple(e, i) => {
            TypedTupleExprKind::TGetTuple(Box::new(subst_tuple(*e, env)), i)
        }
    };
    TypedTupleExpr {
        element_types,
        kind,
    }
}
async fn eval_function_values_once<AC, Parser>(
    func: &TypedFunctionExpr,
    args: Vec<Value>,
    ctx: &AC::Ctx,
) -> Value
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SpannedExpr> + 'static,
{
    let body =
        function_body_with_value_args(func, args.into()).expect("Function application failed");
    let mut stream =
        <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(body, ctx);
    stream.next().await.unwrap_or(Value::NoVal)
}

fn eval_typed_apply<AC, Parser>(apply: TypedApplyExpr, ctx: &AC::Ctx) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SpannedExpr> + 'static,
{
    let func = *apply.func;
    let mut env = FunctionEnv::new();
    if let Some(name) = &func.recursive_name {
        env.insert(name.clone(), SExprTE::Function(func.clone()));
    }
    for ((name, _), arg) in func.params.iter().zip(apply.args.into_iter()) {
        env.insert(name.clone(), arg);
    }
    let body = subst_te(*func.body, &env);
    <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(body, ctx)
}

fn eval_typed_fold<AC, Parser>(fold: TypedFoldExpr, ctx: &AC::Ctx) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SpannedExpr> + 'static,
{
    let func = *fold.func;
    let mut init_stream =
        uc::stream_lift_base(<TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<
            AC,
        >>::to_async_stream(*fold.init, ctx));
    let mut list_stream = mc::stream_lift_base(eval_typed_list::<AC, Parser>(*fold.list, ctx));
    let ctx = ctx.subcontext(0);
    Box::pin(stream! {
        loop {
            let Some(init) = init_stream.next().await else {
                return;
            };
            let Some(list) = list_stream.next().await else {
                return;
            };
            let mut acc = init;
            match list {
                PartialStreamValue::Known(values) => {
                    for value in values {
                        acc = eval_function_values_once::<AC, Parser>(&func, vec![acc, value], &ctx).await;
                    }
                    yield acc;
                }
                PartialStreamValue::Deferred => yield Value::Deferred,
                PartialStreamValue::NoVal => yield Value::NoVal,
            }
        }
    })
}

fn eval_typed_list<AC, Parser>(typed_list: TypedListExpr, ctx: &AC::Ctx) -> mc::ListStream
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SpannedExpr> + 'static,
{
    let list_stream_type = typed_list
        .list_tc_type()
        .to_stream_type()
        .expect("list_tc_type should be a concrete type at runtime");

    match typed_list.kind {
        TypedListExprKind::Var(v) => to_typed_partial_stream::<EcoVec<Value>>(ctx.var(&v).unwrap()),
        TypedListExprKind::If(b, e1, e2) => {
            let b = to_async_stream_bool::<AC, Parser>(*b, ctx);
            let e1 = eval_typed_list::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_list::<AC, Parser>(*e2, ctx);
            mc::if_stm(b, e1, e2)
        }
        TypedListExprKind::SIndex(e, i) => {
            let e = eval_typed_list::<AC, Parser>(*e, ctx);
            mc::sindex(e, i)
        }
        TypedListExprKind::Default(e1, e2) => {
            let e1 = eval_typed_list::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_list::<AC, Parser>(*e2, ctx);
            mc::default(e1, e2)
        }
        TypedListExprKind::Update(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<EcoVec<Value>>>(eval_typed_list::<
                AC,
                Parser,
            >(*e1, ctx));
            let e2 = from_typed_stream::<PartialStreamValue<EcoVec<Value>>>(eval_typed_list::<
                AC,
                Parser,
            >(*e2, ctx));
            to_typed_partial_stream::<EcoVec<Value>>(uc::update(e1, e2))
        }
        TypedListExprKind::Latch(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<EcoVec<Value>>>(eval_typed_list::<
                AC,
                Parser,
            >(*e1, ctx));
            let e2 = from_typed_stream::<PartialStreamValue<EcoVec<Value>>>(eval_typed_list::<
                AC,
                Parser,
            >(*e2, ctx));
            to_typed_partial_stream::<EcoVec<Value>>(uc::latch(e1, e2))
        }
        TypedListExprKind::Init(e1, e2) => {
            let e1 = eval_typed_list::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_list::<AC, Parser>(*e2, ctx);
            mc::init(e1, e2)
        }
        TypedListExprKind::Defer(e, type_ctx, vs) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::defer::<AC, Parser, EcoVec<Value>>(ctx, e, vs, 1, &type_ctx, list_stream_type)
        }
        TypedListExprKind::Dynamic(e, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, EcoVec<Value>>(ctx, e, None, 1, &type_ctx, list_stream_type)
        }
        TypedListExprKind::RestrictedDynamic(e, vs, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, EcoVec<Value>>(
                ctx,
                e,
                vs.resolve(&type_ctx),
                1,
                &type_ctx,
                list_stream_type,
            )
        }
        TypedListExprKind::Literal(exprs) => {
            let streams: Vec<OutputStream<Value>> = exprs
                .into_iter()
                .map(|e| {
                    <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                        e, ctx,
                    )
                })
                .collect();
            mc::list(streams, list_stream_type)
        }
        TypedListExprKind::LTail(inner) => {
            let inner_stream = eval_typed_list::<AC, Parser>(*inner, ctx);
            mc::ltail(inner_stream)
        }
        TypedListExprKind::LConcat(a, b) => {
            let a_stream = eval_typed_list::<AC, Parser>(*a, ctx);
            let b_stream = eval_typed_list::<AC, Parser>(*b, ctx);
            mc::lconcat(a_stream, b_stream, list_stream_type)
        }
        TypedListExprKind::LAppend(list, elem) => {
            let list_stream = eval_typed_list::<AC, Parser>(*list, ctx);
            let elem_stream =
                <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                    *elem, ctx,
                );
            let elem_partial = mc::to_partial_value_stream(elem_stream);
            mc::lappend(list_stream, elem_partial, list_stream_type)
        }
        TypedListExprKind::LMap(func, list) => {
            let mut list_stream = eval_typed_list::<AC, Parser>(*list, ctx);
            let ctx = ctx.subcontext(0);
            Box::pin(stream! {
                while let Some(value) = list_stream.next().await {
                    match value {
                        PartialStreamValue::Known(values) => {
                            let mut mapped = EcoVec::new();
                            for value in values {
                                mapped.push(eval_function_values_once::<AC, Parser>(&func, vec![value], &ctx).await);
                            }
                            yield PartialStreamValue::Known(mapped);
                        }
                        PartialStreamValue::Deferred => yield PartialStreamValue::Deferred,
                        PartialStreamValue::NoVal => yield PartialStreamValue::NoVal,
                    }
                }
            })
        }
        TypedListExprKind::LFilter(func, list) => {
            let mut list_stream = eval_typed_list::<AC, Parser>(*list, ctx);
            let ctx = ctx.subcontext(0);
            Box::pin(stream! {
                while let Some(value) = list_stream.next().await {
                    match value {
                        PartialStreamValue::Known(values) => {
                            let mut filtered = EcoVec::new();
                            for value in values {
                                match eval_function_values_once::<AC, Parser>(&func, vec![value.clone()], &ctx).await {
                                    Value::Bool(true) => filtered.push(value),
                                    Value::Bool(false) => {}
                                    other => panic!("List.filter returned non-bool value {}", other),
                                }
                            }
                            yield PartialStreamValue::Known(filtered);
                        }
                        PartialStreamValue::Deferred => yield PartialStreamValue::Deferred,
                        PartialStreamValue::NoVal => yield PartialStreamValue::NoVal,
                    }
                }
            })
        }
        TypedListExprKind::LHeadList(inner_list) => {
            let inner_stream = eval_typed_list::<AC, Parser>(*inner_list, ctx);
            mc::lhead::<EcoVec<Value>>(inner_stream)
        }
        TypedListExprKind::LIndexList(inner_list, idx) => {
            let inner_stream = eval_typed_list::<AC, Parser>(*inner_list, ctx);
            let idx_stream = to_async_stream_int::<AC, Parser>(*idx, ctx);
            mc::lindex::<EcoVec<Value>>(inner_stream, idx_stream)
        }
        TypedListExprKind::MGetMap(map, key) => to_typed_partial_stream::<EcoVec<Value>>(uc::mget(
            eval_typed_map::<AC, Parser>(*map, ctx),
            key,
        )),
        TypedListExprKind::SGetStruct(st, key) => to_typed_partial_stream::<EcoVec<Value>>(
            uc::mget(eval_typed_struct::<AC, Parser>(*st, ctx), key),
        ),
        TypedListExprKind::SGetTuple(tuple, idx) => to_typed_partial_stream::<EcoVec<Value>>(
            uc::tget(eval_typed_tuple::<AC, Parser>(*tuple, ctx), idx),
        ),
    }
}

fn eval_typed_struct<AC, Parser>(
    typed_struct: TypedStructExpr,
    ctx: &AC::Ctx,
) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SpannedExpr> + 'static,
{
    let typed_struct_stream_type = typed_struct
        .to_stream_type()
        .expect("struct type should be concrete at runtime");

    match typed_struct.kind {
        TypedStructExprKind::Var(v) => ctx.var(&v).unwrap(),
        TypedStructExprKind::Literal(entries) => {
            let streams = entries
                .into_iter()
                .map(|(k, e)| {
                    (
                        k,
                        <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                            e, ctx,
                        ),
                    )
                })
                .collect();
            uc::map(streams)
        }
        TypedStructExprKind::Default(e1, e2) => {
            let e1 = eval_typed_struct::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_struct::<AC, Parser>(*e2, ctx);
            uc::default(e1, e2)
        }
        TypedStructExprKind::Update(e1, e2) => {
            let e1 = eval_typed_struct::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_struct::<AC, Parser>(*e2, ctx);
            uc::update(e1, e2)
        }
        TypedStructExprKind::Latch(e1, e2) => {
            let e1 = eval_typed_struct::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_struct::<AC, Parser>(*e2, ctx);
            uc::latch(e1, e2)
        }
        TypedStructExprKind::If(b, e1, e2) => {
            let b = from_typed_stream::<PartialStreamValue<bool>>(
                to_async_stream_bool::<AC, Parser>(*b, ctx),
            );
            let e1 = eval_typed_struct::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_struct::<AC, Parser>(*e2, ctx);
            uc::if_stm(b, e1, e2)
        }
        TypedStructExprKind::Init(e1, e2) => {
            let e1 = eval_typed_struct::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_struct::<AC, Parser>(*e2, ctx);
            uc::init(e1, e2)
        }
        TypedStructExprKind::SIndex(e, i) => {
            let e = eval_typed_struct::<AC, Parser>(*e, ctx);
            uc::sindex(e, i)
        }
        TypedStructExprKind::SUpdate(st, key, value) => {
            let struct_stream = eval_typed_struct::<AC, Parser>(*st, ctx);
            let value_stream =
                <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                    *value, ctx,
                );
            uc::minsert(struct_stream, key, value_stream)
        }
        TypedStructExprKind::SGet(st, key) => {
            uc::mget(eval_typed_struct::<AC, Parser>(*st, ctx), key)
        }
        TypedStructExprKind::MGetMap(map, key) => {
            uc::mget(eval_typed_map::<AC, Parser>(*map, ctx), key)
        }
        TypedStructExprKind::LHeadList(inner_list) => {
            let inner_stream = eval_typed_list::<AC, Parser>(*inner_list, ctx);
            from_typed_stream::<PartialStreamValue<Value>>(mc::lhead::<Value>(inner_stream))
        }
        TypedStructExprKind::LIndexList(inner_list, idx) => {
            let inner_stream = eval_typed_list::<AC, Parser>(*inner_list, ctx);
            let idx_stream = to_async_stream_int::<AC, Parser>(*idx, ctx);
            from_typed_stream::<PartialStreamValue<Value>>(mc::lindex::<Value>(
                inner_stream,
                idx_stream,
            ))
        }
        TypedStructExprKind::Defer(e, type_ctx, vs) => {
            let stream_type = typed_struct_stream_type.clone();
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            from_typed_stream::<PartialStreamValue<Value>>(mc::defer::<AC, Parser, Value>(
                ctx,
                e,
                vs,
                1,
                &type_ctx,
                stream_type,
            ))
        }
        TypedStructExprKind::Dynamic(e, type_ctx) => {
            let stream_type = typed_struct_stream_type.clone();
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            from_typed_stream::<PartialStreamValue<Value>>(mc::dynamic::<AC, Parser, Value>(
                ctx,
                e,
                None,
                1,
                &type_ctx,
                stream_type,
            ))
        }
        TypedStructExprKind::RestrictedDynamic(e, vs, type_ctx) => {
            let stream_type = typed_struct_stream_type.clone();
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            from_typed_stream::<PartialStreamValue<Value>>(mc::dynamic::<AC, Parser, Value>(
                ctx,
                e,
                vs.resolve(&type_ctx),
                1,
                &type_ctx,
                stream_type,
            ))
        }
    }
}

fn eval_typed_tuple<AC, Parser>(typed_tuple: TypedTupleExpr, ctx: &AC::Ctx) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SpannedExpr> + 'static,
{
    match typed_tuple.kind {
        TypedTupleExprKind::Var(v) => ctx.var(&v).unwrap(),
        TypedTupleExprKind::Literal(items) => {
            let streams = items
                .into_iter()
                .map(|e| {
                    <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                        e, ctx,
                    )
                })
                .collect();
            uc::tuple(streams)
        }
        TypedTupleExprKind::Default(e1, e2) => {
            let e1 = eval_typed_tuple::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_tuple::<AC, Parser>(*e2, ctx);
            uc::default(e1, e2)
        }
        TypedTupleExprKind::Update(e1, e2) => {
            let e1 = eval_typed_tuple::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_tuple::<AC, Parser>(*e2, ctx);
            uc::update(e1, e2)
        }
        TypedTupleExprKind::Latch(e1, e2) => {
            let e1 = eval_typed_tuple::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_tuple::<AC, Parser>(*e2, ctx);
            uc::latch(e1, e2)
        }
        TypedTupleExprKind::If(b, e1, e2) => {
            let b = from_typed_stream::<PartialStreamValue<bool>>(
                to_async_stream_bool::<AC, Parser>(*b, ctx),
            );
            let e1 = eval_typed_tuple::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_tuple::<AC, Parser>(*e2, ctx);
            uc::if_stm(b, e1, e2)
        }
        TypedTupleExprKind::Init(e1, e2) => {
            let e1 = eval_typed_tuple::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_tuple::<AC, Parser>(*e2, ctx);
            uc::init(e1, e2)
        }
        TypedTupleExprKind::SIndex(e, i) => {
            let e = eval_typed_tuple::<AC, Parser>(*e, ctx);
            uc::sindex(e, i)
        }
        TypedTupleExprKind::TGetTuple(e, idx) => {
            uc::tget(eval_typed_tuple::<AC, Parser>(*e, ctx), idx)
        }
    }
}

fn eval_typed_map<AC, Parser>(typed_map: TypedMapExpr, ctx: &AC::Ctx) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SpannedExpr> + 'static,
{
    let typed_map_stream_type = typed_map
        .map_tc_type()
        .to_stream_type()
        .expect("map type should be concrete at runtime");

    match typed_map.kind {
        TypedMapExprKind::Var(v) => ctx.var(&v).unwrap(),
        TypedMapExprKind::Literal(entries) => {
            let streams = entries
                .into_iter()
                .map(|(k, e)| {
                    (
                        k,
                        <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                            e, ctx,
                        ),
                    )
                })
                .collect();
            uc::map(streams)
        }
        TypedMapExprKind::Default(e1, e2) => {
            let e1 = eval_typed_map::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_map::<AC, Parser>(*e2, ctx);
            uc::default(e1, e2)
        }
        TypedMapExprKind::Update(e1, e2) => {
            let e1 = eval_typed_map::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_map::<AC, Parser>(*e2, ctx);
            uc::update(e1, e2)
        }
        TypedMapExprKind::Latch(e1, e2) => {
            let e1 = eval_typed_map::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_map::<AC, Parser>(*e2, ctx);
            uc::latch(e1, e2)
        }
        TypedMapExprKind::If(b, e1, e2) => {
            let b = from_typed_stream::<PartialStreamValue<bool>>(
                to_async_stream_bool::<AC, Parser>(*b, ctx),
            );
            let e1 = eval_typed_map::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_map::<AC, Parser>(*e2, ctx);
            uc::if_stm(b, e1, e2)
        }
        TypedMapExprKind::Init(e1, e2) => {
            let e1 = eval_typed_map::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_map::<AC, Parser>(*e2, ctx);
            uc::init(e1, e2)
        }
        TypedMapExprKind::SIndex(e, i) => {
            let e = eval_typed_map::<AC, Parser>(*e, ctx);
            uc::sindex(e, i)
        }
        TypedMapExprKind::MInsert(map, key, value) => {
            let map_stream = eval_typed_map::<AC, Parser>(*map, ctx);
            let value_stream =
                <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                    *value, ctx,
                );
            uc::minsert(map_stream, key, value_stream)
        }
        TypedMapExprKind::MRemove(map, key) => {
            let map_stream = eval_typed_map::<AC, Parser>(*map, ctx);
            uc::mremove(map_stream, key)
        }
        TypedMapExprKind::MGetMap(map, key) => {
            uc::mget(eval_typed_map::<AC, Parser>(*map, ctx), key)
        }
        TypedMapExprKind::SGetStruct(st, key) => {
            uc::mget(eval_typed_struct::<AC, Parser>(*st, ctx), key)
        }
        TypedMapExprKind::SGetTuple(tuple, idx) => {
            uc::tget(eval_typed_tuple::<AC, Parser>(*tuple, ctx), idx)
        }
        TypedMapExprKind::LHeadList(inner_list) => {
            let inner_stream = eval_typed_list::<AC, Parser>(*inner_list, ctx);
            from_typed_stream::<PartialStreamValue<Value>>(mc::lhead::<Value>(inner_stream))
        }
        TypedMapExprKind::LIndexList(inner_list, idx) => {
            let inner_stream = eval_typed_list::<AC, Parser>(*inner_list, ctx);
            let idx_stream = to_async_stream_int::<AC, Parser>(*idx, ctx);
            from_typed_stream::<PartialStreamValue<Value>>(mc::lindex::<Value>(
                inner_stream,
                idx_stream,
            ))
        }
        TypedMapExprKind::Defer(e, type_ctx, vs) => {
            let stream_type = typed_map_stream_type.clone();
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            from_typed_stream::<PartialStreamValue<Value>>(mc::defer::<AC, Parser, Value>(
                ctx,
                e,
                vs,
                1,
                &type_ctx,
                stream_type,
            ))
        }
        TypedMapExprKind::Dynamic(e, type_ctx) => {
            let stream_type = typed_map_stream_type.clone();
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            from_typed_stream::<PartialStreamValue<Value>>(mc::dynamic::<AC, Parser, Value>(
                ctx,
                e,
                None,
                1,
                &type_ctx,
                stream_type,
            ))
        }
        TypedMapExprKind::RestrictedDynamic(e, vs, type_ctx) => {
            let stream_type = typed_map_stream_type.clone();
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            from_typed_stream::<PartialStreamValue<Value>>(mc::dynamic::<AC, Parser, Value>(
                ctx,
                e,
                vs.resolve(&type_ctx),
                1,
                &type_ctx,
                stream_type,
            ))
        }
    }
}

fn to_async_stream_int<AC, Parser>(
    expr: SExprInt,
    ctx: &AC::Ctx,
) -> OutputStream<PartialStreamValue<i64>>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SpannedExpr> + 'static,
{
    match expr {
        SExprInt::Cast(e) => to_typed_partial_stream::<i64>(
            <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                *e, ctx,
            ),
        ),
        SExprInt::Val(v) => mc::val(v),
        SExprInt::BinOp(e1, e2, op) => {
            let e1 = to_async_stream_int::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_int::<AC, Parser>(*e2, ctx);
            match op {
                IntBinOp::Add => mc::plus(e1, e2),
                IntBinOp::Sub => mc::minus(e1, e2),
                IntBinOp::Mul => mc::mult(e1, e2),
                IntBinOp::Div => mc::div(e1, e2),
                IntBinOp::Mod => mc::modulo(e1, e2),
            }
        }
        SExprInt::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
        SExprInt::SIndex(e, i) => {
            let e = to_async_stream_int::<AC, Parser>(*e, ctx);
            mc::sindex(e, i)
        }
        SExprInt::If(b, e1, e2) => {
            let b = to_async_stream_bool::<AC, Parser>(*b, ctx);
            let e1 = to_async_stream_int::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_int::<AC, Parser>(*e2, ctx);
            mc::if_stm(b, e1, e2)
        }
        SExprInt::Default(e1, e2) => {
            let e1 = to_async_stream_int::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_int::<AC, Parser>(*e2, ctx);
            mc::default(e1, e2)
        }
        SExprInt::Update(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<i64>>(
                to_async_stream_int::<AC, Parser>(*e1, ctx),
            );
            let e2 = from_typed_stream::<PartialStreamValue<i64>>(
                to_async_stream_int::<AC, Parser>(*e2, ctx),
            );
            to_typed_partial_stream::<i64>(uc::update(e1, e2))
        }
        SExprInt::Latch(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<i64>>(
                to_async_stream_int::<AC, Parser>(*e1, ctx),
            );
            let e2 = from_typed_stream::<PartialStreamValue<i64>>(
                to_async_stream_int::<AC, Parser>(*e2, ctx),
            );
            to_typed_partial_stream::<i64>(uc::latch(e1, e2))
        }
        SExprInt::Defer(e, type_ctx, vs) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::defer::<AC, Parser, i64>(ctx, e, vs, 1, &type_ctx, StreamType::Int)
        }
        SExprInt::Dynamic(e, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, i64>(ctx, e, None, 1, &type_ctx, StreamType::Int)
        }
        SExprInt::RestrictedDynamic(e, vs, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, i64>(
                ctx,
                e,
                vs.resolve(&type_ctx),
                1,
                &type_ctx,
                StreamType::Int,
            )
        }
        SExprInt::Abs(e) => {
            let e = to_async_stream_int::<AC, Parser>(*e, ctx);
            mc::abs_int(e)
        }
        SExprInt::Init(e1, e2) => {
            let e1 = to_async_stream_int::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_int::<AC, Parser>(*e2, ctx);
            mc::init(e1, e2)
        }
        SExprInt::LLen(typed_list) => {
            let list_stream = eval_typed_list::<AC, Parser>(typed_list, ctx);
            mc::llen(list_stream)
        }
        SExprInt::LHeadList(typed_list) => {
            let list_stream = eval_typed_list::<AC, Parser>(typed_list, ctx);
            mc::lhead::<i64>(list_stream)
        }
        SExprInt::LIndexList(typed_list, idx) => {
            let list_stream = eval_typed_list::<AC, Parser>(typed_list, ctx);
            let idx_stream = to_async_stream_int::<AC, Parser>(*idx, ctx);
            mc::lindex::<i64>(list_stream, idx_stream)
        }
        SExprInt::MGetMap(map, key) => {
            to_typed_partial_stream::<i64>(uc::mget(eval_typed_map::<AC, Parser>(map, ctx), key))
        }
        SExprInt::SGetStruct(st, key) => {
            to_typed_partial_stream::<i64>(uc::mget(eval_typed_struct::<AC, Parser>(st, ctx), key))
        }
        SExprInt::SGetTuple(tuple, idx) => to_typed_partial_stream::<i64>(uc::tget(
            eval_typed_tuple::<AC, Parser>(tuple, ctx),
            idx,
        )),
    }
}

fn to_async_stream_float<AC, Parser>(
    expr: SExprFloat,
    ctx: &AC::Ctx,
) -> OutputStream<PartialStreamValue<f64>>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SpannedExpr> + 'static,
{
    match expr {
        SExprFloat::Cast(e) => to_typed_partial_stream::<f64>(
            <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                *e, ctx,
            ),
        ),
        SExprFloat::Val(v) => mc::val(v),
        SExprFloat::BinOp(e1, e2, op) => {
            let e1 = to_async_stream_float::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_float::<AC, Parser>(*e2, ctx);
            match op {
                FloatBinOp::Add => mc::plus(e1, e2),
                FloatBinOp::Sub => mc::minus(e1, e2),
                FloatBinOp::Mul => mc::mult(e1, e2),
                FloatBinOp::Div => mc::div(e1, e2),
                FloatBinOp::Mod => mc::modulo(e1, e2),
            }
        }
        SExprFloat::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
        SExprFloat::SIndex(e, i) => {
            let e = to_async_stream_float::<AC, Parser>(*e, ctx);
            mc::sindex(e, i)
        }
        SExprFloat::If(b, e1, e2) => {
            let b = to_async_stream_bool::<AC, Parser>(*b, ctx);
            let e1 = to_async_stream_float::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_float::<AC, Parser>(*e2, ctx);
            mc::if_stm(b, e1, e2)
        }
        SExprFloat::Default(e1, e2) => {
            let e1 = to_async_stream_float::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_float::<AC, Parser>(*e2, ctx);
            mc::default(e1, e2)
        }
        SExprFloat::Update(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<f64>>(to_async_stream_float::<
                AC,
                Parser,
            >(*e1, ctx));
            let e2 = from_typed_stream::<PartialStreamValue<f64>>(to_async_stream_float::<
                AC,
                Parser,
            >(*e2, ctx));
            to_typed_partial_stream::<f64>(uc::update(e1, e2))
        }
        SExprFloat::Latch(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<f64>>(to_async_stream_float::<
                AC,
                Parser,
            >(*e1, ctx));
            let e2 = from_typed_stream::<PartialStreamValue<f64>>(to_async_stream_float::<
                AC,
                Parser,
            >(*e2, ctx));
            to_typed_partial_stream::<f64>(uc::latch(e1, e2))
        }
        SExprFloat::Defer(e, type_ctx, vs) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::defer::<AC, Parser, f64>(ctx, e, vs, 1, &type_ctx, StreamType::Float)
        }
        SExprFloat::Dynamic(e, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, f64>(ctx, e, None, 1, &type_ctx, StreamType::Float)
        }
        SExprFloat::RestrictedDynamic(e, vs, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, f64>(
                ctx,
                e,
                vs.resolve(&type_ctx),
                1,
                &type_ctx,
                StreamType::Float,
            )
        }
        SExprFloat::Sin(e) => {
            let e = to_async_stream_float::<AC, Parser>(*e, ctx);
            mc::sin(e)
        }
        SExprFloat::Cos(e) => {
            let e = to_async_stream_float::<AC, Parser>(*e, ctx);
            mc::cos(e)
        }
        SExprFloat::Tan(e) => {
            let e = to_async_stream_float::<AC, Parser>(*e, ctx);
            mc::tan(e)
        }
        SExprFloat::Abs(e) => {
            let e = to_async_stream_float::<AC, Parser>(*e, ctx);
            mc::abs_float(e)
        }
        SExprFloat::Init(e1, e2) => {
            let e1 = to_async_stream_float::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_float::<AC, Parser>(*e2, ctx);
            mc::init(e1, e2)
        }
        SExprFloat::LHeadList(typed_list) => {
            let list_stream = eval_typed_list::<AC, Parser>(typed_list, ctx);
            mc::lhead::<f64>(list_stream)
        }
        SExprFloat::LIndexList(typed_list, idx) => {
            let list_stream = eval_typed_list::<AC, Parser>(typed_list, ctx);
            let idx_stream = to_async_stream_int::<AC, Parser>(*idx, ctx);
            mc::lindex::<f64>(list_stream, idx_stream)
        }
        SExprFloat::MGetMap(map, key) => {
            to_typed_partial_stream::<f64>(uc::mget(eval_typed_map::<AC, Parser>(map, ctx), key))
        }
        SExprFloat::SGetStruct(st, key) => {
            to_typed_partial_stream::<f64>(uc::mget(eval_typed_struct::<AC, Parser>(st, ctx), key))
        }
        SExprFloat::SGetTuple(tuple, idx) => to_typed_partial_stream::<f64>(uc::tget(
            eval_typed_tuple::<AC, Parser>(tuple, ctx),
            idx,
        )),
    }
}

fn to_async_stream_str<AC, Parser>(
    expr: SExprStr,
    ctx: &AC::Ctx,
) -> OutputStream<PartialStreamValue<String>>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SpannedExpr> + 'static,
{
    match expr {
        SExprStr::Cast(e) => to_typed_partial_stream::<String>(
            <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                *e, ctx,
            ),
        ),
        SExprStr::Val(v) => mc::val(v),
        SExprStr::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
        SExprStr::SIndex(e, i) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::sindex(e, i)
        }
        SExprStr::If(b, e1, e2) => {
            let b = to_async_stream_bool::<AC, Parser>(*b, ctx);
            let e1 = to_async_stream_str::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_str::<AC, Parser>(*e2, ctx);
            mc::if_stm(b, e1, e2)
        }
        SExprStr::Dynamic(e, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, String>(ctx, e, None, 1, &type_ctx, StreamType::Str)
        }
        SExprStr::RestrictedDynamic(e, vs, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, String>(
                ctx,
                e,
                vs.resolve(&type_ctx),
                1,
                &type_ctx,
                StreamType::Str,
            )
        }
        SExprStr::BinOp(e1, e2, op) => {
            let e1 = to_async_stream_str::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_str::<AC, Parser>(*e2, ctx);
            match op {
                StrBinOp::Concat => mc::concat(e1, e2),
            }
        }
        SExprStr::Default(e1, e2) => {
            let e1 = to_async_stream_str::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_str::<AC, Parser>(*e2, ctx);
            mc::default(e1, e2)
        }
        SExprStr::Update(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<String>>(to_async_stream_str::<
                AC,
                Parser,
            >(*e1, ctx));
            let e2 = from_typed_stream::<PartialStreamValue<String>>(to_async_stream_str::<
                AC,
                Parser,
            >(*e2, ctx));
            to_typed_partial_stream::<String>(uc::update(e1, e2))
        }
        SExprStr::Latch(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<String>>(to_async_stream_str::<
                AC,
                Parser,
            >(*e1, ctx));
            let e2 = from_typed_stream::<PartialStreamValue<String>>(to_async_stream_str::<
                AC,
                Parser,
            >(*e2, ctx));
            to_typed_partial_stream::<String>(uc::latch(e1, e2))
        }
        SExprStr::Defer(e, type_ctx, vs) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::defer::<AC, Parser, String>(ctx, e, vs, 1, &type_ctx, StreamType::Str)
        }
        SExprStr::Init(e1, e2) => {
            let e1 = to_async_stream_str::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_str::<AC, Parser>(*e2, ctx);
            mc::init(e1, e2)
        }
        SExprStr::LHeadList(typed_list) => {
            let list_stream = eval_typed_list::<AC, Parser>(typed_list, ctx);
            mc::lhead::<String>(list_stream)
        }
        SExprStr::LIndexList(typed_list, idx) => {
            let list_stream = eval_typed_list::<AC, Parser>(typed_list, ctx);
            let idx_stream = to_async_stream_int::<AC, Parser>(*idx, ctx);
            mc::lindex::<String>(list_stream, idx_stream)
        }
        SExprStr::MGetMap(map, key) => {
            to_typed_partial_stream::<String>(uc::mget(eval_typed_map::<AC, Parser>(map, ctx), key))
        }
        SExprStr::SGetStruct(st, key) => to_typed_partial_stream::<String>(uc::mget(
            eval_typed_struct::<AC, Parser>(st, ctx),
            key,
        )),
        SExprStr::SGetTuple(tuple, idx) => to_typed_partial_stream::<String>(uc::tget(
            eval_typed_tuple::<AC, Parser>(tuple, ctx),
            idx,
        )),
    }
}

/// Evaluate a `Cmp` node by dispatching on the operand type and comparison operator.
fn eval_cmp<AC, Parser>(
    op: CompBinOp,
    e1: SExprTE,
    e2: SExprTE,
    ctx: &AC::Ctx,
) -> OutputStream<PartialStreamValue<bool>>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SpannedExpr> + 'static,
{
    match (e1, e2) {
        (SExprTE::Int(a), SExprTE::Int(b)) => {
            let a = to_async_stream_int::<AC, Parser>(a, ctx);
            let b = to_async_stream_int::<AC, Parser>(b, ctx);
            match op {
                CompBinOp::Eq => mc::eq(a, b),
                CompBinOp::Le => mc::le(a, b),
                CompBinOp::Lt => mc::lt(a, b),
                CompBinOp::Ge => mc::ge(a, b),
                CompBinOp::Gt => mc::gt(a, b),
            }
        }
        (SExprTE::Float(a), SExprTE::Float(b)) => {
            let a = to_async_stream_float::<AC, Parser>(a, ctx);
            let b = to_async_stream_float::<AC, Parser>(b, ctx);
            match op {
                CompBinOp::Eq => mc::eq_partial(a, b),
                CompBinOp::Le => mc::le_partial(a, b),
                CompBinOp::Lt => mc::lt(a, b),
                CompBinOp::Ge => mc::ge(a, b),
                CompBinOp::Gt => mc::gt(a, b),
            }
        }
        (SExprTE::Str(a), SExprTE::Str(b)) => {
            let a = to_async_stream_str::<AC, Parser>(a, ctx);
            let b = to_async_stream_str::<AC, Parser>(b, ctx);
            match op {
                CompBinOp::Eq => mc::eq(a, b),
                CompBinOp::Le => mc::le_partial(a, b),
                CompBinOp::Lt => mc::lt(a, b),
                CompBinOp::Ge => mc::ge(a, b),
                CompBinOp::Gt => mc::gt(a, b),
            }
        }
        (SExprTE::Bool(a), SExprTE::Bool(b)) => {
            let a = to_async_stream_bool::<AC, Parser>(a, ctx);
            let b = to_async_stream_bool::<AC, Parser>(b, ctx);
            // Only Eq is valid for Bool (enforced by the type checker)
            mc::eq(a, b)
        }
        (SExprTE::Unit(a), SExprTE::Unit(b)) => {
            let a = to_async_stream_unit::<AC, Parser>(a, ctx);
            let b = to_async_stream_unit::<AC, Parser>(b, ctx);
            // Only Eq is valid for Unit (enforced by the type checker)
            mc::eq(a, b)
        }
        (SExprTE::Any(a), SExprTE::Any(b)) => {
            let a = eval_dyn::<AC, Parser>(a, ctx);
            let b = eval_dyn::<AC, Parser>(b, ctx);
            to_typed_partial_stream::<bool>(match op {
                CompBinOp::Eq => uc::eq(a, b),
                CompBinOp::Le => uc::le(a, b),
                CompBinOp::Lt => uc::lt(a, b),
                CompBinOp::Ge => uc::ge(a, b),
                CompBinOp::Gt => uc::gt(a, b),
            })
        }
        _ => panic!("eval_cmp: type checker should have ensured operand types match"),
    }
}

/// Evaluate an `IsDefined` node by dispatching on the inner expression type.
fn eval_is_defined<AC, Parser>(
    inner: SExprTE,
    ctx: &AC::Ctx,
) -> OutputStream<PartialStreamValue<bool>>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SpannedExpr> + 'static,
{
    match inner {
        SExprTE::Int(e) => mc::is_defined(to_async_stream_int::<AC, Parser>(e, ctx)),
        SExprTE::Float(e) => mc::is_defined(to_async_stream_float::<AC, Parser>(e, ctx)),
        SExprTE::Str(e) => mc::is_defined(to_async_stream_str::<AC, Parser>(e, ctx)),
        SExprTE::Bool(e) => mc::is_defined(to_async_stream_bool::<AC, Parser>(e, ctx)),
        SExprTE::Unit(e) => mc::is_defined(to_async_stream_unit::<AC, Parser>(e, ctx)),
        SExprTE::List(tl) => mc::is_defined_list(eval_typed_list::<AC, Parser>(tl, ctx)),
        SExprTE::Map(e) => {
            to_typed_partial_stream::<bool>(uc::is_defined(eval_typed_map::<AC, Parser>(e, ctx)))
        }
        SExprTE::Struct(e) => {
            to_typed_partial_stream::<bool>(uc::is_defined(eval_typed_struct::<AC, Parser>(e, ctx)))
        }
        SExprTE::Tuple(e) => {
            to_typed_partial_stream::<bool>(uc::is_defined(eval_typed_tuple::<AC, Parser>(e, ctx)))
        }
        SExprTE::Function(e) => to_typed_partial_stream::<bool>(uc::is_defined(
            eval_typed_function::<AC, Parser>(e, ctx),
        )),
        SExprTE::Fold(e) => {
            to_typed_partial_stream::<bool>(uc::is_defined(eval_typed_fold::<AC, Parser>(e, ctx)))
        }
        SExprTE::Apply(e) => {
            to_typed_partial_stream::<bool>(uc::is_defined(eval_typed_apply::<AC, Parser>(e, ctx)))
        }
        SExprTE::Any(e) => {
            to_typed_partial_stream::<bool>(uc::is_defined(eval_dyn::<AC, Parser>(e, ctx)))
        }
    }
}

/// Evaluate a `When` node by dispatching on the inner expression type.
fn eval_when<AC, Parser>(inner: SExprTE, ctx: &AC::Ctx) -> OutputStream<PartialStreamValue<bool>>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SpannedExpr> + 'static,
{
    match inner {
        SExprTE::Int(e) => mc::when(to_async_stream_int::<AC, Parser>(e, ctx)),
        SExprTE::Float(e) => mc::when(to_async_stream_float::<AC, Parser>(e, ctx)),
        SExprTE::Str(e) => mc::when(to_async_stream_str::<AC, Parser>(e, ctx)),
        SExprTE::Bool(e) => mc::when(to_async_stream_bool::<AC, Parser>(e, ctx)),
        SExprTE::Unit(e) => mc::when(to_async_stream_unit::<AC, Parser>(e, ctx)),
        SExprTE::List(tl) => mc::when_list(eval_typed_list::<AC, Parser>(tl, ctx)),
        SExprTE::Map(e) => {
            to_typed_partial_stream::<bool>(uc::when(eval_typed_map::<AC, Parser>(e, ctx)))
        }
        SExprTE::Struct(e) => {
            to_typed_partial_stream::<bool>(uc::when(eval_typed_struct::<AC, Parser>(e, ctx)))
        }
        SExprTE::Tuple(e) => {
            to_typed_partial_stream::<bool>(uc::when(eval_typed_tuple::<AC, Parser>(e, ctx)))
        }
        SExprTE::Function(e) => {
            to_typed_partial_stream::<bool>(uc::when(eval_typed_function::<AC, Parser>(e, ctx)))
        }
        SExprTE::Fold(e) => {
            to_typed_partial_stream::<bool>(uc::when(eval_typed_fold::<AC, Parser>(e, ctx)))
        }
        SExprTE::Apply(e) => {
            to_typed_partial_stream::<bool>(uc::when(eval_typed_apply::<AC, Parser>(e, ctx)))
        }
        SExprTE::Any(e) => {
            to_typed_partial_stream::<bool>(uc::when(eval_dyn::<AC, Parser>(e, ctx)))
        }
    }
}

fn to_async_stream_bool<AC, Parser>(
    expr: SExprBool,
    ctx: &AC::Ctx,
) -> OutputStream<PartialStreamValue<bool>>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SpannedExpr> + 'static,
{
    match expr {
        SExprBool::Cast(e) => to_typed_partial_stream::<bool>(
            <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                *e, ctx,
            ),
        ),
        SExprBool::Val(b) => mc::val(b),
        SExprBool::Cmp(op, e1, e2) => eval_cmp::<AC, Parser>(op, *e1, *e2, ctx),
        SExprBool::BinOp(e1, e2, op) => {
            let e1 = to_async_stream_bool::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_bool::<AC, Parser>(*e2, ctx);
            match op {
                BoolBinOp::And => mc::and(e1, e2),
                BoolBinOp::Or => mc::or(e1, e2),
                BoolBinOp::Impl => mc::implication(e1, e2),
            }
        }
        SExprBool::Not(b) => {
            let b = to_async_stream_bool::<AC, Parser>(*b, ctx);
            mc::not(b)
        }
        SExprBool::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
        SExprBool::SIndex(e, i) => {
            let e = to_async_stream_bool::<AC, Parser>(*e, ctx);
            mc::sindex(e, i)
        }
        SExprBool::If(b, e1, e2) => {
            let b2 = to_async_stream_bool::<AC, Parser>(*b, ctx);
            let e1 = to_async_stream_bool::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_bool::<AC, Parser>(*e2, ctx);
            mc::if_stm(b2, e1, e2)
        }
        SExprBool::Default(e1, e2) => {
            let e1 = to_async_stream_bool::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_bool::<AC, Parser>(*e2, ctx);
            mc::default(e1, e2)
        }
        SExprBool::Update(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<bool>>(to_async_stream_bool::<
                AC,
                Parser,
            >(*e1, ctx));
            let e2 = from_typed_stream::<PartialStreamValue<bool>>(to_async_stream_bool::<
                AC,
                Parser,
            >(*e2, ctx));
            to_typed_partial_stream::<bool>(uc::update(e1, e2))
        }
        SExprBool::Latch(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<bool>>(to_async_stream_bool::<
                AC,
                Parser,
            >(*e1, ctx));
            let e2 = from_typed_stream::<PartialStreamValue<bool>>(to_async_stream_bool::<
                AC,
                Parser,
            >(*e2, ctx));
            to_typed_partial_stream::<bool>(uc::latch(e1, e2))
        }
        SExprBool::Defer(e, type_ctx, vs) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::defer::<AC, Parser, bool>(ctx, e, vs, 1, &type_ctx, StreamType::Bool)
        }
        SExprBool::Dynamic(e, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, bool>(ctx, e, None, 1, &type_ctx, StreamType::Bool)
        }
        SExprBool::RestrictedDynamic(e, vs, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, bool>(
                ctx,
                e,
                vs.resolve(&type_ctx),
                1,
                &type_ctx,
                StreamType::Bool,
            )
        }
        SExprBool::IsDefined(inner) => eval_is_defined::<AC, Parser>(*inner, ctx),
        SExprBool::When(inner) => eval_when::<AC, Parser>(*inner, ctx),
        SExprBool::Init(e1, e2) => {
            let e1 = to_async_stream_bool::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_bool::<AC, Parser>(*e2, ctx);
            mc::init(e1, e2)
        }
        SExprBool::LHeadList(typed_list) => {
            let list_stream = eval_typed_list::<AC, Parser>(typed_list, ctx);
            mc::lhead::<bool>(list_stream)
        }
        SExprBool::LIndexList(typed_list, idx) => {
            let list_stream = eval_typed_list::<AC, Parser>(typed_list, ctx);
            let idx_stream = to_async_stream_int::<AC, Parser>(*idx, ctx);
            mc::lindex::<bool>(list_stream, idx_stream)
        }
        SExprBool::MGetMap(map, key) => {
            to_typed_partial_stream::<bool>(uc::mget(eval_typed_map::<AC, Parser>(map, ctx), key))
        }
        SExprBool::SGetStruct(st, key) => {
            to_typed_partial_stream::<bool>(uc::mget(eval_typed_struct::<AC, Parser>(st, ctx), key))
        }
        SExprBool::SGetTuple(tuple, idx) => to_typed_partial_stream::<bool>(uc::tget(
            eval_typed_tuple::<AC, Parser>(tuple, ctx),
            idx,
        )),
        SExprBool::MHasKeyMap(map, key) => to_typed_partial_stream::<bool>(uc::mhas_key(
            eval_typed_map::<AC, Parser>(map, ctx),
            key,
        )),
    }
}

fn to_async_stream_unit<AC, Parser>(
    expr: SExprUnit,
    ctx: &AC::Ctx,
) -> OutputStream<PartialStreamValue<()>>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SpannedExpr> + 'static,
{
    match expr {
        SExprUnit::Cast(e) => to_typed_partial_stream::<()>(
            <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                *e, ctx,
            ),
        ),
        SExprUnit::Val(v) => mc::val(v),
        SExprUnit::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
        SExprUnit::SIndex(e, i) => {
            let e = to_async_stream_unit::<AC, Parser>(*e, ctx);
            mc::sindex(e, i)
        }
        SExprUnit::If(b, e1, e2) => {
            let b = to_async_stream_bool::<AC, Parser>(*b, ctx);
            let e1 = to_async_stream_unit::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_unit::<AC, Parser>(*e2, ctx);
            mc::if_stm(b, e1, e2)
        }
        SExprUnit::Default(e1, e2) => {
            let e1 = to_async_stream_unit::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_unit::<AC, Parser>(*e2, ctx);
            mc::default(e1, e2)
        }
        SExprUnit::Update(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<()>>(
                to_async_stream_unit::<AC, Parser>(*e1, ctx),
            );
            let e2 = from_typed_stream::<PartialStreamValue<()>>(
                to_async_stream_unit::<AC, Parser>(*e2, ctx),
            );
            to_typed_partial_stream::<()>(uc::update(e1, e2))
        }
        SExprUnit::Latch(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<()>>(
                to_async_stream_unit::<AC, Parser>(*e1, ctx),
            );
            let e2 = from_typed_stream::<PartialStreamValue<()>>(
                to_async_stream_unit::<AC, Parser>(*e2, ctx),
            );
            to_typed_partial_stream::<()>(uc::latch(e1, e2))
        }
        SExprUnit::Defer(e, type_info, vs) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::defer::<AC, Parser, ()>(ctx, e, vs, 1, &type_info, StreamType::Unit)
        }
        SExprUnit::Dynamic(e, type_info) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, ()>(ctx, e, None, 1, &type_info, StreamType::Unit)
        }
        SExprUnit::RestrictedDynamic(e, vs, type_info) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, ()>(
                ctx,
                e,
                vs.resolve(&type_info),
                1,
                &type_info,
                StreamType::Unit,
            )
        }
        SExprUnit::Init(e1, e2) => {
            let e1 = to_async_stream_unit::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_unit::<AC, Parser>(*e2, ctx);
            mc::init(e1, e2)
        }
        SExprUnit::LHeadList(typed_list) => {
            let list_stream = eval_typed_list::<AC, Parser>(typed_list, ctx);
            mc::lhead::<()>(list_stream)
        }
        SExprUnit::LIndexList(typed_list, idx) => {
            let list_stream = eval_typed_list::<AC, Parser>(typed_list, ctx);
            let idx_stream = to_async_stream_int::<AC, Parser>(*idx, ctx);
            mc::lindex::<()>(list_stream, idx_stream)
        }
        SExprUnit::MGetMap(map, key) => {
            to_typed_partial_stream::<()>(uc::mget(eval_typed_map::<AC, Parser>(map, ctx), key))
        }
        SExprUnit::SGetStruct(st, key) => {
            to_typed_partial_stream::<()>(uc::mget(eval_typed_struct::<AC, Parser>(st, ctx), key))
        }
        SExprUnit::SGetTuple(tuple, idx) => {
            to_typed_partial_stream::<()>(uc::tget(eval_typed_tuple::<AC, Parser>(tuple, ctx), idx))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::StreamType;
    use crate::dsrv_fixtures::TestTypedConfig;
    use crate::lang::dsrv::type_checker::TypeInfo;
    use crate::runtime::asynchronous::Context;
    use crate::{async_test, lang::dsrv::lalr_parser::LALRParser};
    use ecow::eco_vec;
    use futures::stream::{self, StreamExt};
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::rc::Rc;

    type TestCtx = Context<TestTypedConfig>;

    fn type_info(vars: &[(&str, StreamType)]) -> TypeInfo {
        vars.iter().map(|(v, t)| ((*v).into(), t.clone())).collect()
    }

    #[apply(async_test)]
    async fn test_defer_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Val(PartialStreamValue::Known("x + 1".into())));
        let defer_expr = SExprInt::Defer(
            e_str,
            type_info(&[("x", StreamType::Int)]),
            eco_vec!["x".into()],
        );

        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(2), PartialStreamValue::Known(3)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_int_x_squared_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Val(PartialStreamValue::Known("x * x".into())));
        let defer_expr = SExprInt::Defer(
            e_str,
            type_info(&[("x", StreamType::Int)]),
            eco_vec!["x".into()],
        );

        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(4), PartialStreamValue::Known(9)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_bool_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Val(PartialStreamValue::Known("x && y".into())));
        let defer_expr = SExprBool::Defer(
            e_str,
            type_info(&[("x", StreamType::Bool), ("y", StreamType::Bool)]),
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

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_with_deferred_value_runtime(executor: Rc<LocalExecutor<'static>>) {
        // Use a variable stream carrying Deferred instead of Val(Deferred),
        // because Val produces an infinite repeating stream via stream::repeat.
        let e_str = Box::new(SExprStr::Var("e".into()));
        let defer_expr = SExprInt::Defer(
            e_str,
            type_info(&[("e", StreamType::Str), ("x", StreamType::Int)]),
            eco_vec!["x".into(), "e".into()],
        );

        let e = Box::pin(stream::iter(vec![Value::Deferred]));
        let x = Box::pin(stream::iter(vec![2.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> = vec![PartialStreamValue::Deferred];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprInt::Dynamic(
            e_str,
            type_info(&[("e", StreamType::Str), ("x", StreamType::Int)]),
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

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(2), PartialStreamValue::Known(4)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_int_x_squared_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprInt::Dynamic(
            e_str,
            type_info(&[("e", StreamType::Str), ("x", StreamType::Int)]),
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

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(4), PartialStreamValue::Known(9)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_with_start_deferred_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprInt::Dynamic(
            e_str,
            type_info(&[("e", StreamType::Str), ("x", StreamType::Int)]),
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

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Deferred, PartialStreamValue::Known(3)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_with_mid_deferred_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprInt::Dynamic(
            e_str,
            type_info(&[("e", StreamType::Str), ("x", StreamType::Int)]),
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

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Known(2),
            PartialStreamValue::Deferred,
            PartialStreamValue::Known(5),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_bool_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprBool::Dynamic(
            e_str,
            type_info(&[
                ("e", StreamType::Str),
                ("x", StreamType::Bool),
                ("y", StreamType::Bool),
            ]),
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

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Val(PartialStreamValue::Known("x + 1.5".into())));
        let defer_expr = SExprFloat::Defer(
            e_str,
            type_info(&[("x", StreamType::Float)]),
            eco_vec!["x".into()],
        );

        let x = Box::pin(stream::iter(vec![Value::Float(1.0), Value::Float(2.0)]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_float::<TestTypedConfig, LALRParser>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<f64>> = vec![
            PartialStreamValue::Known(2.5),
            PartialStreamValue::Known(3.5),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Val(PartialStreamValue::Known("x ++ y".into())));
        let defer_expr = SExprStr::Defer(
            e_str,
            type_info(&[("x", StreamType::Str), ("y", StreamType::Str)]),
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

        let res_stream = to_async_stream_str::<TestTypedConfig, LALRParser>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<String>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<String>> = vec![
            PartialStreamValue::Known("hello world".into()),
            PartialStreamValue::Known("hi there".into()),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprStr::Dynamic(
            e_str,
            type_info(&[
                ("e", StreamType::Str),
                ("x", StreamType::Str),
                ("y", StreamType::Str),
            ]),
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

        let res_stream = to_async_stream_str::<TestTypedConfig, LALRParser>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<String>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<String>> = vec![
            PartialStreamValue::Known("hello world".into()),
            PartialStreamValue::Known("therehi ".into()),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprFloat::Dynamic(
            e_str,
            type_info(&[("e", StreamType::Str), ("x", StreamType::Float)]),
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

        let res_stream = to_async_stream_float::<TestTypedConfig, LALRParser>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<f64>> = vec![
            PartialStreamValue::Known(2.5),
            PartialStreamValue::Known(4.0),
        ];
        assert_eq!(res, exp);
    }

    // ========== Tests for new comparison operators ==========

    #[apply(async_test)]
    async fn test_lt_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Lt,
            Box::new(SExprTE::Int(SExprInt::Var("x".into()))),
            Box::new(SExprTE::Int(SExprInt::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![1.into(), 5.into(), 3.into()]));
        let y = Box::pin(stream::iter(vec![2.into(), 5.into(), 1.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_ge_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Ge,
            Box::new(SExprTE::Int(SExprInt::Var("x".into()))),
            Box::new(SExprTE::Int(SExprInt::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![1.into(), 5.into(), 3.into()]));
        let y = Box::pin(stream::iter(vec![2.into(), 5.into(), 1.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_gt_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Gt,
            Box::new(SExprTE::Int(SExprInt::Var("x".into()))),
            Box::new(SExprTE::Int(SExprInt::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![1.into(), 5.into(), 3.into()]));
        let y = Box::pin(stream::iter(vec![2.into(), 5.into(), 1.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_eq_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Eq,
            Box::new(SExprTE::Float(SExprFloat::Var("x".into()))),
            Box::new(SExprTE::Float(SExprFloat::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![Value::Float(1.0), Value::Float(3.0)]));
        let y = Box::pin(stream::iter(vec![Value::Float(1.0), Value::Float(2.0)]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_le_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Le,
            Box::new(SExprTE::Float(SExprFloat::Var("x".into()))),
            Box::new(SExprTE::Float(SExprFloat::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![
            Value::Float(1.0),
            Value::Float(3.0),
            Value::Float(2.0),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Float(2.0),
            Value::Float(3.0),
            Value::Float(1.0),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_lt_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Lt,
            Box::new(SExprTE::Float(SExprFloat::Var("x".into()))),
            Box::new(SExprTE::Float(SExprFloat::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![
            Value::Float(1.0),
            Value::Float(3.0),
            Value::Float(2.0),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Float(2.0),
            Value::Float(3.0),
            Value::Float(1.0),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_ge_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Ge,
            Box::new(SExprTE::Float(SExprFloat::Var("x".into()))),
            Box::new(SExprTE::Float(SExprFloat::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![
            Value::Float(1.0),
            Value::Float(3.0),
            Value::Float(2.0),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Float(2.0),
            Value::Float(3.0),
            Value::Float(1.0),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_gt_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Gt,
            Box::new(SExprTE::Float(SExprFloat::Var("x".into()))),
            Box::new(SExprTE::Float(SExprFloat::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![
            Value::Float(1.0),
            Value::Float(3.0),
            Value::Float(2.0),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Float(2.0),
            Value::Float(3.0),
            Value::Float(1.0),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_le_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Le,
            Box::new(SExprTE::Str(SExprStr::Var("x".into()))),
            Box::new(SExprTE::Str(SExprStr::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![
            Value::Str("apple".into()),
            Value::Str("banana".into()),
            Value::Str("cherry".into()),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Str("banana".into()),
            Value::Str("banana".into()),
            Value::Str("banana".into()),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_lt_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Lt,
            Box::new(SExprTE::Str(SExprStr::Var("x".into()))),
            Box::new(SExprTE::Str(SExprStr::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![
            Value::Str("apple".into()),
            Value::Str("banana".into()),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Str("banana".into()),
            Value::Str("banana".into()),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_ge_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Ge,
            Box::new(SExprTE::Str(SExprStr::Var("x".into()))),
            Box::new(SExprTE::Str(SExprStr::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![
            Value::Str("apple".into()),
            Value::Str("cherry".into()),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Str("banana".into()),
            Value::Str("banana".into()),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_gt_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Gt,
            Box::new(SExprTE::Str(SExprStr::Var("x".into()))),
            Box::new(SExprTE::Str(SExprStr::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![
            Value::Str("cherry".into()),
            Value::Str("apple".into()),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Str("banana".into()),
            Value::Str("banana".into()),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    // ========== Tests for trigonometric functions ==========

    #[apply(async_test)]
    async fn test_sin_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprFloat::Sin(Box::new(SExprFloat::Var("x".into())));

        let x = Box::pin(stream::iter(vec![
            Value::Float(0.0),
            Value::Float(std::f64::consts::FRAC_PI_2),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_float::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        assert_eq!(res.len(), 2);
        match &res[0] {
            PartialStreamValue::Known(v) => assert!((v - 0.0).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
        match &res[1] {
            PartialStreamValue::Known(v) => assert!((v - 1.0).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
    }

    #[apply(async_test)]
    async fn test_cos_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprFloat::Cos(Box::new(SExprFloat::Var("x".into())));

        let x = Box::pin(stream::iter(vec![
            Value::Float(0.0),
            Value::Float(std::f64::consts::PI),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_float::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        assert_eq!(res.len(), 2);
        match &res[0] {
            PartialStreamValue::Known(v) => assert!((v - 1.0).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
        match &res[1] {
            PartialStreamValue::Known(v) => assert!((v - (-1.0)).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
    }

    #[apply(async_test)]
    async fn test_tan_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprFloat::Tan(Box::new(SExprFloat::Var("x".into())));

        let x = Box::pin(stream::iter(vec![
            Value::Float(0.0),
            Value::Float(std::f64::consts::FRAC_PI_4),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_float::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        assert_eq!(res.len(), 2);
        match &res[0] {
            PartialStreamValue::Known(v) => assert!((v - 0.0).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
        match &res[1] {
            PartialStreamValue::Known(v) => assert!((v - 1.0).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
    }

    // ========== Tests for abs ==========

    #[apply(async_test)]
    async fn test_abs_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprInt::Abs(Box::new(SExprInt::Var("x".into())));

        let x = Box::pin(stream::iter(vec![
            Value::Int(-5),
            Value::Int(3),
            Value::Int(0),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Known(5),
            PartialStreamValue::Known(3),
            PartialStreamValue::Known(0),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_abs_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprFloat::Abs(Box::new(SExprFloat::Var("x".into())));

        let x = Box::pin(stream::iter(vec![
            Value::Float(-5.5),
            Value::Float(3.2),
            Value::Float(0.0),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_float::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        assert_eq!(res.len(), 3);
        match &res[0] {
            PartialStreamValue::Known(v) => assert!((v - 5.5).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
        match &res[1] {
            PartialStreamValue::Known(v) => assert!((v - 3.2).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
        match &res[2] {
            PartialStreamValue::Known(v) => assert!((v - 0.0).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
    }

    // ========== Tests for is_defined ==========

    #[apply(async_test)]
    async fn test_is_defined_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::IsDefined(Box::new(SExprTE::Int(SExprInt::Var("x".into()))));

        let x = Box::pin(stream::iter(vec![
            Value::Int(1),
            Value::Deferred,
            Value::Int(3),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_is_defined_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::IsDefined(Box::new(SExprTE::Float(SExprFloat::Var("x".into()))));

        let x = Box::pin(stream::iter(vec![Value::Float(1.0), Value::Deferred]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_is_defined_bool_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::IsDefined(Box::new(SExprTE::Bool(SExprBool::Var("x".into()))));

        let x = Box::pin(stream::iter(vec![
            Value::Bool(true),
            Value::Deferred,
            Value::Bool(false),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_is_defined_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::IsDefined(Box::new(SExprTE::Str(SExprStr::Var("x".into()))));

        let x = Box::pin(stream::iter(vec![
            Value::Deferred,
            Value::Str("hello".into()),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    // ========== Tests for when ==========

    #[apply(async_test)]
    async fn test_when_int_never_defined_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::When(Box::new(SExprTE::Int(SExprInt::Var("x".into()))));

        let x = Box::pin(stream::iter(vec![
            Value::Deferred,
            Value::Deferred,
            Value::Deferred,
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_when_int_eventually_defined_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::When(Box::new(SExprTE::Int(SExprInt::Var("x".into()))));

        let x = Box::pin(stream::iter(vec![
            Value::Deferred,
            Value::Int(10),
            Value::Int(20),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_when_bool_immediately_defined_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::When(Box::new(SExprTE::Bool(SExprBool::Var("x".into()))));

        let x = Box::pin(stream::iter(vec![Value::Bool(true), Value::Bool(false)]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    // ========== Tests for init ==========

    #[apply(async_test)]
    async fn test_init_int_no_noval_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprInt::Init(
            Box::new(SExprInt::Var("x".into())),
            Box::new(SExprInt::Var("d".into())),
        );

        let x = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let d = Box::pin(stream::iter(vec![10.into(), 20.into(), 30.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "d".into()],
            vec![x, d],
            10,
        );

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Known(1),
            PartialStreamValue::Known(2),
            PartialStreamValue::Known(3),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_init_int_starts_with_noval_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprInt::Init(
            Box::new(SExprInt::Var("x".into())),
            Box::new(SExprInt::Var("d".into())),
        );

        let x = Box::pin(stream::iter(vec![
            Value::NoVal,
            Value::NoVal,
            Value::Int(3),
            Value::Int(4),
        ]));
        let d = Box::pin(stream::iter(vec![
            Value::Int(10),
            Value::Int(20),
            Value::Int(30),
            Value::Int(40),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "d".into()],
            vec![x, d],
            10,
        );

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Known(10),
            PartialStreamValue::Known(20),
            PartialStreamValue::Known(3),
            PartialStreamValue::Known(4),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_init_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprFloat::Init(
            Box::new(SExprFloat::Var("x".into())),
            Box::new(SExprFloat::Var("d".into())),
        );

        let x = Box::pin(stream::iter(vec![
            Value::NoVal,
            Value::Float(2.5),
            Value::Float(3.5),
        ]));
        let d = Box::pin(stream::iter(vec![
            Value::Float(10.0),
            Value::Float(20.0),
            Value::Float(30.0),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "d".into()],
            vec![x, d],
            10,
        );

        let res_stream = to_async_stream_float::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<f64>> = vec![
            PartialStreamValue::Known(10.0),
            PartialStreamValue::Known(2.5),
            PartialStreamValue::Known(3.5),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_init_bool_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Init(
            Box::new(SExprBool::Var("x".into())),
            Box::new(SExprBool::Var("d".into())),
        );

        let x = Box::pin(stream::iter(vec![
            Value::NoVal,
            Value::Bool(true),
            Value::Bool(false),
        ]));
        let d = Box::pin(stream::iter(vec![
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "d".into()],
            vec![x, d],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_init_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprStr::Init(
            Box::new(SExprStr::Var("x".into())),
            Box::new(SExprStr::Var("d".into())),
        );

        let x = Box::pin(stream::iter(vec![Value::NoVal, Value::Str("hello".into())]));
        let d = Box::pin(stream::iter(vec![
            Value::Str("default".into()),
            Value::Str("default".into()),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "d".into()],
            vec![x, d],
            10,
        );

        let res_stream = to_async_stream_str::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<String>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<String>> = vec![
            PartialStreamValue::Known("default".into()),
            PartialStreamValue::Known("hello".into()),
        ];
        assert_eq!(res, exp);
    }
}
