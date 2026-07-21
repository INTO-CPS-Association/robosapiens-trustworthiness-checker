use std::{collections::BTreeMap, rc::Rc};

use super::combinators as mc;
use super::dynamic;
pub(crate) use super::functions::bind_expression_for_benchmark;
use super::functions::{
    ScopedExpr, eval_apply, eval_fix, eval_list_filter, eval_list_fold, eval_list_map,
    eval_partial, make_function,
};
use super::typed_combinators as typed;
use crate::VarName;
use crate::core::Value;
use crate::core::{
    OutputStream, PartialStreamValue, from_typed_partial_stream, to_typed_partial_stream,
};
use crate::lang::dsrv::ast::{
    BoolBinOp, CheckedExpr, CompBinOp, Expr, ExprRef, ExprView, NumericalBinOp, SBinOp, StrBinOp,
};
use crate::lang::dsrv::type_checker::TCType;
use crate::semantics::{AsyncConfig, MonitoringSemantics, StreamContext};
use tracing::debug;

#[cfg(test)]
use ecow::EcoVec;

#[derive(Clone)]
pub struct UntimedDsrvSemantics;

pub(crate) fn evaluate<AC>(expr: Expr, owner: Option<VarName>, ctx: &AC::Ctx) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value>,
{
    let expression = if let Some(owner) = owner {
        ScopedExpr::unchecked(expr).with_owner(owner)
    } else {
        ScopedExpr::unchecked(expr)
    };
    evaluate_scope::<AC>(expression, ctx)
}

pub(crate) fn evaluate_checked<AC>(
    expr: CheckedExpr,
    owner: Option<VarName>,
    ctx: &AC::Ctx,
) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value>,
{
    let expression = if let Some(owner) = owner {
        ScopedExpr::checked(expr).with_owner(owner)
    } else {
        ScopedExpr::checked(expr)
    };
    checked_typed_dispatch_stream::<AC>(expression, ctx)
}

pub(super) fn evaluate_scope<AC>(expr: ScopedExpr, ctx: &AC::Ctx) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value>,
{
    let owner = expr.owner().cloned();
    evaluate_ref::<AC>(expr.as_ref(), &expr, owner, ctx)
}

pub(super) fn evaluate_scoped_typed<T, AC>(
    expr: ScopedExpr,
    ctx: &AC::Ctx,
) -> OutputStream<PartialStreamValue<T>>
where
    T: TryFrom<Value> + std::fmt::Debug + 'static,
    <T as TryFrom<Value>>::Error: std::fmt::Debug,
    AC: AsyncConfig<Val = Value>,
{
    to_typed_partial_stream(evaluate_ref::<AC>(
        expr.as_ref(),
        &expr,
        expr.owner().cloned(),
        ctx,
    ))
}

pub(super) fn evaluate_ref<'a, AC>(
    node: ExprRef<'a>,
    expression: &ScopedExpr,
    owner: Option<VarName>,
    ctx: &AC::Ctx,
) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value>,
{
    debug!("Creating async stream for expression: {:?}", node);
    let owner = owner.or_else(|| expression.owner().cloned());
    let evaluate = |child| {
        let child_expression = expression.scope(child);
        evaluate_ref::<AC>(
            child_expression.as_ref(),
            &child_expression,
            owner.clone(),
            ctx,
        )
    };
    let own_child = |child| expression.scope(child);
    match node.view() {
        ExprView::Val(v) => {
            debug!("Constant value: {:?}", v);
            mc::val(v.clone())
        }
        ExprView::BinOp(e1, e2, op) => {
            debug!("Binary operation: {:?} {:?} {:?}", e1, op, e2);
            let e1 = evaluate(e1);
            let e2 = evaluate(e2);
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
        ExprView::Not(x) => {
            debug!("Performing logical NOT operation");
            let x = evaluate(x);
            mc::not(x)
        }
        ExprView::Neg(x) => {
            debug!("Performing numeric negation");
            let x = evaluate(x);
            mc::neg(x)
        }
        ExprView::Var(v) => {
            debug!("Accessing variable: {:?}", v);
            if let Some(stream) = expression.resolve_stream(v) {
                return stream;
            }
            match expression.resolve(v) {
                Some(bound) => evaluate_ref::<AC>(bound.as_ref(), &bound, owner.clone(), ctx),
                None => mc::var::<AC>(ctx, v.clone()),
            }
        }
        ExprView::Dynamic(source, _, scope) => {
            let dynamic_type = expression.typ(node).cloned().and_then(|expected| {
                expression
                    .shared_type_info()
                    .map(|info| (Rc::clone(info), expected))
            });
            let e = evaluate(source);
            dynamic::dynamic_checked::<AC>(ctx, e, scope.clone(), owner, 1, dynamic_type)
        }
        ExprView::Defer(source, _, scope) => {
            let dynamic_type = expression.typ(node).cloned().and_then(|expected| {
                expression
                    .shared_type_info()
                    .map(|info| (Rc::clone(info), expected))
            });
            let e = evaluate(source);
            dynamic::defer_checked::<AC>(ctx, e, scope.clone(), owner, 1, dynamic_type)
        }
        ExprView::Update(e1, e2) => {
            let e1 = evaluate(e1);
            let e2 = evaluate(e2);
            mc::update(e1, e2)
        }
        ExprView::Default(e, d) => {
            let e = evaluate(e);
            let d = evaluate(d);
            mc::default(e, d)
        }
        ExprView::IsDefined(e) => {
            let e = evaluate(e);
            mc::is_defined(e)
        }
        ExprView::When(e) => {
            let e = evaluate(e);
            mc::when(e)
        }
        ExprView::Latch(e1, e2) => {
            let e1 = evaluate(e1);
            let e2 = evaluate(e2);
            mc::latch(e1, e2)
        }
        ExprView::Init(e1, e2) => {
            let e1 = evaluate(e1);
            let e2 = evaluate(e2);
            mc::init(e1, e2)
        }
        ExprView::SIndex(e, i) => {
            let e = evaluate(e);
            mc::sindex(e, i)
        }
        ExprView::If(b, e1, e2) => {
            let b = evaluate(b);
            let e1 = evaluate(e1);
            let e2 = evaluate(e2);
            mc::if_stm(b, e1, e2)
        }
        ExprView::List(exprs) => {
            let exprs: Vec<_> = exprs.into_iter().map(|e| evaluate(e)).collect();
            mc::list(exprs)
        }
        ExprView::Tuple(exprs) => {
            let exprs: Vec<_> = exprs.into_iter().map(|e| evaluate(e)).collect();
            mc::tuple(exprs)
        }
        ExprView::LIndex(e, i) => {
            let e = evaluate(e);
            let i = evaluate(i);
            mc::lindex(e, i)
        }
        ExprView::LAppend(lst, el) => {
            let lst = evaluate(lst);
            let el = evaluate(el);
            mc::lappend(lst, el)
        }
        ExprView::LConcat(lst1, lst2) => {
            let lst1 = evaluate(lst1);
            let lst2 = evaluate(lst2);
            mc::lconcat(lst1, lst2)
        }
        ExprView::LHead(lst) => {
            let lst = evaluate(lst);
            mc::lhead(lst)
        }
        ExprView::LTail(lst) => {
            let lst = evaluate(lst);
            mc::ltail(lst)
        }
        ExprView::LLen(lst) => {
            let lst = evaluate(lst);
            mc::llen(lst)
        }
        ExprView::Lambda(params, body) => {
            let params_display = params
                .iter()
                .map(|(name, typ)| format!("{}: {}", name, typ))
                .collect::<Vec<_>>()
                .join(", ");
            let body = own_child(body);
            let display = format!("\\{} -> {}", params_display, body.expr).into();
            mc::val(make_function::<AC>(display, params.clone(), body, ctx))
        }
        ExprView::Apply(func, args) => eval_apply::<AC>(
            own_child(func),
            args.into_iter().map(&own_child).collect(),
            ctx,
        ),
        ExprView::Fix(func) => eval_fix::<AC>(own_child(func), ctx),
        ExprView::Partial(func, args) => eval_partial::<AC>(
            own_child(func),
            args.into_iter().map(&own_child).collect(),
            ctx,
        ),
        ExprView::LMap(func, list) => eval_list_map::<AC>(own_child(func), own_child(list), ctx),
        ExprView::LFilter(func, list) => {
            eval_list_filter::<AC>(own_child(func), own_child(list), ctx)
        }
        ExprView::LFold(func, init, list) => {
            eval_list_fold::<AC>(own_child(func), own_child(init), own_child(list), ctx)
        }
        ExprView::Map(map) | ExprView::Struct(map) | ExprView::ObjectLiteral(map) => {
            let checked_type = expression.typ(node);
            let map: BTreeMap<_, _> = map
                .iter()
                .filter(|(name, _)| field_survives_checked_projection(checked_type, name))
                .map(|(k, v)| (k.clone(), evaluate(v)))
                .collect();
            mc::map(map)
        }
        ExprView::MGet(map, k) => {
            let map = evaluate(map);
            mc::mget(map, k.clone())
        }
        ExprView::SGet(value, key) => {
            let value_type = expression.typ(value).cloned();
            let value = evaluate(value);
            match (value_type, key.parse::<usize>()) {
                (Some(TCType::Tuple(_)), Ok(index)) | (None, Ok(index)) => mc::tget(value, index),
                _ => mc::mget(value, key.clone()),
            }
        }
        ExprView::MRemove(map, k) => {
            let map = evaluate(map);
            mc::mremove(map, k.clone())
        }
        ExprView::MInsert(map, k, v) => {
            let map = evaluate(map);
            let v = evaluate(v);
            mc::minsert(map, k.clone(), v)
        }
        ExprView::MHasKey(map, k) => {
            let map = evaluate(map);
            mc::mhas_key(map, k.clone())
        }
        ExprView::MonitoredAt(_, _) => {
            unimplemented!("Function monitored_at only supported in distributed semantics")
        }
        ExprView::Dist(_, _) => {
            unimplemented!("Function dist only supported in distributed semantics")
        }
        ExprView::Sin(v) => {
            let v = evaluate(v);
            mc::sin(v)
        }
        ExprView::Cos(v) => {
            let v = evaluate(v);
            mc::cos(v)
        }
        ExprView::Tan(v) => {
            let v = evaluate(v);
            mc::tan(v)
        }
        ExprView::Abs(v) => {
            let v = evaluate(v);
            mc::abs(v)
        }
    }
}

/// Width-subtyping projects checked structs to their declared runtime shape.
/// Maps, object literals, and unchecked expressions retain every field.
fn field_survives_checked_projection(typ: Option<&TCType>, name: &str) -> bool {
    match typ {
        Some(TCType::Struct(fields, _)) => fields.iter().any(|(field, _)| field == name),
        _ => true,
    }
}

fn evaluate_float<AC>(expression: ScopedExpr, context: &AC::Ctx) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value>,
{
    from_typed_partial_stream(evaluate_float_typed::<AC>(expression, context))
}

fn evaluate_float_typed<AC>(
    expression: ScopedExpr,
    context: &AC::Ctx,
) -> OutputStream<PartialStreamValue<f64>>
where
    AC: AsyncConfig<Val = Value>,
{
    let node = expression.as_ref();
    match node.view() {
        ExprView::Val(Value::Float(value)) => typed::val(*value),
        ExprView::Var(name) => {
            if let Some(stream) = expression.resolve_stream(name) {
                crate::core::to_typed_stream(stream)
            } else {
                match expression.resolve(name) {
                    Some(bound) => evaluate_float_typed::<AC>(bound, context),
                    None => crate::core::to_typed_stream(
                        context.var(name).expect("checked variable must exist"),
                    ),
                }
            }
        }
        ExprView::BinOp(left, right, SBinOp::NOp(operator)) => {
            let left = evaluate_float_typed::<AC>(expression.scope(left), context);
            let right = evaluate_float_typed::<AC>(expression.scope(right), context);
            match operator {
                NumericalBinOp::Add => typed::add(left, right),
                NumericalBinOp::Sub => typed::sub(left, right),
                NumericalBinOp::Mul => typed::mul(left, right),
                NumericalBinOp::Div => typed::div(left, right),
                NumericalBinOp::Mod => typed::rem(left, right),
            }
        }
        ExprView::Default(input, default) => typed::default(
            evaluate_float_typed::<AC>(expression.scope(input), context),
            evaluate_float_typed::<AC>(expression.scope(default), context),
        ),
        ExprView::SIndex(input, index) => typed::sindex(
            evaluate_float_typed::<AC>(expression.scope(input), context),
            index,
        ),
        ExprView::If(condition, then_expr, else_expr) => typed::if_stream(
            evaluate_bool_typed::<AC>(expression.scope(condition), context),
            evaluate_float_typed::<AC>(expression.scope(then_expr), context),
            evaluate_float_typed::<AC>(expression.scope(else_expr), context),
        ),
        ExprView::Neg(value) => {
            typed::neg(evaluate_float_typed::<AC>(expression.scope(value), context))
        }
        ExprView::Sin(value) => {
            typed::sin(evaluate_float_typed::<AC>(expression.scope(value), context))
        }
        ExprView::Cos(value) => {
            typed::cos(evaluate_float_typed::<AC>(expression.scope(value), context))
        }
        ExprView::Tan(value) => {
            typed::tan(evaluate_float_typed::<AC>(expression.scope(value), context))
        }
        ExprView::Abs(value) => {
            typed::abs(evaluate_float_typed::<AC>(expression.scope(value), context))
        }
        _ => evaluate_typed::<f64, AC>(expression, context),
    }
}

fn evaluate_int<AC>(expression: ScopedExpr, context: &AC::Ctx) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value>,
{
    from_typed_partial_stream(evaluate_int_typed::<AC>(expression, context))
}

fn evaluate_int_typed<AC>(
    expression: ScopedExpr,
    context: &AC::Ctx,
) -> OutputStream<PartialStreamValue<i64>>
where
    AC: AsyncConfig<Val = Value>,
{
    let node = expression.as_ref();
    match node.view() {
        ExprView::Val(Value::Int(value)) => typed::val(*value),
        ExprView::Var(name) => {
            if let Some(stream) = expression.resolve_stream(name) {
                crate::core::to_typed_stream(stream)
            } else {
                match expression.resolve(name) {
                    Some(bound) => evaluate_int_typed::<AC>(bound, context),
                    None => crate::core::to_typed_stream(
                        context.var(name).expect("checked variable must exist"),
                    ),
                }
            }
        }
        ExprView::BinOp(left, right, SBinOp::NOp(operator)) => {
            let left = evaluate_int_typed::<AC>(expression.scope(left), context);
            let right = evaluate_int_typed::<AC>(expression.scope(right), context);
            match operator {
                NumericalBinOp::Add => typed::add(left, right),
                NumericalBinOp::Sub => typed::sub(left, right),
                NumericalBinOp::Mul => typed::mul(left, right),
                NumericalBinOp::Div => typed::div(left, right),
                NumericalBinOp::Mod => typed::rem(left, right),
            }
        }
        ExprView::Default(input, default) => typed::default(
            evaluate_int_typed::<AC>(expression.scope(input), context),
            evaluate_int_typed::<AC>(expression.scope(default), context),
        ),
        ExprView::SIndex(input, index) => typed::sindex(
            evaluate_int_typed::<AC>(expression.scope(input), context),
            index,
        ),
        ExprView::If(condition, then_expr, else_expr) => typed::if_stream(
            evaluate_bool_typed::<AC>(expression.scope(condition), context),
            evaluate_int_typed::<AC>(expression.scope(then_expr), context),
            evaluate_int_typed::<AC>(expression.scope(else_expr), context),
        ),
        ExprView::Neg(value) => {
            typed::neg(evaluate_int_typed::<AC>(expression.scope(value), context))
        }
        _ => evaluate_typed::<i64, AC>(expression, context),
    }
}

fn evaluate_bool<AC>(expression: ScopedExpr, context: &AC::Ctx) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value>,
{
    from_typed_partial_stream(evaluate_bool_typed::<AC>(expression, context))
}

fn evaluate_bool_typed<AC>(
    expression: ScopedExpr,
    context: &AC::Ctx,
) -> OutputStream<PartialStreamValue<bool>>
where
    AC: AsyncConfig<Val = Value>,
{
    let node = expression.as_ref();
    match node.view() {
        ExprView::Val(Value::Bool(value)) => typed::val(*value),
        ExprView::Var(name) => {
            if let Some(stream) = expression.resolve_stream(name) {
                crate::core::to_typed_stream(stream)
            } else {
                match expression.resolve(name) {
                    Some(bound) => evaluate_bool_typed::<AC>(bound, context),
                    None => crate::core::to_typed_stream(
                        context.var(name).expect("checked variable must exist"),
                    ),
                }
            }
        }
        ExprView::Not(input) => {
            typed::not(evaluate_bool_typed::<AC>(expression.scope(input), context))
        }
        ExprView::BinOp(left, right, SBinOp::BOp(operator)) => {
            let left = evaluate_bool_typed::<AC>(expression.scope(left), context);
            let right = evaluate_bool_typed::<AC>(expression.scope(right), context);
            match operator {
                BoolBinOp::And => typed::and(left, right),
                BoolBinOp::Or => typed::or(left, right),
                BoolBinOp::Impl => typed::implies(left, right),
            }
        }
        ExprView::BinOp(left, right, SBinOp::COp(operator))
            if matches!(expression.typ(left), Some(TCType::Int)) =>
        {
            let left = evaluate_int_typed::<AC>(expression.scope(left), context);
            let right = evaluate_int_typed::<AC>(expression.scope(right), context);
            match operator {
                CompBinOp::Eq => typed::compare(left, right, |left, right| left == right),
                CompBinOp::Le => typed::compare(left, right, |left, right| left <= right),
                CompBinOp::Lt => typed::compare(left, right, |left, right| left < right),
                CompBinOp::Ge => typed::compare(left, right, |left, right| left >= right),
                CompBinOp::Gt => typed::compare(left, right, |left, right| left > right),
            }
        }
        ExprView::BinOp(left, right, SBinOp::COp(operator))
            if matches!(expression.typ(left), Some(TCType::Float)) =>
        {
            let left = evaluate_float_typed::<AC>(expression.scope(left), context);
            let right = evaluate_float_typed::<AC>(expression.scope(right), context);
            match operator {
                CompBinOp::Eq => typed::compare(left, right, |left, right| left == right),
                CompBinOp::Le => typed::compare(left, right, |left, right| left <= right),
                CompBinOp::Lt => typed::compare(left, right, |left, right| left < right),
                CompBinOp::Ge => typed::compare(left, right, |left, right| left >= right),
                CompBinOp::Gt => typed::compare(left, right, |left, right| left > right),
            }
        }
        ExprView::Default(input, default) => typed::default(
            evaluate_bool_typed::<AC>(expression.scope(input), context),
            evaluate_bool_typed::<AC>(expression.scope(default), context),
        ),
        ExprView::SIndex(input, index) => typed::sindex(
            evaluate_bool_typed::<AC>(expression.scope(input), context),
            index,
        ),
        ExprView::If(condition, then_expr, else_expr) => typed::if_stream(
            evaluate_bool_typed::<AC>(expression.scope(condition), context),
            evaluate_bool_typed::<AC>(expression.scope(then_expr), context),
            evaluate_bool_typed::<AC>(expression.scope(else_expr), context),
        ),
        _ => evaluate_typed::<bool, AC>(expression, context),
    }
}

fn evaluate_typed<T, AC>(
    expression: ScopedExpr,
    context: &AC::Ctx,
) -> OutputStream<PartialStreamValue<T>>
where
    T: TryFrom<Value> + std::fmt::Debug + 'static,
    <T as TryFrom<Value>>::Error: std::fmt::Debug,
    AC: AsyncConfig<Val = Value>,
{
    evaluate_scoped_typed::<T, AC>(expression, context)
}

fn evaluate_untyped<AC>(expression: ScopedExpr, context: &AC::Ctx) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value>,
{
    evaluate_ref::<AC>(expression.as_ref(), &expression, None, context)
}

fn checked_typed_dispatch_stream<AC>(
    expression: ScopedExpr,
    context: &AC::Ctx,
) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value>,
{
    match expression.typ(expression.as_ref()) {
        Some(TCType::Int) => evaluate_int::<AC>(expression, context),
        Some(TCType::Bool) => evaluate_bool::<AC>(expression, context),
        Some(TCType::Float) => evaluate_float::<AC>(expression, context),
        _ => evaluate_untyped::<AC>(expression, context),
    }
}

impl<AC> MonitoringSemantics<AC> for UntimedDsrvSemantics
where
    AC: AsyncConfig<Val = Value, Expr = Expr>,
{
    fn to_async_stream(expr: &Expr, ctx: &AC::Ctx, owner: Option<VarName>) -> OutputStream<Value> {
        evaluate::<AC>(expr.clone(), owner, ctx)
    }
}

/// Untimed semantics for checked DSRV expressions.
///
/// Immutable type annotations select specialised scalar streams where possible
/// and support type-checking of
/// expressions introduced by `dynamic` and `defer` at runtime.
#[derive(Clone)]
pub struct CheckedUntimedDsrvSemantics;

impl<AC> MonitoringSemantics<AC> for CheckedUntimedDsrvSemantics
where
    AC: AsyncConfig<Val = Value, Expr = CheckedExpr>,
{
    fn to_async_stream(
        expr: &CheckedExpr,
        ctx: &AC::Ctx,
        owner: Option<VarName>,
    ) -> OutputStream<Value> {
        evaluate_checked::<AC>(expr.clone(), owner, ctx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::async_test;
    use crate::core::StreamTypeAscription;
    use crate::dsrv_fixtures::TestConfig;
    use crate::lang::dsrv::ast::Expr;
    use crate::lang::dsrv::parser::parse_sexpr;
    use crate::lang::dsrv::type_checker::type_check;
    use crate::runtime::asynchronous::Context;
    use crate::runtime::builder::CheckedValueConfig;
    use crate::semantics::StreamContext;
    use ecow::eco_vec;
    use futures::stream::{self, StreamExt};
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::rc::Rc;

    #[test]
    fn checked_lexical_frame_preserves_node_annotations() {
        let source =
            "in property: Str\nout result: Int\nresult = (\\p: Str -> dynamic(p : Int))(property)";
        let spec = crate::lang::dsrv::parser::parse_str(source).unwrap();
        let checked = type_check(spec, false).unwrap();
        let expression = ScopedExpr::checked(checked.var_expr(&"result".into()).unwrap());
        let ExprView::Apply(function, mut args) = expression.as_ref().view() else {
            panic!("expected application");
        };
        let argument = expression.scope(args.next().expect("application has an argument"));
        let function = expression.scope(function);
        let ExprView::Lambda(params, body) = function.as_ref().view() else {
            panic!("expected lambda");
        };

        let framed = function
            .scope(body)
            .bind(params, EcoVec::from([argument]))
            .unwrap();

        assert!(framed.typ(framed.as_ref()).is_some());
        assert!(framed.shared_type_info().is_some());
        let ExprView::Dynamic(source, _, _) = framed.as_ref().view() else {
            panic!("expected dynamic expression");
        };
        let source = framed.scope(source);
        assert!(source.typ(source.as_ref()).is_some());
        assert!(framed.resolve(&"p".into()).is_some());
    }

    #[apply(async_test)]
    async fn checked_float_arithmetic_and_comparison_are_specialised(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let specification = crate::dsrv_spec!("out result: Bool\nresult = (1.5 + 2.0) < 4.0");
        let checked = type_check(specification, false).unwrap();
        let expression = checked.var_expr(&"result".into()).unwrap();
        let context = Context::<CheckedValueConfig>::new(executor, Vec::new(), Vec::new(), 0);

        let values: Vec<Value> = <CheckedUntimedDsrvSemantics as MonitoringSemantics<
            CheckedValueConfig,
        >>::to_async_stream(&expression, &context, None)
        .take(1)
        .collect::<Vec<_>>()
        .await;

        assert_eq!(values, [Value::Bool(true)]);
    }

    #[apply(async_test)]
    async fn checked_float_default_repeats_known_values_across_no_val(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let specification =
            crate::dsrv_spec!("in x: Float\nout result: Float\nresult = default(x, 2.0)");
        let checked = type_check(specification, false).unwrap();
        let expression = checked.var_expr(&"result".into()).unwrap();
        let input = Box::pin(futures::stream::iter([Value::Float(1.0), Value::NoVal]));
        let mut context =
            Context::<CheckedValueConfig>::new(executor, vec!["x".into()], vec![input], 0);

        let output = <CheckedUntimedDsrvSemantics as MonitoringSemantics<CheckedValueConfig>>::to_async_stream(
            &expression,
            &context,
            None,
        );
        context.run().await;
        let values: Vec<Value> = output.collect::<Vec<_>>().await;

        assert_eq!(values, [Value::Float(1.0), Value::Float(1.0)]);
    }

    #[apply(async_test)]
    async fn lexical_function_arguments_keep_temporal_state(executor: Rc<LocalExecutor<'static>>) {
        let source = "(\\f: (Int -> Int) -> f(x))(\\v: Int -> v[1])";
        let expression = parse_sexpr(source).unwrap();
        let mut context = Context::<TestConfig>::new(
            executor,
            vec!["x".into()],
            vec![Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]))],
            2,
        );
        let output = to_stream(expression, &context);
        context.run().await;

        assert_eq!(
            output.collect::<Vec<_>>().await,
            vec![Value::Deferred, 1.into(), 2.into(), 3.into()]
        );
    }

    #[apply(async_test)]
    async fn first_class_function_values_keep_per_call_site_state(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let source = "(if choose then \\v: Int -> v[1] else \\v: Int -> v[1])(x)";
        let expression = parse_sexpr(source).unwrap();
        let mut context = Context::<TestConfig>::new(
            executor,
            vec!["choose".into(), "x".into()],
            vec![
                Box::pin(stream::iter(vec![true.into(), true.into(), true.into()])),
                Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()])),
            ],
            2,
        );
        let output = to_stream(expression, &context);
        context.run().await;

        assert_eq!(
            output.collect::<Vec<_>>().await,
            vec![Value::Deferred, 1.into(), 2.into()]
        );
    }

    #[apply(async_test)]
    async fn first_class_functions_share_arguments_and_update_captures(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let source = "(if choose then \\v: Int -> v[1] + bias else \\v: Int -> v + v)(x)";
        let expression = parse_sexpr(source).unwrap();
        let mut context = Context::<TestConfig>::new(
            executor,
            vec!["choose".into(), "x".into(), "bias".into()],
            vec![
                Box::pin(stream::iter(vec![true.into(), true.into(), true.into()])),
                Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()])),
                Box::pin(stream::iter(vec![10.into(), 20.into(), 30.into()])),
            ],
            2,
        );
        let output = to_stream(expression, &context);
        context.run().await;

        assert_eq!(
            output.collect::<Vec<_>>().await,
            vec![Value::Deferred, 21.into(), 32.into()]
        );
    }

    #[apply(async_test)]
    async fn first_class_function_capture_ports_advance_each_tick(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let source = "(if choose then \\v: Int -> bias[1] else \\v: Int -> bias[1])(x)";
        let expression = parse_sexpr(source).unwrap();
        let mut context = Context::<TestConfig>::new(
            executor,
            vec!["choose".into(), "x".into(), "bias".into()],
            vec![
                Box::pin(stream::iter(vec![true.into(), true.into(), true.into()])),
                Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()])),
                Box::pin(stream::iter(vec![10.into(), 20.into(), 30.into()])),
            ],
            2,
        );
        let output = to_stream(expression, &context);
        context.run().await;

        assert_eq!(
            output.collect::<Vec<_>>().await,
            vec![Value::Deferred, 10.into(), 20.into()]
        );
    }

    #[apply(async_test)]
    async fn partial_functions_preserve_stream_bindings(executor: Rc<LocalExecutor<'static>>) {
        let source = "partial(\\a: Int, b: Int -> a[1] + b, x)(y)";
        let expression = parse_sexpr(source).unwrap();
        let mut context = Context::<TestConfig>::new(
            executor,
            vec!["x".into(), "y".into()],
            vec![
                Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()])),
                Box::pin(stream::iter(vec![10.into(), 20.into(), 30.into()])),
            ],
            2,
        );
        let output = to_stream(expression, &context);
        context.run().await;

        assert_eq!(
            output.collect::<Vec<_>>().await,
            vec![Value::Deferred, 21.into(), 32.into()]
        );
    }

    #[apply(async_test)]
    async fn first_class_function_switching_starts_a_new_instance(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let source = "(if choose then \\v: Int -> v[1] else \\v: Int -> v[1])(x)";
        let expression = parse_sexpr(source).unwrap();
        let mut context = Context::<TestConfig>::new(
            executor,
            vec!["choose".into(), "x".into()],
            vec![
                Box::pin(stream::iter(vec![
                    true.into(),
                    true.into(),
                    false.into(),
                    false.into(),
                    true.into(),
                ])),
                Box::pin(stream::iter(vec![
                    1.into(),
                    2.into(),
                    3.into(),
                    4.into(),
                    5.into(),
                ])),
            ],
            2,
        );
        let output = to_stream(expression, &context);
        context.run().await;

        assert_eq!(
            output.collect::<Vec<_>>().await,
            vec![
                Value::Deferred,
                1.into(),
                Value::Deferred,
                3.into(),
                Value::Deferred,
            ]
        );
    }

    fn to_stream(expr: Expr, ctx: &Context<TestConfig>) -> OutputStream<Value> {
        <UntimedDsrvSemantics as MonitoringSemantics<TestConfig>>::to_async_stream(&expr, ctx, None)
    }
    // ============================================================================
    // DEFER TESTS
    // ============================================================================

    #[apply(async_test)]
    async fn test_defer_int(executor: Rc<LocalExecutor<'static>>) {
        let expr = Expr::Defer(
            Box::new(Expr::Val("x + 1")),
            StreamTypeAscription::Unascribed,
            eco_vec!["x".into()],
        );

        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let mut ctx = Context::<TestConfig>::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![2.into(), 3.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_int_x_squared(executor: Rc<LocalExecutor<'static>>) {
        let expr = Expr::Defer(
            Box::new(Expr::Val("x * x")),
            StreamTypeAscription::Unascribed,
            eco_vec!["x".into()],
        );

        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = Context::<TestConfig>::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![4.into(), 9.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_bool(executor: Rc<LocalExecutor<'static>>) {
        let expr = Expr::Defer(
            Box::new(Expr::Val("x && y")),
            StreamTypeAscription::Unascribed,
            eco_vec!["x".into(), "y".into()],
        );

        let x = Box::pin(stream::iter(vec![Value::Bool(true), Value::Bool(false)]));
        let y = Box::pin(stream::iter(vec![Value::Bool(true), Value::Bool(true)]));
        let mut ctx = Context::<TestConfig>::new(
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
        let expr = Expr::Defer(
            Box::new(Expr::Var("e".into())),
            StreamTypeAscription::Unascribed,
            eco_vec!["e".into(), "x".into()],
        );

        let e = Box::pin(stream::iter(vec![Value::Deferred]));
        let x = Box::pin(stream::iter(vec![2.into()]));
        let mut ctx = Context::<TestConfig>::new(
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
        let expr = Expr::Defer(
            Box::new(Expr::Val("x + 1.5")),
            StreamTypeAscription::Unascribed,
            eco_vec!["x".into()],
        );

        let x = Box::pin(stream::iter(vec![Value::Float(1.0), Value::Float(2.0)]));
        let mut ctx = Context::<TestConfig>::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![Value::Float(2.5), Value::Float(3.5)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_str(executor: Rc<LocalExecutor<'static>>) {
        let expr = Expr::Defer(
            Box::new(Expr::Val("x ++ y")),
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
        let mut ctx = Context::<TestConfig>::new(
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
        let expr = Expr::Dynamic(
            Box::new(Expr::Var("e".into())),
            StreamTypeAscription::Unascribed,
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x + 1".into()),
            Value::Str("x + 2".into()),
        ]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let mut ctx = Context::<TestConfig>::new(
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
        let expr = Expr::Dynamic(
            Box::new(Expr::Var("e".into())),
            StreamTypeAscription::Unascribed,
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x * x".into()),
            Value::Str("x * x".into()),
        ]));
        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = Context::<TestConfig>::new(
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
        let expr = Expr::Dynamic(
            Box::new(Expr::Var("e".into())),
            StreamTypeAscription::Unascribed,
        );

        let e = Box::pin(stream::iter(vec![
            Value::Deferred,
            Value::Str("x + 1".into()),
        ]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let mut ctx = Context::<TestConfig>::new(
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
        let expr = Expr::Dynamic(
            Box::new(Expr::Var("e".into())),
            StreamTypeAscription::Unascribed,
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x + 1".into()),
            Value::Deferred,
            Value::Str("x + 2".into()),
        ]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let mut ctx = Context::<TestConfig>::new(
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
        let expr = Expr::Dynamic(
            Box::new(Expr::Var("e".into())),
            StreamTypeAscription::Unascribed,
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x && y".into()),
            Value::Str("x || y".into()),
        ]));
        let x = Box::pin(stream::iter(vec![Value::Bool(true), Value::Bool(false)]));
        let y = Box::pin(stream::iter(vec![Value::Bool(false), Value::Bool(true)]));
        let mut ctx = Context::<TestConfig>::new(
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
        let expr = Expr::Dynamic(
            Box::new(Expr::Var("e".into())),
            StreamTypeAscription::Unascribed,
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x + 1.5".into()),
            Value::Str("x * 2.0".into()),
        ]));
        let x = Box::pin(stream::iter(vec![Value::Float(1.0), Value::Float(2.0)]));
        let mut ctx = Context::<TestConfig>::new(
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
        let expr = Expr::Dynamic(
            Box::new(Expr::Var("e".into())),
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
        let mut ctx = Context::<TestConfig>::new(
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
