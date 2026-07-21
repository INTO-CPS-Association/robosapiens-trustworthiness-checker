use std::collections::BTreeMap;

use crate::VarName;
use crate::core::OutputStream;
use crate::core::{RuntimeFunction, Value};
use crate::lang::dsrv::ast::{
    BoolBinOp, CompBinOp, Expr, ExprRef, ExprView, NumericalBinOp, SBinOp, StrBinOp,
};
use crate::semantics::distributed::combinators as dist_mc;
use crate::semantics::untimed_dsrv::combinators as mc;
use crate::semantics::{AsyncConfig, MonitoringSemantics};

use super::contexts::DistributedContext;

#[derive(Clone)]
pub struct DistributedSemantics;

impl<AC> MonitoringSemantics<AC> for DistributedSemantics
where
    AC: AsyncConfig<Val = Value, Expr = Expr, Ctx = DistributedContext<AC>>,
{
    fn to_async_stream(
        expr: &AC::Expr,
        ctx: &AC::Ctx,
        owner: Option<VarName>,
    ) -> OutputStream<AC::Val> {
        <DistributedExprSemantics as MonitoringSemantics<AC>>::to_async_stream(expr, ctx, owner)
    }
}

#[derive(Clone)]
struct DistributedExprSemantics;

impl<AC> MonitoringSemantics<AC> for DistributedExprSemantics
where
    AC: AsyncConfig<Val = Value, Expr = Expr, Ctx = DistributedContext<AC>>,
{
    fn to_async_stream(
        expr: &AC::Expr,
        ctx: &AC::Ctx,
        owner: Option<VarName>,
    ) -> OutputStream<AC::Val> {
        evaluate_expr::<AC>(expr.as_ref(), ctx, owner)
    }
}

fn evaluate_expr<'a, AC>(
    expr: ExprRef<'a>,
    ctx: &AC::Ctx,
    owner: Option<VarName>,
) -> OutputStream<AC::Val>
where
    AC: AsyncConfig<Val = Value, Expr = Expr, Ctx = DistributedContext<AC>>,
{
    match expr.view() {
        ExprView::Val(v) => mc::val(v.clone()),
        ExprView::BinOp(e1, e2, op) => {
            let e1 = evaluate_expr::<AC>(e1, ctx, owner.clone());
            let e2 = evaluate_expr::<AC>(e2, ctx, owner.clone());
            match op {
                SBinOp::NOp(NumericalBinOp::Add) => mc::plus(e1, e2),
                SBinOp::NOp(NumericalBinOp::Sub) => mc::minus(e1, e2),
                SBinOp::NOp(NumericalBinOp::Mul) => mc::mult(e1, e2),
                SBinOp::NOp(NumericalBinOp::Div) => mc::div(e1, e2),
                SBinOp::NOp(NumericalBinOp::Mod) => mc::modulo(e1, e2),
                SBinOp::BOp(BoolBinOp::Or) => mc::or(e1, e2),
                SBinOp::BOp(BoolBinOp::And) => mc::and(e1, e2),
                SBinOp::BOp(BoolBinOp::Impl) => mc::implication(e1, e2),
                SBinOp::SOp(StrBinOp::Concat) => mc::concat(e1, e2),
                SBinOp::COp(CompBinOp::Eq) => mc::eq(e1, e2),
                SBinOp::COp(CompBinOp::Le) => mc::le(e1, e2),
                SBinOp::COp(CompBinOp::Lt) => mc::lt(e1, e2),
                SBinOp::COp(CompBinOp::Ge) => mc::ge(e1, e2),
                SBinOp::COp(CompBinOp::Gt) => mc::gt(e1, e2),
            }
        }
        ExprView::Not(e) => {
            let e = evaluate_expr::<AC>(e, ctx, owner.clone());
            mc::not(e)
        }
        ExprView::Lambda(params, body) => {
            let params = params
                .iter()
                .map(|(name, typ)| format!("{}: {}", name, typ))
                .collect::<Vec<_>>()
                .join(", ");
            mc::val(Value::Function(RuntimeFunction::opaque(format!(
                "\\{} -> {}",
                params, body
            ))))
        }
        ExprView::Fix(func) => mc::val(Value::Function(RuntimeFunction::opaque(format!(
            "fix({})",
            func
        )))),
        ExprView::Apply(_, _) | ExprView::Partial(_, _) => {
            panic!("function application requires typed DSRV semantics")
        }
        ExprView::Var(v) => mc::var::<AC>(ctx, v.clone()),
        ExprView::Dynamic(source, _, scope) => {
            let e = evaluate_expr::<AC>(source, ctx, owner.clone());
            mc::dynamic::<AC>(ctx, e, scope.clone(), owner.clone(), 10)
        }
        ExprView::Defer(source, _, scope) => {
            let e = evaluate_expr::<AC>(source, ctx, owner.clone());
            mc::defer::<AC>(ctx, e, scope.clone(), owner.clone(), 10)
        }
        ExprView::Update(e1, e2) => {
            let e1 = evaluate_expr::<AC>(e1, ctx, owner.clone());
            let e2 = evaluate_expr::<AC>(e2, ctx, owner.clone());
            mc::update(e1, e2)
        }
        ExprView::Default(e, d) => {
            let e = evaluate_expr::<AC>(e, ctx, owner.clone());
            let d = evaluate_expr::<AC>(d, ctx, owner.clone());
            mc::default(e, d)
        }
        ExprView::IsDefined(e) => {
            let e = evaluate_expr::<AC>(e, ctx, owner.clone());
            mc::is_defined(e)
        }
        ExprView::When(e) => {
            let e = evaluate_expr::<AC>(e, ctx, owner.clone());
            mc::when(e)
        }
        ExprView::Latch(e1, e2) => {
            let e1 = evaluate_expr::<AC>(e1, ctx, owner.clone());
            let e2 = evaluate_expr::<AC>(e2, ctx, owner.clone());
            mc::latch(e1, e2)
        }
        ExprView::Init(e1, e2) => {
            let e1 = evaluate_expr::<AC>(e1, ctx, owner.clone());
            let e2 = evaluate_expr::<AC>(e2, ctx, owner.clone());
            mc::init(e1, e2)
        }
        ExprView::SIndex(e, i) => {
            let e = evaluate_expr::<AC>(e, ctx, owner.clone());
            mc::sindex(e, i)
        }
        ExprView::If(b, e1, e2) => {
            let b = evaluate_expr::<AC>(b, ctx, owner.clone());
            let e1 = evaluate_expr::<AC>(e1, ctx, owner.clone());
            let e2 = evaluate_expr::<AC>(e2, ctx, owner.clone());
            mc::if_stm(b, e1, e2)
        }
        ExprView::List(exprs) => {
            let exprs: Vec<_> = exprs
                .into_iter()
                .map(|e| evaluate_expr::<AC>(e, ctx, owner.clone()))
                .collect();
            mc::list(exprs)
        }
        ExprView::Tuple(exprs) => {
            let exprs: Vec<_> = exprs
                .into_iter()
                .map(|e| evaluate_expr::<AC>(e, ctx, owner.clone()))
                .collect();
            mc::tuple(exprs)
        }
        ExprView::LIndex(e, i) => {
            let e = evaluate_expr::<AC>(e, ctx, owner.clone());
            let i = evaluate_expr::<AC>(i, ctx, owner.clone());
            mc::lindex(e, i)
        }
        ExprView::LAppend(lst, el) => {
            let lst = evaluate_expr::<AC>(lst, ctx, owner.clone());
            let el = evaluate_expr::<AC>(el, ctx, owner.clone());
            mc::lappend(lst, el)
        }
        ExprView::LConcat(lst1, lst2) => {
            let lst1 = evaluate_expr::<AC>(lst1, ctx, owner.clone());
            let lst2 = evaluate_expr::<AC>(lst2, ctx, owner.clone());
            mc::lconcat(lst1, lst2)
        }
        ExprView::LHead(lst) => {
            let lst = evaluate_expr::<AC>(lst, ctx, owner.clone());
            mc::lhead(lst)
        }
        ExprView::LTail(lst) => {
            let lst = evaluate_expr::<AC>(lst, ctx, owner.clone());
            mc::ltail(lst)
        }
        ExprView::LLen(lst) => {
            let lst = evaluate_expr::<AC>(lst, ctx, owner.clone());
            mc::llen(lst)
        }
        ExprView::LMap(_, _) | ExprView::LFilter(_, _) | ExprView::LFold(_, _, _) => {
            panic!("higher-order list operations require typed DSRV semantics")
        }
        ExprView::Map(map) | ExprView::Struct(map) | ExprView::ObjectLiteral(map) => {
            let map: BTreeMap<_, _> = map
                .iter()
                .map(|(k, v)| (k.clone(), evaluate_expr::<AC>(v, ctx, owner.clone())))
                .collect();
            mc::map(map)
        }
        ExprView::MGet(map, k) => {
            let map = evaluate_expr::<AC>(map, ctx, owner.clone());
            mc::mget(map, k.clone())
        }
        ExprView::SGet(struct_expr, key) => {
            let value = evaluate_expr::<AC>(struct_expr, ctx, owner.clone());
            match key.parse::<usize>() {
                Ok(index) => mc::tget(value, index),
                Err(_) => mc::mget(value, key.clone()),
            }
        }
        ExprView::MRemove(map, k) => {
            let map = evaluate_expr::<AC>(map, ctx, owner.clone());
            mc::mremove(map, k.clone())
        }
        ExprView::MInsert(map, k, v) => {
            let map = evaluate_expr::<AC>(map, ctx, owner.clone());
            let v = evaluate_expr::<AC>(v, ctx, owner.clone());
            mc::minsert(map, k.clone(), v)
        }
        ExprView::MHasKey(map, k) => {
            let map = evaluate_expr::<AC>(map, ctx, owner.clone());
            mc::mhas_key(map, k.clone())
        }
        ExprView::Neg(v) => {
            let v = evaluate_expr::<AC>(v, ctx, owner.clone());
            mc::neg(v)
        }
        ExprView::Sin(v) => {
            let v = evaluate_expr::<AC>(v, ctx, owner.clone());
            mc::sin(v)
        }
        ExprView::Cos(v) => {
            let v = evaluate_expr::<AC>(v, ctx, owner.clone());
            mc::cos(v)
        }
        ExprView::Tan(v) => {
            let v = evaluate_expr::<AC>(v, ctx, owner.clone());
            mc::tan(v)
        }
        ExprView::Abs(v) => {
            let v = evaluate_expr::<AC>(v, ctx, owner.clone());
            mc::abs(v)
        }
        ExprView::MonitoredAt(var_name, label) => {
            dist_mc::monitored_at::<AC>(var_name.clone(), label.clone(), ctx)
        }
        ExprView::Dist(u, v) => dist_mc::dist::<AC>(u.clone(), v.clone(), ctx),
    }
}
