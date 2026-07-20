use std::collections::BTreeMap;

use crate::core::OutputStream;
use crate::core::{RuntimeFunction, Value};
use crate::lang::core::parser::ExprParser;
use crate::lang::dsrv::ast::{
    BoolBinOp, CompBinOp, Expr, ExprView, NumericalBinOp, SBinOp, StrBinOp,
};
use crate::semantics::AsyncConfig;
use crate::semantics::MonitoringSemantics;
use crate::semantics::distributed::combinators as dist_mc;
use crate::semantics::untimed_dsrv::combinators as mc;

use super::contexts::DistributedContext;

#[derive(Clone)]
pub struct DistributedSemantics<Parser>
where
    Parser: ExprParser<Expr> + 'static,
{
    _parser: std::marker::PhantomData<Parser>,
}

impl<Parser, AC> MonitoringSemantics<AC> for DistributedSemantics<Parser>
where
    Parser: ExprParser<Expr> + 'static,
    AC: AsyncConfig<Val = Value, Expr = Expr, Ctx = DistributedContext<AC>>,
{
    fn to_async_stream(expr: AC::Expr, ctx: &AC::Ctx) -> OutputStream<AC::Val> {
        use ExprView as ExprKind;

        let child = |child| expr.subtree(child);
        match expr.as_ref().view() {
            ExprKind::Val(v) => mc::val(v.clone()),
            ExprKind::BinOp(e1, e2, op) => {
                let e1 = <Self as MonitoringSemantics<AC>>::to_async_stream(child(e1), ctx);
                let e2 = <Self as MonitoringSemantics<AC>>::to_async_stream(child(e2), ctx);
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
            ExprKind::Not(e) => {
                let e = <Self as MonitoringSemantics<AC>>::to_async_stream(child(e), ctx);
                mc::not(e)
            }
            ExprKind::Lambda(params, body) => {
                let params = params
                    .iter()
                    .map(|(name, typ)| format!("{}: {}", name, typ))
                    .collect::<Vec<_>>()
                    .join(", ");
                mc::val(Value::Function(RuntimeFunction::opaque(format!(
                    "\\{} -> {}",
                    params,
                    child(body)
                ))))
            }
            ExprKind::Fix(func) => mc::val(Value::Function(RuntimeFunction::opaque(format!(
                "fix({})",
                child(func)
            )))),
            ExprKind::Apply(_, _) | ExprKind::Partial(_, _) => {
                panic!("function application requires typed DSRV semantics")
            }
            ExprKind::Var(v) => mc::var::<AC>(ctx, v.clone()),
            ExprKind::Dynamic(source, _, scope) => {
                let e = <Self as MonitoringSemantics<AC>>::to_async_stream(child(source), ctx);
                mc::dynamic::<AC, Parser>(ctx, e, scope.clone(), 10)
            }
            ExprKind::Defer(source, _, scope) => {
                let e = <Self as MonitoringSemantics<AC>>::to_async_stream(child(source), ctx);
                mc::defer::<AC, Parser>(ctx, e, scope.clone(), 10)
            }
            ExprKind::Update(e1, e2) => {
                let e1 = <Self as MonitoringSemantics<AC>>::to_async_stream(child(e1), ctx);
                let e2 = <Self as MonitoringSemantics<AC>>::to_async_stream(child(e2), ctx);
                mc::update(e1, e2)
            }
            ExprKind::Default(e, d) => {
                let e = <Self as MonitoringSemantics<AC>>::to_async_stream(child(e), ctx);
                let d = <Self as MonitoringSemantics<AC>>::to_async_stream(child(d), ctx);
                mc::default(e, d)
            }
            ExprKind::IsDefined(e) => {
                let e = <Self as MonitoringSemantics<AC>>::to_async_stream(child(e), ctx);
                mc::is_defined(e)
            }
            ExprKind::When(e) => {
                let e = <Self as MonitoringSemantics<AC>>::to_async_stream(child(e), ctx);
                mc::when(e)
            }
            ExprKind::Latch(e1, e2) => {
                let e1 = <Self as MonitoringSemantics<AC>>::to_async_stream(child(e1), ctx);
                let e2 = <Self as MonitoringSemantics<AC>>::to_async_stream(child(e2), ctx);
                mc::latch(e1, e2)
            }
            ExprKind::Init(e1, e2) => {
                let e1 = <Self as MonitoringSemantics<AC>>::to_async_stream(child(e1), ctx);
                let e2 = <Self as MonitoringSemantics<AC>>::to_async_stream(child(e2), ctx);
                mc::init(e1, e2)
            }
            ExprKind::SIndex(e, i) => {
                let e = <Self as MonitoringSemantics<AC>>::to_async_stream(child(e), ctx);
                mc::sindex(e, i)
            }
            ExprKind::If(b, e1, e2) => {
                let b = <Self as MonitoringSemantics<AC>>::to_async_stream(child(b), ctx);
                let e1 = <Self as MonitoringSemantics<AC>>::to_async_stream(child(e1), ctx);
                let e2 = <Self as MonitoringSemantics<AC>>::to_async_stream(child(e2), ctx);
                mc::if_stm(b, e1, e2)
            }
            ExprKind::List(exprs) => {
                let exprs: Vec<_> = exprs
                    .into_iter()
                    .map(|e| <Self as MonitoringSemantics<AC>>::to_async_stream(child(e), ctx))
                    .collect();
                mc::list(exprs)
            }
            ExprKind::Tuple(exprs) => {
                let exprs: Vec<_> = exprs
                    .into_iter()
                    .map(|e| <Self as MonitoringSemantics<AC>>::to_async_stream(child(e), ctx))
                    .collect();
                mc::tuple(exprs)
            }
            ExprKind::LIndex(e, i) => {
                let e = <Self as MonitoringSemantics<AC>>::to_async_stream(child(e), ctx);
                let i = <Self as MonitoringSemantics<AC>>::to_async_stream(child(i), ctx);
                mc::lindex(e, i)
            }
            ExprKind::LAppend(lst, el) => {
                let lst = <Self as MonitoringSemantics<AC>>::to_async_stream(child(lst), ctx);
                let el = <Self as MonitoringSemantics<AC>>::to_async_stream(child(el), ctx);
                mc::lappend(lst, el)
            }
            ExprKind::LConcat(lst1, lst2) => {
                let lst1 = <Self as MonitoringSemantics<AC>>::to_async_stream(child(lst1), ctx);
                let lst2 = <Self as MonitoringSemantics<AC>>::to_async_stream(child(lst2), ctx);
                mc::lconcat(lst1, lst2)
            }
            ExprKind::LHead(lst) => {
                let lst = <Self as MonitoringSemantics<AC>>::to_async_stream(child(lst), ctx);
                mc::lhead(lst)
            }
            ExprKind::LTail(lst) => {
                let lst = <Self as MonitoringSemantics<AC>>::to_async_stream(child(lst), ctx);
                mc::ltail(lst)
            }
            ExprKind::LLen(lst) => {
                let lst = <Self as MonitoringSemantics<AC>>::to_async_stream(child(lst), ctx);
                mc::llen(lst)
            }
            ExprKind::LMap(_, _) | ExprKind::LFilter(_, _) | ExprKind::LFold(_, _, _) => {
                panic!("higher-order list operations require typed DSRV semantics")
            }
            ExprKind::Map(map) | ExprKind::Struct(map) | ExprKind::ObjectLiteral(map) => {
                let map: BTreeMap<_, _> = map
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            <Self as MonitoringSemantics<AC>>::to_async_stream(child(v), ctx),
                        )
                    })
                    .collect();
                mc::map(map)
            }
            ExprKind::MGet(map, k) => {
                let map = <Self as MonitoringSemantics<AC>>::to_async_stream(child(map), ctx);
                mc::mget(map, k.clone())
            }
            ExprKind::SGet(struct_expr, key) => {
                let value =
                    <Self as MonitoringSemantics<AC>>::to_async_stream(child(struct_expr), ctx);
                match key.parse::<usize>() {
                    Ok(index) => mc::tget(value, index),
                    Err(_) => mc::mget(value, key.clone()),
                }
            }
            ExprKind::MRemove(map, k) => {
                let map = <Self as MonitoringSemantics<AC>>::to_async_stream(child(map), ctx);
                mc::mremove(map, k.clone())
            }
            ExprKind::MInsert(map, k, v) => {
                let map = <Self as MonitoringSemantics<AC>>::to_async_stream(child(map), ctx);
                let v = <Self as MonitoringSemantics<AC>>::to_async_stream(child(v), ctx);
                mc::minsert(map, k.clone(), v)
            }
            ExprKind::MHasKey(map, k) => {
                let map = <Self as MonitoringSemantics<AC>>::to_async_stream(child(map), ctx);
                mc::mhas_key(map, k.clone())
            }
            ExprKind::Sin(v) => {
                let v = <Self as MonitoringSemantics<AC>>::to_async_stream(child(v), ctx);
                mc::sin(v)
            }
            ExprKind::Cos(v) => {
                let v = <Self as MonitoringSemantics<AC>>::to_async_stream(child(v), ctx);
                mc::cos(v)
            }
            ExprKind::Tan(v) => {
                let v = <Self as MonitoringSemantics<AC>>::to_async_stream(child(v), ctx);
                mc::tan(v)
            }
            ExprKind::Abs(v) => {
                let v = <Self as MonitoringSemantics<AC>>::to_async_stream(child(v), ctx);
                mc::abs(v)
            }
            ExprKind::MonitoredAt(var_name, label) => {
                dist_mc::monitored_at::<AC>(var_name.clone(), label.clone(), ctx)
            }
            ExprKind::Dist(u, v) => dist_mc::dist::<AC>(u.clone(), v.clone(), ctx),
        }
    }
}
