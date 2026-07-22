use std::collections::BTreeMap;

use crate::VarName;
use crate::core::OutputStream;
use crate::core::{RuntimeFunction, Value};
use crate::lang::dsrv::ast::{Expr, ExprRef, ExprView};
use crate::semantics::distributed::combinators as dist_mc;
use crate::semantics::untimed_dsrv::{combinators as mc, core_evaluation};
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
    use ExprView::*;

    let evaluate = |child| evaluate_expr::<AC>(child, ctx, owner.clone());
    if let Some(stream) = core_evaluation::evaluate(expr, &evaluate) {
        return stream;
    }

    match expr.view() {
        Lambda(params, body) => {
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
        Fix(func) => mc::val(Value::Function(RuntimeFunction::opaque(format!(
            "fix({})",
            func
        )))),
        Apply(_, _) | Partial(_, _) => {
            panic!("function application requires typed DSRV semantics")
        }
        Var(v) => mc::var::<AC>(ctx, v.clone()),
        Dynamic(source, _, scope) => {
            let e = evaluate(source);
            mc::dynamic::<AC>(ctx, e, scope.clone(), owner.clone(), 10)
        }
        Defer(source, _, scope) => {
            let e = evaluate(source);
            mc::defer::<AC>(ctx, e, scope.clone(), owner.clone(), 10)
        }
        LMap(_, _) | LFilter(_, _) | LFold(_, _, _) => {
            panic!("higher-order list operations require typed DSRV semantics")
        }
        Map(map) | Struct(map) | ObjectLiteral(map) => {
            let map: BTreeMap<_, _> = map.iter().map(|(k, v)| (k.clone(), evaluate(v))).collect();
            mc::map(map)
        }
        SGet(struct_expr, key) => {
            let value = evaluate(struct_expr);
            match key.parse::<usize>() {
                Ok(index) => mc::tget(value, index),
                Err(_) => mc::mget(value, key.clone()),
            }
        }
        MonitoredAt(var_name, label) => {
            dist_mc::monitored_at::<AC>(var_name.clone(), label.clone(), ctx)
        }
        Dist(u, v) => dist_mc::dist::<AC>(u.clone(), v.clone(), ctx),
        _ => unreachable!("shared DSRV expression was not dispatched"),
    }
}
