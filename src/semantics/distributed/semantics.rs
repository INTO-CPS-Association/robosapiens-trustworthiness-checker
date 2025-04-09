use crate::core::Value;
use crate::core::{MonitoringSemantics, OutputStream};
use crate::lang::dist_lang::ast::DistSExpr;
use crate::lang::dynamic_lola::ast::{BoolBinOp, CompBinOp, NumericalBinOp, SBinOp, StrBinOp};
use crate::semantics::distributed::combinators as dist_mc;
use crate::semantics::untimed_untyped_lola::combinators as mc;

use super::combinators::DistributedContext;

#[derive(Clone)]
pub struct DistributedSemantics;

impl MonitoringSemantics<DistSExpr, Value, DistributedContext<Value>> for DistributedSemantics {
    fn to_async_stream(expr: DistSExpr, ctx: &DistributedContext<Value>) -> OutputStream<Value> {
        match expr {
            DistSExpr::Val(v) => mc::val(v),
            DistSExpr::BinOp(e1, e2, op) => {
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                match op {
                    SBinOp::NOp(NumericalBinOp::Add) => mc::plus(e1, e2),
                    SBinOp::NOp(NumericalBinOp::Sub) => mc::minus(e1, e2),
                    SBinOp::NOp(NumericalBinOp::Mul) => mc::mult(e1, e2),
                    SBinOp::NOp(NumericalBinOp::Div) => mc::div(e1, e2),
                    SBinOp::NOp(NumericalBinOp::Mod) => mc::modulo(e1, e2),
                    SBinOp::BOp(BoolBinOp::Or) => mc::or(e1, e2),
                    SBinOp::BOp(BoolBinOp::And) => mc::and(e1, e2),
                    SBinOp::SOp(StrBinOp::Concat) => mc::concat(e1, e2),
                    SBinOp::COp(CompBinOp::Eq) => mc::eq(e1, e2),
                    SBinOp::COp(CompBinOp::Le) => mc::le(e1, e2),
                    SBinOp::COp(CompBinOp::Lt) => mc::lt(e1, e2),
                    SBinOp::COp(CompBinOp::Ge) => mc::ge(e1, e2),
                    SBinOp::COp(CompBinOp::Gt) => mc::gt(e1, e2),
                }
            }
            DistSExpr::Not(x) => {
                let x = Self::to_async_stream(*x, ctx);
                mc::not(x)
            }
            DistSExpr::Var(v) => mc::var(ctx, v),
            DistSExpr::Dynamic(e) => {
                let e = Self::to_async_stream(*e, ctx);
                mc::dynamic(ctx, e, None, 10)
            }
            DistSExpr::RestrictedDynamic(e, vs) => {
                let e = Self::to_async_stream(*e, ctx);
                mc::dynamic(ctx, e, Some(vs), 10)
            }
            DistSExpr::Defer(e) => {
                let e = Self::to_async_stream(*e, ctx);
                mc::defer(ctx, e, 10)
            }
            DistSExpr::Update(e1, e2) => {
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                mc::update(e1, e2)
            }
            DistSExpr::Default(e, d) => {
                let e = Self::to_async_stream(*e, ctx);
                let d = Self::to_async_stream(*d, ctx);
                mc::default(e, d)
            }
            DistSExpr::IsDefined(e) => {
                let e = Self::to_async_stream(*e, ctx);
                mc::is_defined(e)
            }
            DistSExpr::When(e) => {
                let e = Self::to_async_stream(*e, ctx);
                mc::when(e)
            }
            DistSExpr::SIndex(e, i) => {
                let e = Self::to_async_stream(*e, ctx);
                mc::sindex(e, i)
            }
            DistSExpr::If(b, e1, e2) => {
                let b = Self::to_async_stream(*b, ctx);
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                mc::if_stm(b, e1, e2)
            }
            DistSExpr::List(exprs) => {
                let exprs: Vec<_> = exprs
                    .into_iter()
                    .map(|e| Self::to_async_stream(e, ctx))
                    .collect();
                mc::list(exprs)
            }
            DistSExpr::LIndex(e, i) => {
                let e = Self::to_async_stream(*e, ctx);
                let i = Self::to_async_stream(*i, ctx);
                mc::lindex(e, i)
            }
            DistSExpr::LAppend(lst, el) => {
                let lst = Self::to_async_stream(*lst, ctx);
                let el = Self::to_async_stream(*el, ctx);
                mc::lappend(lst, el)
            }
            DistSExpr::LConcat(lst1, lst2) => {
                let lst1 = Self::to_async_stream(*lst1, ctx);
                let lst2 = Self::to_async_stream(*lst2, ctx);
                mc::lconcat(lst1, lst2)
            }
            DistSExpr::LHead(lst) => {
                let lst = Self::to_async_stream(*lst, ctx);
                mc::lhead(lst)
            }
            DistSExpr::LTail(lst) => {
                let lst = Self::to_async_stream(*lst, ctx);
                mc::ltail(lst)
            }
            DistSExpr::Sin(v) => {
                let v = Self::to_async_stream(*v, ctx);
                mc::sin(v)
            }
            DistSExpr::Cos(v) => {
                let v = Self::to_async_stream(*v, ctx);
                mc::cos(v)
            }
            DistSExpr::Tan(v) => {
                let v = Self::to_async_stream(*v, ctx);
                mc::tan(v)
            }
            DistSExpr::MonitoredAt(var_name, label) => dist_mc::monitored_at(var_name, label, ctx),
        }
    }
}
