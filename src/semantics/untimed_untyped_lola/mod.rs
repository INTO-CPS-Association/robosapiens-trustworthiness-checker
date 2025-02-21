use crate::core::Value;
use crate::core::{MonitoringSemantics, OutputStream, StreamContext, VarName};
use crate::lang::dynamic_lola::ast::{BoolBinOp, CompBinOp, IntBinOp, SBinOp, SExpr, StrBinOp};
use combinators as mc;
pub mod combinators;

#[derive(Clone)]
pub struct UntimedLolaSemantics;

impl MonitoringSemantics<SExpr<VarName>, Value> for UntimedLolaSemantics {
    fn to_async_stream(
        expr: SExpr<VarName>,
        ctx: &dyn StreamContext<Value>,
    ) -> OutputStream<Value> {
        match expr {
            SExpr::Val(v) => mc::val(v),
            SExpr::BinOp(e1, e2, op) => {
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                match op {
                    SBinOp::IOp(IntBinOp::Add) => mc::plus(e1, e2),
                    SBinOp::IOp(IntBinOp::Sub) => mc::minus(e1, e2),
                    SBinOp::IOp(IntBinOp::Mul) => mc::mult(e1, e2),
                    SBinOp::IOp(IntBinOp::Div) => mc::div(e1, e2),
                    SBinOp::BOp(BoolBinOp::Or) => mc::or(e1, e2),
                    SBinOp::BOp(BoolBinOp::And) => mc::and(e1, e2),
                    SBinOp::SOp(StrBinOp::Concat) => mc::concat(e1, e2),
                    SBinOp::COp(CompBinOp::Eq) => mc::eq(e1, e2),
                    SBinOp::COp(CompBinOp::Le) => mc::le(e1, e2),
                }
            }
            SExpr::Not(x) => {
                let x = Self::to_async_stream(*x, ctx);
                mc::not(x)
            }
            SExpr::Var(v) => mc::var(ctx, v),
            SExpr::Eval(e) => {
                let e = Self::to_async_stream(*e, ctx);
                mc::eval(ctx, e, 10)
            }
            SExpr::Defer(e) => {
                let e = Self::to_async_stream(*e, ctx);
                mc::defer(ctx, e, 10)
            }
            SExpr::Update(e1, e2) => {
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                mc::update(e1, e2)
            }
            SExpr::SIndex(e, i, c) => {
                let e = Self::to_async_stream(*e, ctx);
                mc::sindex(e, i, c)
            }
            SExpr::If(b, e1, e2) => {
                let b = Self::to_async_stream(*b, ctx);
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                mc::if_stm(b, e1, e2)
            }
            SExpr::List(exprs) => {
                let exprs: Vec<_> = exprs
                    .into_iter()
                    .map(|e| Self::to_async_stream(e, ctx))
                    .collect();
                mc::list(exprs)
            }
            SExpr::LIndex(e, i) => {
                let e = Self::to_async_stream(*e, ctx);
                let i = Self::to_async_stream(*i, ctx);
                mc::lindex(e, i)
            }
            SExpr::LAppend(lst, el) => {
                let lst = Self::to_async_stream(*lst, ctx);
                let el = Self::to_async_stream(*el, ctx);
                mc::lappend(lst, el)
            }
            SExpr::LConcat(lst1, lst2) => {
                let lst1 = Self::to_async_stream(*lst1, ctx);
                let lst2 = Self::to_async_stream(*lst2, ctx);
                mc::lconcat(lst1, lst2)
            }
            SExpr::LHead(lst) => {
                let lst = Self::to_async_stream(*lst, ctx);
                mc::lhead(lst)
            }
            SExpr::LTail(lst) => {
                let lst = Self::to_async_stream(*lst, ctx);
                mc::ltail(lst)
            }
        }
    }
}
