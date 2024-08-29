use crate::ast::{BExpr, SExpr};
use crate::core::{
    MonitoringSemantics, OutputStream, StreamContext, StreamData, StreamExpr, VarName,
};
use crate::untimed_monitoring_combinators as mc;

#[derive(Clone)]
pub struct UntimedLolaSemantics();
pub const UNTIMED_LOLA_SEMANTICS: UntimedLolaSemantics = UntimedLolaSemantics();

impl MonitoringSemantics<SExpr<VarName>> for UntimedLolaSemantics {
    fn to_async_stream(&self, expr: SExpr<VarName>, ctx: &impl StreamContext) -> OutputStream {
        match expr {
            SExpr::Val(v) => mc::val(v),
            SExpr::Plus(e1, e2) => {
                let e1 = self.to_async_stream(*e1, ctx);
                let e2 = self.to_async_stream(*e2, ctx);
                mc::plus(e1, e2)
            }
            SExpr::Minus(e1, e2) => {
                let e1 = self.to_async_stream(*e1, ctx);
                let e2 = self.to_async_stream(*e2, ctx);
                mc::minus(e1, e2)
            }
            SExpr::Mult(e1, e2) => {
                let e1 = self.to_async_stream(*e1, ctx);
                let e2 = self.to_async_stream(*e2, ctx);
                mc::mult(e1, e2)
            }
            SExpr::Var(v) => mc::var(ctx, v),
            SExpr::Eval(e) => {
                let e = self.to_async_stream(*e, ctx);
                mc::eval(self.clone(), ctx, e)
            }
            SExpr::Index(e, i, c) => {
                let e = self.to_async_stream(*e, ctx);
                mc::index(e, i, c)
            }
            SExpr::If(b, e1, e2) => {
                let b = self.to_async_stream(*b, ctx);
                let e1 = self.to_async_stream(*e1, ctx);
                let e2 = self.to_async_stream(*e2, ctx);
                mc::if_stm(b, e1, e2)
            }
        }
    }
}

impl MonitoringSemantics<BExpr<VarName>> for UntimedLolaSemantics {
    fn to_async_stream(&self, expr: BExpr<VarName>, ctx: &impl StreamContext) -> OutputStream {
        match expr {
            BExpr::Val(b) => mc::val(StreamData::Bool(b)),
            BExpr::Eq(e1, e2) => {
                let e1 = self.to_async_stream(*e1, ctx);
                let e2 = self.to_async_stream(*e2, ctx);
                mc::eq(e1, e2)
            }
            BExpr::Le(e1, e2) => {
                let e1 = self.to_async_stream(*e1, ctx);
                let e2 = self.to_async_stream(*e2, ctx);
                mc::le(e1, e2)
            }
            BExpr::Not(e) => {
                let e = self.to_async_stream(*e, ctx);
                mc::not(e)
            }
            BExpr::And(e1, e2) => {
                let e1 = self.to_async_stream(*e1, ctx);
                let e2 = self.to_async_stream(*e2, ctx);
                mc::and(e1, e2)
            }
            BExpr::Or(e1, e2) => {
                let e1 = self.to_async_stream(*e1, ctx);
                let e2 = self.to_async_stream(*e2, ctx);
                mc::or(e1, e2)
            }
        }
    }
}
