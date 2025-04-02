use crate::core::Value;
use crate::core::{MonitoringSemantics, OutputStream, StreamContext};
use crate::lang::dynamic_lola::ast::{BoolBinOp, FloatBinOp, IntBinOp, StrBinOp};
use crate::lang::dynamic_lola::type_checker::{
    PossiblyUnknown, SExprBool, SExprFloat, SExprInt, SExprStr, SExprTE, SExprUnit,
};
// use crate::semantics::typed_monitoring_semantics as mc;
pub(super) mod combinators;
use combinators as mc;
pub use combinators::{from_typed_stream, to_typed_stream};

#[derive(Clone)]
pub struct TypedUntimedLolaSemantics;

impl MonitoringSemantics<SExprTE, Value, Value> for TypedUntimedLolaSemantics {
    fn to_async_stream(expr: SExprTE, ctx: &dyn StreamContext<Value>) -> OutputStream<Value> {
        match expr {
            SExprTE::Int(e) => {
                from_typed_stream::<PossiblyUnknown<i64>>(Self::to_async_stream(e, ctx))
            }
            SExprTE::Float(e) => {
                from_typed_stream::<PossiblyUnknown<f32>>(Self::to_async_stream(e, ctx))
            }
            SExprTE::Str(e) => {
                from_typed_stream::<PossiblyUnknown<String>>(Self::to_async_stream(e, ctx))
            }
            SExprTE::Bool(e) => {
                from_typed_stream::<PossiblyUnknown<bool>>(Self::to_async_stream(e, ctx))
            }
            SExprTE::Unit(e) => {
                from_typed_stream::<PossiblyUnknown<()>>(Self::to_async_stream(e, ctx))
            }
        }
    }
}

impl MonitoringSemantics<SExprInt, PossiblyUnknown<i64>, Value> for TypedUntimedLolaSemantics {
    fn to_async_stream(
        expr: SExprInt,
        ctx: &dyn StreamContext<Value>,
    ) -> OutputStream<PossiblyUnknown<i64>> {
        match expr {
            SExprInt::Val(v) => mc::val(v),
            SExprInt::BinOp(e1, e2, op) => {
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                match op {
                    IntBinOp::Add => mc::plus(e1, e2),
                    IntBinOp::Sub => mc::minus(e1, e2),
                    IntBinOp::Mul => mc::mult(e1, e2),
                    IntBinOp::Div => mc::div(e1, e2),
                    IntBinOp::Mod => mc::modulo(e1, e2),
                }
            }
            SExprInt::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
            SExprInt::SIndex(e, i, c) => {
                let e = Self::to_async_stream(*e, ctx);
                mc::sindex(e, i, PossiblyUnknown::Known(c))
            }
            SExprInt::If(b, e1, e2) => {
                let b = Self::to_async_stream(*b, ctx);
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                mc::if_stm(b, e1, e2)
            }
        }
    }
}

impl MonitoringSemantics<SExprFloat, PossiblyUnknown<f32>, Value> for TypedUntimedLolaSemantics {
    fn to_async_stream(
        expr: SExprFloat,
        ctx: &dyn StreamContext<Value>,
    ) -> OutputStream<PossiblyUnknown<f32>> {
        match expr {
            SExprFloat::Val(v) => mc::val(v),
            SExprFloat::BinOp(e1, e2, op) => {
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                match op {
                    FloatBinOp::Add => mc::plus(e1, e2),
                    FloatBinOp::Sub => mc::minus(e1, e2),
                    FloatBinOp::Mul => mc::mult(e1, e2),
                    FloatBinOp::Div => mc::div(e1, e2),
                    FloatBinOp::Mod => mc::modulo(e1, e2),
                }
            }
            SExprFloat::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
            SExprFloat::SIndex(e, i, c) => {
                let e = Self::to_async_stream(*e, ctx);
                mc::sindex(e, i, PossiblyUnknown::Known(c))
            }
            SExprFloat::If(b, e1, e2) => {
                let b = Self::to_async_stream(*b, ctx);
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                mc::if_stm(b, e1, e2)
            }
        }
    }
}

impl MonitoringSemantics<SExprStr, PossiblyUnknown<String>, Value> for TypedUntimedLolaSemantics {
    fn to_async_stream(
        expr: SExprStr,
        ctx: &dyn StreamContext<Value>,
    ) -> OutputStream<PossiblyUnknown<String>> {
        match expr {
            SExprStr::Val(v) => mc::val(v),
            SExprStr::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
            SExprStr::SIndex(e, i, c) => {
                let e = Self::to_async_stream(*e, ctx);
                mc::sindex(e, i, PossiblyUnknown::Known(c))
            }
            SExprStr::If(b, e1, e2) => {
                let b = Self::to_async_stream(*b, ctx);
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                mc::if_stm(b, e1, e2)
            }
            SExprStr::Dynamic(_) => {
                // mc::dynamic(ctx, Self::to_async_stream(*e, ctx), None, 10)
                todo!();
            }
            SExprStr::RestrictedDynamic(_, _) => {
                // mc::dynamic(ctx, Self::to_async_stream(*e, ctx), Some(vs), 10)
                todo!();
            }
            SExprStr::BinOp(x, y, StrBinOp::Concat) => mc::concat(
                Self::to_async_stream(*x, ctx),
                Self::to_async_stream(*y, ctx),
            ),
        }
    }
}

impl MonitoringSemantics<SExprUnit, PossiblyUnknown<()>, Value> for TypedUntimedLolaSemantics {
    fn to_async_stream(
        expr: SExprUnit,
        ctx: &dyn StreamContext<Value>,
    ) -> OutputStream<PossiblyUnknown<()>> {
        match expr {
            SExprUnit::Val(v) => mc::val(v),
            SExprUnit::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
            SExprUnit::SIndex(e, i, c) => {
                let e = Self::to_async_stream(*e, ctx);
                mc::sindex(e, i, PossiblyUnknown::Known(c))
            }
            SExprUnit::If(b, e1, e2) => {
                let b = Self::to_async_stream(*b, ctx);
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                mc::if_stm(b, e1, e2)
            }
        }
    }
}

impl MonitoringSemantics<SExprBool, PossiblyUnknown<bool>, Value> for TypedUntimedLolaSemantics {
    fn to_async_stream(
        expr: SExprBool,
        ctx: &dyn StreamContext<Value>,
    ) -> OutputStream<PossiblyUnknown<bool>> {
        match expr {
            SExprBool::Val(b) => mc::val(b),
            SExprBool::EqInt(e1, e2) => {
                let e1: OutputStream<PossiblyUnknown<i64>> = Self::to_async_stream(e1, ctx);
                let e2 = Self::to_async_stream(e2, ctx);
                mc::eq(e1, e2)
            }
            SExprBool::EqStr(e1, e2) => {
                let e1 = Self::to_async_stream(e1, ctx);
                let e2 = Self::to_async_stream(e2, ctx);
                mc::eq(e1, e2)
            }
            SExprBool::EqUnit(e1, e2) => {
                let e1 = Self::to_async_stream(e1, ctx);
                let e2 = Self::to_async_stream(e2, ctx);
                mc::eq(e1, e2)
            }
            SExprBool::EqBool(e1, e2) => {
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                mc::eq(e1, e2)
            }
            SExprBool::LeInt(e1, e2) => {
                let e1 = Self::to_async_stream(e1, ctx);
                let e2 = Self::to_async_stream(e2, ctx);
                mc::le(e1, e2)
            }
            SExprBool::Not(e) => {
                let e = Self::to_async_stream(*e, ctx);
                mc::not(e)
            }
            SExprBool::BinOp(e1, e2, BoolBinOp::And) => {
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                mc::and(e1, e2)
            }
            SExprBool::BinOp(e1, e2, BoolBinOp::Or) => {
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                mc::or(e1, e2)
            }
            SExprBool::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
            SExprBool::SIndex(e, i, c) => {
                let e = Self::to_async_stream(*e, ctx);
                mc::sindex(e, i, PossiblyUnknown::Known(c))
            }
            SExprBool::If(b, e1, e2) => {
                let b = Self::to_async_stream(*b, ctx);
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                mc::if_stm(b, e1, e2)
            }
        }
    }
}
