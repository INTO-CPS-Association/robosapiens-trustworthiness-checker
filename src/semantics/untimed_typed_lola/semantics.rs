use super::combinators as mc;
use super::helpers::{from_typed_stream, to_typed_stream};
use crate::core::Value;
use crate::core::{MonitoringSemantics, OutputStream, StreamContext};
use crate::lang::dynamic_lola::ast::{BoolBinOp, FloatBinOp, IntBinOp, StrBinOp};
use crate::lang::dynamic_lola::type_checker::{
    PossiblyUnknown, SExprBool, SExprFloat, SExprInt, SExprStr, SExprTE, SExprUnit,
};

#[derive(Clone)]
pub struct TypedUntimedLolaSemantics;

impl<Ctx> MonitoringSemantics<SExprTE, Value, Ctx, Value> for TypedUntimedLolaSemantics
where
    Ctx: StreamContext<Value>,
{
    fn to_async_stream(expr: SExprTE, ctx: &Ctx) -> OutputStream<Value> {
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

impl<Ctx> MonitoringSemantics<SExprInt, PossiblyUnknown<i64>, Ctx, Value>
    for TypedUntimedLolaSemantics
where
    Ctx: StreamContext<Value>,
{
    fn to_async_stream(expr: SExprInt, ctx: &Ctx) -> OutputStream<PossiblyUnknown<i64>> {
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
            SExprInt::SIndex(e, i) => {
                let e = Self::to_async_stream(*e, ctx);
                mc::sindex(e, i, PossiblyUnknown::Unknown)
            }
            SExprInt::If(b, e1, e2) => {
                let b = Self::to_async_stream(*b, ctx);
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                mc::if_stm(b, e1, e2)
            }
            SExprInt::Default(x, y) => mc::default(
                Self::to_async_stream(*x, ctx),
                Self::to_async_stream(*y, ctx),
            ),
        }
    }
}

impl<Ctx> MonitoringSemantics<SExprFloat, PossiblyUnknown<f32>, Ctx, Value>
    for TypedUntimedLolaSemantics
where
    Ctx: StreamContext<Value>,
{
    fn to_async_stream(expr: SExprFloat, ctx: &Ctx) -> OutputStream<PossiblyUnknown<f32>> {
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
            SExprFloat::SIndex(e, i) => {
                let e = Self::to_async_stream(*e, ctx);
                mc::sindex(e, i, PossiblyUnknown::Unknown)
            }
            SExprFloat::If(b, e1, e2) => {
                let b = Self::to_async_stream(*b, ctx);
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                mc::if_stm(b, e1, e2)
            }
            SExprFloat::Default(x, y) => mc::default(
                Self::to_async_stream(*x, ctx),
                Self::to_async_stream(*y, ctx),
            ),
        }
    }
}

impl<Ctx> MonitoringSemantics<SExprStr, PossiblyUnknown<String>, Ctx, Value>
    for TypedUntimedLolaSemantics
where
    Ctx: StreamContext<Value>,
{
    fn to_async_stream(expr: SExprStr, ctx: &Ctx) -> OutputStream<PossiblyUnknown<String>> {
        match expr {
            SExprStr::Val(v) => mc::val(v),
            SExprStr::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
            SExprStr::SIndex(e, i) => {
                let e = Self::to_async_stream(*e, ctx);
                mc::sindex(e, i, PossiblyUnknown::Unknown)
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
            SExprStr::Default(x, y) => mc::default(
                Self::to_async_stream(*x, ctx),
                Self::to_async_stream(*y, ctx),
            ),
        }
    }
}

impl<Ctx> MonitoringSemantics<SExprUnit, PossiblyUnknown<()>, Ctx, Value>
    for TypedUntimedLolaSemantics
where
    Ctx: StreamContext<Value>,
{
    fn to_async_stream(expr: SExprUnit, ctx: &Ctx) -> OutputStream<PossiblyUnknown<()>> {
        match expr {
            SExprUnit::Val(v) => mc::val(v),
            SExprUnit::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
            SExprUnit::SIndex(e, i) => {
                let e = Self::to_async_stream(*e, ctx);
                mc::sindex(e, i, PossiblyUnknown::Unknown)
            }
            SExprUnit::If(b, e1, e2) => {
                let b = Self::to_async_stream(*b, ctx);
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                mc::if_stm(b, e1, e2)
            }
            SExprUnit::Default(x, y) => mc::default(
                Self::to_async_stream(*x, ctx),
                Self::to_async_stream(*y, ctx),
            ),
        }
    }
}

impl<Ctx> MonitoringSemantics<SExprBool, PossiblyUnknown<bool>, Ctx, Value>
    for TypedUntimedLolaSemantics
where
    Ctx: StreamContext<Value>,
{
    fn to_async_stream(expr: SExprBool, ctx: &Ctx) -> OutputStream<PossiblyUnknown<bool>> {
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
            SExprBool::SIndex(e, i) => {
                let e = Self::to_async_stream(*e, ctx);
                mc::sindex(e, i, PossiblyUnknown::Unknown)
            }
            SExprBool::If(b, e1, e2) => {
                let b = Self::to_async_stream(*b, ctx);
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                mc::if_stm(b, e1, e2)
            }
            SExprBool::Default(x, y) => mc::default(
                Self::to_async_stream(*x, ctx),
                Self::to_async_stream(*y, ctx),
            ),
        }
    }
}
