use super::combinators as mc;
use super::helpers::{from_typed_stream, to_typed_stream};
use crate::core::OutputStream;
use crate::core::Value;
use crate::lang::dynamic_lola::ast::{BoolBinOp, FloatBinOp, IntBinOp, StrBinOp};
use crate::lang::dynamic_lola::type_checker::{
    PossiblyDeferred, SExprBool, SExprFloat, SExprInt, SExprStr, SExprTE, SExprUnit,
};
use crate::semantics::{AsyncConfig, MonitoringSemantics, StreamContext};

#[derive(Clone)]
pub struct TypedUntimedLolaSemantics;

impl<AC> MonitoringSemantics<AC> for TypedUntimedLolaSemantics
where
    AC: AsyncConfig<Val = Value, CtxVal = Value, Expr = SExprTE>,
{
    fn to_async_stream(expr: AC::Expr, ctx: &AC::Ctx) -> OutputStream<Value> {
        match expr {
            SExprTE::Int(e) => {
                from_typed_stream::<PossiblyDeferred<i64>>(to_async_stream_int::<AC>(e, ctx))
            }
            SExprTE::Float(e) => {
                from_typed_stream::<PossiblyDeferred<f64>>(to_async_stream_float::<AC>(e, ctx))
            }
            SExprTE::Str(e) => {
                from_typed_stream::<PossiblyDeferred<String>>(to_async_stream_str::<AC>(e, ctx))
            }
            SExprTE::Bool(e) => {
                from_typed_stream::<PossiblyDeferred<bool>>(to_async_stream_bool::<AC>(e, ctx))
            }
            SExprTE::Unit(e) => {
                from_typed_stream::<PossiblyDeferred<()>>(to_async_stream_unit::<AC>(e, ctx))
            }
        }
    }
}

fn to_async_stream_int<AC>(expr: SExprInt, ctx: &AC::Ctx) -> OutputStream<PossiblyDeferred<i64>>
where
    AC: AsyncConfig<Val = Value>,
{
    match expr {
        SExprInt::Val(v) => mc::val(v),
        SExprInt::BinOp(e1, e2, op) => {
            let e1 = to_async_stream_int::<AC>(*e1, ctx);
            let e2 = to_async_stream_int::<AC>(*e2, ctx);
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
            let e = to_async_stream_int::<AC>(*e, ctx);
            mc::sindex(e, i)
        }
        SExprInt::If(b, e1, e2) => {
            let b = to_async_stream_bool::<AC>(*b, ctx);
            let e1 = to_async_stream_int::<AC>(*e1, ctx);
            let e2 = to_async_stream_int::<AC>(*e2, ctx);
            mc::if_stm(b, e1, e2)
        }
        SExprInt::Default(e1, e2) => {
            let e1 = to_async_stream_int::<AC>(*e1, ctx);
            let e2 = to_async_stream_int::<AC>(*e2, ctx);
            mc::default(e1, e2)
        }
    }
}

fn to_async_stream_float<AC>(expr: SExprFloat, ctx: &AC::Ctx) -> OutputStream<PossiblyDeferred<f64>>
where
    AC: AsyncConfig<Val = Value>,
{
    match expr {
        SExprFloat::Val(v) => mc::val(v),
        SExprFloat::BinOp(e1, e2, op) => {
            let e1 = to_async_stream_float::<AC>(*e1, ctx);
            let e2 = to_async_stream_float::<AC>(*e2, ctx);
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
            let e = to_async_stream_float::<AC>(*e, ctx);
            mc::sindex(e, i)
        }
        SExprFloat::If(b, e1, e2) => {
            let b = to_async_stream_bool::<AC>(*b, ctx);
            let e1 = to_async_stream_float::<AC>(*e1, ctx);
            let e2 = to_async_stream_float::<AC>(*e2, ctx);
            mc::if_stm(b, e1, e2)
        }
        SExprFloat::Default(e1, e2) => {
            let e1 = to_async_stream_float::<AC>(*e1, ctx);
            let e2 = to_async_stream_float::<AC>(*e2, ctx);
            mc::default(e1, e2)
        }
    }
}

fn to_async_stream_str<AC>(expr: SExprStr, ctx: &AC::Ctx) -> OutputStream<PossiblyDeferred<String>>
where
    AC: AsyncConfig<Val = Value>,
{
    match expr {
        SExprStr::Val(v) => mc::val(v),
        SExprStr::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
        SExprStr::SIndex(e, i) => {
            let e = to_async_stream_str::<AC>(*e, ctx);
            mc::sindex(e, i)
        }
        SExprStr::If(b, e1, e2) => {
            let b = to_async_stream_bool::<AC>(*b, ctx);
            let e1 = to_async_stream_str::<AC>(*e1, ctx);
            let e2 = to_async_stream_str::<AC>(*e2, ctx);
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
        SExprStr::BinOp(e1, e2, op) => {
            let e1 = to_async_stream_str::<AC>(*e1, ctx);
            let e2 = to_async_stream_str::<AC>(*e2, ctx);
            match op {
                StrBinOp::Concat => mc::concat(e1, e2),
            }
        }
        SExprStr::Default(e1, e2) => {
            let e1 = to_async_stream_str::<AC>(*e1, ctx);
            let e2 = to_async_stream_str::<AC>(*e2, ctx);
            mc::default(e1, e2)
        }
    }
}

fn to_async_stream_bool<AC>(expr: SExprBool, ctx: &AC::Ctx) -> OutputStream<PossiblyDeferred<bool>>
where
    AC: AsyncConfig<Val = Value>,
{
    match expr {
        SExprBool::Val(b) => mc::val(b),
        SExprBool::EqInt(e1, e2) => {
            let e1 = to_async_stream_int::<AC>(e1, ctx);
            let e2 = to_async_stream_int::<AC>(e2, ctx);
            mc::eq(e1, e2)
        }
        SExprBool::EqStr(e1, e2) => {
            let e1 = to_async_stream_str::<AC>(e1, ctx);
            let e2 = to_async_stream_str::<AC>(e2, ctx);
            mc::eq(e1, e2)
        }
        SExprBool::EqUnit(e1, e2) => {
            let e1 = to_async_stream_unit::<AC>(e1, ctx);
            let e2 = to_async_stream_unit::<AC>(e2, ctx);
            mc::eq(e1, e2)
        }
        SExprBool::EqBool(e1, e2) => {
            let e1 = to_async_stream_bool::<AC>(*e1, ctx);
            let e2 = to_async_stream_bool::<AC>(*e2, ctx);
            mc::eq(e1, e2)
        }
        SExprBool::LeInt(e1, e2) => {
            let e1 = to_async_stream_int::<AC>(e1, ctx);
            let e2 = to_async_stream_int::<AC>(e2, ctx);
            mc::le(e1, e2)
        }
        SExprBool::Not(e) => {
            let e = to_async_stream_bool::<AC>(*e, ctx);
            mc::not(e)
        }
        SExprBool::BinOp(e1, e2, BoolBinOp::And) => {
            let e1 = to_async_stream_bool::<AC>(*e1, ctx);
            let e2 = to_async_stream_bool::<AC>(*e2, ctx);
            mc::and(e1, e2)
        }
        SExprBool::BinOp(e1, e2, BoolBinOp::Or) => {
            let e1 = to_async_stream_bool::<AC>(*e1, ctx);
            let e2 = to_async_stream_bool::<AC>(*e2, ctx);
            mc::or(e1, e2)
        }
        SExprBool::BinOp(e1, e2, BoolBinOp::Impl) => {
            let e1 = to_async_stream_bool::<AC>(*e1, ctx);
            let e2 = to_async_stream_bool::<AC>(*e2, ctx);
            mc::implication(e1, e2)
        }
        SExprBool::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
        SExprBool::SIndex(e, i) => {
            let e = to_async_stream_bool::<AC>(*e, ctx);
            mc::sindex(e, i)
        }
        SExprBool::If(b, e1, e2) => {
            let b = to_async_stream_bool::<AC>(*b, ctx);
            let e1 = to_async_stream_bool::<AC>(*e1, ctx);
            let e2 = to_async_stream_bool::<AC>(*e2, ctx);
            mc::if_stm(b, e1, e2)
        }
        SExprBool::Default(e1, e2) => {
            let e1 = to_async_stream_bool::<AC>(*e1, ctx);
            let e2 = to_async_stream_bool::<AC>(*e2, ctx);
            mc::default(e1, e2)
        }
    }
}

fn to_async_stream_unit<AC>(expr: SExprUnit, ctx: &AC::Ctx) -> OutputStream<PossiblyDeferred<()>>
where
    AC: AsyncConfig<Val = Value>,
{
    match expr {
        SExprUnit::Val(v) => mc::val(v),
        SExprUnit::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
        SExprUnit::SIndex(e, i) => {
            let e = to_async_stream_unit::<AC>(*e, ctx);
            mc::sindex(e, i)
        }
        SExprUnit::If(b, e1, e2) => {
            let b = to_async_stream_bool::<AC>(*b, ctx);
            let e1 = to_async_stream_unit::<AC>(*e1, ctx);
            let e2 = to_async_stream_unit::<AC>(*e2, ctx);
            mc::if_stm(b, e1, e2)
        }
        SExprUnit::Default(e1, e2) => {
            let e1 = to_async_stream_unit::<AC>(*e1, ctx);
            let e2 = to_async_stream_unit::<AC>(*e2, ctx);
            mc::default(e1, e2)
        }
    }
}
