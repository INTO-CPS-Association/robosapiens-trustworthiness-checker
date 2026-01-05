use super::combinators as mc;
use super::helpers::{from_typed_stream, to_typed_stream};
use crate::core::Value;
use crate::core::{OutputStream, StreamData};
use crate::lang::dynamic_lola::ast::{BoolBinOp, FloatBinOp, IntBinOp, StrBinOp};
use crate::lang::dynamic_lola::type_checker::{
    PossiblyDeferred, SExprBool, SExprFloat, SExprInt, SExprStr, SExprTE, SExprUnit,
};
use crate::semantics::{AsyncConfig, MonitoringSemantics, StreamContext};

#[derive(Clone)]
pub struct TypedUntimedLolaSemantics;

// TODO: The ugly syntax with GenConfig should not be needed when AsyncConfig is finished
// This must be fixed throughout the file
struct GenConfig<Val: StreamData> {
    _marker: std::marker::PhantomData<Val>,
}
impl<Val: StreamData> AsyncConfig for GenConfig<Val> {
    type Val = Val;
}

impl<Ctx, AC> MonitoringSemantics<SExprTE, AC, Ctx> for TypedUntimedLolaSemantics
where
    Ctx: StreamContext<Val = Value>,
    AC: AsyncConfig<Val = Value>,
{
    fn to_async_stream(expr: SExprTE, ctx: &Ctx) -> OutputStream<Value> {
        match expr {
            SExprTE::Int(e) => {
                from_typed_stream::<PossiblyDeferred<i64>>(<Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<i64>>,
                    _,
                >>::to_async_stream(
                    e, ctx
                ))
            }
            SExprTE::Float(e) => {
                from_typed_stream::<PossiblyDeferred<f64>>(<Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<f64>>,
                    _,
                >>::to_async_stream(
                    e, ctx
                ))
            }
            SExprTE::Str(e) => {
                from_typed_stream::<PossiblyDeferred<String>>(<Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<String>>,
                    _,
                >>::to_async_stream(
                    e, ctx
                ))
            }
            SExprTE::Bool(e) => {
                from_typed_stream::<PossiblyDeferred<bool>>(<Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<bool>>,
                    _,
                >>::to_async_stream(
                    e, ctx
                ))
            }
            SExprTE::Unit(e) => {
                from_typed_stream::<PossiblyDeferred<()>>(<Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<()>>,
                    _,
                >>::to_async_stream(
                    e, ctx
                ))
            }
        }
    }
}

impl<Ctx, AC> MonitoringSemantics<SExprInt, AC, Ctx> for TypedUntimedLolaSemantics
where
    Ctx: StreamContext<Val = Value>,
    AC: AsyncConfig<Val = PossiblyDeferred<i64>>,
{
    fn to_async_stream(expr: SExprInt, ctx: &Ctx) -> OutputStream<PossiblyDeferred<i64>> {
        match expr {
            SExprInt::Val(v) => mc::val(v),
            SExprInt::BinOp(e1, e2, op) => {
                let e1 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<i64>>,
                    _,
                >>::to_async_stream(
                    *e1, ctx
                );
                let e2 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<i64>>,
                    _,
                >>::to_async_stream(
                    *e2, ctx
                );
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
                let e = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<i64>>,
                    _,
                >>::to_async_stream(
                    *e, ctx
                );
                mc::sindex(e, i)
            }
            SExprInt::If(b, e1, e2) => {
                let b= <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<bool>>,
                    _,
                >>::to_async_stream(
                    *b, ctx
                );
                let e1 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<i64>>,
                    _,
                >>::to_async_stream(
                    *e1, ctx
                );
                let e2 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<i64>>,
                    _,
                >>::to_async_stream(
                    *e2, ctx
                );
                mc::if_stm(b, e1, e2)
            }
            SExprInt::Default(x, y) => {
                let x = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<i64>>,
                    _,
                >>::to_async_stream(
                    *x, ctx
                );
                let y= <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<i64>>,
                    _,
                >>::to_async_stream(
                    *y, ctx
                );
                mc::default(x, y)
            }
        }
    }
}

impl<Ctx, AC> MonitoringSemantics<SExprFloat, AC, Ctx> for TypedUntimedLolaSemantics
where
    Ctx: StreamContext<Val = Value>,
    AC: AsyncConfig<Val = PossiblyDeferred<f64>>,
{
    fn to_async_stream(expr: SExprFloat, ctx: &Ctx) -> OutputStream<PossiblyDeferred<f64>> {
        match expr {
            SExprFloat::Val(v) => mc::val(v),
            SExprFloat::BinOp(e1, e2, op) => {
                let e1 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<f64>>,
                    _,
                >>::to_async_stream(
                    *e1, ctx
                );
                let e2 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<f64>>,
                    _,
                >>::to_async_stream(
                    *e2, ctx
                );
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
                let e = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<f64>>,
                    _,
                >>::to_async_stream(
                    *e, ctx
                );
                mc::sindex(e, i)
            }
            SExprFloat::If(b, e1, e2) => {
                let b= <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<bool>>,
                    _,
                >>::to_async_stream(
                    *b, ctx
                );
                let e1 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<f64>>,
                    _,
                >>::to_async_stream(
                    *e1, ctx
                );
                let e2 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<f64>>,
                    _,
                >>::to_async_stream(
                    *e2, ctx
                );
                mc::if_stm(b, e1, e2)
            }
            SExprFloat::Default(x, y) => {
                let x = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<f64>>,
                    _,
                >>::to_async_stream(
                    *x, ctx
                );
                let y= <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<f64>>,
                    _,
                >>::to_async_stream(
                    *y, ctx
                );
                mc::default(x, y)
            }
        }
    }
}

impl<Ctx, AC> MonitoringSemantics<SExprStr, AC, Ctx> for TypedUntimedLolaSemantics
where
    Ctx: StreamContext<Val = Value>,
    AC: AsyncConfig<Val = PossiblyDeferred<String>>,
{
    fn to_async_stream(expr: SExprStr, ctx: &Ctx) -> OutputStream<PossiblyDeferred<String>> {
        match expr {
            SExprStr::Val(v) => mc::val(v),
            SExprStr::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
            SExprStr::SIndex(e, i) => {
                let e = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<String>>,
                    _,
                >>::to_async_stream(
                    *e, ctx
                );
                mc::sindex(e, i)
            }
            SExprStr::If(b, e1, e2) => {
                let b= <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<bool>>,
                    _,
                >>::to_async_stream(
                    *b, ctx
                );
                let e1 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<String>>,
                    _,
                >>::to_async_stream(
                    *e1, ctx
                );
                let e2 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<String>>,
                    _,
                >>::to_async_stream(
                    *e2, ctx
                );
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
                let e1 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<String>>,
                    _,
                >>::to_async_stream(
                    *e1, ctx
                );
                let e2 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<String>>,
                    _,
                >>::to_async_stream(
                    *e2, ctx
                );
                match op {
                    StrBinOp::Concat => mc::concat(e1, e2),
                }
            }
            SExprStr::Default(x, y) => {
                let x = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<String>>,
                    _,
                >>::to_async_stream(
                    *x, ctx
                );
                let y= <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<String>>,
                    _,
                >>::to_async_stream(
                    *y, ctx
                );
                mc::default(x, y)
            }
        }
    }
}

impl<Ctx, AC> MonitoringSemantics<SExprUnit, AC, Ctx> for TypedUntimedLolaSemantics
where
    Ctx: StreamContext<Val = Value>,
    AC: AsyncConfig<Val = PossiblyDeferred<()>>,
{
    fn to_async_stream(expr: SExprUnit, ctx: &Ctx) -> OutputStream<PossiblyDeferred<()>> {
        match expr {
            SExprUnit::Val(v) => mc::val(v),
            SExprUnit::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
            SExprUnit::SIndex(e, i) => {
                let e = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<()>>,
                    _,
                >>::to_async_stream(
                    *e, ctx
                );
                mc::sindex(e, i)
            }
            SExprUnit::If(b, e1, e2) => {
                let b= <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<bool>>,
                    _,
                >>::to_async_stream(
                    *b, ctx
                );
                let e1 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<()>>,
                    _,
                >>::to_async_stream(
                    *e1, ctx
                );
                let e2 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<()>>,
                    _,
                >>::to_async_stream(
                    *e2, ctx
                );
                mc::if_stm(b, e1, e2)
            }
            SExprUnit::Default(x, y) => {
                let x = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<()>>,
                    _,
                >>::to_async_stream(
                    *x, ctx
                );
                let y= <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<()>>,
                    _,
                >>::to_async_stream(
                    *y, ctx
                );
                mc::default(x, y)
            }
        }
    }
}

impl<Ctx, AC> MonitoringSemantics<SExprBool, AC, Ctx> for TypedUntimedLolaSemantics
where
    Ctx: StreamContext<Val = Value>,
    AC: AsyncConfig<Val = PossiblyDeferred<bool>>,
{
    fn to_async_stream(expr: SExprBool, ctx: &Ctx) -> OutputStream<PossiblyDeferred<bool>> {
        match expr {
            SExprBool::Val(b) => mc::val(b),
            // TODO: Need to do exactly like above, but for i64 here, for Str in the next one, etc.
            SExprBool::EqInt(e1, e2) => {
                let e1 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<i64>>,
                    _,
                >>::to_async_stream(
                    e1, ctx
                );
                let e2 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<i64>>,
                    _,
                >>::to_async_stream(
                    e2, ctx
                );
                mc::eq(e1, e2)
            }
            SExprBool::EqStr(e1, e2) => {
                let e1 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<String>>,
                    _,
                >>::to_async_stream(
                    e1, ctx
                );
                let e2 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<String>>,
                    _,
                >>::to_async_stream(
                    e2, ctx
                );
                mc::eq(e1, e2)
            }
            SExprBool::EqUnit(e1, e2) => {
                let e1 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<()>>,
                    _,
                >>::to_async_stream(
                    e1, ctx
                );
                let e2 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<()>>,
                    _,
                >>::to_async_stream(
                    e2, ctx
                );
                mc::eq(e1, e2)
            }
            SExprBool::EqBool(e1, e2) => {
                let e1 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<bool>>,
                    _,
                >>::to_async_stream(
                    *e1, ctx
                );
                let e2 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<bool>>,
                    _,
                >>::to_async_stream(
                    *e2, ctx
                );
                mc::eq(e1, e2)
            }
            SExprBool::LeInt(e1, e2) => {
                let e1 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<i64>>,
                    _,
                >>::to_async_stream(
                    e1, ctx
                );
                let e2 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<i64>>,
                    _,
                >>::to_async_stream(
                    e2, ctx
                );
                mc::le(e1, e2)
            }
            SExprBool::Not(e) => {
                let e = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<bool>>,
                    _,
                >>::to_async_stream(
                    *e, ctx
                );
                mc::not(e)
            }
            SExprBool::BinOp(e1, e2, BoolBinOp::And) => {
                let e1 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<bool>>,
                    _,
                >>::to_async_stream(
                    *e1, ctx
                );
                let e2 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<bool>>,
                    _,
                >>::to_async_stream(
                    *e2, ctx
                );
                mc::and(e1, e2)
            }
            SExprBool::BinOp(e1, e2, BoolBinOp::Or) => {
                let e1 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<bool>>,
                    _,
                >>::to_async_stream(
                    *e1, ctx
                );
                let e2 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<bool>>,
                    _,
                >>::to_async_stream(
                    *e2, ctx
                );
                mc::or(e1, e2)
            }
            SExprBool::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
            SExprBool::SIndex(e, i) => {
                let e = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<bool>>,
                    _,
                >>::to_async_stream(
                    *e, ctx
                );
                mc::sindex(e, i)
            }
            SExprBool::If(b, e1, e2) => {
                let b= <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<bool>>,
                    _,
                >>::to_async_stream(
                    *b, ctx
                );
                let e1 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<bool>>,
                    _,
                >>::to_async_stream(
                    *e1, ctx
                );
                let e2 = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<bool>>,
                    _,
                >>::to_async_stream(
                    *e2, ctx
                );
                mc::if_stm(b, e1, e2)
            }
            SExprBool::Default(x, y) => {
                let x = <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<bool>>,
                    _,
                >>::to_async_stream(
                    *x, ctx
                );
                let y= <Self as MonitoringSemantics<
                    _,
                    GenConfig<PossiblyDeferred<bool>>,
                    _,
                >>::to_async_stream(
                    *y, ctx
                );
                mc::default(x, y)
            }
        }
    }
}
