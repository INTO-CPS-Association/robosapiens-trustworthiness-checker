use std::fmt::Debug;

use futures::StreamExt;

use crate::ast::SBinOp;
use crate::core::ConcreteStreamData;
use crate::core::{MonitoringSemantics, OutputStream, StreamContext, StreamData};
use crate::type_checking::{SExprBool, SExprInt, SExprStr, SExprT, SExprTE};
use crate::typed_monitoring_combinators as mc;

#[derive(Clone)]
pub struct TypedUntimedLolaSemantics;

fn to_typed_stream<T: TryFrom<ConcreteStreamData, Error = ()> + Debug>(
    stream: OutputStream<ConcreteStreamData>,
) -> OutputStream<T> {
    Box::pin(stream.map(|x| x.try_into().expect("Type error")))
}

fn from_typed_stream<T: Into<ConcreteStreamData> + StreamData>(
    stream: OutputStream<T>,
) -> OutputStream<ConcreteStreamData> {
    Box::pin(stream.map(|x| x.into()))
}

impl MonitoringSemantics<SExprTE, ConcreteStreamData, ConcreteStreamData>
    for TypedUntimedLolaSemantics
{
    fn to_async_stream(
        expr: SExprTE,
        ctx: &dyn StreamContext<ConcreteStreamData>,
    ) -> OutputStream<ConcreteStreamData> {
        match expr {
            SExprTE::Int(e) => from_typed_stream::<i64>(Self::to_async_stream(e, ctx)),
            SExprTE::Str(e) => from_typed_stream::<String>(Self::to_async_stream(e, ctx)),
            SExprTE::Bool(e) => from_typed_stream::<bool>(Self::to_async_stream(e, ctx)),
            SExprTE::Unit(e) => from_typed_stream::<()>(Self::to_async_stream(e, ctx)),
        }
    }
}

impl MonitoringSemantics<SExprInt, i64, ConcreteStreamData> for TypedUntimedLolaSemantics {
    fn to_async_stream(
        expr: SExprInt,
        ctx: &dyn StreamContext<ConcreteStreamData>,
    ) -> OutputStream<i64> {
        match expr {
            SExprInt::Val(v) => mc::val(v),
            SExprInt::BinOp(e1, e2, op) => {
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                match op {
                    SBinOp::Plus => mc::plus(e1, e2),
                    SBinOp::Minus => mc::minus(e1, e2),
                    SBinOp::Mult => mc::mult(e1, e2),
                }
            }
            SExprInt::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
            SExprInt::Index(e, i, c) => {
                let e = Self::to_async_stream(*e, ctx);
                mc::index(e, i, c)
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

impl MonitoringSemantics<SExprStr, String, ConcreteStreamData> for TypedUntimedLolaSemantics {
    fn to_async_stream(
        expr: SExprStr,
        ctx: &dyn StreamContext<ConcreteStreamData>,
    ) -> OutputStream<String> {
        match expr {
            SExprStr::Val(v) => mc::val(v),
            SExprStr::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
            SExprStr::Index(e, i, c) => {
                let e = Self::to_async_stream(*e, ctx);
                mc::index(e, i, c)
            }
            SExprStr::If(b, e1, e2) => {
                let b = Self::to_async_stream(*b, ctx);
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                mc::if_stm(b, e1, e2)
            }
            SExprStr::Eval(e) => {
                let _ = Self::to_async_stream(*e, ctx);
                unimplemented!("Eval not implemented")
            }
            SExprStr::Concat(x, y) => mc::concat(
                Self::to_async_stream(*x, ctx),
                Self::to_async_stream(*y, ctx),
            ),
        }
    }
}

impl<V: TryFrom<ConcreteStreamData, Error = ()> + StreamData>
    MonitoringSemantics<SExprT<V>, V, ConcreteStreamData> for TypedUntimedLolaSemantics
{
    fn to_async_stream(
        expr: SExprT<V>,
        ctx: &dyn StreamContext<ConcreteStreamData>,
    ) -> OutputStream<V> {
        match expr {
            SExprT::Val(v) => mc::val(v),
            SExprT::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
            SExprT::Index(e, i, c) => {
                let e = Self::to_async_stream(*e, ctx);
                mc::index(e, i, c)
            }
            SExprT::If(b, e1, e2) => {
                let b = Self::to_async_stream(*b, ctx);
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                mc::if_stm(b, e1, e2)
            }
        }
    }
}

impl MonitoringSemantics<SExprBool, bool, ConcreteStreamData> for TypedUntimedLolaSemantics {
    fn to_async_stream(
        expr: SExprBool,
        ctx: &dyn StreamContext<ConcreteStreamData>,
    ) -> OutputStream<bool> {
        match expr {
            SExprBool::Val(b) => mc::val(b),
            SExprBool::EqInt(e1, e2) => {
                let e1: OutputStream<i64> = Self::to_async_stream(e1, ctx);
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
                let e1 = Self::to_async_stream(e1, ctx);
                let e2 = Self::to_async_stream(e2, ctx);
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
            SExprBool::And(e1, e2) => {
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                mc::and(e1, e2)
            }
            SExprBool::Or(e1, e2) => {
                let e1 = Self::to_async_stream(*e1, ctx);
                let e2 = Self::to_async_stream(*e2, ctx);
                mc::or(e1, e2)
            }
        }
    }
}
