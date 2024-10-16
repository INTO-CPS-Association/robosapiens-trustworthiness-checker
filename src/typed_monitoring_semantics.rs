use futures::StreamExt;

use crate::ast::SBinOp;
use crate::core::{MonitoringSemantics, OutputStream, StreamContext, Value};
use crate::lola_streams::{LOLAStream, TypedStreams};
use crate::lola_type_system::{LOLATypeSystem, LOLATypedValue};
use crate::type_checking::{SExprBool, SExprInt, SExprStr, SExprT, SExprTE};
use crate::typed_monitoring_combinators as mc;

#[derive(Clone)]
pub struct TypedUntimedLolaSemantics;

impl MonitoringSemantics<SExprTE, LOLAStream> for TypedUntimedLolaSemantics {
    type StreamSystem = TypedStreams;

    fn to_async_stream(expr: SExprTE, ctx: &dyn StreamContext<Self::StreamSystem>) -> LOLAStream {
        match expr {
            SExprTE::Int(e) => LOLAStream::Int(Self::to_async_stream(e, ctx)),
            SExprTE::Str(e) => LOLAStream::Str(Self::to_async_stream(e, ctx)),
            SExprTE::Bool(e) => LOLAStream::Bool(Self::to_async_stream(e, ctx)),
            SExprTE::Unit(e) => LOLAStream::Unit(Self::to_async_stream(e, ctx)),
        }
    }
}

impl MonitoringSemantics<SExprInt, OutputStream<i64>> for TypedUntimedLolaSemantics {
    type StreamSystem = TypedStreams;

    fn to_async_stream(
        expr: SExprInt,
        ctx: &dyn StreamContext<Self::StreamSystem>,
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
            SExprInt::Var(v) => Box::pin(mc::var(ctx, v).map(|x| match x {
                LOLATypedValue::Int(i) => i,
                _ => panic!("Type error"),
            })),
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

impl MonitoringSemantics<SExprStr, OutputStream<String>> for TypedUntimedLolaSemantics {
    type StreamSystem = TypedStreams;

    fn to_async_stream(
        expr: SExprStr,
        ctx: &dyn StreamContext<Self::StreamSystem>,
    ) -> OutputStream<String> {
        match expr {
            SExprStr::Val(v) => mc::val(v),
            SExprStr::Var(v) => Box::pin(mc::var(ctx, v).map(|x| match x {
                LOLATypedValue::Str(s) => s,
                _ => panic!("Type error"),
            })),
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

impl<V: Value<LOLATypeSystem>> MonitoringSemantics<SExprT<V>, OutputStream<V>>
    for TypedUntimedLolaSemantics
{
    type StreamSystem = TypedStreams;

    fn to_async_stream(
        expr: SExprT<V>,
        ctx: &dyn StreamContext<Self::StreamSystem>,
    ) -> OutputStream<V> {
        match expr {
            SExprT::Val(v) => mc::val(v),
            SExprT::Var(v) => Box::pin(mc::var(ctx, v).map(|x| match V::from_typed_value(x) {
                Some(v) => v,
                None => panic!("Type error"),
            })),
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

impl MonitoringSemantics<SExprBool, OutputStream<bool>> for TypedUntimedLolaSemantics {
    type StreamSystem = TypedStreams;

    fn to_async_stream(
        expr: SExprBool,
        ctx: &dyn StreamContext<Self::StreamSystem>,
    ) -> OutputStream<bool> {
        match expr {
            SExprBool::Val(b) => mc::val(b),
            SExprBool::EqInt(e1, e2) => {
                let e1 = Self::to_async_stream(e1, ctx);
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
