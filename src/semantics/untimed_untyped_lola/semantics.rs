use std::collections::BTreeMap;

use super::combinators as mc;
use crate::core::OutputStream;
use crate::core::Value;
use crate::lang::core::parser::ExprParser;
use crate::lang::dynamic_lola::ast::{
    BoolBinOp, CompBinOp, NumericalBinOp, SBinOp, SExpr, StrBinOp,
};
use crate::semantics::AsyncConfig;
use crate::semantics::MonitoringSemantics;
use crate::semantics::StreamContext;
use tracing::debug;

#[derive(Clone)]
pub struct UntimedLolaSemantics<Parser>
where
    Parser: ExprParser<SExpr> + 'static,
{
    _parser: std::marker::PhantomData<Parser>,
}

impl<Ctx, Parser, AC> MonitoringSemantics<SExpr, AC, Ctx> for UntimedLolaSemantics<Parser>
where
    Ctx: StreamContext<Val = Value>,
    Parser: ExprParser<SExpr> + 'static,
    AC: AsyncConfig<Val = Value>,
{
    fn to_async_stream(expr: SExpr, ctx: &Ctx) -> OutputStream<Value> {
        debug!("Creating async stream for expression: {:?}", expr);
        match expr {
            SExpr::Val(v) => {
                debug!("Constant value: {:?}", v);
                mc::val(v)
            }
            SExpr::BinOp(e1, e2, op) => {
                debug!("Binary operation: {:?} {:?} {:?}", e1, op, e2);
                let e1 = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*e1, ctx);
                let e2 = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*e2, ctx);
                match op {
                    SBinOp::NOp(NumericalBinOp::Add) => {
                        debug!("Performing addition operation");
                        mc::plus(e1, e2)
                    }
                    SBinOp::NOp(NumericalBinOp::Sub) => {
                        debug!("Performing subtraction operation");
                        mc::minus(e1, e2)
                    }
                    SBinOp::NOp(NumericalBinOp::Mul) => {
                        debug!("Performing multiplication operation");
                        mc::mult(e1, e2)
                    }
                    SBinOp::NOp(NumericalBinOp::Div) => {
                        debug!("Performing division operation");
                        mc::div(e1, e2)
                    }
                    SBinOp::NOp(NumericalBinOp::Mod) => {
                        debug!("Performing modulo operation");
                        mc::modulo(e1, e2)
                    }
                    SBinOp::BOp(BoolBinOp::Or) => {
                        debug!("Performing logical OR operation");
                        mc::or(e1, e2)
                    }
                    SBinOp::BOp(BoolBinOp::And) => {
                        debug!("Performing logical AND operation");
                        mc::and(e1, e2)
                    }
                    SBinOp::BOp(BoolBinOp::Impl) => {
                        debug!("Performing logical IMPLICATION operation");
                        mc::implication(e1, e2)
                    }
                    SBinOp::SOp(StrBinOp::Concat) => {
                        debug!("Performing string concatenation");
                        mc::concat(e1, e2)
                    }
                    SBinOp::COp(CompBinOp::Eq) => {
                        debug!("Performing equality comparison");
                        mc::eq(e1, e2)
                    }
                    SBinOp::COp(CompBinOp::Le) => {
                        debug!("Performing less than or equal comparison");
                        mc::le(e1, e2)
                    }
                    SBinOp::COp(CompBinOp::Lt) => {
                        debug!("Performing less than comparison");
                        mc::lt(e1, e2)
                    }
                    SBinOp::COp(CompBinOp::Ge) => {
                        debug!("Performing greater than or equal comparison");
                        mc::ge(e1, e2)
                    }
                    SBinOp::COp(CompBinOp::Gt) => {
                        debug!("Performing greater than comparison");
                        mc::gt(e1, e2)
                    }
                }
            }
            SExpr::Not(x) => {
                debug!("Performing logical NOT operation");
                let x = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*x, ctx);
                mc::not(x)
            }
            SExpr::Var(v) => {
                debug!("Accessing variable: {:?}", v);
                mc::var(ctx, v)
            }
            SExpr::Dynamic(e) => {
                let e = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*e, ctx);
                mc::dynamic::<Ctx, Parser>(ctx, e, None, 1)
            }
            SExpr::RestrictedDynamic(e, vs) => {
                let e = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*e, ctx);
                mc::dynamic::<Ctx, Parser>(ctx, e, Some(vs), 1)
            }
            SExpr::Defer(e) => {
                let e = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*e, ctx);
                mc::defer::<Parser>(ctx, e, 1)
            }
            SExpr::Update(e1, e2) => {
                let e1 = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*e1, ctx);
                let e2 = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*e2, ctx);
                mc::update(e1, e2)
            }
            SExpr::Default(e, d) => {
                let e = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*e, ctx);
                let d = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*d, ctx);
                mc::default(e, d)
            }
            SExpr::IsDefined(e) => {
                let e = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*e, ctx);
                mc::is_defined(e)
            }
            SExpr::When(e) => {
                let e = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*e, ctx);
                mc::when(e)
            }
            SExpr::Latch(e1, e2) => {
                let e1 = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*e1, ctx);
                let e2 = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*e2, ctx);
                mc::latch(e1, e2)
            }
            SExpr::Init(e1, e2) => {
                let e1 = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*e1, ctx);
                let e2 = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*e2, ctx);
                mc::init(e1, e2)
            }
            SExpr::SIndex(e, i) => {
                let e = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*e, ctx);
                mc::sindex(e, i)
            }
            SExpr::If(b, e1, e2) => {
                let b = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*b, ctx);
                let e1 = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*e1, ctx);
                let e2 = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*e2, ctx);
                mc::if_stm(b, e1, e2)
            }
            SExpr::List(exprs) => {
                let exprs: Vec<_> = exprs
                    .into_iter()
                    .map(|e| <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(e, ctx))
                    .collect();
                mc::list(exprs)
            }
            SExpr::LIndex(e, i) => {
                let e = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*e, ctx);
                let i = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*i, ctx);
                mc::lindex(e, i)
            }
            SExpr::LAppend(lst, el) => {
                let lst = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*lst, ctx);
                let el = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*el, ctx);
                mc::lappend(lst, el)
            }
            SExpr::LConcat(lst1, lst2) => {
                let lst1 =
                    <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*lst1, ctx);
                let lst2 =
                    <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*lst2, ctx);
                mc::lconcat(lst1, lst2)
            }
            SExpr::LHead(lst) => {
                let lst = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*lst, ctx);
                mc::lhead(lst)
            }
            SExpr::LTail(lst) => {
                let lst = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*lst, ctx);
                mc::ltail(lst)
            }
            SExpr::LLen(lst) => {
                let lst = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*lst, ctx);
                mc::llen(lst)
            }
            SExpr::Map(map) => {
                let map: BTreeMap<_, _> = map
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k,
                            <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(v, ctx),
                        )
                    })
                    .collect();
                mc::map(map)
            }
            SExpr::MGet(map, k) => {
                let map = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*map, ctx);
                mc::mget(map, k)
            }
            SExpr::MRemove(map, k) => {
                let map = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*map, ctx);
                mc::mremove(map, k)
            }
            SExpr::MInsert(map, k, v) => {
                let map = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*map, ctx);
                let v = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*v, ctx);
                mc::minsert(map, k, v)
            }
            SExpr::MHasKey(map, k) => {
                let map = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*map, ctx);
                mc::mhas_key(map, k)
            }
            SExpr::MonitoredAt(_, _) => {
                unimplemented!("Function monitored_at only supported in distributed semantics")
            }
            SExpr::Dist(_, _) => {
                unimplemented!("Function dist only supported in distributed semantics")
            }
            SExpr::Sin(v) => {
                let v = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*v, ctx);
                mc::sin(v)
            }
            SExpr::Cos(v) => {
                let v = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*v, ctx);
                mc::cos(v)
            }
            SExpr::Tan(v) => {
                let v = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*v, ctx);
                mc::tan(v)
            }
            SExpr::Abs(v) => {
                let v = <Self as MonitoringSemantics<SExpr, AC, Ctx>>::to_async_stream(*v, ctx);
                mc::abs(v)
            }
        }
    }
}
