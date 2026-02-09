use super::combinators as mc;
use crate::core::OutputStream;
use crate::core::Value;
use crate::core::stream_casting::{from_typed_stream, to_typed_stream};
use crate::lang::core::parser::ExprParser;
use crate::lang::dynamic_lola::ast::SExpr;
use crate::lang::dynamic_lola::ast::{BoolBinOp, FloatBinOp, IntBinOp, StrBinOp};
use crate::lang::dynamic_lola::type_checker::{
    PartialStreamValue, SExprBool, SExprFloat, SExprInt, SExprStr, SExprTE, SExprUnit,
};
use crate::semantics::{AsyncConfig, MonitoringSemantics, StreamContext};

#[derive(Clone)]
pub struct TypedUntimedLolaSemantics<Parser>
where
    Parser: ExprParser<SExpr> + 'static,
{
    _parser: std::marker::PhantomData<Parser>,
}

impl<Parser, AC> MonitoringSemantics<AC> for TypedUntimedLolaSemantics<Parser>
where
    Parser: ExprParser<SExpr> + 'static,
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
{
    fn to_async_stream(expr: AC::Expr, ctx: &AC::Ctx) -> OutputStream<Value> {
        match expr {
            SExprTE::Int(e) => from_typed_stream::<PartialStreamValue<i64>>(to_async_stream_int::<
                AC,
                Parser,
            >(e, ctx)),
            SExprTE::Float(e) => from_typed_stream::<PartialStreamValue<f64>>(
                to_async_stream_float::<AC, Parser>(e, ctx),
            ),
            SExprTE::Str(e) => from_typed_stream::<PartialStreamValue<String>>(
                to_async_stream_str::<AC, Parser>(e, ctx),
            ),
            SExprTE::Bool(e) => from_typed_stream::<PartialStreamValue<bool>>(
                to_async_stream_bool::<AC, Parser>(e, ctx),
            ),
            SExprTE::Unit(e) => from_typed_stream::<PartialStreamValue<()>>(
                to_async_stream_unit::<AC, Parser>(e, ctx),
            ),
        }
    }
}

fn to_async_stream_int<AC, Parser>(
    expr: SExprInt,
    ctx: &AC::Ctx,
) -> OutputStream<PartialStreamValue<i64>>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SExpr> + 'static,
{
    match expr {
        SExprInt::Val(v) => mc::val(v),
        SExprInt::BinOp(e1, e2, op) => {
            let e1 = to_async_stream_int::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_int::<AC, Parser>(*e2, ctx);
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
            let e = to_async_stream_int::<AC, Parser>(*e, ctx);
            mc::sindex(e, i)
        }
        SExprInt::If(b, e1, e2) => {
            let b = to_async_stream_bool::<AC, Parser>(*b, ctx);
            let e1 = to_async_stream_int::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_int::<AC, Parser>(*e2, ctx);
            mc::if_stm(b, e1, e2)
        }
        SExprInt::Default(e1, e2) => {
            let e1 = to_async_stream_int::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_int::<AC, Parser>(*e2, ctx);
            mc::default(e1, e2)
        }
        SExprInt::Defer(e, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::defer::<AC, Parser, i64>(ctx, e, 1, &type_ctx)
        }
        SExprInt::Dynamic(e, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, i64>(ctx, e, None, 1, &type_ctx)
        }
        SExprInt::RestrictedDynamic(e, vs, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, i64>(ctx, e, Some(vs), 1, &type_ctx)
        }
        SExprInt::Abs(e) => {
            let e = to_async_stream_int::<AC, Parser>(*e, ctx);
            mc::abs_int(e)
        }
        SExprInt::Init(e1, e2) => {
            let e1 = to_async_stream_int::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_int::<AC, Parser>(*e2, ctx);
            mc::init(e1, e2)
        }
    }
}

fn to_async_stream_float<AC, Parser>(
    expr: SExprFloat,
    ctx: &AC::Ctx,
) -> OutputStream<PartialStreamValue<f64>>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SExpr> + 'static,
{
    match expr {
        SExprFloat::Val(v) => mc::val(v),
        SExprFloat::BinOp(e1, e2, op) => {
            let e1 = to_async_stream_float::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_float::<AC, Parser>(*e2, ctx);
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
            let e = to_async_stream_float::<AC, Parser>(*e, ctx);
            mc::sindex(e, i)
        }
        SExprFloat::If(b, e1, e2) => {
            let b = to_async_stream_bool::<AC, Parser>(*b, ctx);
            let e1 = to_async_stream_float::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_float::<AC, Parser>(*e2, ctx);
            mc::if_stm(b, e1, e2)
        }
        SExprFloat::Default(e1, e2) => {
            let e1 = to_async_stream_float::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_float::<AC, Parser>(*e2, ctx);
            mc::default(e1, e2)
        }
        SExprFloat::Defer(e, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::defer::<AC, Parser, f64>(ctx, e, 1, &type_ctx)
        }
        SExprFloat::Dynamic(e, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, f64>(ctx, e, None, 1, &type_ctx)
        }
        SExprFloat::RestrictedDynamic(e, vs, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, f64>(ctx, e, Some(vs), 1, &type_ctx)
        }
        SExprFloat::Sin(e) => {
            let e = to_async_stream_float::<AC, Parser>(*e, ctx);
            mc::sin(e)
        }
        SExprFloat::Cos(e) => {
            let e = to_async_stream_float::<AC, Parser>(*e, ctx);
            mc::cos(e)
        }
        SExprFloat::Tan(e) => {
            let e = to_async_stream_float::<AC, Parser>(*e, ctx);
            mc::tan(e)
        }
        SExprFloat::Abs(e) => {
            let e = to_async_stream_float::<AC, Parser>(*e, ctx);
            mc::abs_float(e)
        }
        SExprFloat::Init(e1, e2) => {
            let e1 = to_async_stream_float::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_float::<AC, Parser>(*e2, ctx);
            mc::init(e1, e2)
        }
    }
}

fn to_async_stream_str<AC, Parser>(
    expr: SExprStr,
    ctx: &AC::Ctx,
) -> OutputStream<PartialStreamValue<String>>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SExpr> + 'static,
{
    match expr {
        SExprStr::Val(v) => mc::val(v),
        SExprStr::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
        SExprStr::SIndex(e, i) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::sindex(e, i)
        }
        SExprStr::If(b, e1, e2) => {
            let b = to_async_stream_bool::<AC, Parser>(*b, ctx);
            let e1 = to_async_stream_str::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_str::<AC, Parser>(*e2, ctx);
            mc::if_stm(b, e1, e2)
        }
        SExprStr::Dynamic(e, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, String>(ctx, e, None, 1, &type_ctx)
        }
        SExprStr::RestrictedDynamic(e, vs, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, String>(ctx, e, Some(vs), 1, &type_ctx)
        }
        SExprStr::BinOp(e1, e2, op) => {
            let e1 = to_async_stream_str::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_str::<AC, Parser>(*e2, ctx);
            match op {
                StrBinOp::Concat => mc::concat(e1, e2),
            }
        }
        SExprStr::Default(e1, e2) => {
            let e1 = to_async_stream_str::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_str::<AC, Parser>(*e2, ctx);
            mc::default(e1, e2)
        }
        SExprStr::Defer(e, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::defer::<AC, Parser, String>(ctx, e, 1, &type_ctx)
        }
        SExprStr::Init(e1, e2) => {
            let e1 = to_async_stream_str::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_str::<AC, Parser>(*e2, ctx);
            mc::init(e1, e2)
        }
    }
}

fn to_async_stream_bool<AC, Parser>(
    expr: SExprBool,
    ctx: &AC::Ctx,
) -> OutputStream<PartialStreamValue<bool>>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SExpr> + 'static,
{
    match expr {
        SExprBool::Val(b) => mc::val(b),
        SExprBool::EqInt(e1, e2) => {
            let e1 = to_async_stream_int::<AC, Parser>(e1, ctx);
            let e2 = to_async_stream_int::<AC, Parser>(e2, ctx);
            mc::eq(e1, e2)
        }
        SExprBool::EqBool(e1, e2) => {
            let e1 = to_async_stream_bool::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_bool::<AC, Parser>(*e2, ctx);
            mc::eq(e1, e2)
        }
        SExprBool::EqStr(e1, e2) => {
            let e1 = to_async_stream_str::<AC, Parser>(e1, ctx);
            let e2 = to_async_stream_str::<AC, Parser>(e2, ctx);
            mc::eq(e1, e2)
        }
        SExprBool::EqFloat(e1, e2) => {
            let e1 = to_async_stream_float::<AC, Parser>(e1, ctx);
            let e2 = to_async_stream_float::<AC, Parser>(e2, ctx);
            mc::eq_partial(e1, e2)
        }
        SExprBool::EqUnit(e1, e2) => {
            let e1 = to_async_stream_unit::<AC, Parser>(e1, ctx);
            let e2 = to_async_stream_unit::<AC, Parser>(e2, ctx);
            mc::eq(e1, e2)
        }
        SExprBool::LeInt(e1, e2) => {
            let e1 = to_async_stream_int::<AC, Parser>(e1, ctx);
            let e2 = to_async_stream_int::<AC, Parser>(e2, ctx);
            mc::le(e1, e2)
        }
        SExprBool::LeFloat(e1, e2) => {
            let e1 = to_async_stream_float::<AC, Parser>(e1, ctx);
            let e2 = to_async_stream_float::<AC, Parser>(e2, ctx);
            mc::le_partial(e1, e2)
        }
        SExprBool::LeStr(e1, e2) => {
            let e1 = to_async_stream_str::<AC, Parser>(e1, ctx);
            let e2 = to_async_stream_str::<AC, Parser>(e2, ctx);
            mc::le_partial(e1, e2)
        }
        SExprBool::LtInt(e1, e2) => {
            let e1 = to_async_stream_int::<AC, Parser>(e1, ctx);
            let e2 = to_async_stream_int::<AC, Parser>(e2, ctx);
            mc::lt(e1, e2)
        }
        SExprBool::LtFloat(e1, e2) => {
            let e1 = to_async_stream_float::<AC, Parser>(e1, ctx);
            let e2 = to_async_stream_float::<AC, Parser>(e2, ctx);
            mc::lt(e1, e2)
        }
        SExprBool::LtStr(e1, e2) => {
            let e1 = to_async_stream_str::<AC, Parser>(e1, ctx);
            let e2 = to_async_stream_str::<AC, Parser>(e2, ctx);
            mc::lt(e1, e2)
        }
        SExprBool::GeInt(e1, e2) => {
            let e1 = to_async_stream_int::<AC, Parser>(e1, ctx);
            let e2 = to_async_stream_int::<AC, Parser>(e2, ctx);
            mc::ge(e1, e2)
        }
        SExprBool::GeFloat(e1, e2) => {
            let e1 = to_async_stream_float::<AC, Parser>(e1, ctx);
            let e2 = to_async_stream_float::<AC, Parser>(e2, ctx);
            mc::ge(e1, e2)
        }
        SExprBool::GeStr(e1, e2) => {
            let e1 = to_async_stream_str::<AC, Parser>(e1, ctx);
            let e2 = to_async_stream_str::<AC, Parser>(e2, ctx);
            mc::ge(e1, e2)
        }
        SExprBool::GtInt(e1, e2) => {
            let e1 = to_async_stream_int::<AC, Parser>(e1, ctx);
            let e2 = to_async_stream_int::<AC, Parser>(e2, ctx);
            mc::gt(e1, e2)
        }
        SExprBool::GtFloat(e1, e2) => {
            let e1 = to_async_stream_float::<AC, Parser>(e1, ctx);
            let e2 = to_async_stream_float::<AC, Parser>(e2, ctx);
            mc::gt(e1, e2)
        }
        SExprBool::GtStr(e1, e2) => {
            let e1 = to_async_stream_str::<AC, Parser>(e1, ctx);
            let e2 = to_async_stream_str::<AC, Parser>(e2, ctx);
            mc::gt(e1, e2)
        }
        SExprBool::BinOp(e1, e2, op) => {
            let e1 = to_async_stream_bool::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_bool::<AC, Parser>(*e2, ctx);
            match op {
                BoolBinOp::And => mc::and(e1, e2),
                BoolBinOp::Or => mc::or(e1, e2),
                BoolBinOp::Impl => mc::implication(e1, e2),
            }
        }
        SExprBool::Not(b) => {
            let b = to_async_stream_bool::<AC, Parser>(*b, ctx);
            mc::not(b)
        }
        SExprBool::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
        SExprBool::SIndex(e, i) => {
            let e = to_async_stream_bool::<AC, Parser>(*e, ctx);
            mc::sindex(e, i)
        }
        SExprBool::If(b, e1, e2) => {
            let b2 = to_async_stream_bool::<AC, Parser>(*b, ctx);
            let e1 = to_async_stream_bool::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_bool::<AC, Parser>(*e2, ctx);
            mc::if_stm(b2, e1, e2)
        }
        SExprBool::Default(e1, e2) => {
            let e1 = to_async_stream_bool::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_bool::<AC, Parser>(*e2, ctx);
            mc::default(e1, e2)
        }
        SExprBool::Defer(e, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::defer::<AC, Parser, bool>(ctx, e, 1, &type_ctx)
        }
        SExprBool::Dynamic(e, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, bool>(ctx, e, None, 1, &type_ctx)
        }
        SExprBool::RestrictedDynamic(e, vs, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, bool>(ctx, e, Some(vs), 1, &type_ctx)
        }
        SExprBool::IsDefinedInt(e) => {
            let e = to_async_stream_int::<AC, Parser>(e, ctx);
            mc::is_defined(e)
        }
        SExprBool::IsDefinedFloat(e) => {
            let e = to_async_stream_float::<AC, Parser>(e, ctx);
            mc::is_defined(e)
        }
        SExprBool::IsDefinedStr(e) => {
            let e = to_async_stream_str::<AC, Parser>(e, ctx);
            mc::is_defined(e)
        }
        SExprBool::IsDefinedBool(e) => {
            let e = to_async_stream_bool::<AC, Parser>(*e, ctx);
            mc::is_defined(e)
        }
        SExprBool::IsDefinedUnit(e) => {
            let e = to_async_stream_unit::<AC, Parser>(e, ctx);
            mc::is_defined(e)
        }
        SExprBool::WhenInt(e) => {
            let e = to_async_stream_int::<AC, Parser>(e, ctx);
            mc::when(e)
        }
        SExprBool::WhenFloat(e) => {
            let e = to_async_stream_float::<AC, Parser>(e, ctx);
            mc::when(e)
        }
        SExprBool::WhenStr(e) => {
            let e = to_async_stream_str::<AC, Parser>(e, ctx);
            mc::when(e)
        }
        SExprBool::WhenBool(e) => {
            let e = to_async_stream_bool::<AC, Parser>(*e, ctx);
            mc::when(e)
        }
        SExprBool::WhenUnit(e) => {
            let e = to_async_stream_unit::<AC, Parser>(e, ctx);
            mc::when(e)
        }
        SExprBool::Init(e1, e2) => {
            let e1 = to_async_stream_bool::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_bool::<AC, Parser>(*e2, ctx);
            mc::init(e1, e2)
        }
    }
}

fn to_async_stream_unit<AC, Parser>(
    expr: SExprUnit,
    ctx: &AC::Ctx,
) -> OutputStream<PartialStreamValue<()>>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SExpr> + 'static,
{
    match expr {
        SExprUnit::Val(v) => mc::val(v),
        SExprUnit::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
        SExprUnit::SIndex(e, i) => {
            let e = to_async_stream_unit::<AC, Parser>(*e, ctx);
            mc::sindex(e, i)
        }
        SExprUnit::If(b, e1, e2) => {
            let b = to_async_stream_bool::<AC, Parser>(*b, ctx);
            let e1 = to_async_stream_unit::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_unit::<AC, Parser>(*e2, ctx);
            mc::if_stm(b, e1, e2)
        }
        SExprUnit::Default(e1, e2) => {
            let e1 = to_async_stream_unit::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_unit::<AC, Parser>(*e2, ctx);
            mc::default(e1, e2)
        }
        SExprUnit::Defer(e, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::defer::<AC, Parser, ()>(ctx, e, 1, &type_ctx)
        }
        SExprUnit::Dynamic(e, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, ()>(ctx, e, None, 1, &type_ctx)
        }
        SExprUnit::RestrictedDynamic(e, vs, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, ()>(ctx, e, Some(vs), 1, &type_ctx)
        }
        SExprUnit::Init(e1, e2) => {
            let e1 = to_async_stream_unit::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_unit::<AC, Parser>(*e2, ctx);
            mc::init(e1, e2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::StreamType;
    use crate::lang::dynamic_lola::type_checker::TypeInfo;
    use crate::lola_fixtures::TestTypedConfig;
    use crate::runtime::asynchronous::Context;
    use crate::{async_test, lang::dynamic_lola::lalr_parser::LALRParser};
    use futures::stream::{self, StreamExt};
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::rc::Rc;

    type TestCtx = Context<TestTypedConfig>;

    fn type_ctx(vars: &[(&str, StreamType)]) -> TypeInfo {
        vars.iter().map(|(v, t)| ((*v).into(), t.clone())).collect()
    }

    #[apply(async_test)]
    async fn test_defer_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Val(PartialStreamValue::Known("x + 1".into())));
        let defer_expr = SExprInt::Defer(e_str, type_ctx(&[("x", StreamType::Int)]));

        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(2), PartialStreamValue::Known(3)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_int_x_squared_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Val(PartialStreamValue::Known("x * x".into())));
        let defer_expr = SExprInt::Defer(e_str, type_ctx(&[("x", StreamType::Int)]));

        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(4), PartialStreamValue::Known(9)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_bool_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Val(PartialStreamValue::Known("x && y".into())));
        let defer_expr = SExprBool::Defer(
            e_str,
            type_ctx(&[("x", StreamType::Bool), ("y", StreamType::Bool)]),
        );

        let x = Box::pin(stream::iter(vec![Value::Bool(true), Value::Bool(false)]));
        let y = Box::pin(stream::iter(vec![Value::Bool(true), Value::Bool(true)]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_with_deferred_value_runtime(executor: Rc<LocalExecutor<'static>>) {
        // Use a variable stream carrying Deferred instead of Val(Deferred),
        // because Val produces an infinite repeating stream via stream::repeat.
        let e_str = Box::new(SExprStr::Var("e".into()));
        let defer_expr = SExprInt::Defer(
            e_str,
            type_ctx(&[("e", StreamType::Str), ("x", StreamType::Int)]),
        );

        let e = Box::pin(stream::iter(vec![Value::Deferred]));
        let x = Box::pin(stream::iter(vec![2.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> = vec![PartialStreamValue::Deferred];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprInt::Dynamic(
            e_str,
            type_ctx(&[("e", StreamType::Str), ("x", StreamType::Int)]),
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x + 1".into()),
            Value::Str("x + 2".into()),
        ]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(2), PartialStreamValue::Known(4)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_int_x_squared_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprInt::Dynamic(
            e_str,
            type_ctx(&[("e", StreamType::Str), ("x", StreamType::Int)]),
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x * x".into()),
            Value::Str("x * x".into()),
        ]));
        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(4), PartialStreamValue::Known(9)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_with_start_deferred_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprInt::Dynamic(
            e_str,
            type_ctx(&[("e", StreamType::Str), ("x", StreamType::Int)]),
        );

        let e = Box::pin(stream::iter(vec![
            Value::Deferred,
            Value::Str("x + 1".into()),
        ]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Deferred, PartialStreamValue::Known(3)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_with_mid_deferred_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprInt::Dynamic(
            e_str,
            type_ctx(&[("e", StreamType::Str), ("x", StreamType::Int)]),
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x + 1".into()),
            Value::Deferred,
            Value::Str("x + 2".into()),
        ]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Known(2),
            PartialStreamValue::Deferred,
            PartialStreamValue::Known(5),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_bool_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprBool::Dynamic(
            e_str,
            type_ctx(&[
                ("e", StreamType::Str),
                ("x", StreamType::Bool),
                ("y", StreamType::Bool),
            ]),
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x && y".into()),
            Value::Str("x || y".into()),
        ]));
        let x = Box::pin(stream::iter(vec![Value::Bool(true), Value::Bool(false)]));
        let y = Box::pin(stream::iter(vec![Value::Bool(false), Value::Bool(true)]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into(), "y".into()],
            vec![e, x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Val(PartialStreamValue::Known("x + 1.5".into())));
        let defer_expr = SExprFloat::Defer(e_str, type_ctx(&[("x", StreamType::Float)]));

        let x = Box::pin(stream::iter(vec![Value::Float(1.0), Value::Float(2.0)]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_float::<TestTypedConfig, LALRParser>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<f64>> = vec![
            PartialStreamValue::Known(2.5),
            PartialStreamValue::Known(3.5),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Val(PartialStreamValue::Known("x ++ y".into())));
        let defer_expr = SExprStr::Defer(
            e_str,
            type_ctx(&[("x", StreamType::Str), ("y", StreamType::Str)]),
        );

        let x = Box::pin(stream::iter(vec![
            Value::Str("hello".into()),
            Value::Str("hi".into()),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Str(" world".into()),
            Value::Str(" there".into()),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_str::<TestTypedConfig, LALRParser>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<String>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<String>> = vec![
            PartialStreamValue::Known("hello world".into()),
            PartialStreamValue::Known("hi there".into()),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprStr::Dynamic(
            e_str,
            type_ctx(&[
                ("e", StreamType::Str),
                ("x", StreamType::Str),
                ("y", StreamType::Str),
            ]),
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x ++ y".into()),
            Value::Str("y ++ x".into()),
        ]));
        let x = Box::pin(stream::iter(vec![
            Value::Str("hello ".into()),
            Value::Str("hi ".into()),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Str("world".into()),
            Value::Str("there".into()),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into(), "y".into()],
            vec![e, x, y],
            10,
        );

        let res_stream = to_async_stream_str::<TestTypedConfig, LALRParser>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<String>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<String>> = vec![
            PartialStreamValue::Known("hello world".into()),
            PartialStreamValue::Known("therehi ".into()),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprFloat::Dynamic(
            e_str,
            type_ctx(&[("e", StreamType::Str), ("x", StreamType::Float)]),
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x + 1.5".into()),
            Value::Str("x * 2.0".into()),
        ]));
        let x = Box::pin(stream::iter(vec![Value::Float(1.0), Value::Float(2.0)]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_async_stream_float::<TestTypedConfig, LALRParser>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<f64>> = vec![
            PartialStreamValue::Known(2.5),
            PartialStreamValue::Known(4.0),
        ];
        assert_eq!(res, exp);
    }

    // ========== Tests for new comparison operators ==========

    #[apply(async_test)]
    async fn test_lt_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::LtInt(SExprInt::Var("x".into()), SExprInt::Var("y".into()));

        let x = Box::pin(stream::iter(vec![1.into(), 5.into(), 3.into()]));
        let y = Box::pin(stream::iter(vec![2.into(), 5.into(), 1.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_ge_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::GeInt(SExprInt::Var("x".into()), SExprInt::Var("y".into()));

        let x = Box::pin(stream::iter(vec![1.into(), 5.into(), 3.into()]));
        let y = Box::pin(stream::iter(vec![2.into(), 5.into(), 1.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_gt_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::GtInt(SExprInt::Var("x".into()), SExprInt::Var("y".into()));

        let x = Box::pin(stream::iter(vec![1.into(), 5.into(), 3.into()]));
        let y = Box::pin(stream::iter(vec![2.into(), 5.into(), 1.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_eq_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::EqFloat(SExprFloat::Var("x".into()), SExprFloat::Var("y".into()));

        let x = Box::pin(stream::iter(vec![Value::Float(1.0), Value::Float(3.0)]));
        let y = Box::pin(stream::iter(vec![Value::Float(1.0), Value::Float(2.0)]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_le_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::LeFloat(SExprFloat::Var("x".into()), SExprFloat::Var("y".into()));

        let x = Box::pin(stream::iter(vec![
            Value::Float(1.0),
            Value::Float(3.0),
            Value::Float(2.0),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Float(2.0),
            Value::Float(3.0),
            Value::Float(1.0),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_lt_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::LtFloat(SExprFloat::Var("x".into()), SExprFloat::Var("y".into()));

        let x = Box::pin(stream::iter(vec![
            Value::Float(1.0),
            Value::Float(3.0),
            Value::Float(2.0),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Float(2.0),
            Value::Float(3.0),
            Value::Float(1.0),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_ge_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::GeFloat(SExprFloat::Var("x".into()), SExprFloat::Var("y".into()));

        let x = Box::pin(stream::iter(vec![
            Value::Float(1.0),
            Value::Float(3.0),
            Value::Float(2.0),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Float(2.0),
            Value::Float(3.0),
            Value::Float(1.0),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_gt_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::GtFloat(SExprFloat::Var("x".into()), SExprFloat::Var("y".into()));

        let x = Box::pin(stream::iter(vec![
            Value::Float(1.0),
            Value::Float(3.0),
            Value::Float(2.0),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Float(2.0),
            Value::Float(3.0),
            Value::Float(1.0),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_le_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::LeStr(SExprStr::Var("x".into()), SExprStr::Var("y".into()));

        let x = Box::pin(stream::iter(vec![
            Value::Str("apple".into()),
            Value::Str("banana".into()),
            Value::Str("cherry".into()),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Str("banana".into()),
            Value::Str("banana".into()),
            Value::Str("banana".into()),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_lt_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::LtStr(SExprStr::Var("x".into()), SExprStr::Var("y".into()));

        let x = Box::pin(stream::iter(vec![
            Value::Str("apple".into()),
            Value::Str("banana".into()),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Str("banana".into()),
            Value::Str("banana".into()),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_ge_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::GeStr(SExprStr::Var("x".into()), SExprStr::Var("y".into()));

        let x = Box::pin(stream::iter(vec![
            Value::Str("apple".into()),
            Value::Str("cherry".into()),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Str("banana".into()),
            Value::Str("banana".into()),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_gt_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::GtStr(SExprStr::Var("x".into()), SExprStr::Var("y".into()));

        let x = Box::pin(stream::iter(vec![
            Value::Str("cherry".into()),
            Value::Str("apple".into()),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Str("banana".into()),
            Value::Str("banana".into()),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    // ========== Tests for trigonometric functions ==========

    #[apply(async_test)]
    async fn test_sin_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprFloat::Sin(Box::new(SExprFloat::Var("x".into())));

        let x = Box::pin(stream::iter(vec![
            Value::Float(0.0),
            Value::Float(std::f64::consts::FRAC_PI_2),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_float::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        assert_eq!(res.len(), 2);
        match &res[0] {
            PartialStreamValue::Known(v) => assert!((v - 0.0).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
        match &res[1] {
            PartialStreamValue::Known(v) => assert!((v - 1.0).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
    }

    #[apply(async_test)]
    async fn test_cos_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprFloat::Cos(Box::new(SExprFloat::Var("x".into())));

        let x = Box::pin(stream::iter(vec![
            Value::Float(0.0),
            Value::Float(std::f64::consts::PI),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_float::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        assert_eq!(res.len(), 2);
        match &res[0] {
            PartialStreamValue::Known(v) => assert!((v - 1.0).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
        match &res[1] {
            PartialStreamValue::Known(v) => assert!((v - (-1.0)).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
    }

    #[apply(async_test)]
    async fn test_tan_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprFloat::Tan(Box::new(SExprFloat::Var("x".into())));

        let x = Box::pin(stream::iter(vec![
            Value::Float(0.0),
            Value::Float(std::f64::consts::FRAC_PI_4),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_float::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        assert_eq!(res.len(), 2);
        match &res[0] {
            PartialStreamValue::Known(v) => assert!((v - 0.0).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
        match &res[1] {
            PartialStreamValue::Known(v) => assert!((v - 1.0).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
    }

    // ========== Tests for abs ==========

    #[apply(async_test)]
    async fn test_abs_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprInt::Abs(Box::new(SExprInt::Var("x".into())));

        let x = Box::pin(stream::iter(vec![
            Value::Int(-5),
            Value::Int(3),
            Value::Int(0),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Known(5),
            PartialStreamValue::Known(3),
            PartialStreamValue::Known(0),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_abs_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprFloat::Abs(Box::new(SExprFloat::Var("x".into())));

        let x = Box::pin(stream::iter(vec![
            Value::Float(-5.5),
            Value::Float(3.2),
            Value::Float(0.0),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_float::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        assert_eq!(res.len(), 3);
        match &res[0] {
            PartialStreamValue::Known(v) => assert!((v - 5.5).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
        match &res[1] {
            PartialStreamValue::Known(v) => assert!((v - 3.2).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
        match &res[2] {
            PartialStreamValue::Known(v) => assert!((v - 0.0).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
    }

    // ========== Tests for is_defined ==========

    #[apply(async_test)]
    async fn test_is_defined_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::IsDefinedInt(SExprInt::Var("x".into()));

        let x = Box::pin(stream::iter(vec![
            Value::Int(1),
            Value::Deferred,
            Value::Int(3),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_is_defined_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::IsDefinedFloat(SExprFloat::Var("x".into()));

        let x = Box::pin(stream::iter(vec![Value::Float(1.0), Value::Deferred]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_is_defined_bool_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::IsDefinedBool(Box::new(SExprBool::Var("x".into())));

        let x = Box::pin(stream::iter(vec![
            Value::Bool(true),
            Value::Deferred,
            Value::Bool(false),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_is_defined_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::IsDefinedStr(SExprStr::Var("x".into()));

        let x = Box::pin(stream::iter(vec![
            Value::Deferred,
            Value::Str("hello".into()),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    // ========== Tests for when ==========

    #[apply(async_test)]
    async fn test_when_int_never_defined_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::WhenInt(SExprInt::Var("x".into()));

        let x = Box::pin(stream::iter(vec![
            Value::Deferred,
            Value::Deferred,
            Value::Deferred,
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_when_int_eventually_defined_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::WhenInt(SExprInt::Var("x".into()));

        let x = Box::pin(stream::iter(vec![
            Value::Deferred,
            Value::Int(10),
            Value::Int(20),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_when_bool_immediately_defined_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::WhenBool(Box::new(SExprBool::Var("x".into())));

        let x = Box::pin(stream::iter(vec![Value::Bool(true), Value::Bool(false)]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    // ========== Tests for init ==========

    #[apply(async_test)]
    async fn test_init_int_no_noval_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprInt::Init(
            Box::new(SExprInt::Var("x".into())),
            Box::new(SExprInt::Var("d".into())),
        );

        let x = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let d = Box::pin(stream::iter(vec![10.into(), 20.into(), 30.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "d".into()],
            vec![x, d],
            10,
        );

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Known(1),
            PartialStreamValue::Known(2),
            PartialStreamValue::Known(3),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_init_int_starts_with_noval_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprInt::Init(
            Box::new(SExprInt::Var("x".into())),
            Box::new(SExprInt::Var("d".into())),
        );

        let x = Box::pin(stream::iter(vec![
            Value::NoVal,
            Value::NoVal,
            Value::Int(3),
            Value::Int(4),
        ]));
        let d = Box::pin(stream::iter(vec![
            Value::Int(10),
            Value::Int(20),
            Value::Int(30),
            Value::Int(40),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "d".into()],
            vec![x, d],
            10,
        );

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Known(10),
            PartialStreamValue::Known(20),
            PartialStreamValue::Known(3),
            PartialStreamValue::Known(4),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_init_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprFloat::Init(
            Box::new(SExprFloat::Var("x".into())),
            Box::new(SExprFloat::Var("d".into())),
        );

        let x = Box::pin(stream::iter(vec![
            Value::NoVal,
            Value::Float(2.5),
            Value::Float(3.5),
        ]));
        let d = Box::pin(stream::iter(vec![
            Value::Float(10.0),
            Value::Float(20.0),
            Value::Float(30.0),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "d".into()],
            vec![x, d],
            10,
        );

        let res_stream = to_async_stream_float::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<f64>> = vec![
            PartialStreamValue::Known(10.0),
            PartialStreamValue::Known(2.5),
            PartialStreamValue::Known(3.5),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_init_bool_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Init(
            Box::new(SExprBool::Var("x".into())),
            Box::new(SExprBool::Var("d".into())),
        );

        let x = Box::pin(stream::iter(vec![
            Value::NoVal,
            Value::Bool(true),
            Value::Bool(false),
        ]));
        let d = Box::pin(stream::iter(vec![
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "d".into()],
            vec![x, d],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_init_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprStr::Init(
            Box::new(SExprStr::Var("x".into())),
            Box::new(SExprStr::Var("d".into())),
        );

        let x = Box::pin(stream::iter(vec![Value::NoVal, Value::Str("hello".into())]));
        let d = Box::pin(stream::iter(vec![
            Value::Str("default".into()),
            Value::Str("default".into()),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "d".into()],
            vec![x, d],
            10,
        );

        let res_stream = to_async_stream_str::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<String>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<String>> = vec![
            PartialStreamValue::Known("default".into()),
            PartialStreamValue::Known("hello".into()),
        ];
        assert_eq!(res, exp);
    }
}
