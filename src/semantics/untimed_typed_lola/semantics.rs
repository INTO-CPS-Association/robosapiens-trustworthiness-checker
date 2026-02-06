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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::StreamType;
    use crate::lang::dynamic_lola::type_checker::TypeContext;
    use crate::lola_fixtures::TestTypedConfig;
    use crate::runtime::asynchronous::Context;
    use crate::{async_test, lang::dynamic_lola::lalr_parser::LALRExprParser};
    use futures::stream::{self, StreamExt};
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::rc::Rc;

    type TestCtx = Context<TestTypedConfig>;

    fn type_ctx(vars: &[(&str, StreamType)]) -> TypeContext {
        vars.iter().map(|(v, t)| ((*v).into(), t.clone())).collect()
    }

    #[apply(async_test)]
    async fn test_defer_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Val(PartialStreamValue::Known("x + 1".into())));
        let defer_expr = SExprInt::Defer(e_str, type_ctx(&[("x", StreamType::Int)]));

        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRExprParser>(defer_expr, &ctx);
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

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRExprParser>(defer_expr, &ctx);
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

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRExprParser>(defer_expr, &ctx);
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

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRExprParser>(defer_expr, &ctx);
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

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRExprParser>(dynamic_expr, &ctx);
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

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRExprParser>(dynamic_expr, &ctx);
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

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRExprParser>(dynamic_expr, &ctx);
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

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRExprParser>(dynamic_expr, &ctx);
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

        let res_stream =
            to_async_stream_bool::<TestTypedConfig, LALRExprParser>(dynamic_expr, &ctx);
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

        let res_stream = to_async_stream_float::<TestTypedConfig, LALRExprParser>(defer_expr, &ctx);
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

        let res_stream = to_async_stream_str::<TestTypedConfig, LALRExprParser>(defer_expr, &ctx);
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

        let res_stream = to_async_stream_str::<TestTypedConfig, LALRExprParser>(dynamic_expr, &ctx);
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

        let res_stream =
            to_async_stream_float::<TestTypedConfig, LALRExprParser>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<f64>> = vec![
            PartialStreamValue::Known(2.5),
            PartialStreamValue::Known(4.0),
        ];
        assert_eq!(res, exp);
    }
}
