use super::combinators as mc;
use super::helpers::{from_typed_stream, to_typed_stream};
use crate::core::OutputStream;
use crate::core::Value;
use crate::lang::dynamic_lola::ast::{BoolBinOp, FloatBinOp, IntBinOp, StrBinOp};
use crate::lang::dynamic_lola::type_checker::{
    PartialStreamValue, SExprBool, SExprFloat, SExprInt, SExprStr, SExprTE, SExprUnit,
};
use crate::semantics::{AsyncConfig, MonitoringSemantics, StreamContext};

#[derive(Clone)]
pub struct TypedUntimedLolaSemantics;

impl<AC> MonitoringSemantics<AC> for TypedUntimedLolaSemantics
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
{
    fn to_async_stream(expr: AC::Expr, ctx: &AC::Ctx) -> OutputStream<Value> {
        match expr {
            SExprTE::Int(e) => {
                from_typed_stream::<PartialStreamValue<i64>>(to_async_stream_int::<AC>(e, ctx))
            }
            SExprTE::Float(e) => {
                from_typed_stream::<PartialStreamValue<f64>>(to_async_stream_float::<AC>(e, ctx))
            }
            SExprTE::Str(e) => {
                from_typed_stream::<PartialStreamValue<String>>(to_async_stream_str::<AC>(e, ctx))
            }
            SExprTE::Bool(e) => {
                from_typed_stream::<PartialStreamValue<bool>>(to_async_stream_bool::<AC>(e, ctx))
            }
            SExprTE::Unit(e) => {
                from_typed_stream::<PartialStreamValue<()>>(to_async_stream_unit::<AC>(e, ctx))
            }
        }
    }
}

fn to_async_stream_int<AC>(expr: SExprInt, ctx: &AC::Ctx) -> OutputStream<PartialStreamValue<i64>>
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
        SExprInt::Defer(_e) => {
            // Defer contains a string expression that needs to be parsed at runtime
            // This is handled by the untyped semantics
            todo!("Defer with string parsing not implemented in typed semantics")
        }
        SExprInt::Dynamic(_e) => {
            // Dynamic contains a string expression that needs to be parsed at runtime
            // This is handled by the untyped semantics
            todo!("Dynamic with string parsing not implemented in typed semantics")
        }
        SExprInt::RestrictedDynamic(_e, _vs) => {
            // Dynamic contains a string expression that needs to be parsed at runtime
            // This is handled by the untyped semantics
            todo!("RestrictedDynamic with string parsing not implemented in typed semantics")
        }
    }
}

fn to_async_stream_float<AC>(expr: SExprFloat, ctx: &AC::Ctx) -> OutputStream<PartialStreamValue<f64>>
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
        SExprFloat::Defer(_e) => {
            // Defer contains a string expression that needs to be parsed at runtime
            // This is handled by the untyped semantics
            todo!("Defer with string parsing not implemented in typed semantics")
        }
        SExprFloat::Dynamic(_e) => {
            // Dynamic contains a string expression that needs to be parsed at runtime
            // This is handled by the untyped semantics
            todo!("Dynamic with string parsing not implemented in typed semantics")
        }
        SExprFloat::RestrictedDynamic(_e, _vs) => {
            // RestrictedDynamic contains a string expression that needs to be parsed at runtime
            // This is handled by the untyped semantics
            todo!("RestrictedDynamic with string parsing not implemented in typed semantics")
        }
    }
}

fn to_async_stream_str<AC>(expr: SExprStr, ctx: &AC::Ctx) -> OutputStream<PartialStreamValue<String>>
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
        SExprStr::Defer(_e) => {
            // Defer contains a string expression that needs to be parsed at runtime
            // This is handled by the untyped semantics
            todo!("Defer with string parsing not implemented in typed semantics")
        }
    }
}

fn to_async_stream_bool<AC>(expr: SExprBool, ctx: &AC::Ctx) -> OutputStream<PartialStreamValue<bool>>
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
        SExprBool::EqBool(e1, e2) => {
            let e1 = to_async_stream_bool::<AC>(*e1, ctx);
            let e2 = to_async_stream_bool::<AC>(*e2, ctx);
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
        SExprBool::LeInt(e1, e2) => {
            let e1 = to_async_stream_int::<AC>(e1, ctx);
            let e2 = to_async_stream_int::<AC>(e2, ctx);
            mc::le(e1, e2)
        }
        SExprBool::BinOp(e1, e2, op) => {
            let e1 = to_async_stream_bool::<AC>(*e1, ctx);
            let e2 = to_async_stream_bool::<AC>(*e2, ctx);
            match op {
                BoolBinOp::And => mc::and(e1, e2),
                BoolBinOp::Or => mc::or(e1, e2),
                BoolBinOp::Impl => mc::implication(e1, e2),
            }
        }
        SExprBool::Not(b) => {
            let b = to_async_stream_bool::<AC>(*b, ctx);
            mc::not(b)
        }
        SExprBool::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
        SExprBool::SIndex(e, i) => {
            let e = to_async_stream_bool::<AC>(*e, ctx);
            mc::sindex(e, i)
        }
        SExprBool::If(b, e1, e2) => {
            let b2 = to_async_stream_bool::<AC>(*b, ctx);
            let e1 = to_async_stream_bool::<AC>(*e1, ctx);
            let e2 = to_async_stream_bool::<AC>(*e2, ctx);
            mc::if_stm(b2, e1, e2)
        }
        SExprBool::Default(e1, e2) => {
            let e1 = to_async_stream_bool::<AC>(*e1, ctx);
            let e2 = to_async_stream_bool::<AC>(*e2, ctx);
            mc::default(e1, e2)
        }
        SExprBool::Defer(_e) => {
            // Defer contains a string expression that needs to be parsed at runtime
            // This is handled by the untyped semantics
            todo!("Defer with string parsing not implemented in typed semantics")
        }
        SExprBool::Dynamic(_e) => {
            // Dynamic contains a string expression that needs to be parsed at runtime
            // This is handled by the untyped semantics
            todo!("Dynamic with string parsing not implemented in typed semantics")
        }
        SExprBool::RestrictedDynamic(_e, _vs) => {
            // RestrictedDynamic contains a string expression that needs to be parsed at runtime
            // This is handled by the untyped semantics
            todo!("RestrictedDynamic with string parsing not implemented in typed semantics")
        }
    }
}

fn to_async_stream_unit<AC>(expr: SExprUnit, ctx: &AC::Ctx) -> OutputStream<PartialStreamValue<()>>
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
        SExprUnit::Defer(_e) => {
            // Defer contains a string expression that needs to be parsed at runtime
            // This is handled by the untyped semantics
            todo!("Defer with string parsing not implemented in typed semantics")
        }
        SExprUnit::Dynamic(_e) => {
            // Dynamic contains a string expression that needs to be parsed at runtime
            // This is handled by the untyped semantics
            todo!("Dynamic with string parsing not implemented in typed semantics")
        }
        SExprUnit::RestrictedDynamic(_e, _vs) => {
            // RestrictedDynamic contains a string expression that needs to be parsed at runtime
            // This is handled by the untyped semantics
            todo!("RestrictedDynamic with string parsing not implemented in typed semantics")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::async_test;
    use crate::lola_fixtures::TestConfig;
    use crate::runtime::asynchronous::Context;
    use futures::stream::{self, StreamExt};
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::rc::Rc;

    type TestCtx = Context<TestConfig>;

    // ============================================================================
    // RUNTIME TESTS FOR DEFER AND DYNAMIC IN TYPED SEMANTICS
    // ============================================================================
    //
    // These tests execute defer and dynamic operations on typed expressions.
    // They currently FAIL with todo!() panics because the typed semantics
    // for defer/dynamic are not yet implemented.
    //
    // Once implemented, remove #[should_panic] attributes and verify results
    // match expected values.
    //
    // Reference: See untimed_untyped_lola/combinators.rs for working examples.
    // ============================================================================

    #[apply(async_test)]
    #[should_panic(expected = "Defer with string parsing not implemented in typed semantics")]
    async fn test_defer_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        // Test defer with integer type - should evaluate "x + 1"
        let e_str = Box::new(SExprStr::Val(PartialStreamValue::Known("x + 1".into())));
        let defer_expr = SExprInt::Defer(e_str);

        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_int::<TestConfig>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        // Once implemented, should produce: [2, 3]
        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(2), PartialStreamValue::Known(3)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    #[should_panic(expected = "Defer with string parsing not implemented in typed semantics")]
    async fn test_defer_int_x_squared_runtime(executor: Rc<LocalExecutor<'static>>) {
        // Test defer with expression using x twice
        let e_str = Box::new(SExprStr::Val(PartialStreamValue::Known("x * x".into())));
        let defer_expr = SExprInt::Defer(e_str);

        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_int::<TestConfig>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        // Once implemented, should produce: [4, 9]
        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(4), PartialStreamValue::Known(9)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    #[should_panic(expected = "Defer with string parsing not implemented in typed semantics")]
    async fn test_defer_bool_runtime(executor: Rc<LocalExecutor<'static>>) {
        // Test defer with boolean type
        let e_str = Box::new(SExprStr::Val(PartialStreamValue::Known("x && y".into())));
        let defer_expr = SExprBool::Defer(e_str);

        let x = Box::pin(stream::iter(vec![Value::Bool(true), Value::Bool(false)]));
        let y = Box::pin(stream::iter(vec![Value::Bool(true), Value::Bool(true)]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestConfig>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        // Once implemented, should produce: [true, false]
        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    #[should_panic(expected = "Defer with string parsing not implemented in typed semantics")]
    async fn test_defer_with_deferred_value_runtime(executor: Rc<LocalExecutor<'static>>) {
        // Test defer when the string expression itself is deferred
        let e_str = Box::new(SExprStr::Val(PartialStreamValue::Deferred));
        let defer_expr = SExprInt::Defer(e_str);

        let x = Box::pin(stream::iter(vec![2.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_int::<TestConfig>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        // Once implemented, should return Deferred when expression is deferred
        let exp: Vec<PartialStreamValue<i64>> = vec![PartialStreamValue::Deferred];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    #[should_panic(expected = "Dynamic with string parsing not implemented in typed semantics")]
    async fn test_dynamic_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        // Test dynamic with integer type - expression changes over time
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprInt::Dynamic(e_str);

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

        let res_stream = to_async_stream_int::<TestConfig>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        // Once implemented, should produce: [2, 4]
        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(2), PartialStreamValue::Known(4)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    #[should_panic(expected = "Dynamic with string parsing not implemented in typed semantics")]
    async fn test_dynamic_int_x_squared_runtime(executor: Rc<LocalExecutor<'static>>) {
        // Test dynamic with expression using x twice
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprInt::Dynamic(e_str);

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

        let res_stream = to_async_stream_int::<TestConfig>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        // Once implemented, should produce: [4, 9]
        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(4), PartialStreamValue::Known(9)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    #[should_panic(expected = "Dynamic with string parsing not implemented in typed semantics")]
    async fn test_dynamic_with_start_deferred_runtime(executor: Rc<LocalExecutor<'static>>) {
        // Test dynamic when expression starts with Deferred
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprInt::Dynamic(e_str);

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

        let res_stream = to_async_stream_int::<TestConfig>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        // Once implemented, should produce: [Deferred, 3]
        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Deferred, PartialStreamValue::Known(3)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    #[should_panic(expected = "Dynamic with string parsing not implemented in typed semantics")]
    async fn test_dynamic_with_mid_deferred_runtime(executor: Rc<LocalExecutor<'static>>) {
        // Test dynamic when expression has Deferred in the middle
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprInt::Dynamic(e_str);

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

        let res_stream = to_async_stream_int::<TestConfig>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        // Once implemented, should produce: [2, Deferred, 5]
        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Known(2),
            PartialStreamValue::Deferred,
            PartialStreamValue::Known(5),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    #[should_panic(expected = "Dynamic with string parsing not implemented in typed semantics")]
    async fn test_dynamic_bool_runtime(executor: Rc<LocalExecutor<'static>>) {
        // Test dynamic with boolean type
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprBool::Dynamic(e_str);

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

        let res_stream = to_async_stream_bool::<TestConfig>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        // Once implemented, should produce: [false, true]
        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    #[should_panic(expected = "Defer with string parsing not implemented in typed semantics")]
    async fn test_defer_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        // Test defer with float type
        let e_str = Box::new(SExprStr::Val(PartialStreamValue::Known("x + 1.5".into())));
        let defer_expr = SExprFloat::Defer(e_str);

        let x = Box::pin(stream::iter(vec![Value::Float(1.0), Value::Float(2.0)]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_float::<TestConfig>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        // Once implemented, should produce: [2.5, 3.5]
        let exp: Vec<PartialStreamValue<f64>> =
            vec![PartialStreamValue::Known(2.5), PartialStreamValue::Known(3.5)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    #[should_panic(expected = "Defer with string parsing not implemented in typed semantics")]
    async fn test_defer_unit_runtime(executor: Rc<LocalExecutor<'static>>) {
        // Test defer with unit type
        let e_str = Box::new(SExprStr::Val(PartialStreamValue::Known("()".into())));
        let defer_expr = SExprUnit::Defer(e_str);

        let x = Box::pin(stream::iter(vec![Value::Unit, Value::Unit]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_unit::<TestConfig>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<()>> = res_stream.collect().await;

        // Once implemented, should produce: [(), ()]
        let exp: Vec<PartialStreamValue<()>> =
            vec![PartialStreamValue::Known(()), PartialStreamValue::Known(())];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    #[should_panic(expected = "Defer with string parsing not implemented in typed semantics")]
    async fn test_defer_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        // Test defer with string type
        let e_str = Box::new(SExprStr::Val(PartialStreamValue::Known("x ++ y".into())));
        let defer_expr = SExprStr::Defer(e_str);

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

        let res_stream = to_async_stream_str::<TestConfig>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<String>> = res_stream.collect().await;

        // Once implemented, should produce: ["hello world", "hi there"]
        let exp: Vec<PartialStreamValue<String>> = vec![
            PartialStreamValue::Known("hello world".into()),
            PartialStreamValue::Known("hi there".into()),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    #[should_panic(expected = "not yet implemented")]
    async fn test_dynamic_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        // Test dynamic with string type
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprStr::Dynamic(e_str);

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

        let res_stream = to_async_stream_str::<TestConfig>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<String>> = res_stream.collect().await;

        // Once implemented, should produce: ["hello world", "there hi "]
        let exp: Vec<PartialStreamValue<String>> = vec![
            PartialStreamValue::Known("hello world".into()),
            PartialStreamValue::Known("there hi ".into()),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    #[should_panic(expected = "Dynamic with string parsing not implemented in typed semantics")]
    async fn test_dynamic_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        // Test dynamic with float type
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprFloat::Dynamic(e_str);

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

        let res_stream = to_async_stream_float::<TestConfig>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        // Once implemented, should produce: [2.5, 4.0]
        let exp: Vec<PartialStreamValue<f64>> =
            vec![PartialStreamValue::Known(2.5), PartialStreamValue::Known(4.0)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    #[should_panic(expected = "Dynamic with string parsing not implemented in typed semantics")]
    async fn test_dynamic_unit_runtime(executor: Rc<LocalExecutor<'static>>) {
        // Test dynamic with unit type
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprUnit::Dynamic(e_str);

        let e = Box::pin(stream::iter(vec![
            Value::Str("()".into()),
            Value::Str("()".into()),
        ]));
        let x = Box::pin(stream::iter(vec![Value::Unit, Value::Unit]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_async_stream_unit::<TestConfig>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<()>> = res_stream.collect().await;

        // Once implemented, should produce: [(), ()]
        let exp: Vec<PartialStreamValue<()>> =
            vec![PartialStreamValue::Known(()), PartialStreamValue::Known(())];
        assert_eq!(res, exp);
    }
}
