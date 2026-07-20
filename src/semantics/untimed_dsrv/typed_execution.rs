//! Type-specialised execution over the shared checked AST.
//!
//! Scalar nodes use typed streams. Less common and dynamically typed forms fall
//! back to the shared `Value` evaluator, keeping this layer small and complete.

use crate::core::{
    OutputStream, PartialStreamValue, Value, from_typed_partial_stream, to_typed_partial_stream,
    to_typed_stream,
};
use crate::lang::core::parser::ExprParser;
use crate::lang::dsrv::ast::{
    BoolBinOp, CheckedExpr, CompBinOp, Expr, ExprView, NumericalBinOp, SBinOp,
};
use crate::lang::dsrv::type_checker::TCType;
use crate::semantics::{AsyncConfig, StreamContext};

use super::functions::ScopedExpr;
use super::semantics::evaluate_scope;
use super::typed_combinators as typed;

pub(super) fn evaluate_checked<Parser, AC>(
    expression: CheckedExpr,
    context: &AC::Ctx,
) -> OutputStream<Value>
where
    Parser: ExprParser<Expr> + 'static,
    AC: AsyncConfig<Val = Value>,
{
    evaluate_scoped::<Parser, AC>(ScopedExpr::checked(expression), context)
}

pub(super) fn evaluate_scoped<Parser, AC>(
    expression: ScopedExpr,
    context: &AC::Ctx,
) -> OutputStream<Value>
where
    Parser: ExprParser<Expr> + 'static,
    AC: AsyncConfig<Val = Value>,
{
    match expression.typ(expression.as_ref()) {
        Some(TCType::Int) => {
            from_typed_partial_stream(evaluate_int::<Parser, AC>(expression, context))
        }
        Some(TCType::Bool) => {
            from_typed_partial_stream(evaluate_bool::<Parser, AC>(expression, context))
        }
        Some(TCType::Float) => {
            from_typed_partial_stream(evaluate_float::<Parser, AC>(expression, context))
        }
        _ => evaluate_scope::<Parser, AC>(expression, context),
    }
}

fn evaluate_float<Parser, AC>(
    expression: ScopedExpr,
    context: &AC::Ctx,
) -> OutputStream<PartialStreamValue<f64>>
where
    Parser: ExprParser<Expr> + 'static,
    AC: AsyncConfig<Val = Value>,
{
    let node = expression.as_ref();
    match node.view() {
        ExprView::Val(Value::Float(value)) => typed::val(*value),
        ExprView::Var(name) => {
            if let Some(stream) = expression.resolve_stream(name) {
                to_typed_stream(stream)
            } else {
                match expression.resolve(name) {
                    Some(bound) => evaluate_float::<Parser, AC>(bound, context),
                    None => {
                        to_typed_stream(context.var(name).expect("checked variable must exist"))
                    }
                }
            }
        }
        ExprView::BinOp(left, right, SBinOp::NOp(operator)) => {
            let left = evaluate_float::<Parser, AC>(expression.scope(left), context);
            let right = evaluate_float::<Parser, AC>(expression.scope(right), context);
            match operator {
                NumericalBinOp::Add => typed::add(left, right),
                NumericalBinOp::Sub => typed::sub(left, right),
                NumericalBinOp::Mul => typed::mul(left, right),
                NumericalBinOp::Div => typed::div(left, right),
                NumericalBinOp::Mod => typed::rem(left, right),
            }
        }
        ExprView::Default(input, default) => typed::default(
            evaluate_float::<Parser, AC>(expression.scope(input), context),
            evaluate_float::<Parser, AC>(expression.scope(default), context),
        ),
        ExprView::SIndex(input, index) => typed::sindex(
            evaluate_float::<Parser, AC>(expression.scope(input), context),
            index,
        ),
        ExprView::If(condition, then_expr, else_expr) => typed::if_stream(
            evaluate_bool::<Parser, AC>(expression.scope(condition), context),
            evaluate_float::<Parser, AC>(expression.scope(then_expr), context),
            evaluate_float::<Parser, AC>(expression.scope(else_expr), context),
        ),
        ExprView::Sin(value) => typed::sin(evaluate_float::<Parser, AC>(
            expression.scope(value),
            context,
        )),
        ExprView::Cos(value) => typed::cos(evaluate_float::<Parser, AC>(
            expression.scope(value),
            context,
        )),
        ExprView::Tan(value) => typed::tan(evaluate_float::<Parser, AC>(
            expression.scope(value),
            context,
        )),
        ExprView::Abs(value) => typed::abs(evaluate_float::<Parser, AC>(
            expression.scope(value),
            context,
        )),
        _ => fallback::<f64, Parser, AC>(expression, context),
    }
}

fn fallback<T, Parser, AC>(
    expression: ScopedExpr,
    context: &AC::Ctx,
) -> OutputStream<PartialStreamValue<T>>
where
    T: TryFrom<Value> + std::fmt::Debug + 'static,
    <T as TryFrom<Value>>::Error: std::fmt::Debug,
    Parser: ExprParser<Expr> + 'static,
    AC: AsyncConfig<Val = Value>,
{
    to_typed_partial_stream(evaluate_scope::<Parser, AC>(expression, context))
}

fn evaluate_int<Parser, AC>(
    expression: ScopedExpr,
    context: &AC::Ctx,
) -> OutputStream<PartialStreamValue<i64>>
where
    Parser: ExprParser<Expr> + 'static,
    AC: AsyncConfig<Val = Value>,
{
    let node = expression.as_ref();
    match node.view() {
        ExprView::Val(Value::Int(value)) => typed::val(*value),
        ExprView::Var(name) => {
            if let Some(stream) = expression.resolve_stream(name) {
                to_typed_stream(stream)
            } else {
                match expression.resolve(name) {
                    Some(bound) => evaluate_int::<Parser, AC>(bound, context),
                    None => {
                        to_typed_stream(context.var(name).expect("checked variable must exist"))
                    }
                }
            }
        }
        ExprView::BinOp(left, right, SBinOp::NOp(operator)) => {
            let left = evaluate_int::<Parser, AC>(expression.scope(left), context);
            let right = evaluate_int::<Parser, AC>(expression.scope(right), context);
            match operator {
                NumericalBinOp::Add => typed::add(left, right),
                NumericalBinOp::Sub => typed::sub(left, right),
                NumericalBinOp::Mul => typed::mul(left, right),
                NumericalBinOp::Div => typed::div(left, right),
                NumericalBinOp::Mod => typed::rem(left, right),
            }
        }
        ExprView::Default(input, default) => typed::default(
            evaluate_int::<Parser, AC>(expression.scope(input), context),
            evaluate_int::<Parser, AC>(expression.scope(default), context),
        ),
        ExprView::SIndex(input, index) => typed::sindex(
            evaluate_int::<Parser, AC>(expression.scope(input), context),
            index,
        ),
        ExprView::If(condition, then_expr, else_expr) => typed::if_stream(
            evaluate_bool::<Parser, AC>(expression.scope(condition), context),
            evaluate_int::<Parser, AC>(expression.scope(then_expr), context),
            evaluate_int::<Parser, AC>(expression.scope(else_expr), context),
        ),
        _ => fallback::<i64, Parser, AC>(expression, context),
    }
}

fn evaluate_bool<Parser, AC>(
    expression: ScopedExpr,
    context: &AC::Ctx,
) -> OutputStream<PartialStreamValue<bool>>
where
    Parser: ExprParser<Expr> + 'static,
    AC: AsyncConfig<Val = Value>,
{
    let node = expression.as_ref();
    match node.view() {
        ExprView::Val(Value::Bool(value)) => typed::val(*value),
        ExprView::Var(name) => {
            if let Some(stream) = expression.resolve_stream(name) {
                to_typed_stream(stream)
            } else {
                match expression.resolve(name) {
                    Some(bound) => evaluate_bool::<Parser, AC>(bound, context),
                    None => {
                        to_typed_stream(context.var(name).expect("checked variable must exist"))
                    }
                }
            }
        }
        ExprView::Not(input) => typed::not(evaluate_bool::<Parser, AC>(
            expression.scope(input),
            context,
        )),
        ExprView::BinOp(left, right, SBinOp::BOp(operator)) => {
            let left = evaluate_bool::<Parser, AC>(expression.scope(left), context);
            let right = evaluate_bool::<Parser, AC>(expression.scope(right), context);
            match operator {
                BoolBinOp::And => typed::and(left, right),
                BoolBinOp::Or => typed::or(left, right),
                BoolBinOp::Impl => typed::implies(left, right),
            }
        }
        ExprView::BinOp(left, right, SBinOp::COp(operator))
            if matches!(expression.typ(left), Some(TCType::Int)) =>
        {
            let left = evaluate_int::<Parser, AC>(expression.scope(left), context);
            let right = evaluate_int::<Parser, AC>(expression.scope(right), context);
            match operator {
                CompBinOp::Eq => typed::compare(left, right, |left, right| left == right),
                CompBinOp::Le => typed::compare(left, right, |left, right| left <= right),
                CompBinOp::Lt => typed::compare(left, right, |left, right| left < right),
                CompBinOp::Ge => typed::compare(left, right, |left, right| left >= right),
                CompBinOp::Gt => typed::compare(left, right, |left, right| left > right),
            }
        }
        ExprView::BinOp(left, right, SBinOp::COp(operator))
            if matches!(expression.typ(left), Some(TCType::Float)) =>
        {
            let left = evaluate_float::<Parser, AC>(expression.scope(left), context);
            let right = evaluate_float::<Parser, AC>(expression.scope(right), context);
            match operator {
                CompBinOp::Eq => typed::compare(left, right, |left, right| left == right),
                CompBinOp::Le => typed::compare(left, right, |left, right| left <= right),
                CompBinOp::Lt => typed::compare(left, right, |left, right| left < right),
                CompBinOp::Ge => typed::compare(left, right, |left, right| left >= right),
                CompBinOp::Gt => typed::compare(left, right, |left, right| left > right),
            }
        }
        ExprView::Default(input, default) => typed::default(
            evaluate_bool::<Parser, AC>(expression.scope(input), context),
            evaluate_bool::<Parser, AC>(expression.scope(default), context),
        ),
        ExprView::SIndex(input, index) => typed::sindex(
            evaluate_bool::<Parser, AC>(expression.scope(input), context),
            index,
        ),
        ExprView::If(condition, then_expr, else_expr) => typed::if_stream(
            evaluate_bool::<Parser, AC>(expression.scope(condition), context),
            evaluate_bool::<Parser, AC>(expression.scope(then_expr), context),
            evaluate_bool::<Parser, AC>(expression.scope(else_expr), context),
        ),
        _ => fallback::<bool, Parser, AC>(expression, context),
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use futures::{StreamExt, stream};
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;

    use super::evaluate_checked;
    use crate::async_test;
    use crate::core::Value;
    use crate::dsrv_fixtures::TestConfig;
    use crate::lang::dsrv::{lalr_parser::LALRParser, type_checker::type_check};
    use crate::runtime::asynchronous::Context;
    use crate::semantics::StreamContext;

    #[apply(async_test)]
    async fn checked_float_arithmetic_and_comparison_are_specialised(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let specification = crate::dsrv_spec!("out result: Bool\nresult = (1.5 + 2.0) < 4.0");
        let checked = type_check(specification).unwrap();
        let expression = checked.var_expr(&"result".into()).unwrap();
        let context = Context::<TestConfig>::new(executor, Vec::new(), Vec::new(), 0);

        let values = evaluate_checked::<LALRParser, TestConfig>(expression, &context)
            .take(1)
            .collect::<Vec<_>>()
            .await;

        assert_eq!(values, [Value::Bool(true)]);
    }

    #[apply(async_test)]
    async fn checked_float_default_repeats_known_values_across_no_val(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let specification =
            crate::dsrv_spec!("in x: Float\nout result: Float\nresult = default(x, 2.0)");
        let checked = type_check(specification).unwrap();
        let expression = checked.var_expr(&"result".into()).unwrap();
        let input = Box::pin(stream::iter([Value::Float(1.0), Value::NoVal]));
        let mut context = Context::<TestConfig>::new(executor, vec!["x".into()], vec![input], 0);

        let output = evaluate_checked::<LALRParser, TestConfig>(expression, &context);
        context.run().await;
        let values = output.collect::<Vec<_>>().await;

        assert_eq!(values, [Value::Float(1.0), Value::Float(1.0)]);
    }
}
