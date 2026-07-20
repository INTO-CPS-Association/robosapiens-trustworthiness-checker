use std::rc::Rc;

use super::combinators::stream_lift_base;
use super::semantics::{evaluate, evaluate_checked};
use crate::core::Value;
use crate::lang::core::parser::ExprParser;
use crate::lang::dsrv::ast::{DynamicExprScope, Expr};
use crate::lang::dsrv::type_checker::{TCType, TypeInfo, check_expression};
use crate::semantics::{AsyncConfig, StreamContext};
use crate::{OutputStream, VarName};
use async_stream::stream;
use futures::StreamExt;
use tracing::{debug, info};

pub fn dynamic<AC, Parser>(
    ctx: &AC::Ctx,
    eval_stream: OutputStream<AC::Val>,
    scope: DynamicExprScope,
    history_length: usize,
) -> OutputStream<AC::Val>
where
    Parser: ExprParser<Expr> + 'static,
    AC: AsyncConfig<Val = Value>,
{
    dynamic_checked::<AC, Parser>(ctx, eval_stream, scope, None, history_length, None)
}

pub(crate) fn dynamic_checked<AC, Parser>(
    ctx: &AC::Ctx,
    eval_stream: OutputStream<AC::Val>,
    scope: DynamicExprScope,
    owner: Option<VarName>,
    history_length: usize,
    checked: Option<(Rc<TypeInfo>, TCType)>,
) -> OutputStream<AC::Val>
where
    Parser: ExprParser<Expr> + 'static,
    AC: AsyncConfig<Val = Value>,
{
    // `dynamic` propagates `Deferred` from its property stream even when an
    // expression is already installed. A specification can retain the previous
    // result explicitly with `default(dynamic(e), z[-1])`.

    // Create a subcontext with a history window length
    let mut subcontext = match scope {
        DynamicExprScope::Explicit(vs) => ctx.restricted_subcontext(vs, history_length),
        DynamicExprScope::Automatic => match owner {
            Some(owner) => ctx.subcontext_excluding(&owner, history_length),
            None => ctx.subcontext(history_length),
        },
    };
    let mut eval_stream = stream_lift_base(eval_stream);

    // Build an output stream for dynamic of x over the subcontext
    Box::pin(stream! {
        // Store the previous value of the stream we are evaluating so we can
        // check when it changes
        struct PrevData {
            // The previous property provided
            eval_val: Value,
            // The output stream for dynamic
            eval_output_stream: OutputStream<Value>
        }
        let mut prev_data: Option<PrevData> = None;
        while let Some(current) = eval_stream.next().await {
            debug!("Received new dynamic property value: {:?}", current);
            // If we have a previous value and it is the same as the current value (no need to
            // repeat evaluation), then continue using the existing stream as our output
            if let Some(prev_data) = &mut prev_data {
                if prev_data.eval_val == current {
                    // Advance the subcontext to make a new set of input values
                    // available for the dynamic stream
                    subcontext.tick().await;

                    if let Some(eval_res) = prev_data.eval_output_stream.next().await {
                        yield eval_res;
                        continue;
                    } else {
                        return;
                    }
                }
            }
            // This match only happens if we have a new Str to evaluate, received Deferred or if we
            // do not have a `prev_data.eval_output_stream` to evaluate from
            match current {
                Value::Deferred => {
                    // A Deferred property controls the value emitted by `dynamic`, but it must not
                    // pause the already installed expression. DynSRV evaluates the most recently
                    // supplied property at the current global time. For example, with property
                    // `x[1]`, inputs x = [1, 2, 3], and property values
                    // ["x[1]", Deferred, "x[1]"], the final value is 2, not 1: the installed
                    // expression consumes the middle tick even though `dynamic` emits Deferred.
                    // Advance and discard that internal result to keep its temporal state aligned.
                    subcontext.tick().await;
                    if let Some(prev_data) = &mut prev_data {
                        if prev_data.eval_output_stream.next().await.is_none() {
                            return;
                        }
                    }
                    yield Value::Deferred;
                }
                Value::NoVal => {
                    // Consume a sample from the subcontext but return NoVal
                    subcontext.tick().await;
                    yield Value::NoVal;
                }
                Value::Str(s) => {
                    let expr = Parser::parse(&mut s.as_ref())
                        .expect("Invalid dynamic str");
                    let eval_output_stream = if let Some((type_info, expected)) = checked.clone() {
                        let expr = check_expression(expr, &expected, &type_info)
                        .unwrap_or_else(|errors| {
                            panic!("Dynamic expression failed type checking: {errors:?}")
                        });
                        debug!("Dynamic evaluated to checked expression {:?}", expr);
                        evaluate_checked::<Parser, AC>(expr, &subcontext)
                    } else {
                        debug!("Dynamic evaluated to expression {:?}", expr);
                        evaluate::<Parser, AC>(expr, &subcontext)
                    };
                    let mut eval_output_stream = stream_lift_base(eval_output_stream);
                    // Advance the subcontext to make a new set of input values
                    // available for the dynamic stream
                    subcontext.tick().await;
                    if let Some(eval_res) = eval_output_stream.next().await {
                        yield eval_res;
                    } else {
                        return;
                    }
                    prev_data = Some(PrevData{
                        eval_val: Value::Str(s),
                        eval_output_stream
                    });
                }
                cur => panic!("Invalid dynamic property type {:?}", cur)
            }
        }
    })
}

pub fn defer<AC, Parser>(
    ctx: &AC::Ctx,
    eval_stream: OutputStream<AC::Val>,
    scope: DynamicExprScope,
    history_length: usize,
) -> OutputStream<AC::Val>
where
    Parser: ExprParser<Expr> + 'static,
    AC: AsyncConfig<Val = Value>,
{
    defer_checked::<AC, Parser>(ctx, eval_stream, scope, None, history_length, None)
}

pub(crate) fn defer_checked<AC, Parser>(
    ctx: &AC::Ctx,
    eval_stream: OutputStream<AC::Val>,
    scope: DynamicExprScope,
    owner: Option<VarName>,
    history_length: usize,
    checked: Option<(Rc<TypeInfo>, TCType)>,
) -> OutputStream<AC::Val>
where
    Parser: ExprParser<Expr> + 'static,
    AC: AsyncConfig<Val = Value>,
{
    // Create a subcontext with a history window length
    let mut subcontext = match scope {
        DynamicExprScope::Explicit(vs) => ctx.restricted_subcontext(vs, history_length),
        DynamicExprScope::Automatic => match owner {
            Some(owner) => ctx.subcontext_excluding(&owner, history_length),
            None => ctx.subcontext(history_length),
        },
    };
    let mut eval_stream = stream_lift_base(eval_stream);
    let mut eval_output_stream: Option<OutputStream<Value>> = None;

    // Build an output stream for dynamic of x over the subcontext
    Box::pin(stream! {
        while let Some(current) = eval_stream.next().await {
            debug!("Received new defer property value: {:?}", current);
            match current {
                Value::Deferred => {
                    // Consume a sample from the subcontext but return Deferred
                    subcontext.tick().await;
                    yield Value::Deferred;
                }
                Value::NoVal => {
                    // Consume a sample from the subcontext but return NoVal
                    subcontext.tick().await;
                    yield Value::NoVal;
                }
                Value::Str(s) => {
                    let expr = Parser::parse(&mut s.as_ref())
                        .expect("Invalid defer str");
                    let tmp_stream = if let Some((type_info, expected)) = checked.clone() {
                        let expr = check_expression(expr, &expected, &type_info)
                        .unwrap_or_else(|errors| {
                            panic!("Deferred expression failed type checking: {errors:?}")
                        });
                        debug!("Defer evaluated to checked expression {:?}", expr);
                        evaluate_checked::<Parser, AC>(expr, &subcontext)
                    } else {
                        debug!("Defer evaluated to expression {:?}", expr);
                        evaluate::<Parser, AC>(expr, &subcontext)
                    };
                    let mut tmp_stream = stream_lift_base(tmp_stream);
                    // Advance the subcontext to make a new set of input values
                    // available for the dynamic stream
                    subcontext.tick().await;
                    if let Some(eval_res) = tmp_stream.next().await {
                        eval_output_stream = Some(tmp_stream);
                        yield eval_res;
                    } else {
                        return;
                    }
                    break;
                }
                cur => panic!("Invalid defer property type {:?}", cur)
            }
        }
        if eval_output_stream.is_none() {
            info!("Eval stream ended without a valid property to defer on");
            return;
        }
        let mut eval_output_stream = eval_output_stream.unwrap();

        // Use eval_stream as controller for when to tick subcontext. Yield from
        // eval_output_stream.
        debug!("Starting defer output loop");
        while let Some(_) = eval_stream.next().await {
            subcontext.tick().await;
            if let Some(eval_res) = eval_output_stream.next().await {
                yield eval_res;
            } else {
                return;
            }
        }
    })
}

// Evaluates to the l.h.s. until the r.h.s. provides a value.
