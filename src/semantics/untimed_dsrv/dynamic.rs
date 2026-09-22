use super::combinators::stream_lift_base;
use super::{functions::ScopedExpr, semantics::evaluate_scope};
use crate::core::{Capabilities, Capability, Value};
use crate::lang::dsrv::ast::ReconfigurableExprScope;
use crate::lang::dsrv::runtime_expression::RuntimeExpressionSite;
use crate::semantics::{AsyncConfig, StreamContext};
use crate::{LocalStream, VarName};
use async_stream::stream;
use futures::StreamExt;
use tracing::{debug, info};

/// Check a runtime expression on arrival and prepare it for evaluation.
/// Source that does not check is refused, as source that does not parse
/// always was.
fn accept_text(site: &RuntimeExpressionSite, source: &str, owner: Option<&VarName>) -> ScopedExpr {
    let checked = site
        .parse_and_check_for(
            source,
            Capabilities::NONE
                .with(Capability::TaggedUnions)
                .with(Capability::PatternMatching),
            "untimed stream",
        )
        .unwrap_or_else(|error| panic!("{error}"));
    debug!("Runtime expression accepted as {:?}", checked.expr());
    let expression = ScopedExpr::checked(checked);
    match owner {
        Some(owner) => expression.with_owner(owner.clone()),
        None => expression,
    }
}

pub fn dynamic<AC>(
    ctx: &AC::Ctx,
    eval_stream: LocalStream<AC::Val>,
    scope: ReconfigurableExprScope,
    owner: Option<VarName>,
    history_length: usize,
    site: RuntimeExpressionSite,
) -> LocalStream<AC::Val>
where
    AC: AsyncConfig<Val = Value>,
{
    // `dynamic` propagates `Deferred` from its property stream even when an
    // expression is already installed. A specification can retain the previous
    // result explicitly with `default(dynamic(e), z[-1])`.

    // Create a subcontext with a history window length
    let mut subcontext = match scope {
        ReconfigurableExprScope::Explicit(vs) => ctx.restricted_subcontext(vs, history_length),
        ReconfigurableExprScope::Automatic => match owner.as_ref() {
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
            eval_output_stream: LocalStream<Value>
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
                    let expression = accept_text(&site, s.as_ref(), owner.as_ref());
                    let eval_output_stream = evaluate_scope::<AC>(expression, &subcontext);
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

pub fn defer<AC>(
    ctx: &AC::Ctx,
    eval_stream: LocalStream<AC::Val>,
    scope: ReconfigurableExprScope,
    owner: Option<VarName>,
    history_length: usize,
    site: RuntimeExpressionSite,
) -> LocalStream<AC::Val>
where
    AC: AsyncConfig<Val = Value>,
{
    // Create a subcontext with a history window length
    let mut subcontext = match scope {
        ReconfigurableExprScope::Explicit(vs) => ctx.restricted_subcontext(vs, history_length),
        ReconfigurableExprScope::Automatic => match owner.as_ref() {
            Some(owner) => ctx.subcontext_excluding(&owner, history_length),
            None => ctx.subcontext(history_length),
        },
    };
    let mut eval_stream = stream_lift_base(eval_stream);
    let mut eval_output_stream: Option<LocalStream<Value>> = None;

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
                    let expression = accept_text(&site, s.as_ref(), owner.as_ref());
                    let tmp_stream = evaluate_scope::<AC>(expression, &subcontext);
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
