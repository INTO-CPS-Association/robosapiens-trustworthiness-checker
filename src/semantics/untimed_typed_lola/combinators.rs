use crate::OutputStream;
use crate::core::to_typed_partial_stream;
use crate::core::values::StreamTypeAscription;
use crate::core::{StreamData, TypedStreamData};
use crate::lang::core::parser::ExprParser;
use crate::lang::dynamic_lola::type_checker::PartialStreamValue;
use crate::lang::dynamic_lola::type_checker::{SExprTE, TypeCheckable, TypeContext};
use crate::semantics::untimed_untyped_lola::combinators::{CloneFn1, CloneFn2};
use crate::semantics::{
    AsyncConfig, MonitoringSemantics, StreamContext, TypedUntimedLolaSemantics,
};
use crate::{SExpr, Value, VarName};
use async_stream::stream;
use ecow::EcoVec;
use futures::stream::LocalBoxStream;
use futures::{
    StreamExt,
    stream::{self},
};
use tracing::debug;

fn deferred_stream_lift_base<T: StreamData>(
    mut x_mon: OutputStream<PartialStreamValue<T>>,
) -> OutputStream<PartialStreamValue<T>> {
    Box::pin(stream! {
        let mut last : Option<PartialStreamValue<T>>  = None;
        while let Some(curr) = x_mon.next().await {
            match curr {
                PartialStreamValue::NoVal => {
                    if let Some(last) = &last {
                        yield last.clone();
                    } else {
                        // Only happens when the first value is NoVal
                        yield PartialStreamValue::NoVal;
                    }
                }
                PartialStreamValue::Deferred => {
                    last = Some(PartialStreamValue::Deferred);
                    yield PartialStreamValue::Deferred;
                }
                PartialStreamValue::Known(val) => {
                    last = Some(PartialStreamValue::Known(val.clone()));
                    yield PartialStreamValue::Known(val);
                }
            }
        }
    })
}

pub fn deferred_lift1<S: StreamData, R: StreamData>(
    f: impl CloneFn1<S, R>,
    x_mon: OutputStream<PartialStreamValue<S>>,
) -> OutputStream<PartialStreamValue<R>> {
    let f = f.clone();
    Box::pin(x_mon.map(move |x| match x {
        PartialStreamValue::Known(x) => PartialStreamValue::Known(f(x)),
        PartialStreamValue::NoVal => PartialStreamValue::NoVal,
        PartialStreamValue::Deferred => PartialStreamValue::Deferred,
    }))
}

// Note that this might not cover all cases. Certain operators may want to yield
// the known value if either x or y is known.
// Currently wrong
pub fn deferred_lift2<S: StreamData, R: StreamData, U: StreamData>(
    f: impl CloneFn2<S, R, U>,
    x_mon: OutputStream<PartialStreamValue<S>>,
    y_mon: OutputStream<PartialStreamValue<R>>,
) -> OutputStream<PartialStreamValue<U>> {
    let f = f.clone();
    let x_mon = deferred_stream_lift_base(x_mon);
    let y_mon = deferred_stream_lift_base(y_mon);
    Box::pin(x_mon.zip(y_mon).map(move |(x, y)| match (x, y) {
        (PartialStreamValue::Known(x), PartialStreamValue::Known(y)) => {
            PartialStreamValue::Known(f(x, y))
        }
        // Deferred has precedence over NoVal since it every Boolean operator is deferred
        // until both branches are known
        (PartialStreamValue::Deferred, _) | (_, PartialStreamValue::Deferred) => {
            PartialStreamValue::Deferred
        }
        (PartialStreamValue::NoVal, _) | (_, PartialStreamValue::NoVal) => {
            PartialStreamValue::NoVal
        }
    }))
}

pub fn and(
    x: OutputStream<PartialStreamValue<bool>>,
    y: OutputStream<PartialStreamValue<bool>>,
) -> OutputStream<PartialStreamValue<bool>> {
    deferred_lift2(|x, y| x && y, x, y)
}

pub fn or(
    x: OutputStream<PartialStreamValue<bool>>,
    y: OutputStream<PartialStreamValue<bool>>,
) -> OutputStream<PartialStreamValue<bool>> {
    deferred_lift2(|x, y| x || y, x, y)
}

pub fn implication(
    x: OutputStream<PartialStreamValue<bool>>,
    y: OutputStream<PartialStreamValue<bool>>,
) -> OutputStream<PartialStreamValue<bool>> {
    deferred_lift2(|x, y| !x || y, x, y)
}

pub fn not(x: OutputStream<PartialStreamValue<bool>>) -> OutputStream<PartialStreamValue<bool>> {
    deferred_lift1(|x| !x, x)
}

pub fn eq<X: Eq + StreamData>(
    x: OutputStream<PartialStreamValue<X>>,
    y: OutputStream<PartialStreamValue<X>>,
) -> OutputStream<PartialStreamValue<bool>> {
    deferred_lift2(|x, y| x == y, x, y)
}

pub fn le(
    x: OutputStream<PartialStreamValue<i64>>,
    y: OutputStream<PartialStreamValue<i64>>,
) -> OutputStream<PartialStreamValue<bool>> {
    deferred_lift2(|x, y| x <= y, x, y)
}

pub fn val<X: StreamData>(x: X) -> OutputStream<X> {
    Box::pin(stream::repeat(x.clone()))
}

pub fn if_stm<X: StreamData>(
    x: OutputStream<PartialStreamValue<bool>>,
    y: OutputStream<PartialStreamValue<X>>,
    z: OutputStream<PartialStreamValue<X>>,
) -> OutputStream<PartialStreamValue<X>> {
    Box::pin(x.zip(y).zip(z).map(move |((x, y), z)| match x {
        PartialStreamValue::Known(x) => {
            if x {
                y
            } else {
                z
            }
        }
        PartialStreamValue::NoVal => PartialStreamValue::NoVal,
        PartialStreamValue::Deferred => PartialStreamValue::Deferred,
    }))
}

// NOTE: For past-time indexing there is a trade-off between allowing recursive definitions with infinite streams
// (such as the count example) and getting the "correct" number of values with finite streams.
// We chose allowing recursive definitions, which means we get N too many
// values for finite streams where N is the absolute value of index.
//
// (Reason: If we want to get the "correct" number of values we need to skip the N
// last samples. This is accomplished by yielding the x[-N] sample but having the stream
// currently at x[0]. However, with recursive streams that puts us in a deadlock when calling
// x.next()
pub fn sindex<X: Clone + 'static>(
    x: OutputStream<PartialStreamValue<X>>,
    i: u64,
) -> OutputStream<PartialStreamValue<X>> {
    if let Ok(i) = usize::try_from(i) {
        let cs = stream::repeat(PartialStreamValue::Deferred).take(i);
        // Delay x by i defers
        let mut delayed = cs.chain(x);

        // Handle NoVals manually (we cannot use deferred_stream_lift_base
        // since we need to manually handle deferred)
        Box::pin(stream! {
            let mut last : Option<PartialStreamValue<X>> = None;
            while let Some(curr) = delayed.next().await {
                match curr {
                    PartialStreamValue::NoVal => {
                        if let Some(last) = &last {
                            yield last.clone();
                        } else {
                            // Only happens when the first value is NoVal
                            yield PartialStreamValue::NoVal;
                        }
                    }
                    PartialStreamValue::Deferred => {
                        last = Some(PartialStreamValue::Deferred);
                        yield PartialStreamValue::Deferred;
                    }
                    PartialStreamValue::Known(val) => {
                        last = Some(PartialStreamValue::Known(val.clone()));
                        yield PartialStreamValue::Known(val);
                    }
                }
            }
        })
    } else {
        panic!("Index too large for sindex operation")
    }
}

pub fn plus<T>(
    x: OutputStream<PartialStreamValue<T>>,
    y: OutputStream<PartialStreamValue<T>>,
) -> OutputStream<PartialStreamValue<T>>
where
    T: std::ops::Add<Output = T> + StreamData,
{
    deferred_lift2(|x, y| x + y, x, y)
}

pub fn modulo<T>(
    x: OutputStream<PartialStreamValue<T>>,
    y: OutputStream<PartialStreamValue<T>>,
) -> OutputStream<PartialStreamValue<T>>
where
    T: std::ops::Rem<Output = T> + StreamData,
{
    deferred_lift2(|x, y| x % y, x, y)
}

pub fn concat(
    x: OutputStream<PartialStreamValue<String>>,
    y: OutputStream<PartialStreamValue<String>>,
) -> OutputStream<PartialStreamValue<String>> {
    deferred_lift2(
        |mut x, y| {
            x.push_str(&y);
            x
        },
        x,
        y,
    )
}

pub fn minus<T>(
    x: OutputStream<PartialStreamValue<T>>,
    y: OutputStream<PartialStreamValue<T>>,
) -> OutputStream<PartialStreamValue<T>>
where
    T: std::ops::Sub<Output = T> + StreamData,
{
    deferred_lift2(|x, y| x - y, x, y)
}

pub fn mult<T>(
    x: OutputStream<PartialStreamValue<T>>,
    y: OutputStream<PartialStreamValue<T>>,
) -> OutputStream<PartialStreamValue<T>>
where
    T: std::ops::Mul<Output = T> + StreamData,
{
    deferred_lift2(|x, y| x * y, x, y)
}

pub fn div<T>(
    x: OutputStream<PartialStreamValue<T>>,
    y: OutputStream<PartialStreamValue<T>>,
) -> OutputStream<PartialStreamValue<T>>
where
    T: std::ops::Div<Output = T> + StreamData,
{
    deferred_lift2(|x, y| x / y, x, y)
}

// Evaluates to a placeholder value whenever Deferred is received.
pub fn default<T: 'static>(
    x: OutputStream<PartialStreamValue<T>>,
    d: OutputStream<PartialStreamValue<T>>,
) -> OutputStream<PartialStreamValue<T>> {
    let xs = x.zip(d).map(|(x, d)| match x {
        PartialStreamValue::Known(x) => PartialStreamValue::Known(x),
        PartialStreamValue::NoVal => PartialStreamValue::NoVal,
        PartialStreamValue::Deferred => d,
    });
    Box::pin(xs) as LocalBoxStream<'static, PartialStreamValue<T>>
}

// WARNING: this currently mirrors the code of the untyped combinator so both should be updated in ;// tandom
pub fn dynamic<AC, Parser, T>(
    ctx: &AC::Ctx,
    eval_stream: OutputStream<PartialStreamValue<String>>,
    vs: Option<EcoVec<VarName>>,
    history_length: usize,
    type_ctx: &TypeContext,
) -> OutputStream<PartialStreamValue<T>>
where
    Parser: ExprParser<SExpr> + 'static,
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    T: TypedStreamData + TryFrom<Value, Error = ()>,
{
    // Note: Slight change in dynamic's behavior after language became async and we introduced
    // NoVal. Consider the following behavior:
    // eval_stream yields 42
    // eval_stream yields Deferred
    // Before the change, dynamic would evaluate to (42, 42), but now it evaluates
    // to (42, Deferred).
    // This is a design decision, but it is more flexible for scenarios where we have, e.g. nested
    // DUPs. If one wants the old behavior, one can write z = default(dynamic(e), z[-1])

    // Create a subcontext with a history window length
    let mut subcontext = match vs {
        Some(vs) => ctx.restricted_subcontext(vs, history_length),
        None => ctx.subcontext(history_length),
    };
    let type_ctx = type_ctx.clone();
    let mut eval_stream = deferred_stream_lift_base(eval_stream);

    // Build an output stream for dynamic of x over the subcontext
    Box::pin(stream! {
        // Store the previous value of the stream we are evaluating so we can
        // check when it changes
        // We store Value streams to avoid generic parameter issues inside the stream macro
        // TODO: fix this via implementing a parameterised typed semantics
        let mut prev_data: Option<(PartialStreamValue<String>, OutputStream<Value>)> = None;
        while let Some(current) = eval_stream.next().await {
            // If we have a previous value and it is the same as the current value (no need to
            // repeat evaluation), then continue using the existing stream as our output
            if let Some((ref eval_val, ref mut eval_output_stream)) = prev_data {
                if eval_val == &current {
                    // Advance the subcontext to make a new set of input values
                    // available for the dynamic stream
                    subcontext.tick().await;

                    if let Some(eval_res) = eval_output_stream.next().await {
                        // Convert from Value to PartialStreamValue<T>
                        match eval_res {
                            Value::NoVal => yield PartialStreamValue::NoVal,
                            Value::Deferred => yield PartialStreamValue::Deferred,
                            v => yield PartialStreamValue::Known(v.try_into().expect("Type error when casting stream")),
                        }
                        continue;
                    } else {
                        return;
                    }
                }
            }
            // This match only happens if we have a new Str to evaluate, received Deferred or if we
            // do not have a `prev_data.eval_output_stream` to evaluate from
            match current {
                PartialStreamValue::Deferred => {
                    // Consume a sample from the subcontext but return Deferred
                    subcontext.tick().await;
                    yield PartialStreamValue::Deferred;
                }
                PartialStreamValue::NoVal => {
                    // Consume a sample from the subcontext but return NoVal
                    subcontext.tick().await;
                    yield PartialStreamValue::NoVal;
                }
                PartialStreamValue::Known(s) => {
                    let expr = Parser::parse(&mut s.as_ref())
                        .expect("Invalid dynamic str");
                    debug!("Dynamic evaluated to expression {:?}", expr);
                    // Create a typed version of the expression
                    let mut type_ctx_local = type_ctx.clone();
                    let expr = (expr, StreamTypeAscription::Ascribed(T::stream_data_type())).type_check(&mut type_ctx_local)
                        .expect("Type error");
                    let eval_output_stream_raw: OutputStream<Value> = <TypedUntimedLolaSemantics::<Parser> as MonitoringSemantics<AC>>::to_async_stream(expr, &subcontext);
                    // Apply stream lift to handle NoVal by repeating last value
                    let mut eval_output_stream = Box::pin(stream! {
                        let mut last: Option<Value> = None;
                        let mut eval_stream = eval_output_stream_raw;
                        while let Some(curr) = eval_stream.next().await {
                            match curr {
                                Value::NoVal => {
                                    if let Some(last) = &last {
                                        yield last.clone();
                                    } else {
                                        yield Value::NoVal;
                                    }
                                }
                                _ => {
                                    last = Some(curr.clone());
                                    yield curr;
                                }
                            }
                        }
                    });
                    // Advance the subcontext to make a new set of input values
                    // available for the dynamic stream
                    subcontext.tick().await;
                    if let Some(eval_res) = eval_output_stream.next().await {
                        // Convert from Value to PartialStreamValue<T>
                        match eval_res {
                            Value::NoVal => yield PartialStreamValue::NoVal,
                            Value::Deferred => yield PartialStreamValue::Deferred,
                            v => yield PartialStreamValue::Known(v.try_into().expect("Type error when casting stream")),
                        }
                    } else {
                        return;
                    }
                    prev_data = Some((PartialStreamValue::Known(s), eval_output_stream));
                }
            }
        }
    })
}

pub fn var<AC>(ctx: &AC::Ctx, var: VarName) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value>,
{
    debug!(?var, "Accessing variable");
    match ctx.var(&var) {
        Some(stream) => {
            debug!(?var, "Found variable");
            stream
        }
        None => {
            debug!(?var, "Variable not found - this will panic");
            panic!("Variable \"{}\" not found", var)
        }
    }
}

// Defer for an UntimedLolaExpression using the lola_expression parser
// TODO: this currently has unnecessary potentially-panicing casts since the types in the untyped
// semantics are not granular enough
// #[instrument(skip(ctx, prop_stream))]
pub fn defer<AC, Parser, T>(
    ctx: &AC::Ctx,
    mut prop_stream: OutputStream<PartialStreamValue<String>>,
    history_length: usize,
    type_ctx: &TypeContext,
) -> OutputStream<PartialStreamValue<T>>
where
    Parser: ExprParser<SExpr> + 'static,
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    T: TypedStreamData + TryFrom<Value, Error = ()>,
{
    let mut subcontext = ctx.subcontext(history_length);
    let type_ctx = type_ctx.clone();
    Box::pin(stream! {
        let mut eval_output_stream: Option<OutputStream<PartialStreamValue<T>>> = None;
        let mut i = 0;
        let mut prev_received_deferred = false;

        // Yield Deferred until we have a value to evaluate, then evaluate it
        while let Some(current) = prop_stream.next().await {
            debug!(?i, ?current, "Defer");
            match current {
                PartialStreamValue::Known(defer_s) => {
                    // We have a string to evaluate so do so
                    let expr = Parser::parse(&mut defer_s.as_ref())
                        .expect("Invalid dynamic str");
                    // Create a typed version of the expression
                    let mut type_ctx_local = type_ctx.clone();
                    let expr = (expr, StreamTypeAscription::Ascribed(T::stream_data_type())).type_check(&mut type_ctx_local)
                        .expect("Type error");
                    let untyped_eval_output_stream: OutputStream<Value> = <TypedUntimedLolaSemantics::<Parser> as MonitoringSemantics<AC>>::to_async_stream(expr, &subcontext);
                    eval_output_stream = Some(to_typed_partial_stream::<T>(untyped_eval_output_stream));
                    // debug!(s = ?defer_s.as_ref(), "Evaluated defer string");
                    subcontext.run().await;
                    break;
                }
                PartialStreamValue::Deferred => {
                    // Consume a sample from the subcontext but return Deferred
                    debug!("defer combinator received Deferred");
                    if i >= history_length {
                        debug!(?i, ?history_length, "Advancing subcontext to clean history");
                        subcontext.tick().await;
                    }
                    i += 1;
                    prev_received_deferred = true;
                    yield PartialStreamValue::Deferred;
                }
                PartialStreamValue::NoVal => {
                    // Consume a sample from the subcontext but return NoVal
                    debug!("defer combinator received NoVal");
                    if i >= history_length {
                        debug!(?i, ?history_length, "Advancing subcontext to clean history");
                        subcontext.tick().await;
                    }
                    i += 1;

                    // Deferred is sticky compared to NoVal, since Deferred indicates that we have
                    // a pending property that cannot be evaluated yet with the given context.
                    if prev_received_deferred {
                        yield PartialStreamValue::Deferred;
                    } else {
                        yield PartialStreamValue::NoVal;
                    }
                }

            }
        }

        // This is None if the prop_stream is done but we never received a property
        if let Some(eval_output_stream) = eval_output_stream {
            // Wind forward the stream to the current time
            let time_progressed = i.min(history_length);
            debug!(?i, ?time_progressed, ?history_length, "Time progressed");
            let mut eval_output_stream = eval_output_stream.skip(time_progressed);

            // Yield the saved value until the inner stream is done
            while let Some(eval_res) = eval_output_stream.next().await {
                yield eval_res;
            }
        }
    })
}

#[cfg(test)]
// The following functions from the untyped version are not yet implemented in the typed version:
// - update: Update operator for combining two streams
// - list, lindex, lappend, lconcat, lhead, ltail, llen: List operations
// - map, mget, mremove, minsert, mhas_key: Map operations
// - latch: Latch operator for state management
// - lt, gt, ge: Additional comparison operators
// - abs, sin, cos, tan: Mathematical functions
// - init, is_defined, when: Additional stream combinators
// They will require additional tests once ported.
mod tests {
    use super::*;
    use crate::async_test;
    use crate::core::StreamType;

    use crate::runtime::asynchronous::Context;
    use futures::stream;
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::collections::BTreeMap;
    use std::rc::Rc;

    type Parser = crate::lang::dynamic_lola::lalr_parser::LALRParser;

    // Typed test configuration
    #[derive(Clone, Copy)]
    pub struct TypedTestConfig {}
    impl AsyncConfig for TypedTestConfig {
        type Val = Value;
        type Expr = SExprTE;
        type Ctx = Context<Self>;
    }

    type TestCtx = Context<TypedTestConfig>;

    #[apply(async_test)]
    async fn test_not() {
        let x: OutputStream<PartialStreamValue<bool>> = Box::pin(stream::iter(
            vec![
                PartialStreamValue::Known(true),
                PartialStreamValue::Known(false),
            ]
            .into_iter(),
        ));
        let z: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        let res: Vec<PartialStreamValue<bool>> = not(x).collect().await;
        assert_eq!(res, z);
    }

    #[apply(async_test)]
    async fn test_plus() {
        let x: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(
            vec![PartialStreamValue::Known(1), PartialStreamValue::Known(3)].into_iter(),
        ));
        let y: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(
            vec![PartialStreamValue::Known(2), PartialStreamValue::Known(4)].into_iter(),
        ));
        let z: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(3), PartialStreamValue::Known(7)];
        let res: Vec<PartialStreamValue<i64>> = plus(x, y).collect().await;
        assert_eq!(res, z);
    }

    #[apply(async_test)]
    async fn test_str_plus() {
        let x: OutputStream<PartialStreamValue<String>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known("hello ".into()),
            PartialStreamValue::Known("olleh ".into()),
        ]));
        let y: OutputStream<PartialStreamValue<String>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known("world".into()),
            PartialStreamValue::Known("dlrow".into()),
        ]));
        let exp: Vec<PartialStreamValue<String>> = vec![
            PartialStreamValue::Known("hello world".into()),
            PartialStreamValue::Known("olleh dlrow".into()),
        ];
        let res: Vec<PartialStreamValue<String>> = concat(x, y).collect().await;
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_sindex_delay_1() {
        let x: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(1),
            PartialStreamValue::Known(2),
            PartialStreamValue::Known(3),
        ]));
        let res: Vec<PartialStreamValue<i64>> = sindex(x, 1).collect().await;
        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Deferred,
            PartialStreamValue::Known(1),
            PartialStreamValue::Known(2),
            PartialStreamValue::Known(3),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_sindex_delay_2() {
        let x: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(1),
            PartialStreamValue::Known(2),
            PartialStreamValue::Known(3),
        ]));
        let res: Vec<PartialStreamValue<i64>> = sindex(x, 2).collect().await;
        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Deferred,
            PartialStreamValue::Deferred,
            PartialStreamValue::Known(1),
            PartialStreamValue::Known(2),
            PartialStreamValue::Known(3),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_sindex_noval_at_start() {
        let x: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::NoVal,
            PartialStreamValue::Known(1),
            PartialStreamValue::Known(2),
        ]));
        let res: Vec<PartialStreamValue<i64>> = sindex(x, 1).collect().await;
        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Deferred,
            PartialStreamValue::Deferred, // NoVal after Deferred repeats Deferred
            PartialStreamValue::Known(1),
            PartialStreamValue::Known(2),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_sindex_noval_in_middle() {
        let x: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(1),
            PartialStreamValue::NoVal,
            PartialStreamValue::Known(2),
        ]));
        let res: Vec<PartialStreamValue<i64>> = sindex(x, 1).collect().await;
        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Deferred,
            PartialStreamValue::Known(1),
            PartialStreamValue::Known(1), // NoVal repeats the last known value
            PartialStreamValue::Known(2),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_sindex_multiple_noval() {
        let x: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(1),
            PartialStreamValue::NoVal,
            PartialStreamValue::NoVal,
            PartialStreamValue::Known(2),
        ]));
        let res: Vec<PartialStreamValue<i64>> = sindex(x, 1).collect().await;
        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Deferred,
            PartialStreamValue::Known(1),
            PartialStreamValue::Known(1), // First NoVal repeats Known(1)
            PartialStreamValue::Known(1), // Second NoVal also repeats Known(1)
            PartialStreamValue::Known(2),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_sindex_with_deferred_in_stream() {
        let x: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(1),
            PartialStreamValue::Deferred,
            PartialStreamValue::Known(2),
        ]));
        let res: Vec<PartialStreamValue<i64>> = sindex(x, 1).collect().await;
        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Deferred, // Added by sindex
            PartialStreamValue::Known(1),
            PartialStreamValue::Deferred, // Deferred from stream
            PartialStreamValue::Known(2),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_sindex_noval_after_deferred() {
        let x: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Deferred,
            PartialStreamValue::NoVal,
            PartialStreamValue::Known(1),
        ]));
        let res: Vec<PartialStreamValue<i64>> = sindex(x, 1).collect().await;
        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Deferred, // Added by sindex
            PartialStreamValue::Deferred, // From stream
            PartialStreamValue::Deferred, // NoVal repeats last value which was Deferred
            PartialStreamValue::Known(1),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_sindex_complex_pattern() {
        let x: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(1),
            PartialStreamValue::NoVal,
            PartialStreamValue::Deferred,
            PartialStreamValue::Known(2),
            PartialStreamValue::NoVal,
        ]));
        let res: Vec<PartialStreamValue<i64>> = sindex(x, 2).collect().await;
        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Deferred, // Added by sindex
            PartialStreamValue::Deferred, // Added by sindex
            PartialStreamValue::Known(1),
            PartialStreamValue::Known(1), // NoVal repeats Known(1)
            PartialStreamValue::Deferred, // Deferred from stream
            PartialStreamValue::Known(2),
            PartialStreamValue::Known(2), // NoVal repeats Known(2)
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_default_no_deferred() {
        let x: OutputStream<PartialStreamValue<String>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known("x0".into()),
            PartialStreamValue::Known("x1".into()),
            PartialStreamValue::Known("x2".into()),
        ]));
        let d: OutputStream<PartialStreamValue<String>> =
            Box::pin(stream::repeat(PartialStreamValue::Known("d".into())));
        let res: Vec<PartialStreamValue<String>> = default(x, d).take(3).collect().await;
        let exp: Vec<PartialStreamValue<String>> = vec![
            PartialStreamValue::Known("x0".into()),
            PartialStreamValue::Known("x1".into()),
            PartialStreamValue::Known("x2".into()),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_default_all_deferred() {
        let x: OutputStream<PartialStreamValue<String>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Deferred,
            PartialStreamValue::Deferred,
            PartialStreamValue::Deferred,
        ]));
        let d: OutputStream<PartialStreamValue<String>> =
            Box::pin(stream::repeat(PartialStreamValue::Known("d".into())));
        let res: Vec<PartialStreamValue<String>> = default(x, d).collect().await;
        let exp: Vec<PartialStreamValue<String>> = vec![
            PartialStreamValue::Known("d".into()),
            PartialStreamValue::Known("d".into()),
            PartialStreamValue::Known("d".into()),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_default_one_deferred() {
        let x: OutputStream<PartialStreamValue<String>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known("x0".into()),
            PartialStreamValue::Deferred,
            PartialStreamValue::Known("x2".into()),
        ]));
        let d: OutputStream<PartialStreamValue<String>> =
            Box::pin(stream::repeat(PartialStreamValue::Known("d".into())));
        let res: Vec<PartialStreamValue<String>> = default(x, d).collect().await;
        let exp: Vec<PartialStreamValue<String>> = vec![
            PartialStreamValue::Known("x0".into()),
            PartialStreamValue::Known("d".into()),
            PartialStreamValue::Known("x2".into()),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_and() {
        let x: OutputStream<PartialStreamValue<bool>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(false),
        ]));
        let y: OutputStream<PartialStreamValue<bool>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ]));
        let res: Vec<PartialStreamValue<bool>> = and(x, y).collect().await;
        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_or() {
        let x: OutputStream<PartialStreamValue<bool>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(false),
        ]));
        let y: OutputStream<PartialStreamValue<bool>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ]));
        let res: Vec<PartialStreamValue<bool>> = or(x, y).collect().await;
        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_implication() {
        let x: OutputStream<PartialStreamValue<bool>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(false),
        ]));
        let y: OutputStream<PartialStreamValue<bool>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ]));
        let res: Vec<PartialStreamValue<bool>> = implication(x, y).collect().await;
        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_eq() {
        let x: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(1),
            PartialStreamValue::Known(2),
            PartialStreamValue::Known(3),
        ]));
        let y: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(1),
            PartialStreamValue::Known(3),
            PartialStreamValue::Known(3),
        ]));
        let res: Vec<PartialStreamValue<bool>> = eq(x, y).collect().await;
        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_le() {
        let x: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(1),
            PartialStreamValue::Known(2),
            PartialStreamValue::Known(3),
        ]));
        let y: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(1),
            PartialStreamValue::Known(1),
            PartialStreamValue::Known(3),
        ]));
        let res: Vec<PartialStreamValue<bool>> = le(x, y).collect().await;
        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_minus() {
        let x: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(5),
            PartialStreamValue::Known(10),
        ]));
        let y: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(2),
            PartialStreamValue::Known(3),
        ]));
        let res: Vec<PartialStreamValue<i64>> = minus(x, y).collect().await;
        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(3), PartialStreamValue::Known(7)];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_mult() {
        let x: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(2),
            PartialStreamValue::Known(3),
        ]));
        let y: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(4),
            PartialStreamValue::Known(5),
        ]));
        let res: Vec<PartialStreamValue<i64>> = mult(x, y).collect().await;
        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(8), PartialStreamValue::Known(15)];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_div() {
        let x: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(10),
            PartialStreamValue::Known(20),
        ]));
        let y: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(2),
            PartialStreamValue::Known(4),
        ]));
        let res: Vec<PartialStreamValue<i64>> = div(x, y).collect().await;
        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(5), PartialStreamValue::Known(5)];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_modulo() {
        let x: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(10),
            PartialStreamValue::Known(17),
        ]));
        let y: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(3),
            PartialStreamValue::Known(5),
        ]));
        let res: Vec<PartialStreamValue<i64>> = modulo(x, y).collect().await;
        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(1), PartialStreamValue::Known(2)];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_if_then_else() {
        let cond: OutputStream<PartialStreamValue<bool>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ]));
        let then_branch: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(1),
            PartialStreamValue::Known(2),
            PartialStreamValue::Known(3),
        ]));
        let else_branch: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(10),
            PartialStreamValue::Known(20),
            PartialStreamValue::Known(30),
        ]));
        let res: Vec<PartialStreamValue<i64>> =
            if_stm(cond, then_branch, else_branch).collect().await;
        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Known(1),
            PartialStreamValue::Known(20),
            PartialStreamValue::Known(3),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_noval_propagation_binary() {
        // Test that NoVal is properly propagated through binary operators
        let x: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(1),
            PartialStreamValue::NoVal,
            PartialStreamValue::Known(3),
        ]));
        let y: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(2),
            PartialStreamValue::Known(4),
            PartialStreamValue::Known(5),
        ]));
        let res: Vec<PartialStreamValue<i64>> = plus(x, y).collect().await;
        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Known(3),
            PartialStreamValue::Known(5), // NoVal repeats last value (1), 1+4=5
            PartialStreamValue::Known(8),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_noval_propagation_unary() {
        // Test that NoVal is properly propagated through unary operators
        let x: OutputStream<PartialStreamValue<bool>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::NoVal,
            PartialStreamValue::Known(false),
        ]));
        let res: Vec<PartialStreamValue<bool>> = not(x).collect().await;
        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::NoVal, // NoVal passes through unary operators directly
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_deferred_propagation() {
        // Test that Deferred is properly propagated through binary operators
        let x: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(1),
            PartialStreamValue::Deferred,
            PartialStreamValue::Known(3),
        ]));
        let y: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known(2),
            PartialStreamValue::Known(4),
            PartialStreamValue::Known(5),
        ]));
        let res: Vec<PartialStreamValue<i64>> = plus(x, y).collect().await;
        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Known(3),
            PartialStreamValue::Deferred, // Deferred propagates
            PartialStreamValue::Known(8),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_dynamic(executor: Rc<LocalExecutor<'static>>) {
        let e: OutputStream<PartialStreamValue<String>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known("x + 1".into()),
            PartialStreamValue::Known("x + 2".into()),
        ]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let type_ctx: TypeContext = BTreeMap::from([("x".into(), StreamType::Int)]);
        let res_stream = dynamic::<TypedTestConfig, Parser, i64>(&ctx, e, None, 10, &type_ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;
        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(2), PartialStreamValue::Known(4)];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_dynamic_x_squared(executor: Rc<LocalExecutor<'static>>) {
        let e: OutputStream<PartialStreamValue<String>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known("x * x".into()),
            PartialStreamValue::Known("x * x".into()),
        ]));
        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let type_ctx: TypeContext = BTreeMap::from([("x".into(), StreamType::Int)]);
        let res_stream = dynamic::<TypedTestConfig, Parser, i64>(&ctx, e, None, 10, &type_ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;
        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(4), PartialStreamValue::Known(9)];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_dynamic_with_start_deferred(executor: Rc<LocalExecutor<'static>>) {
        let e: OutputStream<PartialStreamValue<String>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Deferred,
            PartialStreamValue::Known("x + 1".into()),
            PartialStreamValue::Known("x + 2".into()),
        ]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let type_ctx: TypeContext = BTreeMap::from([("x".into(), StreamType::Int)]);
        let res_stream = dynamic::<TypedTestConfig, Parser, i64>(&ctx, e, None, 10, &type_ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;
        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Deferred,
            PartialStreamValue::Known(3),
            PartialStreamValue::Known(5),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_dynamic_with_mid_deferred(executor: Rc<LocalExecutor<'static>>) {
        let e: OutputStream<PartialStreamValue<String>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known("x + 1".into()),
            PartialStreamValue::Deferred,
            PartialStreamValue::Known("x + 2".into()),
        ]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let type_ctx: TypeContext = BTreeMap::from([("x".into(), StreamType::Int)]);
        let res_stream = dynamic::<TypedTestConfig, Parser, i64>(&ctx, e, None, 10, &type_ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;
        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Known(2),
            PartialStreamValue::Deferred,
            PartialStreamValue::Known(5),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_dynamic_noval_start(executor: Rc<LocalExecutor<'static>>) {
        let e: OutputStream<PartialStreamValue<String>> = Box::pin(stream::iter([
            PartialStreamValue::NoVal,
            PartialStreamValue::NoVal,
        ]));
        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 1);
        let type_ctx: TypeContext = BTreeMap::from([("x".into(), StreamType::Int)]);
        let res_stream = dynamic::<TypedTestConfig, Parser, i64>(&ctx, e, None, 1, &type_ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;
        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::NoVal, PartialStreamValue::NoVal];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_noval_middle(executor: Rc<LocalExecutor<'static>>) {
        let e: OutputStream<PartialStreamValue<String>> = Box::pin(stream::iter([
            PartialStreamValue::Known("x + 1".into()),
            PartialStreamValue::NoVal,
            PartialStreamValue::Known("42".into()),
        ]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 1);
        let type_ctx: TypeContext = BTreeMap::from([("x".into(), StreamType::Int)]);
        let res_stream = dynamic::<TypedTestConfig, Parser, i64>(&ctx, e, None, 1, &type_ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;
        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Known(2),
            PartialStreamValue::Known(3),
            PartialStreamValue::Known(42),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_deferred_sticky(executor: Rc<LocalExecutor<'static>>) {
        let e: OutputStream<PartialStreamValue<String>> = Box::pin(stream::iter([
            PartialStreamValue::NoVal,
            PartialStreamValue::Deferred,
            PartialStreamValue::NoVal,
            PartialStreamValue::Known("x".into()),
            PartialStreamValue::Deferred,
            PartialStreamValue::NoVal,
        ]));
        let x = Box::pin(stream::iter(vec![
            1.into(),
            2.into(),
            3.into(),
            4.into(),
            5.into(),
            6.into(),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 1);
        let type_ctx: TypeContext = BTreeMap::from([("x".into(), StreamType::Int)]);
        let res_stream = dynamic::<TypedTestConfig, Parser, i64>(&ctx, e, None, 1, &type_ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;
        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::NoVal,
            PartialStreamValue::Deferred,
            PartialStreamValue::Deferred,
            PartialStreamValue::Known(4),
            PartialStreamValue::Deferred,
            PartialStreamValue::Deferred,
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_history_indexing(executor: Rc<LocalExecutor<'static>>) {
        let e: OutputStream<PartialStreamValue<String>> = Box::pin(stream::iter([
            PartialStreamValue::NoVal,
            PartialStreamValue::Known("x[1]".into()),
            PartialStreamValue::NoVal,
            PartialStreamValue::Known("x".into()),
            PartialStreamValue::Deferred,
            PartialStreamValue::Known("x[1]".into()),
            PartialStreamValue::NoVal,
        ]));
        let x = Box::pin(stream::iter(vec![
            1.into(),
            2.into(),
            3.into(),
            4.into(),
            5.into(),
            6.into(),
            7.into(),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 1);
        let type_ctx: TypeContext = BTreeMap::from([("x".into(), StreamType::Int)]);
        let res_stream = dynamic::<TypedTestConfig, Parser, i64>(&ctx, e, None, 1, &type_ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;
        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::NoVal,
            PartialStreamValue::Deferred,
            PartialStreamValue::Known(2),
            PartialStreamValue::Known(4),
            PartialStreamValue::Deferred,
            PartialStreamValue::Deferred,
            PartialStreamValue::Known(6),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer(executor: Rc<LocalExecutor<'static>>) {
        let e: OutputStream<PartialStreamValue<String>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known("x + 1".into()),
            PartialStreamValue::Known("x + 2".into()),
        ]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let type_ctx: TypeContext = BTreeMap::from([("x".into(), StreamType::Int)]);
        let res_stream = defer::<TypedTestConfig, Parser, i64>(&ctx, e, 2, &type_ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;
        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(2), PartialStreamValue::Known(3)];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_defer_x_squared(executor: Rc<LocalExecutor<'static>>) {
        let e: OutputStream<PartialStreamValue<String>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known("x * x".into()),
            PartialStreamValue::Known("x * x + 1".into()),
        ]));
        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let type_ctx: TypeContext = BTreeMap::from([("x".into(), StreamType::Int)]);
        let res_stream = defer::<TypedTestConfig, Parser, i64>(&ctx, e, 10, &type_ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;
        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(4), PartialStreamValue::Known(9)];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_defer_deferred(executor: Rc<LocalExecutor<'static>>) {
        let e: OutputStream<PartialStreamValue<String>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Deferred,
            PartialStreamValue::Known("x + 1".into()),
        ]));
        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let type_ctx: TypeContext = BTreeMap::from([("x".into(), StreamType::Int)]);
        let res_stream = defer::<TypedTestConfig, Parser, i64>(&ctx, e, 10, &type_ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;
        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Deferred, PartialStreamValue::Known(4)];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_defer_deferred2(executor: Rc<LocalExecutor<'static>>) {
        let e = Box::pin(stream::iter(vec![
            PartialStreamValue::Deferred,
            PartialStreamValue::Known("x + 1".into()),
            PartialStreamValue::Deferred,
            PartialStreamValue::Deferred,
        ])) as OutputStream<PartialStreamValue<String>>;
        let x = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into(), 4.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let type_ctx: TypeContext = BTreeMap::from([("x".into(), StreamType::Int)]);
        let res_stream = defer::<TypedTestConfig, Parser, i64>(&ctx, e, 10, &type_ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;
        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Deferred,
            PartialStreamValue::Known(3),
            PartialStreamValue::Known(4),
            PartialStreamValue::Known(5),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_defer_only_deferred(executor: Rc<LocalExecutor<'static>>) {
        let e: OutputStream<PartialStreamValue<String>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Deferred,
            PartialStreamValue::Deferred,
        ]));
        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let type_ctx: TypeContext = BTreeMap::from([("x".into(), StreamType::Int)]);
        let res_stream = defer::<TypedTestConfig, Parser, i64>(&ctx, e, 10, &type_ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;
        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Deferred, PartialStreamValue::Deferred];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_defer_noval_start(executor: Rc<LocalExecutor<'static>>) {
        let e: OutputStream<PartialStreamValue<String>> = Box::pin(stream::iter([
            PartialStreamValue::NoVal,
            PartialStreamValue::NoVal,
        ]));
        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 1);
        let type_ctx: TypeContext = BTreeMap::from([("x".into(), StreamType::Int)]);
        let res_stream = defer::<TypedTestConfig, Parser, i64>(&ctx, e, 1, &type_ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;
        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::NoVal, PartialStreamValue::NoVal];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_noval_middle(executor: Rc<LocalExecutor<'static>>) {
        let e: OutputStream<PartialStreamValue<String>> = Box::pin(stream::iter([
            PartialStreamValue::Known("x + 1".into()),
            PartialStreamValue::NoVal,
            PartialStreamValue::Known("42".into()),
        ]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 1);
        let type_ctx: TypeContext = BTreeMap::from([("x".into(), StreamType::Int)]);
        let res_stream = defer::<TypedTestConfig, Parser, i64>(&ctx, e, 1, &type_ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;
        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Known(2),
            PartialStreamValue::Known(3),
            PartialStreamValue::Known(4),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_deferred_sticky(executor: Rc<LocalExecutor<'static>>) {
        let e: OutputStream<PartialStreamValue<String>> = Box::pin(stream::iter([
            PartialStreamValue::NoVal,
            PartialStreamValue::Deferred,
            PartialStreamValue::NoVal,
            PartialStreamValue::Known("x".into()),
            PartialStreamValue::Deferred,
            PartialStreamValue::NoVal,
        ]));
        let x = Box::pin(stream::iter(vec![
            1.into(),
            2.into(),
            3.into(),
            4.into(),
            5.into(),
            6.into(),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 1);
        let type_ctx: TypeContext = BTreeMap::from([("x".into(), StreamType::Int)]);
        let res_stream = defer::<TypedTestConfig, Parser, i64>(&ctx, e, 1, &type_ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;
        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::NoVal,
            PartialStreamValue::Deferred,
            PartialStreamValue::Deferred,
            PartialStreamValue::Known(4),
            PartialStreamValue::Known(5),
            PartialStreamValue::Known(6),
        ];
        assert_eq!(res, exp);
    }
}
