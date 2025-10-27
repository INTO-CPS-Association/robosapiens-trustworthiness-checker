use crate::SExpr;
use crate::core::StreamData;
use crate::core::Value;
use crate::lang::core::parser::ExprParser;
use crate::semantics::untimed_untyped_lola::semantics::UntimedLolaSemantics;
use crate::semantics::{MonitoringSemantics, StreamContext};
use crate::{OutputStream, VarName};
use async_stream::stream;
use core::panic;
use ecow::EcoString;
use ecow::EcoVec;
use futures::join;
use futures::stream::LocalBoxStream;
use futures::{
    StreamExt,
    future::join_all,
    stream::{self},
};
use std::collections::BTreeMap;
use tracing::debug;
use tracing::info;
use tracing::instrument;
use tracing::warn;

pub trait CloneFn1<T: StreamData, S: StreamData>: Fn(T) -> S + Clone + 'static {}
impl<T, S: StreamData, R: StreamData> CloneFn1<S, R> for T where T: Fn(S) -> R + Clone + 'static {}

fn stream_lift_base(mut x_mon: OutputStream<Value>) -> OutputStream<Value> {
    Box::pin(stream! {
        let mut last : Option<Value>  = None;
        while let Some(curr) = x_mon.next().await {
            match curr {
                Value::NoVal => {
                    if let Some(last) = &last {
                        yield last.clone();
                    } else {
                        // Only happens when the first value is NoVal
                        yield Value::NoVal;
                    }
                }
                _ => {
                    last = Some(curr.clone());
                    yield curr;
                }
            }
        }
    })
}

pub fn stream_lift1(
    f: impl CloneFn1<Value, Value>,
    x_mon: OutputStream<Value>,
) -> OutputStream<Value> {
    Box::pin(stream_lift_base(x_mon).map(move |x| {
        if x == Value::NoVal {
            Value::NoVal
        } else {
            f(x)
        }
    }))
}

pub trait CloneFn2<S: StreamData, R: StreamData, U: StreamData>:
    Fn(S, R) -> U + Clone + 'static
{
}
impl<T, S: StreamData, R: StreamData, U: StreamData> CloneFn2<S, R, U> for T where
    T: Fn(S, R) -> U + Clone + 'static
{
}

pub fn stream_lift2(
    f: impl CloneFn2<Value, Value, Value>,
    x_mon: OutputStream<Value>,
    y_mon: OutputStream<Value>,
) -> OutputStream<Value> {
    Box::pin(
        stream_lift_base(x_mon)
            .zip(stream_lift_base(y_mon))
            .map(move |(x, y)| {
                if x == Value::NoVal || y == Value::NoVal {
                    Value::NoVal
                } else {
                    f(x, y)
                }
            }),
    )
}

pub trait CloneFn3<S: StreamData, R: StreamData, U: StreamData, V: StreamData>:
    Fn(S, R, U) -> V + Clone + 'static
{
}
impl<T, S: StreamData, R: StreamData, U: StreamData, V: StreamData> CloneFn3<S, R, U, V> for T where
    T: Fn(S, R, U) -> V + Clone + 'static
{
}

pub fn stream_lift3(
    f: impl CloneFn3<Value, Value, Value, Value>,
    x_mon: OutputStream<Value>,
    y_mon: OutputStream<Value>,
    z_mon: OutputStream<Value>,
) -> OutputStream<Value> {
    Box::pin(
        stream_lift_base(x_mon)
            .zip(stream_lift_base(y_mon))
            .zip(stream_lift_base(z_mon))
            .map(move |((x, y), z)| {
                if x == Value::NoVal || y == Value::NoVal || z == Value::NoVal {
                    Value::NoVal
                } else {
                    f(x, y, z)
                }
            }),
    )
}

pub fn and(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(
        |x, y| Value::Bool(x == Value::Bool(true) && y == Value::Bool(true)),
        x,
        y,
    )
}

pub fn or(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(
        |x, y| Value::Bool(x == Value::Bool(true) || y == Value::Bool(true)),
        x,
        y,
    )
}

pub fn not(x: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift1(|x| Value::Bool(x == Value::Bool(false)), x)
}

pub fn eq(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(|x, y| Value::Bool(x == y), x, y)
}

pub fn le(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(
        |x, y| match (x, y) {
            (Value::Int(x), Value::Int(y)) => Value::Bool(x <= y),
            (Value::Int(a), Value::Float(b)) => Value::Bool(a as f64 <= b),
            (Value::Float(a), Value::Int(b)) => Value::Bool(a <= b as f64),
            (Value::Float(a), Value::Float(b)) => Value::Bool(a <= b),
            (Value::Bool(a), Value::Bool(b)) => Value::Bool(a <= b),
            (Value::Str(a), Value::Str(b)) => Value::Bool(a <= b),
            (Value::Int(_), Value::Deferred)
            | (Value::Float(_), Value::Deferred)
            | (Value::Str(_), Value::Deferred)
            | (Value::Bool(_), Value::Deferred)
            | (Value::Deferred, Value::Int(_))
            | (Value::Deferred, Value::Float(_))
            | (Value::Deferred, Value::Str(_))
            | (Value::Deferred, Value::Bool(_))
            | (Value::Deferred, Value::Deferred) => Value::Deferred,
            (x, y) => panic!("Invalid comparison with types: {:?}, {:?}", x, y),
        },
        x,
        y,
    )
}

pub fn lt(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(
        |x, y| match (x, y) {
            (Value::Int(x), Value::Int(y)) => Value::Bool(x < y),
            (Value::Int(a), Value::Float(b)) => Value::Bool((a as f64) < b),
            (Value::Float(a), Value::Int(b)) => Value::Bool(a < b as f64),
            (Value::Float(x), Value::Float(y)) => Value::Bool(x < y),
            (Value::Bool(a), Value::Bool(b)) => Value::Bool(!a & b),
            (Value::Str(a), Value::Str(b)) => Value::Bool(a < b),
            (Value::Int(_), Value::Deferred)
            | (Value::Float(_), Value::Deferred)
            | (Value::Str(_), Value::Deferred)
            | (Value::Bool(_), Value::Deferred)
            | (Value::Deferred, Value::Int(_))
            | (Value::Deferred, Value::Float(_))
            | (Value::Deferred, Value::Str(_))
            | (Value::Deferred, Value::Bool(_))
            | (Value::Deferred, Value::Deferred) => Value::Deferred,
            (x, y) => panic!("Invalid comparison with types: {:?}, {:?}", x, y),
        },
        x,
        y,
    )
}

pub fn ge(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(
        |x, y| match (x, y) {
            (Value::Int(x), Value::Int(y)) => Value::Bool(x >= y),
            (Value::Int(a), Value::Float(b)) => Value::Bool(a as f64 >= b),
            (Value::Float(a), Value::Int(b)) => Value::Bool(a > b as f64),
            (Value::Float(x), Value::Float(y)) => Value::Bool(x >= y),
            (Value::Bool(a), Value::Bool(b)) => Value::Bool(a >= b),
            (Value::Str(a), Value::Str(b)) => Value::Bool(a >= b),
            (Value::Int(_), Value::Deferred)
            | (Value::Float(_), Value::Deferred)
            | (Value::Str(_), Value::Deferred)
            | (Value::Bool(_), Value::Deferred)
            | (Value::Deferred, Value::Int(_))
            | (Value::Deferred, Value::Float(_))
            | (Value::Deferred, Value::Str(_))
            | (Value::Deferred, Value::Bool(_))
            | (Value::Deferred, Value::Deferred) => Value::Deferred,
            (x, y) => panic!("Invalid comparison with types: {:?}, {:?}", x, y),
        },
        x,
        y,
    )
}

pub fn gt(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(
        |x, y| match (x, y) {
            (Value::Int(x), Value::Int(y)) => Value::Bool(x > y),
            (Value::Int(a), Value::Float(b)) => Value::Bool((a as f64) > b),
            (Value::Float(a), Value::Int(b)) => Value::Bool(a > b as f64),
            (Value::Float(x), Value::Float(y)) => Value::Bool(x > y),
            (Value::Bool(a), Value::Bool(b)) => Value::Bool(a & !b),
            (Value::Str(a), Value::Str(b)) => Value::Bool(a > b),
            (Value::Int(_), Value::Deferred)
            | (Value::Float(_), Value::Deferred)
            | (Value::Str(_), Value::Deferred)
            | (Value::Bool(_), Value::Deferred)
            | (Value::Deferred, Value::Int(_))
            | (Value::Deferred, Value::Float(_))
            | (Value::Deferred, Value::Str(_))
            | (Value::Deferred, Value::Bool(_))
            | (Value::Deferred, Value::Deferred) => Value::Deferred,
            (x, y) => panic!("Invalid comparison with types: {:?}, {:?}", x, y),
        },
        x,
        y,
    )
}

pub fn val(x: Value) -> OutputStream<Value> {
    Box::pin(stream::repeat(x.clone()))
}

// Should this return a dyn ConcreteStreamData?
pub fn if_stm(
    x: OutputStream<Value>,
    y: OutputStream<Value>,
    z: OutputStream<Value>,
) -> OutputStream<Value> {
    stream_lift3(
        |x, y, z| match x {
            Value::Bool(true) => y,
            Value::Bool(false) => z,
            x => panic!("Invalid conditional for if statement with type: {:?}", x),
        },
        x,
        y,
        z,
    )
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
//
// TODO: There is a bug here introduced by async SRV.
// The bug is that sindex expressions can never yield NoVal, which is usually
// possible if the first value received in an expression is NoVal.
// Fixing it requires a larger refactor, probably with a special case for dealing with
// recursive sindex expressions. Or alternatively, disallowing recursive definitions in sindex.
//
// First consider the spec/trace:
// in x
// out y
// y = x[1]
// 0: x = NoVal
// 1: x = 42
// 2: x = 43
//
// The correct output here is:
// 0: y = NoVal
// 1: y = Deferred
// 2: y = 42
//
// In order to implement this behavior, we need to yield Deferred for i samples,
// not counting those where x is NoVal. Until x is not NoVal for the first time,
// we yield NoVal. This is not too hard to implement, and the core looks something like:
// let val = x.next().await;
// if val != Value::NoVal {...} else {...}
//
// Notice that it requires looking at x in order to decide what to yield.
//
// Now consider this recursive spec with the same trace:
// out z
// z = default(z[1], 0) + x
//
// If we implement sindex like the pseudo-implementation above, we get a deadlock
// as `z[-1]` needs to look at `z` to decide what to yield, but `z` is waiting for the
// rhs of the assignment to finish.
//
// Potential solution:
// If we knew which variable the expression is assigned to, we could have a
// sindex_rec implementation that is implemented more or less like normal sindex,
// and sindex which is implemented like below.
// (The correct call would need to be evaluated in semantics.rs where the SExpr
// is still available).
pub fn sindex(x: OutputStream<Value>, i: u64) -> OutputStream<Value> {
    fn sindex_base(x: OutputStream<Value>, i: u64) -> OutputStream<Value> {
        if let Ok(i) = usize::try_from(i) {
            let cs = stream::repeat(Value::Deferred).take(i);
            // Delay x by i defers
            Box::pin(cs.chain(x))
        } else {
            panic!("Index too large for sindex operation")
        }
    }
    stream_lift_base(sindex_base(x, i))
}

pub fn plus(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    debug!("Creating plus operation stream");
    stream_lift2(
        |x, y| {
            debug!("Executing plus operation");
            let result = match (x, y) {
                (Value::Int(x), Value::Int(y)) => Value::Int(x + y),
                (Value::Int(x), Value::Float(y)) => Value::Float(x as f64 + y),
                (Value::Float(x), Value::Int(y)) => Value::Float(x + y as f64),
                (Value::Float(x), Value::Float(y)) => Value::Float(x + y),
                (Value::Int(_), Value::Deferred)
                | (Value::Float(_), Value::Deferred)
                | (Value::Deferred, Value::Int(_))
                | (Value::Deferred, Value::Float(_))
                | (Value::Deferred, Value::Deferred) => {
                    debug!("Addition with Deferred value, resulting in Deferred");
                    Value::Deferred
                }
                _ => {
                    panic!("Cannot add incompatible types")
                }
            };
            debug!("Plus operation completed");
            result
        },
        x,
        y,
    )
}

pub fn modulo(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(
        |x, y| match (x, y) {
            (Value::Int(x), Value::Int(y)) => Value::Int(x % y),
            (Value::Int(x), Value::Float(y)) => Value::Float(x as f64 % y),
            (Value::Float(x), Value::Int(y)) => Value::Float(x % y as f64),
            (Value::Float(x), Value::Float(y)) => Value::Float(x % y),
            (Value::Int(_), Value::Deferred)
            | (Value::Float(_), Value::Deferred)
            | (Value::Deferred, Value::Int(_))
            | (Value::Deferred, Value::Float(_))
            | (Value::Deferred, Value::Deferred) => Value::Deferred,
            (x, y) => panic!("Invalid modulo with types: {:?}, {:?}", x, y),
        },
        x,
        y,
    )
}

pub fn minus(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(
        |x, y| match (x, y) {
            (Value::Int(x), Value::Int(y)) => Value::Int(x - y),
            (Value::Int(x), Value::Float(y)) => Value::Float(x as f64 - y),
            (Value::Float(x), Value::Int(y)) => Value::Float(x - y as f64),
            (Value::Float(x), Value::Float(y)) => Value::Float(x - y),
            (Value::Int(_), Value::Deferred)
            | (Value::Float(_), Value::Deferred)
            | (Value::Deferred, Value::Int(_))
            | (Value::Deferred, Value::Float(_))
            | (Value::Deferred, Value::Deferred) => Value::Deferred,
            (x, y) => panic!("Invalid subtraction with types: {:?}, {:?}", x, y),
        },
        x,
        y,
    )
}

pub fn mult(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(
        |x, y| match (x, y) {
            (Value::Int(x), Value::Int(y)) => Value::Int(x * y),
            (Value::Int(x), Value::Float(y)) => Value::Float(x as f64 * y),
            (Value::Float(x), Value::Int(y)) => Value::Float(x * y as f64),
            (Value::Float(x), Value::Float(y)) => Value::Float(x * y),
            (Value::Int(_), Value::Deferred)
            | (Value::Float(_), Value::Deferred)
            | (Value::Deferred, Value::Int(_))
            | (Value::Deferred, Value::Float(_))
            | (Value::Deferred, Value::Deferred) => Value::Deferred,
            (x, y) => panic!("Invalid multiplication with types: {:?}, {:?}", x, y),
        },
        x,
        y,
    )
}

pub fn div(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(
        |x, y| match (x, y) {
            (Value::Int(x), Value::Int(y)) => Value::Int(x / y),
            (Value::Int(x), Value::Float(y)) => Value::Float(x as f64 / y),
            (Value::Float(x), Value::Int(y)) => Value::Float(x / y as f64),
            (Value::Float(x), Value::Float(y)) => Value::Float(x / y),
            (Value::Int(_), Value::Deferred)
            | (Value::Float(_), Value::Deferred)
            | (Value::Deferred, Value::Int(_))
            | (Value::Deferred, Value::Float(_))
            | (Value::Deferred, Value::Deferred) => Value::Deferred,
            (x, y) => panic!("Invalid division with types: {:?}, {:?}", x, y),
        },
        x,
        y,
    )
}

pub fn concat(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(
        |x, y| match (x, y) {
            (Value::Str(x), Value::Str(y)) => {
                // ConcreteStreamData::Str(format!("{x}{y}").into());
                Value::Str(format!("{x}{y}").into())
            }
            (Value::Str(_), Value::Deferred)
            | (Value::Deferred, Value::Str(_))
            | (Value::Deferred, Value::Deferred) => Value::Deferred,
            (x, y) => panic!("Invalid concatenation with types: {:?}, {:?}", x, y),
        },
        x,
        y,
    )
}

pub fn dynamic<Ctx: StreamContext<Value>, Parser>(
    ctx: &Ctx,
    mut eval_stream: OutputStream<Value>,
    vs: Option<EcoVec<VarName>>,
    history_length: usize,
) -> OutputStream<Value>
where
    Parser: ExprParser<SExpr> + 'static,
{
    // Create a subcontext with a history window length
    let mut subcontext = match vs {
        Some(vs) => ctx.restricted_subcontext(vs, history_length),
        None => ctx.subcontext(history_length),
    };

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
            // If we have a previous value and it is the same as the current
            // value or if the current value is deferred (not provided),
            // continue using the existing stream as our output
            if let Some(prev_data) = &mut prev_data {
                if prev_data.eval_val == current || current == Value::Deferred {
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
            match current {
                Value::Deferred => {
                    // Consume a sample from the subcontext but return Deferred
                    subcontext.tick().await;
                    yield Value::Deferred;
                }
                Value::Str(s) => {
                    let expr = Parser::parse(&mut s.as_ref())
                        .expect("Invalid dynamic str");
                    debug!("Dynamic evaluated to expression {:?}", expr);
                    let mut eval_output_stream = UntimedLolaSemantics::<Parser>::to_async_stream(expr, &subcontext);
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

pub fn var(ctx: &impl StreamContext<Value>, x: VarName) -> OutputStream<Value> {
    debug!("Accessing variable");
    match ctx.var(&x) {
        Some(stream) => {
            debug!("Found variable");
            stream
        }
        None => {
            debug!("Variable not found - this will panic");
            panic!("Variable \"{}\" not found", x)
        }
    }
}

// Defer for an UntimedLolaExpression using the lola_expression parser
#[instrument(skip(ctx, prop_stream))]
pub fn defer<Parser>(
    ctx: &impl StreamContext<Value>,
    mut prop_stream: OutputStream<Value>,
    history_length: usize,
) -> OutputStream<Value>
where
    Parser: ExprParser<SExpr> + 'static,
{
    /* Subcontext with current values only*/
    let mut subcontext = ctx.subcontext(history_length);
    // let mut prog = subcontext.progress_sender.subscriber();

    Box::pin(stream! {
        let mut eval_output_stream: Option<OutputStream<Value>> = None;
        let mut i = 0;

        // Yield Deferred until we have a value to evaluate, then evaluate it
        while let Some(current) = prop_stream.next().await {
            debug!(?i, ?current, "Defer");
            match current {
                Value::Str(defer_s) => {
                    // We have a string to evaluate so do so
                    let expr = Parser::parse(&mut defer_s.as_ref())
                        .expect("Invalid dynamic str");
                    eval_output_stream = Some(UntimedLolaSemantics::<Parser>::to_async_stream(expr, &subcontext));
                    debug!(s = ?defer_s.as_ref(), "Evaluated defer string");
                    subcontext.run().await;
                    break;
                }
                Value::Deferred => {
                    // Consume a sample from the subcontext but return Deferred
                    info!("Defer waiting on deferred");
                    if i >= history_length {
                        info!(?i, ?history_length, "Advancing subcontext to clean history");
                        subcontext.tick().await;
                    }
                    i += 1;
                    yield Value::Deferred;
                }
                _ => panic!("Invalid defer property type {:?}", current)
            }
        }

        // This is None if the prop_stream is done but we never received a property
        if let Some(eval_output_stream) = eval_output_stream {
            // Wind forward the stream to the current time
            let time_progressed = i.min(history_length);
            debug!(?i, ?time_progressed, ?history_length, "Time progressed");
            // subcontext.tick();
            let mut eval_output_stream = eval_output_stream.skip(time_progressed);

            // Yield the saved value until the inner stream is done
            while let Some(eval_res) = eval_output_stream.next().await {
                yield eval_res;
            }
        }
    })
}

// Evaluates to the l.h.s. until the r.h.s. provides a value.
// Then continues evaluating the r.h.s. (even if it provides Deferred)
pub fn update(mut x: OutputStream<Value>, mut y: OutputStream<Value>) -> OutputStream<Value> {
    Box::pin(stream! {
        while let (Some(x_val), Some(y_val)) = join!(x.next(), y.next()) {
            match (x_val, y_val) {
                (x_val, Value::Deferred) => {
                    yield x_val;
                }
                (_, y_val) => {
                    yield y_val;
                    break;
                }
            }
        }
        while let Some(y_val) = y.next().await {
            yield y_val;
        }
    })
}

// Evaluates to a placeholder stream whenever Deferred is received.
pub fn default(x: OutputStream<Value>, d: OutputStream<Value>) -> OutputStream<Value> {
    let xs = x
        .zip(d)
        .map(|(x, d)| if x == Value::Deferred { d } else { x });
    Box::pin(xs) as LocalBoxStream<'static, Value>
}

pub fn is_defined(x: OutputStream<Value>) -> OutputStream<Value> {
    Box::pin(x.map(|x| Value::Bool(x != Value::Deferred)))
}

// Could also be implemented with is_defined but I think this is more efficient
pub fn when(mut x: OutputStream<Value>) -> OutputStream<Value> {
    Box::pin(stream! {
        while let Some(x_val) = x.next().await {
            if x_val == Value::Deferred {
                yield Value::Bool(false);
            } else {
                yield Value::Bool(true);
                break;
            }
        }
        while x.next().await.is_some() {
            yield Value::Bool(true);
        }
    })
}

pub fn list(mut xs: Vec<OutputStream<Value>>) -> OutputStream<Value> {
    Box::pin(stream! {
        if xs.is_empty() {
            Value::List(EcoVec::new());
        }
        loop {
            let vals = join_all(xs.iter_mut().map(|x| x.next())).await;
            if vals.iter().all(|x| x.is_some()) {
                yield Value::List(vals.iter().map(|x| x.clone().unwrap()).collect());
            } else {
                return;
            }
        }
    })
}

pub fn lindex(mut x: OutputStream<Value>, mut i: OutputStream<Value>) -> OutputStream<Value> {
    Box::pin(stream! {
        while let (Some(l), Some(idx)) = join!(x.next(), i.next()){
            match (l, idx) {
                (Value::List(l), Value::Int(idx)) => {
                    if idx >= 0 {
                        if let Some(val) = l.get(idx as usize) {
                            yield val.clone();
                        } else {
                            panic!("List index out of bounds: {}", idx);
                        }
                    }
                    else {
                        panic!("List index must be non-negative: {}", idx); // For now
                    }
                }
                (l, idx) => panic!("Invalid list index. Expected List and Int expressions. Received: List.get({:?}, {:?})", l, idx)
            }
        }
    })
}

pub fn lappend(mut x: OutputStream<Value>, mut y: OutputStream<Value>) -> OutputStream<Value> {
    Box::pin(stream! {
        while let (Some(l), Some(val)) = join!(x.next(), y.next()){
            match l {
                Value::List(mut l) => {
                    l.push(val);
                    yield Value::List(l);
                }
                l => panic!("Invalid list append. Expected List and Value expressions. Received: List.append({:?}, {:?})", l, val)
            }
        }
    })
}

pub fn lconcat(mut x: OutputStream<Value>, mut y: OutputStream<Value>) -> OutputStream<Value> {
    Box::pin(stream! {
        while let (Some(l1), Some(l2)) = join!(x.next(), y.next()){
            match (l1, l2) {
                (Value::List(mut l1), Value::List(l2)) => {
                    l1.extend(l2);
                    yield Value::List(l1);
                }
                (l1, l2) => panic!("Invalid list concatenation. Expected List and List expressions. Received: List.concat({:?}, {:?})", l1, l2)
            }
        }
    })
}

pub fn lhead(mut x: OutputStream<Value>) -> OutputStream<Value> {
    Box::pin(stream! {
        while let Some(l) = x.next().await {
            match l {
                Value::List(l) => {
                    if let Some(val) = l.first() {
                        yield val.clone();
                    } else {
                        panic!("List is empty");
                    }
                }
                l => panic!("Invalid list head. Expected List expression. Received: List.head({:?})", l)
            }
        }
    })
}

pub fn ltail(mut x: OutputStream<Value>) -> OutputStream<Value> {
    Box::pin(stream! {
        while let Some(l) = x.next().await {
            match l {
                Value::List(l) => {
                    if let Some(val) = l.get(1..) {
                        yield Value::List(val.into());
                    } else {
                        panic!("List is empty");
                    }
                }
                l => panic!("Invalid list tail. Expected List expression. Received: List.tail({:?})", l)
            }
        }
    })
}

pub fn llen(mut x: OutputStream<Value>) -> OutputStream<Value> {
    Box::pin(stream! {
        while let Some(l) = x.next().await {
            match l {
                Value::List(l) => {
                    yield Value::Int(l.len() as i64);
                }
                l => panic!("Invalid list len. Expected List expression. Received: List.len({:?})", l)
            }
        }
    })
}

pub fn map(mut xs: BTreeMap<EcoString, OutputStream<Value>>) -> OutputStream<Value> {
    Box::pin(stream! {
        if xs.is_empty() {
            Value::Map(BTreeMap::new());
        }
        loop {
            let iters = xs.iter_mut().map(|(k, v)| {
                // We need to clone the key because we are moving it into the closure
                let k = k.clone();
                async move {
                    let v = v.next().await;
                    (k, v)
                }
            });
            let k_v = join_all(iters).await;
            if k_v.iter().all(|(_, v)| v.is_some()) {
                yield Value::Map(k_v.iter().map(|(k, v)| (k.clone(), v.clone().unwrap())).collect());
            } else {
                return;
            }
        }
    })
}

pub fn mget(mut xs: OutputStream<Value>, k: EcoString) -> OutputStream<Value> {
    Box::pin(stream! {
        while let Some(val) = xs.next().await {
            match val {
                Value::Map(map) => {
                        if let Some(val) = map.get(&k) {
                            yield val.clone();
                        } else {
                            panic!("Missing key for map get: {}", k);
                        }
                }
                v => panic!("Invalid map get. Expected Map expression. Received: Map.get({:?})", v)
            }
        }
    })
}

pub fn mremove(mut xs: OutputStream<Value>, k: EcoString) -> OutputStream<Value> {
    Box::pin(stream! {
        while let Some(val) = xs.next().await {
            match val {
                Value::Map(mut map) => {
                        map.remove(&k);
                        yield Value::Map(map);
                }
                v => panic!("Invalid map remove. Expected Map expression. Received: Map.remove({:?})", v)
            }
        }
    })
}

pub fn minsert(
    mut xs: OutputStream<Value>,
    k: EcoString,
    mut v: OutputStream<Value>,
) -> OutputStream<Value> {
    Box::pin(stream! {
        while let (Some(m_val), Some(val)) = join!(xs.next(), v.next()) {
            match m_val {
                Value::Map(mut map) => {
                    map.insert(k.clone(), val);
                    yield Value::Map(map);
                }
                v => panic!("Invalid map insert. Expected Map expression. Received: Map.insert({:?})", v)
            }
        }
    })
}

pub fn mhas_key(mut xs: OutputStream<Value>, k: EcoString) -> OutputStream<Value> {
    Box::pin(stream! {
        while let Some(val) = xs.next().await {
            match val {
                Value::Map(map) => {
                        yield Value::Bool(map.contains_key(&k));
                }
                v => panic!("Invalid map has_key. Expected Map expression. Received: Map.has_key({:?})", v)
            }
        }
    })
}

pub fn sin(v: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift1(
        |v| match v {
            Value::Float(v) => v.sin().into(),
            Value::Deferred => Value::Deferred,
            _ => panic!("Invalid type of angle input stream"),
        },
        v,
    )
}

pub fn cos(v: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift1(
        |v| match v {
            Value::Float(v) => v.cos().into(),
            Value::Deferred => Value::Deferred,
            _ => panic!("Invalid type of angle input stream"),
        },
        v,
    )
}

pub fn tan(v: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift1(
        |v| match v {
            Value::Float(v) => v.tan().into(),
            Value::Deferred => Value::Deferred,
            _ => panic!("Invalid type of angle input stream"),
        },
        v,
    )
}

pub fn abs(v: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift1(
        |v| match v {
            Value::Int(v) => v.abs().into(),
            Value::Float(v) => v.abs().into(),
            Value::Deferred => Value::Deferred,
            x => panic!("Invalid abs with type: {:?}", x),
        },
        v,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::async_test;
    use crate::core::Value;
    use crate::runtime::asynchronous::Context;
    use ecow::eco_vec;
    use futures::stream;
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::rc::Rc;

    type Parser = crate::lang::dynamic_lola::lalr_parser::LALRExprParser;

    #[apply(async_test)]
    async fn test_not() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![Value::Bool(true), false.into()]));
        let res: Vec<Value> = not(x).collect().await;
        let exp: Vec<Value> = vec![Value::Bool(false), Value::Bool(true)];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_plus() {
        let x: OutputStream<Value> =
            Box::pin(stream::iter(vec![Value::Int(1), 3.into()].into_iter()));
        let y: OutputStream<Value> = Box::pin(stream::iter(vec![2.into(), 4.into()].into_iter()));
        let z: Vec<Value> = vec![3.into(), 7.into()];
        let res: Vec<Value> = plus(x, y).collect().await;
        assert_eq!(res, z);
    }

    #[apply(async_test)]
    async fn test_str_concat() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec!["hello ".into(), "olleh ".into()]));
        let y: OutputStream<Value> = Box::pin(stream::iter(vec!["world".into(), "dlrow".into()]));
        let exp = vec!["hello world".into(), "olleh dlrow".into()];
        let res: Vec<Value> = concat(x, y).collect().await;
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_dynamic(executor: Rc<LocalExecutor<'static>>) {
        let e: OutputStream<Value> = Box::pin(stream::iter(vec!["x + 1".into(), "x + 2".into()]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let mut ctx = Context::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let res_stream = dynamic::<_, Parser>(&ctx, e, None, 10);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;
        let exp: Vec<Value> = vec![2.into(), 4.into()];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_dynamic_x_squared(executor: Rc<LocalExecutor<'static>>) {
        // This test is interesting since we use x twice in the dynamic strings
        let e: OutputStream<Value> = Box::pin(stream::iter(vec!["x * x".into(), "x * x".into()]));
        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = Context::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let res_stream = dynamic::<_, Parser>(&ctx, e, None, 10);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;
        let exp: Vec<Value> = vec![4.into(), 9.into()];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_dynamic_with_start_deferred(executor: Rc<LocalExecutor<'static>>) {
        let e: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Deferred,
            "x + 1".into(),
            "x + 2".into(),
        ]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let mut ctx = Context::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let res_stream = dynamic::<_, Parser>(&ctx, e, None, 10);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;
        // Continues evaluating to x+1 until we get a non-deferred value
        let exp: Vec<Value> = vec![Value::Deferred, 3.into(), 5.into()];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_dynamic_with_mid_deferred(executor: Rc<LocalExecutor<'static>>) {
        let e: OutputStream<Value> = Box::pin(stream::iter(vec![
            "x + 1".into(),
            Value::Deferred,
            "x + 2".into(),
        ]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let mut ctx = Context::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let res_stream = dynamic::<_, Parser>(&ctx, e, None, 10);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;
        // Continues evaluating to x+1 until we get a non-deferred value
        let exp: Vec<Value> = vec![2.into(), 3.into(), 5.into()];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_defer(executor: Rc<LocalExecutor<'static>>) {
        // Notice that even though we first say "x + 1", "x + 2", it continues evaluating "x + 1"
        let e: OutputStream<Value> = Box::pin(stream::iter(vec!["x + 1".into(), "x + 2".into()]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let mut ctx = Context::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let res_stream = defer::<Parser>(&ctx, e, 2);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;
        let exp: Vec<Value> = vec![2.into(), 3.into()];
        assert_eq!(res, exp)
    }
    #[apply(async_test)]
    async fn test_defer_x_squared(executor: Rc<LocalExecutor<'static>>) {
        // This test is interesting since we use x twice in the dynamic strings
        let e: OutputStream<Value> =
            Box::pin(stream::iter(vec!["x * x".into(), "x * x + 1".into()]));
        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = Context::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let res_stream = defer::<Parser>(&ctx, e, 10);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;
        let exp: Vec<Value> = vec![4.into(), 9.into()];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_defer_deferred(executor: Rc<LocalExecutor<'static>>) {
        // Using deferred to represent no data on the stream
        let e: OutputStream<Value> = Box::pin(stream::iter(vec![Value::Deferred, "x + 1".into()]));
        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = Context::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let res_stream = defer::<Parser>(&ctx, e, 10);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;
        let exp: Vec<Value> = vec![Value::Deferred, 4.into()];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_defer_deferred2(executor: Rc<LocalExecutor<'static>>) {
        // Deferred followed by property followed by deferred returns [U; val; val].
        let e = Box::pin(stream::iter(vec![
            Value::Deferred,
            "x + 1".into(),
            Value::Deferred,
            Value::Deferred,
        ])) as OutputStream<Value>;
        let x = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into(), 4.into()]));
        let mut ctx = Context::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let res_stream = defer::<Parser>(&ctx, e, 10);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;
        let exp: Vec<Value> = vec![Value::Deferred, 3.into(), 4.into(), 5.into()];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_defer_only_deferred(executor: Rc<LocalExecutor<'static>>) {
        // Using deferred to represent no data on the stream
        let e: OutputStream<Value> = Box::pin(stream::iter(vec![Value::Deferred, Value::Deferred]));
        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = Context::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let res_stream = defer::<Parser>(&ctx, e, 10);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;
        let exp: Vec<Value> = vec![Value::Deferred, Value::Deferred];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_update_both_init() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec!["x0".into(), "x1".into()]));
        let y: OutputStream<Value> = Box::pin(stream::iter(vec!["y0".into(), "y1".into()]));
        let res: Vec<Value> = update(x, y).collect().await;
        let exp: Vec<Value> = vec!["y0".into(), "y1".into()];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_default_no_deferred() {
        let x: OutputStream<Value> =
            Box::pin(stream::iter(vec!["x0".into(), "x1".into(), "x2".into()]));
        let d: OutputStream<Value> = Box::pin(stream::repeat("d".into()));
        let res: Vec<Value> = default(x, d).collect().await;
        let exp: Vec<Value> = vec!["x0".into(), "x1".into(), "x2".into()];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_default_all_deferred() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Deferred,
            Value::Deferred,
            Value::Deferred,
        ]));
        let d: OutputStream<Value> = Box::pin(stream::repeat("d".into()));
        let res: Vec<Value> = default(x, d).collect().await;
        let exp: Vec<Value> = vec!["d".into(), "d".into(), "d".into()];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_default_one_deferred() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![
            "x0".into(),
            Value::Deferred,
            "x2".into(),
        ]));
        let d: OutputStream<Value> = Box::pin(stream::repeat("d".into()));
        let res: Vec<Value> = default(x, d).collect().await;
        let exp: Vec<Value> = vec!["x0".into(), "d".into(), "x2".into()];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_update_first_x_then_y() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![
            "x0".into(),
            "x1".into(),
            "x2".into(),
            "x3".into(),
        ]));
        let y: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Deferred,
            "y1".into(),
            Value::Deferred,
            "y3".into(),
        ]));
        let res: Vec<Value> = update(x, y).collect().await;
        let exp: Vec<Value> = vec!["x0".into(), "y1".into(), Value::Deferred, "y3".into()];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_update_first_y_then_x() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Deferred,
            "x1".into(),
            Value::Deferred,
            "x3".into(),
        ]));
        let y: OutputStream<Value> = Box::pin(stream::iter(vec![
            "y0".into(),
            "y1".into(),
            "y2".into(),
            "y3".into(),
        ]));
        let res: Vec<Value> = update(x, y).collect().await;
        let exp: Vec<Value> = vec!["y0".into(), "y1".into(), "y2".into(), "y3".into()];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_update_neither() {
        use Value::Deferred;
        let x: OutputStream<Value> =
            Box::pin(stream::iter(vec![Deferred, Deferred, Deferred, Deferred]));
        let y: OutputStream<Value> =
            Box::pin(stream::iter(vec![Deferred, Deferred, Deferred, Deferred]));
        let res: Vec<Value> = update(x, y).collect().await;
        let exp: Vec<Value> = vec![Deferred, Deferred, Deferred, Deferred];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_update_first_x_then_y_value_sync() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Deferred,
            "x0".into(),
            "x1".into(),
            "x2".into(),
            "x3".into(),
        ]));
        let y: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Deferred,
            "y1".into(),
            Value::Deferred,
            "y3".into(),
        ]));
        let res: Vec<Value> = update(x, y).collect().await;
        let exp: Vec<Value> = vec![Value::Deferred, "y1".into(), Value::Deferred, "y3".into()];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_list() {
        let x: Vec<OutputStream<Value>> = vec![
            Box::pin(stream::iter(vec![1.into(), 2.into()])),
            Box::pin(stream::iter(vec![3.into(), 4.into()])),
        ];
        let res: Vec<Value> = list(x).collect().await;
        let exp: Vec<Value> = vec![
            Value::List(vec![1.into(), 3.into()].into()),
            Value::List(vec![2.into(), 4.into()].into()),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_list_no_stream() {
        let x: Vec<OutputStream<Value>> = vec![];
        let res: Vec<Value> = list(x).take(2).collect().await;
        let exp: Vec<Value> = vec![Value::List(eco_vec![]), Value::List(eco_vec![])];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_list_empty_stream() {
        let x: Vec<OutputStream<Value>> = vec![Box::pin(stream::iter(vec![]))];
        let res: Vec<Value> = list(x).collect().await;
        let exp: Vec<Value> = vec![];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_list_stream_sending_empty_lists() {
        // Stream that sends empty list twice
        let s: OutputStream<Value> = Box::pin(stream::repeat(Value::List(eco_vec![])).take(2));
        let x: Vec<OutputStream<Value>> = vec![s];
        let res: Vec<Value> = list(x).collect().await;
        // Expected is a bit hard to grasp. s is sending List([]) but since we are using the list
        // combinator, we get List(List([]))
        let exp: Vec<Value> = vec![
            Value::List(eco_vec![Value::List(eco_vec![])]),
            Value::List(eco_vec![Value::List(eco_vec![])]),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_list_exprs() {
        // Stream sending Lists containing an int and a string
        let x: Vec<OutputStream<Value>> = vec![
            plus(
                Box::pin(stream::iter(vec![1.into(), 2.into()])),
                Box::pin(stream::iter(vec![3.into(), 4.into()])),
            ),
            concat(
                Box::pin(stream::iter(vec!["Hello ".into(), "Goddag ".into()])),
                Box::pin(stream::iter(vec!["World".into(), "Verden".into()])),
            ),
        ];
        let res: Vec<Value> = list(x).collect().await;
        let exp: Vec<Value> = vec![
            Value::List(vec![4.into(), "Hello World".into()].into()),
            Value::List(vec![6.into(), "Goddag Verden".into()].into()),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_list_idx() {
        let x: Vec<OutputStream<Value>> = vec![
            Box::pin(stream::iter(vec![1.into(), 2.into()])),
            Box::pin(stream::iter(vec![3.into(), 4.into()])),
        ];
        let i = val(0.into());
        let res: Vec<Value> = lindex(list(x), i).collect().await;
        let exp: Vec<Value> = vec![1.into(), 2.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_list_idx_varying() {
        let x: Vec<OutputStream<Value>> = vec![
            Box::pin(stream::iter(vec![1.into(), 2.into()])),
            Box::pin(stream::iter(vec![3.into(), 4.into()])),
        ];
        // First idx 0 then idx 1
        let i: OutputStream<Value> = Box::pin(stream::iter(vec![0.into(), 1.into()].into_iter()));
        let res: Vec<Value> = lindex(list(x), i).collect().await;
        let exp: Vec<Value> = vec![1.into(), 4.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_list_idx_expr() {
        let x: Vec<OutputStream<Value>> = vec![
            Box::pin(stream::iter(vec![1.into(), 2.into()])),
            Box::pin(stream::iter(vec![3.into(), 4.into()])),
        ];
        let i: OutputStream<Value> = minus(
            Box::pin(stream::iter(vec![5.into(), 6.into()])),
            Box::pin(stream::iter(vec![5.into(), 5.into()])),
        );
        let res: Vec<Value> = lindex(list(x), i).collect().await;
        let exp: Vec<Value> = vec![1.into(), 4.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_list_idx_var(executor: Rc<LocalExecutor<'static>>) {
        let x: Vec<OutputStream<Value>> = vec![
            Box::pin(stream::iter(vec![1.into(), 2.into()])),
            Box::pin(stream::iter(vec![3.into(), 4.into()])),
        ];
        let i = Box::pin(stream::iter(vec![0.into(), 1.into()]));
        let mut ctx = Context::new(executor.clone(), vec!["i".into()], vec![i], 10);
        let res_stream = lindex(list(x), var(&ctx, "i".into()));
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;
        let exp: Vec<Value> = vec![1.into(), 4.into()];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_list_append() {
        let x: Vec<OutputStream<Value>> = vec![
            Box::pin(stream::iter(vec![1.into(), 2.into()])),
            Box::pin(stream::iter(vec![3.into(), 4.into()])),
        ];
        let y: OutputStream<Value> = Box::pin(stream::iter(vec![5.into(), 6.into()]));
        let res: Vec<Value> = lappend(list(x), y).collect().await;
        let exp: Vec<Value> = vec![
            Value::List(vec![1.into(), 3.into(), 5.into()].into()),
            Value::List(vec![2.into(), 4.into(), 6.into()].into()),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_list_concat() {
        let x: Vec<OutputStream<Value>> = vec![
            Box::pin(stream::iter(vec![1.into(), 2.into()])),
            Box::pin(stream::iter(vec![3.into(), 4.into()])),
        ];
        let y: Vec<OutputStream<Value>> = vec![
            Box::pin(stream::iter(vec![5.into(), 6.into()])),
            Box::pin(stream::iter(vec![7.into(), 8.into()])),
        ];
        let res: Vec<Value> = lconcat(list(x), list(y)).collect().await;
        let exp: Vec<Value> = vec![
            Value::List(vec![1.into(), 3.into(), 5.into(), 7.into()].into()),
            Value::List(vec![2.into(), 4.into(), 6.into(), 8.into()].into()),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_list_head() {
        let x: Vec<OutputStream<Value>> = vec![
            Box::pin(stream::iter(vec![1.into(), 2.into()])),
            Box::pin(stream::iter(vec![3.into(), 4.into()])),
        ];
        let res: Vec<Value> = lhead(list(x)).collect().await;
        let exp: Vec<Value> = vec![Value::Int(1.into()), Value::Int(2.into())];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_list_tail() {
        let x: Vec<OutputStream<Value>> = vec![
            Box::pin(stream::iter(vec![1.into(), 2.into()])),
            Box::pin(stream::iter(vec![3.into(), 4.into()])),
            Box::pin(stream::iter(vec![5.into(), 6.into()])),
        ];
        let res: Vec<Value> = ltail(list(x)).collect().await;
        let exp: Vec<Value> = vec![
            Value::List(vec![3.into(), 5.into()].into()),
            Value::List(vec![4.into(), 6.into()].into()),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_list_tail_one_el() {
        let x: Vec<OutputStream<Value>> = vec![Box::pin(stream::iter(vec![1.into(), 2.into()]))];
        let res: Vec<Value> = ltail(list(x)).collect().await;
        let exp: Vec<Value> = vec![Value::List(vec![].into()), Value::List(vec![].into())];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_list_len() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![
            vec![].into(),
            vec![1.into()].into(),
            vec![2.into(), "hello".into()].into(),
        ]));
        let res: Vec<Value> = llen(x).collect().await;
        let exp: Vec<Value> = vec![0.into(), 1.into(), 2.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_map() {
        let s1: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let s2: OutputStream<Value> = Box::pin(stream::iter(vec![3.into(), 4.into()]));
        let m: BTreeMap<EcoString, OutputStream<Value>> =
            BTreeMap::from([("x".into(), s1), ("y".into(), s2)]);
        let res: Vec<Value> = map(m).collect().await;
        let exp: Vec<Value> = vec![
            Value::Map(BTreeMap::from([
                ("x".into(), 1.into()),
                ("y".into(), 3.into()),
            ])),
            Value::Map(BTreeMap::from([
                ("x".into(), 2.into()),
                ("y".into(), 4.into()),
            ])),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_map_no_stream() {
        let x: BTreeMap<EcoString, OutputStream<Value>> = BTreeMap::new();
        let res: Vec<Value> = map(x).take(2).collect().await;
        let exp: Vec<Value> = vec![Value::Map(BTreeMap::new()), Value::Map(BTreeMap::new())];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_map_empty_stream() {
        let s: OutputStream<Value> = Box::pin(stream::iter(vec![]));
        let x: BTreeMap<EcoString, OutputStream<Value>> = BTreeMap::from([("x".into(), s)]);
        let res: Vec<Value> = map(x).collect().await;
        // No values in the stream generating values, so we get an empty stream even if we have the
        // "x" key
        let exp: Vec<Value> = vec![];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_map_stream_sending_empty_lists() {
        // Stream that sends empty list twice
        let s: OutputStream<Value> = Box::pin(stream::repeat(Value::Map(BTreeMap::new())).take(2));
        let x: BTreeMap<EcoString, OutputStream<Value>> = BTreeMap::from([("x".into(), s)]);
        let res: Vec<Value> = map(x).collect().await;
        // Streams of Maps with single key (x) sending empty maps
        let exp: Vec<Value> = vec![
            BTreeMap::from([("x".into(), BTreeMap::new().into())]).into(),
            BTreeMap::from([("x".into(), BTreeMap::new().into())]).into(),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_map_exprs() {
        let m = BTreeMap::from([
            (
                "x".into(),
                plus(
                    Box::pin(stream::iter(vec![1.into(), 2.into()])),
                    Box::pin(stream::iter(vec![3.into(), 4.into()])),
                ),
            ),
            (
                "y".into(),
                concat(
                    Box::pin(stream::iter(vec!["Hello ".into(), "Goddag ".into()])),
                    Box::pin(stream::iter(vec!["World".into(), "Verden".into()])),
                ),
            ),
        ]);
        let res: Vec<Value> = map(m).collect().await;
        let exp: Vec<Value> = vec![
            Value::Map(BTreeMap::from([
                ("x".into(), 4.into()),
                ("y".into(), "Hello World".into()),
            ])),
            Value::Map(BTreeMap::from([
                ("x".into(), 6.into()),
                ("y".into(), "Goddag Verden".into()),
            ])),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_map_get() {
        let s1: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 3.into()]));
        let s2: OutputStream<Value> = Box::pin(stream::iter(vec![2.into(), 4.into()]));
        let m = BTreeMap::from([(EcoString::from("x"), s1), (EcoString::from("y"), s2)]);
        let res: Vec<Value> = mget(map(m), "y".into()).collect().await;
        let exp: Vec<Value> = vec![2.into(), 4.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_map_remove() {
        let s1: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 3.into()]));
        let s2: OutputStream<Value> = Box::pin(stream::iter(vec![2.into(), 4.into()]));
        let m = BTreeMap::from([(EcoString::from("x"), s1), (EcoString::from("y"), s2)]);
        let res: Vec<Value> = mremove(map(m), "y".into()).collect().await;
        let exp: Vec<Value> = vec![
            Value::Map(BTreeMap::from([("x".into(), 1.into())])),
            Value::Map(BTreeMap::from([("x".into(), 3.into())])),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_map_insert() {
        let s1: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 3.into()]));
        let s2: OutputStream<Value> = Box::pin(stream::iter(vec![2.into(), 4.into()]));
        let m = BTreeMap::from([(EcoString::from("x"), s1)]);
        let res: Vec<Value> = minsert(map(m), "y".into(), s2).collect().await;
        let exp: Vec<Value> = vec![
            Value::Map(BTreeMap::from([
                ("x".into(), 1.into()),
                ("y".into(), 2.into()),
            ])),
            Value::Map(BTreeMap::from([
                ("x".into(), 3.into()),
                ("y".into(), 4.into()),
            ])),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_map_insert_overwrite() {
        let s1: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 4.into()]));
        let s2: OutputStream<Value> = Box::pin(stream::iter(vec![2.into(), 5.into()]));
        let s3: OutputStream<Value> = Box::pin(stream::iter(vec![3.into(), 6.into()]));
        let m = BTreeMap::from([(EcoString::from("x"), s1), (EcoString::from("y"), s2)]);
        // Overwrite y with new stream
        let res: Vec<Value> = minsert(map(m), "y".into(), s3).collect().await;
        let exp: Vec<Value> = vec![
            Value::Map(BTreeMap::from([
                ("x".into(), 1.into()),
                ("y".into(), 3.into()),
            ])),
            Value::Map(BTreeMap::from([
                ("x".into(), 4.into()),
                ("y".into(), 6.into()),
            ])),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_map_has_key_true() {
        let s1: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 3.into()]));
        let s2: OutputStream<Value> = Box::pin(stream::iter(vec![2.into(), 4.into()]));
        let m = BTreeMap::from([(EcoString::from("x"), s1), (EcoString::from("y"), s2)]);
        let res: Vec<Value> = mhas_key(map(m), "y".into()).collect().await;
        let exp: Vec<Value> = vec![true.into(), true.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_map_has_key_false() {
        let s1: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 3.into()]));
        let s2: OutputStream<Value> = Box::pin(stream::iter(vec![2.into(), 4.into()]));
        let m = BTreeMap::from([(EcoString::from("x"), s1), (EcoString::from("y"), s2)]);
        let res: Vec<Value> = mhas_key(map(m), "z".into()).collect().await;
        let exp: Vec<Value> = vec![false.into(), false.into()];
        assert_eq!(res, exp);
    }
}
