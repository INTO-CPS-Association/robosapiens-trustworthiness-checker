use crate::core::StreamData;
use crate::core::Value;
use crate::core::values::operations as value_operations;
use crate::core::{BinaryOperator, UnaryOperator};
use crate::semantics::AsyncConfig;
use crate::semantics::StreamContext;
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

#[cfg(test)]
use crate::lang::dsrv::ast::DynamicExprScope;

pub use super::dynamic::{defer, dynamic};

fn unwrap_value(result: Result<Value, value_operations::ValueOpError>) -> Value {
    result.unwrap_or_else(|error| panic!("{error}"))
}

fn eval_binary(operation: BinaryOperator, left: Value, right: Value) -> Value {
    unwrap_value(value_operations::binary(operation, left, right))
}

fn eval_unary(operation: UnaryOperator, operand: Value) -> Value {
    unwrap_value(value_operations::unary(operation, operand))
}

pub trait CloneFn1<T: StreamData, S: StreamData>: Fn(T) -> S + Clone + 'static {}
impl<T, S: StreamData, R: StreamData> CloneFn1<S, R> for T where T: Fn(S) -> R + Clone + 'static {}

pub(crate) fn stream_lift_base(mut x_mon: OutputStream<Value>) -> OutputStream<Value> {
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

// Lifting function which propagates both NoVal and Deferred values
pub fn stream_lift1(
    f: impl CloneFn1<Value, Value>,
    x_mon: OutputStream<Value>,
) -> OutputStream<Value> {
    Box::pin(stream_lift_base(x_mon).map(move |x| {
        if x == Value::NoVal {
            Value::NoVal
        } else if x == Value::Deferred {
            Value::Deferred
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

// Lifting function which propagates both NoVal and Deferred values
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
                } else if x == Value::Deferred || y == Value::Deferred {
                    Value::Deferred
                } else {
                    f(x, y)
                }
            }),
    )
}

pub fn and(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(|x, y| eval_binary(BinaryOperator::And, x, y), x, y)
}

pub fn or(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(|x, y| eval_binary(BinaryOperator::Or, x, y), x, y)
}

pub fn implication(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(|x, y| eval_binary(BinaryOperator::Implication, x, y), x, y)
}

pub fn not(x: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift1(|x| eval_unary(UnaryOperator::Not, x), x)
}

// Semantic detail: deferred == deferred === deferred
// rather than true. This is consistent with three-valued logic style semantics.
// For the old, value equality, you can use:
// default(x == y, is_defined(x) == is_defined(y))
pub fn eq(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(|x, y| eval_binary(BinaryOperator::Equal, x, y), x, y)
}

pub fn le(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(|x, y| eval_binary(BinaryOperator::LessEqual, x, y), x, y)
}

pub fn lt(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(|x, y| eval_binary(BinaryOperator::Less, x, y), x, y)
}

pub fn ge(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(|x, y| eval_binary(BinaryOperator::GreaterEqual, x, y), x, y)
}

pub fn gt(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(|x, y| eval_binary(BinaryOperator::Greater, x, y), x, y)
}

pub fn val(x: Value) -> OutputStream<Value> {
    Box::pin(stream::repeat(x))
}

pub fn if_stm(
    x: OutputStream<Value>,
    y: OutputStream<Value>,
    z: OutputStream<Value>,
) -> OutputStream<Value> {
    // Uses manual stream lifting rather than a lifting function since deferred values from
    // excluded branches do not propagate (i.e. we propagate lazily with respect to
    // deferred values — only the selected branch potentially yields Deferred)
    Box::pin(
        stream_lift_base(x)
            .zip(stream_lift_base(y))
            .zip(stream_lift_base(z))
            .map(move |((x, y), z)| {
                if x == Value::NoVal || y == Value::NoVal || z == Value::NoVal {
                    Value::NoVal
                } else {
                    match x {
                        Value::Bool(true) => y,
                        Value::Bool(false) => z,
                        Value::Deferred => Value::Deferred,
                        x => panic!("Invalid conditional for if statement with type: {:?}", x),
                    }
                }
            }),
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
// (The correct call would need to be evaluated in semantics.rs where the syntax
// node is still available).
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
            eval_binary(BinaryOperator::Add, x, y)
        },
        x,
        y,
    )
}

pub fn modulo(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(|x, y| eval_binary(BinaryOperator::Modulo, x, y), x, y)
}

pub fn minus(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(|x, y| eval_binary(BinaryOperator::Subtract, x, y), x, y)
}

pub fn mult(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(|x, y| eval_binary(BinaryOperator::Multiply, x, y), x, y)
}

pub fn div(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(|x, y| eval_binary(BinaryOperator::Divide, x, y), x, y)
}

pub fn concat(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(|x, y| eval_binary(BinaryOperator::Concatenate, x, y), x, y)
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

// Defer for an UntimedDsrvExpression using the dsrv_expression parser
// Then continues evaluating the r.h.s. (even if it provides Deferred)
pub fn update(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    let mut x = stream_lift_base(x);
    let mut y = stream_lift_base(y);
    Box::pin(stream! {
        while let (Some(x_val), Some(y_val)) = join!(x.next(), y.next()) {
            match (x_val, y_val) {
                (x_val, Value::Deferred) => {
                    yield x_val;
                }
                (x_val, Value::NoVal) => {
                    yield x_val;
                }
                (_, y_val) => {
                    yield y_val;
                    break;
                }
            }
        }
        let mut x_active = true;
        loop {
            let y_next = if x_active {
                let (x_next, y_next) = join!(x.next(), y.next());
                x_active = x_next.is_some();
                y_next
            } else {
                y.next().await
            };

            match y_next {
                Some(y_val) => yield y_val,
                None => break,
            }
        }
    })
}

// Evaluates to a placeholder stream whenever Deferred is received.
//
// Note: Intentionally does not default to new value with NoVal - if we want this then we can
// implement a specific combinator for it.
pub fn default(x: OutputStream<Value>, d: OutputStream<Value>) -> OutputStream<Value> {
    let x = stream_lift_base(x);
    let xs = x
        .zip(d)
        .map(|(x, d)| if x == Value::Deferred { d } else { x });
    Box::pin(xs) as LocalBoxStream<'static, Value>
}

// Initializes the x stream with values from the d stream until
// x provides a value that is not NoVal. Then yields from x.
// Can be considered a `default` that applies to NoVal.
pub fn init(mut x: OutputStream<Value>, mut d: OutputStream<Value>) -> OutputStream<Value> {
    Box::pin(stream! {
        while let (Some(x_val), Some(d_val)) = join!(x.next(), d.next()) {
            match x_val {
                Value::NoVal => {
                    yield d_val;
                }
                _ => {
                    yield x_val;
                    break;
                }
            }
        }
        while let Some(x_val) = x.next().await {
            yield x_val;
        }
    })
}

// TODO: Should change to an operator called is_deferred and negate the logic...
pub fn is_defined(x: OutputStream<Value>) -> OutputStream<Value> {
    let x = stream_lift_base(x);
    Box::pin(x.map(|x| Value::Bool(x != Value::Deferred)))
}

// Could also be implemented with is_defined but I think this is more efficient
pub fn when(x: OutputStream<Value>) -> OutputStream<Value> {
    let mut x = stream_lift_base(x);
    debug!("Creating when operation stream");
    Box::pin(stream! {
        while let Some(x_val) = x.next().await {
            debug!("Received x");
            if x_val == Value::Deferred || x_val == Value::NoVal {
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

// Latches a stream x to only yield values when another stream y is not NoVal.
// The result is the latest value received on x
//
// Note that latch expressions propagate interestingly when used inside other expressions. E.g.:
// in x, in y, out a, out b
// a = latch(x, y) // Only update x when y is not NoVal
// b = a + 1
// Here, b is updated whenever x OR y is updated but it is using the current value of a, which is only
// updated when y is updated. This is because a emits a NoVal when x receives a new value but y
// does not.
//
// Note: `latch` is like `last` in TeSSLa but where on simultaneous events it returns the current val
// instead of previous val
pub fn latch(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    let x = stream_lift_base(x);
    let vals = x
        .zip(y)
        .map(|(x, y)| if y == Value::NoVal { Value::NoVal } else { x });
    Box::pin(vals)
}

pub fn list(xs: Vec<OutputStream<Value>>) -> OutputStream<Value> {
    let mut xs = xs
        .into_iter()
        .map(|x| stream_lift_base(x))
        .collect::<Vec<_>>();

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

pub fn tuple(xs: Vec<OutputStream<Value>>) -> OutputStream<Value> {
    let mut xs = xs
        .into_iter()
        .map(|x| stream_lift_base(x))
        .collect::<Vec<_>>();

    Box::pin(stream! {
        if xs.is_empty() {
            Value::Tuple(EcoVec::new());
        }
        loop {
            let vals = join_all(xs.iter_mut().map(|x| x.next())).await;
            if vals.iter().all(|x| x.is_some()) {
                yield Value::Tuple(vals.iter().map(|x| x.clone().unwrap()).collect());
            } else {
                return;
            }
        }
    })
}

pub fn lindex(x: OutputStream<Value>, i: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(
        |list, index| unwrap_value(value_operations::list_index(list, index)),
        x,
        i,
    )
}

pub fn lappend(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(
        |list, value| unwrap_value(value_operations::list_append(list, value)),
        x,
        y,
    )
}

pub fn lconcat(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift2(
        |left, right| unwrap_value(value_operations::list_concat(left, right)),
        x,
        y,
    )
}

pub fn lhead(x: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift1(|list| unwrap_value(value_operations::list_head(list)), x)
}

pub fn ltail(x: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift1(|list| unwrap_value(value_operations::list_tail(list)), x)
}

pub fn llen(x: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift1(|list| unwrap_value(value_operations::list_len(list)), x)
}

pub fn tget(x: OutputStream<Value>, idx: usize) -> OutputStream<Value> {
    stream_lift1(
        move |tuple| match tuple {
            Value::Tuple(values) | Value::List(values) => values
                .get(idx)
                .cloned()
                .unwrap_or_else(|| panic!("Tuple index out of bounds: {}", idx)),
            other => panic!("Expected tuple for .{} access, got {}", idx, other),
        },
        x,
    )
}

pub fn map(xs: BTreeMap<EcoString, OutputStream<Value>>) -> OutputStream<Value> {
    let mut xs = xs
        .into_iter()
        .map(|(k, v)| (k, stream_lift_base(v)))
        .collect::<BTreeMap<_, _>>();

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

pub fn mget(xs: OutputStream<Value>, k: EcoString) -> OutputStream<Value> {
    stream_lift1(
        move |map| unwrap_value(value_operations::map_get(map, &k)),
        xs,
    )
}

pub fn mremove(xs: OutputStream<Value>, k: EcoString) -> OutputStream<Value> {
    stream_lift1(
        move |map| unwrap_value(value_operations::map_remove(map, &k)),
        xs,
    )
}

pub fn minsert(
    xs: OutputStream<Value>,
    k: EcoString,
    v: OutputStream<Value>,
) -> OutputStream<Value> {
    stream_lift2(
        move |map, value| unwrap_value(value_operations::map_insert(map, &k, value)),
        xs,
        v,
    )
}

pub fn mhas_key(xs: OutputStream<Value>, k: EcoString) -> OutputStream<Value> {
    stream_lift1(
        move |map| unwrap_value(value_operations::map_has_key(map, &k)),
        xs,
    )
}

pub fn sin(v: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift1(|v| eval_unary(UnaryOperator::Sin, v), v)
}

pub fn cos(v: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift1(|v| eval_unary(UnaryOperator::Cos, v), v)
}

pub fn tan(v: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift1(|v| eval_unary(UnaryOperator::Tan, v), v)
}

pub fn neg(v: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift1(|v| eval_unary(UnaryOperator::Negate, v), v)
}

pub fn abs(v: OutputStream<Value>) -> OutputStream<Value> {
    stream_lift1(|v| eval_unary(UnaryOperator::Absolute, v), v)
}

#[cfg(test)]
mod combinator_tests {
    use super::*;
    use crate::async_test;
    use crate::core::Value;
    use crate::dsrv_fixtures::TestConfig;
    use crate::runtime::asynchronous::Context;
    use ecow::eco_vec;
    use futures::stream;
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::rc::Rc;
    use tc_testutils::streams::with_timeout;

    // Using this instead of fixture version to in case fixture version changed
    type TestCtx = Context<TestConfig>;

    #[apply(async_test)]
    async fn test_not() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![Value::Bool(true), false.into()]));
        let res: Vec<Value> = not(x).collect().await;
        let exp: Vec<Value> = vec![Value::Bool(false), Value::Bool(true)];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_unordered_float_comparisons_are_false() {
        let comparisons: [(
            &str,
            fn(OutputStream<Value>, OutputStream<Value>) -> OutputStream<Value>,
        ); 4] = [("<", lt), ("<=", le), (">", gt), (">=", ge)];

        for (name, comparison) in comparisons {
            let left: OutputStream<Value> =
                Box::pin(stream::iter([Value::Float(f64::NAN), Value::Int(1)]));
            let right: OutputStream<Value> =
                Box::pin(stream::iter([Value::Int(1), Value::Float(f64::NAN)]));

            assert_eq!(
                comparison(left, right).collect::<Vec<_>>().await,
                vec![Value::Bool(false), Value::Bool(false)],
                "{name} should be false with NaN on either side"
            );
        }
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
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let res_stream = dynamic::<TestConfig>(&ctx, e, DynamicExprScope::Automatic, None, 10);
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
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let res_stream = dynamic::<TestConfig>(&ctx, e, DynamicExprScope::Automatic, None, 10);
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
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let res_stream = dynamic::<TestConfig>(&ctx, e, DynamicExprScope::Automatic, None, 10);
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
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let res_stream = dynamic::<TestConfig>(&ctx, e, DynamicExprScope::Automatic, None, 10);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;
        // Evaluates to Deferred when we get Deferred
        // (Think can happen e.g., with nested DUPs)
        let exp: Vec<Value> = vec![2.into(), Value::Deferred, 5.into()];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    #[ignore = "Bug with dynamic not updating the dependency graph correctly"]
    async fn test_dynamic_history_dependency_graph(executor: Rc<LocalExecutor<'static>>) {
        // Tests that dynamic correctly updates the dependency graph.
        // See comments below

        let e: OutputStream<Value> = Box::pin(stream::iter([
            "x".into(),
            "x[1]".into(), // Introduces saving x history one step back
            "x[2]".into(), // Introduces saving x history two steps back
            Value::NoVal,
            Value::NoVal,
        ]));
        let x = Box::pin(stream::iter(vec![
            1.into(),
            2.into(),
            3.into(),
            4.into(),
            5.into(),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 1);
        let res_stream = dynamic::<TestConfig>(&ctx, e, DynamicExprScope::Automatic, None, 1);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;
        let exp: Vec<Value> = vec![
            1.into(),
            Value::Deferred, // Deferred because x at time 0 is not in memory
            Value::Deferred, // Deferred because x at time 0 is not in memory
            2.into(),        // x at time 1 is in memory - should be solvable
            3.into(),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer1(executor: Rc<LocalExecutor<'static>>) {
        // Notice that even though we first say "x + 1", "x + 2", it continues evaluating "x + 1"
        let e: OutputStream<Value> = Box::pin(stream::iter(vec!["x + 1".into(), "x + 2".into()]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let res_stream = defer::<TestConfig>(&ctx, e, eco_vec!["x".into()].into(), None, 2);
        let _ = with_timeout(ctx.run(), 1, "ctx.run()")
            .await
            .expect("ctx.run() timed out");
        let res: Vec<Value> = with_timeout(res_stream.collect(), 1, "res_stream.collect()")
            .await
            .expect("res_stream.collect() timed out");
        let exp: Vec<Value> = vec![2.into(), 3.into()];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_defer_x_squared(executor: Rc<LocalExecutor<'static>>) {
        // This test is interesting since we use x twice in the dynamic strings
        let e: OutputStream<Value> =
            Box::pin(stream::iter(vec!["x * x".into(), "x * x + 1".into()]));
        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let res_stream = defer::<TestConfig>(&ctx, e, eco_vec!["x".into()].into(), None, 10);
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
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let res_stream = defer::<TestConfig>(&ctx, e, eco_vec!["x".into()].into(), None, 10);
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
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let res_stream = defer::<TestConfig>(&ctx, e, eco_vec!["x".into()].into(), None, 10);
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
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let res_stream = defer::<TestConfig>(&ctx, e, eco_vec!["x".into()].into(), None, 10);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;
        let exp: Vec<Value> = vec![Value::Deferred, Value::Deferred];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_defer_long_regression(executor: Rc<LocalExecutor<'static>>) {
        // Test that checks that defer combinator can handle a large number of ticks without
        // deadlocking or running out of memory.
        // Introduced after regression with runtime test

        // Hack to force log into being INFO for this test.
        // Needed to make CI perform better
        use tracing_subscriber::{filter::LevelFilter, fmt, prelude::*, util::SubscriberInitExt};
        let subscriber = tracing_subscriber::registry()
            .with(LevelFilter::INFO)
            .with(fmt::layer().with_test_writer());
        let _guard = subscriber.set_default(); // active only in this scope

        const SIZE: i64 = 3000;
        let x: OutputStream<Value> = Box::pin(stream::iter((0..SIZE).map(|x| (2 * x).into())));
        let y: OutputStream<Value> = Box::pin(stream::iter((0..SIZE).map(|y| (2 * y + 1).into())));
        let e: OutputStream<Value> = Box::pin(stream::iter((0..SIZE).map(|i| {
            if i < SIZE / 2 {
                Value::Deferred
            } else {
                Value::Str("x + y".into())
            }
        })));
        // Note: Diff here between RT test and Comb test -- e is not given to the context
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );
        let res_stream =
            defer::<TestConfig>(&ctx, e, eco_vec!["x".into(), "y".into()].into(), None, 10);
        ctx.run().await;
        let res: Vec<Value> = with_timeout(res_stream.collect(), 10, "res_stream.collect")
            .await
            .expect("Result timed out");
        assert_eq!(res.len(), SIZE as usize);
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
        let mut ctx = TestCtx::new(executor.clone(), vec!["i".into()], vec![i], 10);
        let res_stream = lindex(list(x), var::<TestConfig>(&ctx, "i".into()));
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
    #[should_panic(expected = "Missing key for map get: z")]
    async fn test_map_get_missing_key() {
        // TODO: we currently do not handle missing map keys well
        // The language would need some kind of error handling strategy to remove the panics here.
        // Neither NoVal nor Deferred make sense, since they interpret the missing values
        // inappropriately in other combinators (since in-language errors do not behave the same as
        // missing or deferred inputs)
        let s1: OutputStream<Value> = Box::pin(stream::iter(vec![1.into()]));
        let s2: OutputStream<Value> = Box::pin(stream::iter(vec![2.into()]));
        let m = BTreeMap::from([(EcoString::from("x"), s1), (EcoString::from("y"), s2)]);
        // "z" is not a key in the map — mget must panic
        let _res: Vec<Value> = mget(map(m), "z".into()).collect().await;
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

    #[apply(async_test)]
    async fn test_map_get_with_deferred() {
        // mget must propagate Deferred rather than panicking
        let map_stream: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Map(BTreeMap::from([("y".into(), 2.into())])),
            Value::Deferred,
            Value::Map(BTreeMap::from([("y".into(), 4.into())])),
        ]));
        let res: Vec<Value> = mget(map_stream, "y".into()).collect().await;
        let exp: Vec<Value> = vec![2.into(), Value::Deferred, 4.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_map_has_key_with_deferred() {
        // mhas_key must propagate Deferred rather than panicking
        let map_stream: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Map(BTreeMap::from([("x".into(), 1.into())])),
            Value::Deferred,
            Value::Map(BTreeMap::from([("x".into(), 3.into())])),
        ]));
        let res: Vec<Value> = mhas_key(map_stream, "x".into()).collect().await;
        let exp: Vec<Value> = vec![true.into(), Value::Deferred, true.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_map_remove_with_deferred() {
        // mremove must propagate Deferred rather than panicking
        let map_stream: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Map(BTreeMap::from([
                ("x".into(), 1.into()),
                ("y".into(), 2.into()),
            ])),
            Value::Deferred,
            Value::Map(BTreeMap::from([
                ("x".into(), 3.into()),
                ("y".into(), 4.into()),
            ])),
        ]));
        let res: Vec<Value> = mremove(map_stream, "y".into()).collect().await;
        let exp: Vec<Value> = vec![
            Value::Map(BTreeMap::from([("x".into(), 1.into())])),
            Value::Deferred,
            Value::Map(BTreeMap::from([("x".into(), 3.into())])),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_map_insert_with_deferred_map() {
        // When the map argument is Deferred the whole output tick is Deferred
        let map_stream: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Map(BTreeMap::from([("x".into(), 1.into())])),
            Value::Deferred,
            Value::Map(BTreeMap::from([("x".into(), 3.into())])),
        ]));
        let val_stream: OutputStream<Value> =
            Box::pin(stream::iter(vec![10.into(), 20.into(), 30.into()]));
        let res: Vec<Value> = minsert(map_stream, "z".into(), val_stream).collect().await;
        let exp: Vec<Value> = vec![
            Value::Map(BTreeMap::from([
                ("x".into(), 1.into()),
                ("z".into(), 10.into()),
            ])),
            Value::Deferred,
            Value::Map(BTreeMap::from([
                ("x".into(), 3.into()),
                ("z".into(), 30.into()),
            ])),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_map_insert_with_deferred_value() {
        // When the value argument is Deferred the whole output tick is also Deferred
        let map_stream: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Map(BTreeMap::from([("x".into(), 1.into())])),
            Value::Map(BTreeMap::from([("x".into(), 2.into())])),
            Value::Map(BTreeMap::from([("x".into(), 3.into())])),
        ]));
        let val_stream: OutputStream<Value> =
            Box::pin(stream::iter(vec![10.into(), Value::Deferred, 30.into()]));
        let res: Vec<Value> = minsert(map_stream, "z".into(), val_stream).collect().await;
        let exp: Vec<Value> = vec![
            Value::Map(BTreeMap::from([
                ("x".into(), 1.into()),
                ("z".into(), 10.into()),
            ])),
            Value::Deferred,
            Value::Map(BTreeMap::from([
                ("x".into(), 3.into()),
                ("z".into(), 30.into()),
            ])),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_eq_with_deferred() {
        // eq now propagates Deferred rather than treating it as a comparable value
        let x: OutputStream<Value> =
            Box::pin(stream::iter(vec![1.into(), Value::Deferred, 3.into()]));
        let y: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 1.into(), 2.into()]));
        let res: Vec<Value> = eq(x, y).collect().await;
        let exp: Vec<Value> = vec![true.into(), Value::Deferred, false.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_lhead_with_deferred() {
        // lhead must propagate Deferred rather than panicking
        let list_stream: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::List(eco_vec![1.into(), 2.into()]),
            Value::Deferred,
            Value::List(eco_vec![3.into(), 4.into()]),
        ]));
        let res: Vec<Value> = lhead(list_stream).collect().await;
        let exp: Vec<Value> = vec![1.into(), Value::Deferred, 3.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_latch_never() {
        let s1: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let s2: OutputStream<Value> = Box::pin(stream::iter(vec![Value::NoVal, Value::NoVal]));
        let res: Vec<Value> = latch(s1, s2).collect().await;
        let exp: Vec<Value> = vec![Value::NoVal, Value::NoVal];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_latch_eventually_always() {
        let s1: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let s2: OutputStream<Value> = Box::pin(stream::iter(vec![Value::NoVal, Value::Unit]));
        let res: Vec<Value> = latch(s1, s2).collect().await;
        let exp: Vec<Value> = vec![Value::NoVal, 2.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_latch_eventually() {
        let s1: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let s2: OutputStream<Value> =
            Box::pin(stream::iter(vec![Value::NoVal, Value::Unit, Value::NoVal]));
        let res: Vec<Value> = latch(s1, s2).collect().await;
        let exp: Vec<Value> = vec![Value::NoVal, 2.into(), Value::NoVal];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_latch_interesting_interaction() {
        let x = [1.into(), 2.into(), 3.into()];
        let y = [Value::Unit, Value::NoVal, Value::NoVal];
        let x1: OutputStream<Value> = Box::pin(stream::iter(x.clone()));
        let x2: OutputStream<Value> = Box::pin(stream::iter(x));
        let y1: OutputStream<Value> = Box::pin(stream::iter(y.clone()));
        let y2: OutputStream<Value> = Box::pin(stream::iter(y));
        let zero = Box::pin(stream::iter(vec![0.into(); 3]));
        let res1: Vec<Value> = latch(x1, y1).collect().await;
        let res2: Vec<Value> = plus(zero, latch(x2, y2)).collect().await;
        let exp1: Vec<Value> = vec![1.into(), Value::NoVal, Value::NoVal];
        let exp2: Vec<Value> = vec![1.into(), 1.into(), 1.into()];
        assert_eq!(res1, exp1);
        assert_eq!(res2, exp2);
    }

    #[apply(async_test)]
    async fn test_sindex_delay_1() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
        ]));
        let res: Vec<Value> = sindex(x, 1).collect().await;
        let exp: Vec<Value> = vec![Value::Deferred, Value::Int(1), Value::Int(2), Value::Int(3)];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_sindex_delay_2() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
        ]));
        let res: Vec<Value> = sindex(x, 2).collect().await;
        let exp: Vec<Value> = vec![
            Value::Deferred,
            Value::Deferred,
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_sindex_noval_at_start() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::NoVal,
            Value::Int(1),
            Value::Int(2),
        ]));
        let res: Vec<Value> = sindex(x, 1).collect().await;
        let exp: Vec<Value> = vec![
            Value::Deferred,
            Value::Deferred, // NoVal after Deferred repeats Deferred
            Value::Int(1),
            Value::Int(2),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_sindex_noval_in_middle() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Int(1),
            Value::NoVal,
            Value::Int(2),
        ]));
        let res: Vec<Value> = sindex(x, 1).collect().await;
        let exp: Vec<Value> = vec![
            Value::Deferred,
            Value::Int(1),
            Value::Int(1), // NoVal repeats the last known value
            Value::Int(2),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_sindex_multiple_noval() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Int(1),
            Value::NoVal,
            Value::NoVal,
            Value::Int(2),
        ]));
        let res: Vec<Value> = sindex(x, 1).collect().await;
        let exp: Vec<Value> = vec![
            Value::Deferred,
            Value::Int(1),
            Value::Int(1), // First NoVal repeats Int(1)
            Value::Int(1), // Second NoVal also repeats Int(1)
            Value::Int(2),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_sindex_with_deferred_in_stream() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Int(1),
            Value::Deferred,
            Value::Int(2),
        ]));
        let res: Vec<Value> = sindex(x, 1).collect().await;
        let exp: Vec<Value> = vec![
            Value::Deferred, // Added by sindex
            Value::Int(1),
            Value::Deferred, // Deferred from stream
            Value::Int(2),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_sindex_noval_after_deferred() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Deferred,
            Value::NoVal,
            Value::Int(1),
        ]));
        let res: Vec<Value> = sindex(x, 1).collect().await;
        let exp: Vec<Value> = vec![
            Value::Deferred, // Added by sindex
            Value::Deferred, // From stream
            Value::Deferred, // NoVal repeats last value which was Deferred
            Value::Int(1),
        ];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_sindex_complex_pattern() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Int(1),
            Value::NoVal,
            Value::Deferred,
            Value::Int(2),
            Value::NoVal,
        ]));
        let res: Vec<Value> = sindex(x, 2).collect().await;
        let exp: Vec<Value> = vec![
            Value::Deferred, // Added by sindex
            Value::Deferred, // Added by sindex
            Value::Int(1),
            Value::Int(1),   // NoVal repeats Int(1)
            Value::Deferred, // Deferred from stream
            Value::Int(2),
            Value::Int(2), // NoVal repeats Int(2)
        ];
        assert_eq!(res, exp)
    }
}

#[cfg(test)]
mod noval_tests {
    use std::rc::Rc;

    use super::*;
    use crate::async_test;
    use crate::core::Value;
    use crate::dsrv_fixtures::TestConfig;
    use crate::runtime::asynchronous::Context;
    use ecow::eco_vec;
    use futures::stream;
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;

    // Using this instead of fixture version to in case fixture version changed
    type TestCtx = Context<TestConfig>;

    #[apply(async_test)]
    async fn test_dynamic_noval_start(executor: Rc<LocalExecutor<'static>>) {
        let e: OutputStream<Value> = Box::pin(stream::iter([Value::NoVal, Value::NoVal]));
        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 1);
        let res_stream = dynamic::<TestConfig>(&ctx, e, DynamicExprScope::Automatic, None, 1);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;
        let exp: Vec<Value> = vec![Value::NoVal, Value::NoVal];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_noval_middle(executor: Rc<LocalExecutor<'static>>) {
        let e: OutputStream<Value> =
            Box::pin(stream::iter(["x + 1".into(), Value::NoVal, "42".into()]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 1);
        let res_stream = dynamic::<TestConfig>(&ctx, e, DynamicExprScope::Automatic, None, 1);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;
        // Continues evaluating to x + 1 until we get a non-deferred value
        let exp: Vec<Value> = vec![2.into(), 3.into(), 42.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_deferred_sticky(executor: Rc<LocalExecutor<'static>>) {
        // Tests that deferred is treated as the new "dynamic value" when coming after NoVal
        let e: OutputStream<Value> = Box::pin(stream::iter([
            Value::NoVal,
            Value::Deferred,
            Value::NoVal,
            "x".into(),
            Value::Deferred,
            Value::NoVal,
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
        let res_stream = dynamic::<TestConfig>(&ctx, e, DynamicExprScope::Automatic, None, 1);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;
        let exp: Vec<Value> = vec![
            Value::NoVal,
            Value::Deferred,
            Value::Deferred,
            4.into(),
            Value::Deferred,
            Value::Deferred,
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_history_indexing(executor: Rc<LocalExecutor<'static>>) {
        // Tests a complex scenario with NoVal, Deferred, and indexing into x
        // First a NoVal. Then x[1] with not enough context produces Deferred.
        // Then we can yield from x[1]. Then the property becomes x followed by Deferred,
        // which we can of course yield from. When we get x[1] again, we once more do not have
        // enough context. Finally, a NoVal, which makes us yield x[1].

        let e: OutputStream<Value> = Box::pin(stream::iter([
            Value::NoVal,
            "x[1]".into(),
            Value::NoVal,
            "x".into(),
            Value::Deferred,
            "x[1]".into(),
            Value::NoVal,
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
        let res_stream = dynamic::<TestConfig>(&ctx, e, DynamicExprScope::Automatic, None, 1);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;
        let exp: Vec<Value> = vec![
            Value::NoVal,
            Value::Deferred,
            2.into(),
            4.into(),
            Value::Deferred,
            Value::Deferred,
            6.into(),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_noval_start(executor: Rc<LocalExecutor<'static>>) {
        let e: OutputStream<Value> = Box::pin(stream::iter([Value::NoVal, Value::NoVal]));
        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 1);
        let res_stream = defer::<TestConfig>(&ctx, e, eco_vec!["x".into()].into(), None, 1);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;
        let exp: Vec<Value> = vec![Value::NoVal, Value::NoVal];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_noval_middle(executor: Rc<LocalExecutor<'static>>) {
        let e: OutputStream<Value> =
            Box::pin(stream::iter(["x + 1".into(), Value::NoVal, "42".into()]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 1);
        let res_stream = defer::<TestConfig>(&ctx, e, eco_vec!["x".into()].into(), None, 1);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;
        // Continues evaluating to x + 1
        let exp: Vec<Value> = vec![2.into(), 3.into(), 4.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_deferred_sticky(executor: Rc<LocalExecutor<'static>>) {
        // Tests that deferred is treated as the new "defer value" when coming after NoVal
        let e: OutputStream<Value> = Box::pin(stream::iter([
            Value::NoVal,
            Value::Deferred,
            Value::NoVal,
            "x".into(),
            Value::Deferred,
            Value::NoVal,
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
        let res_stream = defer::<TestConfig>(&ctx, e, eco_vec!["x".into()].into(), None, 1);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;
        let exp: Vec<Value> = vec![
            Value::NoVal,
            Value::Deferred,
            Value::Deferred,
            4.into(),
            5.into(),
            6.into(),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_update_first_x_then_y() {
        let x: OutputStream<Value> =
            Box::pin(stream::iter(vec!["x0".into(), "x1".into(), "x2".into()]));
        let y: OutputStream<Value> =
            Box::pin(stream::iter(vec![Value::NoVal, "y1".into(), "y2".into()]));
        let res: Vec<Value> = update(x, y).collect().await;
        let exp: Vec<Value> = vec!["x0".into(), "y1".into(), "y2".into()];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_update_first_y_then_x() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![Value::NoVal, "x1".into()]));
        let y: OutputStream<Value> = Box::pin(stream::iter(vec!["y0".into(), "y1".into()]));
        let res: Vec<Value> = update(x, y).collect().await;
        let exp: Vec<Value> = vec!["y0".into(), "y1".into()];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_update_neither() {
        use Value::NoVal;
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![NoVal, NoVal, NoVal, NoVal]));
        let y: OutputStream<Value> = Box::pin(stream::iter(vec![NoVal, NoVal, NoVal, NoVal]));
        let res: Vec<Value> = update(x, y).collect().await;
        let exp: Vec<Value> = vec![NoVal, NoVal, NoVal, NoVal];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_update_noval_deferred_noval_y() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![
            "x0".into(),
            "x1".into(),
            "x2".into(),
            "x3".into(),
        ]));
        let y: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::NoVal,
            Value::Deferred,
            Value::NoVal,
            "y3".into(),
        ]));
        let res: Vec<Value> = update(x, y).collect().await;
        let exp: Vec<Value> = vec!["x0".into(), "x1".into(), "x2".into(), "y3".into()];
        assert_eq!(res, exp)
    }

    #[apply(async_test)]
    async fn test_random_bin_operators() {
        // Tests a selected number of operators for correct handling of NoVal
        let combinators = vec![
            plus as fn(OutputStream<Value>, OutputStream<Value>) -> OutputStream<Value>,
            minus,
            modulo,
        ];

        for comb in combinators {
            let x: OutputStream<Value> = Box::pin(stream::iter(vec![
                Value::NoVal,
                1.into(),
                Value::NoVal,
                3.into(),
                Value::NoVal,
            ]));
            let y: OutputStream<Value> = Box::pin(stream::iter(vec![
                0.into(),
                1.into(),
                Value::NoVal,
                3.into(),
                Value::NoVal,
            ]));
            let res: Vec<Value> = comb(x, y).collect().await;
            assert_eq!(res.len(), 5);
            assert_eq!(res[0], Value::NoVal);
            assert!(res[1] != Value::NoVal);
            assert_eq!(res[2], res[1]);
            assert!(res[3] != Value::NoVal);
            assert_eq!(res[4], res[3]);
        }
    }

    #[apply(async_test)]
    async fn test_random_unary_operators() {
        // Tests a selected number of operators for correct handling of NoVal
        let combinators = vec![
            cos as fn(OutputStream<Value>) -> OutputStream<Value>,
            sin,
            abs,
        ];
        for comb in combinators {
            let x: OutputStream<Value> = Box::pin(stream::iter(vec![
                Value::NoVal,
                1.0.into(),
                Value::NoVal,
                3.0.into(),
                Value::NoVal,
            ]));
            let res: Vec<Value> = comb(x).collect().await;
            assert_eq!(res.len(), 5);
            assert_eq!(res[0], Value::NoVal);
            assert!(res[1] != Value::NoVal);
            assert_eq!(res[2], res[1]);
            assert!(res[3] != Value::NoVal);
            assert_eq!(res[4], res[3]);
        }
    }

    #[apply(async_test)]
    async fn test_map_noval() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::NoVal,
            Value::NoVal,
            "x3".into(),
            "x4".into(),
        ]));
        let y: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::NoVal,
            "y2".into(),
            Value::NoVal,
            "y4".into(),
        ]));
        let m: BTreeMap<EcoString, OutputStream<Value>> =
            BTreeMap::from([("x".into(), x), ("y".into(), y)]);
        let res: Vec<Value> = map(m).collect().await;
        let exp: Vec<Value> = vec![
            Value::Map(BTreeMap::from([
                ("x".into(), Value::NoVal),
                ("y".into(), Value::NoVal),
            ])),
            Value::Map(BTreeMap::from([
                ("x".into(), Value::NoVal),
                ("y".into(), "y2".into()),
            ])),
            Value::Map(BTreeMap::from([
                ("x".into(), "x3".into()),
                ("y".into(), "y2".into()),
            ])),
            Value::Map(BTreeMap::from([
                ("x".into(), "x4".into()),
                ("y".into(), "y4".into()),
            ])),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_list_noval() {
        let x: Vec<OutputStream<Value>> = vec![
            Box::pin(stream::iter(vec![
                Value::NoVal,
                2.into(),
                Value::NoVal,
                4.into(),
            ])),
            Box::pin(stream::iter(vec![
                Value::NoVal,
                Value::NoVal,
                3.into(),
                4.into(),
            ])),
        ];
        let res: Vec<Value> = list(x).collect().await;
        let exp: Vec<Value> = vec![
            Value::List(vec![Value::NoVal, Value::NoVal].into()),
            Value::List(vec![2.into(), Value::NoVal].into()),
            Value::List(vec![2.into(), 3.into()].into()),
            Value::List(vec![4.into(), 4.into()].into()),
        ];
        assert_eq!(res, exp);
    }
}
