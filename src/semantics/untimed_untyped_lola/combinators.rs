use crate::core::StreamData;
use crate::core::Value;
use crate::lang::dynamic_lola::parser::lola_expression;
use crate::semantics::untimed_untyped_lola::UntimedLolaSemantics;
use crate::{MonitoringSemantics, OutputStream, StreamContext, VarName};
use async_stream::stream;
use core::panic;
use futures::stream::LocalBoxStream;
use futures::{
    StreamExt,
    future::join_all,
    stream::{self},
};
use tokio::join;
use tracing::debug;
use tracing::info;
use tracing::instrument;
use winnow::Parser;

pub trait CloneFn1<T: StreamData, S: StreamData>: Fn(T) -> S + Clone + 'static {}
impl<T, S: StreamData, R: StreamData> CloneFn1<S, R> for T where T: Fn(S) -> R + Clone + 'static {}

pub fn lift1<S: StreamData, R: StreamData>(
    f: impl CloneFn1<S, R>,
    x_mon: OutputStream<S>,
) -> OutputStream<R> {
    let f = f.clone();

    Box::pin(x_mon.map(move |x| f(x)))
}

pub trait CloneFn2<S: StreamData, R: StreamData, U: StreamData>:
    Fn(S, R) -> U + Clone + 'static
{
}
impl<T, S: StreamData, R: StreamData, U: StreamData> CloneFn2<S, R, U> for T where
    T: Fn(S, R) -> U + Clone + 'static
{
}

pub fn lift2<S: StreamData, R: StreamData, U: StreamData>(
    f: impl CloneFn2<S, R, U>,
    x_mon: OutputStream<S>,
    y_mon: OutputStream<R>,
) -> OutputStream<U> {
    let f = f.clone();
    Box::pin(x_mon.zip(y_mon).map(move |(x, y)| f(x, y)))
}

pub trait CloneFn3<S: StreamData, R: StreamData, U: StreamData, V: StreamData>:
    Fn(S, R, U) -> V + Clone + 'static
{
}
impl<T, S: StreamData, R: StreamData, U: StreamData, V: StreamData> CloneFn3<S, R, U, V> for T where
    T: Fn(S, R, U) -> V + Clone + 'static
{
}

pub fn lift3<S: StreamData, R: StreamData, U: StreamData, V: StreamData>(
    f: impl CloneFn3<S, R, V, U>,
    x_mon: OutputStream<S>,
    y_mon: OutputStream<R>,
    z_mon: OutputStream<V>,
) -> OutputStream<U> {
    let f = f.clone();

    Box::pin(
        x_mon
            .zip(y_mon)
            .zip(z_mon)
            .map(move |((x, y), z)| f(x, y, z)),
    ) as LocalBoxStream<'static, U>
}

pub fn and(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    lift2(
        |x, y| Value::Bool(x == Value::Bool(true) && y == Value::Bool(true)),
        x,
        y,
    )
}

pub fn or(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    lift2(
        |x, y| Value::Bool(x == Value::Bool(true) || y == Value::Bool(true)),
        x,
        y,
    )
}

pub fn not(x: OutputStream<Value>) -> OutputStream<Value> {
    lift1(|x| Value::Bool(x == Value::Bool(false)), x)
}

pub fn eq(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    lift2(|x, y| Value::Bool(x == y), x, y)
}

pub fn le(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    lift2(
        |x, y| match (x, y) {
            (Value::Int(x), Value::Int(y)) => Value::Bool(x <= y),
            (Value::Bool(a), Value::Bool(b)) => Value::Bool(a <= b),
            _ => panic!("Invalid comparison"),
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
    lift3(
        |x, y, z| match x {
            Value::Bool(true) => y,
            Value::Bool(false) => z,
            _ => panic!("Invalid if condition"),
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
pub fn sindex(x: OutputStream<Value>, i: isize, c: Value) -> OutputStream<Value> {
    let c = c.clone();
    let n = i.abs() as usize;
    let cs = stream::repeat(c).take(n);
    if i < 0 {
        Box::pin(cs.chain(x)) as LocalBoxStream<'static, Value>
    } else {
        Box::pin(x.skip(n).chain(cs)) as LocalBoxStream<'static, Value>
    }
}

pub fn plus(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    lift2(
        |x, y| match (x, y) {
            (Value::Int(x), Value::Int(y)) => Value::Int(x + y),
            (x, y) => panic!("Invalid addition with types: {:?}, {:?}", x, y),
        },
        x,
        y,
    )
}

pub fn minus(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    lift2(
        |x, y| match (x, y) {
            (Value::Int(x), Value::Int(y)) => Value::Int(x - y),
            _ => panic!("Invalid subtraction"),
        },
        x,
        y,
    )
}

pub fn mult(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    lift2(
        |x, y| match (x, y) {
            (Value::Int(x), Value::Int(y)) => Value::Int(x * y),
            _ => panic!("Invalid multiplication"),
        },
        x,
        y,
    )
}

pub fn div(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    lift2(
        |x, y| match (x, y) {
            (Value::Int(x), Value::Int(y)) => Value::Int(x / y),
            _ => panic!("Invalid multiplication"),
        },
        x,
        y,
    )
}

pub fn concat(x: OutputStream<Value>, y: OutputStream<Value>) -> OutputStream<Value> {
    lift2(
        |x, y| match (x, y) {
            (Value::Str(x), Value::Str(y)) => {
                // ConcreteStreamData::Str(format!("{x}{y}").into());
                Value::Str(format!("{x}{y}"))
            }
            _ => panic!("Invalid concatenation"),
        },
        x,
        y,
    )
}

pub fn eval(
    ctx: &dyn StreamContext<Value>,
    mut eval_stream: OutputStream<Value>,
    history_length: usize,
) -> OutputStream<Value> {
    // Create a subcontext with a history window length
    let mut subcontext = ctx.subcontext(history_length);

    // Build an output stream for eval of x over the subcontext
    Box::pin(stream! {
            // Store the previous value of the stream we are evaluating so we can
            // check when it changes
            struct PrevData {
                // The previous property provided
                eval_val: Value,
                // The output stream for eval
                eval_output_stream: OutputStream<Value>
            }
            let mut prev_data: Option<PrevData> = None;
            while let Some(current) = eval_stream.next().await {
                // If we have a previous value and it is the same as the current
                // value or if the current value is unknown (not provided),
            // continue using the existing stream as our output
            if let Some(prev_data) = &mut prev_data {
                if prev_data.eval_val == current || current == Value::Unknown {
                    // Advance the subcontext to make a new set of input values
                    // available for the eval stream
                    // info!("advancing eval clock");
                    subcontext.lazy_advance_clock().await;

                    if let Some(eval_res) = prev_data.eval_output_stream.next().await {
                        yield eval_res;
                        continue;
                    } else {
                        return;
                    }
                }
            }
            match current {
                Value::Unknown => {
                    // Consume a sample from the subcontext but return Unknown (aka. Waiting)
                    subcontext.advance_clock().await;
                    yield Value::Unknown;
                }
                Value::Str(s) => {
                    let expr = lola_expression.parse_next(&mut s.as_str())
                        .expect("Invalid eval str");
                    let mut eval_output_stream = UntimedLolaSemantics::to_async_stream(expr, subcontext.upcast());
                    // Advance the subcontext to make a new set of input values
                    // available for the eval stream
                    subcontext.lazy_advance_clock().await;
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
                cur => panic!("Invalid eval property type {:?}", cur)
            }
        }
    })
}

pub fn var(ctx: &dyn StreamContext<Value>, x: VarName) -> OutputStream<Value> {
    match ctx.var(&x) {
        Some(x) => x,
        None => {
            let VarName(x) = x;
            panic!("Variable {} not found", x)
        }
    }
}

// Defer for an UntimedLolaExpression using the lola_expression parser
#[instrument(skip(ctx, prop_stream))]
pub fn defer(
    ctx: &dyn StreamContext<Value>,
    mut prop_stream: OutputStream<Value>,
    history_length: usize,
) -> OutputStream<Value> {
    /* Subcontext with current values only*/
    let mut subcontext = ctx.subcontext(history_length);
    // let mut prog = subcontext.progress_sender.subscriber();

    Box::pin(stream! {
        let mut eval_output_stream: Option<OutputStream<Value>> = None;
        let mut i = 0;

        // Yield Unknown until we have a value to evaluate, then evaluate it
        while let Some(current) = prop_stream.next().await {
            debug!(?i, ?current, "Defer");
            match current {
                Value::Str(defer_s) => {
                    // We have a string to evaluate so do so
                    let defer_parse = &mut defer_s.as_str();
                    let expr = lola_expression.parse_next(defer_parse)
                        .expect("Invalid eval str");
                    eval_output_stream = Some(UntimedLolaSemantics::to_async_stream(expr, subcontext.upcast()));
                    debug!(defer_s, "Evaluated defer string");
                    subcontext.start_auto_clock().await;
                    break;
                }
                Value::Unknown => {
                    // Consume a sample from the subcontext but return Unknown (aka. Waiting)
                    info!("Defer waiting on unknown");
                    if i >= history_length {
                        info!(?i, ?history_length, "Advancing subcontext to clean history");
                        subcontext.advance_clock().await;
                    }
                    i += 1;
                    yield Value::Unknown;
                }
                _ => panic!("Invalid defer property type {:?}", current)
            }
        }

        let eval_output_stream = eval_output_stream.expect("No eval stream");

        // Wind forward the stream to the current time
        let time_progressed = i.min(history_length);
        debug!(?i, ?time_progressed, ?history_length, "Time progressed");
        // subcontext.advance_clock();
        let mut eval_output_stream = eval_output_stream.skip(time_progressed);

        // Yield the saved value until the inner stream is done
        while let Some(eval_res) = eval_output_stream.next().await {
            yield eval_res;
        }
    })
}

// Update for a synchronized language - in this case UntimedLolaSemantics.
// We use Unknown for simulating no data on the stream
pub fn update(mut x: OutputStream<Value>, mut y: OutputStream<Value>) -> OutputStream<Value> {
    return Box::pin(stream! {
        while let (Some(x_val), Some(y_val)) = join!(x.next(), y.next()) {
            match (x_val, y_val) {
                (x_val, Value::Unknown) => {
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
    });
}

pub fn default(x: OutputStream<Value>, d: OutputStream<Value>) -> OutputStream<Value> {
    let xs = x
        .zip(d)
        .map(|(x, d)| if x == Value::Unknown { d } else { x });
    Box::pin(xs) as LocalBoxStream<'static, Value>
}

pub fn when(mut x: OutputStream<Value>) -> OutputStream<Value> {
    Box::pin(stream! {
        while let Some(x_val) = x.next().await {
            if x_val == Value::Unknown {
                yield Value::Bool(false);
            } else {
                yield Value::Bool(true);
                break;
            }
        }
        while let Some(_) = x.next().await {
            yield Value::Bool(true);
        }
    })
}

pub fn list(mut xs: Vec<OutputStream<Value>>) -> OutputStream<Value> {
    Box::pin(stream! {
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
                        yield Value::List(val.to_vec());
                    } else {
                        panic!("List is empty");
                    }
                }
                l => panic!("Invalid list tail. Expected List expression. Received: List.tail({:?})", l)
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{SyncStreamContext, Value, VarName};
    use async_trait::async_trait;
    use futures::stream;
    use macro_rules_attribute::apply;
    use smol_macros::test as smol_test;
    use std::collections::BTreeMap;
    use std::iter::FromIterator;
    use std::ops::{Deref, DerefMut};
    use std::sync::Mutex;
    use test_log::test;

    pub struct VarMap(BTreeMap<VarName, Mutex<Vec<Value>>>);
    impl Deref for VarMap {
        type Target = BTreeMap<VarName, Mutex<Vec<Value>>>;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl DerefMut for VarMap {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }

    #[allow(dead_code)] // Only used in test code
    struct MockContext {
        xs: VarMap,
    }

    impl FromIterator<(VarName, Vec<Value>)> for VarMap {
        fn from_iter<I: IntoIterator<Item = (VarName, Vec<Value>)>>(iter: I) -> Self {
            let mut map = VarMap(BTreeMap::new());
            for (key, vec) in iter {
                map.insert(key, Mutex::new(vec));
            }
            map
        }
    }

    impl StreamContext<Value> for MockContext {
        fn var(&self, x: &VarName) -> Option<OutputStream<Value>> {
            let mutex = self.xs.get(x)?;
            if let Ok(vec) = mutex.lock() {
                Some(Box::pin(stream::iter(vec.clone())))
            } else {
                std::panic!("Mutex was poisoned");
            }
        }
        fn subcontext(&self, history_length: usize) -> Box<dyn SyncStreamContext<Value>> {
            // Create new xs with only the `history_length` latest values for the Vec
            let new_xs = self
                .xs
                .iter()
                .map(|(key, mutex)| {
                    if let Ok(vec) = mutex.lock() {
                        let start = if vec.len() > history_length {
                            vec.len() - history_length
                        } else {
                            0
                        };
                        let latest_elements = vec[start..].to_vec();
                        (key.clone(), latest_elements)
                    } else {
                        std::panic!("Mutex was poisoned");
                    }
                })
                .collect();
            Box::new(MockContext { xs: new_xs })
        }
    }

    #[async_trait(?Send)]
    impl SyncStreamContext<Value> for MockContext {
        async fn advance_clock(&mut self) {
            // Remove the first element from each Vec (the oldest value)
            for (_, vec_mutex) in self.xs.iter() {
                if let Ok(mut vec) = vec_mutex.lock() {
                    if !vec.is_empty() {
                        let _ = vec.remove(0);
                    }
                } else {
                    std::panic!("Mutex was poisoned");
                }
            }
            return;
        }

        async fn lazy_advance_clock(&mut self) {
            self.advance_clock().await;
        }

        fn clock(&self) -> usize {
            0
        }

        fn is_clock_started(&self) -> bool {
            true
        }

        async fn start_auto_clock(&mut self) {
            // Do nothing
        }

        fn upcast(&self) -> &dyn StreamContext<Value> {
            self
        }
    }

    #[test(apply(smol_test))]
    async fn test_mock_subcontext() {
        let map: VarMap = vec![(VarName("x".into()), vec![1.into(), 2.into(), 3.into()]).into()]
            .into_iter()
            .collect();
        let ctx = MockContext { xs: map };
        let subctx = ctx.subcontext(1);
        // This does not properly model a subcontext since need values need to
        // be able to arrive, after which they will be pruned down to the
        // history length on .advance()
        let res: Vec<Value> = subctx.var(&VarName("x".into())).unwrap().collect().await;
        assert_eq!(res, vec![3.into()]);
    }

    #[test(apply(smol_test))]
    async fn test_not() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![Value::Bool(true), false.into()]));
        let res: Vec<Value> = not(x).collect().await;
        let exp: Vec<Value> = vec![Value::Bool(false), Value::Bool(true)];
        assert_eq!(res, exp)
    }

    #[test(apply(smol_test))]
    async fn test_plus() {
        let x: OutputStream<Value> =
            Box::pin(stream::iter(vec![Value::Int(1), 3.into()].into_iter()));
        let y: OutputStream<Value> = Box::pin(stream::iter(vec![2.into(), 4.into()].into_iter()));
        let z: Vec<Value> = vec![3.into(), 7.into()];
        let res: Vec<Value> = plus(x, y).collect().await;
        assert_eq!(res, z);
    }

    #[test(apply(smol_test))]
    async fn test_str_concat() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec!["hello ".into(), "olleh ".into()]));
        let y: OutputStream<Value> = Box::pin(stream::iter(vec!["world".into(), "dlrow".into()]));
        let exp = vec!["hello world".into(), "olleh dlrow".into()];
        let res: Vec<Value> = concat(x, y).collect().await;
        assert_eq!(res, exp)
    }

    #[test(apply(smol_test))]
    async fn test_eval() {
        let e: OutputStream<Value> = Box::pin(stream::iter(vec!["x + 1".into(), "x + 2".into()]));
        let map: VarMap = vec![(VarName("x".into()), vec![1.into(), 2.into()]).into()]
            .into_iter()
            .collect();
        let ctx = MockContext { xs: map };
        let res: Vec<Value> = eval(&ctx, e, 10).collect().await;
        let exp: Vec<Value> = vec![2.into(), 4.into()];
        assert_eq!(res, exp)
    }

    #[test(apply(smol_test))]
    async fn test_eval_x_squared() {
        // This test is interesting since we use x twice in the eval strings
        let e: OutputStream<Value> = Box::pin(stream::iter(vec!["x * x".into(), "x * x".into()]));
        let map: VarMap = vec![(VarName("x".into()), vec![2.into(), 3.into()]).into()]
            .into_iter()
            .collect();
        let ctx = MockContext { xs: map };
        let res: Vec<Value> = eval(&ctx, e, 10).collect().await;
        let exp: Vec<Value> = vec![4.into(), 9.into()];
        assert_eq!(res, exp)
    }

    #[test(apply(smol_test))]
    async fn test_eval_with_start_unknown() {
        let e: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Unknown,
            "x + 1".into(),
            "x + 2".into(),
        ]));
        let map: VarMap = vec![(VarName("x".into()), vec![1.into(), 2.into(), 3.into()]).into()]
            .into_iter()
            .collect();
        let ctx = MockContext { xs: map };
        let res: Vec<Value> = eval(&ctx, e, 10).collect().await;
        // Continues evaluating to x+1 until we get a non-unknown value
        let exp: Vec<Value> = vec![Value::Unknown, 3.into(), 5.into()];
        assert_eq!(res, exp)
    }

    #[test(apply(smol_test))]
    async fn test_eval_with_mid_unknown() {
        let e: OutputStream<Value> = Box::pin(stream::iter(vec![
            "x + 1".into(),
            Value::Unknown,
            "x + 2".into(),
        ]));
        let map: VarMap = vec![(VarName("x".into()), vec![1.into(), 2.into(), 3.into()]).into()]
            .into_iter()
            .collect();
        let ctx = MockContext { xs: map };
        let res: Vec<Value> = eval(&ctx, e, 10).collect().await;
        // Continues evaluating to x+1 until we get a non-unknown value
        let exp: Vec<Value> = vec![2.into(), 3.into(), 5.into()];
        assert_eq!(res, exp)
    }

    #[test(apply(smol_test))]
    async fn test_defer() {
        // Notice that even though we first say "x + 1", "x + 2", it continues evaluating "x + 1"
        let e: OutputStream<Value> = Box::pin(stream::iter(vec!["x + 1".into(), "x + 2".into()]));
        let map: VarMap = vec![(VarName("x".into()), vec![1.into(), 2.into()]).into()]
            .into_iter()
            .collect();
        let ctx = MockContext { xs: map };
        let res: Vec<Value> = defer(&ctx, e, 2).collect().await;
        let exp: Vec<Value> = vec![2.into(), 3.into()];
        assert_eq!(res, exp)
    }
    #[test(apply(smol_test))]
    async fn test_defer_x_squared() {
        // This test is interesting since we use x twice in the eval strings
        let e: OutputStream<Value> =
            Box::pin(stream::iter(vec!["x * x".into(), "x * x + 1".into()]));
        let map: VarMap = vec![(VarName("x".into()), vec![2.into(), 3.into()]).into()]
            .into_iter()
            .collect();
        let ctx = MockContext { xs: map };
        let res: Vec<Value> = defer(&ctx, e, 10).collect().await;
        let exp: Vec<Value> = vec![4.into(), 9.into()];
        assert_eq!(res, exp)
    }

    #[test(apply(smol_test))]
    async fn test_defer_unknown() {
        // Using unknown to represent no data on the stream
        let e: OutputStream<Value> = Box::pin(stream::iter(vec![Value::Unknown, "x + 1".into()]));
        let map: VarMap = vec![(VarName("x".into()), vec![2.into(), 3.into()]).into()]
            .into_iter()
            .collect();
        let ctx = MockContext { xs: map };
        let res = defer(&ctx, e, 10).collect::<Vec<Value>>().await;
        let exp: Vec<Value> = vec![Value::Unknown, 4.into()];
        assert_eq!(res, exp)
    }

    #[test(apply(smol_test))]
    async fn test_defer_unknown2() {
        // Unknown followed by property followed by unknown returns [U; val; val].
        let e = Box::pin(stream::iter(vec![
            Value::Unknown,
            "x + 1".into(),
            Value::Unknown,
        ])) as OutputStream<Value>;
        let map: VarMap = vec![(VarName("x".into()), vec![2.into(), 3.into(), 4.into()]).into()]
            .into_iter()
            .collect();
        let ctx = MockContext { xs: map };
        let res = defer(&ctx, e, 10).collect::<Vec<Value>>().await;
        let exp: Vec<Value> = vec![Value::Unknown, 4.into(), 5.into()];
        assert_eq!(res, exp)
    }

    #[test(apply(smol_test))]
    async fn test_update_both_init() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec!["x0".into(), "x1".into()]));
        let y: OutputStream<Value> = Box::pin(stream::iter(vec!["y0".into(), "y1".into()]));
        let res: Vec<Value> = update(x, y).collect().await;
        let exp: Vec<Value> = vec!["y0".into(), "y1".into()];
        assert_eq!(res, exp)
    }

    #[test(apply(smol_test))]
    async fn test_default_no_unknown() {
        let x: OutputStream<Value> =
            Box::pin(stream::iter(vec!["x0".into(), "x1".into(), "x2".into()]));
        let d: OutputStream<Value> = Box::pin(stream::repeat("d".into()));
        let res: Vec<Value> = default(x, d).collect().await;
        let exp: Vec<Value> = vec!["x0".into(), "x1".into(), "x2".into()];
        assert_eq!(res, exp)
    }

    #[test(apply(smol_test))]
    async fn test_default_all_unknown() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Unknown,
            Value::Unknown,
            Value::Unknown,
        ]));
        let d: OutputStream<Value> = Box::pin(stream::repeat("d".into()));
        let res: Vec<Value> = default(x, d).collect().await;
        let exp: Vec<Value> = vec!["d".into(), "d".into(), "d".into()];
        assert_eq!(res, exp)
    }

    #[test(apply(smol_test))]
    async fn test_default_one_unknown() {
        let x: OutputStream<Value> =
            Box::pin(stream::iter(vec!["x0".into(), Value::Unknown, "x2".into()]));
        let d: OutputStream<Value> = Box::pin(stream::repeat("d".into()));
        let res: Vec<Value> = default(x, d).collect().await;
        let exp: Vec<Value> = vec!["x0".into(), "d".into(), "x2".into()];
        assert_eq!(res, exp)
    }

    #[test(apply(smol_test))]
    async fn test_update_first_x_then_y() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![
            "x0".into(),
            "x1".into(),
            "x2".into(),
            "x3".into(),
        ]));
        let y: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Unknown,
            "y1".into(),
            Value::Unknown,
            "y3".into(),
        ]));
        let res: Vec<Value> = update(x, y).collect().await;
        let exp: Vec<Value> = vec!["x0".into(), "y1".into(), Value::Unknown, "y3".into()];
        assert_eq!(res, exp)
    }

    #[test(apply(smol_test))]
    async fn test_update_first_y_then_x() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Unknown,
            "x1".into(),
            Value::Unknown,
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

    #[test(apply(smol_test))]
    async fn test_update_neither() {
        use Value::Unknown;
        let x: OutputStream<Value> =
            Box::pin(stream::iter(vec![Unknown, Unknown, Unknown, Unknown]));
        let y: OutputStream<Value> =
            Box::pin(stream::iter(vec![Unknown, Unknown, Unknown, Unknown]));
        let res: Vec<Value> = update(x, y).collect().await;
        let exp: Vec<Value> = vec![Unknown, Unknown, Unknown, Unknown];
        assert_eq!(res, exp)
    }

    #[test(apply(smol_test))]
    async fn test_update_first_x_then_y_value_sync() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Unknown,
            "x0".into(),
            "x1".into(),
            "x2".into(),
            "x3".into(),
        ]));
        let y: OutputStream<Value> = Box::pin(stream::iter(vec![
            Value::Unknown,
            "y1".into(),
            Value::Unknown,
            "y3".into(),
        ]));
        let res: Vec<Value> = update(x, y).collect().await;
        let exp: Vec<Value> = vec![Value::Unknown, "y1".into(), Value::Unknown, "y3".into()];
        assert_eq!(res, exp)
    }

    #[test(apply(smol_test))]
    async fn test_list() {
        let x: Vec<OutputStream<Value>> = vec![
            Box::pin(stream::iter(vec![1.into(), 2.into()])),
            Box::pin(stream::iter(vec![3.into(), 4.into()])),
        ];
        let res: Vec<Value> = list(x).collect().await;
        let exp: Vec<Value> = vec![
            Value::List(vec![1.into(), 3.into()]),
            Value::List(vec![2.into(), 4.into()]),
        ];
        assert_eq!(res, exp);
    }

    #[test(apply(smol_test))]
    async fn test_list_exprs() {
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
            Value::List(vec![4.into(), "Hello World".into()]),
            Value::List(vec![6.into(), "Goddag Verden".into()]),
        ];
        assert_eq!(res, exp);
    }

    #[test(apply(smol_test))]
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

    #[test(apply(smol_test))]
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

    #[test(apply(smol_test))]
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

    #[test(apply(smol_test))]
    async fn test_list_idx_var() {
        let x: Vec<OutputStream<Value>> = vec![
            Box::pin(stream::iter(vec![1.into(), 2.into()])),
            Box::pin(stream::iter(vec![3.into(), 4.into()])),
        ];
        let map: VarMap = vec![(VarName("y".into()), vec![0.into(), 1.into()]).into()]
            .into_iter()
            .collect();
        let ctx = MockContext { xs: map };
        let i: OutputStream<Value> = var(&ctx, VarName("y".into()));
        let res: Vec<Value> = lindex(list(x), i).collect().await;
        let exp: Vec<Value> = vec![1.into(), 4.into()];
        assert_eq!(res, exp)
    }

    #[test(apply(smol_test))]
    async fn test_list_append() {
        let x: Vec<OutputStream<Value>> = vec![
            Box::pin(stream::iter(vec![1.into(), 2.into()])),
            Box::pin(stream::iter(vec![3.into(), 4.into()])),
        ];
        let y: OutputStream<Value> = Box::pin(stream::iter(vec![5.into(), 6.into()]));
        let res: Vec<Value> = lappend(list(x), y).collect().await;
        let exp: Vec<Value> = vec![
            Value::List(vec![1.into(), 3.into(), 5.into()]),
            Value::List(vec![2.into(), 4.into(), 6.into()]),
        ];
        assert_eq!(res, exp);
    }

    #[test(apply(smol_test))]
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
            Value::List(vec![1.into(), 3.into(), 5.into(), 7.into()]),
            Value::List(vec![2.into(), 4.into(), 6.into(), 8.into()]),
        ];
        assert_eq!(res, exp);
    }

    #[test(apply(smol_test))]
    async fn test_list_head() {
        let x: Vec<OutputStream<Value>> = vec![
            Box::pin(stream::iter(vec![1.into(), 2.into()])),
            Box::pin(stream::iter(vec![3.into(), 4.into()])),
        ];
        let res: Vec<Value> = lhead(list(x)).collect().await;
        let exp: Vec<Value> = vec![Value::Int(1.into()), Value::Int(2.into())];
        assert_eq!(res, exp);
    }

    #[test(apply(smol_test))]
    async fn test_list_tail() {
        let x: Vec<OutputStream<Value>> = vec![
            Box::pin(stream::iter(vec![1.into(), 2.into()])),
            Box::pin(stream::iter(vec![3.into(), 4.into()])),
            Box::pin(stream::iter(vec![5.into(), 6.into()])),
        ];
        let res: Vec<Value> = ltail(list(x)).collect().await;
        let exp: Vec<Value> = vec![
            Value::List(vec![3.into(), 5.into()]),
            Value::List(vec![4.into(), 6.into()]),
        ];
        assert_eq!(res, exp);
    }

    #[test(apply(smol_test))]
    async fn test_list_tail_one_el() {
        let x: Vec<OutputStream<Value>> = vec![Box::pin(stream::iter(vec![1.into(), 2.into()]))];
        let res: Vec<Value> = ltail(list(x)).collect().await;
        let exp: Vec<Value> = vec![Value::List(vec![]), Value::List(vec![])];
        assert_eq!(res, exp);
    }
}
