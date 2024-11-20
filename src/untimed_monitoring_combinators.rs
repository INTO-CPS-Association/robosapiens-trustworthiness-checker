use crate::core::StreamData;
use crate::core::Value;
use crate::{
    lola_expression, MonitoringSemantics, OutputStream, StreamContext, UntimedLolaSemantics,
    VarName,
};
use async_stream::stream;
use core::panic;
use futures::{
    stream::{self, BoxStream},
    StreamExt,
};
use std::ops::Deref;
use tokio::join;
use winnow::Parser;

pub trait CloneFn1<T: StreamData, S: StreamData>:
    Fn(T) -> S + Clone + Sync + Send + 'static
{
}
impl<T, S: StreamData, R: StreamData> CloneFn1<S, R> for T where
    T: Fn(S) -> R + Sync + Send + Clone + 'static
{
}

pub fn lift1<S: StreamData, R: StreamData>(
    f: impl CloneFn1<S, R>,
    x_mon: OutputStream<S>,
) -> OutputStream<R> {
    let f = f.clone();

    Box::pin(x_mon.map(move |x| f(x)))
}

pub trait CloneFn2<S: StreamData, R: StreamData, U: StreamData>:
    Fn(S, R) -> U + Clone + Sync + Send + 'static
{
}
impl<T, S: StreamData, R: StreamData, U: StreamData> CloneFn2<S, R, U> for T where
    T: Fn(S, R) -> U + Clone + Sync + Send + 'static
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
    Fn(S, R, U) -> V + Clone + Sync + Send + 'static
{
}
impl<T, S: StreamData, R: StreamData, U: StreamData, V: StreamData> CloneFn3<S, R, U, V> for T where
    T: Fn(S, R, U) -> V + Clone + Sync + Send + 'static
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
    ) as BoxStream<'static, U>
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
    lift1(|x| Value::Bool(x == Value::Bool(true)), x)
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

pub fn index(x: OutputStream<Value>, i: isize, c: Value) -> OutputStream<Value> {
    let c = c.clone();
    if i < 0 {
        let n = i.abs() as usize;
        let cs = stream::repeat(c).take(n);
        Box::pin(cs.chain(x)) as BoxStream<'static, Value>
    } else {
        let n = i as usize;
        Box::pin(x.skip(n)) as BoxStream<'static, Value>
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
    x: OutputStream<Value>,
    history_length: usize,
) -> OutputStream<Value> {
    // Create a subcontext with a history window length of 10
    let subcontext = ctx.subcontext(history_length);
    /*unfold() creates a Stream from a seed value.*/
    Box::pin(stream::unfold(
        (subcontext, x, None::<(Value, OutputStream<Value>)>),
        |(subcontext, mut x, last)| async move {
            /* x.next() returns None if we are done unfolding. Return in that case.*/
            let current = x.next().await?;

            // If the evaled statement has not stopped, continue using the
            // existing stream
            if let Some((prev, mut es)) = last {
                if prev == current {
                    // println!("prev == current == {:?}", current);
                    subcontext.advance();
                    let eval_res = es.next().await;
                    // println!("returning val from existing stream: {:?}", eval_res);
                    return match eval_res {
                        Some(eval_res) => {
                            // println!("eval producing {:?}", eval_res);
                            Some((eval_res, (subcontext, x, Some((current, es)))))
                        }
                        None => {
                            println!("Eval stream ended");
                            None
                        }
                    };
                }
            }

            match current {
                Value::Str(s) => {
                    let s_parse = &mut s.as_str();
                    let expr = match lola_expression.parse_next(s_parse) {
                        Ok(expr) => expr,
                        Err(_) => unimplemented!("Invalid eval str"),
                    };
                    let mut es = UntimedLolaSemantics::to_async_stream(expr, subcontext.deref());
                    // println!("new eval stream");
                    subcontext.advance();
                    let eval_res = es.next().await?;
                    // println!("eval producing {:?}", eval_res);
                    return Some((eval_res, (subcontext, x, Some((Value::Str(s), es)))));
                }
                x => {
                    unimplemented!("Invalid eval type {:?}", x)
                }
            }
        },
    )) as OutputStream<Value>
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
pub fn defer(
    ctx: &dyn StreamContext<Value>,
    prop_stream: OutputStream<Value>,
    history_length: usize,
) -> OutputStream<Value> {
    /* Subcontext with current values only*/
    let subcontext = ctx.subcontext(history_length);
    /*unfold() creates a Stream from a seed value.*/
    Box::pin(stream::unfold(
        (subcontext, prop_stream, None::<Value>),
        |(subcontext, mut x, saved)| async move {
            /* x.next() returns None if we are done unfolding. Return in that case.*/
            let current = x.next().await?;
            /* If we have a saved state then use that otherwise use current */
            let defer_str = saved.unwrap_or_else(|| current);

            match defer_str {
                Value::Str(defer_s) => {
                    let defer_parse = &mut defer_s.as_str();
                    let expr = match lola_expression.parse_next(defer_parse) {
                        Ok(expr) => expr,
                        Err(_) => unimplemented!("Invalid eval str"),
                    };
                    let mut es = UntimedLolaSemantics::to_async_stream(expr, subcontext.deref());
                    let eval_res = es.next().await?;
                    subcontext.advance();
                    return Some((eval_res, (subcontext, x, Some(Value::Str(defer_s)))));
                }
                Value::Unknown => {
                    // Consume a sample from the subcontext but return Unknown (aka. Waiting)
                    subcontext.advance();
                    Some((Value::Unknown, (subcontext, x, None)))
                }
                _ => panic!("We did not have memory and defer_str was not a Str"),
            }
        },
    ))
}

// Update for a synchronized language - in this case UntimedLolaSemantics.
// We use Unknown for simulating no data on the stream
pub fn update(mut x: OutputStream<Value>, mut y: OutputStream<Value>) -> OutputStream<Value> {
    return Box::pin(stream! {
        let mut skip_sync = false;

        // Pre phase
        println!("Pre phase");
        loop {
            let x_val = x.next().await;
            match x_val {
                Some(x_val) => {
                    if x_val == Value::Unknown {
                        // Keep yielding from x until we get a value
                        yield x_val;
                    } else {
                        // We are done with the pre phase as x is ready
                        match y.next().await {
                            Some(Value::Unknown) => {
                                // Move to sync phase
                                yield x_val;
                                break;
                            }
                            Some(y_val) => {
                                // Move to post phase
                                yield y_val;
                                skip_sync = true;
                                break;
                            }
                            None => return
                        }
                    }
                }
                None => return
            }
        }

        // Sync phase
        if !skip_sync {
            // If y_val is unknown start the syncing phase where we return x
            // until y is ready
            while let (Some(x_val), Some(y_val)) = join!(x.next(), y.next()) {
                match y_val {
                    // If y_val is unknown go into syncing phase
                    Value::Unknown => {
                        yield x_val;
                    }
                    // Otherwise go directly to post
                    y_val => {
                        yield y_val;
                        break;
                    }
                }
            }
        }

        // Post phase
        println!("Post phase");
        while let Some(y_val) = y.next().await {
            yield y_val;
        }

        return;
    });
}

mod tests {
    use super::*;
    use crate::core::{Value, VarName};
    use futures::stream;
    use std::collections::BTreeMap;
    use std::iter::FromIterator;
    use std::ops::{Deref, DerefMut};
    use std::sync::Mutex;

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
        fn subcontext(&self, history_length: usize) -> Box<dyn StreamContext<Value>> {
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
        fn advance(&self) {
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
    }

    #[tokio::test]
    async fn test_plus() {
        let x: OutputStream<Value> =
            Box::pin(stream::iter(vec![Value::Int(1), 3.into()].into_iter()));
        let y: OutputStream<Value> = Box::pin(stream::iter(vec![2.into(), 4.into()].into_iter()));
        let z: Vec<Value> = vec![3.into(), 7.into()];
        let res: Vec<Value> = plus(x, y).collect().await;
        assert_eq!(res, z);
    }

    #[tokio::test]
    async fn test_str_concat() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec!["hello ".into(), "olleh ".into()]));
        let y: OutputStream<Value> = Box::pin(stream::iter(vec!["world".into(), "dlrow".into()]));
        let exp = vec!["hello world".into(), "olleh dlrow".into()];
        let res: Vec<Value> = concat(x, y).collect().await;
        assert_eq!(res, exp)
    }

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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
    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
    async fn test_update_both_init() {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec!["x0".into(), "x1".into()]));
        let y: OutputStream<Value> = Box::pin(stream::iter(vec!["y0".into(), "y1".into()]));
        let res: Vec<Value> = update(x, y).collect().await;
        let exp: Vec<Value> = vec!["y0".into(), "y1".into()];
        assert_eq!(res, exp)
    }

    #[tokio::test]
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

    #[tokio::test]
    async fn test_update_first_y_then_x() {
        // Notice that the length of res is longer than the input streams
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
        let exp: Vec<Value> = vec![
            Value::Unknown,
            "y0".into(),
            "y1".into(),
            "y2".into(),
            "y3".into(),
        ];
        assert_eq!(res, exp)
    }

    #[tokio::test]
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

    #[tokio::test]
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
        let exp: Vec<Value> = vec![
            Value::Unknown,
            "x0".into(),
            "y1".into(),
            Value::Unknown,
            "y3".into(),
        ];
        assert_eq!(res, exp)
    }
}
