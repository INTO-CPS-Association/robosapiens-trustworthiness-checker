use crate::core::{StreamData, StreamSystem};
use crate::ConcreteStreamData;
use crate::{
    lola_expression, MonitoringSemantics, OutputStream, StreamContext, UntimedLolaSemantics,
    VarName,
};
use core::panic;
use futures::{
    stream::{self, BoxStream},
    StreamExt,
};
use std::ops::Deref;
use tokio::join;
use winnow::Parser;

use crate::ast::{UntypedLOLA, UntypedStreams};

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

pub fn and(
    x: OutputStream<ConcreteStreamData>,
    y: OutputStream<ConcreteStreamData>,
) -> OutputStream<ConcreteStreamData> {
    lift2(
        |x, y| {
            ConcreteStreamData::Bool(
                x == ConcreteStreamData::Bool(true) && y == ConcreteStreamData::Bool(true),
            )
        },
        x,
        y,
    )
}

pub fn or(
    x: OutputStream<ConcreteStreamData>,
    y: OutputStream<ConcreteStreamData>,
) -> OutputStream<ConcreteStreamData> {
    lift2(
        |x, y| {
            ConcreteStreamData::Bool(
                x == ConcreteStreamData::Bool(true) || y == ConcreteStreamData::Bool(true),
            )
        },
        x,
        y,
    )
}

pub fn not(x: OutputStream<ConcreteStreamData>) -> OutputStream<ConcreteStreamData> {
    lift1(
        |x| ConcreteStreamData::Bool(x == ConcreteStreamData::Bool(true)),
        x,
    )
}

pub fn eq(
    x: OutputStream<ConcreteStreamData>,
    y: OutputStream<ConcreteStreamData>,
) -> OutputStream<ConcreteStreamData> {
    lift2(|x, y| ConcreteStreamData::Bool(x == y), x, y)
}

pub fn le(
    x: OutputStream<ConcreteStreamData>,
    y: OutputStream<ConcreteStreamData>,
) -> OutputStream<ConcreteStreamData> {
    lift2(
        |x, y| match (x, y) {
            (ConcreteStreamData::Int(x), ConcreteStreamData::Int(y)) => {
                ConcreteStreamData::Bool(x <= y)
            }
            (ConcreteStreamData::Bool(a), ConcreteStreamData::Bool(b)) => {
                ConcreteStreamData::Bool(a <= b)
            }
            _ => panic!("Invalid comparison"),
        },
        x,
        y,
    )
}

pub fn val(x: ConcreteStreamData) -> OutputStream<ConcreteStreamData> {
    Box::pin(stream::repeat(x.clone()))
}

// Should this return a dyn ConcreteStreamData?
pub fn if_stm(
    x: OutputStream<ConcreteStreamData>,
    y: OutputStream<ConcreteStreamData>,
    z: OutputStream<ConcreteStreamData>,
) -> OutputStream<ConcreteStreamData> {
    lift3(
        |x, y, z| match x {
            ConcreteStreamData::Bool(true) => y,
            ConcreteStreamData::Bool(false) => z,
            _ => panic!("Invalid if condition"),
        },
        x,
        y,
        z,
    )
}

pub fn index(
    x: OutputStream<ConcreteStreamData>,
    i: isize,
    c: ConcreteStreamData,
) -> OutputStream<ConcreteStreamData> {
    let c = c.clone();
    if i < 0 {
        let n: usize = (-i).try_into().unwrap();
        let cs = stream::repeat(c).take(n);
        Box::pin(cs.chain(x)) as BoxStream<'static, ConcreteStreamData>
    } else {
        let n: usize = i.try_into().unwrap();
        Box::pin(x.skip(n)) as BoxStream<'static, ConcreteStreamData>
    }
}

pub fn plus(
    x: OutputStream<ConcreteStreamData>,
    y: OutputStream<ConcreteStreamData>,
) -> OutputStream<ConcreteStreamData> {
    lift2(
        |x, y| match (x, y) {
            (ConcreteStreamData::Int(x), ConcreteStreamData::Int(y)) => {
                ConcreteStreamData::Int(x + y)
            }
            (ConcreteStreamData::Str(x), ConcreteStreamData::Str(y)) => {
                // ConcreteStreamData::Str(format!("{x}{y}").into());
                ConcreteStreamData::Str(format!("{x}{y}"))
            }
            (x, y) => panic!("Invalid addition with types: {:?}, {:?}", x, y),
        },
        x,
        y,
    )
}

pub fn minus(
    x: OutputStream<ConcreteStreamData>,
    y: OutputStream<ConcreteStreamData>,
) -> OutputStream<ConcreteStreamData> {
    lift2(
        |x, y| match (x, y) {
            (ConcreteStreamData::Int(x), ConcreteStreamData::Int(y)) => {
                ConcreteStreamData::Int(x - y)
            }
            _ => panic!("Invalid subtraction"),
        },
        x,
        y,
    )
}

pub fn mult(
    x: OutputStream<ConcreteStreamData>,
    y: OutputStream<ConcreteStreamData>,
) -> OutputStream<ConcreteStreamData> {
    lift2(
        |x, y| match (x, y) {
            (ConcreteStreamData::Int(x), ConcreteStreamData::Int(y)) => {
                ConcreteStreamData::Int(x * y)
            }
            _ => panic!("Invalid multiplication"),
        },
        x,
        y,
    )
}

pub fn eval(
    ctx: &dyn StreamContext<UntypedStreams>,
    x: OutputStream<ConcreteStreamData>,
    history_length: usize,
) -> OutputStream<ConcreteStreamData> {
    // Create a subcontext with a history window length of 10
    let subcontext = ctx.subcontext(history_length);
    /*unfold() creates a Stream from a seed value.*/
    Box::pin(stream::unfold(
        (
            subcontext,
            x,
            None::<(ConcreteStreamData, OutputStream<ConcreteStreamData>)>,
        ),
        |(subcontext, mut x, last)| async move {
            /* x.next() returns None if we are done unfolding. Return in that case.*/
            let current = x.next().await?;

            // If the evaled statement has not stopped, continue using the
            // existing stream
            if let Some((prev, mut es)) = last {
                if prev == current {
                    println!("prev == current == {:?}", current);
                    subcontext.advance();
                    let eval_res = es.next().await;
                    println!("returning val from existing stream: {:?}", eval_res);
                    return match eval_res {
                        Some(eval_res) => Some((eval_res, (subcontext, x, Some((current, es))))),
                        None => None,
                    };
                }
            }

            match current {
                ConcreteStreamData::Str(s) => {
                    let s_parse = &mut s.as_str();
                    let expr = match lola_expression.parse_next(s_parse) {
                        Ok(expr) => expr,
                        Err(_) => unimplemented!("Invalid eval str"),
                    };
                    let mut es = UntimedLolaSemantics::to_async_stream(expr, subcontext.deref());
                    subcontext.advance();
                    let eval_res = es.next().await?;
                    return Some((
                        eval_res,
                        (subcontext, x, Some((ConcreteStreamData::Str(s), es))),
                    ));
                }
                x => {
                    unimplemented!("Invalid eval type {:?}", x)
                }
            }
        },
    )) as OutputStream<ConcreteStreamData>
}

pub fn var(
    ctx: &dyn StreamContext<UntypedStreams>,
    x: VarName,
) -> OutputStream<ConcreteStreamData> {
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
    ctx: &dyn StreamContext<UntypedStreams>,
    prop_stream: OutputStream<ConcreteStreamData>,
    history_length: usize,
) -> OutputStream<ConcreteStreamData> {
    /* Subcontext with current values only*/
    let subcontext = ctx.subcontext(history_length);
    /*unfold() creates a Stream from a seed value.*/
    Box::pin(stream::unfold(
        (subcontext, prop_stream, None::<ConcreteStreamData>),
        |(subcontext, mut x, saved)| async move {
            /* x.next() returns None if we are done unfolding. Return in that case.*/
            let current = x.next().await?;
            /* If we have a saved state then use that otherwise use current */
            let defer_str = saved.unwrap_or_else(|| current);

            match defer_str {
                ConcreteStreamData::Str(defer_s) => {
                    let defer_parse = &mut defer_s.as_str();
                    let expr = match lola_expression.parse_next(defer_parse) {
                        Ok(expr) => expr,
                        Err(_) => unimplemented!("Invalid eval str"),
                    };
                    let mut es = UntimedLolaSemantics::to_async_stream(expr, subcontext.deref());
                    let eval_res = es.next().await?;
                    subcontext.advance();
                    return Some((
                        eval_res,
                        (subcontext, x, Some(ConcreteStreamData::Str(defer_s))),
                    ));
                }
                ConcreteStreamData::Unknown => {
                    // Consume a sample from the subcontext but return Unknown (aka. Waiting)
                    subcontext.advance();
                    Some((ConcreteStreamData::Unknown, (subcontext, x, None)))
                }
                _ => panic!("We did not have memory and defer_str was not a Str"),
            }
        },
    ))
}

// Update for a synchronized language - in this case UntimedLolaSemantics.
// We use Unknown for simulating no data on the stream
pub fn update(
    x: OutputStream<ConcreteStreamData>,
    y: OutputStream<ConcreteStreamData>,
) -> OutputStream<ConcreteStreamData> {
    // Pre is x isn't ready yet
    enum Phase {
        Pre,
        Sync,
        Post,
    }
    use Phase::*;
    // Pre phase means that x is not ready yet
    // Note: Returns the three values uses by unfold below
    async fn handle_pre_phase(
        mut x: OutputStream<ConcreteStreamData>,
        mut y: OutputStream<ConcreteStreamData>,
    ) -> Option<(
        ConcreteStreamData,
        (
            OutputStream<ConcreteStreamData>,
            OutputStream<ConcreteStreamData>,
            Phase,
        ),
    )> {
        match x.next().await {
            Some(x_val) if x_val != ConcreteStreamData::Unknown => {
                let y_val = y.next().await?;
                match y_val {
                    // If y_val is unknown go into syncing phase
                    ConcreteStreamData::Unknown => Some((x_val, (x, y, Sync))),
                    // Otherwise go directly to post
                    y_val => Some((y_val, (x, y, Post))),
                }
            }
            Some(ConcreteStreamData::Unknown) => Some((ConcreteStreamData::Unknown, (x, y, Pre))),
            _ => None,
        }
    }
    // Sync phase is x is ready but y isn't
    async fn handle_sync_phase(
        mut x: OutputStream<ConcreteStreamData>,
        mut y: OutputStream<ConcreteStreamData>,
    ) -> Option<(
        ConcreteStreamData,
        (
            OutputStream<ConcreteStreamData>,
            OutputStream<ConcreteStreamData>,
            Phase,
        ),
    )> {
        let (x_next, y_next) = join!(x.next(), y.next());
        match (x_next, y_next) {
            // y is still unknown - yield x:
            (Some(x_val), Some(ConcreteStreamData::Unknown)) => Some((x_val, (x, y, Sync))),
            // first time y is known - yield y:
            (_, Some(y_val)) => Some((y_val, (x, y, Post))),
            // End of stream
            _ => None,
        }
    }
    // Post phase is y was ready - so care about x
    async fn handle_post_phase(
        x: OutputStream<ConcreteStreamData>,
        mut y: OutputStream<ConcreteStreamData>,
    ) -> Option<(
        ConcreteStreamData,
        (
            OutputStream<ConcreteStreamData>,
            OutputStream<ConcreteStreamData>,
            Phase,
        ),
    )> {
        let y_val = y.next().await?;
        Some((y_val, (x, y, Post)))
    }
    // Unfold while keeping track of phase
    Box::pin(futures::stream::unfold(
        (x, y, Pre),
        move |(x, y, phase)| async move {
            match phase {
                Pre => handle_pre_phase(x, y).await,
                Sync => handle_sync_phase(x, y).await,
                Post => handle_post_phase(x, y).await,
            }
        },
    ))
}

mod tests {
    use super::*;
    use crate::core::{ConcreteStreamData, VarName};
    use futures::stream;
    use std::collections::BTreeMap;
    use std::iter::FromIterator;
    use std::ops::{Deref, DerefMut};
    use std::sync::Mutex;

    pub struct VarMap(BTreeMap<VarName, Mutex<Vec<ConcreteStreamData>>>);
    impl Deref for VarMap {
        type Target = BTreeMap<VarName, Mutex<Vec<ConcreteStreamData>>>;

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

    impl FromIterator<(VarName, Vec<ConcreteStreamData>)> for VarMap {
        fn from_iter<I: IntoIterator<Item = (VarName, Vec<ConcreteStreamData>)>>(iter: I) -> Self {
            let mut map = VarMap(BTreeMap::new());
            for (key, vec) in iter {
                map.insert(key, Mutex::new(vec));
            }
            map
        }
    }

    impl StreamContext<UntypedStreams> for MockContext {
        fn var(&self, x: &VarName) -> Option<OutputStream<ConcreteStreamData>> {
            let mutex = self.xs.get(x)?;
            if let Ok(vec) = mutex.lock() {
                Some(Box::pin(stream::iter(vec.clone())))
            } else {
                std::panic!("Mutex was poisoned");
            }
        }
        fn subcontext(&self, history_length: usize) -> Box<dyn StreamContext<UntypedStreams>> {
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
        let x: OutputStream<ConcreteStreamData> = Box::pin(stream::iter(
            vec![ConcreteStreamData::Int(1), 3.into()].into_iter(),
        ));
        let y: OutputStream<ConcreteStreamData> =
            Box::pin(stream::iter(vec![2.into(), 4.into()].into_iter()));
        let z: Vec<ConcreteStreamData> = vec![3.into(), 7.into()];
        let res: Vec<ConcreteStreamData> = plus(x, y).collect().await;
        assert_eq!(res, z);
    }

    #[tokio::test]
    async fn test_str_plus() {
        let x: OutputStream<ConcreteStreamData> =
            Box::pin(stream::iter(vec!["hello ".into(), "olleh ".into()]));
        let y: OutputStream<ConcreteStreamData> =
            Box::pin(stream::iter(vec!["world".into(), "dlrow".into()]));
        let exp = vec!["hello world".into(), "olleh dlrow".into()];
        let res: Vec<ConcreteStreamData> = plus(x, y).collect().await;
        assert_eq!(res, exp)
    }

    #[tokio::test]
    async fn test_eval() {
        let e: OutputStream<ConcreteStreamData> =
            Box::pin(stream::iter(vec!["x + 1".into(), "x + 2".into()]));
        let map: VarMap = vec![(VarName("x".into()), vec![1.into(), 2.into()]).into()]
            .into_iter()
            .collect();
        let ctx = MockContext { xs: map };
        let res: Vec<ConcreteStreamData> = eval(&ctx, e, 10).collect().await;
        let exp: Vec<ConcreteStreamData> = vec![2.into(), 4.into()];
        assert_eq!(res, exp)
    }

    #[tokio::test]
    async fn test_eval_x_squared() {
        // This test is interesting since we use x twice in the eval strings
        let e: OutputStream<ConcreteStreamData> =
            Box::pin(stream::iter(vec!["x * x".into(), "x * x".into()]));
        let map: VarMap = vec![(VarName("x".into()), vec![2.into(), 3.into()]).into()]
            .into_iter()
            .collect();
        let ctx = MockContext { xs: map };
        let res: Vec<ConcreteStreamData> = eval(&ctx, e, 10).collect().await;
        let exp: Vec<ConcreteStreamData> = vec![4.into(), 9.into()];
        assert_eq!(res, exp)
    }

    #[tokio::test]
    async fn test_defer() {
        // Notice that even though we first say "x + 1", "x + 2", it continues evaluating "x + 1"
        let e: OutputStream<ConcreteStreamData> =
            Box::pin(stream::iter(vec!["x + 1".into(), "x + 2".into()]));
        let map: VarMap = vec![(VarName("x".into()), vec![1.into(), 2.into()]).into()]
            .into_iter()
            .collect();
        let ctx = MockContext { xs: map };
        let res: Vec<ConcreteStreamData> = defer(&ctx, e, 2).collect().await;
        let exp: Vec<ConcreteStreamData> = vec![2.into(), 3.into()];
        assert_eq!(res, exp)
    }
    #[tokio::test]
    async fn test_defer_x_squared() {
        // This test is interesting since we use x twice in the eval strings
        let e: OutputStream<ConcreteStreamData> =
            Box::pin(stream::iter(vec!["x * x".into(), "x * x + 1".into()]));
        let map: VarMap = vec![(VarName("x".into()), vec![2.into(), 3.into()]).into()]
            .into_iter()
            .collect();
        let ctx = MockContext { xs: map };
        let res: Vec<ConcreteStreamData> = defer(&ctx, e, 10).collect().await;
        let exp: Vec<ConcreteStreamData> = vec![4.into(), 9.into()];
        assert_eq!(res, exp)
    }

    #[tokio::test]
    async fn test_defer_unknown() {
        // Using unknown to represent no data on the stream
        let e: OutputStream<ConcreteStreamData> = Box::pin(stream::iter(vec![
            ConcreteStreamData::Unknown,
            "x + 1".into(),
        ]));
        let map: VarMap = vec![(VarName("x".into()), vec![2.into(), 3.into()]).into()]
            .into_iter()
            .collect();
        let ctx = MockContext { xs: map };
        let res = defer(&ctx, e, 10)
            .collect::<Vec<ConcreteStreamData>>()
            .await;
        let exp: Vec<ConcreteStreamData> = vec![ConcreteStreamData::Unknown, 4.into()];
        assert_eq!(res, exp)
    }

    #[tokio::test]
    async fn test_defer_unknown2() {
        // Unknown followed by property followed by unknown returns [U; val; val].
        let e = Box::pin(stream::iter(vec![
            ConcreteStreamData::Unknown,
            "x + 1".into(),
            ConcreteStreamData::Unknown,
        ])) as OutputStream<ConcreteStreamData>;
        let map: VarMap = vec![(VarName("x".into()), vec![2.into(), 3.into(), 4.into()]).into()]
            .into_iter()
            .collect();
        let ctx = MockContext { xs: map };
        let res = defer(&ctx, e, 10)
            .collect::<Vec<ConcreteStreamData>>()
            .await;
        let exp: Vec<ConcreteStreamData> = vec![ConcreteStreamData::Unknown, 4.into(), 5.into()];
        assert_eq!(res, exp)
    }

    #[tokio::test]
    async fn test_update_both_init() {
        let x: OutputStream<ConcreteStreamData> =
            Box::pin(stream::iter(vec!["x0".into(), "x1".into()]));
        let y: OutputStream<ConcreteStreamData> =
            Box::pin(stream::iter(vec!["y0".into(), "y1".into()]));
        let res: Vec<ConcreteStreamData> = update(x, y).collect().await;
        let exp: Vec<ConcreteStreamData> = vec!["y0".into(), "y1".into()];
        assert_eq!(res, exp)
    }

    #[tokio::test]
    async fn test_update_first_x_then_y() {
        let x: OutputStream<ConcreteStreamData> = Box::pin(stream::iter(vec![
            "x0".into(),
            "x1".into(),
            "x2".into(),
            "x3".into(),
        ]));
        let y: OutputStream<ConcreteStreamData> = Box::pin(stream::iter(vec![
            ConcreteStreamData::Unknown,
            "y1".into(),
            ConcreteStreamData::Unknown,
            "y3".into(),
        ]));
        let res: Vec<ConcreteStreamData> = update(x, y).collect().await;
        let exp: Vec<ConcreteStreamData> = vec![
            "x0".into(),
            "y1".into(),
            ConcreteStreamData::Unknown,
            "y3".into(),
        ];
        assert_eq!(res, exp)
    }

    #[tokio::test]
    async fn test_update_first_y_then_x() {
        // Notice that the length of res is longer than the input streams
        let x: OutputStream<ConcreteStreamData> = Box::pin(stream::iter(vec![
            ConcreteStreamData::Unknown,
            "x1".into(),
            ConcreteStreamData::Unknown,
            "x3".into(),
        ]));
        let y: OutputStream<ConcreteStreamData> = Box::pin(stream::iter(vec![
            "y0".into(),
            "y1".into(),
            "y2".into(),
            "y3".into(),
        ]));
        let res: Vec<ConcreteStreamData> = update(x, y).collect().await;
        let exp: Vec<ConcreteStreamData> = vec![
            ConcreteStreamData::Unknown,
            "y0".into(),
            "y1".into(),
            "y2".into(),
            "y3".into(),
        ];
        assert_eq!(res, exp)
    }

    #[tokio::test]
    async fn test_update_neither() {
        use ConcreteStreamData::Unknown;
        let x: OutputStream<ConcreteStreamData> =
            Box::pin(stream::iter(vec![Unknown, Unknown, Unknown, Unknown]));
        let y: OutputStream<ConcreteStreamData> =
            Box::pin(stream::iter(vec![Unknown, Unknown, Unknown, Unknown]));
        let res: Vec<ConcreteStreamData> = update(x, y).collect().await;
        let exp: Vec<ConcreteStreamData> = vec![Unknown, Unknown, Unknown, Unknown];
        assert_eq!(res, exp)
    }
}
