use core::panic;

use futures::{
    stream::{self, BoxStream},
    Stream, StreamExt,
};
use winnow::Parser;

use crate::{
    ast::SExpr,
    core::{ConcreteStreamData, MonitoringSemantics, OutputStream, StreamContext, StreamData, VarName},
};

pub trait CloneFn1<T: StreamData, S: StreamData>: Fn(T) -> S + Clone + Sync + Send + 'static {}
impl<T, S: StreamData, R: StreamData> CloneFn1<S, R> for T where T: Fn(S) -> R + Sync + Send + Clone + 'static {}

pub fn lift1<S: StreamData, R: StreamData>(f: impl CloneFn1<S, R>, x_mon: OutputStream<S>) -> OutputStream<R> {
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

pub fn lift2<S: StreamData, R: StreamData, U: StreamData>(f: impl CloneFn2<S, R, U>, x_mon: OutputStream<S>, y_mon: OutputStream<R>) -> OutputStream<U> {
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

pub fn and(x: OutputStream<ConcreteStreamData>, y: OutputStream<ConcreteStreamData>) -> OutputStream<ConcreteStreamData> {
    lift2(
        |x, y| ConcreteStreamData::Bool(x == ConcreteStreamData::Bool(true) && y == ConcreteStreamData::Bool(true)),
        x,
        y,
    )
}

pub fn or(x: OutputStream<ConcreteStreamData>, y: OutputStream<ConcreteStreamData>) -> OutputStream<ConcreteStreamData> {
    lift2(
        |x, y| ConcreteStreamData::Bool(x == ConcreteStreamData::Bool(true) || y == ConcreteStreamData::Bool(true)),
        x,
        y,
    )
}

pub fn not(x: OutputStream<ConcreteStreamData>) -> OutputStream<ConcreteStreamData> {
    lift1(|x| ConcreteStreamData::Bool(x == ConcreteStreamData::Bool(true)), x)
}

pub fn eq(x: OutputStream<ConcreteStreamData>, y: OutputStream<ConcreteStreamData>) -> OutputStream<ConcreteStreamData> {
    lift2(|x, y| ConcreteStreamData::Bool(x == y), x, y)
}

pub fn le(x: OutputStream<ConcreteStreamData>, y: OutputStream<ConcreteStreamData>) -> OutputStream<ConcreteStreamData> {
    lift2(
        |x, y| match (x, y) {
            (ConcreteStreamData::Int(x), ConcreteStreamData::Int(y)) => ConcreteStreamData::Bool(x <= y),
            (ConcreteStreamData::Bool(a), ConcreteStreamData::Bool(b)) => ConcreteStreamData::Bool(a <= b),
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
pub fn if_stm(x: OutputStream<ConcreteStreamData>, y: OutputStream<ConcreteStreamData>, z: OutputStream<ConcreteStreamData>) -> OutputStream<ConcreteStreamData> {
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

pub fn index(x: OutputStream<ConcreteStreamData>, i: isize, c: ConcreteStreamData) -> OutputStream<ConcreteStreamData> {
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

pub fn plus(x: OutputStream<ConcreteStreamData>, y: OutputStream<ConcreteStreamData>) -> OutputStream<ConcreteStreamData> {
    lift2(
        |x, y| match (x, y) {
            (ConcreteStreamData::Int(x), ConcreteStreamData::Int(y)) => ConcreteStreamData::Int(x + y),
            _ => panic!("Invalid addition"),
        },
        x,
        y,
    )
}

pub fn minus(x: OutputStream<ConcreteStreamData>, y: OutputStream<ConcreteStreamData>) -> OutputStream<ConcreteStreamData> {
    lift2(
        |x, y| match (x, y) {
            (ConcreteStreamData::Int(x), ConcreteStreamData::Int(y)) => ConcreteStreamData::Int(x - y),
            _ => panic!("Invalid subtraction"),
        },
        x,
        y,
    )
}

pub fn mult(x: OutputStream<ConcreteStreamData>, y: OutputStream<ConcreteStreamData>) -> OutputStream<ConcreteStreamData> {
    lift2(
        |x, y| match (x, y) {
            (ConcreteStreamData::Int(x), ConcreteStreamData::Int(y)) => ConcreteStreamData::Int(x * y),
            _ => panic!("Invalid multiplication"),
        },
        x,
        y,
    )
}

pub fn eval<S: MonitoringSemantics<SExpr<VarName>, ConcreteStreamData>>(
    ctx: &impl StreamContext<ConcreteStreamData>,
    x: OutputStream<ConcreteStreamData>,
) -> OutputStream<ConcreteStreamData> {
    let ctx = ctx.clone();
    Box::pin(stream::unfold(
        (ctx, x, None::<(ConcreteStreamData, OutputStream<ConcreteStreamData>)>),
        |(ctx, mut x, last)| async move {
            let current = x.next().await;
            println!("Current: {:?}", current);

            // If the current stream is has stopped, stop
            // eval steam
            if current == None {
                return None;
            }
            let current = current.unwrap();

            if let Some((prev, mut es)) = last {
                if prev == current {
                    let eval_res = es.next().await;
                    return match eval_res {
                        Some(eval_res) => Some((eval_res, (ctx, x, Some((current, es))))),
                        None => None,
                    };
                }
            }

            match current {
                ConcreteStreamData::Str(s) => {
                    println!("s: {:?}", s);
                    let s_parse = &mut s.as_str();
                    let expr = match crate::parser::lola_expression.parse_next(s_parse) {
                        Ok(expr) => expr,
                        Err(_) => unimplemented!("Invalid eval str"),
                    };
                    println!("expr: {}", expr);
                    let es = S::to_async_stream(expr, &ctx.clone());
                    //let eval_res = es.next().await;
                    return Some((ConcreteStreamData::Unit, (ctx, x, Some((ConcreteStreamData::Str(s), es)))));

                    // return match eval_res {
                    //     Some(eval_res) => {
                    //         Some((eval_res, (sem, ctx, x, Some((StreamData::Str(s), es)))))
                    //     }
                    //     None => None,
                    // };
                }
                x => unimplemented!("Invalid eval type {}", x),
            }
        },
    )) as OutputStream<ConcreteStreamData>
}

pub fn var(ctx: &impl StreamContext<ConcreteStreamData>, x: VarName) -> OutputStream<ConcreteStreamData> {
    match ctx.var(&x) {
        Some(x) => x,
        None => {
            let VarName(x) = x;
            panic!("Variable {} not found", x)
        }
    }
}
