use core::panic;

use futures::{
    stream::{self, BoxStream},
    Stream, StreamExt,
};
use winnow::Parser;

use crate::{
    ast::SExpr,
    core::{MonitoringSemantics, OutputStream, StreamContext, StreamData, VarName},
};

pub trait CloneFn1: Fn(StreamData) -> StreamData + Clone + Sync + Send + 'static {}
impl<T> CloneFn1 for T where T: Fn(StreamData) -> StreamData + Sync + Send + Clone + 'static {}

pub fn lift1(f: impl CloneFn1, x_mon: OutputStream) -> OutputStream {
    let f = f.clone();

    Box::pin(x_mon.map(move |x| f(x)))
}

pub trait CloneFn2:
    Fn(StreamData, StreamData) -> StreamData + Clone + Sync + Send + 'static
{
}
impl<T> CloneFn2 for T where
    T: Fn(StreamData, StreamData) -> StreamData + Clone + Sync + Send + 'static
{
}

pub fn lift2(f: impl CloneFn2, x_mon: OutputStream, y_mon: OutputStream) -> OutputStream {
    let f = f.clone();
    Box::pin(x_mon.zip(y_mon).map(move |(x, y)| f(x, y)))
}

pub trait CloneFn3:
    Fn(StreamData, StreamData, StreamData) -> StreamData + Clone + Sync + Send + 'static
{
}
impl<T> CloneFn3 for T where
    T: Fn(StreamData, StreamData, StreamData) -> StreamData + Clone + Sync + Send + 'static
{
}

pub fn lift3(
    f: impl CloneFn3,
    x_mon: OutputStream,
    y_mon: OutputStream,
    z_mon: OutputStream,
) -> OutputStream {
    let f = f.clone();

    Box::pin(
        x_mon
            .zip(y_mon)
            .zip(z_mon)
            .map(move |((x, y), z)| f(x, y, z)),
    ) as BoxStream<'static, StreamData>
}

pub fn and(x: OutputStream, y: OutputStream) -> OutputStream {
    lift2(
        |x, y| StreamData::Bool(x == StreamData::Bool(true) && y == StreamData::Bool(true)),
        x,
        y,
    )
}

pub fn or(x: OutputStream, y: OutputStream) -> OutputStream {
    lift2(
        |x, y| StreamData::Bool(x == StreamData::Bool(true) || y == StreamData::Bool(true)),
        x,
        y,
    )
}

pub fn not(x: OutputStream) -> OutputStream {
    lift1(|x| StreamData::Bool(x == StreamData::Bool(true)), x)
}

pub fn eq(x: OutputStream, y: OutputStream) -> OutputStream {
    lift2(|x, y| StreamData::Bool(x == y), x, y)
}

pub fn le(x: OutputStream, y: OutputStream) -> OutputStream {
    lift2(
        |x, y| match (x, y) {
            (StreamData::Int(x), StreamData::Int(y)) => StreamData::Bool(x <= y),
            (StreamData::Bool(a), StreamData::Bool(b)) => StreamData::Bool(a <= b),
            _ => panic!("Invalid comparison"),
        },
        x,
        y,
    )
}

pub fn val(x: StreamData) -> OutputStream {
    Box::pin(stream::repeat(x.clone()))
}

pub fn if_stm(x: OutputStream, y: OutputStream, z: OutputStream) -> OutputStream {
    lift3(
        |x, y, z| match x {
            StreamData::Bool(true) => y,
            StreamData::Bool(false) => z,
            _ => panic!("Invalid if condition"),
        },
        x,
        y,
        z,
    )
}

pub fn index(x: OutputStream, i: isize, c: StreamData) -> OutputStream {
    let c = c.clone();
    if i < 0 {
        let n: usize = (-i).try_into().unwrap();
        let cs = stream::repeat(c).take(n);
        Box::pin(cs.chain(x)) as BoxStream<'static, StreamData>
    } else {
        let n: usize = i.try_into().unwrap();
        Box::pin(x.skip(n)) as BoxStream<'static, StreamData>
    }
}

pub fn plus(x: OutputStream, y: OutputStream) -> OutputStream {
    lift2(
        |x, y| match (x, y) {
            (StreamData::Int(x), StreamData::Int(y)) => StreamData::Int(x + y),
            _ => panic!("Invalid addition"),
        },
        x,
        y,
    )
}

pub fn minus(x: OutputStream, y: OutputStream) -> OutputStream {
    lift2(
        |x, y| match (x, y) {
            (StreamData::Int(x), StreamData::Int(y)) => StreamData::Int(x - y),
            _ => panic!("Invalid subtraction"),
        },
        x,
        y,
    )
}

pub fn mult(x: OutputStream, y: OutputStream) -> OutputStream {
    lift2(
        |x, y| match (x, y) {
            (StreamData::Int(x), StreamData::Int(y)) => StreamData::Int(x * y),
            _ => panic!("Invalid multiplication"),
        },
        x,
        y,
    )
}

pub fn eval(
    sem: impl MonitoringSemantics<SExpr<VarName>>,
    ctx: &impl StreamContext,
    x: OutputStream,
) -> OutputStream {
    let ctx = ctx.clone();
    Box::pin(stream::unfold(
        (sem, ctx, x, None::<(StreamData, OutputStream)>),
        |(sem, ctx, mut x, last)| async move {
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
                        Some(eval_res) => Some((eval_res, (sem, ctx, x, Some((current, es))))),
                        None => None,
                    };
                }
            }

            match current {
                StreamData::Str(s) => {
                    println!("s: {:?}", s);
                    let s_parse = &mut s.as_str();
                    let expr = match crate::parser::lola_expression.parse_next(s_parse) {
                        Ok(expr) => expr,
                        Err(_) => unimplemented!("Invalid eval str"),
                    };
                    println!("expr: {}", expr);
                    let es = sem.to_async_stream(expr, &ctx.clone());
                    //let eval_res = es.next().await;
                    return Some((
                        StreamData::Unit,
                        (sem, ctx, x, Some((StreamData::Str(s), es))),
                    ));

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
    )) as OutputStream
}

pub fn var(ctx: &impl StreamContext, x: VarName) -> OutputStream {
    match ctx.var(&x) {
        Some(x) => x,
        None => {
            let VarName(x) = x;
            panic!("Variable {} not found", x)
        }
    }
}
