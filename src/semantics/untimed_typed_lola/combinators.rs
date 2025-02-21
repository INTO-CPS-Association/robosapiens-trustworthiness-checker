use crate::core::Value;
use crate::core::{StreamContext, StreamData};
use crate::lang::dynamic_lola::parser::lola_expression;
use crate::semantics::untimed_untyped_lola::combinators::{lift1, lift2, lift3};
use crate::semantics::UntimedLolaSemantics;
use crate::{MonitoringSemantics, OutputStream};
use futures::{
    stream::{self, BoxStream},
    StreamExt,
};
use std::fmt::Debug;
use winnow::Parser;

pub fn to_typed_stream<T: TryFrom<Value, Error = ()> + Debug>(
    stream: OutputStream<Value>,
) -> OutputStream<T> {
    Box::pin(stream.map(|x| x.try_into().expect("Type error")))
}

pub fn from_typed_stream<T: Into<Value> + StreamData>(
    stream: OutputStream<T>,
) -> OutputStream<Value> {
    Box::pin(stream.map(|x| x.into()))
}

pub fn and(x: OutputStream<bool>, y: OutputStream<bool>) -> OutputStream<bool> {
    lift2(|x, y| x && y, x, y)
}

pub fn or(x: OutputStream<bool>, y: OutputStream<bool>) -> OutputStream<bool> {
    lift2(|x, y| x || y, x, y)
}

pub fn not(x: OutputStream<bool>) -> OutputStream<bool> {
    lift1(|x| !x, x)
}

pub fn eq<X: Eq + StreamData>(x: OutputStream<X>, y: OutputStream<X>) -> OutputStream<bool> {
    lift2(|x, y| x == y, x, y)
}

pub fn le(x: OutputStream<i64>, y: OutputStream<i64>) -> OutputStream<bool> {
    lift2(|x, y| x <= y, x, y)
}

pub fn val<X: StreamData>(x: X) -> OutputStream<X> {
    Box::pin(stream::repeat(x.clone()))
}

// Should this return a dyn ConcreteStreamData?
pub fn if_stm<X: StreamData>(
    x: OutputStream<bool>,
    y: OutputStream<X>,
    z: OutputStream<X>,
) -> OutputStream<X> {
    lift3(|x, y, z| if x { y } else { z }, x, y, z)
}

pub fn sindex<X: StreamData>(x: OutputStream<X>, i: isize, c: X) -> OutputStream<X> {
    let c = c.clone();
    if i < 0 {
        let n: usize = (-i).try_into().unwrap();
        let cs = stream::repeat(c).take(n);
        Box::pin(cs.chain(x)) as BoxStream<'static, X>
    } else {
        let n: usize = i.try_into().unwrap();
        Box::pin(x.skip(n)) as BoxStream<'static, X>
    }
}

pub fn plus(x: OutputStream<i64>, y: OutputStream<i64>) -> OutputStream<i64> {
    lift2(|x, y| x + y, x, y)
}

pub fn concat(x: OutputStream<String>, y: OutputStream<String>) -> OutputStream<String> {
    lift2(
        |mut x, y| {
            x.push_str(&y);
            x
        },
        x,
        y,
    )
}

pub fn minus(x: OutputStream<i64>, y: OutputStream<i64>) -> OutputStream<i64> {
    lift2(|x, y| x - y, x, y)
}

pub fn mult(x: OutputStream<i64>, y: OutputStream<i64>) -> OutputStream<i64> {
    lift2(|x, y| x * y, x, y)
}

pub fn div(x: OutputStream<i64>, y: OutputStream<i64>) -> OutputStream<i64> {
    lift2(|x, y| x / y, x, y)
}

pub fn eval<T: TryFrom<Value, Error = ()> + StreamData>(
    ctx: &dyn StreamContext<Value>,
    x: OutputStream<String>,
    history_length: usize,
) -> OutputStream<T> {
    // Create a subcontext with a history window length of 10
    let subcontext = ctx.subcontext(history_length);
    /*unfold() creates a Stream from a seed value.*/
    Box::pin(stream::unfold(
        (subcontext, x, None::<(String, OutputStream<T>)>),
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

            let s_parse = &mut current.as_str();
            let expr = match lola_expression.parse(s_parse) {
                Ok(expr) => expr,
                Err(_) => unimplemented!("Invalid eval str"),
            };
            // let expr = expr.type_check_with_default().unwrap();
            let es = UntimedLolaSemantics::to_async_stream(expr, subcontext.as_ref());
            let mut es = to_typed_stream(es);
            // println!("new eval stream");
            subcontext.advance();
            let eval_res = es.next().await?;
            // println!("eval producing {:?}", eval_res);
            return Some((eval_res, (subcontext, x, Some((current, es)))));
        },
    )) as OutputStream<T>
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;
    use test_log::test;

    #[test(tokio::test)]
    async fn test_plus() {
        let x: OutputStream<i64> = Box::pin(stream::iter(vec![1, 3].into_iter()));
        let y: OutputStream<i64> = Box::pin(stream::iter(vec![2, 4].into_iter()));
        let z: Vec<i64> = vec![3, 7];
        let res: Vec<i64> = plus(x, y).collect().await;
        assert_eq!(res, z);
    }

    #[test(tokio::test)]
    async fn test_str_plus() {
        let x: OutputStream<String> =
            Box::pin(stream::iter(vec!["hello ".into(), "olleh ".into()]));
        let y: OutputStream<String> = Box::pin(stream::iter(vec!["world".into(), "dlrow".into()]));
        let exp: Vec<String> = vec!["hello world".into(), "olleh dlrow".into()];
        let res: Vec<String> = concat(x, y).collect().await;
        assert_eq!(res, exp)
    }
}
