use crate::OutputStream;
use crate::core::StreamData;
use crate::lang::dynamic_lola::type_checker::PossiblyUnknown;
use crate::semantics::untimed_untyped_lola::combinators::{CloneFn1, CloneFn2};
use futures::stream::LocalBoxStream;
use futures::{
    StreamExt,
    stream::{self},
};

pub fn unknown_lift1<S: StreamData, R: StreamData>(
    f: impl CloneFn1<S, R>,
    x_mon: OutputStream<PossiblyUnknown<S>>,
) -> OutputStream<PossiblyUnknown<R>> {
    let f = f.clone();
    Box::pin(x_mon.map(move |x| match x {
        PossiblyUnknown::Known(x) => PossiblyUnknown::Known(f(x)),
        PossiblyUnknown::Unknown => PossiblyUnknown::Unknown,
    }))
}

// Note that this might not cover all cases. Certain operators may want to yield
// the known value if either x or y is known.
pub fn unknown_lift2<S: StreamData, R: StreamData, U: StreamData>(
    f: impl CloneFn2<S, R, U>,
    x_mon: OutputStream<PossiblyUnknown<S>>,
    y_mon: OutputStream<PossiblyUnknown<R>>,
) -> OutputStream<PossiblyUnknown<U>> {
    let f = f.clone();
    Box::pin(x_mon.zip(y_mon).map(move |(x, y)| match (x, y) {
        (PossiblyUnknown::Known(x), PossiblyUnknown::Known(y)) => PossiblyUnknown::Known(f(x, y)),
        _ => PossiblyUnknown::Unknown,
    }))
}

pub fn and(
    x: OutputStream<PossiblyUnknown<bool>>,
    y: OutputStream<PossiblyUnknown<bool>>,
) -> OutputStream<PossiblyUnknown<bool>> {
    unknown_lift2(|x, y| x && y, x, y)
}

pub fn or(
    x: OutputStream<PossiblyUnknown<bool>>,
    y: OutputStream<PossiblyUnknown<bool>>,
) -> OutputStream<PossiblyUnknown<bool>> {
    unknown_lift2(|x, y| x || y, x, y)
}

pub fn not(x: OutputStream<PossiblyUnknown<bool>>) -> OutputStream<PossiblyUnknown<bool>> {
    unknown_lift1(|x| !x, x)
}

pub fn eq<X: Eq + StreamData>(
    x: OutputStream<PossiblyUnknown<X>>,
    y: OutputStream<PossiblyUnknown<X>>,
) -> OutputStream<PossiblyUnknown<bool>> {
    unknown_lift2(|x, y| x == y, x, y)
}

pub fn le(
    x: OutputStream<PossiblyUnknown<i64>>,
    y: OutputStream<PossiblyUnknown<i64>>,
) -> OutputStream<PossiblyUnknown<bool>> {
    unknown_lift2(|x, y| x <= y, x, y)
}

pub fn val<X: StreamData>(x: X) -> OutputStream<X> {
    Box::pin(stream::repeat(x.clone()))
}

pub fn if_stm<X: StreamData>(
    x: OutputStream<PossiblyUnknown<bool>>,
    y: OutputStream<PossiblyUnknown<X>>,
    z: OutputStream<PossiblyUnknown<X>>,
) -> OutputStream<PossiblyUnknown<X>> {
    Box::pin(x.zip(y).zip(z).map(move |((x, y), z)| match x {
        PossiblyUnknown::Known(x) => {
            if x {
                y
            } else {
                z
            }
        }
        PossiblyUnknown::Unknown => PossiblyUnknown::Unknown,
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
pub fn sindex<X: StreamData>(x: OutputStream<X>, i: isize, c: X) -> OutputStream<X> {
    let c = c.clone();
    let n = i.abs() as usize;
    let cs = stream::repeat(c).take(n);
    if i < 0 {
        Box::pin(cs.chain(x)) as LocalBoxStream<'static, X>
    } else {
        Box::pin(x.skip(n).chain(cs)) as LocalBoxStream<'static, X>
    }
}

pub fn plus<T>(
    x: OutputStream<PossiblyUnknown<T>>,
    y: OutputStream<PossiblyUnknown<T>>,
) -> OutputStream<PossiblyUnknown<T>>
where
    T: std::ops::Add<Output = T> + StreamData,
{
    unknown_lift2(|x, y| x + y, x, y)
}

pub fn modulo<T>(
    x: OutputStream<PossiblyUnknown<T>>,
    y: OutputStream<PossiblyUnknown<T>>,
) -> OutputStream<PossiblyUnknown<T>>
where
    T: std::ops::Rem<Output = T> + StreamData,
{
    unknown_lift2(|x, y| x % y, x, y)
}

pub fn concat(
    x: OutputStream<PossiblyUnknown<String>>,
    y: OutputStream<PossiblyUnknown<String>>,
) -> OutputStream<PossiblyUnknown<String>> {
    unknown_lift2(
        |mut x, y| {
            x.push_str(&y);
            x
        },
        x,
        y,
    )
}

pub fn minus<T>(
    x: OutputStream<PossiblyUnknown<T>>,
    y: OutputStream<PossiblyUnknown<T>>,
) -> OutputStream<PossiblyUnknown<T>>
where
    T: std::ops::Sub<Output = T> + StreamData,
{
    unknown_lift2(|x, y| x - y, x, y)
}

pub fn mult<T>(
    x: OutputStream<PossiblyUnknown<T>>,
    y: OutputStream<PossiblyUnknown<T>>,
) -> OutputStream<PossiblyUnknown<T>>
where
    T: std::ops::Mul<Output = T> + StreamData,
{
    unknown_lift2(|x, y| x * y, x, y)
}

pub fn div<T>(
    x: OutputStream<PossiblyUnknown<T>>,
    y: OutputStream<PossiblyUnknown<T>>,
) -> OutputStream<PossiblyUnknown<T>>
where
    T: std::ops::Div<Output = T> + StreamData,
{
    unknown_lift2(|x, y| x / y, x, y)
}

// Evaluates to a placeholder value whenever Unknown is received.
pub fn default<T: 'static>(
    x: OutputStream<PossiblyUnknown<T>>,
    d: OutputStream<PossiblyUnknown<T>>,
) -> OutputStream<PossiblyUnknown<T>> {
    let xs = x.zip(d).map(|(x, d)| match x {
        PossiblyUnknown::Known(x) => PossiblyUnknown::Known(x),
        PossiblyUnknown::Unknown => d,
    });
    Box::pin(xs) as LocalBoxStream<'static, PossiblyUnknown<T>>
}

#[cfg(test)]
mod tests {
    use super::*;
    use macro_rules_attribute::apply;
    use smol_macros::test as smol_test;
    use test_log::test;

    #[test(apply(smol_test))]
    async fn test_not() {
        let x: OutputStream<PossiblyUnknown<bool>> = Box::pin(stream::iter(
            vec![PossiblyUnknown::Known(true), PossiblyUnknown::Known(false)].into_iter(),
        ));
        let z: Vec<PossiblyUnknown<bool>> =
            vec![PossiblyUnknown::Known(false), PossiblyUnknown::Known(true)];
        let res: Vec<PossiblyUnknown<bool>> = not(x).collect().await;
        assert_eq!(res, z);
    }

    #[test(apply(smol_test))]
    async fn test_plus() {
        let x: OutputStream<PossiblyUnknown<i64>> = Box::pin(stream::iter(
            vec![PossiblyUnknown::Known(1), PossiblyUnknown::Known(3)].into_iter(),
        ));
        let y: OutputStream<PossiblyUnknown<i64>> = Box::pin(stream::iter(
            vec![PossiblyUnknown::Known(2), PossiblyUnknown::Known(4)].into_iter(),
        ));
        let z: Vec<PossiblyUnknown<i64>> =
            vec![PossiblyUnknown::Known(3), PossiblyUnknown::Known(7)];
        let res: Vec<PossiblyUnknown<i64>> = plus(x, y).collect().await;
        assert_eq!(res, z);
    }

    #[test(apply(smol_test))]
    async fn test_str_plus() {
        let x: OutputStream<PossiblyUnknown<String>> = Box::pin(stream::iter(vec![
            PossiblyUnknown::Known("hello ".into()),
            PossiblyUnknown::Known("olleh ".into()),
        ]));
        let y: OutputStream<PossiblyUnknown<String>> = Box::pin(stream::iter(vec![
            PossiblyUnknown::Known("world".into()),
            PossiblyUnknown::Known("dlrow".into()),
        ]));
        let exp: Vec<PossiblyUnknown<String>> = vec![
            PossiblyUnknown::Known("hello world".into()),
            PossiblyUnknown::Known("olleh dlrow".into()),
        ];
        let res: Vec<PossiblyUnknown<String>> = concat(x, y).collect().await;
        assert_eq!(res, exp)
    }
}
