use crate::OutputStream;
use crate::core::StreamData;
use crate::lang::dynamic_lola::type_checker::PossiblyDeferred;
use crate::semantics::untimed_untyped_lola::combinators::{CloneFn1, CloneFn2};
use futures::stream::LocalBoxStream;
use futures::{
    StreamExt,
    stream::{self},
};

pub fn deferred_lift1<S: StreamData, R: StreamData>(
    f: impl CloneFn1<S, R>,
    x_mon: OutputStream<PossiblyDeferred<S>>,
) -> OutputStream<PossiblyDeferred<R>> {
    let f = f.clone();
    Box::pin(x_mon.map(move |x| match x {
        PossiblyDeferred::Known(x) => PossiblyDeferred::Known(f(x)),
        PossiblyDeferred::Deferred => PossiblyDeferred::Deferred,
    }))
}

// Note that this might not cover all cases. Certain operators may want to yield
// the known value if either x or y is known.
pub fn deferred_lift2<S: StreamData, R: StreamData, U: StreamData>(
    f: impl CloneFn2<S, R, U>,
    x_mon: OutputStream<PossiblyDeferred<S>>,
    y_mon: OutputStream<PossiblyDeferred<R>>,
) -> OutputStream<PossiblyDeferred<U>> {
    let f = f.clone();
    Box::pin(x_mon.zip(y_mon).map(move |(x, y)| match (x, y) {
        (PossiblyDeferred::Known(x), PossiblyDeferred::Known(y)) => {
            PossiblyDeferred::Known(f(x, y))
        }
        _ => PossiblyDeferred::Deferred,
    }))
}

pub fn and(
    x: OutputStream<PossiblyDeferred<bool>>,
    y: OutputStream<PossiblyDeferred<bool>>,
) -> OutputStream<PossiblyDeferred<bool>> {
    deferred_lift2(|x, y| x && y, x, y)
}

pub fn or(
    x: OutputStream<PossiblyDeferred<bool>>,
    y: OutputStream<PossiblyDeferred<bool>>,
) -> OutputStream<PossiblyDeferred<bool>> {
    deferred_lift2(|x, y| x || y, x, y)
}

pub fn implication(
    x: OutputStream<PossiblyDeferred<bool>>,
    y: OutputStream<PossiblyDeferred<bool>>,
) -> OutputStream<PossiblyDeferred<bool>> {
    deferred_lift2(|x, y| !x || y, x, y)
}

pub fn not(x: OutputStream<PossiblyDeferred<bool>>) -> OutputStream<PossiblyDeferred<bool>> {
    deferred_lift1(|x| !x, x)
}

pub fn eq<X: Eq + StreamData>(
    x: OutputStream<PossiblyDeferred<X>>,
    y: OutputStream<PossiblyDeferred<X>>,
) -> OutputStream<PossiblyDeferred<bool>> {
    deferred_lift2(|x, y| x == y, x, y)
}

pub fn le(
    x: OutputStream<PossiblyDeferred<i64>>,
    y: OutputStream<PossiblyDeferred<i64>>,
) -> OutputStream<PossiblyDeferred<bool>> {
    deferred_lift2(|x, y| x <= y, x, y)
}

pub fn val<X: StreamData>(x: X) -> OutputStream<X> {
    Box::pin(stream::repeat(x.clone()))
}

pub fn if_stm<X: StreamData>(
    x: OutputStream<PossiblyDeferred<bool>>,
    y: OutputStream<PossiblyDeferred<X>>,
    z: OutputStream<PossiblyDeferred<X>>,
) -> OutputStream<PossiblyDeferred<X>> {
    Box::pin(x.zip(y).zip(z).map(move |((x, y), z)| match x {
        PossiblyDeferred::Known(x) => {
            if x {
                y
            } else {
                z
            }
        }
        PossiblyDeferred::Deferred => PossiblyDeferred::Deferred,
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
    x: OutputStream<PossiblyDeferred<X>>,
    i: u64,
) -> OutputStream<PossiblyDeferred<X>> {
    if let Ok(i) = usize::try_from(i) {
        let cs = stream::repeat(PossiblyDeferred::Deferred).take(i);
        // Delay x by i defers
        Box::pin(cs.chain(x))
    } else {
        panic!("Index too large for sindex operation")
    }
}

pub fn plus<T>(
    x: OutputStream<PossiblyDeferred<T>>,
    y: OutputStream<PossiblyDeferred<T>>,
) -> OutputStream<PossiblyDeferred<T>>
where
    T: std::ops::Add<Output = T> + StreamData,
{
    deferred_lift2(|x, y| x + y, x, y)
}

pub fn modulo<T>(
    x: OutputStream<PossiblyDeferred<T>>,
    y: OutputStream<PossiblyDeferred<T>>,
) -> OutputStream<PossiblyDeferred<T>>
where
    T: std::ops::Rem<Output = T> + StreamData,
{
    deferred_lift2(|x, y| x % y, x, y)
}

pub fn concat(
    x: OutputStream<PossiblyDeferred<String>>,
    y: OutputStream<PossiblyDeferred<String>>,
) -> OutputStream<PossiblyDeferred<String>> {
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
    x: OutputStream<PossiblyDeferred<T>>,
    y: OutputStream<PossiblyDeferred<T>>,
) -> OutputStream<PossiblyDeferred<T>>
where
    T: std::ops::Sub<Output = T> + StreamData,
{
    deferred_lift2(|x, y| x - y, x, y)
}

pub fn mult<T>(
    x: OutputStream<PossiblyDeferred<T>>,
    y: OutputStream<PossiblyDeferred<T>>,
) -> OutputStream<PossiblyDeferred<T>>
where
    T: std::ops::Mul<Output = T> + StreamData,
{
    deferred_lift2(|x, y| x * y, x, y)
}

pub fn div<T>(
    x: OutputStream<PossiblyDeferred<T>>,
    y: OutputStream<PossiblyDeferred<T>>,
) -> OutputStream<PossiblyDeferred<T>>
where
    T: std::ops::Div<Output = T> + StreamData,
{
    deferred_lift2(|x, y| x / y, x, y)
}

// Evaluates to a placeholder value whenever Deferred is received.
pub fn default<T: 'static>(
    x: OutputStream<PossiblyDeferred<T>>,
    d: OutputStream<PossiblyDeferred<T>>,
) -> OutputStream<PossiblyDeferred<T>> {
    let xs = x.zip(d).map(|(x, d)| match x {
        PossiblyDeferred::Known(x) => PossiblyDeferred::Known(x),
        PossiblyDeferred::Deferred => d,
    });
    Box::pin(xs) as LocalBoxStream<'static, PossiblyDeferred<T>>
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::async_test;
    use macro_rules_attribute::apply;

    #[apply(async_test)]
    async fn test_not() {
        let x: OutputStream<PossiblyDeferred<bool>> = Box::pin(stream::iter(
            vec![
                PossiblyDeferred::Known(true),
                PossiblyDeferred::Known(false),
            ]
            .into_iter(),
        ));
        let z: Vec<PossiblyDeferred<bool>> = vec![
            PossiblyDeferred::Known(false),
            PossiblyDeferred::Known(true),
        ];
        let res: Vec<PossiblyDeferred<bool>> = not(x).collect().await;
        assert_eq!(res, z);
    }

    #[apply(async_test)]
    async fn test_plus() {
        let x: OutputStream<PossiblyDeferred<i64>> = Box::pin(stream::iter(
            vec![PossiblyDeferred::Known(1), PossiblyDeferred::Known(3)].into_iter(),
        ));
        let y: OutputStream<PossiblyDeferred<i64>> = Box::pin(stream::iter(
            vec![PossiblyDeferred::Known(2), PossiblyDeferred::Known(4)].into_iter(),
        ));
        let z: Vec<PossiblyDeferred<i64>> =
            vec![PossiblyDeferred::Known(3), PossiblyDeferred::Known(7)];
        let res: Vec<PossiblyDeferred<i64>> = plus(x, y).collect().await;
        assert_eq!(res, z);
    }

    #[apply(async_test)]
    async fn test_str_plus() {
        let x: OutputStream<PossiblyDeferred<String>> = Box::pin(stream::iter(vec![
            PossiblyDeferred::Known("hello ".into()),
            PossiblyDeferred::Known("olleh ".into()),
        ]));
        let y: OutputStream<PossiblyDeferred<String>> = Box::pin(stream::iter(vec![
            PossiblyDeferred::Known("world".into()),
            PossiblyDeferred::Known("dlrow".into()),
        ]));
        let exp: Vec<PossiblyDeferred<String>> = vec![
            PossiblyDeferred::Known("hello world".into()),
            PossiblyDeferred::Known("olleh dlrow".into()),
        ];
        let res: Vec<PossiblyDeferred<String>> = concat(x, y).collect().await;
        assert_eq!(res, exp)
    }
}
