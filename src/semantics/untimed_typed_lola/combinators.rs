use crate::OutputStream;
use crate::core::StreamData;
use crate::lang::dynamic_lola::type_checker::PartialStreamValue;
use crate::semantics::untimed_untyped_lola::combinators::{CloneFn1, CloneFn2};
use async_stream::stream;
use futures::stream::LocalBoxStream;
use futures::{
    StreamExt,
    stream::{self},
};

fn deferred_stream_lift_base<T: StreamData>(
    mut x_mon: OutputStream<PartialStreamValue<T>>,
) -> OutputStream<PartialStreamValue<T>> {
    Box::pin(stream! {
        let mut last : Option<PartialStreamValue<T>>  = None;
        while let Some(curr) = x_mon.next().await {
            match curr {
                PartialStreamValue::NoVal => {
                    if let Some(last) = &last {
                        yield last.clone();
                    } else {
                        // Only happens when the first value is NoVal
                        yield PartialStreamValue::NoVal;
                    }
                }
                PartialStreamValue::Deferred => {
                    last = Some(PartialStreamValue::Deferred);
                    yield PartialStreamValue::Deferred;
                }
                PartialStreamValue::Known(val) => {
                    last = Some(PartialStreamValue::Known(val.clone()));
                    yield PartialStreamValue::Known(val);
                }
            }
        }
    })
}

pub fn deferred_lift1<S: StreamData, R: StreamData>(
    f: impl CloneFn1<S, R>,
    x_mon: OutputStream<PartialStreamValue<S>>,
) -> OutputStream<PartialStreamValue<R>> {
    let f = f.clone();
    Box::pin(x_mon.map(move |x| match x {
        PartialStreamValue::Known(x) => PartialStreamValue::Known(f(x)),
        PartialStreamValue::NoVal => PartialStreamValue::NoVal,
        PartialStreamValue::Deferred => PartialStreamValue::Deferred,
    }))
}

// Note that this might not cover all cases. Certain operators may want to yield
// the known value if either x or y is known.
// Currently wrong
pub fn deferred_lift2<S: StreamData, R: StreamData, U: StreamData>(
    f: impl CloneFn2<S, R, U>,
    x_mon: OutputStream<PartialStreamValue<S>>,
    y_mon: OutputStream<PartialStreamValue<R>>,
) -> OutputStream<PartialStreamValue<U>> {
    let f = f.clone();
    let x_mon = deferred_stream_lift_base(x_mon);
    let y_mon = deferred_stream_lift_base(y_mon);
    Box::pin(x_mon.zip(y_mon).map(move |(x, y)| match (x, y) {
        (PartialStreamValue::Known(x), PartialStreamValue::Known(y)) => {
            PartialStreamValue::Known(f(x, y))
        }
        // Deferred has precedence over NoVal since it every Boolean operator is deferred
        // until both branches are known
        (PartialStreamValue::Deferred, _) | (_, PartialStreamValue::Deferred) => {
            PartialStreamValue::Deferred
        }
        (PartialStreamValue::NoVal, _) | (_, PartialStreamValue::NoVal) => {
            PartialStreamValue::NoVal
        }
    }))
}

pub fn and(
    x: OutputStream<PartialStreamValue<bool>>,
    y: OutputStream<PartialStreamValue<bool>>,
) -> OutputStream<PartialStreamValue<bool>> {
    deferred_lift2(|x, y| x && y, x, y)
}

pub fn or(
    x: OutputStream<PartialStreamValue<bool>>,
    y: OutputStream<PartialStreamValue<bool>>,
) -> OutputStream<PartialStreamValue<bool>> {
    deferred_lift2(|x, y| x || y, x, y)
}

pub fn implication(
    x: OutputStream<PartialStreamValue<bool>>,
    y: OutputStream<PartialStreamValue<bool>>,
) -> OutputStream<PartialStreamValue<bool>> {
    deferred_lift2(|x, y| !x || y, x, y)
}

pub fn not(x: OutputStream<PartialStreamValue<bool>>) -> OutputStream<PartialStreamValue<bool>> {
    deferred_lift1(|x| !x, x)
}

pub fn eq<X: Eq + StreamData>(
    x: OutputStream<PartialStreamValue<X>>,
    y: OutputStream<PartialStreamValue<X>>,
) -> OutputStream<PartialStreamValue<bool>> {
    deferred_lift2(|x, y| x == y, x, y)
}

pub fn le(
    x: OutputStream<PartialStreamValue<i64>>,
    y: OutputStream<PartialStreamValue<i64>>,
) -> OutputStream<PartialStreamValue<bool>> {
    deferred_lift2(|x, y| x <= y, x, y)
}

pub fn val<X: StreamData>(x: X) -> OutputStream<X> {
    Box::pin(stream::repeat(x.clone()))
}

pub fn if_stm<X: StreamData>(
    x: OutputStream<PartialStreamValue<bool>>,
    y: OutputStream<PartialStreamValue<X>>,
    z: OutputStream<PartialStreamValue<X>>,
) -> OutputStream<PartialStreamValue<X>> {
    Box::pin(x.zip(y).zip(z).map(move |((x, y), z)| match x {
        PartialStreamValue::Known(x) => {
            if x {
                y
            } else {
                z
            }
        }
        PartialStreamValue::NoVal => PartialStreamValue::NoVal,
        PartialStreamValue::Deferred => PartialStreamValue::Deferred,
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
    x: OutputStream<PartialStreamValue<X>>,
    i: u64,
) -> OutputStream<PartialStreamValue<X>> {
    if let Ok(i) = usize::try_from(i) {
        let cs = stream::repeat(PartialStreamValue::Deferred).take(i);
        // Delay x by i defers
        let mut delayed = cs.chain(x);

        // Handle NoVals manually (we cannot use deferred_stream_lift_base
        // since we need to manually handle deferred)
        Box::pin(stream! {
            let mut last : Option<PartialStreamValue<X>> = None;
            while let Some(curr) = delayed.next().await {
                match curr {
                    PartialStreamValue::NoVal => {
                        if let Some(last) = &last {
                            yield last.clone();
                        } else {
                            // Only happens when the first value is NoVal
                            yield PartialStreamValue::NoVal;
                        }
                    }
                    PartialStreamValue::Deferred => {
                        last = Some(PartialStreamValue::Deferred);
                        yield PartialStreamValue::Deferred;
                    }
                    PartialStreamValue::Known(val) => {
                        last = Some(PartialStreamValue::Known(val.clone()));
                        yield PartialStreamValue::Known(val);
                    }
                }
            }
        })
    } else {
        panic!("Index too large for sindex operation")
    }
}

pub fn plus<T>(
    x: OutputStream<PartialStreamValue<T>>,
    y: OutputStream<PartialStreamValue<T>>,
) -> OutputStream<PartialStreamValue<T>>
where
    T: std::ops::Add<Output = T> + StreamData,
{
    deferred_lift2(|x, y| x + y, x, y)
}

pub fn modulo<T>(
    x: OutputStream<PartialStreamValue<T>>,
    y: OutputStream<PartialStreamValue<T>>,
) -> OutputStream<PartialStreamValue<T>>
where
    T: std::ops::Rem<Output = T> + StreamData,
{
    deferred_lift2(|x, y| x % y, x, y)
}

pub fn concat(
    x: OutputStream<PartialStreamValue<String>>,
    y: OutputStream<PartialStreamValue<String>>,
) -> OutputStream<PartialStreamValue<String>> {
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
    x: OutputStream<PartialStreamValue<T>>,
    y: OutputStream<PartialStreamValue<T>>,
) -> OutputStream<PartialStreamValue<T>>
where
    T: std::ops::Sub<Output = T> + StreamData,
{
    deferred_lift2(|x, y| x - y, x, y)
}

pub fn mult<T>(
    x: OutputStream<PartialStreamValue<T>>,
    y: OutputStream<PartialStreamValue<T>>,
) -> OutputStream<PartialStreamValue<T>>
where
    T: std::ops::Mul<Output = T> + StreamData,
{
    deferred_lift2(|x, y| x * y, x, y)
}

pub fn div<T>(
    x: OutputStream<PartialStreamValue<T>>,
    y: OutputStream<PartialStreamValue<T>>,
) -> OutputStream<PartialStreamValue<T>>
where
    T: std::ops::Div<Output = T> + StreamData,
{
    deferred_lift2(|x, y| x / y, x, y)
}

// Evaluates to a placeholder value whenever Deferred is received.
pub fn default<T: 'static>(
    x: OutputStream<PartialStreamValue<T>>,
    d: OutputStream<PartialStreamValue<T>>,
) -> OutputStream<PartialStreamValue<T>> {
    let xs = x.zip(d).map(|(x, d)| match x {
        PartialStreamValue::Known(x) => PartialStreamValue::Known(x),
        PartialStreamValue::NoVal => PartialStreamValue::NoVal,
        PartialStreamValue::Deferred => d,
    });
    Box::pin(xs) as LocalBoxStream<'static, PartialStreamValue<T>>
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::async_test;
    use macro_rules_attribute::apply;

    #[apply(async_test)]
    async fn test_not() {
        let x: OutputStream<PartialStreamValue<bool>> = Box::pin(stream::iter(
            vec![
                PartialStreamValue::Known(true),
                PartialStreamValue::Known(false),
            ]
            .into_iter(),
        ));
        let z: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        let res: Vec<PartialStreamValue<bool>> = not(x).collect().await;
        assert_eq!(res, z);
    }

    #[apply(async_test)]
    async fn test_plus() {
        let x: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(
            vec![PartialStreamValue::Known(1), PartialStreamValue::Known(3)].into_iter(),
        ));
        let y: OutputStream<PartialStreamValue<i64>> = Box::pin(stream::iter(
            vec![PartialStreamValue::Known(2), PartialStreamValue::Known(4)].into_iter(),
        ));
        let z: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(3), PartialStreamValue::Known(7)];
        let res: Vec<PartialStreamValue<i64>> = plus(x, y).collect().await;
        assert_eq!(res, z);
    }

    #[apply(async_test)]
    async fn test_str_plus() {
        let x: OutputStream<PartialStreamValue<String>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known("hello ".into()),
            PartialStreamValue::Known("olleh ".into()),
        ]));
        let y: OutputStream<PartialStreamValue<String>> = Box::pin(stream::iter(vec![
            PartialStreamValue::Known("world".into()),
            PartialStreamValue::Known("dlrow".into()),
        ]));
        let exp: Vec<PartialStreamValue<String>> = vec![
            PartialStreamValue::Known("hello world".into()),
            PartialStreamValue::Known("olleh dlrow".into()),
        ];
        let res: Vec<PartialStreamValue<String>> = concat(x, y).collect().await;
        assert_eq!(res, exp)
    }
}
