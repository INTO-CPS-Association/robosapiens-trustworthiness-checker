//! Type-specialised scalar stream combinators used by checked expressions.

use std::ops::{Add, Div, Mul, Rem, Sub};

use futures::{StreamExt, stream};

use crate::core::{OutputStream, PartialStreamValue, StreamData};

// WARNING: These implementations intentionally mirror scalar operations in
// `combinators`. Changes to propagation or operator semantics must be made in
// both modules to avoid implementation drift.

pub(super) fn lift_base<T: StreamData>(
    mut input: OutputStream<PartialStreamValue<T>>,
) -> OutputStream<PartialStreamValue<T>> {
    Box::pin(async_stream::stream! {
        let mut last = None;
        while let Some(current) = input.next().await {
            match current {
                PartialStreamValue::NoVal => {
                    yield last.clone().unwrap_or(PartialStreamValue::NoVal);
                }
                current => {
                    last = Some(current.clone());
                    yield current;
                }
            }
        }
    })
}

fn lift1<T, U>(
    input: OutputStream<PartialStreamValue<T>>,
    operation: impl Fn(T) -> U + 'static,
) -> OutputStream<PartialStreamValue<U>>
where
    T: StreamData,
    U: StreamData,
{
    Box::pin(lift_base(input).map(move |value| match value {
        PartialStreamValue::Known(value) => PartialStreamValue::Known(operation(value)),
        PartialStreamValue::NoVal => PartialStreamValue::NoVal,
        PartialStreamValue::Deferred => PartialStreamValue::Deferred,
    }))
}

fn lift2<T, U, V>(
    left: OutputStream<PartialStreamValue<T>>,
    right: OutputStream<PartialStreamValue<U>>,
    operation: impl Fn(T, U) -> V + 'static,
) -> OutputStream<PartialStreamValue<V>>
where
    T: StreamData,
    U: StreamData,
    V: StreamData,
{
    Box::pin(
        lift_base(left)
            .zip(lift_base(right))
            .map(move |(left, right)| match (left, right) {
                (PartialStreamValue::Known(left), PartialStreamValue::Known(right)) => {
                    PartialStreamValue::Known(operation(left, right))
                }
                (PartialStreamValue::Deferred, _) | (_, PartialStreamValue::Deferred) => {
                    PartialStreamValue::Deferred
                }
                (PartialStreamValue::NoVal, _) | (_, PartialStreamValue::NoVal) => {
                    PartialStreamValue::NoVal
                }
            }),
    )
}

pub(super) fn val<T: StreamData>(value: T) -> OutputStream<PartialStreamValue<T>> {
    Box::pin(stream::repeat(PartialStreamValue::Known(value)))
}

macro_rules! binary {
    ($name:ident, $trait:ident, $method:ident) => {
        pub(super) fn $name<T>(
            left: OutputStream<PartialStreamValue<T>>,
            right: OutputStream<PartialStreamValue<T>>,
        ) -> OutputStream<PartialStreamValue<T>>
        where
            T: StreamData + $trait<Output = T>,
        {
            lift2(left, right, T::$method)
        }
    };
}

binary!(add, Add, add);
binary!(sub, Sub, sub);
binary!(mul, Mul, mul);
binary!(div, Div, div);
binary!(rem, Rem, rem);

pub(super) fn not(
    input: OutputStream<PartialStreamValue<bool>>,
) -> OutputStream<PartialStreamValue<bool>> {
    lift1(input, |value| !value)
}

pub(super) fn sin(
    input: OutputStream<PartialStreamValue<f64>>,
) -> OutputStream<PartialStreamValue<f64>> {
    lift1(input, f64::sin)
}

pub(super) fn cos(
    input: OutputStream<PartialStreamValue<f64>>,
) -> OutputStream<PartialStreamValue<f64>> {
    lift1(input, f64::cos)
}

pub(super) fn tan(
    input: OutputStream<PartialStreamValue<f64>>,
) -> OutputStream<PartialStreamValue<f64>> {
    lift1(input, f64::tan)
}

pub(super) fn abs(
    input: OutputStream<PartialStreamValue<f64>>,
) -> OutputStream<PartialStreamValue<f64>> {
    lift1(input, f64::abs)
}

pub(super) fn and(
    left: OutputStream<PartialStreamValue<bool>>,
    right: OutputStream<PartialStreamValue<bool>>,
) -> OutputStream<PartialStreamValue<bool>> {
    lift2(left, right, |left, right| left && right)
}

pub(super) fn or(
    left: OutputStream<PartialStreamValue<bool>>,
    right: OutputStream<PartialStreamValue<bool>>,
) -> OutputStream<PartialStreamValue<bool>> {
    lift2(left, right, |left, right| left || right)
}

pub(super) fn implies(
    left: OutputStream<PartialStreamValue<bool>>,
    right: OutputStream<PartialStreamValue<bool>>,
) -> OutputStream<PartialStreamValue<bool>> {
    lift2(left, right, |left, right| !left || right)
}

pub(super) fn compare<T: StreamData, F>(
    left: OutputStream<PartialStreamValue<T>>,
    right: OutputStream<PartialStreamValue<T>>,
    comparison: F,
) -> OutputStream<PartialStreamValue<bool>>
where
    F: Fn(T, T) -> bool + 'static,
{
    lift2(left, right, comparison)
}

pub(super) fn default<T: StreamData>(
    input: OutputStream<PartialStreamValue<T>>,
    fallback: OutputStream<PartialStreamValue<T>>,
) -> OutputStream<PartialStreamValue<T>> {
    Box::pin(
        lift_base(input)
            .zip(fallback)
            .map(|(input, fallback)| match input {
                PartialStreamValue::Deferred => fallback,
                input => input,
            }),
    )
}

pub(super) fn sindex<T: StreamData>(
    input: OutputStream<PartialStreamValue<T>>,
    index: u64,
) -> OutputStream<PartialStreamValue<T>> {
    let Ok(index) = usize::try_from(index) else {
        return Box::pin(stream::empty());
    };
    lift_base(Box::pin(
        stream::repeat(PartialStreamValue::Deferred)
            .take(index)
            .chain(input),
    ))
}

pub(super) fn if_stream<T: StreamData>(
    condition: OutputStream<PartialStreamValue<bool>>,
    then_stream: OutputStream<PartialStreamValue<T>>,
    else_stream: OutputStream<PartialStreamValue<T>>,
) -> OutputStream<PartialStreamValue<T>> {
    Box::pin(
        lift_base(condition)
            .zip(lift_base(then_stream))
            .zip(lift_base(else_stream))
            .map(|((condition, then_value), else_value)| match condition {
                PartialStreamValue::Known(true) => then_value,
                PartialStreamValue::Known(false) => else_value,
                PartialStreamValue::NoVal => PartialStreamValue::NoVal,
                PartialStreamValue::Deferred => PartialStreamValue::Deferred,
            }),
    )
}

#[cfg(test)]
mod tests {
    use futures::{StreamExt, stream};

    use super::*;

    fn values<T: StreamData, const N: usize>(
        values: [PartialStreamValue<T>; N],
    ) -> OutputStream<PartialStreamValue<T>> {
        Box::pin(stream::iter(values))
    }

    #[test]
    fn specialised_arithmetic_operations_and_propagation() {
        smol::block_on(async {
            let known = |value| PartialStreamValue::Known(value);
            assert_eq!(
                add(values([known(7)]), values([known(3)]))
                    .collect::<Vec<_>>()
                    .await,
                [known(10)]
            );
            assert_eq!(
                sub(values([known(7)]), values([known(3)]))
                    .collect::<Vec<_>>()
                    .await,
                [known(4)]
            );
            assert_eq!(
                mul(values([known(7)]), values([known(3)]))
                    .collect::<Vec<_>>()
                    .await,
                [known(21)]
            );
            assert_eq!(
                div(values([known(7)]), values([known(3)]))
                    .collect::<Vec<_>>()
                    .await,
                [known(2)]
            );
            assert_eq!(
                rem(values([known(7)]), values([known(3)]))
                    .collect::<Vec<_>>()
                    .await,
                [known(1)]
            );
            assert_eq!(
                add(
                    values([PartialStreamValue::<i64>::NoVal]),
                    values([known(3)])
                )
                .collect::<Vec<_>>()
                .await,
                [PartialStreamValue::NoVal]
            );
            assert_eq!(
                add(
                    values([PartialStreamValue::<i64>::Deferred]),
                    values([PartialStreamValue::NoVal])
                )
                .collect::<Vec<_>>()
                .await,
                [PartialStreamValue::Deferred]
            );
            assert_eq!(
                abs(values([PartialStreamValue::Known(-1.5)]))
                    .collect::<Vec<_>>()
                    .await,
                [PartialStreamValue::Known(1.5)]
            );
            assert_eq!(
                sin(values([PartialStreamValue::Known(0.0)]))
                    .collect::<Vec<_>>()
                    .await,
                [PartialStreamValue::Known(0.0)]
            );
            assert_eq!(
                cos(values([PartialStreamValue::Known(0.0)]))
                    .collect::<Vec<_>>()
                    .await,
                [PartialStreamValue::Known(1.0)]
            );
            assert_eq!(
                tan(values([PartialStreamValue::Known(0.0)]))
                    .collect::<Vec<_>>()
                    .await,
                [PartialStreamValue::Known(0.0)]
            );
        });
    }

    #[test]
    fn specialised_boolean_and_comparison_operations() {
        smol::block_on(async {
            let known = PartialStreamValue::Known;
            assert_eq!(
                not(values([known(true)])).collect::<Vec<_>>().await,
                [known(false)]
            );
            assert_eq!(
                and(values([known(true)]), values([known(false)]))
                    .collect::<Vec<_>>()
                    .await,
                [known(false)]
            );
            assert_eq!(
                or(values([known(true)]), values([known(false)]))
                    .collect::<Vec<_>>()
                    .await,
                [known(true)]
            );
            assert_eq!(
                implies(values([known(true)]), values([known(false)]))
                    .collect::<Vec<_>>()
                    .await,
                [known(false)]
            );
            assert_eq!(
                compare(
                    values([PartialStreamValue::Known(2_i64)]),
                    values([PartialStreamValue::Known(3_i64)]),
                    |a, b| a < b,
                )
                .collect::<Vec<_>>()
                .await,
                [known(true)]
            );
        });
    }

    #[test]
    fn specialised_temporal_selection_operations() {
        smol::block_on(async {
            let known = PartialStreamValue::Known;
            assert_eq!(
                default(
                    values([known(2), PartialStreamValue::NoVal]),
                    values([known(1), known(1)])
                )
                .collect::<Vec<_>>()
                .await,
                [known(2), known(2)]
            );
            assert_eq!(
                sindex(values([known(1), PartialStreamValue::NoVal]), 1)
                    .collect::<Vec<_>>()
                    .await,
                [PartialStreamValue::Deferred, known(1), known(1)]
            );
            assert_eq!(
                if_stream(
                    values([PartialStreamValue::Known(true), PartialStreamValue::NoVal,]),
                    values([known(1), PartialStreamValue::NoVal]),
                    values([known(3), PartialStreamValue::NoVal])
                )
                .collect::<Vec<_>>()
                .await,
                [known(1), known(1)]
            );
        });
    }
}
