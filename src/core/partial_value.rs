use futures::StreamExt;

use super::{DeferrableStreamData, LocalStream, StreamData};

/// Retain the latest present stream value.
///
/// An absent value before any retained history is returned unchanged so representations carrying
/// absence metadata do not lose it.
#[inline]
pub(crate) fn retain_last<T: StreamData>(current: T, retained: &mut Option<T>) -> T {
    if current.is_no_val() {
        retained.clone().unwrap_or(current)
    } else {
        *retained = Some(current.clone());
        current
    }
}

/// Retain the latest present value throughout a stream.
pub(crate) fn retain_stream<T: StreamData>(mut input: LocalStream<T>) -> LocalStream<T> {
    Box::pin(async_stream::stream! {
        let mut retained = None;
        while let Some(current) = input.next().await {
            yield retain_last(current, &mut retained);
        }
    })
}

/// A marker propagated by a strict partial-value operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PartialMarker {
    NoVal,
    Deferred,
}

impl PartialMarker {
    #[inline]
    pub(crate) fn of<T: DeferrableStreamData>(value: &T) -> Option<Self> {
        if value.is_no_val() {
            Some(Self::NoVal)
        } else if value.is_deferred() {
            Some(Self::Deferred)
        } else {
            None
        }
    }

    #[inline]
    pub(crate) fn into_value<T: DeferrableStreamData>(self) -> T {
        match self {
            Self::NoVal => T::no_val_value(),
            Self::Deferred => T::deferred_value(),
        }
    }
}

/// Select the marker for a strict operation, giving absence precedence over deferral.
#[inline]
pub(crate) fn propagated_special(
    markers: impl IntoIterator<Item = Option<PartialMarker>>,
) -> Option<PartialMarker> {
    let mut propagated = None;
    for marker in markers.into_iter().flatten() {
        match marker {
            PartialMarker::NoVal => return Some(PartialMarker::NoVal),
            PartialMarker::Deferred => propagated = Some(PartialMarker::Deferred),
        }
    }
    propagated
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum AnnotatedValue {
        Known(i64),
        NoVal(&'static str),
        Deferred,
    }

    impl StreamData for AnnotatedValue {
        fn is_no_val(&self) -> bool {
            matches!(self, Self::NoVal(_))
        }
    }

    impl DeferrableStreamData for AnnotatedValue {
        fn is_deferred(&self) -> bool {
            matches!(self, Self::Deferred)
        }

        fn deferred_value() -> Self {
            Self::Deferred
        }

        fn no_val_value() -> Self {
            Self::NoVal("constructed")
        }
    }

    #[test]
    fn retention_preserves_leading_absence_and_then_reuses_history() {
        let mut retained = None;
        assert_eq!(
            retain_last(AnnotatedValue::NoVal("observed"), &mut retained),
            AnnotatedValue::NoVal("observed")
        );
        assert_eq!(retained, None);

        assert_eq!(
            retain_last(AnnotatedValue::Known(3), &mut retained),
            AnnotatedValue::Known(3)
        );
        assert_eq!(
            retain_last(AnnotatedValue::NoVal("later"), &mut retained),
            AnnotatedValue::Known(3)
        );
    }

    #[test]
    fn stream_retention_uses_the_same_contract() {
        smol::block_on(async {
            let input = futures::stream::iter([
                AnnotatedValue::NoVal("leading"),
                AnnotatedValue::Known(3),
                AnnotatedValue::NoVal("later"),
            ]);
            assert_eq!(
                retain_stream(Box::pin(input)).collect::<Vec<_>>().await,
                [
                    AnnotatedValue::NoVal("leading"),
                    AnnotatedValue::Known(3),
                    AnnotatedValue::Known(3),
                ]
            );
        });
    }

    #[test]
    fn strict_propagation_gives_no_val_precedence() {
        let deferred = AnnotatedValue::Deferred;
        let absent = AnnotatedValue::NoVal("observed");
        let known = AnnotatedValue::Known(3);

        assert_eq!(
            propagated_special([
                PartialMarker::of(&deferred),
                PartialMarker::of(&absent),
                PartialMarker::of(&known),
            ]),
            Some(PartialMarker::NoVal)
        );
        assert_eq!(
            propagated_special([PartialMarker::of(&known), PartialMarker::of(&deferred)]),
            Some(PartialMarker::Deferred)
        );
        assert_eq!(propagated_special([PartialMarker::of(&known)]), None);
        assert_eq!(
            PartialMarker::NoVal.into_value::<AnnotatedValue>(),
            AnnotatedValue::NoVal("constructed")
        );
    }
}
