use super::super::*;
use crate::core::values::operations as value_operations;
use crate::core::{BinaryOperator, PartialMarker, UnaryOperator};

pub(in crate::dataflow) fn expect_value(
    result: Result<Value, value_operations::ValueOpError>,
) -> Value {
    result.unwrap_or_else(|error| panic!("{error}"))
}

pub(in crate::dataflow) use crate::core::retain_last as retain_last_value;

pub(in crate::dataflow) fn lift_one(value: Value, f: impl FnOnce(Value) -> Value) -> Value {
    match PartialMarker::of(&value) {
        Some(marker) => marker.into_value(),
        None => f(value),
    }
}

pub(in crate::dataflow) fn lift_two(
    lhs: Value,
    rhs: Value,
    f: impl FnOnce(Value, Value) -> Value,
) -> Value {
    match propagated_special([&lhs, &rhs]) {
        Some(value) => value,
        None => f(lhs, rhs),
    }
}

pub(in crate::dataflow) fn propagated_special<'a>(
    values: impl IntoIterator<Item = &'a Value>,
) -> Option<Value> {
    crate::core::propagated_special(values.into_iter().map(PartialMarker::of))
        .map(PartialMarker::into_value)
}

pub(in crate::dataflow) fn lift_many(
    values: Vec<Value>,
    f: impl FnOnce(Vec<Value>) -> Value,
) -> Value {
    if let Some(value) = propagated_special(values.iter()) {
        value
    } else {
        f(values)
    }
}

pub(in crate::dataflow) fn lift_map_values(
    values: Vec<(EcoString, Value)>,
    f: impl FnOnce(BTreeMap<EcoString, Value>) -> Value,
) -> Value {
    if let Some(value) = propagated_special(values.iter().map(|(_, value)| value)) {
        value
    } else {
        f(values.into_iter().collect())
    }
}

pub(in crate::dataflow) fn lift_unary_with_state(
    op: UnaryOperator,
    value: Value,
    last: &mut Option<Value>,
) -> Value {
    let value = retain_last_value(value, last);
    lift_one(value, |value| {
        expect_value(value_operations::unary(op, value))
    })
}

pub(in crate::dataflow) fn lift_binary_with_state(
    op: BinaryOperator,
    lhs: Value,
    rhs: Value,
    last_left: &mut Option<Value>,
    last_right: &mut Option<Value>,
) -> Value {
    let lhs = retain_last_value(lhs, last_left);
    let rhs = retain_last_value(rhs, last_right);
    lift_two(lhs, rhs, |lhs, rhs| {
        expect_value(value_operations::binary(op, lhs, rhs))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_marker_policy_agrees_across_partial_value_representations() {
        use crate::core::{DeferrableStreamData, PartialStreamValue};
        use crate::dataflow::execution::quickening::ScalarValue;

        fn trace<T: DeferrableStreamData>(known: T) -> Vec<Option<PartialMarker>> {
            let mut left = None;
            let mut right = None;
            [
                (T::deferred_value(), T::no_val_value()),
                (T::no_val_value(), known),
                (T::no_val_value(), T::no_val_value()),
            ]
            .into_iter()
            .map(|(a, b)| {
                let a = crate::core::retain_last(a, &mut left);
                let b = crate::core::retain_last(b, &mut right);
                crate::core::propagated_special([PartialMarker::of(&a), PartialMarker::of(&b)])
            })
            .collect()
        }
        let expected = vec![
            Some(PartialMarker::NoVal),
            Some(PartialMarker::Deferred),
            Some(PartialMarker::Deferred),
        ];
        assert_eq!(trace(Value::Int(1)), expected);
        assert_eq!(trace(PartialStreamValue::Known(1_i64)), expected);
        assert_eq!(trace(ScalarValue::Int(1)), expected);
    }

    #[test]
    fn numeric_negation_handles_both_numeric_types_and_stream_markers() {
        for (input, expected) in [
            (Value::Int(7), Value::Int(-7)),
            (Value::Float(1.5), Value::Float(-1.5)),
            (Value::NoVal, Value::NoVal),
            (Value::Deferred, Value::Deferred),
        ] {
            let mut last = None;
            assert_eq!(
                lift_unary_with_state(UnaryOperator::Negate, input, &mut last),
                expected
            );
        }
    }

    #[test]
    fn propagated_special_prioritizes_no_val_over_deferred() {
        assert_eq!(
            propagated_special([&Value::Deferred, &Value::NoVal]),
            Some(Value::NoVal)
        );
        assert_eq!(
            propagated_special([&Value::Int(1), &Value::Deferred]),
            Some(Value::Deferred)
        );
        assert_eq!(propagated_special([&Value::Int(1)]), None);
    }
}
