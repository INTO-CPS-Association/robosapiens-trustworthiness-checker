use super::super::plan::*;
use super::super::*;
use crate::core::values::operations::{self as value_operations, BinaryValueOp, UnaryValueOp};

fn unwrap_value(result: Result<Value, value_operations::ValueOpError>) -> Value {
    result.unwrap_or_else(|error| panic!("{error}"))
}

pub(in crate::dataflow) fn stream_lift_value(value: Value, last: &mut Option<Value>) -> Value {
    match value {
        Value::NoVal => last.clone().unwrap_or(Value::NoVal),
        value => {
            *last = Some(value.clone());
            value
        }
    }
}

pub(in crate::dataflow) fn lift_one(value: Value, f: impl FnOnce(Value) -> Value) -> Value {
    match value {
        Value::NoVal => Value::NoVal,
        Value::Deferred => Value::Deferred,
        value => f(value),
    }
}

pub(in crate::dataflow) fn lift_two(
    lhs: Value,
    rhs: Value,
    f: impl FnOnce(Value, Value) -> Value,
) -> Value {
    match (lhs, rhs) {
        (Value::NoVal, _) | (_, Value::NoVal) => Value::NoVal,
        (Value::Deferred, _) | (_, Value::Deferred) => Value::Deferred,
        (lhs, rhs) => f(lhs, rhs),
    }
}

pub(in crate::dataflow) fn propagated_special<'a>(
    values: impl IntoIterator<Item = &'a Value>,
) -> Option<Value> {
    let mut deferred = false;
    for value in values {
        match value {
            Value::NoVal => return Some(Value::NoVal),
            Value::Deferred => deferred = true,
            _ => {}
        }
    }
    deferred.then_some(Value::Deferred)
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

pub(in crate::dataflow) fn eval_lindex(list: Value, idx: Value) -> Value {
    unwrap_value(value_operations::list_index(list, idx))
}

pub(in crate::dataflow) fn eval_lappend(list: Value, value: Value) -> Value {
    unwrap_value(value_operations::list_append(list, value))
}

pub(in crate::dataflow) fn eval_lconcat(lhs: Value, rhs: Value) -> Value {
    unwrap_value(value_operations::list_concat(lhs, rhs))
}

pub(in crate::dataflow) fn eval_lhead(list: Value) -> Value {
    unwrap_value(value_operations::list_head(list))
}

pub(in crate::dataflow) fn eval_ltail(list: Value) -> Value {
    unwrap_value(value_operations::list_tail(list))
}

pub(in crate::dataflow) fn eval_llen(list: Value) -> Value {
    unwrap_value(value_operations::list_len(list))
}

pub(in crate::dataflow) fn eval_mget(map: Value, key: &EcoString) -> Value {
    unwrap_value(value_operations::map_get(map, key))
}

pub(in crate::dataflow) fn eval_mremove(map: Value, key: &EcoString) -> Value {
    unwrap_value(value_operations::map_remove(map, key))
}

pub(in crate::dataflow) fn eval_minsert(map: Value, key: &EcoString, value: Value) -> Value {
    unwrap_value(value_operations::map_insert(map, key, value))
}

pub(in crate::dataflow) fn eval_mhas_key(map: Value, key: &EcoString) -> Value {
    unwrap_value(value_operations::map_has_key(map, key))
}

pub(in crate::dataflow) fn eval_tget(tuple: Value, index: usize) -> Value {
    match tuple {
        Value::Tuple(values) | Value::List(values) => values
            .get(index)
            .cloned()
            .unwrap_or_else(|| panic!("Tuple index out of bounds: {}", index)),
        other => panic!("Expected tuple for .{} access, got {}", index, other),
    }
}

pub(in crate::dataflow) fn lift_unary_with_state(
    op: DataflowUnaryOp,
    value: Value,
    last: &mut Option<Value>,
) -> Value {
    let value = stream_lift_value(value, last);
    if value == Value::NoVal || value == Value::Deferred {
        return value;
    }
    let operation = match op {
        DataflowUnaryOp::Not => UnaryValueOp::Not,
        DataflowUnaryOp::Neg => UnaryValueOp::Neg,
        DataflowUnaryOp::Sin => UnaryValueOp::Sin,
        DataflowUnaryOp::Cos => UnaryValueOp::Cos,
        DataflowUnaryOp::Tan => UnaryValueOp::Tan,
        DataflowUnaryOp::Abs => UnaryValueOp::Abs,
    };
    unwrap_value(value_operations::unary(operation, value))
}

pub(in crate::dataflow) fn lift_binary_with_state(
    op: DataflowBinaryOp,
    lhs: Value,
    rhs: Value,
    lhs_last: &mut Option<Value>,
    rhs_last: &mut Option<Value>,
) -> Value {
    let lhs = stream_lift_value(lhs, lhs_last);
    let rhs = stream_lift_value(rhs, rhs_last);
    if lhs == Value::NoVal || rhs == Value::NoVal {
        return Value::NoVal;
    }
    if lhs == Value::Deferred || rhs == Value::Deferred {
        return Value::Deferred;
    }
    eval_binary(op, lhs, rhs)
}

pub(in crate::dataflow) fn eval_binary(op: DataflowBinaryOp, lhs: Value, rhs: Value) -> Value {
    let operation = match op {
        DataflowBinaryOp::Add => BinaryValueOp::Add,
        DataflowBinaryOp::Sub => BinaryValueOp::Sub,
        DataflowBinaryOp::Mul => BinaryValueOp::Mul,
        DataflowBinaryOp::Div => BinaryValueOp::Div,
        DataflowBinaryOp::Mod => BinaryValueOp::Mod,
        DataflowBinaryOp::Or => BinaryValueOp::Or,
        DataflowBinaryOp::And => BinaryValueOp::And,
        DataflowBinaryOp::Impl => BinaryValueOp::Implication,
        DataflowBinaryOp::Concat => BinaryValueOp::Concat,
        DataflowBinaryOp::Eq => BinaryValueOp::Equal,
        DataflowBinaryOp::Le => BinaryValueOp::LessEqual,
        DataflowBinaryOp::Lt => BinaryValueOp::Less,
        DataflowBinaryOp::Ge => BinaryValueOp::GreaterEqual,
        DataflowBinaryOp::Gt => BinaryValueOp::Greater,
    };
    unwrap_value(value_operations::binary(operation, lhs, rhs))
}

#[cfg(test)]
mod tests {
    use super::*;

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
                lift_unary_with_state(DataflowUnaryOp::Neg, input, &mut last),
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
