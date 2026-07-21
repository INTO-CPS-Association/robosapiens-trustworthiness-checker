use super::super::plan::*;
use super::super::*;

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
    match (list, idx) {
        (Value::List(list), Value::Int(idx)) if idx >= 0 => list
            .get(idx as usize)
            .cloned()
            .unwrap_or_else(|| panic!("List index out of bounds: {}", idx)),
        (Value::List(_), Value::Int(idx)) => panic!("List index must be non-negative: {}", idx),
        (list, idx) => panic!(
            "Invalid list index. Expected List and Int expressions. Received: List.get({:?}, {:?})",
            list, idx
        ),
    }
}

pub(in crate::dataflow) fn eval_lappend(list: Value, value: Value) -> Value {
    match list {
        Value::List(mut list) => {
            list.push(value);
            Value::List(list)
        }
        list => panic!(
            "Invalid list append. Expected List and Value expressions. Received: List.append({:?}, {:?})",
            list, value
        ),
    }
}

pub(in crate::dataflow) fn eval_lconcat(lhs: Value, rhs: Value) -> Value {
    match (lhs, rhs) {
        (Value::List(mut lhs), Value::List(rhs)) => {
            lhs.extend(rhs);
            Value::List(lhs)
        }
        (lhs, rhs) => panic!(
            "Invalid list concatenation. Expected List and List expressions. Received: List.concat({:?}, {:?})",
            lhs, rhs
        ),
    }
}

pub(in crate::dataflow) fn eval_lhead(list: Value) -> Value {
    match list {
        Value::List(list) => list
            .first()
            .cloned()
            .unwrap_or_else(|| panic!("List is empty")),
        list => panic!(
            "Invalid list head. Expected List expression. Received: List.head({:?})",
            list
        ),
    }
}

pub(in crate::dataflow) fn eval_ltail(list: Value) -> Value {
    match list {
        Value::List(list) => list
            .get(1..)
            .map(|tail| Value::List(EcoVec::from(tail)))
            .unwrap_or_else(|| panic!("List is empty")),
        list => panic!(
            "Invalid list tail. Expected List expression. Received: List.tail({:?})",
            list
        ),
    }
}

pub(in crate::dataflow) fn eval_llen(list: Value) -> Value {
    match list {
        Value::List(list) => Value::Int(list.len() as i64),
        list => panic!(
            "Invalid list len. Expected List expression. Received: List.len({:?})",
            list
        ),
    }
}

pub(in crate::dataflow) fn eval_mget(map: Value, key: &EcoString) -> Value {
    match map {
        Value::Map(map) => map
            .get(key)
            .cloned()
            .unwrap_or_else(|| panic!("Missing key for map get: {}", key)),
        map => panic!(
            "Invalid map get. Expected Map expression. Received: Map.get({:?})",
            map
        ),
    }
}

pub(in crate::dataflow) fn eval_mremove(map: Value, key: &EcoString) -> Value {
    match map {
        Value::Map(mut map) => {
            map.remove(key);
            Value::Map(map)
        }
        map => panic!(
            "Invalid map remove. Expected Map expression. Received: Map.remove({:?})",
            map
        ),
    }
}

pub(in crate::dataflow) fn eval_minsert(map: Value, key: &EcoString, value: Value) -> Value {
    match map {
        Value::Map(mut map) => {
            map.insert(key.clone(), value);
            Value::Map(map)
        }
        map => panic!(
            "Invalid map insert. Expected Map expression. Received: Map.insert({:?})",
            map
        ),
    }
}

pub(in crate::dataflow) fn eval_mhas_key(map: Value, key: &EcoString) -> Value {
    match map {
        Value::Map(map) => Value::Bool(map.contains_key(key)),
        map => panic!(
            "Invalid map has_key. Expected Map expression. Received: Map.has_key({:?})",
            map
        ),
    }
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
    match op {
        DataflowUnaryOp::Not => match value {
            Value::Bool(v) => Value::Bool(!v),
            other => panic!("invalid NOT operand {:?}", other),
        },
        DataflowUnaryOp::Neg => match value {
            Value::Int(v) => Value::Int(-v),
            Value::Float(v) => Value::Float(-v),
            other => panic!("invalid unary minus operand {:?}", other),
        },
        DataflowUnaryOp::Sin => match value {
            Value::Float(v) => Value::Float(v.sin()),
            other => panic!("invalid sin operand {:?}", other),
        },
        DataflowUnaryOp::Cos => match value {
            Value::Float(v) => Value::Float(v.cos()),
            other => panic!("invalid cos operand {:?}", other),
        },
        DataflowUnaryOp::Tan => match value {
            Value::Float(v) => Value::Float(v.tan()),
            other => panic!("invalid tan operand {:?}", other),
        },
        DataflowUnaryOp::Abs => match value {
            Value::Int(v) => Value::Int(v.abs()),
            Value::Float(v) => Value::Float(v.abs()),
            other => panic!("invalid abs operand {:?}", other),
        },
    }
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
    match op {
        DataflowBinaryOp::Add => match (lhs, rhs) {
            (Value::Int(lhs), Value::Int(rhs)) => Value::Int(lhs + rhs),
            (Value::Int(lhs), Value::Float(rhs)) => Value::Float(lhs as f64 + rhs),
            (Value::Float(lhs), Value::Int(rhs)) => Value::Float(lhs + rhs as f64),
            (Value::Float(lhs), Value::Float(rhs)) => Value::Float(lhs + rhs),
            (lhs, rhs) => panic!("invalid add operands {:?}, {:?}", lhs, rhs),
        },
        DataflowBinaryOp::Sub => match (lhs, rhs) {
            (Value::Int(lhs), Value::Int(rhs)) => Value::Int(lhs - rhs),
            (Value::Int(lhs), Value::Float(rhs)) => Value::Float(lhs as f64 - rhs),
            (Value::Float(lhs), Value::Int(rhs)) => Value::Float(lhs - rhs as f64),
            (Value::Float(lhs), Value::Float(rhs)) => Value::Float(lhs - rhs),
            (lhs, rhs) => panic!("invalid subtract operands {:?}, {:?}", lhs, rhs),
        },
        DataflowBinaryOp::Mul => match (lhs, rhs) {
            (Value::Int(lhs), Value::Int(rhs)) => Value::Int(lhs * rhs),
            (Value::Int(lhs), Value::Float(rhs)) => Value::Float(lhs as f64 * rhs),
            (Value::Float(lhs), Value::Int(rhs)) => Value::Float(lhs * rhs as f64),
            (Value::Float(lhs), Value::Float(rhs)) => Value::Float(lhs * rhs),
            (lhs, rhs) => panic!("invalid multiply operands {:?}, {:?}", lhs, rhs),
        },
        DataflowBinaryOp::Div => match (lhs, rhs) {
            (Value::Int(lhs), Value::Int(rhs)) => Value::Int(lhs / rhs),
            (Value::Int(lhs), Value::Float(rhs)) => Value::Float(lhs as f64 / rhs),
            (Value::Float(lhs), Value::Int(rhs)) => Value::Float(lhs / rhs as f64),
            (Value::Float(lhs), Value::Float(rhs)) => Value::Float(lhs / rhs),
            (lhs, rhs) => panic!("invalid divide operands {:?}, {:?}", lhs, rhs),
        },
        DataflowBinaryOp::Mod => match (lhs, rhs) {
            (Value::Int(lhs), Value::Int(rhs)) => Value::Int(lhs % rhs),
            (Value::Int(lhs), Value::Float(rhs)) => Value::Float(lhs as f64 % rhs),
            (Value::Float(lhs), Value::Int(rhs)) => Value::Float(lhs % rhs as f64),
            (Value::Float(lhs), Value::Float(rhs)) => Value::Float(lhs % rhs),
            (lhs, rhs) => panic!("invalid modulo operands {:?}, {:?}", lhs, rhs),
        },
        DataflowBinaryOp::Or => match (lhs, rhs) {
            (Value::Bool(lhs), Value::Bool(rhs)) => Value::Bool(lhs || rhs),
            (lhs, rhs) => panic!("invalid or operands {:?}, {:?}", lhs, rhs),
        },
        DataflowBinaryOp::And => match (lhs, rhs) {
            (Value::Bool(lhs), Value::Bool(rhs)) => Value::Bool(lhs && rhs),
            (lhs, rhs) => panic!("invalid and operands {:?}, {:?}", lhs, rhs),
        },
        DataflowBinaryOp::Impl => match (lhs, rhs) {
            (Value::Bool(lhs), Value::Bool(rhs)) => Value::Bool(!lhs || rhs),
            (lhs, rhs) => panic!("invalid implication operands {:?}, {:?}", lhs, rhs),
        },
        DataflowBinaryOp::Concat => match (lhs, rhs) {
            (Value::Str(lhs), Value::Str(rhs)) => Value::Str(format!("{lhs}{rhs}").into()),
            (lhs, rhs) => panic!("invalid concat operands {:?}, {:?}", lhs, rhs),
        },
        DataflowBinaryOp::Eq => Value::Bool(lhs == rhs),
        DataflowBinaryOp::Le => compare_values(lhs, rhs, |ord| !ord.is_gt()),
        DataflowBinaryOp::Lt => compare_values(lhs, rhs, |ord| ord.is_lt()),
        DataflowBinaryOp::Ge => compare_values(lhs, rhs, |ord| !ord.is_lt()),
        DataflowBinaryOp::Gt => compare_values(lhs, rhs, |ord| ord.is_gt()),
    }
}

pub(in crate::dataflow) fn compare_values(
    lhs: Value,
    rhs: Value,
    f: impl FnOnce(std::cmp::Ordering) -> bool,
) -> Value {
    let ordering = match (lhs, rhs) {
        (Value::Int(lhs), Value::Int(rhs)) => lhs.cmp(&rhs),
        (Value::Int(lhs), Value::Float(rhs)) => (lhs as f64).partial_cmp(&rhs).unwrap(),
        (Value::Float(lhs), Value::Int(rhs)) => lhs.partial_cmp(&(rhs as f64)).unwrap(),
        (Value::Float(lhs), Value::Float(rhs)) => lhs.partial_cmp(&rhs).unwrap(),
        (Value::Bool(lhs), Value::Bool(rhs)) => lhs.cmp(&rhs),
        (Value::Str(lhs), Value::Str(rhs)) => lhs.cmp(&rhs),
        (lhs, rhs) => panic!("invalid comparison operands {:?}, {:?}", lhs, rhs),
    };
    Value::Bool(f(ordering))
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
