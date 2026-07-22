use ecow::EcoString;

use super::Value;
use super::dispatch::{ValueOpError, invalid_binary_named, invalid_unary_named};

pub fn list_index(list: Value, index: Value) -> Result<Value, ValueOpError> {
    match (list, index) {
        (Value::List(_), Value::Int(index)) if index < 0 => {
            Err(ValueOpError::NegativeListIndex(index))
        }
        (Value::List(values), Value::Int(index)) => {
            let index = index as usize;
            values
                .get(index)
                .cloned()
                .ok_or(ValueOpError::ListIndexOutOfBounds {
                    index,
                    len: values.len(),
                })
        }
        (list, index) => invalid_binary_named("list indexing", list, index),
    }
}

pub fn list_append(list: Value, value: Value) -> Result<Value, ValueOpError> {
    match list {
        Value::List(mut values) => {
            values.push(value);
            Ok(Value::List(values))
        }
        list => invalid_binary_named("list append", list, value),
    }
}

pub fn list_concat(left: Value, right: Value) -> Result<Value, ValueOpError> {
    match (left, right) {
        (Value::List(mut left), Value::List(right)) => {
            left.extend(right);
            Ok(Value::List(left))
        }
        (left, right) => invalid_binary_named("list concatenation", left, right),
    }
}

pub fn list_head(list: Value) -> Result<Value, ValueOpError> {
    match list {
        Value::List(values) => values.first().cloned().ok_or(ValueOpError::EmptyList),
        operand => invalid_unary_named("list head", operand),
    }
}

pub fn list_tail(list: Value) -> Result<Value, ValueOpError> {
    match list {
        Value::List(values) => values
            .get(1..)
            .map(|tail| Value::List(tail.into()))
            .ok_or(ValueOpError::EmptyList),
        operand => invalid_unary_named("list tail", operand),
    }
}

pub fn list_len(list: Value) -> Result<Value, ValueOpError> {
    match list {
        Value::List(values) => i64::try_from(values.len())
            .map(Value::Int)
            .map_err(|_| ValueOpError::ListLengthOverflow(values.len())),
        operand => invalid_unary_named("list length", operand),
    }
}

pub fn map_get(map: Value, key: &EcoString) -> Result<Value, ValueOpError> {
    match map {
        Value::Map(values) => values
            .get(key)
            .cloned()
            .ok_or_else(|| ValueOpError::MissingMapKey(key.clone())),
        operand => invalid_unary_named("map get", operand),
    }
}

pub fn map_insert(map: Value, key: &EcoString, value: Value) -> Result<Value, ValueOpError> {
    match map {
        Value::Map(mut values) => {
            values.insert(key.clone(), value);
            Ok(Value::Map(values))
        }
        map => invalid_binary_named("map insert", map, value),
    }
}

pub fn map_remove(map: Value, key: &EcoString) -> Result<Value, ValueOpError> {
    match map {
        Value::Map(mut values) => {
            values.remove(key);
            Ok(Value::Map(values))
        }
        operand => invalid_unary_named("map remove", operand),
    }
}

pub fn map_has_key(map: Value, key: &EcoString) -> Result<Value, ValueOpError> {
    match map {
        Value::Map(values) => Ok(Value::Bool(values.contains_key(key))),
        operand => invalid_unary_named("map has-key", operand),
    }
}
