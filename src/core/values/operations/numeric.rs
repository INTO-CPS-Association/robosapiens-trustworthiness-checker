use std::cmp::Ordering;

use super::Value;
use super::dispatch::{BinaryValueOp, ValueOpError, invalid_binary};

pub(super) fn numeric_binary(
    operation: BinaryValueOp,
    left: Value,
    right: Value,
) -> Result<Value, ValueOpError> {
    use BinaryValueOp as Op;

    match (left, right) {
        (Value::Int(left), Value::Int(right)) => {
            let result = match operation {
                Op::Add => left.checked_add(right),
                Op::Sub => left.checked_sub(right),
                Op::Mul => left.checked_mul(right),
                Op::Div => {
                    if right == 0 {
                        return Err(ValueOpError::IntegerDivisionByZero {
                            operation: operation.name(),
                        });
                    }
                    left.checked_div(right)
                }
                Op::Mod => {
                    if right == 0 {
                        return Err(ValueOpError::IntegerDivisionByZero {
                            operation: operation.name(),
                        });
                    }
                    left.checked_rem(right)
                }
                _ => unreachable!(),
            };
            result.map(Value::Int).ok_or(ValueOpError::IntegerOverflow {
                operation: operation.name(),
            })
        }
        (Value::Int(left), Value::Float(right)) => {
            Ok(Value::Float(float_binary(operation, left as f64, right)))
        }
        (Value::Float(left), Value::Int(right)) => {
            Ok(Value::Float(float_binary(operation, left, right as f64)))
        }
        (Value::Float(left), Value::Float(right)) => {
            Ok(Value::Float(float_binary(operation, left, right)))
        }
        (left, right) => invalid_binary(operation, left, right),
    }
}

fn float_binary(operation: BinaryValueOp, left: f64, right: f64) -> f64 {
    match operation {
        BinaryValueOp::Add => left + right,
        BinaryValueOp::Sub => left - right,
        BinaryValueOp::Mul => left * right,
        BinaryValueOp::Div => left / right,
        BinaryValueOp::Mod => left % right,
        _ => unreachable!(),
    }
}

pub(super) fn compare_ordering(
    operation: BinaryValueOp,
    left: Value,
    right: Value,
) -> Result<Option<Ordering>, ValueOpError> {
    let ordering = match (&left, &right) {
        (Value::Int(left), Value::Int(right)) => Some(left.cmp(right)),
        (Value::Int(left), Value::Float(right)) => (*left as f64).partial_cmp(right),
        (Value::Float(left), Value::Int(right)) => left.partial_cmp(&(*right as f64)),
        (Value::Float(left), Value::Float(right)) => left.partial_cmp(right),
        (Value::Bool(left), Value::Bool(right)) => Some(left.cmp(right)),
        (Value::Str(left), Value::Str(right)) => Some(left.cmp(right)),
        _ => return invalid_binary(operation, left, right),
    };
    Ok(ordering)
}
