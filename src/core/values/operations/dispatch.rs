use std::fmt;

use ecow::EcoString;

use super::{Value, numeric};
use crate::core::{BinaryOperator, UnaryOperator};

#[derive(Debug, Clone, PartialEq)]
pub enum ValueOpError {
    InvalidUnaryOperand {
        operation: &'static str,
        operand: Value,
    },
    InvalidBinaryOperands {
        operation: &'static str,
        left: Value,
        right: Value,
    },
    IntegerOverflow {
        operation: &'static str,
    },
    IntegerDivisionByZero {
        operation: &'static str,
    },
    NegativeListIndex(i64),
    ListIndexOutOfBounds {
        index: usize,
        len: usize,
    },
    TupleIndexOutOfBounds {
        index: usize,
        len: usize,
    },
    EmptyList,
    ListLengthOverflow(usize),
    MissingMapKey(EcoString),
}

impl fmt::Display for ValueOpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUnaryOperand { operation, operand } => {
                write!(f, "invalid operand for {operation}: {operand:?}")
            }
            Self::InvalidBinaryOperands {
                operation,
                left,
                right,
            } => write!(f, "invalid operands for {operation}: {left:?}, {right:?}"),
            Self::IntegerOverflow { operation } => {
                write!(f, "integer overflow during {operation}")
            }
            Self::IntegerDivisionByZero { operation } => {
                write!(f, "integer division by zero during {operation}")
            }
            Self::NegativeListIndex(index) => {
                write!(f, "List index must be non-negative: {index}")
            }
            Self::ListIndexOutOfBounds { index, len } => {
                write!(f, "List index out of bounds: index {index}, length {len}")
            }
            Self::TupleIndexOutOfBounds { index, len } => {
                write!(f, "tuple index out of bounds: index {index}, length {len}")
            }
            Self::EmptyList => write!(f, "List is empty"),
            Self::ListLengthOverflow(len) => {
                write!(f, "list length {len} does not fit in an integer Value")
            }
            Self::MissingMapKey(key) => write!(f, "Missing key for map get: {key}"),
        }
    }
}

impl std::error::Error for ValueOpError {}

pub fn unary(operation: UnaryOperator, operand: Value) -> Result<Value, ValueOpError> {
    match (operation, operand) {
        (UnaryOperator::Not, Value::Bool(value)) => Ok(Value::Bool(!value)),
        (UnaryOperator::Negate, Value::Int(value)) => {
            value
                .checked_neg()
                .map(Value::Int)
                .ok_or(ValueOpError::IntegerOverflow {
                    operation: operation.name(),
                })
        }
        (UnaryOperator::Negate, Value::Float(value)) => Ok(Value::Float(-value)),
        (UnaryOperator::Sin, Value::Float(value)) => Ok(Value::Float(value.sin())),
        (UnaryOperator::Cos, Value::Float(value)) => Ok(Value::Float(value.cos())),
        (UnaryOperator::Tan, Value::Float(value)) => Ok(Value::Float(value.tan())),
        (UnaryOperator::Absolute, Value::Int(value)) => {
            value
                .checked_abs()
                .map(Value::Int)
                .ok_or(ValueOpError::IntegerOverflow {
                    operation: operation.name(),
                })
        }
        (UnaryOperator::Absolute, Value::Float(value)) => Ok(Value::Float(value.abs())),
        (_, operand) => Err(ValueOpError::InvalidUnaryOperand {
            operation: operation.name(),
            operand,
        }),
    }
}

pub fn binary(operation: BinaryOperator, left: Value, right: Value) -> Result<Value, ValueOpError> {
    use BinaryOperator as Op;

    match operation {
        Op::Add | Op::Subtract | Op::Multiply | Op::Divide | Op::Modulo => {
            numeric::numeric_binary(operation, left, right)
        }
        Op::Or | Op::And | Op::Implication => match (left, right) {
            (Value::Bool(left), Value::Bool(right)) => Ok(Value::Bool(match operation {
                Op::Or => left || right,
                Op::And => left && right,
                Op::Implication => !left || right,
                _ => unreachable!(),
            })),
            (left, right) => invalid_binary(operation, left, right),
        },
        Op::Concatenate => match (left, right) {
            (Value::Str(mut left), Value::Str(right)) => {
                left.push_str(right.as_str());
                Ok(Value::Str(left))
            }
            (left, right) => invalid_binary(operation, left, right),
        },
        Op::Equal => Ok(Value::Bool(left == right)),
        Op::LessEqual | Op::Less | Op::GreaterEqual | Op::Greater => {
            let ordering = numeric::compare_ordering(operation, left, right)?;
            Ok(Value::Bool(ordering.is_some_and(
                |ordering| match operation {
                    Op::LessEqual => ordering.is_le(),
                    Op::Less => ordering.is_lt(),
                    Op::GreaterEqual => ordering.is_ge(),
                    Op::Greater => ordering.is_gt(),
                    _ => unreachable!(),
                },
            )))
        }
    }
}

pub(super) fn invalid_binary<T>(
    operation: BinaryOperator,
    left: Value,
    right: Value,
) -> Result<T, ValueOpError> {
    invalid_binary_named(operation.name(), left, right)
}

pub(super) fn invalid_unary_named<T>(
    operation: &'static str,
    operand: Value,
) -> Result<T, ValueOpError> {
    Err(ValueOpError::InvalidUnaryOperand { operation, operand })
}

pub(super) fn invalid_binary_named<T>(
    operation: &'static str,
    left: Value,
    right: Value,
) -> Result<T, ValueOpError> {
    Err(ValueOpError::InvalidBinaryOperands {
        operation,
        left,
        right,
    })
}
