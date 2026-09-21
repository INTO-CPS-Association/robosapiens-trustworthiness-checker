use std::fmt;

use ecow::EcoString;

use super::{Value, numeric};
use crate::core::values::union::check_value_conformance;
use crate::core::{BinaryOperator, StreamType, UnaryOperator};

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
    NegativeIntegerExponent {
        exponent: i64,
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
    /// A rounding function met a `Float` with no `Int` value: NaN, an
    /// infinity, or one outside the `Int` range once rounded.
    UnrepresentableInteger {
        operation: &'static str,
        value: f64,
    },
    /// A cast of a value whose type was only known at run time, to a type it
    /// cannot be converted to.
    InvalidCast {
        value: Value,
        target: StreamType,
    },
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
            Self::NegativeIntegerExponent { exponent } => {
                write!(
                    f,
                    "negative integer exponent for exponentiation: {exponent}"
                )
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
            Self::UnrepresentableInteger { operation, value } => {
                write!(f, "{operation} of {value} is not an Int")
            }
            Self::InvalidCast { value, target } => {
                write!(f, "cannot cast {value} to {target}")
            }
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
        (
            UnaryOperator::Truncate
            | UnaryOperator::Floor
            | UnaryOperator::Ceiling
            | UnaryOperator::Round,
            Value::Float(value),
        ) => round_to_int(operation, value).map(Value::Int),
        (UnaryOperator::CastFloat, Value::Int(value)) => Ok(Value::Float(value as f64)),
        (UnaryOperator::CastFloat, value @ Value::Float(_))
        | (UnaryOperator::CastStr, value @ Value::Str(_)) => Ok(value),
        (
            UnaryOperator::CastStr,
            value @ (Value::Int(_) | Value::Float(_) | Value::Bool(_) | Value::Unit),
        ) => Ok(Value::Str(value.to_string().into())),
        (_, operand) => Err(ValueOpError::InvalidUnaryOperand {
            operation: operation.name(),
            operand,
        }),
    }
}

/// Round `value` to an `Int` as `operation` says. `round` takes a tie to the
/// even neighbour, the same rounding every `Int` to `Float` conversion uses.
pub(crate) fn round_to_int(operation: UnaryOperator, value: f64) -> Result<i64, ValueOpError> {
    let rounded = match operation {
        UnaryOperator::Truncate => value.trunc(),
        UnaryOperator::Floor => value.floor(),
        UnaryOperator::Ceiling => value.ceil(),
        UnaryOperator::Round => value.round_ties_even(),
        _ => unreachable!("{} does not round", operation.name()),
    };
    // -2^63 and 2^63 are exact as floats; the range is [-2^63, 2^63). A NaN
    // or an infinity fails the same comparison.
    const LIMIT: f64 = 9_223_372_036_854_775_808.0;
    if (-LIMIT..LIMIT).contains(&rounded) {
        Ok(rounded as i64)
    } else {
        Err(ValueOpError::UnrepresentableInteger {
            operation: operation.name(),
            value,
        })
    }
}

/// Evaluate `value as target`. An `Int` becomes a `Float`, rounded to the
/// nearest when beyond 2^53; an `Int`, `Float`, `Bool` or `Unit` becomes the
/// `Str` it prints as; any value that already has the target type is
/// returned unchanged. Checking admits nothing else, except from a value whose
/// type was only known at run time, which fails here.
pub fn cast(value: Value, target: &StreamType) -> Result<Value, ValueOpError> {
    match (value, target) {
        (Value::Int(value), StreamType::Float) => Ok(Value::Float(value as f64)),
        (
            value @ (Value::Int(_) | Value::Float(_) | Value::Bool(_) | Value::Unit),
            StreamType::Str,
        ) => Ok(Value::Str(value.to_string().into())),
        // The outer representation is all a function or source value can
        // show at run time.
        (value @ Value::Function(_), StreamType::Function(..))
        | (value @ Value::Str(_), StreamType::Expr(_)) => Ok(value),
        (value, target) if check_value_conformance(&value, target).is_ok() => Ok(value),
        (value, target) => Err(ValueOpError::InvalidCast {
            value,
            target: target.clone(),
        }),
    }
}

pub fn binary(operation: BinaryOperator, left: Value, right: Value) -> Result<Value, ValueOpError> {
    use BinaryOperator as Op;

    match operation {
        Op::Add | Op::Subtract | Op::Multiply | Op::Divide | Op::Modulo | Op::Power => {
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
        Op::NotEqual => Ok(Value::Bool(left != right)),
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
