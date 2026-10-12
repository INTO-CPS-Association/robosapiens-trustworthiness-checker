use std::fmt;

use ecow::EcoString;

use super::{Value, numeric};
use crate::core::values::union::check_value_conformance;
use crate::core::values::{NewtypeSchema, NewtypeValue};
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
    /// `Name::value` of a value that is not of that newtype: another
    /// newtype's, or a bare representation that never became one.
    NewtypeMismatch {
        expected: EcoString,
        value: Value,
    },
    /// `==` or `!=` between a newtype value and a value of another newtype,
    /// or one that is not a newtype value at all: an identity mismatch, not
    /// a difference.
    NewtypeComparison {
        left: Value,
        right: Value,
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
                write!(f, "cannot cast {value} to {target}")?;
                if let Err(reason) = check_value_conformance(value, target) {
                    write!(f, ": {reason}")?;
                }
                Ok(())
            }
            Self::NewtypeComparison { left, right } => {
                let describe = |value: &Value| match value {
                    Value::Newtype(value) => format!("a value of newtype {}", value.brand()),
                    _ => "not a newtype value".to_owned(),
                };
                write!(
                    f,
                    "cannot compare {left} with {right}: {} and {} are different types",
                    describe(left),
                    describe(right)
                )
            }
            Self::NewtypeMismatch { expected, value } => {
                write!(
                    f,
                    "cannot take the value of {value} as newtype {expected}: "
                )?;
                match value {
                    Value::Newtype(value) => {
                        write!(f, "it is a value of newtype {}", value.brand())
                    }
                    _ => write!(f, "it is not a newtype value"),
                }
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

/// Build a value of `newtype` around `value`, which is what `Name::new`
/// elaborates to. Elaboration has already checked the representation, so
/// this only brands it.
pub fn newtype_wrap(value: Value, newtype: &NewtypeSchema<StreamType>) -> Value {
    NewtypeValue::new(newtype.brand().clone(), value).into()
}

/// The representation a value of `newtype` holds, which is what
/// `Name::value` elaborates to. A value of any other newtype, or one that
/// never was a newtype, is refused: this is the first use at which an
/// unchecked input's identity is established.
pub fn newtype_unwrap(
    value: Value,
    newtype: &NewtypeSchema<StreamType>,
) -> Result<Value, ValueOpError> {
    match value {
        Value::Newtype(held) if held.brand() == newtype.brand() => Ok(held.value().clone()),
        value => Err(ValueOpError::NewtypeMismatch {
            expected: newtype.brand().clone(),
            value,
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
        Op::Equal => match nominal_eq(&left, &right) {
            Some(equal) => Ok(Value::Bool(equal)),
            None => Err(ValueOpError::NewtypeComparison { left, right }),
        },
        Op::NotEqual => match nominal_eq(&left, &right) {
            Some(equal) => Ok(Value::Bool(!equal)),
            None => Err(ValueOpError::NewtypeComparison { left, right }),
        },
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

/// Equality that refuses to compare a newtype value with anything but a
/// value of the same newtype, wherever the two meet: `None` for such a
/// mismatch, otherwise what `==` says. Values holding no newtype compare
/// exactly as `==` compares them.
fn nominal_eq(left: &Value, right: &Value) -> Option<bool> {
    match (left, right) {
        (Value::Newtype(left), Value::Newtype(right)) => {
            if left.brand() != right.brand() {
                return None;
            }
            nominal_eq(left.value(), right.value())
        }
        (Value::Newtype(_), _) | (_, Value::Newtype(_)) => None,
        (Value::List(left), Value::List(right)) | (Value::Tuple(left), Value::Tuple(right)) => {
            if left.len() != right.len() {
                return Some(false);
            }
            let mut equal = true;
            for (left, right) in left.iter().zip(right.iter()) {
                equal &= nominal_eq(left, right)?;
            }
            Some(equal)
        }
        (Value::Map(left), Value::Map(right)) => {
            if left.len() != right.len() || !left.keys().eq(right.keys()) {
                return Some(false);
            }
            let mut equal = true;
            for (left, right) in left.values().zip(right.values()) {
                equal &= nominal_eq(left, right)?;
            }
            Some(equal)
        }
        (Value::Union(left), Value::Union(right)) => match (left.payload(), right.payload()) {
            (Some(left_payload), Some(right_payload)) if left.tag() == right.tag() => {
                nominal_eq(left_payload, right_payload)
            }
            _ => Some(left == right),
        },
        _ => Some(left == right),
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
