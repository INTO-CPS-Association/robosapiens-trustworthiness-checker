use super::super::super::ir::ScalarKind;
use super::super::super::*;
use super::super::lifting::expect_value;
use crate::core::values::operations as value_operations;
use crate::core::{BinaryOperator, UnaryOperator};

#[derive(Clone, Copy, Debug, PartialEq)]
pub(in crate::dataflow) enum ScalarValue {
    NoVal,
    Deferred,
    Int(i64),
    Float(f64),
    Bool(bool),
}

impl ScalarValue {
    pub(in crate::dataflow) fn from_untyped_value(value: &Value) -> Option<Self> {
        match value {
            Value::NoVal => Some(Self::NoVal),
            Value::Deferred => Some(Self::Deferred),
            Value::Int(value) => Some(Self::Int(*value)),
            Value::Float(value) => Some(Self::Float(*value)),
            Value::Bool(value) => Some(Self::Bool(*value)),
            _ => None,
        }
    }

    #[inline]
    pub(super) fn from_value(value: &Value, kind: ScalarKind) -> Option<Self> {
        match value {
            Value::NoVal => Some(Self::NoVal),
            Value::Deferred => Some(Self::Deferred),
            Value::Int(value) if kind == ScalarKind::Int => Some(Self::Int(*value)),
            Value::Float(value) if kind == ScalarKind::Float => Some(Self::Float(*value)),
            Value::Bool(value) if kind == ScalarKind::Bool => Some(Self::Bool(*value)),
            _ => None,
        }
    }

    #[inline]
    pub(in crate::dataflow) fn into_value(self) -> Value {
        match self {
            Self::NoVal => Value::NoVal,
            Self::Deferred => Value::Deferred,
            Self::Int(value) => Value::Int(value),
            Self::Float(value) => Value::Float(value),
            Self::Bool(value) => Value::Bool(value),
        }
    }

    #[inline]
    pub(super) fn is_special(self) -> bool {
        matches!(self, Self::NoVal | Self::Deferred)
    }

    #[inline]
    pub(super) fn has_kind(self, kind: ScalarKind) -> bool {
        self.is_special()
            || matches!(
                (self, kind),
                (Self::Int(_), ScalarKind::Int)
                    | (Self::Float(_), ScalarKind::Float)
                    | (Self::Bool(_), ScalarKind::Bool)
            )
    }

    fn from_untyped(value: Value) -> Self {
        match value {
            Value::NoVal => Self::NoVal,
            Value::Deferred => Self::Deferred,
            Value::Int(value) => Self::Int(value),
            Value::Float(value) => Self::Float(value),
            Value::Bool(value) => Self::Bool(value),
            other => panic!("scalar operation unexpectedly produced {other:?}"),
        }
    }
}

#[inline]
pub(super) fn retain_last(value: ScalarValue, last: &mut Option<ScalarValue>) -> ScalarValue {
    match value {
        ScalarValue::NoVal => last.unwrap_or(ScalarValue::NoVal),
        value => {
            *last = Some(value);
            value
        }
    }
}

pub(super) fn supports_unary(op: UnaryOperator, input: ScalarKind, output: ScalarKind) -> bool {
    use ScalarKind as Kind;
    use UnaryOperator as Op;

    matches!(
        (op, input, output),
        (Op::Not, Kind::Bool, Kind::Bool)
            | (Op::Negate | Op::Absolute, Kind::Int, Kind::Int)
            | (
                Op::Negate | Op::Sin | Op::Cos | Op::Tan | Op::Absolute,
                Kind::Float,
                Kind::Float
            )
    )
}

pub(super) fn supports_binary(
    op: BinaryOperator,
    left: ScalarKind,
    right: ScalarKind,
    output: ScalarKind,
) -> bool {
    use BinaryOperator as Op;
    use ScalarKind as Kind;

    match op {
        Op::Add | Op::Subtract | Op::Multiply | Op::Divide | Op::Modulo => {
            matches!(left, Kind::Int | Kind::Float)
                && matches!(right, Kind::Int | Kind::Float)
                && output
                    == if left == Kind::Int && right == Kind::Int {
                        Kind::Int
                    } else {
                        Kind::Float
                    }
        }
        Op::Or | Op::And | Op::Implication => {
            left == Kind::Bool && right == Kind::Bool && output == Kind::Bool
        }
        Op::Equal => output == Kind::Bool,
        Op::Less | Op::LessEqual | Op::Greater | Op::GreaterEqual => {
            output == Kind::Bool
                && ((matches!(left, Kind::Int | Kind::Float)
                    && matches!(right, Kind::Int | Kind::Float))
                    || (left == Kind::Bool && right == Kind::Bool))
        }
        Op::Concatenate => false,
    }
}

#[inline]
pub(super) fn apply_unary(op: UnaryOperator, input: ScalarValue) -> ScalarValue {
    use ScalarValue as Scalar;
    use UnaryOperator as Op;

    match (op, input) {
        (Op::Not, Scalar::Bool(value)) => Scalar::Bool(!value),
        (Op::Negate, Scalar::Int(value)) => value
            .checked_neg()
            .map(Scalar::Int)
            .unwrap_or_else(|| generic_unary(op, input)),
        (Op::Negate, Scalar::Float(value)) => Scalar::Float(-value),
        (Op::Sin, Scalar::Float(value)) => Scalar::Float(value.sin()),
        (Op::Cos, Scalar::Float(value)) => Scalar::Float(value.cos()),
        (Op::Tan, Scalar::Float(value)) => Scalar::Float(value.tan()),
        (Op::Absolute, Scalar::Int(value)) => value
            .checked_abs()
            .map(Scalar::Int)
            .unwrap_or_else(|| generic_unary(op, input)),
        (Op::Absolute, Scalar::Float(value)) => Scalar::Float(value.abs()),
        _ => generic_unary(op, input),
    }
}

#[inline]
pub(super) fn apply_binary(
    op: BinaryOperator,
    left: ScalarValue,
    right: ScalarValue,
) -> ScalarValue {
    use BinaryOperator as Op;
    use ScalarValue as Scalar;

    match (left, right) {
        (Scalar::Int(left), Scalar::Int(right))
            if matches!(
                op,
                Op::Add | Op::Subtract | Op::Multiply | Op::Divide | Op::Modulo
            ) =>
        {
            let result = match op {
                Op::Add => left.checked_add(right),
                Op::Subtract => left.checked_sub(right),
                Op::Multiply => left.checked_mul(right),
                Op::Divide if right != 0 => left.checked_div(right),
                Op::Modulo if right != 0 => left.checked_rem(right),
                Op::Divide | Op::Modulo => None,
                _ => unreachable!(),
            };
            result
                .map(Scalar::Int)
                .unwrap_or_else(|| generic_binary(op, Scalar::Int(left), Scalar::Int(right)))
        }
        (Scalar::Int(left), Scalar::Float(right)) if is_arithmetic(op) => {
            Scalar::Float(float_binary(op, left as f64, right))
        }
        (Scalar::Float(left), Scalar::Int(right)) if is_arithmetic(op) => {
            Scalar::Float(float_binary(op, left, right as f64))
        }
        (Scalar::Float(left), Scalar::Float(right)) if is_arithmetic(op) => {
            Scalar::Float(float_binary(op, left, right))
        }
        (Scalar::Bool(left), Scalar::Bool(right))
            if matches!(op, Op::Or | Op::And | Op::Implication) =>
        {
            Scalar::Bool(match op {
                Op::Or => left || right,
                Op::And => left && right,
                Op::Implication => !left || right,
                _ => unreachable!(),
            })
        }
        (left, right) if op == Op::Equal => Scalar::Bool(left == right),
        (left, right)
            if matches!(
                op,
                Op::Less | Op::LessEqual | Op::Greater | Op::GreaterEqual
            ) =>
        {
            let ordering = match (left, right) {
                (Scalar::Int(left), Scalar::Int(right)) => Some(left.cmp(&right)),
                (Scalar::Int(left), Scalar::Float(right)) => (left as f64).partial_cmp(&right),
                (Scalar::Float(left), Scalar::Int(right)) => left.partial_cmp(&(right as f64)),
                (Scalar::Float(left), Scalar::Float(right)) => left.partial_cmp(&right),
                (Scalar::Bool(left), Scalar::Bool(right)) => Some(left.cmp(&right)),
                _ => return generic_binary(op, left, right),
            };
            Scalar::Bool(ordering.is_some_and(|ordering| match op {
                Op::Less => ordering.is_lt(),
                Op::LessEqual => ordering.is_le(),
                Op::Greater => ordering.is_gt(),
                Op::GreaterEqual => ordering.is_ge(),
                _ => unreachable!(),
            }))
        }
        (left, right) => generic_binary(op, left, right),
    }
}

fn is_arithmetic(op: BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::Add
            | BinaryOperator::Subtract
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo
    )
}

fn float_binary(op: BinaryOperator, left: f64, right: f64) -> f64 {
    match op {
        BinaryOperator::Add => left + right,
        BinaryOperator::Subtract => left - right,
        BinaryOperator::Multiply => left * right,
        BinaryOperator::Divide => left / right,
        BinaryOperator::Modulo => left % right,
        _ => unreachable!(),
    }
}

fn generic_unary(op: UnaryOperator, input: ScalarValue) -> ScalarValue {
    ScalarValue::from_untyped(expect_value(value_operations::unary(
        op,
        input.into_value(),
    )))
}

fn generic_binary(op: BinaryOperator, left: ScalarValue, right: ScalarValue) -> ScalarValue {
    ScalarValue::from_untyped(expect_value(value_operations::binary(
        op,
        left.into_value(),
        right.into_value(),
    )))
}
