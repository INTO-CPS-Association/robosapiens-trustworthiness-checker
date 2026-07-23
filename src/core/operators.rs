#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryOperator {
    Not,
    Negate,
    Sin,
    Cos,
    Tan,
    Absolute,
}

impl UnaryOperator {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Not => "not",
            Self::Negate => "negation",
            Self::Sin => "sin",
            Self::Cos => "cos",
            Self::Tan => "tan",
            Self::Absolute => "absolute value",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Or,
    And,
    Implication,
    Concatenate,
    Equal,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOperatorKind {
    Numeric,
    Boolean,
    String,
    Equality,
    Ordering,
}

impl BinaryOperator {
    pub fn kind(self) -> BinaryOperatorKind {
        match self {
            Self::Add | Self::Subtract | Self::Multiply | Self::Divide | Self::Modulo => {
                BinaryOperatorKind::Numeric
            }
            Self::Or | Self::And | Self::Implication => BinaryOperatorKind::Boolean,
            Self::Concatenate => BinaryOperatorKind::String,
            Self::Equal => BinaryOperatorKind::Equality,
            Self::Less | Self::LessEqual | Self::Greater | Self::GreaterEqual => {
                BinaryOperatorKind::Ordering
            }
        }
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Add => "addition",
            Self::Subtract => "subtraction",
            Self::Multiply => "multiplication",
            Self::Divide => "division",
            Self::Modulo => "modulo",
            Self::Or => "boolean or",
            Self::And => "boolean and",
            Self::Implication => "boolean implication",
            Self::Concatenate => "string concatenation",
            Self::Equal => "equality",
            Self::Less => "less-than comparison",
            Self::LessEqual => "less-than-or-equal comparison",
            Self::Greater => "greater-than comparison",
            Self::GreaterEqual => "greater-than-or-equal comparison",
        }
    }
}
