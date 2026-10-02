#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryOperator {
    Not,
    Negate,
    Sin,
    Cos,
    Tan,
    Absolute,
    /// `trunc`, `floor`, `ceil` and `round`: a `Float` to an `Int`.
    Truncate,
    Floor,
    Ceiling,
    Round,
    CastFloat,
    CastStr,
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
            Self::Truncate => "trunc",
            Self::Floor => "floor",
            Self::Ceiling => "ceil",
            Self::Round => "round",
            Self::CastFloat => "cast to Float",
            Self::CastStr => "cast to Str",
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
    Power,
    Or,
    And,
    Implication,
    Concatenate,
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NumericOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Power,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BooleanOperator {
    Or,
    And,
    Implication,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EqualityOperator {
    Equal,
    NotEqual,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderingOperator {
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}

/// The kind of a binary operator, carrying the operator within that kind.
///
/// Code that dispatches on the kind matches on this, so that each kind's
/// operators are matched exhaustively: adding an operator to a kind is a
/// compile error wherever that kind is evaluated and is missing the case.
/// Code that does not care about the kind uses [`BinaryOperator`] directly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOperatorKind {
    Numeric(NumericOperator),
    Boolean(BooleanOperator),
    /// String concatenation, the only string operator.
    String,
    Equality(EqualityOperator),
    Ordering(OrderingOperator),
}

impl From<NumericOperator> for BinaryOperator {
    fn from(operator: NumericOperator) -> Self {
        match operator {
            NumericOperator::Add => Self::Add,
            NumericOperator::Subtract => Self::Subtract,
            NumericOperator::Multiply => Self::Multiply,
            NumericOperator::Divide => Self::Divide,
            NumericOperator::Modulo => Self::Modulo,
            NumericOperator::Power => Self::Power,
        }
    }
}

impl From<BooleanOperator> for BinaryOperator {
    fn from(operator: BooleanOperator) -> Self {
        match operator {
            BooleanOperator::Or => Self::Or,
            BooleanOperator::And => Self::And,
            BooleanOperator::Implication => Self::Implication,
        }
    }
}

impl From<EqualityOperator> for BinaryOperator {
    fn from(operator: EqualityOperator) -> Self {
        match operator {
            EqualityOperator::Equal => Self::Equal,
            EqualityOperator::NotEqual => Self::NotEqual,
        }
    }
}

impl From<OrderingOperator> for BinaryOperator {
    fn from(operator: OrderingOperator) -> Self {
        match operator {
            OrderingOperator::Less => Self::Less,
            OrderingOperator::LessEqual => Self::LessEqual,
            OrderingOperator::Greater => Self::Greater,
            OrderingOperator::GreaterEqual => Self::GreaterEqual,
        }
    }
}

impl BinaryOperator {
    /// The kind of this operator, with the operator within its kind. This is
    /// the one match over every binary operator that must stay exhaustive.
    pub fn kind(self) -> BinaryOperatorKind {
        use BinaryOperatorKind as Kind;
        match self {
            Self::Add => Kind::Numeric(NumericOperator::Add),
            Self::Subtract => Kind::Numeric(NumericOperator::Subtract),
            Self::Multiply => Kind::Numeric(NumericOperator::Multiply),
            Self::Divide => Kind::Numeric(NumericOperator::Divide),
            Self::Modulo => Kind::Numeric(NumericOperator::Modulo),
            Self::Power => Kind::Numeric(NumericOperator::Power),
            Self::Or => Kind::Boolean(BooleanOperator::Or),
            Self::And => Kind::Boolean(BooleanOperator::And),
            Self::Implication => Kind::Boolean(BooleanOperator::Implication),
            Self::Concatenate => Kind::String,
            Self::Equal => Kind::Equality(EqualityOperator::Equal),
            Self::NotEqual => Kind::Equality(EqualityOperator::NotEqual),
            Self::Less => Kind::Ordering(OrderingOperator::Less),
            Self::LessEqual => Kind::Ordering(OrderingOperator::LessEqual),
            Self::Greater => Kind::Ordering(OrderingOperator::Greater),
            Self::GreaterEqual => Kind::Ordering(OrderingOperator::GreaterEqual),
        }
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Add => "addition",
            Self::Subtract => "subtraction",
            Self::Multiply => "multiplication",
            Self::Divide => "division",
            Self::Modulo => "modulo",
            Self::Power => "exponentiation",
            Self::Or => "boolean or",
            Self::And => "boolean and",
            Self::Implication => "boolean implication",
            Self::Concatenate => "string concatenation",
            Self::Equal => "equality",
            Self::NotEqual => "inequality",
            Self::Less => "less-than comparison",
            Self::LessEqual => "less-than-or-equal comparison",
            Self::Greater => "greater-than comparison",
            Self::GreaterEqual => "greater-than-or-equal comparison",
        }
    }

    pub(crate) fn symbol(self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Subtract => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::Modulo => "%",
            Self::Power => "**",
            Self::Or => "||",
            Self::And => "&&",
            Self::Implication => "=>",
            Self::Concatenate => "++",
            Self::Equal => "==",
            Self::NotEqual => "!=",
            Self::Less => "<",
            Self::LessEqual => "<=",
            Self::Greater => ">",
            Self::GreaterEqual => ">=",
        }
    }
}
