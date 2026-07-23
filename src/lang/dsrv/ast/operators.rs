use std::fmt::Display;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum NumericalBinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

impl Display for NumericalBinOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let symbol = match self {
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::Mod => "%",
        };
        f.write_str(symbol)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum BoolBinOp {
    Or,
    And,
    Impl,
}

impl Display for BoolBinOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let symbol = match self {
            Self::Or => "||",
            Self::And => "&&",
            Self::Impl => "=>",
        };
        f.write_str(symbol)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum StrBinOp {
    Concat,
}

impl Display for StrBinOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("++")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum CompBinOp {
    Eq,
    Le,
    Ge,
    Lt,
    Gt,
}

impl Display for CompBinOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let symbol = match self {
            Self::Eq => "==",
            Self::Le => "<=",
            Self::Ge => ">=",
            Self::Lt => "<",
            Self::Gt => ">",
        };
        f.write_str(symbol)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum SBinOp {
    NOp(NumericalBinOp),
    BOp(BoolBinOp),
    SOp(StrBinOp),
    COp(CompBinOp),
}

impl Display for SBinOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NOp(op) => Display::fmt(op, f),
            Self::BOp(op) => Display::fmt(op, f),
            Self::SOp(op) => Display::fmt(op, f),
            Self::COp(op) => Display::fmt(op, f),
        }
    }
}

impl From<&str> for SBinOp {
    fn from(symbol: &str) -> Self {
        match symbol {
            "+" => Self::NOp(NumericalBinOp::Add),
            "-" => Self::NOp(NumericalBinOp::Sub),
            "*" => Self::NOp(NumericalBinOp::Mul),
            "/" => Self::NOp(NumericalBinOp::Div),
            "||" => Self::BOp(BoolBinOp::Or),
            "&&" => Self::BOp(BoolBinOp::And),
            "++" => Self::SOp(StrBinOp::Concat),
            "==" => Self::COp(CompBinOp::Eq),
            "<=" => Self::COp(CompBinOp::Le),
            _ => panic!("Invalid binary operation: {symbol}"),
        }
    }
}
