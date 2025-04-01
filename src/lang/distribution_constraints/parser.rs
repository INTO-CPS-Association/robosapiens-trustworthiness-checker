use winnow::Parser;
use winnow::Result;
use winnow::combinator::*;
use winnow::token::literal;

use super::super::core::parser::*;
use super::ast::*;
use crate::core::VarName;

// Distribution constraints parser
fn dist_constraint_type(s: &mut &str) -> Result<DistConstraintType> {
    seq!((
        _: whitespace,
        alt((
            literal("can_run").map(|_| DistConstraintType::CanRun),
            literal("locality").map(|_| DistConstraintType::LocalityScore),
            literal("redundancy").map(|_| DistConstraintType::Redundancy),
        )),
        _: whitespace,
    ))
    .map(|(x,)| x)
    .parse_next(s)
}

pub fn dist_constraint(s: &mut &str) -> Result<(VarName, DistConstraint)> {
    seq!((
        _: whitespace,
        dist_constraint_type,
        _: loop_ms_or_lb_or_lc,
        ident,
        _: loop_ms_or_lb_or_lc,
        _: literal(":"),
        _: loop_ms_or_lb_or_lc,
        dist_constraint_body,
        _: whitespace,
    ))
    .map(|(constraint, name, body): (_, &str, _)| (name.into(), DistConstraint(constraint, body)))
    .parse_next(s)
}

pub fn dist_constraints(s: &mut &str) -> Result<Vec<(VarName, DistConstraint)>> {
    separated(0.., dist_constraint, seq!(lb_or_lc, loop_ms_or_lb_or_lc)).parse_next(s)
}

fn paren(s: &mut &str) -> Result<DistConstraintBody> {
    delimited('(', dist_constraint_body, ')').parse_next(s)
}

// Used for Lists in output streams
fn dist_constraint_body_list(s: &mut &str) -> Result<DistConstraintBody> {
    let res = delimited(
        seq!("List", loop_ms_or_lb_or_lc, '('),
        separated(
            0..,
            dist_constraint_body,
            seq!(loop_ms_or_lb_or_lc, ',', loop_ms_or_lb_or_lc),
        ),
        ')',
    )
    .parse_next(s);
    match res {
        Ok(exprs) => Ok(DistConstraintBody::List(exprs)),
        Err(e) => Err(e),
    }
}

fn var(s: &mut &str) -> Result<DistConstraintBody> {
    ident
        .map(|name: &str| DistConstraintBody::Var(name.into()))
        .parse_next(s)
}

// Same as `val` but returns dist_constraint_body::Val
fn sval(s: &mut &str) -> Result<DistConstraintBody> {
    val.map(|v| DistConstraintBody::Val(v)).parse_next(s)
}

fn sindex(s: &mut &str) -> Result<DistConstraintBody> {
    seq!(
        _: whitespace,
        alt((sval, var, paren)),
        _: loop_ms_or_lb_or_lc,
        _: '[',
        _: loop_ms_or_lb_or_lc,
        integer,
        _: loop_ms_or_lb_or_lc,
        _: ',',
        _: loop_ms_or_lb_or_lc,
        val,
        _: loop_ms_or_lb_or_lc,
        _: ']'
    )
    .map(|(e, i, d)| DistConstraintBody::SIndex(Box::new(e), i, d))
    .parse_next(s)
}

fn ifelse(s: &mut &str) -> Result<DistConstraintBody> {
    seq!((
        _: whitespace,
        _: "if",
        _: loop_ms_or_lb_or_lc,
        dist_constraint_body,
        _: loop_ms_or_lb_or_lc,
        _: "then",
        _: loop_ms_or_lb_or_lc,
        dist_constraint_body,
        _: loop_ms_or_lb_or_lc,
        _: "else",
        _: loop_ms_or_lb_or_lc,
        dist_constraint_body,
        _: whitespace,
    ))
    .map(|(b, s1, s2)| DistConstraintBody::If(Box::new(b), Box::new(s1), Box::new(s2)))
    .parse_next(s)
}

fn is_defined(s: &mut &str) -> Result<DistConstraintBody> {
    seq!((
        _: whitespace,
        _: literal("is_defined"),
        _: loop_ms_or_lb_or_lc,
        _: '(',
        _: loop_ms_or_lb_or_lc,
        dist_constraint_body,
        _: loop_ms_or_lb_or_lc,
        _: ')',
    ))
    .map(|(e,)| DistConstraintBody::IsDefined(Box::new(e)))
    .parse_next(s)
}

fn default(s: &mut &str) -> Result<DistConstraintBody> {
    seq!((
        _: whitespace,
        _: literal("default"),
        _: '(',
        _: loop_ms_or_lb_or_lc,
        dist_constraint_body,
        _: loop_ms_or_lb_or_lc,
        _: ',',
        _: loop_ms_or_lb_or_lc,
        dist_constraint_body,
        _: loop_ms_or_lb_or_lc,
        _: ')',
    ))
    .map(|(lhs, rhs)| DistConstraintBody::Default(Box::new(lhs), Box::new(rhs)))
    .parse_next(s)
}

fn not(s: &mut &str) -> Result<DistConstraintBody> {
    seq!((
        _: whitespace,
        _: "!",
        _: loop_ms_or_lb_or_lc,
        atom,
        _: whitespace,
    ))
    .map(|(e,)| DistConstraintBody::Not(Box::new(e)))
    .parse_next(s)
}

fn lindex(s: &mut &str) -> Result<DistConstraintBody> {
    seq!(
        _: whitespace,
        _: "List.get",
        _: loop_ms_or_lb_or_lc,
        _: '(',
        _: loop_ms_or_lb_or_lc,
        dist_constraint_body,
        _: loop_ms_or_lb_or_lc,
        _: ',',
        _: loop_ms_or_lb_or_lc,
        dist_constraint_body,
        _: loop_ms_or_lb_or_lc,
        _: ')',
    )
    .map(|(e, i)| DistConstraintBody::LIndex(Box::new(e), Box::new(i)))
    .parse_next(s)
}

fn lappend(s: &mut &str) -> Result<DistConstraintBody> {
    seq!(
        _: whitespace,
        _: "List.append",
        _: loop_ms_or_lb_or_lc,
        _: '(',
        _: loop_ms_or_lb_or_lc,
        dist_constraint_body,
        _: loop_ms_or_lb_or_lc,
        _: ',',
        _: loop_ms_or_lb_or_lc,
        dist_constraint_body,
        _: loop_ms_or_lb_or_lc,
        _: ')',
    )
    .map(|(lst, el)| DistConstraintBody::LAppend(Box::new(lst), Box::new(el)))
    .parse_next(s)
}

fn lconcat(s: &mut &str) -> Result<DistConstraintBody> {
    seq!(
        _: whitespace,
        _: "List.concat",
        _: loop_ms_or_lb_or_lc,
        _: '(',
        _: loop_ms_or_lb_or_lc,
        dist_constraint_body,
        _: loop_ms_or_lb_or_lc,
        _: ',',
        _: loop_ms_or_lb_or_lc,
        dist_constraint_body,
        _: loop_ms_or_lb_or_lc,
        _: ')',
    )
    .map(|(lst1, lst2)| DistConstraintBody::LConcat(Box::new(lst1), Box::new(lst2)))
    .parse_next(s)
}

fn lhead(s: &mut &str) -> Result<DistConstraintBody> {
    seq!((
        _: whitespace,
        _: "List.head",
        _: loop_ms_or_lb_or_lc,
        _: '(',
        _: loop_ms_or_lb_or_lc,
        dist_constraint_body,
        _: loop_ms_or_lb_or_lc,
        _: ')',
        _: whitespace,
    ))
    .map(|(lst,)| DistConstraintBody::LHead(Box::new(lst)))
    .parse_next(s)
}

fn ltail(s: &mut &str) -> Result<DistConstraintBody> {
    seq!((
        _: whitespace,
        _: "List.tail",
        _: loop_ms_or_lb_or_lc,
        _: '(',
        _: loop_ms_or_lb_or_lc,
        dist_constraint_body,
        _: loop_ms_or_lb_or_lc,
        _: ')',
        _: whitespace,
    ))
    .map(|(lst,)| DistConstraintBody::LTail(Box::new(lst)))
    .parse_next(s)
}

/// Monitors
fn source(s: &mut &str) -> Result<DistConstraintBody> {
    seq!((
        _: whitespace,
        _: "source",
        _: loop_ms_or_lb_or_lc,
        _: "(",
        _: loop_ms_or_lb_or_lc,
        ident,
        _: loop_ms_or_lb_or_lc,
        _: ")",
        _: whitespace,
    ))
    .map(|(v,)| DistConstraintBody::Source(v.into()))
    .parse_next(s)
}

fn monitor(s: &mut &str) -> Result<DistConstraintBody> {
    seq!((
        _: whitespace,
        _: "monitor",
        _: loop_ms_or_lb_or_lc,
        _: "(",
        _: loop_ms_or_lb_or_lc,
        ident,
        _: loop_ms_or_lb_or_lc,
        _: ")",
        _: whitespace,
    ))
    .map(|(v,)| DistConstraintBody::Monitor(v.into()))
    .parse_next(s)
}

/// Trigonometric functions
fn sin(s: &mut &str) -> Result<DistConstraintBody> {
    seq!((
        _: whitespace,
        _: "sin",
        _: loop_ms_or_lb_or_lc,
        _: "(",
        _: loop_ms_or_lb_or_lc,
        dist_constraint_body,
        _: loop_ms_or_lb_or_lc,
        _: ")",
        _: whitespace,
    ))
    .map(|(v,)| DistConstraintBody::Sin(Box::new(v)))
    .parse_next(s)
}
fn cos(s: &mut &str) -> Result<DistConstraintBody> {
    seq!((
        _: whitespace,
        _: "cos",
        _: loop_ms_or_lb_or_lc,
        _: '(',
        _: loop_ms_or_lb_or_lc,
        dist_constraint_body,
        _: loop_ms_or_lb_or_lc,
        _: ')',
        _: whitespace,
    ))
    .map(|(v,)| DistConstraintBody::Cos(Box::new(v)))
    .parse_next(s)
}
fn tan(s: &mut &str) -> Result<DistConstraintBody> {
    seq!((
        _: whitespace,
        _: "tan",
        _: loop_ms_or_lb_or_lc,
        _: '(',
        _: loop_ms_or_lb_or_lc,
        dist_constraint_body,
        _: loop_ms_or_lb_or_lc,
        _: ')',
        _: whitespace,
    ))
    .map(|(v,)| DistConstraintBody::Tan(Box::new(v)))
    .parse_next(s)
}

/// Fundamental expressions of the language
fn atom(s: &mut &str) -> Result<DistConstraintBody> {
    // Break up the large alt into smaller groups to avoid exceeding the trait implementation limit
    delimited(
        whitespace,
        alt((
            // Group 1
            alt((sindex, lindex, lappend, lconcat, lhead, ltail, not)),
            // Group 2
            alt((sval, ifelse, monitor, source, sin, cos, tan)),
            // Group 3
            alt((default, is_defined, dist_constraint_body_list, var, paren)),
        )),
        whitespace,
    )
    .parse_next(s)
}

enum BinaryPrecedences {
    // Lowest to highest precedence
    Concat,
    Or,
    And,
    Le,
    Ge,
    Lt,
    Gt,
    Eq,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}
impl BinaryPrecedences {
    pub fn next(&self) -> Option<Self> {
        use BinaryPrecedences::*;
        match self {
            Concat => Some(Or),
            Or => Some(And),
            And => Some(Le),
            Le => Some(Ge),
            Ge => Some(Lt),
            Lt => Some(Gt),
            Gt => Some(Eq),
            Eq => Some(Sub),
            Sub => Some(Add),
            Add => Some(Mul),
            Mul => Some(Div),
            Div => Some(Mod),
            Mod => None,
        }
    }

    pub fn get_lit(&self) -> &'static str {
        use BinaryPrecedences::*;
        match self {
            Concat => "++",
            Or => "||",
            And => "&&",
            Le => "<=",
            Ge => ">=",
            Lt => "<",
            Gt => ">",
            Eq => "==",
            Add => "+",
            Sub => "-",
            Mul => "*",
            Div => "/",
            Mod => "%",
        }
    }

    pub fn get_binop(&self) -> SBinOp {
        use BinaryPrecedences::*;
        match self {
            Concat => SBinOp::SOp(StrBinOp::Concat),
            Or => SBinOp::BOp(BoolBinOp::Or),
            And => SBinOp::BOp(BoolBinOp::And),
            Le => SBinOp::COp(CompBinOp::Le),
            Ge => SBinOp::COp(CompBinOp::Ge),
            Lt => SBinOp::COp(CompBinOp::Lt),
            Gt => SBinOp::COp(CompBinOp::Gt),
            Eq => SBinOp::COp(CompBinOp::Eq),
            Add => SBinOp::NOp(NumericalBinOp::Add),
            Sub => SBinOp::NOp(NumericalBinOp::Sub),
            Mul => SBinOp::NOp(NumericalBinOp::Mul),
            Div => SBinOp::NOp(NumericalBinOp::Div),
            Mod => SBinOp::NOp(NumericalBinOp::Mod),
        }
    }

    pub fn lowest_precedence() -> Self {
        BinaryPrecedences::Concat
    }
}

/// Parse a binary op
/// First finds the `next_parser` and `lit` in the PrecedenceChain.
/// If the parser is the last it uses `atom` instead.
/// It then attempts to parse with a `separated_foldl1` parser where we look for the pattern
/// `next_parser` `lit` `next_parser`.
///
/// @local_variable `next_parser`: refers to a parser that can parse any expression of a higher precedence.
/// Considering +, * and `atom`, `next_parser` refers to a parser that first tries to parse a `*` expression and then an atom
/// @local_variable `lit`: refers to the operator that is being parsed.
///
/// @param current_op: The current precedence level
///
/// (Inspired by https://github.com/winnow-rs/winnow/blob/main/examples/arithmetic/parser_ast.rs)
fn binary_op(current_op: BinaryPrecedences) -> impl FnMut(&mut &str) -> Result<DistConstraintBody> {
    move |s: &mut &str| {
        let next_parser_op = current_op.next();
        let mut next_parser: Box<dyn FnMut(&mut &str) -> Result<DistConstraintBody>> =
            match next_parser_op {
                Some(next_parser) => Box::new(binary_op(next_parser)),
                None => Box::new(|i: &mut &str| atom.parse_next(i)),
            };
        let lit = current_op.get_lit();
        let res = separated_foldl1(&mut next_parser, literal(lit), |left, _, right| {
            DistConstraintBody::BinOp(Box::new(left), Box::new(right), current_op.get_binop())
        })
        .parse_next(s);
        res
    }
}

pub fn dist_constraint_body(s: &mut &str) -> Result<DistConstraintBody> {
    delimited(
        whitespace,
        binary_op(BinaryPrecedences::lowest_precedence()),
        whitespace,
    )
    .parse_next(s)
}

#[cfg(test)]
mod tests {
    use crate::core::Value;

    use winnow::error::ContextError;

    use super::*;
    use test_log::test;

    #[test]
    fn test_dist_constraint_body_source() -> Result<(), ContextError> {
        let mut input = "source(x)";
        assert_eq!(
            dist_constraint_body(&mut input)?,
            DistConstraintBody::Source("x".into())
        );
        Ok(())
    }

    #[test]
    fn test_dist_constraint_source() -> Result<(), ContextError> {
        let mut input = "can_run x: source(y)";
        assert_eq!(
            dist_constraint(&mut input)?,
            (
                "x".into(),
                DistConstraint(
                    DistConstraintType::CanRun,
                    DistConstraintBody::Source("y".into())
                )
            )
        );
        Ok(())
    }

    #[test]
    fn test_dist_constraints_sources() -> Result<(), ContextError> {
        let mut input = "can_run x: source(y)\n\
            can_run z: source(w)";
        assert_eq!(
            dist_constraints(&mut input)?,
            vec![
                (
                    "x".into(),
                    DistConstraint(
                        DistConstraintType::CanRun,
                        DistConstraintBody::Source("y".into())
                    )
                ),
                (
                    "z".into(),
                    DistConstraint(
                        DistConstraintType::CanRun,
                        DistConstraintBody::Source("w".into())
                    )
                )
            ]
        );
        Ok(())
    }

    #[test]
    fn test_streamdata() {
        assert_eq!(val(&mut (*"42".to_string()).into()), Ok(Value::Int(42)));
        assert_eq!(
            val(&mut (*"42.0".to_string()).into()),
            Ok(Value::Float(42.0)),
        );
        assert_eq!(
            val(&mut (*"1e-1".to_string()).into()),
            Ok(Value::Float(1e-1)),
        );
        assert_eq!(
            val(&mut (*"\"abc2d\"".to_string()).into()),
            Ok(Value::Str("abc2d".into())),
        );
        assert_eq!(
            val(&mut (*"true".to_string()).into()),
            Ok(Value::Bool(true)),
        );
        assert_eq!(
            val(&mut (*"false".to_string()).into()),
            Ok(Value::Bool(false)),
        );
        assert_eq!(
            val(&mut (*"\"x+y\"".to_string()).into()),
            Ok(Value::Str("x+y".into())),
        );
    }

    #[test]
    fn test_dist_constraint_body() -> Result<(), ContextError> {
        assert_eq!(
            dist_constraint_body(&mut (*"1 + 2".to_string()).into())?,
            DistConstraintBody::BinOp(
                Box::new(DistConstraintBody::Val(Value::Int(1))),
                Box::new(DistConstraintBody::Val(Value::Int(2))),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
        );
        assert_eq!(
            dist_constraint_body(&mut (*"1 + 2 * 3".to_string()).into())?,
            DistConstraintBody::BinOp(
                Box::new(DistConstraintBody::Val(Value::Int(1))),
                Box::new(DistConstraintBody::BinOp(
                    Box::new(DistConstraintBody::Val(Value::Int(2))),
                    Box::new(DistConstraintBody::Val(Value::Int(3))),
                    SBinOp::NOp(NumericalBinOp::Mul),
                )),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
        );
        assert_eq!(
            dist_constraint_body(&mut (*"x + (y + 2)".to_string()).into())?,
            DistConstraintBody::BinOp(
                Box::new(DistConstraintBody::Var("x".into())),
                Box::new(DistConstraintBody::BinOp(
                    Box::new(DistConstraintBody::Var("y".into())),
                    Box::new(DistConstraintBody::Val(Value::Int(2))),
                    SBinOp::NOp(NumericalBinOp::Add),
                )),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
        );
        assert_eq!(
            dist_constraint_body(&mut (*"if true then 1 else 2".to_string()).into())?,
            DistConstraintBody::If(
                Box::new(DistConstraintBody::Val(true.into())),
                Box::new(DistConstraintBody::Val(Value::Int(1))),
                Box::new(DistConstraintBody::Val(Value::Int(2))),
            ),
        );
        assert_eq!(
            dist_constraint_body(&mut (*"(x)[-1, 0]".to_string()).into())?,
            DistConstraintBody::SIndex(
                Box::new(DistConstraintBody::Var("x".into())),
                -1,
                Value::Int(0),
            ),
        );
        assert_eq!(
            dist_constraint_body(&mut (*"(x + y)[-3, 2]".to_string()).into())?,
            DistConstraintBody::SIndex(
                Box::new(DistConstraintBody::BinOp(
                    Box::new(DistConstraintBody::Var("x".into())),
                    Box::new(DistConstraintBody::Var("y".into()),),
                    SBinOp::NOp(NumericalBinOp::Add),
                )),
                -3,
                Value::Int(2),
            ),
        );
        assert_eq!(
            dist_constraint_body(&mut (*"1 + (x)[-1, 0]".to_string()).into())?,
            DistConstraintBody::BinOp(
                Box::new(DistConstraintBody::Val(Value::Int(1))),
                Box::new(DistConstraintBody::SIndex(
                    Box::new(DistConstraintBody::Var("x".into())),
                    -1,
                    Value::Int(0),
                ),),
                SBinOp::NOp(NumericalBinOp::Add),
            )
        );
        assert_eq!(
            dist_constraint_body(&mut (*"\"test\"".to_string()).into())?,
            DistConstraintBody::Val(Value::Str("test".into())),
        );
        assert_eq!(
            dist_constraint_body(&mut (*"(stage == \"m\")").into())?,
            DistConstraintBody::BinOp(
                Box::new(DistConstraintBody::Var("stage".into())),
                Box::new(DistConstraintBody::Val("m".into())),
                SBinOp::COp(CompBinOp::Eq),
            )
        );
        Ok(())
    }

    #[test]
    fn test_float_exprs() {
        // Add
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "0.0")),
            "Ok(Val(Float(0.0)))"
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "  1.0 +2.0  ")),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut " 1.0  + 2.0 +3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Add)), Val(Float(3.0)), NOp(Add)))"
        );
        // Sub
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "  1.0 -2.0  ")),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Sub)))"
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut " 1.0  - 2.0 -3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Sub)), Val(Float(3.0)), NOp(Sub)))"
        );
        // Mul
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "  1.0 *2.0  ")),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Mul)))"
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut " 1.0  * 2.0 *3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Mul)), Val(Float(3.0)), NOp(Mul)))"
        );
        // Div
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "  1.0 /2.0  ")),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Div)))"
        );
    }

    #[test]
    fn test_mixed_float_int_exprs() {
        // Add
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "0.0 + 2")),
            "Ok(BinOp(Val(Float(0.0)), Val(Int(2)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "1 + 2.0")),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "1.0 + 2 + 3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Int(2)), NOp(Add)), Val(Float(3.0)), NOp(Add)))"
        );
        // Sub
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "1 - 2.0")),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), NOp(Sub)))"
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "1.0 - 2 - 3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Int(2)), NOp(Sub)), Val(Float(3.0)), NOp(Sub)))"
        );
        // Mul
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "1 * 2.0")),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), NOp(Mul)))"
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "1.0 * 2 * 3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Int(2)), NOp(Mul)), Val(Float(3.0)), NOp(Mul)))"
        );
        // Div
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "1 / 2.0")),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), NOp(Div)))"
        );
    }

    #[test]
    fn test_integer_exprs() {
        // Add
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "0")),
            "Ok(Val(Int(0)))"
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "  1 +2  ")),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut " 1  + 2 +3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), Val(Int(3)), NOp(Add)))"
        );
        // Sub
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "  1 -2  ")),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), NOp(Sub)))"
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut " 1  - 2 -3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Sub)), Val(Int(3)), NOp(Sub)))"
        );
        // Mul
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "  1 *2  ")),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)))"
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut " 1  * 2 *3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), Val(Int(3)), NOp(Mul)))"
        );
        // Div
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "  1 /2  ")),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), NOp(Div)))"
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut " 1  / 2 /3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Div)), Val(Int(3)), NOp(Div)))"
        );
        // Var
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "  x  ")),
            r#"Ok(Var(VarName::new("x")))"#
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "  xsss ")),
            r#"Ok(Var(VarName::new("xsss")))"#
        );
        // Time index
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "x [-1, 0 ]")),
            r#"Ok(SIndex(Var(VarName::new("x")), -1, Int(0)))"#
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "x[1,0]")),
            r#"Ok(SIndex(Var(VarName::new("x")), 1, Int(0)))"#
        );
        // Paren
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "  (1)  ")),
            "Ok(Val(Int(1)))"
        );
        // Don't care about order of eval; care about what the AST looks like
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut " 2 + (2 + 3)")),
            "Ok(BinOp(Val(Int(2)), BinOp(Val(Int(2)), Val(Int(3)), NOp(Add)), NOp(Add)))"
        );
        // If then else
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "if true then 1 else 2")),
            "Ok(If(Val(Bool(true)), Val(Int(1)), Val(Int(2))))"
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "if true then x+x else y+y")),
            r#"Ok(If(Val(Bool(true)), BinOp(Var(VarName::new("x")), Var(VarName::new("x")), NOp(Add)), BinOp(Var(VarName::new("y")), Var(VarName::new("y")), NOp(Add))))"#
        );

        // ChatGPT generated tests with mixed arithmetic and parentheses iexprs. It only had knowledge of the tests above.
        // Basic mixed addition and multiplication
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "1 + 2 * 3")),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), Val(Int(3)), NOp(Mul)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "1 * 2 + 3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), Val(Int(3)), NOp(Add)))"
        );
        // Mixed addition, subtraction, and multiplication
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "1 + 2 * 3 - 4")),
            "Ok(BinOp(BinOp(Val(Int(1)), BinOp(Val(Int(2)), Val(Int(3)), NOp(Mul)), NOp(Add)), Val(Int(4)), NOp(Sub)))"
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "1 * 2 + 3 - 4")),
            "Ok(BinOp(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), Val(Int(3)), NOp(Add)), Val(Int(4)), NOp(Sub)))"
        );
        // Mixed addition and division
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "10 + 20 / 5")),
            "Ok(BinOp(Val(Int(10)), BinOp(Val(Int(20)), Val(Int(5)), NOp(Div)), NOp(Add)))"
        );
        // Nested parentheses with mixed operations
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "(1 + 2) * (3 - 4)")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Sub)), NOp(Mul)))"
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "1 + (2 * (3 + 4))")),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Add)), NOp(Mul)), NOp(Add)))"
        );
        // Complex nested expressions
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "((1 + 2) * 3) + (4 / (5 - 6))")),
            "Ok(BinOp(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), Val(Int(3)), NOp(Mul)), BinOp(Val(Int(4)), BinOp(Val(Int(5)), Val(Int(6)), NOp(Sub)), NOp(Div)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "(1 + (2 * (3 - (4 / 5))))")),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), BinOp(Val(Int(3)), BinOp(Val(Int(4)), Val(Int(5)), NOp(Div)), NOp(Sub)), NOp(Mul)), NOp(Add)))"
        );
        // More complex expressions with deep nesting
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "((1 + 2) * (3 + 4))")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Add)), NOp(Mul)))"
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "((1 * 2) + (3 * 4)) / 5")),
            "Ok(BinOp(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Mul)), NOp(Add)), Val(Int(5)), NOp(Div)))"
        );
        // Multiple levels of nested expressions
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "1 + (2 * (3 + (4 / (5 - 6))))")),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), BinOp(Val(Int(3)), BinOp(Val(Int(4)), BinOp(Val(Int(5)), Val(Int(6)), NOp(Sub)), NOp(Div)), NOp(Add)), NOp(Mul)), NOp(Add)))"
        );

        // ChatGPT generated tests with mixed iexprs. It only had knowledge of the tests above.
        // Mixing addition, subtraction, and variables
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "x + 2 - y")),
            r#"Ok(BinOp(BinOp(Var(VarName::new("x")), Val(Int(2)), NOp(Add)), Var(VarName::new("y")), NOp(Sub)))"#
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "(x + y) * 3")),
            r#"Ok(BinOp(BinOp(Var(VarName::new("x")), Var(VarName::new("y")), NOp(Add)), Val(Int(3)), NOp(Mul)))"#
        );
        // Nested arithmetic with variables and parentheses
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "(a + b) / (c - d)")),
            r#"Ok(BinOp(BinOp(Var(VarName::new("a")), Var(VarName::new("b")), NOp(Add)), BinOp(Var(VarName::new("c")), Var(VarName::new("d")), NOp(Sub)), NOp(Div)))"#
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "x * (y + 3) - z / 2")),
            r#"Ok(BinOp(BinOp(Var(VarName::new("x")), BinOp(Var(VarName::new("y")), Val(Int(3)), NOp(Add)), NOp(Mul)), BinOp(Var(VarName::new("z")), Val(Int(2)), NOp(Div)), NOp(Sub)))"#
        );
        // If-then-else with mixed arithmetic
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "if true then 1 + 2 else 3 * 4")),
            "Ok(If(Val(Bool(true)), BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Mul))))"
        );
        // Time index in arithmetic expression
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "x[0, 1] + y[-1, 0]")),
            r#"Ok(BinOp(SIndex(Var(VarName::new("x")), 0, Int(1)), SIndex(Var(VarName::new("y")), -1, Int(0)), NOp(Add)))"#
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "x[1, 2] * (y + 3)")),
            r#"Ok(BinOp(SIndex(Var(VarName::new("x")), 1, Int(2)), BinOp(Var(VarName::new("y")), Val(Int(3)), NOp(Add)), NOp(Mul)))"#
        );
        // Complex expression with nested if-then-else and mixed operations
        assert_eq!(
            presult_to_string(&dist_constraint_body(
                &mut "(1 + x) * if y then 3 else z / 2"
            )),
            r#"Ok(BinOp(BinOp(Val(Int(1)), Var(VarName::new("x")), NOp(Add)), If(Var(VarName::new("y")), Val(Int(3)), BinOp(Var(VarName::new("z")), Val(Int(2)), NOp(Div))), NOp(Mul)))"#
        );
    }

    #[test]
    fn test_parse_empty_string() {
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "")),
            "Err(ContextError { context: [], cause: None })"
        );
    }

    #[test]
    fn test_parse_invalid_expression() {
        // TODO: Bug here in parser. It should be able to handle these cases.
        // assert_eq!(presult_to_string(&dist_constraint_body(&mut "1 +")), "Err(Backtrack(ContextError { context: [], cause: None }))");
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "&& true")),
            "Err(ContextError { context: [], cause: None })"
        );
    }

    #[test]
    fn test_parse_boolean_expressions() {
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "true && false")),
            "Ok(BinOp(Val(Bool(true)), Val(Bool(false)), BOp(And)))"
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "true || false")),
            "Ok(BinOp(Val(Bool(true)), Val(Bool(false)), BOp(Or)))"
        );
    }

    #[test]
    fn test_parse_mixed_boolean_and_arithmetic() {
        // Expressions do not make sense but parser should allow it
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "1 + 2 && 3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), Val(Int(3)), BOp(And)))"
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut "true || 1 * 2")),
            "Ok(BinOp(Val(Bool(true)), BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), BOp(Or)))"
        );
    }
    #[test]
    fn test_parse_string_concatenation() {
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut r#""foo" ++ "bar""#)),
            r#"Ok(BinOp(Val(Str("foo")), Val(Str("bar")), SOp(Concat)))"#
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut r#""hello" ++ " " ++ "world""#)),
            r#"Ok(BinOp(BinOp(Val(Str("hello")), Val(Str(" ")), SOp(Concat)), Val(Str("world")), SOp(Concat)))"#
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut r#""a" ++ "b" ++ "c""#)),
            r#"Ok(BinOp(BinOp(Val(Str("a")), Val(Str("b")), SOp(Concat)), Val(Str("c")), SOp(Concat)))"#
        );
    }

    #[test]
    fn test_parse_default() {
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut r#"default(x, 0)"#)),
            r#"Ok(Default(Var(VarName::new("x")), Val(Int(0))))"#
        )
    }

    #[test]
    fn test_parse_default_dist_constraint_body() {
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut r#"default(x, y)"#)),
            r#"Ok(Default(Var(VarName::new("x")), Var(VarName::new("y"))))"#
        )
    }

    #[test]
    fn test_parse_lindex() {
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut r#"List.get(List(1, 2), 42)"#)),
            r#"Ok(LIndex(Val(List([Int(1), Int(2)])), Val(Int(42))))"#
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut r#"List.get(x, 42)"#)),
            r#"Ok(LIndex(Var(VarName::new("x")), Val(Int(42))))"#
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut r#"List.get(x, 1+2)"#)),
            r#"Ok(LIndex(Var(VarName::new("x")), BinOp(Val(Int(1)), Val(Int(2)), NOp(Add))))"#
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(
                &mut r#"List.get(List.get(List(List(1, 2), List(3, 4)), 0), 1)"#
            )),
            r#"Ok(LIndex(LIndex(Val(List([List([Int(1), Int(2)]), List([Int(3), Int(4)])])), Val(Int(0))), Val(Int(1))))"#
        );
    }

    #[test]
    fn test_parse_lconcat() {
        assert_eq!(
            presult_to_string(&dist_constraint_body(
                &mut r#"List.concat(List(1, 2), List(3, 4))"#
            )),
            r#"Ok(LConcat(Val(List([Int(1), Int(2)])), Val(List([Int(3), Int(4)]))))"#
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut r#"List.concat(List(), List())"#)),
            r#"Ok(LConcat(Val(List([])), Val(List([]))))"#
        );
    }

    #[test]
    fn test_parse_lappend() {
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut r#"List.append(List(1, 2), 3)"#)),
            r#"Ok(LAppend(Val(List([Int(1), Int(2)])), Val(Int(3))))"#
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut r#"List.append(List(), 3)"#)),
            r#"Ok(LAppend(Val(List([])), Val(Int(3))))"#
        );
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut r#"List.append(List(), x)"#)),
            r#"Ok(LAppend(Val(List([])), Var(VarName::new("x"))))"#
        );
    }

    #[test]
    fn test_parse_lhead() {
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut r#"List.head(List(1, 2))"#)),
            r#"Ok(LHead(Val(List([Int(1), Int(2)]))))"#
        );
        // Ok for parser but will result in runtime error:
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut r#"List.head(List())"#)),
            r#"Ok(LHead(Val(List([]))))"#
        );
    }

    #[test]
    fn test_parse_ltail() {
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut r#"List.tail(List(1, 2))"#)),
            r#"Ok(LTail(Val(List([Int(1), Int(2)]))))"#
        );
        // Ok for parser but will result in runtime error:
        assert_eq!(
            presult_to_string(&dist_constraint_body(&mut r#"List.tail(List())"#)),
            r#"Ok(LTail(Val(List([]))))"#
        );
    }
}
