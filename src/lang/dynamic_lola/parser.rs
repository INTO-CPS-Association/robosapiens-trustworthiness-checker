use winnow::Parser;
use winnow::Result;
use winnow::combinator::*;
use winnow::token::literal;

use super::super::core::parser::*;
use super::ast::*;
use crate::core::StreamType;
use crate::core::VarName;

// This is the top-level parser for LOLA expressions
pub fn lola_expression(s: &mut &str) -> Result<SExpr> {
    sexpr.parse_next(s)
}

fn paren(s: &mut &str) -> Result<SExpr> {
    delimited('(', sexpr, ')').parse_next(s)
}

// Used for Lists in output streams
fn sexpr_list(s: &mut &str) -> Result<SExpr> {
    let res = delimited(
        seq!("List", loop_ms_or_lb_or_lc, '('),
        separated(
            0..,
            sexpr,
            seq!(loop_ms_or_lb_or_lc, ',', loop_ms_or_lb_or_lc),
        ),
        ')',
    )
    .parse_next(s);
    match res {
        Ok(exprs) => Ok(SExpr::List(exprs)),
        Err(e) => Err(e),
    }
}

fn var(s: &mut &str) -> Result<SExpr> {
    ident
        .map(|name: &str| SExpr::Var(name.into()))
        .parse_next(s)
}

// Same as `val` but returns SExpr::Val
fn sval(s: &mut &str) -> Result<SExpr> {
    val.map(|v| SExpr::Val(v)).parse_next(s)
}

fn sindex(s: &mut &str) -> Result<SExpr> {
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
    .map(|(e, i, d)| SExpr::SIndex(Box::new(e), i, d))
    .parse_next(s)
}

fn ifelse(s: &mut &str) -> Result<SExpr> {
    seq!((
        _: whitespace,
        _: "if",
        _: loop_ms_or_lb_or_lc,
        sexpr,
        _: loop_ms_or_lb_or_lc,
        _: "then",
        _: loop_ms_or_lb_or_lc,
        sexpr,
        _: loop_ms_or_lb_or_lc,
        _: "else",
        _: loop_ms_or_lb_or_lc,
        sexpr,
        _: whitespace,
    ))
    .map(|(b, s1, s2)| SExpr::If(Box::new(b), Box::new(s1), Box::new(s2)))
    .parse_next(s)
}

fn defer(s: &mut &str) -> Result<SExpr> {
    seq!((
        _: whitespace,
        _: literal("defer"),
        _: loop_ms_or_lb_or_lc,
        _: '(',
        _: loop_ms_or_lb_or_lc,
        sexpr,
        _: loop_ms_or_lb_or_lc,
        _: ')',
    ))
    .map(|(e,)| SExpr::Defer(Box::new(e)))
    .parse_next(s)
}

fn update(s: &mut &str) -> Result<SExpr> {
    seq!((
        _: whitespace,
        _: literal("update"),
        _: loop_ms_or_lb_or_lc,
        _: '(',
        sexpr,
        _: loop_ms_or_lb_or_lc,
        _: ',',
        _: loop_ms_or_lb_or_lc,
        sexpr,
        _: loop_ms_or_lb_or_lc,
        _: ')',
    ))
    .map(|(lhs, rhs)| SExpr::Update(Box::new(lhs), Box::new(rhs)))
    .parse_next(s)
}

fn is_defined(s: &mut &str) -> Result<SExpr> {
    seq!((
        _: whitespace,
        _: literal("is_defined"),
        _: loop_ms_or_lb_or_lc,
        _: '(',
        _: loop_ms_or_lb_or_lc,
        sexpr,
        _: loop_ms_or_lb_or_lc,
        _: ')',
    ))
    .map(|(e,)| SExpr::IsDefined(Box::new(e)))
    .parse_next(s)
}

fn when(s: &mut &str) -> Result<SExpr> {
    seq!((
        _: whitespace,
        _: literal("when"),
        _: loop_ms_or_lb_or_lc,
        _: '(',
        _: loop_ms_or_lb_or_lc,
        sexpr,
        _: loop_ms_or_lb_or_lc,
        _: ')',
    ))
    .map(|(e,)| SExpr::When(Box::new(e)))
    .parse_next(s)
}

fn eval(s: &mut &str) -> Result<SExpr> {
    seq!((
        _: whitespace,
        _: "eval",
        _: loop_ms_or_lb_or_lc,
        _: '(',
        _: loop_ms_or_lb_or_lc,
        sexpr,
        _: loop_ms_or_lb_or_lc,
        _: ')',
        _: whitespace,
    ))
    .map(|(e,)| SExpr::Eval(Box::new(e)))
    .parse_next(s)
}

fn default(s: &mut &str) -> Result<SExpr> {
    seq!((
        _: whitespace,
        _: literal("default"),
        _: '(',
        _: loop_ms_or_lb_or_lc,
        sexpr,
        _: loop_ms_or_lb_or_lc,
        _: ',',
        _: loop_ms_or_lb_or_lc,
        sexpr,
        _: loop_ms_or_lb_or_lc,
        _: ')',
    ))
    .map(|(lhs, rhs)| SExpr::Default(Box::new(lhs), Box::new(rhs)))
    .parse_next(s)
}

fn not(s: &mut &str) -> Result<SExpr> {
    seq!((
        _: whitespace,
        _: "!",
        _: loop_ms_or_lb_or_lc,
        atom,
        _: whitespace,
    ))
    .map(|(e,)| SExpr::Not(Box::new(e)))
    .parse_next(s)
}

fn lindex(s: &mut &str) -> Result<SExpr> {
    seq!(
        _: whitespace,
        _: "List.get",
        _: loop_ms_or_lb_or_lc,
        _: '(',
        _: loop_ms_or_lb_or_lc,
        sexpr,
        _: loop_ms_or_lb_or_lc,
        _: ',',
        _: loop_ms_or_lb_or_lc,
        sexpr,
        _: loop_ms_or_lb_or_lc,
        _: ')',
    )
    .map(|(e, i)| SExpr::LIndex(Box::new(e), Box::new(i)))
    .parse_next(s)
}

fn lappend(s: &mut &str) -> Result<SExpr> {
    seq!(
        _: whitespace,
        _: "List.append",
        _: loop_ms_or_lb_or_lc,
        _: '(',
        _: loop_ms_or_lb_or_lc,
        sexpr,
        _: loop_ms_or_lb_or_lc,
        _: ',',
        _: loop_ms_or_lb_or_lc,
        sexpr,
        _: loop_ms_or_lb_or_lc,
        _: ')',
    )
    .map(|(lst, el)| SExpr::LAppend(Box::new(lst), Box::new(el)))
    .parse_next(s)
}

fn lconcat(s: &mut &str) -> Result<SExpr> {
    seq!(
        _: whitespace,
        _: "List.concat",
        _: loop_ms_or_lb_or_lc,
        _: '(',
        _: loop_ms_or_lb_or_lc,
        sexpr,
        _: loop_ms_or_lb_or_lc,
        _: ',',
        _: loop_ms_or_lb_or_lc,
        sexpr,
        _: loop_ms_or_lb_or_lc,
        _: ')',
    )
    .map(|(lst1, lst2)| SExpr::LConcat(Box::new(lst1), Box::new(lst2)))
    .parse_next(s)
}

fn lhead(s: &mut &str) -> Result<SExpr> {
    seq!((
        _: whitespace,
        _: "List.head",
        _: loop_ms_or_lb_or_lc,
        _: '(',
        _: loop_ms_or_lb_or_lc,
        sexpr,
        _: loop_ms_or_lb_or_lc,
        _: ')',
        _: whitespace,
    ))
    .map(|(lst,)| SExpr::LHead(Box::new(lst)))
    .parse_next(s)
}

fn ltail(s: &mut &str) -> Result<SExpr> {
    seq!((
        _: whitespace,
        _: "List.tail",
        _: loop_ms_or_lb_or_lc,
        _: '(',
        _: loop_ms_or_lb_or_lc,
        sexpr,
        _: loop_ms_or_lb_or_lc,
        _: ')',
        _: whitespace,
    ))
    .map(|(lst,)| SExpr::LTail(Box::new(lst)))
    .parse_next(s)
}

/// Fundamental expressions of the language
fn atom(s: &mut &str) -> Result<SExpr> {
    delimited(
        whitespace,
        alt((
            sindex, lindex, lappend, lconcat, lhead, ltail, not, eval, sval, ifelse, defer, update,
            default, when, is_defined, sexpr_list, var, paren,
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
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Le,
    Eq,
}
impl BinaryPrecedences {
    pub fn next(&self) -> Option<Self> {
        use BinaryPrecedences::*;
        match self {
            Concat => Some(Or),
            Or => Some(And),
            And => Some(Sub),
            Sub => Some(Add),
            Add => Some(Mul),
            Mul => Some(Div),
            Div => Some(Mod),
            Mod => Some(Le),
            Le => Some(Eq),
            Eq => None,
        }
    }

    pub fn get_lit(&self) -> &'static str {
        use BinaryPrecedences::*;
        match self {
            Concat => "++",
            Or => "||",
            And => "&&",
            Add => "+",
            Sub => "-",
            Mul => "*",
            Div => "/",
            Mod => "%",
            Le => "<=",
            Eq => "==",
        }
    }

    pub fn get_binop(&self) -> SBinOp {
        use BinaryPrecedences::*;
        match self {
            Concat => SBinOp::SOp(StrBinOp::Concat),
            Or => SBinOp::BOp(BoolBinOp::Or),
            And => SBinOp::BOp(BoolBinOp::And),
            Add => SBinOp::NOp(NumericalBinOp::Add),
            Sub => SBinOp::NOp(NumericalBinOp::Sub),
            Mul => SBinOp::NOp(NumericalBinOp::Mul),
            Div => SBinOp::NOp(NumericalBinOp::Div),
            Mod => SBinOp::NOp(NumericalBinOp::Mod),
            Le => SBinOp::COp(CompBinOp::Le),
            Eq => SBinOp::COp(CompBinOp::Eq),
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
fn binary_op(current_op: BinaryPrecedences) -> impl FnMut(&mut &str) -> Result<SExpr> {
    move |s: &mut &str| {
        let next_parser_op = current_op.next();
        let mut next_parser: Box<dyn FnMut(&mut &str) -> Result<SExpr>> = match next_parser_op {
            Some(next_parser) => Box::new(binary_op(next_parser)),
            None => Box::new(|i: &mut &str| atom.parse_next(i)),
        };
        let lit = current_op.get_lit();
        let res = separated_foldl1(&mut next_parser, literal(lit), |left, _, right| {
            SExpr::BinOp(Box::new(left), Box::new(right), current_op.get_binop())
        })
        .parse_next(s);
        res
    }
}

pub fn sexpr(s: &mut &str) -> Result<SExpr> {
    delimited(
        whitespace,
        binary_op(BinaryPrecedences::lowest_precedence()),
        whitespace,
    )
    .parse_next(s)
}

fn type_annotation(s: &mut &str) -> Result<StreamType> {
    seq!((
        _: whitespace,
        _: literal(":"),
        _: loop_ms_or_lb_or_lc,
        alt((literal("Int"), literal("Float"), literal("Bool"), literal("Str"), literal("Unit"))),
        _: whitespace,
    ))
    .map(|(typ,)| match typ {
        "Int" => StreamType::Int,
        "Float" => StreamType::Float,
        "Bool" => StreamType::Bool,
        "Str" => StreamType::Str,
        "Unit" => StreamType::Unit,
        _ => unreachable!(),
    })
    .parse_next(s)
}

fn input_decl(s: &mut &str) -> Result<(VarName, Option<StreamType>)> {
    seq!((
        _: whitespace,
        _: literal("in"),
        _: loop_ms_or_lb_or_lc,
        ident,
        opt(type_annotation),
        _: whitespace,
    ))
    .map(|(name, typ): (&str, _)| (name.into(), typ))
    .parse_next(s)
}

fn input_decls(s: &mut &str) -> Result<Vec<(VarName, Option<StreamType>)>> {
    separated(0.., input_decl, seq!(lb_or_lc, loop_ms_or_lb_or_lc)).parse_next(s)
}

fn output_decl(s: &mut &str) -> Result<(VarName, Option<StreamType>)> {
    seq!((
        _: whitespace,
        _: literal("out"),
        _: loop_ms_or_lb_or_lc,
        ident,
        opt(type_annotation),
        _: whitespace,
    ))
    .map(|(name, typ): (&str, _)| (name.into(), typ))
    .parse_next(s)
}

fn output_decls(s: &mut &str) -> Result<Vec<(VarName, Option<StreamType>)>> {
    separated(0.., output_decl, seq!(lb_or_lc, loop_ms_or_lb_or_lc)).parse_next(s)
}

fn var_decl(s: &mut &str) -> Result<(VarName, SExpr)> {
    seq!((
        _: whitespace,
        ident,
        _: loop_ms_or_lb_or_lc,
        _: literal("="),
        _: loop_ms_or_lb_or_lc,
        sexpr,
        _: whitespace,
    ))
    .map(|(name, expr)| (name.into(), expr))
    .parse_next(s)
}

fn var_decls(s: &mut &str) -> Result<Vec<(VarName, SExpr)>> {
    separated(0.., var_decl, seq!(lb_or_lc, loop_ms_or_lb_or_lc)).parse_next(s)
}

pub fn lola_specification(s: &mut &str) -> Result<LOLASpecification> {
    seq!((
        _: loop_ms_or_lb_or_lc,
        input_decls,
        _: loop_ms_or_lb_or_lc,
        output_decls,
        _: loop_ms_or_lb_or_lc,
        var_decls,
        _: loop_ms_or_lb_or_lc,
    ))
    .map(|(input_vars, output_vars, exprs)| LOLASpecification {
        input_vars: input_vars.iter().map(|(name, _)| name.clone()).collect(),
        output_vars: output_vars.iter().map(|(name, _)| name.clone()).collect(),
        exprs: exprs.into_iter().collect(),
        type_annotations: input_vars
            .iter()
            .chain(output_vars.iter())
            .cloned()
            .filter_map(|(name, typ)| match typ {
                Some(typ) => Some((name, typ)),
                None => None,
            })
            .collect(),
    })
    .parse_next(s)
}

#[cfg(test)]
mod tests {
    use crate::core::Value;
    use std::collections::BTreeMap;

    use winnow::error::ContextError;

    use super::*;
    use test_log::test;

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
    fn test_sexpr() -> Result<(), ContextError> {
        assert_eq!(
            sexpr(&mut (*"1 + 2".to_string()).into())?,
            SExpr::BinOp(
                Box::new(SExpr::Val(Value::Int(1))),
                Box::new(SExpr::Val(Value::Int(2))),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
        );
        assert_eq!(
            sexpr(&mut (*"1 + 2 * 3".to_string()).into())?,
            SExpr::BinOp(
                Box::new(SExpr::Val(Value::Int(1))),
                Box::new(SExpr::BinOp(
                    Box::new(SExpr::Val(Value::Int(2))),
                    Box::new(SExpr::Val(Value::Int(3))),
                    SBinOp::NOp(NumericalBinOp::Mul),
                )),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
        );
        assert_eq!(
            sexpr(&mut (*"x + (y + 2)".to_string()).into())?,
            SExpr::BinOp(
                Box::new(SExpr::Var("x".into())),
                Box::new(SExpr::BinOp(
                    Box::new(SExpr::Var("y".into())),
                    Box::new(SExpr::Val(Value::Int(2))),
                    SBinOp::NOp(NumericalBinOp::Add),
                )),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
        );
        assert_eq!(
            sexpr(&mut (*"if true then 1 else 2".to_string()).into())?,
            SExpr::If(
                Box::new(SExpr::Val(true.into())),
                Box::new(SExpr::Val(Value::Int(1))),
                Box::new(SExpr::Val(Value::Int(2))),
            ),
        );
        assert_eq!(
            sexpr(&mut (*"(x)[-1, 0]".to_string()).into())?,
            SExpr::SIndex(Box::new(SExpr::Var("x".into())), -1, Value::Int(0),),
        );
        assert_eq!(
            sexpr(&mut (*"(x + y)[-3, 2]".to_string()).into())?,
            SExpr::SIndex(
                Box::new(SExpr::BinOp(
                    Box::new(SExpr::Var("x".into())),
                    Box::new(SExpr::Var("y".into()),),
                    SBinOp::NOp(NumericalBinOp::Add),
                )),
                -3,
                Value::Int(2),
            ),
        );
        assert_eq!(
            sexpr(&mut (*"1 + (x)[-1, 0]".to_string()).into())?,
            SExpr::BinOp(
                Box::new(SExpr::Val(Value::Int(1))),
                Box::new(SExpr::SIndex(
                    Box::new(SExpr::Var("x".into())),
                    -1,
                    Value::Int(0),
                ),),
                SBinOp::NOp(NumericalBinOp::Add),
            )
        );
        assert_eq!(
            sexpr(&mut (*"\"test\"".to_string()).into())?,
            SExpr::Val(Value::Str("test".into())),
        );
        assert_eq!(
            sexpr(&mut (*"(stage == \"m\")").into())?,
            SExpr::BinOp(
                Box::new(SExpr::Var("stage".into())),
                Box::new(SExpr::Val("m".into())),
                SBinOp::COp(CompBinOp::Eq),
            )
        );
        Ok(())
    }

    #[test]
    fn test_input_decl() -> Result<(), ContextError> {
        assert_eq!(
            input_decl(&mut (*"in x".to_string()).into())?,
            ("x".into(), None),
        );
        Ok(())
    }

    #[test]
    fn test_typed_input_decl() -> Result<(), ContextError> {
        assert_eq!(
            input_decl(&mut (*"in x: Int".to_string()).into())?,
            ("x".into(), Some(StreamType::Int)),
        );
        assert_eq!(
            input_decl(&mut (*"in x: Float".to_string()).into())?,
            ("x".into(), Some(StreamType::Float)),
        );
        Ok(())
    }

    #[test]
    fn test_input_decls() -> Result<(), ContextError> {
        assert_eq!(input_decls(&mut (*"".to_string()).into())?, vec![],);
        assert_eq!(
            input_decls(&mut (*"in x".to_string()).into())?,
            vec![("x".into(), None)],
        );
        assert_eq!(
            input_decls(&mut (*"in x\nin y".to_string()).into())?,
            vec![("x".into(), None), ("y".into(), None)],
        );
        Ok(())
    }

    #[test]
    fn test_parse_lola_simple_add() -> Result<(), ContextError> {
        let input = crate::lola_fixtures::spec_simple_add_monitor();
        let simple_add_spec = LOLASpecification {
            input_vars: vec!["x".into(), "y".into()],
            output_vars: vec!["z".into()],
            exprs: BTreeMap::from([(
                "z".into(),
                SExpr::BinOp(
                    Box::new(SExpr::Var("x".into())),
                    Box::new(SExpr::Var("y".into())),
                    SBinOp::NOp(NumericalBinOp::Add),
                ),
            )]),
            type_annotations: BTreeMap::new(),
        };
        assert_eq!(lola_specification(&mut (*input).into())?, simple_add_spec);
        Ok(())
    }

    #[test]
    fn test_parse_lola_simple_add_typed() -> Result<(), ContextError> {
        let mut input = crate::lola_fixtures::spec_simple_add_monitor_typed();
        let simple_add_spec = LOLASpecification {
            input_vars: vec!["x".into(), "y".into()],
            output_vars: vec!["z".into()],
            exprs: BTreeMap::from([(
                "z".into(),
                SExpr::BinOp(
                    Box::new(SExpr::Var("x".into())),
                    Box::new(SExpr::Var("y".into())),
                    SBinOp::NOp(NumericalBinOp::Add),
                ),
            )]),
            type_annotations: BTreeMap::from([
                (VarName::new("x"), StreamType::Int),
                (VarName::new("y"), StreamType::Int),
                (VarName::new("z"), StreamType::Int),
            ]),
        };
        assert_eq!(lola_specification(&mut input)?, simple_add_spec);
        Ok(())
    }

    #[test]
    fn test_parse_lola_simple_add_float_typed() -> Result<(), ContextError> {
        let mut input = crate::lola_fixtures::spec_simple_add_monitor_typed_float();
        let simple_add_spec = LOLASpecification {
            input_vars: vec!["x".into(), "y".into()],
            output_vars: vec!["z".into()],
            exprs: BTreeMap::from([(
                "z".into(),
                SExpr::BinOp(
                    Box::new(SExpr::Var("x".into())),
                    Box::new(SExpr::Var("y".into())),
                    SBinOp::NOp(NumericalBinOp::Add),
                ),
            )]),
            type_annotations: BTreeMap::from([
                ("x".into(), StreamType::Float),
                ("y".into(), StreamType::Float),
                ("z".into(), StreamType::Float),
            ]),
        };
        assert_eq!(lola_specification(&mut input)?, simple_add_spec);
        Ok(())
    }

    #[test]
    fn test_parse_lola_count() -> Result<(), ContextError> {
        let input = "\
            out x\n\
            x = 1 + (x)[-1, 0]";
        let count_spec = LOLASpecification {
            input_vars: vec![],
            output_vars: vec!["x".into()],
            exprs: BTreeMap::from([(
                "x".into(),
                SExpr::BinOp(
                    Box::new(SExpr::Val(Value::Int(1))),
                    Box::new(SExpr::SIndex(
                        Box::new(SExpr::Var("x".into())),
                        -1,
                        Value::Int(0),
                    )),
                    SBinOp::NOp(NumericalBinOp::Add),
                ),
            )]),
            type_annotations: BTreeMap::new(),
        };
        assert_eq!(lola_specification(&mut (*input).into())?, count_spec);
        Ok(())
    }

    #[test]
    fn test_parse_lola_eval() -> Result<(), ContextError> {
        let input = "\
            in x\n\
            in y\n\
            in s\n\
            out z\n\
            out w\n\
            z = x + y\n\
            w = eval(s)";
        let eval_spec = LOLASpecification {
            input_vars: vec!["x".into(), "y".into(), "s".into()],
            output_vars: vec!["z".into(), "w".into()],
            exprs: BTreeMap::from([
                (
                    "z".into(),
                    SExpr::BinOp(
                        Box::new(SExpr::Var("x".into())),
                        Box::new(SExpr::Var("y".into())),
                        SBinOp::NOp(NumericalBinOp::Add),
                    ),
                ),
                ("w".into(), SExpr::Eval(Box::new(SExpr::Var("s".into())))),
            ]),
            type_annotations: BTreeMap::new(),
        };
        assert_eq!(lola_specification(&mut (*input).into())?, eval_spec);
        Ok(())
    }

    #[test]
    fn test_float_exprs() {
        // Add
        assert_eq!(presult_to_string(&sexpr(&mut "0.0")), "Ok(Val(Float(0.0)))");
        assert_eq!(
            presult_to_string(&sexpr(&mut "  1.0 +2.0  ")),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut " 1.0  + 2.0 +3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Add)), Val(Float(3.0)), NOp(Add)))"
        );
        // Sub
        assert_eq!(
            presult_to_string(&sexpr(&mut "  1.0 -2.0  ")),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Sub)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut " 1.0  - 2.0 -3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Sub)), Val(Float(3.0)), NOp(Sub)))"
        );
        // Mul
        assert_eq!(
            presult_to_string(&sexpr(&mut "  1.0 *2.0  ")),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Mul)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut " 1.0  * 2.0 *3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Mul)), Val(Float(3.0)), NOp(Mul)))"
        );
        // Div
        assert_eq!(
            presult_to_string(&sexpr(&mut "  1.0 /2.0  ")),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Div)))"
        );
    }

    #[test]
    fn test_mixed_float_int_exprs() {
        // Add
        assert_eq!(
            presult_to_string(&sexpr(&mut "0.0 + 2")),
            "Ok(BinOp(Val(Float(0.0)), Val(Int(2)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "1 + 2.0")),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "1.0 + 2 + 3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Int(2)), NOp(Add)), Val(Float(3.0)), NOp(Add)))"
        );
        // Sub
        assert_eq!(
            presult_to_string(&sexpr(&mut "1 - 2.0")),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), NOp(Sub)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "1.0 - 2 - 3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Int(2)), NOp(Sub)), Val(Float(3.0)), NOp(Sub)))"
        );
        // Mul
        assert_eq!(
            presult_to_string(&sexpr(&mut "1 * 2.0")),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), NOp(Mul)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "1.0 * 2 * 3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Int(2)), NOp(Mul)), Val(Float(3.0)), NOp(Mul)))"
        );
        // Div
        assert_eq!(
            presult_to_string(&sexpr(&mut "1 / 2.0")),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), NOp(Div)))"
        );
    }

    #[test]
    fn test_integer_exprs() {
        // Add
        assert_eq!(presult_to_string(&sexpr(&mut "0")), "Ok(Val(Int(0)))");
        assert_eq!(
            presult_to_string(&sexpr(&mut "  1 +2  ")),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut " 1  + 2 +3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), Val(Int(3)), NOp(Add)))"
        );
        // Sub
        assert_eq!(
            presult_to_string(&sexpr(&mut "  1 -2  ")),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), NOp(Sub)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut " 1  - 2 -3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Sub)), Val(Int(3)), NOp(Sub)))"
        );
        // Mul
        assert_eq!(
            presult_to_string(&sexpr(&mut "  1 *2  ")),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut " 1  * 2 *3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), Val(Int(3)), NOp(Mul)))"
        );
        // Div
        assert_eq!(
            presult_to_string(&sexpr(&mut "  1 /2  ")),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), NOp(Div)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut " 1  / 2 /3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Div)), Val(Int(3)), NOp(Div)))"
        );
        // Var
        assert_eq!(
            presult_to_string(&sexpr(&mut "  x  ")),
            r#"Ok(Var(VarName::new("x")))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "  xsss ")),
            r#"Ok(Var(VarName::new("xsss")))"#
        );
        // Time index
        assert_eq!(
            presult_to_string(&sexpr(&mut "x [-1, 0 ]")),
            r#"Ok(SIndex(Var(VarName::new("x")), -1, Int(0)))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "x[1,0]")),
            r#"Ok(SIndex(Var(VarName::new("x")), 1, Int(0)))"#
        );
        // Paren
        assert_eq!(presult_to_string(&sexpr(&mut "  (1)  ")), "Ok(Val(Int(1)))");
        // Don't care about order of eval; care about what the AST looks like
        assert_eq!(
            presult_to_string(&sexpr(&mut " 2 + (2 + 3)")),
            "Ok(BinOp(Val(Int(2)), BinOp(Val(Int(2)), Val(Int(3)), NOp(Add)), NOp(Add)))"
        );
        // If then else
        assert_eq!(
            presult_to_string(&sexpr(&mut "if true then 1 else 2")),
            "Ok(If(Val(Bool(true)), Val(Int(1)), Val(Int(2))))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "if true then x+x else y+y")),
            r#"Ok(If(Val(Bool(true)), BinOp(Var(VarName::new("x")), Var(VarName::new("x")), NOp(Add)), BinOp(Var(VarName::new("y")), Var(VarName::new("y")), NOp(Add))))"#
        );

        // ChatGPT generated tests with mixed arithmetic and parentheses iexprs. It only had knowledge of the tests above.
        // Basic mixed addition and multiplication
        assert_eq!(
            presult_to_string(&sexpr(&mut "1 + 2 * 3")),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), Val(Int(3)), NOp(Mul)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "1 * 2 + 3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), Val(Int(3)), NOp(Add)))"
        );
        // Mixed addition, subtraction, and multiplication
        assert_eq!(
            presult_to_string(&sexpr(&mut "1 + 2 * 3 - 4")),
            "Ok(BinOp(BinOp(Val(Int(1)), BinOp(Val(Int(2)), Val(Int(3)), NOp(Mul)), NOp(Add)), Val(Int(4)), NOp(Sub)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "1 * 2 + 3 - 4")),
            "Ok(BinOp(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), Val(Int(3)), NOp(Add)), Val(Int(4)), NOp(Sub)))"
        );
        // Mixed addition and division
        assert_eq!(
            presult_to_string(&sexpr(&mut "10 + 20 / 5")),
            "Ok(BinOp(Val(Int(10)), BinOp(Val(Int(20)), Val(Int(5)), NOp(Div)), NOp(Add)))"
        );
        // Nested parentheses with mixed operations
        assert_eq!(
            presult_to_string(&sexpr(&mut "(1 + 2) * (3 - 4)")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Sub)), NOp(Mul)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "1 + (2 * (3 + 4))")),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Add)), NOp(Mul)), NOp(Add)))"
        );
        // Complex nested expressions
        assert_eq!(
            presult_to_string(&sexpr(&mut "((1 + 2) * 3) + (4 / (5 - 6))")),
            "Ok(BinOp(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), Val(Int(3)), NOp(Mul)), BinOp(Val(Int(4)), BinOp(Val(Int(5)), Val(Int(6)), NOp(Sub)), NOp(Div)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "(1 + (2 * (3 - (4 / 5))))")),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), BinOp(Val(Int(3)), BinOp(Val(Int(4)), Val(Int(5)), NOp(Div)), NOp(Sub)), NOp(Mul)), NOp(Add)))"
        );
        // More complex expressions with deep nesting
        assert_eq!(
            presult_to_string(&sexpr(&mut "((1 + 2) * (3 + 4))")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Add)), NOp(Mul)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "((1 * 2) + (3 * 4)) / 5")),
            "Ok(BinOp(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Mul)), NOp(Add)), Val(Int(5)), NOp(Div)))"
        );
        // Multiple levels of nested expressions
        assert_eq!(
            presult_to_string(&sexpr(&mut "1 + (2 * (3 + (4 / (5 - 6))))")),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), BinOp(Val(Int(3)), BinOp(Val(Int(4)), BinOp(Val(Int(5)), Val(Int(6)), NOp(Sub)), NOp(Div)), NOp(Add)), NOp(Mul)), NOp(Add)))"
        );

        // ChatGPT generated tests with mixed iexprs. It only had knowledge of the tests above.
        // Mixing addition, subtraction, and variables
        assert_eq!(
            presult_to_string(&sexpr(&mut "x + 2 - y")),
            r#"Ok(BinOp(BinOp(Var(VarName::new("x")), Val(Int(2)), NOp(Add)), Var(VarName::new("y")), NOp(Sub)))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "(x + y) * 3")),
            r#"Ok(BinOp(BinOp(Var(VarName::new("x")), Var(VarName::new("y")), NOp(Add)), Val(Int(3)), NOp(Mul)))"#
        );
        // Nested arithmetic with variables and parentheses
        assert_eq!(
            presult_to_string(&sexpr(&mut "(a + b) / (c - d)")),
            r#"Ok(BinOp(BinOp(Var(VarName::new("a")), Var(VarName::new("b")), NOp(Add)), BinOp(Var(VarName::new("c")), Var(VarName::new("d")), NOp(Sub)), NOp(Div)))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "x * (y + 3) - z / 2")),
            r#"Ok(BinOp(BinOp(Var(VarName::new("x")), BinOp(Var(VarName::new("y")), Val(Int(3)), NOp(Add)), NOp(Mul)), BinOp(Var(VarName::new("z")), Val(Int(2)), NOp(Div)), NOp(Sub)))"#
        );
        // If-then-else with mixed arithmetic
        assert_eq!(
            presult_to_string(&sexpr(&mut "if true then 1 + 2 else 3 * 4")),
            "Ok(If(Val(Bool(true)), BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Mul))))"
        );
        // Time index in arithmetic expression
        assert_eq!(
            presult_to_string(&sexpr(&mut "x[0, 1] + y[-1, 0]")),
            r#"Ok(BinOp(SIndex(Var(VarName::new("x")), 0, Int(1)), SIndex(Var(VarName::new("y")), -1, Int(0)), NOp(Add)))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "x[1, 2] * (y + 3)")),
            r#"Ok(BinOp(SIndex(Var(VarName::new("x")), 1, Int(2)), BinOp(Var(VarName::new("y")), Val(Int(3)), NOp(Add)), NOp(Mul)))"#
        );
        // Complex expression with nested if-then-else and mixed operations
        assert_eq!(
            presult_to_string(&sexpr(&mut "(1 + x) * if y then 3 else z / 2")),
            r#"Ok(BinOp(BinOp(Val(Int(1)), Var(VarName::new("x")), NOp(Add)), If(Var(VarName::new("y")), Val(Int(3)), BinOp(Var(VarName::new("z")), Val(Int(2)), NOp(Div))), NOp(Mul)))"#
        );
    }

    #[test]
    fn test_var_decl() {
        assert_eq!(
            presult_to_string(&var_decl(&mut "x = 0")),
            r#"Ok((VarName::new("x"), Val(Int(0))))"#
        );
        assert_eq!(
            presult_to_string(&var_decl(&mut r#"x = "hello""#)),
            r#"Ok((VarName::new("x"), Val(Str("hello"))))"#
        );
        assert_eq!(
            presult_to_string(&var_decl(&mut "x = true")),
            r#"Ok((VarName::new("x"), Val(Bool(true))))"#
        );
        assert_eq!(
            presult_to_string(&var_decl(&mut "x = false")),
            r#"Ok((VarName::new("x"), Val(Bool(false))))"#
        );
    }

    #[test]
    fn test_parse_empty_string() {
        assert_eq!(
            presult_to_string(&sexpr(&mut "")),
            "Err(ContextError { context: [], cause: None })"
        );
    }

    #[test]
    fn test_parse_invalid_expression() {
        // TODO: Bug here in parser. It should be able to handle these cases.
        // assert_eq!(presult_to_string(&sexpr(&mut "1 +")), "Err(Backtrack(ContextError { context: [], cause: None }))");
        assert_eq!(
            presult_to_string(&sexpr(&mut "&& true")),
            "Err(ContextError { context: [], cause: None })"
        );
    }

    #[test]
    fn test_parse_boolean_expressions() {
        assert_eq!(
            presult_to_string(&sexpr(&mut "true && false")),
            "Ok(BinOp(Val(Bool(true)), Val(Bool(false)), BOp(And)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "true || false")),
            "Ok(BinOp(Val(Bool(true)), Val(Bool(false)), BOp(Or)))"
        );
    }

    #[test]
    fn test_parse_mixed_boolean_and_arithmetic() {
        // Expressions do not make sense but parser should allow it
        assert_eq!(
            presult_to_string(&sexpr(&mut "1 + 2 && 3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), Val(Int(3)), BOp(And)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "true || 1 * 2")),
            "Ok(BinOp(Val(Bool(true)), BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), BOp(Or)))"
        );
    }
    #[test]
    fn test_parse_string_concatenation() {
        assert_eq!(
            presult_to_string(&sexpr(&mut r#""foo" ++ "bar""#)),
            r#"Ok(BinOp(Val(Str("foo")), Val(Str("bar")), SOp(Concat)))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut r#""hello" ++ " " ++ "world""#)),
            r#"Ok(BinOp(BinOp(Val(Str("hello")), Val(Str(" ")), SOp(Concat)), Val(Str("world")), SOp(Concat)))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut r#""a" ++ "b" ++ "c""#)),
            r#"Ok(BinOp(BinOp(Val(Str("a")), Val(Str("b")), SOp(Concat)), Val(Str("c")), SOp(Concat)))"#
        );
    }

    #[test]
    fn test_parse_defer() {
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"defer(x)"#)),
            r#"Ok(Defer(Var(VarName::new("x"))))"#
        )
    }

    #[test]
    fn test_parse_update() {
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"update(x, y)"#)),
            r#"Ok(Update(Var(VarName::new("x")), Var(VarName::new("y"))))"#
        )
    }

    #[test]
    fn test_parse_default() {
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"default(x, 0)"#)),
            r#"Ok(Default(Var(VarName::new("x")), Val(Int(0))))"#
        )
    }

    #[test]
    fn test_parse_default_sexpr() {
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"default(x, y)"#)),
            r#"Ok(Default(Var(VarName::new("x")), Var(VarName::new("y"))))"#
        )
    }

    #[test]
    fn test_parse_list() {
        // Note: value_list has higher precedence than sexpr_list hence why
        // this becomes a val
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"List()"#)),
            r#"Ok(Val(List([])))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"List () "#)),
            r#"Ok(Val(List([])))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"List(1,2)"#)),
            r#"Ok(Val(List([Int(1), Int(2)])))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"List(1+2,2*5)"#)),
            r#"Ok(List([BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), BinOp(Val(Int(2)), Val(Int(5)), NOp(Mul))]))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"List("hello","world")"#)),
            r#"Ok(Val(List([Str("hello"), Str("world")])))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"List(true || false, true && false)"#)),
            r#"Ok(List([BinOp(Val(Bool(true)), Val(Bool(false)), BOp(Or)), BinOp(Val(Bool(true)), Val(Bool(false)), BOp(And))]))"#
        );
        // Can mix expressions - not that it is necessarily a good idea
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"List(1,"hello")"#)),
            r#"Ok(Val(List([Int(1), Str("hello")])))"#
        );
        assert_eq!(
            var_decl(&mut "y = List()"),
            Ok(("y".into(), SExpr::Val(Value::List(vec![].into()))))
        )
    }

    #[test]
    fn test_parse_lindex() {
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"List.get(List(1, 2), 42)"#)),
            r#"Ok(LIndex(Val(List([Int(1), Int(2)])), Val(Int(42))))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"List.get(x, 42)"#)),
            r#"Ok(LIndex(Var(VarName::new("x")), Val(Int(42))))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"List.get(x, 1+2)"#)),
            r#"Ok(LIndex(Var(VarName::new("x")), BinOp(Val(Int(1)), Val(Int(2)), NOp(Add))))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(
                &mut r#"List.get(List.get(List(List(1, 2), List(3, 4)), 0), 1)"#
            )),
            r#"Ok(LIndex(LIndex(Val(List([List([Int(1), Int(2)]), List([Int(3), Int(4)])])), Val(Int(0))), Val(Int(1))))"#
        );
    }

    #[test]
    fn test_parse_lconcat() {
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"List.concat(List(1, 2), List(3, 4))"#)),
            r#"Ok(LConcat(Val(List([Int(1), Int(2)])), Val(List([Int(3), Int(4)]))))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"List.concat(List(), List())"#)),
            r#"Ok(LConcat(Val(List([])), Val(List([]))))"#
        );
    }

    #[test]
    fn test_parse_lappend() {
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"List.append(List(1, 2), 3)"#)),
            r#"Ok(LAppend(Val(List([Int(1), Int(2)])), Val(Int(3))))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"List.append(List(), 3)"#)),
            r#"Ok(LAppend(Val(List([])), Val(Int(3))))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"List.append(List(), x)"#)),
            r#"Ok(LAppend(Val(List([])), Var(VarName::new("x"))))"#
        );
    }

    #[test]
    fn test_parse_lhead() {
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"List.head(List(1, 2))"#)),
            r#"Ok(LHead(Val(List([Int(1), Int(2)]))))"#
        );
        // Ok for parser but will result in runtime error:
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"List.head(List())"#)),
            r#"Ok(LHead(Val(List([]))))"#
        );
    }

    #[test]
    fn test_parse_ltail() {
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"List.tail(List(1, 2))"#)),
            r#"Ok(LTail(Val(List([Int(1), Int(2)]))))"#
        );
        // Ok for parser but will result in runtime error:
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"List.tail(List())"#)),
            r#"Ok(LTail(Val(List([]))))"#
        );
    }

    fn counter_inf() -> (&'static str, &'static str) {
        (
            "out z\nz = z[-1, 0] + 1",
            r#"Ok(LOLASpecification { input_vars: [], output_vars: [VarName::new("z")], exprs: {VarName::new("z"): BinOp(SIndex(Var(VarName::new("z")), -1, Int(0)), Val(Int(1)), NOp(Add))}, type_annotations: {} })"#,
        )
    }

    fn counter() -> (&'static str, &'static str) {
        (
            "in x\nout z\nz = z[-1, 0] + x",
            "Ok(LOLASpecification { input_vars: [VarName::new(\"x\")], output_vars: [VarName::new(\"z\")], exprs: {VarName::new(\"z\"): BinOp(SIndex(Var(VarName::new(\"z\")), -1, Int(0)), Var(VarName::new(\"x\")), NOp(Add))}, type_annotations: {} })",
        )
    }

    fn future() -> (&'static str, &'static str) {
        (
            "in x\nin y\nout z\nout a\nz = x[1, 0]\na = y",
            "Ok(LOLASpecification { input_vars: [VarName::new(\"x\"), VarName::new(\"y\")], output_vars: [VarName::new(\"z\"), VarName::new(\"a\")], exprs: {VarName::new(\"a\"): Var(VarName::new(\"y\")), VarName::new(\"z\"): SIndex(Var(VarName::new(\"x\")), 1, Int(0))}, type_annotations: {} })",
        )
    }

    fn list() -> (&'static str, &'static str) {
        (
            "in iList\nout oList\nout nestedList\nout listIndex\nout listAppend\nout listConcat\nout listHead\nout listTail\noList = iList\nnestedList = List(iList, iList)\nlistIndex = List.get(iList, 0)\nlistAppend = List.append(iList, (1+1)/2)\nlistConcat = List.concat(iList, iList)\nlistHead = List.head(iList)\nlistTail = List.tail(iList)",
            "Ok(LOLASpecification { input_vars: [VarName::new(\"iList\")], output_vars: [VarName::new(\"oList\"), VarName::new(\"nestedList\"), VarName::new(\"listIndex\"), VarName::new(\"listAppend\"), VarName::new(\"listConcat\"), VarName::new(\"listHead\"), VarName::new(\"listTail\")], exprs: {VarName::new(\"listAppend\"): LAppend(Var(VarName::new(\"iList\")), BinOp(BinOp(Val(Int(1)), Val(Int(1)), NOp(Add)), Val(Int(2)), NOp(Div))), VarName::new(\"listConcat\"): LConcat(Var(VarName::new(\"iList\")), Var(VarName::new(\"iList\"))), VarName::new(\"listHead\"): LHead(Var(VarName::new(\"iList\"))), VarName::new(\"listIndex\"): LIndex(Var(VarName::new(\"iList\")), Val(Int(0))), VarName::new(\"listTail\"): LTail(Var(VarName::new(\"iList\"))), VarName::new(\"nestedList\"): List([Var(VarName::new(\"iList\")), Var(VarName::new(\"iList\"))]), VarName::new(\"oList\"): Var(VarName::new(\"iList\"))}, type_annotations: {} })",
        )
    }

    fn simple_add_typed() -> (&'static str, &'static str) {
        (
            "in x: Int\nin y: Int\nout z: Int\nz = x + y",
            "Ok(LOLASpecification { input_vars: [VarName::new(\"x\"), VarName::new(\"y\")], output_vars: [VarName::new(\"z\")], exprs: {VarName::new(\"z\"): BinOp(Var(VarName::new(\"x\")), Var(VarName::new(\"y\")), NOp(Add))}, type_annotations: {VarName::new(\"x\"): Int, VarName::new(\"y\"): Int, VarName::new(\"z\"): Int} })",
        )
    }

    fn simple_add_typed_start_and_end_comment() -> (&'static str, &'static str) {
        (
            "// Begin\nin x: Int\nin y: Int\nout z: Int\nz = x + y// End",
            "Ok(LOLASpecification { input_vars: [VarName::new(\"x\"), VarName::new(\"y\")], output_vars: [VarName::new(\"z\")], exprs: {VarName::new(\"z\"): BinOp(Var(VarName::new(\"x\")), Var(VarName::new(\"y\")), NOp(Add))}, type_annotations: {VarName::new(\"x\"): Int, VarName::new(\"y\"): Int, VarName::new(\"z\"): Int} })",
        )
    }

    fn if_statement() -> (&'static str, &'static str) {
        (
            "in x\nin y\nout z\nz = if x == 0 then y else 42",
            "Ok(LOLASpecification { input_vars: [VarName::new(\"x\"), VarName::new(\"y\")], output_vars: [VarName::new(\"z\")], exprs: {VarName::new(\"z\"): If(BinOp(Var(VarName::new(\"x\")), Val(Int(0)), COp(Eq)), Var(VarName::new(\"y\")), Val(Int(42)))}, type_annotations: {} })",
        )
    }

    fn if_statement_newlines() -> (&'static str, &'static str) {
        (
            "in x\nin y\nout z\nz = if\nx == 0\nthen\ny\n else\n42",
            "Ok(LOLASpecification { input_vars: [VarName::new(\"x\"), VarName::new(\"y\")], output_vars: [VarName::new(\"z\")], exprs: {VarName::new(\"z\"): If(BinOp(Var(VarName::new(\"x\")), Val(Int(0)), COp(Eq)), Var(VarName::new(\"y\")), Val(Int(42)))}, type_annotations: {} })",
        )
    }

    fn function_name<T>(_: T) -> &'static str {
        std::any::type_name::<T>()
    }

    fn specs() -> Vec<(&'static str, (&'static str, &'static str))> {
        // Unfortunately, can't iterate because that converts them to general function pointers
        // instead of strong types
        Vec::from([
            (function_name(counter), counter()),
            (function_name(counter_inf), counter_inf()),
            (function_name(future), future()),
            (function_name(list), list()),
            (function_name(simple_add_typed), simple_add_typed()),
            (
                function_name(simple_add_typed_start_and_end_comment),
                simple_add_typed_start_and_end_comment(),
            ),
            (function_name(if_statement), if_statement()),
            (
                function_name(if_statement_newlines),
                if_statement_newlines(),
            ),
        ])
    }

    #[test]
    fn test_lola_specs_normal() {
        for &(name, (mut spec, exp)) in specs().iter() {
            let parsed = presult_to_string(&lola_specification(&mut spec));
            assert_eq!(
                format!("{}: {}", name, parsed),
                format!("{}: {}", name, exp)
            );
        }
    }

    #[test]
    fn test_lola_specs_added_newlines() {
        for &(name, (spec, exp)) in specs().iter() {
            let spec = spec.replace("\n", "\n\n");
            let parsed = presult_to_string(&lola_specification(&mut spec.as_str()));
            assert_eq!(
                format!("{}: {}", name, parsed),
                format!("{}: {}", name, exp)
            );
        }
    }

    #[test]
    fn test_lola_specs_added_comments() {
        for &(name, (spec, exp)) in specs().iter() {
            let mod_spec = spec.replace("\n", "\n//This is a comment\n");
            let parsed = presult_to_string(&lola_specification(&mut mod_spec.as_str()));
            assert_eq!(
                format!("{}: {}", name, parsed),
                format!("{}: {}", name, exp)
            );
            let mod_spec = spec.replace("\n", "//This is a comment\n"); // Beginning \n
            let parsed = presult_to_string(&lola_specification(&mut mod_spec.as_str()));
            assert_eq!(
                format!("{}: {}", name, parsed),
                format!("{}: {}", name, exp)
            );
        }
    }
}
