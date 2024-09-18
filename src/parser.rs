use std::collections::BTreeMap;

use winnow::ascii::dec_uint;
use winnow::ascii::line_ending;
use winnow::combinator::*;
use winnow::token::literal;
use winnow::PResult;
use winnow::Parser;

use winnow::ascii::dec_int as integer;
// Be sure to exclude actual tokens from the ident parser
use winnow::ascii::alphanumeric0;
use winnow::ascii::alphanumeric1 as ident;
use winnow::ascii::space0 as whitespace;

use crate::ast::*;
use crate::core::{ConcreteStreamData, VarName};

// This is the top-level parser for LOLA expressions
pub fn lola_expression(s: &mut &str) -> PResult<SExpr<VarName>> {
    sexpr.parse_next(s)
}

// Sub-parsers for the different types of LOLA expressions
pub fn aexpr(s: &mut &str) -> PResult<SExpr<VarName>> {
    aminus.parse_next(s)
}

fn string<'a>(s: &mut &'a str) -> PResult<&'a str> {
    delimited('"', alphanumeric0, '"').parse_next(s)
}

fn streamdata(s: &mut &str) -> PResult<ConcreteStreamData> {
    delimited(
        whitespace,
        alt((
            integer.map(ConcreteStreamData::Int),
            string.map(|s: &str| ConcreteStreamData::Str(s.into())),
            literal("true").map(|_| ConcreteStreamData::Bool(true)),
            literal("false").map(|_| ConcreteStreamData::Bool(false)),
            literal("unit").map(|_| ConcreteStreamData::Unit),
        )),
        whitespace,
    )
    .parse_next(s)
}

fn aval(s: &mut &str) -> PResult<SExpr<VarName>> {
    streamdata.map(SExpr::Val).parse_next(s)
}

fn avar(s: &mut &str) -> PResult<SExpr<VarName>> {
    ident
        .map(|w: &str| SExpr::Var(VarName(w.into())))
        .parse_next(s)
}

fn aeval(s: &mut &str) -> PResult<SExpr<VarName>> {
    seq!((
        _: whitespace,
        _: literal("eval"),
        _: whitespace,
        paren_aexpr,
        _: whitespace,
    ))
    .map(|(x,)| SExpr::Eval(Box::new(x)))
    .parse_next(s)
}

fn aatom(s: &mut &str) -> PResult<SExpr<VarName>> {
    delimited(
        whitespace,
        alt((sindex, aval, aeval, avar, paren_aexpr)),
        whitespace,
    )
    .parse_next(s)
}

fn aplus_raw(s: &mut &str) -> PResult<SExpr<VarName>> {
    separated_foldr1(amult, "+", |x, _, y| SExpr::Plus(Box::new(x), Box::new(y))).parse_next(s)
}

fn aplus(s: &mut &str) -> PResult<SExpr<VarName>> {
    delimited(whitespace, alt((aplus_raw, amult)), whitespace).parse_next(s)
}

fn amult_raw(s: &mut &str) -> PResult<SExpr<VarName>> {
    separated_foldr1(aatom, "*", |x, _, y| SExpr::Mult(Box::new(x), Box::new(y))).parse_next(s)
}

fn amult(s: &mut &str) -> PResult<SExpr<VarName>> {
    delimited(whitespace, alt((amult_raw, aatom)), whitespace).parse_next(s)
}

fn aminus_raw(s: &mut &str) -> PResult<SExpr<VarName>> {
    separated_foldl1(aplus, "-", |x, _, y| SExpr::Minus(Box::new(x), Box::new(y))).parse_next(s)
}

fn aminus(s: &mut &str) -> PResult<SExpr<VarName>> {
    delimited(whitespace, alt((aminus_raw, aplus)), whitespace).parse_next(s)
}

fn paren_aexpr(s: &mut &str) -> PResult<SExpr<VarName>> {
    delimited('(', aexpr, ')').parse_next(s)
}

fn btrue(s: &mut &str) -> PResult<BExpr<VarName>> {
    delimited(whitespace, literal("true"), whitespace)
        .map(|_| BExpr::Val(true))
        .parse_next(s)
}

fn bfalse(s: &mut &str) -> PResult<BExpr<VarName>> {
    delimited(whitespace, literal("false"), whitespace)
        .map(|_| BExpr::Val(false))
        .parse_next(s)
}

fn beq(s: &mut &str) -> PResult<BExpr<VarName>> {
    seq!((
        _: whitespace,
        aexpr,
        _: literal('='),
        aexpr,
        _: whitespace,
    ))
    .map(|(a1, a2)| BExpr::Eq(Box::new(a1), Box::new(a2)))
    .parse_next(s)
}

fn ble(s: &mut &str) -> PResult<BExpr<VarName>> {
    seq!((
        _: whitespace,
        aexpr,
        _: literal("<="),
        aexpr,
        _: whitespace,
    ))
    .map(|(a1, a2)| BExpr::Le(Box::new(a1), Box::new(a2)))
    .parse_next(s)
}

fn bnot(s: &mut &str) -> PResult<BExpr<VarName>> {
    seq!((
        _: whitespace,
        literal('!'),
        bexpr,
        _: whitespace,
    ))
    .map(|(_, b)| BExpr::Not(Box::new(b)))
    .parse_next(s)
}

fn band_raw(s: &mut &str) -> PResult<BExpr<VarName>> {
    separated_foldr1(bexpr, "&&", |b1, _, b2| {
        BExpr::And(Box::new(b1), Box::new(b2))
    })
    .parse_next(s)
}

fn band(s: &mut &str) -> PResult<BExpr<VarName>> {
    delimited(whitespace, band_raw, whitespace).parse_next(s)
}

fn bor_raw(s: &mut &str) -> PResult<BExpr<VarName>> {
    separated_foldr1(bexpr, "||", |b1, _, b2| {
        BExpr::Or(Box::new(b1), Box::new(b2))
    })
    .parse_next(s)
}

fn bor(s: &mut &str) -> PResult<BExpr<VarName>> {
    delimited(whitespace, bor_raw, whitespace).parse_next(s)
}

fn bexpr(s: &mut &str) -> PResult<BExpr<VarName>> {
    delimited(
        whitespace,
        alt((btrue, bfalse, band, bor, beq, ble, bnot)),
        whitespace,
    )
    .parse_next(s)
}

fn sif(s: &mut &str) -> PResult<SExpr<VarName>> {
    seq!((
        _: whitespace,
        _: "if",
        _: whitespace,
        bexpr,
        _: whitespace,
        _: "then",
        _: whitespace,
        sexpr,
        _: whitespace,
        _: "else",
        _: whitespace,
        sexpr,
        _: whitespace,
    ))
    .map(|(b, s1, s2)| SExpr::If(Box::new(b), Box::new(s1), Box::new(s2)))
    .parse_next(s)
}

fn sindex(s: &mut &str) -> PResult<SExpr<VarName>> {
    seq!((
        _: whitespace,
        paren_sexpr,
        _: whitespace,
        _: "[",
        _: whitespace,
        integer,
        _: whitespace,
        _: ",",
        streamdata,
        _: "]",
        _: whitespace,
    ))
    .map(|(s, i, c)| SExpr::Index(Box::new(s), i, c))
    .parse_next(s)
}

fn paren_sexpr(s: &mut &str) -> PResult<SExpr<VarName>> {
    delimited('(', sexpr, ')').parse_next(s)
}

fn sexpr(s: &mut &str) -> PResult<SExpr<VarName>> {
    alt((sif, aexpr)).parse_next(s)
}

fn input_decl(s: &mut &str) -> PResult<VarName> {
    seq!((
        _: whitespace,
        _: literal("in"),
        _: whitespace,
        ident,
        _: whitespace,
    ))
    .map(|(name,): (&str,)| VarName(name.into()))
    .parse_next(s)
}

fn linebreak(s: &mut &str) -> PResult<()> {
    delimited(whitespace, line_ending, whitespace)
        .map(|_| ())
        .parse_next(s)
}

fn input_decls(s: &mut &str) -> PResult<Vec<VarName>> {
    separated(0.., input_decl, linebreak).parse_next(s)
}

fn output_decl(s: &mut &str) -> PResult<VarName> {
    seq!((
        _: whitespace,
        _: literal("out"),
        _: whitespace,
        ident,
        _: whitespace,
    ))
    .map(|(name,): (&str,)| VarName(name.into()))
    .parse_next(s)
}

fn output_decls(s: &mut &str) -> PResult<Vec<VarName>> {
    separated(0.., output_decl, linebreak).parse_next(s)
}

fn expr_decl(s: &mut &str) -> PResult<(VarName, SExpr<VarName>)> {
    seq!((
        _: whitespace,
        ident,
        _: whitespace,
        _: literal("="),
        _: whitespace,
        sexpr,
        _: whitespace,
    ))
    .map(|(name, expr)| (VarName(name.into()), expr))
    .parse_next(s)
}

fn expr_decls(s: &mut &str) -> PResult<Vec<(VarName, SExpr<VarName>)>> {
    separated(0.., expr_decl, linebreak).parse_next(s)
}

pub fn lola_specification(s: &mut &str) -> PResult<LOLASpecification> {
    seq!((
        _: whitespace,
        input_decls,
        _: alt((linebreak.void(), empty)),
        output_decls,
        _: alt((linebreak.void(), empty)),
        expr_decls,
        _: whitespace,
    ))
    .map(|(input_vars, output_vars, exprs)| LOLASpecification {
        input_vars,
        output_vars,
        exprs: exprs.into_iter().collect(),
    })
    .parse_next(s)
}

pub fn value_assignment(s: &mut &str) -> PResult<(VarName, ConcreteStreamData)> {
    seq!((
        _: whitespace,
        ident,
        _: whitespace,
        _: literal("="),
        _: whitespace,
        streamdata,
        _: whitespace,
    ))
    .map(|(name, value)| (VarName(name.into()), value))
    .parse_next(s)
}

pub fn value_assignments(s: &mut &str) -> PResult<BTreeMap<VarName, ConcreteStreamData>> {
    seq!((
        separated(0.., value_assignment, linebreak),
        _: alt((linebreak.void(), empty)),
    ))
    .map(|(x,)| x)
    .parse_next(s)
}

pub fn time_stamped_assignments(
    s: &mut &str,
) -> PResult<(usize, BTreeMap<VarName, ConcreteStreamData>)> {
    seq!((
        _: whitespace,
        dec_uint,
        _: whitespace,
        _: literal(":"),
        _: separated(0.., whitespace, linebreak).map(|_: Vec<_>| ()),
        value_assignments
    ))
    .map(|(time, assignments)| (time, assignments))
    .parse_next(s)
}

pub fn timed_assignments(
    s: &mut &str,
) -> PResult<InputFileData> {
    repeat(0.., time_stamped_assignments).parse_next(s)
}

#[cfg(test)]
mod tests {
    use winnow::error::{ContextError, ErrMode};

    use super::*;

    #[test]
    fn test_streamdata() {
        assert_eq!(
            streamdata(&mut (*"42".to_string()).into()),
            Ok(ConcreteStreamData::Int(42)),
        );
        assert_eq!(
            streamdata(&mut (*"\"abc2d\"".to_string()).into()),
            Ok(ConcreteStreamData::Str("abc2d".to_string())),
        );
        assert_eq!(
            streamdata(&mut (*"true".to_string()).into()),
            Ok(ConcreteStreamData::Bool(true)),
        );
        assert_eq!(
            streamdata(&mut (*"false".to_string()).into()),
            Ok(ConcreteStreamData::Bool(false)),
        );
    }

    #[test]
    fn test_sexpr() -> Result<(), ErrMode<ContextError>> {
        assert_eq!(
            sexpr(&mut (*"1 + 2".to_string()).into())?,
            SExpr::Plus(
                Box::new(SExpr::Val(ConcreteStreamData::Int(1))),
                Box::new(SExpr::Val(ConcreteStreamData::Int(2))),
            ),
        );
        assert_eq!(
            sexpr(&mut (*"1 + 2 * 3".to_string()).into())?,
            SExpr::Plus(
                Box::new(SExpr::Val(ConcreteStreamData::Int(1))),
                Box::new(SExpr::Mult(
                    Box::new(SExpr::Val(ConcreteStreamData::Int(2))),
                    Box::new(SExpr::Val(ConcreteStreamData::Int(3))),
                )),
            ),
        );
        assert_eq!(
            sexpr(&mut (*"x + (y + 2)".to_string()).into())?,
            SExpr::Plus(
                Box::new(SExpr::Var(VarName("x".into()))),
                Box::new(SExpr::Plus(
                    Box::new(SExpr::Var(VarName("y".into()))),
                    Box::new(SExpr::Val(ConcreteStreamData::Int(2))),
                )),
            ),
        );
        assert_eq!(
            sexpr(&mut (*"if true then 1 else 2".to_string()).into())?,
            SExpr::If(
                Box::new(BExpr::Val(true)),
                Box::new(SExpr::Val(ConcreteStreamData::Int(1))),
                Box::new(SExpr::Val(ConcreteStreamData::Int(2))),
            ),
        );
        assert_eq!(
            sexpr(&mut (*"(x)[-1, 0]".to_string()).into())?,
            SExpr::Index(
                Box::new(SExpr::Var(VarName("x".into()))),
                -1,
                ConcreteStreamData::Int(0),
            ),
        );
        assert_eq!(
            sexpr(&mut (*"(x + y)[-3, 2]".to_string()).into())?,
            SExpr::Index(
                Box::new(SExpr::Plus(
                    Box::new(SExpr::Var(VarName("x".into()))),
                    Box::new(SExpr::Var(VarName("y".into())),)
                )),
                -3,
                ConcreteStreamData::Int(2),
            ),
        );
        assert_eq!(
            sexpr(&mut (*"1 + (x)[-1, 0]".to_string()).into())?,
            SExpr::Plus(
                Box::new(SExpr::Val(ConcreteStreamData::Int(1))),
                Box::new(SExpr::Index(
                    Box::new(SExpr::Var(VarName("x".into()))),
                    -1,
                    ConcreteStreamData::Int(0),
                ),)
            )
        );
        Ok(())
    }

    #[test]
    fn test_input_decl() -> Result<(), ErrMode<ContextError>> {
        assert_eq!(
            input_decl(&mut (*"in x".to_string()).into())?,
            VarName("x".into()),
        );
        Ok(())
    }

    #[test]
    fn test_input_decls() -> Result<(), ErrMode<ContextError>> {
        assert_eq!(input_decls(&mut (*"".to_string()).into())?, vec![],);
        assert_eq!(
            input_decls(&mut (*"in x".to_string()).into())?,
            vec![VarName("x".into())],
        );
        assert_eq!(
            input_decls(&mut (*"in x\nin y".to_string()).into())?,
            vec![VarName("x".into()), VarName("y".into())],
        );
        Ok(())
    }

    #[test]
    fn test_parse_lola_simple_add() -> Result<(), ErrMode<ContextError>> {
        let input = "\
            in x\n\
            in y\n\
            out z\n\
            z = x + y";
        let simple_add_spec = LOLASpecification {
            input_vars: vec![VarName("x".into()), VarName("y".into())],
            output_vars: vec![VarName("z".into())],
            exprs: vec![(
                VarName("z".into()),
                SExpr::Plus(
                    Box::new(SExpr::Var(VarName("x".into()))),
                    Box::new(SExpr::Var(VarName("y".into()))),
                ),
            )]
            .into_iter()
            .collect(),
        };
        assert_eq!(lola_specification(&mut (*input).into())?, simple_add_spec);
        Ok(())
    }

    #[test]
    fn test_parse_lola_count() -> Result<(), ErrMode<ContextError>> {
        let input = "\
            out x\n\
            x = 1 + (x)[-1, 0]";
        let count_spec = LOLASpecification {
            input_vars: vec![],
            output_vars: vec![VarName("x".into())],
            exprs: vec![(
                VarName("x".into()),
                SExpr::Plus(
                    Box::new(SExpr::Val(ConcreteStreamData::Int(1))),
                    Box::new(SExpr::Index(
                        Box::new(SExpr::Var(VarName("x".into()))),
                        -1,
                        ConcreteStreamData::Int(0),
                    )),
                ),
            )]
            .into_iter()
            .collect(),
        };
        assert_eq!(lola_specification(&mut (*input).into())?, count_spec);
        Ok(())
    }

    #[test]
    fn test_parse_lola_eval() -> Result<(), ErrMode<ContextError>> {
        let input = "\
            in x\n\
            in y\n\
            in s\n\
            out z\n\
            out w\n\
            z = x + y\n\
            w = eval(s)";
        let eval_spec = LOLASpecification {
            input_vars: vec![
                VarName("x".into()),
                VarName("y".into()),
                VarName("s".into()),
            ],
            output_vars: vec![VarName("z".into()), VarName("w".into())],
            exprs: vec![
                (
                    VarName("z".into()),
                    SExpr::Plus(
                        Box::new(SExpr::Var(VarName("x".into()))),
                        Box::new(SExpr::Var(VarName("y".into()))),
                    ),
                ),
                (
                    VarName("w".into()),
                    SExpr::Eval(Box::new(SExpr::Var(VarName("s".into())))),
                ),
            ]
            .into_iter()
            .collect(),
        };
        assert_eq!(lola_specification(&mut (*input).into())?, eval_spec);
        Ok(())
    }

    #[test]
    fn test_value_assignment() -> Result<(), ErrMode<ContextError>> {
        assert_eq!(
            value_assignment(&mut (*"x = 42".to_string()).into())?,
            (VarName("x".into()), ConcreteStreamData::Int(42)),
        );
        assert_eq!(
            value_assignment(&mut (*"y = 3".to_string()).into())?,
            (VarName("y".into()), ConcreteStreamData::Int(3)),
        );
        Ok(())
    }

    #[test]
    fn test_value_assignments() -> Result<(), ErrMode<ContextError>> {
        assert_eq!(
            value_assignments(&mut (*"x = 42\ny = 3".to_string()).into())?,
            vec![
                (VarName("x".into()), ConcreteStreamData::Int(42)),
                (VarName("y".into()), ConcreteStreamData::Int(3)),
            ]
            .into_iter()
            .collect(),
        );
        assert_eq!(
            value_assignments(&mut (*"".to_string()).into())?,
            BTreeMap::new(),
        );
        Ok(())
    }

    #[test]
    fn test_time_stamped_assignment() -> Result<(), ErrMode<ContextError>> {
        assert_eq!(
            time_stamped_assignments(&mut (*"0: x = 42".to_string()).into())?,
            (
                0,
                vec![(VarName("x".into()), ConcreteStreamData::Int(42))]
                    .into_iter()
                    .collect()
            ),
        );
        assert_eq!(
            time_stamped_assignments(&mut (*"1: x = 42\ny = 3".to_string()).into())?,
            (
                1,
                vec![
                    (VarName("x".into()), ConcreteStreamData::Int(42)),
                    (VarName("y".into()), ConcreteStreamData::Int(3))
                ]
                .into_iter()
                .collect()
            ),
        );
        assert_eq!(
            time_stamped_assignments(&mut (*"2:\n x = 42\ny = 3".to_string()).into())?,
            (
                2,
                vec![
                    (VarName("x".into()), ConcreteStreamData::Int(42)),
                    (VarName("y".into()), ConcreteStreamData::Int(3))
                ]
                .into_iter()
                .collect()
            ),
        );
        Ok(())
    }
}
