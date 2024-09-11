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
use crate::core::StreamData;
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

fn aatom(s: &mut &str) -> PResult<SExpr<VarName>> {
    delimited(whitespace, alt((aval, avar, paren_aexpr)), whitespace).parse_next(s)
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
    delimited(whitespace, alt((amult_raw, paren_aexpr, aval)), whitespace).parse_next(s)
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
    alt((sif, sindex, paren_sexpr, aexpr)).parse_next(s)
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
        Ok(())
    }
}
