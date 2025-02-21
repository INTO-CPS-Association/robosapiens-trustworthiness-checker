use winnow::ascii::multispace0;
use winnow::combinator::*;
use winnow::token::literal;
use winnow::PResult;
use winnow::Parser;

use super::super::core::parser::*;
use super::ast::*;
use crate::core::StreamType;
use crate::core::VarName;

// This is the top-level parser for LOLA expressions
pub fn lola_expression(s: &mut &str) -> PResult<SExpr<VarName>> {
    sexpr.parse_next(s)
}

fn paren(s: &mut &str) -> PResult<SExpr<VarName>> {
    delimited('(', sexpr, ')').parse_next(s)
}

// Used for Lists in output streams
fn sexpr_list(s: &mut &str) -> PResult<SExpr<VarName>> {
    let res = delimited(
        seq!("List", whitespace, '('),
        separated(0.., sexpr, seq!(whitespace, ',', whitespace)),
        ')',
    )
    .parse_next(s);
    match res {
        Ok(exprs) => Ok(SExpr::List(exprs)),
        Err(e) => Err(e),
    }
}

fn var(s: &mut &str) -> PResult<SExpr<VarName>> {
    ident
        .map(|name: &str| SExpr::Var(name.into()))
        .parse_next(s)
}

fn lit(s: &mut &str) -> PResult<SExpr<VarName>> {
    val.map(|v| SExpr::Val(v)).parse_next(s)
}

fn sindex(s: &mut &str) -> PResult<SExpr<VarName>> {
    seq!(
        _: whitespace,
        alt((lit, var, paren)),
        _: whitespace,
        _: '[',
        _: whitespace,
        integer,
        _: whitespace,
        _: ',',
        _: whitespace,
        val,
        _: whitespace,
        _: ']'
    )
    .map(|(e, i, d)| SExpr::SIndex(Box::new(e), i, d))
    .parse_next(s)
}

fn ifelse(s: &mut &str) -> PResult<SExpr<VarName>> {
    seq!((
        _: whitespace,
        _: "if",
        _: whitespace,
        sexpr,
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

fn defer(s: &mut &str) -> PResult<SExpr<VarName>> {
    seq!((
        _: whitespace,
        _: literal("defer"),
        _: '(',
        _: whitespace,
        sexpr,
        _: whitespace,
        _: ')',
    ))
    .map(|(e,)| SExpr::Defer(Box::new(e)))
    .parse_next(s)
}

fn update(s: &mut &str) -> PResult<SExpr<VarName>> {
    seq!((
        _: whitespace,
        _: literal("update"),
        _: '(',
        _: whitespace,
        sexpr,
        _: whitespace,
        _: ',',
        _: whitespace,
        sexpr,
        _: whitespace,
        _: ')',
    ))
    .map(|(lhs, rhs)| SExpr::Update(Box::new(lhs), Box::new(rhs)))
    .parse_next(s)
}

fn eval(s: &mut &str) -> PResult<SExpr<VarName>> {
    seq!((
        _: whitespace,
        _: "eval",
        _: whitespace,
        _: '(',
        _: whitespace,
        sexpr,
        _: whitespace,
        _: ')',
        _: whitespace,
    ))
    .map(|(e,)| SExpr::Eval(Box::new(e)))
    .parse_next(s)
}

fn not(s: &mut &str) -> PResult<SExpr<VarName>> {
    seq!((
        _: whitespace,
        _: "!",
        _: whitespace,
        atom,
        _: whitespace,
        _: whitespace,
    ))
    .map(|(e,)| SExpr::Not(Box::new(e)))
    .parse_next(s)
}

fn lindex(s: &mut &str) -> PResult<SExpr<VarName>> {
    seq!(
        _: whitespace,
        _: "List.get",
        _: whitespace,
        _: '(',
        _: whitespace,
        sexpr,
        _: whitespace,
        _: ',',
        _: whitespace,
        sexpr,
        _: whitespace,
        _: ')',
    )
    .map(|(e, i)| SExpr::LIndex(Box::new(e), Box::new(i)))
    .parse_next(s)
}

fn lappend(s: &mut &str) -> PResult<SExpr<VarName>> {
    seq!(
        _: whitespace,
        _: "List.append",
        _: whitespace,
        _: '(',
        _: whitespace,
        sexpr,
        _: whitespace,
        _: ',',
        _: whitespace,
        sexpr,
        _: whitespace,
        _: ')',
    )
    .map(|(lst, el)| SExpr::LAppend(Box::new(lst), Box::new(el)))
    .parse_next(s)
}

fn lconcat(s: &mut &str) -> PResult<SExpr<VarName>> {
    seq!(
        _: whitespace,
        _: "List.concat",
        _: whitespace,
        _: '(',
        _: whitespace,
        sexpr,
        _: whitespace,
        _: ',',
        _: whitespace,
        sexpr,
        _: whitespace,
        _: ')',
    )
    .map(|(lst1, lst2)| SExpr::LConcat(Box::new(lst1), Box::new(lst2)))
    .parse_next(s)
}

fn lhead(s: &mut &str) -> PResult<SExpr<VarName>> {
    seq!((
        _: whitespace,
        _: "List.head",
        _: whitespace,
        _: '(',
        _: whitespace,
        sexpr,
        _: whitespace,
        _: ')',
        _: whitespace,
    ))
    .map(|(lst,)| SExpr::LHead(Box::new(lst)))
    .parse_next(s)
}

fn ltail(s: &mut &str) -> PResult<SExpr<VarName>> {
    seq!((
        _: whitespace,
        _: "List.tail",
        _: whitespace,
        _: '(',
        _: whitespace,
        sexpr,
        _: whitespace,
        _: ')',
        _: whitespace,
    ))
    .map(|(lst,)| SExpr::LTail(Box::new(lst)))
    .parse_next(s)
}

/// Fundamental expressions of the language
fn atom(s: &mut &str) -> PResult<SExpr<VarName>> {
    delimited(
        whitespace,
        alt((
            sindex, lindex, lappend, lconcat, lhead, ltail, not, eval, lit, ifelse, defer, update,
            sexpr_list, var, paren,
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
            Div => Some(Le),
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
            Add => SBinOp::IOp(IntBinOp::Add),
            Sub => SBinOp::IOp(IntBinOp::Sub),
            Mul => SBinOp::IOp(IntBinOp::Mul),
            Div => SBinOp::IOp(IntBinOp::Div),
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
fn binary_op(current_op: BinaryPrecedences) -> impl FnMut(&mut &str) -> PResult<SExpr<VarName>> {
    move |s: &mut &str| {
        let next_parser_op = current_op.next();
        let mut next_parser: Box<dyn FnMut(&mut &str) -> PResult<SExpr<VarName>>> =
            match next_parser_op {
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

pub fn sexpr(s: &mut &str) -> PResult<SExpr<VarName>> {
    delimited(
        whitespace,
        binary_op(BinaryPrecedences::lowest_precedence()),
        whitespace,
    )
    .parse_next(s)
}

fn type_annotation(s: &mut &str) -> PResult<StreamType> {
    seq!((
        _: whitespace,
        _: literal(":"),
        _: whitespace,
        alt((literal("Int"), literal("Bool"), literal("Str"), literal("Unit"))),
        _: whitespace,
    ))
    .map(|(typ,)| match typ {
        "Int" => StreamType::Int,
        "Bool" => StreamType::Bool,
        "Str" => StreamType::Str,
        "Unit" => StreamType::Unit,
        _ => unreachable!(),
    })
    .parse_next(s)
}

fn input_decl(s: &mut &str) -> PResult<(VarName, Option<StreamType>)> {
    seq!((
        _: whitespace,
        _: literal("in"),
        _: whitespace,
        ident,
        opt(type_annotation),
        _: whitespace,
    ))
    .map(|(name, typ): (&str, _)| (VarName(name.into()), typ))
    .parse_next(s)
}

fn input_decls(s: &mut &str) -> PResult<Vec<(VarName, Option<StreamType>)>> {
    separated(0.., input_decl, linebreak).parse_next(s)
}

fn output_decl(s: &mut &str) -> PResult<(VarName, Option<StreamType>)> {
    seq!((
        _: whitespace,
        _: literal("out"),
        _: whitespace,
        ident,
        opt(type_annotation),
        _: whitespace,
    ))
    .map(|(name, typ): (&str, _)| (VarName(name.into()), typ))
    .parse_next(s)
}

fn output_decls(s: &mut &str) -> PResult<Vec<(VarName, Option<StreamType>)>> {
    separated(0.., output_decl, linebreak).parse_next(s)
}

fn var_decl(s: &mut &str) -> PResult<(VarName, SExpr<VarName>)> {
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

fn var_decls(s: &mut &str) -> PResult<Vec<(VarName, SExpr<VarName>)>> {
    separated(0.., var_decl, linebreak).parse_next(s)
}

pub fn lola_specification(s: &mut &str) -> PResult<LOLASpecification> {
    seq!((
        _: multispace0,
        input_decls,
        _: alt((linebreak.void(), empty)),
        output_decls,
        _: alt((linebreak.void(), empty)),
        var_decls,
        _: multispace0,
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

    use winnow::error::{ContextError, ErrMode};

    use super::*;
    use test_log::test;

    #[test]
    fn test_streamdata() {
        assert_eq!(val(&mut (*"42".to_string()).into()), Ok(Value::Int(42)),);
        assert_eq!(
            val(&mut (*"\"abc2d\"".to_string()).into()),
            Ok(Value::Str("abc2d".to_string())),
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
            Ok(Value::Str("x+y".to_string())),
        );
    }

    #[test]
    fn test_sexpr() -> Result<(), ErrMode<ContextError>> {
        assert_eq!(
            sexpr(&mut (*"1 + 2".to_string()).into())?,
            SExpr::BinOp(
                Box::new(SExpr::Val(Value::Int(1))),
                Box::new(SExpr::Val(Value::Int(2))),
                SBinOp::IOp(IntBinOp::Add),
            ),
        );
        assert_eq!(
            sexpr(&mut (*"1 + 2 * 3".to_string()).into())?,
            SExpr::BinOp(
                Box::new(SExpr::Val(Value::Int(1))),
                Box::new(SExpr::BinOp(
                    Box::new(SExpr::Val(Value::Int(2))),
                    Box::new(SExpr::Val(Value::Int(3))),
                    SBinOp::IOp(IntBinOp::Mul),
                )),
                SBinOp::IOp(IntBinOp::Add),
            ),
        );
        assert_eq!(
            sexpr(&mut (*"x + (y + 2)".to_string()).into())?,
            SExpr::BinOp(
                Box::new(SExpr::Var(VarName("x".into()))),
                Box::new(SExpr::BinOp(
                    Box::new(SExpr::Var(VarName("y".into()))),
                    Box::new(SExpr::Val(Value::Int(2))),
                    SBinOp::IOp(IntBinOp::Add),
                )),
                SBinOp::IOp(IntBinOp::Add),
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
            SExpr::SIndex(Box::new(SExpr::Var(VarName("x".into()))), -1, Value::Int(0),),
        );
        assert_eq!(
            sexpr(&mut (*"(x + y)[-3, 2]".to_string()).into())?,
            SExpr::SIndex(
                Box::new(SExpr::BinOp(
                    Box::new(SExpr::Var(VarName("x".into()))),
                    Box::new(SExpr::Var(VarName("y".into())),),
                    SBinOp::IOp(IntBinOp::Add),
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
                    Box::new(SExpr::Var(VarName("x".into()))),
                    -1,
                    Value::Int(0),
                ),),
                SBinOp::IOp(IntBinOp::Add),
            )
        );
        assert_eq!(
            sexpr(&mut (*"\"test\"".to_string()).into())?,
            SExpr::Val(Value::Str("test".to_string())),
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
    fn test_input_decl() -> Result<(), ErrMode<ContextError>> {
        assert_eq!(
            input_decl(&mut (*"in x".to_string()).into())?,
            (VarName("x".into()), None),
        );
        Ok(())
    }

    #[test]
    fn test_typed_input_decl() -> Result<(), ErrMode<ContextError>> {
        assert_eq!(
            input_decl(&mut (*"in x: Int".to_string()).into())?,
            (VarName("x".into()), Some(StreamType::Int)),
        );
        Ok(())
    }

    #[test]
    fn test_input_decls() -> Result<(), ErrMode<ContextError>> {
        assert_eq!(input_decls(&mut (*"".to_string()).into())?, vec![],);
        assert_eq!(
            input_decls(&mut (*"in x".to_string()).into())?,
            vec![(VarName("x".into()), None)],
        );
        assert_eq!(
            input_decls(&mut (*"in x\nin y".to_string()).into())?,
            vec![(VarName("x".into()), None), (VarName("y".into()), None)],
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
                SExpr::BinOp(
                    Box::new(SExpr::Var(VarName("x".into()))),
                    Box::new(SExpr::Var(VarName("y".into()))),
                    SBinOp::IOp(IntBinOp::Add),
                ),
            )]
            .into_iter()
            .collect(),
            type_annotations: BTreeMap::new(),
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
                SExpr::BinOp(
                    Box::new(SExpr::Val(Value::Int(1))),
                    Box::new(SExpr::SIndex(
                        Box::new(SExpr::Var(VarName("x".into()))),
                        -1,
                        Value::Int(0),
                    )),
                    SBinOp::IOp(IntBinOp::Add),
                ),
            )]
            .into_iter()
            .collect(),
            type_annotations: BTreeMap::new(),
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
                    SExpr::BinOp(
                        Box::new(SExpr::Var(VarName("x".into()))),
                        Box::new(SExpr::Var(VarName("y".into()))),
                        SBinOp::IOp(IntBinOp::Add),
                    ),
                ),
                (
                    VarName("w".into()),
                    SExpr::Eval(Box::new(SExpr::Var(VarName("s".into())))),
                ),
            ]
            .into_iter()
            .collect(),
            type_annotations: BTreeMap::new(),
        };
        assert_eq!(lola_specification(&mut (*input).into())?, eval_spec);
        Ok(())
    }

    #[test]
    fn test_iexpr() {
        // Add
        assert_eq!(presult_to_string(&sexpr(&mut "0")), "Ok(Val(Int(0)))");
        assert_eq!(
            presult_to_string(&sexpr(&mut "  1 +2  ")),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), IOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut " 1  + 2 +3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), IOp(Add)), Val(Int(3)), IOp(Add)))"
        );
        // Sub
        assert_eq!(
            presult_to_string(&sexpr(&mut "  1 -2  ")),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), IOp(Sub)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut " 1  - 2 -3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), IOp(Sub)), Val(Int(3)), IOp(Sub)))"
        );
        // Mul
        assert_eq!(
            presult_to_string(&sexpr(&mut "  1 *2  ")),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), IOp(Mul)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut " 1  * 2 *3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), IOp(Mul)), Val(Int(3)), IOp(Mul)))"
        );
        // Div
        assert_eq!(
            presult_to_string(&sexpr(&mut "  1 /2  ")),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), IOp(Div)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut " 1  / 2 /3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), IOp(Div)), Val(Int(3)), IOp(Div)))"
        );
        // Var
        assert_eq!(
            presult_to_string(&sexpr(&mut "  x  ")),
            r#"Ok(Var(VarName("x")))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "  xsss ")),
            r#"Ok(Var(VarName("xsss")))"#
        );
        // Time index
        assert_eq!(
            presult_to_string(&sexpr(&mut "x [-1, 0 ]")),
            r#"Ok(SIndex(Var(VarName("x")), -1, Int(0)))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "x[1,0]")),
            r#"Ok(SIndex(Var(VarName("x")), 1, Int(0)))"#
        );
        // Paren
        assert_eq!(presult_to_string(&sexpr(&mut "  (1)  ")), "Ok(Val(Int(1)))");
        // Don't care about order of eval; care about what the AST looks like
        assert_eq!(
            presult_to_string(&sexpr(&mut " 2 + (2 + 3)")),
            "Ok(BinOp(Val(Int(2)), BinOp(Val(Int(2)), Val(Int(3)), IOp(Add)), IOp(Add)))"
        );
        // If then else
        assert_eq!(
            presult_to_string(&sexpr(&mut "if true then 1 else 2")),
            "Ok(If(Val(Bool(true)), Val(Int(1)), Val(Int(2))))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "if true then x+x else y+y")),
            r#"Ok(If(Val(Bool(true)), BinOp(Var(VarName("x")), Var(VarName("x")), IOp(Add)), BinOp(Var(VarName("y")), Var(VarName("y")), IOp(Add))))"#
        );

        // ChatGPT generated tests with mixed arithmetic and parentheses iexprs. It only had knowledge of the tests above.
        // Basic mixed addition and multiplication
        assert_eq!(
            presult_to_string(&sexpr(&mut "1 + 2 * 3")),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), Val(Int(3)), IOp(Mul)), IOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "1 * 2 + 3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), IOp(Mul)), Val(Int(3)), IOp(Add)))"
        );
        // Mixed addition, subtraction, and multiplication
        assert_eq!(presult_to_string(&sexpr(&mut "1 + 2 * 3 - 4")),
                   "Ok(BinOp(BinOp(Val(Int(1)), BinOp(Val(Int(2)), Val(Int(3)), IOp(Mul)), IOp(Add)), Val(Int(4)), IOp(Sub)))");
        assert_eq!(presult_to_string(&sexpr(&mut "1 * 2 + 3 - 4")),
                   "Ok(BinOp(BinOp(BinOp(Val(Int(1)), Val(Int(2)), IOp(Mul)), Val(Int(3)), IOp(Add)), Val(Int(4)), IOp(Sub)))");
        // Mixed addition and division
        assert_eq!(
            presult_to_string(&sexpr(&mut "10 + 20 / 5")),
            "Ok(BinOp(Val(Int(10)), BinOp(Val(Int(20)), Val(Int(5)), IOp(Div)), IOp(Add)))"
        );
        // Nested parentheses with mixed operations
        assert_eq!(presult_to_string(&sexpr(&mut "(1 + 2) * (3 - 4)")),
                   "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), IOp(Add)), BinOp(Val(Int(3)), Val(Int(4)), IOp(Sub)), IOp(Mul)))");
        assert_eq!(presult_to_string(&sexpr(&mut "1 + (2 * (3 + 4))")),
                   "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), BinOp(Val(Int(3)), Val(Int(4)), IOp(Add)), IOp(Mul)), IOp(Add)))");
        // Complex nested expressions
        assert_eq!(presult_to_string(&sexpr(&mut "((1 + 2) * 3) + (4 / (5 - 6))")),
                   "Ok(BinOp(BinOp(BinOp(Val(Int(1)), Val(Int(2)), IOp(Add)), Val(Int(3)), IOp(Mul)), BinOp(Val(Int(4)), BinOp(Val(Int(5)), Val(Int(6)), IOp(Sub)), IOp(Div)), IOp(Add)))"
        );
        assert_eq!(presult_to_string(&sexpr(&mut "(1 + (2 * (3 - (4 / 5))))")),
                   "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), BinOp(Val(Int(3)), BinOp(Val(Int(4)), Val(Int(5)), IOp(Div)), IOp(Sub)), IOp(Mul)), IOp(Add)))"
        );
        // More complex expressions with deep nesting
        assert_eq!(presult_to_string(&sexpr(&mut "((1 + 2) * (3 + 4))")),
                   "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), IOp(Add)), BinOp(Val(Int(3)), Val(Int(4)), IOp(Add)), IOp(Mul)))"
        );
        assert_eq!(presult_to_string(&sexpr(&mut "((1 * 2) + (3 * 4)) / 5")),
                   "Ok(BinOp(BinOp(BinOp(Val(Int(1)), Val(Int(2)), IOp(Mul)), BinOp(Val(Int(3)), Val(Int(4)), IOp(Mul)), IOp(Add)), Val(Int(5)), IOp(Div)))"
        );
        // Multiple levels of nested expressions
        assert_eq!(presult_to_string(&sexpr(&mut "1 + (2 * (3 + (4 / (5 - 6))))")),
                   "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), BinOp(Val(Int(3)), BinOp(Val(Int(4)), BinOp(Val(Int(5)), Val(Int(6)), IOp(Sub)), IOp(Div)), IOp(Add)), IOp(Mul)), IOp(Add)))"
        );

        // ChatGPT generated tests with mixed iexprs. It only had knowledge of the tests above.
        // Mixing addition, subtraction, and variables
        assert_eq!(
            presult_to_string(&sexpr(&mut "x + 2 - y")),
            r#"Ok(BinOp(BinOp(Var(VarName("x")), Val(Int(2)), IOp(Add)), Var(VarName("y")), IOp(Sub)))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "(x + y) * 3")),
            r#"Ok(BinOp(BinOp(Var(VarName("x")), Var(VarName("y")), IOp(Add)), Val(Int(3)), IOp(Mul)))"#
        );
        // Nested arithmetic with variables and parentheses
        assert_eq!(
            presult_to_string(&sexpr(&mut "(a + b) / (c - d)")),
            r#"Ok(BinOp(BinOp(Var(VarName("a")), Var(VarName("b")), IOp(Add)), BinOp(Var(VarName("c")), Var(VarName("d")), IOp(Sub)), IOp(Div)))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "x * (y + 3) - z / 2")),
            r#"Ok(BinOp(BinOp(Var(VarName("x")), BinOp(Var(VarName("y")), Val(Int(3)), IOp(Add)), IOp(Mul)), BinOp(Var(VarName("z")), Val(Int(2)), IOp(Div)), IOp(Sub)))"#
        );
        // If-then-else with mixed arithmetic
        assert_eq!(presult_to_string(&sexpr(&mut "if true then 1 + 2 else 3 * 4")),
                   "Ok(If(Val(Bool(true)), BinOp(Val(Int(1)), Val(Int(2)), IOp(Add)), BinOp(Val(Int(3)), Val(Int(4)), IOp(Mul))))");
        // Time index in arithmetic expression
        assert_eq!(
            presult_to_string(&sexpr(&mut "x[0, 1] + y[-1, 0]")),
            r#"Ok(BinOp(SIndex(Var(VarName("x")), 0, Int(1)), SIndex(Var(VarName("y")), -1, Int(0)), IOp(Add)))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "x[1, 2] * (y + 3)")),
            r#"Ok(BinOp(SIndex(Var(VarName("x")), 1, Int(2)), BinOp(Var(VarName("y")), Val(Int(3)), IOp(Add)), IOp(Mul)))"#
        );
        // Complex expression with nested if-then-else and mixed operations
        assert_eq!(
            presult_to_string(&sexpr(&mut "(1 + x) * if y then 3 else z / 2")),
            r#"Ok(BinOp(BinOp(Val(Int(1)), Var(VarName("x")), IOp(Add)), If(Var(VarName("y")), Val(Int(3)), BinOp(Var(VarName("z")), Val(Int(2)), IOp(Div))), IOp(Mul)))"#
        );
    }

    #[test]
    fn test_var_decl() {
        assert_eq!(
            presult_to_string(&var_decl(&mut "x = 0")),
            r#"Ok((VarName("x"), Val(Int(0))))"#
        );
        assert_eq!(
            presult_to_string(&var_decl(&mut r#"x = "hello""#)),
            r#"Ok((VarName("x"), Val(Str("hello"))))"#
        );
        assert_eq!(
            presult_to_string(&var_decl(&mut "x = true")),
            r#"Ok((VarName("x"), Val(Bool(true))))"#
        );
        assert_eq!(
            presult_to_string(&var_decl(&mut "x = false")),
            r#"Ok((VarName("x"), Val(Bool(false))))"#
        );
    }

    #[test]
    fn parse_empty_string() {
        assert_eq!(
            presult_to_string(&sexpr(&mut "")),
            "Err(Backtrack(ContextError { context: [], cause: None }))"
        );
    }

    #[test]
    fn parse_invalid_expression() {
        // TODO: Bug here in parser. It should be able to handle these cases.
        // assert_eq!(presult_to_string(&sexpr(&mut "1 +")), "Err(Backtrack(ContextError { context: [], cause: None }))");
        assert_eq!(
            presult_to_string(&sexpr(&mut "&& true")),
            "Err(Backtrack(ContextError { context: [], cause: None }))"
        );
    }

    #[test]
    fn parse_boolean_expressions() {
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
    fn parse_mixed_boolean_and_arithmetic() {
        // Expressions do not make sense but parser should allow it
        assert_eq!(
            presult_to_string(&sexpr(&mut "1 + 2 && 3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), IOp(Add)), Val(Int(3)), BOp(And)))"
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut "true || 1 * 2")),
            "Ok(BinOp(Val(Bool(true)), BinOp(Val(Int(1)), Val(Int(2)), IOp(Mul)), BOp(Or)))"
        );
    }
    #[test]
    fn parse_string_concatenation() {
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
    fn parse_defer() {
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"defer(x)"#)),
            r#"Ok(Defer(Var(VarName("x"))))"#
        )
    }

    #[test]
    fn parse_update() {
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"update(x, y)"#)),
            r#"Ok(Update(Var(VarName("x")), Var(VarName("y"))))"#
        )
    }

    #[test]
    fn parse_list() {
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
            r#"Ok(List([BinOp(Val(Int(1)), Val(Int(2)), IOp(Add)), BinOp(Val(Int(2)), Val(Int(5)), IOp(Mul))]))"#
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
            Ok((VarName("y".into()), SExpr::Val(Value::List(vec![]))))
        )
    }

    #[test]
    fn parse_lindex() {
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"List.get(List(1, 2), 42)"#)),
            r#"Ok(LIndex(Val(List([Int(1), Int(2)])), Val(Int(42))))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"List.get(x, 42)"#)),
            r#"Ok(LIndex(Var(VarName("x")), Val(Int(42))))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(&mut r#"List.get(x, 1+2)"#)),
            r#"Ok(LIndex(Var(VarName("x")), BinOp(Val(Int(1)), Val(Int(2)), IOp(Add))))"#
        );
        assert_eq!(
            presult_to_string(&sexpr(
                &mut r#"List.get(List.get(List(List(1, 2), List(3, 4)), 0), 1)"#
            )),
            r#"Ok(LIndex(LIndex(Val(List([List([Int(1), Int(2)]), List([Int(3), Int(4)])])), Val(Int(0))), Val(Int(1))))"#
        );
    }

    #[test]
    fn parse_lconcat() {
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
    fn parse_lappend() {
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
            r#"Ok(LAppend(Val(List([])), Var(VarName("x"))))"#
        );
    }

    #[test]
    fn parse_lhead() {
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
    fn parse_ltail() {
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
}
