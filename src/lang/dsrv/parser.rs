use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use anyhow::{Error, anyhow};
use ecow::EcoVec;
use lalrpop_util::lalrpop_mod;

lalrpop_mod!(lalr, "/lang/dsrv/lalr.rs");

#[cfg(test)]
fn presult_to_string<T: std::fmt::Debug, E: std::fmt::Debug>(result: &Result<T, E>) -> String {
    format!("{result:?}")
}

#[cfg(test)]
use self::lalr::DeclarationParser;
use self::lalr::{DeclarationsParser, ExprParser};

use crate::{
    DsrvSpecification,
    core::{StreamType, VarName},
    lang::dsrv::{
        ast::{
            DsrvAstError, Expr, ExprBuilder, ExprId, UnvalidatedAssignment,
            UnvalidatedDsrvSpecification,
        },
        span::Span,
    },
};

/// A failure while parsing or constructing a DSRV syntax tree.
#[derive(Debug, thiserror::Error)]
pub enum DsrvParseError {
    #[error("invalid DSRV syntax: {0}")]
    Syntax(#[source] anyhow::Error),

    #[error("invalid DSRV specification: {0}")]
    Ast(#[from] DsrvAstError),
}

/// A parser-local top-level DSRV declaration.
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Declaration {
    Input(VarName, Option<StreamType>, Span),
    Output(VarName, Option<StreamType>, Span),
    Aux(VarName, Option<StreamType>, Span),
    Assignment(VarName, ExprId, Span),
}

pub fn parse_expr(input: &str) -> Result<Expr, Error> {
    let mut builder = ExprBuilder::with_capacity(input.len() / 4);
    let root = ExprParser::new()
        .parse(&mut builder, input)
        .map_err(|e| anyhow!("Parse error: {:?}", e))?;
    let expr = builder
        .finish(root)
        .map_err(|error| anyhow!("Invalid expression tree: {error}"))?;
    if let Some(key) = expr.as_ref().duplicate_field() {
        return Err(anyhow!("duplicate expression field {key:?}"));
    }
    Ok(expr)
}

#[cfg(test)]
fn parse_declaration(input: &str) -> Result<(Option<Expr>, Declaration), Error> {
    let mut builder = ExprBuilder::with_capacity(input.len() / 4);
    let declaration = DeclarationParser::new()
        .parse(&mut builder, input)
        .map_err(|e| anyhow!("Parse error: {:?}", e))?;
    let expression = match &declaration {
        Declaration::Assignment(_, root, _) => {
            let expression = builder
                .finish(*root)
                .map_err(|error| anyhow!("Invalid expression tree: {error}"))?;
            if let Some(key) = expression.as_ref().duplicate_field() {
                return Err(anyhow!("duplicate expression field {key:?}"));
            }
            Some(expression)
        }
        _ => {
            builder
                .finish_forest([])
                .map_err(|error| anyhow!("Invalid expression forest: {error}"))?;
            None
        }
    };
    Ok((expression, declaration))
}

pub(crate) fn create_dsrv_spec(
    builder: ExprBuilder,
    stmts: EcoVec<Declaration>,
) -> Result<DsrvSpecification, DsrvAstError> {
    let mut inputs = BTreeSet::new();
    let mut outputs = BTreeSet::new();
    let mut aux_vars = Vec::new();
    let mut assignments = Vec::new();
    let mut roots = Vec::new();
    let mut type_annotations = BTreeMap::new();

    for stmt in stmts {
        match stmt {
            Declaration::Input(var, typ, _) => {
                if let Some(typ) = typ {
                    type_annotations.insert(var.clone(), typ);
                }
                inputs.insert(var);
            }
            Declaration::Output(var, typ, _) => {
                if let Some(typ) = typ {
                    type_annotations.insert(var.clone(), typ);
                }
                outputs.insert(var);
            }
            Declaration::Aux(var, typ, _) => {
                if let Some(typ) = typ {
                    type_annotations.insert(var.clone(), typ);
                }
                aux_vars.push(var);
            }
            Declaration::Assignment(name, root, span) => {
                assignments.push(UnvalidatedAssignment { name, span });
                roots.push(root);
            }
        }
    }

    let expressions = builder.finish_forest(roots)?;
    UnvalidatedDsrvSpecification::new(
        inputs,
        outputs,
        aux_vars,
        expressions,
        assignments,
        type_annotations,
    )
    .validate()
}

struct LineCol {
    line: usize,
    col: usize,
}

impl fmt::Display for LineCol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}, column {}", self.line, self.col)
    }
}

// Converts a byte offset into a line and a column
fn line_col(input: &str, byte: usize) -> LineCol {
    let byte = byte.min(input.len());
    let mut line = 1usize;
    let mut col = 1usize;

    for ch in input[..byte].chars() {
        if ch == '\n' {
            line += 1;
            col = 1;
        } else {
            col += 1;
        }
    }
    LineCol { line, col }
}

pub fn parse_str(input: &str) -> Result<DsrvSpecification, DsrvParseError> {
    let mut builder = ExprBuilder::with_capacity(input.len() / 4);
    let stmts = DeclarationsParser::new()
        .parse(&mut builder, &input)
        .map_err(|e| {
            let err_fixed = e.map_location(|byte| line_col(&input, byte));
            DsrvParseError::Syntax(
                anyhow::anyhow!(err_fixed.to_string())
                    .context(format!("Failed to parse input {}", input)),
            )
        })?;
    create_dsrv_spec(builder, stmts).map_err(DsrvParseError::Ast)
}

pub async fn parse_file(file: &str) -> anyhow::Result<DsrvSpecification> {
    crate::io::file::parse_file(parse_str, file).await
}

#[cfg(test)]
mod tests {
    // TODO: Fix the test
    use crate::core::StreamType;

    use crate::VarName;
    use crate::lang::dsrv::ast::NumericalBinOp;
    use crate::lang::dsrv::ast::{Expr, ExprView, SBinOp};
    use crate::lang::dsrv::span::Span;

    use crate::core::StreamTypeAscription;
    use crate::lang::dsrv::span::{presult_strip_span, strip_span, strip_span_ref};

    use super::*;
    use test_log::test;

    #[test]
    fn unary_minus_is_syntax_faithful_and_preserves_spans() {
        let source = "-42";
        let expression = parse_expr(source).expect("unary minus should parse");
        let ExprView::Neg(operand) = expression.as_ref().view() else {
            panic!("expected Neg root, got {:?}", expression.as_ref().kind());
        };

        assert_eq!(
            expression.as_ref().span(),
            Span::new(0, source.len() as u32)
        );
        assert_eq!(operand.span(), Span::new(1, source.len() as u32));
        assert!(matches!(
            operand.view(),
            ExprView::Val(crate::Value::Int(42))
        ));
    }

    #[test]
    fn interleaved_call_and_field_prefixes_have_exact_spans() {
        let source = "factory()(x).result(y)";
        let expression = parse_expr(source).expect("postfix chain should parse");
        assert_eq!(expression.as_ref().span(), Span::new(0, 22));

        let ExprView::Apply(field_access, mut final_args) = expression.as_ref().view() else {
            panic!("expected final call");
        };
        assert_eq!(field_access.span(), Span::new(0, 19));
        assert!(
            matches!(final_args.next().unwrap().view(), ExprView::Var(name) if name == &VarName::new("y"))
        );

        let ExprView::SGet(second_call, field) = field_access.view() else {
            panic!("expected field access");
        };
        assert_eq!(field, "result");
        assert_eq!(second_call.span(), Span::new(0, 12));

        let ExprView::Apply(first_call, mut second_args) = second_call.view() else {
            panic!("expected second call");
        };
        assert!(
            matches!(second_args.next().unwrap().view(), ExprView::Var(name) if name == &VarName::new("x"))
        );
        assert_eq!(first_call.span(), Span::new(0, 9));

        let ExprView::Apply(factory, first_args) = first_call.view() else {
            panic!("expected first call");
        };
        assert_eq!(first_args.len(), 0);
        assert_eq!(factory.span(), Span::new(0, 7));
        assert!(matches!(factory.view(), ExprView::Var(name) if name == &VarName::new("factory")));
    }

    #[test]
    fn unary_minus_display_roundtrips_for_nested_operands() {
        let expression = parse_expr("-(x + 1)").expect("nested negation should parse");
        let displayed = format!("{expression}");
        let reparsed = parse_expr(&displayed).expect("displayed negation should parse");

        assert_eq!(strip_span(&expression), strip_span(&reparsed));
        assert!(matches!(expression.as_ref().view(), ExprView::Neg(_)));
    }

    fn declaration_to_string(result: &Result<(Option<Expr>, Declaration), Error>) -> String {
        match result {
            Ok((Some(expression), Declaration::Assignment(name, _, span))) => {
                format!(
                    "Ok(Assignment({name:?}, {}, {span:?}))",
                    strip_span(expression)
                )
            }
            Ok((_, declaration)) => format!("Ok({declaration:?})"),
            Err(error) => format!("Err({error:?})"),
        }
    }

    fn assert_specs_eq_ignoring_spans(actual: &DsrvSpecification, expected: &DsrvSpecification) {
        assert_eq!(actual.input_vars, expected.input_vars);
        assert_eq!(actual.output_vars, expected.output_vars);
        assert_eq!(actual.aux_vars, expected.aux_vars);
        assert_eq!(actual.stream_vars, expected.stream_vars);
        assert_eq!(actual.type_annotations, expected.type_annotations);

        let actual_exprs = actual
            .exprs
            .iter()
            .map(|(name, expr)| (name.clone(), strip_span_ref(expr)))
            .collect::<BTreeMap<_, _>>();
        let expected_exprs = expected
            .exprs
            .iter()
            .map(|(name, expr)| (name.clone(), strip_span_ref(expr)))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual_exprs, expected_exprs);
    }

    #[test]
    fn test_streamdata() {
        let parsed = parse_expr("42");
        let exp = "Ok(Val(Int(42)))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("42.0");
        let exp = "Ok(Val(Float(42.0)))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        // Unsupported:
        // let parsed = parse_str("1e-1");
        // let exp = "Ok(Val(Float(0.1)))";
        // assert_eq!(presult_to_string(&parsed), exp);

        let parsed = parse_expr("\"abc2d\"");
        let exp = "Ok(Val(Str(\"abc2d\")))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("true");
        let exp = "Ok(Val(Bool(true)))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("false");
        let exp = "Ok(Val(Bool(false)))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("()");
        let exp = "Ok(Val(Unit))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("\"x+y\"");
        let exp = "Ok(Val(Str(\"x+y\")))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);
    }

    #[test]
    fn test_sexpr() {
        let parsed = parse_expr("1 + 2");
        let exp = "Ok(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("1 + 2 * 3");
        let exp = "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), Val(Int(3)), NOp(Mul)), NOp(Add)))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("x + (y + 2)");
        let exp = "Ok(BinOp(Var(VarName::new(\"x\")), BinOp(Var(VarName::new(\"y\")), Val(Int(2)), NOp(Add)), NOp(Add)))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("if true then 1 else 2");
        let exp = "Ok(If(Val(Bool(true)), Val(Int(1)), Val(Int(2))))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("(x)[1]");
        let exp = "Ok(SIndex(Var(VarName::new(\"x\")), 1))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("(x + y)[3]");
        let exp =
            "Ok(SIndex(BinOp(Var(VarName::new(\"x\")), Var(VarName::new(\"y\")), NOp(Add)), 3))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("1 + (x)[1]");
        let exp = "Ok(BinOp(Val(Int(1)), SIndex(Var(VarName::new(\"x\")), 1), NOp(Add)))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("\"test\"");
        let exp = "Ok(Val(Str(\"test\")))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("(stage == \"m\")");
        let exp = "Ok(BinOp(Var(VarName::new(\"stage\")), Val(Str(\"m\")), COp(Eq)))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);
    }

    #[test]
    fn test_input_decl() {
        let parsed = parse_declaration(&mut "in x");
        let exp = r#"Ok(Input(VarName::new("x"), None, Span { start: 0, end: 4 }))"#;
        assert_eq!(declaration_to_string(&parsed), exp);

        // Not sure if we should allow this, but this is how it currently works. As long as we
        // start with "in"
        let parsed = parse_declaration(&mut "inx");
        assert_eq!(parsed.is_err(), true);
        let err = parsed.err().unwrap();
        assert!(err.to_string().contains("Parse error"));
    }

    #[test]
    fn test_typed_input_decl() {
        let parsed = parse_declaration("in x: Int");
        let exp = r#"Ok(Input(VarName::new("x"), Some(Int), Span { start: 0, end: 9 }))"#;
        assert_eq!(declaration_to_string(&parsed), exp);

        let parsed = parse_declaration("in x: Float");
        let exp = r#"Ok(Input(VarName::new("x"), Some(Float), Span { start: 0, end: 11 }))"#;
        assert_eq!(declaration_to_string(&parsed), exp);

        let input = "in xs: List<Int>";
        assert_eq!(
            parse_declaration(input).unwrap().1,
            Declaration::Input(
                "xs".into(),
                Some(StreamType::List(Box::new(StreamType::Int))),
                Span::new(0, input.len() as u32),
            )
        );

        let input = "in m: Map<List<Bool>>";
        assert_eq!(
            parse_declaration(input).unwrap().1,
            Declaration::Input(
                "m".into(),
                Some(StreamType::Map(Box::new(StreamType::List(Box::new(
                    StreamType::Bool
                ))))),
                Span::new(0, input.len() as u32),
            )
        );

        let input = "in robot: Struct<id: Int, label: Str>";
        assert_eq!(
            parse_declaration(input).unwrap().1,
            Declaration::Input(
                "robot".into(),
                Some(StreamType::Struct(
                    vec![
                        ("id".into(), StreamType::Int),
                        ("label".into(), StreamType::Str),
                    ]
                    .into(),
                    false,
                )),
                Span::new(0, input.len() as u32),
            )
        );

        let input = "in robot: Struct<id: Int, ...>";
        assert_eq!(
            parse_declaration(input).unwrap().1,
            Declaration::Input(
                "robot".into(),
                Some(StreamType::Struct(
                    vec![("id".into(), StreamType::Int)].into(),
                    true,
                )),
                Span::new(0, input.len() as u32),
            )
        );

        // Not sure if we should allow this, but this is how it currently works. As long as we
        // start with "in"
        let parsed = parse_declaration("inx:Int");
        assert_eq!(parsed.is_err(), true);
        let err = parsed.err().unwrap();
        assert!(err.to_string().contains("Parse error"));
    }

    #[test]
    fn test_parse_dsrv_simple_add() {
        let input = crate::dsrv_fixtures::spec_simple_add_monitor();
        let simple_add_spec = DsrvSpecification::new(
            BTreeSet::from(["x".into(), "y".into()]),
            BTreeSet::from(["z".into()]),
            BTreeMap::from([(
                "z".into(),
                Expr::BinOp(
                    Box::new(Expr::Var("x".into())),
                    Box::new(Expr::Var("y".into())),
                    SBinOp::NOp(NumericalBinOp::Add),
                ),
            )]),
            BTreeMap::new(),
            Vec::<VarName>::new(),
        );
        let spec = parse_str(input);
        assert!(spec.is_ok());
        let spec = spec.unwrap();
        assert_specs_eq_ignoring_spans(&spec, &simple_add_spec);
    }

    #[test]
    fn test_parse_dsrv_simple_add_typed() {
        let input = crate::dsrv_fixtures::spec_simple_add_monitor_typed();
        let simple_add_spec = DsrvSpecification::new(
            BTreeSet::from(["x".into(), "y".into()]),
            BTreeSet::from(["z".into()]),
            BTreeMap::from([(
                "z".into(),
                Expr::BinOp(
                    Box::new(Expr::Var("x".into())),
                    Box::new(Expr::Var("y".into())),
                    SBinOp::NOp(NumericalBinOp::Add),
                ),
            )]),
            BTreeMap::from([
                (VarName::new("x"), StreamType::Int),
                (VarName::new("y"), StreamType::Int),
                (VarName::new("z"), StreamType::Int),
            ]),
            Vec::<VarName>::new(),
        );
        let spec = parse_str(input);
        assert!(spec.is_ok());
        let spec = spec.unwrap();
        assert_specs_eq_ignoring_spans(&spec, &simple_add_spec);
    }

    #[test]
    fn test_parse_dsrv_simple_add_float_typed() {
        let input = crate::dsrv_fixtures::spec_simple_add_monitor_typed_float();
        let simple_add_spec = DsrvSpecification::new(
            BTreeSet::from(["x".into(), "y".into()]),
            BTreeSet::from(["z".into()]),
            BTreeMap::from([(
                "z".into(),
                Expr::BinOp(
                    Box::new(Expr::Var("x".into())),
                    Box::new(Expr::Var("y".into())),
                    SBinOp::NOp(NumericalBinOp::Add),
                ),
            )]),
            BTreeMap::from([
                ("x".into(), StreamType::Float),
                ("y".into(), StreamType::Float),
                ("z".into(), StreamType::Float),
            ]),
            Vec::<VarName>::new(),
        );
        let spec = parse_str(input);
        assert!(spec.is_ok());
        let spec = spec.unwrap();
        assert_specs_eq_ignoring_spans(&spec, &simple_add_spec);
    }

    #[test]
    fn test_parse_dsrv_count() {
        let input = "\
            out x\n\
            x = 1 + (x)[1]";
        let count_spec = DsrvSpecification::new(
            BTreeSet::from([]),
            BTreeSet::from(["x".into()]),
            BTreeMap::from([(
                "x".into(),
                Expr::BinOp(
                    Box::new(Expr::Val(1)),
                    Box::new(Expr::SIndex(Box::new(Expr::Var("x".into())), 1)),
                    SBinOp::NOp(NumericalBinOp::Add),
                ),
            )]),
            BTreeMap::new(),
            Vec::<VarName>::new(),
        );
        let spec = parse_str(input);
        assert!(spec.is_ok());
        let spec = spec.unwrap();
        assert_specs_eq_ignoring_spans(&spec, &count_spec);
    }

    #[test]
    fn test_parse_dsrv_dynamic() {
        let input = "\
            in x\n\
            in y\n\
            in s\n\
            out z\n\
            out w\n\
            z = x + y\n\
            w = dynamic(s)";
        let dynamic_spec = DsrvSpecification::new(
            BTreeSet::from(["x".into(), "y".into(), "s".into()]),
            BTreeSet::from(["z".into(), "w".into()]),
            BTreeMap::from([
                (
                    "z".into(),
                    Expr::BinOp(
                        Box::new(Expr::Var("x".into())),
                        Box::new(Expr::Var("y".into())),
                        SBinOp::NOp(NumericalBinOp::Add),
                    ),
                ),
                (
                    "w".into(),
                    Expr::Dynamic(
                        Box::new(Expr::Var("s".into())),
                        StreamTypeAscription::Unascribed,
                    ),
                ),
            ]),
            BTreeMap::new(),
            vec![],
        );
        let spec = parse_str(input);
        assert!(spec.is_ok());
        let spec = spec.unwrap();
        assert_specs_eq_ignoring_spans(&spec, &dynamic_spec);
    }

    #[test]
    fn test_unary() {
        assert_eq!(
            presult_strip_span(&parse_expr("-1").unwrap()),
            "Ok(Neg(Val(Int(1))))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("-1.0").unwrap()),
            "Ok(Neg(Val(Float(1.0))))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("-x").unwrap()),
            r#"Ok(Neg(Var(VarName::new("x"))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("-(1+2)").unwrap()),
            "Ok(Neg(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add))))"
        );
    }

    #[test]
    fn test_float_exprs() {
        // Add
        assert_eq!(
            presult_strip_span(&parse_expr("0.0").unwrap()),
            "Ok(Val(Float(0.0)))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("-1.0").unwrap()),
            "Ok(Neg(Val(Float(1.0))))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("  1.0 +2.0  ").unwrap()),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Add)))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr(" 1.0  + 2.0 +3.0").unwrap()),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Add)), Val(Float(3.0)), NOp(Add)))"
        );
        // Sub
        assert_eq!(
            presult_strip_span(&parse_expr("  1.0 -2.0  ").unwrap()),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Sub)))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr(" 1.0  - 2.0 -3.0").unwrap()),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Sub)), Val(Float(3.0)), NOp(Sub)))"
        );
        // Mul
        assert_eq!(
            presult_strip_span(&parse_expr("  1.0 *2.0  ").unwrap()),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Mul)))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr(" 1.0  * 2.0 *3.0").unwrap()),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Mul)), Val(Float(3.0)), NOp(Mul)))"
        );
        // Div
        assert_eq!(
            presult_strip_span(&parse_expr("  1.0 /2.0  ").unwrap()),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Div)))"
        );
    }

    #[test]
    fn test_mixed_float_int_exprs() {
        // Add
        assert_eq!(
            presult_strip_span(&parse_expr("0.0 + 2").unwrap()),
            "Ok(BinOp(Val(Float(0.0)), Val(Int(2)), NOp(Add)))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("1 + 2.0").unwrap()),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), NOp(Add)))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("1.0 + 2 + 3.0").unwrap()),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Int(2)), NOp(Add)), Val(Float(3.0)), NOp(Add)))"
        );
        // Sub
        assert_eq!(
            presult_strip_span(&parse_expr("1 - 2.0").unwrap()),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), NOp(Sub)))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("1.0 - 2 - 3.0").unwrap()),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Int(2)), NOp(Sub)), Val(Float(3.0)), NOp(Sub)))"
        );
        // Mul
        assert_eq!(
            presult_strip_span(&parse_expr("1 * 2.0").unwrap()),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), NOp(Mul)))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("1.0 * 2 * 3.0").unwrap()),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Int(2)), NOp(Mul)), Val(Float(3.0)), NOp(Mul)))"
        );
        // Div
        assert_eq!(
            presult_strip_span(&parse_expr("1 / 2.0").unwrap()),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), NOp(Div)))"
        );
    }

    #[test]
    fn test_integer_exprs() {
        // Add
        assert_eq!(
            presult_strip_span(&parse_expr("0").unwrap()),
            "Ok(Val(Int(0)))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("  1 +2  ").unwrap()),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr(" 1  + 2 +3").unwrap()),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), Val(Int(3)), NOp(Add)))"
        );
        // Sub
        assert_eq!(
            presult_strip_span(&parse_expr("  1 -2  ").unwrap()),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), NOp(Sub)))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr(" 1  - 2 -3").unwrap()),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Sub)), Val(Int(3)), NOp(Sub)))"
        );
        // Mul
        assert_eq!(
            presult_strip_span(&parse_expr("  1 *2  ").unwrap()),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr(" 1  * 2 *3").unwrap()),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), Val(Int(3)), NOp(Mul)))"
        );
        // Div
        assert_eq!(
            presult_strip_span(&parse_expr("  1 /2  ").unwrap()),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), NOp(Div)))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr(" 1  / 2 /3").unwrap()),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Div)), Val(Int(3)), NOp(Div)))"
        );
        // Mod
        assert_eq!(
            presult_strip_span(&parse_expr("  1 %2  ").unwrap()),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mod)))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr(" 1  % 2 %3").unwrap()),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mod)), Val(Int(3)), NOp(Mod)))"
        );
        // Var
        assert_eq!(
            presult_strip_span(&parse_expr("  x  ").unwrap()),
            r#"Ok(Var(VarName::new("x")))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("  xsss ").unwrap()),
            r#"Ok(Var(VarName::new("xsss")))"#
        );
        // Time index
        assert_eq!(
            presult_strip_span(&parse_expr("x [1]").unwrap()),
            r#"Ok(SIndex(Var(VarName::new("x")), 1))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("x[1 ]").unwrap()),
            r#"Ok(SIndex(Var(VarName::new("x")), 1))"#
        );
        // Paren
        assert_eq!(
            presult_strip_span(&parse_expr("  (1)  ").unwrap()),
            "Ok(Val(Int(1)))"
        );
        // Don't care about order of eval; care about what the AST looks like
        assert_eq!(
            presult_strip_span(&parse_expr(" 2 + (2 + 3)").unwrap()),
            "Ok(BinOp(Val(Int(2)), BinOp(Val(Int(2)), Val(Int(3)), NOp(Add)), NOp(Add)))"
        );
        // If then else
        assert_eq!(
            presult_strip_span(&parse_expr("if true then 1 else 2").unwrap()),
            "Ok(If(Val(Bool(true)), Val(Int(1)), Val(Int(2))))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("if true then x+x else y+y").unwrap()),
            r#"Ok(If(Val(Bool(true)), BinOp(Var(VarName::new("x")), Var(VarName::new("x")), NOp(Add)), BinOp(Var(VarName::new("y")), Var(VarName::new("y")), NOp(Add))))"#
        );

        // ChatGPT generated tests with mixed arithmetic and parentheses iexprs. It only had knowledge of the tests above.
        // Basic mixed addition and multiplication
        assert_eq!(
            presult_strip_span(&parse_expr("1 + 2 * 3").unwrap()),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), Val(Int(3)), NOp(Mul)), NOp(Add)))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("1 * 2 + 3").unwrap()),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), Val(Int(3)), NOp(Add)))"
        );
        // Mixed addition, subtraction, and multiplication
        assert_eq!(
            presult_strip_span(&parse_expr("1 + 2 * 3 - 4").unwrap()),
            "Ok(BinOp(BinOp(Val(Int(1)), BinOp(Val(Int(2)), Val(Int(3)), NOp(Mul)), NOp(Add)), Val(Int(4)), NOp(Sub)))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("1 * 2 + 3 - 4").unwrap()),
            "Ok(BinOp(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), Val(Int(3)), NOp(Add)), Val(Int(4)), NOp(Sub)))"
        );
        // Mixed addition and division
        assert_eq!(
            presult_strip_span(&parse_expr("10 + 20 / 5").unwrap()),
            "Ok(BinOp(Val(Int(10)), BinOp(Val(Int(20)), Val(Int(5)), NOp(Div)), NOp(Add)))"
        );
        // Nested parentheses with mixed operations
        assert_eq!(
            presult_strip_span(&parse_expr("(1 + 2) * (3 - 4)").unwrap()),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Sub)), NOp(Mul)))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("1 + (2 * (3 + 4))").unwrap()),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Add)), NOp(Mul)), NOp(Add)))"
        );
        // Complex nested expressions
        assert_eq!(
            presult_strip_span(&parse_expr("((1 + 2) * 3) + (4 / (5 - 6))").unwrap()),
            "Ok(BinOp(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), Val(Int(3)), NOp(Mul)), BinOp(Val(Int(4)), BinOp(Val(Int(5)), Val(Int(6)), NOp(Sub)), NOp(Div)), NOp(Add)))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("(1 + (2 * (3 - (4 / 5))))").unwrap()),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), BinOp(Val(Int(3)), BinOp(Val(Int(4)), Val(Int(5)), NOp(Div)), NOp(Sub)), NOp(Mul)), NOp(Add)))"
        );
        // More complex expressions with deep nesting
        assert_eq!(
            presult_strip_span(&parse_expr("((1 + 2) * (3 + 4))").unwrap()),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Add)), NOp(Mul)))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("((1 * 2) + (3 * 4)) / 5").unwrap()),
            "Ok(BinOp(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Mul)), NOp(Add)), Val(Int(5)), NOp(Div)))"
        );
        // Multiple levels of nested expressions
        assert_eq!(
            presult_strip_span(&parse_expr("1 + (2 * (3 + (4 / (5 - 6))))").unwrap()),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), BinOp(Val(Int(3)), BinOp(Val(Int(4)), BinOp(Val(Int(5)), Val(Int(6)), NOp(Sub)), NOp(Div)), NOp(Add)), NOp(Mul)), NOp(Add)))"
        );

        // ChatGPT generated tests with mixed iexprs. It only had knowledge of the tests above.
        // Mixing addition, subtraction, and variables
        assert_eq!(
            presult_strip_span(&parse_expr("x + 2 - y").unwrap()),
            r#"Ok(BinOp(BinOp(Var(VarName::new("x")), Val(Int(2)), NOp(Add)), Var(VarName::new("y")), NOp(Sub)))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("(x + y) * 3").unwrap()),
            r#"Ok(BinOp(BinOp(Var(VarName::new("x")), Var(VarName::new("y")), NOp(Add)), Val(Int(3)), NOp(Mul)))"#
        );
        // Nested arithmetic with variables and parentheses
        assert_eq!(
            presult_strip_span(&parse_expr("(a + b) / (c - d)").unwrap()),
            r#"Ok(BinOp(BinOp(Var(VarName::new("a")), Var(VarName::new("b")), NOp(Add)), BinOp(Var(VarName::new("c")), Var(VarName::new("d")), NOp(Sub)), NOp(Div)))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("x * (y + 3) - z / 2").unwrap()),
            r#"Ok(BinOp(BinOp(Var(VarName::new("x")), BinOp(Var(VarName::new("y")), Val(Int(3)), NOp(Add)), NOp(Mul)), BinOp(Var(VarName::new("z")), Val(Int(2)), NOp(Div)), NOp(Sub)))"#
        );
        // If-then-else with mixed arithmetic
        assert_eq!(
            presult_strip_span(&parse_expr("if true then 1 + 2 else 3 * 4").unwrap()),
            "Ok(If(Val(Bool(true)), BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Mul))))"
        );
        // Time index in arithmetic expression
        assert_eq!(
            presult_strip_span(&parse_expr("x[0] + y[1]").unwrap()),
            r#"Ok(BinOp(SIndex(Var(VarName::new("x")), 0), SIndex(Var(VarName::new("y")), 1), NOp(Add)))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("x[1] * (y + 3)").unwrap()),
            r#"Ok(BinOp(SIndex(Var(VarName::new("x")), 1), BinOp(Var(VarName::new("y")), Val(Int(3)), NOp(Add)), NOp(Mul)))"#
        );
        // Case to test precedence of if-then-else with arithmetic
        // Most languages implement this as "if a then b else (c + d)" and so should we.
        // Programmers can write "(if a then b else c) + d" if they want the other behavior.
        assert_eq!(
            presult_strip_span(&parse_expr("if a then b else c + d").unwrap()),
            r#"Ok(If(Var(VarName::new("a")), Var(VarName::new("b")), BinOp(Var(VarName::new("c")), Var(VarName::new("d")), NOp(Add))))"#
        );
    }

    // NOTE: I have not been able to find a way to parse this expression. Starting to believe it is not possible with LALR(1).
    // The issue is: We don't have any identifiers determining when the else branch ends.
    // So if we want "if a then b else c + d" to be (c + d), it must be implemented with if
    // expressions having lower precedence than arithmetic.
    // But then we can't parse "1 + if a then b else c" because we cannot the if-statement comes
    // earlier in the precedence chain...
    //
    // Note that I haven't found any other parsers using LALRPop that was able to resolve this.
    // One of the most advanced are:
    // https://github.com/Storyyeller/cubiml-demo/tree/master?tab=readme-ov-file
    // and it has the same issue...
    // See also: https://github.com/lalrpop/lalrpop/issues/1022 and
    // https://github.com/lalrpop/lalrpop/issues/705 for analogous issues
    //
    // (OCaml has syntax similar to ours, and the only way they can support this type of grammar is by
    // using %prec macros. They use the #prec macro to make a special case.)
    //
    // The user can wrap the if-statement in parentheses to get around this.
    // We should probably change our syntax to be easier to support
    #[ignore]
    #[test]
    fn test_ambiguous_case() {
        assert_eq!(
            presult_to_string(&parse_expr("1 + if a then b else c")),
            r#""#
        );
    }

    #[test]
    fn test_assignment_decl() {
        assert_eq!(
            declaration_to_string(&parse_declaration("x = 0")),
            r#"Ok(Assignment(VarName::new("x"), Val(Int(0)), Span { start: 0, end: 5 }))"#
        );
        assert_eq!(
            declaration_to_string(&parse_declaration(r#"x = "hello""#)),
            r#"Ok(Assignment(VarName::new("x"), Val(Str("hello")), Span { start: 0, end: 11 }))"#
        );
        assert_eq!(
            declaration_to_string(&parse_declaration("x = true")),
            r#"Ok(Assignment(VarName::new("x"), Val(Bool(true)), Span { start: 0, end: 8 }))"#
        );
        assert_eq!(
            declaration_to_string(&parse_declaration("x = false")),
            r#"Ok(Assignment(VarName::new("x"), Val(Bool(false)), Span { start: 0, end: 9 }))"#
        );
    }

    #[test]
    fn test_parse_empty_string() {
        let res = parse_str("");
        assert!(res.is_ok());
        let res = res.unwrap();
        assert_eq!(
            res,
            DsrvSpecification::new(
                BTreeSet::new(),
                BTreeSet::new(),
                BTreeMap::<VarName, Expr>::new(),
                BTreeMap::new(),
                vec![]
            )
        );
    }

    #[test]
    fn test_parse_invalid_expression() {
        let res = parse_expr("1 +");
        assert_eq!(res.is_err(), true);
        let err = res.err().unwrap();
        assert!(err.to_string().contains("Parse error"));

        let res = parse_expr("&& true");
        assert_eq!(res.is_err(), true);
        let err = res.err().unwrap();
        assert!(err.to_string().contains("Parse error"));
    }

    #[test]
    fn duplicate_assignments_are_ast_validation_errors() {
        let source = "out x\nx = 1\nx = 2";
        let error = parse_str(source).unwrap_err();
        let DsrvParseError::Ast(DsrvAstError::DuplicateAssignment {
            variable,
            first,
            duplicate,
        }) = error
        else {
            panic!("expected a duplicate-assignment AST error, got {error:?}");
        };

        assert_eq!(variable, VarName::new("x"));
        assert_eq!(&source[first.to_range()], "x = 1");
        assert_eq!(&source[duplicate.to_range()], "x = 2");
    }

    #[test]
    fn test_parse_boolean_expressions() {
        assert_eq!(
            presult_strip_span(&parse_expr("true && false").unwrap()),
            "Ok(BinOp(Val(Bool(true)), Val(Bool(false)), BOp(And)))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("true || false").unwrap()),
            "Ok(BinOp(Val(Bool(true)), Val(Bool(false)), BOp(Or)))"
        );
    }

    #[test]
    fn test_parse_mixed_boolean_and_arithmetic() {
        // Expressions do not make sense but parser should allow it
        assert_eq!(
            presult_strip_span(&parse_expr("1 + 2 && 3").unwrap()),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), Val(Int(3)), BOp(And)))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("true || 1 * 2").unwrap()),
            "Ok(BinOp(Val(Bool(true)), BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), BOp(Or)))"
        );
    }

    #[test]
    fn test_parse_string_concatenation() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#""foo" ++ "bar""#).unwrap()),
            r#"Ok(BinOp(Val(Str("foo")), Val(Str("bar")), SOp(Concat)))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#""hello" ++ " " ++ "world""#).unwrap()),
            r#"Ok(BinOp(BinOp(Val(Str("hello")), Val(Str(" ")), SOp(Concat)), Val(Str("world")), SOp(Concat)))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#""a" ++ "b" ++ "c""#).unwrap()),
            r#"Ok(BinOp(BinOp(Val(Str("a")), Val(Str("b")), SOp(Concat)), Val(Str("c")), SOp(Concat)))"#
        );
    }

    #[test]
    fn test_parse_defer() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"defer(x)"#).unwrap()),
            r#"Ok(Defer(Var(VarName::new("x")), Unascribed, Automatic))"#
        )
    }

    #[test]
    fn test_parse_update() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"update(x, y)"#).unwrap()),
            r#"Ok(Update(Var(VarName::new("x")), Var(VarName::new("y"))))"#
        )
    }

    #[test]
    fn test_parse_default() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"default(x, 0)"#).unwrap()),
            r#"Ok(Default(Var(VarName::new("x")), Val(Int(0))))"#
        )
    }

    #[test]
    fn test_parse_default_parse_expr() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"default(x, y)"#).unwrap()),
            r#"Ok(Default(Var(VarName::new("x")), Var(VarName::new("y"))))"#
        )
    }

    #[test]
    fn test_when() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"when(x)"#).unwrap()),
            r#"Ok(When(Var(VarName::new("x"))))"#
        )
    }

    #[test]
    fn test_is_defined() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"is_defined(x)"#).unwrap()),
            r#"Ok(IsDefined(Var(VarName::new("x"))))"#
        )
    }

    #[test]
    fn test_parse_list() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"[]"#).unwrap()),
            r#"Ok(List([]))"#,
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"[1, 2]"#).unwrap()),
            r#"Ok(List([Val(Int(1)), Val(Int(2))]))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List()"#).unwrap()),
            r#"Ok(List([]))"#,
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List () "#).unwrap()),
            r#"Ok(List([]))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List(1,2)"#).unwrap()),
            r#"Ok(List([Val(Int(1)), Val(Int(2))]))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List(1+2,2*5)"#).unwrap()),
            r#"Ok(List([BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), BinOp(Val(Int(2)), Val(Int(5)), NOp(Mul))]))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List("hello","world")"#).unwrap()),
            r#"Ok(List([Val(Str("hello")), Val(Str("world"))]))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List(true || false, true && false)"#).unwrap()),
            r#"Ok(List([BinOp(Val(Bool(true)), Val(Bool(false)), BOp(Or)), BinOp(Val(Bool(true)), Val(Bool(false)), BOp(And))]))"#
        );
        // Can mix expressions - not that it is necessarily a good idea
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List(1,"hello")"#).unwrap()),
            r#"Ok(List([Val(Int(1)), Val(Str("hello"))]))"#
        );
        assert_eq!(
            declaration_to_string(&parse_declaration("y = List()")),
            r#"Ok(Assignment(VarName::new("y"), List([]), Span { start: 0, end: 10 }))"#
        )
    }

    #[test]
    fn test_parse_lindex() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.get(List(1, 2), 42)"#).unwrap()),
            r#"Ok(LIndex(List([Val(Int(1)), Val(Int(2))]), Val(Int(42))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.get(x, 42)"#).unwrap()),
            r#"Ok(LIndex(Var(VarName::new("x")), Val(Int(42))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.get(x, 1+2)"#).unwrap()),
            r#"Ok(LIndex(Var(VarName::new("x")), BinOp(Val(Int(1)), Val(Int(2)), NOp(Add))))"#
        );
        assert_eq!(
            presult_strip_span(
                &parse_expr(r#"List.get(List.get(List(List(1, 2), List(3, 4)), 0), 1)"#).unwrap()
            ),
            r#"Ok(LIndex(LIndex(List([List([Val(Int(1)), Val(Int(2))]), List([Val(Int(3)), Val(Int(4))])]), Val(Int(0))), Val(Int(1))))"#
        );
    }

    #[test]
    fn test_parse_lconcat() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.concat(List(1, 2), List(3, 4))"#).unwrap()),
            r#"Ok(LConcat(List([Val(Int(1)), Val(Int(2))]), List([Val(Int(3)), Val(Int(4))])))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.concat(List(), List())"#).unwrap()),
            r#"Ok(LConcat(List([]), List([])))"#
        );
    }

    #[test]
    fn test_parse_lappend() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.append(List(1, 2), 3)"#).unwrap()),
            r#"Ok(LAppend(List([Val(Int(1)), Val(Int(2))]), Val(Int(3))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.append(List(), 3)"#).unwrap()),
            r#"Ok(LAppend(List([]), Val(Int(3))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.append(List(), x)"#).unwrap()),
            r#"Ok(LAppend(List([]), Var(VarName::new("x"))))"#
        );
    }

    #[test]
    fn test_parse_lhead() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.head(List(1, 2))"#).unwrap()),
            r#"Ok(LHead(List([Val(Int(1)), Val(Int(2))])))"#
        );
        // Ok for parser but will result in runtime error:
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.head(List())"#).unwrap()),
            r#"Ok(LHead(List([])))"#
        );
    }

    #[test]
    fn test_parse_ltail() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.tail(List(1, 2))"#).unwrap()),
            r#"Ok(LTail(List([Val(Int(1)), Val(Int(2))])))"#
        );
        // Ok for parser but will result in runtime error:
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.tail(List())"#).unwrap()),
            r#"Ok(LTail(List([])))"#
        );
    }

    #[test]
    fn test_parse_llen() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.len(List(1, 2))"#).unwrap()),
            r#"Ok(LLen(List([Val(Int(1)), Val(Int(2))])))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.len(List())"#).unwrap()),
            r#"Ok(LLen(List([])))"#
        );
    }

    #[test]
    fn test_parse_dynamic_type_ascription() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"dynamic(x: Int)"#).unwrap()),
            r#"Ok(Dynamic(Var(VarName::new("x")), Ascribed(Int), Automatic))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"dynamic(x: Int, {x, y})"#).unwrap()),
            r#"Ok(Dynamic(Var(VarName::new("x")), Ascribed(Int), Explicit([VarName::new("x"), VarName::new("y")])))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"defer(x: Int)"#).unwrap()),
            r#"Ok(Defer(Var(VarName::new("x")), Ascribed(Int), Automatic))"#
        );
    }

    #[test]
    fn test_parse_map() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map()"#).unwrap()),
            r#"Ok(Map({}))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"{"x": 1, "y": 2}"#).unwrap()),
            r#"Ok(ObjectLiteral({"x": Val(Int(1)), "y": Val(Int(2))}))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"{x: 1, y: 2}"#).unwrap()),
            r#"Ok(ObjectLiteral({"x": Val(Int(1)), "y": Val(Int(2))}))"#
        );
        assert!(parse_expr(r#"Map(x: 1)"#).is_err());
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map("x": 1, "y": 2)"#).unwrap()),
            r#"Ok(Map({"x": Val(Int(1)), "y": Val(Int(2))}))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map("x": 1+2,"y": 2*5)"#).unwrap()),
            r#"Ok(Map({"x": BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), "y": BinOp(Val(Int(2)), Val(Int(5)), NOp(Mul))}))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map("x": "hello", "y": "world")"#).unwrap()),
            r#"Ok(Map({"x": Val(Str("hello")), "y": Val(Str("world"))}))"#
        );
        assert_eq!(
            presult_strip_span(
                &parse_expr(r#"Map("xxxx": true || false, "yyyy": true && false)"#).unwrap()
            ),
            r#"Ok(Map({"xxxx": BinOp(Val(Bool(true)), Val(Bool(false)), BOp(Or)), "yyyy": BinOp(Val(Bool(true)), Val(Bool(false)), BOp(And))}))"#
        );
        // Can mix expressions - not that it is necessarily a good idea
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map( "x": 1, "y": "hello" )"#).unwrap()),
            r#"Ok(Map({"x": Val(Int(1)), "y": Val(Str("hello"))}))"#
        );
        assert_eq!(
            declaration_to_string(&parse_declaration("y = Map()")),
            r#"Ok(Assignment(VarName::new("y"), Map({}), Span { start: 0, end: 9 }))"#
        )
    }

    #[test]
    fn keyed_expression_fields_preserve_source_order() {
        let expr = parse_expr(r#"Map("z": 1, "a": 2, "m": 3)"#).unwrap();
        let crate::lang::dsrv::ast::ExprKind::Map(fields) = expr.as_ref().kind() else {
            panic!("expected a map expression");
        };
        assert_eq!(
            fields
                .keys()
                .map(|key| AsRef::<str>::as_ref(key))
                .collect::<Vec<_>>(),
            ["z", "a", "m"]
        );
    }

    #[test]
    fn keyed_expression_fields_reject_duplicates() {
        assert!(parse_expr(r#"Map("x": 1, "x": 2)"#).is_err());
        assert!(parse_expr(r#"Struct("x": 1, "x": 2)"#).is_err());
        assert!(parse_expr(r#"{x: 1, x: 2}"#).is_err());
    }

    #[test]
    fn test_parse_mget() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map.get(Map("x": 2, "y": true), "x")"#).unwrap()),
            r#"Ok(MGet(Map({"x": Val(Int(2)), "y": Val(Bool(true))}), "x"))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map.get(x, "key")"#).unwrap()),
            r#"Ok(MGet(Var(VarName::new("x")), "key"))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map.get(x, "")"#).unwrap()),
            r#"Ok(MGet(Var(VarName::new("x")), ""))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(
                r#"Map.get(Map.get(Map.get(Map("three": Map("two": Map("one": 42))), "three"), "two"), "one")"#
            ).unwrap()),
            r#"Ok(MGet(MGet(MGet(Map({"three": Map({"two": Map({"one": Val(Int(42))})})}), "three"), "two"), "one"))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"robot.id"#).unwrap()),
            r#"Ok(SGet(Var(VarName::new("robot")), "id"))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"robot.pose.x + 1"#).unwrap()),
            r#"Ok(BinOp(SGet(SGet(Var(VarName::new("robot")), "pose"), "x"), Val(Int(1)), NOp(Add)))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Struct("id": 1).id"#).unwrap()),
            r#"Ok(SGet(Struct({"id": Val(Int(1))}), "id"))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"robot.sensor_value"#).unwrap()),
            r#"Ok(SGet(Var(VarName::new("robot")), "sensor_value"))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"robot_A.pose.x + 1"#).unwrap()),
            r#"Ok(BinOp(SGet(SGet(Var(VarName::new("robot_A")), "pose"), "x"), Val(Int(1)), NOp(Add)))"#
        );
    }

    #[test]
    fn test_parse_mremove() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map.remove(Map("x": 2, "y": true), "x")"#).unwrap()),
            r#"Ok(MRemove(Map({"x": Val(Int(2)), "y": Val(Bool(true))}), "x"))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map.remove(x, "key")"#).unwrap()),
            r#"Ok(MRemove(Var(VarName::new("x")), "key"))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map.remove(x, "")"#).unwrap()),
            r#"Ok(MRemove(Var(VarName::new("x")), ""))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(
                r#"Map.remove(Map.remove(Map.remove(Map("three": Map("two": Map("one": 42))), "three"), "two"), "one")"#
            ).unwrap()),
            r#"Ok(MRemove(MRemove(MRemove(Map({"three": Map({"two": Map({"one": Val(Int(42))})})}), "three"), "two"), "one"))"#
        );
    }

    #[test]
    fn test_parse_mhas_key() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map.has_key(Map("x": 2, "y": true), "x")"#).unwrap()),
            r#"Ok(MHasKey(Map({"x": Val(Int(2)), "y": Val(Bool(true))}), "x"))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map.has_key(x, "key")"#).unwrap()),
            r#"Ok(MHasKey(Var(VarName::new("x")), "key"))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map.has_key(x, "")"#).unwrap()),
            r#"Ok(MHasKey(Var(VarName::new("x")), ""))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(
                r#"Map.has_key(Map.has_key(Map.has_key(Map("three": Map("two": Map("one": 42))), "three"), "two"), "one")"#
            ).unwrap()),
            r#"Ok(MHasKey(MHasKey(MHasKey(Map({"three": Map({"two": Map({"one": Val(Int(42))})})}), "three"), "two"), "one"))"#
        );
    }

    #[test]
    fn test_parse_minsert() {
        assert_eq!(
            presult_strip_span(
                &parse_expr(r#"Map.insert(Map("x": 2, "y": true), "z", 42)"#).unwrap()
            ),
            r#"Ok(MInsert(Map({"x": Val(Int(2)), "y": Val(Bool(true))}), "z", Val(Int(42))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map.insert(x, "key", true)"#).unwrap()),
            r#"Ok(MInsert(Var(VarName::new("x")), "key", Val(Bool(true))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map.insert(x, "", 1)"#).unwrap()),
            r#"Ok(MInsert(Var(VarName::new("x")), "", Val(Int(1))))"#
        );
    }

    #[test]
    fn test_dangling_else() {
        assert_eq!(
            presult_strip_span(&parse_expr("if a then b else c + d").unwrap()),
            r#"Ok(If(Var(VarName::new("a")), Var(VarName::new("b")), BinOp(Var(VarName::new("c")), Var(VarName::new("d")), NOp(Add))))"#
        )
    }

    #[test]
    fn test_trig() {
        assert_eq!(
            presult_strip_span(&parse_expr("sin(1.0)").unwrap()),
            r#"Ok(Sin(Val(Float(1.0))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("cos(0)").unwrap()),
            r#"Ok(Cos(Val(Int(0))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("tan(3.14)").unwrap()),
            r#"Ok(Tan(Val(Float(3.14))))"#
        );
    }

    #[test]
    fn test_abs() {
        assert_eq!(
            presult_strip_span(&parse_expr("abs(-5)").unwrap()),
            r#"Ok(Abs(Neg(Val(Int(5)))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("abs(3.14)").unwrap()),
            r#"Ok(Abs(Val(Float(3.14))))"#
        );
    }

    #[test]
    fn test_comparison() {
        assert_eq!(
            presult_strip_span(&parse_expr("1 < 2").unwrap()),
            r#"Ok(BinOp(Val(Int(1)), Val(Int(2)), COp(Lt)))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("1 > 2").unwrap()),
            r#"Ok(BinOp(Val(Int(1)), Val(Int(2)), COp(Gt)))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("3.14 >= 2.71").unwrap()),
            r#"Ok(BinOp(Val(Float(3.14)), Val(Float(2.71)), COp(Ge)))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("3.14 <= 2.71").unwrap()),
            r#"Ok(BinOp(Val(Float(3.14)), Val(Float(2.71)), COp(Le)))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("x == y").unwrap()),
            r#"Ok(BinOp(Var(VarName::new("x")), Var(VarName::new("y")), COp(Eq)))"#
        );
        // Test precedence:
        assert_eq!(
            presult_strip_span(&parse_expr("1 + 2 > 3").unwrap()),
            r#"Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), Val(Int(3)), COp(Gt)))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("1 + 2 == 3 * 4").unwrap()),
            r#"Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Mul)), COp(Eq)))"#
        );
        // Equality has lower precedence than other comparisons
        assert_eq!(
            presult_strip_span(&parse_expr("1 < 2 == 3 < 4").unwrap()),
            r#"Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), COp(Lt)), BinOp(Val(Int(3)), Val(Int(4)), COp(Lt)), COp(Eq)))"#
        );
    }

    #[test]
    fn test_not() {
        assert_eq!(
            presult_strip_span(&parse_expr("!true").unwrap()),
            r#"Ok(Not(Val(Bool(true))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("!false").unwrap()),
            r#"Ok(Not(Val(Bool(false))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("! (1 + 2 > 3)").unwrap()),
            r#"Ok(Not(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), Val(Int(3)), COp(Gt))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("!!false").unwrap()),
            r#"Ok(Not(Not(Val(Bool(false)))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("!(if a then b else c)").unwrap()),
            r#"Ok(Not(If(Var(VarName::new("a")), Var(VarName::new("b")), Var(VarName::new("c")))))"#
        );
        // Another edge case:
        assert_eq!(
            presult_strip_span(&parse_expr("!1 + 2").unwrap()),
            r#"Ok(BinOp(Not(Val(Int(1))), Val(Int(2)), NOp(Add)))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("if !true then 1 else 2").unwrap()),
            "Ok(If(Not(Val(Bool(true))), Val(Int(1)), Val(Int(2))))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("dynamic(!s)").unwrap()),
            r#"Ok(Dynamic(Not(Var(VarName::new("s"))), Unascribed, Automatic))"#
        );
    }

    #[test]
    fn test_capital_varname() {
        assert_eq!(
            declaration_to_string(&parse_declaration("in G")),
            r#"Ok(Input(VarName::new("G"), None, Span { start: 0, end: 4 }))"#
        );
        assert_eq!(
            declaration_to_string(&parse_declaration("out F")),
            r#"Ok(Output(VarName::new("F"), None, Span { start: 0, end: 5 }))"#
        );
        assert_eq!(
            declaration_to_string(&parse_declaration("in GANDALF")),
            r#"Ok(Input(VarName::new("GANDALF"), None, Span { start: 0, end: 10 }))"#
        );
        assert_eq!(
            declaration_to_string(&parse_declaration("out FRODO")),
            r#"Ok(Output(VarName::new("FRODO"), None, Span { start: 0, end: 9 }))"#
        );
    }

    #[test]
    fn test_large_expression() {
        let expr = "(((((if !(!(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) == !(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5))) && !(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) == ((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y))) && !((((((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x))) - (((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x)))) * ((((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) - (((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)))) / ((((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y))) - (((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)))) + (((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x)))) <= (((((0.1) * cos(3.14)) - ((0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) == !(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5))) && !(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) == ((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y))) && !((((((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x))) - (((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) * ((((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) - (((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) / ((((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y))) - (((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) + (((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) <= (((((0.1) * cos(3.14)) - ((0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) == !(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5))) && !(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) == ((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y))) && !((((((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x))) - (((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x)))) * ((((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) - (((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)))) / ((((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y))) - (((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)))) + (((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x)))) <= (((((0.1) * cos(3.14)) - ((0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) == !(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5))) && !(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) == ((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y))) && !((((((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x))) - (((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) * ((((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) - (((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) / ((((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y))) - (((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) + (((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) <= (((((0.1) * cos(3.14)) - ((0.153) * sin(3.14))) + 1.0))) then 1 else 0 ))) % 2) == 1) || (((((if !(!(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) == !(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5))) && !(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) == ((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y))) && !((((((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x))) - (((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x)))) * ((((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) - (((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)))) / ((((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y))) - (((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)))) + (((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x)))) <= (((((0.1) * cos(3.14)) - ((-0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) == !(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5))) && !(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) == ((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y))) && !((((((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x))) - (((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) * ((((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) - (((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) / ((((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y))) - (((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) + (((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) <= (((((0.1) * cos(3.14)) - ((-0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) == !(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5))) && !(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) == ((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y))) && !((((((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x))) - (((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x)))) * ((((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) - (((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)))) / ((((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y))) - (((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)))) + (((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x)))) <= (((((0.1) * cos(3.14)) - ((-0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) == !(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5))) && !(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) == ((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y))) && !((((((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x))) - (((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) * ((((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) - (((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) / ((((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y))) - (((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) + (((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) <= (((((0.1) * cos(3.14)) - ((-0.153) * sin(3.14))) + 1.0))) then 1 else 0 ))) % 2) == 1) || (((((if !(!(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) == !(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5))) && !(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) == ((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y))) && !((((((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x))) - (((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x)))) * ((((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) - (((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)))) / ((((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y))) - (((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)))) + (((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x)))) <= (((((-0.181) * cos(3.14)) - ((0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) == !(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5))) && !(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) == ((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y))) && !((((((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x))) - (((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) * ((((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) - (((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) / ((((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y))) - (((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) + (((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) <= (((((-0.181) * cos(3.14)) - ((0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) == !(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5))) && !(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) == ((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y))) && !((((((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x))) - (((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x)))) * ((((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) - (((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)))) / ((((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y))) - (((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)))) + (((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x)))) <= (((((-0.181) * cos(3.14)) - ((0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) == !(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5))) && !(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) == ((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y))) && !((((((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x))) - (((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) * ((((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) - (((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) / ((((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y))) - (((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) + (((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) <= (((((-0.181) * cos(3.14)) - ((0.153) * sin(3.14))) + 1.0))) then 1 else 0 ))) % 2) == 1) || (((((if !(!(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) == !(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5))) && !(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) == ((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y))) && !((((((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x))) - (((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x)))) * ((((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) - (((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)))) / ((((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y))) - (((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)))) + (((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x)))) <= (((((-0.181) * cos(3.14)) - ((-0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) == !(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5))) && !(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) == ((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y))) && !((((((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x))) - (((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) * ((((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) - (((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) / ((((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y))) - (((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) + (((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) <= (((((-0.181) * cos(3.14)) - ((-0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) == !(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5))) && !(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) == ((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y))) && !((((((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x))) - (((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x)))) * ((((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) - (((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)))) / ((((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y))) - (((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)))) + (((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x)))) <= (((((-0.181) * cos(3.14)) - ((-0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) == !(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5))) && !(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) == ((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y))) && !((((((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x))) - (((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) * ((((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) - (((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) / ((((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y))) - (((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) + (((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) <= (((((-0.181) * cos(3.14)) - ((-0.153) * sin(3.14))) + 1.0))) then 1 else 0 ))) % 2) == 1)";
        let res = parse_expr(expr);
        assert!(res.is_ok());
    }
}

#[cfg(test)]
mod spec_tests {

    use super::*;
    use test_log::test;

    fn counter_inf() -> (&'static str, &'static str) {
        (
            "out z\nz = default(z[1], 0) + 1",
            "Ok(DsrvSpecification { input_vars: {}, output_vars: {VarName::new(\"z\")}, aux_vars: {}, stream_vars: {VarName::new(\"z\")}, exprs: {VarName::new(\"z\"): BinOp(Default(SIndex(Var(VarName::new(\"z\")), 1), Val(Int(0))), Val(Int(1)), NOp(Add))}, type_annotations: {} })",
        )
    }

    fn counter() -> (&'static str, &'static str) {
        (
            "in x\nout z\nz = default(z[1], 0) + x",
            "Ok(DsrvSpecification { input_vars: {VarName::new(\"x\")}, output_vars: {VarName::new(\"z\")}, aux_vars: {}, stream_vars: {VarName::new(\"z\")}, exprs: {VarName::new(\"z\"): BinOp(Default(SIndex(Var(VarName::new(\"z\")), 1), Val(Int(0))), Var(VarName::new(\"x\")), NOp(Add))}, type_annotations: {} })",
        )
    }

    fn future() -> (&'static str, &'static str) {
        (
            "in x\nin y\nout z\nout a\nz = x[1]\na = y",
            "Ok(DsrvSpecification { input_vars: {VarName::new(\"x\"), VarName::new(\"y\")}, output_vars: {VarName::new(\"z\"), VarName::new(\"a\")}, aux_vars: {}, stream_vars: {VarName::new(\"z\"), VarName::new(\"a\")}, exprs: {VarName::new(\"z\"): SIndex(Var(VarName::new(\"x\")), 1), VarName::new(\"a\"): Var(VarName::new(\"y\"))}, type_annotations: {} })",
        )
    }

    fn list() -> (&'static str, &'static str) {
        (
            "in iList\nout oList\nout nestedList\nout listIndex\nout listAppend\nout listConcat\nout listHead\nout listTail\noList = iList\nnestedList = List(iList, iList)\nlistIndex = List.get(iList, 0)\nlistAppend = List.append(iList, (1+1)/2)\nlistConcat = List.concat(iList, iList)\nlistHead = List.head(iList)\nlistTail = List.tail(iList)",
            "Ok(DsrvSpecification { input_vars: {VarName::new(\"iList\")}, output_vars: {VarName::new(\"oList\"), VarName::new(\"nestedList\"), VarName::new(\"listIndex\"), VarName::new(\"listAppend\"), VarName::new(\"listConcat\"), VarName::new(\"listHead\"), VarName::new(\"listTail\")}, aux_vars: {}, stream_vars: {VarName::new(\"oList\"), VarName::new(\"nestedList\"), VarName::new(\"listIndex\"), VarName::new(\"listAppend\"), VarName::new(\"listConcat\"), VarName::new(\"listHead\"), VarName::new(\"listTail\")}, exprs: {VarName::new(\"oList\"): Var(VarName::new(\"iList\")), VarName::new(\"nestedList\"): List([Var(VarName::new(\"iList\")), Var(VarName::new(\"iList\"))]), VarName::new(\"listIndex\"): LIndex(Var(VarName::new(\"iList\")), Val(Int(0))), VarName::new(\"listAppend\"): LAppend(Var(VarName::new(\"iList\")), BinOp(BinOp(Val(Int(1)), Val(Int(1)), NOp(Add)), Val(Int(2)), NOp(Div))), VarName::new(\"listConcat\"): LConcat(Var(VarName::new(\"iList\")), Var(VarName::new(\"iList\"))), VarName::new(\"listHead\"): LHead(Var(VarName::new(\"iList\"))), VarName::new(\"listTail\"): LTail(Var(VarName::new(\"iList\")))}, type_annotations: {} })",
        )
    }

    fn simple_add_typed() -> (&'static str, &'static str) {
        (
            "in x: Int\nin y: Int\nout z: Int\nz = x + y",
            "Ok(DsrvSpecification { input_vars: {VarName::new(\"x\"), VarName::new(\"y\")}, output_vars: {VarName::new(\"z\")}, aux_vars: {}, stream_vars: {VarName::new(\"z\")}, exprs: {VarName::new(\"z\"): BinOp(Var(VarName::new(\"x\")), Var(VarName::new(\"y\")), NOp(Add))}, type_annotations: {VarName::new(\"x\"): Int, VarName::new(\"z\"): Int, VarName::new(\"y\"): Int} })",
        )
    }

    fn simple_add_aux() -> (&'static str, &'static str) {
        (
            crate::dsrv_fixtures::spec_simple_add_aux_monitor(),
            "Ok(DsrvSpecification { input_vars: {VarName::new(\"x\"), VarName::new(\"y\")}, output_vars: {VarName::new(\"z\")}, aux_vars: {VarName::new(\"u\"), VarName::new(\"w\")}, stream_vars: {VarName::new(\"z\"), VarName::new(\"u\"), VarName::new(\"w\")}, exprs: {VarName::new(\"z\"): BinOp(Var(VarName::new(\"u\")), Var(VarName::new(\"w\")), NOp(Add)), VarName::new(\"u\"): Var(VarName::new(\"x\")), VarName::new(\"w\"): Var(VarName::new(\"y\"))}, type_annotations: {} })",
        )
    }
    fn simple_add_aux_typed() -> (&'static str, &'static str) {
        (
            crate::dsrv_fixtures::spec_simple_add_aux_typed_monitor(),
            "Ok(DsrvSpecification { input_vars: {VarName::new(\"x\"), VarName::new(\"y\")}, output_vars: {VarName::new(\"z\")}, aux_vars: {VarName::new(\"u\"), VarName::new(\"w\")}, stream_vars: {VarName::new(\"z\"), VarName::new(\"u\"), VarName::new(\"w\")}, exprs: {VarName::new(\"z\"): BinOp(Var(VarName::new(\"u\")), Var(VarName::new(\"w\")), NOp(Add)), VarName::new(\"u\"): Var(VarName::new(\"x\")), VarName::new(\"w\"): Var(VarName::new(\"y\"))}, type_annotations: {VarName::new(\"x\"): Int, VarName::new(\"z\"): Int, VarName::new(\"y\"): Int, VarName::new(\"u\"): Int, VarName::new(\"w\"): Int} })",
        )
    }

    fn simple_add_typed_start_and_end_comment() -> (&'static str, &'static str) {
        (
            "// Begin\nin x: Int\nin y: Int\nout z: Int\nz = x + y// End",
            "Ok(DsrvSpecification { input_vars: {VarName::new(\"x\"), VarName::new(\"y\")}, output_vars: {VarName::new(\"z\")}, aux_vars: {}, stream_vars: {VarName::new(\"z\")}, exprs: {VarName::new(\"z\"): BinOp(Var(VarName::new(\"x\")), Var(VarName::new(\"y\")), NOp(Add))}, type_annotations: {VarName::new(\"x\"): Int, VarName::new(\"z\"): Int, VarName::new(\"y\"): Int} })",
        )
    }

    fn if_statement() -> (&'static str, &'static str) {
        (
            "in x\nin y\nout z\nz = if x == 0 then y else 42",
            "Ok(DsrvSpecification { input_vars: {VarName::new(\"x\"), VarName::new(\"y\")}, output_vars: {VarName::new(\"z\")}, aux_vars: {}, stream_vars: {VarName::new(\"z\")}, exprs: {VarName::new(\"z\"): If(BinOp(Var(VarName::new(\"x\")), Val(Int(0)), COp(Eq)), Var(VarName::new(\"y\")), Val(Int(42)))}, type_annotations: {} })",
        )
    }

    fn if_statement_newlines() -> (&'static str, &'static str) {
        (
            "in x\nin y\nout z\nz = if\nx == 0\nthen\ny\n else\n42",
            "Ok(DsrvSpecification { input_vars: {VarName::new(\"x\"), VarName::new(\"y\")}, output_vars: {VarName::new(\"z\")}, aux_vars: {}, stream_vars: {VarName::new(\"z\")}, exprs: {VarName::new(\"z\"): If(BinOp(Var(VarName::new(\"x\")), Val(Int(0)), COp(Eq)), Var(VarName::new(\"y\")), Val(Int(42)))}, type_annotations: {} })",
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
            (function_name(simple_add_aux), simple_add_aux()),
            (function_name(simple_add_aux_typed), simple_add_aux_typed()),
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
    fn test_dsrv_specs_normal() {
        for &(name, (spec, exp)) in specs().iter() {
            let parsed = presult_to_string(&parse_str(spec));
            assert_eq!(
                format!("{}: {}", name, parsed),
                format!("{}: {}", name, exp)
            );
        }
    }

    #[test]
    fn test_dsrv_specs_added_newlines() {
        for &(name, (spec, exp)) in specs().iter() {
            let spec = spec.replace("\n", "\n\n");
            let parsed = presult_to_string(&parse_str(spec.as_str()));
            assert_eq!(
                format!("{}: {}", name, parsed),
                format!("{}: {}", name, exp)
            );
        }
    }

    #[test]
    fn test_dsrv_specs_added_comments() {
        for &(name, (spec, exp)) in specs().iter() {
            let mod_spec = spec.replace("\n", "\n//This is a comment\n");
            let parsed = presult_to_string(&parse_str(mod_spec.as_str()));
            assert_eq!(
                format!("{}: {}", name, parsed),
                format!("{}: {}", name, exp)
            );

            let mod_spec = spec.replace("\n", "//This is a comment\n"); // Beginning \n
            let parsed = presult_to_string(&parse_str(mod_spec.as_str()));
            assert_eq!(
                format!("{}: {}", name, parsed),
                format!("{}: {}", name, exp)
            );
        }
    }
}
