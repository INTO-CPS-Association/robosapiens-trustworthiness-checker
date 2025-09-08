use std::collections::BTreeMap;

use anyhow::{Error, anyhow};
use ecow::EcoVec;
use tracing::warn;

use super::lalr::{ExprParser, TopDeclParser, TopDeclsParser};
use crate::{LOLASpecification, SExpr, lang::dynamic_lola::ast::STopDecl};

pub fn parse_sexpr<'input>(input: &'input str) -> Result<SExpr, Error> {
    ExprParser::new()
        .parse(input)
        .map_err(|e| anyhow!("Parse error: {:?}", e))
}

pub fn parse_stopdecl<'input>(input: &'input str) -> Result<STopDecl, Error> {
    TopDeclParser::new()
        .parse(input)
        .map_err(|e| anyhow!("Parse error: {:?}", e))
}

pub fn parse_stopdecls<'input>(input: &'input str) -> Result<EcoVec<STopDecl>, Error> {
    TopDeclsParser::new()
        .parse(input)
        .map_err(|e| anyhow!("Parse error: {:?}", e))
}

pub fn create_lola_spec(stmts: &EcoVec<STopDecl>) -> LOLASpecification {
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();
    let mut aux_info = Vec::new();
    let mut assignments = BTreeMap::new();
    let mut type_annotations = BTreeMap::new();

    for stmt in stmts {
        match stmt {
            STopDecl::Input(var, typ) => {
                inputs.push(var.clone());
                if let Some(typ) = typ {
                    type_annotations.insert(var.clone(), typ.clone());
                }
            }
            STopDecl::Output(var, typ) => {
                outputs.push(var.clone());
                if let Some(typ) = typ {
                    type_annotations.insert(var.clone(), typ.clone());
                }
            }
            STopDecl::Aux(var, typ) => {
                outputs.push(var.clone());
                aux_info.push(var.clone());
                if let Some(typ) = typ {
                    type_annotations.insert(var.clone(), typ.clone());
                }
            }
            STopDecl::Assignment(var, sexpr) => {
                assignments.insert(var.clone(), sexpr.clone());
            }
        }
    }

    LOLASpecification::new(inputs, outputs, assignments, type_annotations, aux_info)
}

pub fn parse_str<'input>(input: &'input str) -> anyhow::Result<LOLASpecification> {
    let stmts = TopDeclsParser::new().parse(&input).map_err(|e| {
        anyhow::anyhow!(e.to_string()).context(format!("Failed to parse input {}", input))
    })?;
    Ok(create_lola_spec(&stmts))
}

pub async fn parse_file<'file>(file: &'file str) -> anyhow::Result<LOLASpecification> {
    warn!(
        "Use of LALR parser is incomplete, experimental and currently hardcoded for DynSRV specifications"
    );
    let contents = smol::fs::read_to_string(file).await?;
    let stmts = TopDeclsParser::new().parse(&contents).map_err(|e| {
        anyhow::anyhow!(e.to_string()).context(format!("Failed to parse file {}", file))
    })?;
    Ok(create_lola_spec(&stmts))
}

#[cfg(test)]
mod tests {
    use crate::core::StreamType;
    use crate::lang::core::parser::presult_to_string;

    use crate::VarName;
    use crate::lang::dynamic_lola::ast::NumericalBinOp;
    use crate::lang::dynamic_lola::ast::SBinOp;

    use super::*;
    use test_log::test;

    #[test]
    fn test_streamdata() {
        let parsed = parse_sexpr("42");
        let exp = "Ok(Val(Int(42)))";
        assert_eq!(presult_to_string(&parsed), exp);

        let parsed = parse_sexpr("42.0");
        let exp = "Ok(Val(Float(42.0)))";
        assert_eq!(presult_to_string(&parsed), exp);

        // Unsupported:
        // let parsed = parse_str("1e-1");
        // let exp = "Ok(Val(Float(0.1)))";
        // assert_eq!(presult_to_string(&parsed), exp);

        let parsed = parse_sexpr("\"abc2d\"");
        let exp = "Ok(Val(Str(\"abc2d\")))";
        assert_eq!(presult_to_string(&parsed), exp);

        let parsed = parse_sexpr("true");
        let exp = "Ok(Val(Bool(true)))";
        assert_eq!(presult_to_string(&parsed), exp);

        let parsed = parse_sexpr("false");
        let exp = "Ok(Val(Bool(false)))";
        assert_eq!(presult_to_string(&parsed), exp);

        let parsed = parse_sexpr("\"x+y\"");
        let exp = "Ok(Val(Str(\"x+y\")))";
        assert_eq!(presult_to_string(&parsed), exp);
    }

    #[test]
    fn test_sexpr() {
        let parsed = parse_sexpr("1 + 2");
        let exp = "Ok(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)))";
        assert_eq!(presult_to_string(&parsed), exp);

        let parsed = parse_sexpr("1 + 2 * 3");
        let exp = "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), Val(Int(3)), NOp(Mul)), NOp(Add)))";
        assert_eq!(presult_to_string(&parsed), exp);

        let parsed = parse_sexpr("x + (y + 2)");
        let exp = "Ok(BinOp(Var(VarName::new(\"x\")), BinOp(Var(VarName::new(\"y\")), Val(Int(2)), NOp(Add)), NOp(Add)))";
        assert_eq!(presult_to_string(&parsed), exp);

        let parsed = parse_sexpr("if true then 1 else 2");
        let exp = "Ok(If(Val(Bool(true)), Val(Int(1)), Val(Int(2))))";
        assert_eq!(presult_to_string(&parsed), exp);

        let parsed = parse_sexpr("(x)[-1]");
        let exp = "Ok(SIndex(Var(VarName::new(\"x\")), -1))";
        assert_eq!(presult_to_string(&parsed), exp);

        let parsed = parse_sexpr("(x + y)[-3]");
        let exp =
            "Ok(SIndex(BinOp(Var(VarName::new(\"x\")), Var(VarName::new(\"y\")), NOp(Add)), -3))";
        assert_eq!(presult_to_string(&parsed), exp);

        let parsed = parse_sexpr("1 + (x)[-1]");
        let exp = "Ok(BinOp(Val(Int(1)), SIndex(Var(VarName::new(\"x\")), -1), NOp(Add)))";
        assert_eq!(presult_to_string(&parsed), exp);

        let parsed = parse_sexpr("\"test\"");
        let exp = "Ok(Val(Str(\"test\")))";
        assert_eq!(presult_to_string(&parsed), exp);

        let parsed = parse_sexpr("(stage == \"m\")");
        let exp = "Ok(BinOp(Var(VarName::new(\"stage\")), Val(Str(\"m\")), COp(Eq)))";
        assert_eq!(presult_to_string(&parsed), exp);
    }

    #[test]
    fn test_input_decl() {
        let parsed = parse_stopdecl(&mut "in x");
        let exp = r#"Ok(Input(VarName::new("x"), None))"#;
        assert_eq!(presult_to_string(&parsed), exp);

        // Not sure if we should allow this, but this is how it currently works. As long as we
        // start with "in"
        let parsed = parse_stopdecl(&mut "inx");
        assert_eq!(parsed.is_err(), true);
        let err = parsed.err().unwrap();
        assert!(err.to_string().contains("Parse error"));
    }

    #[test]
    fn test_typed_input_decl() {
        let parsed = parse_stopdecl("in x: Int");
        let exp = r#"Ok(Input(VarName::new("x"), Some(Int)))"#;
        assert_eq!(presult_to_string(&parsed), exp);

        let parsed = parse_stopdecl("in x: Float");
        let exp = r#"Ok(Input(VarName::new("x"), Some(Float)))"#;
        assert_eq!(presult_to_string(&parsed), exp);

        // Not sure if we should allow this, but this is how it currently works. As long as we
        // start with "in"
        let parsed = parse_stopdecl("inx:Int");
        assert_eq!(parsed.is_err(), true);
        let err = parsed.err().unwrap();
        assert!(err.to_string().contains("Parse error"));
    }

    #[test]
    fn test_parse_lola_simple_add() {
        let input = crate::lola_fixtures::spec_simple_add_monitor();
        let simple_add_spec = LOLASpecification {
            input_vars: vec!["x".into(), "y".into()],
            output_vars: vec!["z".into()],
            aux_info: vec![],
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
        let spec = parse_str(input);
        assert!(spec.is_ok());
        let spec = spec.unwrap();
        assert_eq!(spec, simple_add_spec);
    }

    #[test]
    fn test_parse_lola_simple_add_typed() {
        let input = crate::lola_fixtures::spec_simple_add_monitor_typed();
        let simple_add_spec = LOLASpecification {
            input_vars: vec!["x".into(), "y".into()],
            output_vars: vec!["z".into()],
            aux_info: vec![],
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
        let spec = parse_str(input);
        assert!(spec.is_ok());
        let spec = spec.unwrap();
        assert_eq!(spec, simple_add_spec);
    }

    #[test]
    fn test_parse_lola_simple_add_float_typed() {
        let input = crate::lola_fixtures::spec_simple_add_monitor_typed_float();
        let simple_add_spec = LOLASpecification {
            input_vars: vec!["x".into(), "y".into()],
            output_vars: vec!["z".into()],
            aux_info: vec![],
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
        let spec = parse_str(input);
        assert!(spec.is_ok());
        let spec = spec.unwrap();
        assert_eq!(spec, simple_add_spec);
    }

    #[test]
    fn test_parse_lola_count() {
        let input = "\
            out x\n\
            x = 1 + (x)[-1]";
        let count_spec = LOLASpecification {
            input_vars: vec![],
            output_vars: vec!["x".into()],
            aux_info: vec![],
            exprs: BTreeMap::from([(
                "x".into(),
                SExpr::BinOp(
                    Box::new(SExpr::Val(1.into())),
                    Box::new(SExpr::SIndex(Box::new(SExpr::Var("x".into())), -1)),
                    SBinOp::NOp(NumericalBinOp::Add),
                ),
            )]),
            type_annotations: BTreeMap::new(),
        };
        let spec = parse_str(input);
        assert!(spec.is_ok());
        let spec = spec.unwrap();
        assert_eq!(spec, count_spec);
    }

    #[test]
    fn test_parse_lola_dynamic() {
        let input = "\
            in x\n\
            in y\n\
            in s\n\
            out z\n\
            out w\n\
            z = x + y\n\
            w = dynamic(s)";
        let dynamic_spec = LOLASpecification::new(
            vec!["x".into(), "y".into(), "s".into()],
            vec!["z".into(), "w".into()],
            BTreeMap::from([
                (
                    "z".into(),
                    SExpr::BinOp(
                        Box::new(SExpr::Var("x".into())),
                        Box::new(SExpr::Var("y".into())),
                        SBinOp::NOp(NumericalBinOp::Add),
                    ),
                ),
                ("w".into(), SExpr::Dynamic(Box::new(SExpr::Var("s".into())))),
            ]),
            BTreeMap::new(),
            vec![],
        );
        let spec = parse_str(input);
        assert!(spec.is_ok());
        let spec = spec.unwrap();
        assert_eq!(spec, dynamic_spec);
    }

    #[test]
    fn test_unary() {
        assert_eq!(presult_to_string(&parse_sexpr("-1")), "Ok(Val(Int(-1)))");
        assert_eq!(
            presult_to_string(&parse_sexpr("-1.0")),
            "Ok(Val(Float(-1.0)))"
        );
        assert_eq!(
            presult_to_string(&parse_sexpr("-x")),
            r#"Ok(BinOp(Val(Int(0)), Var(VarName::new("x")), NOp(Sub)))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr("-(1+2)")),
            "Ok(BinOp(Val(Int(0)), BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), NOp(Sub)))"
        );
    }

    #[test]
    fn test_float_exprs() {
        // Add
        assert_eq!(
            presult_to_string(&parse_sexpr("0.0")),
            "Ok(Val(Float(0.0)))"
        );
        assert_eq!(
            presult_to_string(&parse_sexpr("-1.0")),
            "Ok(Val(Float(-1.0)))"
        );
        assert_eq!(
            presult_to_string(&parse_sexpr("  1.0 +2.0  ")),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(" 1.0  + 2.0 +3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Add)), Val(Float(3.0)), NOp(Add)))"
        );
        // Sub
        assert_eq!(
            presult_to_string(&parse_sexpr("  1.0 -2.0  ")),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Sub)))"
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(" 1.0  - 2.0 -3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Sub)), Val(Float(3.0)), NOp(Sub)))"
        );
        // Mul
        assert_eq!(
            presult_to_string(&parse_sexpr("  1.0 *2.0  ")),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Mul)))"
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(" 1.0  * 2.0 *3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Mul)), Val(Float(3.0)), NOp(Mul)))"
        );
        // Div
        assert_eq!(
            presult_to_string(&parse_sexpr("  1.0 /2.0  ")),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Div)))"
        );
    }

    #[test]
    fn test_mixed_float_int_exprs() {
        // Add
        assert_eq!(
            presult_to_string(&parse_sexpr("0.0 + 2")),
            "Ok(BinOp(Val(Float(0.0)), Val(Int(2)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&parse_sexpr("1 + 2.0")),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&parse_sexpr("1.0 + 2 + 3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Int(2)), NOp(Add)), Val(Float(3.0)), NOp(Add)))"
        );
        // Sub
        assert_eq!(
            presult_to_string(&parse_sexpr("1 - 2.0")),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), NOp(Sub)))"
        );
        assert_eq!(
            presult_to_string(&parse_sexpr("1.0 - 2 - 3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Int(2)), NOp(Sub)), Val(Float(3.0)), NOp(Sub)))"
        );
        // Mul
        assert_eq!(
            presult_to_string(&parse_sexpr("1 * 2.0")),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), NOp(Mul)))"
        );
        assert_eq!(
            presult_to_string(&parse_sexpr("1.0 * 2 * 3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Int(2)), NOp(Mul)), Val(Float(3.0)), NOp(Mul)))"
        );
        // Div
        assert_eq!(
            presult_to_string(&parse_sexpr("1 / 2.0")),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), NOp(Div)))"
        );
    }

    #[test]
    fn test_integer_exprs() {
        // Add
        assert_eq!(presult_to_string(&parse_sexpr("0")), "Ok(Val(Int(0)))");
        assert_eq!(
            presult_to_string(&parse_sexpr("  1 +2  ")),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(" 1  + 2 +3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), Val(Int(3)), NOp(Add)))"
        );
        // Sub
        assert_eq!(
            presult_to_string(&parse_sexpr("  1 -2  ")),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), NOp(Sub)))"
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(" 1  - 2 -3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Sub)), Val(Int(3)), NOp(Sub)))"
        );
        // Mul
        assert_eq!(
            presult_to_string(&parse_sexpr("  1 *2  ")),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)))"
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(" 1  * 2 *3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), Val(Int(3)), NOp(Mul)))"
        );
        // Div
        assert_eq!(
            presult_to_string(&parse_sexpr("  1 /2  ")),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), NOp(Div)))"
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(" 1  / 2 /3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Div)), Val(Int(3)), NOp(Div)))"
        );
        // Var
        assert_eq!(
            presult_to_string(&parse_sexpr("  x  ")),
            r#"Ok(Var(VarName::new("x")))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr("  xsss ")),
            r#"Ok(Var(VarName::new("xsss")))"#
        );
        // Time index
        assert_eq!(
            presult_to_string(&parse_sexpr("x [-1]")),
            r#"Ok(SIndex(Var(VarName::new("x")), -1))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr("x[1 ]")),
            r#"Ok(SIndex(Var(VarName::new("x")), 1))"#
        );
        // Paren
        assert_eq!(
            presult_to_string(&parse_sexpr("  (1)  ")),
            "Ok(Val(Int(1)))"
        );
        // Don't care about order of eval; care about what the AST looks like
        assert_eq!(
            presult_to_string(&parse_sexpr(" 2 + (2 + 3)")),
            "Ok(BinOp(Val(Int(2)), BinOp(Val(Int(2)), Val(Int(3)), NOp(Add)), NOp(Add)))"
        );
        // If then else
        assert_eq!(
            presult_to_string(&parse_sexpr("if true then 1 else 2")),
            "Ok(If(Val(Bool(true)), Val(Int(1)), Val(Int(2))))"
        );
        assert_eq!(
            presult_to_string(&parse_sexpr("if true then x+x else y+y")),
            r#"Ok(If(Val(Bool(true)), BinOp(Var(VarName::new("x")), Var(VarName::new("x")), NOp(Add)), BinOp(Var(VarName::new("y")), Var(VarName::new("y")), NOp(Add))))"#
        );

        // ChatGPT generated tests with mixed arithmetic and parentheses iexprs. It only had knowledge of the tests above.
        // Basic mixed addition and multiplication
        assert_eq!(
            presult_to_string(&parse_sexpr("1 + 2 * 3")),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), Val(Int(3)), NOp(Mul)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&parse_sexpr("1 * 2 + 3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), Val(Int(3)), NOp(Add)))"
        );
        // Mixed addition, subtraction, and multiplication
        assert_eq!(
            presult_to_string(&parse_sexpr("1 + 2 * 3 - 4")),
            "Ok(BinOp(BinOp(Val(Int(1)), BinOp(Val(Int(2)), Val(Int(3)), NOp(Mul)), NOp(Add)), Val(Int(4)), NOp(Sub)))"
        );
        assert_eq!(
            presult_to_string(&parse_sexpr("1 * 2 + 3 - 4")),
            "Ok(BinOp(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), Val(Int(3)), NOp(Add)), Val(Int(4)), NOp(Sub)))"
        );
        // Mixed addition and division
        assert_eq!(
            presult_to_string(&parse_sexpr("10 + 20 / 5")),
            "Ok(BinOp(Val(Int(10)), BinOp(Val(Int(20)), Val(Int(5)), NOp(Div)), NOp(Add)))"
        );
        // Nested parentheses with mixed operations
        assert_eq!(
            presult_to_string(&parse_sexpr("(1 + 2) * (3 - 4)")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Sub)), NOp(Mul)))"
        );
        assert_eq!(
            presult_to_string(&parse_sexpr("1 + (2 * (3 + 4))")),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Add)), NOp(Mul)), NOp(Add)))"
        );
        // Complex nested expressions
        assert_eq!(
            presult_to_string(&parse_sexpr("((1 + 2) * 3) + (4 / (5 - 6))")),
            "Ok(BinOp(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), Val(Int(3)), NOp(Mul)), BinOp(Val(Int(4)), BinOp(Val(Int(5)), Val(Int(6)), NOp(Sub)), NOp(Div)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&parse_sexpr("(1 + (2 * (3 - (4 / 5))))")),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), BinOp(Val(Int(3)), BinOp(Val(Int(4)), Val(Int(5)), NOp(Div)), NOp(Sub)), NOp(Mul)), NOp(Add)))"
        );
        // More complex expressions with deep nesting
        assert_eq!(
            presult_to_string(&parse_sexpr("((1 + 2) * (3 + 4))")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Add)), NOp(Mul)))"
        );
        assert_eq!(
            presult_to_string(&parse_sexpr("((1 * 2) + (3 * 4)) / 5")),
            "Ok(BinOp(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Mul)), NOp(Add)), Val(Int(5)), NOp(Div)))"
        );
        // Multiple levels of nested expressions
        assert_eq!(
            presult_to_string(&parse_sexpr("1 + (2 * (3 + (4 / (5 - 6))))")),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), BinOp(Val(Int(3)), BinOp(Val(Int(4)), BinOp(Val(Int(5)), Val(Int(6)), NOp(Sub)), NOp(Div)), NOp(Add)), NOp(Mul)), NOp(Add)))"
        );

        // ChatGPT generated tests with mixed iexprs. It only had knowledge of the tests above.
        // Mixing addition, subtraction, and variables
        assert_eq!(
            presult_to_string(&parse_sexpr("x + 2 - y")),
            r#"Ok(BinOp(BinOp(Var(VarName::new("x")), Val(Int(2)), NOp(Add)), Var(VarName::new("y")), NOp(Sub)))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr("(x + y) * 3")),
            r#"Ok(BinOp(BinOp(Var(VarName::new("x")), Var(VarName::new("y")), NOp(Add)), Val(Int(3)), NOp(Mul)))"#
        );
        // Nested arithmetic with variables and parentheses
        assert_eq!(
            presult_to_string(&parse_sexpr("(a + b) / (c - d)")),
            r#"Ok(BinOp(BinOp(Var(VarName::new("a")), Var(VarName::new("b")), NOp(Add)), BinOp(Var(VarName::new("c")), Var(VarName::new("d")), NOp(Sub)), NOp(Div)))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr("x * (y + 3) - z / 2")),
            r#"Ok(BinOp(BinOp(Var(VarName::new("x")), BinOp(Var(VarName::new("y")), Val(Int(3)), NOp(Add)), NOp(Mul)), BinOp(Var(VarName::new("z")), Val(Int(2)), NOp(Div)), NOp(Sub)))"#
        );
        // If-then-else with mixed arithmetic
        assert_eq!(
            presult_to_string(&parse_sexpr("if true then 1 + 2 else 3 * 4")),
            "Ok(If(Val(Bool(true)), BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Mul))))"
        );
        // Time index in arithmetic expression
        assert_eq!(
            presult_to_string(&parse_sexpr("x[0] + y[-1]")),
            r#"Ok(BinOp(SIndex(Var(VarName::new("x")), 0), SIndex(Var(VarName::new("y")), -1), NOp(Add)))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr("x[1] * (y + 3)")),
            r#"Ok(BinOp(SIndex(Var(VarName::new("x")), 1), BinOp(Var(VarName::new("y")), Val(Int(3)), NOp(Add)), NOp(Mul)))"#
        );
        // Case to test precedence of if-then-else with arithmetic
        // Most languages implement this as "if a then b else (c + d)" and so should we.
        // Programmers can write "(if a then b else c) + d" if they want the other behavior.
        assert_eq!(
            presult_to_string(&parse_sexpr("if a then b else c + d")),
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
            presult_to_string(&parse_sexpr("1 + if a then b else c")),
            r#""#
        );
    }

    #[test]
    fn test_assignment_decl() {
        assert_eq!(
            presult_to_string(&parse_stopdecl("x = 0")),
            r#"Ok(Assignment(VarName::new("x"), Val(Int(0))))"#
        );
        assert_eq!(
            presult_to_string(&parse_stopdecl(r#"x = "hello""#)),
            r#"Ok(Assignment(VarName::new("x"), Val(Str("hello"))))"#
        );
        assert_eq!(
            presult_to_string(&parse_stopdecl("x = true")),
            r#"Ok(Assignment(VarName::new("x"), Val(Bool(true))))"#
        );
        assert_eq!(
            presult_to_string(&parse_stopdecl("x = false")),
            r#"Ok(Assignment(VarName::new("x"), Val(Bool(false))))"#
        );
    }

    #[test]
    fn test_parse_empty_string() {
        let res = parse_str("");
        assert!(res.is_ok());
        let res = res.unwrap();
        assert_eq!(
            res,
            LOLASpecification::new(vec![], vec![], BTreeMap::new(), BTreeMap::new(), vec![])
        );
    }

    #[test]
    fn test_parse_invalid_expression() {
        let res = parse_sexpr("1 +");
        assert_eq!(res.is_err(), true);
        let err = res.err().unwrap();
        assert!(err.to_string().contains("Parse error"));

        let res = parse_sexpr("&& true");
        assert_eq!(res.is_err(), true);
        let err = res.err().unwrap();
        assert!(err.to_string().contains("Parse error"));
    }

    #[test]
    fn test_parse_boolean_expressions() {
        assert_eq!(
            presult_to_string(&parse_sexpr("true && false")),
            "Ok(BinOp(Val(Bool(true)), Val(Bool(false)), BOp(And)))"
        );
        assert_eq!(
            presult_to_string(&parse_sexpr("true || false")),
            "Ok(BinOp(Val(Bool(true)), Val(Bool(false)), BOp(Or)))"
        );
    }

    #[test]
    fn test_parse_mixed_boolean_and_arithmetic() {
        // Expressions do not make sense but parser should allow it
        assert_eq!(
            presult_to_string(&parse_sexpr("1 + 2 && 3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), Val(Int(3)), BOp(And)))"
        );
        assert_eq!(
            presult_to_string(&parse_sexpr("true || 1 * 2")),
            "Ok(BinOp(Val(Bool(true)), BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), BOp(Or)))"
        );
    }

    #[test]
    fn test_parse_string_concatenation() {
        assert_eq!(
            presult_to_string(&parse_sexpr(r#""foo" ++ "bar""#)),
            r#"Ok(BinOp(Val(Str("foo")), Val(Str("bar")), SOp(Concat)))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#""hello" ++ " " ++ "world""#)),
            r#"Ok(BinOp(BinOp(Val(Str("hello")), Val(Str(" ")), SOp(Concat)), Val(Str("world")), SOp(Concat)))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#""a" ++ "b" ++ "c""#)),
            r#"Ok(BinOp(BinOp(Val(Str("a")), Val(Str("b")), SOp(Concat)), Val(Str("c")), SOp(Concat)))"#
        );
    }

    #[test]
    fn test_parse_defer() {
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"defer(x)"#)),
            r#"Ok(Defer(Var(VarName::new("x"))))"#
        )
    }

    #[test]
    fn test_parse_update() {
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"update(x, y)"#)),
            r#"Ok(Update(Var(VarName::new("x")), Var(VarName::new("y"))))"#
        )
    }

    #[test]
    fn test_parse_default() {
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"default(x, 0)"#)),
            r#"Ok(Default(Var(VarName::new("x")), Val(Int(0))))"#
        )
    }

    #[test]
    fn test_parse_default_parse_sexpr() {
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"default(x, y)"#)),
            r#"Ok(Default(Var(VarName::new("x")), Var(VarName::new("y"))))"#
        )
    }

    #[test]
    fn test_parse_list() {
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"List()"#)),
            r#"Ok(List([]))"#,
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"List () "#)),
            r#"Ok(List([]))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"List(1,2)"#)),
            r#"Ok(List([Val(Int(1)), Val(Int(2))]))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"List(1+2,2*5)"#)),
            r#"Ok(List([BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), BinOp(Val(Int(2)), Val(Int(5)), NOp(Mul))]))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"List("hello","world")"#)),
            r#"Ok(List([Val(Str("hello")), Val(Str("world"))]))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"List(true || false, true && false)"#)),
            r#"Ok(List([BinOp(Val(Bool(true)), Val(Bool(false)), BOp(Or)), BinOp(Val(Bool(true)), Val(Bool(false)), BOp(And))]))"#
        );
        // Can mix expressions - not that it is necessarily a good idea
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"List(1,"hello")"#)),
            r#"Ok(List([Val(Int(1)), Val(Str("hello"))]))"#
        );
        assert_eq!(
            presult_to_string(&parse_stopdecl("y = List()")),
            r#"Ok(Assignment(VarName::new("y"), List([])))"#
        )
    }

    #[test]
    fn test_parse_lindex() {
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"List.get(List(1, 2), 42)"#)),
            r#"Ok(LIndex(List([Val(Int(1)), Val(Int(2))]), Val(Int(42))))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"List.get(x, 42)"#)),
            r#"Ok(LIndex(Var(VarName::new("x")), Val(Int(42))))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"List.get(x, 1+2)"#)),
            r#"Ok(LIndex(Var(VarName::new("x")), BinOp(Val(Int(1)), Val(Int(2)), NOp(Add))))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(
                r#"List.get(List.get(List(List(1, 2), List(3, 4)), 0), 1)"#
            )),
            r#"Ok(LIndex(LIndex(List([List([Val(Int(1)), Val(Int(2))]), List([Val(Int(3)), Val(Int(4))])]), Val(Int(0))), Val(Int(1))))"#
        );
    }

    #[test]
    fn test_parse_lconcat() {
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"List.concat(List(1, 2), List(3, 4))"#)),
            r#"Ok(LConcat(List([Val(Int(1)), Val(Int(2))]), List([Val(Int(3)), Val(Int(4))])))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"List.concat(List(), List())"#)),
            r#"Ok(LConcat(List([]), List([])))"#
        );
    }

    #[test]
    fn test_parse_lappend() {
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"List.append(List(1, 2), 3)"#)),
            r#"Ok(LAppend(List([Val(Int(1)), Val(Int(2))]), Val(Int(3))))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"List.append(List(), 3)"#)),
            r#"Ok(LAppend(List([]), Val(Int(3))))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"List.append(List(), x)"#)),
            r#"Ok(LAppend(List([]), Var(VarName::new("x"))))"#
        );
    }

    #[test]
    fn test_parse_lhead() {
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"List.head(List(1, 2))"#)),
            r#"Ok(LHead(List([Val(Int(1)), Val(Int(2))])))"#
        );
        // Ok for parser but will result in runtime error:
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"List.head(List())"#)),
            r#"Ok(LHead(List([])))"#
        );
    }

    #[test]
    fn test_parse_ltail() {
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"List.tail(List(1, 2))"#)),
            r#"Ok(LTail(List([Val(Int(1)), Val(Int(2))])))"#
        );
        // Ok for parser but will result in runtime error:
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"List.tail(List())"#)),
            r#"Ok(LTail(List([])))"#
        );
    }

    #[test]
    fn test_parse_llen() {
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"List.len(List(1, 2))"#)),
            r#"Ok(LLen(List([Val(Int(1)), Val(Int(2))])))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"List.len(List())"#)),
            r#"Ok(LLen(List([])))"#
        );
    }

    #[test]
    fn test_parse_map() {
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"Map()"#)),
            r#"Ok(Map({}))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"Map("x": 1, "y": 2)"#)),
            r#"Ok(Map({"x": Val(Int(1)), "y": Val(Int(2))}))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"Map("x": 1+2,"y": 2*5)"#)),
            r#"Ok(Map({"x": BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), "y": BinOp(Val(Int(2)), Val(Int(5)), NOp(Mul))}))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"Map("x": "hello", "y": "world")"#)),
            r#"Ok(Map({"x": Val(Str("hello")), "y": Val(Str("world"))}))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(
                r#"Map("xxxx": true || false, "yyyy": true && false)"#
            )),
            r#"Ok(Map({"xxxx": BinOp(Val(Bool(true)), Val(Bool(false)), BOp(Or)), "yyyy": BinOp(Val(Bool(true)), Val(Bool(false)), BOp(And))}))"#
        );
        // Can mix expressions - not that it is necessarily a good idea
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"Map( "x": 1, "y": "hello" )"#)),
            r#"Ok(Map({"x": Val(Int(1)), "y": Val(Str("hello"))}))"#
        );
        assert_eq!(
            presult_to_string(&parse_stopdecl("y = Map()")),
            r#"Ok(Assignment(VarName::new("y"), Map({})))"#
        )
    }

    #[test]
    fn test_parse_mget() {
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"Map.get(Map("x": 2, "y": true), "x")"#)),
            r#"Ok(MGet(Map({"x": Val(Int(2)), "y": Val(Bool(true))}), "x"))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"Map.get(x, "key")"#)),
            r#"Ok(MGet(Var(VarName::new("x")), "key"))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"Map.get(x, "")"#)),
            r#"Ok(MGet(Var(VarName::new("x")), ""))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(
                r#"Map.get(Map.get(Map.get(Map("three": Map("two": Map("one": 42))), "three"), "two"), "one")"#
            )),
            r#"Ok(MGet(MGet(MGet(Map({"three": Map({"two": Map({"one": Val(Int(42))})})}), "three"), "two"), "one"))"#
        );
    }

    #[test]
    fn test_parse_mremove() {
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"Map.remove(Map("x": 2, "y": true), "x")"#)),
            r#"Ok(MRemove(Map({"x": Val(Int(2)), "y": Val(Bool(true))}), "x"))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"Map.remove(x, "key")"#)),
            r#"Ok(MRemove(Var(VarName::new("x")), "key"))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"Map.remove(x, "")"#)),
            r#"Ok(MRemove(Var(VarName::new("x")), ""))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(
                r#"Map.remove(Map.remove(Map.remove(Map("three": Map("two": Map("one": 42))), "three"), "two"), "one")"#
            )),
            r#"Ok(MRemove(MRemove(MRemove(Map({"three": Map({"two": Map({"one": Val(Int(42))})})}), "three"), "two"), "one"))"#
        );
    }

    #[test]
    fn test_parse_mhas_key() {
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"Map.has_key(Map("x": 2, "y": true), "x")"#)),
            r#"Ok(MHasKey(Map({"x": Val(Int(2)), "y": Val(Bool(true))}), "x"))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"Map.has_key(x, "key")"#)),
            r#"Ok(MHasKey(Var(VarName::new("x")), "key"))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"Map.has_key(x, "")"#)),
            r#"Ok(MHasKey(Var(VarName::new("x")), ""))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(
                r#"Map.has_key(Map.has_key(Map.has_key(Map("three": Map("two": Map("one": 42))), "three"), "two"), "one")"#
            )),
            r#"Ok(MHasKey(MHasKey(MHasKey(Map({"three": Map({"two": Map({"one": Val(Int(42))})})}), "three"), "two"), "one"))"#
        );
    }

    #[test]
    fn test_parse_minsert() {
        assert_eq!(
            presult_to_string(&parse_sexpr(
                r#"Map.insert(Map("x": 2, "y": true), "z", 42)"#
            )),
            r#"Ok(MInsert(Map({"x": Val(Int(2)), "y": Val(Bool(true))}), "z", Val(Int(42))))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"Map.insert(x, "key", true)"#)),
            r#"Ok(MInsert(Var(VarName::new("x")), "key", Val(Bool(true))))"#
        );
        assert_eq!(
            presult_to_string(&parse_sexpr(r#"Map.insert(x, "", 1)"#)),
            r#"Ok(MInsert(Var(VarName::new("x")), "", Val(Int(1))))"#
        );
    }

    #[test]
    fn test_dangling_else() {
        assert_eq!(
            presult_to_string(&parse_sexpr("if a then b else c + d")),
            r#"Ok(If(Var(VarName::new("a")), Var(VarName::new("b")), BinOp(Var(VarName::new("c")), Var(VarName::new("d")), NOp(Add))))"#
        )
    }
}
