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
        let parsed = parse_stopdecl("in x");
        let exp = "Ok(Input(VarName::new(\"x\"), None))";
        assert_eq!(presult_to_string(&parsed), exp);
    }

    #[test]
    fn test_typed_input_decl() {
        let parsed = parse_stopdecl("in x: Int");
        let exp = "Ok(Input(VarName::new(\"x\"), Some(Int)))";
        assert_eq!(presult_to_string(&parsed), exp);

        let parsed = parse_stopdecl("in x: Float");
        let exp = "Ok(Input(VarName::new(\"x\"), Some(Float)))";
        assert_eq!(presult_to_string(&parsed), exp);
    }

    #[test]
    fn test_input_decls() {
        let parsed = parse_stopdecls("");
        let exp = "Ok([])";
        assert_eq!(presult_to_string(&parsed), exp);

        let parsed = parse_stopdecls("in x");
        let exp = r#"Ok([Input(VarName::new("x"), None)])"#;
        assert_eq!(presult_to_string(&parsed), exp);

        let parsed = parse_stopdecls("in x\nin y");
        let exp = r#"Ok([Input(VarName::new("x"), None), Input(VarName::new("y"), None)])"#;
        assert_eq!(presult_to_string(&parsed), exp);
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
}
