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
}
