use std::collections::BTreeMap;

use anyhow::{Error, anyhow};
use tracing::warn;

use super::lalr::{ExprParser, StmtParser, StmtsParser};
use crate::{LOLASpecification, SExpr, lang::dynamic_lola::ast::SStmt};

pub fn parse_sexpr<'input>(input: &'input str) -> Result<SExpr, Error> {
    ExprParser::new()
        .parse(input)
        .map_err(|e| anyhow!("Parse error: {:?}", e))
}

pub fn parse_sstmt<'input>(input: &'input str) -> Result<SStmt, Error> {
    StmtParser::new()
        .parse(input)
        .map_err(|e| anyhow!("Parse error: {:?}", e))
}

pub async fn parse_file<'file>(file: &'file str) -> anyhow::Result<LOLASpecification> {
    warn!(
        "Use of LALR parser is incomplete, experimental and currently hardcoded for DynSRV specifications"
    );
    let contents = smol::fs::read_to_string(file).await?;
    let stmts = StmtsParser::new().parse(&contents).map_err(|e| {
        anyhow::anyhow!(e.to_string()).context(format!("Failed to parse file {}", file))
    })?;
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();
    let mut assignments = BTreeMap::new();

    for stmt in &stmts {
        match stmt {
            SStmt::Input(var) => {
                inputs.push(var.clone());
            }
            SStmt::Output(var) => {
                outputs.push(var.clone());
            }
            SStmt::Assignment(var, sexpr) => {
                assignments.insert(var.clone(), sexpr.clone());
            }
        }
    }

    Ok(LOLASpecification {
        input_vars: inputs,
        output_vars: outputs,
        exprs: assignments,
        type_annotations: BTreeMap::new(), // TODO: No support for type annotations...
    })
}

#[cfg(test)]
mod tests {
    use crate::lang::core::parser::presult_to_string;

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
        let parsed = parse_sstmt("in x");
        let exp = "Ok(Input(VarName::new(\"x\")))";
        assert_eq!(presult_to_string(&parsed), exp);
    }
}
