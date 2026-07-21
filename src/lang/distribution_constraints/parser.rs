use lalrpop_util::lalrpop_mod;

use super::ast::{DistConstraint, DistConstraintExpr};
use crate::core::VarName;

lalrpop_mod!(lalr, "/lang/distribution_constraints/parser.rs");

pub type Result<T> = std::result::Result<T, String>;

pub fn parse_expression(input: &str) -> Result<DistConstraintExpr> {
    lalr::DistConstraintBodyParser::new()
        .parse(input)
        .map_err(|error| error.to_string())
}

pub fn parse_constraint(input: &str) -> Result<(VarName, DistConstraint)> {
    lalr::DistConstraintParser::new()
        .parse(input)
        .map_err(|error| error.to_string())
}

pub fn parse_constraints(input: &str) -> Result<Vec<(VarName, DistConstraint)>> {
    lalr::DistConstraintsParser::new()
        .parse(input)
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use crate::core::Value;
    use crate::lang::distribution_constraints::ast::{
        CompBinOp, DistConstraintType, NumericalBinOp, SBinOp,
    };

    use super::*;

    fn presult_to_string<T: std::fmt::Debug, E: std::fmt::Debug>(
        result: &std::result::Result<T, E>,
    ) -> String {
        format!("{result:?}")
    }
    use test_log::test;

    #[test]
    fn test_parse_expression_source() -> Result<()> {
        let input = "source(x)";
        assert_eq!(
            parse_expression(input)?,
            DistConstraintExpr::Source("x".into())
        );
        Ok(())
    }

    #[test]
    fn test_parse_constraint_source() -> Result<()> {
        let input = "can_run x: source(y)";
        assert_eq!(
            parse_constraint(input)?,
            (
                "x".into(),
                DistConstraint(
                    DistConstraintType::CanRun,
                    DistConstraintExpr::Source("y".into())
                )
            )
        );
        Ok(())
    }

    #[test]
    fn test_parse_constraints_sources() -> Result<()> {
        let input = "can_run x: source(y)\n\
            can_run z: source(w)";
        assert_eq!(
            parse_constraints(input)?,
            vec![
                (
                    "x".into(),
                    DistConstraint(
                        DistConstraintType::CanRun,
                        DistConstraintExpr::Source("y".into())
                    )
                ),
                (
                    "z".into(),
                    DistConstraint(
                        DistConstraintType::CanRun,
                        DistConstraintExpr::Source("w".into())
                    )
                )
            ]
        );
        Ok(())
    }

    #[test]
    fn test_streamdata() {
        for (source, value) in [
            ("42", Value::Int(42)),
            ("42.0", Value::Float(42.0)),
            ("1e-1", Value::Float(1e-1)),
            ("\"abc2d\"", Value::Str("abc2d".into())),
            ("true", Value::Bool(true)),
            ("false", Value::Bool(false)),
            ("\"x+y\"", Value::Str("x+y".into())),
        ] {
            assert_eq!(parse_expression(source), Ok(DistConstraintExpr::Val(value)),);
        }
    }

    #[test]
    fn test_parse_expression() -> Result<()> {
        assert_eq!(
            parse_expression("1 + 2")?,
            DistConstraintExpr::BinOp(
                Box::new(DistConstraintExpr::Val(Value::Int(1))),
                Box::new(DistConstraintExpr::Val(Value::Int(2))),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
        );
        assert_eq!(
            parse_expression("1 + 2 * 3")?,
            DistConstraintExpr::BinOp(
                Box::new(DistConstraintExpr::Val(Value::Int(1))),
                Box::new(DistConstraintExpr::BinOp(
                    Box::new(DistConstraintExpr::Val(Value::Int(2))),
                    Box::new(DistConstraintExpr::Val(Value::Int(3))),
                    SBinOp::NOp(NumericalBinOp::Mul),
                )),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
        );
        assert_eq!(
            parse_expression("x + (y + 2)")?,
            DistConstraintExpr::BinOp(
                Box::new(DistConstraintExpr::Var("x".into())),
                Box::new(DistConstraintExpr::BinOp(
                    Box::new(DistConstraintExpr::Var("y".into())),
                    Box::new(DistConstraintExpr::Val(Value::Int(2))),
                    SBinOp::NOp(NumericalBinOp::Add),
                )),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
        );
        assert_eq!(
            parse_expression("if true then 1 else 2")?,
            DistConstraintExpr::If(
                Box::new(DistConstraintExpr::Val(true.into())),
                Box::new(DistConstraintExpr::Val(Value::Int(1))),
                Box::new(DistConstraintExpr::Val(Value::Int(2))),
            ),
        );
        assert_eq!(
            parse_expression("(x)[-1, 0]")?,
            DistConstraintExpr::SIndex(
                Box::new(DistConstraintExpr::Var("x".into())),
                -1,
                Value::Int(0),
            ),
        );
        assert_eq!(
            parse_expression("(x + y)[-3, 2]")?,
            DistConstraintExpr::SIndex(
                Box::new(DistConstraintExpr::BinOp(
                    Box::new(DistConstraintExpr::Var("x".into())),
                    Box::new(DistConstraintExpr::Var("y".into()),),
                    SBinOp::NOp(NumericalBinOp::Add),
                )),
                -3,
                Value::Int(2),
            ),
        );
        assert_eq!(
            parse_expression("1 + (x)[-1, 0]")?,
            DistConstraintExpr::BinOp(
                Box::new(DistConstraintExpr::Val(Value::Int(1))),
                Box::new(DistConstraintExpr::SIndex(
                    Box::new(DistConstraintExpr::Var("x".into())),
                    -1,
                    Value::Int(0),
                ),),
                SBinOp::NOp(NumericalBinOp::Add),
            )
        );
        assert_eq!(
            parse_expression("\"test\"")?,
            DistConstraintExpr::Val(Value::Str("test".into())),
        );
        assert_eq!(
            parse_expression("(stage == \"m\")")?,
            DistConstraintExpr::BinOp(
                Box::new(DistConstraintExpr::Var("stage".into())),
                Box::new(DistConstraintExpr::Val("m".into())),
                SBinOp::COp(CompBinOp::Eq),
            )
        );
        Ok(())
    }

    #[test]
    fn test_float_exprs() {
        // Add
        assert_eq!(
            presult_to_string(&parse_expression("0.0")),
            "Ok(Val(Float(0.0)))"
        );
        assert_eq!(
            presult_to_string(&parse_expression("  1.0 +2.0  ")),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&parse_expression(" 1.0  + 2.0 +3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Add)), Val(Float(3.0)), NOp(Add)))"
        );
        // Sub
        assert_eq!(
            presult_to_string(&parse_expression("  1.0 -2.0  ")),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Sub)))"
        );
        assert_eq!(
            presult_to_string(&parse_expression(" 1.0  - 2.0 -3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Sub)), Val(Float(3.0)), NOp(Sub)))"
        );
        // Mul
        assert_eq!(
            presult_to_string(&parse_expression("  1.0 *2.0  ")),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Mul)))"
        );
        assert_eq!(
            presult_to_string(&parse_expression(" 1.0  * 2.0 *3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Mul)), Val(Float(3.0)), NOp(Mul)))"
        );
        // Div
        assert_eq!(
            presult_to_string(&parse_expression("  1.0 /2.0  ")),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), NOp(Div)))"
        );
    }

    #[test]
    fn test_mixed_float_int_exprs() {
        // Add
        assert_eq!(
            presult_to_string(&parse_expression("0.0 + 2")),
            "Ok(BinOp(Val(Float(0.0)), Val(Int(2)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&parse_expression("1 + 2.0")),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&parse_expression("1.0 + 2 + 3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Int(2)), NOp(Add)), Val(Float(3.0)), NOp(Add)))"
        );
        // Sub
        assert_eq!(
            presult_to_string(&parse_expression("1 - 2.0")),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), NOp(Sub)))"
        );
        assert_eq!(
            presult_to_string(&parse_expression("1.0 - 2 - 3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Int(2)), NOp(Sub)), Val(Float(3.0)), NOp(Sub)))"
        );
        // Mul
        assert_eq!(
            presult_to_string(&parse_expression("1 * 2.0")),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), NOp(Mul)))"
        );
        assert_eq!(
            presult_to_string(&parse_expression("1.0 * 2 * 3.0")),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Int(2)), NOp(Mul)), Val(Float(3.0)), NOp(Mul)))"
        );
        // Div
        assert_eq!(
            presult_to_string(&parse_expression("1 / 2.0")),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), NOp(Div)))"
        );
    }

    #[test]
    fn test_integer_exprs() {
        // Add
        assert_eq!(presult_to_string(&parse_expression("0")), "Ok(Val(Int(0)))");
        assert_eq!(
            presult_to_string(&parse_expression("  1 +2  ")),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&parse_expression(" 1  + 2 +3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), Val(Int(3)), NOp(Add)))"
        );
        // Sub
        assert_eq!(
            presult_to_string(&parse_expression("  1 -2  ")),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), NOp(Sub)))"
        );
        assert_eq!(
            presult_to_string(&parse_expression(" 1  - 2 -3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Sub)), Val(Int(3)), NOp(Sub)))"
        );
        // Mul
        assert_eq!(
            presult_to_string(&parse_expression("  1 *2  ")),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)))"
        );
        assert_eq!(
            presult_to_string(&parse_expression(" 1  * 2 *3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), Val(Int(3)), NOp(Mul)))"
        );
        // Div
        assert_eq!(
            presult_to_string(&parse_expression("  1 /2  ")),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), NOp(Div)))"
        );
        assert_eq!(
            presult_to_string(&parse_expression(" 1  / 2 /3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Div)), Val(Int(3)), NOp(Div)))"
        );
        // Var
        assert_eq!(
            presult_to_string(&parse_expression("  x  ")),
            r#"Ok(Var(VarName::new("x")))"#
        );
        assert_eq!(
            presult_to_string(&parse_expression("  xsss ")),
            r#"Ok(Var(VarName::new("xsss")))"#
        );
        // Time index
        assert_eq!(
            presult_to_string(&parse_expression("x [-1, 0 ]")),
            r#"Ok(SIndex(Var(VarName::new("x")), -1, Int(0)))"#
        );
        assert_eq!(
            presult_to_string(&parse_expression("x[1,0]")),
            r#"Ok(SIndex(Var(VarName::new("x")), 1, Int(0)))"#
        );
        // Paren
        assert_eq!(
            presult_to_string(&parse_expression("  (1)  ")),
            "Ok(Val(Int(1)))"
        );
        // Don't care about order of eval; care about what the AST looks like
        assert_eq!(
            presult_to_string(&parse_expression(" 2 + (2 + 3)")),
            "Ok(BinOp(Val(Int(2)), BinOp(Val(Int(2)), Val(Int(3)), NOp(Add)), NOp(Add)))"
        );
        // If then else
        assert_eq!(
            presult_to_string(&parse_expression("if true then 1 else 2")),
            "Ok(If(Val(Bool(true)), Val(Int(1)), Val(Int(2))))"
        );
        assert_eq!(
            presult_to_string(&parse_expression("if true then x+x else y+y")),
            r#"Ok(If(Val(Bool(true)), BinOp(Var(VarName::new("x")), Var(VarName::new("x")), NOp(Add)), BinOp(Var(VarName::new("y")), Var(VarName::new("y")), NOp(Add))))"#
        );

        // ChatGPT generated tests with mixed arithmetic and parentheses iexprs. It only had knowledge of the tests above.
        // Basic mixed addition and multiplication
        assert_eq!(
            presult_to_string(&parse_expression("1 + 2 * 3")),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), Val(Int(3)), NOp(Mul)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&parse_expression("1 * 2 + 3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), Val(Int(3)), NOp(Add)))"
        );
        // Mixed addition, subtraction, and multiplication
        assert_eq!(
            presult_to_string(&parse_expression("1 + 2 * 3 - 4")),
            "Ok(BinOp(BinOp(Val(Int(1)), BinOp(Val(Int(2)), Val(Int(3)), NOp(Mul)), NOp(Add)), Val(Int(4)), NOp(Sub)))"
        );
        assert_eq!(
            presult_to_string(&parse_expression("1 * 2 + 3 - 4")),
            "Ok(BinOp(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), Val(Int(3)), NOp(Add)), Val(Int(4)), NOp(Sub)))"
        );
        // Mixed addition and division
        assert_eq!(
            presult_to_string(&parse_expression("10 + 20 / 5")),
            "Ok(BinOp(Val(Int(10)), BinOp(Val(Int(20)), Val(Int(5)), NOp(Div)), NOp(Add)))"
        );
        // Nested parentheses with mixed operations
        assert_eq!(
            presult_to_string(&parse_expression("(1 + 2) * (3 - 4)")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Sub)), NOp(Mul)))"
        );
        assert_eq!(
            presult_to_string(&parse_expression("1 + (2 * (3 + 4))")),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Add)), NOp(Mul)), NOp(Add)))"
        );
        // Complex nested expressions
        assert_eq!(
            presult_to_string(&parse_expression("((1 + 2) * 3) + (4 / (5 - 6))")),
            "Ok(BinOp(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), Val(Int(3)), NOp(Mul)), BinOp(Val(Int(4)), BinOp(Val(Int(5)), Val(Int(6)), NOp(Sub)), NOp(Div)), NOp(Add)))"
        );
        assert_eq!(
            presult_to_string(&parse_expression("(1 + (2 * (3 - (4 / 5))))")),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), BinOp(Val(Int(3)), BinOp(Val(Int(4)), Val(Int(5)), NOp(Div)), NOp(Sub)), NOp(Mul)), NOp(Add)))"
        );
        // More complex expressions with deep nesting
        assert_eq!(
            presult_to_string(&parse_expression("((1 + 2) * (3 + 4))")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Add)), NOp(Mul)))"
        );
        assert_eq!(
            presult_to_string(&parse_expression("((1 * 2) + (3 * 4)) / 5")),
            "Ok(BinOp(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Mul)), NOp(Add)), Val(Int(5)), NOp(Div)))"
        );
        // Multiple levels of nested expressions
        assert_eq!(
            presult_to_string(&parse_expression("1 + (2 * (3 + (4 / (5 - 6))))")),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), BinOp(Val(Int(3)), BinOp(Val(Int(4)), BinOp(Val(Int(5)), Val(Int(6)), NOp(Sub)), NOp(Div)), NOp(Add)), NOp(Mul)), NOp(Add)))"
        );

        // ChatGPT generated tests with mixed iexprs. It only had knowledge of the tests above.
        // Mixing addition, subtraction, and variables
        assert_eq!(
            presult_to_string(&parse_expression("x + 2 - y")),
            r#"Ok(BinOp(BinOp(Var(VarName::new("x")), Val(Int(2)), NOp(Add)), Var(VarName::new("y")), NOp(Sub)))"#
        );
        assert_eq!(
            presult_to_string(&parse_expression("(x + y) * 3")),
            r#"Ok(BinOp(BinOp(Var(VarName::new("x")), Var(VarName::new("y")), NOp(Add)), Val(Int(3)), NOp(Mul)))"#
        );
        // Nested arithmetic with variables and parentheses
        assert_eq!(
            presult_to_string(&parse_expression("(a + b) / (c - d)")),
            r#"Ok(BinOp(BinOp(Var(VarName::new("a")), Var(VarName::new("b")), NOp(Add)), BinOp(Var(VarName::new("c")), Var(VarName::new("d")), NOp(Sub)), NOp(Div)))"#
        );
        assert_eq!(
            presult_to_string(&parse_expression("x * (y + 3) - z / 2")),
            r#"Ok(BinOp(BinOp(Var(VarName::new("x")), BinOp(Var(VarName::new("y")), Val(Int(3)), NOp(Add)), NOp(Mul)), BinOp(Var(VarName::new("z")), Val(Int(2)), NOp(Div)), NOp(Sub)))"#
        );
        // If-then-else with mixed arithmetic
        assert_eq!(
            presult_to_string(&parse_expression("if true then 1 + 2 else 3 * 4")),
            "Ok(If(Val(Bool(true)), BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), BinOp(Val(Int(3)), Val(Int(4)), NOp(Mul))))"
        );
        // Time index in arithmetic expression
        assert_eq!(
            presult_to_string(&parse_expression("x[0, 1] + y[-1, 0]")),
            r#"Ok(BinOp(SIndex(Var(VarName::new("x")), 0, Int(1)), SIndex(Var(VarName::new("y")), -1, Int(0)), NOp(Add)))"#
        );
        assert_eq!(
            presult_to_string(&parse_expression("x[1, 2] * (y + 3)")),
            r#"Ok(BinOp(SIndex(Var(VarName::new("x")), 1, Int(2)), BinOp(Var(VarName::new("y")), Val(Int(3)), NOp(Add)), NOp(Mul)))"#
        );
        // Complex expression with nested if-then-else and mixed operations
        assert_eq!(
            presult_to_string(&parse_expression("(1 + x) * if y then 3 else z / 2")),
            r#"Ok(BinOp(BinOp(Val(Int(1)), Var(VarName::new("x")), NOp(Add)), If(Var(VarName::new("y")), Val(Int(3)), BinOp(Var(VarName::new("z")), Val(Int(2)), NOp(Div))), NOp(Mul)))"#
        );
    }

    #[test]
    fn test_nested_conditional_expressions() {
        for (source, expected) in [
            (
                "(if true then 1 else 2)",
                "Ok(If(Val(Bool(true)), Val(Int(1)), Val(Int(2))))",
            ),
            (
                "sin(if true then 1 else 2)",
                "Ok(Sin(If(Val(Bool(true)), Val(Int(1)), Val(Int(2)))))",
            ),
            (
                "default(if true then x else y, if false then 1 else 2)",
                "Ok(Default(If(Val(Bool(true)), Var(VarName::new(\"x\")), Var(VarName::new(\"y\"))), If(Val(Bool(false)), Val(Int(1)), Val(Int(2)))))",
            ),
            (
                "List(if true then 1 else 2, if false then 3 else 4)",
                "Ok(List([If(Val(Bool(true)), Val(Int(1)), Val(Int(2))), If(Val(Bool(false)), Val(Int(3)), Val(Int(4)))]))",
            ),
            (
                "1 + if true then 2 else 3",
                "Ok(BinOp(Val(Int(1)), If(Val(Bool(true)), Val(Int(2)), Val(Int(3))), NOp(Add)))",
            ),
        ] {
            assert_eq!(
                presult_to_string(&parse_expression(source)),
                expected,
                "{source}"
            );
        }
    }

    #[test]
    fn test_parse_empty_string() {
        assert!(parse_expression("").is_err());
    }

    #[test]
    fn test_parse_invalid_expression() {
        // TODO: Bug here in parser. It should be able to handle these cases.
        // assert_eq!(presult_to_string(&parse_expression("1 +")), "Err(Backtrack(ContextError { context: [], cause: None }))");
        assert!(parse_expression("&& true").is_err());
    }

    #[test]
    fn test_parse_boolean_expressions() {
        assert_eq!(
            presult_to_string(&parse_expression("true && false")),
            "Ok(BinOp(Val(Bool(true)), Val(Bool(false)), BOp(And)))"
        );
        assert_eq!(
            presult_to_string(&parse_expression("true || false")),
            "Ok(BinOp(Val(Bool(true)), Val(Bool(false)), BOp(Or)))"
        );
    }

    #[test]
    fn test_parse_mixed_boolean_and_arithmetic() {
        // Expressions do not make sense but parser should allow it
        assert_eq!(
            presult_to_string(&parse_expression("1 + 2 && 3")),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), Val(Int(3)), BOp(And)))"
        );
        assert_eq!(
            presult_to_string(&parse_expression("true || 1 * 2")),
            "Ok(BinOp(Val(Bool(true)), BinOp(Val(Int(1)), Val(Int(2)), NOp(Mul)), BOp(Or)))"
        );
    }
    #[test]
    fn test_parse_string_concatenation() {
        assert_eq!(
            presult_to_string(&parse_expression(r#""foo" ++ "bar""#)),
            r#"Ok(BinOp(Val(Str("foo")), Val(Str("bar")), SOp(Concat)))"#
        );
        assert_eq!(
            presult_to_string(&parse_expression(r#""hello" ++ " " ++ "world""#)),
            r#"Ok(BinOp(BinOp(Val(Str("hello")), Val(Str(" ")), SOp(Concat)), Val(Str("world")), SOp(Concat)))"#
        );
        assert_eq!(
            presult_to_string(&parse_expression(r#""a" ++ "b" ++ "c""#)),
            r#"Ok(BinOp(BinOp(Val(Str("a")), Val(Str("b")), SOp(Concat)), Val(Str("c")), SOp(Concat)))"#
        );
    }

    #[test]
    fn test_parse_default() {
        assert_eq!(
            presult_to_string(&parse_expression(r#"default(x, 0)"#)),
            r#"Ok(Default(Var(VarName::new("x")), Val(Int(0))))"#
        )
    }

    #[test]
    fn test_parse_default_parse_expression() {
        assert_eq!(
            presult_to_string(&parse_expression(r#"default(x, y)"#)),
            r#"Ok(Default(Var(VarName::new("x")), Var(VarName::new("y"))))"#
        )
    }

    #[test]
    fn test_parse_lindex() {
        assert_eq!(
            presult_to_string(&parse_expression(r#"List.get(List(1, 2), 42)"#)),
            r#"Ok(LIndex(List([Val(Int(1)), Val(Int(2))]), Val(Int(42))))"#
        );
        assert_eq!(
            presult_to_string(&parse_expression(r#"List.get(x, 42)"#)),
            r#"Ok(LIndex(Var(VarName::new("x")), Val(Int(42))))"#
        );
        assert_eq!(
            presult_to_string(&parse_expression(r#"List.get(x, 1+2)"#)),
            r#"Ok(LIndex(Var(VarName::new("x")), BinOp(Val(Int(1)), Val(Int(2)), NOp(Add))))"#
        );
        assert_eq!(
            presult_to_string(&parse_expression(
                r#"List.get(List.get(List(List(1, 2), List(3, 4)), 0), 1)"#
            )),
            r#"Ok(LIndex(LIndex(List([List([Val(Int(1)), Val(Int(2))]), List([Val(Int(3)), Val(Int(4))])]), Val(Int(0))), Val(Int(1))))"#
        );
    }

    #[test]
    fn test_parse_lconcat() {
        assert_eq!(
            presult_to_string(&parse_expression(r#"List.concat(List(1, 2), List(3, 4))"#)),
            r#"Ok(LConcat(List([Val(Int(1)), Val(Int(2))]), List([Val(Int(3)), Val(Int(4))])))"#
        );
        assert_eq!(
            presult_to_string(&parse_expression(r#"List.concat(List(), List())"#)),
            r#"Ok(LConcat(List([]), List([])))"#
        );
    }

    #[test]
    fn test_parse_lappend() {
        assert_eq!(
            presult_to_string(&parse_expression(r#"List.append(List(1, 2), 3)"#)),
            r#"Ok(LAppend(List([Val(Int(1)), Val(Int(2))]), Val(Int(3))))"#
        );
        assert_eq!(
            presult_to_string(&parse_expression(r#"List.append(List(), 3)"#)),
            r#"Ok(LAppend(List([]), Val(Int(3))))"#
        );
        assert_eq!(
            presult_to_string(&parse_expression(r#"List.append(List(), x)"#)),
            r#"Ok(LAppend(List([]), Var(VarName::new("x"))))"#
        );
    }

    #[test]
    fn test_parse_lhead() {
        assert_eq!(
            presult_to_string(&parse_expression(r#"List.head(List(1, 2))"#)),
            r#"Ok(LHead(List([Val(Int(1)), Val(Int(2))])))"#
        );
        // Ok for parser but will result in runtime error:
        assert_eq!(
            presult_to_string(&parse_expression(r#"List.head(List())"#)),
            r#"Ok(LHead(List([])))"#
        );
    }

    #[test]
    fn test_parse_ltail() {
        assert_eq!(
            presult_to_string(&parse_expression(r#"List.tail(List(1, 2))"#)),
            r#"Ok(LTail(List([Val(Int(1)), Val(Int(2))])))"#
        );
        // Ok for parser but will result in runtime error:
        assert_eq!(
            presult_to_string(&parse_expression(r#"List.tail(List())"#)),
            r#"Ok(LTail(List([])))"#
        );
    }
}
