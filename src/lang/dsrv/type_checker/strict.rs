//! The strict type-checking driver, which requires an explicit type
//! annotation for every variable in the specification.

use super::*;
use crate::DsrvSpecification;
use crate::lang::dsrv::ast::CheckedDsrvSpecification;

/// Strictly type-check a specification and attach type metadata to its nodes.
pub fn type_check(
    spec: DsrvSpecification,
    distributed: bool,
) -> SemanticResult<CheckedDsrvSpecification> {
    super::checker::check_specification(spec, distributed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VarName;
    use crate::core::StreamType;
    use crate::lang::dsrv::ast::Expr;
    use crate::lang::dsrv::span::Span;
    use ecow::EcoVec;
    use std::collections::{BTreeMap, BTreeSet};
    use test_log::test;

    #[test]
    fn unary_minus_accepts_int_and_float_and_rejects_non_numeric_values() {
        for source in ["out z: Int\nz = -1", "out z: Float\nz = -1.5"] {
            type_check(source.parse().unwrap(), false)
                .expect("numeric negation should be accepted");
        }

        let errors = type_check("out z: Bool\nz = -true".parse().unwrap(), false)
            .expect_err("boolean negation should be rejected");
        assert!(errors.iter().any(|error| matches!(
            error,
            SemanticError::TypeError(type_error)
                if type_error.kind() == &TypeErrorKind::OperatorTypeMismatch
        )));
    }

    #[test]
    fn test_top_level_type_check_empty_list_output() {
        // Simulates a full spec where an output variable is assigned []
        // and its declared type is List<Int>.
        let mut exprs = BTreeMap::new();
        let var: VarName = "y".into();
        exprs.insert(var.clone(), Expr::List(EcoVec::new()));
        let mut type_annotations = BTreeMap::new();
        type_annotations.insert(var.clone(), StreamType::List(Box::new(StreamType::Int)));
        let spec = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from([var.clone()]),
            exprs,
            type_annotations,
            BTreeSet::new(),
        );
        let result = type_check(spec, false);
        assert!(
            result.is_ok(),
            "Expected Ok for spec with y : List<Int> = [], got {:?}",
            result
        );
        let typed_spec = result.unwrap();
        let te = typed_spec.var_expr(&var).unwrap();
        assert_eq!(te.typ(), &TCType::list(TCType::Int));
    }

    #[test]
    fn test_type_check_preserves_aux_vars() {
        let x: VarName = "strict_aux_x".into();
        let z: VarName = "strict_aux_z".into();
        let u: VarName = "strict_aux_u".into();
        let exprs = BTreeMap::from([
            (u.clone(), Expr::Var(x.clone())),
            (z.clone(), Expr::Var(u.clone())),
        ]);
        let type_annotations = BTreeMap::from([
            (x.clone(), StreamType::Int),
            (u.clone(), StreamType::Int),
            (z.clone(), StreamType::Int),
        ]);
        let spec = DsrvSpecification::new(
            BTreeSet::from([x]),
            BTreeSet::from([z.clone()]),
            exprs,
            type_annotations,
            BTreeSet::from([u.clone()]),
        );

        let typed = type_check(spec, false).expect("strict type check should preserve aux vars");

        assert_eq!(typed.aux_vars(), &BTreeSet::from([u.clone()]));
        assert_eq!(typed.stream_vars(), &BTreeSet::from([z.clone(), u]));
        assert_eq!(typed.output_vars(), &BTreeSet::from([z]));
    }

    #[test]
    fn shared_source_handles_get_independent_contextual_types() {
        let int_output = VarName::new("int_output");
        let bool_output = VarName::new("bool_output");
        let empty_list = Expr::List(EcoVec::new());
        let spec = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from([int_output.clone(), bool_output.clone()]),
            BTreeMap::from([
                (int_output.clone(), empty_list.clone()),
                (bool_output.clone(), empty_list),
            ]),
            BTreeMap::from([
                (
                    int_output.clone(),
                    StreamType::List(Box::new(StreamType::Int)),
                ),
                (
                    bool_output.clone(),
                    StreamType::List(Box::new(StreamType::Bool)),
                ),
            ]),
            Vec::new(),
        );

        let checked = type_check(spec, false).expect("both contextual types are valid");

        assert_eq!(
            checked.var_expr(&int_output).unwrap().typ(),
            &TCType::list(TCType::Int)
        );
        assert_eq!(
            checked.var_expr(&bool_output).unwrap().typ(),
            &TCType::list(TCType::Bool)
        );
        assert_ne!(
            checked.var_expr(&int_output).unwrap().as_ref().expr().id(),
            checked.var_expr(&bool_output).unwrap().as_ref().expr().id()
        );
    }

    #[test]
    fn top_level_type_error_uses_document_span() {
        let input = "in x: Int\nout z: Int\nz = x + true";
        let source = input;
        let spec = source.parse().unwrap();

        let errors = type_check(spec, false).expect_err("spec should fail type checking");
        let expected_start = input.find("x + true").expect("expression should exist") as u32;
        let expected_end = expected_start + "x + true".len() as u32;

        assert!(
            errors.iter().any(|error| matches!(
                error,
                SemanticError::TypeError(type_error)
                    if type_error.kind() == &TypeErrorKind::OperatorTypeMismatch
                        && error.span() == Some(Span::new(expected_start, expected_end))
            )),
            "expected top-level operator error at {:?}, got {:?}",
            Span::new(expected_start, expected_end),
            errors
        );
    }

    #[test]
    fn lalr_top_level_type_error_uses_document_span() {
        let input = "in x: Int\nout z: Int\nz = x + true";
        let source = input;
        let spec = source.parse().unwrap();

        let errors = type_check(spec, false).expect_err("spec should fail type checking");
        let expected_start = input.find("x + true").expect("expression should exist") as u32;
        let expected_end = expected_start + "x + true".len() as u32;

        assert!(
            errors.iter().any(|error| matches!(
                error,
                SemanticError::TypeError(type_error)
                    if type_error.kind() == &TypeErrorKind::OperatorTypeMismatch
                        && error.span() == Some(Span::new(expected_start, expected_end))
            )),
            "expected LALR top-level operator error at {:?}, got {:?}",
            Span::new(expected_start, expected_end),
            errors
        );
    }

    #[test]
    fn annotation_mismatch_uses_expression_span() {
        let input = "out z: Bool\nz = 1";
        let source = input;
        let spec = source.parse().unwrap();

        let errors = type_check(spec, false).expect_err("spec should fail type checking");
        let expected_start = input.find('1').expect("expression should exist") as u32;
        let expected_end = expected_start + 1;

        assert!(
            errors.iter().any(|error| matches!(
                error,
                SemanticError::TypeError(type_error)
                    if type_error.kind() == &TypeErrorKind::AnnotationTypeMismatch
                        && error.span() == Some(Span::new(expected_start, expected_end))
            )),
            "expected annotation mismatch at {:?}, got {:?}",
            Span::new(expected_start, expected_end),
            errors
        );
    }

    #[test]
    fn explicit_runtime_scopes_reject_duplicates_unknown_variables_and_self_references() {
        for expression in [
            "dynamic(source: Int, {x, x})",
            "defer(source: Int, {x, x})",
            "dynamic(source: Int, {missing})",
            "defer(source: Int, {missing})",
            "dynamic(source: Int, {z})",
            "defer(source: Int, {z})",
        ] {
            let document = format!("in x: Int\nin source: Str\nout z: Int\nz = {expression}");
            let combinator_source = document.as_str();
            let lalr_source = document.as_str();
            let specs: [DsrvSpecification; 2] = [
                combinator_source.parse().unwrap(),
                lalr_source.parse().unwrap(),
            ];
            for spec in specs {
                let errors =
                    type_check(spec, false).expect_err("invalid runtime scope should be rejected");
                assert!(
                    errors
                        .iter()
                        .any(|error| matches!(error, SemanticError::InvalidRuntimeScope(_, _))),
                    "expected a scope error for `{expression}`, got {errors:?}"
                );
            }
        }
    }

    #[test]
    fn explicit_defer_scope_type_checks() {
        let document = "in x: Int\nin source: Str\nout z: Int\nz = defer(source: Int, {x, source})";
        type_check(document.parse().unwrap(), false).expect("defer scope should type-check");
    }

    #[test]
    fn recursive_function_application_type_checks() {
        let source = "in n: Int\nin bias: Int\nout z: Int\n\
                      z = fix(\\self: (Int -> Int), k: Int -> \
                      if k == 0 then bias else self(k - 1) + 1)(n)";
        type_check(source.parse().unwrap(), false)
            .expect("recursive function application should type-check");
    }

    #[test]
    fn checking_a_clone_does_not_annotate_or_poison_the_original() {
        let original: DsrvSpecification = "out z: Int\nz = 1".parse().unwrap();
        let checked = type_check(original.clone(), false).unwrap();

        assert_eq!(checked.var_expr(&"z".into()).unwrap().typ(), &TCType::Int);

        let mut contradictory = original;
        contradictory
            .type_annotations
            .insert("z".into(), StreamType::Bool);
        type_check(contradictory, false)
            .expect_err("a prior check must not cache away a new constraint");
    }
}
