//! Structural and lexical invariants checked independently of type inference.

use crate::dsrv_fixtures::WithoutWarnings;
use crate::lang::dsrv::diagnostics::SemanticError;
use crate::lang::dsrv::validation::validate_specification;
use crate::{DsrvSpecification, VarName};
use test_log::test;

#[test]
fn validation_respects_lambda_bindings() {
    let specification = "out z: Int\nz = (\\x: Int -> x)(1)".parse().unwrap();

    validate_specification(&specification).expect("lambda parameter should be in scope");
}

#[test]
fn duplicate_declaration_diagnostics_preserve_source_order_and_spans() {
    for source in [
        "in z: Int\nin z: Bool\nout result\nresult = z",
        "in z: Int\nin z: Bool\nin z: Str\nout result\nresult = z",
    ] {
        let specification = source.parse::<DsrvSpecification>().unwrap();
        let errors = specification.validate().unwrap_err();
        let duplicate_errors = errors
            .iter()
            .filter_map(|error| match error {
                SemanticError::DuplicateDeclaration {
                    variable,
                    first,
                    duplicate,
                    ..
                } if *variable == VarName::new("z") => Some((*first, *duplicate)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(duplicate_errors.len(), source.matches("in z:").count() - 1);
        for (index, (first, duplicate)) in duplicate_errors.into_iter().enumerate() {
            assert_eq!(&source[first.to_range()], "in z: Int");
            let expected_duplicate = match index {
                0 => "in z: Bool",
                1 => "in z: Str",
                _ => unreachable!(),
            };
            assert_eq!(&source[duplicate.to_range()], expected_duplicate);
        }
    }
}

#[test]
fn duplicate_declaration_matrix_covers_roles_annotations_and_orders() {
    let role_pairs = [
        ("in", "in"),
        ("out", "out"),
        ("aux", "aux"),
        ("in", "out"),
        ("out", "in"),
        ("in", "aux"),
        ("aux", "in"),
        ("out", "aux"),
        ("aux", "out"),
    ];
    let annotations = [
        ("", ""),
        (": Int", ": Int"),
        (": Int", ": Bool"),
        (": Int", ""),
    ];
    for (left_role, right_role) in role_pairs {
        for (left_annotation, right_annotation) in annotations {
            let source = format!(
                "{left_role} z{left_annotation}\n\
                 {right_role} z{right_annotation}\n\
                 out result\n\
                 result = 1"
            );
            let specification = source.parse::<DsrvSpecification>().unwrap();
            assert_eq!(
                specification
                    .declarations()
                    .iter()
                    .filter(|entry| entry.stream() == Some(&VarName::new("z")))
                    .count(),
                2
            );
            let errors = specification.validate().unwrap_err();
            assert!(errors.iter().any(|error| matches!(
                error,
                SemanticError::DuplicateDeclaration { variable, .. }
                    if *variable == VarName::new("z")
            )));
        }
    }
}

#[test]
fn duplicate_declaration_validation_reports_independent_conflicts_in_order() {
    let specification = "in z: Int\nout z: Bool\nin q\naux q\nout result\nresult = 1"
        .parse::<DsrvSpecification>()
        .unwrap();
    let errors = specification.validate().unwrap_err();
    let names = errors
        .iter()
        .filter_map(|error| match error {
            SemanticError::DuplicateDeclaration { variable, .. } => Some(variable.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(names, [VarName::new("z"), VarName::new("q")]);
}

#[test]
fn runtime_scopes_reject_duplicates_self_references_and_undeclared_streams() {
    for expression in [
        "dynamic(source: Int, {x, x})",
        "defer(source: Int, {x, x})",
        "dynamic(source: Int, {z})",
        "defer(source: Int, {z})",
        "dynamic(source: Int, {missing})",
        "defer(source: Int, {missing})",
    ] {
        let source = format!("in x: Int\nin source: Str\nout z: Int\nz = {expression}");
        let specification = source.parse::<DsrvSpecification>().unwrap();
        let errors = specification.validate().unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| matches!(error, SemanticError::InvalidRuntimeScope(_, _, _))),
            "{expression}: {errors:?}"
        );
    }
    "in source: Str\nout z: Int\nz = dynamic(source: Int, {})"
        .parse::<DsrvSpecification>()
        .unwrap()
        .validate()
        .expect("an empty runtime scope is valid");
}

#[test]
fn validation_visits_unused_auxiliary_equations() {
    let specification = "in x\nout result\naux unused\nresult = x\nunused = missing"
        .parse::<DsrvSpecification>()
        .unwrap();
    let errors = specification.validate().unwrap_err();
    assert!(
        errors.iter().any(|error| matches!(error,
            SemanticError::UndeclaredVariable(message, _, _) if message.contains("missing")
        )),
        "{errors:?}"
    );
}

#[test]
fn lambda_shadowing_does_not_leak_between_siblings() {
    for header in ["", "language distributed\n"] {
        let shadowed =
            format!("{header}in x: Int\nout result: Int\nresult = (\\x: Int -> x)(1) + x")
                .parse::<DsrvSpecification>()
                .unwrap();
        shadowed
            .validate()
            .expect("lambda shadowing should be valid in either dialect");

        let sibling =
            format!("{header}out result: Int\nresult = Tuple((\\x: Int -> x)(1), missing)")
                .parse::<DsrvSpecification>()
                .unwrap();
        let errors = sibling.validate().unwrap_err();
        assert!(
            errors.iter().any(|error| matches!(error,
                SemanticError::UndeclaredVariable(message, _, _) if message.contains("missing")
            )),
            "{header}: {errors:?}"
        );
    }
}

#[test]
fn validation_accepts_untyped_streams_while_checking_requires_input_types() {
    for header in ["", "language distributed\n"] {
        let typed = format!("{header}in x:Int\nout y=x+1")
            .parse::<DsrvSpecification>()
            .unwrap()
            .validate()
            .unwrap();
        typed
            .check()
            .without_warnings()
            .expect("typed input should check in either dialect");

        let untyped = format!("{header}in x\nout y=x+1")
            .parse::<DsrvSpecification>()
            .unwrap()
            .validate()
            .expect("structural validation does not require input types");
        let errors = untyped.check().into_parts().0.unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| matches!(error, SemanticError::MissingTypeAnnotation(_, _, _))),
            "{header}: {errors:?}"
        );
    }
}
