use super::*;
use crate::core::StreamType;
use crate::{
    VarName,
    dsrv_fixtures::WithoutWarnings,
    lang::dsrv::{ast::Declaration, diagnostics::SemanticError},
};

use std::cell::Cell;

thread_local! {
    static PIPELINE_COUNTS: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
}

pub(crate) fn reset_test_pipeline_counts() {
    PIPELINE_COUNTS.with(|counts| counts.set((0, 0)));
}

pub(crate) fn test_pipeline_counts() -> (usize, usize) {
    PIPELINE_COUNTS.with(Cell::get)
}

pub(super) fn record_parse() {
    PIPELINE_COUNTS.with(|counts| {
        let (parse, check) = counts.get();
        counts.set((parse + 1, check));
    });
}

pub(super) fn record_check() {
    PIPELINE_COUNTS.with(|counts| {
        let (parse, check) = counts.get();
        counts.set((parse, check + 1));
    });
}

/// Parse and check `source`, which must parse and must not warn.
#[track_caller]
fn checked(source: &str) -> SemanticResult<CheckedDsrvSpecification> {
    CheckedDsrvSpecification::parse(source)
        .unwrap_or_else(|errors| panic!("{source}: {errors:?}"))
        .without_warnings()
}

#[test]
fn unchecked_specifications_parse_with_from_str() {
    let specification = "in x: Int\nout y: Int\ny = x + 1"
        .parse::<DsrvSpecification>()
        .unwrap();

    assert!(specification.input_vars().contains(&VarName::new("x")));
    assert!(specification.output_vars().contains(&VarName::new("y")));
    assert!(specification.var_expr_ref(&VarName::new("y")).is_some());
}

#[test]
fn struct_equality_needs_operands_of_the_same_struct_type() {
    checked("in a: Struct<x: Int>\nin b: Struct<x: Int>\nout same: Bool = a == b").unwrap();
    checked("in n: Int\nout same: Bool = {x: n, y: 2} != {y: 2, x: 1}").unwrap();
    for source in [
        "in n: Int\nout same: Bool = {x: 1} == {x: \"a\"}",
        "in n: Int\nout same: Bool = {x: 1} == {x: 1, y: 2}",
        "in n: Int\nout same: Bool = {x: 1} == n",
    ] {
        let Err(errors) = checked(source) else {
            panic!("{source} must not type check");
        };
        assert!(
            errors
                .iter()
                .any(|error| format!("{error:?}").contains("equality operator")),
            "{source}: {errors:?}"
        );
    }
}

#[test]
fn lambda_parameter_types_are_inferred_from_their_context() {
    let require_checked =
        |source: &str| checked(source).unwrap_or_else(|errors| panic!("{source}: {errors:?}"));
    for (source, output, expected) in [
        (
            "in xs: List<Int>\nout ys: List<Int> = List.map(\\x -> x + 1, xs)",
            "ys",
            StreamType::List(Box::new(StreamType::Int)),
        ),
        (
            "in xs: List<Int>\nout ys: List<Int> = List.filter(\\x -> x > 0, xs)",
            "ys",
            StreamType::List(Box::new(StreamType::Int)),
        ),
        (
            "in xs: List<Int>\nout s: Int = List.fold(\\acc, x -> acc + x, 0, xs)",
            "s",
            StreamType::Int,
        ),
        (
            "in x: Int\nout y: Int = (\\n -> n * 2)(x)",
            "y",
            StreamType::Int,
        ),
        (
            "in x: Float\nout y: Float = (\\n, m -> n + m)(x, 1.5)",
            "y",
            StreamType::Float,
        ),
    ] {
        let checked = require_checked(source);
        assert_eq!(
            checked.var_expr_ref(&VarName::new(output)).unwrap().typ(),
            &expected,
            "{source}"
        );
    }

    // Annotations still win, and must agree with the context.
    require_checked("in xs: List<Int>\nout ys: List<Int> = List.map(\\x: Int -> x + 1, xs)");
    assert!(checked("in xs: List<Int>\nout ys: List<Int> = List.map(\\x: Str -> 1, xs)",).is_err());
}

#[test]
fn a_lambda_parameter_without_context_requests_an_annotation() {
    // A lambda stored in a list is checked with nothing to infer from.
    let source = "in x: Int\nout y: Int = List.len(List(\\f -> 1))";

    assert!(checked(source).is_err());
}

#[test]
fn inferred_lambda_parameters_print_without_annotations() {
    let source = "in xs: List<Int>\nout ys: List<Int>\nys = List.map(\\x -> x + 1, xs)\n";
    let spec = source.parse::<DsrvSpecification>().unwrap();
    let printed = spec.to_string();
    assert!(printed.contains("\\x -> "), "{printed}");
    assert_eq!(printed.parse::<DsrvSpecification>().unwrap(), spec);
}

#[test]
fn check_reports_the_checked_specification_and_no_warnings() {
    let report = "in x: Int\nout y: Int\ny = x + 1"
        .parse::<DsrvSpecification>()
        .unwrap()
        .check();
    assert!(report.warnings().is_empty());
    let (specification, warnings) = report.into_parts();
    assert!(warnings.is_empty());
    assert_eq!(
        specification
            .unwrap()
            .var_expr_ref(&VarName::new("y"))
            .unwrap()
            .typ(),
        &StreamType::Int
    );
    let report = "in x\nout y\ny = x + 1"
        .parse::<DsrvSpecification>()
        .unwrap()
        .check();
    assert!(report.warnings().is_empty());
    assert!(report.result().is_err());
}

#[test]
fn every_check_entry_point_produces_the_same_concrete_type() {
    let source = "in x:Int\nout y=x+1";
    let spec = source.parse::<DsrvSpecification>().unwrap();
    let direct = spec.clone().check().without_warnings().unwrap();
    let validated = spec
        .clone()
        .validate()
        .unwrap()
        .check()
        .without_warnings()
        .unwrap();
    let parsed = checked(source).unwrap();
    for checked in [direct, validated, parsed] {
        assert_eq!(
            checked.var_expr_ref(&"y".into()).unwrap().typ(),
            &StreamType::Int
        );
        assert_eq!(
            checked.elaborate().var_expr_ref(&"y".into()).unwrap().typ(),
            &StreamType::Int
        );
    }
    let elaborated = spec.check_and_elaborate().without_warnings().unwrap();
    let parsed = crate::ElaboratedDsrvSpecification::parse(source)
        .unwrap()
        .without_warnings()
        .unwrap();
    assert_eq!(parsed.to_string(), elaborated.to_string());
}

#[test]
fn parse_infers_concrete_output_types() {
    let specification = checked("in x:Int\nout y\ny = x + 1").unwrap();

    assert_eq!(
        specification
            .var_expr_ref(&VarName::new("y"))
            .unwrap()
            .typ(),
        &StreamType::Int
    );
}

#[test]
fn checked_source_facade_keeps_parse_and_semantic_errors_distinct() {
    assert!(CheckedDsrvSpecification::parse("out y\ny = ").is_err());
    let semantic = checked("out y\ny = missing")
        .expect_err("undeclared references must fail common admission");
    assert!(semantic.iter().any(|error| matches!(
        error,
        SemanticError::UndeclaredVariable(message, _, _) if message.contains("missing")
    )));
    let type_error = checked("out y: Bool\ny = 1")
        .expect_err("well-formed but ill-typed source must reach type checking");
    assert!(
        type_error
            .iter()
            .any(|error| matches!(error, SemanticError::TypeError(_)))
    );
}

#[test]
fn interleaved_entries_drive_order_and_display() {
    let source = "in z: Int\n\
                  out b: Int\n\
                  b = a + z\n\
                  in q: Int\n\
                  aux a: Int\n\
                  a = q + 1\n\
                  out c: Int\n\
                  c = z - q";
    let specification = source.parse::<DsrvSpecification>().unwrap();
    assert_eq!(
        specification.input_vars_in_order(),
        vec![VarName::new("z"), VarName::new("q")]
    );
    assert!(matches!(
        specification.declarations()[1],
        Declaration::Output {
            ref name,
            annotation: Some(crate::core::StreamType::Int),
            ..
        }
            if name == &VarName::new("b")
    ));
    assert_eq!(
        specification.to_string(),
        "in z: Int\n\
         out b: Int\n\
         b = (a + z)\n\
         in q: Int\n\
         aux a: Int\n\
         a = (q + 1)\n\
         out c: Int\n\
         c = (z - q)\n"
    );
    assert_eq!(
        specification.stream_vars_in_order(),
        [VarName::new("b"), VarName::new("a"), VarName::new("c")]
    );
    assert_eq!(
        specification
            .declarations()
            .iter()
            .filter_map(|entry| {
                matches!(entry, Declaration::Equation { .. })
                    .then(|| entry.stream().expect("a stream declaration").clone())
            })
            .collect::<Vec<_>>(),
        [VarName::new("b"), VarName::new("a"), VarName::new("c")]
    );
}

#[test]
fn duplicate_declarations_fail_validation_and_checking() {
    let specification = "in z: Int\nout a: Int\nin z: Bool\na = z"
        .parse::<DsrvSpecification>()
        .unwrap();
    for errors in [
        specification.clone().validate().unwrap_err(),
        specification.check().into_parts().0.unwrap_err(),
    ] {
        assert!(errors.iter().any(|error| matches!(
            error,
            SemanticError::DuplicateDeclaration { variable, .. }
                if variable == &VarName::new("z")
        )));
    }
}

#[test]
fn distribution_primitives_need_the_distributed_dialect() {
    use crate::lang::dsrv::{DsrvParseError, LanguageError};
    for body in [
        "out z: Float\nz = dist(node1, node2)",
        "out z\nz = Tuple(dist(node1, node2))",
        "out z\nz = if true then dist(node1, node2) else dist(node1, node2)",
        "out z\nz = (\\x: Int -> dist(node1, node2))",
        "out z\nz = dynamic(dist(node1, node2), {})",
        "in x\nout z\nz = monitored_at(x, node1)",
    ] {
        assert!(
            matches!(
                body.parse::<DsrvSpecification>(),
                Err(DsrvParseError::Language(
                    LanguageError::NeedsDistributed { .. }
                ))
            ),
            "{body}"
        );
        let specification = format!("language distributed\n{body}")
            .parse::<DsrvSpecification>()
            .unwrap_or_else(|error| panic!("{body}: {error}"));
        specification
            .validate()
            .unwrap_or_else(|errors| panic!("{body}: {errors:?}"));
    }
}

#[test]
fn validation_and_typechecking_cover_the_local_distributed_mode_matrix() {
    for source in [
        "in x:Int\nout y:Int=x+1",
        "language distributed\nin x:Int\nout y=x+1",
    ] {
        let validated = source
            .parse::<DsrvSpecification>()
            .unwrap()
            .validate()
            .unwrap();
        let checked = validated.check().without_warnings().unwrap();
        assert_eq!(
            checked.var_expr_ref(&"y".into()).unwrap().typ(),
            &StreamType::Int
        );
    }
    let bare = "in x\nout y=x+1"
        .parse::<DsrvSpecification>()
        .unwrap()
        .validate()
        .unwrap();
    assert!(
        bare.check().result().is_err(),
        "valid scope does not provide an input type"
    );
}

#[test]
fn validation_errors_are_preserved_by_checking_and_visit_all_roots() {
    fn duplicate(error: &SemanticError) -> bool {
        matches!(error, SemanticError::DuplicateDeclaration { variable, .. }
            if *variable == VarName::new("z"))
    }
    fn undeclared(error: &SemanticError) -> bool {
        matches!(error, SemanticError::UndeclaredVariable(message, _, _)
            if message.contains("missing"))
    }
    fn invalid_scope(error: &SemanticError) -> bool {
        matches!(error, SemanticError::InvalidRuntimeScope(message, _, _)
            if message.contains("missing"))
    }
    let cases = [
        (
            "in z\nin z\nout result\nresult = z",
            duplicate as fn(&SemanticError) -> bool,
        ),
        ("out result\nresult = missing", undeclared),
        (
            "in source: Str\nout result\nresult = dynamic(source: Int, {missing})",
            invalid_scope,
        ),
    ];
    for (source, expected) in cases {
        let specification = source.parse::<DsrvSpecification>().unwrap();
        let local_errors = specification.clone().validate().unwrap_err();
        let checking_errors = specification.check().into_parts().0.unwrap_err();
        assert!(
            local_errors.iter().any(expected),
            "local errors: {local_errors:?}"
        );
        assert!(
            checking_errors.iter().any(expected),
            "checking errors: {checking_errors:?}"
        );
    }

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
        for errors in [
            specification.clone().validate().unwrap_err(),
            specification.validate().unwrap_err(),
        ] {
            assert!(
                errors
                    .iter()
                    .any(|error| matches!(error, SemanticError::InvalidRuntimeScope(_, _, _)))
            );
        }
    }
    let empty_scope = "in source: Str\nout z: Int\nz = dynamic(source: Int, {})"
        .parse::<DsrvSpecification>()
        .unwrap();
    empty_scope.clone().validate().unwrap();
    empty_scope.validate().unwrap();

    let unused_aux = "in x\nout result\naux unused\nresult = x\nunused = missing"
        .parse::<DsrvSpecification>()
        .unwrap();
    for errors in [
        unused_aux.clone().validate().unwrap_err(),
        unused_aux.validate().unwrap_err(),
    ] {
        assert!(errors.iter().any(|error| matches!(
            error,
            SemanticError::UndeclaredVariable(message, _, _)
                if message.contains("missing")
        )));
    }
}

#[test]
fn lambda_shadowing_does_not_leak_between_siblings() {
    let shadowed = "in x: Int\nout result: Int\nresult = (\\x: Int -> x)(1) + x"
        .parse::<DsrvSpecification>()
        .unwrap();
    shadowed
        .clone()
        .validate()
        .expect("lambda shadowing should be valid");
    shadowed
        .validate()
        .expect("lambda shadowing should be valid in distributed mode");

    let sibling = "out result: Int\nresult = Tuple((\\x: Int -> x)(1), missing)"
        .parse::<DsrvSpecification>()
        .unwrap();
    for errors in [
        sibling.clone().validate().unwrap_err(),
        sibling.validate().unwrap_err(),
    ] {
        assert!(errors.iter().any(|error| matches!(
            error,
            SemanticError::UndeclaredVariable(message, _, _)
                if message.contains("missing")
        )));
    }
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
            for errors in [
                specification.clone().validate().unwrap_err(),
                specification.validate().unwrap_err(),
            ] {
                assert!(errors.iter().any(|error| matches!(
                    error,
                    SemanticError::DuplicateDeclaration { variable, .. }
                        if *variable == VarName::new("z")
                )));
            }
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
