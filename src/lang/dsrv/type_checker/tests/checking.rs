use super::super::relations::flow;
use crate::DsrvSpecification;
use crate::VarName;
use crate::core::{BinaryOperator, StreamType};
use crate::lang::dsrv::ast::{Expr, SyntaxLiteral};
use crate::lang::dsrv::diagnostics::{SemanticError, TypeErrorKind};
use crate::lang::dsrv::span::Span;
use crate::lang::dsrv::test_support::type_check;

use ecow::{EcoString, EcoVec, eco_vec};
use std::collections::{BTreeMap, BTreeSet};
use test_log::test;

#[test]
fn unary_minus_accepts_int_and_float_and_rejects_non_numeric_values() {
    for source in ["out z: Int\nz = -1", "out z: Float\nz = -1.5"] {
        type_check(source.parse().unwrap()).expect("numeric negation should be accepted");
    }

    let errors = type_check("out z: Bool\nz = -true".parse().unwrap())
        .expect_err("boolean negation should be rejected");
    assert!(errors.iter().any(|error| matches!(
        error,
        SemanticError::TypeError(type_error)
            if type_error.kind() == &TypeErrorKind::OperatorTypeMismatch
    )));
}

#[test]
fn equality_and_ordering_have_distinct_operand_rules() {
    type_check("out z: Bool\nz = [1] == [1]".parse().unwrap())
        .expect("equal lists should be comparable for equality");

    let errors = type_check("out z: Bool\nz = [1] < [1]".parse().unwrap())
        .expect_err("lists should not support ordering");
    assert!(errors.iter().any(|error| matches!(
        error,
        SemanticError::TypeError(type_error)
            if type_error.kind() == &TypeErrorKind::OperatorTypeMismatch
    )));
}

// Concrete numeric power rules and static exponent diagnostics.
#[test]
fn power_type_table_and_static_negative_exponent_rule() {
    for (source, expected) in [
        ("out z: Int\nz = 2 ** 3", StreamType::Int),
        ("out z: Float\nz = 4 ** 0.5", StreamType::Float),
        ("out z: Float\nz = 2.0 ** -3", StreamType::Float),
        ("out z: Float\nz = 4.0 ** 0.5", StreamType::Float),
        ("out z: Int\nz = 2 ** -0", StreamType::Int),
        ("out z: Int\nz = 0 ** (-0)", StreamType::Int),
    ] {
        let strict = type_check(source.parse().unwrap()).expect("concrete power should type check");
        assert_eq!(
            strict.var_expr_ref(&VarName::new("z")).unwrap().typ(),
            &expected
        );
    }

    for source in ["out z: Int\nz = 2 ** -1", "out z: Int\nz = 2 ** (-1)"] {
        for errors in [type_check(source.parse().unwrap())
            .expect_err("checker must reject known negative exponent")]
        {
            assert!(errors.iter().any(|error| matches!(
                error,
                SemanticError::TypeError(type_error)
                    if type_error.kind() == &TypeErrorKind::OperatorTypeMismatch
                        && type_error.message().contains("integer exponent must be non-negative")
                        && type_error.span().is_some()
            )));
        }
    }

    let dynamic = "in base: Int\nin exponent: Int\nout z: Int\nz = base ** exponent";
    type_check(dynamic.parse().unwrap()).expect("dynamic Int exponent is valid");

    let z = VarName::new("z");
    let direct_negative_exponent = DsrvSpecification::new(
        BTreeSet::new(),
        BTreeSet::from([z.clone()]),
        BTreeMap::from([(
            z.clone(),
            Expr::BinOp(
                Box::new(Expr::Val(SyntaxLiteral::Int(2))),
                Box::new(Expr::Val(SyntaxLiteral::Int(-1))),
                BinaryOperator::Power,
            ),
        )]),
        BTreeMap::from([(z, StreamType::Int)]),
        BTreeSet::new(),
    );
    for errors in [type_check(direct_negative_exponent)
        .expect_err("checker must reject a direct negative integer literal")]
    {
        assert!(errors.iter().any(|error| matches!(
            error,
            SemanticError::TypeError(type_error)
                if type_error.message().contains("integer exponent must be non-negative")
        )));
    }

    for source in [
        "out z: Int\nz = true ** 1",
        "out z: Int\nz = \"text\" ** 1",
        "out z: Int\nz = 1 ** true",
    ] {
        assert!(
            type_check(source.parse().unwrap()).is_err(),
            "{source} should fail"
        );
    }
}

// inequality has equality's admissibility, rather than ordering's.
#[test]
fn inequality_shares_equality_admissibility() {
    for source in [
        "out z: Bool\nz = 1 != 2",
        "out z: Bool\nz = 1.0 != 2.0",
        "out z: Bool\nz = 1 != 2.0",
        "out z: Bool\nz = true != false",
        "out z: Bool\nz = \"a\" != \"b\"",
        "out z: Bool\nz = [1] != [1]",
        "out z: Bool\nz = Tuple(1) != Tuple(1)",
        "out z: Bool\nz = Map(\"x\": 1) != Map(\"x\": 1)",
    ] {
        type_check(source.parse().unwrap())
            .unwrap_or_else(|errors| panic!("{source} unexpectedly failed: {errors:?}"));
    }
    for source in [
        "out z: Bool\nz = [1] != true",
        "out z: Bool\nz = Tuple(1) != 1",
    ] {
        let errors = type_check(source.parse().unwrap())
            .expect_err("incompatible inequality operands should fail");
        assert!(
            errors.iter().any(|error| {
                matches!(
                    error,
                    SemanticError::TypeError(type_error)
                        if type_error.message().contains("inequality")
                            || type_error.message().contains("!=")
                )
            }),
            "diagnostic should identify inequality: {errors:?}"
        );
        assert!(type_check(source.parse().unwrap()).is_err());
    }
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
    let result = type_check(spec);
    assert!(
        result.is_ok(),
        "Expected Ok for spec with y : List<Int> = [], got {:?}",
        result
    );
    let typed_spec = result.unwrap();
    let te = typed_spec.var_expr_ref(&var).unwrap();
    assert_eq!(te.typ(), &StreamType::List(Box::new(StreamType::Int)));
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

    let typed = type_check(spec).expect("strict type check should preserve aux vars");

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

    let checked = type_check(spec).expect("both contextual types are valid");

    assert_eq!(
        checked.var_expr_ref(&int_output).unwrap().typ(),
        &StreamType::List(Box::new(StreamType::Int))
    );
    assert_eq!(
        checked.var_expr_ref(&bool_output).unwrap().typ(),
        &StreamType::List(Box::new(StreamType::Bool))
    );
    assert_ne!(
        checked.var_expr_ref(&int_output).unwrap().expr().id(),
        checked.var_expr_ref(&bool_output).unwrap().expr().id()
    );
}

#[test]
fn top_level_type_error_uses_document_span() {
    let input = "in x: Int\nout z: Int\nz = x + true";
    let source = input;
    let spec = source.parse().unwrap();

    let errors = type_check(spec).expect_err("spec should fail type checking");
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

    let errors = type_check(spec).expect_err("spec should fail type checking");
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
fn latch_trigger_may_have_a_different_type_from_its_value() {
    let spec = "in x: Int\nin trigger: Bool\nout z: Int\nz = latch(x, trigger)"
        .parse()
        .unwrap();

    type_check(spec).expect("latch trigger type must not constrain its value type");
}

#[test]
fn annotation_mismatch_uses_expression_span() {
    let input = "out z: Bool\nz = 1";
    let source = input;
    let spec = source.parse().unwrap();

    let errors = type_check(spec).expect_err("spec should fail type checking");
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
fn explicit_runtime_scopes_name_only_distinct_known_streams_other_than_their_owner() {
    for expression in [
        "dynamic(source: Int, {x, x})",
        "defer(source: Int, {x, x})",
        "dynamic(source: Int, {missing})",
        "defer(source: Int, {missing})",
        "dynamic(source: Int, {z})",
        "defer(source: Int, {z})",
        "(\\x: Int -> dynamic(source: Int, {x}))(x)",
        "(\\x: Int -> defer(source: Int, {x}))(x)",
    ] {
        let document = format!("in x: Int\nin source: Str\nout z: Int\nz = {expression}");
        let combinator_source = document.as_str();
        let lalr_source = document.as_str();
        let specs: [DsrvSpecification; 2] = [
            combinator_source.parse().unwrap(),
            lalr_source.parse().unwrap(),
        ];
        for spec in specs {
            let errors = type_check(spec).expect_err("invalid runtime scope should be rejected");
            assert!(
                errors
                    .iter()
                    .any(|error| matches!(error, SemanticError::InvalidRuntimeScope(_, _, _))),
                "expected a scope error for `{expression}`, got {errors:?}"
            );
        }
    }
}

#[test]
fn explicit_defer_scope_type_checks() {
    let document = "in x: Int\nin source: Str\nout z: Int\nz = defer(source: Int, {x, source})";
    type_check(document.parse().unwrap()).expect("defer scope should type-check");
}

#[test]
fn typed_expression_sources_determine_dynamic_and_defer_results() {
    for operator in ["dynamic", "defer"] {
        let source = format!(
            "in property: Expr<List<Int>>\nout result: List<Int>\nresult = {operator}(property)"
        );
        let checked = type_check(source.parse().unwrap())
            .unwrap_or_else(|errors| panic!("{operator} should accept Expr<T>: {errors:?}"));
        let result = checked.var_expr_ref(&VarName::new("result")).unwrap();
        assert_eq!(result.typ(), &StreamType::List(Box::new(StreamType::Int)));
        let runtime_source = match result.view() {
            crate::lang::dsrv::ast::ExprView::Dynamic(source, _, _)
            | crate::lang::dsrv::ast::ExprView::Defer(source, _, _) => source,
            _ => panic!("expected runtime expression"),
        };
        assert_eq!(
            runtime_source.typ(),
            &StreamType::Expr(Box::new(StreamType::List(Box::new(StreamType::Int))))
        );
    }
}

#[test]
fn string_sources_accept_explicit_or_result_context() {
    for operator in ["dynamic", "defer"] {
        let accepted =
            format!("in property: Str\nout result: Int\nresult = {operator}(property: Int)");
        type_check(accepted.parse().unwrap())
            .unwrap_or_else(|errors| panic!("legacy {operator} source should pass: {errors:?}"));

        let contextual =
            format!("in property: Str\nout result: Int\nresult = {operator}(property)");
        type_check(contextual.parse().unwrap()).expect("concrete result types a string source");
    }
}

#[test]
fn expression_source_type_must_match_the_result_type() {
    for expression in ["dynamic(property)", "defer(property: Int)"] {
        let source = format!("in property: Expr<Bool>\nout result: Int\nresult = {expression}");
        type_check(source.parse().unwrap()).expect_err("an Expr<Bool> source cannot produce Int");
    }
}

#[test]
fn string_literals_can_initialize_typed_expression_streams() {
    let source = "aux property: Expr<Int>\nout result: Int\n\
                      property = \"1 + 1\"\nresult = dynamic(property)";
    type_check(source.parse().unwrap())
        .expect("a string literal should initialize an explicitly typed Expr<T> stream");
}

#[test]
fn recursive_function_application_type_checks() {
    let source = "in n: Int\nin bias: Int\nout z: Int\n\
                      z = fix(\\self: (Int -> Int), k: Int -> \
                      if k == 0 then bias else self(k - 1) + 1)(n)";
    type_check(source.parse().unwrap()).expect("recursive function application should type-check");
}

#[test]
fn checking_a_clone_does_not_annotate_or_poison_the_original() {
    let original: DsrvSpecification = "out z: Int\nz = 1".parse().unwrap();
    let checked = type_check(original.clone()).unwrap();

    assert_eq!(
        checked.var_expr_ref(&"z".into()).unwrap().typ(),
        &StreamType::Int
    );

    let mut contradictory = original;
    contradictory
        .type_annotations
        .insert("z".into(), StreamType::Bool);
    type_check(contradictory).expect_err("a prior check must not cache away a new constraint");
}

/// `List.concat` takes two lists, so anything else is refused at the call,
/// annotated or not, as it is by `List.append`.
#[test]
fn list_concat_requires_lists() {
    for source in [
        "in x: Int\nout y: Int\ny = List.concat(x, 2)",
        "in x: Str\nout y: Str\ny = List.concat(x, \"b\")",
        "in x: Int\nout y\ny = List.concat([x], 2)",
        "in x: Int\nout y\ny = List.concat(2, [x])",
        "in x: Str\nout y\ny = List.concat(x, 2)",
        "in x: Int\nout y\ny = List.concat(x, x)",
    ] {
        let errors = type_check(source.parse().unwrap())
            .expect_err(&format!("{source}: only lists can be concatenated"));
        assert!(
            errors.iter().any(|error| matches!(
                error,
                SemanticError::TypeError(type_error)
                    if type_error.kind() == &TypeErrorKind::ListOperationTypeMismatch
                        && type_error.span().is_some()
            )),
            "{source}: {errors:?}"
        );
    }
}

/// Concrete and contextual empty lists are accepted.
#[test]
fn list_concat_accepts_concrete_lists() {
    for source in [
        "in x: List<Int>\nout y: List<Int>\ny = List.concat(x, [1])",
        "in x: List<Int>\nout y: List<Int>\ny = List.concat(x, [])",
        "out y: List<Int>\ny = List.concat([], [])",
    ] {
        type_check(source.parse().unwrap()).unwrap_or_else(|errors| panic!("{source}: {errors:?}"));
    }
}

/// The errors from the single concrete checking policy.
#[track_caller]
fn refusals(source: &str) -> [(&'static str, Vec<SemanticError>); 1] {
    let spec = || source.parse::<DsrvSpecification>().expect("source parses");
    [("checked", type_check(spec()).expect_err(source))]
}

/// An open struct has only the fields it lists, so it cannot be used where
/// a closed struct with more fields is needed. The error is located at the
/// value that does not fit.
#[test]
fn an_open_struct_is_refused_where_a_closed_struct_needs_more_fields() {
    for source in [
        "in s: Struct<a: Int, ...>\nout t: Struct<a: Int, b: Int>\nt = s",
        "in s: Struct<a: Int, ...>\naux t: Struct<a: Int, b: Int>\nout z: Int\nt = s\nz = t.b",
        "in s: Struct<a: Int, ...>\nout t: Struct<a: Int, b: Int, ...>\nt = s",
        "in s: Struct<a: Int, ...>\nout t: List<Struct<a: Int, b: Int>>\nt = [s]",
    ] {
        for (mode, errors) in refusals(source) {
            let located = errors.iter().any(|error| {
                matches!(
                    error,
                    SemanticError::TypeError(type_error)
                        if type_error.kind() == &TypeErrorKind::AnnotationTypeMismatch
                            && type_error.message().contains("expected ")
                            && type_error.message().contains("a: Int, ...")
                            && error.span().is_some()
                            && !error.location().is_unset()
                )
            });
            assert!(located, "{mode}: {source}: {errors:?}");
        }
    }
}

/// A closed place has no fields beyond those it lists, and an open struct
/// does not say that of its value, even where it lists just those fields.
/// Building a closed value is what works.
#[test]
fn an_open_struct_is_refused_at_a_closed_struct_with_the_same_fields() {
    for source in [
        "in s: Struct<a: Int, ...>\nout t: Struct<a: Int>\nt = s",
        "in s: Struct<a: Int, b: Str, ...>\nout t: Struct<a: Int, b: Str>\nt = s",
        "in s: Struct<a: Int, ...>\nout t: List<Struct<a: Int>>\nt = [s]",
        "in s: Struct<a: Int, ...>\nout t: Struct<b: Struct<a: Int>>\nt = {b: s}",
        "in s: List<Struct<a: Int, ...>>\nout t: List<Struct<a: Int>>\nt = s",
    ] {
        for (mode, errors) in refusals(source) {
            assert!(
                errors.iter().any(|error| error.span().is_some()
                    && !error.location().is_unset()
                    && format!("{error:?}").contains(", ...")),
                "{mode}: {source}: {errors:?}"
            );
        }
    }
    for source in [
        "in s: Struct<a: Int, ...>\nout t: Struct<a: Int>\nt = {a: s.a}",
        "in s: Struct<a: Int, b: Int, ...>\nout t: Struct<a: Int>\nt = {a: s.a}",
    ] {
        type_check(source.parse().unwrap())
            .unwrap_or_else(|errors| panic!("strict: {source}: {errors:?}"));
    }
}

/// The other directions stay allowed: a closed struct is an open struct
/// with fewer demands, and an open struct meets an open struct that lists
/// the same fields or fewer.
#[test]
fn a_struct_is_accepted_where_an_open_struct_needing_fewer_fields_is() {
    for source in [
        "in s: Struct<a: Int, b: Int>\nout t: Struct<a: Int, ...>\nt = s",
        "in s: Struct<a: Int, ...>\nout t: Struct<a: Int, ...>\nt = s",
        "in s: Struct<a: Int, b: Int, ...>\nout t: Struct<a: Int, ...>\nt = s",
        "in s: Struct<a: Int, b: Int>\nout t: Struct<a: Int, b: Int>\nt = s",
    ] {
        type_check(source.parse().unwrap())
            .unwrap_or_else(|errors| panic!("strict: {source}: {errors:?}"));
    }
}

/// A callback of the wrong type is reported at the callback, with both types.
#[test]
fn a_callback_of_the_wrong_type_says_what_and_where() {
    for (source, callback, message) in [
        (
            "in xs: List<Int>\nout y: List<Str>\ny = List.map(\\q: Str -> q, xs)",
            "\\q: Str -> q",
            "expected Str, got Int",
        ),
        (
            "in xs: List<Int>\nout y: List<Int>\ny = List.filter(\\q: Int -> q, xs)",
            "\\q: Int -> q",
            "expected Bool, got Int",
        ),
        (
            "in xs: List<Int>\nout y: Int\ny = List.fold(\\q: Int -> q, 0, xs)",
            "\\q: Int -> q",
            "callback argument count differs",
        ),
    ] {
        for (mode, errors) in refusals(source) {
            let [SemanticError::TypeError(error)] = &errors[..] else {
                panic!("{mode}: {source}: {errors:?}");
            };
            assert_eq!(error.message(), message, "{mode}: {source}");
            let span = error.span().expect("a callback's error has a span");
            let start = source
                .find(callback)
                .expect("the callback is in the source");
            if error.message().contains("argument count") {
                assert_eq!(error.kind(), &TypeErrorKind::FunctionArityMismatch);
                assert_eq!(
                    span,
                    Span::new(start as u32, (start + callback.len()) as u32),
                    "{mode}"
                );
            }
        }
    }
}

/// A list operation passes its elements into its callback's parameter, and
/// fold's accumulator too, as a value into a place: a parameter that needs
/// a field an open struct does not list is refused, and one that needs
/// fewer fields than a closed struct has is not.
#[test]
fn a_callback_parameter_takes_what_it_is_passed_as_a_place_takes_a_value() {
    for (source, message) in [
        (
            "in xs: List<Struct<a: Int, ...>>\nout ys: List<Int>\n\
                 ys = List.map(\\s: Struct<a: Int, b: Int> -> s.b, xs)",
            "map function has wrong type",
        ),
        (
            "in xs: List<Struct<a: Int, ...>>\nout ys: List<Struct<a: Int, ...>>\n\
                 ys = List.filter(\\s: Struct<a: Int, b: Int> -> s.b > 0, xs)",
            "filter predicate has wrong type",
        ),
        (
            "in xs: List<Struct<a: Int, ...>>\nout n: Int\n\
                 n = List.fold(\\acc: Int, s: Struct<a: Int, b: Int> -> acc + s.b, 0, xs)",
            "fold function has wrong type",
        ),
        (
            "in s: Struct<a: Int, ...>\nin xs: List<Int>\nout n: Struct<a: Int, ...>\n\
                 n = List.fold(\\acc: Struct<a: Int, b: Int>, x: Int -> acc, s, xs)",
            "fold function has wrong type",
        ),
        (
            "in xs: List<Struct<a: Int, ...>>\naux f: (Struct<a: Int, b: Int> -> Int)\n\
                 out ys: List<Int>\nf = \\s: Struct<a: Int, b: Int> -> s.b\nys = List.map(f, xs)",
            "map function has wrong type",
        ),
    ] {
        for (mode, errors) in refusals(source) {
            assert!(
                errors.iter().any(|error| matches!(
                    error,
                    SemanticError::TypeError(type_error)
                        if matches!(type_error.kind(),TypeErrorKind::FunctionTypeMismatch|TypeErrorKind::AnnotationTypeMismatch)
                            && (type_error.message().starts_with(message) || (type_error.message().contains("expected Struct<a: Int, b: Int>") && type_error.message().contains("got Struct<a: Int, ...>")))
                            && error.span().is_some()
                )),
                "{mode}: {source}: {errors:?}"
            );
        }
    }
    for source in [
        "in xs: List<Struct<a: Int, b: Int>>\nout ys: List<Int>\n\
             ys = List.map(\\s: Struct<a: Int, ...> -> s.a, xs)",
        "in xs: List<Struct<a: Int, b: Int>>\nout ys: List<Struct<a: Int, b: Int>>\n\
             ys = List.filter(\\s: Struct<a: Int, ...> -> s.a > 0, xs)",
        "in xs: List<Struct<a: Int, b: Int>>\nout n: Int\n\
             n = List.fold(\\acc: Int, s: Struct<a: Int, ...> -> acc + s.a, 0, xs)",
        "in xs: List<Struct<a: Int, b: Int>>\naux f: (Struct<a: Int, ...> -> Int)\n\
             out ys: List<Int>\nf = \\s: Struct<a: Int, ...> -> s.a\nys = List.map(f, xs)",
    ] {
        type_check(source.parse().unwrap())
            .unwrap_or_else(|errors| panic!("strict: {source}: {errors:?}"));
    }
}

/// Whether the concrete specification checks.
#[track_caller]
fn accepted(source: &str) -> bool {
    type_check(source.parse::<DsrvSpecification>().expect("source parses")).is_ok()
}

/// The fixed point goes where `self` is needed, as a value goes into a place.
#[test]
fn a_fixed_point_flows_into_its_self_parameter() {
    // `self` needs less of its argument's value than the function gives it.
    assert!(accepted(
        "in x: Struct<a: Int, b: Int>\nout y: Int = \
             fix(\\self: (Struct<a: Int, b: Int> -> Int), s: Struct<a: Int, ...> -> s.a)(x)"
    ));
    // A concretely annotated recursive function is accepted.
    assert!(accepted(
        "in x: Int\nout y: Int = fix(\\self: (Int -> Int), n: Int -> n)(x)"
    ));
    // A `self` that promises to take fewer fields than the function reads.
    assert!(!accepted(
        "in x: Struct<a: Int, b: Int>\nout y: Int = \
             fix(\\self: (Struct<a: Int, ...> -> Int), s: Struct<a: Int, b: Int> -> \
             if s.a == 0 then s.b else self({a: s.a - 1}))(x)"
    ));
    // A recursive function cannot promise Int while returning Str.
    assert!(!accepted(
        "in x: Str\nin n0: Int\nout y: Str = \
             fix(\\self: (Int -> Int), n: Int -> \
             if n == 0 then x else if self(n - 1) == 7 then 1 else 2)(n0)"
    ));
}

/// `if` at a function type asks of each function what the place passes it.
#[test]
fn a_merge_of_functions_is_accepted_at_a_type_each_is_accepted_at() {
    let spec = |place: &str| {
        format!(
            "in c: Bool\n\
                 aux f1: (Struct<a: Int, ...> -> Int) = \\s: Struct<a: Int, ...> -> s.a\n\
                 aux f2: (Struct<b: Int, ...> -> Int) = \\s: Struct<b: Int, ...> -> s.b\n\
                 aux g: {place} = if c then f1 else f2"
        )
    };
    assert!(accepted(&spec("(Struct<a: Int, b: Int> -> Int)")));
    assert!(accepted(&spec("(Struct<a: Int, b: Int, ...> -> Int)")));
    // Each is refused alone, and the merge is no more than the two of them.
    assert!(!accepted(&spec("(Struct<a: Int, ...> -> Int)")));
    assert!(!accepted(&spec("(Struct<a: Int> -> Int)")));
}

/// `==` asks that a value of one operand's type could be one of the other's.
#[test]
fn operands_are_comparable_when_one_flows_into_the_other() {
    for (left, right, comparable) in [
        ("Struct<a: Int, b: Int>", "Struct<a: Int, ...>", true),
        ("Struct<a: Int, ...>", "Struct<a: Int>", true),
        ("Struct<a: Int, b: Int, ...>", "Struct<a: Int, ...>", true),
        ("Struct<a: Int>", "Struct<a: Int, b: Int>", false),
        ("Struct<a: Int, ...>", "Struct<a: Str, ...>", false),
        ("Struct<a: Int, b: Int, ...>", "Struct<a: Int>", false),
        ("Int", "Float", false),
    ] {
        for (first, second) in [(left, right), (right, left)] {
            let source = format!("in s: {first}\nin t: {second}\nout y: Bool = s == t");
            assert_eq!(accepted(&source), comparable, "{source}");
        }
    }
}

/// A field's own mistake is reported as what it is, with or without an
/// expected struct type around it.
#[test]
fn a_mistake_inside_a_field_is_reported_at_that_mistake() {
    for (source, message) in [
        ("in x: Int\nout y = {a: !x}", "expected Bool, got Int"),
        (
            "in x: Int\nout y: Struct<a: Bool> = {a: !x}",
            "expected Bool, got Int",
        ),
        (
            "in x: Int\nout y: Struct<a: Bool> = {a: x}",
            "field a has type Int, expected Bool",
        ),
    ] {
        let errors =
            type_check(source.parse().unwrap()).expect_err("the field is of the wrong type");
        assert!(
            errors.iter().any(|error| matches!(
                error,
                SemanticError::TypeError(type_error)
                    if type_error.message() == message
                        && error.span().is_some()
                        && !error.location().is_unset()
            )),
            "{source}: {errors:?}"
        );
    }
}

/// A struct written out is a closed struct of what is written, which the
/// place it meets decides on: a map's literal is the exception, and an open
/// place leaves its other fields as they are.
#[test]
fn a_struct_written_out_meets_its_place_like_any_other_value() {
    let of = |source: &str| {
        let checked = crate::dsrv_fixtures::checked(source);
        let y = checked.var_expr_ref(&VarName::new("y")).unwrap();
        y.typ().clone()
    };
    let fields = |open: bool| {
        StreamType::Struct(
            EcoVec::from([
                (EcoString::from("a"), StreamType::Int),
                (EcoString::from("b"), StreamType::Int),
            ]),
            open,
        )
    };
    assert_eq!(of("out y = {a: 1, b: 2}"), fields(false));
    assert_eq!(
        of("out y: Struct<a: Int, ...> = {a: 1, b: 2}"),
        fields(false)
    );
    assert_eq!(
        of("out y: Map<Float> = {a: 1}"),
        StreamType::Map(Box::new(StreamType::Float))
    );
    for source in [
        "in c: Bool\nout y: Struct<a: Int, ...> = if c then {a: 1, b: 2} else {a: 2}",
        "in c: Bool\nout y: Struct<a: Int, ...> = if c then {a: 2} else {a: 1, b: 2}",
    ] {
        assert!(accepted(source), "{source}");
    }
    for (source, kind) in [
        (
            "out y: Struct<a: Int> = {a: 1, b: 2}",
            TypeErrorKind::StructUnknownField,
        ),
        (
            "out y: Struct<a: Int, b: Int> = {a: 1}",
            TypeErrorKind::StructMissingField,
        ),
        (
            "out y: Struct<a: Struct<x: Int, y: Int>> = {a: {x: 1}}",
            TypeErrorKind::StructMissingField,
        ),
        (
            "out y: Struct<a: Int> = {a: \"s\"}",
            TypeErrorKind::StructFieldTypeMismatch,
        ),
        ("out y: Int = {a: 1}", TypeErrorKind::AnnotationTypeMismatch),
        (
            "out y: Map<Int> = Struct(\"a\": 1)",
            TypeErrorKind::StructExpected,
        ),
    ] {
        for (mode, errors) in refusals(source) {
            assert!(
                errors.iter().any(|error| matches!(
                    error,
                    SemanticError::TypeError(type_error)
                        if *type_error.kind() == kind && error.span().is_some()
                )),
                "{mode}: {source}: {errors:?}"
            );
        }
    }
}

/// Each reason a value cannot flow is told apart, and said.
#[test]
fn a_failed_flow_says_why() {
    use super::super::relations::Mismatch;
    let closed = |names: &[&str]| {
        StreamType::Struct(
            names
                .iter()
                .map(|name| ((*name).into(), StreamType::Int))
                .collect(),
            false,
        )
    };
    let open = |names: &[&str]| {
        StreamType::Struct(
            names
                .iter()
                .map(|name| ((*name).into(), StreamType::Int))
                .collect(),
            true,
        )
    };
    let why = |actual: &StreamType, expected: &StreamType| flow(actual, expected).unwrap_err();
    assert_eq!(why(&StreamType::Int, &StreamType::Str), Mismatch::Types);
    assert_eq!(
        why(&closed(&["a"]), &closed(&["a", "b"])),
        Mismatch::MissingField("b".into())
    );
    assert_eq!(
        why(&open(&["a"]), &open(&["a", "b"])),
        Mismatch::MissingField("b".into())
    );
    assert_eq!(
        why(&closed(&["a", "b"]), &closed(&["a"])),
        Mismatch::ExtraField("b".into())
    );
    assert_eq!(why(&open(&["a"]), &closed(&["a"])), Mismatch::OpenValue);
}

/// A checking failure is the error, in both modes. The checks waiting for types it
/// left undecided do not run and replace it.
#[test]
fn a_failure_is_not_replaced_by_a_check_that_waited_on_it() {
    let source = "in x:Int\naux f = \\v:List<Int> -> Tuple(v, !1, v == [1])\nout y = x";
    for (mode, errors) in refusals(source) {
        let [SemanticError::TypeError(error)] = &errors[..] else {
            panic!("{mode}: {errors:?}");
        };
        assert_eq!(error.message(), "expected Bool, got Int", "{mode}");
    }
}

/// A struct constructor needs no expected type: one of another type is a plain
/// mismatch of the two.
#[test]
fn a_struct_constructor_of_the_wrong_type_is_a_plain_mismatch() {
    for (mode, errors) in refusals("in x: Int\nout y: Int = Struct(\"a\": x)") {
        let [SemanticError::TypeError(error)] = &errors[..] else {
            panic!("{mode}: {errors:?}");
        };
        assert_eq!(error.kind(), &TypeErrorKind::StructExpected, "{mode}");
        assert_eq!(
            error.message(),
            "expected Int, got Struct<a: Int>",
            "{mode}"
        );
    }
    type_check("in x: Int\nout y = Struct(\"a\": x)".parse().unwrap())
        .expect("a struct constructor with no expected type is accepted");
}

/// A function's parameter goes the other way round, so what it lacks is said of
/// the parameter expected, and a parameter of a parameter is the right way round
/// again.
#[test]
fn a_failed_flow_of_a_parameter_says_which_side_it_is_of() {
    use super::super::relations::Mismatch;
    let structure = |names: &[&str], open| {
        StreamType::Struct(
            names
                .iter()
                .map(|name| ((*name).into(), StreamType::Int))
                .collect(),
            open,
        )
    };
    let taking = |parameter: StreamType| {
        StreamType::Function(eco_vec![parameter], Box::new(StreamType::Int))
    };
    let why = |actual: &StreamType, expected: &StreamType| flow(actual, expected).unwrap_err();
    assert_eq!(
        why(
            &taking(structure(&["a", "b"], false)),
            &taking(structure(&["a"], true))
        ),
        Mismatch::Parameter(Box::new(Mismatch::MissingField("b".into())))
    );
    assert_eq!(
        why(
            &taking(taking(structure(&["a"], true))),
            &taking(taking(structure(&["a", "b"], false)))
        ),
        Mismatch::Parameter(Box::new(Mismatch::Parameter(Box::new(
            Mismatch::MissingField("b".into())
        ))))
    );
    let (_, errors) = &refusals(
        "in x: Int\n\
             aux f: (Struct<a: Int, b: Int> -> Int) = \\s: Struct<a: Int, b: Int> -> s.a\n\
             aux g: (Struct<a: Int, ...> -> Int) = f\nout y: Int = x",
    )[0];
    let [SemanticError::TypeError(error)] = &errors[..] else {
        panic!("{errors:?}");
    };
    assert_eq!(
        error.message(),
        "expected (Struct<a: Int, ...> -> Int), got (Struct<a: Int, b: Int> -> Int); its \
             parameter needs the field `b`, which the parameter expected here does not list"
    );
}

/// An open struct with no fields is a type that can be written, checked,
/// and reached by a merge; nothing can be read from it.
#[test]
fn an_open_struct_with_no_fields_is_a_type() {
    let source = "in s: Struct<...>\nout y: Struct<...>\ny = s";
    type_check(source.parse().unwrap()).unwrap_or_else(|errors| panic!("{errors:?}"));
    for (mode, errors) in refusals("in s: Struct<...>\nout y: Int\ny = s.a") {
        assert!(
            errors.iter().any(|error| matches!(
                error,
                SemanticError::TypeError(type_error)
                    if type_error.kind() == &TypeErrorKind::StructFieldAccess
            )),
            "{mode}: {errors:?}"
        );
    }
}

/// The fields a struct type does not list are not readable, so asking
/// whether one is there is refused as reading it is.
#[test]
fn has_key_of_a_field_the_struct_type_does_not_list_is_refused_like_get() {
    for struct_type in ["Struct<a: Int, ...>", "Struct<a: Int>"] {
        let body = |call: &str| {
            format!("in p: {struct_type}\nout t: {}\nt = {call}", {
                if call.starts_with("Map.has_key") {
                    "Bool"
                } else {
                    "Int"
                }
            })
        };
        let refused_get = refusals(&body("Map.get(p, \"b\")"));
        let refused_has_key = refusals(&body("Map.has_key(p, \"b\")"));
        for ((mode, get), (_, has_key)) in refused_get.iter().zip(&refused_has_key) {
            let describe = |errors: &[SemanticError]| match errors {
                [error @ SemanticError::TypeError(type_error)]
                    if error.span().is_some() && !error.location().is_unset() =>
                {
                    (
                        format!("{:?}", type_error.kind()),
                        type_error.message().to_owned(),
                    )
                }
                other => panic!("{mode}: {struct_type}: {other:?}"),
            };
            assert_eq!(describe(get), describe(has_key), "{mode}: {struct_type}");
            assert_eq!(describe(has_key).0, "StructFieldAccess");
        }
        for call in ["Map.has_key(p, \"a\")"] {
            type_check(body(call).parse().unwrap())
                .unwrap_or_else(|errors| panic!("{struct_type}: {call}: {errors:?}"));
        }
    }
}

#[test]
fn an_anonymous_call_passes_its_expected_union_type_to_its_body() {
    let source = "use experimental::tagged_unions\n\
                  type Reading = Union<Some: Float, None>\n\
                  in x: Float\n\
                  out y: Reading = (\\v: Float -> Some(v))(x)\n";

    let checked = crate::dsrv_fixtures::checked(source);
    assert!(matches!(
        checked.var_expr_ref(&"y".into()).unwrap().typ(),
        StreamType::Union(_)
    ));
}

#[test]
fn struct_equality_needs_operands_of_the_same_struct_type() {
    type_check(
        "in a: Struct<x: Int>\nin b: Struct<x: Int>\nout same: Bool = a == b"
            .parse()
            .unwrap(),
    )
    .unwrap();
    type_check(
        "in n: Int\nout same: Bool = {x: n, y: 2} != {y: 2, x: 1}"
            .parse()
            .unwrap(),
    )
    .unwrap();
    for source in [
        "in n: Int\nout same: Bool = {x: 1} == {x: \"a\"}",
        "in n: Int\nout same: Bool = {x: 1} == {x: 1, y: 2}",
        "in n: Int\nout same: Bool = {x: 1} == n",
    ] {
        let Err(errors) = type_check(source.parse().unwrap()) else {
            panic!("{source} must not type check");
        };
        assert!(
            errors
                .iter()
                .any(|error| matches!(error, SemanticError::TypeError(error)
                    if error.kind() == &TypeErrorKind::OperatorTypeMismatch)),
            "{source}: {errors:?}"
        );
    }
}
