//! How checking reports warnings, exercised with a redundant cast, `"a" as Str`,
//! which warns at itself. How a collector orders and identifies what is emitted to
//! it is tested in `type_checker::warnings`.

use crate::core::StreamType;
use crate::lang::dsrv::ElaboratedDsrvSpecification;
use crate::lang::dsrv::ast::DsrvSpecification;
use crate::lang::dsrv::diagnostics::{
    SemanticAnalysisReport, SemanticError, SemanticWarning, SemanticWarningKind,
};
use crate::lang::dsrv::parser::parse_expr;
use crate::lang::dsrv::type_checker::type_check_expression;

use test_log::test;

fn check(source: &str) -> SemanticAnalysisReport<crate::CheckedDsrvSpecification> {
    source
        .parse::<DsrvSpecification>()
        .unwrap_or_else(|error| panic!("{source}: {error}"))
        .check()
}

/// Each warning as its kind and the text its span covers.
fn found<'a>(
    source: &'a str,
    warnings: &[SemanticWarning],
) -> Vec<(SemanticWarningKind, Option<&'a str>)> {
    warnings
        .iter()
        .map(|warning| {
            (
                warning.kind(),
                warning.span().map(|span| &source[span.to_range()]),
            )
        })
        .collect()
}

#[test]
fn a_specification_that_checks_keeps_its_warnings() {
    let source = "out y: Str = \"a\" as Str";

    let report = check(source);
    assert!(report.result().is_ok(), "");
    assert_eq!(
        found(source, report.warnings()),
        [(SemanticWarningKind::RedundantCast, Some("\"a\" as Str"))],
        ""
    );
    let warning = &report.warnings()[0];
    assert_eq!(warning.code(), "dsrv.redundant-cast");
    assert_eq!(warning.message(), "cast from Str to Str is redundant");
}

#[test]
fn a_failed_check_keeps_the_warnings_it_had_proved() {
    let source = "out y: Str = \"a\" as Str\nout z: Bool = 1";
    let (result, warnings) = check(source).into_parts();
    let errors = result.expect_err("z is ill-typed");
    assert!(
        errors
            .iter()
            .any(|error| matches!(error, SemanticError::TypeError(_)))
    );
    assert_eq!(
        found(source, &warnings),
        [(SemanticWarningKind::RedundantCast, Some("\"a\" as Str"))]
    );
}

#[test]
fn validation_failure_stops_before_anything_warns() {
    let source = "out y: Str = \"a\" as Str\nout z: Int = missing";

    let report = check(source);
    assert!(report.result().is_err(), "");
    assert!(report.warnings().is_empty(), "");
}

#[test]
fn an_inferred_dependency_reports_a_warning_once() {
    // A dependency is checked once, even when a second root reads it.
    let source = "out z = y\nout y = \"a\" as Str";
    let report = check(source);
    assert!(report.result().is_ok());
    assert_eq!(
        found(source, report.warnings()),
        [(SemanticWarningKind::RedundantCast, Some("\"a\" as Str"))]
    );
}

#[test]
fn inference_failure_keeps_the_findings_of_the_failed_root() {
    let source = "out y: Bool = if true then \"a\" as Str else 1";
    let report = check(source);
    assert!(report.result().is_err());
    assert_eq!(
        found(source, report.warnings()),
        [(SemanticWarningKind::RedundantCast, Some("\"a\" as Str"))]
    );
}

/// Matching concrete contributions produce no merge warning.
#[test]
fn merges_that_are_not_surprising_do_not_warn() {
    for source in [
        "in c: Bool\nout z: Int\nz = if c then 1 else 2",
        "in c: Bool\nin f: Float\nout z: Float\nz = if c then 1 else f",
    ] {
        let report = check(source);
        assert!(report.result().is_ok(), "{source} ");
        assert!(report.warnings().is_empty(), "{source} ");
    }
    let source = "out z\nz = [1, 2]";
    let report = check(source);
    assert!(report.result().is_ok());
    assert!(report.warnings().is_empty());
}

#[test]
fn redundant_cast_is_reported_once_by_the_authoritative_pass() {
    let source = "out y = 1 as Int";
    let report = check(source);
    assert!(report.result().is_ok());
    assert_eq!(
        found(source, report.warnings()),
        [(SemanticWarningKind::RedundantCast, Some("1 as Int"))]
    );
}

#[test]
fn failed_or_unresolved_casts_do_not_warn() {
    for source in ["out y: Int = true as Int", "out y: Int = missing as Int"] {
        let report = check(source);
        assert!(report.result().is_err(), ": {source}");
        assert!(report.warnings().is_empty(), ": {source}");
    }
}

#[test]
fn an_explicit_integer_stream_to_float_conversion_is_not_redundant() {
    let source = "in x:Int\nout y:Float = x as Float";
    let report = check(source);
    assert!(report.result().is_ok());
    assert!(report.warnings().is_empty());
}

#[test]
fn warnings_are_ordered_by_position_whatever_the_visiting_order() {
    // Roots are checked by name, so `a` is visited before `z`.
    let source = "out z: Str = \"z\" as Str\nout a: Str = \"a\" as Str";

    let report = check(source);
    assert_eq!(
        found(source, report.warnings()),
        [
            (SemanticWarningKind::RedundantCast, Some("\"z\" as Str")),
            (SemanticWarningKind::RedundantCast, Some("\"a\" as Str")),
        ],
        ""
    );
}

#[test]
fn two_copies_of_a_def_body_warn_separately_though_they_share_a_span() {
    // A def from the same file keeps its body's spans, so both inlined
    // copies of the literal carry one span and one code; they are still two
    // expanded nodes.
    let source = "use experimental::{functions}\n\
                  def tag(n: Int) -> Str = \"a\" as Str\n\
                  in x: Int\n\
                  out y: Str = tag(x)\n\
                  out z: Str = tag(x)";

    let report = check(source);
    assert!(report.result().is_ok(), "");
    let warnings = report.warnings();
    assert_eq!(warnings.len(), 2, ": {warnings:?}");
    assert_eq!(warnings[0], warnings[1]);
    assert_eq!(
        &source[warnings[0].span().unwrap().to_range()],
        "\"a\" as Str"
    );
}

#[test]
fn elaboration_and_parsing_keep_the_warnings() {
    let source = "out y: Str = \"a\" as Str";

    let elaborated = check(source).map_checked(crate::CheckedDsrvSpecification::elaborate);
    assert_eq!(elaborated.warnings().len(), 1);
    let report = ElaboratedDsrvSpecification::parse(source).unwrap();
    assert_eq!(
        found(source, report.warnings()),
        [(SemanticWarningKind::RedundantCast, Some("\"a\" as Str"))]
    );
    let report = source
        .parse::<DsrvSpecification>()
        .unwrap()
        .check_and_elaborate();
    assert_eq!(report.warnings().len(), 1);
    assert!(report.discard_warnings().is_ok());
}

#[test]
fn standalone_expression_checking_reports_warnings_with_its_result() {
    let environment = Default::default();
    let text = "\"a\" as Str";
    let report = type_check_expression(&parse_expr(text).unwrap(), &StreamType::Str, &environment);
    assert_eq!(
        found(text, report.warnings()),
        [(SemanticWarningKind::RedundantCast, Some(text))]
    );
    assert!(report.result().is_ok());

    // The first element is proved before the second fails.
    let text = "[\"a\" as Str, 1]";
    let report = type_check_expression(
        &parse_expr(text).unwrap(),
        &StreamType::List(Box::new(StreamType::Str)),
        &environment,
    );
    assert!(report.result().is_err());
    assert_eq!(
        found(text, report.warnings()),
        [(SemanticWarningKind::RedundantCast, Some("\"a\" as Str"))]
    );
}

#[test]
fn runtime_expressions_discard_their_warnings() {
    let enclosing = crate::dsrv_fixtures::elaborated("out y:Str=dynamic(\"a\":Str)");
    let site = enclosing
        .var_expr_ref(&"y".into())
        .unwrap()
        .runtime_expression();
    let checked = site
        .parse_and_check("\"a\" as Str")
        .expect("a runtime expression that warns still checks");
    assert_eq!(checked.expr().to_string(), "(\"a\" as Str)");
}

#[test]
fn runtime_expressions_deliberately_discard_a_redundant_cast_warning() {
    let enclosing = crate::dsrv_fixtures::elaborated("out y:Int=dynamic(\"1\":Int)");
    let runtime_expression = enclosing
        .var_expr_ref(&"y".into())
        .unwrap()
        .runtime_expression();
    let checked = runtime_expression
        .parse_and_check("1 as Int")
        .expect("a runtime expression with a redundant cast still checks");
    // Elaboration makes the identity cheap without widening its checked type.
    assert_eq!(checked.expr().to_string(), "1");
    assert_eq!(checked.typ(), &crate::core::StreamType::Int);
}
