//! How checking reports warnings, exercised with the test-only fixture rules
//! in `type_checker::warnings`: a string literal `"warn:alpha"`,
//! `"warn:beta"` or `"warn:both"` proves those warnings at itself, and
//! `"warn:unplaced"` proves a beta warning without a position.

use crate::core::StreamType;
use crate::lang::dsrv::ast::DsrvSpecification;
use crate::lang::dsrv::diagnostics::{
    SemanticAnalysisReport, SemanticError, SemanticWarning, SemanticWarningKind,
};
use crate::lang::dsrv::parser::parse_expr;
use crate::lang::dsrv::type_checker::type_check_expression;
use crate::lang::dsrv::{ElaboratedDsrvSpecification, TypeCheckOptions};

use test_log::test;

const MODES: [TypeCheckOptions; 2] = [TypeCheckOptions::STRICT, TypeCheckOptions::GRADUAL];

fn check(
    source: &str,
    options: TypeCheckOptions,
) -> SemanticAnalysisReport<crate::CheckedDsrvSpecification> {
    source
        .parse::<DsrvSpecification>()
        .unwrap_or_else(|error| panic!("{source}: {error}"))
        .check(options)
}

/// Each warning as its rule and the text its span covers.
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
    let source = "out y: Str = \"warn:alpha\"";
    for options in MODES {
        let report = check(source, options);
        assert!(report.result().is_ok(), "{options:?}");
        assert_eq!(
            found(source, report.warnings()),
            [(SemanticWarningKind::TestAlpha, Some("\"warn:alpha\""))],
            "{options:?}"
        );
        let warning = &report.warnings()[0];
        assert_eq!(warning.code(), "test-alpha");
        assert_eq!(warning.message(), "alpha fixture");
    }
}

#[test]
fn a_failed_check_keeps_the_warnings_it_had_proved() {
    let source = "out y: Str = \"warn:alpha\"\nout z: Bool = 1";
    let (result, warnings) = check(source, TypeCheckOptions::STRICT).into_parts();
    let errors = result.expect_err("z is ill-typed");
    assert!(
        errors
            .iter()
            .any(|error| matches!(error, SemanticError::TypeError(_)))
    );
    assert_eq!(
        found(source, &warnings),
        [(SemanticWarningKind::TestAlpha, Some("\"warn:alpha\""))]
    );
}

#[test]
fn validation_failure_stops_before_anything_warns() {
    let source = "out y: Str = \"warn:alpha\"\nout z: Int = missing";
    for options in MODES {
        let report = check(source, options);
        assert!(report.result().is_err(), "{options:?}");
        assert!(report.warnings().is_empty(), "{options:?}");
    }
}

#[test]
fn gradual_checking_revisits_a_node_but_reports_it_once() {
    // `y` is inferred before `z` can be, so inference visits it more than
    // once; only the final pass is authoritative.
    let source = "out z = y\nout y = \"warn:alpha\"";
    let report = check(source, TypeCheckOptions::GRADUAL);
    assert!(report.result().is_ok());
    assert_eq!(
        found(source, report.warnings()),
        [(SemanticWarningKind::TestAlpha, Some("\"warn:alpha\""))]
    );
}

#[test]
fn gradual_inference_failure_reports_no_warning() {
    // Inference fails before the authoritative pass runs.
    let source = "out y = \"warn:alpha\"\nout z: Bool = 1";
    let report = check(source, TypeCheckOptions::GRADUAL);
    assert!(report.result().is_err());
    assert!(report.warnings().is_empty());
}

#[test]
fn warnings_are_ordered_by_position_then_rule_whatever_the_visiting_order() {
    // Roots are checked by name, so `a` is visited before `z`.
    let source = "out z: Str = \"warn:both\"\n\
                  out b: Str = \"warn:unplaced\"\n\
                  out a: Str = \"warn:beta\"";
    for options in MODES {
        let report = check(source, options);
        assert_eq!(
            found(source, report.warnings()),
            [
                (SemanticWarningKind::TestAlpha, Some("\"warn:both\"")),
                (SemanticWarningKind::TestBeta, Some("\"warn:both\"")),
                (SemanticWarningKind::TestBeta, Some("\"warn:beta\"")),
                (SemanticWarningKind::TestBeta, None),
            ],
            "{options:?}"
        );
    }
}

#[test]
fn two_copies_of_a_def_body_warn_separately_though_they_share_a_span() {
    // A def from the same file keeps its body's spans, so both inlined
    // copies of the literal carry one span and one code; they are still two
    // expanded nodes.
    let source = "use experimental::{functions}\n\
                  def tag(n: Int) -> Str = \"warn:alpha\"\n\
                  in x: Int\n\
                  out y: Str = tag(x)\n\
                  out z: Str = tag(x)";
    for options in MODES {
        let report = check(source, options);
        assert!(report.result().is_ok(), "{options:?}");
        let warnings = report.warnings();
        assert_eq!(warnings.len(), 2, "{options:?}: {warnings:?}");
        assert_eq!(warnings[0], warnings[1]);
        assert_eq!(
            &source[warnings[0].span().unwrap().to_range()],
            "\"warn:alpha\""
        );
    }
}

#[test]
fn elaboration_and_parsing_with_options_keep_the_warnings() {
    let source = "out y: Str = \"warn:alpha\"";
    for options in MODES {
        let elaborated =
            check(source, options).map_checked(crate::CheckedDsrvSpecification::elaborate);
        assert_eq!(elaborated.warnings().len(), 1);
        let report = ElaboratedDsrvSpecification::parse_with(source, options).unwrap();
        assert_eq!(
            found(source, report.warnings()),
            [(SemanticWarningKind::TestAlpha, Some("\"warn:alpha\""))]
        );
        let report = source
            .parse::<DsrvSpecification>()
            .unwrap()
            .check_and_elaborate(options);
        assert_eq!(report.warnings().len(), 1);
        assert!(report.discard_warnings().is_ok());
    }
}

#[test]
fn standalone_expression_checking_reports_warnings_with_its_result() {
    let environment = Default::default();
    let text = "\"warn:alpha\"";
    let report = type_check_expression(&parse_expr(text).unwrap(), &StreamType::Str, &environment);
    assert_eq!(
        found(text, report.warnings()),
        [(SemanticWarningKind::TestAlpha, Some(text))]
    );
    assert!(report.result().is_ok());

    // The first element is proved before the second fails.
    let text = "[\"warn:alpha\", 1]";
    let report = type_check_expression(
        &parse_expr(text).unwrap(),
        &StreamType::List(Box::new(StreamType::Str)),
        &environment,
    );
    assert!(report.result().is_err());
    assert_eq!(
        found(text, report.warnings()),
        [(SemanticWarningKind::TestAlpha, Some("\"warn:alpha\""))]
    );
}

#[test]
fn runtime_text_discards_its_warnings() {
    use crate::lang::dsrv::runtime_text::RuntimeText;
    // Runtime text has no channel for warnings: it checks, and a runtime
    // sees only the checked expression.
    let checked = RuntimeText::default()
        .accept("\"warn:alpha\"")
        .expect("runtime text that warns still checks");
    assert_eq!(checked.expr().to_string(), "\"warn:alpha\"");
}
