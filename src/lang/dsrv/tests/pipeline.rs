//! Contracts shared by the specification entry points.

use crate::core::StreamType;
use crate::lang::dsrv::ast::{
    CheckedDsrvSpecification, DsrvSpecification, ExprView, SyntaxLiteral,
};
use crate::lang::dsrv::diagnostics::{SemanticError, SemanticWarningKind};
use crate::lang::dsrv::{DsrvParseError, ElaboratedDsrvSpecification};

#[test]
fn specification_entry_points_agree_on_types_rewrites_and_diagnostics() {
    let source = "in text: Str\nin x: Int\nout promoted: Float = 1\n\
                  out redundant: Int = x as Int\nout late: Int = dynamic(text: Int, {x})";
    let specification = source.parse::<DsrvSpecification>().unwrap();
    let context = specification.source_context().fingerprint().clone();
    let reports = [
        specification
            .clone()
            .check()
            .map_checked(CheckedDsrvSpecification::elaborate),
        specification
            .clone()
            .validate()
            .unwrap()
            .check()
            .map_checked(CheckedDsrvSpecification::elaborate),
        CheckedDsrvSpecification::parse(source)
            .unwrap()
            .map_checked(CheckedDsrvSpecification::elaborate),
        specification.check_and_elaborate(),
        ElaboratedDsrvSpecification::parse(source).unwrap(),
    ];

    for report in reports {
        let (result, warnings) = report.into_parts();
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert_eq!(warnings[0].kind(), SemanticWarningKind::RedundantCast);
        assert_eq!(&source[warnings[0].span().unwrap().to_range()], "x as Int");
        let elaborated = result.expect("every entry point should check the specification");

        let written = elaborated
            .source()
            .var_expr_ref(&"promoted".into())
            .unwrap();
        assert_eq!(written.typ(), &StreamType::Float);
        assert!(matches!(
            written.view(),
            ExprView::Val(SyntaxLiteral::Int(1))
        ));
        let promoted = elaborated.var_expr_ref(&"promoted".into()).unwrap();
        assert_eq!(promoted.typ(), &StreamType::Float);
        assert!(
            matches!(promoted.view(), ExprView::Val(SyntaxLiteral::Float(value)) if *value == 1.0)
        );
        assert_eq!(&source[promoted.expr().span().to_range()], "1");
        assert_eq!(
            promoted.expr().source_context().unwrap().fingerprint(),
            &context
        );

        let redundant = elaborated.var_expr_ref(&"redundant".into()).unwrap();
        assert_eq!(redundant.typ(), &StreamType::Int);
        assert!(matches!(redundant.view(), ExprView::Var(name) if name == &"x".into()));
        let late = elaborated.var_expr_ref(&"late".into()).unwrap();
        let expression = late
            .runtime_expression()
            .parse_and_check("x + 1")
            .expect("every elaboration entry point prepares the runtime scope and typing");
        assert_eq!(expression.as_ref().typ(), &StreamType::Int);
    }
}

#[test]
fn source_entry_points_keep_parse_and_semantic_errors_distinct() {
    for source in ["out y\ny = ", "out y: Missing = 1"] {
        let reports = [
            CheckedDsrvSpecification::parse(source).map(|report| report.map_checked(|_| ())),
            ElaboratedDsrvSpecification::parse(source).map(|report| report.map_checked(|_| ())),
        ];
        for report in reports {
            assert!(
                matches!(
                    report,
                    Err(DsrvParseError::Syntax(_) | DsrvParseError::Resolve(_))
                ),
                "syntax/expansion should fail before producing a semantic report: {source}: {report:?}"
            );
        }
    }
    let source = "out y: Bool = 1";
    let reports = [
        CheckedDsrvSpecification::parse(source).map(|report| report.map_checked(|_| ())),
        ElaboratedDsrvSpecification::parse(source).map(|report| report.map_checked(|_| ())),
    ];
    for report in reports {
        let (result, _) = report
            .expect("well-formed source reaches semantic checking")
            .into_parts();
        let errors = result.expect_err("the equation does not match its declared type");
        assert!(
            errors
                .iter()
                .any(|error| matches!(error, SemanticError::TypeError(_))),
            "{errors:?}"
        );
    }
}

#[test]
fn checking_preserves_validation_errors() {
    for source in [
        "in z\nin z\nout result = z",
        "out result = missing",
        "in source: Str\nout result = dynamic(source: Int, {missing})",
    ] {
        let specification = source.parse::<DsrvSpecification>().unwrap();
        let expected = specification.clone().validate().unwrap_err();
        let (result, warnings) = specification.check().into_parts();
        assert_eq!(result.unwrap_err(), expected, "{source}");
        assert!(
            warnings.is_empty(),
            "validation must stop before type-checking warnings: {warnings:?}"
        );
    }
}
