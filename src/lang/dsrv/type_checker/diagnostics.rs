//! Located static errors, ordered warnings and completed analysis reports.

use super::relations::Mismatch;

use crate::VarName;
use crate::core::StreamType;
use crate::lang::dsrv::ast::{Declaration, DsrvSpecification, ExprRef, ExprView};
use crate::lang::dsrv::diagnostics::{SemanticError, TypeErrorKind};
use crate::lang::dsrv::source_map::NodeOrigin;
use crate::lang::dsrv::source_map::SourceLocation;
use crate::lang::dsrv::span::Span;

use crate::lang::dsrv::ast::ExprId;
use crate::lang::dsrv::diagnostics::{
    SemanticAnalysisReport, SemanticErrors, SemanticWarning, materialise_errors,
};
use crate::lang::dsrv::source_map::SourceArchive;

#[derive(Default)]
pub(super) struct WarningCollector {
    found: Vec<(ExprId, SemanticWarning)>,
}

impl WarningCollector {
    pub(super) fn record(&mut self, occurrence: ExprId, warning: SemanticWarning) {
        self.found.push((occurrence, warning));
    }

    /// Close the attempt. Warnings are ordered by source position, those
    /// without one last, then by node, whatever order they were
    /// proved in.
    ///
    /// Every finding takes owned files from `sources`, the archive the
    /// checked nodes' IDs belong to, so the report outlives it.
    pub(super) fn finish_report<T>(
        self,
        result: Result<T, SemanticErrors>,
        sources: Option<&SourceArchive>,
    ) -> SemanticAnalysisReport<T> {
        let mut findings = self.found;
        findings.sort_by_key(|(node, warning)| (warning.span().is_none(), warning.span(), *node));
        let mut warnings = findings
            .into_iter()
            .map(|(_, warning)| warning)
            .collect::<Vec<_>>();
        for warning in &mut warnings {
            warning.materialise(sources);
        }
        let result = result.map_err(|mut errors| {
            materialise_errors(&mut errors, sources);
            errors
        });
        SemanticAnalysisReport::new(result, warnings)
    }
}

/// A value of type `actual` that could not go where `expected` is needed.
pub(super) struct RejectedFlow<'types> {
    pub(super) actual: &'types StreamType,
    pub(super) expected: &'types StreamType,
    pub(super) why: Mismatch,
}

impl RejectedFlow<'_> {
    /// What to add to a message that names the two types, starting with `; `
    /// if there is anything.
    pub(super) fn detail(&self) -> String {
        match &self.why {
            Mismatch::Types => match (self.expected, self.actual) {
                (StreamType::Float, StreamType::Int) => {
                    "; convert the Int explicitly with `as Float`".to_owned()
                }
                _ => String::new(),
            },
            Mismatch::MissingField(name) => format!("; it does not list the field `{name}`"),
            Mismatch::ExtraField(name) => {
                format!("; it lists the field `{name}`, and a closed struct has no others")
            }
            Mismatch::OpenValue => {
                "; a closed struct has no other fields, and an open struct does not say that \
                 of its value"
                    .to_owned()
            }
            Mismatch::Parameter(why) => match why.as_ref() {
                Mismatch::MissingField(name) => format!(
                    "; its parameter needs the field `{name}`, which the parameter expected \
                     here does not list"
                ),
                Mismatch::ExtraField(name) => format!(
                    "; its parameter is a closed struct without the field `{name}`, which the \
                     parameter expected here lists"
                ),
                Mismatch::OpenValue => "; its parameter is a closed struct, and the parameter \
                     expected here does not say it has no other fields"
                    .to_owned(),
                Mismatch::Types | Mismatch::Parameter(_) => String::new(),
            },
        }
    }
}

/// That the stream `var` of `spec` has no annotation, where it is declared.
pub(super) fn missing_annotation(spec: &DsrvSpecification, var: &VarName) -> SemanticError {
    let declared = spec
        .declarations()
        .iter()
        .find_map(|declaration| match declaration {
            Declaration::Input { name, span, .. }
            | Declaration::Output { name, span, .. }
            | Declaration::Aux { name, span, .. }
                if name == var =>
            {
                Some(*span)
            }
            _ => None,
        });
    SemanticError::MissingTypeAnnotation(
        format!("Variable {var} is missing a type annotation"),
        declared,
        SourceLocation::default(),
    )
    .located(NodeOrigin::new(spec.sources().root(), None))
}

/// The error of a value that could not go where it was needed, at `expr`. A
/// struct written out has its own words for not having the fields wanted.
pub(super) fn mismatch(expr: ExprRef<'_>, rejected: RejectedFlow<'_>) -> SemanticError {
    let written = matches!(
        expr.view(),
        ExprView::Struct(_) | ExprView::ObjectLiteral(_)
    );
    match &rejected.why {
        Mismatch::MissingField(name) if written => error(
            expr,
            TypeErrorKind::StructMissingField,
            format!("Struct constructor is missing required field {name}"),
        ),
        Mismatch::ExtraField(name) if written => error(
            expr,
            TypeErrorKind::StructUnknownField,
            format!("Struct constructor contains unknown fields, such as {name}"),
        ),
        why => error(
            expr,
            match why {
                Mismatch::Types if matches!(expr.view(), ExprView::Struct(_)) => {
                    TypeErrorKind::StructExpected
                }
                _ => TypeErrorKind::AnnotationTypeMismatch,
            },
            format!(
                "expected {}, got {}{}",
                rejected.expected,
                rejected.actual,
                rejected.detail()
            ),
        ),
    }
}

pub(super) fn error(
    expr: ExprRef<'_>,
    kind: TypeErrorKind,
    message: impl Into<String>,
) -> SemanticError {
    error_at(Site::of(expr), kind, message)
}

/// A type error at `site`, for a check run after the node was checked.
pub(super) fn error_at(
    site: Site,
    kind: TypeErrorKind,
    message: impl Into<String>,
) -> SemanticError {
    SemanticError::type_error_at(kind, message.into(), site.span).located(site.origin)
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Site {
    pub(super) id: ExprId,
    pub(super) span: Span,
    pub(super) origin: NodeOrigin,
}
impl Site {
    pub(super) fn of(expr: ExprRef<'_>) -> Self {
        Self {
            id: expr.id(),
            span: expr.span(),
            origin: expr.origin(),
        }
    }
}
pub(super) fn annotation_needed(expr: ExprRef<'_>, message: impl Into<String>) -> SemanticError {
    SemanticError::unresolved_type_at(
        crate::lang::dsrv::diagnostics::UnresolvedTypeKind::VariableType,
        message.into(),
        expr.span(),
    )
    .located(expr.origin())
}
