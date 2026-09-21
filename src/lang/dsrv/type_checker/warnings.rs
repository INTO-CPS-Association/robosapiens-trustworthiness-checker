//! Warnings collected during one checking attempt.
//!
//! A warning rule looks at one checked node and its type and proves its
//! finding from that node alone. Only authoritative work is retained: strict
//! checking, successful or accepted-widening gradual inference attempts, the
//! final expression pass, and standalone expression checking. Findings from a
//! failed or unresolved gradual inference attempt are discarded.
//!
//! A finding is identified by the expanded node it was proved at and the rule
//! that proved it, so a node visited twice reports once, while two copies of
//! one def body report separately even though both carry the call site's
//! span.

use std::collections::BTreeMap;

use super::TCType;
use crate::lang::dsrv::ast::{ExprId, ExprRef};
use crate::lang::dsrv::diagnostics::{
    SemanticAnalysisReport, SemanticErrors, SemanticWarning, SemanticWarningKind,
};

/// A rule proving a warning from one checked node and its type.
pub(super) type WarningRule = fn(ExprRef<'_>, &TCType) -> Option<SemanticWarning>;

/// Every warning rule.
#[cfg(not(test))]
pub(super) const WARNING_RULES: &[WarningRule] = &[];
#[cfg(test)]
pub(super) const WARNING_RULES: &[WarningRule] = &[fixture::alpha, fixture::beta];

#[derive(Default)]
pub(super) struct WarningCollector {
    found: BTreeMap<(ExprId, SemanticWarningKind), SemanticWarning>,
}

impl WarningCollector {
    /// Apply every rule to a checked node.
    pub(super) fn observe(&mut self, expr: ExprRef<'_>, typ: &TCType) {
        for rule in WARNING_RULES {
            if let Some(warning) = rule(expr, typ) {
                self.emit(expr.id(), warning);
            }
        }
    }

    pub(super) fn emit(&mut self, occurrence: ExprId, warning: SemanticWarning) {
        self.found
            .entry((occurrence, warning.kind()))
            .or_insert(warning);
    }

    /// Retain findings from an inference attempt that became authoritative.
    pub(super) fn absorb(&mut self, other: Self) {
        for (key, warning) in other.found {
            self.found.entry(key).or_insert(warning);
        }
    }

    /// Close the attempt. Warnings are ordered by source position, those
    /// without one last, then by node and rule, whatever order they were
    /// proved in.
    pub(super) fn report<T>(self, result: Result<T, SemanticErrors>) -> SemanticAnalysisReport<T> {
        let mut warnings = self.found.into_values().collect::<Vec<_>>();
        warnings.sort_by_key(|warning| (warning.span().is_none(), warning.span()));
        SemanticAnalysisReport::new(result, warnings)
    }
}

/// Test-only rules standing in for real ones. A string literal
/// `"warn:alpha"` or `"warn:beta"` proves that rule's warning at itself,
/// `"warn:both"` proves both, and `"warn:unplaced"` proves the beta warning
/// without a position.
#[cfg(test)]
mod fixture {
    use super::*;
    use crate::lang::dsrv::ast::{ExprView, SyntaxLiteral};

    fn literal(expr: ExprRef<'_>) -> Option<&str> {
        match expr.view() {
            ExprView::Val(SyntaxLiteral::Str(text)) => Some(text.as_str()),
            _ => None,
        }
    }

    pub(super) fn alpha(expr: ExprRef<'_>, _: &TCType) -> Option<SemanticWarning> {
        matches!(literal(expr)?, "warn:alpha" | "warn:both").then(|| {
            SemanticWarning::new(
                SemanticWarningKind::TestAlpha,
                "alpha fixture",
                Some(expr.span()),
            )
        })
    }

    pub(super) fn beta(expr: ExprRef<'_>, _: &TCType) -> Option<SemanticWarning> {
        match literal(expr)? {
            "warn:beta" | "warn:both" => Some(SemanticWarning::new(
                SemanticWarningKind::TestBeta,
                "beta fixture",
                Some(expr.span()),
            )),
            "warn:unplaced" => Some(SemanticWarning::new(
                SemanticWarningKind::TestBeta,
                "beta fixture",
                None,
            )),
            _ => None,
        }
    }
}
