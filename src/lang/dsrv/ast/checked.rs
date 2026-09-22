//! Type-checked expression handles and cursors.

use contiguous_tree::{ContextCursor, TreeCursorExt};

#[cfg(test)]
use super::AstWeak;
use super::{AstShared, Expr, ExprArena, ExprKind, ExprRef, ExprView};
use crate::lang::dsrv::runtime_expression::{
    RuntimeExpressionSite, RuntimeExpressionSites, RuntimeExpressionTyping, UntypedExpr,
    prepare_site_table,
};
use crate::lang::dsrv::source_map::SourceArchive;
use crate::lang::dsrv::type_checker::{StreamTypeEnvironment, TCType};

pub(crate) type ExprTypes = contiguous_tree::NodeAnnotations<ExprArena, TCType>;
pub(crate) type ExprTypesBuilder = contiguous_tree::NodeAnnotationsBuilder<ExprArena, TCType>;

/// Immutable checked state shared by every checked expression handle: each
/// node's type, the stream environment, and, once an executable artefact has
/// been prepared, the sites of its runtime expressions.
#[derive(Clone, Debug)]
pub(crate) struct CheckedExpressionContext {
    expr_types: ExprTypes,
    environment: AstShared<StreamTypeEnvironment>,
    runtime_expressions: PreparationState,
}

/// Whether runtime-expression sites were prepared. A prepared artefact
/// without `dynamic` or `defer` is `Ready` with an empty table, which a
/// runtime can tell apart from a missed preparation.
#[derive(Clone, Debug)]
enum PreparationState {
    Unprepared,
    Ready(AstShared<RuntimeExpressionSites>),
}

impl CheckedExpressionContext {
    pub(crate) fn new(
        expr_types: ExprTypes,
        environment: AstShared<StreamTypeEnvironment>,
    ) -> Self {
        Self {
            expr_types,
            environment,
            runtime_expressions: PreparationState::Unprepared,
        }
    }

    pub(crate) fn type_of(&self, expr: ExprRef<'_>) -> &TCType {
        self.expr_types
            .get(expr)
            .expect("checked expression belongs to the typed tree or forest")
    }

    #[cfg(test)]
    pub(crate) fn shared_type_environment(&self) -> &AstShared<StreamTypeEnvironment> {
        &self.environment
    }

    pub(crate) fn has_type(&self, expr: ExprRef<'_>) -> bool {
        self.expr_types.get(expr).is_some()
    }

    /// Prepare the sites of every runtime-expression occurrence among
    /// `nodes`, as a new context; this one is unchanged. Each site captures
    /// from `sources`, the archive the nodes' IDs belong to, what its text
    /// may need.
    pub(super) fn prepare_sites<'arena>(
        &self,
        builder: contiguous_tree::SparseNodeAnnotationsBuilder<ExprArena, RuntimeExpressionSite>,
        nodes: impl IntoIterator<Item = ExprRef<'arena>>,
        sources: Option<&SourceArchive>,
    ) -> Self {
        let sites = prepare_site_table(builder, nodes, sources, |node| {
            Some(RuntimeExpressionTyping {
                environment: AstShared::clone(&self.environment),
                expected: self.type_of(node).clone(),
            })
        });
        Self {
            expr_types: self.expr_types.clone(),
            environment: AstShared::clone(&self.environment),
            runtime_expressions: PreparationState::Ready(AstShared::new(sites)),
        }
    }

    /// Whether this context's sites were prepared for the storage of `expr`.
    pub(super) fn sites_are_ready_for(&self, expr: ExprRef<'_>) -> bool {
        match &self.runtime_expressions {
            PreparationState::Unprepared => false,
            PreparationState::Ready(sites) => sites.belongs_to(expr),
        }
    }

    /// # Panics
    ///
    /// If no preparation ran: a sourced execution path must never fall back
    /// to a default site.
    fn runtime_expression_sites(&self) -> &AstShared<RuntimeExpressionSites> {
        match &self.runtime_expressions {
            PreparationState::Unprepared => {
                panic!("runtime-expression sites were not prepared before execution")
            }
            PreparationState::Ready(sites) => sites,
        }
    }

    pub(crate) fn runtime_expression(&self, expr: ExprRef<'_>) -> &RuntimeExpressionSite {
        self.runtime_expression_sites().site(expr)
    }

    pub(crate) fn prepared_site_count(&self) -> Option<usize> {
        match &self.runtime_expressions {
            PreparationState::Unprepared => None,
            PreparationState::Ready(sites) => Some(sites.len()),
        }
    }
}

/// An expression whose complete syntax tree has been type checked.
#[derive(Clone)]
pub struct CheckedExpr {
    pub(super) expr: Expr,
    checked: AstShared<CheckedExpressionContext>,
}

/// A borrowed syntax cursor paired with its checked type.
#[derive(Clone, Copy, contiguous_tree::TreeCursor)]
#[tree_cursor(delegate = cursor, target = ExprRef<'arena>)]
pub struct CheckedExprRef<'arena> {
    cursor: ContextCursor<ExprRef<'arena>, &'arena CheckedExpressionContext>,
}

impl CheckedExpr {
    pub(crate) fn new(
        expr: Expr,
        expr_types: ExprTypes,
        environment: AstShared<StreamTypeEnvironment>,
    ) -> Self {
        let checked = AstShared::new(CheckedExpressionContext::new(expr_types, environment));
        Self::from_checked_types(expr, checked)
    }

    pub(super) fn from_checked_types(
        expr: Expr,
        checked: AstShared<CheckedExpressionContext>,
    ) -> Self {
        Self { expr, checked }
    }

    pub fn expr(&self) -> &Expr {
        &self.expr
    }

    pub fn typ(&self) -> &TCType {
        self.checked.type_of(self.expr.as_ref())
    }

    pub fn as_ref(&self) -> CheckedExprRef<'_> {
        self.expr.as_ref().with_checked_types(&self.checked)
    }

    pub(crate) fn cursor<'arena>(&'arena self, expr: ExprRef<'arena>) -> CheckedExprRef<'arena> {
        CheckedExprRef::new(expr, &self.checked)
    }

    /// The sites of this expression's runtime-expression occurrences, or
    /// `None` if it was never prepared.
    #[cfg(test)]
    pub(crate) fn prepared_site_count(&self) -> Option<usize> {
        self.checked.prepared_site_count()
    }

    /// Prepare the sites of this unlocated expression's runtime-expression
    /// occurrences before it escapes to a runtime.
    #[cfg(test)]
    pub(crate) fn prepare_sites(self) -> Self {
        self.prepare_sites_with(None)
    }

    /// Prepare the sites of this expression's runtime-expression occurrences
    /// before it escapes to a runtime, capturing from `sources`, the archive
    /// its IDs belong to, the files each needs.
    pub(crate) fn prepare_sites_with(self, sources: Option<&SourceArchive>) -> Self {
        let checked = self.checked.prepare_sites(
            self.expr.sparse_annotations_builder(),
            self.expr.as_ref().postorder(),
            sources,
        );
        Self {
            expr: self.expr,
            checked: AstShared::new(checked),
        }
    }

    /// This expression for a runtime that ignores types, keeping its sites.
    ///
    /// # Panics
    ///
    /// If its sites were not prepared.
    pub(crate) fn untyped(&self) -> UntypedExpr {
        UntypedExpr::new(
            self.expr.clone(),
            AstShared::clone(self.checked.runtime_expression_sites()),
        )
    }

    #[cfg(test)]
    pub(crate) fn shares_context_with(&self, other: &Self) -> bool {
        AstShared::ptr_eq(&self.checked, &other.checked)
    }

    #[cfg(test)]
    pub(crate) fn downgrade_context(&self) -> AstWeak<CheckedExpressionContext> {
        AstShared::downgrade(&self.checked)
    }
}

impl PartialEq for CheckedExpr {
    fn eq(&self, other: &Self) -> bool {
        if AstShared::ptr_eq(&self.checked, &other.checked) && self.expr.same_root(&other.expr) {
            return true;
        }
        if self.checked.environment != other.checked.environment {
            return false;
        }

        self.as_ref()
            .try_zip_with(other.as_ref(), |left, right| {
                Ok::<_, std::convert::Infallible>(
                    left.typ() == right.typ() && left.kind().same_payload(right.kind()),
                )
            })
            .unwrap_or_else(|never| match never {})
    }
}

impl<'arena> CheckedExprRef<'arena> {
    pub(super) fn new(expr: ExprRef<'arena>, checked: &'arena CheckedExpressionContext) -> Self {
        assert!(
            checked.expr_types.get(expr).is_some(),
            "checked type belongs to different expression storage or scope"
        );
        Self {
            cursor: ContextCursor::new(expr, checked),
        }
    }

    pub fn expr(self) -> ExprRef<'arena> {
        self.cursor.cursor()
    }

    pub fn view(self) -> ExprView<'arena, Self> {
        self.expr().view_with(self)
    }

    pub(crate) fn kind(self) -> &'arena ExprKind {
        self.expr().kind()
    }

    pub fn typ(self) -> &'arena TCType {
        self.cursor.context().type_of(self.expr())
    }

    #[cfg(test)]
    pub(crate) fn shared_type_environment(self) -> &'arena AstShared<StreamTypeEnvironment> {
        self.cursor.context().shared_type_environment()
    }

    pub(crate) fn runtime_expression(self) -> &'arena RuntimeExpressionSite {
        self.cursor.context().runtime_expression(self.expr())
    }
}
