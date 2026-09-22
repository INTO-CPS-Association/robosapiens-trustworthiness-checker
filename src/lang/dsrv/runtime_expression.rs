//! Environments for expressions supplied at run time to `dynamic` and `defer`.
//!
//! Every runtime checks such source when it arrives and refuses source that does
//! not check, whether it then consults the types or not. The text is parsed
//! in the source context of the node it was supplied to, may call the same
//! defs that node's file could, and is checked against the type and
//! environment elaboration gave that node.
//!
//! Warnings proved while checking such text are discarded: runtime text has
//! no channel to present them on, so a runtime sees only whether the text
//! checked.

use std::collections::BTreeMap;

use contiguous_tree::TreeCursorExt;

use crate::core::StreamType;
use crate::lang::dsrv::ast::{AstShared, CheckedExpr, Expr, ExprArena, ExprKind, ExprRef};
use crate::lang::dsrv::diagnostics::SemanticErrors;
use crate::lang::dsrv::expand::functions::Callable;
use crate::lang::dsrv::parser::{DsrvParseError, parse_expr_with_functions};
use crate::lang::dsrv::source::SourceContext;
use crate::lang::dsrv::type_checker::{StreamTypeEnvironment, TCType, check_expression};

/// The type and environment runtime text is checked against: what elaboration
/// gave the `dynamic` or `defer` node the text was supplied to.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RuntimeExpressionTyping {
    pub(crate) environment: AstShared<StreamTypeEnvironment>,
    pub(crate) expected: TCType,
}

/// How text supplied to one `dynamic` or `defer` node is accepted.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct RuntimeExpressionSite {
    context: AstShared<SourceContext>,
    /// The defs the node's file could call, which its text may call too.
    callable: AstShared<Callable>,
    /// `None` only where no elaborated node stands behind the text: an
    /// expression built directly by a test, or one nested inside text that is
    /// itself evaluated without types. Such text is checked with every
    /// variable it mentions, and its result, of type `Any`.
    typing: Option<RuntimeExpressionTyping>,
}

/// Sparse, arena-bound environments for `dynamic` and `defer` occurrences.
pub(crate) type RuntimeExpressionSites =
    contiguous_tree::SparseNodeAnnotations<ExprArena, RuntimeExpressionSite>;

#[derive(Clone)]
pub(crate) struct UntypedExpr {
    expr: Expr,
    sites: AstShared<RuntimeExpressionSites>,
}

impl UntypedExpr {
    pub(crate) fn new(expr: Expr, sites: AstShared<RuntimeExpressionSites>) -> Self {
        Self { expr, sites }
    }

    pub(crate) fn expr(&self) -> &Expr {
        &self.expr
    }

    pub(crate) fn site(&self, node: ExprRef<'_>) -> RuntimeExpressionSite {
        self.sites.site(node).untyped()
    }

    pub(crate) fn sites(&self) -> &AstShared<RuntimeExpressionSites> {
        &self.sites
    }
}

/// Why a runtime expression was refused.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RuntimeExpressionError {
    #[error("runtime expression {text:?} does not parse: {error}")]
    Parse {
        text: String,
        #[source]
        error: DsrvParseError,
    },
    #[error("runtime expression {text:?} failed type checking: {errors:?}")]
    TypeCheck {
        text: String,
        errors: SemanticErrors,
    },
}

impl RuntimeExpressionSite {
    /// Environment for source supplied to a node with `context` that may call `callable`,
    /// checked against `typing`.
    pub(crate) fn new(
        context: AstShared<SourceContext>,
        callable: AstShared<Callable>,
        typing: Option<RuntimeExpressionTyping>,
    ) -> Self {
        Self {
            context,
            callable,
            typing,
        }
    }

    pub(crate) fn unlocated(root: ExprRef<'_>) -> Self {
        Self::new(
            root.metadata().context.clone().unwrap_or_default(),
            root.metadata().callable.clone().unwrap_or_default(),
            None,
        )
    }

    pub(crate) fn untyped(&self) -> Self {
        Self::new(
            AstShared::clone(&self.context),
            AstShared::clone(&self.callable),
            None,
        )
    }

    pub(crate) fn context(&self) -> &SourceContext {
        &self.context
    }

    pub(crate) fn callable(&self) -> &Callable {
        &self.callable
    }

    pub(crate) fn typing(&self) -> Option<&RuntimeExpressionTyping> {
        self.typing.as_ref()
    }

    pub(crate) fn same_lexical_environment(&self, other: &Self) -> bool {
        self.context == other.context && self.callable == other.callable
    }

    pub(crate) fn parse(&self, text: &str) -> Result<Expr, RuntimeExpressionError> {
        parse_expr_with_functions(
            text,
            AstShared::clone(&self.context),
            AstShared::clone(&self.callable),
        )
        .map_err(|error| RuntimeExpressionError::Parse {
            text: text.to_owned(),
            error,
        })
    }

    pub(crate) fn parse_unchecked(
        &self,
        text: &str,
    ) -> Result<UntypedExpr, RuntimeExpressionError> {
        let expr = self.parse(text)?;
        let sites = prepare_site_table(
            expr.sparse_annotations_builder(),
            expr.as_ref().postorder(),
            |_| None,
        );
        Ok(UntypedExpr::new(expr, AstShared::new(sites)))
    }

    pub(crate) fn check(
        &self,
        text: &str,
        expr: Expr,
    ) -> Result<CheckedExpr, RuntimeExpressionError> {
        let checked = match &self.typing {
            Some(typing) => check_expression(expr, &typing.expected, &typing.environment),
            None => {
                let environment = expr
                    .as_ref()
                    .free_variables()
                    .into_iter()
                    .map(|name| (name, StreamType::Any))
                    .collect::<BTreeMap<_, _>>();
                check_expression(expr, &TCType::Any, &AstShared::new(environment))
            }
        };
        // Runtime text has nowhere to present warnings.
        checked
            .discard_warnings()
            .map_err(|errors| RuntimeExpressionError::TypeCheck {
                text: text.to_owned(),
                errors,
            })
    }

    /// Parse and check `text`, giving the prepared expression a runtime evaluates.
    pub(crate) fn parse_and_check(
        &self,
        text: &str,
    ) -> Result<CheckedExpr, RuntimeExpressionError> {
        let expr = self.parse(text)?;
        self.check(text, expr).map(CheckedExpr::prepare_sites)
    }
}

pub(crate) fn prepare_site_table<'arena>(
    mut builder: contiguous_tree::SparseNodeAnnotationsBuilder<ExprArena, RuntimeExpressionSite>,
    nodes: impl IntoIterator<Item = ExprRef<'arena>>,
    typing: impl Fn(ExprRef<'arena>) -> Option<RuntimeExpressionTyping>,
) -> RuntimeExpressionSites {
    for node in nodes {
        if is_runtime_expression(node) {
            builder
                .insert(
                    node,
                    RuntimeExpressionSite::new(
                        node.metadata().context.clone().unwrap_or_default(),
                        node.metadata().callable.clone().unwrap_or_default(),
                        typing(node),
                    ),
                )
                .expect("runtime-expression node belongs to its expression storage");
        }
    }
    builder.finish()
}

pub(crate) fn is_runtime_expression(node: ExprRef<'_>) -> bool {
    matches!(node.kind(), ExprKind::Dynamic(..) | ExprKind::Defer(..))
}

#[cfg(test)]
mod tests;
