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
//!
//! A site prepared from a program captures the files its text may need to
//! be located: the site's own, and every file a def the text may call was
//! written in. Text is archived beside that capture while it is parsed and
//! checked, so its errors own their files, and sites nested in it capture
//! it in turn. The capture is not part of what a site means: two sites that
//! differ only in where they were written accept text identically.

use std::collections::{BTreeMap, BTreeSet};

use contiguous_tree::TreeCursorExt;

use crate::core::StreamType;
use crate::lang::dsrv::ast::{AstShared, CheckedExpr, Expr, ExprArena, ExprKind, ExprRef};
use crate::lang::dsrv::diagnostics::SemanticErrors;
use crate::lang::dsrv::expand::functions::Callable;
use crate::lang::dsrv::parser::{DsrvParseError, parse_expr_with_functions};
use crate::lang::dsrv::source::SourceContext;
use crate::lang::dsrv::source_map::{
    NodeOrigin, RUNTIME_EXPRESSION_LABEL, SourceArchive, SourceFile, SourceId, SourceLabel,
};
use crate::lang::dsrv::type_checker::{StreamTypeEnvironment, TCType, check_expression};

/// The type and environment runtime text is checked against: what elaboration
/// gave the `dynamic` or `defer` node the text was supplied to.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RuntimeExpressionTyping {
    pub(crate) environment: AstShared<StreamTypeEnvironment>,
    pub(crate) expected: TCType,
}

/// How text supplied to one `dynamic` or `defer` node is accepted.
#[derive(Clone, Debug, Default)]
pub(crate) struct RuntimeExpressionSite {
    context: AstShared<SourceContext>,
    /// The defs the node's file could call, which its text may call too.
    callable: AstShared<Callable>,
    /// `None` only where no elaborated node stands behind the text: an
    /// expression built directly by a test, or one nested inside text that is
    /// itself evaluated without types. Such text is checked with every
    /// variable it mentions, and its result, of type `Any`.
    typing: Option<RuntimeExpressionTyping>,
    /// The files text supplied here may need to be located. `None` where
    /// no archive stood behind the site, whose text is then unlocated.
    sources: Option<AstShared<SourceArchive>>,
}

/// Where a site was written is presentation: it is not compared.
impl PartialEq for RuntimeExpressionSite {
    fn eq(&self, other: &Self) -> bool {
        self.context == other.context
            && self.callable == other.callable
            && self.typing == other.typing
    }
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
            sources: None,
        }
    }

    /// This site, with the files its text may need.
    pub(crate) fn with_sources(mut self, sources: AstShared<SourceArchive>) -> Self {
        self.sources = Some(sources);
        self
    }

    /// This site without its captured files, as a cache that outlives the
    /// occurrence keeps it: a reused template must not locate later text at
    /// the occurrence that first compiled it.
    pub(crate) fn without_sources(&self) -> Self {
        Self {
            sources: None,
            ..self.clone()
        }
    }

    /// The files this site captured.
    #[cfg(test)]
    pub(crate) fn sources(&self) -> Option<&AstShared<SourceArchive>> {
        self.sources.as_ref()
    }

    pub(crate) fn unlocated(root: ExprRef<'_>) -> Self {
        Self::new(
            root.metadata().context.clone().unwrap_or_default(),
            root.metadata().callable.clone().unwrap_or_default(),
            None,
        )
    }

    pub(crate) fn untyped(&self) -> Self {
        Self {
            typing: None,
            ..self.clone()
        }
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

    /// Parse `text` without locating it, for inspecting what it expands to.
    #[cfg(test)]
    pub(crate) fn parse(&self, text: &str) -> Result<Expr, RuntimeExpressionError> {
        Ok(self.parse_located(text)?.0)
    }

    /// Parse `text`, archived beside this site's captured files. The
    /// expression's IDs are the returned archive's, which is `None` for an
    /// unlocated site.
    fn parse_located(
        &self,
        text: &str,
    ) -> Result<(Expr, Option<SourceArchive>), RuntimeExpressionError> {
        let located = self.sources.as_ref().map(|capture| {
            let (supplied, local) = SourceArchive::single(SourceFile::new(
                SourceLabel::Supplied(RUNTIME_EXPRESSION_LABEL.into()),
                Vec::new(),
                text,
            ));
            let (archive, remap) = capture.merge(&supplied);
            let source = remap.id(local).expect("a merge keeps every file it adds");
            (archive, source)
        });
        let expr = parse_expr_with_functions(
            text,
            AstShared::clone(&self.context),
            AstShared::clone(&self.callable),
            located.as_ref().map(|(archive, source)| (archive, *source)),
        )
        .map_err(|error| RuntimeExpressionError::Parse {
            text: text.to_owned(),
            error,
        })?;
        Ok((expr, located.map(|(archive, _)| archive)))
    }

    pub(crate) fn parse_unchecked(
        &self,
        text: &str,
    ) -> Result<UntypedExpr, RuntimeExpressionError> {
        let (expr, sources) = self.parse_located(text)?;
        let sites = prepare_site_table(
            expr.sparse_annotations_builder(),
            expr.as_ref().postorder(),
            sources.as_ref(),
            |_| None,
        );
        Ok(UntypedExpr::new(expr, AstShared::new(sites)))
    }

    fn check(
        &self,
        text: &str,
        expr: Expr,
        sources: Option<&SourceArchive>,
    ) -> Result<CheckedExpr, RuntimeExpressionError> {
        let checked = match &self.typing {
            Some(typing) => check_expression(expr, &typing.expected, &typing.environment, sources),
            None => {
                let environment = expr
                    .as_ref()
                    .free_variables()
                    .into_iter()
                    .map(|name| (name, StreamType::Any))
                    .collect::<BTreeMap<_, _>>();
                check_expression(expr, &TCType::Any, &AstShared::new(environment), sources)
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
        let (expr, sources) = self.parse_located(text)?;
        self.check(text, expr, sources.as_ref())
            .map(|checked| checked.prepare_sites_with(sources.as_ref()))
    }
}

/// Prepare a site for every runtime-expression occurrence among `nodes`.
///
/// Each site of a located tree captures, from `sources`, the files its
/// text may need; sites that would capture the same files share one
/// capture.
pub(crate) fn prepare_site_table<'arena>(
    mut builder: contiguous_tree::SparseNodeAnnotationsBuilder<ExprArena, RuntimeExpressionSite>,
    nodes: impl IntoIterator<Item = ExprRef<'arena>>,
    sources: Option<&SourceArchive>,
    typing: impl Fn(ExprRef<'arena>) -> Option<RuntimeExpressionTyping>,
) -> RuntimeExpressionSites {
    let mut captures = SourceCaptures::default();
    for node in nodes {
        if is_runtime_expression(node) {
            let context = node.metadata().context.clone().unwrap_or_default();
            let callable = node.metadata().callable.clone().unwrap_or_default();
            let mut site = RuntimeExpressionSite::new(context, callable, typing(node));
            if let Some(sources) = sources {
                let capture = captures.capture(sources, node.origin(), &site.callable);
                site = site.with_sources(capture);
            }
            builder
                .insert(node, site)
                .expect("runtime-expression node belongs to its expression storage");
        }
    }
    builder.finish()
}

/// The captures made while preparing one table, by the files they hold.
#[derive(Default)]
struct SourceCaptures {
    /// What each distinct callable's defs were written in.
    closures: Vec<(AstShared<Callable>, BTreeSet<SourceId>)>,
    made: BTreeMap<BTreeSet<SourceId>, AstShared<SourceArchive>>,
}

impl SourceCaptures {
    fn capture(
        &mut self,
        sources: &SourceArchive,
        origin: NodeOrigin,
        callable: &AstShared<Callable>,
    ) -> AstShared<SourceArchive> {
        let closure = match self
            .closures
            .iter()
            .find(|(seen, _)| AstShared::ptr_eq(seen, callable))
        {
            Some((_, closure)) => closure.clone(),
            None => {
                let closure = match callable.archive() {
                    Some(token) => {
                        assert!(
                            sources.resolves(token),
                            "a site's defs belong to the archive it is prepared against"
                        );
                        callable.source_closure()
                    }
                    None => BTreeSet::new(),
                };
                self.closures
                    .push((AstShared::clone(callable), closure.clone()));
                closure
            }
        };
        let mut needed = closure;
        needed.extend(origin.source);
        needed.extend(origin.definition.map(|site| site.source));
        AstShared::clone(
            self.made.entry(needed).or_insert_with_key(|needed| {
                AstShared::new(sources.capture(needed.iter().copied()))
            }),
        )
    }
}

pub(crate) fn is_runtime_expression(node: ExprRef<'_>) -> bool {
    matches!(node.kind(), ExprKind::Dynamic(..) | ExprKind::Defer(..))
}

#[cfg(test)]
mod tests;
