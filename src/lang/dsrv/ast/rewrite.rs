//! DSRV adapters for rewriting keyed expression roots into compact shared storage.

use std::collections::BTreeMap;
use std::fmt;

use contiguous_tree::CloneTreeError;

use super::{ExprBuilder, ExprId, ExprRef};
use crate::lang::dsrv::span::Span;

/// A failure while rewriting keyed DSRV expression roots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RewriteForestError<PolicyError> {
    Policy(PolicyError),
    ReplacementCycle { replacement_spans: Vec<Span> },
}

impl<PolicyError: fmt::Display> fmt::Display for RewriteForestError<PolicyError> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Policy(error) => error.fmt(formatter),
            Self::ReplacementCycle { replacement_spans } => write!(
                formatter,
                "cyclic replacement expansion through spans {replacement_spans:?}"
            ),
        }
    }
}

impl<PolicyError: std::error::Error + 'static> std::error::Error
    for RewriteForestError<PolicyError>
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Policy(error) => Some(error),
            Self::ReplacementCycle { .. } => None,
        }
    }
}

/// Compact shared storage and keyed root IDs produced by expression rewriting.
#[derive(Debug)]
pub(crate) struct CompactExprForest<Key> {
    arena: super::ExprArena,
    roots: BTreeMap<Key, ExprId>,
}

impl<Key: Ord> CompactExprForest<Key> {
    pub(crate) fn into_arena_and_roots(self) -> (super::ExprArena, BTreeMap<Key, ExprId>) {
        (self.arena, self.roots)
    }

    #[cfg(test)]
    fn into_expressions(self) -> BTreeMap<Key, super::Expr> {
        let (arena, roots) = self.into_arena_and_roots();
        let entries = roots.into_iter().collect::<Vec<_>>();
        let expressions = super::Expr::forest(arena, entries.iter().map(|(_, root)| *root));
        entries
            .into_iter()
            .zip(expressions)
            .map(|((key, _), expression)| (key, expression))
            .collect()
    }
}

/// Rewrite keyed expression roots into one compact shared arena.
///
/// Replacement subtrees are themselves passed through `replace`, allowing transitive expansion.
/// Keys remain in `BTreeMap` order and all roots share storage containing only reachable nodes.
pub(crate) fn rewrite_forest<'arena, Key, Policy, PolicyError>(
    roots: BTreeMap<Key, ExprRef<'arena>>,
    mut replace: Policy,
) -> Result<CompactExprForest<Key>, RewriteForestError<PolicyError>>
where
    Key: Ord,
    Policy: FnMut(ExprRef<'arena>) -> Result<Option<ExprRef<'arena>>, PolicyError>,
{
    let mut target = ExprBuilder::with_capacity(0);
    let mut rewritten_roots = BTreeMap::new();
    for (key, root) in roots {
        let rewritten = target
            .try_clone_subtree_with(root, &mut replace)
            .map_err(|error| match error {
                CloneTreeError::Policy(error) => RewriteForestError::Policy(error),
                CloneTreeError::ReplacementCycle { cursors } => {
                    RewriteForestError::ReplacementCycle {
                        replacement_spans: cursors
                            .into_iter()
                            .map(|cursor| cursor.span())
                            .collect(),
                    }
                }
            })?;
        rewritten_roots.insert(key, rewritten);
    }

    Ok(CompactExprForest {
        arena: target.finish_arena(),
        roots: rewritten_roots,
    })
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use super::*;
    use crate::VarName;
    use crate::lang::dsrv::ast::ExprView;
    use crate::sexpr;
    use contiguous_tree::TreeCursorExt;

    #[test]
    fn rewrites_multiple_roots_into_shared_compact_storage() {
        let first = sexpr!(BinOp(Var("replace"), Add, Val(2)));
        let second = sexpr!(Not(Var("keep")));
        let replacement = sexpr!(BinOp(Val(3), Mul, Val(4)));
        let roots = BTreeMap::from([("first", first), ("second", second)]);
        let replace: VarName = "replace".into();

        let rewritten = rewrite_forest(
            roots
                .iter()
                .map(|(name, root)| (*name, root.as_ref()))
                .collect(),
            |expression| {
                Ok::<_, Infallible>(match expression.view() {
                    ExprView::Var(var) if var == &replace => Some(replacement.as_ref()),
                    _ => None,
                })
            },
        )
        .unwrap()
        .into_expressions();

        assert_eq!(
            rewritten["first"],
            sexpr!(BinOp(BinOp(Val(3), Mul, Val(4)), Add, Val(2)))
        );
        assert_eq!(rewritten["second"], sexpr!(Not(Var("keep"))));
        assert!(rewritten["first"].shares_storage_with(&rewritten["second"]));
        assert_eq!(
            rewritten["first"].arena().len(),
            rewritten
                .values()
                .map(|root| root.as_ref().postorder().len())
                .sum::<usize>()
        );
    }

    #[test]
    fn recursively_rewrites_replacements_and_reports_cycles() {
        let a = sexpr!(Var("b"));
        let b = sexpr!(Var("a"));
        let root = sexpr!(Var("a"));
        let a_name: VarName = "a".into();
        let b_name: VarName = "b".into();

        let error = rewrite_forest(BTreeMap::from([("root", root.as_ref())]), |expression| {
            Ok::<_, Infallible>(match expression.view() {
                ExprView::Var(var) if var == &a_name => Some(a.as_ref()),
                ExprView::Var(var) if var == &b_name => Some(b.as_ref()),
                _ => None,
            })
        })
        .unwrap_err();

        match error {
            RewriteForestError::ReplacementCycle { replacement_spans } => {
                assert_eq!(replacement_spans.len(), 3);
            }
            RewriteForestError::Policy(never) => match never {},
        }
    }

    #[test]
    fn policy_errors_stop_before_later_siblings() {
        let root = sexpr!(BinOp(Var("stop"), Add, Var("later")));
        let stop: VarName = "stop".into();
        let later: VarName = "later".into();
        let mut visited_later = false;

        let error =
            rewrite_forest(
                BTreeMap::from([("root", root.as_ref())]),
                |expression| match expression.view() {
                    ExprView::Var(var) if var == &stop => Err("stopped"),
                    ExprView::Var(var) if var == &later => {
                        visited_later = true;
                        Ok(None)
                    }
                    _ => Ok(None),
                },
            )
            .unwrap_err();

        assert_eq!(error, RewriteForestError::Policy("stopped"));
        assert!(!visited_later);
    }
}
