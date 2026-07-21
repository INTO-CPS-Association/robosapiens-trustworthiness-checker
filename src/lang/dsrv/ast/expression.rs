//! DSRV expression representation.
//!
//! The schema below is the authoritative inventory of expression forms. Most
//! code should use [`Expr`] for an owning root, [`ExprRef`] for traversal,
//! [`ExprView`] to match nodes with resolved child cursors, and [`ExprBuilder`]
//! for parsing or structural transformation. [`ExprKind`] and [`ExprId`] are
//! the corresponding stored node data and typed IDs.
//!
//! Type checking does not construct another AST. [`CheckedExprRef`] traverses
//! the same expression tree while carrying immutable [`TypeAnnotations`].

use std::fmt::{Debug, Display};

use contiguous_tree::TreeCursorExt;
use ecow::{EcoString, EcoVec};

use super::{CheckedExprRef, ExprDisplay, SBinOp, TypeAnnotations};
use crate::core::{StreamType, Value};
use crate::core::{StreamTypeAscription, VarName};
use crate::distributed::distribution_graphs::NodeName;
use crate::lang::dsrv::span::Span;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum DynamicExprScope {
    Automatic,
    Explicit(EcoVec<VarName>),
}

// This generates Expr, ExprKind, ExprId, ExprRef, ExprView, ExprBuilder, and
// keyed-field support from a shared schema of DSRV expression forms.
contiguous_tree::tree_schema! {
    pub tree Expr {
        internals: pub(crate),
        owned_constructors: pub(crate),
        metadata: span: Span = Span::default(),
        id: u32,
        key: EcoString,
        children: EcoVec,
        keyed_children: EcoVec,

        If(condition: child, then_expr: child, else_expr: child),
        SIndex(input: child, offset: copy(u64)),
        Val(value: into_data(Value)),
        BinOp(left: child, right: child, operator: data(SBinOp)),
        Var(variable: data(VarName)),

        Dynamic(
            source: child,
            result_type: data(StreamTypeAscription),
            scope: data(DynamicExprScope) = DynamicExprScope::Automatic,
        ),
        Defer(
            source: child,
            result_type: data(StreamTypeAscription),
            scope: into_data(DynamicExprScope),
        ),
        Update(value: child, update: child),
        Default(value: child, default: child),
        IsDefined(value: child),
        When(value: child),
        Latch(value: child, trigger: child),
        Init(value: child, initial: child),
        Not(value: child),
        Neg(value: child),

        Lambda(parameters: data(EcoVec<(VarName, StreamType)>), body: child),
        Apply(function: child, arguments: children),
        Fix(function: child),
        Partial(function: child, arguments: children),

        List(items: children),
        Tuple(items: children),
        LIndex(list: child, index: child),
        LAppend(list: child, value: child),
        LConcat(left: child, right: child),
        LHead(list: child),
        LTail(list: child),
        LLen(list: child),
        LMap(function: child, list: child),
        LFilter(function: child, list: child),
        LFold(function: child, initial: child, list: child),

        Map(entries: keyed_children),
        Struct(entries: keyed_children),
        ObjectLiteral(entries: keyed_children),
        MGet(map: child, key: data(EcoString)),
        SGet(value: child, key: data(EcoString)),
        MInsert(map: child, key: data(EcoString), value: child),
        MRemove(map: child, key: data(EcoString)),
        MHasKey(map: child, key: data(EcoString)),

        Sin(value: child),
        Cos(value: child),
        Tan(value: child),
        Abs(value: child),

        MonitoredAt(variable: data(VarName), node: data(NodeName)),
        Dist(left: data(VarOrNodeName), right: data(VarOrNodeName)),
    }
}

impl ExprBuilder {
    pub(crate) fn for_source(source: &str) -> Self {
        Self::with_capacity(source.len() / 4)
    }

    pub fn finish(self, root: ExprId) -> Expr {
        Expr::new(ExprHandle::new(self.arena, root))
    }
}

// Programmatic construction helpers that are not schema variant constructors.
#[allow(non_snake_case)]
impl Expr {
    pub(crate) fn with_span(node: ExprKind, span: Span) -> Self {
        let mut arena = ExprArena::with_capacity(1);
        let root = arena.alloc(node, span);
        Self::new(ExprHandle::new(arena, root))
    }

    #[cfg(test)]
    pub(crate) fn RestrictedDynamic<E: Into<Self>>(
        source: Box<E>,
        result_type: StreamTypeAscription,
        vars: EcoVec<VarName>,
    ) -> Self {
        Self::__merge_owned_children([(*source).into()], |ids| {
            ExprKind::Dynamic(ids[0], result_type, DynamicExprScope::Explicit(vars))
        })
    }

    #[cfg(test)]
    pub(crate) fn MapOrdered(items: impl IntoIterator<Item = (EcoString, Self)>) -> Self {
        Self::Map(items)
    }
}

#[derive(Clone, PartialEq, Debug, serde::Serialize)]
pub struct VarOrNodeName(pub String);

impl Display for VarOrNodeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<VarOrNodeName> for VarName {
    fn from(value: VarOrNodeName) -> Self {
        value.0.into()
    }
}

impl From<VarOrNodeName> for NodeName {
    fn from(value: VarOrNodeName) -> Self {
        value.0.into()
    }
}

impl From<VarOrNodeName> for String {
    fn from(value: VarOrNodeName) -> Self {
        value.0
    }
}

impl DynamicExprScope {
    pub fn explicit(vars: EcoVec<VarName>) -> Self {
        Self::Explicit(vars)
    }

    pub fn explicit_vars(&self) -> Option<&EcoVec<VarName>> {
        match self {
            Self::Automatic => None,
            Self::Explicit(vars) => Some(vars),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &VarName> {
        self.explicit_vars().into_iter().flatten()
    }
}

impl From<EcoVec<VarName>> for DynamicExprScope {
    fn from(vars: EcoVec<VarName>) -> Self {
        if vars.is_empty() {
            Self::Automatic
        } else {
            Self::Explicit(vars)
        }
    }
}

impl Expr {
    pub fn span(&self) -> Span {
        self.as_ref().span()
    }
    pub(crate) fn checked_ref<'a>(&'a self, checked: &'a TypeAnnotations) -> CheckedExprRef<'a> {
        self.as_ref().with_annotations(checked)
    }
    pub fn display(&self) -> ExprDisplay<'_> {
        ExprDisplay {
            arena: self.arena(),
            id: self.id(),
        }
    }
}

impl<'arena> ExprRef<'arena> {
    pub(super) fn with_annotations(
        self,
        checked: &'arena TypeAnnotations,
    ) -> CheckedExprRef<'arena> {
        debug_assert_eq!(checked.len(), self.arena().len());
        CheckedExprRef::new(self, checked)
    }

    pub fn span(self) -> Span {
        self.node().span
    }

    pub fn display(self) -> ExprDisplay<'arena> {
        ExprDisplay {
            arena: self.arena(),
            id: self.id(),
        }
    }
}

impl PartialEq for Expr {
    fn eq(&self, other: &Self) -> bool {
        self.same_root(other)
            || self
                .as_ref()
                .try_zip_with::<std::convert::Infallible, _>(other.as_ref(), |left, right| {
                    let left = left.node();
                    let right = right.node();
                    Ok(left.span == right.span && left.node.same_payload(&right.node))
                })
                .unwrap_or_else(|never| match never {})
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use contiguous_tree::TreeCursorExt;

    use crate::lang::dsrv::ast::{Expr, ExprBuilder, ExprKind, ExprView, NumericalBinOp, SBinOp};

    fn tree(right: i64) -> Expr {
        Expr::If(
            Box::new(Expr::Var("condition".into())),
            Box::new(Expr::BinOp(
                Box::new(Expr::Val(1)),
                Box::new(Expr::Val(2)),
                SBinOp::NOp(NumericalBinOp::Add),
            )),
            Box::new(Expr::Val(right)),
        )
    }

    #[test]
    fn generated_traversal_and_zip_preserve_source_order() {
        let left = tree(3);
        let right = tree(4);
        let mut visited = Vec::new();

        let matched = left
            .as_ref()
            .try_zip_with::<Infallible, _>(right.as_ref(), |left, right| {
                visited.push((left.id(), right.id()));
                Ok(std::mem::discriminant(&left.view()) == std::mem::discriminant(&right.view()))
            })
            .unwrap_or_else(|never| match never {});

        assert!(matched);
        assert_eq!(visited.len(), left.as_ref().postorder().len());
        assert_eq!(visited.first().unwrap(), &(left.id(), right.id()));
    }

    #[test]
    fn generated_zip_stops_on_shape_mismatch_and_propagates_errors() {
        let tree = tree(3);
        let leaf = Expr::Val(3);
        assert!(
            !tree
                .as_ref()
                .try_zip_with::<Infallible, _>(leaf.as_ref(), |_, _| Ok(true))
                .unwrap_or_else(|never| match never {})
        );

        assert_eq!(
            tree.as_ref()
                .try_zip_with(tree.as_ref(), |_, _| Err::<bool, _>("stopped")),
            Err("stopped")
        );
    }

    #[test]
    fn generated_cloning_preserves_the_selected_tree_and_spans() {
        let source = tree(3);
        let mut builder = ExprBuilder::with_capacity(0);
        let root = builder.clone_subtree(source.as_ref());
        let cloned = builder.finish(root);

        assert_eq!(cloned, source);
    }

    #[test]
    fn generated_fold_resolves_sequence_and_field_children() {
        let expression = Expr::ObjectLiteral(
            [
                ("condition".into(), Expr::Var("enabled".into())),
                (
                    "values".into(),
                    Expr::List(vec![Expr::Val(1), Expr::Val(2)].into()),
                ),
            ]
            .into_iter(),
        );

        let node_count = expression.as_ref().fold(|node| match node.cursor().kind() {
            ExprKind::ObjectLiteral(fields) => {
                1 + node
                    .fields(fields.raw_slice())
                    .map(|(_, size)| size)
                    .sum::<usize>()
            }
            _ => 1 + node.children().copied().sum::<usize>(),
        });

        assert_eq!(node_count, expression.as_ref().postorder().len());
    }

    #[test]
    fn generated_views_resolve_collections_and_keyed_children() {
        let object = Expr::ObjectLiteral([("answer".into(), Expr::Val(42))]);
        let ExprView::ObjectLiteral(fields) = object.as_ref().view() else {
            unreachable!()
        };
        assert!(matches!(
            fields.get("answer").unwrap().view(),
            ExprView::Val(crate::Value::Int(42))
        ));

        let list = Expr::List(vec![Expr::Val(1), Expr::Val(2)].into());
        let ExprView::List(items) = list.as_ref().view() else {
            unreachable!()
        };
        assert_eq!(items.len(), 2);
        assert!(
            items
                .into_iter()
                .all(|item| matches!(item.view(), ExprView::Val(_)))
        );
    }

    #[test]
    fn generated_views_resolve_fixed_children() {
        let expression = tree(3);
        let ExprView::If(condition, then_expr, else_expr) = expression.as_ref().view() else {
            unreachable!()
        };
        assert!(matches!(condition.view(), ExprView::Var(_)));
        assert!(matches!(then_expr.view(), ExprView::BinOp(..)));
        assert!(matches!(else_expr.view(), ExprView::Val(_)));
    }

    #[test]
    fn boxed_construction_clones_only_the_selected_subtree() {
        let container = Expr::Tuple(vec![Expr::Val(1), Expr::Val(2)].into());
        let ExprView::Tuple(mut items) = container.as_ref().view() else {
            unreachable!()
        };
        let selected = container.subtree(items.nth(1).unwrap());
        let wrapped = Expr::Not(Box::new(selected));

        assert_eq!(wrapped.as_ref().postorder().len(), 2);
        assert!(matches!(wrapped.as_ref().view(), ExprView::Not(_)));
    }
}
