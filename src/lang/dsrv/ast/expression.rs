//! DSRV expression representation.
//!
//! The schema below is the authoritative inventory of expression forms. Most
//! code should use [`Expr`] for an owning root, [`ExprRef`] for traversal,
//! [`ExprView`] to match nodes with resolved child cursors, and [`ExprBuilder`]
//! for parsing or structural transformation. [`ExprKind`] and [`ExprId`] are
//! the corresponding stored node data and typed IDs.
//!
//! Type checking does not construct another AST. [`CheckedExprRef`] traverses
//! the same expression tree while carrying immutable [`CheckedTypes`].

use std::fmt::{Debug, Display};

use contiguous_tree::TreeCursorExt;
use ecow::{EcoString, EcoVec};

use super::CheckedExprRef;
use super::checked::CheckedTypes;
use crate::core::{BinaryOperator, StreamType, Value};
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
        schema: pub(crate),
        serialize: display,
        owned_constructors: pub,
        metadata: span: Span = Span::default(),
        id: u32,
        key: EcoString,
        children: EcoVec,
        keyed_children: EcoVec,

        If(condition: child, then_expr: child, else_expr: child),
        SIndex(input: child, offset: copy(u64)),
        Val(value: into_data(Value)),
        BinOp(left: child, right: child, operator: copy(BinaryOperator)),
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

impl<'arena> ExprRef<'arena> {
    pub(crate) fn duplicate_field(self) -> Option<&'arena EcoString> {
        self.postorder()
            .find_map(|expression| expression.kind().duplicate_key())
    }
}

impl Expr {
    pub(crate) fn value_with_span(value: Value, span: Span) -> Self {
        let mut builder = ExprBuilder::with_capacity(1);
        let root = builder.alloc(ExprKind::Val(value), span);
        builder
            .finish(root)
            .expect("a single allocated expression is a valid tree")
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

impl From<EcoVec<VarName>> for DynamicExprScope {
    fn from(vars: EcoVec<VarName>) -> Self {
        Self::Explicit(vars)
    }
}

impl<'arena> ExprRef<'arena> {
    pub(super) fn with_checked_types(
        self,
        checked: &'arena CheckedTypes,
    ) -> CheckedExprRef<'arena> {
        CheckedExprRef::new(self, checked)
    }

    pub fn span(self) -> Span {
        self.node().span
    }
}

impl PartialEq for Expr {
    fn eq(&self, other: &Self) -> bool {
        self.same_root(other) || self.as_ref() == other.as_ref()
    }
}

impl<'left, 'right> PartialEq<ExprRef<'right>> for ExprRef<'left> {
    fn eq(&self, other: &ExprRef<'right>) -> bool {
        self.try_zip_with::<std::convert::Infallible, _>(*other, |left, right| {
            Ok(left.span() == right.span() && left.kind().same_payload(right.kind()))
        })
        .unwrap_or_else(|never| match never {})
    }
}

#[cfg(test)]
mod tests {
    use ecow::EcoVec;

    use crate::core::BinaryOperator;
    use crate::lang::dsrv::ast::{DynamicExprScope, Expr};

    #[test]
    fn empty_scope_conversion_remains_explicit() {
        assert_eq!(
            DynamicExprScope::from(EcoVec::new()),
            DynamicExprScope::Explicit(EcoVec::new())
        );
    }

    fn expression(right: i64) -> Expr {
        Expr::If(
            Box::new(Expr::Var("condition".into())),
            Box::new(Expr::BinOp(
                Box::new(Expr::Val(1)),
                Box::new(Expr::Val(2)),
                BinaryOperator::Add,
            )),
            Box::new(Expr::Val(right)),
        )
    }

    #[test]
    fn borrowed_expressions_support_semantic_equality_and_serialization() {
        let left = expression(3);
        let equal = expression(3);
        let different = expression(4);

        assert_eq!(left.as_ref(), equal.as_ref());
        assert_ne!(left.as_ref(), different.as_ref());
        assert_eq!(
            serde_json::to_string(&left.as_ref()).unwrap(),
            serde_json::to_string(&left).unwrap()
        );
    }
}
