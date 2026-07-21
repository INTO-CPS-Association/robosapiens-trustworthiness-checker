//! Type-checked expression handles and cursors.

use std::rc::Rc;

use contiguous_tree::{ContextCursor, TreeCursorExt};

use super::{Expr, ExprId, ExprKind, ExprRef, ExprView};
use crate::lang::dsrv::span::Span;
use crate::lang::dsrv::type_checker::{TCType, TypeInfo};

/// Immutable type-checking results shared by every checked expression handle.
#[derive(Clone, Debug)]
pub(crate) struct TypeAnnotations {
    node_types: contiguous_tree::NodeAnnotations<ExprId, TCType>,
    variable_types: Rc<TypeInfo>,
}

impl TypeAnnotations {
    pub(crate) fn new(types: Vec<TCType>, variable_types: Rc<TypeInfo>) -> Self {
        Self {
            node_types: contiguous_tree::NodeAnnotations::new(types),
            variable_types,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.node_types.len()
    }

    pub(crate) fn type_of(&self, id: ExprId) -> &TCType {
        self.node_types.get(id)
    }

    pub(crate) fn shared_type_info(&self) -> &Rc<TypeInfo> {
        &self.variable_types
    }
}

/// An expression whose complete syntax tree has been type checked.
#[derive(Clone)]
pub struct CheckedExpr {
    pub(super) expr: Expr,
    checked: Rc<TypeAnnotations>,
}

/// A borrowed syntax cursor paired with guaranteed type annotations.
#[derive(Clone, Copy, contiguous_tree::TreeCursor)]
#[tree_cursor(delegate = cursor, target = ExprRef<'arena>)]
pub struct CheckedExprRef<'arena> {
    cursor: ContextCursor<ExprRef<'arena>, &'arena TypeAnnotations>,
}

impl CheckedExpr {
    pub(crate) fn new(expr: Expr, types: Vec<TCType>, type_info: Rc<TypeInfo>) -> Self {
        assert_eq!(expr.arena().len(), types.len());
        Self::from_annotations(expr, Rc::new(TypeAnnotations::new(types, type_info)))
    }

    pub(super) fn from_annotations(expr: Expr, checked: Rc<TypeAnnotations>) -> Self {
        Self { expr, checked }
    }

    pub fn expr(&self) -> &Expr {
        &self.expr
    }

    pub fn typ(&self) -> &TCType {
        self.checked.type_of(self.expr.id())
    }

    pub fn as_ref(&self) -> CheckedExprRef<'_> {
        self.expr.as_ref().with_annotations(&self.checked)
    }

    pub(crate) fn into_parts(self) -> (Expr, Rc<TypeAnnotations>) {
        (self.expr, self.checked)
    }
}

impl PartialEq for CheckedExpr {
    fn eq(&self, other: &Self) -> bool {
        if Rc::ptr_eq(&self.checked, &other.checked) && self.expr.same_root(&other.expr) {
            return true;
        }
        if self.checked.variable_types != other.checked.variable_types {
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
    pub(super) fn new(expr: ExprRef<'arena>, checked: &'arena TypeAnnotations) -> Self {
        debug_assert_eq!(checked.len(), expr.arena().len());
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

    pub fn kind(self) -> &'arena ExprKind {
        self.expr().kind()
    }

    pub fn span(self) -> Span {
        self.expr().span()
    }

    pub fn id(self) -> ExprId {
        self.expr().id()
    }

    pub fn typ(self) -> &'arena TCType {
        self.cursor.context().type_of(self.id())
    }

    pub fn type_info(self) -> &'arena TypeInfo {
        self.cursor.context().variable_types.as_ref()
    }
}
