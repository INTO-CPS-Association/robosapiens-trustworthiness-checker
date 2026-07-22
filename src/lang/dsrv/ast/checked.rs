//! Type-checked expression handles and cursors.

use std::rc::Rc;

use contiguous_tree::{ContextCursor, TreeCursorExt};

use super::{Expr, ExprArena, ExprId, ExprKind, ExprRef, ExprView};
use crate::lang::dsrv::span::Span;
use crate::lang::dsrv::type_checker::{StreamTypeEnvironment, TCType};

pub(crate) type ExprTypes = contiguous_tree::NodeAnnotations<ExprArena, TCType>;
pub(crate) type ExprTypesBuilder = contiguous_tree::NodeAnnotationsBuilder<ExprArena, TCType>;

/// Immutable checked types shared by every checked expression handle.
#[derive(Clone, Debug)]
pub(crate) struct CheckedTypes {
    expr_types: ExprTypes,
    environment: Rc<StreamTypeEnvironment>,
}

impl CheckedTypes {
    pub(crate) fn new(expr_types: ExprTypes, environment: Rc<StreamTypeEnvironment>) -> Self {
        Self {
            expr_types,
            environment,
        }
    }

    pub(crate) fn type_of(&self, expr: ExprRef<'_>) -> &TCType {
        self.expr_types
            .get(expr)
            .expect("checked expression belongs to the typed tree or forest")
    }

    pub(crate) fn shared_type_environment(&self) -> &Rc<StreamTypeEnvironment> {
        &self.environment
    }
}

/// An expression whose complete syntax tree has been type checked.
#[derive(Clone)]
pub struct CheckedExpr {
    pub(super) expr: Expr,
    checked: Rc<CheckedTypes>,
}

/// A borrowed syntax cursor paired with its checked type.
#[derive(Clone, Copy, contiguous_tree::TreeCursor)]
#[tree_cursor(delegate = cursor, target = ExprRef<'arena>)]
pub struct CheckedExprRef<'arena> {
    cursor: ContextCursor<ExprRef<'arena>, &'arena CheckedTypes>,
}

/// Internal cursor used by consumers that accept checked or unchecked syntax.
#[derive(Clone, Copy)]
enum CheckContext<'arena> {
    Unchecked,
    Checked(&'arena CheckedTypes),
}

#[derive(Clone, Copy, contiguous_tree::TreeCursor)]
#[tree_cursor(delegate = cursor, target = ExprRef<'arena>)]
pub(crate) struct ExprCursor<'arena> {
    cursor: ContextCursor<ExprRef<'arena>, CheckContext<'arena>>,
}

impl CheckedExpr {
    pub(crate) fn new(
        expr: Expr,
        expr_types: ExprTypes,
        environment: Rc<StreamTypeEnvironment>,
    ) -> Self {
        let checked = Rc::new(CheckedTypes::new(expr_types, environment));
        Self::from_checked_types(expr, checked)
    }

    pub(super) fn from_checked_types(expr: Expr, checked: Rc<CheckedTypes>) -> Self {
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
}

impl PartialEq for CheckedExpr {
    fn eq(&self, other: &Self) -> bool {
        if Rc::ptr_eq(&self.checked, &other.checked) && self.expr.same_root(&other.expr) {
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
    pub(super) fn new(expr: ExprRef<'arena>, checked: &'arena CheckedTypes) -> Self {
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

    pub fn span(self) -> Span {
        self.expr().span()
    }

    pub fn id(self) -> ExprId {
        self.expr().id()
    }

    pub fn typ(self) -> &'arena TCType {
        self.cursor.context().type_of(self.expr())
    }

    pub fn type_environment(self) -> &'arena StreamTypeEnvironment {
        self.shared_type_environment().as_ref()
    }

    pub(crate) fn shared_type_environment(self) -> &'arena Rc<StreamTypeEnvironment> {
        self.cursor.context().shared_type_environment()
    }

    pub(crate) fn erased(self) -> ExprCursor<'arena> {
        ExprCursor::checked(self.expr(), self.cursor.context())
    }
}

impl<'arena> ExprCursor<'arena> {
    pub(crate) fn unchecked(expr: ExprRef<'arena>) -> Self {
        Self {
            cursor: ContextCursor::new(expr, CheckContext::Unchecked),
        }
    }

    fn checked(expr: ExprRef<'arena>, checked: &'arena CheckedTypes) -> Self {
        Self {
            cursor: ContextCursor::new(expr, CheckContext::Checked(checked)),
        }
    }

    pub(crate) fn expr(self) -> ExprRef<'arena> {
        self.cursor.cursor()
    }

    pub(crate) fn view(self) -> ExprView<'arena, Self> {
        self.expr().view_with(self)
    }

    pub(crate) fn typ(self) -> Option<&'arena TCType> {
        match self.cursor.context() {
            CheckContext::Unchecked => None,
            CheckContext::Checked(checked) => Some(checked.type_of(self.expr())),
        }
    }

    pub(crate) fn shared_type_environment(self) -> Option<&'arena Rc<StreamTypeEnvironment>> {
        match self.cursor.context() {
            CheckContext::Unchecked => None,
            CheckContext::Checked(checked) => Some(checked.shared_type_environment()),
        }
    }
}
