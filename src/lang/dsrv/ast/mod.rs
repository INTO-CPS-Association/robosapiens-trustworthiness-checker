//! DSRV syntax, specifications, and expression APIs.

#[cfg(feature = "thread-safe-ast")]
pub(crate) type AstShared<T> = std::sync::Arc<T>;
#[cfg(not(feature = "thread-safe-ast"))]
pub(crate) type AstShared<T> = std::rc::Rc<T>;
#[cfg(all(test, feature = "thread-safe-ast"))]
pub(crate) type AstWeak<T> = std::sync::Weak<T>;
#[cfg(all(test, not(feature = "thread-safe-ast")))]
pub(crate) type AstWeak<T> = std::rc::Weak<T>;

// Core nodes and expression handles.
mod expression;
// Type-checking decorates the shared expression representation.
mod checked;

// Specifications and their checked metadata.
mod requirements;
mod specification;

// Semantic queries over expressions.
mod analysis;

// Expression formatting.
mod display;

pub(crate) use analysis::DependencyKind;
pub use checked::{CheckedExpr, CheckedExprRef};
pub(crate) use checked::{ExprTypes, ExprTypesBuilder};

pub use expression::{
    Expr, ExprId, ExprRef, ExprView, ReconfigurableExprScope, SyntaxLiteral, SyntaxLiteralError,
    VarOrNodeName,
};
pub(crate) use expression::{
    ExprArena, ExprBuilder, ExprFieldRefs, ExprForest, ExprForestMap, ExprKind, ExprMetadata,
    ExprRefs, ExprRewriteNode,
};

pub(crate) use specification::UnvalidatedDsrvSpecification;
pub use specification::{
    CheckedDsrvSpecification, Declaration, DsrvAstError, DsrvSpecification,
    ValidatedDsrvSpecification,
};

#[cfg(feature = "thread-safe-ast")]
static_assertions::assert_impl_all!(DsrvSpecification: Send, Sync);
#[cfg(feature = "thread-safe-ast")]
static_assertions::assert_impl_all!(CheckedDsrvSpecification: Send, Sync);
#[cfg(feature = "thread-safe-ast")]
static_assertions::assert_impl_all!(ValidatedDsrvSpecification: Send, Sync);
#[cfg(feature = "thread-safe-ast")]
static_assertions::assert_impl_all!(Expr: Send, Sync);
#[cfg(feature = "thread-safe-ast")]
static_assertions::assert_impl_all!(ExprRef<'static>: Send, Sync);
#[cfg(feature = "thread-safe-ast")]
static_assertions::assert_impl_all!(ExprView<'static>: Send, Sync);
#[cfg(feature = "thread-safe-ast")]
static_assertions::assert_impl_all!(CheckedExpr: Send, Sync);
#[cfg(feature = "thread-safe-ast")]
static_assertions::assert_impl_all!(CheckedExprRef<'static>: Send, Sync);
