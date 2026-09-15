//! DSRV syntax, specifications, and expression APIs.

#[cfg(feature = "thread-safe-ast")]
pub(crate) type AstShared<T> = std::sync::Arc<T>;
#[cfg(not(feature = "thread-safe-ast"))]
pub(crate) type AstShared<T> = std::rc::Rc<T>;

// Core nodes and expression handles.
mod expression;
// Type-checking decorates the shared expression representation.
mod checked;

// Specifications and their checked metadata.
mod specification;

// Semantic queries over expressions.
mod analysis;

// Expression formatting.
mod display;

pub(crate) use analysis::DependencyKind;
pub use checked::{CheckedExpr, CheckedExprRef};
pub(crate) use checked::{ExprCursor, ExprTypes, ExprTypesBuilder};

pub use expression::{
    Expr, ExprId, ExprRef, ExprView, ReconfigurableExprScope, SyntaxLiteral, SyntaxLiteralError,
    VarOrNodeName,
};
pub(crate) use expression::{
    ExprArena, ExprBuilder, ExprFieldRefs, ExprForest, ExprForestMap, ExprKind, ExprRefs,
};

pub(crate) use specification::UnvalidatedDsrvSpecification;
pub use specification::{
    CheckedDsrvSpecification, Distributed, DsrvAstError, DsrvSpecification, LanguageMode, Local,
    SemanticEntry, ValidatedDsrvSpecification,
};

#[cfg(feature = "thread-safe-ast")]
static_assertions::assert_impl_all!(DsrvSpecification: Send, Sync);
#[cfg(feature = "thread-safe-ast")]
static_assertions::assert_impl_all!(CheckedDsrvSpecification: Send, Sync);
#[cfg(feature = "thread-safe-ast")]
type ThreadSafeLocalValidated = ValidatedDsrvSpecification<Local>;
#[cfg(feature = "thread-safe-ast")]
type ThreadSafeDistributedValidated = ValidatedDsrvSpecification<Distributed>;
#[cfg(feature = "thread-safe-ast")]
type ThreadSafeLocalChecked = CheckedDsrvSpecification<Local>;
#[cfg(feature = "thread-safe-ast")]
type ThreadSafeDistributedChecked = CheckedDsrvSpecification<Distributed>;
#[cfg(feature = "thread-safe-ast")]
static_assertions::assert_impl_all!(ThreadSafeLocalValidated: Send, Sync);
#[cfg(feature = "thread-safe-ast")]
static_assertions::assert_impl_all!(ThreadSafeDistributedValidated: Send, Sync);
#[cfg(feature = "thread-safe-ast")]
static_assertions::assert_impl_all!(ThreadSafeLocalChecked: Send, Sync);
#[cfg(feature = "thread-safe-ast")]
static_assertions::assert_impl_all!(ThreadSafeDistributedChecked: Send, Sync);
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
