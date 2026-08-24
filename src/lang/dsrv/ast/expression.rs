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

use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
};

use contiguous_tree::TreeCursorExt;
use ecow::{EcoString, EcoVec};

use super::CheckedExprRef;
use super::checked::CheckedTypes;
use crate::core::{BinaryOperator, StreamType, Value};
use crate::core::{StreamTypeAscription, VarName};
use crate::distributed::distribution_graphs::NodeName;
use crate::lang::dsrv::span::Span;

/// A literal that can occur in the syntax tree.
///
/// Runtime-only values such as functions and deferred states are deliberately
/// not part of this type. This keeps parsed ASTs independent of the local
/// runtime and allows them to cross threads when `thread-safe-ast` is enabled.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub enum SyntaxLiteral {
    Int(i64),
    Float(f64),
    Str(EcoString),
    Bool(bool),
    List(EcoVec<SyntaxLiteral>),
    Tuple(EcoVec<SyntaxLiteral>),
    Map(BTreeMap<EcoString, SyntaxLiteral>),
    Struct(BTreeMap<EcoString, SyntaxLiteral>),
    Unit,
    /// Retained for AST construction tests and runtime absence propagation.
    /// Source type checking rejects it as a source literal.
    NoVal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SyntaxLiteralError {
    #[error("runtime functions cannot be represented as syntax literals")]
    Function,
    #[error("deferred runtime values cannot be represented as syntax literals")]
    Deferred,
}

impl SyntaxLiteral {
    /// Lower this syntax literal to the corresponding runtime value.
    pub fn into_runtime_value(self) -> Value {
        match self {
            Self::Int(value) => Value::Int(value),
            Self::Float(value) => Value::Float(value),
            Self::Str(value) => Value::Str(value),
            Self::Bool(value) => Value::Bool(value),
            Self::List(values) => {
                Value::List(values.into_iter().map(Self::into_runtime_value).collect())
            }
            Self::Tuple(values) => {
                Value::Tuple(values.into_iter().map(Self::into_runtime_value).collect())
            }
            Self::Map(values) | Self::Struct(values) => Value::Map(
                values
                    .into_iter()
                    .map(|(key, value)| (key, value.into_runtime_value()))
                    .collect(),
            ),
            Self::Unit => Value::Unit,
            Self::NoVal => Value::NoVal,
        }
    }
}

impl From<SyntaxLiteral> for Value {
    fn from(value: SyntaxLiteral) -> Self {
        value.into_runtime_value()
    }
}

impl TryFrom<Value> for SyntaxLiteral {
    type Error = SyntaxLiteralError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Int(value) => Ok(Self::Int(value)),
            Value::Float(value) => Ok(Self::Float(value)),
            Value::Str(value) => Ok(Self::Str(value)),
            Value::Bool(value) => Ok(Self::Bool(value)),
            Value::List(values) => values
                .into_iter()
                .map(Self::try_from)
                .collect::<Result<EcoVec<_>, _>>()
                .map(Self::List),
            Value::Tuple(values) => values
                .into_iter()
                .map(Self::try_from)
                .collect::<Result<EcoVec<_>, _>>()
                .map(Self::Tuple),
            Value::Map(values) => values
                .into_iter()
                .map(|(key, value)| Self::try_from(value).map(|value| (key, value)))
                .collect::<Result<BTreeMap<_, _>, _>>()
                .map(Self::Map),
            Value::Unit => Ok(Self::Unit),
            Value::NoVal => Ok(Self::NoVal),
            Value::Function(_) => Err(SyntaxLiteralError::Function),
            Value::Deferred => Err(SyntaxLiteralError::Deferred),
        }
    }
}

impl From<i64> for SyntaxLiteral {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<f64> for SyntaxLiteral {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<String> for SyntaxLiteral {
    fn from(value: String) -> Self {
        Self::Str(value.into())
    }
}

impl From<&str> for SyntaxLiteral {
    fn from(value: &str) -> Self {
        Self::Str(value.into())
    }
}

impl From<EcoString> for SyntaxLiteral {
    fn from(value: EcoString) -> Self {
        Self::Str(value)
    }
}

impl From<bool> for SyntaxLiteral {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<()> for SyntaxLiteral {
    fn from((): ()) -> Self {
        Self::Unit
    }
}

impl From<EcoVec<SyntaxLiteral>> for SyntaxLiteral {
    fn from(value: EcoVec<SyntaxLiteral>) -> Self {
        Self::List(value)
    }
}

impl From<Vec<SyntaxLiteral>> for SyntaxLiteral {
    fn from(value: Vec<SyntaxLiteral>) -> Self {
        Self::List(value.into())
    }
}

impl From<BTreeMap<EcoString, SyntaxLiteral>> for SyntaxLiteral {
    fn from(value: BTreeMap<EcoString, SyntaxLiteral>) -> Self {
        Self::Map(value)
    }
}

impl Display for SyntaxLiteral {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int(value) => write!(f, "{value}"),
            Self::Float(value) if value.is_finite() && value.fract() == 0.0 => {
                write!(f, "{value:.1}")
            }
            Self::Float(value) => write!(f, "{value}"),
            Self::Str(value) => write!(f, "{value:?}"),
            Self::Bool(value) => write!(f, "{value}"),
            Self::List(values) => {
                let values = values
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "[{values}]")
            }
            Self::Tuple(values) => {
                let values = values
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "Tuple({values})")
            }
            Self::Map(values) => {
                let values = values
                    .iter()
                    .map(|(key, value)| format!("{key:?}: {value}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "Map({values})")
            }
            Self::Struct(values) => {
                let values = values
                    .iter()
                    .map(|(key, value)| format!("{key:?}: {value}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "Struct({values})")
            }
            Self::Unit => write!(f, "()"),
            Self::NoVal => write!(f, "no_val"),
        }
    }
}

// Keep the syntax literal independently thread-safe in every configuration.
static_assertions::assert_impl_all!(SyntaxLiteral: Send, Sync);

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
        Val(value: into_data(SyntaxLiteral)),
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

    use crate::core::{BinaryOperator, RuntimeFunction, Value};
    use crate::lang::dsrv::ast::{DynamicExprScope, Expr, SyntaxLiteral, SyntaxLiteralError};

    #[test]
    fn runtime_functions_are_rejected_from_syntax_literals() {
        let runtime = Value::Function(RuntimeFunction::opaque("\\x -> x"));

        assert_eq!(
            SyntaxLiteral::try_from(runtime),
            Err(SyntaxLiteralError::Function)
        );
    }

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
