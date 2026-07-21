//! Errors produced while checking DSRV expressions and specifications.

use crate::lang::dsrv::span::Span;

#[derive(Debug, PartialEq, Eq)]
pub enum TypeErrorKind {
    AnnotationTypeMismatch,
    DefaultTypeMismatch,
    IfBranchTypeMismatch,
    ListElementTypeMismatch,
    ListOperationTypeMismatch,
    ListIndexTypeMismatch,
    MapValueTypeMismatch,
    MapOperationTypeMismatch,
    OperatorTypeMismatch,
    NumericArgumentTypeMismatch,
    ExpectedBooleanCondition,
    ExpectedDynamicString,
    StructMissingField,
    StructUnknownField,
    StructFieldTypeMismatch,
    StructExpected,
    StructFieldAccess,
    StructUnresolvedFieldType,
    DuplicateField,
    StructOperationTypeMismatch,
    FunctionTypeMismatch,
    FunctionArityMismatch,
    ExpectedFunction,
}

#[derive(Debug, PartialEq, Eq)]
pub struct TypeError {
    kind: TypeErrorKind,
    message: String,
    span: Option<Span>,
}

impl TypeError {
    pub fn new(kind: TypeErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            span: None,
        }
    }

    pub fn with_span(kind: TypeErrorKind, message: impl Into<String>, span: Span) -> Self {
        Self {
            kind,
            message: message.into(),
            span: Some(span),
        }
    }

    pub fn kind(&self) -> &TypeErrorKind {
        &self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn span(&self) -> Option<Span> {
        self.span
    }

    fn set_span_if_absent(&mut self, span: Span) {
        self.span.get_or_insert(span);
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum UnresolvedTypeKind {
    EmptyMapValueType,
    MapGetValueType,
    EmptyListIndexElementType,
    ListIndexElementType,
    EmptyListHeadElementType,
    ListHeadElementType,
    VariableType,
}

#[derive(Debug, PartialEq, Eq)]
pub struct UnresolvedTypeError {
    kind: UnresolvedTypeKind,
    message: String,
    span: Option<Span>,
}

impl UnresolvedTypeError {
    pub fn new(kind: UnresolvedTypeKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            span: None,
        }
    }

    pub fn with_span(kind: UnresolvedTypeKind, message: impl Into<String>, span: Span) -> Self {
        Self {
            kind,
            message: message.into(),
            span: Some(span),
        }
    }

    pub fn kind(&self) -> &UnresolvedTypeKind {
        &self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn span(&self) -> Option<Span> {
        self.span
    }

    fn set_span_if_absent(&mut self, span: Span) {
        self.span.get_or_insert(span);
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum SemanticError {
    TypeError(TypeError),
    DeferredError(String, Option<Span>),
    UndeclaredVariable(String, Option<Span>),
    MissingTypeAnnotation(String, Option<Span>),
    MissingTypeAscription(String, Option<Span>),
    UnsupportedDistributionConstraint(String, Option<Span>),
    UnsupportedLiteral(String, Option<Span>),
    UnsupportedExpression(String, Option<Span>),
    InvalidRuntimeScope(String, Option<Span>),
    UnresolvedType(UnresolvedTypeError),
}

impl SemanticError {
    pub fn type_error(kind: TypeErrorKind, message: String) -> Self {
        Self::TypeError(TypeError::new(kind, message))
    }

    pub fn type_error_at(kind: TypeErrorKind, message: String, span: Span) -> Self {
        Self::TypeError(TypeError::with_span(kind, message, span))
    }

    pub fn unresolved_type(kind: UnresolvedTypeKind, message: String) -> Self {
        Self::UnresolvedType(UnresolvedTypeError::new(kind, message))
    }

    pub fn unresolved_type_at(kind: UnresolvedTypeKind, message: String, span: Span) -> Self {
        Self::UnresolvedType(UnresolvedTypeError::with_span(kind, message, span))
    }

    pub fn span(&self) -> Option<Span> {
        match self {
            Self::TypeError(error) => error.span(),
            Self::DeferredError(_, span)
            | Self::UndeclaredVariable(_, span)
            | Self::MissingTypeAnnotation(_, span)
            | Self::MissingTypeAscription(_, span)
            | Self::UnsupportedLiteral(_, span)
            | Self::UnsupportedExpression(_, span)
            | Self::InvalidRuntimeScope(_, span)
            | Self::UnsupportedDistributionConstraint(_, span) => *span,
            Self::UnresolvedType(error) => error.span(),
        }
    }

    pub fn set_span_if_absent(&mut self, span: Span) {
        match self {
            Self::TypeError(error) => error.set_span_if_absent(span),
            Self::DeferredError(_, error_span)
            | Self::UndeclaredVariable(_, error_span)
            | Self::MissingTypeAnnotation(_, error_span)
            | Self::MissingTypeAscription(_, error_span)
            | Self::UnsupportedLiteral(_, error_span)
            | Self::UnsupportedExpression(_, error_span)
            | Self::InvalidRuntimeScope(_, error_span)
            | Self::UnsupportedDistributionConstraint(_, error_span) => {
                error_span.get_or_insert(span);
            }
            Self::UnresolvedType(error) => error.set_span_if_absent(span),
        }
    }
}

pub type SemanticErrors = Vec<SemanticError>;

pub type SemanticResult<Expected> = Result<Expected, SemanticErrors>;
