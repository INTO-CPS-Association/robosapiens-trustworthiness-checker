//! Diagnostics produced while checking DSRV expressions and specifications.
//!
//! Checking yields a [`SemanticAnalysisReport`]: the checked artefact or the
//! [`SemanticErrors`] that prevented it, together with every
//! [`SemanticWarning`] the attempt proved. Warnings never decide whether
//! checking succeeds, and a failed check keeps the warnings it had already
//! proved. Parse and expansion errors are reported before checking begins and
//! are not diagnostics of this kind.
//!
//! Every finding keeps its byte [`span`](SemanticError::span) as before, and
//! owns a [`SourceLocation`]: the file its primary site is in, and for code
//! inlined from another module, the definition site as a note. The location
//! owns the files it names, so a report can be split with
//! [`into_parts`](SemanticAnalysisReport::into_parts), and either half
//! rendered, after the program that was checked has been dropped. A finding
//! about a programmatically constructed tree has no location.

use crate::lang::dsrv::source_map::{NodeOrigin, SourceArchive, SourceLocation};
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
    ExpectedExpressionSource,
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
    ExpectedUnion,
    UnknownUnionTag,
    ConstructorPayloadArity,
    ConstructorPayloadTypeMismatch,
    PatternTypeMismatch,
    OrPatternBindings,
    MatchArmTypeMismatch,
    MatchNotExhaustive,
    MatchWithoutArms,
}

#[derive(Debug, PartialEq, Eq)]
pub struct TypeError {
    kind: TypeErrorKind,
    message: String,
    span: Option<Span>,
    location: SourceLocation,
}

impl TypeError {
    pub fn new(kind: TypeErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            span: None,
            location: SourceLocation::default(),
        }
    }

    pub fn with_span(kind: TypeErrorKind, message: impl Into<String>, span: Span) -> Self {
        Self {
            kind,
            message: message.into(),
            span: Some(span),
            location: SourceLocation::default(),
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

    pub fn location(&self) -> &SourceLocation {
        &self.location
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
    ConstructorUnion,
}

#[derive(Debug, PartialEq, Eq)]
pub struct UnresolvedTypeError {
    kind: UnresolvedTypeKind,
    message: String,
    span: Option<Span>,
    location: SourceLocation,
}

impl UnresolvedTypeError {
    pub fn new(kind: UnresolvedTypeKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            span: None,
            location: SourceLocation::default(),
        }
    }

    pub fn with_span(kind: UnresolvedTypeKind, message: impl Into<String>, span: Span) -> Self {
        Self {
            kind,
            message: message.into(),
            span: Some(span),
            location: SourceLocation::default(),
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

    pub fn location(&self) -> &SourceLocation {
        &self.location
    }

    fn set_span_if_absent(&mut self, span: Span) {
        self.span.get_or_insert(span);
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum SemanticError {
    DuplicateDeclaration {
        variable: crate::VarName,
        first: Span,
        duplicate: Span,
        location: SourceLocation,
    },
    TypeError(TypeError),
    DeferredError(String, Option<Span>, SourceLocation),
    UndeclaredVariable(String, Option<Span>, SourceLocation),
    MissingTypeAnnotation(String, Option<Span>, SourceLocation),
    MissingTypeAscription(String, Option<Span>, SourceLocation),
    UnsupportedLiteral(String, Option<Span>, SourceLocation),
    UnsupportedExpression(String, Option<Span>, SourceLocation),
    InvalidRuntimeScope(String, Option<Span>, SourceLocation),
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
            Self::DuplicateDeclaration { duplicate, .. } => Some(*duplicate),
            Self::TypeError(error) => error.span(),
            Self::DeferredError(_, span, _)
            | Self::UndeclaredVariable(_, span, _)
            | Self::MissingTypeAnnotation(_, span, _)
            | Self::MissingTypeAscription(_, span, _)
            | Self::UnsupportedLiteral(_, span, _)
            | Self::UnsupportedExpression(_, span, _)
            | Self::InvalidRuntimeScope(_, span, _) => *span,
            Self::UnresolvedType(error) => error.span(),
        }
    }

    /// Where the error applies, owning the files it names: the primary
    /// site, and where inlined code was defined.
    pub fn location(&self) -> &SourceLocation {
        match self {
            Self::DuplicateDeclaration { location, .. }
            | Self::DeferredError(_, _, location)
            | Self::UndeclaredVariable(_, _, location)
            | Self::MissingTypeAnnotation(_, _, location)
            | Self::MissingTypeAscription(_, _, location)
            | Self::UnsupportedLiteral(_, _, location)
            | Self::UnsupportedExpression(_, _, location)
            | Self::InvalidRuntimeScope(_, _, location) => location,
            Self::TypeError(error) => &error.location,
            Self::UnresolvedType(error) => &error.location,
        }
    }

    fn location_mut(&mut self) -> &mut SourceLocation {
        match self {
            Self::DuplicateDeclaration { location, .. }
            | Self::DeferredError(_, _, location)
            | Self::UndeclaredVariable(_, _, location)
            | Self::MissingTypeAnnotation(_, _, location)
            | Self::MissingTypeAscription(_, _, location)
            | Self::UnsupportedLiteral(_, _, location)
            | Self::UnsupportedExpression(_, _, location)
            | Self::InvalidRuntimeScope(_, _, location) => location,
            Self::TypeError(error) => &mut error.location,
            Self::UnresolvedType(error) => &mut error.location,
        }
    }

    /// Record, while checking, the node this error's span came from. An
    /// error without a span, or already located, is unchanged.
    pub(crate) fn located(mut self, origin: NodeOrigin) -> Self {
        if let Some(span) = self.span()
            && self.location().is_unset()
        {
            *self.location_mut() = SourceLocation::pending(origin, span);
        }
        self
    }

    /// Locate this error where `cause` was located, or failing that at
    /// `origin`: for an error that restates another at the other's span.
    pub(crate) fn located_as(mut self, cause: &Self, origin: NodeOrigin) -> Self {
        if !cause.location().is_unset() && self.span() == cause.span() {
            *self.location_mut() = cause.location().clone();
            self
        } else {
            self.located(origin)
        }
    }

    /// Give the error owned files from `archive`, the one its checking
    /// attempt addressed, as that attempt finishes.
    pub(crate) fn materialise(&mut self, archive: Option<&SourceArchive>) {
        self.location_mut().materialise(archive);
    }

    pub fn set_span_if_absent(&mut self, span: Span) {
        match self {
            Self::DuplicateDeclaration { duplicate, .. } => *duplicate = span,
            Self::TypeError(error) => error.set_span_if_absent(span),
            Self::DeferredError(_, error_span, _)
            | Self::UndeclaredVariable(_, error_span, _)
            | Self::MissingTypeAnnotation(_, error_span, _)
            | Self::MissingTypeAscription(_, error_span, _)
            | Self::UnsupportedLiteral(_, error_span, _)
            | Self::UnsupportedExpression(_, error_span, _)
            | Self::InvalidRuntimeScope(_, error_span, _) => {
                error_span.get_or_insert(span);
            }
            Self::UnresolvedType(error) => error.set_span_if_absent(span),
        }
    }
}

/// Give every error owned files from `archive` as a checking attempt that
/// addressed it finishes.
pub(crate) fn materialise_errors(errors: &mut [SemanticError], archive: Option<&SourceArchive>) {
    for error in errors {
        error.materialise(archive);
    }
}

pub type SemanticErrors = Vec<SemanticError>;

pub type SemanticResult<Expected> = Result<Expected, SemanticErrors>;

/// What a [`SemanticWarning`] reports. Each kind is one warning rule, and
/// its [`code`](Self::code) is the stable name consumers match on; messages
/// may change wording.
///
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum SemanticWarningKind {
    RedundantCast,
    #[cfg(test)]
    TestAlpha,
    #[cfg(test)]
    TestBeta,
}

impl SemanticWarningKind {
    /// The stable, descriptive code of this warning rule.
    pub fn code(self) -> &'static str {
        match self {
            Self::RedundantCast => "dsrv.redundant-cast",
            #[cfg(test)]
            Self::TestAlpha => "test-alpha",
            #[cfg(test)]
            Self::TestBeta => "test-beta",
        }
    }
}

/// A finding that does not stop a specification from checking. Every
/// warning is a warning: there is no severity to configure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SemanticWarning {
    kind: SemanticWarningKind,
    message: String,
    span: Option<Span>,
    location: SourceLocation,
}

impl SemanticWarning {
    pub(crate) fn new(
        kind: SemanticWarningKind,
        message: impl Into<String>,
        span: Option<Span>,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            span,
            location: SourceLocation::default(),
        }
    }

    /// Record, while checking, the node this warning's span came from.
    pub(crate) fn located(mut self, origin: NodeOrigin) -> Self {
        if let Some(span) = self.span
            && self.location.is_unset()
        {
            self.location = SourceLocation::pending(origin, span);
        }
        self
    }

    /// Give the warning owned files from `archive` as its checking attempt
    /// finishes.
    pub(crate) fn materialise(&mut self, archive: Option<&SourceArchive>) {
        self.location.materialise(archive);
    }

    #[cfg(test)]
    pub(crate) fn with_location(mut self, location: SourceLocation) -> Self {
        self.location = location;
        self
    }

    pub fn kind(&self) -> SemanticWarningKind {
        self.kind
    }

    /// The stable code of the rule that produced this warning.
    pub fn code(&self) -> &'static str {
        self.kind.code()
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    /// Where the warning applies, in the coordinates of the text that was
    /// checked. Code from another module is attributed to the call site.
    pub fn span(&self) -> Option<Span> {
        self.span
    }

    /// Where the warning applies, owning the files it names: the primary
    /// site, which is [`span`](Self::span) in its file, and where inlined
    /// code was defined.
    pub fn location(&self) -> &SourceLocation {
        &self.location
    }
}

/// The outcome of one semantic checking attempt: the checked artefact or the
/// errors that prevented it, and the warnings proved along the way, in a
/// deterministic order.
///
/// A report is not a `Result`. Take it apart with
/// [`into_parts`](Self::into_parts) to handle both halves, or call
/// [`discard_warnings`](Self::discard_warnings) to drop the warnings
/// deliberately.
#[must_use = "a semantic analysis report carries warnings as well as its result"]
#[derive(Debug)]
pub struct SemanticAnalysisReport<T> {
    result: Result<T, SemanticErrors>,
    warnings: Vec<SemanticWarning>,
}

impl<T> SemanticAnalysisReport<T> {
    pub(crate) fn new(result: Result<T, SemanticErrors>, warnings: Vec<SemanticWarning>) -> Self {
        Self { result, warnings }
    }

    pub fn result(&self) -> &Result<T, SemanticErrors> {
        &self.result
    }

    pub fn warnings(&self) -> &[SemanticWarning] {
        &self.warnings
    }

    pub fn into_parts(self) -> (Result<T, SemanticErrors>, Vec<SemanticWarning>) {
        (self.result, self.warnings)
    }

    /// Transform the checked artefact, keeping the warnings.
    pub fn map_checked<U>(self, transform: impl FnOnce(T) -> U) -> SemanticAnalysisReport<U> {
        SemanticAnalysisReport {
            result: self.result.map(transform),
            warnings: self.warnings,
        }
    }

    /// Keep only the result. The warnings are lost.
    pub fn discard_warnings(self) -> Result<T, SemanticErrors> {
        self.result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn warnings() -> Vec<SemanticWarning> {
        vec![
            SemanticWarning::new(SemanticWarningKind::TestBeta, "second", None),
            SemanticWarning::new(
                SemanticWarningKind::TestAlpha,
                "first",
                Some(Span::new(1, 2)),
            ),
        ]
    }

    #[test]
    fn a_report_keeps_its_warnings_in_order_on_success_and_failure() {
        let success = SemanticAnalysisReport::new(Ok(1), warnings());
        assert_eq!(success.result(), &Ok(1));
        assert_eq!(success.warnings(), warnings().as_slice());
        assert_eq!(success.into_parts(), (Ok(1), warnings()));

        let failure = SemanticAnalysisReport::<i32>::new(
            Err(vec![SemanticError::DeferredError(
                "no".into(),
                None,
                Default::default(),
            )]),
            warnings(),
        );
        assert!(failure.result().is_err());
        assert_eq!(failure.warnings(), warnings().as_slice());
    }

    #[test]
    fn mapping_the_checked_artefact_keeps_the_warnings() {
        let mapped = SemanticAnalysisReport::new(Ok(1), warnings()).map_checked(|n| n + 1);
        assert_eq!(mapped.into_parts(), (Ok(2), warnings()));

        let failed = SemanticAnalysisReport::<i32>::new(Err(Vec::new()), warnings())
            .map_checked(|_| unreachable!("a failed check has nothing to map"));
        assert_eq!(failed.warnings(), warnings().as_slice());
    }

    #[test]
    fn discarding_warnings_keeps_only_the_result() {
        assert_eq!(
            SemanticAnalysisReport::new(Ok("checked"), warnings()).discard_warnings(),
            Ok("checked")
        );
    }

    #[test]
    fn a_warning_exposes_its_rule_code_message_and_span() {
        let warning = &warnings()[1];
        assert_eq!(warning.kind(), SemanticWarningKind::TestAlpha);
        assert_eq!(warning.code(), "test-alpha");
        assert_eq!(warning.message(), "first");
        assert_eq!(warning.span(), Some(Span::new(1, 2)));
    }
}
