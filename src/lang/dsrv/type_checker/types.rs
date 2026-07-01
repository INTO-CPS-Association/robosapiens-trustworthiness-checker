//! Semantic errors, the type-checking traits, and runtime validation of
//! values against checker types.

use super::*;
use crate::Value;
use crate::core::StreamType;
use crate::lang::dsrv::span::Span;
use ecow::EcoVec;

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
    UnsupportedLiteral(String, Option<Span>),
    UnsupportedExpression(String, Option<Span>),
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
            | Self::UnsupportedExpression(_, span) => *span,
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
            | Self::UnsupportedExpression(_, error_span) => {
                error_span.get_or_insert(span);
            }
            Self::UnresolvedType(error) => error.set_span_if_absent(span),
        }
    }
}

pub type SemanticErrors = Vec<SemanticError>;

pub type SemanticResult<Expected> = Result<Expected, SemanticErrors>;

pub trait TypeCheckableHelper<TypedExpr> {
    fn type_check_raw(
        &self,
        expected: Option<&StreamType>,
        ctx: &mut TypeInfo,
        errs: &mut SemanticErrors,
    ) -> Result<TypedExpr, ()>;
}
impl<TypedExpr, R: TypeCheckableHelper<TypedExpr>> TypeCheckable<TypedExpr> for R {
    fn type_check(&self, context: &mut TypeInfo) -> SemanticResult<TypedExpr> {
        let mut errors = Vec::new();
        let res = self.type_check_raw(None, context, &mut errors);
        match res {
            Ok(se) => Ok(se),
            Err(()) => Err(errors),
        }
    }
}
pub trait TypeCheckable<TypedExpr> {
    fn type_check_with_default(&self) -> SemanticResult<TypedExpr> {
        self.type_check(&mut TypeInfo::new())
    }

    fn type_check(&self, context: &mut TypeInfo) -> SemanticResult<TypedExpr>;
}

fn check_value_type_ref(typ: &TCType, value: &Value) -> Result<(), String> {
    match (typ, value) {
        (TCType::Str, Value::Str(_)) => Ok(()),
        (TCType::Int, Value::Int(_)) => Ok(()),
        (TCType::Bool, Value::Bool(_)) => Ok(()),
        (TCType::Float, Value::Float(_)) => Ok(()),
        (TCType::Unit, Value::Unit) => Ok(()),
        (TCType::Function(_, _), Value::Function(_)) => Ok(()),
        (TCType::Any, _) => Ok(()),
        (TCType::EmptyList | TCType::EmptyMap | TCType::Unknown, _) => Ok(()),
        (TCType::Tuple(types), Value::Tuple(values) | Value::List(values))
            if types.len() == values.len() =>
        {
            types
                .iter()
                .zip(values.iter())
                .try_for_each(|(typ, val)| check_value_type_ref(typ, val))
        }
        (typ, Value::List(inner_values)) if typ.list_element_type().is_some() => {
            let inner_type = typ.list_element_type().expect("checked above");
            inner_values
                .iter()
                .try_for_each(|val| check_value_type_ref(inner_type, val))
        }
        (TCType::Map(value_type), Value::Map(values)) => values
            .values()
            .try_for_each(|val| check_value_type_ref(value_type, val)),
        (TCType::Struct(struct_types, _), Value::Map(values)) => {
            struct_types.iter().try_for_each(|(k, v)| {
                let k: &str = k.as_str();
                check_value_type_ref(v, values.get(k).unwrap_or(&Value::Unit))
            })
        }
        (typ, value) => Err(format!("Type mismatch between {} and {}", typ, value)),
    }
}

pub fn check_value_type(typ: TCType, value: &Value) -> Result<(), String> {
    check_value_type_ref(&typ, value)
}

pub fn check_value_stream_type(typ: &StreamType, value: &Value) -> Result<(), String> {
    let typ = TCType::from_stream_type(typ);
    check_value_type_ref(&typ, value)
}

pub fn extract_value_type(value: Value) -> TCType {
    match value {
        Value::Str(_) => TCType::Str,
        Value::Function(_) => TCType::Function(EcoVec::new(), Box::new(TCType::Any)),
        Value::Int(_) => TCType::Int,
        Value::Bool(_) => TCType::Bool,
        Value::Float(_) => TCType::Float,
        Value::Unit => TCType::Unit,
        Value::List(values) => {
            if values.is_empty() {
                TCType::EmptyList
            } else {
                TCType::List(Box::new(extract_value_type(values[0].clone())))
            }
        }
        Value::Tuple(values) => {
            TCType::Tuple(values.iter().cloned().map(extract_value_type).collect())
        }
        Value::Map(values) => {
            if values.is_empty() {
                TCType::EmptyMap
            } else {
                let first_key = values.keys().next().unwrap();
                TCType::Map(Box::new(extract_value_type(values[first_key].clone())))
            }
        }
        Value::Deferred => TCType::Unknown,
        Value::NoVal => TCType::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use test_log::test;

    #[test]
    fn test_basic_types_matching() {
        // These types are explicitly handled in the implementation
        assert!(check_value_type(TCType::Int, &Value::Int(42)).is_ok());
        assert!(check_value_type(TCType::Str, &Value::Str("hello".into())).is_ok());
        assert!(check_value_type(TCType::Bool, &Value::Bool(true)).is_ok());

        // Testing Float and Unit types to match expected functionality
        assert!(check_value_type(TCType::Float, &Value::Float(3.14)).is_ok());
        assert!(check_value_type(TCType::Unit, &Value::Unit).is_ok());
    }

    #[test]
    fn test_type_mismatches() {
        // Int type with non-Int values
        assert!(check_value_type(TCType::Int, &Value::Str("42".into())).is_err());
        assert!(check_value_type(TCType::Int, &Value::Bool(true)).is_err());
        assert!(check_value_type(TCType::Int, &Value::Float(42.0)).is_err());

        // Str type with non-Str values
        assert!(check_value_type(TCType::Str, &Value::Int(42)).is_err());
        assert!(check_value_type(TCType::Str, &Value::Bool(true)).is_err());

        // Bool type with non-Bool values
        assert!(check_value_type(TCType::Bool, &Value::Int(0)).is_err());
        assert!(check_value_type(TCType::Bool, &Value::Str("true".into())).is_err());
    }

    #[test]
    fn test_empty_literal_and_unknown_placeholders_accept_values() {
        // Empty container placeholders accept any value while the literal is still unconstrained.
        assert!(check_value_type(TCType::EmptyList, &Value::Int(42)).is_ok());
        assert!(check_value_type(TCType::EmptyList, &Value::Str("hello".into())).is_ok());
        assert!(check_value_type(TCType::EmptyMap, &Value::Bool(true)).is_ok());
        assert!(check_value_type(TCType::EmptyMap, &Value::Float(3.14)).is_ok());

        let list_val = Value::List(vec![Value::Int(1), Value::Int(2)].into());
        let map_val = Value::Map(BTreeMap::from([("x".into(), Value::Int(1))]));
        assert!(check_value_type(TCType::EmptyList, &list_val).is_ok());
        assert!(check_value_type(TCType::EmptyMap, &map_val).is_ok());

        // Special value placeholder
        assert!(check_value_type(TCType::Unknown, &Value::Deferred).is_ok());
        assert!(check_value_type(TCType::Unknown, &Value::NoVal).is_ok());
    }

    #[test]
    fn test_stream_type_validation_checks_recursive_lists() {
        let list_type = StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int))));
        let valid = Value::List(
            vec![
                Value::List(vec![Value::Int(1), Value::Int(2)].into()),
                Value::List(vec![Value::Int(3)].into()),
            ]
            .into(),
        );
        let invalid = Value::List(
            vec![
                Value::List(vec![Value::Int(1)].into()),
                Value::List(vec![Value::Str("wrong".into())].into()),
            ]
            .into(),
        );

        assert!(check_value_stream_type(&list_type, &valid).is_ok());
        assert!(check_value_stream_type(&list_type, &invalid).is_err());
    }

    #[test]
    fn test_list_type_matching() {
        // Empty list
        let empty_list = Value::List(vec![].into());
        assert!(check_value_type(TCType::list(TCType::Int), &empty_list).is_ok());
        assert!(check_value_type(TCType::list(TCType::Str), &empty_list).is_ok());
        assert!(check_value_type(TCType::list(TCType::Bool), &empty_list).is_ok());

        // Homogeneous lists
        let int_list = Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)].into());
        assert!(check_value_type(TCType::list(TCType::Int), &int_list).is_ok());
        assert!(check_value_type(TCType::list(TCType::Str), &int_list).is_err());

        let str_list = Value::List(
            vec![
                Value::Str("a".into()),
                Value::Str("b".into()),
                Value::Str("c".into()),
            ]
            .into(),
        );
        assert!(check_value_type(TCType::list(TCType::Str), &str_list).is_ok());
        assert!(check_value_type(TCType::list(TCType::Int), &str_list).is_err());

        // Mixed list (should fail for any specific element type)
        let mixed_list =
            Value::List(vec![Value::Int(1), Value::Str("a".into()), Value::Bool(true)].into());
        assert!(check_value_type(TCType::list(TCType::Int), &mixed_list).is_err());
        assert!(check_value_type(TCType::list(TCType::Str), &mixed_list).is_err());
        assert!(check_value_type(TCType::list(TCType::Bool), &mixed_list).is_err());
    }

    #[test]
    fn test_nested_list_type_matching() {
        // List of List of Int
        let nested_int_list = Value::List(
            vec![
                Value::List(vec![Value::Int(1), Value::Int(2)].into()),
                Value::List(vec![Value::Int(3), Value::Int(4)].into()),
            ]
            .into(),
        );

        let nested_int_type = TCType::list(TCType::list(TCType::Int));
        assert!(check_value_type(nested_int_type.clone(), &nested_int_list).is_ok());

        // Nested list with type errors
        let nested_mixed_list = Value::List(
            vec![
                Value::List(vec![Value::Int(1), Value::Int(2)].into()),
                Value::List(vec![Value::Str("a".into())].into()),
            ]
            .into(),
        );
        assert!(check_value_type(nested_int_type, &nested_mixed_list).is_err());
    }

    #[test]
    fn test_deeply_nested_list() {
        // Triple nested list
        let triple_nested_list = Value::List(
            vec![Value::List(
                vec![Value::List(
                    vec![Value::Bool(true), Value::Bool(false)].into(),
                )]
                .into(),
            )]
            .into(),
        );

        let triple_nested_type = TCType::list(TCType::list(TCType::list(TCType::Bool)));
        assert!(check_value_type(triple_nested_type, &triple_nested_list).is_ok());
    }

    #[test]
    fn test_list_depth_mismatches() {
        // A list of integers (depth 1)
        let simple_list = Value::List(vec![Value::Int(1), Value::Int(2)].into());

        // A list of lists of integers (depth 2)
        let nested_list = Value::List(
            vec![
                Value::List(vec![Value::Int(1), Value::Int(2)].into()),
                Value::List(vec![Value::Int(3), Value::Int(4)].into()),
            ]
            .into(),
        );

        // Test: Cannot use a simple list where a nested list is expected
        assert!(check_value_type(TCType::list(TCType::list(TCType::Int)), &simple_list).is_err());

        // Test: Cannot use a nested list where a simple list is expected
        assert!(check_value_type(TCType::list(TCType::Int), &nested_list).is_err());

        // Test: Make sure the error message mentions something about a type mismatch
        let result = check_value_type(TCType::list(TCType::list(TCType::Int)), &simple_list);
        assert!(result.is_err());
        let error_msg = result.unwrap_err();
        assert!(error_msg.contains("Type mismatch"));
    }

    #[test]
    fn test_special_values() {
        // These should fail since they're not the expected concrete types
        assert!(check_value_type(TCType::Int, &Value::Deferred).is_err());
        assert!(check_value_type(TCType::Str, &Value::NoVal).is_err());
        assert!(check_value_type(TCType::list(TCType::Int), &Value::Deferred).is_err());
    }

    #[test]
    fn test_error_messages() {
        // Test that error messages contain useful information
        let result = check_value_type(TCType::Int, &Value::Str("42".into()));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("Type mismatch"));

        // Test error propagation in nested lists
        let str_in_int_list = Value::List(
            vec![
                Value::Int(1),
                Value::Str("not an int".into()), // This will cause error
                Value::Int(3),
            ]
            .into(),
        );
        let result = check_value_type(TCType::list(TCType::Int), &str_in_int_list);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("Type mismatch"));
    }

    // These tests validate boundary cases
    #[test]
    fn test_boundary_cases() {
        // Non-list value with list type
        assert!(check_value_type(TCType::list(TCType::Int), &Value::Int(42)).is_err());

        // Trying to treat a non-primitive as primitive
        let list_val = Value::List(vec![Value::Int(1), Value::Int(2)].into());
        assert!(check_value_type(TCType::Int, &list_val).is_err());

        // Empty nested list
        let empty_nested = Value::List(vec![Value::List(vec![].into())].into());
        assert!(check_value_type(TCType::list(TCType::list(TCType::Int)), &empty_nested).is_ok());
    }

    #[test]
    fn test_extract_value_type_primitives() {
        // Test primitives
        assert_eq!(extract_value_type(Value::Int(42)), TCType::Int);
        assert_eq!(extract_value_type(Value::Str("hello".into())), TCType::Str);
        assert_eq!(extract_value_type(Value::Bool(true)), TCType::Bool);
        assert_eq!(extract_value_type(Value::Float(3.14)), TCType::Float);
        assert_eq!(extract_value_type(Value::Unit), TCType::Unit);
    }

    #[test]
    fn test_extract_value_type_list() {
        // Test list type
        let list_value = Value::List(vec![Value::Int(1), Value::Int(2)].into());
        match extract_value_type(list_value) {
            TCType::List(inner) => {
                assert_eq!(*inner, TCType::Int);
            }
            _ => panic!("Expected List type for list value"),
        }
    }

    #[test]
    fn test_extract_value_type_special_values() {
        assert_eq!(
            extract_value_type(Value::Map(Default::default())),
            TCType::EmptyMap
        );
        assert_eq!(extract_value_type(Value::Deferred), TCType::Unknown);
        assert_eq!(extract_value_type(Value::NoVal), TCType::Unknown);
    }

    #[test]
    fn test_map_value_type_matching() {
        let map = Value::Map(BTreeMap::from([
            ("x".into(), Value::Int(1)),
            ("y".into(), Value::Int(2)),
        ]));
        assert!(check_value_type(TCType::map(TCType::Int), &map).is_ok());
        assert!(check_value_type(TCType::map(TCType::Str), &map).is_err());

        let nested = Value::Map(BTreeMap::from([(
            "xs".into(),
            Value::List(vec![Value::Bool(true)].into()),
        )]));
        assert!(check_value_type(TCType::map(TCType::list(TCType::Bool)), &nested).is_ok());
    }

    #[test]
    fn test_extract_and_check_compatibility() {
        // Extract a type from a value, then check the same value against that type
        let int_value = Value::Int(42);
        let extracted_type = extract_value_type(int_value.clone());
        assert_eq!(extracted_type, TCType::Int);
        assert!(check_value_type(extracted_type, &int_value).is_ok());

        // Same for string
        let str_value = Value::Str("test".into());
        let extracted_type = extract_value_type(str_value.clone());
        assert_eq!(extracted_type, TCType::Str);
        assert!(check_value_type(extracted_type, &str_value).is_ok());

        // Same for list
        let list_value = Value::List(vec![Value::Bool(true), Value::Bool(false)].into());
        let extracted_type = extract_value_type(list_value.clone());
        assert!(matches!(extracted_type, TCType::List(_)));
        assert!(check_value_type(extracted_type, &list_value).is_ok());
    }
}
