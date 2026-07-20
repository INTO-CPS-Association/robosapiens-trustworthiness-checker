//! Expression checking for the DSRV AST.
//!
//! Checking records one type per [`ExprId`](crate::lang::dsrv::ast::ExprId).
//! Postorder validation then verifies that every reachable node was annotated
//! before the immutable results are attached to checked expression cursors.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use contiguous_tree::TreeCursorExt;
use ecow::EcoVec;

use super::{SemanticError, SemanticResult, TypeErrorKind};
use super::{TCType, TypeInfo};
use crate::VarName;
use crate::core::{StreamType, StreamTypeAscription, Value};
use crate::lang::dsrv::ast::{
    CheckedDsrvSpecification, CheckedExpr, DsrvSpecification, DynamicExprScope, Expr,
    ExprFieldRefs, ExprRef, ExprRefs, ExprView, SBinOp,
};

struct TypeContext<'types> {
    types: Cow<'types, BTreeMap<VarName, StreamType>>,
    bindings: Vec<(VarName, StreamType)>,
    annotations: Option<Vec<Option<TCType>>>,
    owner: Option<VarName>,
}

impl TypeContext<'_> {
    fn get(&self, name: &VarName) -> Option<&StreamType> {
        self.bindings
            .iter()
            .rev()
            .find_map(|(bound, typ)| (bound == name).then_some(typ))
            .or_else(|| self.types.get(name))
    }

    fn contains_key(&self, name: &VarName) -> bool {
        self.get(name).is_some()
    }
}

pub fn check_specification(spec: DsrvSpecification) -> SemanticResult<CheckedDsrvSpecification> {
    let spec = spec.into_compact_arena();
    super::validation::validate_specification(&spec)?;
    let mut errors = Vec::new();
    let mut context = TypeContext {
        types: Cow::Owned(spec.type_annotations().clone()),
        bindings: Vec::new(),
        annotations: Some(vec![None; spec.arena_len()]),
        owner: None,
    };
    for (var, expr) in spec.roots() {
        context.owner = Some(var.clone());
        let Some(expected) = spec.type_annotations.get(var) else {
            errors.push(SemanticError::MissingTypeAnnotation(
                format!("Variable {var} is missing a type annotation"),
                None,
            ));
            continue;
        };
        let expected = TCType::from_stream_type(expected);
        if let Err(error) = check(expr, Some(&expected), &mut context) {
            errors.push(error);
        }
    }
    if errors.is_empty() {
        #[cfg(debug_assertions)]
        assert_nodes_annotated(&spec, context.annotations.as_ref().unwrap());
        let annotations = finish_annotations(
            context
                .annotations
                .expect("strict checking records annotations"),
        );
        Ok(CheckedDsrvSpecification::new(spec, annotations))
    } else {
        Err(errors)
    }
}

pub(crate) fn check_expression(
    expr: Expr,
    expected: &TCType,
    type_info: &Rc<TypeInfo>,
) -> SemanticResult<CheckedExpr> {
    let mut context = TypeContext {
        types: Cow::Borrowed(type_info.as_ref()),
        bindings: Vec::new(),
        annotations: Some(vec![None; expr.arena().len()]),
        owner: None,
    };
    check(expr.as_ref(), Some(expected), &mut context).map_err(|error| vec![error])?;
    #[cfg(debug_assertions)]
    assert_expr_annotated(expr.as_ref(), context.annotations.as_ref().unwrap());
    let annotations = finish_annotations(
        context
            .annotations
            .expect("expression checking records annotations"),
    );
    Ok(CheckedExpr::new(expr, annotations, Rc::clone(type_info)))
}

/// Check a standalone expression against an expected stream type.
pub fn type_check_expression(
    expr: &Expr,
    expected: &StreamType,
    context: &mut super::TypeInfo,
) -> SemanticResult<()> {
    check_expression(
        expr.clone(),
        &TCType::from_stream_type(expected),
        &Rc::new(context.clone()),
    )
    .map(|_| ())
}

#[cfg(debug_assertions)]
fn assert_nodes_annotated(spec: &DsrvSpecification, annotations: &[Option<TCType>]) {
    for expr in spec.nodes() {
        assert!(
            annotations[expr.id().index()].is_some(),
            "successful type checking left a reachable AST node unannotated"
        );
    }
}

#[cfg(debug_assertions)]
fn assert_expr_annotated(expr: ExprRef<'_>, annotations: &[Option<TCType>]) {
    for node in expr.postorder() {
        assert!(
            annotations[node.id().index()].is_some(),
            "successful type checking left a reachable AST node unannotated"
        );
    }
}

pub(crate) fn infer_expression(
    expr: &Expr,
    expected: Option<&TCType>,
    types: &BTreeMap<VarName, StreamType>,
) -> Result<TCType, SemanticError> {
    let mut context = TypeContext {
        types: Cow::Borrowed(types),
        bindings: Vec::new(),
        annotations: None,
        owner: None,
    };
    check(expr.as_ref(), expected, &mut context)
}

pub(crate) fn check_gradual_annotations(
    spec: &DsrvSpecification,
    root_types: &BTreeMap<VarName, TCType>,
    types: &mut BTreeMap<VarName, StreamType>,
) -> SemanticResult<Vec<TCType>> {
    let mut context = TypeContext {
        types: Cow::Owned(std::mem::take(types)),
        bindings: Vec::new(),
        annotations: Some(vec![None; spec.arena_len()]),
        owner: None,
    };
    for (var, expr) in spec.roots() {
        context.owner = Some(var.clone());
        let expected = root_types
            .get(var)
            .cloned()
            .unwrap_or_else(|| TCType::from_stream_type(&context.types[var]));
        if expected == TCType::Any {
            mark_as_any(expr, context.annotations.as_mut().unwrap());
        } else if let Err(error) = check(expr, Some(&expected), &mut context) {
            *types = context.types.into_owned();
            return Err(vec![error]);
        }
    }
    *types = context.types.into_owned();
    #[cfg(debug_assertions)]
    assert_nodes_annotated(spec, context.annotations.as_ref().unwrap());
    Ok(finish_annotations(
        context
            .annotations
            .expect("gradual checking records annotations"),
    ))
}

fn finish_annotations(annotations: Vec<Option<TCType>>) -> Vec<TCType> {
    annotations
        .into_iter()
        .map(|typ| typ.expect("successful type checking must annotate every expression node"))
        .collect()
}

fn mark_as_any(expr: ExprRef<'_>, annotations: &mut [Option<TCType>]) {
    for node in expr.postorder() {
        annotations[node.id().index()] = Some(TCType::Any);
    }
}

fn check(
    expr: ExprRef<'_>,
    expected: Option<&TCType>,
    context: &mut TypeContext<'_>,
) -> Result<TCType, SemanticError> {
    let (typ, _resolved_operator) = match expr.view() {
        ExprView::Val(Value::Deferred | Value::NoVal) => {
            return Err(SemanticError::UnsupportedLiteral(
                "Deferred and NoVal are runtime states, not source literals".to_owned(),
                Some(expr.span()),
            ));
        }
        ExprView::Val(value) => (value_type(value, expected)?, None),
        ExprView::Var(var) => (
            context
                .get(var)
                .map(TCType::from_stream_type)
                .ok_or_else(|| {
                    SemanticError::UndeclaredVariable(
                        format!("undeclared variable {var}"),
                        Some(expr.span()),
                    )
                })?,
            None,
        ),
        ExprView::BinOp(lhs, rhs, parsed) => {
            // The result constraint is not an operand constraint: comparisons
            // produce `Bool` while comparing values of another type.
            let lhs = check(lhs, None, context)?;
            let rhs = check(rhs, None, context)?;
            resolve_binary(expr, parsed, &lhs, &rhs)?
        }
        ExprView::If(cond, yes, no) => {
            require(
                check(cond, Some(&TCType::Bool), context)?,
                &TCType::Bool,
                expr,
            )?;
            let yes = check(yes, expected, context)?;
            let no = check(no, expected, context)?;
            (
                unify(&yes, &no).ok_or_else(|| mismatch(expr, &yes, &no))?,
                None,
            )
        }
        ExprView::SIndex(value, _) => (check(value, expected, context)?, None),
        ExprView::Dynamic(source, result_type, scope)
        | ExprView::Defer(source, result_type, scope) => {
            validate_runtime_scope(expr, scope, context)?;
            require(
                check(source, Some(&TCType::Str), context)?,
                &TCType::Str,
                expr,
            )?;
            let typ = match result_type {
                StreamTypeAscription::Ascribed(typ) => TCType::from_stream_type(typ),
                StreamTypeAscription::Unascribed => expected.cloned().unwrap_or(TCType::Any),
            };
            (typ, None)
        }
        ExprView::Update(a, b)
        | ExprView::Default(a, b)
        | ExprView::Latch(a, b)
        | ExprView::Init(a, b) => {
            let a = check(a, expected, context)?;
            let b = check(b, Some(&a), context)?;
            (unify(&a, &b).ok_or_else(|| mismatch(expr, &a, &b))?, None)
        }
        ExprView::IsDefined(value) | ExprView::When(value) => {
            check(value, None, context)?;
            (TCType::Bool, None)
        }
        ExprView::Not(value) => {
            require(
                check(value, Some(&TCType::Bool), context)?,
                &TCType::Bool,
                expr,
            )?;
            (TCType::Bool, None)
        }
        ExprView::Lambda(params, body) => {
            let frame_start = context.bindings.len();
            context.bindings.extend(params.iter().cloned());
            let result = check(body, None, context);
            context.bindings.truncate(frame_start);
            (
                TCType::Function(
                    params
                        .iter()
                        .map(|(_, typ)| TCType::from_stream_type(typ))
                        .collect(),
                    Box::new(result?),
                ),
                None,
            )
        }
        ExprView::Apply(function, args) => {
            let function = check(function, None, context)?;
            let TCType::Function(params, result) = function else {
                return Err(error(
                    expr,
                    TypeErrorKind::ExpectedFunction,
                    "application requires a function",
                ));
            };
            if params.len() != args.len() {
                return Err(error(
                    expr,
                    TypeErrorKind::FunctionArityMismatch,
                    "function argument count differs",
                ));
            }
            for (arg, expected) in args.zip(&params) {
                let actual = check(arg, Some(expected), context)?;
                require(actual, expected, expr)?;
            }
            (*result, None)
        }
        ExprView::Fix(function) => {
            let function = check(function, None, context)?;
            let TCType::Function(params, result) = function else {
                return Err(error(
                    expr,
                    TypeErrorKind::ExpectedFunction,
                    "fix requires a function",
                ));
            };
            if params.is_empty() {
                return Err(error(
                    expr,
                    TypeErrorKind::FunctionArityMismatch,
                    "fix function requires a self parameter",
                ));
            }
            let fixed = if params.len() == 1 {
                *result
            } else {
                TCType::Function(params[1..].to_vec().into(), result)
            };
            require(params[0].clone(), &fixed, expr)?;
            (fixed, None)
        }
        ExprView::Partial(function, args) => {
            let function = check(function, None, context)?;
            let TCType::Function(params, result) = function else {
                return Err(error(
                    expr,
                    TypeErrorKind::ExpectedFunction,
                    "partial requires a function",
                ));
            };
            if args.len() > params.len() {
                return Err(error(
                    expr,
                    TypeErrorKind::FunctionArityMismatch,
                    "too many partial arguments",
                ));
            }
            for (arg, expected) in args.clone().zip(&params) {
                require(check(arg, Some(expected), context)?, expected, expr)?;
            }
            (
                TCType::Function(params[args.len()..].iter().cloned().collect(), result),
                None,
            )
        }
        ExprView::List(items) => {
            let expected_element = expected.and_then(TCType::list_element_type);
            let element = check_elements(expr, items, expected_element, context)?;
            (TCType::list(element), None)
        }
        ExprView::Tuple(items) => {
            let expected_items = match expected {
                Some(TCType::Tuple(items)) => Some(items),
                _ => None,
            };
            let mut types = EcoVec::new();
            for (index, item) in items.enumerate() {
                types.push(check(
                    item,
                    expected_items.and_then(|items| items.get(index)),
                    context,
                )?);
            }
            (TCType::Tuple(types), None)
        }
        ExprView::LIndex(list, index) => {
            require(
                check(index, Some(&TCType::Int), context)?,
                &TCType::Int,
                expr,
            )?;
            let list = check(list, None, context)?;
            let TCType::List(element) = list else {
                return Err(error(
                    expr,
                    TypeErrorKind::ListIndexTypeMismatch,
                    "indexing requires a list",
                ));
            };
            (*element, None)
        }
        ExprView::LAppend(list, value) => {
            let list_type = check(list, expected, context)?;
            let TCType::List(element) = list_type else {
                return Err(error(
                    expr,
                    TypeErrorKind::ListOperationTypeMismatch,
                    "append requires a list",
                ));
            };
            require(check(value, Some(&element), context)?, &element, expr)?;
            (TCType::List(element), None)
        }
        ExprView::LConcat(a, b) => {
            let a = check(a, expected, context)?;
            let b = check(b, Some(&a), context)?;
            (unify(&a, &b).ok_or_else(|| mismatch(expr, &a, &b))?, None)
        }
        ExprView::LHead(list) => {
            let TCType::List(element) = check(list, None, context)? else {
                return Err(error(
                    expr,
                    TypeErrorKind::ListOperationTypeMismatch,
                    "head requires a list",
                ));
            };
            (*element, None)
        }
        ExprView::LTail(list) => {
            let typ = check(list, expected, context)?;
            if !matches!(typ, TCType::List(_)) {
                return Err(error(
                    expr,
                    TypeErrorKind::ListOperationTypeMismatch,
                    "tail requires a list",
                ));
            }
            (typ, None)
        }
        ExprView::LLen(list) => {
            let typ = check(list, None, context)?;
            if !matches!(typ, TCType::List(_)) {
                return Err(error(
                    expr,
                    TypeErrorKind::ListOperationTypeMismatch,
                    "length requires a list",
                ));
            }
            (TCType::Int, None)
        }
        ExprView::LMap(function, list) => {
            let list = check(list, None, context)?;
            let TCType::List(input) = list else {
                return Err(error(
                    expr,
                    TypeErrorKind::ListOperationTypeMismatch,
                    "map requires a list",
                ));
            };
            let function = check(function, None, context)?;
            let TCType::Function(params, output) = function else {
                return Err(error(
                    expr,
                    TypeErrorKind::ExpectedFunction,
                    "map requires a function",
                ));
            };
            if params.len() != 1 || unify(&params[0], &input).is_none() {
                return Err(error(
                    expr,
                    TypeErrorKind::FunctionTypeMismatch,
                    "map function has wrong type",
                ));
            }
            (TCType::List(output), None)
        }
        ExprView::LFilter(function, list) => {
            let list = check(list, expected, context)?;
            let TCType::List(input) = &list else {
                return Err(error(
                    expr,
                    TypeErrorKind::ListOperationTypeMismatch,
                    "filter requires a list",
                ));
            };
            let function = check(function, None, context)?;
            let TCType::Function(params, output) = function else {
                return Err(error(
                    expr,
                    TypeErrorKind::ExpectedFunction,
                    "filter requires a function",
                ));
            };
            if params.len() != 1 || unify(&params[0], input).is_none() || *output != TCType::Bool {
                return Err(error(
                    expr,
                    TypeErrorKind::FunctionTypeMismatch,
                    "filter predicate has wrong type",
                ));
            }
            (list, None)
        }
        ExprView::LFold(function, init, list) => {
            let accumulator = check(init, expected, context)?;
            let TCType::List(element) = check(list, None, context)? else {
                return Err(error(
                    expr,
                    TypeErrorKind::ListOperationTypeMismatch,
                    "fold requires a list",
                ));
            };
            let function = check(function, None, context)?;
            let TCType::Function(params, output) = function else {
                return Err(error(
                    expr,
                    TypeErrorKind::ExpectedFunction,
                    "fold requires a function",
                ));
            };
            if params.len() != 2
                || unify(&params[0], &accumulator).is_none()
                || unify(&params[1], &element).is_none()
                || unify(&output, &accumulator).is_none()
            {
                return Err(error(
                    expr,
                    TypeErrorKind::FunctionTypeMismatch,
                    "fold function has wrong type",
                ));
            }
            (accumulator, None)
        }
        ExprView::Map(fields) => {
            reject_duplicate_fields(expr, &fields)?;
            let expected_value = expected.and_then(TCType::map_value_type);
            let value = check_fields(
                expr,
                fields.iter().map(|(_, value)| value),
                expected_value,
                context,
            )?;
            (TCType::map(value), None)
        }
        ExprView::ObjectLiteral(fields) if matches!(expected, Some(TCType::Map(_))) => {
            reject_duplicate_fields(expr, &fields)?;
            let expected_value = expected.and_then(TCType::map_value_type);
            let value = check_fields(
                expr,
                fields.iter().map(|(_, value)| value),
                expected_value,
                context,
            )?;
            (TCType::map(value), None)
        }
        ExprView::Struct(fields) => {
            reject_duplicate_fields(expr, &fields)?;
            let Some(TCType::Struct(expected_fields, allow_extra)) = expected else {
                if context.annotations.is_some() {
                    return Err(error(
                        expr,
                        TypeErrorKind::StructExpected,
                        "Struct constructor requires an expected Struct type",
                    ));
                }
                let mut inferred = EcoVec::new();
                for (name, value) in fields.iter() {
                    inferred.push((name.clone(), check(value, None, context)?));
                }
                return Ok(TCType::Struct(inferred, false));
            };
            if !allow_extra
                && fields
                    .keys()
                    .any(|name| !expected_fields.iter().any(|(expected, _)| expected == name))
            {
                return Err(error(
                    expr,
                    TypeErrorKind::StructUnknownField,
                    "Struct constructor contains unknown fields",
                ));
            }
            for (name, expected_type) in expected_fields {
                let Some(value) = fields.get(name) else {
                    return Err(error(
                        expr,
                        TypeErrorKind::StructMissingField,
                        format!("Struct constructor is missing required field {name}"),
                    ));
                };
                require(
                    check(value, Some(expected_type), context)?,
                    expected_type,
                    expr,
                )?;
            }
            (expected.cloned().expect("matched expected Struct"), None)
        }
        ExprView::ObjectLiteral(fields) => {
            reject_duplicate_fields(expr, &fields)?;
            let expected_fields = match expected {
                Some(TCType::Struct(fields, _)) => Some(fields),
                _ => None,
            };
            if let Some(TCType::Struct(expected_fields, allow_extra)) = expected {
                if !allow_extra
                    && fields
                        .keys()
                        .any(|name| !expected_fields.iter().any(|(expected, _)| expected == name))
                {
                    return Err(error(
                        expr,
                        TypeErrorKind::StructUnknownField,
                        "Struct constructor contains unknown fields",
                    ));
                }
                if let Some((missing, _)) = expected_fields
                    .iter()
                    .find(|(name, _)| !fields.contains_key(name))
                {
                    return Err(error(
                        expr,
                        TypeErrorKind::StructMissingField,
                        format!("Struct constructor is missing required field {missing}"),
                    ));
                }
            }
            let mut types = EcoVec::new();
            for (name, value) in fields.iter() {
                let field_expected = expected_fields.and_then(|fields| {
                    fields
                        .iter()
                        .find(|(field, _)| field == name)
                        .map(|(_, typ)| typ)
                });
                let actual = check(value, field_expected, context).map_err(|err| match err {
                    SemanticError::TypeError(type_error)
                        if type_error.kind() == &TypeErrorKind::AnnotationTypeMismatch =>
                    {
                        let actual = type_error
                            .message()
                            .split_once(", got ")
                            .map_or("an incompatible type", |(_, actual)| actual);
                        error(
                            expr,
                            TypeErrorKind::StructFieldTypeMismatch,
                            format!(
                                "field {name} has type {actual}, expected {}",
                                field_expected.expect("annotation mismatch needs an expectation")
                            ),
                        )
                    }
                    err => err,
                })?;
                types.push((name.clone(), actual));
            }
            let actual = TCType::Struct(types, false);
            let checked = match expected {
                Some(expected @ TCType::Struct(_, _)) if unify(expected, &actual).is_some() => {
                    expected.clone()
                }
                _ => actual,
            };
            (checked, None)
        }
        ExprView::MGet(map, key) => match check(map, None, context)? {
            TCType::Map(value) => (*value, None),
            TCType::Struct(fields, _) => (
                fields
                    .iter()
                    .find(|(name, _)| name == key)
                    .map(|(_, typ)| typ.clone())
                    .ok_or_else(|| {
                        error(
                            expr,
                            TypeErrorKind::StructFieldAccess,
                            "Struct has no field with this name",
                        )
                    })?,
                None,
            ),
            other => {
                return Err(error(
                    expr,
                    TypeErrorKind::MapOperationTypeMismatch,
                    format!("get requires a map or struct, got {other}"),
                ));
            }
        },
        ExprView::SGet(value, key) => match check(value, None, context)? {
            TCType::Struct(fields, _) => (
                fields
                    .iter()
                    .find(|(name, _)| name == key)
                    .map(|(_, typ)| typ.clone())
                    .ok_or_else(|| {
                        error(
                            expr,
                            TypeErrorKind::StructFieldAccess,
                            "Struct has no field with this name",
                        )
                    })?,
                None,
            ),
            TCType::Tuple(items) => (
                items
                    .get(key.parse::<usize>().unwrap_or(usize::MAX))
                    .cloned()
                    .ok_or_else(|| {
                        error(
                            expr,
                            TypeErrorKind::StructFieldAccess,
                            "tuple index out of bounds",
                        )
                    })?,
                None,
            ),
            _ => {
                return Err(error(
                    expr,
                    TypeErrorKind::StructExpected,
                    "Dot field access requires a Struct",
                ));
            }
        },
        ExprView::MInsert(map, key, value) => {
            let map_type = check(map, expected, context)?;
            let element = match &map_type {
                TCType::Map(element) => element.as_ref(),
                TCType::Struct(fields, _) => fields
                    .iter()
                    .find(|(name, _)| name == key)
                    .map(|(_, typ)| typ)
                    .ok_or_else(|| {
                        error(
                            expr,
                            TypeErrorKind::StructUnknownField,
                            format!("unknown struct field {key}"),
                        )
                    })?,
                _ => {
                    return Err(error(
                        expr,
                        TypeErrorKind::MapOperationTypeMismatch,
                        "insert requires a map or struct",
                    ));
                }
            };
            require(check(value, Some(element), context)?, element, expr)?;
            (map_type, None)
        }
        ExprView::MRemove(map, _) => {
            let typ = check(map, expected, context)?;
            if !matches!(typ, TCType::Map(_)) {
                return Err(error(
                    expr,
                    TypeErrorKind::MapOperationTypeMismatch,
                    "remove requires a map",
                ));
            }
            (typ, None)
        }
        ExprView::MHasKey(map, _) => {
            let typ = check(map, None, context)?;
            if !matches!(typ, TCType::Map(_) | TCType::Struct(_, _)) {
                return Err(error(
                    expr,
                    TypeErrorKind::MapOperationTypeMismatch,
                    "has-key requires a map",
                ));
            }
            (TCType::Bool, None)
        }
        ExprView::Sin(value) | ExprView::Cos(value) | ExprView::Tan(value) => {
            require(
                check(value, Some(&TCType::Float), context)?,
                &TCType::Float,
                expr,
            )?;
            (TCType::Float, None)
        }
        ExprView::Abs(value) => {
            let typ = check(value, expected, context)?;
            if !matches!(typ, TCType::Int | TCType::Float) {
                return Err(error(
                    expr,
                    TypeErrorKind::OperatorTypeMismatch,
                    "abs requires a number",
                ));
            }
            (typ, None)
        }
        ExprView::MonitoredAt(_, _) | ExprView::Dist(_, _) => (TCType::Bool, None),
    };
    if let Some(expected) = expected {
        require(typ.clone(), expected, expr)?;
    }
    if let Some(annotations) = &mut context.annotations {
        annotations[expr.id().index()] = Some(typ.clone());
    }
    Ok(typ)
}

fn check_elements(
    expr: ExprRef<'_>,
    items: ExprRefs<'_>,
    expected: Option<&TCType>,
    context: &mut TypeContext<'_>,
) -> Result<TCType, SemanticError> {
    check_fields(expr, items, expected, context)
}

fn check_fields<'arena>(
    expr: ExprRef<'arena>,
    items: impl IntoIterator<Item = ExprRef<'arena>>,
    expected: Option<&TCType>,
    context: &mut TypeContext<'_>,
) -> Result<TCType, SemanticError> {
    let mut result = expected.cloned();
    for item in items {
        let actual = check(item, result.as_ref(), context)?;
        result = Some(match result {
            Some(current) => {
                unify(&current, &actual).ok_or_else(|| mismatch(expr, &current, &actual))?
            }
            None => actual,
        });
    }
    Ok(result.unwrap_or(TCType::Unknown))
}

fn value_type(value: &Value, expected: Option<&TCType>) -> Result<TCType, SemanticError> {
    Ok(match value {
        Value::Int(_) => TCType::Int,
        Value::Float(_) => TCType::Float,
        Value::Str(_) => TCType::Str,
        Value::Bool(_) => TCType::Bool,
        Value::Unit => TCType::Unit,
        Value::List(values) => {
            let inner = expected
                .and_then(TCType::list_element_type)
                .cloned()
                .or_else(|| {
                    values
                        .first()
                        .and_then(|value| value_type(value, None).ok())
                })
                .unwrap_or(TCType::Unknown);
            TCType::list(inner)
        }
        Value::Tuple(values) => TCType::Tuple(
            values
                .iter()
                .map(|value| value_type(value, None).unwrap_or(TCType::Any))
                .collect(),
        ),
        Value::Map(_) => expected
            .cloned()
            .unwrap_or_else(|| TCType::map(TCType::Any)),
        Value::Function(_) => expected.cloned().unwrap_or(TCType::Any),
        Value::Deferred | Value::NoVal => expected.cloned().unwrap_or(TCType::Unknown),
    })
}

fn resolve_binary(
    expr: ExprRef<'_>,
    op: &SBinOp,
    lhs: &TCType,
    rhs: &TCType,
) -> Result<(TCType, Option<()>), SemanticError> {
    let invalid = || {
        error(
            expr,
            TypeErrorKind::OperatorTypeMismatch,
            format!("operator cannot combine {lhs} and {rhs}"),
        )
    };
    Ok(match op {
        SBinOp::NOp(_) => match (lhs, rhs) {
            (TCType::Int, TCType::Int) => (TCType::Int, Some(())),
            (TCType::Float, TCType::Float)
            | (TCType::Int, TCType::Float)
            | (TCType::Float, TCType::Int) => (TCType::Float, Some(())),
            (TCType::Any, TCType::Int) | (TCType::Int, TCType::Any) => (TCType::Int, Some(())),
            (TCType::Any, TCType::Float) | (TCType::Float, TCType::Any) => {
                (TCType::Float, Some(()))
            }
            (TCType::Any, TCType::Any) => (TCType::Any, None),
            _ => return Err(invalid()),
        },
        SBinOp::BOp(_)
            if matches!(lhs, TCType::Bool | TCType::Any)
                && matches!(rhs, TCType::Bool | TCType::Any) =>
        {
            (TCType::Bool, Some(()))
        }
        SBinOp::SOp(_)
            if matches!(lhs, TCType::Str | TCType::Any)
                && matches!(rhs, TCType::Str | TCType::Any) =>
        {
            (TCType::Str, Some(()))
        }
        SBinOp::COp(_) if unify(lhs, rhs).is_some() => (TCType::Bool, Some(())),
        _ => return Err(invalid()),
    })
}

fn validate_runtime_scope(
    expr: ExprRef<'_>,
    scope: &DynamicExprScope,
    context: &TypeContext<'_>,
) -> Result<(), SemanticError> {
    let DynamicExprScope::Explicit(vars) = scope else {
        return Ok(());
    };
    let mut seen = BTreeSet::new();
    for var in vars {
        let problem = if !seen.insert(var) {
            Some(format!("runtime scope contains duplicate variable {var}"))
        } else if context.owner.as_ref() == Some(var) {
            Some(format!(
                "runtime scope cannot contain its owning stream {var}"
            ))
        } else if !context.contains_key(var) {
            Some(format!("runtime scope contains unknown variable {var}"))
        } else {
            None
        };
        if let Some(message) = problem {
            return Err(SemanticError::InvalidRuntimeScope(
                message,
                Some(expr.span()),
            ));
        }
    }
    Ok(())
}

fn unify(a: &TCType, b: &TCType) -> Option<TCType> {
    if a == b {
        Some(a.clone())
    } else if let (TCType::Struct(a_fields, a_extra), TCType::Struct(b_fields, b_extra)) = (a, b) {
        let compatible = |required: &EcoVec<_>, actual: &EcoVec<_>| {
            required.iter().all(|(name, required_type)| {
                actual
                    .iter()
                    .find(|(actual_name, _)| actual_name == name)
                    .and_then(|(_, actual_type)| unify(required_type, actual_type))
                    .is_some()
            })
        };
        if (compatible(a_fields, b_fields) && compatible(b_fields, a_fields))
            || (*a_extra && compatible(a_fields, b_fields))
            || (*b_extra && compatible(b_fields, a_fields))
        {
            Some(if *a_extra { a.clone() } else { b.clone() })
        } else {
            None
        }
    } else if matches!(
        a,
        TCType::Any | TCType::Unknown | TCType::EmptyList | TCType::EmptyMap
    ) {
        Some(b.clone())
    } else if matches!(
        b,
        TCType::Any | TCType::Unknown | TCType::EmptyList | TCType::EmptyMap
    ) {
        Some(a.clone())
    } else if matches!(
        (a, b),
        (TCType::Int, TCType::Float) | (TCType::Float, TCType::Int)
    ) {
        Some(TCType::Float)
    } else {
        None
    }
}

fn require(actual: TCType, expected: &TCType, expr: ExprRef<'_>) -> Result<(), SemanticError> {
    unify(&actual, expected)
        .map(|_| ())
        .ok_or_else(|| mismatch(expr, expected, &actual))
}

fn mismatch(expr: ExprRef<'_>, expected: &TCType, actual: &TCType) -> SemanticError {
    error(
        expr,
        TypeErrorKind::AnnotationTypeMismatch,
        format!("expected {expected}, got {actual}"),
    )
}

fn reject_duplicate_fields(
    expr: ExprRef<'_>,
    fields: &ExprFieldRefs<'_>,
) -> Result<(), SemanticError> {
    if let Some(key) = fields.duplicate_key() {
        return Err(error(
            expr,
            TypeErrorKind::DuplicateField,
            format!("expression contains duplicate field {key:?}"),
        ));
    }
    Ok(())
}

fn error(expr: ExprRef<'_>, kind: TypeErrorKind, message: impl Into<String>) -> SemanticError {
    SemanticError::type_error_at(kind, message.into(), expr.span())
}
