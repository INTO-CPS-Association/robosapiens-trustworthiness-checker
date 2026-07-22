//! Expression checking for the DSRV AST.
//!
//! Checking infers one type per [`ExprId`](crate::lang::dsrv::ast::ExprId).
//! Postorder validation verifies that every reachable expression has a type
//! before the immutable results are attached to checked expression cursors.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use contiguous_tree::TreeCursorExt;
use ecow::EcoVec;

use super::{SemanticError, SemanticResult, TypeErrorKind};
use super::{StreamTypeEnvironment, TCType};
use crate::VarName;
use crate::core::{StreamType, StreamTypeAscription, Value};
use crate::lang::dsrv::ast::{
    CheckedDsrvSpecification, CheckedExpr, DsrvSpecification, DynamicExprScope, Expr,
    ExprFieldRefs, ExprRef, ExprRefs, ExprTypes, ExprTypesBuilder, ExprView, SBinOp,
};

struct TypeContext<'types> {
    environment: Cow<'types, StreamTypeEnvironment>,
    local_bindings: Vec<(VarName, StreamType)>,
    expr_types: Option<ExprTypesBuilder>,
    owner: Option<VarName>,
}

impl TypeContext<'_> {
    fn get(&self, name: &VarName) -> Option<&StreamType> {
        self.local_bindings
            .iter()
            .rev()
            .find_map(|(bound, typ)| (bound == name).then_some(typ))
            .or_else(|| self.environment.get(name))
    }

    fn contains_key(&self, name: &VarName) -> bool {
        self.get(name).is_some()
    }
}

pub fn check_specification(
    spec: DsrvSpecification,
    distributed: bool,
) -> SemanticResult<CheckedDsrvSpecification> {
    super::validation::validate_specification(&spec, distributed)?;
    let mut errors = Vec::new();
    let mut context = TypeContext {
        environment: Cow::Owned(spec.type_annotations().clone()),
        local_bindings: Vec::new(),
        expr_types: Some(spec.exprs.annotations_builder()),
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
        assert_all_exprs_typed(&spec, context.expr_types.as_ref().unwrap());
        let expr_types = context
            .expr_types
            .expect("strict checking records expression types")
            .finish()
            .expect("successful checking typed every expression");
        Ok(CheckedDsrvSpecification::new(spec, expr_types))
    } else {
        Err(errors)
    }
}

pub(crate) fn check_expression(
    expr: Expr,
    expected: &TCType,
    environment: &Rc<StreamTypeEnvironment>,
) -> SemanticResult<CheckedExpr> {
    let mut context = TypeContext {
        environment: Cow::Borrowed(environment.as_ref()),
        local_bindings: Vec::new(),
        expr_types: Some(expr.annotations_builder()),
        owner: None,
    };
    check(expr.as_ref(), Some(expected), &mut context).map_err(|error| vec![error])?;
    #[cfg(debug_assertions)]
    assert_expr_fully_typed(expr.as_ref(), context.expr_types.as_ref().unwrap());
    let expr_types = context
        .expr_types
        .expect("expression checking records expression types")
        .finish()
        .expect("successful checking typed every expression");
    Ok(CheckedExpr::new(expr, expr_types, Rc::clone(environment)))
}

/// Check a standalone expression against an expected stream type.
pub fn type_check_expression(
    expr: &Expr,
    expected: &StreamType,
    environment: &mut super::StreamTypeEnvironment,
) -> SemanticResult<()> {
    check_expression(
        expr.clone(),
        &TCType::from_stream_type(expected),
        &Rc::new(environment.clone()),
    )
    .map(|_| ())
}

#[cfg(debug_assertions)]
fn assert_all_exprs_typed(spec: &DsrvSpecification, expr_types: &ExprTypesBuilder) {
    for expr in spec.nodes() {
        assert!(
            expr_types.get(expr).is_some(),
            "successful type checking left a reachable expression without a type"
        );
    }
}

#[cfg(debug_assertions)]
fn assert_expr_fully_typed(expr: ExprRef<'_>, expr_types: &ExprTypesBuilder) {
    for node in expr.postorder() {
        assert!(
            expr_types.get(node).is_some(),
            "successful type checking left a reachable expression without a type"
        );
    }
}

pub(crate) fn infer_expression(
    expr: ExprRef<'_>,
    expected: Option<&TCType>,
    environment: &StreamTypeEnvironment,
) -> Result<TCType, SemanticError> {
    let mut context = TypeContext {
        environment: Cow::Borrowed(environment),
        local_bindings: Vec::new(),
        expr_types: None,
        owner: None,
    };
    check(expr, expected, &mut context)
}

pub(crate) fn check_gradual_expr_types(
    spec: &DsrvSpecification,
    root_types: &BTreeMap<VarName, TCType>,
    environment: &mut StreamTypeEnvironment,
) -> SemanticResult<ExprTypes> {
    let mut context = TypeContext {
        environment: Cow::Owned(std::mem::take(environment)),
        local_bindings: Vec::new(),
        expr_types: Some(spec.exprs.annotations_builder()),
        owner: None,
    };
    for (var, expr) in spec.roots() {
        context.owner = Some(var.clone());
        let expected = root_types
            .get(var)
            .cloned()
            .unwrap_or_else(|| TCType::from_stream_type(&context.environment[var]));
        if expected == TCType::Any {
            assign_any_to_subtree(expr, context.expr_types.as_mut().unwrap());
        } else if let Err(error) = check(expr, Some(&expected), &mut context) {
            *environment = context.environment.into_owned();
            return Err(vec![error]);
        }
    }
    *environment = context.environment.into_owned();
    #[cfg(debug_assertions)]
    assert_all_exprs_typed(spec, context.expr_types.as_ref().unwrap());
    Ok(context
        .expr_types
        .expect("gradual checking records expression types")
        .finish()
        .expect("successful gradual checking typed every expression"))
}

fn assign_any_to_subtree(expr: ExprRef<'_>, expr_types: &mut ExprTypesBuilder) {
    for node in expr.postorder() {
        expr_types
            .insert(node, TCType::Any)
            .expect("expression belongs to the expression-type scope");
    }
}

fn check(
    expr: ExprRef<'_>,
    expected: Option<&TCType>,
    context: &mut TypeContext<'_>,
) -> Result<TCType, SemanticError> {
    use ExprView::*;

    let (typ, _resolved_operator) = match expr.view() {
        Val(Value::Deferred | Value::NoVal) => {
            return Err(SemanticError::UnsupportedLiteral(
                "Deferred and NoVal are runtime states, not source literals".to_owned(),
                Some(expr.span()),
            ));
        }
        Val(value) => (value_type(value, expected)?, None),
        Var(var) => (
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
        BinOp(lhs, rhs, parsed) => {
            // The result constraint is not an operand constraint: comparisons
            // produce `Bool` while comparing values of another type.
            let lhs = check(lhs, None, context)?;
            let rhs = check(rhs, None, context)?;
            resolve_binary(expr, parsed, &lhs, &rhs)?
        }
        If(cond, yes, no) => {
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
        SIndex(value, _) => (check(value, expected, context)?, None),
        Dynamic(source, result_type, scope) | Defer(source, result_type, scope) => {
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
        Update(a, b) | Default(a, b) | Latch(a, b) | Init(a, b) => {
            let a = check(a, expected, context)?;
            let b = check(b, Some(&a), context)?;
            (unify(&a, &b).ok_or_else(|| mismatch(expr, &a, &b))?, None)
        }
        IsDefined(value) | When(value) => {
            check(value, None, context)?;
            (TCType::Bool, None)
        }
        Not(value) => {
            require(
                check(value, Some(&TCType::Bool), context)?,
                &TCType::Bool,
                expr,
            )?;
            (TCType::Bool, None)
        }
        Neg(value) => {
            let typ = check(value, expected, context)?;
            if !matches!(typ, TCType::Int | TCType::Float) {
                return Err(error(
                    expr,
                    TypeErrorKind::OperatorTypeMismatch,
                    "unary minus requires a number",
                ));
            }
            (typ, None)
        }
        Lambda(params, body) => {
            let frame_start = context.local_bindings.len();
            context.local_bindings.extend(params.iter().cloned());
            let result = check(body, None, context);
            context.local_bindings.truncate(frame_start);
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
        Apply(function, args) => {
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
        Fix(function) => {
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
        Partial(function, args) => {
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
        List(items) => {
            let expected_element = expected.and_then(TCType::list_element_type);
            let element = check_elements(expr, items, expected_element, context)?;
            (TCType::list(element), None)
        }
        Tuple(items) => {
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
        LIndex(list, index) => {
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
        LAppend(list, value) => {
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
        LConcat(a, b) => {
            let a = check(a, expected, context)?;
            let b = check(b, Some(&a), context)?;
            (unify(&a, &b).ok_or_else(|| mismatch(expr, &a, &b))?, None)
        }
        LHead(list) => {
            let TCType::List(element) = check(list, None, context)? else {
                return Err(error(
                    expr,
                    TypeErrorKind::ListOperationTypeMismatch,
                    "head requires a list",
                ));
            };
            (*element, None)
        }
        LTail(list) => {
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
        LLen(list) => {
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
        LMap(function, list) => {
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
        LFilter(function, list) => {
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
        LFold(function, init, list) => {
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
        Map(fields) => {
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
        ObjectLiteral(fields) if matches!(expected, Some(TCType::Map(_))) => {
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
        Struct(fields) => {
            reject_duplicate_fields(expr, &fields)?;
            let Some(TCType::Struct(expected_fields, allow_extra)) = expected else {
                if context.expr_types.is_some() {
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
        ObjectLiteral(fields) => {
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
        MGet(map, key) => match check(map, None, context)? {
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
        SGet(value, key) => match check(value, None, context)? {
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
        MInsert(map, key, value) => {
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
        MRemove(map, _) => {
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
        MHasKey(map, _) => {
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
        Sin(value) | Cos(value) | Tan(value) => {
            require(
                check(value, Some(&TCType::Float), context)?,
                &TCType::Float,
                expr,
            )?;
            (TCType::Float, None)
        }
        Abs(value) => {
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
        MonitoredAt(_, _) => (TCType::Bool, None),
        Dist(_, _) => (TCType::Int, None),
    };
    if let Some(expected) = expected {
        require(typ.clone(), expected, expr)?;
    }
    if let Some(expr_types) = &mut context.expr_types {
        expr_types
            .insert(expr, typ.clone())
            .expect("expression belongs to the expression-type scope");
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
