//! Syntax-directed expression rules, with the source cases kept together.
//!
//! The exhaustive `type_of` match applies one source rule. Shared expectation,
//! admission and annotation mechanics belong to `checking`; supporting typing
//! policies live in `typing_support`, and source-type translation in `instantiation`.

use super::checking::{
    Expected, check, check_with_mismatch_error, infer, merge, require, type_part,
};
use super::diagnostics::{Site, annotation_needed, error};
use super::instantiation::instantiate;
use super::relations::comparable;
use super::type_contexts::TypeContext;

use crate::core::StreamType;
use crate::core::{BinaryOperator, BinaryOperatorKind, StreamTypeAscription, UnionPayload};
use crate::lang::dsrv::ast::{ExprRef, ExprView, SyntaxLiteral};
use crate::lang::dsrv::diagnostics::{SemanticError, TypeErrorKind, UnresolvedTypeKind};
use crate::lang::dsrv::source_map::SourceLocation;
use ecow::EcoVec;
use imbl::OrdMap;

use super::typing_support::collections::{check_field, list_element, type_collection_items};
use super::typing_support::literals::{
    list_element as expected_element, literal_too_large, map_value, value_type,
};
use super::typing_support::operators::{
    check_cast, is_empty_collection_of, is_negative_integer_literal, negative_exponent,
    operator_error, resolve_binary,
};
use super::typing_support::patterns::{
    check_exhaustive, infer_pattern_bindings, tags_found_in, union_named,
};

/// Apply the source rule for `expr` at an already-resolved expectation.
/// The caller owns resolving that expectation and recording the returned type.
pub(super) fn type_of(
    expr: ExprRef<'_>,
    expected: Option<&StreamType>,
    how: Expected,
    context: &mut TypeContext<'_>,
) -> Result<StreamType, SemanticError> {
    use ExprView::*;

    let typ = match expr.view() {
        // Literals, variables and tuples.
        Val(SyntaxLiteral::NoVal) => {
            return Err(SemanticError::UnsupportedLiteral(
                "Deferred and NoVal are runtime states, not source literals".to_owned(),
                Some(expr.span()),
                SourceLocation::default(),
            )
            .located(expr.origin()));
        }
        Val(SyntaxLiteral::Int(literal)) => {
            if matches!(expected, Some(StreamType::Float)) {
                if *literal as f64 as i128 != i128::from(*literal) {
                    return Err(literal_too_large(Site::of(expr), *literal));
                }
                StreamType::Float
            } else {
                StreamType::Int
            }
        }
        Val(value) => value_type(expr, value, expected)?,
        Var(var) | SIndex(var, _) => context.lookup(var).ok_or_else(|| {
            SemanticError::UndeclaredVariable(
                format!("undeclared variable {var}"),
                Some(expr.span()),
                SourceLocation::default(),
            )
            .located(expr.origin())
        })?,
        Tuple(items) => {
            let expected_items = match expected {
                Some(StreamType::Tuple(items)) => Some(items),
                _ => None,
            };
            let mut types = EcoVec::new();
            for (index, item) in items.enumerate() {
                types.push(type_part(
                    item,
                    expected_items.and_then(|items| items.get(index)),
                    how,
                    context,
                )?);
            }
            StreamType::Tuple(types)
        }

        // Operators, conversions, conditionals and stream operations.
        BinOp(lhs, rhs, operator) => {
            let literals = integer_syntax(lhs) && integer_syntax(rhs);
            let division = operator == BinaryOperator::Divide && literals;
            let result_hint = match operator.kind() {
                BinaryOperatorKind::Numeric(_) if !division => {
                    expected.filter(|typ| matches!(typ, StreamType::Int | StreamType::Float))
                }
                _ => None,
            };
            // Numeric syntax can take an immediately known operand's type. No unresolved
            // call or empty shape is postponed while another operand is checked.
            let (left, right) = if integer_syntax(lhs) && !integer_syntax(rhs) {
                let right = check(rhs, None, context)?;
                let hint = matches!(right, StreamType::Int | StreamType::Float)
                    .then_some(&right)
                    .or(result_hint);
                (infer(lhs, hint, context)?, right)
            } else {
                let left = infer(lhs, result_hint, context)?;
                let hint = matches!(left, StreamType::Int | StreamType::Float).then_some(&left);
                let right = infer(
                    rhs,
                    hint.or_else(|| {
                        matches!(operator.kind(), BinaryOperatorKind::Equality(_)).then_some(&left)
                    }),
                    context,
                )?;
                (left, right)
            };
            if !comparable(&left, &right) {
                return Err(operator_error(Site::of(expr), operator, &left, &right));
            }
            if operator == BinaryOperator::Power
                && left == StreamType::Int
                && right == StreamType::Int
                && is_negative_integer_literal(rhs)
            {
                return Err(negative_exponent(Site::of(rhs)));
            }
            resolve_binary(Site::of(expr), operator, &left, &right)?
        }
        Ascribe(value, target) => {
            let target_type = target.clone();
            check_with_mismatch_error(value, Some(&target_type), context, |rejected| {
                let site = if expr.origin().definition.is_some()
                    && matches!(rejected.why, super::relations::Mismatch::Types)
                {
                    expr
                } else {
                    value
                };
                super::diagnostics::mismatch(site, rejected)
            })?;
            target_type
        }
        // Nothing else says what an empty collection holds, so this is the
        // collection's type rather than a conversion of one, and not
        // redundant.
        Cast(value, target) if is_empty_collection_of(value, target) => {
            let target_type = target.clone();
            check(value, Some(&target_type), context)?;
            target_type
        }
        Cast(value, target) => {
            let source = check(value, None, context)?;
            check_cast(Site::of(expr), &source, target, context)?;
            target.clone()
        }
        If(cond, yes, no) => {
            check(cond, Some(&StreamType::Bool), context)?;
            let yes_type = type_part(yes, expected, how, context)?;
            let no_type = type_part(no, expected, how, context)?;
            merge(expr, &[yes_type, no_type], expected)?
        }
        Update(a, b) | Default(a, b) | Init(a, b) => {
            let a_type = type_part(a, expected, how, context)?;
            let b_type = type_part(b, expected, how, context)?;
            merge(expr, &[a_type, b_type], expected)?
        }
        Latch(value, trigger) => {
            let value = type_part(value, expected, how, context)?;
            check(trigger, None, context)?;
            value
        }
        IsDefined(value) | When(value) => {
            check(value, None, context)?;
            StreamType::Bool
        }
        Not(value) => {
            check(value, Some(&StreamType::Bool), context)?;
            StreamType::Bool
        }
        Neg(value) | Abs(value) => {
            let typ = infer(value, expected, context)?;
            let numeric = matches!(typ, StreamType::Int | StreamType::Float);
            if !numeric {
                let operation = match expr.view() {
                    Neg(_) => "unary minus",
                    _ => "abs",
                };
                return Err(error(
                    expr,
                    TypeErrorKind::OperatorTypeMismatch,
                    format!("{operation} requires a number"),
                ));
            }
            typ
        }
        Trunc(value) | Floor(value) | Ceil(value) | Round(value) => {
            check(value, Some(&StreamType::Float), context)?;
            StreamType::Int
        }
        Sin(value) | Cos(value) | Tan(value) => {
            check(value, Some(&StreamType::Float), context)?;
            StreamType::Float
        }

        // Lambdas, application, fixed points and partial application.
        Lambda(..) => type_lambda(expr, expected, None, context)?,
        Apply(function, args) if function.generic_function_signature().is_some() => {
            let signature = function.generic_function_signature().expect("guard");
            if signature.parameters.len() != args.len() {
                return Err(error(
                    expr,
                    TypeErrorKind::FunctionArityMismatch,
                    "function argument count differs",
                ));
            }
            let definitions = function
                .source_context()
                .expect("expanded generic def source context");
            let mut instance = OrdMap::new();
            if let Some(required) = expected {
                super::instantiation::match_concrete(
                    definitions,
                    &signature.result,
                    required,
                    &signature.type_parameters,
                    &mut instance,
                )
                .map_err(|message| annotation_needed(expr, message))?;
            }
            let mut params = EcoVec::new();
            for (arg, parameter) in args.clone().zip(&signature.parameters) {
                let wanted = instantiate(definitions, parameter, &instance, &mut Vec::new());
                let actual = if wanted.is_none() && matches!(arg.view(), Lambda(..)) {
                    match super::instantiation::callback_context(
                        definitions,
                        parameter,
                        &instance,
                        &mut Vec::new(),
                    ) {
                        Some((inputs, result)) => {
                            check_callback(arg, &inputs, result.as_ref(), context)?;
                            context.expr_types[&arg.id()].clone()
                        }
                        None => check(arg, None, context)?,
                    }
                } else {
                    check_with_mismatch_error(arg, wanted.as_ref(), context, |rejected| {
                        super::diagnostics::mismatch(expr, rejected)
                    })?
                };
                super::instantiation::match_concrete(
                    definitions,
                    parameter,
                    &actual,
                    &signature.type_parameters,
                    &mut instance,
                )
                .map_err(|message| annotation_needed(expr, message))?;
                let target = instantiate(definitions, parameter, &instance, &mut Vec::new())
                    .ok_or_else(|| {
                        annotation_needed(
                            expr,
                            "generic parameter needs a concrete type annotation",
                        )
                    })?;
                require(&actual, &target, expr)?;
                params.push(target);
            }
            let result = instantiate(definitions, &signature.result, &instance, &mut Vec::new())
                .ok_or_else(|| {
                    annotation_needed(expr, "generic result needs a concrete type annotation")
                })?;
            let wanted = StreamType::Function(params, Box::new(result.clone()));
            let actual = type_lambda(function, Some(&wanted), Some(expr), context)?;
            require(&actual, &wanted, expr)?;
            context.record(function, &actual);
            result
        }
        // `(\x -> …)(a)`: the arguments are checked first, each against its
        // parameter's annotation if it has one, so that the lambda's
        // parameters have their types before its body is checked.
        Apply(function, args) if matches!(function.view(), Lambda(..)) => {
            let Lambda(params, body) = function.view() else {
                unreachable!("matched a lambda");
            };
            if params.len() != args.len() {
                return Err(error(
                    expr,
                    TypeErrorKind::FunctionArityMismatch,
                    "function argument count differs",
                ));
            }
            let mut arguments = EcoVec::new();
            for (arg, (_, ascription)) in args.zip(params.iter()) {
                arguments.push(match ascription {
                    StreamTypeAscription::Ascribed(typ) => {
                        let annotation = typ.clone();
                        check_with_mismatch_error(arg, Some(&annotation), context, |rejected| {
                            let site = if expr.origin().definition.is_some() {
                                expr
                            } else {
                                arg
                            };
                            super::diagnostics::mismatch(site, rejected)
                        })?;
                        annotation
                    }
                    StreamTypeAscription::Unascribed => check(arg, None, context)?,
                });
            }
            let saved = context.local_bindings.clone();
            context.local_bindings.extend(
                params
                    .iter()
                    .zip(&arguments)
                    .map(|((name, _), typ)| (name.clone(), typ.clone())),
            );
            let result = check_with_mismatch_error(body, expected, context, |rejected| {
                let site = if expr.origin().definition.is_some() {
                    expr
                } else {
                    body
                };
                super::diagnostics::mismatch(site, rejected)
            });
            context.local_bindings = saved;
            let result = result?;
            context.record(
                function,
                &StreamType::Function(arguments, Box::new(result.clone())),
            );
            result
        }
        Apply(function, args) => {
            let actual = check(function, None, context)?;
            let StreamType::Function(params, result) = actual else {
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
            for (arg, typ) in args.zip(&params) {
                check(arg, Some(typ), context)?;
            }
            *result
        }
        Fix(function) => {
            let function = check(function, None, context)?;
            let (params, result) = match function {
                StreamType::Function(params, result) => (params, result),
                _ => {
                    return Err(error(
                        expr,
                        TypeErrorKind::ExpectedFunction,
                        "fix requires a function",
                    ));
                }
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
                StreamType::Function(params[1..].to_vec().into(), result)
            };
            // The fixed point is a value that goes where `self` is needed.
            require(&fixed, &params[0], expr)?;
            fixed
        }
        Partial(function, args) => {
            let function = check(function, None, context)?;
            let (params, result) = match function {
                StreamType::Function(params, result) => (params, result),
                _ => {
                    return Err(error(
                        expr,
                        TypeErrorKind::ExpectedFunction,
                        "partial requires a function",
                    ));
                }
            };
            if args.len() > params.len() {
                return Err(error(
                    expr,
                    TypeErrorKind::FunctionArityMismatch,
                    "too many partial arguments",
                ));
            }
            for (arg, expected) in args.clone().zip(&params) {
                check(arg, Some(expected), context)?;
            }
            StreamType::Function(params[args.len()..].iter().cloned().collect(), result)
        }

        // List construction and operations.
        List(items) => {
            let expected_element = expected.and_then(expected_element);
            let element = type_collection_items(expr, items, expected_element, how, context)?;
            StreamType::List(Box::new(element))
        }
        LIndex(list, index) => {
            check(index, Some(&StreamType::Int), context)?;
            // A dynamic value is indexed at runtime.
            let list = check(list, None, context)?;
            list_element(
                expr,
                &list,
                TypeErrorKind::ListIndexTypeMismatch,
                "indexing requires a list",
            )?
            .clone()
        }
        LAppend(list, value) => {
            let typ = infer(list, expected, context)?;
            let element = list_element(
                expr,
                &typ,
                TypeErrorKind::ListOperationTypeMismatch,
                "append requires a list",
            )?;
            check(value, Some(element), context)?;
            typ
        }
        LConcat(a, b) => {
            let a_type = type_part(a, expected, how, context)?;
            let b_type = type_part(b, expected, how, context)?;
            for typ in [&a_type, &b_type] {
                list_element(
                    expr,
                    typ,
                    TypeErrorKind::ListOperationTypeMismatch,
                    "concat requires lists",
                )?;
            }
            merge(expr, &[a_type, b_type], expected)?
        }
        LHead(list) => {
            let list = check(list, None, context)?;
            list_element(
                expr,
                &list,
                TypeErrorKind::ListOperationTypeMismatch,
                "head requires a list",
            )?
            .clone()
        }
        LTail(list) => {
            let typ = infer(list, expected, context)?;
            list_element(
                expr,
                &typ,
                TypeErrorKind::ListOperationTypeMismatch,
                "tail requires a list",
            )?;
            typ
        }
        LLen(list) => {
            let typ = check(list, None, context)?;
            list_element(
                expr,
                &typ,
                TypeErrorKind::ListOperationTypeMismatch,
                "length requires a list",
            )?;
            StreamType::Int
        }
        LMap(function, list) => {
            let list_context = known_callback_input(function, 0, context)
                .map(|input| StreamType::List(Box::new(input)));
            let list_type = infer(list, list_context.as_ref(), context)?;
            let input = list_element(
                expr,
                &list_type,
                TypeErrorKind::ListOperationTypeMismatch,
                "map requires a list",
            )?;
            let result = check_callback(
                function,
                &[input.clone()],
                expected.and_then(expected_element),
                context,
            )?;
            StreamType::List(Box::new(result))
        }
        LFilter(function, list) => {
            let list_context = known_callback_input(function, 0, context)
                .map(|input| StreamType::List(Box::new(input)));
            let typ = infer(list, expected.or(list_context.as_ref()), context)?;
            let input = list_element(
                expr,
                &typ,
                TypeErrorKind::ListOperationTypeMismatch,
                "filter requires a list",
            )?;
            check_callback(function, &[input.clone()], Some(&StreamType::Bool), context)?;
            typ
        }
        LFold(function, init, list) => {
            let annotation = known_callback_input(function, 0, context);
            let accumulator = expected.or(annotation.as_ref());
            let initial = check(init, accumulator, context)?;
            let list_context = known_callback_input(function, 1, context)
                .map(|input| StreamType::List(Box::new(input)));
            let list_type = infer(list, list_context.as_ref(), context)?;
            let element = list_element(
                expr,
                &list_type,
                TypeErrorKind::ListOperationTypeMismatch,
                "fold requires a list",
            )?;
            let common = accumulator.unwrap_or(&initial);
            let result = check_callback(
                function,
                &[common.clone(), element.clone()],
                Some(common),
                context,
            )?;
            merge(expr, &[initial, result], accumulator)?
        }

        // Maps, structs and object literals.
        Map(fields) | Struct(fields) | ObjectLiteral(fields) => {
            // A literal where a map is expected is a map.
            if matches!(expr.view(), Map(_))
                || matches!(expr.view(), ObjectLiteral(_))
                    && matches!(expected, Some(StreamType::Map(_)))
            {
                let expected_value = expected.and_then(map_value);
                StreamType::Map(Box::new(type_collection_items(
                    expr,
                    fields.iter().map(|(_, value)| value),
                    expected_value,
                    how,
                    context,
                )?))
            } else {
                // Otherwise it is a closed struct of the fields written, each
                // hinted by what is expected of it, and where it meets a struct
                // type is where having the fields that type lists is decided.
                let mut types = EcoVec::new();
                for (name, value) in fields.iter() {
                    let hint = match expected {
                        Some(StreamType::Struct(listed, _)) => listed
                            .iter()
                            .find(|(field, _)| field == name)
                            .map(|(_, typ)| typ),
                        _ => None,
                    };
                    let typ = match how {
                        Expected::Required => check_field(expr, name, value, hint, context)?,
                        Expected::Hint => infer(value, hint, context)?,
                    };
                    types.push((name.clone(), typ));
                }
                StreamType::Struct(types, false)
            }
        }
        MGet(map, key) => {
            let map = check(map, None, context)?;
            match map {
                StreamType::Map(value) => *value,
                StreamType::Struct(fields, _) => fields
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
                _ => {
                    return Err(error(
                        expr,
                        TypeErrorKind::MapOperationTypeMismatch,
                        format!("get requires a map or struct, got {}", &map),
                    ));
                }
            }
        }
        SGet(value, key) => match check(value, None, context)? {
            StreamType::Struct(fields, _) => fields
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
            StreamType::Tuple(items) => {
                let Ok(index) = key.parse::<usize>() else {
                    return Err(error(
                        expr,
                        TypeErrorKind::StructFieldAccess,
                        format!("tuple has no field {key}"),
                    ));
                };
                items.get(index).cloned().ok_or_else(|| {
                    error(
                        expr,
                        TypeErrorKind::StructFieldAccess,
                        "tuple index out of bounds",
                    )
                })?
            }
            _ => {
                return Err(error(
                    expr,
                    TypeErrorKind::StructExpected,
                    "Dot field access requires a Struct",
                ));
            }
        },
        MInsert(map, key, value) => {
            let map_type = infer(map, expected, context)?;
            let element = match &map_type {
                StreamType::Map(element) => element.as_ref(),
                StreamType::Struct(fields, _) => fields
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
            check(value, Some(element), context)?;
            map_type
        }
        MRemove(map, _) => {
            let typ = infer(map, expected, context)?;
            if !matches!(&typ, StreamType::Map(_)) {
                return Err(error(
                    expr,
                    TypeErrorKind::MapOperationTypeMismatch,
                    "remove requires a map",
                ));
            }
            typ
        }
        MHasKey(map, key) => {
            let typ = check(map, None, context)?;
            match &typ {
                StreamType::Map(_) => {}
                // A struct type lists the fields that can be read, as for
                // `get`, so the others are not asked about either.
                StreamType::Struct(fields, _) => {
                    if !fields.iter().any(|(name, _)| name == key) {
                        return Err(error(
                            expr,
                            TypeErrorKind::StructFieldAccess,
                            "Struct has no field with this name",
                        ));
                    }
                }
                _ => {
                    return Err(error(
                        expr,
                        TypeErrorKind::MapOperationTypeMismatch,
                        "has-key requires a map",
                    ));
                }
            }
            StreamType::Bool
        }

        // Pattern matching and union constructors.
        Match(scrutinee, arms, shape) => {
            let scrutinee_type = check(scrutinee, None, context)?;
            let mut children = arms.into_iter();
            let mut arm_types = Vec::new();
            for arm in shape.iter() {
                let bindings = infer_pattern_bindings(&arm.pattern, &scrutinee_type, expr)?;
                let frame = context.local_bindings.clone();
                context.local_bindings.extend(bindings);
                let checked = (|context: &mut TypeContext<'_>| {
                    if arm.guarded {
                        let guard = children.next().expect("match guard exists");
                        check(guard, Some(&StreamType::Bool), context)?;
                    }
                    let body = children.next().expect("match body exists");
                    type_part(body, expected, how, context)
                })(context);
                context.local_bindings = frame;
                arm_types.push(checked?);
            }
            if arm_types.is_empty() {
                return Err(error(
                    expr,
                    TypeErrorKind::MatchWithoutArms,
                    "a `match` decides between arms, and this one has none".to_owned(),
                ));
            }
            let result = merge(expr, &arm_types, expected)?;
            check_exhaustive(expr, shape, &scrutinee_type)?;
            result
        }
        Matches(scrutinee, guard, pattern) => {
            let scrutinee_type = check(scrutinee, None, context)?;
            let bindings = infer_pattern_bindings(pattern, &scrutinee_type, expr)?;
            if let Some(guard) = guard.into_iter().next() {
                let frame = context.local_bindings.clone();
                context.local_bindings.extend(bindings);
                let checked = check(guard, Some(&StreamType::Bool), context);
                context.local_bindings = frame;
                checked?;
            }
            StreamType::Bool
        }
        // A tag names an alternative, not a union, so which union it belongs
        // to comes from the qualifier the writer gave or the type the
        // expression is expected to have.
        Constructor(payload, tag, qualifier) => {
            let payload = payload.into_iter().next();
            let qualifier = qualifier.as_ref();
            let union = match qualifier {
                Some(qualifier) => union_named(expr, qualifier)?,
                None => match expected {
                    Some(StreamType::Union(schema)) => schema.clone(),
                    // An imported tag names its union outright, but only where
                    // the expected type has not already said which one.
                    None if expr
                        .source_context()
                        .and_then(|context| context.constructor_union(tag))
                        .is_some() =>
                    {
                        let path = expr
                            .source_context()
                            .and_then(|context| context.constructor_union(tag))
                            .expect("just found")
                            .clone();
                        union_named(expr, &path)?
                    }
                    None => {
                        return Err(SemanticError::unresolved_type_at(
                            UnresolvedTypeKind::ConstructorUnion,
                            format!(
                                "`{tag}` needs a union to belong to: nothing here says which one{}",
                                tags_found_in(expr, tag)
                            ),
                            expr.span(),
                        )
                        .located(expr.origin()));
                    }
                    Some(other) => {
                        return Err(error(
                            expr,
                            TypeErrorKind::ExpectedUnion,
                            format!("`{tag}` builds a union, but {} is expected here", other),
                        ));
                    }
                },
            };
            let Some((_, alternative)) = union.alternative(tag) else {
                let known = union
                    .alternatives()
                    .iter()
                    .map(|alternative| alternative.tag().as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(error(
                    expr,
                    TypeErrorKind::UnknownUnionTag,
                    format!(
                        "`{tag}` is not an alternative of {}, which has {known}{}",
                        &StreamType::Union(union.clone()),
                        tags_found_in(expr, tag)
                    ),
                ));
            };
            match (alternative.payload(), payload) {
                (UnionPayload::Nullary, None) => {}
                (UnionPayload::Of(declared), Some(payload)) => {
                    // The payload is checked against the alternative's type, which
                    // is also what resolves a bare constructor written inside it. A
                    // disagreement is reported as this constructor's, so the message
                    // names the tag rather than only the types.
                    check_with_mismatch_error(payload, Some(declared), context, |rejected| {
                        error(
                            payload,
                            TypeErrorKind::ConstructorPayloadTypeMismatch,
                            format!(
                                "`{tag}` carries {}, got {}{}",
                                rejected.expected,
                                rejected.actual,
                                rejected.detail()
                            ),
                        )
                    })?;
                }
                (UnionPayload::Nullary, Some(_)) => {
                    return Err(error(
                        expr,
                        TypeErrorKind::ConstructorPayloadArity,
                        format!("`{tag}` carries no payload"),
                    ));
                }
                (UnionPayload::Of(declared), None) => {
                    return Err(error(
                        expr,
                        TypeErrorKind::ConstructorPayloadArity,
                        format!("`{tag}` carries {}, which is missing", declared),
                    ));
                }
            }
            StreamType::Union(union)
        }

        // Runtime expression sources and monitor placement.
        Dynamic(source, result_type, _) | Defer(source, result_type, _) => {
            let ascribed = match result_type {
                StreamTypeAscription::Ascribed(typ) => Some(typ.clone()),
                StreamTypeAscription::Unascribed => None,
            };
            let source_type = check(source, None, context)?;
            let typ = match source_type {
                StreamType::Expr(inner) => {
                    let inner = *inner;
                    if let Some(ascribed) = ascribed {
                        require(&inner, &ascribed, source)?;
                        ascribed
                    } else {
                        inner
                    }
                }
                StreamType::Str if ascribed.as_ref().or(expected).is_some() => ascribed
                    .or_else(|| expected.cloned())
                    .expect("known runtime-expression result"),
                actual => {
                    return Err(error(
                        source,
                        TypeErrorKind::ExpectedExpressionSource,
                        format!(
                            "runtime expression source must have type Expr<T>, or Str with a known result type; got {}",
                            &actual
                        ),
                    ));
                }
            };
            typ
        }
        MonitoredAt(_, _) => StreamType::Bool,
        Dist(_, _) => StreamType::Int,
    };
    Ok(typ)
}

/// Immediate numeric literal context includes an explicitly negated integer.
fn integer_syntax(expr: ExprRef<'_>) -> bool {
    match expr.view() {
        ExprView::Val(SyntaxLiteral::Int(_)) => true,
        ExprView::Neg(inner) => integer_syntax(inner),
        _ => false,
    }
}
/// A callback written here gets known input types; its body supplies the result.
fn check_callback(
    function: ExprRef<'_>,
    inputs: &[StreamType],
    result: Option<&StreamType>,
    context: &mut TypeContext<'_>,
) -> Result<StreamType, SemanticError> {
    if let ExprView::Lambda(params, body) = function.view() {
        if params.len() != inputs.len() {
            return Err(error(
                function,
                TypeErrorKind::FunctionArityMismatch,
                "callback argument count differs",
            ));
        }
        let saved = context.local_bindings.clone();
        let mut parameter_types = EcoVec::new();
        for ((name, annotation), input) in params.iter().zip(inputs) {
            let typ = match annotation {
                StreamTypeAscription::Ascribed(typ) => {
                    require(input, typ, function)?;
                    typ.clone()
                }
                StreamTypeAscription::Unascribed => input.clone(),
            };
            parameter_types.push(typ.clone());
            context.local_bindings.insert(name.clone(), typ);
        }
        let checked = check(body, result, context);
        context.local_bindings = saved;
        let checked = checked?;
        context.record(
            function,
            &StreamType::Function(parameter_types, Box::new(checked.clone())),
        );
        return Ok(checked);
    }
    let actual = check(function, None, context)?;
    let StreamType::Function(params, output) = actual else {
        return Err(error(
            function,
            TypeErrorKind::ExpectedFunction,
            "collection callback requires a function",
        ));
    };
    if params.len() != inputs.len() {
        return Err(error(
            function,
            TypeErrorKind::FunctionArityMismatch,
            "callback argument count differs",
        ));
    }
    for (input, param) in inputs.iter().zip(&params) {
        require(input, param, function)?;
    }
    if let Some(result) = result {
        require(&output, result, function)?;
    }
    Ok(*output)
}

/// Read explicit input information without checking a callback body or guessing
/// a list element from the fold accumulator.
fn known_callback_input(
    function: ExprRef<'_>,
    index: usize,
    context: &TypeContext<'_>,
) -> Option<StreamType> {
    match function.view() {
        ExprView::Lambda(params, _) => match params.get(index) {
            Some((_, StreamTypeAscription::Ascribed(typ))) => Some(typ.clone()),
            _ => None,
        },
        ExprView::Var(name) | ExprView::SIndex(name, _) => match context.lookup(name)? {
            StreamType::Function(inputs, _) => inputs.get(index).cloned(),
            _ => None,
        },
        ExprView::Ascribe(_, StreamType::Function(inputs, _)) => inputs.get(index).cloned(),
        _ => None,
    }
}

/// A declared result contract uses the call's retained declaration origin for
/// direct admission failures; errors inside the body keep their own locations.
fn type_lambda<'arena>(
    expr: ExprRef<'arena>,
    expected: Option<&StreamType>,
    contract: Option<ExprRef<'arena>>,
    context: &mut TypeContext<'_>,
) -> Result<StreamType, SemanticError> {
    let ExprView::Lambda(params, body) = expr.view() else {
        unreachable!("lambda typing requires a lambda")
    };
    let wanted = match expected {
        Some(StreamType::Function(inputs, result)) if inputs.len() == params.len() => {
            Some((inputs, result.as_ref()))
        }
        _ => None,
    };
    let mut parameters = Vec::new();
    for (index, (name, annotation)) in params.iter().enumerate() {
        let typ = match annotation {
            StreamTypeAscription::Ascribed(typ) => typ.clone(),
            StreamTypeAscription::Unascribed => wanted
                .map(|(inputs, _)| inputs[index].clone())
                .ok_or_else(|| {
                    annotation_needed(
                        expr,
                        format!("lambda parameter {name} needs a type annotation"),
                    )
                })?,
        };
        parameters.push((name.clone(), typ));
    }
    let saved = context.local_bindings.clone();
    context.local_bindings.extend(parameters.iter().cloned());
    let result = check_with_mismatch_error(
        body,
        wanted.map(|(_, result)| result),
        context,
        |rejected| super::diagnostics::mismatch(contract.unwrap_or(body), rejected),
    );
    context.local_bindings = saved;
    Ok(StreamType::Function(
        parameters.into_iter().map(|(_, typ)| typ).collect(),
        Box::new(result?),
    ))
}
