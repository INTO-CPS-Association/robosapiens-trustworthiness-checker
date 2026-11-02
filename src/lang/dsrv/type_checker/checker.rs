//! Expression checking for the DSRV AST.
//!
//! Checking infers one type per [`ExprId`](crate::lang::dsrv::ast::ExprId).
//! Postorder validation verifies that every reachable expression has a type
//! before the immutable results are attached to checked expression cursors.

use contiguous_tree::TreeCursorExt;
use ecow::{EcoString, EcoVec};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use super::warnings::WarningCollector;
use super::{RuntimeChecks, StreamTypeEnvironment, TCType};
use crate::VarName;
use crate::core::{
    BinaryOperator, BinaryOperatorKind, ClosedUnion, StreamType, StreamTypeAscription, UnionPayload,
};
use crate::lang::dsrv::ast::{
    AstShared, CheckedDsrvSpecification, CheckedExpr, DsrvSpecification, Expr, ExprArena,
    ExprFieldRefs, ExprId, ExprRef, ExprRefs, ExprTypes, ExprTypesBuilder, ExprView,
    ReconfigurableExprScope, SyntaxLiteral,
};
use crate::lang::dsrv::diagnostics::{
    SemanticAnalysisReport, SemanticError, SemanticResult, SemanticWarning, SemanticWarningKind,
    TypeErrorKind, UnresolvedTypeKind,
};
use crate::lang::dsrv::path::TypePath;
use crate::lang::dsrv::patterns::{MatchArm, MatchPattern, PatternKind};
use crate::lang::dsrv::source::{SourceType, TypeName, match_source_type, substitute_source};
use crate::lang::dsrv::source_map::{SourceArchive, SourceLocation};

struct TypeContext<'types> {
    environment: Cow<'types, StreamTypeEnvironment>,
    local_bindings: Vec<(VarName, StreamType)>,
    expr_types: Option<ExprTypesBuilder>,
    /// Each node whose `Any` type was accepted where a value of a concrete
    /// type is needed, with that type, in the order they were accepted; a
    /// node accepted again takes its latest type. Recorded only alongside
    /// `expr_types`, in the pass whose types are kept.
    runtime_checks: Vec<(ExprId, StreamType)>,
    owner: Option<VarName>,
    strict_runtime_sources: bool,
    /// Gradual checking gives a lambda parameter with neither annotation nor
    /// hint the dynamic type; strict checking reports it.
    dynamic_unhinted_parameters: bool,
    /// Parameter types for the lambda about to be checked, from where it is
    /// used (a list callback's element type, or an immediate call's
    /// arguments). Taken by that lambda; callers clear it afterwards.
    parameter_hints: Option<EcoVec<TCType>>,
    /// Present only in a driver's authoritative phase, which alone may emit
    /// warnings.
    warnings: Option<&'types mut WarningCollector>,
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

pub(super) fn check_specification(
    spec: DsrvSpecification,
) -> SemanticAnalysisReport<CheckedDsrvSpecification> {
    let mut warnings = WarningCollector::default();
    let sources = AstShared::clone(spec.sources());
    let result = check_specification_with(spec, &mut warnings);
    warnings.report(result, Some(&sources))
}

fn check_specification_with(
    spec: DsrvSpecification,
    warnings: &mut WarningCollector,
) -> SemanticResult<CheckedDsrvSpecification> {
    super::validation::validate_specification(&spec)?;
    let mut errors = Vec::new();
    let mut context = TypeContext {
        environment: Cow::Owned(spec.type_annotations().clone()),
        local_bindings: Vec::new(),
        expr_types: Some(spec.exprs.annotations_builder()),
        runtime_checks: Vec::new(),
        owner: None,
        strict_runtime_sources: true,
        dynamic_unhinted_parameters: false,
        parameter_hints: None,
        warnings: Some(warnings),
    };
    for (var, expr) in spec.roots() {
        context.owner = Some(var.clone());
        let Some(expected) = spec.type_annotations.get(var) else {
            errors.push(SemanticError::MissingTypeAnnotation(
                format!("Variable {var} is missing a type annotation"),
                None,
                SourceLocation::default(),
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
        let runtime_checks = finish_runtime_checks(
            context.runtime_checks,
            spec.exprs.sparse_annotations_builder(),
            spec.nodes(),
        );
        Ok(CheckedDsrvSpecification::new(
            spec,
            expr_types,
            crate::lang::dsrv::TypeCheckMode::Strict,
        )
        .with_runtime_checks(runtime_checks))
    } else {
        Err(errors)
    }
}

/// Check a standalone expression. Its findings are located in `sources`,
/// the archive its IDs belong to, or are unlocated without one.
pub(crate) fn check_expression(
    expr: Expr,
    expected: &TCType,
    environment: &AstShared<StreamTypeEnvironment>,
    sources: Option<&SourceArchive>,
) -> SemanticAnalysisReport<CheckedExpr> {
    let mut warnings = WarningCollector::default();
    let result = check_expression_with(expr, expected, environment, &mut warnings);
    warnings.report(result, sources)
}

fn check_expression_with(
    expr: Expr,
    expected: &TCType,
    environment: &AstShared<StreamTypeEnvironment>,
    warnings: &mut WarningCollector,
) -> SemanticResult<CheckedExpr> {
    let mut context = TypeContext {
        environment: Cow::Borrowed(environment.as_ref()),
        local_bindings: Vec::new(),
        expr_types: Some(expr.annotations_builder()),
        runtime_checks: Vec::new(),
        owner: None,
        strict_runtime_sources: true,
        dynamic_unhinted_parameters: false,
        parameter_hints: None,
        warnings: Some(warnings),
    };
    check(expr.as_ref(), Some(expected), &mut context).map_err(|error| vec![error])?;
    #[cfg(debug_assertions)]
    assert_expr_fully_typed(expr.as_ref(), context.expr_types.as_ref().unwrap());
    let expr_types = context
        .expr_types
        .expect("expression checking records expression types")
        .finish()
        .expect("successful checking typed every expression");
    Ok(CheckedExpr::new(
        expr,
        expr_types,
        AstShared::clone(environment),
    ))
}

/// Check a standalone expression against an expected stream type in
/// `environment`.
pub fn type_check_expression(
    expr: &Expr,
    expected: &StreamType,
    environment: &StreamTypeEnvironment,
) -> SemanticAnalysisReport<CheckedExpr> {
    check_expression(
        expr.clone(),
        &TCType::from_stream_type(expected),
        &AstShared::new(environment.clone()),
        None,
    )
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
    warnings: &mut WarningCollector,
) -> Result<TCType, SemanticError> {
    let mut context = TypeContext {
        environment: Cow::Borrowed(environment),
        local_bindings: Vec::new(),
        expr_types: None,
        runtime_checks: Vec::new(),
        owner: None,
        strict_runtime_sources: false,
        dynamic_unhinted_parameters: true,
        parameter_hints: None,
        warnings: Some(warnings),
    };
    check(expr, expected, &mut context)
}

/// The authoritative gradual pass: each node's type, and the run-time checks
/// of the `Any` values it accepted at a concrete type.
pub(crate) fn check_gradual_expr_types(
    spec: &DsrvSpecification,
    root_types: &BTreeMap<VarName, TCType>,
    environment: &mut StreamTypeEnvironment,
    warnings: &mut WarningCollector,
) -> SemanticResult<(ExprTypes, RuntimeChecks)> {
    let mut context = TypeContext {
        environment: Cow::Owned(std::mem::take(environment)),
        local_bindings: Vec::new(),
        expr_types: Some(spec.exprs.annotations_builder()),
        runtime_checks: Vec::new(),
        owner: None,
        strict_runtime_sources: false,
        dynamic_unhinted_parameters: true,
        parameter_hints: None,
        warnings: Some(warnings),
    };
    for (var, expr) in spec.roots() {
        context.owner = Some(var.clone());
        let expected = root_types
            .get(var)
            .cloned()
            .unwrap_or_else(|| TCType::from_stream_type(&context.environment[var]));
        if expected == TCType::Any {
            assign_any_to_subtree(expr, context.expr_types.as_mut().unwrap());
            // A root inferred as `Any` is still used at its stream's declared
            // type.
            let declared = TCType::from_stream_type(&context.environment[var]);
            accept_at_run_time(expr, &TCType::Any, &declared, &mut context);
        } else if let Err(error) = check(expr, Some(&expected), &mut context) {
            *environment = context.environment.into_owned();
            return Err(vec![error]);
        }
    }
    *environment = context.environment.into_owned();
    #[cfg(debug_assertions)]
    assert_all_exprs_typed(spec, context.expr_types.as_ref().unwrap());
    let expr_types = context
        .expr_types
        .expect("gradual checking records expression types")
        .finish()
        .expect("successful gradual checking typed every expression");
    let runtime_checks = finish_runtime_checks(
        context.runtime_checks,
        spec.exprs.sparse_annotations_builder(),
        spec.nodes(),
    );
    Ok((expr_types, runtime_checks))
}

fn assign_any_to_subtree(expr: ExprRef<'_>, expr_types: &mut ExprTypesBuilder) {
    for node in expr.postorder() {
        expr_types
            .insert(node, TCType::Any)
            .expect("expression belongs to the expression-type scope");
    }
}

/// Check a list callback. A lambda written in place gets its parameter types
/// from `hints`; any other function expression is checked as it stands.
fn check_callback(
    function: ExprRef<'_>,
    hints: EcoVec<TCType>,
    context: &mut TypeContext<'_>,
) -> Result<TCType, SemanticError> {
    if matches!(function.view(), ExprView::Lambda(..)) {
        context.parameter_hints = Some(hints);
    }
    let checked = check(function, None, context);
    context.parameter_hints = None;
    checked
}

fn check(
    expr: ExprRef<'_>,
    expected: Option<&TCType>,
    context: &mut TypeContext<'_>,
) -> Result<TCType, SemanticError> {
    use ExprView::*;

    let (typ, _resolved_operator) = match expr.view() {
        Val(SyntaxLiteral::NoVal) => {
            return Err(SemanticError::UnsupportedLiteral(
                "Deferred and NoVal are runtime states, not source literals".to_owned(),
                Some(expr.span()),
                SourceLocation::default(),
            )
            .located(expr.origin()));
        }
        Val(value) => {
            let typ = value_type(value, expected)?;
            if let (SyntaxLiteral::Int(literal), TCType::Float) = (value, &typ)
                && *literal as f64 as i128 != i128::from(*literal)
            {
                return Err(error(
                    expr,
                    TypeErrorKind::OperatorTypeMismatch,
                    format!(
                        "integer literal {literal} cannot be represented exactly as a Float; \
                         write it as a Float literal"
                    ),
                ));
            }
            (typ, None)
        }
        Match(scrutinee, arms, shape) => {
            let scrutinee_type = check(scrutinee, None, context)?;
            let children: Vec<ExprRef<'_>> = arms.into_iter().collect();
            let mut place = 0;
            let mut result: Option<TCType> = None;
            let mut arm_types = Vec::new();
            for arm in shape.iter() {
                let bindings = bind_pattern(&arm.pattern, &scrutinee_type, expr)?;
                let frame = context.local_bindings.len();
                context.local_bindings.extend(bindings);
                let checked = (|context: &mut TypeContext<'_>| {
                    if arm.guarded {
                        let guard = children[place];
                        place += 1;
                        let guard_type = check(guard, Some(&TCType::Bool), context)?;
                        require(guard_type, &TCType::Bool, guard)?;
                    }
                    let body = children[place];
                    place += 1;
                    check(body, expected, context)
                })(context);
                context.local_bindings.truncate(frame);
                let body_type = checked?;
                arm_types.push(body_type.clone());
                result = Some(match result {
                    None => body_type,
                    Some(previous) => join(&previous, &body_type),
                });
            }
            warn_if_heterogeneous(expr, &arm_types, expected, context);
            let Some(result) = result else {
                return Err(error(
                    expr,
                    TypeErrorKind::MatchWithoutArms,
                    "a `match` decides between arms, and this one has none".to_owned(),
                ));
            };
            check_exhaustive(expr, shape, &scrutinee_type)?;
            (result, None)
        }
        Matches(scrutinee, guard, pattern) => {
            let scrutinee_type = check(scrutinee, None, context)?;
            let bindings = bind_pattern(pattern, &scrutinee_type, expr)?;
            if let Some(guard) = guard.into_iter().next() {
                let frame = context.local_bindings.len();
                context.local_bindings.extend(bindings);
                let checked = check(guard, Some(&TCType::Bool), context);
                context.local_bindings.truncate(frame);
                require(checked?, &TCType::Bool, guard)?;
            }
            (TCType::Bool, None)
        }
        // A tag names an alternative, not a union, so which union it belongs
        // to comes from the qualifier the writer gave or the type the
        // expression is expected to have.
        Constructor(payload, tag, qualifier) => (
            check_constructor(
                expr,
                payload.into_iter().next(),
                tag,
                qualifier.as_ref(),
                expected,
                context,
            )?,
            None,
        ),
        Var(var) | SIndex(var, _) => (
            context
                .get(var)
                .map(TCType::from_stream_type)
                .ok_or_else(|| {
                    SemanticError::UndeclaredVariable(
                        format!("undeclared variable {var}"),
                        Some(expr.span()),
                        SourceLocation::default(),
                    )
                    .located(expr.origin())
                })?,
            None,
        ),
        BinOp(lhs, rhs, parsed) => {
            // The result constraint is not an operand constraint: comparisons
            // produce `Bool` while comparing values of another type. The
            // operands of `+`, `-` and `*` are numbers of the result's type,
            // so an `Int` or `Float` expectation passes down to them.
            let operand_expected = match (parsed, expected) {
                (
                    BinaryOperator::Add | BinaryOperator::Subtract | BinaryOperator::Multiply,
                    Some(expected @ (TCType::Int | TCType::Float)),
                ) => Some(expected),
                _ => None,
            };
            let (lhs_type, rhs_type) = check_operands(lhs, rhs, operand_expected, context)?;
            if parsed == BinaryOperator::Power
                && lhs_type == TCType::Int
                && rhs_type == TCType::Int
                && is_negative_integer_literal(rhs)
            {
                return Err(error(
                    rhs,
                    TypeErrorKind::OperatorTypeMismatch,
                    "integer exponent must be non-negative".to_owned(),
                ));
            }
            let resolved = resolve_binary(expr, parsed, &lhs_type, &rhs_type)?;
            if let Some(operand) = operand_type(parsed, &lhs_type, &rhs_type) {
                accept_at_run_time(lhs, &lhs_type, &operand, context);
                accept_at_run_time(rhs, &rhs_type, &operand, context);
            }
            resolved
        }
        Ascribe(value, target) => {
            let target_type = TCType::from_stream_type(target);
            let source = check(value, Some(&target_type), context)?;
            require(source, &target_type, expr)?;
            (target_type, None)
        }
        Cast(value, target) => {
            let source = check(value, None, context)?;
            let target_type = TCType::from_stream_type(target);
            if source == target_type
                && let Some(warnings) = context.warnings.as_deref_mut()
            {
                warnings.emit(
                    expr.id(),
                    SemanticWarning::new(
                        SemanticWarningKind::RedundantCast,
                        format!("cast from {target} to {target} is redundant"),
                        Some(expr.span()),
                    )
                    .located(expr.origin()),
                );
            }
            let valid = source == target_type
                || source == TCType::Int && target_type == TCType::Float
                || matches!(
                    source,
                    TCType::Int | TCType::Float | TCType::Bool | TCType::Unit
                ) && target_type == TCType::Str;
            if !valid {
                let detail = if source == TCType::Float && target_type == TCType::Int {
                    "; use trunc, floor, ceil, or round instead"
                } else {
                    ""
                };
                return Err(error(
                    expr,
                    TypeErrorKind::OperatorTypeMismatch,
                    format!("cannot cast {source} to {target}{detail}"),
                ));
            }
            (target_type, None)
        }
        If(cond, yes, no) => {
            require(
                check(cond, Some(&TCType::Bool), context)?,
                &TCType::Bool,
                expr,
            )?;
            let yes_type = check(yes, expected, context)?;
            let no_type = check(no, expected, context)?;
            (
                merge(
                    Some(expr),
                    &[(yes, yes_type), (no, no_type)],
                    expected,
                    context,
                )?,
                None,
            )
        }
        Dynamic(source, result_type, scope) | Defer(source, result_type, scope) => {
            validate_runtime_scope(expr, scope, context)?;
            let ascribed = match result_type {
                StreamTypeAscription::Ascribed(typ) => Some(TCType::from_stream_type(typ)),
                StreamTypeAscription::Unascribed => None,
            };
            let source_type = check(source, None, context)?;
            let typ = match source_type {
                TCType::Expr(inner) => {
                    let inner = *inner;
                    if let Some(ascribed) = ascribed {
                        require(inner, &ascribed, source)?;
                        ascribed
                    } else {
                        inner
                    }
                }
                TCType::Str if ascribed.is_some() => {
                    ascribed.expect("checked local runtime-expression ascription")
                }
                TCType::Str | TCType::Any if !context.strict_runtime_sources => ascribed
                    .or_else(|| expected.cloned())
                    .unwrap_or(TCType::Any),
                actual => {
                    return Err(error(
                        source,
                        TypeErrorKind::ExpectedExpressionSource,
                        format!(
                            "runtime expression source must have type Expr<T>, or Str with a local : T ascription; got {actual}"
                        ),
                    ));
                }
            };
            (typ, None)
        }
        Update(a, b) | Default(a, b) | Init(a, b) => {
            let a_type = check(a, expected, context)?;
            let b_type = check(b, expected, context)?;
            (
                merge(Some(expr), &[(a, a_type), (b, b_type)], expected, context)?,
                None,
            )
        }
        Latch(value, trigger) => {
            let value = check(value, expected, context)?;
            check(trigger, None, context)?;
            (value, None)
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
            if !matches!(typ, TCType::Int | TCType::Float | TCType::Any) {
                return Err(error(
                    expr,
                    TypeErrorKind::OperatorTypeMismatch,
                    "unary minus requires a number",
                ));
            }
            (typ, None)
        }
        Trunc(value) | Floor(value) | Ceil(value) | Round(value) => {
            require(
                check(value, Some(&TCType::Float), context)?,
                &TCType::Float,
                expr,
            )?;
            (TCType::Int, None)
        }
        Lambda(params, body) => {
            // A parameter's type is its annotation, else the type the
            // context expects for that argument.
            let hints = context
                .parameter_hints
                .take()
                .or_else(|| match expected {
                    Some(TCType::Function(hinted, _)) => Some(hinted.clone()),
                    _ => None,
                })
                .filter(|hinted| hinted.len() == params.len());
            let mut parameters = Vec::with_capacity(params.len());
            for (index, (name, ascription)) in params.iter().enumerate() {
                let typ = match ascription {
                    StreamTypeAscription::Ascribed(typ) => typ.clone(),
                    StreamTypeAscription::Unascribed => {
                        match hints
                            .as_ref()
                            .and_then(|hinted| hinted[index].to_stream_type())
                        {
                            Some(typ) => typ,
                            None if context.dynamic_unhinted_parameters => StreamType::Any,
                            None => {
                                return Err(SemanticError::MissingTypeAnnotation(
                                    format!(
                                        "cannot infer the type of lambda parameter `{name}` from its context; annotate it"
                                    ),
                                    Some(expr.span()),
                                    SourceLocation::default(),
                                ).located(expr.origin()));
                            }
                        }
                    }
                };
                parameters.push((name.clone(), typ));
            }
            let result_hint = match expected {
                Some(TCType::Function(_, result)) => Some(result.as_ref()),
                _ => None,
            };
            let frame_start = context.local_bindings.len();
            context.local_bindings.extend(parameters.iter().cloned());
            let result = check(body, result_hint, context);
            context.local_bindings.truncate(frame_start);
            (
                TCType::Function(
                    parameters
                        .iter()
                        .map(|(_, typ)| TCType::from_stream_type(typ))
                        .collect(),
                    Box::new(result?),
                ),
                None,
            )
        }
        Apply(function, args) if function.generic_function_signature().is_some() => {
            check_generic_def_call(expr, function, args, expected, context)?
        }
        Apply(function, args)
            if matches!(
                function.view(),
                Lambda(params, _) if params
                    .iter()
                    .any(|(_, ascription)| matches!(ascription, StreamTypeAscription::Unascribed))
            ) =>
        {
            // `(\x -> …)(a)`: the arguments are checked first and give the
            // lambda's parameters their types.
            let nodes: Vec<ExprRef<'_>> = args.collect();
            let arguments = nodes
                .iter()
                .map(|arg| check(*arg, None, context))
                .collect::<Result<EcoVec<_>, _>>()?;
            context.parameter_hints = Some(arguments.clone());
            let checked = check(function, None, context);
            context.parameter_hints = None;
            let TCType::Function(params, result) = checked? else {
                unreachable!("a lambda checks to a function type");
            };
            if params.len() != arguments.len() {
                return Err(error(
                    expr,
                    TypeErrorKind::FunctionArityMismatch,
                    "function argument count differs",
                ));
            }
            for ((node, actual), expected) in nodes.into_iter().zip(arguments).zip(&params) {
                require(actual.clone(), expected, expr)?;
                accept_at_run_time(node, &actual, expected, context);
            }
            (*result, None)
        }
        Apply(function, args) => 'apply: {
            let function = check(function, None, context)?;
            if function == TCType::Any {
                // A dynamic function is applied at runtime.
                for arg in args {
                    check(arg, None, context)?;
                }
                break 'apply (TCType::Any, None);
            }
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
        Fix(function) => 'fix: {
            let function = check(function, None, context)?;
            if function == TCType::Any {
                break 'fix (TCType::Any, None);
            }
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
        Partial(function, args) => 'partial: {
            let function = check(function, None, context)?;
            if function == TCType::Any {
                for arg in args {
                    check(arg, None, context)?;
                }
                break 'partial (TCType::Any, None);
            }
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
            let element = check_elements(items, expected_element, context)?;
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
            match check(list, None, context)? {
                TCType::List(element) => (*element, None),
                // A dynamic value is indexed at runtime.
                TCType::Any => (TCType::Any, None),
                _ => {
                    return Err(error(
                        expr,
                        TypeErrorKind::ListIndexTypeMismatch,
                        "indexing requires a list",
                    ));
                }
            }
        }
        LAppend(list, value) => match check(list, expected, context)? {
            TCType::List(element) => {
                require(check(value, Some(&element), context)?, &element, expr)?;
                (TCType::List(element), None)
            }
            TCType::Any => {
                check(value, None, context)?;
                (TCType::Any, None)
            }
            _ => {
                return Err(error(
                    expr,
                    TypeErrorKind::ListOperationTypeMismatch,
                    "append requires a list",
                ));
            }
        },
        LConcat(a, b) => {
            let a_type = check(a, expected, context)?;
            let b_type = check(b, expected, context)?;
            (
                merge(None, &[(a, a_type), (b, b_type)], expected, context)?,
                None,
            )
        }
        LHead(list) => match check(list, None, context)? {
            TCType::List(element) => (*element, None),
            TCType::Any => (TCType::Any, None),
            _ => {
                return Err(error(
                    expr,
                    TypeErrorKind::ListOperationTypeMismatch,
                    "head requires a list",
                ));
            }
        },
        LTail(list) => {
            let typ = check(list, expected, context)?;
            if !matches!(typ, TCType::List(_) | TCType::Any) {
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
            if !matches!(typ, TCType::List(_) | TCType::Any) {
                return Err(error(
                    expr,
                    TypeErrorKind::ListOperationTypeMismatch,
                    "length requires a list",
                ));
            }
            (TCType::Int, None)
        }
        LMap(function, list) => {
            let list = match check(list, None, context)? {
                TCType::Any => TCType::List(Box::new(TCType::Any)),
                list => list,
            };
            let TCType::List(input) = list else {
                return Err(error(
                    expr,
                    TypeErrorKind::ListOperationTypeMismatch,
                    "map requires a list",
                ));
            };
            let function = check_callback(function, EcoVec::from([(*input).clone()]), context)?;
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
            let list = match check(list, expected, context)? {
                TCType::Any => TCType::List(Box::new(TCType::Any)),
                list => list,
            };
            let TCType::List(input) = &list else {
                return Err(error(
                    expr,
                    TypeErrorKind::ListOperationTypeMismatch,
                    "filter requires a list",
                ));
            };
            let function = check_callback(function, EcoVec::from([(**input).clone()]), context)?;
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
            let list = match check(list, None, context)? {
                TCType::Any => TCType::List(Box::new(TCType::Any)),
                list => list,
            };
            let TCType::List(element) = list else {
                return Err(error(
                    expr,
                    TypeErrorKind::ListOperationTypeMismatch,
                    "fold requires a list",
                ));
            };
            let function = check_callback(
                function,
                EcoVec::from([accumulator.clone(), (*element).clone()]),
                context,
            )?;
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
                fields.iter().map(|(_, value)| value),
                expected_value,
                context,
            )?;
            (TCType::map(value), None)
        }
        Struct(fields) => {
            reject_duplicate_fields(expr, &fields)?;
            match expected {
                Some(TCType::Struct(expected_fields, allow_extra)) => {
                    if !allow_extra
                        && fields.keys().any(|name| {
                            !expected_fields.iter().any(|(expected, _)| expected == name)
                        })
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
                    // Fields beyond those an open struct requires still need
                    // a recorded type.
                    for (name, value) in fields.iter() {
                        if !expected_fields.iter().any(|(expected, _)| expected == name) {
                            check(value, None, context)?;
                        }
                    }
                    (expected.cloned().expect("matched expected Struct"), None)
                }
                // `Any` expects nothing of the fields, so they are inferred as
                // they are with no expectation at all.
                None | Some(TCType::Any) => {
                    let mut inferred = EcoVec::new();
                    for (name, value) in fields.iter() {
                        inferred.push((name.clone(), check(value, None, context)?));
                    }
                    (TCType::Struct(inferred, false), None)
                }
                Some(_) => {
                    return Err(error(
                        expr,
                        TypeErrorKind::StructExpected,
                        "Struct constructor requires an expected Struct type",
                    ));
                }
            }
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
                let actual = check_field(expr, name, value, field_expected, context)?;
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
            TCType::Any => (TCType::Any, None),
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
            TCType::Any => (TCType::Any, None),
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
                TCType::Any => &TCType::Any,
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
            if !matches!(typ, TCType::Map(_) | TCType::Any) {
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
            if !matches!(typ, TCType::Map(_) | TCType::Struct(_, _) | TCType::Any) {
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
            if !matches!(typ, TCType::Int | TCType::Float | TCType::Any) {
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
        Check(..) => unreachable!("only elaboration builds a run-time check, after checking"),
    };
    if let Some(expected) = expected {
        require(typ.clone(), expected, expr)?;
        accept_at_run_time(expr, &typ, expected, context);
    }
    if let Some(warnings) = context.warnings.as_deref_mut() {
        warnings.observe(expr, &typ);
    }
    if let Some(expr_types) = &mut context.expr_types {
        expr_types
            .insert(expr, typ.clone())
            .expect("expression belongs to the expression-type scope");
    }
    Ok(typ)
}

fn check_elements(
    items: ExprRefs<'_>,
    expected: Option<&TCType>,
    context: &mut TypeContext<'_>,
) -> Result<TCType, SemanticError> {
    check_fields(items, expected, context)
}

fn check_fields<'arena>(
    items: impl IntoIterator<Item = ExprRef<'arena>>,
    expected: Option<&TCType>,
    context: &mut TypeContext<'_>,
) -> Result<TCType, SemanticError> {
    let mut checked = Vec::new();
    for item in items {
        let actual = check(item, expected, context)?;
        checked.push((item, actual));
    }
    if checked.is_empty() {
        return Ok(expected.cloned().unwrap_or(TCType::Unknown));
    }
    merge(None, &checked, expected, context)
}

fn value_type(value: &SyntaxLiteral, expected: Option<&TCType>) -> Result<TCType, SemanticError> {
    Ok(match value {
        // An integer literal is flexible: where a `Float` is expected it is a
        // `Float` literal, and elaboration rewrites it as one.
        SyntaxLiteral::Int(_) if matches!(expected, Some(TCType::Float)) => TCType::Float,
        SyntaxLiteral::Int(_) => TCType::Int,
        SyntaxLiteral::Float(_) => TCType::Float,
        SyntaxLiteral::Str(_) => match expected {
            Some(TCType::Expr(inner)) => TCType::Expr(inner.clone()),
            _ => TCType::Str,
        },
        SyntaxLiteral::Bool(_) => TCType::Bool,
        SyntaxLiteral::Unit => TCType::Unit,
        SyntaxLiteral::List(values) => {
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
        SyntaxLiteral::Tuple(values) => TCType::Tuple(
            values
                .iter()
                .map(|value| value_type(value, None).unwrap_or(TCType::Any))
                .collect(),
        ),
        SyntaxLiteral::Map(_) => expected
            .cloned()
            .unwrap_or_else(|| TCType::map(TCType::Any)),
        SyntaxLiteral::Struct(fields) => {
            let fields = fields
                .iter()
                .map(|(name, value)| (name.clone(), value_type(value, None).unwrap_or(TCType::Any)))
                .collect();
            TCType::Struct(fields, false)
        }
        SyntaxLiteral::NoVal => expected.cloned().unwrap_or(TCType::Unknown),
    })
}

fn is_negative_integer_literal(expr: ExprRef<'_>) -> bool {
    match expr.view() {
        ExprView::Neg(inner) => {
            matches!(inner.view(), ExprView::Val(SyntaxLiteral::Int(value)) if *value > 0)
        }
        // Source literals represent negativity as `Neg`, but programmatically
        // constructed ASTs may contain any negative `SyntaxLiteral::Int` directly.
        ExprView::Val(SyntaxLiteral::Int(value)) => *value < 0,
        _ => false,
    }
}

fn resolve_binary(
    expr: ExprRef<'_>,
    operator: BinaryOperator,
    lhs: &TCType,
    rhs: &TCType,
) -> Result<(TCType, Option<()>), SemanticError> {
    let invalid = || {
        let hint = match (lhs, rhs) {
            (TCType::Int, TCType::Float) | (TCType::Float, TCType::Int) => {
                "; convert the Int explicitly with `as Float`"
            }
            _ => "",
        };
        error(
            expr,
            TypeErrorKind::OperatorTypeMismatch,
            format!(
                "{} operator `{}` cannot combine {lhs} and {rhs}{hint}",
                operator.name(),
                operator.symbol()
            ),
        )
    };
    let numeric_result = || match (lhs, rhs) {
        (TCType::Int, TCType::Int) => Some((TCType::Int, Some(()))),
        (TCType::Float, TCType::Float) => Some((TCType::Float, Some(()))),
        (TCType::Any, TCType::Int) | (TCType::Int, TCType::Any) => Some((TCType::Int, Some(()))),
        (TCType::Any, TCType::Float) | (TCType::Float, TCType::Any) => {
            Some((TCType::Float, Some(())))
        }
        (TCType::Any, TCType::Any) => Some((TCType::Any, None)),
        _ => None,
    };

    Ok(match operator.kind() {
        BinaryOperatorKind::Numeric(_) => numeric_result().ok_or_else(invalid)?,
        BinaryOperatorKind::Boolean(_)
            if matches!(lhs, TCType::Bool | TCType::Any)
                && matches!(rhs, TCType::Bool | TCType::Any) =>
        {
            (TCType::Bool, Some(()))
        }
        BinaryOperatorKind::String
            if matches!(lhs, TCType::Str | TCType::Any)
                && matches!(rhs, TCType::Str | TCType::Any) =>
        {
            (TCType::Str, Some(()))
        }
        BinaryOperatorKind::Equality(_) if unify(lhs, rhs).is_some() => (TCType::Bool, Some(())),
        BinaryOperatorKind::Ordering(_) if numeric_result().is_some() => (TCType::Bool, Some(())),
        BinaryOperatorKind::Ordering(_)
            if matches!(lhs, TCType::Bool | TCType::Any)
                && matches!(rhs, TCType::Bool | TCType::Any) =>
        {
            (TCType::Bool, Some(()))
        }
        BinaryOperatorKind::Ordering(_)
            if matches!(lhs, TCType::Str | TCType::Any)
                && matches!(rhs, TCType::Str | TCType::Any) =>
        {
            (TCType::Bool, Some(()))
        }
        _ => return Err(invalid()),
    })
}

fn validate_runtime_scope(
    expr: ExprRef<'_>,
    scope: &ReconfigurableExprScope,
    context: &TypeContext<'_>,
) -> Result<(), SemanticError> {
    let ReconfigurableExprScope::Explicit(vars) = scope else {
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
                SourceLocation::default(),
            )
            .located(expr.origin()));
        }
    }
    Ok(())
}

/// The type of a stream that carries a value of either type, where two
/// streams become one: the branches of an `if`, the arms of a `match`, the two
/// sides of `default`, the elements of a list.
///
/// This is not `unify`. `unify` asks whether a value of one type may be used
/// as another, and lets `Any` instantiate to the type it meets, because
/// using an `Any` value as an `Int` is checked when it runs. A merged stream
/// is one that may carry either, so an `Any` among them makes the merge `Any`,
/// and so do two unrelated types: `join` always has an answer.
pub(super) fn join(a: &TCType, b: &TCType) -> TCType {
    match (a, b) {
        _ if a == b => a.clone(),
        (TCType::Any, _) | (_, TCType::Any) => TCType::Any,
        // Placeholders stand for a type that is not known yet.
        (TCType::Unknown, other) | (other, TCType::Unknown) => other.clone(),
        (TCType::EmptyList, list @ TCType::List(_))
        | (list @ TCType::List(_), TCType::EmptyList) => list.clone(),
        (TCType::EmptyMap, map @ TCType::Map(_)) | (map @ TCType::Map(_), TCType::EmptyMap) => {
            map.clone()
        }
        (TCType::EmptyList | TCType::EmptyMap, _) | (_, TCType::EmptyList | TCType::EmptyMap) => {
            TCType::Any
        }
        // A merged list carries elements of either list, so the elements
        // join rather than unify: `unify` lets an `Any` element take the
        // other side's type, which would make the merge depend on order.
        (TCType::List(a), TCType::List(b)) => TCType::list(join(a, b)),
        (TCType::Map(a), TCType::Map(b)) => TCType::map(join(a, b)),
        // Compatible structs and functions unify; anything else is unrelated.
        _ => unify(a, b).unwrap_or(TCType::Any),
    }
}

/// Whether `typ` says what its values are: no `Any` or unresolved part.
fn is_fully_known(typ: &TCType) -> bool {
    match typ {
        TCType::Any | TCType::Unknown | TCType::EmptyList | TCType::EmptyMap => false,
        TCType::List(inner) | TCType::Map(inner) | TCType::Expr(inner) => is_fully_known(inner),
        TCType::Tuple(items) => items.iter().all(is_fully_known),
        TCType::Struct(fields, _) => fields.iter().all(|(_, field)| is_fully_known(field)),
        _ => true,
    }
}

/// Warn where branches that each say what they are, and differ, are merged
/// into an `Any` nobody asked for. That is usually a mistake, while an
/// annotation (an expectation) says it is meant, and an `Any` branch is
/// already dynamic, so neither warns.
fn warn_if_heterogeneous(
    node: ExprRef<'_>,
    branches: &[TCType],
    expected: Option<&TCType>,
    context: &mut TypeContext<'_>,
) {
    if expected.is_some() || branches.len() < 2 || !branches.iter().all(is_fully_known) {
        return;
    }
    let joined = branches
        .iter()
        .skip(1)
        .fold(branches[0].clone(), |joined, typ| join(&joined, typ));
    // Every branch is fully known, so any `Any` in the merge, at the top or
    // inside a list or map, is one the merge introduced.
    if is_fully_known(&joined) {
        return;
    }
    let mut distinct: Vec<String> = Vec::new();
    for typ in branches {
        let typ = typ.to_string();
        if !distinct.contains(&typ) {
            distinct.push(typ);
        }
    }
    if let Some(warnings) = context.warnings.as_deref_mut() {
        warnings.emit(
            node.id(),
            SemanticWarning::new(
                SemanticWarningKind::HeterogeneousBranches,
                format!(
                    "the branches have different types ({}), so the result is {joined}; \
                     annotate the result as {joined} to say so",
                    distinct.join(", ")
                ),
                Some(node.span()),
            )
            .located(node.origin()),
        );
    }
}

/// The type of an expression whose value is one of `branches`, each already
/// checked against `expected`.
///
/// A flexible branch (see `is_flexible`) first takes what its siblings need:
/// an integer literal among `Float` branches is a `Float` literal, and an
/// empty list or map takes the element type of the others. The branches are
/// then joined, and where an expectation exists the merge takes its type,
/// which is what lets each `Any` branch be checked at that type.
fn merge(
    node: Option<ExprRef<'_>>,
    branches: &[(ExprRef<'_>, TCType)],
    expected: Option<&TCType>,
    context: &mut TypeContext<'_>,
) -> Result<TCType, SemanticError> {
    let mut types: Vec<TCType> = branches.iter().map(|(_, typ)| typ.clone()).collect();
    let resolved = branches
        .iter()
        .zip(&types)
        .filter(|((expr, _), typ)| !is_flexible(*expr, typ))
        .fold(None, |joined: Option<TCType>, (_, typ)| {
            Some(joined.map_or_else(|| typ.clone(), |joined| join(&joined, typ)))
        });
    if let Some(resolved) = resolved {
        for (index, (expr, _)) in branches.iter().enumerate() {
            if is_flexible(*expr, &types[index]) && can_adopt(*expr, &types[index], &resolved) {
                types[index] = check(*expr, Some(&resolved), context)?;
            }
        }
    }
    if let Some(node) = node {
        warn_if_heterogeneous(node, &types, expected, context);
    }
    let joined = types
        .iter()
        .skip(1)
        .fold(types[0].clone(), |joined, typ| join(&joined, typ));
    Ok(match expected {
        Some(expected) => unify(&joined, expected).unwrap_or(joined),
        None => joined,
    })
}

pub(super) fn unify(a: &TCType, b: &TCType) -> Option<TCType> {
    if a == b {
        Some(a.clone())
    } else if let (TCType::Expr(a_inner), TCType::Expr(b_inner)) = (a, b) {
        unify(a_inner, b_inner).map(|inner| TCType::Expr(Box::new(inner)))
    } else if let (TCType::List(a_inner), TCType::List(b_inner)) = (a, b) {
        // An empty list literal is `List<Unknown>`: it takes its element
        // type from the list it joins.
        unify(a_inner, b_inner).map(TCType::list)
    } else if let (TCType::Map(a_inner), TCType::Map(b_inner)) = (a, b) {
        unify(a_inner, b_inner).map(TCType::map)
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
    } else if let (TCType::Function(a_params, a_result), TCType::Function(b_params, b_result)) =
        (a, b)
    {
        (a_params.len() == b_params.len()
            && a_params
                .iter()
                .zip(b_params)
                .all(|(a_param, b_param)| unify(a_param, b_param).is_some())
            && unify(a_result, b_result).is_some())
        .then(|| a.clone())
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
    } else {
        None
    }
}

/// What a pattern binds, and their types, when matched against `scrutinee`.
///
/// A pattern that cannot describe a value of that type is reported here,
/// where the shape is known, rather than as a mismatch in the arm's body.
fn bind_pattern(
    pattern: &MatchPattern,
    scrutinee: &TCType,
    expr: ExprRef<'_>,
) -> Result<Vec<(VarName, StreamType)>, SemanticError> {
    let mut bindings = Vec::new();
    collect_pattern_bindings(pattern, scrutinee, expr, &mut bindings)?;
    Ok(bindings)
}

fn collect_pattern_bindings(
    pattern: &MatchPattern,
    scrutinee: &TCType,
    expr: ExprRef<'_>,
    bindings: &mut Vec<(VarName, StreamType)>,
) -> Result<(), SemanticError> {
    let bind = |bindings: &mut Vec<(VarName, StreamType)>, name: &VarName, typ: &TCType| {
        bindings.push((
            name.clone(),
            typ.to_stream_type().unwrap_or(StreamType::Any),
        ));
    };
    let mismatch = |wanted: &str| {
        Err(error(
            expr,
            TypeErrorKind::PatternTypeMismatch,
            format!("`{pattern}` matches {wanted}, but the value here is {scrutinee}"),
        ))
    };
    match &pattern.kind {
        PatternKind::Wildcard => {}
        PatternKind::Bind(name) => bind(bindings, name, scrutinee),
        PatternKind::As(name, inner) => {
            bind(bindings, name, scrutinee);
            collect_pattern_bindings(inner, scrutinee, expr, bindings)?;
        }
        PatternKind::Tag { tag, payload } => {
            let TCType::Union(schema) = scrutinee else {
                if *scrutinee == TCType::Any {
                    return Ok(());
                }
                return mismatch("a union");
            };
            let Some((_, alternative)) = schema.alternative(tag) else {
                return Err(error(
                    expr,
                    TypeErrorKind::UnknownUnionTag,
                    format!("`{tag}` is not an alternative of {scrutinee}"),
                ));
            };
            match (alternative.payload(), payload) {
                (UnionPayload::Nullary, None) => {}
                (UnionPayload::Of(declared), Some(payload)) => {
                    collect_pattern_bindings(payload, declared, expr, bindings)?;
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
                        format!("`{tag}` carries {declared}, which this pattern does not name"),
                    ));
                }
            }
        }
        PatternKind::Tuple(items) => match scrutinee {
            TCType::Any => {}
            TCType::Tuple(types) if types.len() == items.len() => {
                for (item, typ) in items.iter().zip(types.iter()) {
                    collect_pattern_bindings(item, typ, expr, bindings)?;
                }
            }
            _ => return mismatch("a tuple of that width"),
        },
        PatternKind::List(items) => match scrutinee {
            TCType::Any | TCType::EmptyList => {}
            TCType::List(element) => {
                for item in items {
                    collect_pattern_bindings(item, element, expr, bindings)?;
                }
            }
            _ => return mismatch("a list"),
        },
        PatternKind::Struct { fields, .. } => match scrutinee {
            TCType::Any => {}
            TCType::Struct(declared, _) => {
                for (name, pattern) in fields {
                    let Some((_, typ)) = declared.iter().find(|(field, _)| field == name) else {
                        return Err(error(
                            expr,
                            TypeErrorKind::StructUnknownField,
                            format!("{scrutinee} has no field `{name}`"),
                        ));
                    };
                    collect_pattern_bindings(pattern, typ, expr, bindings)?;
                }
            }
            TCType::Map(value) => {
                for (_, pattern) in fields {
                    collect_pattern_bindings(pattern, value, expr, bindings)?;
                }
            }
            _ => return mismatch("a struct"),
        },
        PatternKind::Literal(value) => {
            let literal = match value {
                SyntaxLiteral::Int(_) => TCType::Int,
                SyntaxLiteral::Str(_) => TCType::Str,
                SyntaxLiteral::Bool(_) => TCType::Bool,
                _ => TCType::Unit,
            };
            if *scrutinee != TCType::Any && unify(scrutinee, &literal).is_none() {
                return mismatch(&literal.to_string());
            }
        }
        PatternKind::Range { .. } => {
            if *scrutinee != TCType::Any && *scrutinee != TCType::Int {
                return mismatch("an Int");
            }
        }
        // Every alternative sees the same value, so each must bind the same
        // names with the same types for the arm to have one meaning.
        PatternKind::Or(alternatives) => {
            let mut first: Option<Vec<(VarName, StreamType)>> = None;
            for alternative in alternatives {
                let mut bound = Vec::new();
                collect_pattern_bindings(alternative, scrutinee, expr, &mut bound)?;
                bound.sort_by(|left, right| left.0.cmp(&right.0));
                match &first {
                    None => first = Some(bound),
                    Some(expected) if *expected == bound => {}
                    Some(expected) => {
                        return Err(error(
                            expr,
                            TypeErrorKind::OrPatternBindings,
                            format!(
                                "`{alternative}` binds {}, while an earlier alternative binds {}",
                                names_of(&bound),
                                names_of(expected)
                            ),
                        ));
                    }
                }
            }
            bindings.extend(first.unwrap_or_default());
        }
    }
    Ok(())
}

fn names_of(bindings: &[(VarName, StreamType)]) -> String {
    if bindings.is_empty() {
        return "nothing".to_owned();
    }
    bindings
        .iter()
        .map(|(name, _)| name.name())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Whether the arms leave a value unmatched.
///
/// A guarded arm does not count: its guard may refuse the value it matched,
/// so something after it still has to catch that value.
fn check_exhaustive(
    expr: ExprRef<'_>,
    shape: &[MatchArm],
    scrutinee: &TCType,
) -> Result<(), SemanticError> {
    let unguarded = shape.iter().filter(|arm| !arm.guarded);
    if unguarded.clone().any(|arm| arm.pattern.is_irrefutable()) {
        return Ok(());
    }
    let TCType::Union(schema) = scrutinee else {
        // Only a union is covered by naming its alternatives; anything else
        // needs a pattern that matches whatever it is given.
        return Err(error(
            expr,
            TypeErrorKind::MatchNotExhaustive,
            format!("a `match` on {scrutinee} needs an arm that matches every value, such as `_`"),
        ));
    };
    let covered: BTreeSet<&str> = unguarded
        .filter_map(|arm| match &arm.pattern.kind {
            PatternKind::Tag { tag, payload } => payload
                .as_ref()
                .is_none_or(|payload| payload.is_irrefutable())
                .then_some(tag.as_str()),
            _ => None,
        })
        .collect();
    let missing: Vec<&str> = schema
        .alternatives()
        .iter()
        .map(|alternative| alternative.tag().as_str())
        .filter(|tag| !covered.contains(tag))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    Err(error(
        expr,
        TypeErrorKind::MatchNotExhaustive,
        format!("no arm matches {}", missing.join(", ")),
    ))
}

/// Resolve a constructor to the union it builds, and check its payload.
///
/// A qualifier names the union outright. Without one the expected type
/// decides, as a tag in a pattern is decided by the scrutinee's type; where
/// there is no expected type, or it is dynamic, there is nothing to resolve
/// against and the constructor is reported rather than guessed at.
fn check_constructor(
    expr: ExprRef<'_>,
    payload: Option<ExprRef<'_>>,
    tag: &EcoString,
    qualifier: Option<&TypePath>,
    expected: Option<&TCType>,
    context: &mut TypeContext<'_>,
) -> Result<TCType, SemanticError> {
    let union = match qualifier {
        Some(qualifier) => union_named(expr, qualifier)?,
        None => match expected {
            Some(TCType::Union(schema)) => schema.clone(),
            // An imported tag names its union outright, but only where
            // the expected type has not already said which one.
            Some(TCType::Any) | None
                if expr
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
            Some(TCType::Any) | None => {
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
                    format!("`{tag}` builds a union, but {other} is expected here"),
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
                TCType::Union(union.clone()),
                tags_found_in(expr, tag)
            ),
        ));
    };
    match (alternative.payload(), payload) {
        (UnionPayload::Nullary, None) => {}
        (UnionPayload::Of(declared), Some(payload)) => {
            // The payload is checked against the alternative's type, which
            // is also what resolves a bare constructor written inside it. A
            // plain disagreement is reported as this constructor's, so the
            // message names the tag rather than only the types.
            let actual = check(payload, Some(declared), context).map_err(|error| match &error {
                SemanticError::TypeError(mismatch)
                    if *mismatch.kind() == TypeErrorKind::AnnotationTypeMismatch =>
                {
                    SemanticError::type_error_at(
                        TypeErrorKind::ConstructorPayloadTypeMismatch,
                        format!("`{tag}` carries {declared}, and {}", mismatch.message()),
                        mismatch.span().unwrap_or_else(|| payload.span()),
                    )
                    .located_as(&error, payload.origin())
                }
                _ => error,
            })?;
            if unify(&actual, declared).is_none() {
                return Err(error(
                    payload,
                    TypeErrorKind::ConstructorPayloadTypeMismatch,
                    format!("`{tag}` carries {declared}, got {actual}"),
                ));
            }
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
                format!("`{tag}` carries {declared}, which is missing"),
            ));
        }
    }
    Ok(TCType::Union(union))
}

/// The union a qualifier names, in the namespace the node was expanded in.
fn union_named(
    expr: ExprRef<'_>,
    qualifier: &TypePath,
) -> Result<ClosedUnion<TCType>, SemanticError> {
    let named = expr
        .source_context()
        .and_then(|context| context.get(qualifier));
    match named.map(TCType::from_stream_type) {
        Some(TCType::Union(schema)) => Ok(schema),
        Some(other) => Err(error(
            expr,
            TypeErrorKind::ExpectedUnion,
            format!("{qualifier} is {other}, not a union"),
        )),
        None => Err(error(
            expr,
            TypeErrorKind::ExpectedUnion,
            format!("{qualifier} names no type here"),
        )),
    }
}

/// The unions in scope that do have this tag, to say in a message where it
/// could have come from.
fn tags_found_in(expr: ExprRef<'_>, tag: &EcoString) -> String {
    let Some(context) = expr.source_context() else {
        return String::new();
    };
    let names: Vec<_> = context
        .aliases()
        .iter()
        .filter(|(_, ty)| match ty {
            StreamType::Union(schema) => schema.alternative(tag).is_some(),
            _ => false,
        })
        .map(|(name, _)| name.to_string())
        .collect();
    match names.len() {
        0 => String::new(),
        _ => format!("; `{tag}` is an alternative of {}", names.join(", ")),
    }
}

fn check_generic_def_call(
    call: ExprRef<'_>,
    function: ExprRef<'_>,
    args: ExprRefs<'_>,
    expected: Option<&TCType>,
    context: &mut TypeContext<'_>,
) -> Result<(TCType, Option<()>), SemanticError> {
    let signature = function
        .generic_function_signature()
        .expect("generic call guard supplies a signature");
    if signature.parameters.len() != args.len() {
        return Err(error(
            call,
            TypeErrorKind::FunctionArityMismatch,
            "function argument count differs",
        ));
    }
    let definition_context = function
        .source_context()
        .expect("expanded generic definitions retain their source context");
    let parameters: BTreeSet<TypeName> = signature.type_parameters.iter().cloned().collect();
    let mut bound = BTreeMap::new();
    // Prefer result-context inference, including `Any` for a result-only
    // parameter. Nested recursive aliases can fail this structural pre-bind;
    // that case falls back to argument inference below and is still checked
    // against the call-site result after instantiation.
    if let Some(expected) = expected.and_then(TCType::to_stream_type) {
        if !match_source_type(
            definition_context,
            &signature.result,
            &expected,
            &parameters,
            &mut bound,
        ) {
            // Recursive/nested aliases can be structurally compatible while
            // still being impossible to pre-bind from the result alone. Fall
            // back to argument inference; the instantiated result is checked
            // against `expected` below, so this does not relax type safety.
            bound.clear();
        }
    }
    // Keep successful argument checks: rechecking every grounded subtree
    // would multiply work at each level of nested generic applications.
    // Result context and independently grounded siblings both supply hints.
    let mut inferred = bound.clone();
    let mut hints = generic_argument_hints(definition_context, &signature.parameters, &inferred);
    let mut arguments: Vec<_> = args
        .clone()
        .zip(&hints)
        .map(|(arg, hint)| check_generic_argument(arg, hint.as_ref(), context))
        .collect();
    loop {
        for (pattern, actual) in signature.parameters.iter().zip(&arguments) {
            if let Some(actual) = actual.as_ref().ok().and_then(TCType::to_stream_type) {
                let mut candidate = inferred.clone();
                if match_source_type(
                    definition_context,
                    pattern,
                    &actual,
                    &parameters,
                    &mut candidate,
                ) {
                    inferred = candidate;
                }
            }
        }
        let next_hints =
            generic_argument_hints(definition_context, &signature.parameters, &inferred);
        let mut retried = false;
        for (((arg, actual), hint), previous) in args
            .clone()
            .zip(&mut arguments)
            .zip(&next_hints)
            .zip(&hints)
        {
            if actual.as_ref().map_or(true, contains_unresolved_type)
                && hint.is_some()
                && hint != previous
            {
                *actual = check_generic_argument(arg, hint.as_ref(), context);
                retried = true;
            }
        }
        hints = next_hints;
        if !retried {
            break;
        }
    }
    let arguments = arguments.into_iter().collect::<Result<EcoVec<_>, _>>()?;
    // Infer argument-owned parameters from the arguments themselves. A result
    // context may have pre-bound the same parameter through a recursively
    // expanded alias whose open structural details are less precise than the
    // argument. Argument inference is authoritative for those parameters;
    // result-only bindings are merged back afterwards.
    let result_bound = std::mem::take(&mut bound);
    for (pattern, actual_type) in signature.parameters.iter().zip(&arguments) {
        let Some(actual) = actual_type.to_stream_type() else {
            return Err(error(
                call,
                TypeErrorKind::AnnotationTypeMismatch,
                "generic def argument has no concrete type",
            ));
        };
        if !match_source_type(
            definition_context,
            pattern,
            &actual,
            &parameters,
            &mut bound,
        ) {
            let mut fallback_bound = result_bound.clone();
            fallback_bound.extend(bound.clone());
            let source_bound: BTreeMap<&TypeName, SourceType> = fallback_bound
                .iter()
                .map(|(name, ty)| (name, ty.clone().into()))
                .collect();
            let source_bound_refs: BTreeMap<&TypeName, &SourceType> =
                source_bound.iter().map(|(name, ty)| (*name, ty)).collect();
            let resolved_expected = definition_context
                .resolve_type(&substitute_source(pattern, &source_bound_refs))
                .ok()
                .map(|expected| TCType::from_stream_type(&expected));
            let compatible = resolved_expected
                .as_ref()
                .is_some_and(|expected| unify(actual_type, expected).is_some());
            if !compatible {
                return Err(error(
                    call,
                    TypeErrorKind::AnnotationTypeMismatch,
                    "generic def argument does not satisfy its declared type",
                ));
            }
        }
    }
    for (parameter, result_type) in result_bound {
        bound.entry(parameter).or_insert(result_type);
    }
    if bound.len() != parameters.len() {
        return Err(error(
            call,
            TypeErrorKind::AnnotationTypeMismatch,
            "generic function signature has an ungrounded type parameter",
        ));
    }
    let hint_bound: BTreeMap<&TypeName, SourceType> = bound
        .iter()
        .map(|(name, ty)| (name, ty.clone().into()))
        .collect();
    let hint_bound_refs: BTreeMap<&TypeName, &SourceType> =
        hint_bound.iter().map(|(name, ty)| (*name, ty)).collect();
    let parameter_types = signature
        .parameters
        .iter()
        .map(|pattern| {
            definition_context
                .resolve_type(&substitute_source(pattern, &hint_bound_refs))
                .map(|ty| TCType::from_stream_type(&ty))
        })
        .collect::<Result<EcoVec<_>, _>>()
        .map_err(|_| {
            error(
                call,
                TypeErrorKind::AnnotationTypeMismatch,
                "generic def parameter type could not be instantiated",
            )
        })?;
    let result = definition_context
        .resolve_type(&substitute_source(&signature.result, &hint_bound_refs))
        .map(|ty| TCType::from_stream_type(&ty))
        .map_err(|_| {
            error(
                call,
                TypeErrorKind::AnnotationTypeMismatch,
                "generic def result type could not be instantiated",
            )
        })?;
    let function_type = TCType::Function(parameter_types.clone(), Box::new(result.clone()));
    require(
        check(function, Some(&function_type), context)?,
        &function_type,
        call,
    )?;
    for ((node, actual), expected) in args.zip(arguments).zip(&parameter_types) {
        require(actual.clone(), expected, call)?;
        accept_at_run_time(node, &actual, expected, context);
    }
    if let Some(expected) = expected {
        require(result.clone(), expected, call)?;
    }
    Ok((result, None))
}

/// Check the operands of a binary operator. Either grounded operand can supply
/// the other operand's context: a flexible operand is checked again with the
/// type of the other, unless that is flexible too. Whether the operator accepts
/// the resulting pair remains the operator's job.
fn check_operands(
    lhs: ExprRef<'_>,
    rhs: ExprRef<'_>,
    expected: Option<&TCType>,
    context: &mut TypeContext<'_>,
) -> Result<(TCType, TCType), SemanticError> {
    let mut left = check_generic_argument(lhs, expected, context);
    let mut right = check_generic_argument(rhs, expected, context);
    if let Ok(hint) = &right
        && !is_flexible(rhs, hint)
    {
        let hint = hint.clone();
        left = adopt(lhs, left, &hint, context);
    }
    if let Ok(hint) = &left
        && !is_flexible(lhs, hint)
    {
        let hint = hint.clone();
        right = adopt(rhs, right, &hint, context);
    }
    Ok((left?, right?))
}

/// Check a flexible operand again at the type of the other one. An operand
/// that fails to check, or whose type cannot become `hint`, is left to the
/// operator to report, except for an integer literal taken as a `Float`,
/// whose own error (one that a `Float` cannot hold exactly) is the useful one.
fn adopt(
    operand: ExprRef<'_>,
    current: Result<TCType, SemanticError>,
    hint: &TCType,
    context: &mut TypeContext<'_>,
) -> Result<TCType, SemanticError> {
    match &current {
        Err(_) => check_generic_argument(operand, Some(hint), context),
        Ok(typ) if is_flexible(operand, typ) && can_adopt(operand, typ, hint) => {
            match check_attempt(operand, Some(hint), context) {
                Err(error) if *typ == TCType::Int && *hint == TCType::Float => Err(error),
                Err(_) => current,
                adopted => adopted,
            }
        }
        Ok(_) => current,
    }
}

/// Delay a failed argument while siblings supply context. Successful checks
/// already carry their authoritative annotations; failed attempts emit no
/// warnings and cannot alter the caller's local bindings or parameter hints.
fn check_generic_argument(
    argument: ExprRef<'_>,
    expected: Option<&TCType>,
    context: &mut TypeContext<'_>,
) -> Result<TCType, SemanticError> {
    let result = check_attempt(argument, expected, context);
    // A concrete argument owns its type even when a result-derived hint
    // disagrees. Let the call's signature check report that mismatch at the
    // call, retaining the embedded definition note.
    if result.is_err() && expected.is_some() {
        if let Ok(actual) = check_generic_argument(argument, None, context) {
            return Ok(actual);
        }
    }
    result
}

/// Check `argument` in a scratch context, keeping its warnings and the
/// run-time checks it recorded only if the check succeeds.
fn check_attempt(
    argument: ExprRef<'_>,
    expected: Option<&TCType>,
    context: &mut TypeContext<'_>,
) -> Result<TCType, SemanticError> {
    let recorded = context.runtime_checks.len();
    let mut warnings = WarningCollector::default();
    let mut attempt = TypeContext {
        environment: Cow::Borrowed(context.environment.as_ref()),
        local_bindings: context.local_bindings.clone(),
        expr_types: context.expr_types.take(),
        runtime_checks: std::mem::take(&mut context.runtime_checks),
        owner: context.owner.clone(),
        strict_runtime_sources: context.strict_runtime_sources,
        dynamic_unhinted_parameters: context.dynamic_unhinted_parameters,
        parameter_hints: None,
        warnings: context.warnings.is_some().then_some(&mut warnings),
    };
    let result = check(argument, expected, &mut attempt);
    context.expr_types = attempt.expr_types.take();
    context.runtime_checks = std::mem::take(&mut attempt.runtime_checks);
    if result.is_ok() {
        if let Some(collector) = context.warnings.as_deref_mut() {
            collector.absorb(warnings);
        }
    } else {
        context.runtime_checks.truncate(recorded);
    }
    result
}

/// Instantiate only parameter patterns whose generic arguments are grounded.
fn generic_argument_hints(
    definition_context: &crate::lang::dsrv::source::SourceContext,
    patterns: &[SourceType],
    bound: &BTreeMap<TypeName, StreamType>,
) -> Vec<Option<TCType>> {
    let source_bound: BTreeMap<&TypeName, SourceType> = bound
        .iter()
        .map(|(name, ty)| (name, ty.clone().into()))
        .collect();
    let source_bound_refs = source_bound.iter().map(|(name, ty)| (*name, ty)).collect();
    patterns
        .iter()
        .map(|pattern| {
            definition_context
                .resolve_type(&substitute_source(pattern, &source_bound_refs))
                .ok()
                .map(|ty| TCType::from_stream_type(&ty))
        })
        .collect()
}

/// Whether `expr`, checked to `typ`, can still take its type from context:
/// a type that is not yet resolved (`Any`, an empty list or map, ...), or an
/// `Int` made only of integer literals, which is a `Float` literal where a
/// `Float` is expected. A variable or any other `Int` expression is not
/// flexible, so `n + 1.5` stays an error.
fn is_flexible(expr: ExprRef<'_>, typ: &TCType) -> bool {
    contains_unresolved_type(typ) || (*typ == TCType::Int && is_integer_literal(expr))
}

/// Whether a flexible `expr` of type `typ` can be checked again at `target`.
fn can_adopt(expr: ExprRef<'_>, typ: &TCType, target: &TCType) -> bool {
    unify(typ, target).is_some()
        || (*typ == TCType::Int && *target == TCType::Float && is_integer_literal(expr))
}

/// An integer literal, or arithmetic on integer literals that stays `Int` when
/// checked again at `Float`: negation, `abs`, `+`, `-` and `*`, and an `if`
/// between such expressions. Division, remainder and power are not included:
/// `7 / 2` is integer division whatever its context.
fn is_integer_literal(expr: ExprRef<'_>) -> bool {
    match expr.view() {
        ExprView::Val(SyntaxLiteral::Int(_)) => true,
        ExprView::Neg(inner) | ExprView::Abs(inner) => is_integer_literal(inner),
        ExprView::BinOp(lhs, rhs, operator) => {
            matches!(
                operator,
                BinaryOperator::Add | BinaryOperator::Subtract | BinaryOperator::Multiply
            ) && is_integer_literal(lhs)
                && is_integer_literal(rhs)
        }
        ExprView::If(_, yes, no) => is_integer_literal(yes) && is_integer_literal(no),
        _ => false,
    }
}

fn require(actual: TCType, expected: &TCType, expr: ExprRef<'_>) -> Result<(), SemanticError> {
    unify(&actual, expected)
        .map(|_| ())
        .ok_or_else(|| mismatch(expr, expected, &actual))
}

/// Accept `node`, of type `actual`, where a value of `expected` is needed.
/// Where `actual` is `Any` and `expected` says what its values are, the
/// value is checked when it runs: the node is recorded with `expected`, and
/// elaboration wraps it in a run-time check. Every place checking lets an
/// `Any` stand for a concrete type comes through here, and only the pass
/// whose types are kept records anything.
fn accept_at_run_time(
    node: ExprRef<'_>,
    actual: &TCType,
    expected: &TCType,
    context: &mut TypeContext<'_>,
) {
    if *actual != TCType::Any || context.expr_types.is_none() || !is_checkable(expected) {
        return;
    }
    let expected = expected
        .to_stream_type()
        .expect("a checkable type is a stream type");
    context.runtime_checks.push((node.id(), expected));
}

/// Whether a value can be checked against `typ` when it runs: a scalar, or a
/// list, map, tuple or struct of checkable types or `Any`. A function, a
/// runtime-expression source, a union and an unresolved placeholder are not
/// checked.
fn is_checkable(typ: &TCType) -> bool {
    let part = |typ: &TCType| *typ == TCType::Any || is_checkable(typ);
    match typ {
        TCType::Int | TCType::Float | TCType::Str | TCType::Bool | TCType::Unit => true,
        TCType::List(inner) | TCType::Map(inner) => part(inner),
        TCType::Tuple(items) => items.iter().all(part),
        TCType::Struct(fields, _) => fields.iter().all(|(_, field)| part(field)),
        _ => false,
    }
}

/// The type a binary operator uses its operands at, where one of them is
/// `Any`: `Bool` for a boolean operator and `Str` for concatenation whatever
/// the other operand is, and otherwise the other operand's type. Two `Any`
/// operands of another operator say nothing about each other.
fn operand_type(operator: BinaryOperator, lhs: &TCType, rhs: &TCType) -> Option<TCType> {
    match operator.kind() {
        BinaryOperatorKind::Boolean(_) => Some(TCType::Bool),
        BinaryOperatorKind::String => Some(TCType::Str),
        _ => match (lhs, rhs) {
            (TCType::Any, TCType::Any) => None,
            (TCType::Any, other) | (other, TCType::Any) => Some(other.clone()),
            _ => None,
        },
    }
}

/// The run-time checks `context` recorded, as annotations of the nodes of
/// `nodes`, the storage they were recorded against.
fn finish_runtime_checks<'arena>(
    recorded: Vec<(ExprId, StreamType)>,
    builder: contiguous_tree::SparseNodeAnnotationsBuilder<ExprArena, StreamType>,
    nodes: impl IntoIterator<Item = ExprRef<'arena>>,
) -> RuntimeChecks {
    let mut builder = builder;
    if !recorded.is_empty() {
        // A node accepted more than once is checked at its latest type.
        let recorded: BTreeMap<ExprId, StreamType> = recorded.into_iter().collect();
        for node in nodes {
            if let Some(expected) = recorded.get(&node.id()) {
                builder
                    .insert(node, expected.clone())
                    .expect("a checked node belongs to the storage it was checked in");
            }
        }
    }
    builder.finish()
}

fn contains_unresolved_type(typ: &TCType) -> bool {
    match typ {
        TCType::Any | TCType::Unknown | TCType::EmptyList | TCType::EmptyMap => true,
        TCType::List(inner) | TCType::Map(inner) | TCType::Expr(inner) => {
            contains_unresolved_type(inner)
        }
        TCType::Tuple(items) => items.iter().any(contains_unresolved_type),
        TCType::Struct(fields, _) => fields
            .iter()
            .any(|(_, field)| contains_unresolved_type(field)),
        TCType::Function(arguments, result) => {
            arguments.iter().any(contains_unresolved_type) || contains_unresolved_type(result)
        }
        TCType::Union(schema) => schema.alternatives().iter().any(|alternative| {
            use crate::core::UnionPayload;
            match alternative.payload() {
                UnionPayload::Nullary => false,
                UnionPayload::Of(payload) => contains_unresolved_type(payload),
            }
        }),
        _ => false,
    }
}

fn mismatch(expr: ExprRef<'_>, expected: &TCType, actual: &TCType) -> SemanticError {
    let hint = match (expected, actual) {
        (TCType::Float, TCType::Int) => "; convert the Int explicitly with `as Float`",
        _ => "",
    };
    error(
        expr,
        TypeErrorKind::AnnotationTypeMismatch,
        format!("expected {expected}, got {actual}{hint}"),
    )
}

/// Check the value of struct field `name` of `expr` against the type the
/// field is expected to have, reporting a mismatch as one of that field.
fn check_field(
    expr: ExprRef<'_>,
    name: &str,
    value: ExprRef<'_>,
    expected: Option<&TCType>,
    context: &mut TypeContext<'_>,
) -> Result<TCType, SemanticError> {
    check(value, expected, context).map_err(|err| match err {
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
                    expected.expect("annotation mismatch needs an expectation")
                ),
            )
        }
        err => err,
    })
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
    SemanticError::type_error_at(kind, message.into(), expr.span()).located(expr.origin())
}
