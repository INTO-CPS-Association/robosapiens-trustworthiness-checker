//! Shared structural and lexical validation for DSRV specifications.
//!
//! Validation checks declarations, bindings and source scopes without inferring
//! types. The public validation stage locates its findings; `pipeline.rs`
//! runs it before type checking. Standalone expression checking uses the same
//! invariants.

use std::collections::BTreeSet;

use contiguous_tree::TreeCursorExt;

use crate::VarName;
use crate::lang::dsrv::ast::{
    Declaration, DsrvSpecification, ExprFieldRefs, ExprRef, ExprView, ReconfigurableExprScope,
    SyntaxLiteral, ValidatedDsrvSpecification,
};
use crate::lang::dsrv::diagnostics::{
    SemanticError, SemanticResult, TypeErrorKind, materialise_errors,
};
use crate::lang::dsrv::source_map::{NodeOrigin, SourceLocation};

struct AstValidationContext<'spec> {
    globals: &'spec BTreeSet<VarName>,
    bindings: Vec<VarName>,
    owner: Option<&'spec VarName>,
}

impl AstValidationContext<'_> {
    fn contains(&self, name: &VarName) -> bool {
        self.bindings.iter().rev().any(|bound| bound == name) || self.globals.contains(name)
    }
}

/// Validate source invariants before type checking.
pub(super) fn validate_specification(spec: &DsrvSpecification) -> SemanticResult<()> {
    let globals = spec
        .input_vars
        .iter()
        .chain(spec.exprs.keys())
        .chain(spec.type_annotations.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut errors = Vec::new();
    let mut declarations = std::collections::BTreeMap::new();
    for declaration in spec.declarations() {
        // Equations are checked against their forest; a duplicate type alias
        // is rejected when the namespace is built.
        let (Declaration::Input { name, span, .. }
        | Declaration::Output { name, span, .. }
        | Declaration::Aux { name, span, .. }) = declaration
        else {
            continue;
        };
        if let Some(first) = declarations.get(name) {
            // Declarations are the root file's.
            errors.push(SemanticError::DuplicateDeclaration {
                variable: name.clone(),
                first: *first,
                duplicate: *span,
                location: SourceLocation::pending(
                    NodeOrigin::new(spec.sources().root(), None),
                    *span,
                ),
            });
        } else {
            declarations.insert(name.clone(), *span);
        }
    }

    for (owner, expression) in spec.roots() {
        let mut context = AstValidationContext {
            globals: &globals,
            bindings: Vec::new(),
            owner: Some(owner),
        };
        if let Err(error) = validate_expression(expression, &mut context) {
            errors.push(error);
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Validate a runtime expression against its concrete stream environment.
pub(super) fn validate_standalone_expression(
    expression: ExprRef<'_>,
    globals: &BTreeSet<VarName>,
) -> SemanticResult<()> {
    let mut context = AstValidationContext {
        globals,
        bindings: Vec::new(),
        owner: None,
    };
    validate_expression(expression, &mut context).map_err(|error| vec![error])
}

pub fn validate(spec: DsrvSpecification) -> SemanticResult<ValidatedDsrvSpecification> {
    validate_specification(&spec).map_err(|mut errors| {
        materialise_errors(&mut errors, Some(spec.sources()));
        errors
    })?;
    Ok(ValidatedDsrvSpecification::new(spec))
}

fn validate_expression(
    expression: ExprRef<'_>,
    context: &mut AstValidationContext<'_>,
) -> Result<(), SemanticError> {
    use ExprView::*;

    match expression.view() {
        Val(SyntaxLiteral::NoVal) => Err(SemanticError::UnsupportedLiteral(
            "Deferred and NoVal are runtime states, not source literals".to_owned(),
            Some(expression.span()),
            SourceLocation::default(),
        )
        .located(expression.origin())),
        Var(var) | SIndex(var, _) if !context.contains(var) => {
            Err(SemanticError::UndeclaredVariable(
                format!("undeclared variable {var}"),
                Some(expression.span()),
                SourceLocation::default(),
            )
            .located(expression.origin()))
        }
        Lambda(parameters, body) => {
            let frame_start = context.bindings.len();
            context
                .bindings
                .extend(parameters.iter().map(|(name, _)| name.clone()));
            let result = validate_expression(body, context);
            context.bindings.truncate(frame_start);
            result
        }
        // What a pattern binds is in scope for that arm's guard and body,
        // and nowhere else.
        Match(scrutinee, arms, shape) => {
            validate_expression(scrutinee, context)?;
            let children: Vec<ExprRef<'_>> = arms.into_iter().collect();
            let mut place = 0;
            for arm in shape.iter() {
                let frame_start = context.bindings.len();
                context.bindings.extend(arm.pattern.bound_names());
                let mut result = Ok(());
                for _ in 0..arm.children() {
                    result = result.and(validate_expression(children[place], context));
                    place += 1;
                }
                context.bindings.truncate(frame_start);
                result?;
            }
            Ok(())
        }
        Matches(scrutinee, guard, pattern) => {
            validate_expression(scrutinee, context)?;
            let Some(guard) = guard.into_iter().next() else {
                return Ok(());
            };
            let frame_start = context.bindings.len();
            context.bindings.extend(pattern.bound_names());
            let result = validate_expression(guard, context);
            context.bindings.truncate(frame_start);
            result
        }
        Dynamic(source, _, scope) | Defer(source, _, scope) => {
            validate_runtime_scope(expression, scope, |variable| {
                if context.owner == Some(variable) {
                    Some(format!(
                        "runtime scope cannot contain its owning stream {variable}"
                    ))
                } else if context.bindings.contains(variable) {
                    // The runtime supplies a scope's streams, never a binder's value.
                    Some(format!(
                        "runtime scope names {variable}, a parameter or pattern binding; \
                         it can name only streams"
                    ))
                } else if !context.contains(variable) {
                    Some(format!(
                        "runtime scope contains unknown variable {variable}"
                    ))
                } else {
                    None
                }
            })?;
            validate_expression(source, context)
        }
        Map(fields) | Struct(fields) | ObjectLiteral(fields) => {
            validate_unique_fields(expression, &fields)?;
            validate_children(expression, context)
        }
        _ => validate_children(expression, context),
    }
}

fn validate_children(
    expression: ExprRef<'_>,
    context: &mut AstValidationContext<'_>,
) -> Result<(), SemanticError> {
    for child in expression.children() {
        validate_expression(child, context)?;
    }
    Ok(())
}

/// That `scope` names each variable once, and that `problem` has nothing to say of
/// any of them.
pub(super) fn validate_runtime_scope(
    expression: ExprRef<'_>,
    scope: &ReconfigurableExprScope,
    problem: impl Fn(&VarName) -> Option<String>,
) -> Result<(), SemanticError> {
    let ReconfigurableExprScope::Explicit(variables) = scope else {
        return Ok(());
    };
    let mut seen = BTreeSet::new();
    for variable in variables {
        let problem = if seen.insert(variable) {
            problem(variable)
        } else {
            Some(format!(
                "runtime scope contains duplicate variable {variable}"
            ))
        };
        if let Some(message) = problem {
            return Err(SemanticError::InvalidRuntimeScope(
                message,
                Some(expression.span()),
                SourceLocation::default(),
            )
            .located(expression.origin()));
        }
    }
    Ok(())
}

pub(super) fn validate_unique_fields(
    expression: ExprRef<'_>,
    fields: &ExprFieldRefs<'_>,
) -> Result<(), SemanticError> {
    if let Some(key) = fields.duplicate_key() {
        return Err(SemanticError::type_error_at(
            TypeErrorKind::DuplicateField,
            format!("expression contains duplicate field {key:?}"),
            expression.span(),
        )
        .located(expression.origin()));
    }
    Ok(())
}
