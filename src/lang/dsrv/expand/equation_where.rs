//! Lower equation-local scopes into hygienic equations on the enclosing clock.
//!
//! This precedes ordinary def inlining and stream-function instantiation. The
//! existing graft pass performs alpha-renaming with its lambda/pattern binder
//! rules; the ordinary equation pipeline retains typing, history and cycles.

use super::language::{Feature, LanguageConfig, LanguageError};
use super::{DsrvExpandError, inline};
use crate::VarName;
use crate::lang::dsrv::span::Span;
use crate::lang::dsrv::syntax::parsed::{
    ParsedDeclaration, ParsedExpr, ParsedExprBuilder, ParsedExprId, ParsedSpecification,
};
use contiguous_tree::TreeCursor;
use ecow::EcoVec;
use std::collections::{BTreeMap, BTreeSet};

fn equation(declaration: &ParsedDeclaration) -> &ParsedDeclaration {
    match declaration {
        ParsedDeclaration::EquationScope {
            equation: inner, ..
        } => equation(inner),
        other => other,
    }
}

fn name(declaration: &ParsedDeclaration) -> &VarName {
    match equation(declaration) {
        ParsedDeclaration::Aux(name, ..)
        | ParsedDeclaration::Output(name, ..)
        | ParsedDeclaration::Equation(name, ..) => name,
        _ => unreachable!("equation scopes contain stream equations"),
    }
}

/// Shared validation for def locals and equation locals, including split forms.
pub(super) fn validate_locals(
    locals: &[ParsedDeclaration],
    owner: Option<&VarName>,
    span: Span,
) -> Result<(), DsrvExpandError> {
    let mut declared = BTreeSet::new();
    let mut defined = BTreeSet::new();
    let invalid = |problem, span| DsrvExpandError::InvalidEquationWhere { problem, span };
    for local in locals {
        let (local_name, annotation, has_equation, site) = match equation(local) {
            ParsedDeclaration::Aux(name, _, root, span) => (name, true, root.is_some(), *span),
            ParsedDeclaration::Equation(name, _, span) => (name, false, true, *span),
            _ => unreachable!("local grammar admits only aux declarations and equations"),
        };
        if owner == Some(local_name) {
            return Err(invalid(
                format!("local stream `{local_name}` has its owning equation's name"),
                site,
            ));
        }
        if annotation && !declared.insert(local_name.clone()) {
            return Err(invalid(
                format!("local stream `{local_name}` is declared twice"),
                site,
            ));
        }
        if has_equation && !defined.insert(local_name.clone()) {
            return Err(invalid(
                format!("local stream `{local_name}` is defined twice"),
                site,
            ));
        }
    }
    for local_name in declared.difference(&defined) {
        return Err(invalid(
            format!("local stream `{local_name}` has no equation"),
            span,
        ));
    }
    Ok(())
}

pub(super) fn lower(
    parsed: ParsedSpecification,
    language: &LanguageConfig,
) -> Result<ParsedSpecification, DsrvExpandError> {
    fn scoped(declaration: &ParsedDeclaration) -> bool {
        match declaration {
            ParsedDeclaration::EquationScope { .. } => true,
            ParsedDeclaration::Def { locals, .. } => locals.iter().flatten().any(scoped),
            _ => false,
        }
    }
    if !parsed.declarations().iter().any(scoped) {
        return Ok(parsed);
    }
    let source = parsed.source();
    let held = parsed.roots();
    let mut trees = held.iter();
    let mut builder = ParsedExprBuilder::with_capacity(
        held.iter()
            .map(|tree| tree.as_ref().subtree_ids().len())
            .sum(),
    );
    let mut declarations = EcoVec::new();
    for declaration in parsed.declarations() {
        lower_declaration(
            declaration,
            &BTreeMap::new(),
            &mut trees,
            &mut builder,
            &mut declarations,
            language,
        )?;
    }
    let parsed = ParsedSpecification::new(builder, declarations)
        .map_err(|error| DsrvExpandError::Inlined(error.to_string()))?;
    Ok(match source {
        Some(source) => parsed.with_source(source),
        None => parsed,
    })
}

fn root<'a>(
    trees: &mut impl Iterator<Item = &'a ParsedExpr>,
    names: &BTreeMap<VarName, VarName>,
    builder: &mut ParsedExprBuilder,
) -> Result<ParsedExprId, DsrvExpandError> {
    inline::rename_tree(
        trees
            .next()
            .expect("one tree per declaration root")
            .as_ref(),
        names,
        builder,
    )
}

fn lower_declaration<'a>(
    declaration: &ParsedDeclaration,
    names: &BTreeMap<VarName, VarName>,
    trees: &mut impl Iterator<Item = &'a ParsedExpr>,
    builder: &mut ParsedExprBuilder,
    output: &mut EcoVec<ParsedDeclaration>,
    language: &LanguageConfig,
) -> Result<(), DsrvExpandError> {
    let renamed = |name: &VarName| names.get(name).unwrap_or(name).clone();
    let rewritten = match declaration {
        ParsedDeclaration::EquationScope {
            equation: owner,
            locals,
            span,
        } => {
            if !language.has(Feature::LocalStreams) {
                return Err(LanguageError::NeedsExperiment {
                    construct: "an equation where block",
                    feature: "local_streams",
                    span: *span,
                }
                .into());
            }
            validate_locals(locals, Some(name(owner)), *span)?;
            let owner_name = renamed(name(owner));
            let mut nested = names.clone();
            for local in locals {
                let local_name = name(local);
                super::language::check_declared_name(
                    "a local auxiliary stream",
                    &local_name.name(),
                    *span,
                    language,
                )?;
                nested.insert(
                    local_name.clone(),
                    VarName::from(format!("{owner_name}$where${local_name}")),
                );
            }
            // The owner's binding remains in its parent scope. Only its RHS
            // sees the nested locals; its declaration name must not be captured.
            let before = output.len();
            lower_declaration(owner, &nested, trees, builder, output, language)?;
            match &mut output.make_mut()[before] {
                ParsedDeclaration::Aux(name, ..)
                | ParsedDeclaration::Output(name, ..)
                | ParsedDeclaration::Equation(name, ..) => *name = owner_name,
                _ => unreachable!(),
            }
            let declared: BTreeSet<_> = locals
                .iter()
                .filter_map(|local| match equation(local) {
                    ParsedDeclaration::Aux(name, ..) => Some(name.clone()),
                    _ => None,
                })
                .collect();
            for local in locals {
                let before = output.len();
                lower_declaration(local, &nested, trees, builder, output, language)?;
                if !declared.contains(name(local))
                    && let ParsedDeclaration::Equation(name, root, span) = output[before].clone()
                {
                    output.make_mut()[before] =
                        ParsedDeclaration::Aux(name, None, Some(root), span);
                }
            }
            return Ok(());
        }
        ParsedDeclaration::Equation(name, _, span) => {
            ParsedDeclaration::Equation(renamed(name), root(trees, names, builder)?, *span)
        }
        ParsedDeclaration::Aux(name, ty, value, span) => ParsedDeclaration::Aux(
            renamed(name),
            ty.clone(),
            value.map(|_| root(trees, names, builder)).transpose()?,
            *span,
        ),
        ParsedDeclaration::Output(name, ty, value, span) => ParsedDeclaration::Output(
            renamed(name),
            ty.clone(),
            value.map(|_| root(trees, names, builder)).transpose()?,
            *span,
        ),
        ParsedDeclaration::Def {
            name,
            type_parameters,
            parameters,
            fixed,
            locals,
            result,
            internal,
            span,
            ..
        } => {
            let body = root(trees, names, builder)?;
            let locals = if let Some(locals) = locals {
                let mut flattened = EcoVec::new();
                for local in locals {
                    lower_declaration(local, names, trees, builder, &mut flattened, language)?;
                }
                Some(flattened)
            } else {
                None
            };
            ParsedDeclaration::Def {
                name: name.clone(),
                type_parameters: type_parameters.clone(),
                parameters: parameters.clone(),
                fixed: fixed.clone(),
                locals,
                result: result.clone(),
                body,
                internal: *internal,
                span: *span,
            }
        }
        ParsedDeclaration::Const {
            name,
            ty,
            internal,
            span,
            ..
        } => ParsedDeclaration::Const {
            name: name.clone(),
            ty: ty.clone(),
            internal: *internal,
            span: *span,
            body: root(trees, names, builder)?,
        },
        other => other.clone(),
    };
    output.push(rewritten);
    Ok(())
}
