//! Instantiate source types with supplied checker-type bindings.
//!
//! Translation resolves source aliases, substitutes supplied types and tracks
//! active alias paths. Generic calls bind names directly to concrete types.

use crate::core::StreamType;
use crate::core::{ClosedUnion, UnionAlternative, UnionPayload};
use crate::lang::dsrv::path::TypePath;
use crate::lang::dsrv::source::{SourceContext, SourceType, SourceTypeKind, TypeName};
use imbl::OrdMap;

/// The type `source` names in `definition_context`, with each type parameter
/// in it replaced by its type in `instance`: a generic `def`'s parameters,
/// or a generic alias's while its body is expanded. `None` where it names no
/// type.
pub(super) fn instantiate(
    definition_context: &SourceContext,
    source: &SourceType,
    instance: &OrdMap<TypeName, StreamType>,
    active: &mut Vec<TypePath>,
) -> Option<StreamType> {
    if let SourceTypeKind::Named(path, arguments) = &source.kind
        && !arguments.is_empty()
    {
        return instantiate_alias(definition_context, path, arguments, instance, active);
    }
    let mut each = |source: &SourceType| instantiate(definition_context, source, instance, active);
    Some(match &source.kind {
        SourceTypeKind::Named(path, arguments) if arguments.is_empty() => {
            match (!path.is_qualified())
                .then(|| instance.get(path.name()))
                .flatten()
            {
                Some(typ) => typ.clone(),
                None => definition_context.get(path)?.clone(),
            }
        }
        SourceTypeKind::Named(..) => unreachable!("applied above"),
        SourceTypeKind::Int => StreamType::Int,
        SourceTypeKind::Float => StreamType::Float,
        SourceTypeKind::Str => StreamType::Str,
        SourceTypeKind::Bool => StreamType::Bool,
        SourceTypeKind::Unit => StreamType::Unit,
        SourceTypeKind::List(inner) => StreamType::List(Box::new(each(inner)?)),
        SourceTypeKind::Map(inner) => StreamType::Map(Box::new(each(inner)?)),
        SourceTypeKind::Expr(inner) => StreamType::Expr(Box::new(each(inner)?)),
        SourceTypeKind::Tuple(items) => {
            StreamType::Tuple(items.iter().map(each).collect::<Option<_>>()?)
        }
        SourceTypeKind::Struct(fields, open) => StreamType::Struct(
            fields
                .iter()
                .map(|(name, field)| Some((name.clone(), each(field)?)))
                .collect::<Option<_>>()?,
            *open,
        ),
        SourceTypeKind::Function(arguments, result) => StreamType::Function(
            arguments.iter().map(&mut each).collect::<Option<_>>()?,
            Box::new(each(result)?),
        ),
        SourceTypeKind::Union(alternatives) => {
            let alternatives = alternatives
                .iter()
                .map(|alternative| {
                    Some(UnionAlternative::new(
                        alternative.tag.clone(),
                        match &alternative.payload {
                            None => UnionPayload::Nullary,
                            Some(payload) => UnionPayload::Of(each(payload)?),
                        },
                    ))
                })
                .collect::<Option<Vec<_>>>()?;
            StreamType::Union(ClosedUnion::new(alternatives).ok()?)
        }
    })
}

/// A generic alias, `path`, applied to `arguments`, which may name the type
/// parameters in `instance`.
fn instantiate_alias(
    definition_context: &SourceContext,
    path: &TypePath,
    arguments: &[SourceType],
    instance: &OrdMap<TypeName, StreamType>,
    active: &mut Vec<TypePath>,
) -> Option<StreamType> {
    let alias = generic_alias(definition_context, path)?;
    if alias.parameters.len() != arguments.len() || active.contains(path) {
        return None;
    }
    // The arguments are types of the use site; the alias's body sees
    // only its own parameters, bound to them.
    let arguments = alias
        .parameters
        .iter()
        .cloned()
        .zip(arguments)
        .map(|(parameter, argument)| {
            let argument = instantiate(definition_context, argument, instance, active)?;
            Some((parameter, argument))
        })
        .collect::<Option<OrdMap<_, _>>>()?;
    active.push(path.clone());
    let typ = instantiate(definition_context, &alias.ty, &arguments, active);
    active.pop();
    typ
}

/// Match declared type parameters directly against a known concrete type.
/// Existing bindings are retained; ordinary admission checks repeated occurrences.
pub(super) fn match_concrete(
    context: &SourceContext,
    source: &SourceType,
    concrete: &StreamType,
    parameters: &[TypeName],
    instance: &mut OrdMap<TypeName, StreamType>,
) -> Result<(), String> {
    check_alias_cycles(context, source, &mut Vec::new())?;
    match_inner(
        context,
        source,
        concrete,
        parameters,
        instance,
        &OrdMap::new(),
    )
}

fn match_inner(
    context: &SourceContext,
    source: &SourceType,
    concrete: &StreamType,
    parameters: &[TypeName],
    instance: &mut OrdMap<TypeName, StreamType>,
    templates: &OrdMap<TypeName, SourceType>,
) -> Result<(), String> {
    if let SourceTypeKind::Named(path, arguments) = &source.kind {
        if !path.is_qualified() && arguments.is_empty() {
            if let Some(template) = templates.get(path.name()) {
                return match_inner(
                    context,
                    template,
                    concrete,
                    parameters,
                    instance,
                    &OrdMap::new(),
                );
            }
            if parameters.contains(path.name()) {
                instance
                    .entry(path.name().clone())
                    .or_insert_with(|| concrete.clone());
                return Ok(());
            }
        }
        if !arguments.is_empty() {
            let alias = generic_alias(context, path)
                .ok_or_else(|| format!("unknown generic type {path}"))?;
            let arguments: ecow::EcoVec<_> = arguments
                .iter()
                .map(|argument| substitute_template(argument, templates))
                .collect();
            if alias.parameters.len() != arguments.len() {
                return Err(format!("cannot instantiate generic alias {path}"));
            }
            let bindings = alias.parameters.iter().cloned().zip(arguments).collect();
            return match_inner(
                context, &alias.ty, concrete, parameters, instance, &bindings,
            );
        }
    }
    // Only matching constructors reveal nested parameters; shape compatibility
    // remains the responsibility of the instantiated signature's admission.
    let mut each = |source: &SourceType, concrete: &StreamType| {
        match_inner(context, source, concrete, parameters, instance, templates)
    };
    match (&source.kind, concrete) {
        (SourceTypeKind::List(a), StreamType::List(b))
        | (SourceTypeKind::Map(a), StreamType::Map(b))
        | (SourceTypeKind::Expr(a), StreamType::Expr(b)) => each(a, b)?,
        (SourceTypeKind::Tuple(a), StreamType::Tuple(b)) if a.len() == b.len() => {
            for (a, b) in a.iter().zip(b) {
                each(a, b)?;
            }
        }
        (SourceTypeKind::Struct(a, _), StreamType::Struct(b, _)) => {
            for (name, a) in a {
                if let Some((_, b)) = b.iter().find(|(field, _)| field == name) {
                    each(a, b)?;
                }
            }
        }
        (SourceTypeKind::Function(a, r), StreamType::Function(b, s)) if a.len() == b.len() => {
            for (a, b) in a.iter().zip(b) {
                each(a, b)?;
            }
            each(r, s)?;
        }
        (SourceTypeKind::Union(a), StreamType::Union(b)) => {
            for a in a {
                if let (Some(a), Some((_, b))) = (&a.payload, b.alternative(&a.tag)) {
                    if let UnionPayload::Of(b) = b.payload() {
                        each(a, b)?;
                    }
                }
            }
        }
        _ => {}
    }
    Ok(())
}

/// Expand arguments before entering the alias's parameter namespace.
fn substitute_template(source: &SourceType, bindings: &OrdMap<TypeName, SourceType>) -> SourceType {
    let borrowed = bindings.iter().collect();
    crate::lang::dsrv::source::substitute_source(source, &borrowed)
}

/// Known callback inputs can contextualize a lambda before its result parameter
/// is bound. This never creates a partially known checker type.
pub(super) fn callback_context(
    context: &SourceContext,
    source: &SourceType,
    instance: &OrdMap<TypeName, StreamType>,
    active: &mut Vec<TypePath>,
) -> Option<(ecow::EcoVec<StreamType>, Option<StreamType>)> {
    match &source.kind {
        SourceTypeKind::Function(inputs, result) => Some((
            inputs
                .iter()
                .map(|input| instantiate(context, input, instance, &mut Vec::new()))
                .collect::<Option<_>>()?,
            instantiate(context, result, instance, &mut Vec::new()),
        )),
        SourceTypeKind::Named(path, args) if !args.is_empty() && !active.contains(path) => {
            let alias = generic_alias(context, path)?;
            if alias.parameters.len() != args.len() {
                return None;
            }
            let bindings = alias.parameters.iter().zip(args).collect();
            let expanded = crate::lang::dsrv::source::substitute_source(&alias.ty, &bindings);
            active.push(path.clone());
            let result = callback_context(context, &expanded, instance, active);
            active.pop();
            result
        }
        _ => None,
    }
}

fn generic_alias<'a>(
    context: &'a SourceContext,
    path: &TypePath,
) -> Option<&'a crate::lang::dsrv::source::AliasDeclaration> {
    context.generic().get(path).or_else(|| {
        (!path.is_qualified())
            .then(|| {
                context
                    .generic()
                    .iter()
                    .find(|(name, _)| name.name() == path.name())
                    .map(|(_, alias)| alias)
            })
            .flatten()
    })
}

/// Generic signatures can mention aliases before a concrete source resolver
/// visits them. Guard declaration paths, independently of supplied arguments:
/// growing recursive applications must not evade cycle detection.
fn check_alias_cycles(
    context: &SourceContext,
    source: &SourceType,
    active: &mut Vec<TypePath>,
) -> Result<(), String> {
    let mut each = |source: &SourceType| check_alias_cycles(context, source, active);
    match &source.kind {
        SourceTypeKind::Named(path, arguments) => {
            for argument in arguments {
                each(argument)?;
            }
            if !arguments.is_empty() {
                let alias = generic_alias(context, path)
                    .ok_or_else(|| format!("unknown generic type {path}"))?;
                if active.contains(path) {
                    return Err(format!(
                        "cannot instantiate generic alias {path}: cyclic definition"
                    ));
                }
                active.push(path.clone());
                let result = check_alias_cycles(context, &alias.ty, active);
                active.pop();
                result?;
            }
        }
        SourceTypeKind::List(inner) | SourceTypeKind::Map(inner) | SourceTypeKind::Expr(inner) => {
            each(inner)?
        }
        SourceTypeKind::Tuple(items) => {
            for item in items {
                each(item)?;
            }
        }
        SourceTypeKind::Struct(fields, _) => {
            for (_, field) in fields {
                each(field)?;
            }
        }
        SourceTypeKind::Function(inputs, result) => {
            for input in inputs {
                each(input)?;
            }
            each(result)?;
        }
        SourceTypeKind::Union(alternatives) => {
            for alternative in alternatives {
                if let Some(payload) = &alternative.payload {
                    each(payload)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}
