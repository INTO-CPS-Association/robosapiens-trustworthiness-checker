//! Replacing a call to a `def` with the function's body.
//!
//! A def is inlined as an **immediate lambda application**: `f(a)` becomes
//! `(\p -> body)(a)`. Binding the parameters with a lambda rather than
//! substituting them avoids capture entirely — the lambda's own scope does
//! what a careful substitution would have had to.
//!
//! This runs on the parsed tree, before expansion resolves any name, which
//! is where the pipeline puts inlining. Each call site gets its own copy of
//! the body, so a def called twice is expanded twice.

use std::collections::BTreeMap;

use contiguous_tree::{TreeCursor, TreeNodeMut};
use ecow::EcoVec;

use crate::VarName;
use crate::lang::dsrv::path::{ModuleName, ValuePath};
use crate::lang::dsrv::source::{SourceType, SourceTypeKind, TypeName};

use super::super::syntax::parsed::{
    self, ParsedExpr, ParsedExprBuilder, ParsedExprId, ParsedExprKind, ParsedExprRef,
};

use super::super::syntax::{ParsedDeclaration, ParsedSpecification};
use super::DsrvExpandError;

/// A function as its declaration gave it, with its body wherever it lives:
/// this module's own forest, or a tree the function table owns.
#[derive(Clone)]
pub(crate) struct Def<'a> {
    pub(crate) parameters: EcoVec<(VarName, SourceType)>,
    /// Names bound by `<…>`. A parameter whose type mentions one cannot be
    /// ascribed at a call site, so it is left to inference.
    pub(crate) type_parameters: EcoVec<TypeName>,
    /// Where the body is. Grafting copies from here.
    pub(crate) body: ParsedExprRef<'a>,
    /// Whether the call site must take the body's spans or replace them.
    ///
    /// A body from another module carries that module's byte offsets, which
    /// mean nothing in this one, so those nodes take the call's span.
    pub(crate) foreign: bool,
}

impl Def<'_> {
    fn ascription(&self, ty: &SourceType) -> Option<SourceType> {
        (!self.mentions_a_type_parameter(ty)).then(|| ty.clone())
    }

    fn mentions_a_type_parameter(&self, ty: &SourceType) -> bool {
        let mentions = |ty| self.mentions_a_type_parameter(ty);
        match &ty.kind {
            SourceTypeKind::Named(path, arguments) => {
                (!path.is_qualified() && self.type_parameters.contains(path.name()))
                    || arguments.iter().any(mentions)
            }
            SourceTypeKind::List(ty) | SourceTypeKind::Map(ty) | SourceTypeKind::Expr(ty) => {
                mentions(ty)
            }
            SourceTypeKind::Tuple(types) => types.iter().any(mentions),
            SourceTypeKind::Struct(fields, _) => fields.iter().any(|(_, ty)| mentions(ty)),
            SourceTypeKind::Function(arguments, result) => {
                arguments.iter().any(mentions) || mentions(result)
            }
            SourceTypeKind::Union(alternatives) => alternatives
                .iter()
                .any(|alternative| alternative.payload.as_ref().is_some_and(mentions)),
            _ => false,
        }
    }
}

/// What a file's own `def` declarations say, before their bodies are known.
struct Declared {
    name: VarName,
    parameters: EcoVec<(VarName, SourceType)>,
    type_parameters: EcoVec<TypeName>,
    /// Which root of the file's forest holds the body.
    root: usize,
}

/// Inline every call to a `def` the file declares.
pub(crate) fn inline_functions(
    parsed: ParsedSpecification,
    imported: Scope<'_>,
) -> Result<ParsedSpecification, DsrvExpandError> {
    let mut declared: Vec<Declared> = Vec::new();
    let mut root = 0usize;
    for declaration in parsed.declarations() {
        match declaration {
            ParsedDeclaration::Def {
                name,
                type_parameters,
                parameters,
                ..
            } => {
                declared.push(Declared {
                    name: name.clone(),
                    parameters: parameters.clone(),
                    type_parameters: type_parameters.clone(),
                    root,
                });
                root += 1;
            }
            // Every declaration that owns a root, in the order
            // `ParsedSpecification::new` collects them.
            ParsedDeclaration::Equation(..)
            | ParsedDeclaration::Output(_, _, Some(_), _)
            | ParsedDeclaration::Aux(_, _, Some(_), _) => root += 1,
            _ => {}
        }
    }
    // A file that declares no def may still call an imported one.
    if declared.is_empty() && imported.bare.is_empty() && imported.qualified.is_empty() {
        return Ok(parsed);
    }

    let (forest, declarations) = parsed.into_parts();
    let node_count = forest.nodes().count();
    let held: Vec<_> = forest.into_roots().collect();
    // The handles stay alive here so every root cursor, including a def's
    // body, remains valid for the whole pass.
    let trees: Vec<ParsedExprRef<'_>> = held.iter().map(|tree| tree.as_ref()).collect();
    // Imports first, then this file's own defs, so a local name wins as it
    // does for types (S11).
    let mut scope = imported;
    for def in declared {
        scope.bare.insert(
            def.name,
            Def {
                parameters: def.parameters,
                type_parameters: def.type_parameters,
                body: trees[def.root],
                foreign: false,
            },
        );
    }

    let mut builder = ParsedExprBuilder::with_capacity(node_count);
    let mut roots = Vec::with_capacity(trees.len());
    for tree in &trees {
        let mut active = Vec::new();
        roots.push(graft(*tree, &scope, &mut builder, &mut active, None)?);
    }

    // Declarations name their root by id, so each takes the id its tree was
    // rewritten to, in the same order.
    let mut next = roots.into_iter();
    let declarations: EcoVec<ParsedDeclaration> = declarations
        .iter()
        .map(|declaration| match declaration.clone() {
            ParsedDeclaration::Equation(name, _, span) => {
                ParsedDeclaration::Equation(name, next.next().expect("one root each"), span)
            }
            ParsedDeclaration::Output(name, ty, Some(_), span) => {
                ParsedDeclaration::Output(name, ty, Some(next.next().expect("one root each")), span)
            }
            ParsedDeclaration::Aux(name, ty, Some(_), span) => {
                ParsedDeclaration::Aux(name, ty, Some(next.next().expect("one root each")), span)
            }
            ParsedDeclaration::Def {
                name,
                type_parameters,
                parameters,
                result,
                internal,
                span,
                ..
            } => ParsedDeclaration::Def {
                name,
                type_parameters,
                parameters,
                result,
                body: next.next().expect("one root each"),
                internal,
                span,
            },
            other => other,
        })
        .collect();
    ParsedSpecification::new(builder, declarations).map_err(|error| match error {
        super::super::syntax::DsrvSyntaxError::Ast(error) => DsrvExpandError::Ast(error),
        other => DsrvExpandError::Inlined(other.to_string()),
    })
}

/// What a call site may name: bare names, and defs reached through a
/// module.
#[derive(Default)]
pub(crate) struct Scope<'a> {
    pub(crate) bare: BTreeMap<VarName, Def<'a>>,
    pub(crate) qualified: BTreeMap<(Vec<ModuleName>, VarName), Def<'a>>,
}

impl<'a> Scope<'a> {
    fn bare(&self, name: &VarName) -> Option<&Def<'a>> {
        self.bare.get(name)
    }

    fn through_module(&self, path: &ValuePath) -> Option<&Def<'a>> {
        self.qualified
            .get(&(path.module().to_vec(), path.name().clone()))
    }
}

/// One inlined body as a tree of its own, which a caller can copy.
pub(crate) fn standalone(
    cursor: ParsedExprRef<'_>,
    scope: &Scope<'_>,
) -> Result<ParsedExpr, DsrvExpandError> {
    let mut builder = ParsedExprBuilder::with_capacity(cursor.subtree_ids().len());
    let mut active = Vec::new();
    let root = graft(cursor, scope, &mut builder, &mut active, None)?;
    let forest = builder
        .finish_forest([root])
        .map_err(|error| DsrvExpandError::Inlined(error.to_string()))?;
    Ok(forest.into_roots().next().expect("one body makes one tree"))
}

/// Copy a subtree into the builder, inlining any call to a def as it goes.
///
/// The builder requires a node's children to be the trailing roots, so this
/// descends: each child is allocated immediately before its parent, and an
/// inlined call allocates its lambda before its arguments because the
/// function is `Apply`'s first child.
fn graft(
    cursor: ParsedExprRef<'_>,
    scope: &Scope<'_>,
    builder: &mut ParsedExprBuilder,
    active: &mut Vec<VarName>,
    stamp: Option<crate::lang::dsrv::span::Span>,
) -> Result<ParsedExprId, DsrvExpandError> {
    if let ParsedExprKind::Apply(function, arguments) = cursor.kind()
        && let ParsedExprKind::Var(name) = cursor.child(*function).kind()
        && scope.bare(name).is_some()
    {
        let name = name.clone();
        let arguments = arguments.clone();
        return inline_call(&name, cursor, &arguments, scope, builder, active, stamp);
    }
    if let ParsedExprKind::Apply(function, arguments) = cursor.kind()
        && let ParsedExprKind::ModuleItem(path) = cursor.child(*function).kind()
    {
        let path = path.clone();
        let arguments = arguments.clone();
        let def = scope
            .through_module(&path)
            .ok_or_else(|| DsrvExpandError::UnknownFunction {
                name: path.to_string(),
            })?;
        let named = path.to_string();
        return build_call(
            def, None, named, cursor, &arguments, scope, builder, active, stamp,
        );
    }
    let mut grafted = Vec::new();
    for child in cursor.child_ids() {
        grafted.push(graft(cursor.child(child), scope, builder, active, stamp)?);
    }
    let mut kind = cursor.kind().clone();
    let mut next = grafted.into_iter();
    kind.for_each_child_id_mut(|child| {
        *child = next.next().expect("one grafted child per child id");
    });
    Ok(builder.alloc(kind, stamp.unwrap_or_else(|| parsed::span_of(cursor))))
}

/// Build `(\p… -> body)(a…)` for a call to a def named without a qualifier.
fn inline_call(
    name: &VarName,
    call: ParsedExprRef<'_>,
    arguments: &EcoVec<ParsedExprId>,
    scope: &Scope<'_>,
    builder: &mut ParsedExprBuilder,
    active: &mut Vec<VarName>,
    stamp: Option<crate::lang::dsrv::span::Span>,
) -> Result<ParsedExprId, DsrvExpandError> {
    let def = scope.bare(name).expect("the caller matched on it");
    // Only a def of this file can recurse: a def from the table was already
    // inlined when its entry was built, so its body calls nothing.
    if !def.foreign && active.contains(name) {
        return Err(DsrvExpandError::RecursiveFunction {
            name: name.to_string(),
        });
    }
    build_call(
        def,
        (!def.foreign).then(|| name.clone()),
        name.to_string(),
        call,
        arguments,
        scope,
        builder,
        active,
        stamp,
    )
}

/// Build `(\p… -> body)(a…)` once the def is in hand.
#[allow(clippy::too_many_arguments)]
fn build_call(
    def: &Def<'_>,
    guard: Option<VarName>,
    named: String,
    call: ParsedExprRef<'_>,
    arguments: &EcoVec<ParsedExprId>,
    scope: &Scope<'_>,
    builder: &mut ParsedExprBuilder,
    active: &mut Vec<VarName>,
    stamp: Option<crate::lang::dsrv::span::Span>,
) -> Result<ParsedExprId, DsrvExpandError> {
    if def.parameters.len() != arguments.len() {
        return Err(DsrvExpandError::FunctionArity {
            name: named,
            expected: def.parameters.len(),
            found: arguments.len(),
        });
    }
    let span = stamp.unwrap_or_else(|| parsed::span_of(call));

    // `Apply`'s first child is the function, so the lambda is allocated
    // before the arguments. A body from another module takes this call's
    // span throughout, since its own offsets belong to a different file.
    let inner = if def.foreign { Some(span) } else { stamp };
    if let Some(guard) = &guard {
        active.push(guard.clone());
    }
    let body = graft(def.body, scope, builder, active, inner)?;
    if guard.is_some() {
        active.pop();
    }
    let parameters: EcoVec<(VarName, Option<SourceType>)> = def
        .parameters
        .iter()
        .map(|(parameter, ty)| (parameter.clone(), def.ascription(ty)))
        .collect();
    let lambda = builder.alloc(ParsedExprKind::Lambda(parameters, body), span);

    let mut grafted = EcoVec::new();
    for argument in arguments {
        grafted.push(graft(call.child(*argument), scope, builder, active, stamp)?);
    }
    Ok(builder.alloc(ParsedExprKind::Apply(lambda, grafted), span))
}
