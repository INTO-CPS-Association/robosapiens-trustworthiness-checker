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
//!
//! A body from another module is reported at the call: its nodes take the
//! call's span, since their own offsets belong to a different file. Each
//! such node records where it was written as its definition site, which
//! survives further inlining unchanged, so code reached through several
//! modules names the module that wrote it. Arguments are the caller's
//! text and keep the caller's provenance.
//!
//! Such a body also keeps its lexical environment. It was inlined where it
//! was written, so nothing the caller can name reaches into it: a caller's
//! constant or def of the same name as something in the body is not
//! substituted there. Each of its nodes records the environment it was
//! written in, which survives further inlining as the definition site does,
//! and expansion reads that node in that environment. Arguments are read in
//! the caller's.

use std::collections::BTreeMap;

use contiguous_tree::{TreeCursor, TreeNodeMut};
use ecow::EcoVec;

use crate::VarName;
use crate::lang::dsrv::path::{ModuleName, ValuePath};
use crate::lang::dsrv::source::{SourceType, SourceTypeKind, TypeName};
use crate::lang::dsrv::source_map::{SourceId, SourceSite};
use crate::lang::dsrv::span::Span;

use super::functions::LexicalId;

use super::super::syntax::parsed::{
    self, ParsedExpr, ParsedExprBuilder, ParsedExprId, ParsedExprKind, ParsedExprRef, ParsedOrigin,
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
    /// The archived file that declared the def, which a foreign body's
    /// nodes name as their definition site.
    pub(crate) source: Option<SourceId>,
    /// The environment a foreign body was written in, which its nodes are
    /// read in. `None` for a def of the text being inlined into.
    pub(crate) lexical: Option<LexicalId>,
}

/// How a subtree grafted from another module's def is placed: at the
/// call's span, remembering the file and environment it was written in.
#[derive(Clone, Copy)]
struct Stamp {
    span: Span,
    definition: Option<SourceId>,
    lexical: Option<LexicalId>,
}

impl Stamp {
    /// Where the copy of `cursor` is placed.
    fn place(stamp: Option<Self>, cursor: ParsedExprRef<'_>) -> ParsedOrigin {
        let origin = parsed::origin_of(cursor);
        match stamp {
            None => origin,
            Some(stamp) => ParsedOrigin {
                span: stamp.span,
                // Code inlined into the def was written elsewhere again,
                // and keeps naming where.
                definition: origin.definition.or_else(|| {
                    stamp
                        .definition
                        .map(|source| SourceSite::new(source, origin.span))
                }),
                lexical: origin.lexical.or(stamp.lexical),
            },
        }
    }
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
            | ParsedDeclaration::Const { .. }
            | ParsedDeclaration::Output(_, _, Some(_), _)
            | ParsedDeclaration::Aux(_, _, Some(_), _) => root += 1,
            _ => {}
        }
    }
    // A file that declares no def may still call an imported one, and a
    // file that declares neither may still name a constant.
    let names_nothing = declared.is_empty()
        && imported.bare.is_empty()
        && imported.qualified.is_empty()
        && imported
            .constants
            .is_none_or(super::constants::Constants::is_empty);
    if names_nothing {
        return Ok(parsed);
    }

    let (forest, declarations, source) = parsed.into_parts();
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
                source: None,
                lexical: None,
            },
        );
    }

    let mut builder = ParsedExprBuilder::with_capacity(node_count);
    let mut roots = Vec::with_capacity(trees.len());
    for tree in &trees {
        let mut active = Vec::new();
        roots.push(graft(
            *tree,
            &scope,
            &mut builder,
            &mut active,
            &mut Vec::new(),
            None,
        )?);
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
            ParsedDeclaration::Const {
                name,
                ty,
                internal,
                span,
                ..
            } => ParsedDeclaration::Const {
                name,
                ty,
                body: next.next().expect("one root each"),
                internal,
                span,
            },
            other => other,
        })
        .collect();
    let inlined = ParsedSpecification::new(builder, declarations).map_err(|error| match error {
        super::super::syntax::DsrvSyntaxError::Ast(error) => DsrvExpandError::Ast(error),
        other => DsrvExpandError::Inlined(other.to_string()),
    })?;
    Ok(match source {
        Some(source) => inlined.with_source(source),
        None => inlined,
    })
}

/// What a call site may name: bare names, and defs reached through a
/// module.
#[derive(Default)]
pub(crate) struct Scope<'a> {
    /// What a bare or qualified name stands for, where it names a constant
    /// rather than a stream. A constant is a value, not a tree, so it is
    /// written in as a literal rather than grafted.
    pub(crate) constants: Option<&'a super::constants::Constants>,
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
    bound: impl IntoIterator<Item = VarName>,
) -> Result<ParsedExpr, DsrvExpandError> {
    let mut builder = ParsedExprBuilder::with_capacity(cursor.subtree_ids().len());
    let mut active = Vec::new();
    let mut bound = bound.into_iter().collect();
    let root = graft(cursor, scope, &mut builder, &mut active, &mut bound, None)?;
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
    bound: &mut Vec<VarName>,
    stamp: Option<Stamp>,
) -> Result<ParsedExprId, DsrvExpandError> {
    if let ParsedExprKind::Apply(function, arguments) = cursor.kind()
        && let ParsedExprKind::Var(name) = cursor.child(*function).kind()
        && scope.bare(name).is_some()
    {
        let name = name.clone();
        let arguments = arguments.clone();
        return inline_call(
            &name, cursor, &arguments, scope, builder, active, bound, stamp,
        );
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
            def, None, named, cursor, &arguments, scope, builder, active, bound, stamp,
        );
    }
    // A constant is a value, so it replaces the name with a literal leaf.
    // Allocating a leaf keeps the trailing-roots discipline trivially.
    if let Some(constants) = scope.constants {
        if let ParsedExprKind::Var(name) = cursor.kind()
            && !bound.contains(name)
            && let Some(value) = constants.bare(name)
        {
            return Ok(builder.alloc(
                ParsedExprKind::Val(value.clone()),
                Stamp::place(stamp, cursor),
            ));
        }
        if let ParsedExprKind::ModuleItem(path) = cursor.kind()
            && let Some(value) = constants.through_module(path.module(), path.name())
        {
            return Ok(builder.alloc(
                ParsedExprKind::Val(value.clone()),
                Stamp::place(stamp, cursor),
            ));
        }
        // An offset is a number rather than an expression, so a constant
        // standing in for one is resolved to the number itself.
        if let ParsedExprKind::SIndex(input, parsed::SourceOffset::Named(path)) = cursor.kind() {
            let span = parsed::span_of(cursor);
            let found = if path.module().is_empty() {
                constants.bare(path.name())
            } else {
                constants.through_module(path.module(), path.name())
            };
            if let Some(value) = found {
                let offset = offset_of(value, path, span)?;
                let input = graft(cursor.child(*input), scope, builder, active, bound, stamp)?;
                return Ok(builder.alloc(
                    ParsedExprKind::SIndex(input, parsed::SourceOffset::Literal(offset)),
                    Stamp::place(stamp, cursor),
                ));
            }
        }
    }
    if let ParsedExprKind::Lambda(parameters, body) = cursor.kind() {
        let count = bound.len();
        bound.extend(parameters.iter().map(|(name, _)| name.clone()));
        let body = graft(cursor.child(*body), scope, builder, active, bound, stamp)?;
        bound.truncate(count);
        return Ok(builder.alloc(
            ParsedExprKind::Lambda(parameters.clone(), body),
            Stamp::place(stamp, cursor),
        ));
    }
    if let ParsedExprKind::Match(scrutinee, arms, shape) = cursor.kind() {
        let scrutinee = graft(
            cursor.child(*scrutinee),
            scope,
            builder,
            active,
            bound,
            stamp,
        )?;
        let mut source_arms = arms.iter();
        let mut grafted_arms = EcoVec::new();
        for arm in shape {
            let count = bound.len();
            bound.extend(arm.pattern.bound_names());
            for _ in 0..arm.children() {
                let child = source_arms.next().expect("match shape covers its children");
                grafted_arms.push(graft(
                    cursor.child(*child),
                    scope,
                    builder,
                    active,
                    bound,
                    stamp,
                )?);
            }
            bound.truncate(count);
        }
        return Ok(builder.alloc(
            ParsedExprKind::Match(scrutinee, grafted_arms, shape.clone()),
            Stamp::place(stamp, cursor),
        ));
    }
    if let ParsedExprKind::Matches(scrutinee, guard, pattern) = cursor.kind() {
        let scrutinee = graft(
            cursor.child(*scrutinee),
            scope,
            builder,
            active,
            bound,
            stamp,
        )?;
        let count = bound.len();
        bound.extend(pattern.bound_names());
        let guard = guard
            .iter()
            .map(|child| graft(cursor.child(*child), scope, builder, active, bound, stamp))
            .collect::<Result<EcoVec<_>, _>>()?;
        bound.truncate(count);
        return Ok(builder.alloc(
            ParsedExprKind::Matches(scrutinee, guard, pattern.clone()),
            Stamp::place(stamp, cursor),
        ));
    }
    let mut grafted = Vec::new();
    for child in cursor.child_ids() {
        grafted.push(graft(
            cursor.child(child),
            scope,
            builder,
            active,
            bound,
            stamp,
        )?);
    }
    let mut kind = cursor.kind().clone();
    let mut next = grafted.into_iter();
    kind.for_each_child_id_mut(|child| {
        *child = next.next().expect("one grafted child per child id");
    });
    Ok(builder.alloc(kind, Stamp::place(stamp, cursor)))
}

/// The number an offset's constant stands for, which must be a count.
fn offset_of(
    value: &crate::lang::dsrv::ast::SyntaxLiteral,
    path: &ValuePath,
    span: crate::lang::dsrv::span::Span,
) -> Result<u64, DsrvExpandError> {
    match value {
        crate::lang::dsrv::ast::SyntaxLiteral::Int(offset) if *offset >= 0 => Ok(*offset as u64),
        _ => Err(DsrvExpandError::ConstantOffset {
            name: path.to_string(),
            span,
        }),
    }
}

/// Build `(\p… -> body)(a…)` for a call to a def named without a qualifier.
fn inline_call(
    name: &VarName,
    call: ParsedExprRef<'_>,
    arguments: &EcoVec<ParsedExprId>,
    scope: &Scope<'_>,
    builder: &mut ParsedExprBuilder,
    active: &mut Vec<VarName>,
    bound: &mut Vec<VarName>,
    stamp: Option<Stamp>,
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
        bound,
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
    bound: &mut Vec<VarName>,
    stamp: Option<Stamp>,
) -> Result<ParsedExprId, DsrvExpandError> {
    if def.parameters.len() != arguments.len() {
        return Err(DsrvExpandError::FunctionArity {
            name: named,
            expected: def.parameters.len(),
            found: arguments.len(),
        });
    }
    // The lambda and application stand for the call itself.
    let at_call = Stamp::place(stamp, call);

    // `Apply`'s first child is the function, so the lambda is allocated
    // before the arguments. A body from another module takes this call's
    // span throughout, since its own offsets belong to a different file.
    let inner = if def.foreign {
        Some(Stamp {
            span: at_call.span,
            definition: def.source,
            lexical: def.lexical,
        })
    } else {
        stamp
    };
    // A foreign body was inlined where it was written, so the caller's
    // names must not reach into it.
    let written = Scope::default();
    let body_scope = if def.foreign { &written } else { scope };
    if let Some(guard) = &guard {
        active.push(guard.clone());
    }
    let mut body_bound = def
        .parameters
        .iter()
        .map(|(name, _)| name.clone())
        .collect();
    let body = graft(
        def.body,
        body_scope,
        builder,
        active,
        &mut body_bound,
        inner,
    )?;
    if guard.is_some() {
        active.pop();
    }
    let parameters: EcoVec<(VarName, Option<SourceType>)> = def
        .parameters
        .iter()
        .map(|(parameter, ty)| (parameter.clone(), def.ascription(ty)))
        .collect();
    // The parameters and their types were written with the body, so the
    // lambda binding them is read where the body is, though it stands at
    // the call.
    let lambda = builder.alloc(
        ParsedExprKind::Lambda(parameters, body),
        ParsedOrigin {
            lexical: def.lexical.or(at_call.lexical),
            ..at_call
        },
    );

    let mut grafted = EcoVec::new();
    for argument in arguments {
        grafted.push(graft(
            call.child(*argument),
            scope,
            builder,
            active,
            bound,
            stamp,
        )?);
    }
    Ok(builder.alloc(ParsedExprKind::Apply(lambda, grafted), at_call))
}
