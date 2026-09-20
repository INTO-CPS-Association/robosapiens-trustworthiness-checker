//! Expansion: the parsed tree becomes the core specification.
//!
//! The DSRV front end runs in three stages. [`super::syntax`] parses source
//! text into a parsed tree and resolves no names. Expansion, this module,
//! turns that tree into a [`DsrvSpecification`]. Validation and type checking
//! ([`super::type_checker`]) then judge the specification. Expansion is the
//! only stage that reads the language settings, and the only one that sees
//! source syntax; everything after it works on the core AST. Today the
//! conversion is one core node per parsed node; language features that
//! rewrite syntax will do so here, which is why this stage, not the parser,
//! owns it.
//!
//! # Steps
//!
//! [`expand_specification`] runs them in this order, and each failure is a
//! [`DsrvExpandError`]:
//!
//! 1. **Language.** The header lines (`language`, `edition`) and any request
//!    from outside the file resolve to one [`language::LanguageConfig`]
//!    ([`language::resolve_language`]).
//! 2. **Namespace.** The type aliases resolve into a [`SourceContext`], which
//!    also carries the language settings. Every expression expanded here, and
//!    every runtime expression expanded later against this context, shares it.
//! 3. **Declarations and equations** ([`expand_declarations`]). Each
//!    top-level form becomes a [`Declaration`] with its types resolved, in
//!    source order. The header produces none: it lives on in the context. The
//!    expression of each equation `x = e` is converted into core nodes, with
//!    types resolved against the namespace, into one shared builder.
//! 4. **Assembling the specification** (below).
//! 5. **Dialect.** The distribution primitives are rejected outside Distributed
//!    DSRV, and a Core specification must be Core throughout
//!    ([`language::CoreDsrvSpecification::check`]).
//!
//! [`expand_expression`] expands one runtime expression (the source of a
//! `dynamic` or `defer`) against an existing namespace, with the same
//! conversion and the same dialect checks, and no declarations.
//!
//! # Assembling the specification
//!
//! [`assemble_specification`] turns the declarations and the converted
//! equations into the specification itself:
//!
//! - **Indexes.** The input and output sets, the aux list and the map of type
//!   annotations are derived from the `in`, `out` and `aux` declarations.
//!   Equations and type aliases add nothing to them.
//! - **The forest.** The equations' expressions, given as roots in the order
//!   of the equations, become one expression forest: each tree is stored once
//!   and shared by everything that later reads the specification.
//! - **Pairing** ([`UnvalidatedDsrvSpecification::validate`]). Each equation
//!   is paired with its tree by stream name. A stream with two equations, or
//!   an object with a repeated field, is rejected here, because the
//!   specification cannot represent either.
//! - **Declarations are kept in source order,** type aliases included, so the
//!   specification prints back in the order it was written.
//!
//! Assembling checks only what the specification's representation needs.
//! Whether each stream is declared once, whether every variable is declared,
//! and whether equations fit their types are validation's and the type
//! checker's questions, asked of the assembled specification.

pub(crate) mod language;

use std::collections::{BTreeMap, BTreeSet};

use ecow::EcoVec;

use super::ast::AstShared as Rc;
use super::ast::{
    Declaration, DsrvAstError, Expr, ExprBuilder, ExprId, ExprKind, ExprMetadata,
    UnvalidatedDsrvSpecification,
};
use super::source::SourceType;
use super::source::{SourceContext, SourceResolveError};
use super::syntax::parsed::{self, ParsedExprKind, ParsedExprRef};
use super::syntax::{ParsedDeclaration, ParsedExpr, ParsedSpecification, SourceAscription};
use crate::core::StreamType;
use crate::core::StreamTypeAscription;
use crate::lang::dsrv::ast::DsrvSpecification;
use contiguous_tree::TreeCursor as _;
use language::{Dialect, LanguageError, LanguageRequest};

/// A failure while expanding a parsed specification.
#[derive(Debug, thiserror::Error)]
pub enum DsrvExpandError {
    #[error("invalid DSRV source: {0}")]
    Resolve(#[from] SourceResolveError),

    #[error("invalid DSRV specification: {0}")]
    Ast(#[from] DsrvAstError),

    #[error("invalid source-to-semantic tree conversion: {0}")]
    Transcode(#[source] contiguous_tree::TranscodeError<SourceResolveError, ExprId>),

    #[error("invalid language settings: {0}")]
    Language(#[from] LanguageError),
}

/// A parsed specification's declarations, with names and types resolved.
pub(crate) struct ExpandedDeclarations {
    /// Holds every equation's expression.
    pub(crate) builder: ExprBuilder,
    /// In source order.
    pub(crate) declarations: EcoVec<Declaration>,
    /// Each equation's expression, in the order of the equations.
    pub(crate) roots: EcoVec<ExprId>,
    pub(crate) context: Rc<SourceContext>,
}

impl SourceAscription {
    fn resolve(&self, context: &SourceContext) -> Result<StreamTypeAscription, SourceResolveError> {
        Ok(match self {
            Self::Unascribed => StreamTypeAscription::Unascribed,
            Self::Ascribed(ty) => StreamTypeAscription::Ascribed(context.resolve_type(ty)?),
        })
    }

    fn source_type(&self) -> Option<&SourceType> {
        match self {
            Self::Unascribed => None,
            Self::Ascribed(ty) => Some(ty),
        }
    }
}

/// Resolve a parsed specification's names into semantic declarations.
pub(crate) fn expand_declarations(
    parsed: ParsedSpecification,
    request: LanguageRequest,
) -> Result<ExpandedDeclarations, DsrvExpandError> {
    let (expressions, parsed_declarations) = parsed.into_parts();
    let language = language::resolve_language(&parsed_declarations, request)?;
    let core = language.dialect() == Dialect::Core;
    let mut context = SourceContext::builder();
    for declaration in &parsed_declarations {
        if let ParsedDeclaration::Alias(alias) = declaration {
            if core {
                return Err(LanguageError::NotCore {
                    construct: "a type alias",
                    span: alias.span,
                }
                .into());
            }
            language::check_experiment_type(&alias.ty, &language)?;
            context.insert_source(alias.clone())?;
        }
    }
    context.language(language.clone());
    let context = Rc::new(context.build()?);
    let mut builder = ExprBuilder::with_capacity(expressions.nodes().count());
    let mut roots = expressions.into_roots();
    let mut declarations = EcoVec::new();
    let mut equation_roots = EcoVec::new();
    for declaration in &parsed_declarations {
        let (construct, name, span) = match declaration {
            ParsedDeclaration::Input(name, _, span) => ("the input", name, span),
            ParsedDeclaration::Output(name, _, _, span) => ("the output", name, span),
            ParsedDeclaration::Aux(name, _, _, span) => ("the auxiliary stream", name, span),
            ParsedDeclaration::Equation(name, _, span) => ("the stream", name, span),
            _ => continue,
        };
        language::check_declared_name(construct, &name.name(), *span, &language)?;
    }
    for declaration in parsed_declarations {
        let resolved = match declaration {
            ParsedDeclaration::Input(name, ty, span) => Declaration::Input {
                name,
                annotation: resolve_annotation(ty.as_ref(), &context)?,
                span,
            },
            // A one-line definition declares the stream, then defines it:
            // two declarations sharing the line's span.
            ParsedDeclaration::Output(name, ty, definition, span) => {
                declarations.push(Declaration::Output {
                    name: name.clone(),
                    annotation: resolve_annotation(ty.as_ref(), &context)?,
                    span,
                });
                if definition.is_none() {
                    continue;
                }
                let expression = roots.next().expect("each definition owns one parsed root");
                equation_roots.push(expand_tree(expression.as_ref(), &mut builder, &context)?);
                Declaration::Equation { name, span }
            }
            ParsedDeclaration::Aux(name, ty, definition, span) => {
                declarations.push(Declaration::Aux {
                    name: name.clone(),
                    annotation: resolve_annotation(ty.as_ref(), &context)?,
                    span,
                });
                if definition.is_none() {
                    continue;
                }
                let expression = roots.next().expect("each definition owns one parsed root");
                equation_roots.push(expand_tree(expression.as_ref(), &mut builder, &context)?);
                Declaration::Equation { name, span }
            }
            ParsedDeclaration::Equation(name, _, span) => {
                let expression = roots.next().expect("each equation owns one parsed root");
                equation_roots.push(expand_tree(expression.as_ref(), &mut builder, &context)?);
                Declaration::Equation { name, span }
            }
            ParsedDeclaration::Alias(alias) => Declaration::TypeAlias {
                name: alias.name,
                span: alias.span,
            },
            // The header is expanded into the source context, not kept.
            ParsedDeclaration::Language(..)
            | ParsedDeclaration::Edition(..)
            | ParsedDeclaration::Use { .. } => continue,
        };
        declarations.push(resolved);
    }
    Ok(ExpandedDeclarations {
        builder,
        declarations,
        roots: equation_roots,
        context,
    })
}

/// Resolve a declared stream's annotation, refusing a type its file did not
/// opt into.
fn resolve_annotation(
    ty: Option<&SourceType>,
    context: &SourceContext,
) -> Result<Option<StreamType>, DsrvExpandError> {
    let Some(ty) = ty else { return Ok(None) };
    language::check_experiment_type(ty, context.language())?;
    Ok(Some(context.resolve_type(ty)?))
}

/// The types written inside an expression — a lambda parameter's, and the
/// result a `dynamic` or `defer` ascribes — are checked before the tree is
/// converted, so the conversion itself only resolves names.
fn check_expression_types(
    expression: ParsedExprRef<'_>,
    context: &SourceContext,
) -> Result<(), DsrvExpandError> {
    use contiguous_tree::TreeCursorExt;
    for node in expression.postorder() {
        let types: Vec<&SourceType> = match node.kind() {
            ParsedExprKind::Lambda(parameters, _) => {
                for (name, _) in parameters {
                    language::check_declared_name(
                        "the lambda parameter",
                        &name.name(),
                        parsed::span_of(node),
                        context.language(),
                    )?;
                }
                parameters
                    .iter()
                    .filter_map(|(_, ty)| ty.as_ref())
                    .collect()
            }
            ParsedExprKind::Dynamic(_, ascription, _) | ParsedExprKind::Defer(_, ascription, _) => {
                ascription.source_type().into_iter().collect()
            }
            _ => continue,
        };
        for ty in types {
            language::check_experiment_type(ty, context.language())?;
        }
    }
    Ok(())
}

/// Expand a parsed specification into the core specification.
pub(crate) fn expand_specification(
    parsed: ParsedSpecification,
    request: LanguageRequest,
) -> Result<DsrvSpecification, DsrvExpandError> {
    let ExpandedDeclarations {
        builder,
        declarations,
        roots,
        context,
    } = expand_declarations(parsed, request)?;
    let mut specification = assemble_specification(builder, declarations, roots)?;
    specification.source_context = context;
    let language = specification.source_context.language().clone();
    for node in specification.nodes() {
        language::check_experiment_node(node, &language)?;
        language::check_dialect_node(node, language.dialect())?;
    }
    if language.dialect() == Dialect::Core {
        // A Core file is accepted only if it is Core throughout.
        specification = language::CoreDsrvSpecification::check(specification)?.into_specification();
    }
    Ok(specification)
}

/// Expand one parsed expression against an existing namespace, as `dynamic`
/// and `defer` sources are.
pub(crate) fn expand_expression(
    parsed: &ParsedExpr,
    context: &Rc<SourceContext>,
) -> Result<Expr, DsrvExpandError> {
    let mut builder = ExprBuilder::with_capacity(parsed.as_ref().subtree_ids().len());
    let root = expand_tree(parsed.as_ref(), &mut builder, context)?;
    let expr = builder.finish(root).map_err(DsrvAstError::from)?;
    if let Some(key) = expr.as_ref().duplicate_field() {
        return Err(DsrvAstError::DuplicateExpressionField { field: key.clone() }.into());
    }
    {
        use contiguous_tree::TreeCursorExt;
        let language = context.language();
        for node in expr.as_ref().postorder() {
            language::check_experiment_node(node, language)?;
            language::check_dialect_node(node, language.dialect())?;
        }
    }
    if context.language().dialect() == Dialect::Core {
        // Runtime sources of a Core specification stay within Core.
        language::is_core_fragment(expr.as_ref())?;
    }
    Ok(expr)
}

/// Assemble the specification from its declarations and the expressions of
/// its equations, given in the order of the equations. See the module
/// documentation, "Assembling the specification".
pub(crate) fn assemble_specification(
    builder: ExprBuilder,
    declarations: EcoVec<Declaration>,
    roots: EcoVec<ExprId>,
) -> Result<DsrvSpecification, DsrvAstError> {
    let mut inputs = BTreeSet::new();
    let mut outputs = BTreeSet::new();
    let mut aux_vars = Vec::with_capacity(declarations.len());
    let mut type_annotations = BTreeMap::new();

    for declaration in &declarations {
        let (name, annotation) = match declaration {
            Declaration::Input {
                name, annotation, ..
            } => {
                inputs.insert(name.clone());
                (name, annotation)
            }
            Declaration::Output {
                name, annotation, ..
            } => {
                outputs.insert(name.clone());
                (name, annotation)
            }
            Declaration::Aux {
                name, annotation, ..
            } => {
                aux_vars.push(name.clone());
                (name, annotation)
            }
            Declaration::Equation { .. } | Declaration::TypeAlias { .. } => continue,
        };
        if let Some(annotation) = annotation {
            type_annotations.insert(name.clone(), annotation.clone());
        }
    }

    let expressions = builder.finish_forest(roots)?;
    UnvalidatedDsrvSpecification::new(
        inputs,
        outputs,
        aux_vars,
        expressions,
        declarations.into_iter().collect(),
        type_annotations,
    )
    .validate()
}

/// All child IDs come from the generic transcode's destination mapping.
/// A conversion failure rolls back the destination builder's allocation.
pub(crate) fn expand_tree(
    expression: ParsedExprRef<'_>,
    builder: &mut ExprBuilder,
    context: &Rc<SourceContext>,
) -> Result<ExprId, DsrvExpandError> {
    check_expression_types(expression, context)?;
    builder
        .try_transcode(expression, |node| {
            use ParsedExprKind::*;
            let metadata = ExprMetadata {
                span: parsed::span_of(node.cursor()),
                context: Some(context.clone()),
            };
            let kind = match node.cursor().kind() {
                If(a, b, c) => ExprKind::If(*node.child(*a), *node.child(*b), *node.child(*c)),
                SIndex(a, offset) => ExprKind::SIndex(*node.child(*a), *offset),
                Val(value) => ExprKind::Val(value.clone()),
                BinOp(a, b, op) => ExprKind::BinOp(*node.child(*a), *node.child(*b), *op),
                // Case decides: in a file that took on tagged unions, a
                // capitalised name is a tag whose union elaboration settles,
                // not a name expansion could resolve.
                Var(name)
                    if context.language().has(language::Feature::TaggedUnions)
                        && language::is_tag_name(&name.name()) =>
                {
                    ExprKind::Constructor(EcoVec::new(), name.name().into(), None)
                }
                Var(name) => ExprKind::Var(name.clone()),
                Match(scrutinee, arms, shape) => ExprKind::Match(
                    *node.child(*scrutinee),
                    arms.iter().map(|id| *node.child(*id)).collect(),
                    shape.clone(),
                ),
                Matches(scrutinee, guard, pattern) => ExprKind::Matches(
                    *node.child(*scrutinee),
                    guard.iter().map(|id| *node.child(*id)).collect(),
                    pattern.clone(),
                ),
                Constructor(payload, tag, qualifier) => ExprKind::Constructor(
                    payload.iter().map(|id| *node.child(*id)).collect(),
                    tag.clone(),
                    qualifier.clone(),
                ),
                Dynamic(a, ty, scope) => {
                    ExprKind::Dynamic(*node.child(*a), ty.resolve(context)?, scope.clone())
                }
                Defer(a, ty, scope) => {
                    ExprKind::Defer(*node.child(*a), ty.resolve(context)?, scope.clone())
                }
                Update(a, b) => ExprKind::Update(*node.child(*a), *node.child(*b)),
                Default(a, b) => ExprKind::Default(*node.child(*a), *node.child(*b)),
                IsDefined(a) => ExprKind::IsDefined(*node.child(*a)),
                When(a) => ExprKind::When(*node.child(*a)),
                Latch(a, b) => ExprKind::Latch(*node.child(*a), *node.child(*b)),
                Init(a, b) => ExprKind::Init(*node.child(*a), *node.child(*b)),
                Not(a) => ExprKind::Not(*node.child(*a)),
                Neg(a) => ExprKind::Neg(*node.child(*a)),
                Lambda(params, body) => ExprKind::Lambda(
                    params
                        .iter()
                        .map(|(name, ty)| {
                            let ascription = match ty {
                                Some(ty) => {
                                    StreamTypeAscription::Ascribed(context.resolve_type(ty)?)
                                }
                                None => StreamTypeAscription::Unascribed,
                            };
                            Ok((name.clone(), ascription))
                        })
                        .collect::<Result<_, SourceResolveError>>()?,
                    *node.child(*body),
                ),
                Apply(a, args) => ExprKind::Apply(
                    *node.child(*a),
                    args.iter().map(|id| *node.child(*id)).collect(),
                ),
                Fix(a) => ExprKind::Fix(*node.child(*a)),
                Partial(a, args) => ExprKind::Partial(
                    *node.child(*a),
                    args.iter().map(|id| *node.child(*id)).collect(),
                ),
                List(items) => ExprKind::List(items.iter().map(|id| *node.child(*id)).collect()),
                Tuple(items) => ExprKind::Tuple(items.iter().map(|id| *node.child(*id)).collect()),
                LIndex(a, b) => ExprKind::LIndex(*node.child(*a), *node.child(*b)),
                LAppend(a, b) => ExprKind::LAppend(*node.child(*a), *node.child(*b)),
                LConcat(a, b) => ExprKind::LConcat(*node.child(*a), *node.child(*b)),
                LHead(a) => ExprKind::LHead(*node.child(*a)),
                LTail(a) => ExprKind::LTail(*node.child(*a)),
                LLen(a) => ExprKind::LLen(*node.child(*a)),
                LMap(a, b) => ExprKind::LMap(*node.child(*a), *node.child(*b)),
                LFilter(a, b) => ExprKind::LFilter(*node.child(*a), *node.child(*b)),
                LFold(a, b, c) => {
                    ExprKind::LFold(*node.child(*a), *node.child(*b), *node.child(*c))
                }
                Map(fields) => ExprKind::Map(
                    fields
                        .iter()
                        .map(|(key, id)| (key.clone(), *node.child(*id)))
                        .collect::<EcoVec<_>>()
                        .into(),
                ),
                Struct(fields) => ExprKind::Struct(
                    fields
                        .iter()
                        .map(|(key, id)| (key.clone(), *node.child(*id)))
                        .collect::<EcoVec<_>>()
                        .into(),
                ),
                ObjectLiteral(fields) => ExprKind::ObjectLiteral(
                    fields
                        .iter()
                        .map(|(key, id)| (key.clone(), *node.child(*id)))
                        .collect::<EcoVec<_>>()
                        .into(),
                ),
                MGet(a, key) => ExprKind::MGet(*node.child(*a), key.clone()),
                SGet(a, key) => ExprKind::SGet(*node.child(*a), key.clone()),
                MInsert(a, key, b) => {
                    ExprKind::MInsert(*node.child(*a), key.clone(), *node.child(*b))
                }
                MRemove(a, key) => ExprKind::MRemove(*node.child(*a), key.clone()),
                MHasKey(a, key) => ExprKind::MHasKey(*node.child(*a), key.clone()),
                Sin(a) => ExprKind::Sin(*node.child(*a)),
                Cos(a) => ExprKind::Cos(*node.child(*a)),
                Tan(a) => ExprKind::Tan(*node.child(*a)),
                Abs(a) => ExprKind::Abs(*node.child(*a)),
                MonitoredAt(name, location) => {
                    ExprKind::MonitoredAt(name.clone(), location.clone())
                }
                Dist(a, b) => ExprKind::Dist(a.clone(), b.clone()),
            };
            Ok::<_, SourceResolveError>((kind, metadata))
        })
        .map_err(|error| match error {
            contiguous_tree::TranscodeError::Convert(error) => DsrvExpandError::Resolve(error),
            error => DsrvExpandError::Transcode(error),
        })
}
