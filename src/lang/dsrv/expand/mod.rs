//! Expansion: the parsed tree becomes the core specification.
//!
//! This stage owns every step between parsing and type checking: resolving
//! type names against the specification's own namespace, converting the
//! parsed tree into semantic nodes, and assembling the specification itself.
//! Today the conversion is one node per parsed node; later language features
//! rewrite here instead, which is why this stage owns the conversion rather
//! than the parser.

use std::collections::{BTreeMap, BTreeSet};

use ecow::EcoVec;

use super::ast::AstShared as Rc;
use super::ast::{
    DsrvAstError, Expr, ExprBuilder, ExprId, ExprKind, ExprMetadata, SemanticEntry,
    UnvalidatedDsrvSpecification,
};
use super::source::{SourceContext, SourceResolveError};
use super::span::Span;
use super::syntax::parsed::{self, ParsedExprKind, ParsedExprRef};
use super::syntax::{ParsedDeclaration, ParsedExpr, ParsedSpecification, SourceAscription};
use crate::core::{StreamType, StreamTypeAscription, VarName};
use crate::lang::dsrv::ast::DsrvSpecification;
use contiguous_tree::TreeCursor as _;

/// A failure while expanding a parsed specification.
#[derive(Debug, thiserror::Error)]
pub enum DsrvExpandError {
    #[error("invalid DSRV source: {0}")]
    Resolve(#[from] SourceResolveError),

    #[error("invalid DSRV specification: {0}")]
    Ast(#[from] DsrvAstError),

    #[error("invalid source-to-semantic tree conversion: {0}")]
    Transcode(#[source] contiguous_tree::TranscodeError<SourceResolveError, ExprId>),
}

/// A top-level declaration with its names and types resolved.

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Declaration {
    Input(VarName, Option<StreamType>, Span),
    Output(VarName, Option<StreamType>, Span),
    Aux(VarName, Option<StreamType>, Span),
    Assignment(VarName, ExprId, Span),
}

impl SourceAscription {
    fn resolve(&self, context: &SourceContext) -> Result<StreamTypeAscription, SourceResolveError> {
        Ok(match self {
            Self::Unascribed => StreamTypeAscription::Unascribed,
            Self::Ascribed(ty) => StreamTypeAscription::Ascribed(context.resolve_type(ty)?),
        })
    }
}

/// Resolve a parsed specification's names into semantic declarations.
pub(crate) fn expand_declarations(
    parsed: ParsedSpecification,
) -> Result<(ExprBuilder, EcoVec<Declaration>, Rc<SourceContext>), DsrvExpandError> {
    let (expressions, parsed_declarations) = parsed.into_parts();
    let mut context = SourceContext::builder();
    for declaration in &parsed_declarations {
        if let ParsedDeclaration::Alias(alias) = declaration {
            context.insert_source(alias.clone())?;
        }
    }
    let context = Rc::new(context.build()?);
    let mut builder = ExprBuilder::with_capacity(expressions.nodes().count());
    let mut roots = expressions.into_roots();
    let mut declarations = EcoVec::new();
    for declaration in parsed_declarations {
        let resolved = match declaration {
            ParsedDeclaration::Input(name, ty, span) => Declaration::Input(
                name,
                ty.as_ref().map(|ty| context.resolve_type(ty)).transpose()?,
                span,
            ),
            ParsedDeclaration::Output(name, ty, span) => Declaration::Output(
                name,
                ty.as_ref().map(|ty| context.resolve_type(ty)).transpose()?,
                span,
            ),
            ParsedDeclaration::Aux(name, ty, span) => Declaration::Aux(
                name,
                ty.as_ref().map(|ty| context.resolve_type(ty)).transpose()?,
                span,
            ),
            ParsedDeclaration::Assignment(name, _, span) => {
                let expression = roots.next().expect("each assignment owns one parsed root");
                Declaration::Assignment(
                    name,
                    expand_tree(expression.as_ref(), &mut builder, &context)?,
                    span,
                )
            }
            ParsedDeclaration::Alias(_) => continue,
        };
        declarations.push(resolved);
    }
    Ok((builder, declarations, context))
}

/// Expand a parsed specification into the core specification.
pub(crate) fn expand_specification(
    parsed: ParsedSpecification,
) -> Result<DsrvSpecification, DsrvExpandError> {
    let (builder, declarations, context) = expand_declarations(parsed)?;
    let mut specification = create_dsrv_spec(builder, declarations)?;
    specification.source_context = context;
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
    Ok(expr)
}

pub(crate) fn create_dsrv_spec(
    builder: ExprBuilder,
    stmts: EcoVec<Declaration>,
) -> Result<DsrvSpecification, DsrvAstError> {
    let mut inputs = BTreeSet::new();
    let mut outputs = BTreeSet::new();
    let mut stream_names = BTreeSet::new();
    let mut aux_vars = Vec::with_capacity(stmts.len());
    let mut entries = Vec::with_capacity(stmts.len());
    let mut roots = Vec::with_capacity(stmts.len());
    let mut type_annotations = BTreeMap::new();

    for stmt in stmts {
        match stmt {
            Declaration::Input(var, typ, span) => {
                if let Some(typ) = &typ {
                    type_annotations.insert(var.clone(), typ.clone());
                }
                inputs.insert(var.clone());
                entries.push(SemanticEntry::Input {
                    name: var,
                    annotation: typ,
                    span,
                });
            }
            Declaration::Output(var, typ, span) => {
                if let Some(typ) = &typ {
                    type_annotations.insert(var.clone(), typ.clone());
                }
                outputs.insert(var.clone());
                stream_names.insert(var.clone());
                entries.push(SemanticEntry::Output {
                    name: var,
                    annotation: typ,
                    span,
                });
            }
            Declaration::Aux(var, typ, span) => {
                if let Some(typ) = &typ {
                    type_annotations.insert(var.clone(), typ.clone());
                }
                stream_names.insert(var.clone());
                aux_vars.push(var.clone());
                entries.push(SemanticEntry::Aux {
                    name: var,
                    annotation: typ,
                    span,
                });
            }
            Declaration::Assignment(name, root, span) => {
                entries.push(SemanticEntry::Assignment { name, span });
                roots.push(root);
            }
        }
    }

    let expressions = builder.finish_forest(roots)?;
    UnvalidatedDsrvSpecification::new(
        inputs,
        outputs,
        aux_vars,
        expressions,
        entries,
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
                Var(name) => ExprKind::Var(name.clone()),
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
                        .map(|(name, ty)| Ok((name.clone(), context.resolve_type(ty)?)))
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
