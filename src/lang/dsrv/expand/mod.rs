//! Expansion: the parsed tree becomes the core specification.
//!
//! The DSRV front end runs in three stages. [`super::syntax`] parses source
//! text into a parsed tree and resolves no names. Expansion, this module,
//! turns that tree into a [`DsrvSpecification`]. Validation and type checking
//! ([`super::type_checker`]) then judge the specification. Expansion is the
//! only stage that reads the language settings, apart from the `if` policy
//! each node's settings imply ([`language::IfPolicy`]), and the only one that
//! sees source syntax; everything after it works on the core AST. Today the
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
//!    also carries the language settings. Every expression written in this
//!    file, and every runtime expression expanded later against this context,
//!    shares it. Code inlined from another module's def is expanded in that
//!    module's context instead, so its syntax is judged by that module's
//!    header and its names resolve where it was written; only the dialect,
//!    which decides admission, is the program's ([`functions::Lexical`]).
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

pub(crate) mod constants;
pub(crate) mod functions;
pub(crate) mod graph;
pub(crate) mod inline;
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
use super::source_map::{NodeOrigin, ProvenanceError, SourceArchive, SourceId};
use super::span::Span;
use super::syntax::parsed::{self, ParsedExprKind, ParsedExprRef};
use super::syntax::{ParsedDeclaration, ParsedExpr, ParsedSpecification, SourceAscription};
use crate::core::StreamType;
use crate::core::StreamTypeAscription;
use crate::lang::dsrv::ast::DsrvSpecification;
use crate::lang::dsrv::modules::{ImportError, ModulePath};
use contiguous_tree::TreeCursor as _;
use functions::{Callable, LexicalId};
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

    #[error("invalid import: {0}")]
    Import(#[from] ImportError),

    #[error("invalid source provenance: {0}")]
    Provenance(#[from] ProvenanceError),

    #[error("modules import each other in a cycle: {path}")]
    ModuleCycle { path: String },

    #[error("no module {path} is declared, but {importer} imports it")]
    UnknownModule { path: String, importer: String },

    #[error("inlining produced an invalid expression: {0}")]
    Inlined(String),

    #[error("`{name}` names no function")]
    UnknownFunction { name: String },

    #[error("`{name}` is defined in terms of itself; a `def` may not recurse")]
    RecursiveFunction { name: String },

    #[error("`{name}` takes {expected} arguments, given {found}")]
    FunctionArity {
        name: String,
        expected: usize,
        found: usize,
    },

    #[error("`{tag}` is not a constructor of {ty}")]
    UnknownConstructor { tag: String, ty: String },

    #[error("an exported type cannot be resolved where it was declared: {name}")]
    UnknownExport { name: String },

    #[error("{name} is internal to its module and cannot be imported")]
    InternalImport { name: String },

    #[error("a constant cannot be built from {construct}")]
    NotConstant { construct: &'static str, span: Span },

    #[error("`{name}` names no constant")]
    UnknownConstant { name: String, span: Span },

    #[error("`{name}` is defined in terms of itself; a `const` may not recurse")]
    RecursiveConstant { name: String },

    #[error("a constant could not be worked out: {message}")]
    ConstantValue { message: String, span: Span },

    #[error(
        "constant `{name}` in {location} does not have its declared type {expected} at {span:?}: {message}"
    )]
    ConstantType {
        name: String,
        location: String,
        expected: crate::core::StreamType,
        message: String,
        span: Span,
    },

    #[error("`{name}` is not a count, so it cannot be a stream offset")]
    ConstantOffset { name: String, span: Span },
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
    archive: &SourceArchive,
) -> Result<ExpandedDeclarations, DsrvExpandError> {
    expand_declarations_in(parsed, request, None, None, archive)
}

/// Expand a file's declarations, optionally against a namespace built
/// elsewhere — which is how a module's imports reach it.
pub(crate) fn expand_declarations_in(
    parsed: ParsedSpecification,
    request: LanguageRequest,
    supplied: Option<Rc<SourceContext>>,
    callable: Option<Rc<Callable>>,
    archive: &SourceArchive,
) -> Result<ExpandedDeclarations, DsrvExpandError> {
    debug_assert!(
        parsed
            .source()
            .is_none_or(|source| archive.file(source).is_some()),
        "a parsed file is expanded against the archive that holds it"
    );
    let language = language::resolve_language(parsed.declarations(), request)?;
    let core = language.dialect() == Dialect::Core;
    let mut context = SourceContext::builder();
    // What a file may write is settled before anything is inlined, so a
    // file that may not declare a def is refused for declaring one rather
    // than for how the call it wrote then reads.
    for declaration in parsed.declarations() {
        // Importing an item is what `modules` adds; `use experimental` is
        // the header line every file may write.
        let module_construct = match declaration {
            ParsedDeclaration::Use { tree, span } if !tree.is_experimental() => {
                Some(("an import", *span))
            }
            ParsedDeclaration::Mod { span, .. } => Some(("a module declaration", *span)),
            // An internal def is hidden from importers, which only modules
            // have; the def itself needs `functions`.
            ParsedDeclaration::Def {
                internal: true,
                span,
                ..
            } => Some(("an internal function", *span)),
            // An internal constant is hidden the same way, and likewise
            // needs `constants` for the constant itself.
            ParsedDeclaration::Const {
                internal: true,
                span,
                ..
            } => Some(("an internal constant", *span)),
            _ => None,
        };
        if let Some((construct, span)) = module_construct
            && !language.has_modules()
        {
            return Err(LanguageError::NeedsExperiment {
                construct,
                feature: "modules",
                span,
            }
            .into());
        }
        if let ParsedDeclaration::Def { span, .. } = declaration
            && !language.has_functions()
        {
            return Err(LanguageError::NeedsExperiment {
                construct: "a function",
                feature: "functions",
                span: *span,
            }
            .into());
        }
        if let ParsedDeclaration::Const { span, .. } = declaration {
            if !language.has_constants() {
                return Err(LanguageError::NeedsExperiment {
                    construct: "a constant",
                    feature: "constants",
                    span: *span,
                }
                .into());
            }
            if core {
                return Err(LanguageError::NotCore {
                    construct: "a constant",
                    span: *span,
                }
                .into());
            }
        }
        if let ParsedDeclaration::Alias(alias) = declaration {
            if core {
                return Err(LanguageError::NotCore {
                    construct: "a type alias",
                    span: alias.span,
                }
                .into());
            }
            language::check_alias(alias, &language)?;
            if supplied.is_none() {
                context.insert_source(alias.clone())?;
            }
        }
    }
    let context = match supplied {
        Some(context) => context,
        None => {
            context.language(language.clone());
            Rc::new(context.build()?)
        }
    };
    // A program of several files was handed tables built over all of them;
    // a program of one file builds its own, so text supplied to it at
    // runtime reaches the defs and constants that file declared.
    let callable = match callable {
        // A program whose modules declared neither leaves its nodes
        // carrying nothing, as a program without the experiments does.
        Some(callable) if !callable.is_empty() => Some(callable),
        Some(_) => None,
        None if declares_a_def_or_constant(parsed.declarations()) => {
            let constants = Rc::new(constants::single_file_constants(&parsed, &context)?);
            let table = functions::single_file_table(
                &parsed,
                parsed.source().map(|_| archive.token()),
                Rc::clone(&context),
                &constants,
            )?;
            Some(Rc::new(Callable::new(Rc::new(table), &ModulePath::new())))
        }
        None => None,
    };
    if let Some(callable) = &callable {
        check_provenance(callable, parsed.source().map(|_| archive))?;
    }
    // A call to a def becomes an immediate lambda application before any
    // name is resolved, which is where the pipeline puts inlining.
    let scope = callable
        .as_ref()
        .map_or_else(inline::Scope::default, |callable| callable.scope());
    let parsed = inline::inline_functions(parsed, scope)?;
    let (expressions, parsed_declarations, source) = parsed.into_parts();
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
                equation_roots.push(expand_tree(
                    expression.as_ref(),
                    &mut builder,
                    &context,
                    callable.as_ref(),
                    source,
                )?);
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
                equation_roots.push(expand_tree(
                    expression.as_ref(),
                    &mut builder,
                    &context,
                    callable.as_ref(),
                    source,
                )?);
                Declaration::Equation { name, span }
            }
            ParsedDeclaration::Equation(name, _, span) => {
                let expression = roots.next().expect("each equation owns one parsed root");
                equation_roots.push(expand_tree(
                    expression.as_ref(),
                    &mut builder,
                    &context,
                    callable.as_ref(),
                    source,
                )?);
                Declaration::Equation { name, span }
            }
            ParsedDeclaration::Alias(alias) => Declaration::TypeAlias {
                name: alias.name,
                span: alias.span,
            },
            // A def is inlined at its call sites rather than kept as a
            // declaration, but it owns a root and must consume it here so
            // the remaining declarations still line up with theirs.
            // A def is inlined at its call sites and a constant is folded
            // into them, so neither is kept as a declaration; both own a
            // root and must consume it here so the remaining declarations
            // still line up with theirs.
            ParsedDeclaration::Def { .. } | ParsedDeclaration::Const { .. } => {
                roots
                    .next()
                    .expect("each def and constant owns one parsed root");
                continue;
            }
            // The header is expanded into the source context, not kept.
            ParsedDeclaration::Language(..)
            | ParsedDeclaration::Edition(..)
            | ParsedDeclaration::Use { .. }
            | ParsedDeclaration::Mod { .. } => continue,
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

/// Refuse to inline defs whose provenance `target` cannot address.
///
/// A located expansion takes definition sites from `callable`'s defs, so
/// they must be IDs of the archive the result will be located against, or
/// of one it was derived from. Grafting them anywhere else would name the
/// wrong file; they are never silently dropped. An unlocated expansion
/// keeps no provenance at all, so it grafts from anywhere.
fn check_provenance(
    callable: &Callable,
    target: Option<&SourceArchive>,
) -> Result<(), ProvenanceError> {
    match (target, callable.archive()) {
        (Some(target), Some(defs)) if !target.resolves(defs) => {
            Err(ProvenanceError::ForeignArchive)
        }
        _ => Ok(()),
    }
}

/// Whether a file declares a function or a constant, which is what makes
/// a table worth building for it.
fn declares_a_def_or_constant(declarations: &[ParsedDeclaration]) -> bool {
    declarations.iter().any(|declaration| {
        matches!(
            declaration,
            ParsedDeclaration::Def { .. } | ParsedDeclaration::Const { .. }
        )
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

/// The lexical environment each node of one tree is expanded in.
///
/// A node belongs to the text it sits in unless it was inlined from a def
/// of another module, in which case it records the environment that def
/// was written in: its names resolve in that module's namespace, and its
/// syntax is judged by that module's header.
struct Environments<'a> {
    context: &'a Rc<SourceContext>,
    callable: Option<&'a Rc<Callable>>,
    inlined: BTreeMap<LexicalId, (Rc<SourceContext>, Option<Rc<Callable>>)>,
}

impl<'a> Environments<'a> {
    fn new(
        expression: ParsedExprRef<'_>,
        context: &'a Rc<SourceContext>,
        callable: Option<&'a Rc<Callable>>,
    ) -> Self {
        use contiguous_tree::TreeCursorExt;
        let mut inlined = BTreeMap::new();
        for node in expression.postorder() {
            let Some(id) = parsed::origin_of(node).lexical else {
                continue;
            };
            let callable = callable.expect("only a callable's table inlines code from elsewhere");
            if !callable.is_own(id) {
                inlined
                    .entry(id)
                    .or_insert_with(|| callable.environment(id));
            }
        }
        Self {
            context,
            callable,
            inlined,
        }
    }

    /// The namespace `node` is read in, and what text supplied to it may
    /// call.
    fn of(&self, node: ParsedExprRef<'_>) -> (&Rc<SourceContext>, Option<&Rc<Callable>>) {
        match parsed::origin_of(node)
            .lexical
            .and_then(|id| self.inlined.get(&id))
        {
            Some((context, callable)) => (context, callable.as_ref()),
            None => (self.context, self.callable),
        }
    }
}

/// The types written inside an expression — a lambda parameter's, and the
/// result a `dynamic` or `defer` ascribes — are checked before the tree is
/// converted, so the conversion itself only resolves names. Each is checked
/// against the settings of the module that wrote it.
fn check_expression_types(
    expression: ParsedExprRef<'_>,
    environments: &Environments<'_>,
) -> Result<(), DsrvExpandError> {
    use contiguous_tree::TreeCursorExt;
    for node in expression.postorder() {
        let (context, _) = environments.of(node);
        // Writing an offset as a name is what `constants` adds to a
        // spelling every file can now parse, so the check belongs here
        // rather than in the grammar.
        if let ParsedExprKind::SIndex(_, parsed::SourceOffset::Named(_)) = node.kind()
            && !context.language().has_constants()
        {
            return Err(LanguageError::NeedsExperiment {
                construct: "a named stream offset",
                feature: "constants",
                span: parsed::span_of(node),
            }
            .into());
        }
        // Naming a value through a module is what `modules` adds. The
        // check belongs here because `ModuleItem` is a parsed kind that
        // inlining resolves away, so no core node carries it.
        if matches!(node.kind(), ParsedExprKind::ModuleItem(_)) && !context.language().has_modules()
        {
            return Err(LanguageError::NeedsExperiment {
                construct: "a value named through a module",
                feature: "modules",
                span: parsed::span_of(node),
            }
            .into());
        }
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
            ParsedExprKind::Cast(_, target) | ParsedExprKind::Ascribe(_, target) => vec![target],
            _ => continue,
        };
        for ty in types {
            language::check_experiment_type(ty, context.language())?;
        }
    }
    Ok(())
}

/// Expand a parsed specification into the core specification.
/// Expand a whole program: every module's namespace, then the root against
/// its own.
pub(crate) fn expand_program(
    sources: crate::lang::dsrv::modules::ModuleSources,
    request: LanguageRequest,
) -> Result<DsrvSpecification, DsrvExpandError> {
    let graph = graph::build_graph(&sources, request)?;
    let root_path = ModulePath::new();
    let context = graph
        .get(&root_path)
        .expect("the root has a namespace")
        .clone();
    // Def bodies fold the constants of the module that wrote them, so the
    // constants come first.
    let folded = Rc::new(constants::build_constant_table(&sources, &graph)?);
    let table = Rc::new(functions::build_function_table(
        &sources,
        &graph,
        &folded,
        context.language().dialect(),
    )?);
    let callable = Callable::new(table, &root_path);
    let archive = Rc::clone(sources.archive());
    let root = sources.into_root();
    finish_specification(
        expand_declarations_in(
            root,
            request,
            Some(context),
            Some(Rc::new(callable)),
            &archive,
        )?,
        archive,
    )
}

/// Expand one file, which `archive` holds, as a program of its own.
pub(crate) fn expand_specification(
    parsed: ParsedSpecification,
    request: LanguageRequest,
    archive: Rc<SourceArchive>,
) -> Result<DsrvSpecification, DsrvExpandError> {
    let expanded = expand_declarations(parsed, request, &archive)?;
    finish_specification(expanded, archive)
}

fn finish_specification(
    expanded: ExpandedDeclarations,
    archive: Rc<SourceArchive>,
) -> Result<DsrvSpecification, DsrvExpandError> {
    let ExpandedDeclarations {
        builder,
        declarations,
        roots,
        context,
    } = expanded;
    let mut specification = assemble_specification(builder, declarations, roots)?;
    specification.source_context = context;
    specification.sources = archive;
    let language = specification.source_context.language().clone();
    for node in specification.nodes() {
        // Syntax is authorised by the header of the module that wrote it;
        // admission is the program's.
        let written = node
            .source_context()
            .map_or(&language, SourceContext::language);
        language::check_experiment_node(node, written)?;
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
///
/// `located` names the archive the result is located against and the file
/// of that archive the text is; without it the result is unlocated.
pub(crate) fn expand_expression(
    parsed: &ParsedExpr,
    context: &Rc<SourceContext>,
    callable: &Rc<Callable>,
    located: Option<(&SourceArchive, SourceId)>,
) -> Result<Expr, DsrvExpandError> {
    check_provenance(callable, located.map(|(archive, _)| archive))?;
    // Runtime expression source may call a def, so it is inlined first, exactly as a
    // file's own expressions are.
    let scope = callable.scope();
    let inlined = inline::standalone(parsed.as_ref(), &scope, [])?;
    let parsed = &inlined;
    // Text nested inside this text may call a def in turn, so what this
    // text could call travels on into it.
    let onwards = (!callable.is_empty()).then(|| Rc::clone(callable));
    let mut builder = ExprBuilder::with_capacity(parsed.as_ref().subtree_ids().len());
    let root = expand_tree(
        parsed.as_ref(),
        &mut builder,
        context,
        onwards.as_ref(),
        located.map(|(_, source)| source),
    )?;
    let expr = builder.finish(root).map_err(DsrvAstError::from)?;
    if let Some(key) = expr.as_ref().duplicate_field() {
        return Err(DsrvAstError::DuplicateExpressionField { field: key.clone() }.into());
    }
    {
        use contiguous_tree::TreeCursorExt;
        let language = context.language();
        for node in expr.as_ref().postorder() {
            let written = node
                .source_context()
                .map_or(language, SourceContext::language);
            language::check_experiment_node(node, written)?;
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
///
/// `source` is the archived file the tree's spans are in. Without one the
/// tree is unlocated, and definition sites are not kept either.
///
/// `context` and `callable` are the tree's own environment. A node inlined
/// from another module's def is expanded in that module's instead, which
/// `callable`'s table holds.
pub(crate) fn expand_tree(
    expression: ParsedExprRef<'_>,
    builder: &mut ExprBuilder,
    context: &Rc<SourceContext>,
    callable: Option<&Rc<Callable>>,
    source: Option<SourceId>,
) -> Result<ExprId, DsrvExpandError> {
    let environments = Environments::new(expression, context, callable);
    check_expression_types(expression, &environments)?;
    builder
        .try_transcode(expression, |node| {
            use ParsedExprKind::*;
            let origin = parsed::origin_of(node.cursor());
            let (context, callable) = environments.of(node.cursor());
            let metadata = ExprMetadata {
                span: origin.span,
                origin: NodeOrigin::new(source, source.and(origin.definition)),
                context: Some(context.clone()),
                callable: callable.cloned(),
                generic_def: origin.generic_def,
            };
            let kind = match node.cursor().kind() {
                If(a, b, c) => ExprKind::If(*node.child(*a), *node.child(*b), *node.child(*c)),
                // Folding replaces a named offset with the number its
                // constant stands for, so one reaching here named none.
                SIndex(a, offset) => match offset {
                    parsed::SourceOffset::Literal(offset) => {
                        ExprKind::SIndex(*node.child(*a), *offset)
                    }
                    parsed::SourceOffset::Named(path) => {
                        return Err(SourceResolveError::UnknownOffset {
                            name: path.to_string().into(),
                            span: metadata.span,
                        });
                    }
                },
                Val(value) => ExprKind::Val(value.clone()),
                BinOp(a, b, op) => ExprKind::BinOp(*node.child(*a), *node.child(*b), *op),
                Cast(value, target) => {
                    ExprKind::Cast(*node.child(*value), context.resolve_type(target)?)
                }
                Ascribe(value, target) => {
                    ExprKind::Ascribe(*node.child(*value), context.resolve_type(target)?)
                }
                Trunc(value) => ExprKind::Trunc(*node.child(*value)),
                Floor(value) => ExprKind::Floor(*node.child(*value)),
                Ceil(value) => ExprKind::Ceil(*node.child(*value)),
                Round(value) => ExprKind::Round(*node.child(*value)),
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
                // Inlining replaces a qualified value with the def it names,
                // so one reaching here named none.
                ModuleItem(path) => {
                    return Err(SourceResolveError::UnknownModuleItem {
                        name: path.to_string().into(),
                        span: metadata.span,
                    });
                }
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
