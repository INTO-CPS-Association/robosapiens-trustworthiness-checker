//! Local stream functions: `def` results with private `where` equations.
//!
//! Each written whole-equation call is a static instance. Its stream
//! parameters and local aux equations become private streams on the enclosing
//! clock; its result expression defines the calling equation. Nested calls
//! inherit the defining module and create fresh child instances.
//!
//! Bodies are inlined against their defining module's pure defs and constants
//! in [`build_entry`]. [`Instantiator`] expands calls after use-site argument
//! inlining. Fixed function arguments keep their named identity until binding;
//! fixed data must be closed, and callbacks must resolve to pure named defs.
//!
//! Private names derive from the result stream and nested instance path, not
//! allocation order. `$` cannot occur in a source stream name. Parameters bind
//! once per enclosing tick, and ordinary delays/defaults retain that clock.
//! Copied body nodes retain definition provenance and lexical settings; copied
//! argument nodes retain their use-site environment.

use std::collections::{BTreeMap, BTreeSet};

use contiguous_tree::{TreeCursor, TreeNodeMut};
use ecow::EcoVec;

use crate::VarName;
use crate::core::{StreamType, UnionPayload};
use crate::lang::dsrv::ast::{StreamFunctionInstance, SyntaxLiteral};
use crate::lang::dsrv::modules::ModulePath;
use crate::lang::dsrv::path::ValuePath;
use crate::lang::dsrv::source::{SourceContext, SourceType, SourceTypeKind, TypeName};
use crate::lang::dsrv::source_map::SourceId;
use crate::lang::dsrv::span::Span;
use crate::lang::dsrv::syntax::SourceAscription;
use crate::lang::dsrv::syntax::parsed::{
    self, ParsedExpr, ParsedExprBuilder, ParsedExprId, ParsedExprKind, ParsedExprRef, ParsedOrigin,
    StreamFunctionDeclaration,
};

use super::DsrvExpandError;
use super::functions::{Callable, LexicalId};
use super::inline::{self, Scope, Stamp, standalone};
use super::language::{self, Dialect, Feature, LanguageConfig, LanguageError};

/// How deeply stream function calls may nest inside each other.
const MAX_DEPTH: usize = 16;
/// How many streams the instances of one program may generate.
const MAX_STREAMS: usize = 4096;

/// A stream function as its module declared it, with each body equation inlined
/// against that module's defs and constants.
#[derive(Debug)]
pub(crate) struct StreamFunctionEntry {
    pub(crate) internal: bool,
    name: TypeName,
    module: ModulePath,
    type_parameters: EcoVec<TypeName>,
    fixed: EcoVec<(VarName, SourceType)>,
    streams: EcoVec<(VarName, SourceType)>,
    result: Option<SourceType>,
    body: Vec<BodyEquation>,
    /// The archived file that declared the stream function.
    source: Option<SourceId>,
    /// The environment of the module that declared it.
    lexical: LexicalId,
}

#[derive(Debug)]
struct BodyEquation {
    output: bool,
    name: VarName,
    ty: Option<SourceType>,
    tree: ParsedExpr,
}

impl StreamFunctionEntry {
    fn display(&self) -> String {
        qualified(&self.module, &self.name)
    }

    /// The single `out` equation.
    fn output(&self) -> &BodyEquation {
        self.body
            .iter()
            .find(|equation| equation.output)
            .expect("a stream function entry has one result equation")
    }

    /// The declared result: `-> R`, or failing that the `out` equation's
    /// own annotation.
    fn result_type(&self) -> Option<&SourceType> {
        self.result.as_ref().or(self.output().ty.as_ref())
    }
}

fn qualified(module: &ModulePath, name: &TypeName) -> String {
    module
        .iter()
        .map(|segment| segment.as_str().to_owned())
        .chain([name.to_string()])
        .collect::<Vec<_>>()
        .join("::")
}

fn is_function(ty: &SourceType) -> bool {
    matches!(ty.kind, SourceTypeKind::Function(..))
}

/// Check a stream function declaration and inline its body against `scope`, the
/// defs and constants its module may name.
///
/// `trees` are the body equations' roots, in order.
pub(super) fn build_entry(
    declaration: &StreamFunctionDeclaration,
    trees: &[ParsedExprRef<'_>],
    scope: &Scope<'_>,
    language: &LanguageConfig,
    module: &ModulePath,
    source: Option<SourceId>,
    lexical: LexicalId,
) -> Result<StreamFunctionEntry, DsrvExpandError> {
    let stream_function = qualified(module, &declaration.name);
    let span = declaration.span;
    if !language.has(Feature::LocalStreams) {
        return Err(LanguageError::NeedsExperiment {
            construct: "a def with local streams",
            feature: Feature::LocalStreams.name(),
            span,
        }
        .into());
    }
    if language.dialect() == Dialect::Core {
        return Err(LanguageError::NotCore {
            construct: "a stream function",
            span,
        }
        .into());
    }
    if !declaration.type_parameters.is_empty() && !language.has(Feature::Generics) {
        return Err(LanguageError::NeedsExperiment {
            construct: "a generic stream function",
            feature: Feature::Generics.name(),
            span,
        }
        .into());
    }
    let invalid = |problem: String, span: Span| DsrvExpandError::InvalidStreamFunction {
        stream_function: stream_function.clone(),
        problem,
        span,
    };

    let mut type_names = BTreeSet::new();
    for parameter in &declaration.type_parameters {
        if !type_names.insert(parameter) {
            return Err(invalid(
                format!("type parameter `{parameter}` is declared twice"),
                span,
            ));
        }
    }
    let mut names: BTreeSet<VarName> = BTreeSet::new();
    let parameters = declaration
        .fixed
        .iter()
        .map(|(name, ty)| ("the fixed parameter", name, Some(ty), span))
        .chain(
            declaration
                .streams
                .iter()
                .map(|(name, ty)| ("the stream parameter", name, Some(ty), span)),
        )
        .chain(declaration.body.iter().map(|equation| {
            (
                "the local stream",
                &equation.name,
                equation.ty.as_ref(),
                equation.span,
            )
        }));
    for (construct, name, ty, span) in parameters {
        language::check_declared_name(construct, &name.name(), span, language)?;
        if !names.insert(name.clone()) {
            return Err(invalid(format!("`{name}` is declared twice"), span));
        }
        if let Some(ty) = ty {
            language::check_experiment_type(ty, language)?;
        }
    }
    if let Some(result) = &declaration.result {
        language::check_experiment_type(result, language)?;
    }
    let outputs = declaration
        .body
        .iter()
        .filter(|equation| equation.output)
        .count();
    if outputs != 1 {
        return Err(invalid(
            format!("a stream function has exactly one result expression, not {outputs}"),
            span,
        ));
    }

    let mut body = Vec::with_capacity(declaration.body.len());
    for (equation, tree) in declaration.body.iter().zip(trees) {
        // Only the whole right-hand side may be a use, so that each use is
        // exactly one statically known instance.
        refuse_nested_uses(*tree)?;
        // The stream function's own names are its, not a def's or a constant's of
        // the same name.
        let inlined = standalone(*tree, scope, names.iter().cloned())?;
        check_closed_body(inlined.as_ref(), &names, language, &stream_function)?;
        body.push(BodyEquation {
            output: equation.output,
            name: equation.name.clone(),
            ty: equation.ty.clone(),
            tree: inlined,
        });
    }
    Ok(StreamFunctionEntry {
        internal: declaration.internal,
        name: declaration.name.clone(),
        module: module.clone(),
        type_parameters: declaration.type_parameters.clone(),
        fixed: declaration.fixed.clone(),
        streams: declaration.streams.clone(),
        result: declaration.result.clone(),
        body,
        source,
        lexical,
    })
}

/// Refuse any stream function call inside `tree`, which is the body of `construct`
/// or an expression a use may not stand in.
pub(crate) fn refuse_uses(
    tree: ParsedExprRef<'_>,
    construct: &'static str,
) -> Result<(), DsrvExpandError> {
    use contiguous_tree::TreeCursorExt;
    match tree.postorder().find_map(|node| match node.kind() {
        ParsedExprKind::StreamFunctionCall(_, path, _) => {
            Some((path.to_string(), parsed::span_of(node)))
        }
        _ => None,
    }) {
        Some((stream_function, span)) => Err(DsrvExpandError::MisplacedStreamFunctionCall {
            stream_function,
            construct,
            span,
        }),
        None => Ok(()),
    }
}

/// Refuse a use anywhere below the root of a body equation.
fn refuse_nested_uses(tree: ParsedExprRef<'_>) -> Result<(), DsrvExpandError> {
    match tree.kind() {
        ParsedExprKind::StreamFunctionCall(arguments, ..) => {
            for argument in arguments {
                refuse_uses(
                    tree.child(*argument),
                    "an argument of a stream function call",
                )?;
            }
            Ok(())
        }
        _ => refuse_uses(tree, "a larger expression"),
    }
}

/// Whether to look inside a node's children.
enum Walk {
    Descend,
    Skip,
}

/// Visit every node below `cursor`, with the names the binders around it
/// bind: lambda parameters, and the names a pattern binds in its arm.
fn walk<'c>(
    cursor: ParsedExprRef<'c>,
    bound: &mut Vec<VarName>,
    visit: &mut impl FnMut(ParsedExprRef<'c>, &[VarName]) -> Result<Walk, DsrvExpandError>,
) -> Result<(), DsrvExpandError> {
    if let Walk::Skip = visit(cursor, bound)? {
        return Ok(());
    }
    match cursor.kind() {
        ParsedExprKind::Lambda(parameters, body) => {
            let count = bound.len();
            bound.extend(parameters.iter().map(|(name, _)| name.clone()));
            walk(cursor.child(*body), bound, visit)?;
            bound.truncate(count);
        }
        ParsedExprKind::Match(scrutinee, arms, shape) => {
            walk(cursor.child(*scrutinee), bound, visit)?;
            let mut children = arms.iter();
            for arm in shape {
                let count = bound.len();
                bound.extend(arm.pattern.bound_names());
                for _ in 0..arm.children() {
                    let child = children.next().expect("match shape covers its children");
                    walk(cursor.child(*child), bound, visit)?;
                }
                bound.truncate(count);
            }
        }
        ParsedExprKind::Matches(scrutinee, guard, pattern) => {
            walk(cursor.child(*scrutinee), bound, visit)?;
            let count = bound.len();
            bound.extend(pattern.bound_names());
            for child in guard {
                walk(cursor.child(*child), bound, visit)?;
            }
            bound.truncate(count);
        }
        _ => {
            for child in cursor.child_ids() {
                walk(cursor.child(child), bound, visit)?;
            }
        }
    }
    Ok(())
}

/// A stream function reads its parameters and its own streams, nothing the use
/// site declares: refuse a free name that is neither. A nested use's fixed
/// arguments are resolved when it is instantiated, so they are skipped here.
fn check_closed_body(
    tree: ParsedExprRef<'_>,
    names: &BTreeSet<VarName>,
    language: &LanguageConfig,
    stream_function: &str,
) -> Result<(), DsrvExpandError> {
    let root = tree.id();
    walk(tree, &mut Vec::new(), &mut |node, bound| {
        match node.kind() {
            ParsedExprKind::StreamFunctionCall(arguments, _, fixed) if node.id() == root => {
                for argument in arguments.iter().skip(*fixed) {
                    check_closed_body(node.child(*argument), names, language, stream_function)?;
                }
                return Ok(Walk::Skip);
            }
            ParsedExprKind::Var(name)
                if !bound.contains(name)
                    && !names.contains(name)
                    && !(language.has(Feature::TaggedUnions)
                        && language::is_tag_name(&name.name())) =>
            {
                return Err(DsrvExpandError::InvalidStreamFunction {
                    stream_function: stream_function.to_owned(),
                    problem: format!(
                        "its body reads `{name}`, which is neither a parameter nor a stream of \
                         the stream function"
                    ),
                    span: parsed::span_of(node),
                });
            }
            _ => {}
        }
        Ok(Walk::Descend)
    })
}

/// Why a fixed argument is not closed, if it is not: the first stream it
/// reads, or construct that depends on streams.
fn closed_violation(
    tree: ParsedExprRef<'_>,
    bound: Vec<VarName>,
) -> Result<Option<(String, Span)>, DsrvExpandError> {
    let mut found = None;
    walk(tree, &mut { bound }, &mut |node, bound| {
        if found.is_some() {
            return Ok(Walk::Skip);
        }
        let problem = match node.kind() {
            ParsedExprKind::Var(name)
                if !bound.contains(name) && !language::is_tag_name(&name.name()) =>
            {
                Some(format!("reads the stream `{name}`"))
            }
            ParsedExprKind::SIndex(..) => Some("reads stream history".to_owned()),
            ParsedExprKind::Dynamic(..) => Some("uses `dynamic`".to_owned()),
            ParsedExprKind::Defer(..) => Some("uses `defer`".to_owned()),
            ParsedExprKind::MonitoredAt(..) | ParsedExprKind::Dist(..) => {
                Some("reads stream placement".to_owned())
            }
            ParsedExprKind::StreamFunctionCall(..) => Some("calls a stream function".to_owned()),
            ParsedExprKind::ModuleItem(path) => {
                Some(format!("names `{path}`, which is not a constant"))
            }
            _ => None,
        };
        if let Some(problem) = problem {
            found = Some((problem, parsed::span_of(node)));
            return Ok(Walk::Skip);
        }
        Ok(Walk::Descend)
    })?;
    Ok(found)
}

/// Copy a subtree as it stands, provenance included.
fn copy_verbatim(cursor: ParsedExprRef<'_>, builder: &mut ParsedExprBuilder) -> ParsedExprId {
    let children: Vec<_> = cursor
        .child_ids()
        .map(|child| copy_verbatim(cursor.child(child), builder))
        .collect();
    let mut kind = cursor.kind().clone();
    let mut next = children.into_iter();
    kind.for_each_child_id_mut(|child| *child = next.next().expect("one copy per child"));
    builder.alloc(kind, parsed::origin_of(cursor))
}

/// A subtree as a tree of its own.
fn finish(builder: ParsedExprBuilder, root: ParsedExprId) -> Result<ParsedExpr, DsrvExpandError> {
    Ok(builder
        .finish_forest([root])
        .map_err(|error| DsrvExpandError::Inlined(error.to_string()))?
        .into_roots()
        .next()
        .expect("one root makes one tree"))
}

fn verbatim(cursor: ParsedExprRef<'_>) -> Result<ParsedExpr, DsrvExpandError> {
    let mut builder = ParsedExprBuilder::with_capacity(cursor.subtree_ids().len());
    let root = copy_verbatim(cursor, &mut builder);
    finish(builder, root)
}

/// A def the table holds, by the module that declared it and its name.
type DefKey = (ModulePath, VarName);

/// A use's arguments, in their final form: each is read where the use was
/// written, so it is copied into the instance as it stands.
struct Arguments {
    fixed: Vec<Fixed>,
    streams: Vec<ParsedExpr>,
}

enum Fixed {
    Data(ParsedExpr),
    Function(DefKey),
}

/// One instance being copied out of its stream function.
struct Instance {
    /// What each body stream and stream parameter is called in the program.
    names: BTreeMap<VarName, VarName>,
    data: BTreeMap<VarName, ParsedExpr>,
    functions: BTreeMap<VarName, DefKey>,
    /// Each type parameter's inferred type, as a structural source type
    /// that resolves in any namespace.
    types: BTreeMap<TypeName, SourceType>,
    stamp: Stamp,
}

impl Instance {
    fn substitute(&self, ty: &SourceType) -> SourceType {
        let bound = self.types.iter().collect();
        crate::lang::dsrv::source::substitute_source(ty, &bound)
    }

    /// Copy one body subtree into `builder` as this instance's code.
    fn copy(
        &self,
        cursor: ParsedExprRef<'_>,
        callable: &Callable,
        builder: &mut ParsedExprBuilder,
        bound: &mut Vec<VarName>,
    ) -> Result<ParsedExprId, DsrvExpandError> {
        let origin = Stamp::place(Some(self.stamp), cursor);
        let free = |name: &VarName| !bound.contains(name);
        if let ParsedExprKind::Apply(function, arguments) = cursor.kind()
            && let ParsedExprKind::Var(name) = cursor.child(*function).kind()
            && free(name)
            && let Some(key) = self.functions.get(name)
        {
            let def = callable.def(key).expect("a resolved def is in the table");
            if def.parameters.len() != arguments.len() {
                return Err(DsrvExpandError::FunctionArity {
                    name: name.to_string(),
                    expected: def.parameters.len(),
                    found: arguments.len(),
                });
            }
            let lambda = inline::foreign_call_lambda(&def, &origin, builder)?;
            let mut copied = EcoVec::new();
            for argument in arguments {
                copied.push(self.copy(cursor.child(*argument), callable, builder, bound)?);
            }
            return Ok(inline::finish_call(&def, &origin, lambda, copied, builder));
        }
        if let ParsedExprKind::Var(name) = cursor.kind()
            && free(name)
        {
            if let Some(key) = self.functions.get(name) {
                return eta_expand(name, key, &origin, callable, builder);
            }
            if let Some(argument) = self.data.get(name) {
                return Ok(copy_verbatim(argument.as_ref(), builder));
            }
            if let Some(renamed) = self.names.get(name) {
                return Ok(builder.alloc(ParsedExprKind::Var(renamed.clone()), origin));
            }
        }
        if let ParsedExprKind::StreamFunctionCall(_, path, _) = cursor.kind() {
            return Err(DsrvExpandError::MisplacedStreamFunctionCall {
                stream_function: path.to_string(),
                construct: "a larger expression",
                span: parsed::span_of(cursor),
            });
        }
        let binders: Vec<Vec<VarName>> = match cursor.kind() {
            ParsedExprKind::Lambda(parameters, _) => {
                vec![parameters.iter().map(|(name, _)| name.clone()).collect()]
            }
            _ => Vec::new(),
        };
        let mut children = Vec::new();
        match cursor.kind() {
            ParsedExprKind::Match(scrutinee, arms, shape) => {
                children.push(self.copy(cursor.child(*scrutinee), callable, builder, bound)?);
                let mut source = arms.iter();
                for arm in shape {
                    let count = bound.len();
                    bound.extend(arm.pattern.bound_names());
                    for _ in 0..arm.children() {
                        let child = source.next().expect("match shape covers its children");
                        children.push(self.copy(cursor.child(*child), callable, builder, bound)?);
                    }
                    bound.truncate(count);
                }
            }
            ParsedExprKind::Matches(scrutinee, guard, pattern) => {
                children.push(self.copy(cursor.child(*scrutinee), callable, builder, bound)?);
                let count = bound.len();
                bound.extend(pattern.bound_names());
                for child in guard {
                    children.push(self.copy(cursor.child(*child), callable, builder, bound)?);
                }
                bound.truncate(count);
            }
            _ => {
                let count = bound.len();
                bound.extend(binders.into_iter().flatten());
                for child in cursor.child_ids() {
                    children.push(self.copy(cursor.child(child), callable, builder, bound)?);
                }
                bound.truncate(count);
            }
        }
        let mut kind = cursor.kind().clone();
        // The body's own types may name the stream function's type parameters,
        // which are this instance's inferred types.
        match &mut kind {
            ParsedExprKind::Lambda(parameters, _) => {
                for (_, ty) in parameters.make_mut() {
                    if let Some(ty) = ty {
                        *ty = self.substitute(ty);
                    }
                }
            }
            ParsedExprKind::Cast(_, target) | ParsedExprKind::Ascribe(_, target) => {
                *target = self.substitute(target);
            }
            ParsedExprKind::Dynamic(_, ascription, _) | ParsedExprKind::Defer(_, ascription, _) => {
                if let SourceAscription::Ascribed(ty) = ascription {
                    *ty = self.substitute(ty);
                }
            }
            _ => {}
        }
        let mut next = children.into_iter();
        kind.for_each_child_id_mut(|child| *child = next.next().expect("one copy per child"));
        Ok(builder.alloc(kind, origin))
    }

    /// Copy one body subtree as a tree of its own.
    fn copy_tree(
        &self,
        cursor: ParsedExprRef<'_>,
        callable: &Callable,
    ) -> Result<ParsedExpr, DsrvExpandError> {
        let mut builder = ParsedExprBuilder::with_capacity(cursor.subtree_ids().len());
        let root = self.copy(cursor, callable, &mut builder, &mut Vec::new())?;
        finish(builder, root)
    }
}

/// `\a… -> f(a…)` for a def passed on as a value rather than called. The
/// parameters and their types are the def's, read where it was written.
fn eta_expand(
    name: &VarName,
    key: &DefKey,
    origin: &ParsedOrigin,
    callable: &Callable,
    builder: &mut ParsedExprBuilder,
) -> Result<ParsedExprId, DsrvExpandError> {
    let def = callable.def(key).expect("a resolved def is in the table");
    let parameters: Vec<VarName> = (0..def.parameters.len())
        .map(|index| VarName::new(&format!("{name}${index}")))
        .collect();
    let lambda = inline::foreign_call_lambda(&def, origin, builder)?;
    let arguments = parameters
        .iter()
        .map(|parameter| builder.alloc(ParsedExprKind::Var(parameter.clone()), origin.clone()))
        .collect();
    let call = inline::finish_call(&def, origin, lambda, arguments, builder);
    let typed = parameters
        .into_iter()
        .zip(&def.parameters)
        .map(|(parameter, (_, ty))| (parameter, def.ascription(ty)))
        .collect();
    Ok(builder.alloc(
        ParsedExprKind::Lambda(typed, call),
        ParsedOrigin {
            lexical: def.lexical.or(origin.lexical),
            ..origin.clone()
        },
    ))
}

/// What an instance's streams are named after: the stream its result
/// defines, and the prefix of its private streams.
struct Names {
    result: VarName,
    prefix: String,
}

/// A private stream an instance generates.
pub(crate) struct GeneratedStream {
    pub(crate) name: VarName,
    pub(crate) annotation: Option<StreamType>,
    pub(crate) expression: ParsedExpr,
}

/// What the instances of one use have generated so far.
#[derive(Default)]
struct Collected {
    generated: Vec<GeneratedStream>,
    instances: Vec<StreamFunctionInstance>,
}

/// What one use at the root expands to.
pub(crate) struct Instantiated {
    /// The equation of the use site's own stream.
    pub(crate) result: ParsedExpr,
    /// The stream function's declared result type, if it declares one.
    pub(crate) result_type: Option<StreamType>,
    /// Private streams, in a stable order.
    pub(crate) generated: Vec<GeneratedStream>,
    /// This instance and every one nested in it.
    pub(crate) instances: Vec<StreamFunctionInstance>,
}

/// Instantiates the stream function uses of one program's root.
pub(crate) struct Instantiator<'a> {
    callable: Option<&'a Callable>,
    context: &'a SourceContext,
    /// Every stream whose type is known, for inference.
    known: BTreeMap<VarName, StreamType>,
    generated: usize,
}

impl<'a> Instantiator<'a> {
    /// `declared` are the root's declared streams with their types, which
    /// are what a use's stream arguments can be inferred from.
    pub(crate) fn new(
        callable: Option<&'a Callable>,
        context: &'a SourceContext,
        declared: BTreeMap<VarName, StreamType>,
    ) -> Self {
        Self {
            callable,
            context,
            known: declared,
            generated: 0,
        }
    }

    /// Instantiate `expression` as the definition of `result`, if it is a
    /// stream function call.
    pub(crate) fn instantiate(
        &mut self,
        result: &VarName,
        expression: ParsedExprRef<'_>,
    ) -> Result<Option<Instantiated>, DsrvExpandError> {
        let ParsedExprKind::StreamFunctionCall(arguments, path, fixed) = expression.kind() else {
            return Ok(None);
        };
        let span = parsed::span_of(expression);
        let unknown = || DsrvExpandError::UnknownStreamFunction {
            name: path.to_string(),
            span,
        };
        let callable = self.callable.ok_or_else(unknown)?;
        let own = callable.own();
        let entry = callable.stream_function(own, path).ok_or_else(unknown)?;
        if !self.context.language().has(Feature::LocalStreams) {
            return Err(LanguageError::NeedsExperiment {
                construct: "a stream function call",
                feature: Feature::LocalStreams.name(),
                span,
            }
            .into());
        }
        let arguments = resolve_arguments(
            entry,
            expression,
            arguments,
            *fixed,
            callable,
            own,
            None,
            span,
            |cursor| verbatim(cursor),
        )?;
        let mut collected = Collected::default();
        let (tree, result_type) = self.expand(
            callable,
            entry,
            arguments,
            own,
            Names {
                result: result.clone(),
                prefix: result.name(),
            },
            span,
            &mut Vec::new(),
            &mut collected,
        )?;
        if let Some(ty) = &result_type {
            self.known.entry(result.clone()).or_insert(ty.clone());
        }
        Ok(Some(Instantiated {
            result: tree,
            result_type,
            generated: collected.generated,
            instances: collected.instances,
        }))
    }

    /// Generate one instance of `entry` defining `result`, returning the
    /// result's equation and declared type.
    #[allow(clippy::too_many_arguments)]
    fn expand(
        &mut self,
        callable: &Callable,
        entry: &StreamFunctionEntry,
        arguments: Arguments,
        from: LexicalId,
        names: Names,
        span: Span,
        chain: &mut Vec<(String, VarName)>,
        out: &mut Collected,
    ) -> Result<(ParsedExpr, Option<StreamType>), DsrvExpandError> {
        let Names { result, prefix } = names;
        let stream_function = entry.display();
        let describe = |chain: &[(String, VarName)], last: &str| {
            chain
                .iter()
                .map(|(stream_function, result)| format!("{result} = {stream_function}"))
                .chain([format!("{result} = {last}")])
                .collect::<Vec<_>>()
                .join(" -> ")
        };
        if chain.iter().any(|(active, _)| *active == stream_function) {
            return Err(DsrvExpandError::RecursiveStreamFunction {
                chain: describe(chain, &stream_function),
            });
        }
        if chain.len() >= MAX_DEPTH {
            return Err(DsrvExpandError::StreamFunctionLimit {
                limit: MAX_DEPTH,
                what: "levels of nesting",
                chain: describe(chain, &stream_function),
            });
        }
        let (context, _) = callable.environment(entry.lexical);
        let hints = self.hints(callable, &arguments, from);
        let bindings = infer(entry, &context, &hints, span)?;
        let types: BTreeMap<TypeName, SourceType> = bindings
            .into_iter()
            .map(|(name, ty)| (name, SourceType::from(ty)))
            .collect();
        let substitution = types.clone();
        let resolve = |ty: &SourceType| -> Result<StreamType, DsrvExpandError> {
            let bound = substitution.iter().collect();
            let ty = crate::lang::dsrv::source::substitute_source(ty, &bound);
            Ok(context.resolve_type(&ty)?)
        };

        let private = |name: &VarName| VarName::new(&format!("{prefix}${name}"));
        let mut names = BTreeMap::new();
        for (name, _) in &entry.streams {
            names.insert(name.clone(), private(name));
        }
        for equation in &entry.body {
            let renamed = if equation.output {
                result.clone()
            } else {
                private(&equation.name)
            };
            names.insert(equation.name.clone(), renamed);
        }
        self.generated += names.len();
        if self.generated > MAX_STREAMS {
            return Err(DsrvExpandError::StreamFunctionLimit {
                limit: MAX_STREAMS,
                what: "generated streams",
                chain: describe(chain, &stream_function),
            });
        }

        let mut data = BTreeMap::new();
        let mut functions = BTreeMap::new();
        for ((name, _), argument) in entry.fixed.iter().zip(arguments.fixed) {
            match argument {
                Fixed::Data(tree) => {
                    data.insert(name.clone(), tree);
                }
                Fixed::Function(key) => {
                    functions.insert(name.clone(), key);
                }
            }
        }
        // Each stream argument is read on every tick, where it was written.
        for ((name, ty), expression) in entry.streams.iter().zip(arguments.streams) {
            let annotation = resolve(ty)?;
            let name = names[name].clone();
            self.known.insert(name.clone(), annotation.clone());
            out.generated.push(GeneratedStream {
                name,
                annotation: Some(annotation),
                expression,
            });
        }
        let result_type = entry.result_type().map(&resolve).transpose()?;
        for equation in &entry.body {
            let ty = if equation.output {
                result_type.clone()
            } else {
                equation.ty.as_ref().map(&resolve).transpose()?
            };
            if let Some(ty) = ty {
                self.known.insert(names[&equation.name].clone(), ty);
            }
        }
        out.instances.push(StreamFunctionInstance {
            stream_function: stream_function.clone().into(),
            result: result.clone(),
            span,
        });

        let instance = Instance {
            names,
            data,
            functions,
            types,
            stamp: Stamp {
                span,
                definition: entry.source,
                lexical: Some(entry.lexical),
            },
        };
        chain.push((stream_function.clone(), result.clone()));
        let mut result_tree = None;
        for equation in &entry.body {
            let name = instance.names[&equation.name].clone();
            let tree = equation.tree.as_ref();
            let (expression, nested_type) =
                if let ParsedExprKind::StreamFunctionCall(arguments, path, fixed) = tree.kind() {
                    let inner = callable
                        .stream_function(entry.lexical, path)
                        .ok_or_else(|| DsrvExpandError::UnknownStreamFunction {
                            name: path.to_string(),
                            span,
                        })?;
                    let arguments = resolve_arguments(
                        inner,
                        tree,
                        arguments,
                        *fixed,
                        callable,
                        entry.lexical,
                        Some(&instance),
                        span,
                        |cursor| instance.copy_tree(cursor, callable),
                    )?;
                    // An `out` that is itself a use shares the result's name,
                    // so its private streams take the equation's name as
                    // well, and cannot meet this instance's own.
                    let prefix = if equation.output {
                        format!("{prefix}${}", equation.name)
                    } else {
                        name.name()
                    };
                    self.expand(
                        callable,
                        inner,
                        arguments,
                        entry.lexical,
                        Names {
                            result: name.clone(),
                            prefix,
                        },
                        span,
                        chain,
                        out,
                    )?
                } else {
                    (instance.copy_tree(tree, callable)?, None)
                };
            let ascribed = if equation.output {
                result_type.clone()
            } else {
                equation.ty.as_ref().map(&resolve).transpose()?
            };
            // A declared result is part of the stream function's contract, whatever
            // the use site declares its own stream as.
            let expression = match (&ascribed, equation.output) {
                (Some(ty), true) => ascribe(expression, ty, &instance.stamp)?,
                _ => expression,
            };
            if equation.output {
                result_tree = Some(expression);
            } else {
                let annotation = ascribed.or(nested_type);
                if let Some(ty) = &annotation {
                    self.known.insert(name.clone(), ty.clone());
                }
                out.generated.push(GeneratedStream {
                    name,
                    annotation,
                    expression,
                });
            }
        }
        chain.pop();
        Ok((
            result_tree.expect("a stream function entry has one result equation"),
            result_type,
        ))
    }

    /// What the use site states about each argument's type.
    fn hints(&self, callable: &Callable, arguments: &Arguments, from: LexicalId) -> Hints {
        let fixed = arguments
            .fixed
            .iter()
            .map(|argument| match argument {
                Fixed::Data(tree) => self.type_of(tree.as_ref(), callable, from),
                Fixed::Function(key) => signature(callable, key),
            })
            .collect();
        let streams = arguments
            .streams
            .iter()
            .map(|tree| self.type_of(tree.as_ref(), callable, from))
            .collect();
        Hints { fixed, streams }
    }

    /// The type an argument states: a stream with a known type, a literal,
    /// or an ascription. Anything else states none.
    fn type_of(
        &self,
        cursor: ParsedExprRef<'_>,
        callable: &Callable,
        from: LexicalId,
    ) -> Option<StreamType> {
        match cursor.kind() {
            ParsedExprKind::Var(name) => self.known.get(name).cloned(),
            ParsedExprKind::Val(value) => literal_type(value),
            ParsedExprKind::Ascribe(_, ty) => {
                let lexical = parsed::origin_of(cursor).lexical.unwrap_or(from);
                let (context, _) = callable.environment(lexical);
                context.resolve_type(ty).ok()
            }
            _ => None,
        }
    }
}

/// Wrap a result's equation in its declared type.
fn ascribe(
    expression: ParsedExpr,
    ty: &StreamType,
    stamp: &Stamp,
) -> Result<ParsedExpr, DsrvExpandError> {
    let mut builder = ParsedExprBuilder::with_capacity(expression.as_ref().subtree_ids().len() + 1);
    let inner = copy_verbatim(expression.as_ref(), &mut builder);
    let root = builder.alloc(
        ParsedExprKind::Ascribe(inner, SourceType::from(ty.clone())),
        ParsedOrigin {
            span: stamp.span,
            definition: None,
            lexical: stamp.lexical,
            generic_def: None,
            duration_seconds: None,
        },
    );
    finish(builder, root)
}

fn literal_type(value: &SyntaxLiteral) -> Option<StreamType> {
    Some(match value {
        SyntaxLiteral::Int(_) => StreamType::Int,
        SyntaxLiteral::Float(_) => StreamType::Float,
        SyntaxLiteral::Str(_) => StreamType::Str,
        SyntaxLiteral::Bool(_) => StreamType::Bool,
        SyntaxLiteral::Unit => StreamType::Unit,
        _ => return None,
    })
}

/// A def's declared signature, when it names no type parameter of its own.
fn signature(callable: &Callable, key: &DefKey) -> Option<StreamType> {
    let def = callable.def(key)?;
    if !def.type_parameters.is_empty() {
        return None;
    }
    let (context, _) = callable.environment(def.lexical?);
    let parameters = def
        .parameters
        .iter()
        .map(|(_, ty)| context.resolve_type(ty).ok())
        .collect::<Option<EcoVec<_>>>()?;
    let result = context.resolve_type(&def.result).ok()?;
    Some(StreamType::Function(parameters, Box::new(result)))
}

/// Resolve a use's arguments against the stream function it names: count them,
/// and settle what each fixed argument stands for.
///
/// `copy` gives an argument subtree in its final form; `enclosing` is the
/// instance a nested use is written in, whose own fixed function parameters
/// it may forward.
#[allow(clippy::too_many_arguments)]
fn resolve_arguments(
    entry: &StreamFunctionEntry,
    use_site: ParsedExprRef<'_>,
    arguments: &EcoVec<ParsedExprId>,
    fixed: usize,
    callable: &Callable,
    from: LexicalId,
    enclosing: Option<&Instance>,
    span: Span,
    mut copy: impl FnMut(ParsedExprRef<'_>) -> Result<ParsedExpr, DsrvExpandError>,
) -> Result<Arguments, DsrvExpandError> {
    let stream_function = entry.display();
    let streams = arguments.len() - fixed;
    for (kind, expected, found) in [
        ("fixed", entry.fixed.len(), fixed),
        ("stream", entry.streams.len(), streams),
    ] {
        if expected != found {
            return Err(DsrvExpandError::StreamFunctionArity {
                stream_function,
                kind,
                expected,
                found,
                span,
            });
        }
    }
    let problem = |parameter: &VarName, problem: String, at: Span| {
        DsrvExpandError::StreamFunctionFixedArgument {
            stream_function: stream_function.clone(),
            parameter: parameter.to_string(),
            problem,
            span: at,
        }
    };
    let mut resolved = Vec::with_capacity(fixed);
    for ((parameter, ty), argument) in entry.fixed.iter().zip(&arguments[..fixed]) {
        let cursor = use_site.child(*argument);
        let at = parsed::span_of(cursor);
        if is_function(ty) {
            let path = match cursor.kind() {
                ParsedExprKind::Var(name) => {
                    if let Some(key) = enclosing.and_then(|outer| outer.functions.get(name)) {
                        resolved.push(Fixed::Function(key.clone()));
                        continue;
                    }
                    ValuePath::new(EcoVec::new(), name.clone())
                }
                ParsedExprKind::ModuleItem(path) => path.clone(),
                ParsedExprKind::Lambda(..) => {
                    return Err(problem(
                        parameter,
                        "is an inline lambda; a fixed function argument names a `def` or \
                         forwards a fixed function parameter"
                            .to_owned(),
                        at,
                    ));
                }
                _ => {
                    return Err(problem(
                        parameter,
                        "must name a `def` or forward a fixed function parameter".to_owned(),
                        at,
                    ));
                }
            };
            let key = callable.resolve_def(from, &path).ok_or_else(|| {
                problem(parameter, format!("names `{path}`, which is no `def`"), at)
            })?;
            let def = callable.def(&key).expect("a resolved def is in the table");
            let bound = def
                .parameters
                .iter()
                .map(|(name, _)| name.clone())
                .collect();
            if let Some((why, _)) = closed_violation(def.body, bound)? {
                return Err(problem(
                    parameter,
                    format!("names `{path}`, which {why}"),
                    at,
                ));
            }
            resolved.push(Fixed::Function(key));
        } else {
            let tree = copy(cursor)?;
            if let Some((why, _)) = closed_violation(tree.as_ref(), Vec::new())? {
                return Err(problem(
                    parameter,
                    format!("is not a closed expression: it {why}"),
                    at,
                ));
            }
            resolved.push(Fixed::Data(tree));
        }
    }
    let streams = arguments[fixed..]
        .iter()
        .map(|argument| copy(use_site.child(*argument)))
        .collect::<Result<_, _>>()?;
    Ok(Arguments {
        fixed: resolved,
        streams,
    })
}

/// The types a use's arguments state, parameter by parameter.
struct Hints {
    fixed: Vec<Option<StreamType>>,
    streams: Vec<Option<StreamType>>,
}

/// Infer the stream function's type parameters from the types its arguments
/// state. Every parameter must be inferred; one inferred twice must agree.
fn infer(
    entry: &StreamFunctionEntry,
    context: &SourceContext,
    hints: &Hints,
    span: Span,
) -> Result<BTreeMap<TypeName, StreamType>, DsrvExpandError> {
    let stream_function = entry.display();
    let mut bindings = BTreeMap::new();
    let parameters = entry
        .fixed
        .iter()
        .zip(&hints.fixed)
        .chain(entry.streams.iter().zip(&hints.streams));
    for ((_, declared), stated) in parameters {
        let Some(stated) = stated else { continue };
        if let Err((parameter, first, second)) = unify(
            context,
            &entry.type_parameters,
            declared,
            stated,
            &mut bindings,
            0,
        ) {
            return Err(DsrvExpandError::StreamFunctionTypeInference {
                stream_function,
                parameter: parameter.to_string(),
                reason: format!("its arguments make it both {first} and {second}"),
                span,
            });
        }
    }
    for parameter in &entry.type_parameters {
        if !bindings.contains_key(parameter) {
            return Err(DsrvExpandError::StreamFunctionTypeInference {
                stream_function,
                parameter: parameter.to_string(),
                reason: "no argument states it; pass a stream with a declared type, or a \
                         typed fixed argument"
                    .to_owned(),
                span,
            });
        }
    }
    Ok(bindings)
}

type Conflict = (TypeName, StreamType, StreamType);

/// Match a declared parameter type against a stated argument type, binding
/// the type parameters it mentions. A shape that does not match binds
/// nothing: the type checker reports the mismatch on the generated stream.
fn unify(
    context: &SourceContext,
    parameters: &[TypeName],
    declared: &SourceType,
    stated: &StreamType,
    bindings: &mut BTreeMap<TypeName, StreamType>,
    depth: usize,
) -> Result<(), Conflict> {
    if depth > 64 {
        return Ok(());
    }
    let mut unify = |declared: &SourceType, stated: &StreamType| {
        unify(context, parameters, declared, stated, bindings, depth + 1)
    };
    match (&declared.kind, stated) {
        (SourceTypeKind::Named(path, arguments), _)
            if arguments.is_empty() && !path.is_qualified() && parameters.contains(path.name()) =>
        {
            let name = path.name().clone();
            match bindings.get(&name) {
                Some(first) if first != stated => {
                    return Err((name, first.clone(), stated.clone()));
                }
                Some(_) => {}
                None => {
                    bindings.insert(name, stated.clone());
                }
            }
        }
        (SourceTypeKind::Named(path, arguments), _) => {
            if let Some(alias) = context.generic().get(path)
                && alias.parameters.len() == arguments.len()
            {
                // A newtype's parameters need not occur in its
                // representation, so they are read from the stated
                // instance's arguments, not from the body.
                if let Some(stem) = alias.kind.newtype_stem() {
                    if let StreamType::Newtype(schema) = stated
                        && schema.stem() == stem.as_str()
                    {
                        for (declared, stated) in arguments.iter().zip(schema.arguments()) {
                            unify(declared, stated)?;
                        }
                    }
                    return Ok(());
                }
                let bound = alias.parameters.iter().zip(arguments.iter()).collect();
                let body = crate::lang::dsrv::source::substitute_source(&alias.ty, &bound);
                unify(&body, stated)?;
            }
        }
        (SourceTypeKind::NewtypeInstance(instance), StreamType::Newtype(schema))
            if schema.stem() == instance.stem.as_str() =>
        {
            for (declared, stated) in instance.arguments.iter().zip(schema.arguments()) {
                unify(declared, stated)?;
            }
        }
        (SourceTypeKind::List(declared), StreamType::List(stated))
        | (SourceTypeKind::Map(declared), StreamType::Map(stated))
        | (SourceTypeKind::Expr(declared), StreamType::Expr(stated)) => unify(declared, stated)?,
        (SourceTypeKind::Tuple(declared), StreamType::Tuple(stated))
            if declared.len() == stated.len() =>
        {
            for (declared, stated) in declared.iter().zip(stated) {
                unify(declared, stated)?;
            }
        }
        (SourceTypeKind::Struct(declared, _), StreamType::Struct(stated, _)) => {
            for (field, declared) in declared {
                if let Some((_, stated)) = stated.iter().find(|(name, _)| name == field) {
                    unify(declared, stated)?;
                }
            }
        }
        (SourceTypeKind::Function(declared, result), StreamType::Function(stated, returned))
            if declared.len() == stated.len() =>
        {
            for (declared, stated) in declared.iter().zip(stated) {
                unify(declared, stated)?;
            }
            unify(result, returned)?;
        }
        (SourceTypeKind::Union(declared), StreamType::Union(stated)) => {
            for alternative in declared {
                let Some(payload) = &alternative.payload else {
                    continue;
                };
                if let Some(UnionPayload::Of(stated)) = stated
                    .alternatives()
                    .iter()
                    .find(|candidate| *candidate.tag() == alternative.tag)
                    .map(|candidate| candidate.payload())
                {
                    unify(payload, stated)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}
