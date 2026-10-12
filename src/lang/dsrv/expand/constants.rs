//! Every `const` of a program, folded to the literal it stands for.
//!
//! A constant is not a def of no arguments. A def's body travels as a tree
//! and is copied into each caller (S18); a constant's body is **evaluated
//! once**, and what it yields is a value. The difference is forced by where
//! a constant has to be usable: a stream offset `x[n]` holds a number, not
//! a subtree, so nothing that only copies trees could put a constant there.
//!
//! A body is folded in one of two ways, and both settle it while the
//! program is still being read:
//!
//! - A body of literals, operators and other constants is folded directly
//!   over values. Folding borrows the runtime's own arithmetic rather than
//!   repeating it, so a constant and the same expression written out agree
//!   by construction, and `1 / 0` is refused.
//! - A body that calls a def, builds a value with a newtype's `new`, or
//!   names a constant holding a newtype value is folded as an equation of
//!   its module would be run: its calls are inlined, it is expanded in the
//!   module's namespace, so `new` and `value` are refused outside the
//!   declaring module exactly as they are in an equation, it is checked
//!   strictly against the constant's declared type, which is what infers a
//!   generic newtype's phantom arguments, and it is elaborated and evaluated
//!   once by the atemporal evaluator the runtimes use within a tick.
//!
//! A newtype value is kept with the instance it was built as
//! ([`SyntaxLiteral::Newtype`]), so wherever the constant is named, in its
//! own module or another, it is a value of that newtype with the brand its
//! body gave it. No source spells such a literal, so a constant cannot hold
//! a newtype value its declaring module's code did not build.
//!
//! Entries are built in the dependency order the namespaces are, since a
//! constant follows the same `use` edges as a type or a def. A constant may
//! call a def and a def may name a constant, so each module's constants are
//! folded after the defs of every module it imports are in the function
//! table and before its own defs are added: a def of its own module that a
//! constant calls is inlined from its source.

use std::collections::{BTreeMap, BTreeSet};

use contiguous_tree::{TreeCursor, TreeCursorExt};
use ecow::EcoString;

use crate::VarName;
use crate::core::values::operations;
use crate::core::{NewtypeSchema, StreamType, Value};
use crate::lang::dsrv::ast::{
    AstShared, DsrvAstError, ExprBuilder, ExprKind, ExprRef, SyntaxLiteral,
};
use crate::lang::dsrv::modules::{ModulePath, ModuleSources};
use crate::lang::dsrv::path::ModuleName;
use crate::lang::dsrv::source::SourceContext;
use crate::lang::dsrv::source_map::{SourceArchive, SourceId};
use crate::lang::dsrv::span::Span;
use crate::lang::dsrv::syntax::parsed::{self, ParsedExprKind, ParsedExprRef};
use crate::lang::dsrv::syntax::{ParsedDeclaration, ParsedSpecification};
use crate::lang::dsrv::type_checker::{TCType, check_expression, check_value_stream_type};

use super::functions::{self, Callable, FunctionTable};
use super::inline::{self, Def};
use super::{DsrvExpandError, language};

/// One constant, already folded.
#[derive(Debug)]
pub(crate) struct ConstEntry {
    /// Hidden from importers, usable inside its own module (S12).
    pub(crate) internal: bool,
    pub(crate) value: SyntaxLiteral,
}

/// Every constant of a program, by the module that declared it.
///
/// Each module's constants are shared, so a copy of the table copies one
/// pointer per module: the function table keeps, for each module, the table
/// as it stood once that module's constants were folded.
#[derive(Clone, Debug, Default)]
pub(crate) struct ConstantTable {
    modules: BTreeMap<ModulePath, AstShared<BTreeMap<VarName, ConstEntry>>>,
}

impl ConstantTable {
    pub(crate) fn is_empty(&self) -> bool {
        self.modules.values().all(|entries| entries.is_empty())
    }

    /// Every constant a module exports, which is all but its internal ones.
    pub(crate) fn exported(&self, module: &[ModuleName]) -> Vec<(&VarName, &ConstEntry)> {
        self.declared(module)
            .into_iter()
            .filter(|(_, entry)| !entry.internal)
            .collect()
    }

    /// Every constant a module declared, the internal ones included: what
    /// that module's own source text may name (S12). Text supplied at run
    /// time is not the module's own and sees only [`Self::exported`].
    pub(crate) fn declared(&self, module: &[ModuleName]) -> Vec<(&VarName, &ConstEntry)> {
        self.modules
            .get(module)
            .map(|entries| entries.iter().collect())
            .unwrap_or_default()
    }
}

/// What a module's constants are folded against, beyond the constants
/// themselves: the defs they may call and the namespace they are read in.
pub(crate) struct Folding<'a> {
    /// Every def of the modules this one imports, and this module's own
    /// environment, which a call is expanded in.
    pub(crate) functions: &'a AstShared<FunctionTable>,
    /// The module's own namespace and settings.
    pub(crate) context: &'a AstShared<SourceContext>,
    /// The archive the module's file is in, which a finding of checking is
    /// located in; `None` for an unlocated program.
    pub(crate) archive: Option<&'a SourceArchive>,
    /// How the module's file is named in a diagnostic.
    pub(crate) label: &'a str,
}

/// The label a diagnostic names a module's file by.
pub(crate) fn label_of(sources: &ModuleSources, path: &ModulePath) -> String {
    sources
        .get(path)
        .and_then(|parsed| parsed.source())
        .and_then(|source| sources.archive().file(source))
        .map(|file| file.label().to_string())
        .unwrap_or_else(|| crate::lang::dsrv::modules::show_path(path))
}

/// Add one module's constants to `table`, folding each against what the
/// module imported and against the module's own constants and defs.
///
/// Every module this one imports must already be in `table` and in
/// `folding`'s function table, which is what the dependency order gives,
/// and this module's environment must be in the function table too.
pub(crate) fn add_module(
    table: &mut ConstantTable,
    path: &ModulePath,
    parsed: &ParsedSpecification,
    folding: &Folding<'_>,
) -> Result<(), DsrvExpandError> {
    let declarations = parsed.declarations();
    let held = parsed.roots();
    let trees: Vec<ParsedExprRef<'_>> = held.iter().map(|tree| tree.as_ref()).collect();

    let mut local = Vec::new();
    let mut defs = BTreeMap::new();
    let mut root = 0usize;
    for declaration in declarations {
        match declaration {
            ParsedDeclaration::Const {
                name,
                ty,
                internal,
                span,
                ..
            } => {
                let typ = folding.context.resolve_type(ty)?;
                local.push((name.clone(), root, *internal, typ, *span));
                root += 1;
            }
            ParsedDeclaration::Def {
                name,
                type_parameters,
                parameters,
                result,
                ..
            } => {
                defs.insert(
                    name.clone(),
                    Def {
                        parameters: parameters.clone(),
                        type_parameters: type_parameters.clone(),
                        result: result.clone(),
                        body: trees[root],
                        foreign: false,
                        source: parsed.source(),
                        lexical: None,
                    },
                );
                root += 1;
            }
            ParsedDeclaration::Equation(..)
            | ParsedDeclaration::Output(_, _, Some(_), _)
            | ParsedDeclaration::Aux(_, _, Some(_), _) => root += 1,
            _ => {}
        }
    }
    if local.is_empty() {
        return Ok(());
    }

    // What this module's own bodies may name: its imports, then its own
    // constants, so a local name wins as it does for types (S11).
    let imported = functions::imported_modules(path, declarations)?;
    let mut scope = ConstScope {
        own: path.clone(),
        ..ConstScope::default()
    };
    for (module, bare) in &imported {
        for (name, entry) in table.exported(module) {
            if *bare {
                scope.bare.insert(name.clone(), entry.value.clone());
            }
            scope
                .qualified
                .insert((module.clone(), name.clone()), entry.value.clone());
        }
    }

    // A constant may be written before or after the one it names, so the
    // bodies are folded by need rather than in declaration order.
    let folder = Folder {
        pending: local
            .iter()
            .map(|(name, index, _, typ, _)| (name.clone(), (*index, typ)))
            .collect(),
        trees: &trees,
        defs,
        imported,
        path,
        source: parsed.source(),
        folding,
    };
    let mut active = Vec::new();
    for (name, ..) in &local {
        folder.fold_pending(name, &mut scope, &mut active)?;
    }
    let mut entries = BTreeMap::new();
    for (name, _, internal, expected, span) in local {
        let value = scope
            .bare
            .get(&name)
            .expect("every local constant was folded")
            .clone();
        check_value_stream_type(&expected, &value.clone().into_runtime_value()).map_err(
            |message| DsrvExpandError::ConstantType {
                name: name.to_string(),
                location: folding.label.to_owned(),
                expected,
                message,
                span,
            },
        )?;
        entries.insert(name, ConstEntry { internal, value });
    }
    table.modules.insert(path.clone(), AstShared::new(entries));
    Ok(())
}

/// One module's constants while they are being folded.
struct Folder<'a, 'tree> {
    /// Each constant this module declares: its body's root, and its
    /// declared type.
    pending: BTreeMap<VarName, (usize, &'a StreamType)>,
    trees: &'a [ParsedExprRef<'tree>],
    /// The module's own defs, by name.
    defs: BTreeMap<VarName, Def<'tree>>,
    /// Every module this one imported, and whether it brought its names in
    /// bare.
    imported: BTreeMap<ModulePath, bool>,
    path: &'a ModulePath,
    /// The archived file the module is, by ID.
    source: Option<SourceId>,
    folding: &'a Folding<'a>,
}

impl Folder<'_, '_> {
    /// Fold one of this module's own constants, and whatever it names,
    /// first.
    fn fold_pending(
        &self,
        name: &VarName,
        scope: &mut ConstScope,
        active: &mut Vec<VarName>,
    ) -> Result<(), DsrvExpandError> {
        if scope.local.contains(name) {
            return Ok(());
        }
        let Some((index, expected)) = self.pending.get(name) else {
            return Ok(());
        };
        if active.contains(name) {
            return Err(DsrvExpandError::RecursiveConstant {
                name: name.name().to_string(),
            });
        }
        active.push(name.clone());
        // Whatever this body names, directly or through a def of this
        // module it calls, must be folded before it is.
        let body = self.trees[*index];
        let mut named = Vec::new();
        names_in(
            body,
            &self.defs,
            &mut Vec::new(),
            &mut BTreeSet::new(),
            &mut named,
        );
        for named in named {
            self.fold_pending(&named, scope, active)?;
        }
        let value = if needs_checking(body, scope, self.folding.context) {
            self.fold_checked(name, body, expected, scope)?
        } else {
            fold(body, scope)?
        };
        active.pop();
        scope.local.insert(name.clone());
        scope.bare.insert(name.clone(), value);
        Ok(())
    }

    /// Fold a body that calls a def or builds a newtype value: inline,
    /// expand, check against `expected`, elaborate and evaluate it, as an
    /// equation of this module would be.
    fn fold_checked(
        &self,
        name: &VarName,
        body: ParsedExprRef<'_>,
        expected: &StreamType,
        scope: &ConstScope,
    ) -> Result<SyntaxLiteral, DsrvExpandError> {
        let span = parsed::span_of(body);
        // A constant is a value, so no template instance can live in one.
        let inlined = {
            let mut inline_scope =
                functions::imported_scope(&self.imported, self.folding.functions);
            for (name, def) in &self.defs {
                inline_scope.bare.insert(name.clone(), def.clone());
            }
            inline_scope.constants = Some(scope);
            inline::standalone(body, &inline_scope, [])?
        };
        // What only a running monitor settles is refused here, inside a
        // def the body calls as much as in the body itself.
        for node in inlined.as_ref().postorder() {
            if let Some(construct) = unsettled(node.kind()) {
                return Err(DsrvExpandError::NotConstant {
                    construct,
                    span: parsed::span_of(node),
                });
            }
        }

        let callable = AstShared::new(Callable::new(
            AstShared::clone(self.folding.functions),
            self.path,
        ));
        let mut builder = ExprBuilder::with_capacity(inlined.as_ref().subtree_ids().len());
        let root = super::expand_tree(
            inlined.as_ref(),
            &mut builder,
            self.folding.context,
            Some(&callable),
            self.source,
            false,
        )?;
        let expr = builder.finish(root).map_err(DsrvAstError::from)?;
        if let Some(key) = expr.as_ref().duplicate_field() {
            return Err(DsrvAstError::DuplicateExpressionField { field: key.clone() }.into());
        }
        let language = self.folding.context.language();
        for node in expr.as_ref().postorder() {
            let written = node
                .source_context()
                .map_or(language, SourceContext::language);
            language::check_experiment_node(node, written)?;
            language::check_dialect_node(node, language.dialect())?;
        }
        // A name that is neither a constant nor bound in the body is a
        // stream, or nothing at all.
        if let Some(free) = expr.as_ref().free_variables().into_iter().next() {
            let at = expr
                .as_ref()
                .postorder()
                .find(|node| matches!(node.kind(), ExprKind::Var(name) if *name == free))
                .map_or(span, |node| node.span());
            return Err(DsrvExpandError::UnknownConstant {
                name: free.name().to_string(),
                span: at,
            });
        }

        let checked = check_expression(
            expr,
            &TCType::from_stream_type(expected),
            &AstShared::new(BTreeMap::new()),
            self.folding.archive,
        )
        .discard_warnings()
        .map_err(|errors| DsrvExpandError::ConstantCheck {
            name: name.to_string(),
            location: self.folding.label.to_owned(),
            errors,
            span,
        })?;
        // As text supplied at run time is: only a body that reaches a
        // newtype has anything to elaborate.
        let nominal = checked
            .as_ref()
            .postorder()
            .any(|node| crate::lang::dsrv::type_checker::mentions_newtype(node.typ()));
        let checked = if nominal {
            checked.elaborate()
        } else {
            checked
        };

        use crate::semantics::untimed_dsrv::atemporal::{AtemporalError, eval_atemporal};
        let tree = checked.expr().as_ref();
        let value = eval_atemporal(tree, &BTreeMap::new()).map_err(|error| match error {
            AtemporalError::Unbound { name, span } => DsrvExpandError::UnknownConstant {
                name: name.name().to_string(),
                span,
            },
            AtemporalError::Temporal { span, .. } => DsrvExpandError::NotConstant {
                construct: "a stream operation",
                span,
            },
            error => DsrvExpandError::ConstantValue {
                message: error.to_string(),
                span,
            },
        })?;
        if matches!(value, Value::NoVal | Value::Deferred) {
            return Err(DsrvExpandError::ConstantValue {
                message: format!("it is absent ({value})"),
                span,
            });
        }
        literal_of(value, &instances_in(tree))
            .map_err(|message| DsrvExpandError::ConstantValue { message, span })
    }
}

/// Every bare name a body mentions, which is what it might be built from,
/// following a call to a def of this module into the def's body. A name a
/// lambda, a pattern or a def's parameter binds is not a constant there.
fn names_in(
    cursor: ParsedExprRef<'_>,
    defs: &BTreeMap<VarName, Def<'_>>,
    bound: &mut Vec<VarName>,
    entered: &mut BTreeSet<VarName>,
    found: &mut Vec<VarName>,
) {
    match cursor.kind() {
        ParsedExprKind::Var(name) if !bound.contains(name) => found.push(name.clone()),
        ParsedExprKind::Lambda(parameters, body) => {
            let count = bound.len();
            bound.extend(parameters.iter().map(|(name, _)| name.clone()));
            names_in(cursor.child(*body), defs, bound, entered, found);
            bound.truncate(count);
            return;
        }
        ParsedExprKind::Match(scrutinee, arms, shape) => {
            names_in(cursor.child(*scrutinee), defs, bound, entered, found);
            let mut children = arms.iter();
            for arm in shape {
                let count = bound.len();
                bound.extend(arm.pattern.bound_names());
                for _ in 0..arm.children() {
                    let child = children.next().expect("match shape covers its children");
                    names_in(cursor.child(*child), defs, bound, entered, found);
                }
                bound.truncate(count);
            }
            return;
        }
        ParsedExprKind::Matches(scrutinee, guard, pattern) => {
            names_in(cursor.child(*scrutinee), defs, bound, entered, found);
            let count = bound.len();
            bound.extend(pattern.bound_names());
            for child in guard {
                names_in(cursor.child(*child), defs, bound, entered, found);
            }
            bound.truncate(count);
            return;
        }
        ParsedExprKind::Apply(function, _) => {
            if let ParsedExprKind::Var(callee) = cursor.child(*function).kind()
                && !bound.contains(callee)
                && let Some(def) = defs.get(callee)
                && entered.insert(callee.clone())
            {
                let mut parameters = def
                    .parameters
                    .iter()
                    .map(|(name, _)| name.clone())
                    .collect();
                names_in(def.body, defs, &mut parameters, entered, found);
            }
        }
        _ => {}
    }
    for child in cursor.child_ids() {
        names_in(cursor.child(child), defs, bound, entered, found);
    }
}

/// Whether a body must be folded as an equation is run rather than over
/// values: it calls something, builds or reads a newtype value with `new`
/// or `value`, or names a constant that holds a newtype value, whose
/// operations only checking can judge.
fn needs_checking(cursor: ParsedExprRef<'_>, scope: &ConstScope, context: &SourceContext) -> bool {
    cursor.postorder().any(|node| match node.kind() {
        ParsedExprKind::Apply(..) => true,
        ParsedExprKind::Constructor(_, _, Some(qualifier)) => {
            super::newtype_target(context, qualifier).is_some()
        }
        ParsedExprKind::Var(name) => scope.bare.get(name).is_some_and(holds_newtype),
        ParsedExprKind::ModuleItem(path) => {
            ConstantNames::through_module(scope, path.module(), path.name())
                .is_some_and(holds_newtype)
        }
        _ => false,
    })
}

fn holds_newtype(value: &SyntaxLiteral) -> bool {
    match value {
        SyntaxLiteral::Newtype(..) => true,
        SyntaxLiteral::List(values) | SyntaxLiteral::Tuple(values) => {
            values.iter().any(holds_newtype)
        }
        SyntaxLiteral::Map(values) | SyntaxLiteral::Struct(values) => {
            values.values().any(holds_newtype)
        }
        _ => false,
    }
}

/// The construct a node is, where it is one that only a running monitor
/// settles.
fn unsettled(kind: &ParsedExprKind) -> Option<&'static str> {
    Some(match kind {
        ParsedExprKind::SIndex(..) => "a stream offset",
        ParsedExprKind::Dynamic(..) | ParsedExprKind::Defer(..) => "text supplied at runtime",
        ParsedExprKind::Update(..)
        | ParsedExprKind::Latch(..)
        | ParsedExprKind::Init(..)
        | ParsedExprKind::When(..)
        | ParsedExprKind::IsDefined(..)
        | ParsedExprKind::Default(..) => "a stream operation",
        _ => return None,
    })
}

/// Every newtype instance a checked body builds, or names through another
/// constant, by brand: what a value it evaluated to was built as.
fn instances_in(tree: ExprRef<'_>) -> BTreeMap<EcoString, NewtypeSchema<StreamType>> {
    fn in_literal(
        value: &SyntaxLiteral,
        found: &mut BTreeMap<EcoString, NewtypeSchema<StreamType>>,
    ) {
        match value {
            SyntaxLiteral::Newtype(schema, value) => {
                found.insert(schema.brand().clone(), schema.clone());
                in_literal(value, found);
            }
            SyntaxLiteral::List(values) | SyntaxLiteral::Tuple(values) => {
                values.iter().for_each(|value| in_literal(value, found));
            }
            SyntaxLiteral::Map(values) | SyntaxLiteral::Struct(values) => {
                values.values().for_each(|value| in_literal(value, found));
            }
            _ => {}
        }
    }
    let mut found = BTreeMap::new();
    for node in tree.postorder() {
        match node.kind() {
            ExprKind::Wrap(_, schema) => {
                found.insert(schema.brand().clone(), schema.clone());
            }
            ExprKind::Val(value) => in_literal(value, &mut found),
            _ => {}
        }
    }
    found
}

/// The literal a checked body's value is, each newtype value in it kept
/// with the instance it was built as.
fn literal_of(
    value: Value,
    instances: &BTreeMap<EcoString, NewtypeSchema<StreamType>>,
) -> Result<SyntaxLiteral, String> {
    let items = |values: ecow::EcoVec<Value>| {
        values
            .into_iter()
            .map(|value| literal_of(value, instances))
            .collect::<Result<_, _>>()
    };
    Ok(match value {
        Value::Newtype(value) => {
            let schema = instances
                .get(value.brand())
                .ok_or_else(|| format!("no instance of `{}` was built", value.brand()))?;
            SyntaxLiteral::Newtype(
                schema.clone(),
                Box::new(literal_of(value.value().clone(), instances)?),
            )
        }
        Value::List(values) => SyntaxLiteral::List(items(values)?),
        Value::Tuple(values) => SyntaxLiteral::Tuple(items(values)?),
        Value::Map(values) => SyntaxLiteral::Map(
            values
                .into_iter()
                .map(|(key, value)| Ok((key, literal_of(value, instances)?)))
                .collect::<Result<_, String>>()?,
        ),
        other => SyntaxLiteral::try_from(other).map_err(|error| error.to_string())?,
    })
}

/// What a constant's body may name.
#[derive(Default)]
struct ConstScope {
    /// The module the constants being folded belong to.
    own: ModulePath,
    bare: BTreeMap<VarName, SyntaxLiteral>,
    qualified: BTreeMap<(Vec<ModuleName>, VarName), SyntaxLiteral>,
    /// The names this module declared itself, which have been folded.
    local: BTreeSet<VarName>,
}

/// What a name stands for where it names a constant, as inlining writes
/// one in.
pub(crate) trait ConstantNames {
    /// The constant a bare name stands for.
    fn bare(&self, name: &VarName) -> Option<&SyntaxLiteral>;
    /// The constant `name` stands for in `module`.
    fn through_module(&self, module: &[ModuleName], name: &VarName) -> Option<&SyntaxLiteral>;
    /// Whether no name stands for a constant.
    fn is_empty(&self) -> bool;
}

impl ConstantNames for ConstScope {
    fn bare(&self, name: &VarName) -> Option<&SyntaxLiteral> {
        self.bare.get(name)
    }

    fn through_module(&self, module: &[ModuleName], name: &VarName) -> Option<&SyntaxLiteral> {
        if module == self.own.as_slice() {
            return self
                .local
                .contains(name)
                .then(|| self.bare.get(name))
                .flatten();
        }
        self.qualified.get(&(module.to_vec(), name.clone()))
    }

    fn is_empty(&self) -> bool {
        self.bare.is_empty() && self.qualified.is_empty()
    }
}

/// Fold one body to the literal it stands for, refusing anything that is
/// not settled where the file is read.
fn fold(cursor: ParsedExprRef<'_>, scope: &ConstScope) -> Result<SyntaxLiteral, DsrvExpandError> {
    let span = parsed::span_of(cursor);
    let refuse = |construct: &'static str| DsrvExpandError::NotConstant { construct, span };
    if let Some(construct) = unsettled(cursor.kind()) {
        return Err(refuse(construct));
    }
    Ok(match cursor.kind() {
        ParsedExprKind::Val(value) => value.clone(),
        ParsedExprKind::Var(name) => {
            scope
                .bare
                .get(name)
                .cloned()
                .ok_or_else(|| DsrvExpandError::UnknownConstant {
                    name: name.name().to_string(),
                    span,
                })?
        }
        ParsedExprKind::ModuleItem(path) => scope
            .qualified
            .get(&(path.module().to_vec(), path.name().clone()))
            .cloned()
            .ok_or_else(|| DsrvExpandError::UnknownConstant {
                name: path.to_string(),
                span,
            })?,
        ParsedExprKind::BinOp(left, right, operator) => {
            let left = fold(cursor.child(*left), scope)?;
            let right = fold(cursor.child(*right), scope)?;
            evaluated(
                operations::binary(*operator, left.into(), right.into()),
                span,
            )?
        }
        ParsedExprKind::Not(value) => {
            let value = fold(cursor.child(*value), scope)?;
            evaluated(
                operations::unary(crate::core::UnaryOperator::Not, value.into()),
                span,
            )?
        }
        ParsedExprKind::Neg(value) => {
            let value = fold(cursor.child(*value), scope)?;
            evaluated(
                operations::unary(crate::core::UnaryOperator::Negate, value.into()),
                span,
            )?
        }
        ParsedExprKind::List(items) => SyntaxLiteral::List(
            items
                .iter()
                .map(|item| fold(cursor.child(*item), scope))
                .collect::<Result<_, _>>()?,
        ),
        ParsedExprKind::Tuple(items) => SyntaxLiteral::Tuple(
            items
                .iter()
                .map(|item| fold(cursor.child(*item), scope))
                .collect::<Result<_, _>>()?,
        ),
        ParsedExprKind::Map(fields) => SyntaxLiteral::Map(
            fields
                .iter()
                .map(|(key, id)| Ok((key.clone(), fold(cursor.child(*id), scope)?)))
                .collect::<Result<_, DsrvExpandError>>()?,
        ),
        ParsedExprKind::Struct(fields) | ParsedExprKind::ObjectLiteral(fields) => {
            SyntaxLiteral::Struct(
                fields
                    .iter()
                    .map(|(key, id)| Ok((key.clone(), fold(cursor.child(*id), scope)?)))
                    .collect::<Result<_, DsrvExpandError>>()?,
            )
        }
        ParsedExprKind::Partial(..) => return Err(refuse("a call")),
        ParsedExprKind::Lambda(..) | ParsedExprKind::Fix(..) => {
            return Err(refuse("a function"));
        }
        ParsedExprKind::Match(..) | ParsedExprKind::Matches(..) => {
            return Err(refuse("a match"));
        }
        ParsedExprKind::Constructor(..) => return Err(refuse("a constructor")),
        _ => return Err(refuse("this expression")),
    })
}

/// One folded operation, with a failure reported where it was written.
fn evaluated(
    result: Result<crate::core::Value, crate::core::values::operations::ValueOpError>,
    span: Span,
) -> Result<SyntaxLiteral, DsrvExpandError> {
    let value = result.map_err(|error| DsrvExpandError::ConstantValue {
        message: error.to_string(),
        span,
    })?;
    SyntaxLiteral::try_from(value).map_err(|error| DsrvExpandError::ConstantValue {
        message: error.to_string(),
        span,
    })
}

/// The constants one module may name, in a form that can be stored.
#[derive(Clone, Debug, Default)]
pub(crate) struct Constants {
    table: AstShared<ConstantTable>,
    own: ModulePath,
    imported: BTreeMap<ModulePath, bool>,
    /// Whether this is the view text supplied at run time has: the module's
    /// own constants are then only the ones it exports, as an importer sees
    /// them.
    runtime_text: bool,
}

impl Constants {
    pub(crate) fn new(
        table: AstShared<ConstantTable>,
        path: &ModulePath,
        declarations: &[ParsedDeclaration],
    ) -> Result<Self, DsrvExpandError> {
        Ok(Self {
            table,
            own: path.clone(),
            imported: functions::imported_modules(path, declarations)?,
            runtime_text: false,
        })
    }

    /// The constants text supplied at run time to a node of this module may
    /// name: the module's exported constants, never its internal ones, and
    /// what it imports.
    pub(crate) fn for_runtime_text(&self) -> Self {
        Self {
            runtime_text: true,
            ..self.clone()
        }
    }

    /// The constants the module itself declared that this view may name.
    fn own_entries(&self) -> Vec<(&VarName, &ConstEntry)> {
        if self.runtime_text {
            self.table.exported(&self.own)
        } else {
            self.table.declared(&self.own)
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// What this module may name, written into a node's identity (S20).
    pub(crate) fn describe(&self, out: &mut String) {
        use std::fmt::Write as _;
        if self.is_empty() {
            return;
        }
        for (name, entry) in self.own_entries() {
            let _ = write!(out, "{}={:?};", name.name(), entry.value);
        }
        for (module, _) in &self.imported {
            for (name, entry) in self.table.exported(module) {
                let _ = write!(
                    out,
                    "{}::{}={:?};",
                    crate::lang::dsrv::modules::show_path(module),
                    name.name(),
                    entry.value
                );
            }
        }
    }
}

impl ConstantNames for Constants {
    /// What a name in this module stands for, bare or through a module.
    fn bare(&self, name: &VarName) -> Option<&SyntaxLiteral> {
        self.own_entries()
            .into_iter()
            .find(|(declared, _)| *declared == name)
            .map(|(_, entry)| &entry.value)
            .or_else(|| {
                self.imported
                    .iter()
                    .filter(|(_, bare)| **bare)
                    .find_map(|(module, _)| {
                        self.table
                            .exported(module)
                            .into_iter()
                            .find(|(exported, _)| *exported == name)
                            .map(|(_, entry)| &entry.value)
                    })
            })
    }

    fn through_module(&self, module: &[ModuleName], name: &VarName) -> Option<&SyntaxLiteral> {
        let reachable = module == self.own.as_slice() || self.imported.contains_key(module);
        if !reachable {
            return None;
        }
        let entries = if module == self.own.as_slice() {
            self.own_entries()
        } else {
            self.table.exported(module)
        };
        entries
            .into_iter()
            .find(|(declared, _)| *declared == name)
            .map(|(_, entry)| &entry.value)
    }

    fn is_empty(&self) -> bool {
        Constants::is_empty(self)
    }
}

/// Two are the same where they offer the same constants, which sharing a
/// table and standing in the same module is what says (S20).
impl PartialEq for Constants {
    fn eq(&self, other: &Self) -> bool {
        if self.is_empty() && other.is_empty() {
            return true;
        }
        AstShared::ptr_eq(&self.table, &other.table)
            && self.own == other.own
            && self.imported == other.imported
            && self.runtime_text == other.runtime_text
    }
}
