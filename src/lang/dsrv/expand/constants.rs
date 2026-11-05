//! Fold literal, arithmetic and container constants once at load time.
//!
//! Initializers are checked at their declared types before folding. Each use
//! retains that type, so a named constant never becomes a fresh literal.

use std::collections::{BTreeMap, BTreeSet};

use contiguous_tree::{TreeCursor, TreeCursorExt};

use crate::VarName;
use crate::core::{StreamType, Value};
use crate::lang::dsrv::ast::{AstShared, DsrvAstError, ExprBuilder, ExprKind, SyntaxLiteral};
use crate::lang::dsrv::modules::{ModulePath, ModuleSources};
use crate::lang::dsrv::path::ModuleName;
use crate::lang::dsrv::source::SourceContext;
use crate::lang::dsrv::source_map::{SourceArchive, SourceId};
use crate::lang::dsrv::syntax::parsed::{self, ParsedExprKind, ParsedExprRef};
use crate::lang::dsrv::syntax::{ParsedDeclaration, ParsedSpecification};
use crate::lang::dsrv::type_checker::check_and_elaborate_expression;

use super::functions;
use super::graph::{self, ModuleGraph};
use super::inline;
use super::{DsrvExpandError, language};

/// One constant, already folded.
#[derive(Debug)]
pub(crate) struct ConstEntry {
    /// Hidden from importers, usable inside its own module.
    pub(crate) internal: bool,
    pub(crate) value: TypedConstant,
}

/// A normalized constant retains its declared type at every use.
#[derive(Clone, Debug)]
pub(crate) struct TypedConstant {
    pub(crate) literal: SyntaxLiteral,
    pub(crate) typ: AstShared<StreamType>,
}

/// Every constant of a program, by the module that declared it.
#[derive(Debug, Default)]
pub(crate) struct ConstantTable {
    entries: BTreeMap<(ModulePath, VarName), ConstEntry>,
}

impl ConstantTable {
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Every constant a module exports, which is all but its internal ones.
    pub(crate) fn exported(&self, module: &[ModuleName]) -> Vec<(&VarName, &ConstEntry)> {
        self.declared(module)
            .into_iter()
            .filter(|(_, entry)| !entry.internal)
            .collect()
    }

    /// Every constant a module declared, the internal ones included: what
    /// that module's own text may name.
    pub(crate) fn declared(&self, module: &[ModuleName]) -> Vec<(&VarName, &ConstEntry)> {
        self.entries
            .iter()
            .filter(|((path, _), _)| path.as_slice() == module)
            .map(|((_, name), entry)| (name, entry))
            .collect()
    }
}

/// Build every module's constants, importers after what they import.
pub(crate) fn build_constant_table(
    sources: &ModuleSources,
    graph: &ModuleGraph,
) -> Result<ConstantTable, DsrvExpandError> {
    let mut table = ConstantTable::default();
    for path in graph::dependency_order(sources)? {
        let parsed = sources
            .get(&path)
            .expect("every path in the order was collected");
        let context = graph
            .get(&path)
            .expect("every path in the order has a namespace");
        let source = parsed
            .source()
            .and_then(|source| sources.archive().file(source))
            .map(|file| file.label().to_string())
            .unwrap_or_else(|| crate::lang::dsrv::modules::show_path(&path));
        add_module(
            &mut table,
            &path,
            parsed,
            context,
            Some(sources.archive()),
            &source,
        )?;
    }
    Ok(table)
}

/// The constants of a program that is one file, which is every program
/// until it takes on modules. The file is its own root module.
pub(crate) fn single_file_constants(
    parsed: &ParsedSpecification,
    context: &AstShared<SourceContext>,
    archive: Option<&SourceArchive>,
) -> Result<ConstantTable, DsrvExpandError> {
    let mut table = ConstantTable::default();
    add_module(
        &mut table,
        &ModulePath::new(),
        parsed,
        context,
        archive,
        "<string>",
    )?;
    Ok(table)
}

/// Add one module's constants to `table`, folding each against what the
/// module imported and against the module's own constants.
///
/// Every module this one imports must already be in `table`, which is what
/// the dependency order gives.
fn add_module(
    table: &mut ConstantTable,
    path: &ModulePath,
    parsed: &ParsedSpecification,
    context: &AstShared<SourceContext>,
    archive: Option<&SourceArchive>,
    source: &str,
) -> Result<(), DsrvExpandError> {
    let declarations = parsed.declarations();
    let held = parsed.roots();
    let trees: Vec<ParsedExprRef<'_>> = held.iter().map(|tree| tree.as_ref()).collect();

    let mut local = Vec::new();
    let mut root = 0usize;
    for declaration in declarations {
        match declaration {
            ParsedDeclaration::Const {
                name, ty, internal, ..
            } => {
                language::check_experiment_type(ty, context.language())?;
                let typ = context.resolve_type(ty)?;
                local.push((name.clone(), root, *internal, typ));
                root += 1;
            }
            ParsedDeclaration::Def { .. }
            | ParsedDeclaration::Equation(..)
            | ParsedDeclaration::Output(_, _, Some(_), _)
            | ParsedDeclaration::Aux(_, _, Some(_), _) => root += 1,
            _ => {}
        }
    }
    if local.is_empty() {
        return Ok(());
    }

    // What this module's own bodies may name: its imports, then its own
    // constants, so a local name wins as it does for types.
    let imported = functions::imported_modules(path, declarations)?;
    let mut scope = ConstScope {
        own: path.clone(),
        bare: BTreeMap::new(),
        qualified: BTreeMap::new(),
        local: BTreeSet::new(),
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
            .map(|(name, index, _, typ)| (name.clone(), (*index, typ)))
            .collect(),
        trees: &trees,
        source: parsed.source(),
        context,
        archive,
        label: source,
    };
    let mut active = Vec::new();
    for (name, ..) in &local {
        folder.fold_pending(name, &mut scope, &mut active)?;
    }
    for (name, _, internal, _) in local {
        let value = scope
            .bare
            .get(&name)
            .expect("every local constant was folded")
            .clone();
        table
            .entries
            .insert((path.clone(), name), ConstEntry { internal, value });
    }
    Ok(())
}

/// The pending constants and typing namespace of one module.
struct Folder<'a, 'tree> {
    pending: BTreeMap<VarName, (usize, &'a StreamType)>,
    trees: &'a [ParsedExprRef<'tree>],
    source: Option<SourceId>,
    context: &'a AstShared<SourceContext>,
    archive: Option<&'a SourceArchive>,
    label: &'a str,
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
        let body = self.trees[*index];
        for node in body.postorder() {
            if let Some(construct) = unsupported(node.kind()) {
                return Err(DsrvExpandError::NotConstant {
                    construct,
                    span: parsed::span_of(node),
                });
            }
        }
        for node in body.postorder() {
            let named = match node.kind() {
                ParsedExprKind::Var(name) => Some(name),
                ParsedExprKind::ModuleItem(path) if path.module() == scope.own.as_slice() => {
                    Some(path.name())
                }
                _ => None,
            };
            if let Some(named) = named {
                self.fold_pending(named, scope, active)?;
            }
        }
        let literal = self.fold_checked(name, body, expected, scope)?;
        let value = TypedConstant {
            literal,
            typ: AstShared::new((*expected).clone()),
        };
        active.pop();
        scope.local.insert(name.clone());
        scope.bare.insert(name.clone(), value);
        Ok(())
    }

    /// Resolve, check, elaborate and evaluate a constant as an equation.
    fn fold_checked(
        &self,
        name: &VarName,
        body: ParsedExprRef<'_>,
        expected: &StreamType,
        scope: &ConstScope,
    ) -> Result<SyntaxLiteral, DsrvExpandError> {
        let span = parsed::span_of(body);
        let inline_scope = inline::Scope {
            constants: Some(scope),
            ..Default::default()
        };
        let inlined = inline::standalone(body, &inline_scope, [])?;
        let mut builder = ExprBuilder::with_capacity(inlined.as_ref().subtree_ids().len());
        let root = super::expand_tree(
            inlined.as_ref(),
            &mut builder,
            self.context,
            None,
            self.source,
        )?;
        let expr = builder.finish(root).map_err(DsrvAstError::from)?;
        if let Some(key) = expr.as_ref().duplicate_field() {
            return Err(DsrvAstError::DuplicateExpressionField { field: key.clone() }.into());
        }
        let language = self.context.language();
        for node in expr.as_ref().postorder() {
            let written = node
                .source_context()
                .map_or(language, SourceContext::language);
            language::check_experiment_node(node, written)?;
            language::check_dialect_node(node, language.dialect())?;
        }
        // Any remaining name refers to a stream or is undeclared.
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

        let checked = check_and_elaborate_expression(
            expr,
            expected,
            &AstShared::new(BTreeMap::new()),
            self.archive,
        )
        .discard_warnings()
        .map_err(|errors| DsrvExpandError::ConstantCheck {
            name: name.to_string(),
            location: self.label.to_owned(),
            errors,
            span,
        })?;

        let value = crate::semantics::untimed_dsrv::atemporal::eval_atemporal(
            checked.expr().as_ref(),
            &BTreeMap::new(),
        )
        .map_err(|error| DsrvExpandError::ConstantValue {
            message: error.to_string(),
            span,
        })?;
        literal_of(value, Some(expected))
            .map_err(|message| DsrvExpandError::ConstantValue { message, span })
    }
}

/// Keep initializers within the existing literal/arithmetic subset.
fn unsupported(kind: &ParsedExprKind) -> Option<&'static str> {
    Some(match kind {
        ParsedExprKind::Val(..)
        | ParsedExprKind::Var(..)
        | ParsedExprKind::ModuleItem(..)
        | ParsedExprKind::BinOp(..)
        | ParsedExprKind::Not(..)
        | ParsedExprKind::Neg(..)
        | ParsedExprKind::Cast(..)
        | ParsedExprKind::List(..)
        | ParsedExprKind::Tuple(..)
        | ParsedExprKind::Map(..)
        | ParsedExprKind::Struct(..)
        | ParsedExprKind::ObjectLiteral(..) => return None,
        ParsedExprKind::SIndex(..) => "a stream offset",
        ParsedExprKind::Apply(..) | ParsedExprKind::Partial(..) => "a call",
        ParsedExprKind::Lambda(..) | ParsedExprKind::Fix(..) => "a function",
        ParsedExprKind::Dynamic(..) | ParsedExprKind::Defer(..) => "text supplied at runtime",
        ParsedExprKind::Update(..)
        | ParsedExprKind::Latch(..)
        | ParsedExprKind::Init(..)
        | ParsedExprKind::When(..)
        | ParsedExprKind::IsDefined(..)
        | ParsedExprKind::Default(..) => "a stream operation",
        ParsedExprKind::Match(..) | ParsedExprKind::Matches(..) => "a match",
        ParsedExprKind::Constructor(..) => "a constructor",
        _ => "this expression",
    })
}

/// The literal a checked body yields, retaining its expected container shape.
fn literal_of(value: Value, expected: Option<&StreamType>) -> Result<SyntaxLiteral, String> {
    Ok(match value {
        Value::List(values) => {
            let element = match expected {
                Some(StreamType::List(element)) => Some(element.as_ref()),
                _ => None,
            };
            SyntaxLiteral::List(
                values
                    .into_iter()
                    .map(|value| literal_of(value, element))
                    .collect::<Result<_, _>>()?,
            )
        }
        Value::Tuple(values) => {
            let types = match expected {
                Some(StreamType::Tuple(types)) => Some(types),
                _ => None,
            };
            SyntaxLiteral::Tuple(
                values
                    .into_iter()
                    .enumerate()
                    .map(|(i, value)| literal_of(value, types.and_then(|types| types.get(i))))
                    .collect::<Result<_, _>>()?,
            )
        }
        Value::Map(values) => {
            let fields = values
                .into_iter()
                .map(|(key, value)| {
                    let typ = match expected {
                        Some(StreamType::Map(element)) => Some(element.as_ref()),
                        Some(StreamType::Struct(fields, _)) => fields
                            .iter()
                            .find(|(name, _)| name == &key)
                            .map(|(_, typ)| typ),
                        _ => None,
                    };
                    Ok((key, literal_of(value, typ)?))
                })
                .collect::<Result<_, String>>()?;
            if matches!(expected, Some(StreamType::Struct(..))) {
                SyntaxLiteral::Struct(fields)
            } else {
                SyntaxLiteral::Map(fields)
            }
        }
        other => SyntaxLiteral::try_from(other).map_err(|error| error.to_string())?,
    })
}

/// What a constant's body may name.
struct ConstScope {
    own: ModulePath,
    bare: BTreeMap<VarName, TypedConstant>,
    qualified: BTreeMap<(Vec<ModuleName>, VarName), TypedConstant>,
    /// The names this module declared itself, which have been folded.
    local: BTreeSet<VarName>,
}

/// What a name stands for where it names a constant, as inlining writes
/// one in.
pub(crate) trait ConstantNames {
    /// The constant a bare name stands for.
    fn bare(&self, name: &VarName) -> Option<&TypedConstant>;
    /// The constant `name` stands for in `module`.
    fn through_module(&self, module: &[ModuleName], name: &VarName) -> Option<&TypedConstant>;
    /// Whether no name stands for a constant.
    fn is_empty(&self) -> bool;
}

impl ConstantNames for ConstScope {
    fn bare(&self, name: &VarName) -> Option<&TypedConstant> {
        self.bare.get(name)
    }

    fn through_module(&self, module: &[ModuleName], name: &VarName) -> Option<&TypedConstant> {
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

/// The constants one module may name, in a form that can be stored.
#[derive(Clone, Debug, Default)]
pub(crate) struct Constants {
    table: AstShared<ConstantTable>,
    own: ModulePath,
    imported: BTreeMap<ModulePath, bool>,
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
        })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// What this module may name, written into a node's identity.
    pub(crate) fn describe(&self, out: &mut String) {
        use std::fmt::Write as _;
        if self.is_empty() {
            return;
        }
        for (name, entry) in self.table.declared(&self.own) {
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
    fn bare(&self, name: &VarName) -> Option<&TypedConstant> {
        self.table
            .declared(&self.own)
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

    fn through_module(&self, module: &[ModuleName], name: &VarName) -> Option<&TypedConstant> {
        let reachable = module == self.own.as_slice() || self.imported.contains_key(module);
        if !reachable {
            return None;
        }
        let entries = if module == self.own.as_slice() {
            self.table.declared(&self.own)
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
/// table and standing in the same module is what says.
impl PartialEq for Constants {
    fn eq(&self, other: &Self) -> bool {
        if self.is_empty() && other.is_empty() {
            return true;
        }
        AstShared::ptr_eq(&self.table, &other.table)
            && self.own == other.own
            && self.imported == other.imported
    }
}
