//! Every `const` of a program, folded to the literal it stands for.
//!
//! A constant is not a def of no arguments. A def's body travels as a tree
//! and is copied into each caller (S18); a constant's body is **evaluated
//! once**, and what it yields is a value. The difference is forced by where
//! a constant has to be usable: a stream offset `x[n]` holds a number, not
//! a subtree, so nothing that only copies trees could put a constant there.
//!
//! Folding borrows the runtime's own arithmetic rather than repeating it,
//! so a constant and the same expression written out agree by construction,
//! and `1 / 0` is refused while the program is still being read.
//!
//! Entries are built in the dependency order the namespaces are, since a
//! constant follows the same `use` edges as a type or a def.

use std::collections::BTreeMap;

use contiguous_tree::TreeCursor;

use crate::VarName;
use crate::core::values::operations;
use crate::lang::dsrv::ast::{AstShared, SyntaxLiteral};
use crate::lang::dsrv::modules::{ModulePath, ModuleSources};
use crate::lang::dsrv::path::ModuleName;
use crate::lang::dsrv::source::SourceContext;
use crate::lang::dsrv::span::Span;
use crate::lang::dsrv::syntax::parsed::{self, ParsedExprKind, ParsedExprRef};
use crate::lang::dsrv::syntax::{ParsedDeclaration, ParsedSpecification};
use crate::lang::dsrv::type_checker::check_value_stream_type;

use super::graph::ModuleGraph;
use super::{DsrvExpandError, functions, graph};

/// One constant, already folded.
#[derive(Debug)]
pub(crate) struct ConstEntry {
    /// Hidden from importers, usable inside its own module (S12).
    pub(crate) internal: bool,
    pub(crate) value: SyntaxLiteral,
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
    /// that module's own text may name (S12).
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
        add_module(&mut table, &path, parsed, context, &source)?;
    }
    Ok(table)
}

/// The constants of a program that is one file, which is every program
/// until it takes on modules. The file is its own root module.
pub(crate) fn single_file_constants(
    parsed: &ParsedSpecification,
    context: &SourceContext,
) -> Result<ConstantTable, DsrvExpandError> {
    let mut table = ConstantTable::default();
    add_module(&mut table, &ModulePath::new(), parsed, context, "<string>")?;
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
    context: &SourceContext,
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
                name,
                ty,
                internal,
                span,
                ..
            } => {
                let typ = context.resolve_type(ty).map_err(|error| {
                    let _ = span;
                    error
                })?;
                local.push((name.clone(), root, *internal, typ, *span));
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
    // constants, so a local name wins as it does for types (S11).
    let imported = functions::imported_modules(path, declarations)?;
    let mut scope = ConstScope::default();
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
    let pending: BTreeMap<VarName, usize> = local
        .iter()
        .map(|(name, index, ..)| (name.clone(), *index))
        .collect();
    let mut active = Vec::new();
    for (name, ..) in &local {
        fold_pending(name, &pending, &trees, &mut scope, &mut active)?;
    }
    for (name, _, internal, expected, span) in local {
        let value = scope
            .bare
            .get(&name)
            .expect("every local constant was folded")
            .clone();
        check_value_stream_type(&expected, &value.clone().into_runtime_value()).map_err(
            |message| DsrvExpandError::ConstantType {
                name: name.to_string(),
                location: source.to_owned(),
                expected,
                message,
                span,
            },
        )?;
        table
            .entries
            .insert((path.clone(), name), ConstEntry { internal, value });
    }
    Ok(())
}

/// Fold one of this module's own constants, and whatever it names, first.
fn fold_pending(
    name: &VarName,
    pending: &BTreeMap<VarName, usize>,
    trees: &[ParsedExprRef<'_>],
    scope: &mut ConstScope,
    active: &mut Vec<VarName>,
) -> Result<(), DsrvExpandError> {
    if scope.local.contains(name) {
        return Ok(());
    }
    let Some(index) = pending.get(name) else {
        return Ok(());
    };
    if active.contains(name) {
        return Err(DsrvExpandError::RecursiveConstant {
            name: name.name().to_string(),
        });
    }
    active.push(name.clone());
    // Whatever this body names must be folded before it is.
    let body = trees[*index];
    for named in named_in(body) {
        fold_pending(&named, pending, trees, scope, active)?;
    }
    let value = fold(body, scope)?;
    active.pop();
    scope.local.insert(name.clone());
    scope.bare.insert(name.clone(), value);
    Ok(())
}

/// Every bare name a body mentions, which is what it might be built from.
fn named_in(cursor: ParsedExprRef<'_>) -> Vec<VarName> {
    let mut found = Vec::new();
    if let ParsedExprKind::Var(name) = cursor.kind() {
        found.push(name.clone());
    }
    for child in cursor.child_ids() {
        found.extend(named_in(cursor.child(child)));
    }
    found
}

/// What a constant's body may name.
#[derive(Default)]
struct ConstScope {
    bare: BTreeMap<VarName, SyntaxLiteral>,
    qualified: BTreeMap<(Vec<ModuleName>, VarName), SyntaxLiteral>,
    /// The names this module declared itself, which have been folded.
    local: std::collections::BTreeSet<VarName>,
}

/// Fold one body to the literal it stands for, refusing anything that is
/// not settled where the file is read.
fn fold(cursor: ParsedExprRef<'_>, scope: &ConstScope) -> Result<SyntaxLiteral, DsrvExpandError> {
    let span = parsed::span_of(cursor);
    let refuse = |construct: &'static str| DsrvExpandError::NotConstant { construct, span };
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
        ParsedExprKind::SIndex(..) => return Err(refuse("a stream offset")),
        ParsedExprKind::Apply(..) | ParsedExprKind::Partial(..) => {
            return Err(refuse("a call"));
        }
        ParsedExprKind::Lambda(..) | ParsedExprKind::Fix(..) => {
            return Err(refuse("a function"));
        }
        ParsedExprKind::Dynamic(..) | ParsedExprKind::Defer(..) => {
            return Err(refuse("text supplied at runtime"));
        }
        ParsedExprKind::Update(..)
        | ParsedExprKind::Latch(..)
        | ParsedExprKind::Init(..)
        | ParsedExprKind::When(..)
        | ParsedExprKind::IsDefined(..)
        | ParsedExprKind::Default(..) => return Err(refuse("a stream operation")),
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

    /// What a name in this module stands for, bare or through a module.
    pub(crate) fn bare(&self, name: &VarName) -> Option<&SyntaxLiteral> {
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

    pub(crate) fn through_module(
        &self,
        module: &[ModuleName],
        name: &VarName,
    ) -> Option<&SyntaxLiteral> {
        let reachable = module == self.own.as_slice() || self.imported.contains_key(module);
        if !reachable {
            return None;
        }
        let entries = if module == self.own.as_slice() {
            self.table.declared(module)
        } else {
            self.table.exported(module)
        };
        entries
            .into_iter()
            .find(|(declared, _)| *declared == name)
            .map(|(_, entry)| &entry.value)
    }

    /// What this module may name, written into a node's identity (S20).
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
    }
}
