//! Every `def` of a program, with its body already inlined.
//!
//! A def's body is an expression, and an expression cannot be erased into a
//! structural form the way a type can (S16). So a body travels as a **tree
//! of its own**: built once per module, fully inlined against what its
//! imports gave it, and copied into each caller from there.
//!
//! Entries are built in the same dependency order the namespaces are, since
//! function imports follow the same `use` edges as type imports.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use ecow::EcoVec;

use crate::VarName;
use crate::lang::dsrv::ast::AstShared;
use crate::lang::dsrv::modules::{ImportItems, ModulePath, ModuleSources, imports_of, show_path};
use crate::lang::dsrv::source::{SourceType, TypeName};
use crate::lang::dsrv::syntax::parsed::{ParsedExpr, ParsedExprRef};
use crate::lang::dsrv::syntax::{ParsedDeclaration, ParsedSpecification};

use super::constants::{ConstantTable, Constants};
use super::inline::{Def, Scope, standalone};
use super::{DsrvExpandError, graph};

/// One function, with a body that stands on its own.
#[derive(Debug)]
pub(crate) struct Entry {
    pub(crate) parameters: EcoVec<(VarName, SourceType)>,
    pub(crate) type_parameters: EcoVec<TypeName>,
    /// Hidden from importers, usable inside its own module (S12).
    pub(crate) internal: bool,
    /// The body, inlined, in a tree this entry owns.
    pub(crate) body: ParsedExpr,
}

/// Every def of a program, by the module that declared it.
#[derive(Debug, Default)]
pub(crate) struct FunctionTable {
    entries: BTreeMap<(ModulePath, VarName), Entry>,
}

impl FunctionTable {
    /// Every def a module exports, which is all but its internal ones.
    fn exported(&self, module: &[ModuleName]) -> Vec<(&VarName, &Entry)> {
        self.declared(module)
            .into_iter()
            .filter(|(_, entry)| !entry.internal)
            .collect()
    }

    /// Every def a module declared, the internal ones included: what that
    /// module's own text may call (S12).
    fn declared(&self, module: &[ModuleName]) -> Vec<(&VarName, &Entry)> {
        self.entries
            .iter()
            .filter(|((path, _), _)| path.as_slice() == module)
            .map(|((_, name), entry)| (name, entry))
            .collect()
    }
}

use crate::lang::dsrv::path::ModuleName;

/// Build every module's defs, importers after what they import.
pub(crate) fn build_function_table(
    sources: &ModuleSources,
) -> Result<FunctionTable, DsrvExpandError> {
    let mut table = FunctionTable::default();
    for path in graph::dependency_order(sources)? {
        let parsed = sources
            .get(&path)
            .expect("every path in the order was collected");
        add_module(&mut table, &path, parsed)?;
    }
    Ok(table)
}

/// The defs of a program that is one file, which is every program until it
/// takes on modules. The file is its own root module.
pub(crate) fn single_file_table(
    parsed: &ParsedSpecification,
) -> Result<FunctionTable, DsrvExpandError> {
    let mut table = FunctionTable::default();
    add_module(&mut table, &ModulePath::new(), parsed)?;
    Ok(table)
}

/// Add one module's defs to `table`, inlining each body against what the
/// module imported and against the module's own defs.
///
/// Every module this one imports must already be in `table`, which is what
/// the dependency order gives.
fn add_module(
    table: &mut FunctionTable,
    path: &ModulePath,
    parsed: &ParsedSpecification,
) -> Result<(), DsrvExpandError> {
    let declarations = parsed.declarations();
    let held = parsed.roots();
    let trees: Vec<ParsedExprRef<'_>> = held.iter().map(|tree| tree.as_ref()).collect();

    // What this module's own bodies may call: its imports, then its own
    // defs, so a local name wins as it does for types (S11).
    let mut scope = imported_scope(path, declarations, table)?;
    let mut local: Vec<(
        VarName,
        usize,
        EcoVec<(VarName, SourceType)>,
        EcoVec<TypeName>,
        bool,
    )> = Vec::new();
    let mut root = 0usize;
    for declaration in declarations {
        match declaration {
            ParsedDeclaration::Def {
                name,
                type_parameters,
                parameters,
                internal,
                ..
            } => {
                local.push((
                    name.clone(),
                    root,
                    parameters.clone(),
                    type_parameters.clone(),
                    *internal,
                ));
                root += 1;
            }
            ParsedDeclaration::Equation(..)
            | ParsedDeclaration::Const { .. }
            | ParsedDeclaration::Output(_, _, Some(_), _)
            | ParsedDeclaration::Aux(_, _, Some(_), _) => root += 1,
            _ => {}
        }
    }
    for (name, index, parameters, type_parameters, _) in &local {
        scope.bare.insert(
            name.clone(),
            Def {
                parameters: parameters.clone(),
                type_parameters: type_parameters.clone(),
                body: trees[*index],
                foreign: false,
            },
        );
    }
    let mut built = Vec::with_capacity(local.len());
    for (name, index, parameters, type_parameters, internal) in local {
        let body = standalone(trees[index], &scope)?;
        built.push((
            (path.clone(), name),
            Entry {
                parameters,
                type_parameters,
                internal,
                body,
            },
        ));
    }
    drop(scope);
    table.entries.extend(built);
    Ok(())
}

/// The functions one module may call, in a form that can be stored.
///
/// A `Scope` borrows the table, so it cannot live in node metadata. This
/// owns the table instead and builds a scope on demand, which is what lets
/// `dynamic` and `defer` text call a def.
#[derive(Clone, Debug, Default)]
pub(crate) struct Callable {
    table: AstShared<FunctionTable>,
    /// What a name in this module stands for, folded once (19b).
    constants: Constants,
    /// The module whose text this is. Its own defs are callable bare, the
    /// ones it keeps internal included (S12).
    own: ModulePath,
    /// Every module this one imported, and whether the import brought its
    /// defs in bare.
    imported: BTreeMap<ModulePath, bool>,
}

impl Callable {
    pub(crate) fn new(
        table: AstShared<FunctionTable>,
        constants: AstShared<ConstantTable>,
        path: &ModulePath,
        declarations: &[ParsedDeclaration],
    ) -> Result<Self, DsrvExpandError> {
        Ok(Self {
            table,
            constants: Constants::new(constants, path, declarations)?,
            own: path.clone(),
            imported: imported_modules(path, declarations)?,
        })
    }

    /// Whether any def is callable at all, which is what says a node needs
    /// to carry this on into the text it is given.
    pub(crate) fn is_empty(&self) -> bool {
        self.table.entries.is_empty() && self.constants.is_empty()
    }

    /// What the text given to a node could call, written into the node's
    /// identity. Two nodes offering different defs are not the same node,
    /// however much else about them agrees.
    pub(crate) fn describe(&self, out: &mut String) {
        if self.is_empty() {
            return;
        }
        let scope = self.scope();
        self.constants.describe(out);
        for (name, def) in &scope.bare {
            let _ = write!(out, "{}", name.name());
            describe_def(out, def);
        }
        for ((module, name), def) in &scope.qualified {
            let _ = write!(out, "{}::{}", show_path(module), name.name());
            describe_def(out, def);
        }
    }

    /// Borrow the table into a scope for one expansion.
    pub(crate) fn scope(&self) -> Scope<'_> {
        let mut scope = scope_of(&self.imported, &self.table);
        scope.constants = Some(&self.constants);
        // A file's own defs win over the ones it imported, as types do
        // (S11), and a file may call the ones it keeps to itself.
        for (name, entry) in self.table.declared(&self.own) {
            scope.bare.insert(name.clone(), entry.as_def());
            scope
                .qualified
                .insert((self.own.clone(), name.clone()), entry.as_def());
        }
        scope
    }
}

/// Two callables are the same where they offer the same defs, which
/// sharing a table and standing in the same module is what says. A def's
/// body is a tree, which has no equality of its own, so two tables built
/// separately are taken to differ even where they hold the same defs.
impl PartialEq for Callable {
    fn eq(&self, other: &Self) -> bool {
        // Offering nothing is offering nothing, whichever table said so.
        if self.is_empty() && other.is_empty() {
            return true;
        }
        AstShared::ptr_eq(&self.table, &other.table)
            && self.constants == other.constants
            && self.own == other.own
            && self.imported == other.imported
    }
}

impl Entry {
    fn as_def(&self) -> Def<'_> {
        Def {
            parameters: self.parameters.clone(),
            type_parameters: self.type_parameters.clone(),
            body: self.body.as_ref(),
            foreign: true,
        }
    }
}

/// The defs a module's `use` lines bring into scope.
fn imported_scope<'a>(
    path: &ModulePath,
    declarations: &[ParsedDeclaration],
    table: &'a FunctionTable,
) -> Result<Scope<'a>, DsrvExpandError> {
    Ok(scope_of(&imported_modules(path, declarations)?, table))
}

/// Which modules a file's `use` lines name, and which of them bring their
/// defs in bare. A glob is the only form that does: a named import names a
/// type, and a qualified call reaches the rest.
pub(super) fn imported_modules(
    path: &ModulePath,
    declarations: &[ParsedDeclaration],
) -> Result<BTreeMap<ModulePath, bool>, DsrvExpandError> {
    let mut imported: BTreeMap<ModulePath, bool> = BTreeMap::new();
    for declaration in declarations {
        let ParsedDeclaration::Use { tree, .. } = declaration else {
            continue;
        };
        if tree.is_experimental() {
            continue;
        }
        for import in imports_of(tree, path)? {
            let bare = matches!(import.items, ImportItems::All);
            // One module named twice brings its defs in bare if any of the
            // lines naming it was a glob.
            *imported.entry(import.module).or_default() |= bare;
        }
    }
    Ok(imported)
}

/// The defs those modules export, as a scope borrowing `table`.
fn scope_of<'a>(imported: &BTreeMap<ModulePath, bool>, table: &'a FunctionTable) -> Scope<'a> {
    let mut scope = Scope::default();
    for (module, bare) in imported {
        for (name, entry) in table.exported(module) {
            if *bare {
                scope.bare.insert(name.clone(), entry.as_def());
            }
            scope
                .qualified
                .insert((module.clone(), name.clone()), entry.as_def());
        }
    }
    scope
}

/// One def of a `Callable`'s description: what calling it substitutes.
fn describe_def(out: &mut String, def: &Def<'_>) {
    if !def.type_parameters.is_empty() {
        let names = def
            .type_parameters
            .iter()
            .map(|name| name.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let _ = write!(out, "<{names}>");
    }
    let parameters = def
        .parameters
        .iter()
        .map(|(name, ty)| format!("{}:{ty:?}", name.name()))
        .collect::<Vec<_>>()
        .join(",");
    let _ = write!(out, "({parameters})={:?};", def.body);
}
