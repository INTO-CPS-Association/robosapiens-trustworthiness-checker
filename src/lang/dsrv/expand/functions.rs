//! Every `def` of a program, with its body already inlined.
//!
//! A def's body is an expression, and an expression cannot be erased into a
//! structural form the way a type can (S16). So a body travels as a **tree
//! of its own**: built once per module, fully inlined against what its
//! imports gave it, and copied into each caller from there.
//!
//! Entries are built in the same dependency order the namespaces are, since
//! function imports follow the same `use` edges as type imports.
//!
//! A body keeps the lexical environment of the module that wrote it: that
//! module's namespace and settings, the constants it may name and the
//! modules it imported. The table holds one [`Lexical`] per module, and a
//! node copied out of a body names its environment by [`LexicalId`], so
//! the syntax it uses is judged by its own module's header and the names
//! it resolves are that module's, wherever it is called from.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use contiguous_tree::TreeCursorExt;

use ecow::EcoVec;

use crate::VarName;
use crate::lang::dsrv::ast::AstShared;
use crate::lang::dsrv::modules::{ImportItems, ModulePath, ModuleSources, imports_of, show_path};
use crate::lang::dsrv::source::{SourceContext, SourceType, TypeName};
use crate::lang::dsrv::source_map::{ArchiveToken, SourceId};
use crate::lang::dsrv::syntax::parsed::{ParsedExpr, ParsedExprRef, origin_of};
use crate::lang::dsrv::syntax::{ParsedDeclaration, ParsedSpecification};

use super::constants::{ConstantTable, Constants};
use super::graph::ModuleGraph;
use super::inline::{Def, Scope, standalone};
use super::language::Dialect;
use super::{DsrvExpandError, graph};

/// One module's lexical environment, by its place in a function table.
///
/// An ID means something only in the table that issued it, as a
/// [`SourceId`] does in its archive, so it is never written into an
/// identity: the module it names is.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct LexicalId(u32);

impl LexicalId {
    fn from_index(index: usize) -> Self {
        Self(u32::try_from(index).expect("a program has fewer than u32::MAX modules"))
    }

    fn index(self) -> usize {
        self.0 as usize
    }
}

/// What one module's text is read in: its namespace and settings, the
/// constants it may name, and which modules it imported.
#[derive(Debug)]
pub(crate) struct Lexical {
    module: ModulePath,
    /// The module's own namespace and settings, with the dialect of the
    /// program it is part of: which runtimes may ensure_runtime_support code is the
    /// program's to decide, wherever that code was written.
    context: AstShared<SourceContext>,
    /// What a name in this module stands for, folded once (19b).
    constants: Constants,
    /// Every module this one imported, and whether the import brought its
    /// defs in bare.
    imported: BTreeMap<ModulePath, bool>,
}

/// One function, with a body that stands on its own.
#[derive(Debug)]
pub(crate) struct Entry {
    pub(crate) parameters: EcoVec<(VarName, SourceType)>,
    pub(crate) type_parameters: EcoVec<TypeName>,
    pub(crate) result: SourceType,
    /// Hidden from importers, usable inside its own module (S12).
    pub(crate) internal: bool,
    /// The body, inlined, in a tree this entry owns.
    pub(crate) body: ParsedExpr,
    /// The archived file that declared it, by ID: an entry holds no file.
    pub(crate) source: Option<SourceId>,
    /// The environment of the module that declared it.
    pub(crate) lexical: LexicalId,
}

/// Every def of a program, by the module that declared it, and every
/// module's lexical environment.
#[derive(Debug, Default)]
pub(crate) struct FunctionTable {
    entries: BTreeMap<(ModulePath, VarName), Entry>,
    /// The archive whose IDs the entries carry.
    archive: Option<ArchiveToken>,
    /// By [`LexicalId`].
    lexicals: Vec<Lexical>,
}

impl FunctionTable {
    fn lexical(&self, id: LexicalId) -> Option<&Lexical> {
        self.lexicals.get(id.index())
    }

    /// The environment of `module`, which every module added to the table
    /// has.
    fn lexical_of(&self, module: &[ModuleName]) -> Option<LexicalId> {
        self.lexicals
            .iter()
            .position(|lexical| lexical.module.as_slice() == module)
            .map(LexicalId::from_index)
    }

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

/// Build every module's defs, importers after what they import, each in
/// the namespace `graph` gives its module and admitted as `dialect`, the
/// program's.
pub(crate) fn build_function_table(
    sources: &ModuleSources,
    graph: &ModuleGraph,
    constants: &AstShared<ConstantTable>,
    dialect: Dialect,
) -> Result<FunctionTable, DsrvExpandError> {
    let mut table = FunctionTable {
        archive: Some(sources.archive().token()),
        ..FunctionTable::default()
    };
    for path in graph::dependency_order(sources)? {
        let parsed = sources
            .get(&path)
            .expect("every path in the order was collected");
        let context = graph
            .get(&path)
            .expect("every path in the order has a namespace");
        let context = if context.language().dialect() == dialect {
            AstShared::clone(context)
        } else {
            AstShared::new(context.admitted_as(dialect))
        };
        add_module(&mut table, &path, parsed, context, constants)?;
    }
    Ok(table)
}

/// The defs of a program that is one file, which is every program until it
/// takes on modules. The file is its own root module.
pub(crate) fn single_file_table(
    parsed: &ParsedSpecification,
    archive: Option<ArchiveToken>,
    context: AstShared<SourceContext>,
    constants: &AstShared<ConstantTable>,
) -> Result<FunctionTable, DsrvExpandError> {
    let mut table = FunctionTable {
        archive,
        ..FunctionTable::default()
    };
    add_module(&mut table, &ModulePath::new(), parsed, context, constants)?;
    Ok(table)
}

/// Add one module's environment and defs to `table`, inlining each body
/// against what the module imported and against the module's own defs and
/// constants.
///
/// Every module this one imports must already be in `table`, which is what
/// the dependency order gives.
fn add_module(
    table: &mut FunctionTable,
    path: &ModulePath,
    parsed: &ParsedSpecification,
    context: AstShared<SourceContext>,
    constants: &AstShared<ConstantTable>,
) -> Result<(), DsrvExpandError> {
    let declarations = parsed.declarations();
    let held = parsed.roots();
    let trees: Vec<ParsedExprRef<'_>> = held.iter().map(|tree| tree.as_ref()).collect();
    let lexical = Lexical {
        module: path.clone(),
        context,
        constants: Constants::new(AstShared::clone(constants), path, declarations)?,
        imported: imported_modules(path, declarations)?,
    };
    let id = LexicalId::from_index(table.lexicals.len());

    // What this module's own bodies may call: its imports, then its own
    // defs, so a local name wins as it does for types (S11). A constant
    // they name is this module's, folded here rather than at a caller that
    // may name something else by it.
    let mut scope = scope_of(&lexical.imported, table);
    scope.constants = Some(&lexical.constants);
    let mut local: Vec<(
        VarName,
        usize,
        EcoVec<(VarName, SourceType)>,
        EcoVec<TypeName>,
        SourceType,
        bool,
    )> = Vec::new();
    let mut root = 0usize;
    for declaration in declarations {
        match declaration {
            ParsedDeclaration::Def {
                name,
                type_parameters,
                parameters,
                result,
                internal,
                ..
            } => {
                local.push((
                    name.clone(),
                    root,
                    parameters.clone(),
                    type_parameters.clone(),
                    result.clone(),
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
    for (name, index, parameters, type_parameters, result, _) in &local {
        scope.bare.insert(
            name.clone(),
            Def {
                parameters: parameters.clone(),
                type_parameters: type_parameters.clone(),
                result: result.clone(),
                body: trees[*index],
                foreign: false,
                source: parsed.source(),
                lexical: None,
            },
        );
    }
    let mut built = Vec::with_capacity(local.len());
    for (name, index, parameters, type_parameters, result, internal) in local {
        let body = standalone(
            trees[index],
            &scope,
            parameters.iter().map(|(name, _)| name.clone()),
        )?;
        built.push((
            (path.clone(), name),
            Entry {
                parameters,
                type_parameters,
                result,
                internal,
                body,
                source: parsed.source(),
                lexical: id,
            },
        ));
    }
    drop(scope);
    table.entries.extend(built);
    table.lexicals.push(lexical);
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
    /// The environment of the module whose text this is. Its own defs are
    /// callable bare, the ones it keeps internal included (S12).
    own: LexicalId,
}

impl Callable {
    /// What text written in `module` may call.
    ///
    /// # Panics
    ///
    /// If `module` was never added to `table`.
    pub(crate) fn new(table: AstShared<FunctionTable>, module: &ModulePath) -> Self {
        let own = table
            .lexical_of(module)
            .unwrap_or_else(|| panic!("{} is in the table", show_path(module)));
        Self { table, own }
    }

    fn lexical(&self) -> Option<&Lexical> {
        self.table.lexical(self.own)
    }

    /// Whether any def is callable at all, which is what says a node needs
    /// to carry this on into the text it is given.
    pub(crate) fn is_empty(&self) -> bool {
        self.table.entries.is_empty()
            && self
                .lexical()
                .is_none_or(|lexical| lexical.constants.is_empty())
    }

    /// The environment `id` names in this callable's table: the namespace
    /// code written there is expanded in, and what text supplied to it may
    /// call, which is `None` where it may call nothing.
    ///
    /// # Panics
    ///
    /// If `id` was issued by another table.
    pub(crate) fn environment(
        &self,
        id: LexicalId,
    ) -> (AstShared<SourceContext>, Option<AstShared<Callable>>) {
        let lexical = self
            .table
            .lexical(id)
            .expect("a lexical ID belongs to the table of the callable that reached it");
        let callable = Self {
            table: AstShared::clone(&self.table),
            own: id,
        };
        let callable = (!callable.is_empty()).then(|| AstShared::new(callable));
        (AstShared::clone(&lexical.context), callable)
    }

    /// Whether `id` names this callable's own environment.
    pub(crate) fn is_own(&self, id: LexicalId) -> bool {
        self.own == id
    }

    fn at(&self, own: LexicalId) -> Self {
        Self {
            table: AstShared::clone(&self.table),
            own,
        }
    }

    /// Environments whose callables may be needed by text accepted through
    /// this callable. A runtime-expression occurrence in an inlined body
    /// retains the environment where that occurrence was written.
    fn runtime_environment_closure(&self) -> BTreeSet<LexicalId> {
        let mut pending = vec![self.own];
        let mut reached = BTreeSet::new();
        while let Some(id) = pending.pop() {
            if !reached.insert(id) {
                continue;
            }
            let callable = self.at(id);
            let scope = callable.scope();
            for def in scope.bare.values().chain(scope.qualified.values()) {
                let defining = def.lexical.unwrap_or(id);
                for node in def.body.postorder() {
                    if matches!(
                        node.kind(),
                        crate::lang::dsrv::syntax::parsed::ParsedExprKind::Dynamic(..)
                            | crate::lang::dsrv::syntax::parsed::ParsedExprKind::Defer(..)
                    ) {
                        pending.push(origin_of(node).lexical.unwrap_or(defining));
                    }
                }
            }
        }
        reached
    }

    /// What the text given to a node could call, written into the node's
    /// identity. Two nodes offering different defs are not the same node,
    /// however much else about them agrees, and neither are defs whose
    /// bodies were written under different settings or namespaces.
    pub(crate) fn describe(&self, out: &mut String) {
        if self.is_empty() {
            return;
        }
        let scope = self.scope();
        if let Some(lexical) = self.lexical() {
            lexical.constants.describe(out);
        }
        let mut reached = BTreeSet::new();
        for (name, def) in &scope.bare {
            let _ = write!(out, "{}", name.name());
            self.describe_def(out, def, &mut reached);
        }
        for ((module, name), def) in &scope.qualified {
            let _ = write!(out, "{}::{}", show_path(module), name.name());
            self.describe_def(out, def, &mut reached);
        }
        // Runtime expression source accepted by a reachable definition may itself contain
        // dynamic/defer. Its lexical callable is semantic input even though
        // those helper definitions are not statically inlined here.
        let mut runtime_environments = self
            .runtime_environment_closure()
            .into_iter()
            .filter(|id| *id != self.own)
            .collect::<Vec<_>>();
        runtime_environments.sort_by_key(|id| {
            self.table
                .lexical(*id)
                .map(|lexical| lexical.module.clone())
                .unwrap_or_default()
        });
        for id in runtime_environments {
            let callable = self.at(id);
            let Some(lexical) = callable.lexical() else {
                continue;
            };
            let _ = write!(out, "runtime-env {};", show_path(&lexical.module));
            lexical.constants.describe(out);
            let scope = callable.scope();
            for (name, def) in &scope.bare {
                let _ = write!(out, "{}", name.name());
                self.describe_def(out, def, &mut reached);
            }
            for ((module, name), def) in &scope.qualified {
                let _ = write!(out, "{}::{}", show_path(module), name.name());
                self.describe_def(out, def, &mut reached);
            }
        }
        // Modules are named rather than numbered, so the description does
        // not depend on the order the table happened to add them in.
        let mut environments = reached
            .into_iter()
            .filter_map(|id| self.table.lexical(id))
            .collect::<Vec<_>>();
        environments.sort_by(|left, right| left.module.cmp(&right.module));
        for lexical in environments {
            let _ = write!(
                out,
                "env {}={};",
                show_path(&lexical.module),
                serde_json::to_string(lexical.context.fingerprint())
                    .expect("source fingerprints are serializable")
            );
        }
    }

    /// One def of the description: what calling it substitutes, and the
    /// module whose environment each part of it was written in.
    fn describe_def(&self, out: &mut String, def: &Def<'_>, reached: &mut BTreeSet<LexicalId>) {
        describe_def(out, def);
        let module = |id: LexicalId| {
            self.table
                .lexical(id)
                .map_or_else(String::new, |lexical| show_path(&lexical.module))
        };
        if let Some(id) = def.lexical {
            reached.insert(id);
            let _ = write!(out, "@{};", module(id));
        }
        for (index, node) in def.body.postorder().enumerate() {
            if let Some(id) = origin_of(node).lexical {
                reached.insert(id);
                let _ = write!(out, "{index}@{};", module(id));
            }
        }
    }

    /// The archive whose IDs this callable's defs carry, if any.
    pub(crate) fn archive(&self) -> Option<ArchiveToken> {
        self.table.archive
    }

    /// Every archived file the defs this callable reaches were written in.
    pub(crate) fn source_closure(&self) -> BTreeSet<SourceId> {
        let mut sources = BTreeSet::new();
        for id in self.runtime_environment_closure() {
            let callable = self.at(id);
            let scope = callable.scope();
            for def in scope.bare.values().chain(scope.qualified.values()) {
                sources.extend(def.source);
                sources.extend(
                    def.body
                        .postorder()
                        .filter_map(|node| origin_of(node).definition)
                        .map(|site| site.source),
                );
            }
        }
        sources
    }

    /// Borrow the table into a scope for one expansion.
    pub(crate) fn scope(&self) -> Scope<'_> {
        let Some(lexical) = self.lexical() else {
            return Scope::default();
        };
        let mut scope = scope_of(&lexical.imported, &self.table);
        scope.constants = Some(&lexical.constants);
        // A file's own defs win over the ones it imported, as types do
        // (S11), and a file may call the ones it keeps to itself.
        for (name, entry) in self.table.declared(&lexical.module) {
            scope.bare.insert(name.clone(), entry.as_def());
            scope
                .qualified
                .insert((lexical.module.clone(), name.clone()), entry.as_def());
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
        AstShared::ptr_eq(&self.table, &other.table) && self.own == other.own
    }
}

impl Entry {
    fn as_def(&self) -> Def<'_> {
        Def {
            parameters: self.parameters.clone(),
            type_parameters: self.type_parameters.clone(),
            result: self.result.clone(),
            body: self.body.as_ref(),
            foreign: true,
            source: self.source,
            lexical: Some(self.lexical),
        }
    }
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
    let _ = write!(out, "({parameters})->{:?}={:?};", def.result, def.body);
}
