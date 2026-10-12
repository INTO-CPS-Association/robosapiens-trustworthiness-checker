//! Every `def` of a program, with its body already inlined.
//!
//! A def's body is an expression, and an expression cannot be erased into a
//! structural form the way a type can (S16). So a body travels as a **tree
//! of its own**: built once per module, fully inlined against what its
//! imports gave it, and copied into each caller from there.
//!
//! Entries are built in the same dependency order the namespaces are, since
//! function imports follow the same `use` edges as type imports. A module's
//! constants are folded in the same pass ([`build_tables`]): after the defs
//! of every module it imports are in the table, since a constant may call
//! one, and before its own defs are added, since a def's body has the
//! constants it names written in.
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
use crate::lang::dsrv::source_map::{ArchiveToken, SourceArchive, SourceId};
use crate::lang::dsrv::syntax::parsed::{ParsedExpr, ParsedExprRef, origin_of};
use crate::lang::dsrv::syntax::{ParsedDeclaration, ParsedSpecification};

use super::constants::{ConstantTable, Constants};
use super::graph::ModuleGraph;
use super::inline::{Def, Scope, standalone};
use super::language::Dialect;
use super::stream_functions::{self, StreamFunctionEntry};
use super::{DsrvExpandError, graph};
use crate::lang::dsrv::path::{TypePath, ValuePath};

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
    /// The same constants as text supplied at run time sees them.
    runtime_constants: Constants,
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
    /// Defs whose calls instantiate private stream equations instead of
    /// inlining a pure expression, keyed in their defining module.
    stream_functions: BTreeMap<(ModulePath, TypeName), StreamFunctionEntry>,
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
    /// module's own source text may call (S12). Text supplied at run time is
    /// not the module's own and sees only [`Self::exported`].
    fn declared(&self, module: &[ModuleName]) -> Vec<(&VarName, &Entry)> {
        self.entries
            .iter()
            .filter(|((path, _), _)| path.as_slice() == module)
            .map(|((_, name), entry)| (name, entry))
            .collect()
    }
}

use crate::lang::dsrv::path::ModuleName;

/// Build every module's constants and defs, importers after what they
/// import, each in the namespace `graph` gives its module and admitted as
/// `dialect`, the program's.
///
/// A module's constants are folded once the defs of every module it imports
/// are in the table, and before its own defs are added, since a def's body
/// has the constants it names written in.
pub(crate) fn build_tables(
    sources: &ModuleSources,
    graph: &ModuleGraph,
    dialect: Dialect,
) -> Result<AstShared<FunctionTable>, DsrvExpandError> {
    let mut table = AstShared::new(FunctionTable {
        archive: Some(sources.archive().token()),
        ..FunctionTable::default()
    });
    let mut constants = ConstantTable::default();
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
        let label = super::constants::label_of(sources, &path);
        add_module(
            &mut table,
            &mut constants,
            &path,
            parsed,
            context,
            Some(sources.archive()),
            &label,
        )?;
    }
    Ok(table)
}

/// The constants and defs of a program that is one file, which is every
/// program until it takes on modules. The file is its own root module, and
/// `archive` the archive it is in, if it is located.
pub(crate) fn single_file_table(
    parsed: &ParsedSpecification,
    archive: Option<&SourceArchive>,
    context: AstShared<SourceContext>,
) -> Result<AstShared<FunctionTable>, DsrvExpandError> {
    let mut table = AstShared::new(FunctionTable {
        archive: archive.map(SourceArchive::token),
        ..FunctionTable::default()
    });
    add_module(
        &mut table,
        &mut ConstantTable::default(),
        &ModulePath::new(),
        parsed,
        context,
        archive,
        "<string>",
    )?;
    Ok(table)
}

/// The table, to add to. Folding a constant shares the table with the
/// trees it expands and checks, and none of them outlives the fold, so no
/// other reference to the table remains between modules.
fn unshared(table: &mut AstShared<FunctionTable>) -> &mut FunctionTable {
    AstShared::get_mut(table).expect("no view of the function table outlives a module's build")
}

/// Add one module's environment, constants and defs to the tables, folding
/// each constant and inlining each body against what the module imported
/// and against the module's own defs and constants.
///
/// Every module this one imports must already be in both tables, which is
/// what the dependency order gives.
fn add_module(
    table: &mut AstShared<FunctionTable>,
    constants: &mut ConstantTable,
    path: &ModulePath,
    parsed: &ParsedSpecification,
    context: AstShared<SourceContext>,
    archive: Option<&SourceArchive>,
    label: &str,
) -> Result<(), DsrvExpandError> {
    let imported = imported_modules(path, parsed.declarations())?;
    // Discover stateful forwarding transitively before ordinary inlining.
    let mut stream_function_paths: BTreeSet<TypePath> = table
        .stream_functions
        .keys()
        .filter(|(module, _)| imported.contains_key(module))
        .flat_map(|(module, name)| {
            let mut paths = vec![TypePath::new(
                module.iter().cloned().collect(),
                name.clone(),
            )];
            if imported[module] {
                paths.push(TypePath::new(EcoVec::new(), name.clone()));
            }
            paths
        })
        .collect();
    let mut local_names = BTreeSet::new();
    for declaration in parsed.declarations() {
        if let ParsedDeclaration::Def { name, span, .. } = declaration {
            if !local_names.insert(name.clone()) {
                return Err(DsrvExpandError::InvalidStreamFunction {
                    stream_function: name.to_string(),
                    problem: "the def name is declared twice".into(),
                    span: *span,
                });
            }
            // A local def shadows a glob-imported def regardless of state.
            stream_function_paths.remove(&TypePath::new(
                EcoVec::new(),
                TypeName::new(name.name()).unwrap(),
            ));
        }
    }
    let held = parsed.roots();
    let mut candidates = Vec::new();
    let mut root = 0;
    for declaration in parsed.declarations() {
        match declaration {
            ParsedDeclaration::Def { name, locals, .. } => {
                let name = TypeName::new(name.name()).expect("a function name");
                if locals.is_some() {
                    stream_function_paths.insert(TypePath::new(EcoVec::new(), name.clone()));
                    stream_function_paths
                        .insert(TypePath::new(path.iter().cloned().collect(), name.clone()));
                }
                candidates.push((name, root, super::inline::declaration_bindings(declaration)));
            }
            _ => {}
        }
        root += declaration.root_count();
    }
    loop {
        let before = stream_function_paths.len();
        for (name, root, bound) in &candidates {
            let normalised = super::inline::normalise_stream_expression(
                held[*root].as_ref(),
                &stream_function_paths,
                bound.clone(),
            )?;
            let stateful = normalised.as_ref().postorder().any(|node| {
                matches!(
                    node.kind(),
                    crate::lang::dsrv::syntax::parsed::ParsedExprKind::StreamFunctionCall(..)
                )
            });
            if stateful {
                stream_function_paths.insert(TypePath::new(EcoVec::new(), name.clone()));
                stream_function_paths
                    .insert(TypePath::new(path.iter().cloned().collect(), name.clone()));
            }
        }
        if before == stream_function_paths.len() {
            break;
        }
    }
    let stream_paths = stream_function_paths.clone();
    let normalised = super::inline::normalise_stream_functions(parsed, stream_function_paths)?;
    let parsed = &normalised;
    let declarations = parsed.declarations();
    let held = parsed.roots();
    let trees: Vec<ParsedExprRef<'_>> = held.iter().map(|tree| tree.as_ref()).collect();
    // The module's environment is in the table while its constants are
    // folded, since a call they make is expanded in it; the constants it
    // may name are settled once they are.
    let id = LexicalId::from_index(table.lexicals.len());
    let lexical = |constants: &ConstantTable| -> Result<Lexical, DsrvExpandError> {
        let own_constants = Constants::new(AstShared::new(constants.clone()), path, declarations)?;
        Ok(Lexical {
            module: path.clone(),
            context: AstShared::clone(&context),
            runtime_constants: own_constants.for_runtime_text(),
            constants: own_constants,
            imported: imported_modules(path, declarations)?,
        })
    };
    unshared(table).lexicals.push(lexical(constants)?);
    super::constants::add_module(
        constants,
        path,
        parsed,
        &super::constants::Folding {
            functions: table,
            context: &context,
            archive,
            label,
        },
    )?;
    unshared(table).lexicals[id.index()] = lexical(constants)?;

    let (built, built_stream_functions) = {
        let table: &FunctionTable = table;
        let lexical = table
            .lexical(id)
            .expect("the module's environment was just added");
        // What this module's own bodies may call: its imports, then its own
        // defs, so a local name wins as it does for types (S11). A constant
        // they name is this module's, folded here rather than at a caller
        // that may name something else by it.
        let mut scope = scope_of(&lexical.imported, table);
        scope
            .stream_values
            .extend(super::inline::parsed_stream_names(declarations));
        for name in &local_names {
            scope.stream_functions.remove(&TypePath::new(
                EcoVec::new(),
                TypeName::new(name.name()).unwrap(),
            ));
        }
        scope.stream_functions.extend(stream_paths.iter().cloned());
        scope.constants = Some(&lexical.constants);
        let mut local: Vec<(
            VarName,
            usize,
            EcoVec<(VarName, SourceType)>,
            EcoVec<TypeName>,
            SourceType,
            bool,
        )> = Vec::new();
        let mut stream_functions = Vec::new();
        let mut root = 0usize;
        for declaration in declarations {
            match declaration {
                ParsedDeclaration::Def { name, fixed, .. } => {
                    let name = TypePath::new(EcoVec::new(), TypeName::new(name.name()).unwrap());
                    if stream_paths.contains(&name) {
                        stream_functions.push((stream_declaration(declaration)?, root));
                    } else {
                        if !fixed.is_empty() {
                            return Err(DsrvExpandError::InvalidStreamFunction {
                                stream_function: name.to_string(),
                                problem: "fixed parameters require a stream function".into(),
                                span: declaration_span(declaration),
                            });
                        }
                        stream_functions::refuse_uses(trees[root], "a `def`")?;
                        local_def(declaration, &mut local, root);
                    }
                    root += declaration.root_count();
                }
                ParsedDeclaration::Const { .. } => {
                    stream_functions::refuse_uses(trees[root], "a `const`")?;
                    root += 1;
                }
                other => root += other.root_count(),
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
        // A stream_function body reads this module's defs and constants as a def
        // body does, so it is inlined against the same scope.
        let mut built_stream_functions = Vec::with_capacity(stream_functions.len());
        for (stream_function, first) in stream_functions {
            let entry = stream_functions::build_entry(
                &stream_function,
                &trees[first..first + stream_function.body.len()],
                &scope,
                lexical.context.language(),
                path,
                parsed.source(),
                id,
            )?;
            built_stream_functions.push(((path.clone(), stream_function.name.clone()), entry));
        }
        (built, built_stream_functions)
    };
    let table = unshared(table);
    table.entries.extend(built);
    table.stream_functions.extend(built_stream_functions);
    Ok(())
}

/// Record a def of this module, whose body is root `root`.
#[allow(clippy::type_complexity)]
fn local_def(
    declaration: &ParsedDeclaration,
    local: &mut Vec<(
        VarName,
        usize,
        EcoVec<(VarName, SourceType)>,
        EcoVec<TypeName>,
        SourceType,
        bool,
    )>,
    root: usize,
) {
    if let ParsedDeclaration::Def {
        name,
        type_parameters,
        parameters,
        result,
        internal,
        ..
    } = declaration
    {
        local.push((
            name.clone(),
            root,
            parameters.clone(),
            type_parameters.clone(),
            result.clone(),
            *internal,
        ));
    }
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
    /// callable bare, the ones it keeps internal included (S12), unless
    /// `runtime_text` says this is the view of text supplied at run time.
    own: LexicalId,
    /// Whether this is what text supplied at run time to a `dynamic` or
    /// `defer` node may call. Such text is never the owning module: it sees
    /// that module as an importer does, without its internal defs or
    /// constants.
    runtime_text: bool,
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
        Self {
            table,
            own,
            runtime_text: false,
        }
    }

    fn lexical(&self) -> Option<&Lexical> {
        self.table.lexical(self.own)
    }

    /// Whether this is the view of text supplied at run time.
    pub(crate) fn is_runtime_text(&self) -> bool {
        self.runtime_text
    }

    /// The constants this view may name.
    fn constants(&self) -> Option<&Constants> {
        self.lexical().map(|lexical| {
            if self.runtime_text {
                &lexical.runtime_constants
            } else {
                &lexical.constants
            }
        })
    }

    /// Whether any def is callable at all, which is what says a node needs
    /// to carry this on into the text it is given. A program with a
    /// stream_function keeps its table too: instantiated code is read in the
    /// environment of the module that wrote it.
    pub(crate) fn is_empty(&self) -> bool {
        self.table.entries.is_empty()
            && self.table.stream_functions.is_empty()
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
            runtime_text: false,
        };
        let callable = (!callable.is_empty()).then(|| AstShared::new(callable));
        (AstShared::clone(&lexical.context), callable)
    }

    /// Whether `id` names this callable's own environment.
    pub(crate) fn is_own(&self, id: LexicalId) -> bool {
        self.own == id
    }

    /// The module whose environment `id` names in this callable's table.
    pub(crate) fn module_of(&self, id: LexicalId) -> Option<&ModulePath> {
        self.table.lexical(id).map(|lexical| &lexical.module)
    }

    /// The module whose text this callable's own environment is.
    pub(crate) fn own_module(&self) -> Option<&ModulePath> {
        self.module_of(self.own)
    }

    fn at(&self, own: LexicalId) -> Self {
        Self {
            table: AstShared::clone(&self.table),
            own,
            runtime_text: self.runtime_text,
        }
    }

    /// What text supplied at run time to a node that may call `self` may
    /// call: the owning module as an importer sees it (its exported defs and
    /// constants), what it imports, and never its internal defs or
    /// constants.
    pub(crate) fn for_runtime_text(&self) -> Self {
        Self {
            runtime_text: true,
            ..self.clone()
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
        if let Some(constants) = self.constants() {
            constants.describe(out);
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
            if let Some(constants) = callable.constants() {
                constants.describe(out);
            }
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

    /// The environment of the module whose text this is.
    pub(crate) fn own(&self) -> LexicalId {
        self.own
    }

    /// The stream_function `path` names in text written in environment `from`:
    /// one of a module that text imported.
    pub(crate) fn stream_function(
        &self,
        from: LexicalId,
        path: &TypePath,
    ) -> Option<&StreamFunctionEntry> {
        let lexical = self.table.lexical(from)?;
        let name = path.name().clone();
        if path.module().is_empty() {
            if let Some(entry) = self
                .table
                .stream_functions
                .get(&(lexical.module.clone(), name.clone()))
            {
                return Some(entry);
            }
            return lexical
                .imported
                .iter()
                .filter(|(_, bare)| **bare)
                .find_map(|(module, _)| {
                    self.table
                        .stream_functions
                        .get(&(module.clone(), name.clone()))
                        .filter(|entry| !entry.internal)
                });
        }
        let module = path.module().to_vec();
        if module != lexical.module && !lexical.imported.contains_key(&module) {
            return None;
        }
        self.table
            .stream_functions
            .get(&(module.clone(), name))
            .filter(|entry| module == lexical.module || !entry.internal)
    }

    /// The def `path` names in text written in environment `from`, as the
    /// module and name the table holds it under. A bare name finds the
    /// module's own defs before the ones its globs imported, as a call does.
    pub(crate) fn resolve_def(
        &self,
        from: LexicalId,
        path: &ValuePath,
    ) -> Option<(ModulePath, VarName)> {
        let lexical = self.table.lexical(from)?;
        let name = path.name();
        if path.module().is_empty() {
            if self
                .table
                .entries
                .contains_key(&(lexical.module.clone(), name.clone()))
            {
                return Some((lexical.module.clone(), name.clone()));
            }
            return lexical
                .imported
                .iter()
                .filter(|(_, bare)| **bare)
                .map(|(module, _)| (module.clone(), name.clone()))
                .filter(|key| {
                    self.table
                        .entries
                        .get(key)
                        .is_some_and(|entry| !entry.internal)
                })
                .last();
        }
        let key = (path.module().to_vec(), name.clone());
        let own = key.0 == lexical.module;
        let reachable = own || lexical.imported.contains_key(&key.0);
        let entry = self.table.entries.get(&key)?;
        (reachable && (own || !entry.internal)).then_some(key)
    }

    /// The def the table holds under `key`.
    pub(crate) fn def(&self, key: &(ModulePath, VarName)) -> Option<Def<'_>> {
        self.table.entries.get(key).map(Entry::as_def)
    }

    /// Borrow the table into a scope for one expansion.
    pub(crate) fn scope(&self) -> Scope<'_> {
        let Some(lexical) = self.lexical() else {
            return Scope::default();
        };
        let mut scope = scope_of(&lexical.imported, &self.table);
        for ((module, name), entry) in &self.table.stream_functions {
            if *module == lexical.module && (!self.runtime_text || !entry.internal) {
                scope
                    .stream_functions
                    .insert(TypePath::new(EcoVec::new(), name.clone()));
                scope.stream_functions.insert(TypePath::new(
                    module.iter().cloned().collect(),
                    name.clone(),
                ));
            }
        }
        scope.constants = Some(if self.runtime_text {
            &lexical.runtime_constants
        } else {
            &lexical.constants
        });
        // A file's own defs win over the ones it imported, as types do
        // (S11), and a file may call the ones it keeps to itself. Text
        // supplied at run time is not the file: it gets only the ones the
        // file exports.
        let own = if self.runtime_text {
            self.table.exported(&lexical.module)
        } else {
            self.table.declared(&lexical.module)
        };
        for (name, entry) in own {
            scope.stream_functions.remove(&TypePath::new(
                EcoVec::new(),
                TypeName::new(name.name()).unwrap(),
            ));
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
        AstShared::ptr_eq(&self.table, &other.table)
            && self.own == other.own
            && self.runtime_text == other.runtime_text
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
pub(super) fn imported_scope<'a>(
    imported: &BTreeMap<ModulePath, bool>,
    table: &'a FunctionTable,
) -> Scope<'a> {
    scope_of(imported, table)
}

fn scope_of<'a>(imported: &BTreeMap<ModulePath, bool>, table: &'a FunctionTable) -> Scope<'a> {
    let mut scope = Scope::default();
    for (module, bare) in imported {
        for ((owner, name), entry) in &table.stream_functions {
            if owner == module && !entry.internal {
                scope.stream_functions.insert(TypePath::new(
                    module.iter().cloned().collect(),
                    name.clone(),
                ));
                if *bare {
                    scope
                        .stream_functions
                        .insert(TypePath::new(EcoVec::new(), name.clone()));
                }
            }
        }
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

fn declaration_span(declaration: &ParsedDeclaration) -> crate::lang::dsrv::span::Span {
    let ParsedDeclaration::Def { span, .. } = declaration else {
        unreachable!()
    };
    *span
}

/// Assemble split local declarations while retaining source equation order.
fn stream_declaration(
    declaration: &ParsedDeclaration,
) -> Result<crate::lang::dsrv::syntax::parsed::StreamFunctionDeclaration, DsrvExpandError> {
    use crate::lang::dsrv::syntax::parsed::{LocalEquation, StreamFunctionDeclaration};
    let ParsedDeclaration::Def {
        name,
        type_parameters,
        fixed,
        parameters,
        result,
        body,
        locals,
        internal,
        span,
    } = declaration
    else {
        unreachable!()
    };
    let invalid = |problem: String, span| DsrvExpandError::InvalidStreamFunction {
        stream_function: name.to_string(),
        problem,
        span,
    };
    let mut annotations = BTreeMap::new();
    let mut equations = Vec::new();
    let mut defined = BTreeSet::new();
    for local in locals.iter().flatten() {
        match local {
            ParsedDeclaration::Aux(name, ty, root, span) => {
                if annotations.insert(name.clone(), ty.clone()).is_some() {
                    return Err(invalid(
                        format!("local stream `{name}` is declared twice"),
                        *span,
                    ));
                }
                if let Some(root) = root {
                    if !defined.insert(name.clone()) {
                        return Err(invalid(
                            format!("local stream `{name}` is defined twice"),
                            *span,
                        ));
                    }
                    equations.push((name.clone(), *root, *span));
                }
            }
            ParsedDeclaration::Equation(name, root, span) => {
                if !defined.insert(name.clone()) {
                    return Err(invalid(
                        format!("local stream `{name}` is defined twice"),
                        *span,
                    ));
                }
                equations.push((name.clone(), *root, *span));
            }
            _ => unreachable!("local grammar admits only aux declarations and equations"),
        }
    }
    for local in annotations.keys() {
        if !defined.contains(local) {
            return Err(invalid(
                format!("local stream `{local}` has no equation"),
                *span,
            ));
        }
    }
    let mut equations_body: EcoVec<LocalEquation> = EcoVec::new();
    equations_body.push(LocalEquation {
        output: true,
        name: VarName::from("$result"),
        ty: Some(result.clone()),
        root: *body,
        span: *span,
    });
    for (name, root, span) in equations {
        equations_body.push(LocalEquation {
            output: false,
            ty: annotations.remove(&name).flatten(),
            name,
            root,
            span,
        });
    }
    Ok(StreamFunctionDeclaration {
        internal: *internal,
        name: TypeName::new(name.name()).unwrap(),
        type_parameters: type_parameters.clone(),
        fixed: fixed.clone(),
        streams: parameters.clone(),
        result: Some(result.clone()),
        body: equations_body,
        span: *span,
    })
}
