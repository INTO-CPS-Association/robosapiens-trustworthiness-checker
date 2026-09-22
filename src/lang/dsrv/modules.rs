//! Collecting the modules one program is made of.
//!
//! Expansion reads no files, so the collector does not either: it *asks* for
//! a module's source and the caller at the IO edge answers. That keeps the
//! whole front end testable without a filesystem, and leaves the one read in
//! the program where it already was.
//!
//! A module's path is absolute from the root, which has the empty path. A
//! `mod a::b` inside module `m` therefore declares `m::a::b`, and the file it
//! names is that path joined with `/` and suffixed `.dsrv`, relative to the
//! root file's directory.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;

use crate::lang::dsrv::ast::AstShared;
use crate::lang::dsrv::path::{ImportKind, ModuleName, PathSegment, UseTree};
use crate::lang::dsrv::source::TypeName;
use crate::lang::dsrv::source_map::{SourceArchive, SourceFile, SourceLabel};
use crate::lang::dsrv::span::Span;
use crate::lang::dsrv::syntax::{DsrvSyntaxError, ParsedDeclaration, ParsedSpecification};

/// A module's name, absolute from the root module.
pub type ModulePath = Vec<ModuleName>;

/// A path as a diagnostic should print it.
pub fn show_path(path: &[ModuleName]) -> String {
    if path.is_empty() {
        return "the root module".to_owned();
    }
    path.iter()
        .map(ModuleName::as_str)
        .collect::<Vec<_>>()
        .join("::")
}

/// The file a module path names, relative to the root file's directory.
///
/// `a::b` is `a/b.dsrv`, so a module with submodules sits beside the
/// directory holding them.
pub fn module_file(path: &[ModuleName]) -> String {
    let mut file = path
        .iter()
        .map(ModuleName::as_str)
        .collect::<Vec<_>>()
        .join("/");
    file.push_str(".dsrv");
    file
}

/// What one leaf of a `use` names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ImportItems {
    /// `use m` — the module itself, so `m::X` may be written.
    Module,
    /// `use m::*` — every item of the module.
    All,
    /// `use m::T` — one type.
    Type(TypeName),
    /// `use m::T::X` — one constructor of a type.
    Constructor { ty: TypeName, tag: TypeName },
    /// `use m::T::*` — every constructor of a type.
    Constructors(TypeName),
}

/// One thing a `use` line asks for, with the module resolved to an absolute
/// path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Import {
    pub module: ModulePath,
    pub items: ImportItems,
    pub span: Span,
}

#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    #[error("`{spelling}` at {span:?} names no module: a path must begin with a module")]
    NoModule { spelling: String, span: Span },

    #[error("`{spelling}` at {span:?} reaches past a constructor")]
    TooDeep { spelling: String, span: Span },

    #[error("`self` at {span:?} may only begin a path")]
    MisplacedSelf { span: Span },
}

/// Every item a `use` asks for, with `self` resolved against the module the
/// line was written in.
///
/// A group entry is a `use` relative to the enclosing path, so the tree is
/// flattened here and each leaf becomes one `Import`.
pub fn imports_of(tree: &UseTree, current: &[ModuleName]) -> Result<Vec<Import>, ImportError> {
    let mut found = Vec::new();
    flatten(tree, &[], current, &mut found)?;
    Ok(found)
}

fn flatten(
    tree: &UseTree,
    prefix: &[PathSegment],
    current: &[ModuleName],
    found: &mut Vec<Import>,
) -> Result<(), ImportError> {
    // `self` as a whole group entry names the module the prefix reaches,
    // which is how `use a::b::{self, C}` imports `b` alongside `b::C`.
    if tree.path() == [PathSegment::Zelf] && !prefix.is_empty() {
        return match tree.kind() {
            ImportKind::Item => {
                found.push(classify(prefix, false, current, tree)?);
                Ok(())
            }
            _ => Err(ImportError::MisplacedSelf { span: tree.span() }),
        };
    }
    let mut segments = prefix.to_vec();
    for (index, segment) in tree.path().iter().enumerate() {
        // Anywhere else, `self` may only begin the whole path.
        if matches!(segment, PathSegment::Zelf) && !(prefix.is_empty() && index == 0) {
            return Err(ImportError::MisplacedSelf { span: tree.span() });
        }
        segments.push(segment.clone());
    }
    match tree.kind() {
        ImportKind::Group(items) => {
            for item in items {
                flatten(item, &segments, current, found)?;
            }
            Ok(())
        }
        kind => {
            let glob = matches!(kind, ImportKind::Glob);
            found.push(classify(&segments, glob, current, tree)?);
            Ok(())
        }
    }
}

/// Split a path into the module it reaches and the names inside it.
///
/// The split is the lexical one (S1): the leading lowercase run names the
/// module, and the capitalised segments after it name a type and its tag.
fn classify(
    segments: &[PathSegment],
    glob: bool,
    current: &[ModuleName],
    tree: &UseTree,
) -> Result<Import, ImportError> {
    let spelling = || {
        segments
            .iter()
            .map(PathSegment::as_str)
            .collect::<Vec<_>>()
            .join("::")
    };
    let mut module: ModulePath = Vec::new();
    let mut names: Vec<TypeName> = Vec::new();
    // A path through `self` names a module even in the root, whose path is
    // empty; one with no segments at all names none.
    let mut rooted = false;
    for segment in segments {
        match segment {
            PathSegment::Zelf => {
                module.extend_from_slice(current);
                rooted = true;
            }
            PathSegment::Module(name) if names.is_empty() => module.push(name.clone()),
            // A lowercase segment after a capitalised one would be an item
            // inside a type, which nothing names.
            PathSegment::Module(_) => {
                return Err(ImportError::TooDeep {
                    spelling: spelling(),
                    span: tree.span(),
                });
            }
            PathSegment::Name(name) => names.push(name.clone()),
        }
    }
    if module.is_empty() && !rooted {
        return Err(ImportError::NoModule {
            spelling: spelling(),
            span: tree.span(),
        });
    }
    let items = match (names.as_slice(), glob) {
        ([], false) => ImportItems::Module,
        ([], true) => ImportItems::All,
        ([ty], false) => ImportItems::Type(ty.clone()),
        ([ty], true) => ImportItems::Constructors(ty.clone()),
        ([ty, tag], false) => ImportItems::Constructor {
            ty: ty.clone(),
            tag: tag.clone(),
        },
        _ => {
            return Err(ImportError::TooDeep {
                spelling: spelling(),
                span: tree.span(),
            });
        }
    };
    Ok(Import {
        module,
        items,
        span: tree.span(),
    })
}

#[derive(Debug, thiserror::Error)]
pub enum ModuleCollectError {
    #[error("module {path} is declared more than once")]
    DuplicateModule { path: String },

    #[error("module {path} was declared but never supplied")]
    MissingModule { path: String },

    // `show_path` already spells the root as a phrase, so the message reads
    // around the path rather than putting "module" in front of it.
    #[error("{path} does not parse: {source}")]
    Syntax {
        path: String,
        #[source]
        source: DsrvSyntaxError,
    },
}

/// Every module of one program, parsed, keyed by absolute path, with the
/// archive of their text.
pub struct ModuleSources {
    modules: BTreeMap<ModulePath, ParsedSpecification>,
    root_source: String,
    archive: AstShared<SourceArchive>,
}

impl ModuleSources {
    /// Take the root module out, leaving the rest behind.
    pub(crate) fn into_root(mut self) -> ParsedSpecification {
        self.modules
            .remove(&ModulePath::new())
            .expect("the root module is always collected")
    }

    /// The root module's text, exactly as it was supplied.
    pub fn root_source(&self) -> &str {
        &self.root_source
    }

    /// The text of every module, which the expanded program owns.
    pub(crate) fn archive(&self) -> &AstShared<SourceArchive> {
        &self.archive
    }

    pub(crate) fn get(&self, path: &[ModuleName]) -> Option<&ParsedSpecification> {
        self.modules.get(path)
    }

    /// Whether a module of this path was collected.
    pub fn contains(&self, path: &[ModuleName]) -> bool {
        self.modules.contains_key(path)
    }

    /// Every collected path, the root's empty one included, in order.
    pub fn paths(&self) -> impl Iterator<Item = &[ModuleName]> {
        self.modules.keys().map(Vec::as_slice)
    }

    pub fn len(&self) -> usize {
        self.modules.len()
    }

    pub fn is_empty(&self) -> bool {
        self.modules.is_empty()
    }
}

/// Printed by path: the parsed trees themselves are too large to be useful
/// in a failure message.
impl fmt::Debug for ModuleSources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set()
            .entries(self.modules.keys().map(|path| show_path(path)))
            .finish()
    }
}

/// Drives module collection without performing any IO.
///
/// Ask it for the next module it needs, supply that module's source, repeat
/// until it asks for nothing.
pub struct ModuleCollector {
    modules: BTreeMap<ModulePath, ParsedSpecification>,
    pending: VecDeque<ModulePath>,
    root_source: String,
    archive: SourceArchive,
}

/// The label of a module supplied without one.
fn supplied_label(path: &[ModuleName]) -> SourceLabel {
    SourceLabel::Supplied(format!("<{}>", show_path(path)).into())
}

impl ModuleCollector {
    /// Begin from the root module's source.
    pub fn new(root: &str) -> Result<Self, ModuleCollectError> {
        Self::with_label(root, supplied_label(&[]))
    }

    /// Begin from the root module's source, which diagnostics name `label`.
    pub fn with_label(root: &str, label: SourceLabel) -> Result<Self, ModuleCollectError> {
        let mut collector = Self {
            modules: BTreeMap::new(),
            pending: VecDeque::new(),
            root_source: root.to_owned(),
            archive: SourceArchive::new(),
        };
        collector.add(ModulePath::new(), root, label)?;
        Ok(collector)
    }

    /// The next module whose source is needed, or `None` when complete.
    pub fn next_request(&self) -> Option<&[ModuleName]> {
        self.pending.front().map(Vec::as_slice)
    }

    /// Supply the source of the module `next_request` last named.
    pub fn supply(&mut self, source: &str) -> Result<(), ModuleCollectError> {
        let label = supplied_label(
            self.pending
                .front()
                .expect("supply answers an outstanding request"),
        );
        self.supply_labelled(source, label)
    }

    /// Supply the source of the module `next_request` last named, which
    /// diagnostics name `label`.
    pub fn supply_labelled(
        &mut self,
        source: &str,
        label: SourceLabel,
    ) -> Result<(), ModuleCollectError> {
        let path = self
            .pending
            .pop_front()
            .expect("supply answers an outstanding request");
        self.add(path, source, label)
    }

    pub fn finish(self) -> Result<ModuleSources, ModuleCollectError> {
        if let Some(path) = self.pending.front() {
            return Err(ModuleCollectError::MissingModule {
                path: show_path(path),
            });
        }
        Ok(ModuleSources {
            modules: self.modules,
            root_source: self.root_source,
            archive: AstShared::new(self.archive),
        })
    }

    fn add(
        &mut self,
        path: ModulePath,
        source: &str,
        label: SourceLabel,
    ) -> Result<(), ModuleCollectError> {
        let parsed = crate::lang::dsrv::syntax::parse_specification(source).map_err(|source| {
            ModuleCollectError::Syntax {
                path: show_path(&path),
                source,
            }
        })?;
        let parsed = parsed.with_source(self.archive.push(SourceFile::new(
            label,
            path.clone(),
            source,
        )));
        for declaration in parsed.declarations() {
            let ParsedDeclaration::Mod { path: declared, .. } = declaration else {
                continue;
            };
            let mut full = path.clone();
            full.extend(declared.iter().cloned());
            // A path names one file, so two declarations of it would read the
            // same file twice under one name.
            if self.modules.contains_key(&full) || self.pending.contains(&full) {
                return Err(ModuleCollectError::DuplicateModule {
                    path: show_path(&full),
                });
            }
            self.pending.push_back(full);
        }
        self.modules.insert(path, parsed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use test_log::test;

    const HEADER: &str = "use experimental::{modules}\n";

    fn name(s: &str) -> ModuleName {
        ModuleName::new(s).expect("a lowercase name")
    }

    /// Collect a program whose modules are supplied from a table rather than
    /// a filesystem, which is the whole point of the sans-IO shape.
    fn collect(root: &str, sources: &[(&str, &str)]) -> Result<ModuleSources, ModuleCollectError> {
        let mut collector = ModuleCollector::new(root)?;
        while let Some(path) = collector.next_request().map(<[ModuleName]>::to_vec) {
            let wanted = show_path(&path);
            let source = sources
                .iter()
                .find(|(name, _)| *name == wanted)
                .unwrap_or_else(|| panic!("no source for {wanted}"))
                .1;
            collector.supply(source)?;
        }
        collector.finish()
    }

    #[test]
    fn a_program_with_no_modules_is_just_its_root() {
        let sources = collect(&format!("{HEADER}in x: Int\n"), &[]).expect("collected");
        assert_eq!(sources.len(), 1);
        assert_eq!(sources.paths().collect::<Vec<_>>(), [[].as_slice()]);
    }

    #[test]
    fn a_declared_module_is_collected() {
        let sources = collect(
            &format!("{HEADER}mod store\nin x: Int\n"),
            &[("store", &format!("{HEADER}in y: Int\n"))],
        )
        .expect("collected");
        assert_eq!(sources.len(), 2);
        assert!(sources.contains(&[name("store")]));
    }

    #[test]
    fn a_module_may_declare_its_own_submodules() {
        let sources = collect(
            &format!("{HEADER}mod a\nin x: Int\n"),
            &[
                ("a", &format!("{HEADER}mod b\nin y: Int\n")),
                ("a::b", &format!("{HEADER}in z: Int\n")),
            ],
        )
        .expect("collected");
        assert_eq!(sources.len(), 3);
        assert!(sources.contains(&[name("a"), name("b")]));
    }

    /// A submodule's path is absolute, so `mod b` inside `a` is `a::b` rather
    /// than a second root-level `b`.
    #[test]
    fn a_submodule_path_is_absolute_from_the_root() {
        let sources = collect(
            &format!("{HEADER}mod a\nin x: Int\n"),
            &[
                ("a", &format!("{HEADER}mod b\nin y: Int\n")),
                ("a::b", &format!("{HEADER}in z: Int\n")),
            ],
        )
        .expect("collected");
        assert!(!sources.contains(&[name("b")]), "b is not at the root");
    }

    #[test]
    fn a_nested_declaration_names_a_nested_path() {
        let sources = collect(
            &format!("{HEADER}mod lib::inner\nin x: Int\n"),
            &[("lib::inner", &format!("{HEADER}in y: Int\n"))],
        )
        .expect("collected");
        assert!(sources.contains(&[name("lib"), name("inner")]));
    }

    #[test]
    fn declaring_one_module_twice_is_an_error() {
        let error = collect(
            &format!("{HEADER}mod a\nmod a\nin x: Int\n"),
            &[("a", &format!("{HEADER}in y: Int\n"))],
        )
        .expect_err("a duplicate");
        assert!(
            matches!(&error, ModuleCollectError::DuplicateModule { path } if path == "a"),
            "got {error:?}",
        );
    }

    /// Two different files can name the same module: the root's `mod a::b`
    /// and `a`'s own `mod b` both mean `a::b`.
    #[test]
    fn two_files_may_not_name_the_same_module() {
        let error = collect(
            &format!("{HEADER}mod a\nmod a::b\nin x: Int\n"),
            &[
                ("a", &format!("{HEADER}mod b\nin y: Int\n")),
                ("a::b", &format!("{HEADER}in z: Int\n")),
            ],
        )
        .expect_err("a duplicate");
        assert!(
            matches!(&error, ModuleCollectError::DuplicateModule { path } if path == "a::b"),
            "got {error:?}",
        );
    }

    #[test]
    fn a_module_that_is_never_supplied_is_reported() {
        let collector =
            ModuleCollector::new(&format!("{HEADER}mod missing\nin x: Int\n")).expect("root");
        let error = collector.finish().expect_err("unsupplied");
        assert!(
            matches!(&error, ModuleCollectError::MissingModule { path } if path == "missing"),
            "got {error:?}",
        );
    }

    #[test]
    fn a_syntax_error_names_the_module_it_is_in() {
        let error = collect(
            &format!("{HEADER}mod a\nin x: Int\n"),
            &[("a", "this is not dsrv @@@")],
        )
        .expect_err("a syntax error");
        assert!(
            matches!(&error, ModuleCollectError::Syntax { path, .. } if path == "a"),
            "got {error:?}",
        );
    }

    /// The root has no name of its own, so a message about it must not read
    /// "in module the root module".
    #[test]
    fn a_syntax_error_in_the_root_reads_as_a_sentence() {
        let Err(error) = ModuleCollector::new("this is not dsrv @@@") else {
            panic!("a syntax error");
        };
        let message = error.to_string();
        assert!(
            message.starts_with("the root module does not parse:"),
            "got {message}",
        );
    }

    #[test]
    fn a_module_path_names_a_file_under_its_parents() {
        assert_eq!(module_file(&[name("store")]), "store.dsrv");
        assert_eq!(module_file(&[name("lib"), name("inner")]), "lib/inner.dsrv");
    }

    #[test]
    fn the_root_module_prints_as_itself() {
        assert_eq!(show_path(&[]), "the root module");
        assert_eq!(show_path(&[name("a"), name("b")]), "a::b");
    }
}

#[cfg(test)]
mod import_tests {
    use super::*;

    use crate::lang::dsrv::syntax::parse_specification;
    use test_log::test;

    fn name(s: &str) -> ModuleName {
        ModuleName::new(s).expect("a lowercase name")
    }

    fn ty(s: &str) -> TypeName {
        TypeName::new(s).expect("a type name")
    }

    /// Every import one `use` line asks for, read from the module `current`.
    fn imports(line: &str, current: &[ModuleName]) -> Vec<Import> {
        let source = format!("use experimental::{{modules}}\n{line}\nin x: Int\n");
        let parsed = parse_specification(&source).unwrap_or_else(|e| panic!("{source}: {e}"));
        let (_, declarations, _) = parsed.into_parts();
        declarations
            .iter()
            .filter_map(|declaration| match declaration {
                ParsedDeclaration::Use { tree, .. } if !tree.is_experimental() => Some(tree),
                _ => None,
            })
            .flat_map(|tree| imports_of(tree, current).expect("a resolvable import"))
            .collect()
    }

    fn error(line: &str) -> ImportError {
        let source = format!("use experimental::{{modules}}\n{line}\nin x: Int\n");
        let parsed = parse_specification(&source).unwrap_or_else(|e| panic!("{source}: {e}"));
        let (_, declarations, _) = parsed.into_parts();
        let tree = declarations
            .iter()
            .find_map(|declaration| match declaration {
                ParsedDeclaration::Use { tree, .. } if !tree.is_experimental() => Some(tree),
                _ => None,
            })
            .expect("a use line");
        imports_of(tree, &[]).expect_err("an import error")
    }

    #[test]
    fn a_bare_path_names_the_module_itself() {
        let found = imports("use lib::inner", &[]);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].module, vec![name("lib"), name("inner")]);
        assert_eq!(found[0].items, ImportItems::Module);
    }

    #[test]
    fn a_glob_takes_every_item() {
        let found = imports("use store::*", &[]);
        assert_eq!(found[0].module, vec![name("store")]);
        assert_eq!(found[0].items, ImportItems::All);
    }

    #[test]
    fn a_capitalised_leaf_is_a_type() {
        let found = imports("use lib::inner::Colour", &[]);
        assert_eq!(found[0].module, vec![name("lib"), name("inner")]);
        assert_eq!(found[0].items, ImportItems::Type(ty("Colour")));
    }

    #[test]
    fn two_capitalised_segments_name_a_constructor() {
        let found = imports("use lib::opt::Colour::Red", &[]);
        assert_eq!(found[0].module, vec![name("lib"), name("opt")]);
        assert_eq!(
            found[0].items,
            ImportItems::Constructor {
                ty: ty("Colour"),
                tag: ty("Red"),
            }
        );
    }

    #[test]
    fn a_glob_under_a_type_takes_its_constructors() {
        let found = imports("use lib::opt::Colour::*", &[]);
        assert_eq!(found[0].items, ImportItems::Constructors(ty("Colour")));
    }

    /// A group is flattened: each leaf becomes its own import, relative to
    /// the enclosing path.
    #[test]
    fn a_group_becomes_one_import_per_leaf() {
        let found = imports("use lib::opt::Colour::{Red, Green}", &[]);
        assert_eq!(found.len(), 2);
        assert!(
            found
                .iter()
                .all(|i| i.module == vec![name("lib"), name("opt")])
        );
        assert_eq!(
            found[1].items,
            ImportItems::Constructor {
                ty: ty("Colour"),
                tag: ty("Green"),
            }
        );
    }

    #[test]
    fn self_in_a_group_imports_the_module_alongside_its_items() {
        let found = imports("use lib::other::{self, Mode}", &[]);
        assert_eq!(found.len(), 2);
        assert_eq!(found[0].items, ImportItems::Module);
        assert_eq!(found[1].items, ImportItems::Type(ty("Mode")));
    }

    #[test]
    fn nested_groups_flatten_to_their_leaves() {
        let found = imports("use lib::{inner::{Colour}, other::*}", &[]);
        assert_eq!(found.len(), 2);
        assert_eq!(found[0].module, vec![name("lib"), name("inner")]);
        assert_eq!(found[0].items, ImportItems::Type(ty("Colour")));
        assert_eq!(found[1].module, vec![name("lib"), name("other")]);
        assert_eq!(found[1].items, ImportItems::All);
    }

    /// `self` is the module the line was written in, so the same text means
    /// different things in different modules.
    #[test]
    fn self_resolves_against_the_module_it_is_written_in() {
        let found = imports("use self::Colour::{Red}", &[name("store")]);
        assert_eq!(found[0].module, vec![name("store")]);
        assert_eq!(
            found[0].items,
            ImportItems::Constructor {
                ty: ty("Colour"),
                tag: ty("Red"),
            }
        );
    }

    #[test]
    fn self_in_the_root_names_the_root() {
        let found = imports("use self::Colour", &[]);
        assert!(found[0].module.is_empty(), "the root has the empty path");
    }

    #[test]
    fn a_path_reaching_past_a_constructor_is_refused() {
        assert!(matches!(
            error("use lib::opt::Colour::Red::Deeper"),
            ImportError::TooDeep { .. }
        ));
    }

    #[test]
    fn a_lowercase_segment_after_a_type_is_refused() {
        assert!(matches!(
            error("use lib::Colour::inner"),
            ImportError::TooDeep { .. }
        ));
    }

    #[test]
    fn a_type_with_no_module_is_refused() {
        assert!(matches!(error("use Colour"), ImportError::NoModule { .. }));
    }
}
