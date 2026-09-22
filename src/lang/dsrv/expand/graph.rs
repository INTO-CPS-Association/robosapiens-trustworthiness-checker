//! Every module's namespace, built in dependency order.
//!
//! A module's `SourceContext` is self-contained (S14): the names it imports
//! are copied into it, so looking one up needs no graph and a node carries
//! one `Rc` as before. Imports are applied in tiers so that a local
//! declaration shadows a glob silently while an explicit clash is an error
//! (S15).

use std::collections::{BTreeMap, BTreeSet};

use ecow::{EcoString, EcoVec};

use crate::lang::dsrv::ast::AstShared as Rc;
use crate::lang::dsrv::modules::{
    Import, ImportItems, ModulePath, ModuleSources, imports_of, show_path,
};
use crate::lang::dsrv::path::TypePath;
use crate::lang::dsrv::source::{
    AliasDeclaration, SourceAlternative, SourceContext, SourceType, TypeName, substitute_source,
};
use crate::lang::dsrv::syntax::ParsedDeclaration;

use super::{DsrvExpandError, LanguageRequest, language};

/// One namespace per module, keyed by absolute path.
pub(crate) type ModuleGraph = BTreeMap<ModulePath, Rc<SourceContext>>;

/// Build every module's namespace, importers after the modules they import.
pub(crate) fn build_graph(
    sources: &ModuleSources,
    request: LanguageRequest,
) -> Result<ModuleGraph, DsrvExpandError> {
    let order = dependency_order(sources)?;
    let mut graph: ModuleGraph = BTreeMap::new();
    for path in order {
        let context = build_module(&path, sources, &graph, request)?;
        graph.insert(path, Rc::new(context));
    }
    Ok(graph)
}

/// Every module a file imports from, as absolute paths.
pub(super) fn imports_in(
    path: &ModulePath,
    sources: &ModuleSources,
) -> Result<Vec<Import>, DsrvExpandError> {
    let parsed = sources
        .get(path)
        .expect("every path in the graph was collected");
    let mut found = Vec::new();
    for declaration in parsed.declarations() {
        let ParsedDeclaration::Use { tree, .. } = declaration else {
            continue;
        };
        if tree.is_experimental() {
            continue;
        }
        found.extend(imports_of(tree, path)?);
    }
    Ok(found)
}

/// Modules ordered so that each follows everything it imports.
///
/// A cycle is an error naming the path (S13): alias-only modules would
/// sometimes admit a fixpoint, but the simpler rule is the agreed one.
pub(super) fn dependency_order(
    sources: &ModuleSources,
) -> Result<Vec<ModulePath>, DsrvExpandError> {
    let mut order = Vec::new();
    let mut done: BTreeSet<ModulePath> = BTreeSet::new();
    let mut active: Vec<ModulePath> = Vec::new();
    for path in sources.paths() {
        visit(&path.to_vec(), sources, &mut order, &mut done, &mut active)?;
    }
    Ok(order)
}

fn visit(
    path: &ModulePath,
    sources: &ModuleSources,
    order: &mut Vec<ModulePath>,
    done: &mut BTreeSet<ModulePath>,
    active: &mut Vec<ModulePath>,
) -> Result<(), DsrvExpandError> {
    if done.contains(path) {
        return Ok(());
    }
    if let Some(start) = active.iter().position(|entry| entry == path) {
        let mut chain: Vec<String> = active[start..].iter().map(|p| show_path(p)).collect();
        chain.push(show_path(path));
        return Err(DsrvExpandError::ModuleCycle {
            path: chain.join(" -> "),
        });
    }
    active.push(path.clone());
    for import in imports_in(path, sources)? {
        // A module may import its own items, which is not a cycle: it is
        // how `use self::T::{Tag}` brings a local union's tags into scope.
        if import.module == *path {
            continue;
        }
        if !sources.contains(&import.module) {
            return Err(DsrvExpandError::UnknownModule {
                path: show_path(&import.module),
                importer: show_path(path),
            });
        }
        visit(&import.module, sources, order, done, active)?;
    }
    active.pop();
    done.insert(path.clone());
    order.push(path.clone());
    Ok(())
}

/// The items one import contributes, keyed as the importer will write them.
///
/// A resolved alias travels as its type; a template travels as a declaration
/// whose body was resolved in the module that declared it (S16).
fn contributed(
    import: &Import,
    graph: &ModuleGraph,
) -> Result<BTreeMap<TypePath, AliasDeclaration>, DsrvExpandError> {
    let Some(exporter) = graph.get(&import.module) else {
        return Ok(BTreeMap::new());
    };
    let mut taken = BTreeMap::new();
    let mut add = |key: TypePath, under: TypePath| -> Result<(), DsrvExpandError> {
        // An internal name stays in the module that declared it (S12).
        if exporter.is_internal(&key) {
            return Err(DsrvExpandError::InternalImport {
                name: format!("{}::{key}", show_path(&import.module)),
            });
        }
        if let Some(ty) = exporter.get(&key) {
            taken.insert(
                under,
                AliasDeclaration {
                    name: key.name().clone(),
                    parameters: EcoVec::new(),
                    internal: false,
                    ty: SourceType::from(ty.clone()),
                    span: import.span,
                },
            );
            return Ok(());
        }
        if let Some(template) = exporter.generic().get(&key) {
            let mut active = vec![key.clone()];
            let body =
                resolved_template(&template.ty, &template.parameters, exporter, &mut active)?;
            taken.insert(
                under,
                AliasDeclaration {
                    name: key.name().clone(),
                    parameters: template.parameters.clone(),
                    internal: false,
                    ty: body,
                    span: import.span,
                },
            );
        }
        Ok(())
    };

    // Every name the exporter declares, resolved or generic.
    let exported = || -> Vec<TypePath> {
        exporter
            .aliases()
            .keys()
            .chain(exporter.generic().keys())
            .filter(|key| !key.is_qualified() && !exporter.is_internal(key))
            .cloned()
            .collect()
    };

    match &import.items {
        // `use a::b::*` — every item, under local names.
        ImportItems::All => {
            for key in exported() {
                add(key.clone(), key)?;
            }
        }
        // `use a::b` — every item, reachable as `b::X`.
        ImportItems::Module => {
            let Some(last) = import.module.last() else {
                return Ok(taken);
            };
            for key in exported() {
                let under = TypePath::new(EcoVec::from([last.clone()]), key.name().clone());
                add(key, under)?;
            }
        }
        // `use a::b::T` — one item.
        ImportItems::Type(name) => {
            let key = TypePath::local(name.clone());
            add(key.clone(), key)?;
        }
        // Constructors carry no type of their own; they are recorded
        // separately once the importing module's aliases are built.
        ImportItems::Constructor { .. } | ImportItems::Constructors(_) => {}
    }
    Ok(taken)
}

fn build_module(
    path: &ModulePath,
    sources: &ModuleSources,
    graph: &ModuleGraph,
    request: LanguageRequest,
) -> Result<SourceContext, DsrvExpandError> {
    let parsed = sources
        .get(path)
        .expect("every path in the graph was collected");
    let declarations = parsed.declarations();
    let config = language::resolve_language(declarations, request)?;

    // Three tiers, merged by precedence: a local declaration shadows a glob
    // silently, and an explicit clash is an error (S15).
    let mut glob: BTreeMap<TypePath, AliasDeclaration> = BTreeMap::new();
    let mut explicit: BTreeMap<TypePath, AliasDeclaration> = BTreeMap::new();
    for declaration in declarations {
        let ParsedDeclaration::Use { tree, .. } = declaration else {
            continue;
        };
        if tree.is_experimental() {
            continue;
        }
        for import in imports_of(tree, path)? {
            let into = match import.items {
                ImportItems::All => &mut glob,
                _ => &mut explicit,
            };
            for (key, declared) in contributed(&import, graph)? {
                if into.insert(key.clone(), declared).is_some()
                    && !matches!(import.items, ImportItems::All)
                {
                    return Err(
                        crate::lang::dsrv::source::SourceResolveError::DuplicateAlias {
                            name: key.name().clone(),
                            first_span: import.span,
                            span: import.span,
                        }
                        .into(),
                    );
                }
            }
        }
    }

    let mut builder = SourceContext::builder();
    let local: BTreeSet<TypePath> = declarations
        .iter()
        .filter_map(|declaration| match declaration {
            ParsedDeclaration::Alias(alias) => Some(TypePath::local(alias.name.clone())),
            _ => None,
        })
        .collect();
    for (key, declared) in glob {
        // A local declaration wins over a glob without complaint.
        if local.contains(&key) || explicit.contains_key(&key) {
            continue;
        }
        builder.insert_keyed(key, declared)?;
    }
    for (key, declared) in explicit {
        builder.insert_keyed(key, declared)?;
    }
    for declaration in declarations {
        if let ParsedDeclaration::Alias(alias) = declaration {
            language::check_alias(alias, &config)?;
            builder.insert_source(alias.clone())?;
        }
    }
    builder.language(config);
    let context = builder.build()?;

    // Constructor imports are resolved last: a module may import its own
    // tags, whose union only exists once its aliases are built.
    let mut constructors: BTreeMap<EcoString, TypePath> = BTreeMap::new();
    for declaration in declarations {
        let ParsedDeclaration::Use { tree, .. } = declaration else {
            continue;
        };
        if tree.is_experimental() {
            continue;
        }
        for import in imports_of(tree, path)? {
            let (ty, wanted) = match &import.items {
                ImportItems::Constructor { ty, tag } => (ty, Some(tag)),
                ImportItems::Constructors(ty) => (ty, None),
                _ => continue,
            };
            let key = TypePath::local(ty.clone());
            // A module's own union is in the namespace just built; another
            // module's is in the graph.
            let found = if import.module == *path {
                context.get(&key)
            } else {
                graph.get(&import.module).and_then(|other| other.get(&key))
            };
            let Some(crate::core::StreamType::Union(union)) = found else {
                return Err(DsrvExpandError::UnknownConstructor {
                    tag: wanted.map_or_else(|| "*".to_owned(), ToString::to_string),
                    ty: format!("{}::{ty}", show_path(&import.module)),
                });
            };
            let tags: Vec<EcoString> = union
                .alternatives()
                .iter()
                .map(|alternative| alternative.tag().clone())
                .collect();
            match wanted {
                Some(tag) => {
                    let spelled = EcoString::from(tag.as_str());
                    if !tags.contains(&spelled) {
                        return Err(DsrvExpandError::UnknownConstructor {
                            tag: tag.to_string(),
                            ty: format!("{}::{ty}", show_path(&import.module)),
                        });
                    }
                    constructors.insert(spelled, key.clone());
                }
                None => {
                    for tag in tags {
                        constructors.insert(tag, key.clone());
                    }
                }
            }
        }
    }
    Ok(context.with_constructors(constructors))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::core::StreamType;
    use crate::lang::dsrv::modules::{ModuleCollector, module_file};
    use crate::lang::dsrv::path::ModuleName;
    use TypeName;
    use test_log::test;

    const HEADER: &str = "use experimental::{modules}\n";

    fn name(s: &str) -> ModuleName {
        ModuleName::new(s).expect("a lowercase name")
    }

    fn local(s: &str) -> TypePath {
        TypePath::local(TypeName::new(s).expect("a type name"))
    }

    fn qualified(module: &str, s: &str) -> TypePath {
        TypePath::new(
            EcoVec::from([name(module)]),
            TypeName::new(s).expect("a type name"),
        )
    }

    /// Collect a program from a table, then build every module's namespace.
    fn graph_of(root: &str, sources: &[(&str, &str)]) -> Result<ModuleGraph, DsrvExpandError> {
        let mut collector = ModuleCollector::new(root).expect("a parsable root");
        while let Some(path) = collector.next_request().map(<[ModuleName]>::to_vec) {
            let wanted = show_path(&path);
            let source = sources
                .iter()
                .find(|(name, _)| *name == wanted)
                .unwrap_or_else(|| panic!("no source for {wanted}"))
                .1;
            collector.supply(source).expect("a parsable module");
        }
        build_graph(
            &collector.finish().expect("collected"),
            LanguageRequest::default(),
        )
    }

    fn root_of(graph: &ModuleGraph) -> &Rc<SourceContext> {
        graph.get(&ModulePath::new()).expect("a root namespace")
    }

    #[test]
    fn a_module_keeps_its_own_names_to_itself() {
        let graph = graph_of(
            &format!("{HEADER}mod store\nin x: Int\n"),
            &[("store", &format!("{HEADER}type Colour = Int\n"))],
        )
        .expect("built");
        assert!(
            root_of(&graph).get(&local("Colour")).is_none(),
            "the root did not import it",
        );
    }

    #[test]
    fn a_named_import_brings_one_type() {
        let graph = graph_of(
            &format!("{HEADER}mod store\nuse store::Colour\nin x: Int\n"),
            &[(
                "store",
                &format!("{HEADER}type Colour = Int\ntype Other = Str\n"),
            )],
        )
        .expect("built");
        assert_eq!(
            root_of(&graph).get(&local("Colour")),
            Some(&StreamType::Int)
        );
        assert!(root_of(&graph).get(&local("Other")).is_none());
    }

    #[test]
    fn a_glob_brings_every_type() {
        let graph = graph_of(
            &format!("{HEADER}mod store\nuse store::*\nin x: Int\n"),
            &[(
                "store",
                &format!("{HEADER}type Colour = Int\ntype Other = Str\n"),
            )],
        )
        .expect("built");
        assert_eq!(
            root_of(&graph).get(&local("Colour")),
            Some(&StreamType::Int)
        );
        assert_eq!(root_of(&graph).get(&local("Other")), Some(&StreamType::Str));
    }

    /// `use a` reaches its items as `a::X` rather than bringing them in bare.
    #[test]
    fn importing_a_module_qualifies_its_names() {
        let graph = graph_of(
            &format!("{HEADER}mod store\nuse store\nin x: Int\n"),
            &[("store", &format!("{HEADER}type Colour = Int\n"))],
        )
        .expect("built");
        assert_eq!(
            root_of(&graph).get(&qualified("store", "Colour")),
            Some(&StreamType::Int),
        );
        assert!(root_of(&graph).get(&local("Colour")).is_none());
    }

    /// S11: a declaration in the file displaces a glob import without
    /// complaint.
    #[test]
    fn a_local_declaration_shadows_a_glob_silently() {
        let graph = graph_of(
            &format!("{HEADER}mod store\nuse store::*\ntype Colour = Str\nin x: Int\n"),
            &[("store", &format!("{HEADER}type Colour = Int\n"))],
        )
        .expect("built");
        assert_eq!(
            root_of(&graph).get(&local("Colour")),
            Some(&StreamType::Str)
        );
    }

    #[test]
    fn a_named_import_clashing_with_a_declaration_is_an_error() {
        let error = graph_of(
            &format!("{HEADER}mod store\nuse store::Colour\ntype Colour = Str\nin x: Int\n"),
            &[("store", &format!("{HEADER}type Colour = Int\n"))],
        )
        .expect_err("a clash");
        assert!(
            matches!(&error, DsrvExpandError::Resolve(_)),
            "got {error:?}",
        );
    }

    #[test]
    fn a_module_may_import_from_another_module() {
        let graph = graph_of(
            &format!("{HEADER}mod a\nmod b\nin x: Int\n"),
            &[
                (
                    "a",
                    &format!("{HEADER}use b::Colour\ntype Twice = Colour\n"),
                ),
                ("b", &format!("{HEADER}type Colour = Int\n")),
            ],
        )
        .expect("built");
        let a = graph.get(&vec![name("a")]).expect("a namespace");
        assert_eq!(a.get(&local("Twice")), Some(&StreamType::Int));
    }

    #[test]
    fn modules_importing_each_other_are_refused() {
        let error = graph_of(
            &format!("{HEADER}mod a\nmod b\nin x: Int\n"),
            &[
                ("a", &format!("{HEADER}use b::Colour\ntype T = Int\n")),
                ("b", &format!("{HEADER}use a::T\ntype Colour = Int\n")),
            ],
        )
        .expect_err("a cycle");
        assert!(
            matches!(&error, DsrvExpandError::ModuleCycle { path } if path.contains("->")),
            "got {error:?}",
        );
    }

    #[test]
    fn importing_a_module_that_was_never_declared_is_refused() {
        let error = graph_of(&format!("{HEADER}use absent::Colour\nin x: Int\n"), &[])
            .expect_err("unknown module");
        assert!(
            matches!(&error, DsrvExpandError::UnknownModule { path, .. } if path == "absent"),
            "got {error:?}",
        );
    }

    const UNIONS: &str = "use experimental::{modules, tagged_unions}\n";

    fn tag(s: &str) -> EcoString {
        EcoString::from(s)
    }

    /// S10: importing a tag records the union it builds, so a bare
    /// constructor resolves where nothing else says which union it is.
    #[test]
    fn importing_a_tag_records_the_union_it_builds() {
        let graph = graph_of(
            &format!("{UNIONS}mod store\nuse store::Colour::Red\nin x: Int\n"),
            &[(
                "store",
                &format!("{UNIONS}type Colour = Union<Red: Int, Green>\n"),
            )],
        )
        .expect("built");
        assert_eq!(
            root_of(&graph).constructor_union(&tag("Red")),
            Some(&local("Colour")),
        );
        assert!(
            root_of(&graph).constructor_union(&tag("Green")).is_none(),
            "only the imported tag was recorded",
        );
    }

    #[test]
    fn a_tag_glob_records_every_constructor() {
        let graph = graph_of(
            &format!("{UNIONS}mod store\nuse store::Colour::*\nin x: Int\n"),
            &[(
                "store",
                &format!("{UNIONS}type Colour = Union<Red: Int, Green>\n"),
            )],
        )
        .expect("built");
        assert_eq!(
            root_of(&graph).constructor_union(&tag("Green")),
            Some(&local("Colour")),
        );
    }

    /// A module may import its own tags, which is why constructors are
    /// resolved after the module's aliases rather than alongside them.
    #[test]
    fn a_module_may_import_its_own_tags() {
        let graph = graph_of(
            &format!("{UNIONS}mod store\nin x: Int\n"),
            &[(
                "store",
                &format!(
                    "{UNIONS}type Colour = Union<Red: Int, Green>\nuse self::Colour::{{Red}}\n"
                ),
            )],
        )
        .expect("built");
        let store = graph.get(&vec![name("store")]).expect("a namespace");
        assert_eq!(store.constructor_union(&tag("Red")), Some(&local("Colour")));
    }

    #[test]
    fn importing_a_tag_that_is_not_an_alternative_is_refused() {
        let error = graph_of(
            &format!("{UNIONS}mod store\nuse store::Colour::Blue\nin x: Int\n"),
            &[(
                "store",
                &format!("{UNIONS}type Colour = Union<Red: Int, Green>\n"),
            )],
        )
        .expect_err("an unknown tag");
        assert!(
            matches!(&error, DsrvExpandError::UnknownConstructor { tag, .. } if tag == "Blue"),
            "got {error:?}",
        );
    }

    #[test]
    fn importing_a_tag_of_something_that_is_not_a_union_is_refused() {
        let error = graph_of(
            &format!("{UNIONS}mod store\nuse store::Colour::Red\nin x: Int\n"),
            &[("store", &format!("{UNIONS}type Colour = Int\n"))],
        )
        .expect_err("not a union");
        assert!(
            matches!(&error, DsrvExpandError::UnknownConstructor { .. }),
            "got {error:?}",
        );
    }

    const GENERIC: &str = "use experimental::{modules, generics}\n";

    /// The alias a use site of an imported template resolves to, read back
    /// from the importing module's namespace.
    fn applied(root: &str, sources: &[(&str, &str)]) -> StreamType {
        let graph = graph_of(root, sources).expect("built");
        root_of(&graph)
            .get(&local("Used"))
            .expect("Used resolves")
            .clone()
    }

    #[test]
    fn a_generic_alias_can_be_imported_and_applied() {
        assert_eq!(
            applied(
                &format!(
                    "{GENERIC}mod store\nuse store::Boxed\ntype Used = Boxed<Int>\nin x: Int\n"
                ),
                &[("store", &format!("{GENERIC}type Boxed<A> = List<A>\n"))],
            ),
            StreamType::List(Box::new(StreamType::Int)),
        );
    }

    /// S16: a name private to the exporter is resolved away, so the importer
    /// never needs it in scope.
    #[test]
    fn a_templates_private_names_are_resolved_before_it_travels() {
        let StreamType::Struct(fields, _) = applied(
            &format!("{GENERIC}mod store\nuse store::Boxed\ntype Used = Boxed<Str>\nin x: Int\n"),
            &[(
                "store",
                &format!("{GENERIC}type Helper = Int\ntype Boxed<A> = Struct<v: A, h: Helper>\n"),
            )],
        ) else {
            panic!("expected a struct");
        };
        assert_eq!(
            fields
                .iter()
                .find(|(name, _)| name == "h")
                .map(|(_, ty)| ty),
            Some(&StreamType::Int),
            "Helper was resolved away",
        );
    }

    /// The shape the journal library forces: a template applying another
    /// template to its own free parameters, which cannot be resolved to a
    /// structural type and so must be inlined.
    #[test]
    fn a_template_may_apply_another_to_its_own_parameters() {
        let StreamType::Struct(fields, _) = applied(
            &format!("{GENERIC}mod store\nuse store::Outer\ntype Used = Outer<Int>\nin x: Int\n"),
            &[(
                "store",
                &format!(
                    "{GENERIC}type Inner<B> = List<B>\ntype Outer<A> = Struct<got: Inner<A>>\n"
                ),
            )],
        ) else {
            panic!("expected a struct");
        };
        assert_eq!(
            fields
                .iter()
                .find(|(name, _)| name == "got")
                .map(|(_, ty)| ty),
            Some(&StreamType::List(Box::new(StreamType::Int))),
        );
    }

    #[test]
    fn a_glob_brings_templates_too() {
        assert_eq!(
            applied(
                &format!("{GENERIC}mod store\nuse store::*\ntype Used = Boxed<Int>\nin x: Int\n"),
                &[("store", &format!("{GENERIC}type Boxed<A> = List<A>\n"))],
            ),
            StreamType::List(Box::new(StreamType::Int)),
        );
    }

    #[test]
    fn a_module_import_qualifies_a_template_too() {
        let graph = graph_of(
            &format!("{GENERIC}mod store\nuse store\ntype Used = store::Boxed<Int>\nin x: Int\n"),
            &[("store", &format!("{GENERIC}type Boxed<A> = List<A>\n"))],
        )
        .expect("built");
        assert_eq!(
            root_of(&graph).get(&local("Used")),
            Some(&StreamType::List(Box::new(StreamType::Int))),
        );
    }

    /// S12: an internal name is usable inside its module and invisible to
    /// every importer.
    #[test]
    fn an_internal_name_is_not_brought_in_by_a_glob() {
        let graph = graph_of(
            &format!("{HEADER}mod store\nuse store::*\nin x: Int\n"),
            &[(
                "store",
                &format!("{HEADER}internal type Hidden = Int\ntype Shown = Str\n"),
            )],
        )
        .expect("built");
        assert_eq!(root_of(&graph).get(&local("Shown")), Some(&StreamType::Str));
        assert!(root_of(&graph).get(&local("Hidden")).is_none());
    }

    #[test]
    fn naming_an_internal_type_in_an_import_is_refused() {
        let error = graph_of(
            &format!("{HEADER}mod store\nuse store::Hidden\nin x: Int\n"),
            &[("store", &format!("{HEADER}internal type Hidden = Int\n"))],
        )
        .expect_err("an internal import");
        assert!(
            matches!(&error, DsrvExpandError::InternalImport { name } if name.contains("Hidden")),
            "got {error:?}",
        );
    }

    #[test]
    fn an_internal_name_is_still_usable_inside_its_own_module() {
        let graph = graph_of(
            &format!("{HEADER}mod store\nin x: Int\n"),
            &[(
                "store",
                &format!("{HEADER}internal type Hidden = Int\ntype Shown = Hidden\n"),
            )],
        )
        .expect("built");
        let store = graph.get(&vec![name("store")]).expect("a namespace");
        assert_eq!(store.get(&local("Shown")), Some(&StreamType::Int));
    }

    /// The reason S16 resolves a template rather than copying it: an
    /// exported template may rest on an internal name, which the importer
    /// must never need.
    #[test]
    fn an_exported_template_may_rest_on_an_internal_name() {
        let graph = graph_of(
            &format!("{GENERIC}mod store\nuse store::Boxed\ntype Used = Boxed<Str>\nin x: Int\n"),
            &[(
                "store",
                &format!(
                    "{GENERIC}internal type Hidden = Int\ntype Boxed<A> = Struct<v: A, h: Hidden>\n"
                ),
            )],
        )
        .expect("built");
        let StreamType::Struct(fields, _) = root_of(&graph).get(&local("Used")).expect("resolves")
        else {
            panic!("expected a struct");
        };
        assert_eq!(
            fields
                .iter()
                .find(|(name, _)| name == "h")
                .map(|(_, ty)| ty),
            Some(&StreamType::Int),
        );
        assert!(root_of(&graph).get(&local("Hidden")).is_none());
    }

    #[test]
    fn a_module_file_is_named_after_its_path() {
        assert_eq!(module_file(&[name("a"), name("b")]), "a/b.dsrv");
    }
}

/// A template with every non-parameter name resolved in the module that
/// declared it, so it can be copied into another namespace (S16).
///
/// A parameter stays free. An application of another of the exporter's
/// templates is inlined, because it cannot be resolved to a structural type
/// while its arguments are free.
fn resolved_template(
    ty: &SourceType,
    parameters: &[TypeName],
    exporter: &SourceContext,
    active: &mut Vec<TypePath>,
) -> Result<SourceType, DsrvExpandError> {
    use crate::lang::dsrv::source::SourceTypeKind as Kind;

    let recur = |ty: &SourceType, active: &mut Vec<TypePath>| {
        resolved_template(ty, parameters, exporter, active)
    };
    let kind = match &ty.kind {
        Kind::Named(path, arguments) if arguments.is_empty() => {
            // A parameter of this template stays as written.
            if !path.is_qualified() && parameters.contains(path.name()) {
                Kind::Named(path.clone(), EcoVec::new())
            } else if let Some(resolved) = exporter.get(path) {
                return Ok(SourceType::from(resolved.clone()));
            } else {
                return Err(DsrvExpandError::UnknownExport {
                    name: path.to_string(),
                });
            }
        }
        Kind::Named(path, arguments) => {
            // Applying one of the exporter's templates: inline it, since the
            // arguments may themselves be free parameters.
            let Some(declaration) = exporter.generic().get(path) else {
                return Err(DsrvExpandError::UnknownExport {
                    name: path.to_string(),
                });
            };
            if active.contains(path) {
                return Err(DsrvExpandError::ModuleCycle {
                    path: format!("{path} is defined in terms of itself"),
                });
            }
            let arguments = arguments
                .iter()
                .map(|argument| recur(argument, active))
                .collect::<Result<Vec<_>, _>>()?;
            if declaration.parameters.len() != arguments.len() {
                return Err(DsrvExpandError::UnknownExport {
                    name: format!(
                        "{path} takes {} type arguments, given {}",
                        declaration.parameters.len(),
                        arguments.len()
                    ),
                });
            }
            let bound: BTreeMap<&TypeName, &SourceType> = declaration
                .parameters
                .iter()
                .zip(arguments.iter())
                .collect();
            let inlined = substitute_source(&declaration.ty, &bound);
            active.push(path.clone());
            let resolved = recur(&inlined, active)?;
            active.pop();
            return Ok(resolved);
        }
        Kind::List(inner) => Kind::List(Box::new(recur(inner, active)?)),
        Kind::Map(inner) => Kind::Map(Box::new(recur(inner, active)?)),
        Kind::Expr(inner) => Kind::Expr(Box::new(recur(inner, active)?)),
        Kind::Tuple(types) => Kind::Tuple(
            types
                .iter()
                .map(|inner| recur(inner, active))
                .collect::<Result<_, _>>()?,
        ),
        Kind::Struct(fields, open) => Kind::Struct(
            fields
                .iter()
                .map(|(field, inner)| Ok((field.clone(), recur(inner, active)?)))
                .collect::<Result<_, DsrvExpandError>>()?,
            *open,
        ),
        Kind::Function(arguments, result) => Kind::Function(
            arguments
                .iter()
                .map(|inner| recur(inner, active))
                .collect::<Result<_, _>>()?,
            Box::new(recur(result, active)?),
        ),
        Kind::Union(alternatives) => Kind::Union(
            alternatives
                .iter()
                .map(|alternative| {
                    Ok(SourceAlternative {
                        tag: alternative.tag.clone(),
                        payload: alternative
                            .payload
                            .as_ref()
                            .map(|inner| recur(inner, active))
                            .transpose()?,
                        span: alternative.span,
                    })
                })
                .collect::<Result<_, DsrvExpandError>>()?,
        ),
        other => other.clone(),
    };
    Ok(SourceType {
        kind,
        span: ty.span,
    })
}

#[cfg(test)]
mod function_tests {
    use super::*;

    use crate::lang::dsrv::expand::functions::build_function_table;
    use crate::lang::dsrv::modules::ModuleCollector;
    use crate::lang::dsrv::path::ModuleName;
    use test_log::test;

    const F: &str = "use experimental::{modules, functions}\n";

    fn table_of(root: &str, sources: &[(&str, &str)]) -> Result<(), DsrvExpandError> {
        let mut collector = ModuleCollector::new(root).expect("a parsable root");
        while let Some(path) = collector.next_request().map(<[ModuleName]>::to_vec) {
            let wanted = show_path(&path);
            let source = sources
                .iter()
                .find(|(name, _)| *name == wanted)
                .unwrap_or_else(|| panic!("no source for {wanted}"))
                .1;
            collector.supply(source).expect("a parsable module");
        }
        let sources = collector.finish().expect("collected");
        let graph = build_graph(&sources, LanguageRequest::default())?;
        let constants = Rc::new(crate::lang::dsrv::expand::constants::build_constant_table(
            &sources, &graph,
        )?);
        build_function_table(&sources, &graph, &constants, Default::default()).map(|_| ())
    }

    #[test]
    fn a_modules_defs_reach_the_table() {
        table_of(
            &format!("{F}mod store\nin x: Int\n"),
            &[("store", &format!("{F}def twice(n: Int) -> Int = n * 2\n"))],
        )
        .expect("built");
    }

    /// A def may call one from a module it imports, and the body stored for
    /// it is already inlined.
    #[test]
    fn a_def_may_call_an_imported_def() {
        table_of(
            &format!("{F}mod a\nmod b\nin x: Int\n"),
            &[
                (
                    "a",
                    &format!("{F}use b::*\ndef quad(n: Int) -> Int = twice(twice(n))\n"),
                ),
                ("b", &format!("{F}def twice(n: Int) -> Int = n * 2\n")),
            ],
        )
        .expect("built");
    }

    #[test]
    fn a_def_may_be_called_through_its_module() {
        table_of(
            &format!("{F}mod a\nmod b\nin x: Int\n"),
            &[
                (
                    "a",
                    &format!("{F}use b\ndef quad(n: Int) -> Int = b::twice(b::twice(n))\n"),
                ),
                ("b", &format!("{F}def twice(n: Int) -> Int = n * 2\n")),
            ],
        )
        .expect("built");
    }

    /// S12 for functions: a glob skips an internal def silently, exactly as
    /// it skips an internal type. The call is left alone and fails later as
    /// an unknown name.
    #[test]
    fn a_glob_does_not_bring_an_internal_def() {
        table_of(
            &format!("{F}mod a\nmod b\nin x: Int\n"),
            &[
                ("a", &format!("{F}use b::*\ndef quad(n: Int) -> Int = n\n")),
                (
                    "b",
                    &format!("{F}internal def twice(n: Int) -> Int = n * 2\n"),
                ),
            ],
        )
        .expect("a glob simply skips it");
    }

    /// Naming it outright is the case that must be refused, and the message
    /// names the function.
    #[test]
    fn calling_an_internal_def_through_its_module_is_refused() {
        let error = table_of(
            &format!("{F}mod a\nmod b\nin x: Int\n"),
            &[
                (
                    "a",
                    &format!("{F}use b\ndef quad(n: Int) -> Int = b::twice(n)\n"),
                ),
                (
                    "b",
                    &format!("{F}internal def twice(n: Int) -> Int = n * 2\n"),
                ),
            ],
        )
        .expect_err("internal stays home");
        assert!(
            matches!(&error, DsrvExpandError::UnknownFunction { name } if name == "b::twice"),
            "got {error:?}",
        );
    }
}
