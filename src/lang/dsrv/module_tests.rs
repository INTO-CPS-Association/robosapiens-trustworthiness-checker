//! Module paths: where a qualified name's module part ends.
//!
//! Parsing resolves nothing, so the boundary is fixed by spelling alone: the
//! leading lowercase segments are the module, and the capitalised one is the
//! type whose tag follows. These tests pin that split and the experiment that
//! gates reaching outside the file at all; what a module path then *means* is
//! resolution's work, which does not exist yet.

use crate::VarName;
use crate::lang::dsrv::LanguageError;
use crate::lang::dsrv::ast::{DsrvSpecification, ExprView};
use crate::lang::dsrv::parser::{DsrvParseError, parse_str};
use crate::lang::dsrv::path::{ModuleName, TypePath};
use crate::lang::dsrv::runtime_expression::RuntimeExpressionSite;

use test_log::test;

const HEADER: &str = "use experimental::{tagged_unions, modules}\n";

fn parse(source: &str) -> DsrvSpecification {
    let source = format!("{HEADER}{source}");
    parse_str(&source).unwrap_or_else(|error| panic!("{source}: {error}"))
}

/// The qualifier `y`'s constructor was written with, as it prints.
fn qualifier_of(source: &str) -> Option<String> {
    let spec = parse(source);
    let expression = spec
        .var_expr_ref(&VarName::from("y"))
        .expect("y is defined");
    let ExprView::Constructor(_, _, qualifier) = expression.view() else {
        panic!("expected a constructor, got {expression}");
    };
    qualifier.as_ref().map(TypePath::to_string)
}

/// Expand a program of several files, supplying each module from `sources`
/// as the collector asks for it.
fn program(root: &str, sources: &[(&str, &str)]) -> DsrvSpecification {
    use crate::lang::dsrv::modules::{ModuleCollector, show_path};
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
    crate::lang::dsrv::expand::expand_program(
        collector.finish().expect("collected"),
        crate::lang::dsrv::expand::language::LanguageRequest::default(),
    )
    .expect("expands")
}

/// The untyped site of `var`'s expression in an unchecked specification:
/// what source supplied to that node would be parsed and expanded against.
fn runtime_expression_at(spec: &DsrvSpecification, var: &str) -> RuntimeExpressionSite {
    RuntimeExpressionSite::unlocated(
        spec.var_expr_ref(&VarName::from(var))
            .expect("the variable is defined"),
    )
}

fn language_error(source: &str) -> LanguageError {
    match parse_str(source) {
        Err(DsrvParseError::Language(error)) => error,
        other => panic!("expected a language error for {source:?}, got {other:?}"),
    }
}

const BODY: &str = "in x: Int\nout y: Any\n";

#[test]
fn a_local_qualifier_still_has_no_module() {
    assert_eq!(
        qualifier_of(&format!("{BODY}y = State::Moving(x)\n")),
        Some("State".to_owned()),
    );
}

#[test]
fn one_lowercase_segment_is_a_module() {
    assert_eq!(
        qualifier_of(&format!("{BODY}y = option::Option::Some(x)\n")),
        Some("option::Option".to_owned()),
    );
}

#[test]
fn every_leading_lowercase_segment_joins_the_module_path() {
    assert_eq!(
        qualifier_of(&format!("{BODY}y = lib::opt::Option::Some(x)\n")),
        Some("lib::opt::Option".to_owned()),
    );
}

#[test]
fn a_nullary_constructor_may_also_be_reached_through_a_module() {
    assert_eq!(
        qualifier_of(&format!("{BODY}y = lib::opt::Option::None\n")),
        Some("lib::opt::Option".to_owned()),
    );
}

/// The capitalised segment ends the module path, so what follows it is the
/// tag rather than another module.
#[test]
fn the_capitalised_segment_ends_the_module_path() {
    let spec = parse(&format!("{BODY}y = lib::inner::Colour::Red\n"));
    let expression = spec
        .var_expr_ref(&VarName::from("y"))
        .expect("y is defined");
    let ExprView::Constructor(_, tag, qualifier) = expression.view() else {
        panic!("expected a constructor");
    };
    assert_eq!(tag, "Red");
    let qualifier = qualifier.as_ref().expect("a qualifier");
    assert_eq!(
        qualifier
            .module()
            .iter()
            .map(|segment| segment.to_string())
            .collect::<Vec<_>>(),
        ["lib", "inner"],
    );
    assert_eq!(qualifier.name().as_str(), "Colour");
}

#[test]
fn a_constructor_prints_the_path_it_was_written_with() {
    let spec = parse(&format!("{BODY}y = lib::opt::Option::Some(x)\n"));
    let printed = spec
        .var_expr_ref(&VarName::from("y"))
        .expect("y is defined")
        .to_string();
    assert!(
        printed.contains("lib::opt::Option::Some"),
        "printed as {printed}",
    );
}

#[test]
fn reaching_through_a_module_needs_the_experiment() {
    let error = language_error(&format!(
        "use experimental::{{tagged_unions}}\n{BODY}y = lib::opt::Option::Some(x)\n"
    ));
    assert!(
        matches!(
            &error,
            LanguageError::NeedsExperiment {
                feature: "modules",
                ..
            }
        ),
        "got {error:?}",
    );
}

#[test]
fn a_local_qualifier_does_not_need_the_modules_experiment() {
    let source = format!("use experimental::{{tagged_unions}}\n{BODY}y = State::Moving(x)\n");
    parse_str(&source).expect("a local qualifier needs only tagged_unions");
}

// ---------------------------------------------------------------------------
// `use`: the path and the tree that follows it
// ---------------------------------------------------------------------------

use crate::lang::dsrv::path::{ImportKind, PathSegment, UseTree};
use crate::lang::dsrv::syntax::{ParsedDeclaration, parse_specification};

/// Every `use` in a file, as it was written.
fn imports(source: &str) -> Vec<UseTree> {
    let parsed = parse_specification(source).unwrap_or_else(|error| panic!("{source}: {error}"));
    let (_, declarations, _) = parsed.into_parts();
    declarations
        .iter()
        .filter_map(|declaration| match declaration {
            ParsedDeclaration::Use { tree, .. } => Some(tree.clone()),
            _ => None,
        })
        .collect()
}

/// The sole `use` in a file, printed back.
fn import(source: &str) -> String {
    let found = imports(source);
    assert_eq!(found.len(), 1, "expected one use in {source:?}");
    found[0].to_string()
}

#[test]
fn a_grouped_import_keeps_its_path_and_items() {
    assert_eq!(
        import("use lib::inner::{Colour, Label}\nin x: Int\n"),
        "lib::inner::{Colour, Label}",
    );
}

#[test]
fn self_in_a_group_is_a_path_segment_like_any_other() {
    assert_eq!(
        import("use lib::other::{self, Mode, Kind}\nin x: Int\n"),
        "lib::other::{self, Mode, Kind}",
    );
}

#[test]
fn a_group_may_hang_off_a_capitalised_segment() {
    assert_eq!(
        import("use lib::opt::Option::{Some, None}\nin x: Int\n"),
        "lib::opt::Option::{Some, None}",
    );
}

#[test]
fn a_glob_imports_the_whole_module() {
    assert_eq!(import("use store::*\nin x: Int\n"), "store::*");
}

#[test]
fn a_path_with_no_group_imports_one_item() {
    assert_eq!(import("use lib::inner\nin x: Int\n"), "lib::inner");
}

#[test]
fn groups_nest() {
    assert_eq!(
        import("use lib::{inner::{Colour}, other::*}\nin x: Int\n"),
        "lib::{inner::{Colour}, other::*}",
    );
}

#[test]
fn a_segment_is_classified_by_its_case() {
    let found = imports("use lib::opt::Option::{Some}\nin x: Int\n");
    let path = found[0].path();
    assert!(matches!(path[0], PathSegment::Module(_)), "std is a module");
    assert!(
        matches!(path[1], PathSegment::Module(_)),
        "option is a module"
    );
    assert!(matches!(path[2], PathSegment::Name(_)), "Option is a name");
    let ImportKind::Group(items) = found[0].kind() else {
        panic!("expected a group");
    };
    assert!(
        matches!(items[0].path()[0], PathSegment::Name(_)),
        "Some is a name"
    );
}

#[test]
fn self_is_recognised_as_a_segment_not_a_module_called_self() {
    let found = imports("use self::Option::{Some}\nin x: Int\n");
    assert!(matches!(found[0].path()[0], PathSegment::Zelf));
}

/// S9: an item import populates a namespace rather than configuring the file,
/// so it may follow a declaration. `lib/std/option.dsrv` writes exactly this.
#[test]
fn an_item_import_may_follow_a_declaration() {
    let source = format!(
        "{HEADER}type Option = Union<Some: Int, None>\nuse self::Option::{{Some, None}}\nin x: Int\n"
    );
    parse_str(&source).unwrap_or_else(|error| panic!("{source}: {error}"));
}

#[test]
fn a_late_use_experimental_is_still_an_error() {
    let error = language_error("in x: Int\nuse experimental::{tagged_unions}\n");
    assert!(
        matches!(
            &error,
            LanguageError::HeaderAfterDeclaration {
                keyword: "use experimental",
                ..
            }
        ),
        "got {error:?}",
    );
}

#[test]
fn a_late_edition_is_still_an_error_even_after_an_item_import() {
    let source = format!("{HEADER}use lib::inner\nedition 2026-09\nin x: Int\n");
    match parse_str(&source) {
        Err(DsrvParseError::Language(LanguageError::HeaderAfterDeclaration {
            keyword: "edition",
            ..
        })) => {}
        other => panic!("expected a late-edition error, got {other:?}"),
    }
}

#[test]
fn importing_an_item_needs_the_modules_experiment() {
    let error = language_error("use experimental::{tagged_unions}\nuse lib::inner\nin x: Int\n");
    assert!(
        matches!(
            &error,
            LanguageError::NeedsExperiment {
                feature: "modules",
                ..
            }
        ),
        "got {error:?}",
    );
}

#[test]
fn use_experimental_still_enables_features_by_name_and_by_glob() {
    parse_str("use experimental::{tagged_unions, modules}\nin x: Int\n").expect("named");
    parse_str("use experimental::*\nin x: Int\n").expect("glob");
}

#[test]
fn an_unknown_experiment_is_still_named_in_the_error() {
    let error = language_error("use experimental::{teleporting}\nin x: Int\n");
    assert!(
        matches!(&error, LanguageError::UnknownFeature { name, .. } if name == "teleporting"),
        "got {error:?}",
    );
}

/// An experiment may be named without braces. The first segment decides that
/// a line is the header form, so this is not read as an item import.
#[test]
fn an_experiment_may_be_named_without_a_group() {
    parse_str("use experimental::tagged_unions\nin x: Int\nout y: Any\ny = State::Moving(x)\n")
        .expect("unbraced experiment");
}

#[test]
fn an_unbraced_unknown_experiment_is_still_named() {
    let error = language_error("use experimental::teleporting\nin x: Int\n");
    assert!(
        matches!(&error, LanguageError::UnknownFeature { name, .. } if name == "teleporting"),
        "got {error:?}",
    );
}

#[test]
fn use_experimental_with_nothing_after_it_is_not_an_experiment() {
    let error = language_error("use experimental\nin x: Int\n");
    assert!(
        matches!(&error, LanguageError::UnknownFeature { .. }),
        "got {error:?}",
    );
}

// ---------------------------------------------------------------------------
// `mod`: naming a submodule
// ---------------------------------------------------------------------------

/// Every `mod` path in a file, each printed as it was written.
fn modules(source: &str) -> Vec<String> {
    let parsed = parse_specification(source).unwrap_or_else(|error| panic!("{source}: {error}"));
    let (_, declarations, _) = parsed.into_parts();
    declarations
        .iter()
        .filter_map(|declaration| match declaration {
            ParsedDeclaration::Mod { path, .. } => Some(
                path.iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("::"),
            ),
            _ => None,
        })
        .collect()
}

#[test]
fn a_module_is_declared_by_name() {
    assert_eq!(modules("mod store\nin x: Int\n"), ["store"]);
}

#[test]
fn a_module_declaration_may_nest() {
    assert_eq!(modules("mod lib::inner\nin x: Int\n"), ["lib::inner"]);
}

#[test]
fn several_modules_may_be_declared() {
    assert_eq!(
        modules("mod store\nmod lib::inner\nin x: Int\n"),
        ["store", "lib::inner"],
    );
}

#[test]
fn a_declared_module_must_be_lowercase() {
    assert!(
        parse_specification("mod Second\nin x: Int\n").is_err(),
        "a capitalised name is a type, not a module",
    );
}

#[test]
fn self_does_not_name_a_module_to_declare() {
    assert!(parse_specification("mod self\nin x: Int\n").is_err());
}

#[test]
fn declaring_a_module_needs_the_experiment() {
    let error = language_error("use experimental::{tagged_unions}\nmod store\nin x: Int\n");
    assert!(
        matches!(
            &error,
            LanguageError::NeedsExperiment {
                feature: "modules",
                ..
            }
        ),
        "got {error:?}",
    );
}

/// S6: `mod` pulls a file into the program, so unlike an item import it stays
/// in the header region.
#[test]
fn a_module_declaration_may_not_follow_a_declaration() {
    let error = language_error(&format!("{HEADER}in x: Int\nmod store\n"));
    assert!(
        matches!(
            &error,
            LanguageError::HeaderAfterDeclaration { keyword: "mod", .. }
        ),
        "got {error:?}",
    );
}

#[test]
fn a_module_declaration_may_precede_an_item_import() {
    let source = format!("{HEADER}mod store\nuse store::*\nin x: Int\n");
    parse_str(&source).unwrap_or_else(|error| panic!("{source}: {error}"));
}

// ---------------------------------------------------------------------------
// Qualified names in type position
// ---------------------------------------------------------------------------

use crate::lang::dsrv::source::SourceResolveError;

fn resolve_error(source: &str) -> SourceResolveError {
    let source = format!("{HEADER}{source}");
    match parse_str(&source) {
        Err(DsrvParseError::Resolve(error)) => error,
        other => panic!("expected a resolution error for {source:?}, got {other:?}"),
    }
}

/// One segment is the type itself whatever its case, so an alias written in
/// lowercase keeps working and is not read as a module.
#[test]
fn one_segment_in_type_position_is_a_local_name() {
    let spec = parse("type count = Int\nin x: count\n");
    assert_eq!(
        spec.type_annotations().get(&VarName::from("x")),
        Some(&crate::core::StreamType::Int),
    );
}

#[test]
fn a_type_may_be_named_through_a_module() {
    // It cannot resolve until imports populate the namespace, but it parses
    // and the failure names the whole path rather than one segment.
    let error = resolve_error("in x: lib::inner::Colour\n");
    assert!(
        matches!(&error, SourceResolveError::UnknownAlias { name, .. }
            if name.to_string() == "lib::inner::Colour"),
        "got {error:?}",
    );
}

#[test]
fn a_qualified_type_may_take_generic_arguments() {
    let source = "use experimental::{tagged_unions, modules, generics}\n                  in x: lib::inner::Boxed<Int>\n";
    let error = match parse_str(source) {
        Err(DsrvParseError::Resolve(error)) => error,
        other => panic!("expected a resolution error, got {other:?}"),
    };
    assert!(
        matches!(&error, SourceResolveError::UnknownAlias { name, .. }
            if name.to_string() == "lib::inner::Boxed"),
        "got {error:?}",
    );
}

#[test]
fn a_qualified_type_name_needs_the_modules_experiment() {
    let error = language_error("use experimental::{tagged_unions}\nin x: lib::inner::Colour\n");
    assert!(
        matches!(
            &error,
            LanguageError::NeedsExperiment {
                feature: "modules",
                ..
            }
        ),
        "got {error:?}",
    );
}

#[test]
fn an_internal_alias_needs_the_modules_experiment() {
    let error = language_error(
        "use experimental::{tagged_unions}\ninternal type Hidden = Int\nin x: Int\n",
    );
    assert!(
        matches!(
            &error,
            LanguageError::NeedsExperiment {
                feature: "modules",
                ..
            }
        ),
        "got {error:?}",
    );
}

#[test]
fn an_internal_alias_is_an_ordinary_type_in_its_own_file() {
    let spec = parse("internal type Hidden = Int\nin x: Hidden\n");
    assert_eq!(
        spec.type_annotations().get(&VarName::from("x")),
        Some(&crate::core::StreamType::Int),
    );
}

// ---------------------------------------------------------------------------
// `def`: pure functions
// ---------------------------------------------------------------------------

const FUNCTIONS: &str = "use experimental::{functions}\n";

/// Every `def` the library writes, in the four shapes it uses.
#[test]
fn a_def_parses_in_every_shape_the_library_uses() {
    for source in [
        format!("{FUNCTIONS}def twice(n: Int) -> Int = n * 2\nin x: Int\n"),
        format!("use experimental::{{functions, generics}}\ndef id<T>(v: T) -> T = v\nin x: Int\n"),
        format!("{FUNCTIONS}def ap(f: (Int -> Bool), n: Int) -> Bool = f(n)\nin x: Int\n"),
        format!(
            "use experimental::{{functions, modules}}\ninternal def hid(n: Int) -> Int = n\nin x: Int\n"
        ),
    ] {
        parse_specification(&source).unwrap_or_else(|error| panic!("{source}: {error}"));
    }
}

/// A def owns a parsed root. If expansion did not consume it in declaration
/// order, every later equation would silently take the wrong tree — which
/// type-checks and gives wrong answers.
#[test]
fn an_equation_after_a_def_still_takes_its_own_expression() {
    let spec = parse_str(&format!(
        "{FUNCTIONS}def twice(n: Int) -> Int = n * 2\nin x: Int\nout y: Int\ny = x + 1\n"
    ))
    .expect("parses");
    let printed = spec
        .var_expr_ref(&VarName::from("y"))
        .expect("y is defined")
        .to_string();
    assert!(printed.contains('+'), "y kept its own body, got {printed}");
}

#[test]
fn a_def_needs_the_functions_experiment() {
    let error = language_error("def twice(n: Int) -> Int = n * 2\nin x: Int\n");
    assert!(
        matches!(
            &error,
            LanguageError::NeedsExperiment {
                feature: "functions",
                ..
            }
        ),
        "got {error:?}",
    );
}

#[test]
fn an_internal_def_needs_the_modules_experiment_too() {
    let error = language_error(&format!(
        "{FUNCTIONS}internal def hid(n: Int) -> Int = n\nin x: Int\n"
    ));
    assert!(
        matches!(
            &error,
            LanguageError::NeedsExperiment {
                feature: "modules",
                ..
            }
        ),
        "got {error:?}",
    );
}

/// Text supplied at runtime is expanded where the node stands, so it may
/// call the defs that node's file could. The body is inlined into the text
/// exactly as it is into an equation.
#[test]
fn text_supplied_at_runtime_may_call_a_def() {
    let spec = parse_str(&format!(
        "{FUNCTIONS}def twice(n: Int) -> Int = n * 2\nin x: Int\nout y: Int\ny = dynamic(\"1\": Int)\n"
    ))
    .expect("parses");
    let expr = runtime_expression_at(&spec, "y")
        .parse("twice(x)")
        .expect("the text may call twice");
    let printed = expr.to_string();
    assert!(
        printed.contains('*') && !printed.contains("twice"),
        "the call was inlined, got {printed}",
    );
}

/// A file that declares no def leaves its nodes carrying nothing, so a call
/// in its runtime expression source names a variable, as it did before defs existed.
#[test]
fn text_supplied_at_runtime_calls_nothing_where_no_def_was_declared() {
    let spec = parse_str("in x: Int\nout y: Int\ny = dynamic(\"1\": Int)\n").expect("parses");
    let expr = runtime_expression_at(&spec, "y")
        .parse("twice(x)")
        .expect("parses");
    assert!(
        expr.to_string().contains("twice"),
        "the name was left alone, got {expr}",
    );
}

/// What runtime expression source may call is part of the node, not of the program: a
/// def declared after the node is still one the node's file declared.
#[test]
fn text_supplied_at_runtime_may_call_a_def_declared_after_the_node() {
    let spec = parse_str(&format!(
        "{FUNCTIONS}in x: Int\nout y: Int\ny = dynamic(\"1\": Int)\ndef twice(n: Int) -> Int = n * 2\n"
    ))
    .expect("parses");
    let expr = runtime_expression_at(&spec, "y")
        .parse("twice(x)")
        .expect("the text may call twice");
    assert!(expr.to_string().contains('*'), "got {expr}");
}

/// Runtime expression source a def's own body supplies is expanded the same way, so a
/// def may be called from inside text nested in a call to another.
#[test]
fn source_nested_in_a_runtime_expression_may_call_a_def_in_turn() {
    let spec = parse_str(&format!(
        "{FUNCTIONS}def twice(n: Int) -> Int = n * 2\nin x: Int\nout y: Int\ny = dynamic(\"1\": Int)\n"
    ))
    .expect("parses");
    let outer = runtime_expression_at(&spec, "y")
        .parse("dynamic(\"1\": Int)")
        .expect("the text may name another dynamic");
    let inner = RuntimeExpressionSite::unlocated(outer.as_ref());
    let expr = inner
        .parse("twice(x)")
        .expect("the nested text may call it");
    assert!(expr.to_string().contains('*'), "got {expr}");
}

/// The runtime expression source of a node in a file that took defs from elsewhere may
/// call them too, by the same names its own expressions could.
#[test]
fn text_supplied_at_runtime_may_call_an_imported_def() {
    let spec = program(
        "use experimental::{modules, functions}\nmod store\nuse store::*\nin x: Int\nout y: Int\ny = dynamic(\"1\": Int)\n",
        &[(
            "store",
            "use experimental::{modules, functions}\ndef twice(n: Int) -> Int = n * 2\n",
        )],
    );
    for text in ["twice(x)", "store::twice(x)"] {
        let expr = runtime_expression_at(&spec, "y")
            .parse(text)
            .unwrap_or_else(|error| panic!("{text}: {error}"));
        let printed = expr.to_string();
        assert!(
            printed.contains('*') && !printed.contains("twice"),
            "{text} was inlined, got {printed}",
        );
    }
}

/// What a module keeps to itself stays hidden from the text supplied to
/// another module's node, as it is from that module's own expressions (S12).
#[test]
fn text_supplied_at_runtime_cannot_call_an_internal_def() {
    let spec = program(
        "use experimental::{modules, functions}\nmod store\nuse store::*\nin x: Int\nout y: Int\ny = dynamic(\"1\": Int)\n",
        &[(
            "store",
            "use experimental::{modules, functions}\ninternal def twice(n: Int) -> Int = n * 2\n",
        )],
    );
    let expr = runtime_expression_at(&spec, "y")
        .parse("twice(x)")
        .expect("parses");
    assert!(
        expr.to_string().contains("twice"),
        "the name was left alone, got {expr}",
    );
}

// ---------------------------------------------------------------------------
// A value named through a module
// ---------------------------------------------------------------------------

/// Restricting the constructor qualifier to capitalised names left a
/// lowercase-qualified call unparseable. It parses again, as a module item.
#[test]
fn a_value_may_be_named_through_a_module() {
    let source = format!(
        "use experimental::{{modules, functions}}\nin x: Int\nout y: Int\ny = store::twice(x)\n"
    );
    parse_specification(&source).unwrap_or_else(|error| panic!("{source}: {error}"));
}

#[test]
fn a_module_item_and_a_constructor_qualifier_are_told_apart() {
    // `a::b` is an item; `a::B::C` keeps the qualifier list going.
    for source in [
        "use experimental::{modules, functions}\nin x: Int\nout y: Int\ny = lib::inner::twice(x)\n",
        "use experimental::{modules, tagged_unions}\nin x: Int\nout y: Any\ny = lib::inner::Colour::Red\n",
    ] {
        parse_specification(source).unwrap_or_else(|error| panic!("{source}: {error}"));
    }
}

/// Until a def is inlined, a qualified value names nothing; the message says
/// so rather than reporting an unknown variable.
#[test]
fn a_module_item_naming_no_function_is_refused() {
    let error = resolve_error("in x: Int\nout y: Int\ny = store::twice(x)\n");
    assert!(
        matches!(&error, SourceResolveError::UnknownModuleItem { name, .. }
            if name == "store::twice"),
        "got {error:?}",
    );
}

#[test]
fn naming_a_value_through_a_module_needs_the_experiment() {
    let error = language_error(
        "use experimental::{functions}\nin x: Int\nout y: Int\ny = store::twice(x)\n",
    );
    assert!(
        matches!(
            &error,
            LanguageError::NeedsExperiment {
                feature: "modules",
                ..
            }
        ),
        "got {error:?}",
    );
}

// ---------------------------------------------------------------------------
// Inlining a call to a def
// ---------------------------------------------------------------------------

/// A call becomes an immediate lambda application, so the body appears at
/// the call site rather than the name.
#[test]
fn a_call_to_a_def_is_inlined() {
    let spec = parse_str(&format!(
        "{FUNCTIONS}def twice(n: Int) -> Int = n * 2\nin x: Int\nout y: Int\ny = twice(x)\n"
    ))
    .expect("parses");
    let printed = spec
        .var_expr_ref(&VarName::from("y"))
        .expect("y is defined")
        .to_string();
    assert!(
        printed.contains('*') && !printed.contains("twice"),
        "expected the body, got {printed}",
    );
}

#[test]
fn a_def_may_be_called_more_than_once() {
    let spec = parse_str(&format!(
        "{FUNCTIONS}def twice(n: Int) -> Int = n * 2\nin x: Int\nout y: Int\ny = twice(x) + twice(x)\n"
    ))
    .expect("parses");
    let printed = spec
        .var_expr_ref(&VarName::from("y"))
        .expect("y is defined")
        .to_string();
    assert_eq!(
        printed.matches('*').count(),
        2,
        "one body per call: {printed}"
    );
}

#[test]
fn a_def_may_call_another_def() {
    let spec = parse_str(&format!(
        "{FUNCTIONS}def twice(n: Int) -> Int = n * 2\ndef quad(n: Int) -> Int = twice(twice(n))\nin x: Int\nout y: Int\ny = quad(x)\n"
    ))
    .expect("parses");
    let printed = spec
        .var_expr_ref(&VarName::from("y"))
        .expect("y is defined")
        .to_string();
    assert!(
        !printed.contains("quad") && !printed.contains("twice"),
        "got {printed}"
    );
}

#[test]
fn a_def_defined_in_terms_of_itself_is_refused() {
    let source = format!(
        "{FUNCTIONS}def loop(n: Int) -> Int = loop(n)\nin x: Int\nout y: Int\ny = loop(x)\n"
    );
    assert!(parse_str(&source).is_err(), "recursion must be refused");
}

#[test]
fn calling_a_def_with_the_wrong_number_of_arguments_is_refused() {
    let source = format!(
        "{FUNCTIONS}def twice(n: Int) -> Int = n * 2\nin x: Int\nout y: Int\ny = twice(x, x)\n"
    );
    assert!(parse_str(&source).is_err(), "arity must be checked");
}
