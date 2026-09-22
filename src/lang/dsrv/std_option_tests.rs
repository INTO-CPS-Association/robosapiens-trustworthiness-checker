//! `std::option`, and the embedded modules that carry it.
//!
//! A module under a registered package root is activated by an ordinary
//! `use`, from text compiled into the checker, with no `mod` and no
//! prelude. `std::option` itself is ordinary DSRV: a structural union and
//! three defs, inlined like any other module's.

use contiguous_tree::TreeCursorExt;

use crate::VarName;
use crate::core::{StreamType, UnionAlternative, UnionPayload, UnionValue, Value};
use crate::dataflow::DataflowMonitor;
use crate::dsrv_fixtures::WithoutWarnings;
#[cfg(feature = "thread-safe-ast")]
use crate::lang::dsrv::TypeCheckMode;
use crate::lang::dsrv::TypeCheckOptions;
use crate::lang::dsrv::ast::{CheckedDsrvSpecification, DsrvSpecification};
use crate::lang::dsrv::catalogue::{Catalogue, EmbeddedModule};
use crate::lang::dsrv::diagnostics::SemanticError;
use crate::lang::dsrv::expand::DsrvExpandError;
use crate::lang::dsrv::expand::language::{Dialect, LanguageError, LanguageRequest};
use crate::lang::dsrv::modules::{ModuleCollectError, ModuleCollector, ModuleSources, show_path};
use crate::lang::dsrv::parser::{DsrvParseError, parse_str, parse_str_with};
use crate::lang::dsrv::path::ModuleName;
use crate::lang::dsrv::runtime_expression::{RuntimeExpressionSite, is_runtime_expression};
use crate::lang::dsrv::source_map::SourceLabel;
use crate::lang::dsrv::syntax::{ParsedDeclaration, parse_specification};
use crate::lang::dsrv::type_checker::TCType;

use test_log::test;

/// What a caller needs to name `Option<T>`, call the defs, and write the
/// constructors. The experiments `std::option` itself uses are its own.
const HEADER: &str = "use experimental::{modules, generics, tagged_unions}\nuse std::option::*\n";

const BOTH: [TypeCheckOptions; 2] = [TypeCheckOptions::STRICT, TypeCheckOptions::GRADUAL];

fn name(segment: &str) -> ModuleName {
    ModuleName::new(segment).expect("a lowercase name")
}

fn path(spelled: &str) -> Vec<ModuleName> {
    spelled.split("::").map(name).collect()
}

fn parse(source: &str) -> DsrvSpecification {
    parse_str(source).unwrap_or_else(|error| panic!("{source}: {error}"))
}

fn check(source: &str, options: TypeCheckOptions) -> CheckedDsrvSpecification {
    parse(source)
        .check(options)
        .without_warnings()
        .unwrap_or_else(|errors| panic!("{source} ({:?}): {errors:?}", options.mode))
}

fn check_err(source: &str, options: TypeCheckOptions) -> Vec<SemanticError> {
    parse(source)
        .check(options)
        .without_warnings()
        .err()
        .unwrap_or_else(|| panic!("{source} ({:?}): expected checking to fail", options.mode))
}

fn type_of(checked: &CheckedDsrvSpecification, var: &str) -> TCType {
    checked
        .var_expr_ref(&VarName::from(var))
        .unwrap_or_else(|| panic!("{var} is defined"))
        .typ()
        .clone()
}

fn option_of(payload: StreamType) -> StreamType {
    StreamType::Union(
        crate::core::ClosedUnion::new([
            UnionAlternative::new("Some", UnionPayload::Of(payload)),
            UnionAlternative::new("None", UnionPayload::Nullary),
        ])
        .expect("distinct tags"),
    )
}

fn some(value: Value) -> Value {
    Value::Union(UnionValue::new("Some", Some(value)).into())
}

fn none() -> Value {
    Value::Union(UnionValue::new("None", None).into())
}

/// Collect a program from its root alone, as a program with no filesystem
/// is: every request would be a module a `mod` declared.
fn collect_with(
    root: &str,
    catalogue: &'static Catalogue,
) -> Result<ModuleSources, ModuleCollectError> {
    let collector =
        ModuleCollector::with_catalogue(root, SourceLabel::Path("root.dsrv".into()), catalogue)?;
    assert!(
        collector.next_request().is_none(),
        "an import asks for no file"
    );
    collector.finish()
}

fn collect(root: &str) -> Result<ModuleSources, ModuleCollectError> {
    collect_with(root, &Catalogue::STANDARD)
}

// -----------------------------------------------------------------------------
// The module
// -----------------------------------------------------------------------------

/// The module holds exactly the agreed API: one structural type and three
/// defs, none internal.
#[test]
fn std_option_declares_exactly_its_api() {
    let module = Catalogue::STANDARD
        .get(&path("std::option"))
        .expect("std::option is embedded");
    assert_eq!(module.file, "std/option.dsrv");
    let parsed = parse_specification(module.source).expect("the embedded text parses");
    let mut items = Vec::new();
    for declaration in parsed.declarations() {
        match declaration {
            ParsedDeclaration::Alias(alias) => {
                assert!(!alias.internal);
                assert_eq!(alias.parameters.len(), 1);
                items.push(alias.name.to_string());
            }
            ParsedDeclaration::Def {
                name,
                internal,
                type_parameters,
                ..
            } => {
                assert!(!internal);
                assert_eq!(type_parameters.len(), 1);
                items.push(name.name().to_string());
            }
            ParsedDeclaration::Edition(..) | ParsedDeclaration::Use { .. } => {}
            other => panic!("std::option declares something else: {other:?}"),
        }
    }
    assert_eq!(items, ["Option", "is_some", "is_none", "unwrap_or"]);
}

/// `Option<T>` is the union a caller could have written, not a type of its
/// own: the two spellings are the same type.
#[test]
fn option_is_the_structural_union_of_some_and_none() {
    let spec = parse(&format!(
        "{HEADER}in a: Option<Int>\nin b: Union<None, Some: Int>\nin c: Option<Option<Str>>\n"
    ));
    let annotation = |var: &str| spec.type_annotation(&VarName::from(var)).cloned();
    assert_eq!(annotation("a"), Some(option_of(StreamType::Int)));
    assert_eq!(annotation("a"), annotation("b"));
    assert_eq!(annotation("c"), Some(option_of(option_of(StreamType::Str))));
}

// -----------------------------------------------------------------------------
// Activation
// -----------------------------------------------------------------------------

#[test]
fn a_use_activates_the_embedded_module_without_a_mod() {
    let sources = collect(&format!("{HEADER}in x: Int\n")).expect("collected");
    assert_eq!(
        sources.paths().map(show_path).collect::<Vec<_>>(),
        ["the root module", "std::option"]
    );
    assert!(sources.is_embedded(&path("std::option")));
    assert!(!sources.is_embedded(&[]));
    let labels = sources
        .archive()
        .files()
        .map(|(_, file)| file.label().to_string())
        .collect::<Vec<_>>();
    assert_eq!(labels, ["root.dsrv", "<embedded std/option.dsrv>"]);
}

/// There is no prelude: a program that imports nothing from `std` has no
/// `Option`, and collects no module it did not ask for.
#[test]
fn nothing_is_activated_without_an_import() {
    let root = "use experimental::{modules, generics}\nin x: Int\n";
    let sources = collect(root).expect("collected");
    assert_eq!(sources.len(), 1);
    let unknown = "use experimental::{modules, generics}\nin x: Option<Int>\n";
    assert!(
        matches!(parse_str(unknown), Err(DsrvParseError::Resolve(_))),
        "Option is not in scope without an import"
    );
}

/// Every import form that reaches the module activates it, and naming it
/// more than once reads it once.
#[test]
fn repeated_imports_activate_the_module_once() {
    let root = "use experimental::{modules, generics}\nuse std::option\nuse std::option::*\n\
                use std::option::Option\nuse std::{option::{self, Option}}\nin x: Int\n";
    let sources = collect(root).expect("collected");
    assert_eq!(sources.len(), 2);
    assert_eq!(sources.archive().files().count(), 2);
}

/// A string handed to the parser is a program too, so it may import the
/// standard library without a filesystem.
#[test]
fn the_parser_facade_activates_embedded_modules() {
    let spec = parse(&format!(
        "{HEADER}in o: Option<Int>\nout y: Bool\ny = is_some(o)\n"
    ));
    let labels = spec
        .sources()
        .files()
        .map(|(_, file)| file.label().to_string())
        .collect::<Vec<_>>();
    assert_eq!(labels, ["<string>", "<embedded std/option.dsrv>"]);
}

#[test]
fn a_qualified_import_reaches_the_type_and_defs_through_the_module() {
    let source = "use experimental::{modules, generics}\nuse std::option\n\
                  in o: option::Option<Int>\nout y: Bool\ny = std::option::is_none(o)\n";
    for options in BOTH {
        assert_eq!(type_of(&check(source, options), "y"), TCType::Bool);
    }
}

// -----------------------------------------------------------------------------
// Checking and evaluation
// -----------------------------------------------------------------------------

const PROGRAM: &str = "in x: Int\nin o: Option<Int>\n\
    out present: Bool = is_some(o)\n\
    out absent: Bool = is_none(o)\n\
    out unwrapped: Int = unwrap_or(o, x)\n\
    out wrapped: Option<Int> = Some(x)\n\
    out empty: Option<Int> = None\n\
    out nested: Option<Option<Int>> = Some(o)\n\
    out flat: Int = unwrap_or(unwrap_or(nested, empty), x + 1)\n";

#[test]
fn the_api_checks_strictly_and_gradually() {
    let source = format!("{HEADER}{PROGRAM}");
    for options in BOTH {
        let checked = check(&source, options);
        assert_eq!(checked.check_mode(), options.mode);
        for (var, expected) in [
            ("present", TCType::Bool),
            ("absent", TCType::Bool),
            ("unwrapped", TCType::Int),
            ("flat", TCType::Int),
        ] {
            assert_eq!(
                type_of(&checked, var),
                expected,
                "{var} ({:?})",
                options.mode
            );
        }
    }
}

/// Unannotated streams are the gradual checker's to infer, through the
/// inlined bodies.
#[test]
fn gradual_checking_infers_through_the_defs() {
    let source = format!(
        "{HEADER}in o: Option<Str>\nout present\npresent = is_some(o)\nout value\nvalue = unwrap_or(o, \"\")\n"
    );
    let checked = check(&source, TypeCheckOptions::GRADUAL);
    assert_eq!(type_of(&checked, "present"), TCType::Bool);
    assert_eq!(type_of(&checked, "value"), TCType::Str);
    assert!(
        parse(&source)
            .check(TypeCheckOptions::STRICT)
            .result()
            .is_err(),
        "strict checking still wants the annotations"
    );
}

/// A constructor takes its union from where it stands: an annotated stream,
/// or the other arm of the match it flows into.
#[test]
fn constructors_are_contextual() {
    let source = format!(
        "{HEADER}in x: Int\nout y: Option<Int> = Some(x)\nout z: Option<Int> = None\n\
         out w: Option<Int> = if x > 0 then Some(x) else None\n"
    );
    for options in BOTH {
        let checked = check(&source, options);
        for var in ["y", "z", "w"] {
            assert_eq!(
                type_of(&checked, var),
                TCType::from_stream_type(&option_of(StreamType::Int)),
                "{var}"
            );
        }
    }
    // Nothing gives a bare constructor a union, and no inference invents one.
    let unanchored = format!("{HEADER}in x: Int\nout y: Int = unwrap_or(None, x)\n");
    for options in BOTH {
        check_err(&unanchored, options);
    }
}

#[test]
fn payloads_keep_their_types() {
    for (payload, fallback, expected) in [
        ("Str", "\"none\"", TCType::Str),
        ("Float", "0.5", TCType::Float),
        (
            "List<Int>",
            "List(1, 2)",
            TCType::List(Box::new(TCType::Int)),
        ),
        (
            "(Int, Bool)",
            "Tuple(1, true)",
            TCType::Tuple(vec![TCType::Int, TCType::Bool].into()),
        ),
        (
            "Struct<a: Int, b: Str>",
            "{a: 1, b: \"b\"}",
            TCType::from_stream_type(&StreamType::Struct(
                vec![("a".into(), StreamType::Int), ("b".into(), StreamType::Str)].into(),
                false,
            )),
        ),
    ] {
        let source = format!(
            "{HEADER}in o: Option<{payload}>\nout y: {payload} = unwrap_or(o, {fallback})\n\
             out p: Bool = is_some(o)\n"
        );
        for options in BOTH {
            assert_eq!(type_of(&check(&source, options), "y"), expected, "{source}");
        }
    }
}

/// A payload is not what the defs say it is, and an Option is not a plain
/// value: both are type errors, reported at the call with a note naming the
/// embedded file the body was written in.
#[test]
fn misuse_is_reported_at_the_call_with_the_embedded_definition() {
    for (body, call) in [
        ("in x: Int\nout y: Bool = is_some(x)\n", "is_some(x)"),
        (
            "in o: Option<Int>\nout y: Int = unwrap_or(o, \"s\")\n",
            "unwrap_or(o, \"s\")",
        ),
        ("in o: Option<Int>\nout y: Int = o + 1\n", "o + 1"),
    ] {
        let source = format!("{HEADER}{body}");
        for options in BOTH {
            let errors = check_err(&source, options);
            let location = errors[0].location();
            let primary = location.primary().expect("a primary site");
            assert_eq!(primary.label().to_string(), "<string>");
            assert_eq!(primary.snippet(), Some(call));
            if call != "o + 1" {
                let definition = location.definition().expect("a definition note");
                assert_eq!(definition.label().to_string(), "<embedded std/option.dsrv>");
            }
        }
    }
}

fn evaluate(source: &str, rows: &[Vec<Value>]) -> Vec<Vec<(String, Value)>> {
    let elaborated = parse(source)
        .check_and_elaborate(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap_or_else(|errors| panic!("{source}: {errors:?}"));
    let mut monitor = DataflowMonitor::compile_checked(elaborated).expect("compiles");
    let names = monitor
        .output_vars()
        .iter()
        .map(|var| var.name().to_string())
        .collect::<Vec<_>>();
    rows.iter()
        .map(|row| {
            let mut output = vec![Value::NoVal; names.len()];
            monitor.evaluate(row, &mut output).expect("evaluates");
            names.iter().cloned().zip(output).collect()
        })
        .collect()
}

#[test]
fn the_defs_evaluate_to_what_they_say() {
    let source = format!("{HEADER}{PROGRAM}");
    let rows = evaluate(
        &source,
        &[
            vec![Value::Int(7), some(Value::Int(3))],
            vec![Value::Int(7), none()],
        ],
    );
    let value = |row: &[(String, Value)], var: &str| {
        row.iter()
            .find(|(name, _)| name == var)
            .unwrap_or_else(|| panic!("{var} is an output"))
            .1
            .clone()
    };
    let (present, absent) = (&rows[0], &rows[1]);
    assert_eq!(value(present, "present"), Value::Bool(true));
    assert_eq!(value(present, "absent"), Value::Bool(false));
    assert_eq!(value(present, "unwrapped"), Value::Int(3));
    assert_eq!(value(present, "wrapped"), some(Value::Int(7)));
    assert_eq!(value(present, "empty"), none());
    assert_eq!(value(present, "nested"), some(some(Value::Int(3))));
    assert_eq!(value(present, "flat"), Value::Int(3));

    assert_eq!(value(absent, "present"), Value::Bool(false));
    assert_eq!(value(absent, "absent"), Value::Bool(true));
    assert_eq!(value(absent, "unwrapped"), Value::Int(7));
    assert_eq!(value(absent, "nested"), some(none()));
    // The inner default is the empty option, so the outer one is used.
    assert_eq!(value(absent, "flat"), Value::Int(8));
}

/// `unwrap_or` is an ordinary call: its default is an argument of the
/// application the call becomes, evaluated whichever arm is taken, rather
/// than an expression deferred into the `None` arm.
#[test]
fn unwrap_or_evaluates_its_default_eagerly() {
    let source = format!(
        "{HEADER}in x: Int\nin o: Option<Int>\nout y: Int = unwrap_or(o, default(x[1], 0))\n"
    );
    let printed = parse(&source)
        .var_expr_ref(&VarName::from("y"))
        .expect("y is defined")
        .to_string();
    assert!(
        printed.starts_with("\\option, fallback -> match(option)")
            && printed.ends_with("None -> fallback, }(o, default(x[1], 0))"),
        "{printed}"
    );
    let rows = evaluate(
        &source,
        &[
            vec![Value::Int(1), some(Value::Int(10))],
            vec![Value::Int(2), none()],
        ],
    );
    assert_eq!(rows[0][0].1, Value::Int(10));
    assert_eq!(rows[1][0].1, Value::Int(1));
}

// -----------------------------------------------------------------------------
// Settings
// -----------------------------------------------------------------------------

/// The caller's header grants `std::option` nothing and withholds nothing:
/// its body is judged by its own, so a caller need not enable pattern
/// matching, tagged unions, or functions to call it.
#[test]
fn the_embedded_header_authorises_the_embedded_body() {
    let source = "use experimental::{modules, generics}\nuse std::option::*\n\
                  in o: Option<Int>\nout y: Bool = is_some(o)\n";
    check(source, TypeCheckOptions::STRICT);
}

/// Settings requested for the application are the application's. A
/// requested dialect reaches the root and not the embedded module, which
/// would otherwise be read as Core and refused for declaring a type.
#[test]
fn requested_settings_do_not_reach_the_embedded_module() {
    let root = format!("{HEADER}in x: Int\n");
    let sources = collect(&root).expect("collected");
    let request = LanguageRequest {
        dialect: Some(Dialect::Distributed),
        edition: None,
    };
    let graph = crate::lang::dsrv::expand::graph::build_graph(&sources, request).expect("built");
    assert_eq!(
        graph[&Vec::new()].language().dialect(),
        Dialect::Distributed
    );
    assert_eq!(
        graph[&path("std::option")].language().dialect(),
        Dialect::Full
    );

    // A Core request refuses the root's experiments, not the library's alias.
    let core = LanguageRequest {
        dialect: Some(Dialect::Core),
        edition: None,
    };
    match parse_str_with(&root, core) {
        Err(DsrvParseError::Language(LanguageError::ExperimentsInCore { span })) => {
            assert_eq!(&root[span.to_range()], HEADER.lines().next().unwrap());
        }
        other => panic!("expected the root's experiments to be refused, got {other:?}"),
    }
}

// -----------------------------------------------------------------------------
// Ownership and diagnostics
// -----------------------------------------------------------------------------

/// `std` owns every path under it, so an application may not declare one,
/// from a string or from a file tree.
#[test]
fn an_application_may_not_declare_a_module_under_std() {
    for (declared, reported) in [
        ("std", "std"),
        ("std::option", "std::option"),
        ("std::mine", "std::mine"),
    ] {
        let root = format!("use experimental::{{modules}}\nmod {declared}\nin x: Int\n");
        match collect(&root) {
            Err(ModuleCollectError::ReservedModule {
                path,
                root: owner,
                span,
            }) => {
                assert_eq!(path, reported);
                assert_eq!(owner, "std");
                assert_eq!(&root[span.to_range()], format!("mod {declared}"));
            }
            other => panic!("{declared}: expected a reserved module, got {other:?}"),
        }
        assert!(
            matches!(
                parse_str(&root),
                Err(DsrvParseError::Collect(
                    ModuleCollectError::ReservedModule { .. }
                ))
            ),
            "{declared}: the facade refuses it too"
        );
    }
    // Ownership is of the absolute prefix: `lib::std` is the application's.
    let mut collector =
        ModuleCollector::new("use experimental::{modules}\nmod lib\nin x: Int\n").expect("root");
    collector
        .supply("use experimental::{modules}\nmod std\n")
        .expect("lib may declare its own std");
    assert_eq!(
        collector.next_request().map(show_path),
        Some("lib::std".to_owned())
    );
}

#[test]
fn a_missing_owned_path_is_reported_at_its_import() {
    for (import, missing) in [
        ("std::missing", "std::missing"),
        ("std::missing::*", "std::missing"),
        ("std", "std"),
        ("std::option::inner::Option", "std::option::inner"),
    ] {
        let root = format!("use experimental::{{modules}}\nuse {import}\nin x: Int\n");
        match collect(&root) {
            Err(ModuleCollectError::UnknownEmbeddedModule {
                path,
                importer,
                root: owner,
                span,
            }) => {
                assert_eq!(path, missing);
                assert_eq!(importer, "the root module");
                assert_eq!(owner, "std");
                assert_eq!(&root[span.to_range()], import);
            }
            other => panic!("{import}: expected a missing embedded module, got {other:?}"),
        }
        let message = parse_str(&root)
            .expect_err("the facade refuses it")
            .to_string();
        assert!(message.contains("no embedded module"), "{message}");
    }
}

/// A def the module does not export is the ordinary unknown function.
#[test]
fn an_item_std_option_does_not_export_is_an_ordinary_error() {
    let source = format!("{HEADER}in o: Option<Int>\nout y: Int = std::option::map(o)\n");
    assert!(matches!(
        parse_str(&source),
        Err(DsrvParseError::Modules(message)) if message.contains("names no function")
    ));
}

/// Names the application declares are its own: a local `Option` shadows
/// the glob silently, and two explicit imports of it clash.
#[test]
fn application_names_collide_with_std_names_as_any_module_s_do() {
    let shadowed = "use experimental::{modules}\nuse std::option::*\ntype Option = Int\n\
                    in o: Option\nout y: Int = o + 1\n";
    check(shadowed, TypeCheckOptions::STRICT);
    let clash = "use experimental::{modules}\nuse std::option::Option\ntype Option = Int\n\
                 in o: Int\n";
    assert!(matches!(parse_str(clash), Err(DsrvParseError::Resolve(_))));
}

// -----------------------------------------------------------------------------
// Transitive activation, with catalogues of test modules
// -----------------------------------------------------------------------------

const TYPES: &str = "use experimental::{modules}\n";

static TRANSITIVE: Catalogue = Catalogue {
    roots: &["std"],
    modules: &[
        EmbeddedModule {
            path: &["std", "top"],
            file: "std/top.dsrv",
            source: "use experimental::{modules}\nuse std::left::*\nuse std::right::*\n\
                     type Top = (Left, Right)\n",
        },
        EmbeddedModule {
            path: &["std", "left"],
            file: "std/left.dsrv",
            source: "use experimental::{modules}\nuse std::base::*\ntype Left = List<Base>\n",
        },
        EmbeddedModule {
            path: &["std", "right"],
            file: "std/right.dsrv",
            source: "use experimental::{modules}\nuse std::base::Base\ntype Right = Base\n",
        },
        EmbeddedModule {
            path: &["std", "base"],
            file: "std/base.dsrv",
            source: "use experimental::{modules}\ntype Base = Int\n",
        },
    ],
};

/// A module the root never names is activated because another embedded
/// module imports it, and a diamond reads the shared module once.
#[test]
fn embedded_imports_activate_transitively_and_deduplicate() {
    let root = format!("{TYPES}use std::top::*\nuse std::base::*\nin x: Top\nin y: Base\n");
    let sources = collect_with(&root, &TRANSITIVE).expect("collected");
    assert_eq!(
        sources.paths().map(show_path).collect::<Vec<_>>(),
        [
            "the root module",
            "std::base",
            "std::left",
            "std::right",
            "std::top"
        ]
    );
    assert_eq!(sources.archive().files().count(), 5);
    assert!(
        sources
            .paths()
            .skip(1)
            .all(|path| sources.is_embedded(path))
    );
    let spec = crate::lang::dsrv::expand::expand_program(sources, LanguageRequest::default())
        .expect("expands");
    assert_eq!(
        spec.type_annotation(&VarName::from("x")),
        Some(&StreamType::Tuple(
            vec![StreamType::List(Box::new(StreamType::Int)), StreamType::Int].into()
        ))
    );
}

static CYCLIC: Catalogue = Catalogue {
    roots: &["std"],
    modules: &[
        EmbeddedModule {
            path: &["std", "a"],
            file: "std/a.dsrv",
            source: "use experimental::{modules}\nuse std::b::*\ntype A = Int\n",
        },
        EmbeddedModule {
            path: &["std", "b"],
            file: "std/b.dsrv",
            source: "use experimental::{modules}\nuse std::a::*\ntype B = Int\n",
        },
    ],
};

/// Embedded modules that import each other collect once each and are then
/// refused as any cycle is.
#[test]
fn an_embedded_cycle_is_an_ordinary_cycle() {
    let sources = collect_with(&format!("{TYPES}use std::a::*\nin x: A\n"), &CYCLIC)
        .expect("a cycle still collects");
    assert_eq!(sources.len(), 3);
    match crate::lang::dsrv::expand::expand_program(sources, LanguageRequest::default()) {
        Err(DsrvExpandError::ModuleCycle { path }) => {
            assert_eq!(path, "std::a -> std::b -> std::a");
        }
        other => panic!("expected a cycle, got {other:?}"),
    }
}

static ESCAPING: Catalogue = Catalogue {
    roots: &["std"],
    modules: &[
        EmbeddedModule {
            path: &["std", "reaches_out"],
            file: "std/reaches_out.dsrv",
            source: "use experimental::{modules}\nuse lib::*\ntype A = Int\n",
        },
        EmbeddedModule {
            path: &["std", "declares"],
            file: "std/declares.dsrv",
            source: "use experimental::{modules}\nmod inner\ntype A = Int\n",
        },
        EmbeddedModule {
            path: &["std", "dangling"],
            file: "std/dangling.dsrv",
            source: "use experimental::{modules}\nuse std::gone\ntype A = Int\n",
        },
        EmbeddedModule {
            path: &["std", "own"],
            file: "std/own.dsrv",
            source: "use experimental::{tagged_unions, modules}\n\
                     type T = Union<X, Y>\nuse self::T::*\n",
        },
    ],
};

/// An embedded module depends only on its catalogue: it may import itself,
/// but may neither reach an application module nor declare one.
#[test]
fn an_embedded_module_depends_only_on_its_catalogue() {
    let root = |module: &str| format!("{TYPES}use std::{module}\nin x: Int\n");
    assert!(matches!(
        collect_with(&root("reaches_out"), &ESCAPING),
        Err(ModuleCollectError::EmbeddedImportOutsideCatalogue { path, importer, .. })
            if path == "lib" && importer == "std::reaches_out"
    ));
    assert!(matches!(
        collect_with(&root("declares"), &ESCAPING),
        Err(ModuleCollectError::EmbeddedModuleDeclaration { path, .. })
            if path == "std::declares"
    ));
    assert!(matches!(
        collect_with(&root("dangling"), &ESCAPING),
        Err(ModuleCollectError::UnknownEmbeddedModule { path, importer, .. })
            if path == "std::gone" && importer == "std::dangling"
    ));
    let sources = collect_with(&root("own"), &ESCAPING).expect("a module may import itself");
    assert_eq!(sources.len(), 2);
}

// -----------------------------------------------------------------------------
// The filesystem
// -----------------------------------------------------------------------------

/// A scratch directory of model files, removed afterwards.
struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new(name: &str, files: &[(&str, &str)]) -> Self {
        let directory =
            std::env::temp_dir().join(format!("tc-std-option-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&directory);
        for (file, text) in files {
            let location = directory.join(file);
            std::fs::create_dir_all(location.parent().expect("a parent")).expect("created");
            std::fs::write(location, text).expect("written");
        }
        Self(directory)
    }

    fn root(&self) -> String {
        self.0.join("root.dsrv").display().to_string()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Files that happen to sit where an import would name them are never
/// read: the embedded module is the one used, and an import of a module no
/// `mod` declared is unknown however many files match it.
#[test]
fn imports_never_read_the_filesystem() {
    const GARBAGE: &str = "this is not dsrv @@@";
    let scratch = Scratch::new(
        "imports",
        &[
            (
                "root.dsrv",
                "use experimental::{modules, generics}\nuse std::option::*\n\
                 in o: Option<Int>\nout y: Bool = is_some(o)\n",
            ),
            ("std/option.dsrv", GARBAGE),
            ("std.dsrv", GARBAGE),
            ("option.dsrv", GARBAGE),
        ],
    );
    let program = smol::block_on(crate::lang::dsrv::parser::parse_program_file(
        &scratch.root(),
        LanguageRequest::default(),
    ))
    .expect("the embedded module is used");
    let labels = program
        .specification
        .sources()
        .files()
        .map(|(_, file)| file.label().to_string())
        .collect::<Vec<_>>();
    assert_eq!(labels[1], "<embedded std/option.dsrv>");

    let scratch = Scratch::new(
        "unknown",
        &[
            (
                "root.dsrv",
                "use experimental::{modules}\nuse lib::*\nin x: Int\n",
            ),
            ("lib.dsrv", GARBAGE),
        ],
    );
    let error = smol::block_on(crate::lang::dsrv::parser::parse_program_file(
        &scratch.root(),
        LanguageRequest::default(),
    ))
    .err()
    .expect("lib was never declared");
    let message = format!("{error:#}");
    assert!(message.contains("no module lib is declared"), "{message}");
}

/// A `mod` under `std` is refused before any file is looked for.
#[test]
fn a_reserved_mod_is_refused_before_reading() {
    let scratch = Scratch::new(
        "reserved",
        &[(
            "root.dsrv",
            "use experimental::{modules}\nmod std::option\nin x: Int\n",
        )],
    );
    let error = smol::block_on(crate::lang::dsrv::parser::collect_modules_from_file(
        &scratch.root(),
    ))
    .err()
    .expect("refused");
    assert!(
        matches!(
            error.downcast_ref::<ModuleCollectError>(),
            Some(ModuleCollectError::ReservedModule { .. })
        ),
        "{error:#}"
    );
}

// -----------------------------------------------------------------------------
// Identity and runtime text
// -----------------------------------------------------------------------------

fn runtime_site(spec: &DsrvSpecification, var: &str) -> RuntimeExpressionSite {
    RuntimeExpressionSite::unlocated(
        spec.var_expr_ref(&VarName::from(var))
            .expect("the stream is defined")
            .postorder()
            .find(|node| is_runtime_expression(*node))
            .expect("a dynamic node"),
    )
}

/// What the dataflow descriptor records of `var`'s runtime-expression site.
fn identity(spec: &DsrvSpecification, var: &str) -> String {
    let site = runtime_site(spec, var);
    let mut described = serde_json::to_string(site.context().fingerprint()).unwrap();
    site.callable().describe(&mut described);
    described
}

const DYNAMIC: &str = "in s: Str\nin o: Option<Int>\nout y: Bool = dynamic(s: Bool)\n";

/// Text supplied at run time may call what the file imported, so the bodies
/// an import activated are part of the site's identity: named by module,
/// not by where the text came from.
#[test]
fn activated_bodies_are_part_of_a_runtime_sites_identity() {
    let source = format!("{HEADER}{DYNAMIC}");
    let spec = parse(&source);
    let described = identity(&spec, "y");
    for fragment in ["is_some", "is_none", "unwrap_or", "env std::option="] {
        assert!(described.contains(fragment), "{fragment}: {described}");
    }
    assert!(!described.contains("option.dsrv"), "{described}");

    // Collected from a file tree instead of a string: the same identity.
    let collected = crate::lang::dsrv::expand::expand_program(
        collect(&source).expect("collected"),
        LanguageRequest::default(),
    )
    .expect("expands");
    assert_eq!(identity(&collected, "y"), described);

    // Importing the type alone offers no defs, and a site offering none is
    // a different site.
    let typed_only = parse(&format!(
        "use experimental::{{modules, generics}}\nuse std::option::Option\n{DYNAMIC}"
    ));
    assert_ne!(identity(&typed_only, "y"), described);
}

#[test]
fn runtime_text_may_call_the_imported_defs() {
    let source = format!("{HEADER}{DYNAMIC}");
    let parsed = runtime_site(&parse(&source), "y")
        .parse("is_some(o)")
        .expect("the text may call is_some");
    assert!(!parsed.to_string().contains("is_some"), "{parsed}");

    let rows = evaluate(
        &source,
        &[
            vec![Value::Str("is_some(o)".into()), some(Value::Int(1))],
            vec![Value::Str("is_some(o)".into()), none()],
        ],
    );
    assert_eq!(rows[0][0].1, Value::Bool(true));
    assert_eq!(rows[1][0].1, Value::Bool(false));
}

// -----------------------------------------------------------------------------
// Sharing
// -----------------------------------------------------------------------------

#[cfg(feature = "thread-safe-ast")]
#[test]
fn a_program_using_std_option_crosses_threads() {
    fn shareable<T: Send + Sync>(_: &T) {}
    let spec = parse(&format!("{HEADER}{PROGRAM}"));
    shareable(&spec);
    let checked = std::thread::spawn(move || spec.check(TypeCheckOptions::STRICT))
        .join()
        .expect("the thread finished")
        .without_warnings()
        .expect("checks on another thread");
    assert_eq!(checked.check_mode(), TypeCheckMode::Strict);
}
