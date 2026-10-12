//! Stream functions with local aux equations.
//!
//! Each written use is one statically known instance with private streams
//! and history of its own, expanded into ordinary declarations before type
//! checking. These tests pin the parsed forms, what expansion generates, the
//! uses and arguments it refuses, and what the generated streams compute.

use std::collections::BTreeMap;

use crate::Value;
use crate::VarName;
use crate::core::{
    RuntimeCapability, StreamType, ensure_no_stream_function_instances, ensure_runtime_support,
};
use crate::dataflow::DataflowMonitor;
use crate::dsrv_fixtures::WithoutWarnings;
use crate::lang::dsrv::TypeCheckOptions;
use crate::lang::dsrv::ast::{Declaration, DsrvSpecification};
use crate::lang::dsrv::expand::DsrvExpandError;
use crate::lang::dsrv::expand::language::LanguageError;
use crate::lang::dsrv::modules::{ModuleCollector, show_path};
use crate::lang::dsrv::path::ModuleName;
use crate::lang::dsrv::source_map::SourceLabel;
use crate::lang::dsrv::syntax::parsed::ParsedExprKind;
use crate::lang::dsrv::syntax::{ParsedDeclaration, parse_specification};
use crate::semantics::{MonitoringSemantics, UntimedDsrvSemantics};

use proptest::prelude::*;
use test_log::test;

const BOTH: [TypeCheckOptions; 2] = [TypeCheckOptions::STRICT, TypeCheckOptions::GRADUAL];

/// A library of stream functions the tests instantiate.
const COUNTERS: &str = r#"use experimental::{modules, functions, generics, local_streams, tagged_unions, pattern_matching}
use std::option::*

def is_even(n: Int) -> Bool = n % 2 == 0

// How many ticks `hit` has held on, this one included.
def count(hit: Bool) -> Int = count
where {
  aux before: Int = default(count[1], 0)
  aux count: Int = if hit then before + 1 else before
}

// The value the stream had on the previous tick.
def previous<T>(value: T) -> Option<T> = default(seen[1], None)
where {
  aux seen: Option<T> = Some(value)
  }

def stats(value: Int) -> Struct<count: Int, total: Int, high: Int> = { count: count, total: total, high: high }
where {
  aux count: Int = default(count[1], 0) + 1
  aux total: Int = default(total[1], 0) + value
  aux before: Int = default(high[1], value)
  aux high: Int = if value > before then value else before
  }

def threshold<limit: Int>(value: Int) -> Bool = value > limit
where {
  }

def inside<bounds: Struct<low: Int, high: Int>>(value: Int) -> Bool = value >= bounds.low && value <= bounds.high
where {
  }

// How many ticks `project` accepted the value on.
def accepted<project: (Int -> Bool)>(value: Int) -> Int = count
where {
  aux hit: Bool = project(value)
  aux count: Int = default(count[1], 0) + (if hit then 1 else 0)
}

// Passes `project` on as a value rather than calling it.
def passed<project: (Int -> Bool)>(value: Int) -> Bool = (\check, v -> check(v))(project, value)
where {
  }

// The newest value `project` extracted, kept while it extracts none. The
// history is read at the top level, since this slice gives a read inside a
// match arm no meaning of its own.
def latest<K, T, project: (K -> Option<T>)>(record: K) -> Option<T> = latest
where {
  aux hit: Option<T> = project(record)
  aux before: Option<T> = default(latest[1], None)
  aux latest: Option<T> = match(hit) { Some(v) -> Some(v), None -> before }
}
"#;

/// A library whose stream functions use `counters`'s.
const OUTER: &str = r#"use experimental::{modules, functions, local_streams}
use counters

// Two counters, each with its own history.
def pair(a: Bool, b: Bool) -> Struct<a: Int, b: Int> = { a: left, b: right }
where {
  aux left: Int = counters::count(a)
  aux right: Int = counters::count(b)
  }

// Forwards its fixed function to a nested instance.
def forward<project: (Int -> Bool)>(value: Int) -> Int = counters::accepted::<project>(value)
where {
  }
"#;

const ROOT: &str = r#"use experimental::{modules, functions, generics, local_streams, tagged_unions, pattern_matching, constants}
mod counters
mod outer
use std::option::*
use counters
use outer
"#;

const MODULES: &[(&str, &str)] = &[("counters", COUNTERS), ("outer", OUTER)];

/// Expand a program whose root is `root` and whose modules come from
/// `modules`, each labelled with its own file name.
fn expand_with(root: &str, modules: &[(&str, &str)]) -> Result<DsrvSpecification, DsrvExpandError> {
    let label = |name: &str| SourceLabel::Path(format!("{name}.dsrv").into());
    let mut collector = ModuleCollector::with_label(root, label("root")).expect("a parsable root");
    while let Some(module) = collector.next_request().map(<[ModuleName]>::to_vec) {
        let wanted = show_path(&module);
        let text = modules
            .iter()
            .find(|(name, _)| *name == wanted)
            .unwrap_or_else(|| panic!("no source for {wanted}"))
            .1;
        collector
            .supply_labelled(text, label(&wanted))
            .expect("a parsable module");
    }
    crate::lang::dsrv::expand::expand_program(collector.finish().expect("collected"), None)
}

fn expand(body: &str) -> Result<DsrvSpecification, DsrvExpandError> {
    expand_with(&format!("{ROOT}{body}"), MODULES)
}

fn program(body: &str) -> DsrvSpecification {
    expand(body).unwrap_or_else(|error| panic!("{body}: {error}"))
}

fn refused(body: &str) -> DsrvExpandError {
    match expand(body) {
        Ok(_) => panic!("{body}: expected expansion to fail"),
        Err(error) => error,
    }
}

/// Check under both policies, then run on the dataflow runtime, typed and
/// untyped, which must agree. Each row names its inputs; each result names
/// every output.
fn run(spec: &DsrvSpecification, rows: &[&[(&str, Value)]]) -> Vec<BTreeMap<String, Value>> {
    let mut agreed: Option<Vec<BTreeMap<String, Value>>> = None;
    for options in BOTH {
        let elaborated = spec
            .clone()
            .check_and_elaborate(options)
            .without_warnings()
            .unwrap_or_else(|errors| panic!("{:?}: {errors:?}\n{spec}", options.mode));
        let monitors = [
            DataflowMonitor::compile_checked(elaborated.clone()).unwrap(),
            DataflowMonitor::compile_with_semantics(elaborated, crate::core::Semantics::Untimed)
                .unwrap(),
        ];
        for mut monitor in monitors {
            let inputs = monitor.input_vars().to_vec();
            let outputs = monitor.output_vars().to_vec();
            let results = rows
                .iter()
                .map(|row| {
                    let input: Vec<Value> = inputs
                        .iter()
                        .map(|name| {
                            row.iter()
                                .find(|(written, _)| VarName::from(*written) == *name)
                                .map_or(Value::NoVal, |(_, value)| value.clone())
                        })
                        .collect();
                    let mut output = vec![Value::NoVal; outputs.len()];
                    monitor.evaluate(&input, &mut output).unwrap();
                    outputs
                        .iter()
                        .map(|name| name.name())
                        .zip(output)
                        .collect::<BTreeMap<_, _>>()
                })
                .collect::<Vec<_>>();
            match &agreed {
                Some(agreed) => assert_eq!(&results, agreed, "{:?}", options.mode),
                None => agreed = Some(results),
            }
        }
    }
    agreed.expect("at least one monitor")
}

/// One output over every row.
fn column(results: &[BTreeMap<String, Value>], name: &str) -> Vec<Value> {
    results.iter().map(|row| row[name].clone()).collect()
}

fn int(value: i64) -> Value {
    Value::Int(value)
}

fn boolean(value: bool) -> Value {
    Value::Bool(value)
}

fn annotation(spec: &DsrvSpecification, name: &str) -> Option<StreamType> {
    spec.type_annotation(&VarName::from(name)).cloned()
}

fn aux_names(spec: &DsrvSpecification) -> Vec<String> {
    spec.aux_vars_in_order()
        .into_iter()
        .map(|name| name.name())
        .collect()
}

/// A chain of one-function modules whose leaf is an identity.
fn nested_stream_function_modules(instances: usize) -> (String, Vec<(String, String)>) {
    assert!(instances > 0);
    let mut root = String::from(
        r#"use experimental::{functions, modules, local_streams}
"#,
    );
    for level in 0..instances {
        root.push_str(&format!("mod level{level:02}\n"));
    }
    root.push_str("use level00\nin x: Int\nout observed: Int = level00::step(x)\n");
    let mut modules = Vec::with_capacity(instances);
    for level in 0..instances {
        let name = format!("level{level:02}");
        let source = if level + 1 == instances {
            r#"use experimental::{functions, modules, local_streams}
def step(x: Int) -> Int = x
where { }
"#
            .to_owned()
        } else {
            let next = format!("level{:02}", level + 1);
            format!(
                "use experimental::{{modules, functions, local_streams}}\n\
                 use {next}\n\
                 def step(x: Int) -> Int = {next}::step(x) where {{}}\n"
            )
        };
        modules.push((name, source));
    }
    (root, modules)
}

/// One stream function instance contributes its stream parameters and body equations
/// to the generated-stream limit. Make that count explicit in the source.
fn wide_stream_function(name: &str, generated: usize) -> String {
    assert!(generated >= 2, "one parameter and one output are required");
    let auxiliary = generated - 2;
    let mut source = String::from(
        r#"use experimental::{functions, modules, local_streams}
"#,
    );
    source.push_str(&format!("def {name}(x: Int) -> Int = x where {{\n"));
    for index in 0..auxiliary {
        source.push_str(&format!("  aux value{index:04}: Int = x\n"));
    }
    source.push_str("}\n");
    source
}

/// Reach a requested generated-stream total with moderate 16-stream
/// instances and one final instance that supplies the remainder.
fn wide_stream_function_program(total_generated: usize) -> (String, Vec<(String, String)>, String) {
    let full_instances = (total_generated - 2) / 16;
    let final_generated = total_generated - full_instances * 16;
    assert!((2..=17).contains(&final_generated));

    let mut root = String::from(
        r#"use experimental::{functions, modules, local_streams}
mod wide
mod boundary
use wide
use boundary
in x: Int
"#,
    );
    for index in 0..full_instances {
        root.push_str(&format!("out ordinary{index:03}: Int = wide::wide(x)\n"));
    }
    let crossing = "boundary_result".to_owned();
    root.push_str(&format!("out {crossing}: Int = boundary::boundary(x)\n"));
    (
        root,
        vec![
            ("wide".to_owned(), wide_stream_function("wide", 16)),
            (
                "boundary".to_owned(),
                wide_stream_function("boundary", final_generated),
            ),
        ],
        crossing,
    )
}

/// A generated acyclic stream-function-call chain. Every level owns one private count
/// history and forwards the input to the next level; the root exposes their
/// sum, which has a small independent per-level reference model.
fn counted_stream_function_tree(
    depth: usize,
    with_unrelated: bool,
) -> (String, Vec<(String, String)>) {
    assert!((1..=8).contains(&depth));
    let mut root = String::from(
        r#"use experimental::{functions, modules, local_streams}
mod counter
"#,
    );
    for level in 0..depth {
        root.push_str(&format!("mod level{level:02}\n"));
    }
    if with_unrelated {
        root.push_str(
            r#"def noise(x: Int) -> Int = x + 99
where { }
"#,
        );
    }
    root.push_str("use level00\nin x: Int\nout observed: Int = level00::step(x)\n");
    let mut modules = vec![(
        "counter".to_owned(),
        r#"use experimental::{functions, modules, local_streams}
def count(hit: Bool) -> Int = count
where {
aux before: Int = default(count[1], 0)
aux count: Int = if hit then before + 1 else before
}
"#
        .to_owned(),
    )];
    for level in 0..depth {
        let threshold = level as i64 - 3;
        let source = if level + 1 == depth {
            format!(
                "use experimental::{{modules, functions, local_streams}}\nuse counter\n\
                 def step(x: Int) -> Int = counter::count(x >= {threshold}) where {{}}\n"
            )
        } else {
            let next = format!("level{:02}", level + 1);
            format!(
                "use experimental::{{modules, functions, local_streams}}\nuse counter\nuse {next}\n\
                 def step(x: Int) -> Int = local + nested where {{\n\
                   aux local: Int = counter::count(x >= {threshold})\n\
                   aux nested: Int = {next}::step(x)\n\
                 }}\n"
            )
        };
        modules.push((format!("level{level:02}"), source));
    }
    (root, modules)
}

// -----------------------------------------------------------------------------
// Parsing
// -----------------------------------------------------------------------------

#[test]
fn a_stream_function_declaration_keeps_its_parameters_and_body_apart() {
    let parsed = parse_specification(
        r#"def latest<K, T, project: (K -> Option<T>)>(record: K) -> Option<T> = hit
where {
aux hit: Option<T> = project(record)
}
"#,
    )
    .unwrap();
    let [
        ParsedDeclaration::Def {
            name,
            type_parameters,
            fixed,
            parameters,
            locals,
            ..
        },
    ] = parsed.declarations()
    else {
        panic!("one def declaration");
    };
    assert_eq!(name.to_string(), "latest");
    assert_eq!(
        type_parameters
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        ["K", "T"]
    );
    assert_eq!(fixed.len(), 1);
    assert_eq!(fixed[0].0, VarName::from("project"));
    assert_eq!(parameters.len(), 1);
    assert_eq!(locals.as_ref().unwrap().len(), 1);
    assert_eq!(parsed.roots().len(), 2);
}

#[test]
fn a_use_names_its_stream_function_through_a_module_and_orders_fixed_arguments_first() {
    let parsed =
        parse_specification("aux y = lib::latest::<store::key, 3>(record, x + 1)\n").unwrap();
    let roots = parsed.roots();
    let ParsedExprKind::StreamFunctionCall(arguments, path, fixed) = roots[0].as_ref().kind()
    else {
        panic!("a stream function call");
    };
    assert_eq!(path.to_string(), "lib::latest");
    assert_eq!((*fixed, arguments.len()), (2, 4));
    // Without fixed arguments parsing cannot distinguish a stream_function from
    // an ordinary function. Expansion resolves the qualified callee.
    let parsed = parse_specification("aux y = rpio::journal::timing::tick(observation)\n").unwrap();
    let roots = parsed.roots();
    let ParsedExprKind::Apply(_, arguments) = roots[0].as_ref().kind() else {
        panic!("a call awaiting name resolution");
    };
    assert_eq!(arguments.len(), 1);
}

#[test]
fn ordinary_def_and_unqualified_fixed_call_syntax_are_accepted() {
    assert!(parse_specification("def count(x: Int) -> Int = x where {}\n").is_ok());
    // Unqualified fixed calls are resolved as defs during expansion.
    assert!(parse_specification("aux y = count::<2>(x)\n").is_ok());
}

#[test]
fn existing_expressions_keep_their_meaning() {
    // A capitalised name applied to a payload is still a constructor, a
    // qualified tag still names its union, and `<` still compares.
    for source in [
        "out y = Some(x)\n",
        "out y = lib::State::Moving(x)\n",
        "out y = a < b\n",
        "out y = lib::value < limit\n",
        "out y = lib::value < limit > other\n",
        "out y = (A < b) > c\n",
    ] {
        let parsed = parse_specification(source).unwrap();
        let roots = parsed.roots();
        assert!(
            !matches!(
                roots[0].as_ref().kind(),
                ParsedExprKind::StreamFunctionCall(..)
            ),
            "{source}"
        );
    }
}

// -----------------------------------------------------------------------------
// Language settings
// -----------------------------------------------------------------------------

#[test]
fn a_stream_function_needs_the_local_streams_experiment_in_its_own_module() {
    let library = r#"use experimental::{modules, functions}
def id(x: Int) -> Int = x
where { }
"#;
    let root = r#"use experimental::{functions, modules, local_streams}
mod lib
use lib
in x: Int
"#;
    let error = expand_with(root, &[("lib", library)]).expect_err("the library did not opt in");
    assert!(
        matches!(
            &error,
            DsrvExpandError::Language(LanguageError::NeedsExperiment {
                feature: "local_streams",
                ..
            })
        ),
        "{error:?}"
    );
    // A generic stream_function also needs generics.
    let library = r#"use experimental::{functions, modules, local_streams}
def id<T>(x: T) -> T = x
where { }
"#;
    let error = expand_with(root, &[("lib", library)]).expect_err("no generics");
    assert!(
        matches!(
            error,
            DsrvExpandError::Language(LanguageError::NeedsExperiment {
                feature: "generics",
                ..
            })
        ),
        "{error:?}"
    );
}

#[test]
fn a_use_needs_the_local_streams_experiment_where_it_is_written() {
    let root = "use experimental::modules\nmod counters\nuse counters\n\
                in hit: Bool\nout n: Int = counters::count(hit)\n";
    let error = expand_with(root, MODULES).expect_err("the root did not opt in");
    assert!(
        matches!(
            error,
            DsrvExpandError::Language(LanguageError::NeedsExperiment {
                feature: "local_streams",
                construct: "a stream function call",
                ..
            })
        ),
        "{error:?}"
    );
}

#[test]
fn high_level_dsrv_enables_local_stream_functions() {
    use crate::lang::dsrv::expand::language::{Feature, MetaFeature};
    assert!(Feature::ALL.contains(&Feature::LocalStreams));
    assert!(
        MetaFeature::HighLevelDsrv
            .experiments()
            .any(|feature| feature == Feature::LocalStreams)
    );
    crate::lang::dsrv::parser::parse_str(
        r#"use experimental::high_level_dsrv
def identity(value: Int) -> Int = value
where { }
in x: Int
"#,
    )
    .expect("the high-level bundle accepts a stream function");
}

// -----------------------------------------------------------------------------
// Instances
// -----------------------------------------------------------------------------

#[test]
fn lowercase_qualified_calls_dispatch_to_functions_and_stream_functions() {
    let spec = program(
        "in x: Int\nout even: Bool = counters::is_even(x)\n\
         out total: Int = counters::count(counters::is_even(x))\n",
    );
    assert_eq!(spec.stream_function_instances().len(), 1);
    let rows = run(&spec, &[&[("x", int(2))], &[("x", int(3))]]);
    assert_eq!(rows[0]["even"], boolean(true));
    assert_eq!(rows[1]["even"], boolean(false));
    assert_eq!(rows[0]["total"], int(1));
    assert_eq!(rows[1]["total"], int(1));
}

#[test]
fn each_use_is_an_instance_with_private_streams_and_history_of_its_own() {
    let spec = program(
        "in hit: Bool\n\
         out first: Int = counters::count(hit)\n\
         out second: Int = counters::count(!hit)\n\
         out again: Int = counters::count(hit)\n",
    );
    // Uses with equal arguments are still separate instances.
    assert_eq!(
        aux_names(&spec),
        [
            "first$hit",
            "first$before",
            "first$count",
            "second$hit",
            "second$before",
            "second$count",
            "again$hit",
            "again$before",
            "again$count",
        ]
    );
    let instances: Vec<_> = spec
        .stream_function_instances()
        .iter()
        .map(|instance| (instance.stream_function.to_string(), instance.result.name()))
        .collect();
    assert_eq!(
        instances,
        [
            ("counters::count".to_owned(), "first".to_owned()),
            ("counters::count".to_owned(), "second".to_owned()),
            ("counters::count".to_owned(), "again".to_owned()),
        ]
    );
    let results = run(
        &spec,
        &[
            &[("hit", boolean(true))],
            &[("hit", boolean(false))],
            &[("hit", boolean(true))],
            &[("hit", boolean(true))],
        ],
    );
    assert_eq!(column(&results, "first"), [int(1), int(1), int(2), int(3)]);
    assert_eq!(column(&results, "second"), [int(0), int(1), int(1), int(1)]);
    assert_eq!(column(&results, "again"), column(&results, "first"));
}

#[test]
fn unrelated_declarations_do_not_change_lexical_instance_identity() {
    let before = program(
        "in hit: Bool\n\
         aux unrelated: Int = 41\n\
         out count: Int = counters::count(hit)\n",
    );
    let after = program(
        "in hit: Bool\n\
         out count: Int = counters::count(hit)\n\
         aux unrelated: Int = 41\n",
    );
    let private = |spec: &DsrvSpecification| {
        aux_names(spec)
            .into_iter()
            .filter(|name| name.contains('$'))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        private(&before),
        ["count$hit", "count$before", "count$count"]
    );
    assert_eq!(private(&before), private(&after));
    let ticks: &[&[(&str, Value)]] = &[
        &[("hit", boolean(true))],
        &[("hit", boolean(false))],
        &[("hit", boolean(true))],
    ];
    assert_eq!(
        column(&run(&before, ticks), "count"),
        [int(1), int(1), int(2)]
    );
    assert_eq!(
        column(&run(&after, ticks), "count"),
        [int(1), int(1), int(2)]
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn generated_acyclic_stream_function_trees_match_private_history_model(
        depth in 1_usize..=8,
        inputs in prop::collection::vec(-5_i64..=8, 1..=12),
    ) {
        let (root, owned_modules) = counted_stream_function_tree(depth, false);
        let modules = owned_modules
            .iter()
            .map(|(name, source)| (name.as_str(), source.as_str()))
            .collect::<Vec<_>>();
        let spec = expand_with(&root, &modules).unwrap();

        let owned_rows = inputs
            .iter()
            .map(|value| vec![("x", int(*value))])
            .collect::<Vec<_>>();
        let rows = owned_rows.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let observed = column(&run(&spec, &rows), "observed");

        let mut counts = vec![0_i64; depth];
        let expected = inputs
            .iter()
            .map(|value| {
                for (level, count) in counts.iter_mut().enumerate() {
                    if *value >= level as i64 - 3 {
                        *count += 1;
                    }
                }
                int(counts.iter().sum())
            })
            .collect::<Vec<_>>();
        prop_assert_eq!(&observed, &expected);

        let (noisy_root, noisy_owned_modules) = counted_stream_function_tree(depth, true);
        let noisy_modules = noisy_owned_modules
            .iter()
            .map(|(name, source)| (name.as_str(), source.as_str()))
            .collect::<Vec<_>>();
        let noisy = expand_with(&noisy_root, &noisy_modules).unwrap();
        prop_assert_eq!(column(&run(&noisy, &rows), "observed"), expected);
    }
}

#[test]
fn private_names_cannot_capture_or_be_captured_by_the_use_site() {
    // The root declares streams named like the function's own; neither
    // reads the other.
    let spec = program(
        "in hit: Bool\n\
         in before: Int\n\
         out count: Int = counters::count(hit)\n\
         out echoed: Int = before\n",
    );
    let results = run(
        &spec,
        &[
            &[("hit", boolean(true)), ("before", int(40))],
            &[("hit", boolean(true)), ("before", int(50))],
        ],
    );
    assert_eq!(column(&results, "count"), [int(1), int(2)]);
    assert_eq!(column(&results, "echoed"), [int(40), int(50)]);
}

#[test]
fn a_struct_result_types_the_use_site_stream_and_its_fields() {
    let spec = program(
        "in x: Int\n\
         aux stats = counters::stats(x)\n\
         out count: Int = stats.count\n\
         out total: Int = stats.total\n\
         out high: Int = stats.high\n",
    );
    // The use site declared no type, so it takes the function's result.
    assert_eq!(
        annotation(&spec, "stats"),
        Some(StreamType::Struct(
            [
                ("count".into(), StreamType::Int),
                ("total".into(), StreamType::Int),
                ("high".into(), StreamType::Int),
            ]
            .into_iter()
            .collect(),
            false,
        ))
    );
    let results = run(
        &spec,
        &[&[("x", int(3))], &[("x", int(7))], &[("x", int(5))]],
    );
    assert_eq!(column(&results, "count"), [int(1), int(2), int(3)]);
    assert_eq!(column(&results, "total"), [int(3), int(10), int(15)]);
    assert_eq!(column(&results, "high"), [int(3), int(7), int(7)]);
}

#[test]
fn a_result_type_the_use_site_contradicts_is_a_type_error() {
    let spec = program("in x: Int\nout wrong: Bool = counters::stats(x)\n");
    for options in BOTH {
        spec.clone()
            .check(options)
            .without_warnings()
            .expect_err("a struct is not a Bool");
    }
}

// -----------------------------------------------------------------------------
// Type parameters
// -----------------------------------------------------------------------------

#[test]
fn type_parameters_are_inferred_from_what_the_arguments_state() {
    let spec = program(
        "in x: Int\n\
         in s: Str\n\
         aux px = counters::previous(x)\n\
         aux ps = counters::previous(s)\n\
         out x_before: Int = unwrap_or(px, -1)\n\
         out s_before: Str = unwrap_or(ps, \"none\")\n",
    );
    let option = |payload| {
        crate::lang::dsrv::parser::parse_str(&format!(
            "use experimental::{{modules, generics, tagged_unions}}\nuse std::option::*\n\
             in probe: Option<{payload}>\n"
        ))
        .unwrap()
        .type_annotation(&VarName::from("probe"))
        .cloned()
    };
    assert_eq!(annotation(&spec, "px"), option("Int"));
    assert_eq!(annotation(&spec, "ps"), option("Str"));
    let results = run(
        &spec,
        &[
            &[("x", int(1)), ("s", Value::Str("a".into()))],
            &[("x", int(2)), ("s", Value::Str("b".into()))],
        ],
    );
    assert_eq!(column(&results, "x_before"), [int(-1), int(1)]);
    assert_eq!(
        column(&results, "s_before"),
        [Value::Str("none".into()), Value::Str("a".into())]
    );
}

#[test]
fn a_type_parameter_is_inferred_through_a_fixed_function_signature() {
    let spec = program(
        "type Parity = Option<Str>\n\
         def evens(n: Int) -> Parity =\n\
           if n % 2 == 0 then Parity::Some(\"even\") else Parity::None\n\
         in x: Int\n\
         aux latest = counters::latest::<evens>(x)\n\
         out shown: Str = unwrap_or(latest, \"-\")\n",
    );
    let results = run(
        &spec,
        &[&[("x", int(1))], &[("x", int(2))], &[("x", int(3))]],
    );
    assert_eq!(
        column(&results, "shown"),
        [
            Value::Str("-".into()),
            Value::Str("even".into()),
            Value::Str("even".into())
        ]
    );
}

#[test]
fn a_type_parameter_no_argument_states_is_refused() {
    // An expression argument states no type, so T cannot be inferred.
    let error = refused("in x: Int\naux p = counters::previous(x + 1)\n");
    assert!(
        matches!(
            &error,
            DsrvExpandError::StreamFunctionTypeInference { parameter, .. } if parameter == "T"
        ),
        "{error:?}"
    );
}

// -----------------------------------------------------------------------------
// Fixed arguments
// -----------------------------------------------------------------------------

#[test]
fn fixed_data_arguments_are_closed_expressions_read_at_the_use_site() {
    let spec = program(
        "const LIMIT: Int = 4\n\
         in x: Int\n\
         out over_two: Bool = counters::threshold::<2>(x)\n\
         out over_limit: Bool = counters::threshold::<LIMIT>(x)\n\
         out over_sum: Bool = counters::threshold::<(LIMIT - 1)>(x)\n\
         out inside: Bool = counters::inside::<{ low: 2, high: LIMIT }>(x)\n",
    );
    let results = run(
        &spec,
        &[&[("x", int(1))], &[("x", int(3))], &[("x", int(5))]],
    );
    assert_eq!(
        column(&results, "over_two"),
        [boolean(false), boolean(true), boolean(true)]
    );
    assert_eq!(
        column(&results, "over_limit"),
        [boolean(false), boolean(false), boolean(true)]
    );
    assert_eq!(
        column(&results, "over_sum"),
        [boolean(false), boolean(false), boolean(true)]
    );
    assert_eq!(
        column(&results, "inside"),
        [boolean(false), boolean(true), boolean(false)]
    );
}

#[test]
fn a_tagged_constructor_is_a_fixed_argument() {
    let library = r#"use experimental::{functions, modules, local_streams, tagged_unions, pattern_matching}
type Level = Union<Low, High>
def matches_level<level: Level>(value: Level) -> Bool = value == level
where {
}
"#;
    let root = r#"use experimental::{functions, modules, local_streams, tagged_unions}
mod lib
use lib
in level: lib::Level
out high: Bool = lib::matches_level::<lib::Level::High>(level)
"#;
    let spec = expand_with(root, &[("lib", library)]).unwrap();
    for options in BOTH {
        spec.clone()
            .check(options)
            .without_warnings()
            .unwrap_or_else(|errors| panic!("{:?}: {errors:?}", options.mode));
    }
}

#[test]
fn a_fixed_data_argument_may_not_depend_on_a_stream() {
    for (argument, why) in [
        ("x", "reads the stream `x`"),
        ("(x + 1)", "reads the stream `x`"),
        ("x[1]", "reads stream history"),
        ("(dynamic(s))", "uses `dynamic`"),
        ("(defer(s))", "uses `defer`"),
    ] {
        let body =
            format!("in x: Int\nin s: Str\nout y: Bool = counters::threshold::<{argument}>(x)\n");
        let error = refused(&body);
        let DsrvExpandError::StreamFunctionFixedArgument {
            parameter, problem, ..
        } = &error
        else {
            panic!("{argument}: {error:?}");
        };
        assert_eq!(parameter, "limit", "{argument}");
        assert!(problem.contains(why), "{argument}: {problem}");
    }
}

#[test]
fn a_fixed_function_names_a_def_inlined_where_it_was_written() {
    let spec = program(
        "def positive(n: Int) -> Bool = n > 0\n\
         in x: Int\n\
         out positives: Int = counters::accepted::<positive>(x)\n\
         out evens: Int = counters::accepted::<counters::is_even>(x)\n\
         out passed: Bool = counters::passed::<positive>(x)\n",
    );
    let results = run(
        &spec,
        &[&[("x", int(2))], &[("x", int(-3))], &[("x", int(-4))]],
    );
    assert_eq!(column(&results, "positives"), [int(1), int(1), int(1)]);
    assert_eq!(column(&results, "evens"), [int(1), int(1), int(2)]);
    assert_eq!(
        column(&results, "passed"),
        [boolean(true), boolean(false), boolean(false)]
    );
}

#[test]
fn a_fixed_function_keeps_transitive_definition_site_module_context() {
    let projectors = "use experimental::{modules, functions}\n\
        def threshold(n: Int) -> Bool = n > 10\n\
        def selected(n: Int) -> Bool = threshold(n)\n";
    let root = r#"use experimental::{modules, functions, local_streams}
mod counters
mod projectors
use counters
use projectors
def threshold(n: Int) -> Bool = n > 0
in x: Int
out selected: Int = counters::accepted::<projectors::selected>(x)
"#;
    let mut modules = vec![("counters", COUNTERS)];
    modules.push(("projectors", projectors));
    let spec = expand_with(&root, &modules).unwrap();
    let results = run(
        &spec,
        &[&[("x", int(5))], &[("x", int(11))], &[("x", int(-1))]],
    );
    assert_eq!(column(&results, "selected"), [int(0), int(1), int(1)]);
}

#[test]
fn an_inline_lambda_is_not_a_fixed_function() {
    let error = refused("in x: Int\nout y: Int = counters::accepted::<(\\n -> n > 0)>(x)\n");
    let DsrvExpandError::StreamFunctionFixedArgument { problem, .. } = &error else {
        panic!("{error:?}");
    };
    assert!(problem.contains("inline lambda"), "{problem}");
    let error = refused("in x: Int\nout y: Int = counters::accepted::<nothing>(x)\n");
    assert!(
        matches!(&error, DsrvExpandError::StreamFunctionFixedArgument { problem, .. }
            if problem.contains("which is no `def`")),
        "{error:?}"
    );
}

#[test]
fn a_def_that_reads_a_stream_is_not_a_fixed_function() {
    let error = refused(
        "def above(n: Int) -> Bool = n > x\n\
         in x: Int\n\
         out y: Int = counters::accepted::<above>(x)\n",
    );
    let DsrvExpandError::StreamFunctionFixedArgument { problem, .. } = &error else {
        panic!("{error:?}");
    };
    assert!(problem.contains("reads the stream `x`"), "{problem}");
}

// -----------------------------------------------------------------------------
// Nesting
// -----------------------------------------------------------------------------

#[test]
fn a_nested_use_is_one_instance_per_enclosing_instance() {
    let root = format!(
        "{ROOT}in a: Bool\nin b: Bool\n\
         aux p = outer::pair(a, b)\n\
         aux q = outer::pair(b, a)\n\
         out pa: Int = p.a\nout pb: Int = p.b\n\
         out qa: Int = q.a\nout qb: Int = q.b\n"
    );
    let spec = expand_with(&root, MODULES).unwrap();
    // Paths name the enclosing instance, so siblings never meet.
    let names = aux_names(&spec);
    for name in [
        "p$left",
        "p$left$before",
        "p$right$before",
        "q$left$before",
        "q$right$before",
    ] {
        assert!(
            names.iter().any(|written| written == name),
            "{name}: {names:?}"
        );
    }
    assert_eq!(spec.stream_function_instances().len(), 6);
    let results = run(
        &spec,
        &[
            &[("a", boolean(true)), ("b", boolean(false))],
            &[("a", boolean(true)), ("b", boolean(true))],
            &[("a", boolean(false)), ("b", boolean(true))],
        ],
    );
    assert_eq!(column(&results, "pa"), [int(1), int(2), int(2)]);
    assert_eq!(column(&results, "pb"), [int(0), int(1), int(2)]);
    assert_eq!(column(&results, "qa"), column(&results, "pb"));
    assert_eq!(column(&results, "qb"), column(&results, "pa"));
}

#[test]
fn a_forwarded_fixed_function_reaches_the_nested_instance() {
    let root = format!(
        "{ROOT}def positive(n: Int) -> Bool = n > 0\n\
         in x: Int\n\
         out forwarded: Int = outer::forward::<positive>(x)\n"
    );
    let spec = expand_with(&root, MODULES).unwrap();
    let results = run(
        &spec,
        &[&[("x", int(1))], &[("x", int(-1))], &[("x", int(5))]],
    );
    assert_eq!(column(&results, "forwarded"), [int(1), int(1), int(2)]);
}

#[test]
fn an_instantiation_that_recurses_names_its_chain() {
    let library = r#"use experimental::{functions, modules, local_streams}
use self
def forever(x: Int) -> Int = lib::forever(x)
where { }
"#;
    let root = r#"use experimental::{functions, modules, local_streams}
mod lib
use lib
in x: Int
out y: Int = lib::forever(x)
"#;
    let error = expand_with(root, &[("lib", library)]).expect_err("recursion");
    let DsrvExpandError::RecursiveStreamFunction { chain } = &error else {
        panic!("{error:?}");
    };
    assert_eq!(chain, "y = lib::forever -> y = lib::forever");
}

#[test]
fn an_indirect_recursive_instantiation_names_the_complete_cycle() {
    let library = r#"use experimental::{functions, modules, local_streams}
use self
def a(x: Int) -> Int = lib::b(x)
where { }
def b(x: Int) -> Int = lib::c(x)
where { }
def c(x: Int) -> Int = lib::a(x)
where { }
"#;
    let root = r#"use experimental::{functions, modules, local_streams}
mod lib
use lib
in x: Int
out observed: Int = lib::a(x)
"#;
    let error = expand_with(root, &[("lib", library)]).expect_err("indirect recursion");
    let DsrvExpandError::RecursiveStreamFunction { chain } = &error else {
        panic!("{error:?}");
    };
    assert_eq!(
        chain,
        "observed = lib::a -> observed = lib::b -> observed = lib::c -> observed = lib::a"
    );
}

#[test]
fn stream_function_nesting_accepts_sixteen_instances_and_releases_depth_between_uses() {
    for instances in [15, 16] {
        let (mut root, owned_modules) = nested_stream_function_modules(instances);
        // A sibling traverses the same maximum-depth chain. It succeeds only
        // if expansion popped all depth state after the first lexical use.
        root.push_str("out sibling: Int = level00::step(x)\n");
        let modules = owned_modules
            .iter()
            .map(|(name, source)| (name.as_str(), source.as_str()))
            .collect::<Vec<_>>();
        let spec = expand_with(&root, &modules).unwrap_or_else(|error| {
            panic!("{instances} nested instances should expand: {error:?}")
        });
        let results = run(&spec, &[&[("x", int(37))]]);
        assert_eq!(column(&results, "observed"), [int(37)]);
        assert_eq!(column(&results, "sibling"), [int(37)]);
    }
}

#[test]
fn stream_function_nesting_rejects_the_seventeenth_instance_with_the_complete_chain() {
    let (root, owned_modules) = nested_stream_function_modules(17);
    let modules = owned_modules
        .iter()
        .map(|(name, source)| (name.as_str(), source.as_str()))
        .collect::<Vec<_>>();
    let error = expand_with(&root, &modules).expect_err("a seventeenth nested instance");
    let DsrvExpandError::StreamFunctionLimit { limit, what, chain } = &error else {
        panic!("{error:?}");
    };
    assert_eq!((*limit, *what), (16, "levels of nesting"));
    let entries = chain.split(" -> ").collect::<Vec<_>>();
    assert_eq!(entries.len(), 17, "{chain}");
    for (level, entry) in entries.iter().enumerate() {
        assert_eq!(
            *entry,
            format!("observed = level{level:02}::step"),
            "{chain}"
        );
    }
}

#[test]
fn placement_is_refused_independently_of_the_nesting_limit() {
    let (root, mut owned_modules) = nested_stream_function_modules(17);
    owned_modules[15].1 = r#"use experimental::{functions, modules, local_streams}
use level16
def step(x: Int) -> Int = level16::step(x) + 1
where { }
"#
    .to_owned();
    let modules = owned_modules
        .iter()
        .map(|(name, source)| (name.as_str(), source.as_str()))
        .collect::<Vec<_>>();
    let error = expand_with(&root, &modules).expect_err("a use within an expression");
    assert!(
        matches!(
            &error,
            DsrvExpandError::MisplacedStreamFunctionCall {
                stream_function,
                construct: "a larger expression",
                ..
            } if stream_function == "level16::step"
        ),
        "{error:?}"
    );
}

#[test]
fn generated_stream_limit_accepts_its_boundary_and_rejects_the_next_stream() {
    for total in [4095, 4096] {
        let (root, owned_modules, crossing) = wide_stream_function_program(total);
        let modules = owned_modules
            .iter()
            .map(|(name, source)| (name.as_str(), source.as_str()))
            .collect::<Vec<_>>();
        let spec = expand_with(&root, &modules)
            .unwrap_or_else(|error| panic!("{total} generated streams: {error:?}"));
        assert_eq!(spec.stream_function_instances().len(), (total - 2) / 16 + 1);
        assert_eq!(annotation(&spec, &crossing), Some(StreamType::Int));
    }

    let (root, owned_modules, crossing) = wide_stream_function_program(4097);
    let modules = owned_modules
        .iter()
        .map(|(name, source)| (name.as_str(), source.as_str()))
        .collect::<Vec<_>>();
    let error = expand_with(&root, &modules).expect_err("the 4097th generated stream");
    let DsrvExpandError::StreamFunctionLimit { limit, what, chain } = &error else {
        panic!("{error:?}");
    };
    assert_eq!((*limit, *what), (4096, "generated streams"));
    assert_eq!(chain, &format!("{crossing} = boundary::boundary"));
}

#[test]
fn generated_stream_limit_does_not_count_an_unused_stream_function_body() {
    let library = wide_stream_function("wide", 4097);
    let root = r#"use experimental::{functions, modules, local_streams}
mod lib
use lib
in x: Int
out observed: Int = x
"#;
    let spec = expand_with(root, &[("lib", &library)])
        .expect("a declaration alone generates no private streams");
    assert!(spec.stream_function_instances().is_empty());
}

// -----------------------------------------------------------------------------
// Uses and declarations expansion refuses
// -----------------------------------------------------------------------------

#[test]
fn a_use_is_only_ever_the_whole_right_hand_side_of_an_equation() {
    for (body, construct) in [
        (
            "in c: Bool\nin hit: Bool\nout y: Int = if c then counters::count(hit) else 0\n",
            "an expression",
        ),
        (
            "in hit: Bool\nout y: Int = counters::count(hit) + 1\n",
            "an expression",
        ),
        (
            "in xs: List<Bool>\nout y: List<Int> = List.map(\\h -> counters::count(h), xs)\n",
            "an expression",
        ),
    ] {
        let error = refused(body);
        assert!(
            matches!(
                &error,
                DsrvExpandError::MisplacedStreamFunctionCall { stream_function, construct: found, .. }
                    if stream_function == "counters::count" && *found == construct
            ),
            "{body}: {error:?}"
        );
    }
}

#[test]
fn a_stream_function_call_cannot_supply_a_constant() {
    let error = refused("const counted: Int = counters::count(true)\n");
    assert!(
        matches!(error, DsrvExpandError::NotConstant { .. }),
        "{error:?}"
    );
}

#[test]
fn a_use_inside_a_stream_function_body_is_only_ever_a_whole_equation() {
    let library = r#"use experimental::{functions, modules, local_streams}
use counters
def bad(h: Bool) -> Int = counters::count(h) + 1
where { }
"#;
    let root = r#"use experimental::{functions, modules, local_streams}
mod lib
mod counters
use lib
in x: Int
"#;
    let error = expand_with(root, &[("lib", library), ("counters", COUNTERS)])
        .expect_err("a nested use in a larger expression");
    assert!(
        matches!(error, DsrvExpandError::MisplacedStreamFunctionCall { .. }),
        "{error:?}"
    );
}

#[test]
fn unknown_stream_functions_and_wrong_argument_counts_are_refused_at_the_use() {
    let error = refused("in hit: Bool\nout y: Int = counters::missing::<>(hit)\n");
    assert!(
        matches!(&error, DsrvExpandError::UnknownStreamFunction { name, .. } if name == "counters::missing"),
        "{error:?}"
    );
    let body = "in hit: Bool\nout y: Int = counters::count(hit, hit)\n";
    let error = refused(body);
    let DsrvExpandError::StreamFunctionArity {
        kind,
        expected,
        found,
        span,
        ..
    } = &error
    else {
        panic!("{error:?}");
    };
    assert_eq!((*kind, *expected, *found), ("stream", 1, 2));
    let root = format!("{ROOT}{body}");
    assert_eq!(&root[span.to_range()], "counters::count(hit, hit)");
    let error = refused("in x: Int\nout y: Bool = counters::threshold(x)\n");
    assert!(
        matches!(
            error,
            DsrvExpandError::StreamFunctionArity { kind: "fixed", .. }
        ),
        "{error:?}"
    );
}

#[test]
fn a_stream_function_body_is_closed_and_has_one_result() {
    let cases = [
        (
            r#"def leaky(h: Bool) -> Bool = h && outside
where { }
"#,
            "`outside`",
        ),
        (
            "def shadowed(h: Bool) -> Bool = h where { aux h: Bool = true }\n",
            "`h` is declared twice",
        ),
    ];
    for (stream_function, why) in cases {
        let library =
            format!("use experimental::{{modules, functions, local_streams}}\n{stream_function}");
        let root = r#"use experimental::{functions, modules, local_streams}
mod lib
use lib
in outside: Bool
"#;
        let error = expand_with(root, &[("lib", &library)]).expect_err(stream_function);
        let DsrvExpandError::InvalidStreamFunction { problem, .. } = &error else {
            panic!("{stream_function}: {error:?}");
        };
        assert!(problem.contains(why), "{stream_function}: {problem}");
    }
}

#[test]
fn an_unused_stream_function_changes_nothing_about_a_program() {
    let with = program("in x: Int\nout y: Int = x + 1\n");
    let root = r#"use experimental::{modules, functions, generics, local_streams, tagged_unions, pattern_matching, constants}
use std::option::*
in x: Int
out y: Int = x + 1
"#;
    let without = expand_with(root, &[]).unwrap();
    assert!(with.stream_function_instances().is_empty());
    assert_eq!(with.to_string(), without.to_string());
}

// -----------------------------------------------------------------------------
// Runtime admission
// -----------------------------------------------------------------------------

#[test]
fn only_runtimes_that_declare_it_admit_a_stream_function_instance() {
    let body = "in hit: Bool\nout y: Int = counters::count(hit)\n";
    let root = format!("{ROOT}{body}");
    let spec = program(body);
    ensure_runtime_support(&spec, crate::dataflow::RUNTIME_CAPABILITIES, "dataflow").unwrap();
    let semi_sync = <UntimedDsrvSemantics as MonitoringSemantics<
        crate::runtime::builder::SemiSyncValueConfig,
    >>::RUNTIME_CAPABILITIES
        .union(crate::runtime::semi_sync::RUNTIME_CAPABILITIES);
    ensure_runtime_support(&spec, semi_sync, "semi-sync").unwrap();
    let asynchronous = <UntimedDsrvSemantics as MonitoringSemantics<
        crate::runtime::builder::ValueConfig,
    >>::RUNTIME_CAPABILITIES;
    let refusal = ensure_runtime_support(&spec, asynchronous, "async").unwrap_err();
    assert_eq!(
        refusal.requirement.capability,
        RuntimeCapability::StreamFunctions
    );
    assert_eq!(
        &root[refusal.requirement.span.to_range()],
        "counters::count(hit)"
    );
    // The record survives checking and elaboration, which is what runtimes
    // are given.
    let elaborated = spec
        .check_and_elaborate(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap();
    assert!(ensure_runtime_support(&elaborated, asynchronous, "async").is_err());
}

#[test]
fn a_reconfigurable_runtime_refuses_instances_but_not_declarations() {
    let used = program("in hit: Bool\nout y: Int = counters::count(hit)\n")
        .check_and_elaborate(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap();
    let refusal = ensure_no_stream_function_instances(&used, "reconf-dataflow").unwrap_err();
    assert_eq!(
        refusal.requirement.capability,
        RuntimeCapability::StreamFunctions
    );
    assert_eq!(refusal.runtime, "reconf-dataflow");
    // Enabling the experiment and importing stream functions instantiates nothing.
    let declared = program("in hit: Bool\nout y: Bool = hit\n")
        .check_and_elaborate(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap();
    ensure_no_stream_function_instances(&declared, "reconf-dataflow").unwrap();
    // Nor does a program that declares one of its own.
    let own = crate::lang::dsrv::parser::parse_str(
        r#"use experimental::{functions, local_streams}
def id(x: Int) -> Int = x
where { }
in x: Int
out y: Int = x
"#,
    )
    .unwrap();
    ensure_no_stream_function_instances(&own, "reconf-semi-sync").unwrap();
    assert!(
        own.declarations()
            .iter()
            .all(|declaration| !matches!(declaration, Declaration::Aux { .. }))
    );
}
