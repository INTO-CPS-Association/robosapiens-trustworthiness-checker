//! Struct update, `{ field: value, ..base }`: a struct built from another
//! with some of its fields replaced. What it parses and prints back as, what
//! checking accepts and refuses, and what it evaluates to on every runtime.

use std::collections::BTreeMap;
use std::rc::Rc;

use smol::LocalExecutor;

use crate::core::{ExecutionPolicy, OutputInterface, RuntimeSpec, Semantics, Value};
use crate::dsrv_fixtures::WithoutWarnings;
use crate::lang::dsrv::ElaboratedDsrvSpecification;
use crate::lang::dsrv::TypeCheckOptions;
use crate::lang::dsrv::ast::{DsrvSpecification, ExprView};
use crate::lang::dsrv::diagnostics::{SemanticError, TypeErrorKind};
use crate::lang::dsrv::parser::parse_str;
use crate::lang::dsrv::type_checker::TCType;
use crate::runtime::builder::GeneralRuntimeBuilder;
use crate::{ValueMap, VarName};

use test_log::test;

const BOTH: [TypeCheckOptions; 2] = [TypeCheckOptions::STRICT, TypeCheckOptions::GRADUAL];

const HEADER: &str = "use experimental::high_level_dsrv\n";
const POINT: &str = "type Point = Struct<x: Int, y: Int, label: Str>\n";

fn source(body: &str) -> String {
    format!("{HEADER}{POINT}in p: Point\n{body}")
}

fn parse(body: &str) -> DsrvSpecification {
    let source = source(body);
    parse_str(&source).unwrap_or_else(|error| panic!("{source}: {error}"))
}

fn check_errors(body: &str, options: TypeCheckOptions) -> Vec<SemanticError> {
    parse(body)
        .check(options)
        .without_warnings()
        .err()
        .unwrap_or_else(|| panic!("{}: expected checking to fail", source(body)))
}

fn kind_of(errors: &[SemanticError]) -> &TypeErrorKind {
    match &errors[0] {
        SemanticError::TypeError(error) => error.kind(),
        other => panic!("expected a type error, got {other:?}"),
    }
}

fn message_of(errors: &[SemanticError]) -> &str {
    match &errors[0] {
        SemanticError::TypeError(error) => error.message(),
        other => panic!("expected a type error, got {other:?}"),
    }
}

fn point_type() -> TCType {
    TCType::Struct(
        [
            ("x".into(), TCType::Int),
            ("y".into(), TCType::Int),
            ("label".into(), TCType::Str),
        ]
        .into_iter()
        .collect(),
        false,
    )
}

// -----------------------------------------------------------------------------
// Parsing
// -----------------------------------------------------------------------------

// The base comes last, after the fields it does not supply, as the rest `..`
// of a struct pattern does; a trailing comma and field shorthand are allowed
// as in a struct literal, and the update prints back as one.
#[test]
fn an_update_parses_with_its_base_last_and_prints_back() {
    for (written, printed_as) in [
        ("{ x: 1, ..p }", r#"{"x": 1, ..p}"#),
        ("{ x: 1, y: 2, ..p, }", r#"{"x": 1, "y": 2, ..p}"#),
        ("{ y, ..p }", r#"{"y": y, ..p}"#),
        ("{ ..p }", "{..p}"),
        (
            "{ label: \"moved\", ..{ x: 0, ..p } }",
            r#"{"label": "moved", ..{"x": 0, ..p}}"#,
        ),
    ] {
        let spec = parse(&format!("in y: Int\nout q: Point = {written}\n"));
        let expr = spec.var_expr_ref(&VarName::from("q")).unwrap();
        assert!(
            matches!(expr.view(), ExprView::StructUpdate(..)),
            "{written} is not an update: {expr}"
        );
        assert_eq!(expr.to_string(), printed_as, "{written}");
        let printed = spec.to_string();
        assert_eq!(
            parse_str(&printed).unwrap().to_string(),
            printed,
            "{written} did not round-trip"
        );
    }
}

#[test]
fn the_base_comes_last_and_only_once() {
    for written in [
        "{ ..p, x: 1 }",
        "{ x: 1, ..p, ..p }",
        "{ x: 1, .. }",
        "{ x: 1 ..p }",
    ] {
        let source = source(&format!("out q: Point = {written}\n"));
        assert!(parse_str(&source).is_err(), "{written} parsed");
    }
}

// -----------------------------------------------------------------------------
// Checking
// -----------------------------------------------------------------------------

#[test]
fn an_update_has_the_type_of_its_base() {
    for options in BOTH {
        let checked = parse("out q: Point = { x: p.x + 1, label: \"moved\", ..p }\n")
            .check(options)
            .without_warnings()
            .unwrap_or_else(|errors| panic!("{errors:?}"));
        let q = checked.var_expr_ref(&VarName::from("q")).unwrap();
        assert_eq!(q.typ(), &point_type(), "{options:?}");
    }
}

// A field is checked against the type the base gives it, so an `Int` is
// accepted where the base holds a `Float`.
#[test]
fn a_field_is_checked_against_the_base_field_type() {
    for options in BOTH {
        let source = format!(
            "{HEADER}in s: Struct<a: Float, b: Bool>\nout t: Struct<a: Float, b: Bool> = {{ a: 1, ..s }}\n"
        );
        let checked = parse_str(&source)
            .unwrap()
            .check(options)
            .without_warnings()
            .unwrap_or_else(|errors| panic!("{options:?}: {errors:?}"));
        let t = checked.var_expr_ref(&VarName::from("t")).unwrap();
        assert_eq!(
            t.typ().to_string(),
            "Struct<a: Float, b: Bool>",
            "{options:?}"
        );
    }
}

// A generic struct is updated at each instance, from a generic def.
#[test]
fn a_generic_struct_is_updated_in_a_generic_def() {
    let source = format!(
        "{HEADER}type Tick<D> = Struct<clock: D, n: Int, ok: Bool>\n\
         def bump<D>(t: Tick<D>) -> Tick<D> = {{ n: t.n + 1, ..t }}\n\
         def reset<D>(clock: D) -> Tick<D> = {{ clock, ..{{ clock, n: 0, ok: true }} }}\n\
         in t: Tick<Str>\nin f: Tick<Float>\n\
         out u: Tick<Str> = bump(t)\nout g: Tick<Float> = bump(reset(f.clock))\n"
    );
    for options in BOTH {
        parse_str(&source)
            .unwrap()
            .check(options)
            .without_warnings()
            .unwrap_or_else(|errors| panic!("{options:?}: {errors:?}"));
    }
    // A field the generic struct does not have is refused at its instance.
    let source = source.replace("{ n: t.n + 1, ..t }", "{ m: t.n + 1, ..t }");
    let errors = parse_str(&source)
        .unwrap()
        .check(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap_err();
    assert_eq!(kind_of(&errors), &TypeErrorKind::StructUnknownField);
}

#[test]
fn an_unknown_field_is_refused() {
    for options in BOTH {
        let errors = check_errors("out q: Point = { z: 1, ..p }\n", options);
        assert_eq!(kind_of(&errors), &TypeErrorKind::StructUnknownField);
        let message = message_of(&errors);
        assert!(
            message.contains("z") && message.contains("Struct<x: Int, y: Int, label: Str>"),
            "{message}"
        );
    }
}

#[test]
fn a_base_that_is_not_a_struct_is_refused() {
    for base in ["p.x", "Map(\"x\": 1)", "[p]"] {
        for options in BOTH {
            let errors = check_errors(&format!("out q: Point = {{ x: 1, ..{base} }}\n"), options);
            assert_eq!(kind_of(&errors), &TypeErrorKind::StructExpected, "{base}");
            assert!(
                message_of(&errors).contains("struct update"),
                "{base}: {errors:?}"
            );
        }
    }
}

// As in a struct literal, a field named twice is refused when the
// specification is read.
#[test]
fn a_duplicate_field_is_refused() {
    let source = source("out q: Point = { x: 1, x: 2, ..p }\n");
    let error = parse_str(&source).expect_err("a duplicate field");
    assert!(
        error.to_string().contains("duplicate field \"x\""),
        "{error}"
    );
}

#[test]
fn a_field_of_the_wrong_type_is_refused() {
    for options in BOTH {
        let errors = check_errors("out q: Point = { x: \"one\", ..p }\n", options);
        assert_eq!(kind_of(&errors), &TypeErrorKind::StructFieldTypeMismatch);
        let message = message_of(&errors);
        assert!(message.contains("field x"), "{message}");
    }
}

// The update's type is its base's, so it cannot stand where another struct
// type is expected.
#[test]
fn an_update_is_not_a_struct_of_another_type() {
    let errors = check_errors(
        "out q: Struct<x: Int, y: Int> = { x: 1, ..p }\n",
        TypeCheckOptions::STRICT,
    );
    assert!(!errors.is_empty());
}

// Under gradual checking a base of unknown type is accepted, as a field read
// of one is.
#[test]
fn a_base_of_unknown_type_is_gradual() {
    let source = format!("{HEADER}in a\nout b = {{ x: 1, ..a }}\n");
    parse_str(&source)
        .unwrap()
        .check(TypeCheckOptions::GRADUAL)
        .without_warnings()
        .unwrap_or_else(|errors| panic!("{errors:?}"));
}

// -----------------------------------------------------------------------------
// Evaluation
// -----------------------------------------------------------------------------

const PROGRAM: &str = "\
use experimental::high_level_dsrv
type Point = Struct<x: Int, y: Int, label: Str>
def moved(p: Point, dx: Int) -> Point = { x: p.x + dx, ..p }
in p: Point
in dx: Int
aux origin: Point = { x: 0, y: 0, ..p }
out shifted: Point = moved(p, dx)
out relabelled: Point = { label: \"origin\", ..origin }
out picked: Point = match(dx) {
  0 -> p,
  n if n > 0 -> { y: n, ..p },
  _ -> { x: dx, y: dx, ..moved(p, 1) },
}
out same: Point = { ..p }
out previous: Point = { label: \"previous\", ..default(p[1], origin) }
in text: Str
out evaluated: Point = dynamic(text: Point)
";

fn point(x: i64, y: i64, label: &str) -> Value {
    Value::Map(ValueMap::from([
        ("x".into(), Value::Int(x)),
        ("y".into(), Value::Int(y)),
        ("label".into(), Value::Str(label.into())),
    ]))
}

fn elaborate(options: TypeCheckOptions) -> ElaboratedDsrvSpecification {
    ElaboratedDsrvSpecification::parse_with(PROGRAM, options)
        .unwrap_or_else(|error| panic!("{error}"))
        .without_warnings()
        .unwrap_or_else(|errors| panic!("{options:?}: {errors:?}"))
}

/// Run `spec` on `runtime` under `semantics`, returning what it published for
/// each output, in order.
fn run(
    runtime: RuntimeSpec,
    semantics: Semantics,
    spec: ElaboratedDsrvSpecification,
    inputs: BTreeMap<VarName, Vec<Value>>,
) -> BTreeMap<VarName, Vec<Value>> {
    let executor = Rc::new(LocalExecutor::new());
    let (sender, mut receiver) = crate::io::channel::output(1024);
    let outputs = spec.output_vars().clone();
    let run = {
        let executor = Rc::clone(&executor);
        async move {
            let writer = crate::io::channel::open_output(
                sender,
                OutputInterface::outputs(outputs.iter().cloned()).expect("an interface"),
            )
            .await
            .expect("the channel opens");
            let monitor = GeneralRuntimeBuilder::new()
                .executor(Rc::clone(&executor))
                .model(spec)
                .input(crate::io::map::input_stream(inputs))
                .output_writer(writer)
                .runtime(runtime)
                .semantics(semantics)
                .build()
                .await
                .expect("the runtime builds");
            crate::core::Runtime::run(monitor).await
        }
    };
    smol::block_on(executor.run(run))
        .unwrap_or_else(|error| panic!("{runtime:?}/{semantics:?}: {error:#}"));
    let mut published: BTreeMap<VarName, Vec<Value>> = BTreeMap::new();
    while let Ok(row) = receiver.try_recv() {
        for (var, value) in row {
            published.entry(var).or_default().push(value);
        }
    }
    published
}

// In a def, an auxiliary and an output equation, inside match arms, over a
// past value and in text supplied at run time, an update replaces exactly
// the fields it names, on every runtime and under every semantics that runs
// structs.
#[test]
fn an_update_runs_the_same_on_every_runtime() {
    let inputs = BTreeMap::from([
        (
            VarName::from("p"),
            vec![point(1, 2, "a"), point(3, 4, "b"), point(5, 6, "c")],
        ),
        (
            VarName::from("dx"),
            vec![Value::Int(0), Value::Int(10), Value::Int(-1)],
        ),
        (
            VarName::from("text"),
            vec![
                Value::Str("{ x: 7, ..p }".into()),
                Value::Str("{ ..p }".into()),
                Value::Str("{ y: dx, label: \"text\", ..p }".into()),
            ],
        ),
    ]);
    let expected = BTreeMap::from([
        (
            VarName::from("shifted"),
            vec![point(1, 2, "a"), point(13, 4, "b"), point(4, 6, "c")],
        ),
        (
            VarName::from("relabelled"),
            vec![
                point(0, 0, "origin"),
                point(0, 0, "origin"),
                point(0, 0, "origin"),
            ],
        ),
        (
            VarName::from("picked"),
            vec![point(1, 2, "a"), point(3, 10, "b"), point(-1, -1, "c")],
        ),
        (
            VarName::from("same"),
            vec![point(1, 2, "a"), point(3, 4, "b"), point(5, 6, "c")],
        ),
        (
            VarName::from("previous"),
            vec![
                point(0, 0, "previous"),
                point(1, 2, "previous"),
                point(3, 4, "previous"),
            ],
        ),
        (
            VarName::from("evaluated"),
            vec![point(7, 2, "a"), point(3, 4, "b"), point(5, -1, "text")],
        ),
    ]);
    for (options, semantics) in [
        (TypeCheckOptions::STRICT, Semantics::TypedUntimed),
        (TypeCheckOptions::GRADUAL, Semantics::GradualTypedUntimed),
        (TypeCheckOptions::GRADUAL, Semantics::Untimed),
    ] {
        for runtime in [
            RuntimeSpec::Dataflow(ExecutionPolicy::Synchronous),
            RuntimeSpec::Dataflow(ExecutionPolicy::Buffered),
            RuntimeSpec::SemiSync,
            RuntimeSpec::Async,
        ] {
            let published = run(runtime, semantics, elaborate(options), inputs.clone());
            assert_eq!(published, expected, "{runtime:?}/{semantics:?}");
        }
    }
}

// A constant may be built by updating another, and folds to the struct it
// names; an unknown field is refused as it is in an equation.
#[test]
fn an_update_folds_in_a_constant() {
    let constants = "const home: Point = { x: 0, y: 0, label: \"home\" }\n";
    let spec = parse(&format!(
        "{constants}const away: Point = {{ label: \"away\", ..home }}\nout q: Point = away\n"
    ));
    let q = spec.var_expr_ref(&VarName::from("q")).unwrap().to_string();
    assert!(q.contains("\"away\"") && !q.contains("home"), "{q}");

    let source = source(&format!(
        "{constants}const away: Point = {{ z: 1, ..home }}\nout q: Point = away\n"
    ));
    let error = parse_str(&source).expect_err("an unknown field");
    assert!(error.to_string().contains("field z"), "{error}");
}
