//! `const`: a value folded once and written in wherever it is named.
//!
//! A constant is not a stream and is never emitted. It is folded where the
//! file is read, so what reaches the core tree is a literal, and a constant
//! that could not be worked out is refused there rather than at runtime.

use super::support::{check_errors, elaborate, expand, inputs, is_type_error, run, run_both};
use crate::core::{ExecutionPolicy, RuntimeSpec, Semantics};
use crate::dataflow::TypedDataflowMonitor;
use crate::dsrv_fixtures::WithoutWarnings;
use crate::lang::dsrv::LanguageError;
use crate::lang::dsrv::ast::{DsrvSpecification, ExprView};
use crate::lang::dsrv::diagnostics::SemanticError;
use crate::lang::dsrv::expand::DsrvExpandError;
use crate::lang::dsrv::modules::{ModuleCollector, show_path};
use crate::lang::dsrv::parser::{DsrvParseError, parse_str};
use crate::lang::dsrv::path::ModuleName;
use crate::lang::dsrv::source_map::SourceLabel;
use crate::{Value, VarName};

use test_log::test;

const C: &str = "use experimental::{constants}\n";
const CM: &str = "use experimental::{constants, modules}\n";

fn spec(source: &str) -> DsrvSpecification {
    parse_str(source).unwrap_or_else(|error| panic!("{source}: {error}"))
}

/// How `y`'s body prints, which is what folding produced.
fn body(source: &str) -> String {
    let spec = spec(source);
    let expr = spec
        .var_expr_ref(&VarName::from("y"))
        .expect("y is defined");
    match expr.view() {
        ExprView::Ascribe(value, _) => value.to_string(),
        _ => expr.to_string(),
    }
}

/// The message of whatever refused this source.
fn refusal(source: &str) -> String {
    match parse_str(source) {
        Err(error) => error.to_string(),
        Ok(_) => panic!("expected a refusal for {source:?}"),
    }
}

fn language_error(source: &str) -> LanguageError {
    match parse_str(source) {
        Err(DsrvParseError::Language(error)) => error,
        other => panic!("expected a language error for {source:?}, got {other:?}"),
    }
}

/// Expand a program of several files, supplying each module as it is asked
/// for.
fn program(root: &str, sources: &[(&str, &str)]) -> DsrvSpecification {
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
    crate::lang::dsrv::expand::expand_program(collector.finish().expect("collected"), None)
        .expect("expands")
}

fn program_body(root: &str, sources: &[(&str, &str)], var: &str) -> String {
    let spec = program(root, sources);
    let expr = spec
        .var_expr_ref(&VarName::from(var))
        .expect("the variable is defined");
    match expr.view() {
        ExprView::Ascribe(value, _) => value.to_string(),
        _ => expr.to_string(),
    }
}

// ---------------------------------------------------------------------------
// What a constant stands for
// ---------------------------------------------------------------------------

/// A cast in a constant makes the conversion a cast makes when it runs, so
/// `as Float` does what the refusal of `2.0 * n` recommends.
#[test]
fn a_cast_in_a_constant_converts_as_it_does_at_runtime() {
    for (declaration, typ, expected) in [
        (
            "const n: Int = 3\nconst c: Float = 2.0 * (n as Float)",
            "Float",
            "6.0",
        ),
        (
            "const c: Float = 2.0 * (n as Float)\nconst n: Int = 3",
            "Float",
            "6.0",
        ),
        ("const c: Float = 3 as Float", "Float", "3.0"),
        (
            "const n: Int = 3\nconst c: Float = n as Float",
            "Float",
            "3.0",
        ),
        ("const c: Str = 3 as Str", "Str", "\"3\""),
        ("const c: Str = 2.5 as Str", "Str", "\"2.5\""),
        ("const c: Str = true as Str", "Str", "\"true\""),
        ("const c: Str = \"s\" as Str", "Str", "\"s\""),
        ("const c: Int = 3 as Int", "Int", "3"),
        ("const c: Float = 2.5 as Float", "Float", "2.5"),
        // An alias of the type works as it does in an equation.
        (
            "type Real = Float\nconst n: Int = 3\nconst c: Real = n as Real",
            "Float",
            "3.0",
        ),
        ("const c: List<Int> = [] as List<Int>", "List<Int>", "[]"),
        ("const c: List<Int> = [1] as List<Int>", "List<Int>", "[1]"),
    ] {
        let source = format!("{C}{declaration}\nin x: Int\nout y: {typ}\ny = c\n");
        assert_eq!(body(&source), expected, "{declaration}");
    }
}

/// A struct stays a struct through a cast of its own type, so its fields can
/// still be read.
#[test]
fn a_cast_of_a_struct_constant_to_its_type_stays_a_struct() {
    let source = format!(
        "{C}type S = Struct<a: Int>\nconst c: S = {{a: 1}} as S\n\
         out y: Int\ny = c.a\n"
    );
    crate::dsrv_fixtures::checked(&source);
}

/// A cast a run-time value could not make is refused where the file is read,
/// with what could not be converted to what, located at the cast.
#[test]
fn a_cast_in_a_constant_that_cannot_convert_is_refused() {
    for (declaration, reported) in [
        (
            "const n: Float = 1.5\nconst c: Int = n as Int",
            "cannot cast Float to Int",
        ),
        ("const c: Int = 1.5 as Int", "cannot cast Float to Int"),
        (
            "const c: Float = \"s\" as Float",
            "cannot cast Str to Float",
        ),
        ("const c: Int = true as Int", "cannot cast Bool to Int"),
        ("const c: Str = [1] as Str", "cannot cast List<Int> to Str"),
        (
            "const c: List<Float> = [1] as List<Float>",
            "cannot cast List<Int> to List<Float>",
        ),
    ] {
        let source = format!("{C}{declaration}\nin x: Int\nout y: Int\ny = x\n");
        let error = refusal(&source);
        assert!(error.contains(reported), "{declaration}: {error}");
    }
}

/// Without the cast it stays what it was: `Int` and `Float` do not convert
/// implicitly, in a constant as in an equation.
#[test]
fn a_constant_mixing_int_and_float_is_still_refused_without_a_cast() {
    let error = refusal(&format!(
        "{C}const n: Int = 3\nconst c: Float = 2.0 * n\nin x: Int\nout y: Int\ny = x\n"
    ));
    assert!(error.contains("as Float"), "{error}");
}

#[test]
fn a_constant_is_written_in_as_a_literal() {
    let printed = body(&format!(
        "{C}const limit: Int = 3\nin x: Int\nout y: Int\ny = x + limit\n"
    ));
    assert!(
        printed.contains('3') && !printed.contains("limit"),
        "got {printed}",
    );
}

#[test]
fn a_constant_may_be_arithmetic_over_literals() {
    let printed = body(&format!(
        "{C}const limit: Int = 2 * 3 + 1\nin x: Int\nout y: Int\ny = limit\n"
    ));
    assert!(printed.contains('7'), "folded to 7, got {printed}");
}

/// An integer literal takes the type its context expects, and a constant's
/// declared type is such a context, as an equation's is.
#[test]
fn an_integer_literal_takes_a_constants_declared_float_type() {
    for (declaration, literal) in [
        ("const c: Float = 3", "3.0"),
        ("const c: Float = -3", "-3.0"),
        ("const c: Float = 2 + 1", "3.0"),
        ("const c: Float = 2 * 3 - 1", "5.0"),
        ("const c: Float = 3.0 * 2", "6.0"),
        ("const c: Float = 2 * 3.0", "6.0"),
        ("const a: Float = 3\nconst c: Float = a * 2", "6.0"),
        ("const c: Float = a * 2\nconst a: Float = 3", "6.0"),
    ] {
        let source = format!("{C}{declaration}\nin x: Int\nout y: Float\ny = c\n");
        assert_eq!(body(&source), literal, "{source}");
        spec(&source)
            .check()
            .without_warnings()
            .unwrap_or_else(|errors| panic!("{source}: {errors:?}"));
    }
}

#[test]
fn an_integer_literal_takes_the_float_type_of_an_aggregate_constant() {
    for (declaration, typ, literal) in [
        ("[1, 2.5]", "List<Float>", "[1.0, 2.5]"),
        ("Tuple(1, 2)", "(Float, Int)", "Tuple(1.0, 2)"),
        ("[[1], [2]]", "List<List<Float>>", "[[1.0], [2.0]]"),
        (
            "{ a: 1, b: 2 }",
            "Struct<a: Float, b: Int>",
            "Struct(\"a\": 1.0, \"b\": 2)",
        ),
    ] {
        let source = format!("{C}const c: {typ} = {declaration}\nin x: Int\nout y: {typ}\ny = c\n");
        assert_eq!(body(&source), literal, "{source}");
    }
}

#[test]
fn only_what_a_written_expression_would_adopt_becomes_a_float() {
    for (declaration, message) in [
        // Integer division is `Int` whatever its context.
        ("const c: Float = 7 / 2", "expected Float"),
        // A name is never flexible, a constant's included.
        ("const n: Int = 3\nconst c: Float = n", "expected Float"),
        ("const n: Int = 3\nconst c: Float = 2.0 * n", "as Float"),
        ("const c: Float = 9007199254740993", "exactly"),
        ("const c: Int = 3.0", "expected Int"),
        ("const c: Str = 3", "expected Str"),
        ("const c: List<Int> = [1.5]", "expected Int"),
    ] {
        let source = format!("{C}{declaration}\nin x: Int\nout y: Int\ny = x\n");
        let error = refusal(&source);
        assert!(error.contains(message), "{source}: {error}");
    }
}

#[test]
fn a_constant_may_be_defined_from_another() {
    let printed = body(&format!(
        "{C}const base: Int = 4\nconst limit: Int = base * 2\nin x: Int\nout y: Int\ny = limit\n"
    ));
    assert!(printed.contains('8'), "folded to 8, got {printed}");
}

/// A file is read as a whole, so a constant may be written after the one
/// that is defined from it.
#[test]
fn a_constant_declared_after_the_one_that_names_it_is_still_folded() {
    let printed = body(&format!(
        "{C}const limit: Int = base * 2\nconst base: Int = 4\nin x: Int\nout y: Int\ny = limit\n"
    ));
    assert!(printed.contains('8'), "folded to 8, got {printed}");
}

#[test]
fn a_constant_may_be_a_list_literal() {
    let printed = body(&format!(
        "{C}const sizes: List<Int> = [1, 2]\nin x: Int\nout y: List<Int>\ny = sizes\n"
    ));
    assert!(
        printed.contains('1') && printed.contains('2'),
        "got {printed}"
    );
}

#[test]
fn a_constant_is_written_in_at_each_use() {
    let printed = body(&format!(
        "{C}const limit: Int = 3\nin x: Int\nout y: Int\ny = limit + limit\n"
    ));
    assert_eq!(printed.matches('3').count(), 2, "got {printed}");
}

#[test]
fn a_constant_may_be_named_in_a_def_body() {
    let printed = body(&format!(
        "use experimental::{{constants, functions}}\nconst factor: Int = 2\n\
         def scaled(n: Int) -> Int = n * factor\nin x: Int\nout y: Int\ny = scaled(x)\n"
    ));
    assert!(
        printed.contains('2') && !printed.contains("factor"),
        "got {printed}",
    );
}

/// A def and a constant both own a parsed root but produce no declaration.
/// If either were consumed out of order, every later equation would take
/// the wrong tree — which type-checks and gives wrong answers.
#[test]
fn an_equation_after_a_constant_still_takes_its_own_expression() {
    let printed = body(&format!(
        "{C}const limit: Int = 3\nin x: Int\nout y: Int\ny = x + 1\n"
    ));
    assert!(
        printed.contains('+') && printed.contains('1'),
        "y kept its own body, got {printed}",
    );
}

#[test]
fn a_constant_and_a_def_may_be_interleaved_with_equations() {
    let source = format!(
        "use experimental::{{constants, functions}}\nin x: Int\nout a: Int\n\
         const limit: Int = 3\na = x + 1\ndef twice(n: Int) -> Int = n * 2\n\
         out y: Int\ny = twice(x) + limit\n"
    );
    let printed = body(&source);
    assert!(
        printed.contains('2') && printed.contains('3'),
        "got {printed}"
    );
    let other = spec(&source)
        .var_expr_ref(&VarName::from("a"))
        .expect("a is defined")
        .to_string();
    assert!(other.contains('1') && !other.contains('2'), "got {other}");
}

// ---------------------------------------------------------------------------
// What a constant may not be built from
// ---------------------------------------------------------------------------

#[test]
fn a_constant_naming_a_stream_is_refused() {
    let message = refusal(&format!(
        "{C}in x: Int\nconst limit: Int = x + 1\nout y: Int\ny = limit\n"
    ));
    assert!(message.contains("names no constant"), "got {message}");
}

#[test]
fn a_constant_holding_a_stream_operation_is_refused() {
    let message = refusal(&format!(
        "{C}in x: Int\nconst limit: Int = default(1, 2)\nout y: Int\ny = limit\n"
    ));
    assert!(
        message.contains("cannot be built from a stream operation"),
        "got {message}",
    );
}

#[test]
fn a_constant_holding_a_runtime_expression_is_refused() {
    let message = refusal(&format!(
        "{C}in x: Int\nconst limit: Int = dynamic(\"1\": Int)\nout y: Int\ny = limit\n"
    ));
    assert!(
        message.contains("text supplied at runtime"),
        "got {message}",
    );
}

/// Arithmetic is folded where the file is read, so a failure is reported
/// there rather than once the program is running.
#[test]
fn a_constant_whose_arithmetic_fails_is_refused() {
    let message = refusal(&format!(
        "{C}const limit: Int = 1 / 0\nin x: Int\nout y: Int\ny = limit\n"
    ));
    assert!(message.contains("could not be worked out"), "got {message}",);
}

#[test]
fn a_constant_defined_in_terms_of_itself_is_refused() {
    let message = refusal(&format!(
        "{C}const limit: Int = limit + 1\nin x: Int\nout y: Int\ny = limit\n"
    ));
    assert!(message.contains("in terms of itself"), "got {message}");
}

#[test]
fn two_constants_defined_in_terms_of_each_other_are_refused() {
    let message = refusal(&format!(
        "{C}const a: Int = b + 1\nconst b: Int = a + 1\nin x: Int\nout y: Int\ny = a\n"
    ));
    assert!(message.contains("in terms of itself"), "got {message}");
}

#[test]
fn a_constant_whose_declared_type_is_unknown_is_refused() {
    let message = refusal(&format!(
        "{C}const limit: Missing = 3\nin x: Int\nout y: Int\ny = limit\n"
    ));
    assert!(message.contains("Missing"), "got {message}");
}

#[test]
fn a_constant_value_must_have_its_declared_type() {
    let source = format!("{C}type Flag = Bool\nconst wrong: Flag = 1\nout y: Int\ny = wrong\n");
    let message = refusal(&source);
    assert!(message.contains("constant `wrong`"), "got {message}");
    assert!(message.contains("expected Bool"), "got {message}");
    assert!(message.contains("Span"), "got {message}");
}

#[test]
fn an_imported_constant_type_error_names_its_source() {
    let root = format!("{CM}mod lib\nuse lib::*\nout y: Int\ny = wrong\n");
    let mut collector =
        ModuleCollector::with_label(&root, SourceLabel::Path("root.dsrv".into())).unwrap();
    collector
        .supply_labelled(
            &format!("{CM}const wrong: Bool = 1\n"),
            SourceLabel::Path("library.dsrv".into()),
        )
        .unwrap();
    let error = crate::lang::dsrv::expand::expand_program(collector.finish().unwrap(), None)
        .expect_err("the imported constant has the wrong type");
    let message = error.to_string();
    assert!(message.contains("library.dsrv"), "got {message}");
    assert!(message.contains("Span"), "got {message}");
}

#[test]
fn declared_int_constants_do_not_become_fresh_float_literals() {
    let declaration = "const n: Int = 3\n";
    for body in ["const c: Float = n\n", "const c: Float = 2.0 * n\n"] {
        assert!(parse_str(&format!("{C}{declaration}{body}out y: Int = 0\n")).is_err());
    }
    for expression in ["n", "2.0 * n"] {
        let source = format!("{C}{declaration}out y: Float = {expression}\n");

        assert!(spec(&source).check().discard_warnings().is_err());
    }
    let source = format!("{C}{declaration}const c: Float = 2.0 * (n as Float)\nout y: Float = c\n");
    assert_eq!(body(&source), "6.0");
    let source = format!("{C}const c: Float = 3\nout y: Float = c\n");
    assert_eq!(body(&source), "3.0");
}

#[test]
fn checked_constant_dependencies_work_as_offsets_and_contextual_results() {
    let source = "use experimental::{constants}\nconst n: Int = seed * 2\nconst seed: Int = 1\nin x: Int\nout y: Int = default(x[n], 0)\n";
    assert!(body(source).contains("x[2]"));
}

#[test]
fn resolved_imported_constant_types_keep_the_writers_experiments() {
    let library = "use experimental::{constants, tagged_unions, modules}\ntype U = Union<A>\nconst empty: List<U> = []\n";
    let root = "use experimental::{constants, modules}\nmod lib\nuse lib\nout y = lib::empty\n";
    program(root, &[("lib", library)])
        .check()
        .discard_warnings()
        .expect("imported resolved type is not caller syntax");
    for source in [
        "use experimental::{constants}\nconst empty: List<Union<A>> = []\nout y: List<Union<A>> = empty\n",
        "out y: List<Union<A>> = [] as List<Union<A>>\n",
    ] {
        assert!(matches!(
            language_error(source),
            LanguageError::NeedsExperiment {
                feature: "tagged_unions",
                ..
            }
        ));
    }
}

// ---------------------------------------------------------------------------
// Gating
// ---------------------------------------------------------------------------

#[test]
fn a_constant_needs_the_constants_experiment() {
    let error = language_error("const limit: Int = 3\nin x: Int\n");
    assert!(
        matches!(
            &error,
            LanguageError::NeedsExperiment {
                feature: "constants",
                ..
            }
        ),
        "got {error:?}",
    );
}

#[test]
fn an_internal_constant_needs_the_modules_experiment_too() {
    let error = language_error(&format!("{C}internal const limit: Int = 3\nin x: Int\n"));
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
// Constants across modules
// ---------------------------------------------------------------------------

#[test]
fn a_glob_brings_a_constant_in_bare() {
    let printed = program_body(
        &format!("{CM}mod store\nuse store::*\nin x: Int\nout y: Int\ny = x + limit\n"),
        &[("store", &format!("{CM}const limit: Int = 3\n"))],
        "y",
    );
    assert!(
        printed.contains('3') && !printed.contains("limit"),
        "got {printed}",
    );
}

#[test]
fn a_constant_may_be_named_through_its_module() {
    let printed = program_body(
        &format!("{CM}mod store\nuse store\nin x: Int\nout y: Int\ny = x + store::limit\n"),
        &[("store", &format!("{CM}const limit: Int = 3\n"))],
        "y",
    );
    assert!(
        printed.contains('3') && !printed.contains("limit"),
        "got {printed}",
    );
}

/// A glob skips an internal one silently, exactly as it
/// skips an internal type or def.
#[test]
fn a_glob_does_not_bring_an_internal_constant() {
    let printed = program_body(
        &format!("{CM}mod store\nuse store::*\nin x: Int\nout y: Int\ny = x\n"),
        &[("store", &format!("{CM}internal const limit: Int = 3\n"))],
        "y",
    );
    assert!(!printed.contains('3'), "got {printed}");
}

#[test]
fn a_local_constant_wins_over_an_imported_one() {
    let printed = program_body(
        &format!(
            "{CM}mod store\nuse store::*\nconst limit: Int = 9\nin x: Int\nout y: Int\ny = limit\n"
        ),
        &[("store", &format!("{CM}const limit: Int = 3\n"))],
        "y",
    );
    assert!(
        printed.contains('9') && !printed.contains('3'),
        "got {printed}"
    );
}

#[test]
fn a_constant_may_be_defined_from_one_in_another_module() {
    let printed = program_body(
        &format!("{CM}mod store\nuse store::*\nin x: Int\nout y: Int\ny = doubled\n"),
        &[(
            "store",
            &format!("{CM}const base: Int = 4\nconst doubled: Int = base * 2\n"),
        )],
        "y",
    );
    assert!(printed.contains('8'), "got {printed}");
}

#[test]
fn a_constant_kept_internal_is_still_usable_in_its_own_module() {
    let printed = program_body(
        &format!("{CM}mod store\nuse store::*\nin x: Int\nout y: Int\ny = doubled\n"),
        &[(
            "store",
            &format!("{CM}internal const base: Int = 4\nconst doubled: Int = base * 2\n"),
        )],
        "y",
    );
    assert!(printed.contains('8'), "got {printed}");
}

#[test]
fn nested_expression_literals_keep_their_declared_context() {
    for (typ, literal) in [
        ("(Expr<Int>, Int)", "Tuple(\"1\", 0)"),
        ("Struct<a: (Expr<Int>, Int)>", "{a: Tuple(\"1\", 0)}"),
        ("(Struct<a: Expr<Int>>, Int)", "Tuple({a: \"1\"}, 0)"),
    ] {
        let source = format!(
            "{C}const first: {typ} = {literal}\nconst second: {typ} = first\nout y: {typ} = second\n"
        );

        spec(&source)
            .check()
            .without_warnings()
            .unwrap_or_else(|error| panic!("{source}: {error:?}"));
    }
}

// ---------------------------------------------------------------------------
// A constant as a stream offset
// ---------------------------------------------------------------------------

/// An offset is a number, not an expression, so a constant standing in for
/// one is resolved to the number itself rather than written in as a tree.
#[test]
fn a_constant_may_be_named_as_a_stream_offset() {
    let printed = body(&format!(
        "{C}const window: Int = 2\nin x: Int\nout y: Int\ny = x[window]\n"
    ));
    assert!(
        printed.contains("x[2]") && !printed.contains("window"),
        "got {printed}",
    );
}

#[test]
fn a_capitalised_constant_may_be_named_as_a_stream_offset() {
    let printed = body(&format!(
        "{C}const WINDOW: Int = 3\nin x: Int\nout y: Int\ny = default(x[WINDOW], 0)\n"
    ));
    assert!(
        printed.contains("x[3]") && !printed.contains("WINDOW"),
        "got {printed}",
    );
}

#[test]
fn a_capitalised_constant_offset_may_be_named_through_its_module() {
    let printed = program_body(
        &format!("{CM}mod store\nuse store\nin x: Int\nout y: Int\ny = x[store::WINDOW]\n"),
        &[("store", &format!("{CM}const WINDOW: Int = 2\n"))],
        "y",
    );
    assert!(printed.contains("x[2]"), "got {printed}");
}

#[test]
fn a_stream_offset_may_still_be_written_as_a_number() {
    let printed = body("in x: Int\nout y: Int\ny = x[2]\n");
    assert!(printed.contains("x[2]"), "got {printed}");
}

#[test]
fn a_constant_offset_may_be_named_through_its_module() {
    let printed = program_body(
        &format!("{CM}mod store\nuse store\nin x: Int\nout y: Int\ny = x[store::window]\n"),
        &[("store", &format!("{CM}const window: Int = 2\n"))],
        "y",
    );
    assert!(printed.contains("x[2]"), "got {printed}");
}

#[test]
fn a_negative_constant_is_refused_as_a_stream_offset() {
    let message = refusal(&format!(
        "{C}const window: Int = 0 - 1\nin x: Int\nout y: Int\ny = x[window]\n"
    ));
    assert!(message.contains("is not a count"), "got {message}");
}

#[test]
fn a_constant_that_is_not_a_number_is_refused_as_a_stream_offset() {
    let message = refusal(&format!(
        "{C}const window: Str = \"two\"\nin x: Int\nout y: Int\ny = x[window]\n"
    ));
    assert!(message.contains("is not a count"), "got {message}");
}

#[test]
fn an_offset_naming_no_constant_is_refused() {
    let message = refusal(&format!(
        "{C}const other: Int = 2\nin x: Int\nout y: Int\ny = x[window]\n"
    ));
    assert!(message.contains("names no constant"), "got {message}");
}

#[test]
fn a_constant_cannot_be_indexed() {
    let message = refusal(&format!(
        "{C}const k: Int = 2\nin x: Int\nout y: Int\ny = x + k[1]\n"
    ));
    assert!(message.contains("`k` is a constant"), "got {message}");
}

/// A parameter that shadows a constant is the parameter's, and is refused
/// as a parameter rather than as a constant.
#[test]
fn a_parameter_shadowing_a_constant_is_refused_as_a_parameter() {
    let message = refusal(
        "use experimental::{constants, functions}\n\
         const k: Int = 2\ndef prev(k: Int) -> Int = k[1]\nin x: Int\nout y: Int\ny = prev(x)\n",
    );
    assert!(
        message.contains("`k`") && message.contains("parameter"),
        "got {message}"
    );
}

/// The spelling parses for every file, so what refuses it without the
/// experiment is expansion, naming the experiment to add.
#[test]
fn a_named_stream_offset_needs_the_constants_experiment() {
    let error = language_error("in x: Int\nout y: Int\ny = x[window]\n");
    assert!(
        matches!(
            &error,
            LanguageError::NeedsExperiment {
                construct: "a named stream offset",
                feature: "constants",
                ..
            }
        ),
        "got {error:?}",
    );
}

#[test]
fn arithmetic_constants_keep_the_typed_path() {
    let source = "use experimental::{constants}\n\
                  const lag: Int = 1 * 2\n\
                  const scale: Float = 3.0 / 2.0 + 0.5\n\
                  in x: Float\n\
                  out y: Float = default(x[lag], 0.0) * scale\n";
    let spec = parse_str(source).unwrap();
    let printed = spec.var_expr_ref(&VarName::from("y")).unwrap().to_string();
    assert!(
        printed.contains("x[2]") && printed.contains("2.0") && !printed.contains("scale"),
        "{printed}"
    );
    let elaborated = elaborate(spec);
    let mut typed = TypedDataflowMonitor::<(f64,), (f64,)>::compile_checked(elaborated)
        .expect("a scalar program binds typed rows");
    assert_eq!(typed.evaluate(&(1.0,)), (0.0,));
    assert_eq!(typed.evaluate(&(2.0,)), (0.0,));
    assert_eq!(typed.evaluate(&(3.0,)), (2.0,));
}

#[test]
fn constants_refuse_calls_even_when_the_def_is_available() {
    let header = "use experimental::{functions, constants}\n\
                  def twice(n: Int) -> Int = n * 2\n";
    for (initializer, construct) in [
        ("twice(1)", "a call"),
        ("missing(1)", "a call"),
        ("(\\n: Int -> n)(1)", "a function"),
    ] {
        let source = format!("{header}const c: Int = {initializer}\n");
        assert!(refusal(&source).contains(&format!("cannot be built from {construct}")));
    }
    let root = "use experimental::{functions, constants, modules}\nmod numbers\nuse numbers\n\
                const c: Int = numbers::twice(1)\n";
    let error = expand(root, &[("numbers", header)]).expect_err("imported calls are refused too");
    assert!(matches!(
        error,
        DsrvExpandError::NotConstant {
            construct: "a call",
            ..
        }
    ));
}

// Constants and equations checked against the same expected type.

const CONSTANTS: &str = "use experimental::{constants}\n";

/// Keep the expression and expected type identical. The extra output supplies
/// a finite tick clock on dataflow/semi-sync without changing the expression's
/// checking context. Async's independent constant outputs need not end with it.
fn paired_sources(header: &str, ty: &str, expression: &str) -> [String; 2] {
    [true, false].map(|constant| {
        let (declaration, body) = if constant {
            (format!("const c: {ty} = {expression}\n"), "c")
        } else {
            (String::new(), expression)
        };
        format!("{header}in pulse: Int\n{declaration}out y: {ty} = {body}\nout tick: Int = pulse\n")
    })
}

fn successful_pair(
    header: &str,
    modules: &[(&str, &str)],
    ty: &str,
    expression: &str,
    expected: Value,
) {
    for source in paired_sources(header, ty, expression) {
        let spec = expand(&source, modules).unwrap_or_else(|error| panic!("{source}: {error}"));

        let (rows, failure) = run_both(
            elaborate(spec.clone()),
            inputs([("pulse", vec![Value::Int(1), Value::Int(2)])]),
        );
        assert_eq!(failure, None, "{source}");
        assert_eq!(rows.len(), 2, "{source}: {rows:?}");
        for row in rows {
            assert_eq!(row[&VarName::from("y")], expected, "{source}");
        }
    }
}

fn initializer_range(source: &str, prefix: &str, expression: &str) -> std::ops::Range<u32> {
    let start =
        u32::try_from(source.find(prefix).expect("the declaration") + prefix.len()).unwrap();
    start..start + u32::try_from(expression.len()).unwrap()
}

/// A constant's ordinary type error occurs during expansion; the equation's
/// same error occurs during checking. Neither may be accepted for execution.
fn type_rejected_pair(header: &str, modules: &[(&str, &str)], ty: &str, expression: &str) {
    let [constant, equation] = paired_sources(header, ty, expression);
    let error = expand(&constant, modules).expect_err("the initializer must not type-check");
    let DsrvExpandError::ConstantCheck { errors, .. } = error else {
        panic!("expected ordinary initializer type errors: {constant}: {error}");
    };
    assert!(is_type_error(&errors), "{constant}: {errors:?}");
    let range = initializer_range(&constant, &format!("const c: {ty} = "), expression);
    assert!(
        errors
            .iter()
            .any(|error| matches!(error, SemanticError::TypeError(error)
        if error.span().is_some_and(|span| span.start >= range.start && span.end <= range.end))),
        "initializer diagnostic: {constant}: {errors:?}"
    );
    let spec = expand(&equation, modules).expect("the equation expands before checking");

    let errors = check_errors(spec.clone());
    assert!(is_type_error(&errors), "{equation}: {errors:?}");
    let range = initializer_range(&equation, &format!("out y: {ty} = "), expression);
    assert!(errors.iter().any(|error| matches!(error, SemanticError::TypeError(error)
            if error.span().is_some_and(|span| span.start >= range.start && span.end <= range.end))),
            "equation diagnostic: {equation}: {errors:?}");
}

#[test]
fn constants_and_equations_use_the_same_expected_type() {
    let aggregate = Value::Tuple(
        vec![
            Value::Float(3.0),
            Value::List(vec![Value::Float(1.0), Value::Float(2.0)].into()),
            Value::Map([("a".into(), Value::Float(4.0))].into()),
        ]
        .into(),
    );
    for (header, ty, expression, expected) in [
        (CONSTANTS.to_owned(), "Float", "3", Value::Float(3.0)),
        (
            format!("{CONSTANTS}const n: Int = 3\n"),
            "Float",
            "2.0 * (n as Float)",
            Value::Float(6.0),
        ),
        (
            CONSTANTS.to_owned(),
            "(Float, List<Float>, Struct<a: Float>)",
            "Tuple(3, [1, 2], {a: 4})",
            aggregate,
        ),
        (
            CONSTANTS.to_owned(),
            "(Expr<Int>, List<Expr<Int>>, Struct<a: Expr<Int>>)",
            "Tuple(\"1\", [\"2\"], {a: \"3\"})",
            Value::Tuple(
                vec![
                    Value::Str("1".into()),
                    Value::List(vec![Value::Str("2".into())].into()),
                    Value::Map([("a".into(), Value::Str("3".into()))].into()),
                ]
                .into(),
            ),
        ),
    ] {
        successful_pair(&header, &[], ty, expression, expected);
    }
    let header = format!("{CONSTANTS}const n: Int = 3\n");
    for expression in ["n", "2.0 * n"] {
        type_rejected_pair(&header, &[], "Float", expression);
    }
    type_rejected_pair(CONSTANTS, &[], "Struct<a: Int>", "{a: \"wrong\"}");
}

const NUMBERS: &str = "use experimental::{constants}\n\
    const offset: Int = one * 2\nconst one: Int = 1\n";

fn failing_pair(header: &str, ty: &str, expression: &str, mentions: &[&str]) {
    let [constant, equation] = paired_sources(header, ty, expression);
    let error = expand(&constant, &[]).expect_err("a failed initializer cannot be normalized");
    let DsrvExpandError::ConstantValue { message, span } = error else {
        panic!("expected initializer evaluation failure: {constant}: {error}");
    };
    let range = initializer_range(&constant, &format!("const c: {ty} = "), expression);
    assert!(
        span.start >= range.start && span.end <= range.end,
        "initializer span: {constant}: {span:?}"
    );
    for text in mentions {
        assert!(message.contains(text), "{constant}: {message}");
    }
    let spec = expand(&equation, &[]).expect("the corresponding equation reaches its checked use");
    for runtime in [
        RuntimeSpec::Dataflow(ExecutionPolicy::Synchronous),
        RuntimeSpec::SemiSync,
    ] {
        let (rows, failure) = run(
            runtime,
            Semantics::TypedUntimed,
            elaborate(spec.clone()),
            inputs([("pulse", vec![Value::Int(1)])]),
        );
        assert!(
            rows.is_empty(),
            "the failed output must not succeed: {equation}: {rows:?}"
        );
        let failure = failure.expect("the corresponding operation must fail");
        for text in mentions {
            assert!(failure.contains(text), "{runtime:?}: {equation}: {failure}");
        }
    }
}

#[test]
fn a_failed_constant_check_cannot_be_normalized_to_an_accepted_value() {
    let source = format!("{CONSTANTS}const c:List<Struct<n:Int>> = [{{n: \"wrong\"}}]\n");
    let error = expand(&source, &[]).expect_err("a mismatched constant is not normalized");
    let DsrvExpandError::ConstantCheck { errors, .. } = error else {
        panic!("{error}");
    };
    assert!(errors.iter().any(|e|matches!(e,SemanticError::TypeError(e) if e.message().contains("Int")&&e.message().contains("Str"))));
    failing_pair(CONSTANTS, "Int", "1 / 0", &["zero"]);
}

#[test]
fn constant_dependencies_are_ready_for_stream_offsets() {
    let local = "use experimental::{constants}\n\
        const lag: Int = seed * 2\nconst seed: Int = 1\n";
    let imported = "use experimental::{constants, modules}\nmod numbers\nuse numbers\n";
    for (header, offset, modules) in [
        (local.to_owned(), "lag", vec![]),
        (
            format!("{imported}const lag: Int = numbers::one * 2\n"),
            "lag",
            vec![("numbers", NUMBERS)],
        ),
        (
            imported.to_owned(),
            "numbers::offset",
            vec![("numbers", NUMBERS)],
        ),
    ] {
        for offset in [offset, "2"] {
            let source = format!(
                "{header}in x: Int\nout y: Int = default(x[{offset}], 0)\nout tick: Int = x\n"
            );
            let spec =
                expand(&source, &modules).unwrap_or_else(|error| panic!("{source}: {error}"));

            let (rows, failure) = run_both(
                elaborate(spec.clone()),
                inputs([("x", vec![Value::Int(1), Value::Int(2), Value::Int(3)])]),
            );
            assert_eq!(failure, None);
            let values = rows
                .iter()
                .map(|row| row[&VarName::from("y")].clone())
                .collect::<Vec<_>>();
            assert_eq!(
                values,
                vec![Value::Int(0), Value::Int(0), Value::Int(1)],
                "{source}"
            );
        }
    }
}
