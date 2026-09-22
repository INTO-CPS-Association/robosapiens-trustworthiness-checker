//! `const`: a value folded once and written in wherever it is named.
//!
//! A constant is not a stream and is never emitted. It is folded where the
//! file is read, so what reaches the core tree is a literal, and a constant
//! that could not be worked out is refused there rather than at runtime.

use crate::VarName;
use crate::lang::dsrv::LanguageError;
use crate::lang::dsrv::ast::DsrvSpecification;
use crate::lang::dsrv::modules::{ModuleCollector, show_path};
use crate::lang::dsrv::parser::{DsrvParseError, parse_str};
use crate::lang::dsrv::path::ModuleName;

use test_log::test;

const C: &str = "use experimental::{constants}\n";
const CM: &str = "use experimental::{constants, modules}\n";

fn spec(source: &str) -> DsrvSpecification {
    parse_str(source).unwrap_or_else(|error| panic!("{source}: {error}"))
}

/// How `y`'s body prints, which is what folding produced.
fn body(source: &str) -> String {
    spec(source)
        .var_expr_ref(&VarName::from("y"))
        .expect("y is defined")
        .to_string()
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
    crate::lang::dsrv::expand::expand_program(
        collector.finish().expect("collected"),
        crate::lang::dsrv::expand::language::LanguageRequest::default(),
    )
    .expect("expands")
}

fn program_body(root: &str, sources: &[(&str, &str)], var: &str) -> String {
    program(root, sources)
        .var_expr_ref(&VarName::from(var))
        .expect("the variable is defined")
        .to_string()
}

// ---------------------------------------------------------------------------
// What a constant stands for
// ---------------------------------------------------------------------------

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

/// S12 for constants: a glob skips an internal one silently, exactly as it
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
