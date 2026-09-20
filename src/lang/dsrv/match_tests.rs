//! `match` and `matches`: what they accept, what they refuse, and what a
//! specification carrying one prints back as.

use crate::VarName;
use crate::lang::dsrv::ast::{CheckedDsrvSpecification, DsrvSpecification};
use crate::lang::dsrv::parser::{DsrvParseError, check_core_source, parse_str};
use crate::lang::dsrv::pipeline::TypeCheckOptions;
use crate::lang::dsrv::type_checker::{SemanticError, TCType, TypeErrorKind};
use crate::lang::dsrv::{Feature, LanguageError};

use test_log::test;

const HEADER: &str = "use experimental::{tagged_unions, pattern_matching}\n";
const STATE: &str = "type State = Union<Stopped, Moving: Int>\n";

fn specification(body: &str) -> String {
    format!("{HEADER}{STATE}in x: Int\naux state: State\nstate = Moving(x)\n{body}")
}

fn parse(body: &str) -> DsrvSpecification {
    let source = specification(body);
    parse_str(&source).unwrap_or_else(|error| panic!("{source}: {error}"))
}

fn check(body: &str) -> CheckedDsrvSpecification {
    parse(body)
        .type_check(TypeCheckOptions::GRADUAL)
        .unwrap_or_else(|errors| panic!("{}: {errors:?}", specification(body)))
}

fn check_err(body: &str) -> Vec<SemanticError> {
    parse(body)
        .type_check(TypeCheckOptions::GRADUAL)
        .err()
        .unwrap_or_else(|| panic!("{}: expected checking to fail", specification(body)))
}

fn kind_of(errors: &[SemanticError]) -> &TypeErrorKind {
    match &errors[0] {
        SemanticError::TypeError(error) => error.kind(),
        other => panic!("expected a type error, got {other:?}"),
    }
}

fn type_of(checked: &CheckedDsrvSpecification, name: &str) -> TCType {
    checked
        .var_expr_ref(&VarName::from(name))
        .unwrap_or_else(|| panic!("{name} is defined"))
        .typ()
        .clone()
}

// R12.1: every pattern form parses and prints back as it was written, so a
// specification carrying one round-trips.
#[test]
fn patterns_print_back_as_written() {
    let arms = [
        "Moving(n) -> n",
        "Moving(n) if n > 1 -> n",
        "Moving(_) -> 0",
        "whole @ Moving(n) -> n",
        "Moving(1) -> 1",
        "Moving(-1) -> 3",
        "Moving(1..3) -> 5",
        "Moving(1..=3) -> 2",
        "Stopped | Moving(_) -> 4",
    ];
    for arm in arms {
        let body = format!("out y: Int\ny = match(state) {{ {arm}, _ -> 0, }}\n");
        let printed = parse(&body).to_string();
        let pattern = arm
            .split(" if ")
            .next()
            .unwrap()
            .split(" -> ")
            .next()
            .unwrap();
        assert!(printed.contains(pattern), "{pattern}: {printed}");
        // What it prints is what it parses.
        assert_eq!(
            parse_str(&printed).unwrap().to_string(),
            printed,
            "{arm} did not round-trip"
        );
    }
}

// R12.2: the container patterns carry the same spelling as the values they
// match.
#[test]
fn container_patterns_print_back_as_written() {
    let source = format!(
        "{HEADER}in xs: List<Int>\nin pair: (Int, Int)\nin row: Struct<a: Int, b: Int>\n\
         out y: Int\ny = match(xs) {{ [a, b] -> a + b, _ -> 0, }}\n\
         out z: Int\nz = match(pair) {{ (a, b) -> a + b, }}\n\
         out w: Int\nw = match(row) {{ {{ a: v, .. }} -> v, }}\n"
    );
    let printed = parse_str(&source).unwrap().to_string();
    for arm in ["[a, b] -> ", "(a, b) -> ", "{ a: v, .. } -> "] {
        assert!(printed.contains(arm), "{arm}: {printed}");
    }
    assert_eq!(parse_str(&printed).unwrap().to_string(), printed);
}

// R12.3: `matches` is kept as itself rather than becoming a `match`, so it
// prints back as the writer wrote it.
#[test]
fn matches_prints_back_as_itself() {
    for (written, printed_as) in [
        ("matches(state, Moving(_))", "matches(state, Moving(_))"),
        // The guard is an expression, so it prints as expressions do.
        (
            "matches(state, Moving(n) if n > 1)",
            "matches(state, Moving(n) if (n > 1))",
        ),
    ] {
        let printed = parse(&format!("out y: Bool\ny = {written}\n")).to_string();
        assert!(printed.contains(printed_as), "{written}: {printed}");
        assert_eq!(parse_str(&printed).unwrap().to_string(), printed);
    }
}

// R12.4: a binder takes the type of what it matched, and is in scope for its
// own arm only.
#[test]
fn a_binder_takes_the_type_of_what_it_matched() {
    let checked = check("out y: Int\ny = match(state) { Moving(n) -> n, Stopped -> 0, }\n");
    assert_eq!(type_of(&checked, "y").to_string(), "Int");

    let errors = check_err("out y: Int\ny = match(state) { Moving(n) -> 0, Stopped -> n, }\n");
    assert!(
        matches!(&errors[0], SemanticError::UndeclaredVariable(message, _) if message.contains('n')),
        "{errors:?}"
    );
}

// R12.5: a value that no arm matches is what exhaustiveness is for, and a
// guarded arm does not cover the value it matched, because its guard may
// refuse it.
#[test]
fn a_match_covers_every_alternative() {
    check("out y: Int\ny = match(state) { Moving(n) -> n, Stopped -> 0, }\n");
    check("out y: Int\ny = match(state) { Moving(n) -> n, _ -> 0, }\n");
    for body in [
        "out y: Int\ny = match(state) { Moving(n) -> n, }\n",
        "out y: Int\ny = match(state) { Moving(n) if n > 1 -> n, Stopped -> 0, }\n",
    ] {
        assert_eq!(
            kind_of(&check_err(body)),
            &TypeErrorKind::MatchNotExhaustive,
            "{body}"
        );
    }
}

// R12.6: each way an arm can disagree with the value or with its neighbours.
#[test]
fn arms_are_checked_against_the_value_and_each_other() {
    let cases = [
        (
            "out y: Int\ny = match(state) { Moving(n) -> n, Stopped -> 0, Running -> 1, }\n",
            TypeErrorKind::UnknownUnionTag,
        ),
        (
            "in label: Str\nout y: Any\ny = match(state) { Moving(n) -> n, Stopped -> label, }\n",
            TypeErrorKind::MatchArmTypeMismatch,
        ),
        (
            "out y: Int\ny = match(state) { Moving(n) if n -> n, Stopped -> 0, }\n",
            TypeErrorKind::AnnotationTypeMismatch,
        ),
        (
            "out y: Int\ny = match(state) { Moving(n) | Stopped -> 0, }\n",
            TypeErrorKind::OrPatternBindings,
        ),
        (
            "out y: Int\ny = match(x) { 1 -> 1, _ -> 0, }\nout z: Int\nz = match(state) { Moving(\"a\") -> 1, Stopped -> 0, }\n",
            TypeErrorKind::PatternTypeMismatch,
        ),
    ];
    for (body, kind) in cases {
        assert_eq!(kind_of(&check_err(body)), &kind, "{body}");
    }
}

// R12.9: a range names the Ints it covers, and says whether its end is one
// of them.
#[test]
fn a_range_pattern_covers_its_ends_as_written() {
    let checked = check(
        "out y: Str\ny = match(x) { 1..3 -> \"below\", 3..=4 -> \"through\", _ -> \"beyond\", }\n",
    );
    assert_eq!(type_of(&checked, "y").to_string(), "Str");

    // A range describes Ints, and a payload that is one is matched by it.
    check("out y: Int\ny = match(state) { Moving(1..3) -> 1, Moving(_) -> 0, Stopped -> 0, }\n");

    // Anything else it does not describe.
    assert_eq!(
        kind_of(&check_err(
            "in label: Str\nout y: Int\ny = match(label) { 1..3 -> 1, _ -> 0, }\n"
        )),
        &TypeErrorKind::PatternTypeMismatch
    );
}

// R12.7: a `match` on something that is not a union needs a pattern that
// matches whatever it is given.
#[test]
fn a_match_on_a_scalar_needs_a_catch_all() {
    check("out y: Int\ny = match(x) { 1 -> 10, _ -> 0, }\n");
    assert_eq!(
        kind_of(&check_err(
            "out y: Int\ny = match(x) { 1 -> 10, 2 -> 20, }\n"
        )),
        &TypeErrorKind::MatchNotExhaustive
    );
}

// R12.8: both constructs are gated, and both are outside Core.
#[test]
fn match_needs_its_experiment_and_is_outside_core() {
    let source = format!(
        "use experimental::{{tagged_unions}}\n{STATE}in x: Int\naux state: State\n\
         state = Moving(x)\nout y: Int\ny = match(state) {{ _ -> 0, }}\n"
    );
    let error = match parse_str(&source) {
        Err(DsrvParseError::Language(error)) => error,
        other => panic!("expected a language error, got {other:?}"),
    };
    assert!(
        matches!(&error, LanguageError::NeedsExperiment { construct, feature, .. }
            if *construct == "`match`" && *feature == Feature::PatternMatching.name()),
        "{error}"
    );
    assert!(matches!(
        check_core_source(&format!("language core\n{HEADER}in x: Int\n")),
        Err(DsrvParseError::Language(
            LanguageError::ExperimentsInCore { .. }
        ))
    ));
}
