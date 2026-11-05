//! `match` and `matches`: what they accept, what they refuse, and what a
//! specification carrying one prints back as.

use crate::VarName;
use crate::dsrv_fixtures::WithoutWarnings;
use crate::lang::dsrv::ast::{CheckedDsrvSpecification, DsrvSpecification};
use crate::lang::dsrv::diagnostics::{SemanticError, TypeErrorKind};
use crate::lang::dsrv::parser::{DsrvParseError, check_core_source, parse_str};

use crate::core::StreamType;
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
        .check()
        .without_warnings()
        .unwrap_or_else(|errors| panic!("{}: {errors:?}", specification(body)))
}

fn check_err(body: &str) -> Vec<SemanticError> {
    parse(body)
        .check()
        .without_warnings()
        .err()
        .unwrap_or_else(|| panic!("{}: expected checking to fail", specification(body)))
}

fn kind_of(errors: &[SemanticError]) -> &TypeErrorKind {
    match &errors[0] {
        SemanticError::TypeError(error) => error.kind(),
        other => panic!("expected a type error, got {other:?}"),
    }
}

fn type_of(checked: &CheckedDsrvSpecification, name: &str) -> StreamType {
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
        matches!(&errors[0], SemanticError::UndeclaredVariable(message, _, _) if message.contains('n')),
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

// An or-pattern covers each tag its alternatives cover, however they are nested
// or named.
#[test]
fn an_or_pattern_covers_the_alternatives_it_names() {
    for body in [
        "out y: Int\ny = match(state) { Moving(_) | Stopped -> 0, }\n",
        "out y: Int\ny = match(state) { Stopped | Moving(_) -> 0, }\n",
        "out y: Int\ny = match(state) { Moving(1 | _) | Stopped -> 0, }\n",
        "out y: Int\ny = match(state) { Moving(1) | Moving(_) | Stopped -> 0, }\n",
        "out y: Int\ny = match(state) { m @ Moving(_) -> 0, Stopped -> 1, }\n",
        "out y: Int\ny = match(state) { Moving(_) -> 1, Stopped | Moving(_) -> 0, }\n",
    ] {
        check(body);
    }
}

#[test]
fn an_or_pattern_covering_only_some_tags_is_refused() {
    let three_tags = |arms: &str| {
        format!(
            "{HEADER}type Light = Union<Red, Amber, Green>\nin l: Light\nout y: Int\n\
             y = match(l) {{ {arms} }}\n"
        )
    };
    let refusal = |source: &str| {
        parse_str(source)
            .unwrap_or_else(|error| panic!("{source}: {error}"))
            .check()
            .without_warnings()
            .err()
            .unwrap_or_else(|| panic!("{source}: expected checking to fail"))
    };
    let errors = refusal(&three_tags("Red | Amber -> 0,"));
    assert_eq!(kind_of(&errors), &TypeErrorKind::MatchNotExhaustive);
    assert!(
        format!("{errors:?}").contains("no arm matches Green"),
        "{errors:?}"
    );
    assert!(
        parse_str(&three_tags("Red | Amber -> 0, Green | Red -> 1,"))
            .unwrap()
            .check()
            .without_warnings()
            .is_ok()
    );

    for body in [
        "out y: Int\ny = match(state) { Moving(1) | Stopped -> 0, }\n",
        "out y: Int\ny = match(state) { Moving(1 | 2) | Stopped -> 0, }\n",
        "out y: Int\ny = match(state) { Moving(_) | Stopped if x > 0 -> 0, }\n",
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
            "in label: Str\nout y: Int\ny = match(state) { Moving(n) -> n, Stopped -> label, }\n",
            TypeErrorKind::AnnotationTypeMismatch,
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

// A closed struct has all its fields, so a struct pattern that does not end in
// `..` and leaves one out can never match it. The omitted fields are named.
#[test]
fn a_struct_pattern_that_omits_fields_of_a_closed_struct_is_refused() {
    for (arm, omitted) in [
        ("{ a: v }", "`b`"),
        ("{ b: v }", "`a`"),
        ("{ }", "`a`, `b`"),
        ("{ a: 1 }", "`b`"),
        ("{ a: v } | { b: v }", "`b`"),
    ] {
        for scrutinee in ["s", "t"] {
            let errors = check_err(&format!(
                "in s: Struct<a: Int, b: Int>\nin t: Struct<a: Int, b: Int>\nout y: Int\n\
                 y = match({scrutinee}) {{ {arm} -> 1, _ -> 0, }}\n"
            ));
            assert_eq!(
                kind_of(&errors),
                &TypeErrorKind::PatternTypeMismatch,
                "{arm}"
            );
            let SemanticError::TypeError(error) = &errors[0] else {
                panic!("{arm}: {errors:?}");
            };
            assert!(
                error.message().contains(&format!("leaves out {omitted}"))
                    && error.message().contains("add `..`"),
                "{arm}: {}",
                error.message()
            );
            assert!(
                error.span().is_some() && !error.location().is_unset(),
                "{arm}"
            );
        }
    }
    let errors = check_err("in s: Struct<a: Int, b: Int>\nout y: Bool\ny = matches(s, { a: v })\n");
    assert_eq!(kind_of(&errors), &TypeErrorKind::PatternTypeMismatch);
}

// Listing every field, or ending in `..`, describes a closed struct, and an
// open struct type promises only the fields it lists.
#[test]
fn a_struct_pattern_that_covers_or_ignores_the_other_fields_is_accepted() {
    for (struct_type, arm) in [
        ("Struct<a: Int, b: Int>", "{ a: v, .. }"),
        ("Struct<a: Int, b: Int>", "{ a: v, b: _ }"),
        ("Struct<a: Int, b: Int>", "{ b: _, a: v }"),
        ("Struct<a: Int, b: Int>", "{ .. }"),
        ("Struct<a: Int, ...>", "{ a: v }"),
        ("Struct<a: Int, ...>", "{ a: v, .. }"),
        ("Struct<a: Int, b: Int, ...>", "{ a: v }"),
        ("Map<Int>", "{ a: v }"),
    ] {
        check(&format!(
            "in s: {struct_type}\nout y: Int\ny = match(s) {{ {arm} -> 1, _ -> 0, }}\n"
        ));
    }
    // An open struct type does not list the field, so there is none to bind.
    let errors = check_err(
        "in s: Struct<a: Int, ...>\nout y: Int\ny = match(s) { { b: v } -> 1, _ -> 0, }\n",
    );
    assert_eq!(kind_of(&errors), &TypeErrorKind::StructUnknownField);
}

// The alternatives of an or-pattern must agree on their names at any scrutinee.
#[test]
fn an_or_pattern_on_a_list_must_bind_the_same_names() {
    assert_eq!(
        kind_of(&check_err(
            "in v: List<Int>\nout y: Int\ny = match(v) { [a] | [b] -> 0, _ -> 0, }\n"
        )),
        &TypeErrorKind::OrPatternBindings
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

#[test]
fn a_pattern_binding_cannot_be_indexed() {
    let source =
        specification("out y: Int\ny = match(state) { Moving(n) -> n[1], Stopped -> 0, }\n");
    let message = parse_str(&source)
        .expect_err("indexing a binding is refused")
        .to_string();
    assert!(
        message.contains("`n`") && message.contains("not a declared stream"),
        "got {message}"
    );
}

// An empty list literal takes its element type from the list it joins, so a
// `match` may fall back to `[]` beside arms of a typed list.
#[test]
fn an_empty_list_arm_takes_the_element_type_of_the_other_arms() {
    let checked =
        check("out ys: List<Str>\nys = match(state) {\n  Stopped -> [\"a\"],\n  _ -> [],\n}\n");
    assert_eq!(
        type_of(&checked, "ys"),
        StreamType::List(Box::new(StreamType::Str))
    );
    let checked =
        check("out ys: List<Str>\nys = match(state) {\n  Moving(_) -> [],\n  _ -> [\"a\"],\n}\n");
    assert_eq!(
        type_of(&checked, "ys"),
        StreamType::List(Box::new(StreamType::Str))
    );
}

#[test]
fn an_empty_list_arm_of_a_def_takes_its_element_type_from_the_other_arms() {
    let source = format!(
        "use experimental::{{tagged_unions, pattern_matching, functions}}\n{STATE}in x: Int\naux state: State\nstate = Moving(x)\n\
         def needs(s: State) -> List<Str> = match(s) {{\n  Stopped -> [\"a\"],\n  _ -> [],\n}}\n\
         out ys: List<Str>\nys = needs(state)\n"
    );
    let checked = parse_str(&source)
        .unwrap_or_else(|error| panic!("{source}: {error}"))
        .check()
        .without_warnings()
        .unwrap_or_else(|errors| panic!("{source}: {errors:?}"));
    assert_eq!(
        type_of(&checked, "ys"),
        StreamType::List(Box::new(StreamType::Str))
    );
}

#[test]
fn an_empty_list_branch_joins_a_typed_list_in_an_if() {
    let checked = check("out ys: List<Int>\nys = if x > 0 then [x] else []\n");
    assert_eq!(
        type_of(&checked, "ys"),
        StreamType::List(Box::new(StreamType::Int))
    );
}

#[test]
fn an_empty_list_arm_still_refuses_a_different_list_type() {
    let errors = check_err(
        "out ys: List<Str>\nys = match(state) {\n  Stopped -> [1],\n  _ -> [\"a\"],\n}\n",
    );
    assert!(!errors.is_empty());
}

/// The output `y` of a `match` on a tuple of literals, whose alternatives
/// bind `a` and `b` to swapped positions.
fn swapped_bindings(output: &str, scrutinee: &str) -> String {
    format!(
        "out y: {output}\n\
         y = match(Tuple({scrutinee})) {{ (0, a, b) | (1, b, a) -> a, _ -> 0, }}\n"
    )
}

fn or_pattern_error(body: &str) -> String {
    let errors = check_err(body);
    assert_eq!(
        kind_of(&errors),
        &TypeErrorKind::OrPatternBindings,
        "{body}: {errors:?}"
    );
    let SemanticError::TypeError(error) = &errors[0] else {
        unreachable!("kind_of found a type error");
    };
    error.message().to_owned()
}

// Pattern bindings use the already concrete scrutinee type.
#[test]
fn or_pattern_alternatives_share_concrete_binding_types() {
    let int = swapped_bindings("Int", "1,10,100");
    assert_eq!(type_of(&check(&int), "y"), StreamType::Int);
    let inferred = "out y\ny=match(Tuple(1,10,100)) { (0,a,b)|(1,b,a)->b, _->0, }";
    assert_eq!(type_of(&check(inferred), "y"), StreamType::Int);
    let float = swapped_bindings("Float", "1,10.0,100.0");
    assert_eq!(type_of(&check(&float), "y"), StreamType::Float);
    or_pattern_error(&swapped_bindings("Float", "1,10,2.5"));
    let lists = "out y:List<Int> =match(Tuple([] as List<Int>,[1])) { (a,_)|(_,a)->a, _->[], }";
    assert_eq!(
        type_of(&check(lists), "y"),
        StreamType::List(Box::new(StreamType::Int))
    );
    assert!(
        !check_err("out y:List<Int> =match(Tuple([],[1])) { (a,_)|(_,a)->a, _->[], }").is_empty()
    );
}

// Alternatives that bind a name at two different types have no one meaning
// for the arm, whichever alternative comes first.
#[test]
fn or_pattern_alternatives_still_refuse_different_types() {
    for scrutinee in [
        "x, \"s\"", "\"s\", x", "1, \"s\"", "\"s\", 1", "x, 2.5", "2.5, x",
    ] {
        or_pattern_error(&format!(
            "out y\ny = match(Tuple({scrutinee})) {{ (a, _) | (_, a) -> a, _ -> 0, }}\n"
        ));
    }
    or_pattern_error(&swapped_bindings("Int", "1, 10, \"s\""));
}

// A refusal says which types disagree, as they were before the comparison:
// `a` could have become a `Float` on the way to `b` refusing.
#[test]
fn an_or_pattern_refusal_names_the_types_and_decides_nothing() {
    let message = or_pattern_error(
        "out y\ny = match(Tuple(1, 2.5, \"s\")) { (a, b, _) | (_, a, b) -> a, _ -> 0, }\n",
    );
    assert!(
        message.contains("binds a: Float, b: Str")
            && message.contains("earlier alternative binds a: Int, b: Float"),
        "{message}"
    );
    // Different names are said as such, with nothing to compare.
    let message = or_pattern_error("out y: Int\ny = match(state) { Moving(n) | Stopped -> 0, }\n");
    assert!(
        message.contains("binds nothing") && message.contains("binds n: Int"),
        "{message}"
    );
}

#[test]
fn pattern_names_derive_from_the_known_scrutinee() {
    for (typ, arm) in [
        ("(Int, Int)", "(a,b) -> a"),
        ("List<Int>", "[a] -> a"),
        ("Struct<f:Int>", "{f:a,..} -> a"),
        ("List<(Int, Struct<f:List<Int>>)> ", "[(a,{f:[b]})] -> b"),
    ] {
        assert_eq!(
            type_of(
                &check(&format!(
                    "in v:{typ}\nout y:Int=match(v) {{ {arm}, _ -> 0, }}"
                )),
                "y"
            ),
            StreamType::Int
        );
    }
    check("in v:List<Int>\nout y:Bool=matches(v,[a] if a>0)");
    assert!(!check_err("out y:Bool=matches([],[a] if a>0)").is_empty());
}
