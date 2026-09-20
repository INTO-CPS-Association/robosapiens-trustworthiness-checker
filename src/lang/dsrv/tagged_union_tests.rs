//! Tagged union syntax: the types, the constructors, and the experiment that
//! gates both.
//!
//! Parsing builds a constructor from what the writer spelled and no more, so
//! these tests describe the shape of the node and the errors a file meets
//! before elaboration; resolving a tag to its union is elaboration's work.

use crate::VarName;
use crate::core::{StreamType, UnionPayload};
use crate::lang::dsrv::ElaboratedDsrvSpecification;
use crate::lang::dsrv::LanguageError;
use crate::lang::dsrv::ast::{CheckedDsrvSpecification, DsrvSpecification, ExprView};
use crate::lang::dsrv::parser::{DsrvParseError, check_core_source, parse_str};
use crate::lang::dsrv::pipeline::TypeCheckOptions;
use crate::lang::dsrv::source::TypeName;
use crate::lang::dsrv::type_checker::SemanticError;
use crate::lang::dsrv::type_checker::{TCType, TypeErrorKind, UnresolvedTypeKind};

use test_log::test;

const HEADER: &str = "use experimental::{tagged_unions}\n";

fn parse(source: &str) -> DsrvSpecification {
    parse_str(source).unwrap_or_else(|error| panic!("{source}: {error}"))
}

fn language_error(source: &str) -> LanguageError {
    match parse_str(source) {
        Err(DsrvParseError::Language(error)) => error,
        other => panic!("expected a language error for {source:?}, got {other:?}"),
    }
}

fn check(source: &str) -> CheckedDsrvSpecification {
    parse(source)
        .type_check(TypeCheckOptions::GRADUAL)
        .unwrap_or_else(|errors| panic!("{source}: {errors:?}"))
}

fn check_err(source: &str) -> Vec<SemanticError> {
    parse(source)
        .type_check(TypeCheckOptions::GRADUAL)
        .err()
        .unwrap_or_else(|| panic!("{source}: expected checking to fail"))
}

fn type_of(checked: &CheckedDsrvSpecification, name: &str) -> TCType {
    checked
        .var_expr_ref(&VarName::from(name))
        .unwrap_or_else(|| panic!("{name} is defined"))
        .typ()
        .clone()
}

fn annotation(specification: &DsrvSpecification, name: &str) -> StreamType {
    specification
        .type_annotation(&VarName::from(name))
        .unwrap_or_else(|| panic!("{name} has no resolved annotation"))
        .clone()
}

// R16.1-a: a union type is written with its tags, and a payload after the
// tag it belongs to.
#[test]
fn a_union_type_resolves_to_its_schema() {
    let specification = parse(&format!(
        "{HEADER}in s: Union<Stopped, Moving: Int>\nout y: Int\ny = 1\n"
    ));
    let StreamType::Union(schema) = annotation(&specification, "s") else {
        panic!("expected a union type");
    };
    let tags: Vec<_> = schema
        .alternatives()
        .iter()
        .map(|alternative| alternative.tag().as_str())
        .collect();
    assert_eq!(tags, ["Moving", "Stopped"], "alternatives sort by tag");
    let (_, moving) = schema.alternative("Moving").expect("Moving");
    assert_eq!(moving.payload(), &UnionPayload::Of(StreamType::Int));
    let (_, stopped) = schema.alternative("Stopped").expect("Stopped");
    assert_eq!(stopped.payload(), &UnionPayload::Nullary);
}

// R16.1-b: a union is structural, so an alias for one is the schema itself
// and two spellings of the same alternatives are one type.
#[test]
fn a_union_alias_is_the_schema_it_expands_to() {
    let specification = parse(&format!(
        "{HEADER}type State = Union<Stopped, Moving: Int>\n\
         in s: State\nin t: Union<Moving: Int, Stopped>\nout y: Int\ny = 1\n"
    ));
    assert_eq!(
        annotation(&specification, "s"),
        annotation(&specification, "t")
    );
}

// R16.1-c: the schema rejects what it cannot represent.
#[test]
fn a_union_type_needs_distinct_tags() {
    let source = format!("{HEADER}in s: Union<Moving: Int, Moving>\nout y: Int\ny = 1\n");
    assert!(
        matches!(parse_str(&source), Err(DsrvParseError::Resolve(_))),
        "a repeated tag is refused"
    );
}

// R16.1-d: `Union` names the type constructor, so it is not an alias name.
#[test]
fn union_is_a_reserved_type_name() {
    assert!(TypeName::new("Union").is_err());
    assert!(matches!(
        parse_str(&format!("{HEADER}type Union = Int\nin x: Union\n")),
        Err(DsrvParseError::Syntax(_))
    ));
}

// R16.2-a: a constructor carries what was written — a tag, the payload it
// was given, and the qualifier if there was one.
#[test]
fn a_constructor_keeps_its_tag_payload_and_qualifier() {
    let specification = parse(&format!(
        "{HEADER}type State = Union<Stopped, Moving: Int>\n\
         out y: State\ny = State::Moving(3)\n"
    ));
    let expression = specification
        .var_expr_ref(&VarName::from("y"))
        .expect("y is defined");
    let ExprView::Constructor(payload, tag, qualifier) = expression.view() else {
        panic!("expected a constructor, got {expression}");
    };
    assert_eq!(tag, "Moving");
    assert_eq!(qualifier.as_ref().map(TypeName::as_str), Some("State"));
    assert_eq!(payload.into_iter().count(), 1);
}

// R16.2-b: a nullary constructor takes no parentheses and carries no payload.
#[test]
fn a_nullary_constructor_carries_no_payload() {
    let specification = parse(&format!(
        "{HEADER}type State = Union<Stopped, Moving: Int>\n\
         out y: State\ny = State::Stopped\n"
    ));
    let expression = specification
        .var_expr_ref(&VarName::from("y"))
        .expect("y is defined");
    let ExprView::Constructor(payload, tag, _) = expression.view() else {
        panic!("expected a constructor, got {expression}");
    };
    assert_eq!(tag, "Stopped");
    assert_eq!(payload.into_iter().count(), 0);
}

// R16.2-c: a constructor is not a function, so its parentheses hold its
// payload rather than making a call of it.
#[test]
fn a_constructor_is_not_applied_to_its_payload() {
    let specification = parse(&format!(
        "{HEADER}type State = Union<Stopped, Moving: Int>\n\
         out y: State\ny = State::Moving(3)\n"
    ));
    let expression = specification
        .var_expr_ref(&VarName::from("y"))
        .expect("y is defined");
    assert!(
        matches!(expression.view(), ExprView::Constructor(..)),
        "got {expression}"
    );
    // A call on an ordinary name still parses as a call.
    let called = parse("in f\nout y\ny = f(3)\n");
    let expression = called
        .var_expr_ref(&VarName::from("y"))
        .expect("y is defined");
    assert!(
        matches!(expression.view(), ExprView::Apply(..)),
        "got {expression}"
    );
}

// R16.2-d: a specification prints back the constructor spelling it was given.
#[test]
fn a_constructor_prints_as_it_was_written() {
    for constructor in ["State::Stopped", "State::Moving(3)"] {
        let source = format!(
            "{HEADER}type State = Union<Stopped, Moving: Int>\n\
             out y: State\ny = {constructor}\n"
        );
        let printed = parse(&source).to_string();
        assert!(printed.contains(constructor), "{constructor}: {printed}");
        // What it prints is what it parses.
        assert_eq!(parse(&printed).to_string(), printed);
    }
}

// R16.3-a: both halves of the feature are gated, and the message names the
// experiment to declare.
#[test]
fn unions_need_their_experiment() {
    let cases = [
        (
            "in s: Union<Stopped>\nout y: Int\ny = 1\n",
            "a tagged union type",
        ),
        (
            "type State = Union<Stopped>\nin s: State\n",
            "a tagged union type",
        ),
        (
            "in x: Int\nout y: Int\ny = List.map(\\v: Union<A> -> 1, [])\n",
            "a tagged union type",
        ),
        (
            "in x: Int\nout y: Int\ny = dynamic(\"1\": Union<A>)\n",
            "a tagged union type",
        ),
        (
            "in x: Int\nout y: Int\ny = State::Stopped\n",
            "a union constructor",
        ),
    ];
    for (source, construct) in cases {
        match language_error(source) {
            LanguageError::NeedsExperiment {
                construct: found,
                feature,
                ..
            } => {
                assert_eq!(found, construct, "{source}");
                assert_eq!(feature, "tagged_unions", "{source}");
            }
            other => panic!("{source}: {other}"),
        }
    }
}

// R16.3-b: Core DSRV takes no experiments, and names both constructs as
// outside Core in case a later edition stabilises them.
#[test]
fn unions_are_outside_core() {
    assert!(matches!(
        check_core_source("language core\nin s: Union<Stopped>\nout y: Int\ny = 1\n"),
        Err(DsrvParseError::Language(
            LanguageError::ExperimentsInCore { .. }
        )) | Err(DsrvParseError::Language(
            LanguageError::NeedsExperiment { .. }
        ))
    ));
    let error = language_error(&format!(
        "language core\n{HEADER}in x: Int\nout y: Int\ny = State::Stopped\n"
    ));
    assert!(
        matches!(error, LanguageError::ExperimentsInCore { .. }),
        "{error}"
    );
}

// R16.4-a: a qualified constructor names its union outright, so it checks
// wherever it stands.
#[test]
fn a_qualified_constructor_resolves_through_its_qualifier() {
    let checked = check(&format!(
        "{HEADER}type State = Union<Stopped, Moving: Int>\n\
         out y: State\ny = State::Moving(3)\n"
    ));
    assert_eq!(
        type_of(&checked, "y").to_string(),
        "Union<Moving: Int, Stopped>"
    );
}

// R16.4-b: a bare tag takes the union the expression is expected to have,
// including when that expectation comes from the payload it is written in.
#[test]
fn a_bare_constructor_resolves_against_the_expected_type() {
    let checked = check(&format!(
        "{HEADER}type Cycle = Union<Idle, Active: Int>\n\
         type Step = Union<Stayed, Moved: Cycle>\n\
         out y: Step\ny = Moved(Idle)\n\
         out z: Step\nz = Stayed\n"
    ));
    for name in ["y", "z"] {
        assert_eq!(
            type_of(&checked, name).to_string(),
            "Union<Moved: Union<Active: Int, Idle>, Stayed>",
            "{name}"
        );
    }
}

// R16.4-c: unions sharing a tag are what resolution by expected type is for.
#[test]
fn a_tag_shared_by_two_unions_resolves_to_the_expected_one() {
    let checked = check(&format!(
        "{HEADER}type CycleStep = Union<Stayed, Moved: Int>\n\
         type EpisodeStep = Union<Stayed, Moved: Str>\n\
         out y: CycleStep\ny = Moved(1)\n\
         out z: EpisodeStep\nz = Moved(\"a\")\n"
    ));
    assert_eq!(
        type_of(&checked, "y").to_string(),
        "Union<Moved: Int, Stayed>"
    );
    assert_eq!(
        type_of(&checked, "z").to_string(),
        "Union<Moved: Str, Stayed>"
    );
}

// R16.4-d: with nothing to resolve against, the constructor is reported and
// the message says where the tag does belong.
#[test]
fn a_bare_constructor_needs_something_to_resolve_against() {
    let errors = check_err(&format!(
        "{HEADER}type State = Union<Stopped, Moving: Int>\n\
         out y: Any\ny = Stopped\n"
    ));
    let error = &errors[0];
    assert!(
        matches!(error, SemanticError::UnresolvedType(error)
            if *error.kind() == UnresolvedTypeKind::ConstructorUnion
                && error.message().contains("`Stopped` is an alternative of State")),
        "{error:?}"
    );
}

// R16.4-e: each way a constructor can disagree with its union is reported
// as that, rather than as a mismatch further out.
#[test]
fn a_constructor_is_checked_against_its_alternative() {
    let cases = [
        (
            "out y: State\ny = Stopped(1)\n",
            TypeErrorKind::ConstructorPayloadArity,
        ),
        (
            "out y: State\ny = Moving\n",
            TypeErrorKind::ConstructorPayloadArity,
        ),
        (
            "in s: Str\nout y: State\ny = Moving(s)\n",
            TypeErrorKind::ConstructorPayloadTypeMismatch,
        ),
        (
            "out y: State\ny = Running\n",
            TypeErrorKind::UnknownUnionTag,
        ),
        ("out y: Int\ny = Stopped\n", TypeErrorKind::ExpectedUnion),
        (
            "out y: State\ny = Count::Stopped\n",
            TypeErrorKind::ExpectedUnion,
        ),
        (
            "out y: State\ny = Missing::Stopped\n",
            TypeErrorKind::ExpectedUnion,
        ),
    ];
    for (body, kind) in cases {
        let errors = check_err(&format!(
            "{HEADER}type State = Union<Stopped, Moving: Int>\ntype Count = Int\n{body}"
        ));
        let error = &errors[0];
        assert!(
            matches!(error, SemanticError::TypeError(error) if *error.kind() == kind),
            "{body}: {error:?}"
        );
    }
}

// R16.4-f: elaboration is what every runtime receives, and it keeps the
// constructor it was given, now with the union it resolved to.
#[test]
fn elaboration_keeps_a_resolved_constructor() {
    let source = format!(
        "{HEADER}type State = Union<Stopped, Moving: Int>\n\
         out y: State\ny = State::Moving(3)\n"
    );
    let elaborated = ElaboratedDsrvSpecification::parse_with(&source, TypeCheckOptions::GRADUAL)
        .unwrap_or_else(|error| panic!("{source}: {error}"));
    assert!(
        elaborated.to_string().contains("y = State::Moving(3)"),
        "{elaborated}"
    );
}

// R16.5-a: in a file that opted in, case alone says a name is a tag, so a
// bare constructor needs no qualifier and no declaration.
#[test]
fn a_capitalised_name_is_a_tag_in_a_file_that_opted_in() {
    let specification = parse(&format!(
        "{HEADER}type State = Union<Stopped, Moving: Int>\n\
         out y: State\ny = Moving(3)\nout z: State\nz = Stopped\n"
    ));
    for (name, tag, payloads) in [("y", "Moving", 1), ("z", "Stopped", 0)] {
        let expression = specification
            .var_expr_ref(&VarName::from(name))
            .expect("defined");
        let ExprView::Constructor(payload, found, qualifier) = expression.view() else {
            panic!("{name}: expected a constructor, got {expression}");
        };
        assert_eq!(found, tag);
        assert_eq!(qualifier, &None, "{name} was written without a qualifier");
        assert_eq!(payload.into_iter().count(), payloads, "{name}");
    }
}

// R16.5-b: a file that did not opt in keeps its capitalised streams, which
// is what the shipped distributed examples rely on.
#[test]
fn a_capitalised_name_is_a_variable_without_the_experiment() {
    let specification = parse("in DSUAllowed: Bool\nout y: Bool\ny = DSUAllowed\n");
    let expression = specification
        .var_expr_ref(&VarName::from("y"))
        .expect("y is defined");
    assert!(
        matches!(expression.view(), ExprView::Var(_)),
        "got {expression}"
    );
}

// R16.5-c: applying a capitalised name is always a constructor, so a file
// that did not opt in is told what to declare rather than given a call.
#[test]
fn applying_a_capitalised_name_needs_the_experiment() {
    let error = language_error("in f: Int\nout y: Int\ny = Foo(1)\n");
    assert!(
        matches!(&error, LanguageError::NeedsExperiment { construct, .. }
            if *construct == "a union constructor"),
        "{error}"
    );
}

// R16.6: because case decides, a capitalised stream or binder in a file that
// opted in could never be read back, so it is refused where it is written.
#[test]
fn a_capitalised_declaration_is_refused_once_a_file_opts_in() {
    let cases = [
        (
            format!("{HEADER}in Sensor: Int\nout y: Int\ny = 1\n"),
            "the input",
        ),
        (
            format!("{HEADER}out Sensor: Int\nSensor = 1\n"),
            "the output",
        ),
        (
            format!("{HEADER}aux Sensor: Int\nSensor = 1\nout y: Int\ny = 1\n"),
            "the auxiliary stream",
        ),
        (
            format!("{HEADER}in x: Int\nout y: Int\ny = List.len(List.map(\\V -> V, []))\n"),
            "the lambda parameter",
        ),
    ];
    for (source, construct) in cases {
        match language_error(&source) {
            LanguageError::CapitalisedName {
                construct: found, ..
            } => assert_eq!(found, construct, "{source}"),
            other => panic!("{source}: {other}"),
        }
    }
    // The same file is accepted while it uses no unions.
    parse("in Sensor: Int\nout y: Int\ny = Sensor\n");
}

// R16.5-d: a bare constructor prints back as it was written, without
// acquiring a qualifier it was not given.
#[test]
fn a_bare_constructor_prints_without_a_qualifier() {
    for constructor in ["Stopped", "Moving(3)"] {
        let source = format!(
            "{HEADER}type State = Union<Stopped, Moving: Int>\n\
             out y: State\ny = {constructor}\n"
        );
        let printed = parse(&source).to_string();
        assert!(printed.contains(&format!("y = {constructor}")), "{printed}");
        assert_eq!(parse(&printed).to_string(), printed);
    }
}
