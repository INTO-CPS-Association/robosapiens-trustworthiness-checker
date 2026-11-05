//! Generic type aliases: a name that takes type parameters, and the uses that
//! supply them.
//!
//! A generic alias has no type of its own, so it is resolved once per use site
//! rather than once for the namespace. These tests pin what that resolution
//! produces, and the errors a file meets when the parameters and the arguments
//! do not agree.

use crate::VarName;
use crate::core::{StreamType, UnionPayload};
use crate::dsrv_fixtures::WithoutWarnings;
use crate::lang::dsrv::LanguageError;
use crate::lang::dsrv::ast::DsrvSpecification;
use crate::lang::dsrv::parser::{DsrvParseError, parse_str};
use crate::lang::dsrv::source::SourceResolveError;

use test_log::test;

const HEADER: &str = "use experimental::{generics}\n";

fn parse(source: &str) -> DsrvSpecification {
    let source = format!("{HEADER}{source}");
    parse_str(&source).unwrap_or_else(|error| panic!("{source}: {error}"))
}

/// The resolved type of the annotation on `x`, which is where every test
/// below reads a use site's answer from.
fn annotation(source: &str) -> StreamType {
    let spec = parse(source);
    spec.type_annotations()
        .get(&VarName::from("x"))
        .unwrap_or_else(|| panic!("{source}: x has no annotation"))
        .clone()
}

fn resolve_error(source: &str) -> SourceResolveError {
    let source = format!("{HEADER}{source}");
    match parse_str(&source) {
        Err(DsrvParseError::Resolve(error)) => error,
        other => panic!("expected a resolution error for {source:?}, got {other:?}"),
    }
}

fn language_error(source: &str) -> LanguageError {
    match parse_str(source) {
        Err(DsrvParseError::Language(error)) => error,
        other => panic!("expected a language error for {source:?}, got {other:?}"),
    }
}

fn generic_def_checks(source: &str) -> bool {
    parse_str(&format!(
        "use experimental::{{functions, generics, tagged_unions}}\n{source}"
    ))
    .expect("generic def parses")
    .check()
    .without_warnings()
    .inspect_err(|errors| eprintln!("generic source: {source}\n{errors:?}"))
    .is_ok()
}

#[test]
fn generic_function_signature_is_shared_by_parameters_and_result() {
    assert!(generic_def_checks(
        "def pick<T, U>(x: T, y: U) -> T = x\nin a: Int\nin b: Str\nout x: Int = pick(a, b)\n"
    ));
    assert!(!generic_def_checks(
        "def bad<T>(x: T) -> T = true\nin a: Int\nout x: Int = bad(a)\n"
    ));
    assert!(!generic_def_checks(
        "def same<T>(x: T, y: T) -> T = x\nin a: Int\nin b: Str\nout x: Int = same(a, b)\n"
    ));
}

#[test]
fn generic_function_signature_matches_nested_aliases() {
    assert!(generic_def_checks(
        "type Box<T> = List<T>\ndef take<T>(x: Box<T>, y: T) -> T = y\nin a: List<Int>\nin b: Int\nout x: Int = take(a, b)\n"
    ));
    assert!(!generic_def_checks(
        "type Box<T> = List<T>\ndef same<T>(x: Box<T>, y: T) -> T = y\nin a: List<Int>\nin b: Str\nout x: Int = same(a, b)\n"
    ));
}

#[test]
fn generic_struct_matching_uses_open_structural_compatibility() {
    assert!(generic_def_checks(
        "def get<T>(x: Struct<value: T, ...>) -> T = 1\nin a: Struct<value: Int, extra: Str>\nout x: Int = get(a)\n"
    ));
    // A closed parameter has no other fields, which an open argument does not
    // say of its value.
    assert!(!generic_def_checks(
        "def get<T>(x: Struct<value: T>) -> T = 1\nin a: Struct<value: Int, ...>\nout x: Int = get(a)\n"
    ));
    assert!(!generic_def_checks(
        "def get<T>(x: Struct<value: T, ...>) -> T = 1\nin a: Struct<value: Str, extra: Int>\nout x: Int = get(a)\n"
    ));
}

#[test]
fn expected_result_anchors_generic_parameter_before_checking_empty_list() {
    assert!(generic_def_checks(
        "def id<T>(x: T) -> T = x\nout x: List<Int> = id([])\n"
    ));
}

#[test]
fn generic_def_result_can_anchor_an_otherwise_unbound_parameter() {
    assert!(generic_def_checks(
        "def make<T>() -> T = 1\nout x: Int = make()\n"
    ));
    assert!(!generic_def_checks(
        "def make<T>() -> T = 1\nout x = make()\n"
    ));
}

#[test]
fn instantiated_generic_result_context_reaches_constructors_in_the_body() {
    assert!(generic_def_checks(
        "type Maybe<T> = Union<Some: T, None>\n\
         def wrap<T>(x: T) -> Maybe<T> = Some(x)\n\
         in x: Int\n\
         out wrapped: Maybe<Int> = wrap(x)\n"
    ));
}

#[test]
fn argument_inference_handles_repeated_parameters_in_nested_open_aliases() {
    assert!(generic_def_checks(
        "type Maybe<T> = Union<Some: T, None>\n\
         type Resolved<T> = Struct<value: T, ...>\n\
         type Acceptance<T> = Struct<result: Resolved<T>>\n\
         type Plan<T> = Struct<plan: Resolved<T>, accepted: Maybe<Acceptance<T>>>\n\
         type Episode<T> = Union<Idle, Active: Struct<plan: Maybe<Plan<T>>>>\n\
         type Step<T> = Union<Stayed, Moved: Episode<T>>\n\
         def advance<T>(witness: T, current: Episode<T>) -> Step<T> = Stayed\n\
         in witness: Struct<key: Int, ...>\n\
         in current: Episode<Struct<key: Int, ...>>\n\
         out step: Step<Struct<key: Int, ...>> = advance(witness, current)\n"
    ));
}

#[test]
fn a_parameter_becomes_the_argument_it_was_applied_to() {
    assert_eq!(
        annotation("type Box<A> = List<A>\nin x: Box<Int>\n"),
        StreamType::List(Box::new(StreamType::Int)),
    );
}

#[test]
fn each_use_site_resolves_the_alias_again() {
    assert_eq!(
        annotation("type Box<A> = List<A>\nin x: Box<Str>\nin y: Box<Int>\n"),
        StreamType::List(Box::new(StreamType::Str)),
    );
}

#[test]
fn an_argument_may_itself_be_an_application() {
    assert_eq!(
        annotation("type Box<A> = List<A>\nin x: Box<Box<Int>>\n"),
        StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int)))),
    );
}

#[test]
fn parameters_substitute_positionally_not_by_name() {
    let StreamType::Tuple(fields) = annotation("type Pair<A, B> = (B, A)\nin x: Pair<Int, Str>\n")
    else {
        panic!("expected a tuple");
    };
    assert_eq!(fields.as_slice(), &[StreamType::Str, StreamType::Int]);
}

#[test]
fn a_generic_alias_may_forward_a_subset_of_its_parameters() {
    assert_eq!(
        annotation("type Inner<A> = List<A>\ntype Outer<A, B> = Inner<B>\nin x: Outer<Str, Int>\n"),
        StreamType::List(Box::new(StreamType::Int)),
    );
}

#[test]
fn a_parameter_stands_for_a_whole_type_not_just_a_name() {
    assert_eq!(
        annotation("type Box<A> = List<A>\nin x: Box<(Int, Str)>\n"),
        StreamType::List(Box::new(StreamType::Tuple(
            [StreamType::Int, StreamType::Str].into_iter().collect()
        ))),
    );
}

#[test]
fn a_concrete_alias_still_resolves_once_for_the_namespace() {
    assert_eq!(
        annotation("type Count = Int\nin x: Count\n"),
        StreamType::Int,
    );
}

#[test]
fn too_few_arguments_is_an_arity_error_not_an_unknown_alias() {
    let error = resolve_error("type Pair<A, B> = (A, B)\nin x: Pair<Int>\n");
    assert!(
        matches!(
            &error,
            SourceResolveError::AliasArity {
                expected: 2,
                found: 1,
                ..
            }
        ),
        "got {error:?}",
    );
}

#[test]
fn naming_a_generic_alias_without_arguments_is_an_arity_error() {
    let error = resolve_error("type Box<A> = List<A>\nin x: Box\n");
    assert!(
        matches!(
            &error,
            SourceResolveError::AliasArity {
                expected: 1,
                found: 0,
                ..
            }
        ),
        "got {error:?}",
    );
}

#[test]
fn applying_a_concrete_alias_is_an_arity_error() {
    let error = resolve_error("type Count = Int\nin x: Count<Int>\n");
    assert!(
        matches!(
            &error,
            SourceResolveError::AliasArity {
                expected: 0,
                found: 1,
                ..
            }
        ),
        "got {error:?}",
    );
}

#[test]
fn applying_an_undeclared_name_is_an_unknown_alias() {
    let error = resolve_error("in x: Missing<Int>\n");
    assert!(
        matches!(&error, SourceResolveError::UnknownAlias { .. }),
        "got {error:?}",
    );
}

#[test]
fn a_generic_alias_that_expands_to_itself_is_a_cycle_not_a_hang() {
    let error = resolve_error("type Loop<A> = Loop<A>\nin x: Loop<Int>\n");
    assert!(
        matches!(&error, SourceResolveError::AliasCycle { .. }),
        "got {error:?}",
    );
}

#[test]
fn generic_result_matching_refuses_recursive_aliases_with_growing_arguments() {
    for aliases in [
        "type Loop<A> = Loop<List<A>>",
        "type Loop<A> = Next<List<A>>\ntype Next<B> = Loop<B>",
        "type Identity<B> = B\ntype Loop<A> = Identity<Loop<List<A>>>",
    ] {
        let source = format!(
            "use experimental::{{functions, generics}}\n{aliases}\ndef f<T>(x: T) -> Loop<T> = x\nout y: Int = f(1)\n"
        );
        let errors = parse_str(&source)
            .expect("generic signature defers concrete alias resolution")
            .check()
            .discard_warnings()
            .expect_err("recursive alias must be refused");
        assert!(
            errors
                .iter()
                .any(|error| error.to_string().contains("cyclic definition")),
            "{errors:?}"
        );
        assert!(errors[0].location().primary().is_some(), "{errors:?}");
    }
}

#[test]
fn an_unused_generic_alias_does_not_have_to_resolve() {
    // It has no type until it is applied, so an unknown name in its body is
    // only an error at a use site.
    parse("type Box<A> = List<A>\nin x: Int\n");
}

/// A union whose arms nest a second generic, applied to three parameters
/// at once.
#[test]
fn a_generic_union_may_nest_another_generic_in_its_arms() {
    let spec = parse(concat!(
        "use experimental::{tagged_unions}\n",
        "type Boxed<A> = Struct<value: A, count: Int>\n",
        "type Entry<K, E, M> = Union<",
        "First: Struct<step: E, trigger: M>, ",
        "Second: Boxed<K>",
        ">\n",
        "in x: Entry<Str, Bool, Int>\n",
    ));
    let StreamType::Union(union) = spec
        .type_annotations()
        .get(&VarName::from("x"))
        .expect("x has an annotation")
        .clone()
    else {
        panic!("expected a union");
    };
    let held = union
        .alternatives()
        .iter()
        .find(|alternative| alternative.tag().as_str() == "Second")
        .expect("the Second arm");
    let UnionPayload::Of(StreamType::Struct(fields, _)) = held.payload() else {
        panic!("expected a struct payload, got {:?}", held.payload());
    };
    assert_eq!(
        fields
            .iter()
            .find(|(name, _)| name == "value")
            .map(|(_, ty)| ty),
        Some(&StreamType::Str),
    );
}

#[test]
fn declaring_parameters_needs_the_experiment() {
    let error = language_error("type Box<A> = List<A>\nin x: Int\n");
    assert!(
        matches!(
            &error,
            LanguageError::NeedsExperiment {
                feature: "generics",
                ..
            }
        ),
        "got {error:?}",
    );
}

#[test]
fn applying_a_name_needs_the_experiment() {
    let error = language_error("type Count = Int\nin x: Count<Int>\n");
    assert!(
        matches!(
            &error,
            LanguageError::NeedsExperiment {
                feature: "generics",
                ..
            }
        ),
        "got {error:?}",
    );
}

#[test]
fn nested_generic_identity_calls_do_not_repeat_grounded_subtrees() {
    let nested = (0..24).fold("1".to_owned(), |inner, _| format!("id({inner})"));
    for output in [
        format!("out x: Int = {nested}"),
        format!("out ok: Bool = {nested} == 1"),
    ] {
        assert!(generic_def_checks(&format!(
            "def id<T>(x: T) -> T = x\n{output}\n"
        )));
    }
}

/// The first concrete argument determines the generic parameter.
#[test]
fn a_list_of_structs_at_a_type_parameter_takes_the_first_argument_s_type() {
    let call = |output: &str, call: &str| {
        format!(
            "def two<T>(p: List<T>, q: List<T>) -> List<T> = p\n\
             in s: Struct<a: Int, b: Int>\n\
             in t: Struct<a: Int, ...>\n\
             out x{output} = {call}\n"
        )
    };
    assert!(!generic_def_checks(&call("", "two([s], [t])")));
    assert!(generic_def_checks(&call(
        ": List<Struct<a: Int, ...>>",
        "two([t], [s])"
    )));
}

#[test]
fn generic_arguments_are_grounded_in_source_order() {
    let header = "def make<T>() -> List<T> = []\ndef same<T>(a: List<T>, b: List<T>) -> Bool = true\nin a: List<Int>\n";
    assert!(generic_def_checks(&format!(
        "{header}out ok: Bool = same(a, make())"
    )));
    for call in ["same(make(), a)", "same(make(), make())"] {
        assert!(
            !generic_def_checks(&format!("{header}out ok: Bool = {call}")),
            "{call}"
        );
    }
    assert!(generic_def_checks(&format!(
        "{header}aux tmp: List<Int> = make()\nout ok = same(tmp, a)"
    )));
    assert!(generic_def_checks(
        "def make<T>() -> List<T> = []\ndef id<T>(x:T)->T=x\nout x:List<Int> = id(make())"
    ));
    assert!(!generic_def_checks(
        "def make<T>() -> List<T> = []\nout x = make()"
    ));
}

#[test]
fn comparisons_only_pass_already_known_operand_context() {
    let header = "def make<T>() -> List<T> = []\nin value: List<Int>\n";
    assert!(generic_def_checks(&format!(
        "{header}out same:Bool = value == make()"
    )));
    for expression in ["make() == value", "make() != value", "make() == make()"] {
        assert!(
            !generic_def_checks(&format!("{header}out same:Bool = {expression}")),
            "{expression}"
        );
    }
}

#[test]
fn concrete_arguments_and_results_ground_generic_signatures() {
    for typ in ["Int", "List<Int>", "Struct<a:Int,...>"] {
        assert!(generic_def_checks(&format!(
            "def id<T>(x:T)->T=x\nin a:{typ}\nout x=id(a)"
        )));
    }
    assert!(!generic_def_checks(
        "def same<T>(a:T,b:T)->T=a\nin a:Int\nin b:Str\nout x=same(a,b)"
    ));
}

#[test]
fn known_generic_callback_parameters_allow_body_result_inference() {
    assert!(generic_def_checks(
        "def apply<T,U>(x:T,f:(T -> U))->U=f(x)\nout y=apply(1, \\x -> x+1)"
    ));
    assert!(!generic_def_checks(
        "def apply<T,U>(f:(T -> U),x:T)->U=f(x)\nout y=apply(\\x -> x+1,1)"
    ));
    assert!(!generic_def_checks(
        "def apply<T,U>(f:(T -> U),x:T)->U=f(x)\nout y:Int=apply(\\x -> 1,1)"
    ));
}
