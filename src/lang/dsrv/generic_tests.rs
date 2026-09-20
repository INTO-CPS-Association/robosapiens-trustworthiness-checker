//! Generic type aliases: a name that takes type parameters, and the uses that
//! supply them.
//!
//! A generic alias has no type of its own, so it is resolved once per use site
//! rather than once for the namespace. These tests pin what that resolution
//! produces, and the errors a file meets when the parameters and the arguments
//! do not agree.

use crate::VarName;
use crate::core::{StreamType, UnionPayload};
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
