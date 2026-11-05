//! Deterministic contextual checking: paired acceptance and refusal witnesses.
use crate::core::StreamType;
use crate::dsrv_fixtures::WithoutWarnings;
use crate::lang::dsrv::ast::DsrvSpecification;
use crate::lang::dsrv::diagnostics::{SemanticError, TypeErrorKind};
use crate::lang::dsrv::parser::{DsrvParseError, parse_str};
use crate::lang::dsrv::source::SourceResolveError;
use crate::lang::dsrv::test_support::type_check;
use contiguous_tree::TreeCursorExt;

fn checked(source: &str, expected: &StreamType) {
    let checked = type_check(source.parse().expect("valid source syntax"))
        .unwrap_or_else(|errors| panic!("{source}: {errors:?}"));
    let y = checked.var_expr_ref(&"y".into()).expect("output y");
    assert_eq!(y.typ(), expected, "{source}");
    for node in y.postorder() {
        let _concrete: StreamType = node.typ().clone();
    }
    let elaborated = checked.elaborate();
    let y = elaborated.var_expr_ref(&"y".into()).unwrap();
    assert_eq!(y.typ(), expected, "{source}");
    for node in y.postorder() {
        let _concrete: StreamType = node.typ().clone();
    }
}

fn unresolved(source: &str, fragment: &str) {
    let errors = type_check(source.parse().expect("valid refusal syntax")).expect_err(source);
    let error = errors
        .iter()
        .find(|e| matches!(e, SemanticError::UnresolvedType(_)))
        .unwrap_or_else(|| panic!("expected an annotation request: {errors:?}"));
    let site = error
        .location()
        .primary()
        .expect("located annotation request");
    let snippet = site.snippet().expect("unresolved source snippet");
    assert!(snippet.contains(fragment), "{source}: {errors:?}");
}

#[test]
fn removed_any_type_is_refused_at_root_and_recursive_positions() {
    for typ in [
        "Any",
        "List<Any>",
        "Map<Any>",
        "(Int, Any)",
        "Struct<a:Any>",
        "(Int -> Any)",
        "Union<A:Any,B>",
        "Box<Any>",
    ] {
        let source = format!(
            "use experimental::{{generics,tagged_unions}}\ntype Box<T> =List<T>\nin x:{typ}"
        );
        match parse_str(&source).expect_err("Any is not a language type") {
            DsrvParseError::Resolve(SourceResolveError::UnknownAlias { name, span }) => {
                assert_eq!(name.to_string(), "Any");
                assert_eq!(&source[span.to_range()], "Any");
            }
            other => panic!("{source}: {other:?}"),
        }
    }
}

#[test]
fn immediate_context_types_empty_collections_and_lambda_parameters() {
    for (source, expected) in [
        (
            "out y:List<Int> =[]",
            StreamType::List(Box::new(StreamType::Int)),
        ),
        (
            "out y:Map<Int> =Map()",
            StreamType::Map(Box::new(StreamType::Int)),
        ),
        (
            "out y:(Int -> Int)=\\a -> a",
            StreamType::Function(
                [StreamType::Int].into_iter().collect(),
                Box::new(StreamType::Int),
            ),
        ),
        (
            "in xs:List<Int>\nout y=List.map(\\a -> a+1,xs)",
            StreamType::List(Box::new(StreamType::Int)),
        ),
        (
            "out y=List.fold(\\acc,a -> acc+a,0,List() as List<Int>)",
            StreamType::Int,
        ),
    ] {
        checked(source, &expected);
    }
    for (source, fragment) in [
        ("out y=[]", "[]"),
        ("out y=Map()", "Map()"),
        ("out y=\\a -> a", "\\a"),
        ("out y:Bool=[]==[]", "[]"),
        ("out y=List.len(List())", "List()"),
        ("out y=List.fold(\\acc,a -> acc+a,0,List())", "List()"),
    ] {
        unresolved(source, fragment);
    }
}

#[test]
fn differing_record_shapes_require_an_explicit_common_target() {
    let header = "use experimental::pattern_matching\nin c:Bool\nin a:Struct<a:Int,b:Int>\nin b:Struct<a:Int,c:Int,...>\n";
    let base = StreamType::Struct([("a".into(), StreamType::Int)].into_iter().collect(), true);
    for expression in [
        "if c then a else b",
        "if c then b else a",
        "default(a,b)",
        "init(a,b)",
        "update(a,b)",
        "match(c) { true -> a, _ -> b, }",
        "List.fold(\\acc,n -> b,a,[1])",
    ] {
        checked(
            &format!("{header}out y:Struct<a:Int,...> ={expression}"),
            &base,
        );
        let source = format!("{header}out y={expression}");
        let errors =
            type_check(source.parse().unwrap()).expect_err("no inferred record intersection");
        assert!(
            errors.iter().any(|e| matches!(
                e,
                SemanticError::TypeError(_) | SemanticError::UnresolvedType(_)
            )),
            "{source}: {errors:?}"
        );
        assert!(
            errors
                .iter()
                .any(|e| e.span().is_some() && !e.location().is_unset())
        );
    }
    checked(
        &format!("{header}out y=if c then a else a"),
        &StreamType::Struct(
            [("a".into(), StreamType::Int), ("b".into(), StreamType::Int)]
                .into_iter()
                .collect(),
            false,
        ),
    );
    checked(
        &format!("{header}out y:List<Struct<a:Int,...>> =[a,b]"),
        &StreamType::List(Box::new(base.clone())),
    );
    checked(
        &format!("{header}out y:Map<Struct<a:Int,...>> =Map(\"a\":a,\"b\":b)"),
        &StreamType::Map(Box::new(base)),
    );
    for expression in ["[a,b]", "Map(\"a\":a,\"b\":b)"] {
        let errors = type_check(format!("{header}out y={expression}").parse().unwrap())
            .expect_err("collection elements need a common concrete shape");
        assert!(errors.iter().any(|e| matches!(
            e,
            SemanticError::TypeError(_) | SemanticError::UnresolvedType(_)
        )));
    }
}

#[test]
fn explicit_generic_targets_admit_width_without_common_field_probing() {
    let header = "use experimental::{functions,generics}\ndef same<T>(x:T,y:T)->T=x\nin a:Struct<a:Int,b:Int>\nin b:Struct<a:Int,c:Int,...>\n";
    let base = StreamType::Struct([("a".into(), StreamType::Int)].into_iter().collect(), true);
    checked(
        &format!("{header}out y:Struct<a:Int,...> =same(a,b)"),
        &base,
    );
    for expression in ["same(a,b)", "same(b,a)"] {
        let errors = type_check(format!("{header}out y={expression}").parse().unwrap())
            .expect_err("source-order substitutions do not synthesize intersections");
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, SemanticError::TypeError(_)))
        );
    }
}

#[test]
fn numeric_context_only_converts_integer_syntax() {
    for source in [
        "in x:Float\nout y=x+2",
        "in x:Float\nout y=2+x",
        "out y:Float=2",
    ] {
        checked(source, &StreamType::Float);
    }
    for source in ["in x:Float\nout y=x==2", "in x:Float\nout y=2<x"] {
        checked(source, &StreamType::Bool);
    }
    checked("out y=7/2", &StreamType::Int);
    checked("in x:Int\nout y:Float=x as Float", &StreamType::Float);
    for source in [
        "out y:Float=7/2",
        "out y:Float=9007199254740993",
        "in x:Int\nout y:Float=x",
        "out y:Int=2**-1",
    ] {
        let errors = type_check(source.parse().unwrap()).expect_err("concrete numeric refusal");
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, SemanticError::TypeError(_))),
            "{source}: {errors:?}"
        );
        assert!(
            errors
                .iter()
                .any(|e| e.span().is_some() && !e.location().is_unset())
        );
    }
}

#[test]
fn an_unannotated_input_is_not_typed_by_its_uses() {
    let source = "in x\nout y=x+1";
    let errors =
        type_check(source.parse().unwrap()).expect_err("inputs require concrete annotations");
    let [SemanticError::MissingTypeAnnotation(_, _, location)] = errors.as_slice() else {
        panic!("{errors:?}");
    };
    assert_eq!(
        location.primary().and_then(|site| site.snippet()),
        Some("in x")
    );
}

#[test]
fn failed_roots_do_not_publish_drafts_or_create_dependent_errors() {
    let source = "out bad = !1\nout dependent=bad+1\nout good=2";
    let report = source.parse::<DsrvSpecification>().unwrap().check();
    let errors = report
        .without_warnings()
        .expect_err("one failed root refuses publication");
    let [SemanticError::TypeError(error)] = errors.as_slice() else {
        panic!("{errors:?}");
    };
    assert_eq!(error.kind(), &TypeErrorKind::AnnotationTypeMismatch);
    assert!(error.message().contains("Bool") && error.message().contains("Int"));
    assert_eq!(
        error.location().primary().and_then(|site| site.snippet()),
        Some("1")
    );
    checked("out y=2", &StreamType::Int);
}

#[test]
fn generic_substitutions_use_concrete_results_then_arguments_in_source_order() {
    let header = "use experimental::{functions,generics}\ndef id<T>(x:T)->T=x\ndef make<T>()->List<T> =[]\ndef same<T>(a:List<T>,b:List<T>)->Bool=true\nin xs:List<Int>\n";
    let list = StreamType::List(Box::new(StreamType::Int));
    checked(&format!("{header}out y=id(xs)"), &list);
    checked(&format!("{header}out y:List<Int> =id([])"), &list);
    checked(&format!("{header}out y:List<Int> =id(make())"), &list);
    checked(
        &format!("{header}out y:Bool=same(xs,make())"),
        &StreamType::Bool,
    );
    checked(
        &format!("{header}aux tmp:List<Int> =make()\nout y=same(tmp,xs)"),
        &StreamType::Bool,
    );
    for call in [
        "same(make(),xs)",
        "same(make(),make())",
        "make()==xs",
        "make()!=xs",
        "make()==make()",
    ] {
        unresolved(&format!("{header}out y:Bool={call}"), "make()");
    }
    let source = "use experimental::{functions,generics}\ndef same<T>(a:T,b:T)->T=a\nin a:Int\nin b:Str\nout y=same(a,b)";
    let errors = type_check(source.parse().unwrap()).expect_err("repeated T cannot widen");
    let error = errors
        .iter()
        .find_map(|e| match e {
            SemanticError::TypeError(e) => Some(e),
            _ => None,
        })
        .expect("concrete mismatch");
    assert!(error.message().contains("Int") && error.message().contains("Str"));
    assert_eq!(
        error.location().primary().and_then(|site| site.snippet()),
        Some("same(a,b)")
    );
}

#[test]
fn higher_order_union_payloads_require_a_common_declared_signature() {
    let header = "use experimental::tagged_unions\nin c:Bool\nin first:((Union<A:Int> -> Int) -> Int)\nin second:((Union<A:Str> -> Int) -> Int)\n";
    let errors = type_check(
        format!("{header}out y=if c then first else second")
            .parse()
            .unwrap(),
    )
    .expect_err("union payloads do not gain a common top type");
    assert!(errors.iter().any(|e| matches!(
        e,
        SemanticError::TypeError(_) | SemanticError::UnresolvedType(_)
    )));
    let source = "use experimental::tagged_unions\nin c:Bool\nin first:((Union<A:Int> -> Int) -> Int)\nin second:((Union<A:Int> -> Int) -> Int)\nout y=if c then first else second";
    let checked =
        type_check(source.parse().unwrap()).expect("equal higher-order union signatures survive");
    assert_eq!(
        checked.var_expr_ref(&"y".into()).unwrap().typ(),
        &checked.type_annotations()[&"first".into()]
    );
}

#[test]
fn union_constructors_require_a_known_concrete_union() {
    let header = "use experimental::tagged_unions\ntype U=Union<A:Int,B>\n";
    let source = format!("{header}out y:U=A(1)");
    let parsed = source.parse::<DsrvSpecification>().unwrap();
    let expected = parsed.type_annotations()[&"y".into()].clone();
    checked(&source, &expected);
    unresolved(&format!("{header}out y=A(1)"), "A(1)");
}

#[test]
fn known_callback_inputs_contextualize_empty_collections() {
    let list = StreamType::List(Box::new(StreamType::Int));
    for source in [
        "out y=List.map(\\x:Int -> x,[])",
        "in f:(Int -> Int)\nout y=List.map(f,[])",
        "out y=List.filter(\\x:Int -> x>0,[])",
        "in f:(Int -> Bool)\nout y=List.filter(f,[])",
    ] {
        checked(source, &list);
    }
    checked(
        "out y=List.fold(\\a:Int,x:Int -> a+x,0,[])",
        &StreamType::Int,
    );
    for source in [
        "out y=List.map(\\x -> x,[])",
        "out y:List<Int> =List.map(\\x -> 1,[])",
        "out y=List.filter(\\x -> x>0,[])",
    ] {
        unresolved(source, "[]");
    }
}

#[test]
fn standalone_validation_rejects_binding_scopes_before_type_errors() {
    use crate::lang::dsrv::ast::ExprView;
    use crate::lang::dsrv::type_checker::{StreamTypes, type_check_expression};
    let environment = StreamTypes::from([
        ("x".into(), StreamType::Int),
        ("source".into(), StreamType::Str),
    ]);
    for operator in ["dynamic", "defer"] {
        for binder in [
            format!("(\\local:Int -> {operator}(source:Int,{{local}}))(x)"),
            format!("match(x) {{ local -> {operator}(source:Int,{{local}}), }}"),
        ] {
            for expression in [binder.clone(), format!("!1 + ({binder})")] {
                let source = format!(
                    "use experimental::pattern_matching\nin x:Int\nin source:Str\nout result:Int={expression}"
                );
                let spec = source
                    .parse::<DsrvSpecification>()
                    .expect("scope fixture parses");
                let expression = spec.var_expr(&"result".into()).unwrap();
                let scope_span = expression
                    .as_ref()
                    .postorder()
                    .find(|node| matches!(node.view(), ExprView::Dynamic(..) | ExprView::Defer(..)))
                    .unwrap()
                    .span();
                let errors = type_check_expression(&expression, &StreamType::Int, &environment)
                    .without_warnings()
                    .expect_err("only streams may appear in runtime source scopes");
                let [SemanticError::InvalidRuntimeScope(message, span, _)] = errors.as_slice()
                else {
                    panic!("{source}: {errors:?}");
                };
                assert!(
                    message.contains("local") && message.contains("parameter or pattern binding"),
                    "{message}"
                );
                assert_eq!(*span, Some(scope_span));
                assert!(
                    source[scope_span.to_range()]
                        .contains(&format!("{operator}(source:Int,{{local}})"))
                );
            }
        }
        // A standalone expression has no owning stream; its explicit scope
        // may name every declared stream in its supplied environment.
        let expression =
            crate::lang::dsrv::parser::parse_expr(&format!("{operator}(source:Int,{{x}})"))
                .unwrap();
        let checked = type_check_expression(&expression, &StreamType::Int, &environment)
            .without_warnings()
            .expect("global stream scopes remain valid");
        assert_eq!(checked.typ(), &StreamType::Int);
    }
}

#[test]
fn an_annotated_failed_dependency_emits_no_cascaded_consumer_error() {
    // VarName orders by interning, so establish the consumer before its
    // dependency as well as giving it the earlier source spelling.
    let consumer = crate::VarName::new("a_c6_annotated_failure_consumer");
    let dependency = crate::VarName::new("z_c6_annotated_failure_dependency");
    assert!(consumer < dependency);
    for source in [
        "out a_c6_annotated_failure_consumer:Bool=z_c6_annotated_failure_dependency+1\nout z_c6_annotated_failure_dependency:Int=!1\nout independent_c6=2",
        "out z_c6_annotated_failure_dependency:Int=!1\nout a_c6_annotated_failure_consumer:Bool=z_c6_annotated_failure_dependency+1\nout independent_c6=2",
    ] {
        let errors = type_check(source.parse().unwrap())
            .expect_err("a declared dependency type does not prove its equation valid");
        let [SemanticError::TypeError(error)] = errors.as_slice() else {
            panic!("{source}: {errors:?}");
        };
        assert_eq!(error.kind(), &TypeErrorKind::AnnotationTypeMismatch);
        assert!(error.message().contains("expected Bool") && error.message().contains("got Int"));
        let span = error.span().expect("underlying failure is located");
        assert_eq!(&source[span.to_range()], "1");
        assert_eq!(
            error.location().primary().and_then(|site| site.snippet()),
            Some("1")
        );
    }
}

#[test]
fn missing_input_types_skip_direct_and_transitive_consumers() {
    for declarations in [
        "out a_c6_missing_consumer:Bool=c6_untyped_input+1\naux b_c6_transitive_consumer:Int=a_c6_missing_consumer+1",
        "aux b_c6_transitive_consumer:Int=a_c6_missing_consumer+1\nout a_c6_missing_consumer:Bool=c6_untyped_input+1",
    ] {
        let source =
            format!("{declarations}\nin c6_untyped_input\nout independent_c6_input_control=2");
        let errors = type_check(source.parse().unwrap())
            .expect_err("an untyped input refuses its dependency chain");
        let [SemanticError::MissingTypeAnnotation(message, span, location)] = errors.as_slice()
        else {
            panic!("{source}: {errors:?}");
        };
        assert!(message.contains("c6_untyped_input"));
        assert_eq!(
            &source[span.expect("input declaration span").to_range()],
            "in c6_untyped_input"
        );
        assert_eq!(
            location.primary().and_then(|site| site.snippet()),
            Some("in c6_untyped_input")
        );
    }
}

#[test]
fn one_recursive_root_annotation_can_establish_a_concrete_cycle() {
    for source in [
        "out a_c6_cycle=b_c6_cycle+1\nout b_c6_cycle:Int=default(a_c6_cycle[1],0)",
        "out b_c6_cycle:Int=default(a_c6_cycle[1],0)\nout a_c6_cycle=b_c6_cycle+1",
    ] {
        let checked =
            type_check(source.parse().unwrap()).expect("one concrete root type grounds this cycle");
        for name in ["a_c6_cycle", "b_c6_cycle"] {
            assert_eq!(checked.type_annotations()[&name.into()], StreamType::Int);
            for node in checked.var_expr_ref(&name.into()).unwrap().postorder() {
                let _: StreamType = node.typ().clone();
            }
        }
        let elaborated = checked.elaborate();
        for name in ["a_c6_cycle", "b_c6_cycle"] {
            assert_eq!(
                elaborated.var_expr_ref(&name.into()).unwrap().typ(),
                &StreamType::Int
            );
        }
    }
    let source = "out a_c6_ungrounded_cycle=b_c6_ungrounded_cycle+1\nout b_c6_ungrounded_cycle=default(a_c6_ungrounded_cycle[1],0)";
    let errors = type_check(source.parse().unwrap())
        .expect_err("an unannotated cycle has no concrete starting type");
    assert!(!errors.is_empty());
    assert!(
        errors
            .iter()
            .all(|error| matches!(error, SemanticError::MissingTypeAnnotation(..)))
    );
    assert!(
        errors
            .iter()
            .all(|error| error.span().is_some() && !error.location().is_unset())
    );
}

#[test]
fn reversed_declaration_chains_infer_each_root_after_its_dependencies() {
    for source in [
        "in c6_chain_input:Int\nout a_c6_chain=z_c6_chain+1\naux z_c6_chain=m_c6_chain+1\naux m_c6_chain=c6_chain_input+1",
        "aux m_c6_chain=c6_chain_input+1\naux z_c6_chain=m_c6_chain+1\nout a_c6_chain=z_c6_chain+1\nin c6_chain_input:Int",
    ] {
        let checked = type_check(source.parse().unwrap())
            .expect("acyclic dependencies determine concrete root types");
        for name in ["a_c6_chain", "z_c6_chain", "m_c6_chain"] {
            assert_eq!(checked.type_annotations()[&name.into()], StreamType::Int);
            assert_eq!(
                checked.var_expr_ref(&name.into()).unwrap().typ(),
                &StreamType::Int
            );
        }
    }
}
