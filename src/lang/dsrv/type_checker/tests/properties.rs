use super::super::*;
use crate::core::BinaryOperator;
use crate::core::{StreamType, Value};
use crate::dataflow::DataflowMonitor;
use crate::lang::dsrv::ast::{Expr, SyntaxLiteral};
use crate::lang::dsrv::diagnostics::SemanticError;
use crate::lang::dsrv::test_support::arb_dsrv_spec;
use crate::lang::dsrv::test_support::type_check;
use crate::{DsrvSpecification, VarName};
use ecow::eco_vec;
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug)]
struct TypeDirectedCase {
    expr: Expr,
    expected: StreamType,
    inputs: BTreeMap<VarName, StreamType>,
    requires_collection_context: bool,
}

fn scalar_case(
    expression: impl Strategy<Value = Expr> + 'static,
    expected: StreamType,
    input: (&'static str, StreamType),
) -> BoxedStrategy<TypeDirectedCase> {
    expression
        .prop_map(move |expr| TypeDirectedCase {
            expr,
            expected: expected.clone(),
            inputs: BTreeMap::from([(VarName::new(input.0), input.1.clone())]),
            requires_collection_context: false,
        })
        .boxed()
}

fn arb_type_directed_case() -> impl Strategy<Value = TypeDirectedCase> {
    let integers = scalar_case(
        (any::<i16>(), any::<i16>(), any::<bool>(), any::<u8>()).prop_map(
            |(lhs, rhs, condition, shape)| match shape % 4 {
                0 => Expr::Var(VarName::new("i")).into(),
                1 => Expr::BinOp(
                    Box::new(Expr::Val(i64::from(lhs))),
                    Box::new(Expr::Val(i64::from(rhs))),
                    BinaryOperator::Add,
                )
                .into(),
                2 => Expr::If(
                    Box::new(Expr::Val(condition)),
                    Box::new(Expr::Val(i64::from(lhs))),
                    Box::new(Expr::Var(VarName::new("i"))),
                )
                .into(),
                _ => Expr::Default(
                    Box::new(Expr::Var(VarName::new("i"))),
                    Box::new(Expr::Val(i64::from(rhs))),
                )
                .into(),
            },
        ),
        StreamType::Int,
        ("i", StreamType::Int),
    );
    let floats = scalar_case(
        (any::<f32>(), any::<f32>(), any::<u8>()).prop_map(|(lhs, rhs, shape)| {
            let lhs = f64::from(lhs);
            let rhs = f64::from(rhs);
            match shape % 3 {
                0 => Expr::Var(VarName::new("f")).into(),
                1 => Expr::BinOp(
                    Box::new(Expr::Val(lhs)),
                    Box::new(Expr::Val(rhs)),
                    BinaryOperator::Add,
                )
                .into(),
                _ => Expr::Abs(Box::new(Expr::Var(VarName::new("f")))).into(),
            }
        }),
        StreamType::Float,
        ("f", StreamType::Float),
    );
    let booleans = scalar_case(
        (any::<bool>(), any::<bool>(), any::<u8>()).prop_map(|(lhs, rhs, shape)| match shape % 3 {
            0 => Expr::Var(VarName::new("b")).into(),
            1 => Expr::BinOp(
                Box::new(Expr::Val(lhs)),
                Box::new(Expr::Val(rhs)),
                BinaryOperator::And,
            )
            .into(),
            _ => Expr::Not(Box::new(Expr::Var(VarName::new("b")))).into(),
        }),
        StreamType::Bool,
        ("b", StreamType::Bool),
    );
    let strings = scalar_case(
        ("[a-zA-Z0-9]{0,8}", "[a-zA-Z0-9]{0,8}", any::<u8>()).prop_map(|(lhs, rhs, shape)| {
            match shape % 3 {
                0 => Expr::Var(VarName::new("s")).into(),
                1 => Expr::BinOp(
                    Box::new(Expr::Val(lhs)),
                    Box::new(Expr::Val(rhs)),
                    BinaryOperator::Concatenate,
                )
                .into(),
                _ => Expr::If(
                    Box::new(Expr::Val(true)),
                    Box::new(Expr::Var(VarName::new("s"))),
                    Box::new(Expr::Val(rhs)),
                )
                .into(),
            }
        }),
        StreamType::Str,
        ("s", StreamType::Str),
    );
    let lists = prop::collection::vec(any::<i16>(), 0..8)
        .prop_map(|values| TypeDirectedCase {
            requires_collection_context: values.is_empty(),
            expr: Expr::List(
                values
                    .into_iter()
                    .map(|value| Expr::Val(i64::from(value)).into())
                    .collect(),
            )
            .into(),
            expected: StreamType::List(Box::new(StreamType::Int)),
            inputs: BTreeMap::new(),
        })
        .boxed();
    let maps = prop::collection::btree_map("[a-z]{1,3}", any::<bool>(), 0..8)
        .prop_map(|values| TypeDirectedCase {
            requires_collection_context: values.is_empty(),
            expr: Expr::Map(
                values
                    .into_iter()
                    .map(|(key, value)| (key.into(), Expr::Val(value).into())),
            )
            .into(),
            expected: StreamType::Map(Box::new(StreamType::Bool)),
            inputs: BTreeMap::new(),
        })
        .boxed();
    let tuples = (any::<i16>(), any::<bool>(), "[a-z]{0,8}")
        .prop_map(|(integer, boolean, string)| TypeDirectedCase {
            expr: Expr::Tuple(eco_vec![
                Expr::Val(i64::from(integer)).into(),
                Expr::Val(boolean).into(),
                Expr::Val(string).into(),
            ])
            .into(),
            expected: StreamType::Tuple(eco_vec![
                StreamType::Int,
                StreamType::Bool,
                StreamType::Str,
            ]),
            inputs: BTreeMap::new(),
            requires_collection_context: false,
        })
        .boxed();
    let structs = (any::<i16>(), any::<bool>())
        .prop_map(|(count, enabled)| TypeDirectedCase {
            expr: Expr::Struct(BTreeMap::from([
                ("count".into(), Expr::Val(i64::from(count)).into()),
                ("enabled".into(), Expr::Val(enabled).into()),
            ]))
            .into(),
            expected: StreamType::Struct(
                eco_vec![
                    ("count".into(), StreamType::Int),
                    ("enabled".into(), StreamType::Bool),
                ],
                false,
            ),
            inputs: BTreeMap::new(),
            requires_collection_context: false,
        })
        .boxed();
    let functions = any::<i16>()
        .prop_map(|argument| TypeDirectedCase {
            expr: Expr::Apply(
                Box::new(Expr::Lambda(
                    eco_vec![(
                        VarName::new("x"),
                        crate::core::StreamTypeAscription::Ascribed(StreamType::Int)
                    )],
                    Box::new(Expr::Var(VarName::new("x"))),
                )),
                eco_vec![Expr::Val(i64::from(argument)).into()],
            )
            .into(),
            expected: StreamType::Int,
            inputs: BTreeMap::new(),
            requires_collection_context: false,
        })
        .boxed();
    let unit = Just(TypeDirectedCase {
        expr: Expr::Val(()).into(),
        expected: StreamType::Unit,
        inputs: BTreeMap::new(),
        requires_collection_context: false,
    })
    .boxed();
    let powers = prop_oneof![
        (-3_i16..=3, 0_u8..=8).prop_map(|(base, exponent)| TypeDirectedCase {
            expr: Expr::BinOp(
                Box::new(Expr::Val(SyntaxLiteral::Int(i64::from(base)))),
                Box::new(Expr::Val(SyntaxLiteral::Int(i64::from(exponent)))),
                BinaryOperator::Power,
            )
            .into(),
            expected: StreamType::Int,
            inputs: BTreeMap::new(),
            requires_collection_context: false,
        }),
        (-3_i16..=3, 0_u8..=8).prop_map(|(base, exponent)| TypeDirectedCase {
            expr: Expr::BinOp(
                Box::new(Expr::Val(SyntaxLiteral::Int(i64::from(base)))),
                Box::new(Expr::Val(SyntaxLiteral::Float(f64::from(exponent)))),
                BinaryOperator::Power,
            )
            .into(),
            expected: StreamType::Float,
            inputs: BTreeMap::new(),
            requires_collection_context: false,
        }),
        (-3_i16..=3, 0_u8..=8).prop_map(|(base, exponent)| TypeDirectedCase {
            expr: Expr::BinOp(
                Box::new(Expr::Val(SyntaxLiteral::Float(f64::from(base)))),
                Box::new(Expr::Val(SyntaxLiteral::Int(i64::from(exponent)))),
                BinaryOperator::Power,
            )
            .into(),
            expected: StreamType::Float,
            inputs: BTreeMap::new(),
            requires_collection_context: false,
        }),
        (-3_i16..=3, 0_u8..=8).prop_map(|(base, exponent)| TypeDirectedCase {
            expr: Expr::BinOp(
                Box::new(Expr::Val(SyntaxLiteral::Float(f64::from(base)))),
                Box::new(Expr::Val(SyntaxLiteral::Float(f64::from(exponent)))),
                BinaryOperator::Power,
            )
            .into(),
            expected: StreamType::Float,
            inputs: BTreeMap::new(),
            requires_collection_context: false,
        }),
    ]
    .boxed();
    let inequalities = prop_oneof![
        (any::<i16>(), any::<i16>()).prop_map(|(left, right)| TypeDirectedCase {
            expr: Expr::BinOp(
                Box::new(Expr::Val(SyntaxLiteral::Int(i64::from(left)))),
                Box::new(Expr::Val(SyntaxLiteral::Int(i64::from(right)))),
                BinaryOperator::NotEqual,
            )
            .into(),
            expected: StreamType::Bool,
            inputs: BTreeMap::new(),
            requires_collection_context: false,
        }),
        (any::<bool>(), any::<bool>()).prop_map(|(left, right)| TypeDirectedCase {
            expr: Expr::BinOp(
                Box::new(Expr::Val(SyntaxLiteral::Bool(left))),
                Box::new(Expr::Val(SyntaxLiteral::Bool(right))),
                BinaryOperator::NotEqual,
            )
            .into(),
            expected: StreamType::Bool,
            inputs: BTreeMap::new(),
            requires_collection_context: false,
        }),
    ]
    .boxed();

    prop_oneof![
        integers,
        floats,
        booleans,
        strings,
        lists,
        maps,
        tuples,
        structs,
        functions,
        unit,
        powers,
        inequalities,
    ]
}

fn specification(case: &TypeDirectedCase, annotate_output: bool) -> DsrvSpecification {
    let output = VarName::new("result");
    let mut annotations = case.inputs.clone();
    if annotate_output {
        annotations.insert(output.clone(), case.expected.clone());
    }
    DsrvSpecification::new(
        case.inputs.keys().cloned().collect(),
        BTreeSet::from([output.clone()]),
        BTreeMap::from([(output, case.expr.clone())]),
        annotations,
        Vec::new(),
    )
}

fn incompatible_type(typ: &StreamType) -> StreamType {
    if typ == &StreamType::Bool {
        StreamType::Int
    } else {
        StreamType::Bool
    }
}

fn sample_value(typ: &StreamType) -> Value {
    match typ {
        StreamType::Int => Value::Int(2),
        StreamType::Float => Value::Float(2.5),
        StreamType::Str => Value::Str("sample".into()),
        StreamType::Bool => Value::Bool(true),
        other => panic!("generator only declares primitive inputs, got {other}"),
    }
}

/// Reading the type on every node detects missing checked annotations in
/// interiors as well as at roots, before and after syntax elaboration.
fn assert_complete_types(checked: &crate::CheckedDsrvSpecification) {
    use contiguous_tree::TreeCursorExt;
    for name in checked.stream_vars() {
        let expression = checked.var_expr_ref(name).expect("checked stream");
        assert!(expression.postorder().count() > 0);
        for node in expression.postorder() {
            let _concrete: StreamType = node.typ().clone();
        }
    }
}

fn matching_merge(case: &TypeDirectedCase, shape: u8) -> DsrvSpecification {
    let mut case = case.clone();
    let first = Box::new(case.expr.clone());
    let second = Box::new(case.expr.clone());
    case.expr = match shape % 2 {
        0 => Expr::If(Box::new(Expr::Val(true)), first, second).into(),
        _ => Expr::Default(first, second).into(),
    };
    specification(&case, false)
}

proptest! {
    #![proptest_config(super::property_config())]

    #[test]
    fn exported_inferred_annotations_recheck_matching_merges(
        case in arb_type_directed_case().prop_filter("inferred interiors have concrete shapes", |case| !case.requires_collection_context),
        shape in any::<u8>()
    ) {
        let spec=matching_merge(&case,shape);
        let inferred=type_check(spec.clone()).expect("matching concrete contributions infer");
        assert_complete_types(&inferred);
        let mut annotated=spec;
        annotated.type_annotations=inferred.type_annotations().clone();
        let rechecked=type_check(annotated).expect("exported annotations agree");
        let output=VarName::new("result");
        prop_assert_eq!(inferred.var_expr_ref(&output).unwrap().typ(),rechecked.var_expr_ref(&output).unwrap().typ());
        prop_assert_eq!(inferred.var_expr_ref(&output).unwrap().typ(),&case.expected);
    }

    #[test]
    fn contextual_type_directed_programs_have_complete_concrete_trees(case in arb_type_directed_case()) {
        let checked=type_check(specification(&case,true)).expect("concrete context checks");
        assert_complete_types(&checked);
        let output=VarName::new("result");
        prop_assert_eq!(checked.var_expr_ref(&output).unwrap().typ(),&case.expected);
        let elaborated=checked.elaborate();
        assert_complete_types(elaborated.checked());
        prop_assert_eq!(elaborated.checked().var_expr_ref(&output).unwrap().typ(),&case.expected);
    }

    #[test]
    fn inferred_programs_match_contextual_types_or_request_collection_annotations(case in arb_type_directed_case()) {
        let result=type_check(specification(&case,false));
        if case.requires_collection_context {
            let errors=result.expect_err("empty interiors need annotation");
            prop_assert!(errors.iter().any(|e| matches!(e,SemanticError::UnresolvedType(_))));
        } else {
            let checked=result.expect("known children determine the root");
            prop_assert_eq!(checked.var_expr_ref(&VarName::new("result")).unwrap().typ(),&case.expected);
            assert_complete_types(&checked);
        }
    }

    #[test]
    fn incompatible_annotations_are_rejected(case in arb_type_directed_case()) {
        let mut spec=specification(&case,true);
        spec.type_annotations.insert(VarName::new("result"),incompatible_type(&case.expected));
        prop_assert!(!type_check(spec).expect_err("incompatible annotation").is_empty());
    }

    #[test]
    fn incompatible_concrete_merge_shapes_are_refused(integer in any::<i16>(),boolean in any::<bool>(),reverse in any::<bool>()) {
        let (a,b)=if reverse {(format!("{boolean}"),format!("{integer}"))}else{(format!("{integer}"),format!("{boolean}"))};
        let source=format!("out result=if true then {a} else {b}");
        let errors=type_check(source.parse().unwrap()).expect_err("different concrete shapes need an explicit compatible target");
        prop_assert!(errors.iter().any(|e|matches!(e,SemanticError::UnresolvedType(error) if error.message().contains("different concrete types"))));
    }

    #[test]
    fn dependency_order_preserves_acyclic_inference(case in arb_type_directed_case()) {
        let source=VarName::new("z_source");
        let consumer=VarName::new("a_consumer");
        let mut annotations=case.inputs.clone();
        // Empty collections still have positive coverage under their explicit context.
        if case.requires_collection_context { annotations.insert(source.clone(),case.expected.clone()); }
        let spec=DsrvSpecification::new(case.inputs.keys().cloned().collect(),BTreeSet::from([source.clone(),consumer.clone()]),BTreeMap::from([(consumer.clone(),Expr::Var(source.clone()).into()),(source.clone(),case.expr.clone())]),annotations,Vec::new());
        let checked=type_check(spec).expect("dependencies are checked before their users");
        prop_assert_eq!(checked.var_expr_ref(&source).unwrap().typ(),&case.expected);
        prop_assert_eq!(checked.var_expr_ref(&consumer).unwrap().typ(),&case.expected);
    }

    #[test]
    fn accepted_typed_programs_produce_values_of_the_declared_type(case in arb_type_directed_case()) {
        let checked=type_check(specification(&case,true)).expect("type-directed program checks");
        let mut monitor=DataflowMonitor::compile_checked(checked.elaborate()).expect("checked program compiles");
        let input=monitor.input_vars().iter().map(|name|sample_value(&case.inputs[name])).collect::<Vec<_>>();
        let mut output=vec![Value::NoVal;monitor.output_vars().len()];
        monitor.evaluate(&input,&mut output).expect("well-typed expression evaluates");
        prop_assert_eq!(output.len(),1);
        prop_assert!(check_value_stream_type(&case.expected,&output[0]).is_ok(),"output {:?} did not conform to {}",output[0],case.expected);
    }

    #[test]
    fn concrete_typechecking_is_total(spec in arb_dsrv_spec()) {
        let _=super::super::type_check(spec).into_parts();
    }
}
