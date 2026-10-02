use super::*;
use crate::core::BinaryOperator;
use crate::core::{StreamType, Value};
use crate::dataflow::DataflowMonitor;
use crate::lang::dsrv::ast::{Expr, SyntaxLiteral};
use crate::lang::dsrv::diagnostics::SemanticError;
use crate::lang::dsrv::test_support::arb_dsrv_spec;
use crate::lang::dsrv::test_support::{type_check, type_check_gradual};
use crate::{DsrvSpecification, VarName};
use ::core::cfg_select;
use ecow::eco_vec;
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug)]
struct TypeDirectedCase {
    expr: Expr,
    expected: StreamType,
    inputs: BTreeMap<VarName, StreamType>,
    may_widen_without_annotation: bool,
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
            may_widen_without_annotation: false,
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
            may_widen_without_annotation: values.is_empty(),
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
            may_widen_without_annotation: values.is_empty(),
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
            may_widen_without_annotation: false,
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
            may_widen_without_annotation: false,
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
            may_widen_without_annotation: false,
        })
        .boxed();
    let unit = Just(TypeDirectedCase {
        expr: Expr::Val(()).into(),
        expected: StreamType::Unit,
        inputs: BTreeMap::new(),
        may_widen_without_annotation: false,
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
            may_widen_without_annotation: false,
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
            may_widen_without_annotation: false,
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
            may_widen_without_annotation: false,
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
            may_widen_without_annotation: false,
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
            may_widen_without_annotation: false,
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
            may_widen_without_annotation: false,
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

/// Two programs of different types combined by a merge: the result may be
/// either of them, so its type is a join of theirs.
#[derive(Clone, Debug)]
struct MergeCase {
    expr: Expr,
    inputs: BTreeMap<VarName, StreamType>,
    /// Inputs given no annotation, so that gradual checking types them `Any`.
    unannotated: BTreeSet<VarName>,
}

fn arb_merge_case() -> impl Strategy<Value = MergeCase> {
    let guard = || Box::new(Expr::Var(VarName::new("guard")));
    (
        arb_type_directed_case(),
        arb_type_directed_case(),
        any::<bool>(),
        0..3u8,
    )
        .prop_map(move |(first, second, dynamic_second, shape)| {
            let mut inputs = first.inputs.clone();
            inputs.extend(second.inputs.clone());
            inputs.insert(VarName::new("guard"), StreamType::Bool);
            let mut unannotated = BTreeSet::new();
            // An operand read from an input with no annotation.
            let second_expr = if dynamic_second {
                let dynamic = VarName::new("dynamic");
                unannotated.insert(dynamic.clone());
                Expr::Var(dynamic).into()
            } else {
                second.expr.clone()
            };
            let (left, right) = (Box::new(first.expr), Box::new(second_expr));
            let expr = match shape {
                0 => Expr::If(guard(), left, right).into(),
                1 => Expr::Default(left, right).into(),
                _ => Expr::If(guard(), right, left).into(),
            };
            MergeCase {
                expr,
                inputs,
                unannotated,
            }
        })
}

fn merge_specification(case: &MergeCase) -> DsrvSpecification {
    let output = VarName::new("result");
    let mut inputs: BTreeSet<VarName> = case.inputs.keys().cloned().collect();
    inputs.extend(case.unannotated.iter().cloned());
    DsrvSpecification::new(
        inputs,
        BTreeSet::from([output.clone()]),
        BTreeMap::from([(output, case.expr.clone())]),
        case.inputs.clone(),
        Vec::new(),
    )
}

/// Scalars, `Any`, the placeholders, and lists and maps of them.
fn arb_tc_type() -> impl Strategy<Value = TCType> {
    prop_oneof![
        Just(TCType::Int),
        Just(TCType::Float),
        Just(TCType::Bool),
        Just(TCType::Str),
        Just(TCType::Unit),
        Just(TCType::Any),
        Just(TCType::Unknown),
        Just(TCType::EmptyList),
        Just(TCType::EmptyMap),
    ]
    .prop_recursive(2, 6, 2, |inner| {
        prop_oneof![
            inner.clone().prop_map(TCType::list),
            inner.prop_map(TCType::map),
        ]
    })
}

const TYPECHECK_PROPTEST_CASES: u32 = cfg_select! {
    feature = "extended-proptests" => 10_000,
    _ => 256,
};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(TYPECHECK_PROPTEST_CASES))]

    #[test]
    fn join_is_idempotent_and_commutative(a in arb_tc_type(), b in arb_tc_type()) {
        prop_assert_eq!(super::checker::join(&a, &a), a.clone());
        prop_assert_eq!(super::checker::join(&a, &b), super::checker::join(&b, &a));
    }

    #[test]
    fn join_with_any_is_any(a in arb_tc_type()) {
        prop_assert_eq!(super::checker::join(&a, &TCType::Any), TCType::Any);
        prop_assert_eq!(super::checker::join(&TCType::Any, &a), TCType::Any);
    }

    /// Either side may be used where the join is expected, which is what
    /// makes it a type for a stream that carries either.
    #[test]
    fn join_is_an_upper_bound_under_unify(a in arb_tc_type(), b in arb_tc_type()) {
        let joined = super::checker::join(&a, &b);
        prop_assert!(super::checker::unify(&a, &joined).is_some(), "{a} against {joined}");
        prop_assert!(super::checker::unify(&b, &joined).is_some(), "{b} against {joined}");
    }

    /// Gradual checking is inference followed by strict checking: annotate
    /// every variable with the type gradual checking inferred, and strict
    /// checking must accept the result, giving the output the same type.
    #[test]
    fn gradual_acceptance_is_inference_followed_by_strict_checking(case in arb_merge_case()) {
        let spec = merge_specification(&case);
        let (gradual, _warnings) = super::gradual::type_check_gradual(spec.clone()).into_parts();
        // Whether gradual checking accepts is not the property's concern.
        let Ok(gradual) = gradual else {
            return Ok(());
        };
        let mut annotated = spec;
        annotated.type_annotations = gradual.type_annotations().clone();
        let (strict, _warnings) = super::strict::type_check(annotated).into_parts();
        let strict = match strict {
            Ok(strict) => strict,
            Err(errors) => {
                return Err(TestCaseError::fail(format!(
                    "gradual checking inferred {:?} but strict checking rejects it: {errors:?}",
                    gradual.type_annotations()
                )));
            }
        };
        let output = VarName::new("result");
        prop_assert_eq!(
            strict.var_expr_ref(&output).unwrap().typ(),
            gradual.var_expr_ref(&output).unwrap().typ(),
        );
    }

    #[test]
    fn strict_and_gradual_accept_type_directed_programs(case in arb_type_directed_case()) {
        let spec = specification(&case, true);
        let strict = type_check(spec.clone()).expect("type-directed strict program must type check");
        let gradual = type_check_gradual(spec).expect("type-directed gradual program must type check");
        let output = VarName::new("result");
        let expected = TCType::from_stream_type(&case.expected);
        let output_expr = strict.var_expr_ref(&output).unwrap();
        prop_assert_eq!(output_expr.typ(), &expected);
        let gradual_output = gradual.var_expr_ref(&output).unwrap();
        prop_assert_eq!(gradual_output.typ(), &expected);
    }

    #[test]
    fn gradual_infers_unannotated_type_directed_programs(case in arb_type_directed_case()) {
        let typed = type_check_gradual(specification(&case, false))
            .expect("gradual checker must infer a type-directed expression");
        let result = typed.var_expr_ref(&VarName::new("result")).unwrap();
        let actual = result.typ().clone();
        if !case.may_widen_without_annotation {
            prop_assert_eq!(actual, TCType::from_stream_type(&case.expected));
        }
    }

    #[test]
    fn strict_requires_output_annotations(case in arb_type_directed_case()) {
        let errors = type_check(specification(&case, false))
            .expect_err("strict checker must reject a missing output annotation");
        prop_assert!(errors.iter().any(|error| matches!(error, SemanticError::MissingTypeAnnotation(_, _, _))));
    }

    #[test]
    fn incompatible_annotations_are_rejected_by_both_drivers(case in arb_type_directed_case()) {
        let mut spec = specification(&case, true);
        spec.type_annotations.insert(VarName::new("result"), incompatible_type(&case.expected));
        let strict = type_check(spec.clone()).expect_err("strict checker must reject contradiction");
        let gradual = type_check_gradual(spec).expect_err("gradual checker must reject contradiction");
        for errors in [strict, gradual] {
            prop_assert!(!errors.is_empty());
        }
    }

    #[test]
    fn gradual_inference_reaches_a_fixed_point_across_forward_dependencies(
        case in arb_type_directed_case()
    ) {
        let source = VarName::new("z_source");
        let consumer = VarName::new("a_consumer");
        let spec = DsrvSpecification::new(
            case.inputs.keys().cloned().collect(),
            BTreeSet::from([source.clone(), consumer.clone()]),
            BTreeMap::from([
                (consumer.clone(), Expr::Var(source.clone()).into()),
                (source.clone(), case.expr.clone()),
            ]),
            case.inputs.clone(),
            Vec::new(),
        );
        let typed = type_check_gradual(spec)
            .expect("gradual inference must resolve a forward dependency chain");
        let source_type = typed.var_expr_ref(&source).unwrap().typ().clone();
        let consumer_type = typed.var_expr_ref(&consumer).unwrap().typ().clone();
        prop_assert_eq!(consumer_type, source_type.clone());
        if !case.may_widen_without_annotation {
            prop_assert_eq!(source_type, TCType::from_stream_type(&case.expected));
        }
    }

    #[test]
    fn accepted_typed_programs_produce_values_of_the_declared_type(
        case in arb_type_directed_case()
    ) {
        let typed = type_check(specification(&case, true))
            .expect("type-directed program must pass strict checking");
        let mut monitor = DataflowMonitor::compile_checked(typed.elaborate())
            .expect("type-directed program must compile to dataflow");
        let input = monitor
            .input_vars()
            .iter()
            .map(|name| sample_value(&case.inputs[name]))
            .collect::<Vec<_>>();
        let mut output = vec![Value::NoVal; monitor.output_vars().len()];
        monitor.evaluate(&input, &mut output)
            .expect("well-typed generated expression must evaluate");
        prop_assert_eq!(output.len(), 1);
        prop_assert!(
            check_value_stream_type(&case.expected, &output[0]).is_ok(),
            "output {:?} did not conform to {}",
            output[0],
            case.expected,
        );
    }

    #[test]
    fn strict_typechecking_is_total(spec in arb_dsrv_spec()) {
        let _ = type_check(spec);
    }

    #[test]
    fn gradual_typechecking_is_total(spec in arb_dsrv_spec()) {
        let _ = type_check_gradual(spec);
    }
}
