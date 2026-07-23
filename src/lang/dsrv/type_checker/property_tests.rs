use super::*;
use crate::core::{StreamType, Value};
use crate::dataflow::DataflowMonitor;
use crate::lang::dsrv::ast::{BoolBinOp, Expr, NumericalBinOp, SBinOp, StrBinOp};
use crate::lang::dsrv::test_support::arb_dsrv_spec;
use crate::{DsrvSpecification, VarName};
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
                    Box::new(Expr::Val(Value::Int(i64::from(lhs)))),
                    Box::new(Expr::Val(Value::Int(i64::from(rhs)))),
                    SBinOp::NOp(NumericalBinOp::Add),
                )
                .into(),
                2 => Expr::If(
                    Box::new(Expr::Val(Value::Bool(condition))),
                    Box::new(Expr::Val(Value::Int(i64::from(lhs)))),
                    Box::new(Expr::Var(VarName::new("i"))),
                )
                .into(),
                _ => Expr::Default(
                    Box::new(Expr::Var(VarName::new("i"))),
                    Box::new(Expr::Val(Value::Int(i64::from(rhs)))),
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
                    Box::new(Expr::Val(Value::Float(lhs))),
                    Box::new(Expr::Val(Value::Float(rhs))),
                    SBinOp::NOp(NumericalBinOp::Add),
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
                Box::new(Expr::Val(Value::Bool(lhs))),
                Box::new(Expr::Val(Value::Bool(rhs))),
                SBinOp::BOp(BoolBinOp::And),
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
                    Box::new(Expr::Val(Value::Str(lhs.into()))),
                    Box::new(Expr::Val(Value::Str(rhs.into()))),
                    SBinOp::SOp(StrBinOp::Concat),
                )
                .into(),
                _ => Expr::If(
                    Box::new(Expr::Val(Value::Bool(true))),
                    Box::new(Expr::Var(VarName::new("s"))),
                    Box::new(Expr::Val(Value::Str(rhs.into()))),
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
                    .map(|value| Expr::Val(Value::Int(i64::from(value))).into())
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
                    .map(|(key, value)| (key.into(), Expr::Val(Value::Bool(value)).into())),
            )
            .into(),
            expected: StreamType::Map(Box::new(StreamType::Bool)),
            inputs: BTreeMap::new(),
        })
        .boxed();
    let tuples = (any::<i16>(), any::<bool>(), "[a-z]{0,8}")
        .prop_map(|(integer, boolean, string)| TypeDirectedCase {
            expr: Expr::Tuple(eco_vec![
                Expr::Val(Value::Int(i64::from(integer))).into(),
                Expr::Val(Value::Bool(boolean)).into(),
                Expr::Val(Value::Str(string.into())).into(),
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
                (
                    "count".into(),
                    Expr::Val(Value::Int(i64::from(count))).into(),
                ),
                ("enabled".into(), Expr::Val(Value::Bool(enabled)).into()),
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
                    eco_vec![(VarName::new("x"), StreamType::Int)],
                    Box::new(Expr::Var(VarName::new("x"))),
                )),
                eco_vec![Expr::Val(Value::Int(i64::from(argument))).into()],
            )
            .into(),
            expected: StreamType::Int,
            inputs: BTreeMap::new(),
            may_widen_without_annotation: false,
        })
        .boxed();
    let unit = Just(TypeDirectedCase {
        expr: Expr::Val(Value::Unit).into(),
        expected: StreamType::Unit,
        inputs: BTreeMap::new(),
        may_widen_without_annotation: false,
    })
    .boxed();

    prop_oneof![
        integers, floats, booleans, strings, lists, maps, tuples, structs, functions, unit,
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

const TYPECHECK_PROPTEST_CASES: u32 = if cfg!(feature = "extended-proptests") {
    10_000
} else {
    256
};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(TYPECHECK_PROPTEST_CASES))]

    #[test]
    fn strict_and_gradual_accept_type_directed_programs(case in arb_type_directed_case()) {
        let spec = specification(&case, true);
        let strict = type_check(spec.clone(), false).expect("type-directed strict program must type check");
        let gradual = type_check_gradual(spec, false).expect("type-directed gradual program must type check");
        let output = VarName::new("result");
        let expected = TCType::from_stream_type(&case.expected);
        let output_expr = strict.var_expr(&output).unwrap();
        prop_assert_eq!(output_expr.typ(), &expected);
        let gradual_output = gradual.var_expr(&output).unwrap();
        prop_assert_eq!(gradual_output.typ(), &expected);
    }

    #[test]
    fn gradual_infers_unannotated_type_directed_programs(case in arb_type_directed_case()) {
        let typed = type_check_gradual(specification(&case, false), false)
            .expect("gradual checker must infer a type-directed expression");
        let result = typed.var_expr(&VarName::new("result")).unwrap();
        let actual = result.typ().clone();
        if !case.may_widen_without_annotation {
            prop_assert_eq!(actual, TCType::from_stream_type(&case.expected));
        }
    }

    #[test]
    fn strict_requires_output_annotations(case in arb_type_directed_case()) {
        let errors = type_check(specification(&case, false), false)
            .expect_err("strict checker must reject a missing output annotation");
        prop_assert!(errors.iter().any(|error| matches!(error, SemanticError::MissingTypeAnnotation(_, _))));
    }

    #[test]
    fn incompatible_annotations_are_rejected_by_both_drivers(case in arb_type_directed_case()) {
        let mut spec = specification(&case, true);
        spec.type_annotations.insert(VarName::new("result"), incompatible_type(&case.expected));
        let strict = type_check(spec.clone(), false).expect_err("strict checker must reject contradiction");
        let gradual = type_check_gradual(spec, false).expect_err("gradual checker must reject contradiction");
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
        let typed = type_check_gradual(spec, false)
            .expect("gradual inference must resolve a forward dependency chain");
        let source_type = typed.var_expr(&source).unwrap().typ().clone();
        let consumer_type = typed.var_expr(&consumer).unwrap().typ().clone();
        prop_assert_eq!(consumer_type, source_type.clone());
        if !case.may_widen_without_annotation {
            prop_assert_eq!(source_type, TCType::from_stream_type(&case.expected));
        }
    }

    #[test]
    fn accepted_typed_programs_produce_values_of_the_declared_type(
        case in arb_type_directed_case()
    ) {
        let typed = type_check(specification(&case, true), false)
            .expect("type-directed program must pass strict checking");
        let mut monitor = DataflowMonitor::try_compile_checked(typed)
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
        let _ = type_check(spec, false);
    }

    #[test]
    fn gradual_typechecking_is_total(spec in arb_dsrv_spec()) {
        let _ = type_check_gradual(spec, false);
    }
}
