use std::collections::BTreeMap;

use ecow::{EcoString, eco_vec};

use super::*;
use crate::core::{BinaryOperator, StreamType, UnaryOperator};
use crate::lang::dsrv::test_support::arb_int_power_case;
use proptest::prelude::*;

#[test]
fn casts_and_float_to_integer_operations_cover_boundaries() {
    assert_eq!(
        cast(Value::Int((1_i64 << 53) + 1), &StreamType::Float),
        Ok(Value::Float((1_i64 << 53) as f64))
    );
    for (value, expected) in [
        (Value::Int(-12), "-12"),
        (Value::Float(1.0), "1.0"),
        (Value::Bool(true), "true"),
        (Value::Unit, "()"),
        (Value::Str("text".into()), "text"),
    ] {
        assert_eq!(
            cast(value, &StreamType::Str),
            Ok(Value::Str(expected.into()))
        );
    }
    assert_eq!(
        unary(UnaryOperator::CastFloat, Value::Float(1.5)),
        Ok(Value::Float(1.5))
    );
    assert_eq!(
        unary(UnaryOperator::CastStr, Value::Str("text".into())),
        Ok(Value::Str("text".into()))
    );
    for (operation, input, expected) in [
        (UnaryOperator::Truncate, -1.9, -1),
        (UnaryOperator::Floor, -1.1, -2),
        (UnaryOperator::Ceiling, 1.1, 2),
        (UnaryOperator::Round, 2.4, 2),
        (UnaryOperator::Round, 3.6, 4),
        (UnaryOperator::Round, -3.5, -4),
        (UnaryOperator::Round, -2.5, -2),
        (UnaryOperator::Round, -1.5, -2),
        (UnaryOperator::Round, -0.5, 0),
        (UnaryOperator::Round, 0.5, 0),
        (UnaryOperator::Round, 1.5, 2),
        (UnaryOperator::Round, 2.5, 2),
        (UnaryOperator::Round, 3.5, 4),
        (UnaryOperator::Round, -0.0, 0),
    ] {
        assert_eq!(
            unary(operation, Value::Float(input)),
            Ok(Value::Int(expected))
        );
    }
    for operation in [
        UnaryOperator::Truncate,
        UnaryOperator::Floor,
        UnaryOperator::Ceiling,
        UnaryOperator::Round,
    ] {
        for input in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, i64::MAX as f64] {
            assert!(matches!(
                unary(operation, Value::Float(input)),
                Err(ValueOpError::UnrepresentableInteger { .. })
            ));
        }
    }

    const TWO_TO_53: i64 = 1_i64 << 53;
    for input in [
        TWO_TO_53 - 1,
        TWO_TO_53,
        TWO_TO_53 + 1,
        -(TWO_TO_53 - 1),
        -TWO_TO_53,
        -(TWO_TO_53 + 1),
    ] {
        assert_eq!(
            cast(Value::Int(input), &StreamType::Float),
            Ok(Value::Float(input as f64))
        );
    }

    const TWO_TO_63: f64 = 9_223_372_036_854_775_808.0;
    let below_positive_limit = f64::from_bits(TWO_TO_63.to_bits() - 1);
    assert_eq!(
        unary(UnaryOperator::Truncate, Value::Float(below_positive_limit)),
        Ok(Value::Int(9_223_372_036_854_774_784))
    );
    assert_eq!(
        unary(UnaryOperator::Truncate, Value::Float(-TWO_TO_63)),
        Ok(Value::Int(i64::MIN))
    );
    for input in [
        TWO_TO_63,
        f64::from_bits((-TWO_TO_63).to_bits() + 1),
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ] {
        assert!(matches!(
            unary(UnaryOperator::Truncate, Value::Float(input)),
            Err(ValueOpError::UnrepresentableInteger { .. })
        ));
    }
}

#[test]
fn numeric_operations_promote_mixed_operands_and_check_integer_failures() {
    assert_eq!(
        binary(BinaryOperator::Add, Value::Int(2), Value::Float(0.5)),
        Ok(Value::Float(2.5))
    );
    assert!(matches!(
        binary(BinaryOperator::Add, Value::Int(i64::MAX), Value::Int(1)),
        Err(ValueOpError::IntegerOverflow { .. })
    ));
    assert!(matches!(
        binary(BinaryOperator::Divide, Value::Int(1), Value::Int(0)),
        Err(ValueOpError::IntegerDivisionByZero { .. })
    ));
}

#[test]
fn power_and_inequality_have_canonical_value_semantics() {
    assert_eq!(
        binary(BinaryOperator::Power, Value::Int(2), Value::Int(62)),
        Ok(Value::Int(1_i64 << 62))
    );
    assert_eq!(
        binary(BinaryOperator::Power, Value::Int(-1), Value::Int(i64::MAX)),
        Ok(Value::Int(-1))
    );
    assert_eq!(
        binary(BinaryOperator::Power, Value::Int(4), Value::Float(0.5)),
        Ok(Value::Float(2.0))
    );
    assert!(matches!(
        binary(BinaryOperator::Power, Value::Int(2), Value::Int(-1)),
        Err(ValueOpError::NegativeIntegerExponent { exponent: -1 })
    ));
    assert!(matches!(
        binary(BinaryOperator::Power, Value::Int(2), Value::Int(63)),
        Err(ValueOpError::IntegerOverflow { .. })
    ));
    assert_eq!(
        binary(BinaryOperator::NotEqual, Value::Int(1), Value::Int(2)),
        Ok(Value::Bool(true))
    );
}

// SYN-R13/V3: checked integer power must use the complete i64 exponent domain.
#[test]
fn checked_integer_power_boundaries_are_logarithmic_and_structured() {
    let exact = [
        (0, 0, 1),
        (0, 1, 0),
        (1, i64::MAX, 1),
        (-1, i64::MAX, -1),
        (-1, i64::MAX - 1, 1),
        (2, 62, 1_i64 << 62),
        (-2, 63, i64::MIN),
    ];
    for (base, exponent, expected) in exact {
        assert_eq!(
            binary(
                BinaryOperator::Power,
                Value::Int(base),
                Value::Int(exponent)
            ),
            Ok(Value::Int(expected)),
            "{base} ** {exponent}"
        );
    }
    for (base, exponent) in [(2, 63), (-2, 64), (i64::MAX, 2), (i64::MIN, 2)] {
        assert_eq!(
            binary(
                BinaryOperator::Power,
                Value::Int(base),
                Value::Int(exponent)
            ),
            Err(ValueOpError::IntegerOverflow {
                operation: "exponentiation"
            }),
            "{base} ** {exponent} should overflow"
        );
    }
    for exponent in [-1, i64::MIN] {
        assert_eq!(
            binary(BinaryOperator::Power, Value::Int(0), Value::Int(exponent)),
            Err(ValueOpError::NegativeIntegerExponent { exponent })
        );
        assert!(
            binary(BinaryOperator::Power, Value::Int(1), Value::Int(exponent))
                .unwrap_err()
                .to_string()
                .contains("exponentiation")
        );
    }
    for base in [-1, 0, 1] {
        for exponent in [u32::MAX as i64, u32::MAX as i64 + 1, i64::MAX - 1, i64::MAX] {
            let expected = match base {
                -1 if exponent % 2 == 0 => 1,
                -1 => -1,
                0 if exponent == 0 => 1,
                0 => 0,
                1 => 1,
                _ => unreachable!(),
            };
            assert_eq!(
                binary(
                    BinaryOperator::Power,
                    Value::Int(base),
                    Value::Int(exponent)
                ),
                Ok(Value::Int(expected)),
                "{base} ** {exponent}"
            );
        }
    }
}

// SYN-R14/V4: Float-containing powers use f64::powf, including IEEE special values.
#[test]
fn float_power_uses_promoted_ieee_values() {
    let finite = [
        (Value::Float(4.0), Value::Float(0.5), 2.0),
        (Value::Int(2), Value::Float(0.5), 2.0_f64.sqrt()),
        (Value::Int(2), Value::Float(-1.0), 0.5),
        (Value::Float(2.0), Value::Int(3), 8.0),
        (Value::Float(2.0), Value::Float(-3.0), 0.125),
    ];
    for (left, right, expected) in finite {
        let Value::Float(actual) = binary(BinaryOperator::Power, left, right).unwrap() else {
            panic!("float-containing power must produce Float");
        };
        assert_eq!(actual, expected);
    }
    let Value::Float(promoted_limit) = binary(
        BinaryOperator::Power,
        Value::Int(i64::MAX),
        Value::Float(2.0),
    )
    .unwrap() else {
        panic!("mixed power must produce Float");
    };
    assert_eq!(promoted_limit, (i64::MAX as f64).powf(2.0));
    let Value::Float(zero_to_zero) =
        binary(BinaryOperator::Power, Value::Float(-0.0), Value::Int(0)).unwrap()
    else {
        panic!("float-containing power must produce Float");
    };
    assert_eq!(zero_to_zero, 1.0);
    let Value::Float(infinity) =
        binary(BinaryOperator::Power, Value::Float(0.0), Value::Float(-1.0)).unwrap()
    else {
        panic!("float-containing power must produce Float");
    };
    assert!(infinity.is_infinite() && infinity.is_sign_positive());
    let Value::Float(nan) =
        binary(BinaryOperator::Power, Value::Float(-1.0), Value::Float(0.5)).unwrap()
    else {
        panic!("float-containing power must produce Float");
    };
    assert!(nan.is_nan());
    let Value::Float(overflow) = binary(
        BinaryOperator::Power,
        Value::Float(1e308),
        Value::Float(2.0),
    )
    .unwrap() else {
        panic!("float-containing power must produce Float");
    };
    assert!(overflow.is_infinite() && overflow.is_sign_positive());
    let Value::Float(underflow) = binary(
        BinaryOperator::Power,
        Value::Float(1e-308),
        Value::Float(2.0),
    )
    .unwrap() else {
        panic!("float-containing power must produce Float");
    };
    assert_eq!(underflow, 0.0);
}

// SYN-R12/V1/P05: inequality is the direct complement of canonical equality for every Value.
#[test]
fn inequality_complements_equality_for_special_and_nested_values() {
    let mut map = BTreeMap::new();
    map.insert(
        EcoString::from("nested"),
        Value::List(eco_vec![Value::Int(1)]),
    );
    let values = [
        Value::Int(1),
        Value::Float(1.0),
        Value::Float(f64::NAN),
        Value::Float(f64::INFINITY),
        Value::Float(-0.0),
        Value::Bool(true),
        Value::Str("x".into()),
        Value::Unit,
        Value::List(eco_vec![Value::Int(1), Value::Bool(false)]),
        Value::Tuple(eco_vec![Value::Str("x".into()), Value::Unit]),
        Value::Map(map),
        Value::NoVal,
        Value::Deferred,
    ];
    for left in &values {
        for right in &values {
            let Value::Bool(equal) =
                binary(BinaryOperator::Equal, left.clone(), right.clone()).unwrap()
            else {
                panic!("equality must return Bool");
            };
            assert_eq!(
                binary(BinaryOperator::NotEqual, left.clone(), right.clone()),
                Ok(Value::Bool(!equal)),
                "!= did not complement == for {left:?} and {right:?}"
            );
        }
    }
}

fn arb_supported_value() -> BoxedStrategy<Value> {
    let leaf = prop_oneof![
        any::<i16>().prop_map(|value| Value::Int(i64::from(value))),
        any::<bool>().prop_map(Value::Bool),
        any::<f64>()
            .prop_filter("finite float", |value| value.is_finite())
            .prop_map(Value::Float),
        "[a-z]{0,4}".prop_map(|value| Value::Str(value.into())),
        Just(Value::Unit),
        Just(Value::NoVal),
        Just(Value::Deferred),
    ];
    leaf.prop_recursive(3, 32, 8, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4)
                .prop_map(|values| Value::List(values.into_iter().collect())),
            (inner.clone(), inner.clone())
                .prop_map(|(left, right)| Value::Tuple(eco_vec![left, right])),
            ("[a-z]{1,3}", inner.clone()).prop_map(|(key, value)| {
                let mut map = BTreeMap::new();
                map.insert(key.into(), value);
                Value::Map(map)
            }),
        ]
    })
    .boxed()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // SYN-P02: the canonical checked-power operation agrees with an independent oracle.
    #[test]
    fn integer_power_matches_the_generated_checked_oracle(case in arb_int_power_case()) {
        let actual = binary(
            BinaryOperator::Power,
            Value::Int(case.base),
            Value::Int(case.exponent),
        );
        match case.expected {
            Some(expected) => prop_assert_eq!(actual, Ok(Value::Int(expected))),
            None if case.exponent < 0 => prop_assert_eq!(
                actual,
                Err(ValueOpError::NegativeIntegerExponent { exponent: case.exponent })
            ),
            None => prop_assert_eq!(
                actual,
                Err(ValueOpError::IntegerOverflow { operation: "exponentiation" })
            ),
        }
    }

    // SYN-P05: arbitrary supported values retain the canonical equality complement invariant.
    #[test]
    fn generated_inequality_is_the_canonical_equality_complement(
        left in arb_supported_value(),
        right in arb_supported_value(),
    ) {
        let Value::Bool(equal) =
            binary(BinaryOperator::Equal, left.clone(), right.clone()).unwrap()
        else {
            panic!("equality must return Bool");
        };
        prop_assert_eq!(
            binary(BinaryOperator::NotEqual, left, right),
            Ok(Value::Bool(!equal))
        );
    }
}

#[test]
fn comparison_supports_numbers_booleans_and_strings() {
    assert_eq!(
        binary(BinaryOperator::Less, Value::Int(1), Value::Float(1.5)),
        Ok(Value::Bool(true))
    );
    assert_eq!(
        binary(
            BinaryOperator::Greater,
            Value::Bool(true),
            Value::Bool(false)
        ),
        Ok(Value::Bool(true))
    );
    assert_eq!(
        binary(BinaryOperator::LessEqual, "a".into(), "b".into()),
        Ok(Value::Bool(true))
    );
}

#[test]
fn unordered_float_comparisons_are_false() {
    let operations = [
        BinaryOperator::Less,
        BinaryOperator::LessEqual,
        BinaryOperator::Greater,
        BinaryOperator::GreaterEqual,
    ];
    let operands = [
        (Value::Float(f64::NAN), Value::Float(1.0)),
        (Value::Float(1.0), Value::Float(f64::NAN)),
        (Value::Float(f64::NAN), Value::Int(1)),
        (Value::Int(1), Value::Float(f64::NAN)),
    ];

    for operation in operations {
        for (left, right) in &operands {
            assert_eq!(
                binary(operation, left.clone(), right.clone()),
                Ok(Value::Bool(false)),
                "{operation:?} should be false for {left:?} and {right:?}"
            );
        }
    }
}

#[test]
fn tuple_access_supports_tuples_and_lists() {
    assert_eq!(
        tuple_get(Value::Tuple(eco_vec![Value::Int(1)]), 0),
        Ok(Value::Int(1))
    );
    assert_eq!(
        tuple_get(Value::List(eco_vec![Value::Int(2)]), 0),
        Ok(Value::Int(2))
    );
}

#[test]
fn tuple_access_failures_are_structured() {
    assert_eq!(
        tuple_get(Value::Tuple(eco_vec![Value::Int(1)]), 1),
        Err(ValueOpError::TupleIndexOutOfBounds { index: 1, len: 1 })
    );
    assert_eq!(
        tuple_get(Value::Int(1), 0),
        Err(ValueOpError::InvalidUnaryOperand {
            operation: "tuple indexing",
            operand: Value::Int(1),
        })
    );
}

#[test]
fn list_failures_are_structured() {
    assert_eq!(
        list_index(Value::List(eco_vec![Value::Int(1)]), Value::Int(-1)),
        Err(ValueOpError::NegativeListIndex(-1))
    );
    assert_eq!(
        list_head(Value::List(eco_vec![])),
        Err(ValueOpError::EmptyList)
    );
}

#[test]
fn map_operations_are_copy_on_write_and_report_missing_keys() {
    let key = EcoString::from("key");
    let map = Value::Map(BTreeMap::new());
    let inserted = map_insert(map, &key, Value::Int(3)).unwrap();
    assert_eq!(map_get(inserted.clone(), &key), Ok(Value::Int(3)));
    assert_eq!(map_has_key(inserted, &key), Ok(Value::Bool(true)));
    assert_eq!(
        map_get(Value::Map(BTreeMap::new()), &key),
        Err(ValueOpError::MissingMapKey(key))
    );
}
