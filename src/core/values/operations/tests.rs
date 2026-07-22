use std::collections::BTreeMap;

use ecow::{EcoString, eco_vec};

use super::*;

#[test]
fn numeric_operations_promote_mixed_operands_and_check_integer_failures() {
    assert_eq!(
        binary(BinaryValueOp::Add, Value::Int(2), Value::Float(0.5)),
        Ok(Value::Float(2.5))
    );
    assert!(matches!(
        binary(BinaryValueOp::Add, Value::Int(i64::MAX), Value::Int(1)),
        Err(ValueOpError::IntegerOverflow { .. })
    ));
    assert!(matches!(
        binary(BinaryValueOp::Div, Value::Int(1), Value::Int(0)),
        Err(ValueOpError::IntegerDivisionByZero { .. })
    ));
}

#[test]
fn comparison_supports_numbers_booleans_and_strings() {
    assert_eq!(
        binary(BinaryValueOp::Less, Value::Int(1), Value::Float(1.5)),
        Ok(Value::Bool(true))
    );
    assert_eq!(
        binary(
            BinaryValueOp::Greater,
            Value::Bool(true),
            Value::Bool(false)
        ),
        Ok(Value::Bool(true))
    );
    assert_eq!(
        binary(BinaryValueOp::LessEqual, "a".into(), "b".into()),
        Ok(Value::Bool(true))
    );
}

#[test]
fn unordered_float_comparisons_are_false() {
    let operations = [
        BinaryValueOp::Less,
        BinaryValueOp::LessEqual,
        BinaryValueOp::Greater,
        BinaryValueOp::GreaterEqual,
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
