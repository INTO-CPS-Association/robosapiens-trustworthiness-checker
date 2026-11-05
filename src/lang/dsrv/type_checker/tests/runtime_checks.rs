use super::super::*;
use crate::core::ValueMap;
use ecow::EcoString;
use test_log::test;

use crate::Value;
use crate::core::{StreamType, UnionPayload};
use ecow::EcoVec;

fn check_value_type(typ: StreamType, value: &Value) -> Result<(), String> {
    check_value_stream_type(&typ, value)
}

#[test]
fn a_listed_struct_field_must_be_in_the_value() {
    let fields = |typ: StreamType| EcoVec::from([(EcoString::from("a"), typ)]);
    let value = |name: &str| Value::Map(ValueMap::from([(name.into(), Value::Unit)]));
    let empty = Value::Map(ValueMap::new());
    for open in [false, true] {
        for typ in [StreamType::Unit] {
            let structure = StreamType::Struct(fields(typ), open);
            assert!(check_value_type(structure.clone(), &empty).is_err());
            assert!(check_value_type(structure.clone(), &value("b")).is_err());
            assert!(check_value_type(structure, &value("a")).is_ok());
        }
    }
}

/// A successful runtime check of nested containers allocates nothing.
#[test]
fn a_runtime_check_that_passes_allocates_nothing() {
    use crate::allocation_counter::assert_no_allocations;
    let target = StreamType::List(Box::new(StreamType::Struct(
        [
            ("a".into(), StreamType::Float),
            ("b".into(), StreamType::Str),
        ]
        .into_iter()
        .collect(),
        true,
    )));
    let record = Value::Map(ValueMap::from([
        ("a".into(), Value::Float(1.5)),
        ("b".into(), Value::Str("s".into())),
        ("c".into(), Value::Int(1)),
    ]));
    let value = Value::List(vec![record; 4].into());
    let result = assert_no_allocations(|| check_value_stream_type(&target, &value));
    assert!(result.is_ok());
}

#[test]
fn a_union_value_is_checked_against_its_alternatives() {
    use crate::core::{ClosedUnion, UnionAlternative, UnionValue};
    let union = StreamType::Union(
        ClosedUnion::new([
            UnionAlternative::new("R", UnionPayload::Of(StreamType::Int)),
            UnionAlternative::new("S", UnionPayload::Nullary),
        ])
        .unwrap(),
    );
    let value = |tag: &str, payload| Value::from(UnionValue::new(tag, payload));

    for accepted in [value("R", Some(Value::Int(1))), value("S", None)] {
        assert!(check_value_type(union.clone(), &accepted).is_ok());
    }
    for refused in [
        value("T", None),
        value("R", None),
        value("S", Some(Value::Int(1))),
        value("R", Some(Value::Str("s".into()))),
        Value::Int(7),
        Value::Map(ValueMap::new()),
    ] {
        assert!(
            check_value_type(union.clone(), &refused).is_err(),
            "{refused}"
        );
    }
}

#[test]
fn test_basic_types_matching() {
    // These types are explicitly handled in the implementation
    assert!(check_value_type(StreamType::Int, &Value::Int(42)).is_ok());
    assert!(check_value_type(StreamType::Str, &Value::Str("hello".into())).is_ok());
    assert!(check_value_type(StreamType::Bool, &Value::Bool(true)).is_ok());

    // Testing Float and Unit types to match expected functionality
    assert!(check_value_type(StreamType::Float, &Value::Float(3.14)).is_ok());
    assert!(check_value_type(StreamType::Unit, &Value::Unit).is_ok());
}

#[test]
fn test_type_mismatches() {
    // Int type with non-Int values
    assert!(check_value_type(StreamType::Int, &Value::Str("42".into())).is_err());
    assert!(check_value_type(StreamType::Int, &Value::Bool(true)).is_err());
    assert!(check_value_type(StreamType::Int, &Value::Float(42.0)).is_err());

    // Str type with non-Str values
    assert!(check_value_type(StreamType::Str, &Value::Int(42)).is_err());
    assert!(check_value_type(StreamType::Str, &Value::Bool(true)).is_err());

    // Bool type with non-Bool values
    assert!(check_value_type(StreamType::Bool, &Value::Int(0)).is_err());
    assert!(check_value_type(StreamType::Bool, &Value::Str("true".into())).is_err());
}

#[test]
fn test_stream_type_validation_checks_recursive_lists() {
    let list_type = StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int))));
    let valid = Value::List(
        vec![
            Value::List(vec![Value::Int(1), Value::Int(2)].into()),
            Value::List(vec![Value::Int(3)].into()),
        ]
        .into(),
    );
    let invalid = Value::List(
        vec![
            Value::List(vec![Value::Int(1)].into()),
            Value::List(vec![Value::Str("wrong".into())].into()),
        ]
        .into(),
    );

    assert!(check_value_stream_type(&list_type, &valid).is_ok());
    assert!(check_value_stream_type(&list_type, &invalid).is_err());
}

#[test]
fn test_list_type_matching() {
    // Empty list
    let empty_list = Value::List(vec![].into());
    assert!(check_value_type(StreamType::List(Box::new(StreamType::Int)), &empty_list).is_ok());
    assert!(check_value_type(StreamType::List(Box::new(StreamType::Str)), &empty_list).is_ok());
    assert!(check_value_type(StreamType::List(Box::new(StreamType::Bool)), &empty_list).is_ok());

    // Homogeneous lists
    let int_list = Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)].into());
    assert!(check_value_type(StreamType::List(Box::new(StreamType::Int)), &int_list).is_ok());
    assert!(check_value_type(StreamType::List(Box::new(StreamType::Str)), &int_list).is_err());

    let str_list = Value::List(
        vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str("c".into()),
        ]
        .into(),
    );
    assert!(check_value_type(StreamType::List(Box::new(StreamType::Str)), &str_list).is_ok());
    assert!(check_value_type(StreamType::List(Box::new(StreamType::Int)), &str_list).is_err());

    // Mixed list (should fail for any specific element type)
    let mixed_list =
        Value::List(vec![Value::Int(1), Value::Str("a".into()), Value::Bool(true)].into());
    assert!(check_value_type(StreamType::List(Box::new(StreamType::Int)), &mixed_list).is_err());
    assert!(check_value_type(StreamType::List(Box::new(StreamType::Str)), &mixed_list).is_err());
    assert!(check_value_type(StreamType::List(Box::new(StreamType::Bool)), &mixed_list).is_err());
}

#[test]
fn test_nested_list_type_matching() {
    // List of List of Int
    let nested_int_list = Value::List(
        vec![
            Value::List(vec![Value::Int(1), Value::Int(2)].into()),
            Value::List(vec![Value::Int(3), Value::Int(4)].into()),
        ]
        .into(),
    );

    let nested_int_type = StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int))));
    assert!(check_value_type(nested_int_type.clone(), &nested_int_list).is_ok());

    // Nested list with type errors
    let nested_mixed_list = Value::List(
        vec![
            Value::List(vec![Value::Int(1), Value::Int(2)].into()),
            Value::List(vec![Value::Str("a".into())].into()),
        ]
        .into(),
    );
    assert!(check_value_type(nested_int_type, &nested_mixed_list).is_err());
}

#[test]
fn test_deeply_nested_list() {
    // Triple nested list
    let triple_nested_list = Value::List(
        vec![Value::List(
            vec![Value::List(
                vec![Value::Bool(true), Value::Bool(false)].into(),
            )]
            .into(),
        )]
        .into(),
    );

    let triple_nested_type = StreamType::List(Box::new(StreamType::List(Box::new(
        StreamType::List(Box::new(StreamType::Bool)),
    ))));
    assert!(check_value_type(triple_nested_type, &triple_nested_list).is_ok());
}

#[test]
fn test_list_depth_mismatches() {
    // A list of integers (depth 1)
    let simple_list = Value::List(vec![Value::Int(1), Value::Int(2)].into());

    // A list of lists of integers (depth 2)
    let nested_list = Value::List(
        vec![
            Value::List(vec![Value::Int(1), Value::Int(2)].into()),
            Value::List(vec![Value::Int(3), Value::Int(4)].into()),
        ]
        .into(),
    );

    // Test: Cannot use a simple list where a nested list is expected
    assert!(
        check_value_type(
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int)))),
            &simple_list
        )
        .is_err()
    );

    // Test: Cannot use a nested list where a simple list is expected
    assert!(check_value_type(StreamType::List(Box::new(StreamType::Int)), &nested_list).is_err());

    // Test: Make sure the error message mentions something about a type mismatch
    let result = check_value_type(
        StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int)))),
        &simple_list,
    );
    assert!(result.is_err());
    let error_msg = result.unwrap_err();
    assert!(error_msg.contains("Type mismatch"));
}

#[test]
fn test_special_values() {
    // These should fail since they're not the expected concrete types
    assert!(check_value_type(StreamType::Int, &Value::Deferred).is_err());
    assert!(check_value_type(StreamType::Str, &Value::NoVal).is_err());
    assert!(
        check_value_type(
            StreamType::List(Box::new(StreamType::Int)),
            &Value::Deferred
        )
        .is_err()
    );
}

#[test]
fn test_error_messages() {
    // Test that error messages contain useful information
    let result = check_value_type(StreamType::Int, &Value::Str("42".into()));
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("Type mismatch"));

    // Test error propagation in nested lists
    let str_in_int_list = Value::List(
        vec![
            Value::Int(1),
            Value::Str("not an int".into()), // This will cause error
            Value::Int(3),
        ]
        .into(),
    );
    let result = check_value_type(
        StreamType::List(Box::new(StreamType::Int)),
        &str_in_int_list,
    );
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("Type mismatch"));
}

#[test]
fn test_boundary_cases() {
    // Non-list value with list type
    assert!(
        check_value_type(StreamType::List(Box::new(StreamType::Int)), &Value::Int(42)).is_err()
    );

    // Trying to treat a non-primitive as primitive
    let list_val = Value::List(vec![Value::Int(1), Value::Int(2)].into());
    assert!(check_value_type(StreamType::Int, &list_val).is_err());

    // Empty nested list
    let empty_nested = Value::List(vec![Value::List(vec![].into())].into());
    assert!(
        check_value_type(
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int)))),
            &empty_nested
        )
        .is_ok()
    );
}

#[test]
fn test_map_value_type_matching() {
    let map = Value::Map(ValueMap::from([
        ("x".into(), Value::Int(1)),
        ("y".into(), Value::Int(2)),
    ]));
    assert!(check_value_type(StreamType::Map(Box::new(StreamType::Int)), &map).is_ok());
    assert!(check_value_type(StreamType::Map(Box::new(StreamType::Str)), &map).is_err());

    let nested = Value::Map(ValueMap::from([(
        "xs".into(),
        Value::List(vec![Value::Bool(true)].into()),
    )]));
    assert!(
        check_value_type(
            StreamType::Map(Box::new(StreamType::List(Box::new(StreamType::Bool)))),
            &nested
        )
        .is_ok()
    );
}
