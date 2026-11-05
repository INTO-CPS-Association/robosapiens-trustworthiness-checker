//! Concrete structural compatibility without synthesized common types.

use super::super::relations::{comparable, flow};
use crate::core::StreamType;
use crate::core::{ClosedUnion, UnionAlternative, UnionPayload};
use ecow::eco_vec;
use proptest::prelude::*;

fn list(t: StreamType) -> StreamType {
    StreamType::List(Box::new(t))
}
fn map(t: StreamType) -> StreamType {
    StreamType::Map(Box::new(t))
}
fn record(t: StreamType, wide: bool, open: bool) -> StreamType {
    let mut fields = eco_vec![("a".into(), t)];
    if wide {
        fields.push(("b".into(), StreamType::Str));
    }
    StreamType::Struct(fields, open)
}
fn function(p: StreamType, r: StreamType) -> StreamType {
    StreamType::Function(eco_vec![p], Box::new(r))
}

fn arb_tc_type() -> impl Strategy<Value = StreamType> {
    arb_tc_type_naming()
}

/// As [`arb_tc_type`], of the type variables in `vars`.
fn arb_tc_type_naming() -> impl Strategy<Value = StreamType> {
    prop_oneof![
        Just(StreamType::Int),
        Just(StreamType::Float),
        Just(StreamType::Bool),
        Just(StreamType::Str),
        Just(StreamType::Unit),
    ]
    .prop_recursive(2, 6, 2, |inner| {
        prop_oneof![
            inner.clone().prop_map(list),
            inner.clone().prop_map(map),
            (inner.clone(), inner).prop_map(|(a, b)| StreamType::Tuple(eco_vec![a, b])),
        ]
    })
}

/// As [`arb_tc_type`], with what function parameters and struct widths are
/// about: open and closed structs of overlapping fields, and function types.
fn arb_shaped_type() -> impl Strategy<Value = StreamType> {
    arb_tc_type().prop_recursive(2, 8, 2, |inner| {
        prop_oneof![
            (inner.clone(), inner.clone(), any::<bool>()).prop_map(|(a, b, open)| {
                StreamType::Struct(eco_vec![("a".into(), a), ("b".into(), b)], open)
            }),
            (inner.clone(), any::<bool>())
                .prop_map(|(a, open)| StreamType::Struct(eco_vec![("a".into(), a)], open)),
            inner.clone().prop_map(|a| StreamType::Expr(Box::new(a))),
            inner.clone().prop_map(|a| StreamType::Union(
                ClosedUnion::new([
                    UnionAlternative::new("A", UnionPayload::Of(a)),
                    UnionAlternative::new("B", UnionPayload::Nullary),
                ])
                .unwrap()
            )),
            (inner.clone(), inner)
                .prop_map(|(a, b)| StreamType::Function(eco_vec![a], Box::new(b))),
        ]
    })
}

proptest! {
    #![proptest_config(super::property_config())]

    #[test]
    fn concrete_flow_is_reflexive(t in arb_shaped_type()) {
        prop_assert_eq!(flow(&t,&t), Ok(()));
    }

    #[test]
    fn comparison_compatibility_is_symmetric(a in arb_shaped_type(), b in arb_shaped_type()) {
        prop_assert_eq!(comparable(&a,&b), comparable(&b,&a));
    }

    #[test]
    fn explicit_open_target_admits_width(t in arb_shaped_type()) {
        let narrow=record(t.clone(),false,true);
        let wide=record(t.clone(),true,false);
        prop_assert_eq!(flow(&wide,&narrow),Ok(()));
        prop_assert!(flow(&narrow,&wide).is_err());
        prop_assert!(flow(&wide,&record(t,false,false)).is_err());
    }

    #[test]
    fn functions_reverse_parameter_width_and_preserve_result_width(t in arb_shaped_type(), r in arb_shaped_type()) {
        let narrow=record(t.clone(),false,true);
        let wide=record(t,true,false);
        let result_narrow=record(r.clone(),false,true);
        let result_wide=record(r,true,false);
        prop_assert_eq!(flow(&function(narrow.clone(),result_wide.clone()),&function(wide.clone(),result_narrow.clone())),Ok(()));
        prop_assert!(flow(&function(wide.clone(),result_wide.clone()),&function(narrow.clone(),result_narrow.clone())).is_err());
        prop_assert!(flow(&function(narrow.clone(),result_narrow),&function(wide,result_wide)).is_err());
    }




}

#[test]
fn unrelated_types_union_payloads_and_function_arities_are_refused() {
    for (actual, expected) in [
        (StreamType::Int, StreamType::Str),
        (
            StreamType::Expr(Box::new(StreamType::Int)),
            StreamType::Expr(Box::new(StreamType::Str)),
        ),
        (
            function(StreamType::Int, StreamType::Int),
            StreamType::Function(
                eco_vec![StreamType::Int, StreamType::Int],
                Box::new(StreamType::Int),
            ),
        ),
    ] {
        assert!(flow(&actual, &expected).is_err());
    }
    let union = |payload| {
        StreamType::Union(
            ClosedUnion::new([UnionAlternative::new("A", UnionPayload::Of(payload))]).unwrap(),
        )
    };
    assert!(flow(&union(StreamType::Int), &union(StreamType::Str)).is_err());
}
