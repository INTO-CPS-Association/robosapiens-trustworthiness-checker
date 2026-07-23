use std::collections::{BTreeMap, BTreeSet};

use proptest::prelude::*;

use crate::{DsrvSpecification, Value, VarName, core::BinaryOperator, lang::dsrv::ast::Expr};
// Mixed type expressions. Note that these are not fully recursively mixed-type as we switch to
// single type expressions within the individual branches of the mixed type expression
pub fn arb_mixed_sexpr(vars: Vec<VarName>) -> impl Strategy<Value = Expr> {
    let bool_leaf = prop_oneof![
        any::<bool>().prop_map(Expr::Val),
        proptest::sample::select(vars.clone()).prop_map(|x| Expr::Var(x.clone())),
    ];

    let int_cmp = prop_oneof![
        (arb_int_sexpr(vars.clone()), arb_int_sexpr(vars.clone()))
            .prop_map(|(a, b)| { Expr::BinOp(Box::new(a), Box::new(b), BinaryOperator::Equal) }),
        (arb_int_sexpr(vars.clone()), arb_int_sexpr(vars.clone())).prop_map(|(a, b)| {
            Expr::BinOp(Box::new(a), Box::new(b), BinaryOperator::LessEqual)
        }),
        (arb_int_sexpr(vars.clone()), arb_int_sexpr(vars.clone()))
            .prop_map(|(a, b)| { Expr::BinOp(Box::new(a), Box::new(b), BinaryOperator::Less) }),
        (arb_int_sexpr(vars.clone()), arb_int_sexpr(vars.clone())).prop_map(|(a, b)| {
            Expr::BinOp(Box::new(a), Box::new(b), BinaryOperator::GreaterEqual)
        }),
        (arb_int_sexpr(vars.clone()), arb_int_sexpr(vars.clone()))
            .prop_map(|(a, b)| { Expr::BinOp(Box::new(a), Box::new(b), BinaryOperator::Greater) }),
    ];

    let float_cmp = prop_oneof![
        (arb_float_sexpr(vars.clone()), arb_float_sexpr(vars.clone()))
            .prop_map(|(a, b)| { Expr::BinOp(Box::new(a), Box::new(b), BinaryOperator::Equal) }),
        (arb_float_sexpr(vars.clone()), arb_float_sexpr(vars.clone())).prop_map(|(a, b)| {
            Expr::BinOp(Box::new(a), Box::new(b), BinaryOperator::LessEqual)
        }),
        (arb_float_sexpr(vars.clone()), arb_float_sexpr(vars.clone()))
            .prop_map(|(a, b)| { Expr::BinOp(Box::new(a), Box::new(b), BinaryOperator::Less) }),
        (arb_float_sexpr(vars.clone()), arb_float_sexpr(vars.clone())).prop_map(|(a, b)| {
            Expr::BinOp(Box::new(a), Box::new(b), BinaryOperator::GreaterEqual)
        }),
        (arb_float_sexpr(vars.clone()), arb_float_sexpr(vars.clone()))
            .prop_map(|(a, b)| { Expr::BinOp(Box::new(a), Box::new(b), BinaryOperator::Greater) }),
    ];

    let string_cmp = prop_oneof![
        (
            arb_string_sexpr(vars.clone()),
            arb_string_sexpr(vars.clone())
        )
            .prop_map(|(a, b)| { Expr::BinOp(Box::new(a), Box::new(b), BinaryOperator::Equal) }),
    ];

    let comparison_leaf = prop_oneof![int_cmp, float_cmp, string_cmp];

    prop_oneof![bool_leaf, comparison_leaf].prop_recursive(5, 50, 10, |inner| {
        prop_oneof![
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Expr::BinOp(
                Box::new(a),
                Box::new(b),
                BinaryOperator::Or
            )),
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Expr::BinOp(
                Box::new(a),
                Box::new(b),
                BinaryOperator::And
            )),
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Expr::BinOp(
                Box::new(a),
                Box::new(b),
                BinaryOperator::Implication
            )),
            (inner.clone(), inner.clone(), inner.clone()).prop_map(|(c, t, e)| Expr::If(
                Box::new(c),
                Box::new(t),
                Box::new(e),
            )),
            inner.clone().prop_map(|a| Expr::Not(Box::new(a))),
        ]
    })
}

pub fn arb_boolean_sexpr(vars: Vec<VarName>) -> impl Strategy<Value = Expr> {
    let leaf = prop_oneof![
        any::<bool>().prop_map(|x| Expr::Val(x)),
        proptest::sample::select(vars.clone()).prop_map(|x| Expr::Var(x.clone())),
    ];
    leaf.prop_recursive(5, 50, 10, |inner| {
        prop_oneof![
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Expr::BinOp(
                Box::new(a),
                Box::new(b),
                BinaryOperator::Or
            )),
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Expr::BinOp(
                Box::new(a),
                Box::new(b),
                BinaryOperator::And
            )),
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Expr::BinOp(
                Box::new(a),
                Box::new(b),
                BinaryOperator::And
            )),
        ]
    })
}

pub fn arb_int_sexpr(vars: Vec<VarName>) -> impl Strategy<Value = Expr> {
    let leaf = prop_oneof![
        (0..=i64::MAX).prop_map(Expr::Val),
        proptest::sample::select(vars.clone()).prop_map(|x| Expr::Var(x.clone())),
    ];
    leaf.prop_recursive(5, 50, 10, move |inner| {
        prop_oneof![
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Expr::BinOp(
                Box::new(a),
                Box::new(b),
                BinaryOperator::Add
            )),
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Expr::BinOp(
                Box::new(a),
                Box::new(b),
                BinaryOperator::Subtract
            )),
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Expr::BinOp(
                Box::new(a),
                Box::new(b),
                BinaryOperator::Multiply
            )),
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Expr::BinOp(
                Box::new(a),
                Box::new(b),
                BinaryOperator::Divide
            )),
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Expr::BinOp(
                Box::new(a),
                Box::new(b),
                BinaryOperator::Modulo
            )),
            (
                arb_boolean_sexpr(vars.clone()),
                inner.clone(),
                inner.clone()
            )
                .prop_map(|(c, t, e)| Expr::If(Box::new(c), Box::new(t), Box::new(e),)),
            inner.clone().prop_map(|value| Expr::Neg(Box::new(value))),
        ]
    })
}

pub fn arb_float_sexpr(vars: Vec<VarName>) -> impl Strategy<Value = Expr> {
    let leaf = prop_oneof![
        any::<f64>()
            .prop_filter("finite positive non-integer float", |x| x.is_finite()
                && x.is_sign_positive()
                && x.fract() != 0.0)
            .prop_map(Expr::Val),
        proptest::sample::select(vars.clone()).prop_map(|x| Expr::Var(x.clone())),
    ];
    leaf.prop_recursive(5, 50, 10, move |inner| {
        prop_oneof![
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Expr::BinOp(
                Box::new(a),
                Box::new(b),
                BinaryOperator::Add
            )),
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Expr::BinOp(
                Box::new(a),
                Box::new(b),
                BinaryOperator::Subtract
            )),
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Expr::BinOp(
                Box::new(a),
                Box::new(b),
                BinaryOperator::Multiply
            )),
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Expr::BinOp(
                Box::new(a),
                Box::new(b),
                BinaryOperator::Divide
            )),
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Expr::BinOp(
                Box::new(a),
                Box::new(b),
                BinaryOperator::Modulo
            )),
            (
                arb_boolean_sexpr(vars.clone()),
                inner.clone(),
                inner.clone()
            )
                .prop_map(|(c, t, e)| Expr::If(Box::new(c), Box::new(t), Box::new(e),)),
            inner.clone().prop_map(|a| Expr::Sin(Box::new(a))),
            inner.clone().prop_map(|a| Expr::Cos(Box::new(a))),
            inner.clone().prop_map(|a| Expr::Tan(Box::new(a))),
            inner.clone().prop_map(|a| Expr::Abs(Box::new(a))),
            inner.clone().prop_map(|value| Expr::Neg(Box::new(value))),
        ]
    })
}

pub fn arb_string_sexpr(vars: Vec<VarName>) -> impl Strategy<Value = Expr> {
    let leaf = prop_oneof![
        "[a-zA-Z0-9 _-]{1,24}".prop_map(|s| Expr::Val(Value::Str(s.into()))),
        proptest::sample::select(vars.clone()).prop_map(|x| Expr::Var(x.clone())),
    ];

    leaf.prop_recursive(5, 50, 10, move |inner| {
        prop_oneof![
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Expr::BinOp(
                Box::new(a),
                Box::new(b),
                BinaryOperator::Concatenate
            )),
            (
                arb_boolean_sexpr(vars.clone()),
                inner.clone(),
                inner.clone()
            )
                .prop_map(|(c, t, e)| Expr::If(Box::new(c), Box::new(t), Box::new(e))),
            (inner.clone(), inner.clone())
                .prop_map(|(a, b)| Expr::Default(Box::new(a), Box::new(b))),
            inner.clone().prop_map(|a| Expr::When(Box::new(a))),
            (inner.clone(), inner.clone())
                .prop_map(|(a, b)| Expr::Update(Box::new(a), Box::new(b))),
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Expr::Latch(Box::new(a), Box::new(b))),
        ]
    })
}

pub fn arb_boolean_dsrv_spec() -> impl Strategy<Value = DsrvSpecification> {
    (
        // Generate a hash set of inputs from 'a' to 'h' with at least one element.
        prop::collection::hash_set("[a-h]", 1..5),
        // Generate a hash set of outputs from 'i' to 'z'. Could be empty.
        prop::collection::hash_set("[i-z]", 0..5),
    )
        .prop_flat_map(|(input_set, output_set)| {
            // Convert the sets into Vec<VarName>
            let input_vars: BTreeSet<VarName> = input_set.into_iter().map(|s| s.into()).collect();
            let output_vars: BTreeSet<_> = output_set.into_iter().map(|s| s.into()).collect();

            // Combine input and output variables.
            let all_vars = input_vars
                .clone()
                .into_iter()
                .chain(output_vars.clone().into_iter())
                .collect::<Vec<VarName>>();

            // Create a strategy for generating the expression map.
            // For each key (chosen from the union of variables) generate an expression.
            prop::collection::btree_map(
                prop::sample::select(all_vars.clone()),
                arb_boolean_sexpr(all_vars.clone()),
                0..=all_vars.len(),
            )
            .prop_map(move |exprs| {
                DsrvSpecification::new(
                    input_vars.clone(),
                    output_vars.clone(),
                    exprs,
                    BTreeMap::new(),
                    Vec::new(),
                )
            })
        })
}

pub fn arb_dsrv_spec() -> impl Strategy<Value = DsrvSpecification> {
    (
        prop::collection::btree_set("[a-h]", 0..5),
        prop::collection::btree_set("[i-z]", 0..5),
    )
        .prop_flat_map(|(input_set, stream_set)| {
            let input_vars = input_set
                .into_iter()
                .map(VarName::from)
                .collect::<BTreeSet<_>>();
            let stream_vars = stream_set
                .into_iter()
                .map(VarName::from)
                .collect::<BTreeSet<_>>();
            let mut vars = input_vars
                .iter()
                .chain(&stream_vars)
                .cloned()
                .collect::<Vec<_>>();
            // Keep expression generation defined for empty declarations and include an
            // undeclared name so unavailable-reference handling is exercised routinely.
            vars.push(VarName::new("unknown"));
            let expression = prop_oneof![
                arb_boolean_sexpr(vars.clone()).boxed(),
                arb_int_sexpr(vars.clone()).boxed(),
                arb_float_sexpr(vars.clone()).boxed(),
                arb_string_sexpr(vars.clone()).boxed(),
                arb_mixed_sexpr(vars.clone()).boxed(),
            ];

            prop::collection::btree_map("[a-z]".prop_map(VarName::from), expression, 0..8).prop_map(
                move |exprs| {
                    DsrvSpecification::new(
                        input_vars.clone(),
                        stream_vars.clone(),
                        exprs,
                        BTreeMap::new(),
                        Vec::new(),
                    )
                },
            )
        })
}
