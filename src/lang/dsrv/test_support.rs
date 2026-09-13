#![allow(
    dead_code,
    reason = "reusable strategies precede their test implementations"
)]

use std::collections::{BTreeMap, BTreeSet};

use proptest::prelude::*;

use crate::{DsrvSpecification, Value, VarName, core::BinaryOperator, lang::dsrv::ast::Expr};

/// A generated integer-power input and its independent checked-arithmetic oracle.
///
/// `expected == None` means either a negative exponent or mathematical overflow.
#[derive(Clone, Copy, Debug)]
pub(crate) struct IntPowerCase {
    pub(crate) base: i64,
    pub(crate) exponent: i64,
    pub(crate) expected: Option<i64>,
}

/// Small-domain oracle deliberately uses repeated multiplication rather than
/// exponentiation by squaring. Keeping the model algorithmically different from
/// the implementation makes the property useful for catching a shared bug.
fn model_checked_int_power(base: i64, exponent: i64) -> Option<i64> {
    if exponent < 0 {
        return None;
    }
    let mut result = 1_i64;
    for _ in 0..exponent {
        result = result.checked_mul(base)?;
    }
    Some(result)
}

fn model_full_width_unit_power(base: i64, exponent: i64) -> Option<i64> {
    if exponent < 0 {
        return None;
    }
    Some(match base {
        -1 if exponent % 2 == 0 => 1,
        -1 => -1,
        0 if exponent == 0 => 1,
        0 => 0,
        1 => 1,
        _ => unreachable!("full-width unit power only generates -1, 0, and 1"),
    })
}

/// Full-width cases use identities for `-1`, `0`, and `1`, so they remain
/// constant-time even when the exponent is `i64::MAX`.
pub(crate) fn arb_full_width_int_power_case() -> impl Strategy<Value = IntPowerCase> {
    let base = proptest::sample::select(vec![-1_i64, 0, 1]);
    let exponent = prop_oneof![
        4 => 0_i64..=i64::MAX,
        1 => proptest::sample::select(vec![i64::MIN, -1]),
    ];
    (base, exponent).prop_map(|(base, exponent)| IntPowerCase {
        base,
        exponent,
        expected: model_full_width_unit_power(base, exponent),
    })
}

/// Integer-power cases keep the repeated-multiplication oracle bounded while
/// retaining a separate full-width identity strategy for exponent narrowing.
///
/// Bounded operands shrink naturally toward zero. The full-width branch shrinks
/// toward the zero exponent and retains the `-1`/`0`/`1` identities needed to
/// exercise the complete runtime exponent domain.
pub(crate) fn arb_int_power_case() -> impl Strategy<Value = IntPowerCase> {
    let bounded = (-16_i64..=16, -16_i64..=16).prop_map(|(base, exponent)| IntPowerCase {
        base,
        exponent,
        expected: model_checked_int_power(base, exponent),
    });
    prop_oneof![
        8 => bounded,
        2 => arb_full_width_int_power_case(),
    ]
}

/// Valid integer source literals and their values.
///
/// Magnitudes never exceed `i64::MAX`; optional leading zeroes and a syntactic
/// unary minus are included. Consequently this strategy never emits `i64::MIN`.
pub(crate) fn arb_valid_integer_literal_source() -> impl Strategy<Value = (String, i64)> {
    (0..=i64::MAX, 0_usize..=4, any::<bool>()).prop_map(|(magnitude, leading_zeroes, negative)| {
        let digits = format!("{}{magnitude}", "0".repeat(leading_zeroes));
        if negative {
            (format!("-{digits}"), -magnitude)
        } else {
            (digits, magnitude)
        }
    })
}

/// Deliberately out-of-range integer source literals, with and without `-`.
pub(crate) fn arb_invalid_integer_literal_source() -> impl Strategy<Value = String> {
    ("[1-9][0-9]{19,39}", any::<bool>(), 0_usize..=3).prop_map(
        |(digits, negative, leading_zeroes)| {
            let sign = if negative { "-" } else { "" };
            format!("{sign}{}{digits}", "0".repeat(leading_zeroes))
        },
    )
}

/// Syntactically valid, finite decimal/scientific float literals and their oracle values.
///
/// Exponents are bounded for fast parsing and finite output. The mandatory
/// decimal point prevents integral-looking strings from entering this domain.
pub(crate) fn arb_finite_float_literal_source() -> impl Strategy<Value = (String, f64)> {
    (
        0_u32..=999_999,
        0_u32..=999_999,
        -100_i16..=100,
        0_usize..=3,
    )
        .prop_map(|(whole, fraction, exponent, leading_zeroes)| {
            let source = format!(
                "{}{whole}.{fraction:06}e{exponent:+}",
                "0".repeat(leading_zeroes)
            );
            let value = source
                .parse::<f64>()
                .expect("bounded generated float must be finite and parseable");
            (source, value)
        })
}

/// Pairs of equivalent accepted delimited forms (without and with one trailing comma).
pub(crate) fn arb_trailing_comma_pair() -> impl Strategy<Value = (&'static str, &'static str)> {
    proptest::sample::select(vec![
        ("[1, 2]", "[1, 2,]"),
        ("f(1, 2)", "f(1, 2,)"),
        ("{x, y}", "{x, y,}"),
        ("Map(\"x\": 1)", "Map(\"x\": 1,)"),
        ("Struct(\"x\": 1)", "Struct(\"x\": 1,)"),
        ("Tuple(1, 2)", "Tuple(1, 2,)"),
        ("List<Int>", "List<Int,>"),
        ("(Int, Bool)", "(Int, Bool,)"),
        ("Struct<x: Int, ...>", "Struct<x: Int, ...,>"),
        ("dynamic(source, {x})", "dynamic(source, {x,},)"),
    ])
}

fn arb_small_int_display_expr() -> BoxedStrategy<Expr> {
    let atom = prop_oneof![
        (0_i64..=9).prop_map(Expr::Val),
        proptest::sample::select(vec!["base", "exponent", "left", "right"])
            .prop_map(|name| Expr::Var(VarName::new(name))),
    ];
    atom.prop_recursive(1, 4, 2, |inner| {
        prop_oneof![
            (inner.clone(), inner.clone()).prop_map(|(left, right)| Expr::BinOp(
                Box::new(left),
                Box::new(right),
                BinaryOperator::Add,
            )),
            (inner.clone(), inner.clone()).prop_map(|(left, right)| Expr::BinOp(
                Box::new(left),
                Box::new(right),
                BinaryOperator::Multiply,
            )),
            inner.prop_map(|value| Expr::Neg(Box::new(value))),
        ]
    })
    .boxed()
}

/// Bounded, type-shaped expressions for SYN-P06.
///
/// Every generated tree is an integer expression containing a first-class
/// `NotEqual`, a `Power` with a syntactically negative base, a `Neg` node, and
/// an adjacent-precedence arithmetic operator. The fixed outer shape makes
/// those reachability guarantees survive shrinking instead of relying on
/// filtering random recursive trees.
pub(crate) fn arb_revised_operator_display_expr() -> impl Strategy<Value = Expr> {
    let integer = arb_small_int_display_expr();
    let adjacent = prop_oneof![
        (integer.clone(), integer.clone()).prop_map(|(left, right)| Expr::BinOp(
            Box::new(left),
            Box::new(right),
            BinaryOperator::Add,
        )),
        (integer.clone(), integer.clone()).prop_map(|(left, right)| Expr::BinOp(
            Box::new(left),
            Box::new(right),
            BinaryOperator::Multiply,
        )),
    ]
    .boxed();
    let power = (adjacent.clone(), integer.clone())
        .prop_map(|(base, exponent)| {
            Expr::BinOp(
                Box::new(Expr::Neg(Box::new(base))),
                Box::new(exponent),
                BinaryOperator::Power,
            )
        })
        .boxed();
    let signed_power = prop_oneof![
        3 => power.clone(),
        1 => power.prop_map(|value| Expr::Neg(Box::new(value))),
    ];
    prop_oneof![
        (signed_power.clone(), adjacent.clone()).prop_map(|(left, right)| Expr::BinOp(
            Box::new(left),
            Box::new(right),
            BinaryOperator::NotEqual,
        )),
        (adjacent, signed_power).prop_map(|(left, right)| Expr::BinOp(
            Box::new(left),
            Box::new(right),
            BinaryOperator::NotEqual,
        )),
    ]
}

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
        (arb_int_sexpr(vars.clone()), arb_int_sexpr(vars.clone()))
            .prop_map(|(a, b)| { Expr::BinOp(Box::new(a), Box::new(b), BinaryOperator::NotEqual) }),
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
        (arb_float_sexpr(vars.clone()), arb_float_sexpr(vars.clone()))
            .prop_map(|(a, b)| { Expr::BinOp(Box::new(a), Box::new(b), BinaryOperator::NotEqual) }),
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
        (
            arb_string_sexpr(vars.clone()),
            arb_string_sexpr(vars.clone())
        )
            .prop_map(|(a, b)| { Expr::BinOp(Box::new(a), Box::new(b), BinaryOperator::NotEqual) }),
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
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Expr::BinOp(
                Box::new(a),
                Box::new(b),
                BinaryOperator::NotEqual
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
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Expr::BinOp(
                Box::new(a),
                Box::new(b),
                BinaryOperator::Power
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
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Expr::BinOp(
                Box::new(a),
                Box::new(b),
                BinaryOperator::Power
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
