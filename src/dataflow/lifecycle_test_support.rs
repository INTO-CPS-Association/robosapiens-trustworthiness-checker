//! Reusable, bounded generators for Dataflow monitor lifecycle properties.
//!
//! This module deliberately generates typed-by-construction definitions. Invalid
//! runtime expression sources are represented by [`DynamicBody`] and are not
//! mixed into the valid compositional definition strategy.

use super::*;
use crate::core::BinaryOperator;
use crate::lang::dsrv::ElaboratedDsrvSpecification;
use crate::lang::dsrv::ast::{DsrvSpecification, Expr, SyntaxLiteral};
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};

pub(in crate::dataflow) const NORMAL_MAX_STREAMS: usize = 8;
pub(in crate::dataflow) const NORMAL_MAX_TRACE_ROWS: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::dataflow) enum TemporalOffset {
    Current,
    One,
    Two,
    Four,
}

impl TemporalOffset {
    pub(in crate::dataflow) fn get(self) -> u64 {
        match self {
            Self::Current => 0,
            Self::One => 1,
            Self::Two => 2,
            Self::Four => 4,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::dataflow) enum LifecycleRecipe {
    Constant(i8),
    Absolute { source: u8 },
    Add { left: u8, right: u8 },
    Lazy { then_source: u8, else_source: u8 },
    Past { source: u8, offset: TemporalOffset },
    PastOrInput { source: u8, offset: TemporalOffset },
    RecursiveSum { offset: TemporalOffset },
}

#[derive(Clone, Debug)]
pub(in crate::dataflow) struct LifecycleCase {
    pub specification: ElaboratedDsrvSpecification,
    pub trace_a: Vec<Vec<Value>>,
    pub trace_b: Vec<Vec<Value>>,
    pub recipes: Vec<LifecycleRecipe>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::dataflow) enum DynamicBody {
    SameDependencies,
    ChangedDependencies,
    DeeperPast,
    Cycle,
    ParseError,
    TypeError,
    ContextError,
    NoValue,
    Deferred,
}

impl DynamicBody {
    pub(in crate::dataflow) fn source(self) -> Value {
        match self {
            Self::SameDependencies => Value::Str("x".into()),
            Self::ChangedDependencies => Value::Str("sum".into()),
            Self::DeeperPast => Value::Str("default(x[4], 0)".into()),
            Self::Cycle => Value::Str("z".into()),
            Self::ParseError => Value::Str("(".into()),
            Self::TypeError => Value::Str("x > 0".into()),
            Self::ContextError => Value::Str("unknown".into()),
            Self::NoValue => Value::NoVal,
            Self::Deferred => Value::Deferred,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::dataflow) enum LifecycleOperation {
    TickA,
    TickB,
    ResetA,
    ResetB,
    EmptyA,
    EmptyB,
}

pub(in crate::dataflow) fn arb_temporal_offset() -> impl Strategy<Value = TemporalOffset> {
    prop_oneof![
        1 => Just(TemporalOffset::Current),
        4 => Just(TemporalOffset::One),
        3 => Just(TemporalOffset::Two),
        2 => Just(TemporalOffset::Four),
    ]
}

fn arb_recipe() -> impl Strategy<Value = LifecycleRecipe> {
    prop_oneof![
        any::<i8>().prop_map(LifecycleRecipe::Constant),
        any::<u8>().prop_map(|source| LifecycleRecipe::Absolute { source }),
        (any::<u8>(), any::<u8>()).prop_map(|(left, right)| LifecycleRecipe::Add { left, right }),
        (any::<u8>(), any::<u8>()).prop_map(|(then_source, else_source)| {
            LifecycleRecipe::Lazy {
                then_source,
                else_source,
            }
        }),
        (any::<u8>(), arb_temporal_offset())
            .prop_map(|(source, offset)| LifecycleRecipe::Past { source, offset }),
        (any::<u8>(), arb_temporal_offset())
            .prop_map(|(source, offset)| { LifecycleRecipe::PastOrInput { source, offset } }),
        arb_temporal_offset().prop_map(|offset| LifecycleRecipe::RecursiveSum { offset }),
    ]
}

fn sparse_int() -> impl Strategy<Value = Value> {
    prop_oneof![
        7 => (-32_i64..=32).prop_map(Value::Int),
        2 => Just(Value::NoVal),
        1 => Just(Value::Deferred),
    ]
}

fn sparse_bool() -> impl Strategy<Value = Value> {
    prop_oneof![
        7 => any::<bool>().prop_map(Value::Bool),
        2 => Just(Value::NoVal),
        1 => Just(Value::Deferred),
    ]
}

fn arb_trace() -> impl Strategy<Value = Vec<Vec<Value>>> {
    prop::collection::vec(
        (sparse_int(), sparse_bool(), -2_i64..=2),
        0..=NORMAL_MAX_TRACE_ROWS,
    )
    .prop_map(|rows| {
        let mut previous_timestamp = None;
        rows.into_iter()
            .map(|(x, flag, delta)| {
                let timestamp = previous_timestamp.map_or(delta, |previous| previous + delta);
                previous_timestamp = Some(timestamp);
                vec![x, flag, Value::Int(timestamp)]
            })
            .collect()
    })
}

pub(in crate::dataflow) fn arb_lifecycle_case() -> impl Strategy<Value = LifecycleCase> {
    (
        prop::collection::vec(arb_recipe(), 1..=NORMAL_MAX_STREAMS),
        arb_trace(),
        arb_trace(),
    )
        .prop_map(|(recipes, trace_a, trace_b)| LifecycleCase {
            specification: specification_from_recipes(&recipes)
                .check_and_elaborate(crate::TypeCheckOptions::GRADUAL)
                .expect("lifecycle specifications check"),
            trace_a,
            trace_b,
            recipes,
        })
}

pub(in crate::dataflow) fn arb_dynamic_body() -> impl Strategy<Value = DynamicBody> {
    prop_oneof![
        4 => Just(DynamicBody::SameDependencies),
        3 => Just(DynamicBody::ChangedDependencies),
        3 => Just(DynamicBody::DeeperPast),
        1 => Just(DynamicBody::Cycle),
        1 => Just(DynamicBody::ParseError),
        1 => Just(DynamicBody::TypeError),
        1 => Just(DynamicBody::ContextError),
        2 => Just(DynamicBody::NoValue),
        2 => Just(DynamicBody::Deferred),
    ]
}

pub(in crate::dataflow) fn arb_operation_schedule() -> impl Strategy<Value = Vec<LifecycleOperation>>
{
    prop::collection::vec(
        prop_oneof![
            5 => Just(LifecycleOperation::TickA),
            5 => Just(LifecycleOperation::TickB),
            2 => Just(LifecycleOperation::ResetA),
            2 => Just(LifecycleOperation::ResetB),
            1 => Just(LifecycleOperation::EmptyA),
            1 => Just(LifecycleOperation::EmptyB),
        ],
        0..=32,
    )
}

fn specification_from_recipes(recipes: &[LifecycleRecipe]) -> DsrvSpecification {
    let x = VarName::new("x");
    let flag = VarName::new("flag");
    let timestamp = VarName::new("timestamp");
    let mut expressions = BTreeMap::new();
    let mut annotations = BTreeMap::from([
        (x.clone(), StreamType::Int),
        (flag.clone(), StreamType::Bool),
        (timestamp.clone(), StreamType::Int),
    ]);

    for (index, recipe) in recipes.iter().enumerate() {
        let name = VarName::from(format!("s{index}"));
        let dependency = |selector: u8| {
            let selected = usize::from(selector) % (index + 1);
            if selected == 0 {
                x.clone()
            } else {
                VarName::from(format!("s{}", selected - 1))
            }
        };
        let var = |name| Expr::Var(name);
        let expression = match recipe {
            LifecycleRecipe::Constant(value) => Expr::Val(SyntaxLiteral::Int(i64::from(*value))),
            LifecycleRecipe::Absolute { source } => Expr::Abs(Box::new(var(dependency(*source)))),
            LifecycleRecipe::Add { left, right } => Expr::BinOp(
                Box::new(var(dependency(*left))),
                Box::new(var(dependency(*right))),
                BinaryOperator::Add,
            ),
            LifecycleRecipe::Lazy {
                then_source,
                else_source,
            } => Expr::If(
                Box::new(var(flag.clone())),
                Box::new(var(dependency(*then_source))),
                Box::new(var(dependency(*else_source))),
            ),
            LifecycleRecipe::Past { source, offset } => {
                Expr::SIndex(Box::new(var(dependency(*source))), offset.get())
            }
            LifecycleRecipe::PastOrInput { source, offset } => Expr::Default(
                Box::new(Expr::SIndex(
                    Box::new(var(dependency(*source))),
                    offset.get(),
                )),
                Box::new(var(x.clone())),
            ),
            LifecycleRecipe::RecursiveSum { offset } => Expr::BinOp(
                Box::new(Expr::Default(
                    Box::new(Expr::SIndex(
                        Box::new(var(name.clone())),
                        offset.get().max(1),
                    )),
                    Box::new(Expr::Val(SyntaxLiteral::Int(0))),
                )),
                Box::new(var(x.clone())),
                BinaryOperator::Add,
            ),
        };
        expressions.insert(name.clone(), expression.into());
        annotations.insert(name, StreamType::Int);
    }

    let outputs = expressions.keys().cloned().collect::<BTreeSet<_>>();
    DsrvSpecification::new(
        BTreeSet::from([x, flag, timestamp]),
        outputs,
        expressions,
        annotations,
        Vec::new(),
    )
}

#[cfg(test)]
mod generator_contract_tests {
    use super::*;
    use crate::core::Semantics;
    use proptest::test_runner::{Config, TestRunner};

    #[test]
    fn lifecycle_generator_samples_compile_and_obey_bounds() {
        let mut runner = TestRunner::new(Config {
            cases: 32,
            failure_persistence: None,
            ..Config::default()
        });
        runner
            .run(&arb_lifecycle_case(), |case| {
                prop_assert!(case.recipes.len() <= NORMAL_MAX_STREAMS);
                prop_assert!(case.trace_a.len() <= NORMAL_MAX_TRACE_ROWS);
                prop_assert!(case.trace_b.len() <= NORMAL_MAX_TRACE_ROWS);
                prop_assert!(
                    DataflowProgram::compile_with_semantics(case.specification, Semantics::Untimed)
                        .is_ok()
                );
                Ok(())
            })
            .unwrap();
    }
}
