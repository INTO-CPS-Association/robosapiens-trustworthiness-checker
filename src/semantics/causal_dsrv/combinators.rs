use async_stream::stream;
use futures::{StreamExt, join, stream as futures_stream};

use crate::causal::{CausalDomain, CausalRole, CausalValue};
use crate::core::values::operations;
use crate::core::{BinaryOperator, UnaryOperator};
use crate::{OutputStream, Value};

fn marker<D: CausalDomain>(value: Value, explanation: D) -> CausalValue<D> {
    CausalValue::new(value, explanation)
}

pub fn constant<D: CausalDomain>(value: Value) -> OutputStream<CausalValue<D>> {
    Box::pin(futures_stream::repeat(CausalValue::constant(value)))
}

pub fn lift_base<D: CausalDomain>(
    mut input: OutputStream<CausalValue<D>>,
) -> OutputStream<CausalValue<D>> {
    Box::pin(stream! {
        let mut last: Option<CausalValue<D>> = None;
        while let Some(current) = input.next().await {
            if current.value == Value::NoVal {
                if let Some(previous) = &mut last {
                    previous.explanation = previous
                        .explanation
                        .clone()
                        .with_context(current.explanation, CausalRole::Retention);
                    yield previous.clone();
                } else {
                    yield current;
                }
            } else {
                last = Some(current.clone());
                yield current;
            }
        }
    })
}

pub fn unary<D: CausalDomain>(
    operation: UnaryOperator,
    input: OutputStream<CausalValue<D>>,
) -> OutputStream<CausalValue<D>> {
    Box::pin(lift_base(input).map(move |input| {
        let value = match input.value {
            Value::NoVal => Value::NoVal,
            Value::Deferred => Value::Deferred,
            value => operations::unary(operation, value).unwrap_or_else(|error| panic!("{error}")),
        };
        marker(value, input.explanation)
    }))
}

pub fn binary<D: CausalDomain>(
    operation: BinaryOperator,
    left: OutputStream<CausalValue<D>>,
    right: OutputStream<CausalValue<D>>,
) -> OutputStream<CausalValue<D>> {
    Box::pin(
        lift_base(left)
            .zip(lift_base(right))
            .map(move |(left, right)| {
                let explanation = left.explanation.joint(right.explanation);
                let value = if left.value == Value::NoVal || right.value == Value::NoVal {
                    Value::NoVal
                } else if left.value == Value::Deferred || right.value == Value::Deferred {
                    Value::Deferred
                } else {
                    operations::binary(operation, left.value, right.value)
                        .unwrap_or_else(|error| panic!("{error}"))
                };
                marker(value, explanation)
            }),
    )
}

pub fn and<D: CausalDomain>(
    left: OutputStream<CausalValue<D>>,
    right: OutputStream<CausalValue<D>>,
) -> OutputStream<CausalValue<D>> {
    boolean_binary(BinaryOperator::And, left, right)
}

pub fn or<D: CausalDomain>(
    left: OutputStream<CausalValue<D>>,
    right: OutputStream<CausalValue<D>>,
) -> OutputStream<CausalValue<D>> {
    boolean_binary(BinaryOperator::Or, left, right)
}

pub fn implication<D: CausalDomain>(
    antecedent: OutputStream<CausalValue<D>>,
    consequent: OutputStream<CausalValue<D>>,
) -> OutputStream<CausalValue<D>> {
    Box::pin(
        lift_base(antecedent)
            .zip(lift_base(consequent))
            .map(|(antecedent, consequent)| {
                let value = if antecedent.value == Value::NoVal || consequent.value == Value::NoVal
                {
                    Value::NoVal
                } else if antecedent.value == Value::Deferred || consequent.value == Value::Deferred
                {
                    Value::Deferred
                } else {
                    operations::binary(
                        BinaryOperator::Implication,
                        antecedent.value.clone(),
                        consequent.value.clone(),
                    )
                    .unwrap_or_else(|error| panic!("{error}"))
                };
                let explanation = match (&antecedent.value, &consequent.value) {
                    (Value::Bool(false), Value::Bool(_)) => antecedent.explanation,
                    (Value::Bool(true), Value::Bool(_)) => consequent
                        .explanation
                        .with_context(antecedent.explanation, CausalRole::Selection),
                    _ => antecedent.explanation.joint(consequent.explanation),
                };
                marker(value, explanation)
            }),
    )
}

fn boolean_binary<D: CausalDomain>(
    operation: BinaryOperator,
    left: OutputStream<CausalValue<D>>,
    right: OutputStream<CausalValue<D>>,
) -> OutputStream<CausalValue<D>> {
    Box::pin(
        lift_base(left)
            .zip(lift_base(right))
            .map(move |(left, right)| {
                let value = if left.value == Value::NoVal || right.value == Value::NoVal {
                    Value::NoVal
                } else if left.value == Value::Deferred || right.value == Value::Deferred {
                    Value::Deferred
                } else {
                    operations::binary(operation, left.value.clone(), right.value.clone())
                        .unwrap_or_else(|error| panic!("{error}"))
                };
                let explanation = match (operation, &left.value, &right.value) {
                    (BinaryOperator::And, Value::Bool(false), Value::Bool(false)) => {
                        left.explanation.alternative(right.explanation)
                    }
                    (BinaryOperator::And, Value::Bool(false), Value::Bool(true)) => {
                        left.explanation
                    }
                    (BinaryOperator::And, Value::Bool(true), Value::Bool(false)) => {
                        right.explanation
                    }
                    (BinaryOperator::Or, Value::Bool(true), Value::Bool(true)) => {
                        left.explanation.alternative(right.explanation)
                    }
                    (BinaryOperator::Or, Value::Bool(true), Value::Bool(false)) => left.explanation,
                    (BinaryOperator::Or, Value::Bool(false), Value::Bool(true)) => {
                        right.explanation
                    }
                    _ => left.explanation.joint(right.explanation),
                };
                marker(value, explanation)
            }),
    )
}

pub fn if_stream<D: CausalDomain>(
    condition: OutputStream<CausalValue<D>>,
    then_stream: OutputStream<CausalValue<D>>,
    else_stream: OutputStream<CausalValue<D>>,
) -> OutputStream<CausalValue<D>> {
    Box::pin(
        lift_base(condition)
            .zip(lift_base(then_stream))
            .zip(lift_base(else_stream))
            .map(|((condition, then_value), else_value)| {
                // Preserve the ordinary semi-sync semantics: a NoVal in the
                // condition or either branch prevents selection, even when
                // that branch is not chosen.
                let mut missing = Vec::new();
                if condition.value == Value::NoVal {
                    missing.push(condition.explanation.clone().used_as(CausalRole::Selection));
                }
                if then_value.value == Value::NoVal {
                    missing.push(then_value.explanation.clone());
                }
                if else_value.value == Value::NoVal {
                    missing.push(else_value.explanation.clone());
                }
                if let Some(explanation) = D::alternative_all(missing) {
                    return marker(Value::NoVal, explanation);
                }
                match condition.value {
                    Value::Bool(true) => marker(
                        then_value.value,
                        then_value
                            .explanation
                            .with_context(condition.explanation, CausalRole::Selection),
                    ),
                    Value::Bool(false) => marker(
                        else_value.value,
                        else_value
                            .explanation
                            .with_context(condition.explanation, CausalRole::Selection),
                    ),
                    Value::Deferred => marker(
                        Value::Deferred,
                        condition.explanation.used_as(CausalRole::Selection),
                    ),
                    value => panic!("invalid conditional value {value:?}"),
                }
            }),
    )
}

pub fn sindex<D: CausalDomain>(
    input: OutputStream<CausalValue<D>>,
    offset: u64,
) -> OutputStream<CausalValue<D>> {
    let offset = usize::try_from(offset).expect("causal sindex offset is too large");
    let prefix = futures_stream::repeat(CausalValue::constant(Value::Deferred)).take(offset);
    lift_base(Box::pin(prefix.chain(input)))
}

pub fn default<D: CausalDomain>(
    primary: OutputStream<CausalValue<D>>,
    fallback: OutputStream<CausalValue<D>>,
) -> OutputStream<CausalValue<D>> {
    Box::pin(lift_base(primary).zip(fallback).map(|(primary, fallback)| {
        if primary.value == Value::Deferred {
            marker(
                fallback.value,
                fallback
                    .explanation
                    .with_context(primary.explanation, CausalRole::Selection),
            )
        } else {
            primary
        }
    }))
}

pub fn init<D: CausalDomain>(
    mut value: OutputStream<CausalValue<D>>,
    mut initial: OutputStream<CausalValue<D>>,
) -> OutputStream<CausalValue<D>> {
    Box::pin(stream! {
        while let (Some(value), Some(initial)) = join!(value.next(), initial.next()) {
            if value.value == Value::NoVal {
                yield marker(
                    initial.value,
                    initial
                        .explanation
                        .used_as(CausalRole::Initialization)
                        .with_context(value.explanation, CausalRole::Selection),
                );
            } else {
                yield value;
                break;
            }
        }
        while let Some(value) = value.next().await {
            yield value;
        }
    })
}

pub fn is_defined<D: CausalDomain>(
    input: OutputStream<CausalValue<D>>,
) -> OutputStream<CausalValue<D>> {
    Box::pin(lift_base(input).map(|input| {
        marker(
            Value::Bool(input.value != Value::Deferred),
            input.explanation,
        )
    }))
}

pub fn when<D: CausalDomain>(
    mut input: OutputStream<CausalValue<D>>,
) -> OutputStream<CausalValue<D>> {
    Box::pin(stream! {
        let mut absence = D::unit();
        let mut receipt: Option<D> = None;
        while let Some(current) = input.next().await {
            if let Some(receipt) = &receipt {
                yield marker(Value::Bool(true), receipt.clone());
                continue;
            }
            if matches!(current.value, Value::NoVal | Value::Deferred) {
                absence = absence.joint(current.explanation);
                yield marker(Value::Bool(false), absence.clone());
            } else {
                receipt = Some(current.explanation.clone());
                yield marker(Value::Bool(true), current.explanation);
            }
        }
    })
}

pub fn latch<D: CausalDomain>(
    value: OutputStream<CausalValue<D>>,
    trigger: OutputStream<CausalValue<D>>,
) -> OutputStream<CausalValue<D>> {
    Box::pin(lift_base(value).zip(trigger).map(|(value, trigger)| {
        if trigger.value == Value::NoVal {
            marker(
                Value::NoVal,
                trigger.explanation.used_as(CausalRole::Selection),
            )
        } else {
            marker(
                value.value,
                value
                    .explanation
                    .with_context(trigger.explanation, CausalRole::Selection),
            )
        }
    }))
}

pub fn update<D: CausalDomain>(
    value: OutputStream<CausalValue<D>>,
    mut update: OutputStream<CausalValue<D>>,
) -> OutputStream<CausalValue<D>> {
    Box::pin(stream! {
        let mut value = lift_base(value);
        let mut absence = D::unit();
        let (selection_evidence, mut last_update) = loop {
            let (next_value, next_update) = join!(value.next(), update.next());
            let (Some(current_value), Some(current_update)) = (next_value, next_update) else {
                return;
            };
            if matches!(current_update.value, Value::NoVal | Value::Deferred) {
                absence = absence.joint(current_update.explanation);
                yield marker(
                    current_value.value,
                    current_value
                        .explanation
                        .with_context(absence.clone(), CausalRole::Retention),
                );
                continue;
            }
            let selection_evidence = current_update.explanation.clone();
            yield marker(
                current_update.value.clone(),
                current_update
                    .explanation
                    .clone()
                    .with_context(selection_evidence.clone(), CausalRole::Selection),
            );
            break (selection_evidence, current_update);
        };
        while let Some(current_update) = update.next().await {
            let current_update = if current_update.value == Value::NoVal {
                last_update.explanation = last_update
                    .explanation
                    .clone()
                    .with_context(current_update.explanation, CausalRole::Retention);
                last_update.clone()
            } else {
                last_update = current_update.clone();
                current_update
            };
            yield marker(
                current_update.value,
                current_update
                    .explanation
                    .with_context(selection_evidence.clone(), CausalRole::Selection),
            );
        }
    })
}
