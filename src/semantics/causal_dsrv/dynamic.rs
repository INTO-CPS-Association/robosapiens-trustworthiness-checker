use async_stream::stream;
use futures::StreamExt;

use crate::causal::{CausalDomain, CausalRole, CausalValue};
use crate::lang::core::DependencyGraphExpr;
use crate::lang::dsrv::ast::{CheckedExpr, DynamicExprScope, Expr};
use crate::lang::dsrv::type_checker::{StreamTypeEnvironment, TCType, check_expression};
use crate::semantics::{AsyncConfig, StreamContext};
use crate::{OutputStream, Value, VarName};

type Evaluator<AC, D> =
    fn(Expr, &<AC as AsyncConfig>::Ctx, Option<VarName>) -> OutputStream<CausalValue<D>>;
type CheckedEvaluator<AC, D> =
    fn(CheckedExpr, &<AC as AsyncConfig>::Ctx, Option<VarName>) -> OutputStream<CausalValue<D>>;

#[derive(Clone)]
enum RuntimeEvaluator<AC: AsyncConfig, D: CausalDomain> {
    Unchecked(Evaluator<AC, D>),
    Checked {
        environment: std::rc::Rc<StreamTypeEnvironment>,
        expected: TCType,
        evaluator: CheckedEvaluator<AC, D>,
    },
}

fn subcontext<AC, D>(ctx: &AC::Ctx, scope: DynamicExprScope, owner: Option<&VarName>) -> AC::Ctx
where
    D: CausalDomain,
    AC: AsyncConfig<Val = CausalValue<D>>,
    AC::Expr: DependencyGraphExpr,
    AC::Ctx: StreamContext<AC = AC>,
{
    match scope {
        DynamicExprScope::Explicit(vars) => ctx.restricted_subcontext(vars, 1),
        DynamicExprScope::Automatic => match owner {
            Some(owner) => ctx.subcontext_excluding(owner, 1),
            None => ctx.subcontext(1),
        },
    }
}

/// Repeat the last non-`NoVal` runtime property like ordinary semi-sync
/// evaluation while retaining the absence observations crossed by that value.
fn lift_property_stream<D: CausalDomain>(
    mut source: OutputStream<CausalValue<D>>,
) -> OutputStream<CausalValue<D>> {
    Box::pin(stream! {
        let mut last = None;
        let mut leading_absence = D::unit();
        while let Some(current) = source.next().await {
            if current.value != Value::NoVal {
                last = Some(current.clone());
                yield current;
            } else if let Some(previous) = &mut last {
                previous.explanation = previous
                    .explanation
                    .clone()
                    .with_context(current.explanation, CausalRole::Retention);
                yield previous.clone();
            } else {
                leading_absence = leading_absence.joint(current.explanation);
                yield CausalValue::new(Value::NoVal, leading_absence.clone());
            }
        }
    })
}

pub fn dynamic<AC, D>(
    ctx: &AC::Ctx,
    source: OutputStream<CausalValue<D>>,
    scope: DynamicExprScope,
    owner: Option<VarName>,
    evaluator: Evaluator<AC, D>,
) -> OutputStream<CausalValue<D>>
where
    D: CausalDomain,
    AC: AsyncConfig<Val = CausalValue<D>>,
    AC::Expr: DependencyGraphExpr,
    AC::Ctx: StreamContext<AC = AC>,
{
    dynamic_inner::<AC, D>(
        ctx,
        source,
        scope,
        owner,
        RuntimeEvaluator::<AC, D>::Unchecked(evaluator),
    )
}

pub fn dynamic_checked<AC, D>(
    ctx: &AC::Ctx,
    source: OutputStream<CausalValue<D>>,
    scope: DynamicExprScope,
    owner: Option<VarName>,
    expected: TCType,
    environment: std::rc::Rc<StreamTypeEnvironment>,
    evaluator: CheckedEvaluator<AC, D>,
) -> OutputStream<CausalValue<D>>
where
    D: CausalDomain,
    AC: AsyncConfig<Val = CausalValue<D>>,
    AC::Expr: DependencyGraphExpr,
    AC::Ctx: StreamContext<AC = AC>,
{
    dynamic_inner::<AC, D>(
        ctx,
        source,
        scope,
        owner,
        RuntimeEvaluator::<AC, D>::Checked {
            environment,
            expected,
            evaluator,
        },
    )
}

fn dynamic_inner<AC, D>(
    ctx: &AC::Ctx,
    source: OutputStream<CausalValue<D>>,
    scope: DynamicExprScope,
    owner: Option<VarName>,
    evaluator: RuntimeEvaluator<AC, D>,
) -> OutputStream<CausalValue<D>>
where
    D: CausalDomain,
    AC: AsyncConfig<Val = CausalValue<D>>,
    AC::Expr: DependencyGraphExpr,
    AC::Ctx: StreamContext<AC = AC>,
{
    let mut ctx = subcontext::<AC, D>(ctx, scope, owner.as_ref());
    let mut source = lift_property_stream(source);
    Box::pin(stream! {
        let mut installed: Option<(Value, OutputStream<CausalValue<D>>)> = None;

        while let Some(current) = source.next().await {
            match current.value.clone() {
                Value::NoVal => {
                    ctx.tick().await;
                    yield current;
                }
                Value::Deferred => {
                    // Match ordinary dynamic: temporal state advances even
                    // while the controlling property is deferred.
                    ctx.tick().await;
                    if let Some((_, output)) = &mut installed
                        && output.next().await.is_none()
                    {
                        return;
                    }
                    yield current;
                }
                Value::Str(property) => {
                    let selection = current.explanation;
                    let same_property = installed
                        .as_ref()
                        .is_some_and(|(value, _)| value == &Value::Str(property.clone()));
                    if same_property {
                        ctx.tick().await;
                        let output = &mut installed.as_mut().expect("installed").1;
                        let Some(result) = output.next().await else { return; };
                        yield CausalValue::new(
                            result.value,
                            result
                                .explanation
                                .with_context(selection.clone(), CausalRole::Activation),
                        );
                        continue;
                    }
                    let mut output = evaluate_property::<AC, D>(
                        property.as_ref(),
                        &ctx,
                        owner.clone(),
                        evaluator.clone(),
                    );
                    ctx.tick().await;
                    let Some(result) = output.next().await else { return; };
                    yield CausalValue::new(
                        result.value,
                        result
                            .explanation
                            .with_context(selection.clone(), CausalRole::Activation),
                    );
                    installed = Some((Value::Str(property), output));
                }
                value => panic!("causal dynamic expected a string property, got {value:?}"),
            }
        }
    })
}

fn evaluate_property<AC, D>(
    property: &str,
    ctx: &AC::Ctx,
    owner: Option<VarName>,
    evaluator: RuntimeEvaluator<AC, D>,
) -> OutputStream<CausalValue<D>>
where
    D: CausalDomain,
    AC: AsyncConfig<Val = CausalValue<D>>,
    AC::Expr: DependencyGraphExpr,
    AC::Ctx: StreamContext<AC = AC>,
{
    let expr = crate::lang::dsrv::parser::parse_expr(property)
        .expect("invalid scalar dynamic DSRV expression");
    match evaluator {
        RuntimeEvaluator::Unchecked(evaluator) => evaluator(expr, ctx, owner),
        RuntimeEvaluator::Checked {
            environment,
            expected,
            evaluator,
        } => {
            let checked =
                check_expression(expr, &expected, &environment).unwrap_or_else(|errors| {
                    panic!("Dynamic expression failed type checking: {errors:?}")
                });
            evaluator(checked, ctx, owner)
        }
    }
}

pub fn defer<AC, D>(
    ctx: &AC::Ctx,
    source: OutputStream<CausalValue<D>>,
    scope: DynamicExprScope,
    owner: Option<VarName>,
    evaluator: Evaluator<AC, D>,
) -> OutputStream<CausalValue<D>>
where
    D: CausalDomain,
    AC: AsyncConfig<Val = CausalValue<D>>,
    AC::Expr: DependencyGraphExpr,
    AC::Ctx: StreamContext<AC = AC>,
{
    defer_inner::<AC, D>(
        ctx,
        source,
        scope,
        owner,
        RuntimeEvaluator::<AC, D>::Unchecked(evaluator),
    )
}

pub fn defer_checked<AC, D>(
    ctx: &AC::Ctx,
    source: OutputStream<CausalValue<D>>,
    scope: DynamicExprScope,
    owner: Option<VarName>,
    expected: TCType,
    environment: std::rc::Rc<StreamTypeEnvironment>,
    evaluator: CheckedEvaluator<AC, D>,
) -> OutputStream<CausalValue<D>>
where
    D: CausalDomain,
    AC: AsyncConfig<Val = CausalValue<D>>,
    AC::Expr: DependencyGraphExpr,
    AC::Ctx: StreamContext<AC = AC>,
{
    defer_inner::<AC, D>(
        ctx,
        source,
        scope,
        owner,
        RuntimeEvaluator::<AC, D>::Checked {
            environment,
            expected,
            evaluator,
        },
    )
}

fn defer_inner<AC, D>(
    ctx: &AC::Ctx,
    source: OutputStream<CausalValue<D>>,
    scope: DynamicExprScope,
    owner: Option<VarName>,
    evaluator: RuntimeEvaluator<AC, D>,
) -> OutputStream<CausalValue<D>>
where
    D: CausalDomain,
    AC: AsyncConfig<Val = CausalValue<D>>,
    AC::Expr: DependencyGraphExpr,
    AC::Ctx: StreamContext<AC = AC>,
{
    let mut ctx = subcontext::<AC, D>(ctx, scope, owner.as_ref());
    let mut source = lift_property_stream(source);
    Box::pin(stream! {
        let mut absence = D::unit();
        let (mut output, selection) = loop {
            let Some(current) = source.next().await else { return; };
            match current.value {
                Value::NoVal => {
                    absence = absence.joint(current.explanation.clone());
                    ctx.tick().await;
                    yield current;
                }
                Value::Deferred => {
                    absence = absence.joint(current.explanation.clone());
                    ctx.tick().await;
                    yield current;
                }
                Value::Str(property) => {
                    let selection = absence
                        .joint(current.explanation);
                    let mut output = evaluate_property::<AC, D>(
                        property.as_ref(),
                        &ctx,
                        owner.clone(),
                        evaluator.clone(),
                    );
                    ctx.tick().await;
                    let Some(result) = output.next().await else { return; };
                    yield CausalValue::new(
                        result.value,
                        result
                            .explanation
                            .with_context(selection.clone(), CausalRole::Activation),
                    );
                    break (output, selection);
                }
                value => panic!("causal defer expected a string property, got {value:?}"),
            }
        };

        while source.next().await.is_some() {
            ctx.tick().await;
            let Some(result) = output.next().await else { return; };
            yield CausalValue::new(
                result.value,
                result
                    .explanation
                    .with_context(selection.clone(), CausalRole::Activation),
            );
        }
    })
}

#[cfg(test)]
mod tests {
    use futures::{StreamExt, stream};

    use crate::causal::{
        CausalDomain, CausalRole, CausalSet, CausalValue, RoleCausalSet, TimedAtom,
    };
    use crate::{Value, VarName};

    use super::lift_property_stream;

    fn property(value: Value, tick: u64) -> CausalValue<CausalSet> {
        CausalValue::new(
            value,
            CausalSet::atom(TimedAtom::new(VarName::new("property"), tick)),
        )
    }

    fn role_property(value: Value, tick: u64) -> CausalValue<RoleCausalSet> {
        CausalValue::new(
            value,
            RoleCausalSet::atom(TimedAtom::new(VarName::new("property"), tick)),
        )
    }

    #[test]
    fn property_stream_lift_repeats_values_and_retains_absence_evidence() {
        smol::block_on(async {
            let lifted = lift_property_stream(Box::pin(stream::iter([
                property(Value::Str("x".into()), 0),
                property(Value::NoVal, 1),
                property(Value::Deferred, 2),
                property(Value::NoVal, 3),
            ])))
            .collect::<Vec<_>>()
            .await;

            assert_eq!(
                lifted
                    .iter()
                    .map(|value| value.value.clone())
                    .collect::<Vec<_>>(),
                [
                    Value::Str("x".into()),
                    Value::Str("x".into()),
                    Value::Deferred,
                    Value::Deferred,
                ]
            );
            assert_eq!(
                lifted[1]
                    .explanation
                    .support()
                    .iter()
                    .map(|atom| atom.logical_tick)
                    .collect::<Vec<_>>(),
                [0, 1]
            );
            assert_eq!(
                lifted[3]
                    .explanation
                    .support()
                    .iter()
                    .map(|atom| atom.logical_tick)
                    .collect::<Vec<_>>(),
                [2, 3]
            );
        });
    }

    #[test]
    fn property_stream_marks_absence_as_retention_for_role_domains() {
        smol::block_on(async {
            let lifted = lift_property_stream(Box::pin(stream::iter([
                role_property(Value::Str("x".into()), 0),
                role_property(Value::NoVal, 1),
            ])))
            .collect::<Vec<_>>()
            .await;

            assert_eq!(
                lifted[1]
                    .explanation
                    .causes()
                    .iter()
                    .map(|cause| (
                        cause.atom.logical_tick,
                        cause.roles.iter().collect::<Vec<_>>()
                    ))
                    .collect::<Vec<_>>(),
                vec![
                    (0, vec![CausalRole::Direct]),
                    (1, vec![CausalRole::Retention]),
                ]
            );
        });
    }
}
