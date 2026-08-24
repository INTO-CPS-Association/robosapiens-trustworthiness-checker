//! Direct causal interpretations of the scalar DSRV stream semantics.
//!
//! This evaluator is deliberately semi-synchronous: every external observation
//! is labelled with its logical input tick before it enters the runtime.  It
//! supports the scalar fragment plus the stateful stream operators used by
//! runtime monitors.  Collection, higher-order, and distributed expressions
//! are rejected with an explicit panic until their causal rules are defined.

mod builder;
mod combinators;
mod dynamic;
mod input;

use std::marker::PhantomData;

use crate::causal::{CausalDomain, CausalSet, CausalValue, RoleCausalDomain};
use crate::core::{BinaryOperator, UnaryOperator};
use crate::lang::core::DependencyGraphExpr;
use crate::lang::dsrv::ast::{
    CheckedDsrvSpecification, CheckedExpr, CheckedExprRef, Expr, ExprRef, ExprView,
};
use crate::runtime::semi_sync::SemiSyncContext;
use crate::semantics::{AsyncConfig, MonitoringSemantics, StreamContext};
use crate::{DsrvSpecification, OutputStream, VarName};

pub use builder::{CausalRuntimeBuilder, CheckedCausalRuntimeBuilder};
pub use input::annotate_input;
pub use input::annotate_input_for_spec;

/// Semi-synchronous unchecked DSRV configuration with causal values.
#[derive(Debug, Default, PartialEq)]
pub struct CausalSemiSyncConfig<D>(PhantomData<D>);

impl<D> Clone for CausalSemiSyncConfig<D> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<D: CausalDomain> AsyncConfig for CausalSemiSyncConfig<D> {
    type Val = CausalValue<D>;
    type Expr = Expr;
    type Ctx = SemiSyncContext<Self>;
    type Spec = DsrvSpecification;
}

/// Semi-synchronous checked DSRV configuration with causal values.
#[derive(Debug, Default, PartialEq)]
pub struct CausalCheckedSemiSyncConfig<D>(PhantomData<D>);

impl<D> Clone for CausalCheckedSemiSyncConfig<D> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<D: CausalDomain> AsyncConfig for CausalCheckedSemiSyncConfig<D> {
    type Val = CausalValue<D>;
    type Expr = CheckedExpr;
    type Ctx = SemiSyncContext<Self>;
    type Spec = CheckedDsrvSpecification;
}

/// Default reference causal semantics over [`CausalSet`].
#[derive(Clone)]
pub struct CausalDsrvSemantics;

impl MonitoringSemantics<CausalSemiSyncConfig<CausalSet>> for CausalDsrvSemantics {
    fn to_async_stream(
        expr: &Expr,
        ctx: &SemiSyncContext<CausalSemiSyncConfig<CausalSet>>,
        owner: Option<VarName>,
    ) -> OutputStream<CausalValue<CausalSet>> {
        evaluate::<CausalSemiSyncConfig<CausalSet>, CausalSet>(expr.clone(), ctx, owner)
    }
}

impl MonitoringSemantics<CausalCheckedSemiSyncConfig<CausalSet>> for CausalDsrvSemantics {
    fn to_async_stream(
        expr: &CheckedExpr,
        ctx: &SemiSyncContext<CausalCheckedSemiSyncConfig<CausalSet>>,
        owner: Option<VarName>,
    ) -> OutputStream<CausalValue<CausalSet>> {
        evaluate_checked::<CausalCheckedSemiSyncConfig<CausalSet>, CausalSet>(expr, ctx, owner)
    }
}

/// Role-aware causal semantics parameterised by its compact or antichain
/// explanation domain.
#[derive(Clone)]
pub struct RoleCausalDsrvSemantics<D>(PhantomData<D>);

impl<D: RoleCausalDomain> MonitoringSemantics<CausalSemiSyncConfig<D>>
    for RoleCausalDsrvSemantics<D>
{
    fn to_async_stream(
        expr: &Expr,
        ctx: &SemiSyncContext<CausalSemiSyncConfig<D>>,
        owner: Option<VarName>,
    ) -> OutputStream<CausalValue<D>> {
        evaluate::<CausalSemiSyncConfig<D>, D>(expr.clone(), ctx, owner)
    }
}

impl<D: RoleCausalDomain> MonitoringSemantics<CausalCheckedSemiSyncConfig<D>>
    for RoleCausalDsrvSemantics<D>
{
    fn to_async_stream(
        expr: &CheckedExpr,
        ctx: &SemiSyncContext<CausalCheckedSemiSyncConfig<D>>,
        owner: Option<VarName>,
    ) -> OutputStream<CausalValue<D>> {
        evaluate_checked::<CausalCheckedSemiSyncConfig<D>, D>(expr, ctx, owner)
    }
}

fn evaluate<AC, D>(
    expr: Expr,
    ctx: &AC::Ctx,
    owner: Option<VarName>,
) -> OutputStream<CausalValue<D>>
where
    D: CausalDomain,
    AC: AsyncConfig<Val = CausalValue<D>>,
    AC::Expr: DependencyGraphExpr,
    AC::Ctx: StreamContext<AC = AC>,
{
    evaluate_ref::<AC, D>(expr.as_ref(), ctx, owner)
}

fn evaluate_ref<AC, D>(
    node: ExprRef<'_>,
    ctx: &AC::Ctx,
    owner: Option<VarName>,
) -> OutputStream<CausalValue<D>>
where
    D: CausalDomain,
    AC: AsyncConfig<Val = CausalValue<D>>,
    AC::Expr: DependencyGraphExpr,
    AC::Ctx: StreamContext<AC = AC>,
{
    use ExprView::*;

    let child = |node: ExprRef<'_>| evaluate_ref::<AC, D>(node, ctx, owner.clone());

    match node.view() {
        Val(value) => combinators::constant(value.clone().into_runtime_value()),
        Var(name) => ctx
            .var(name)
            .unwrap_or_else(|| panic!("causal DSRV variable `{name}` was not declared")),
        Not(value) => combinators::unary(UnaryOperator::Not, child(value)),
        Neg(value) => combinators::unary(UnaryOperator::Negate, child(value)),
        Sin(value) => combinators::unary(UnaryOperator::Sin, child(value)),
        Cos(value) => combinators::unary(UnaryOperator::Cos, child(value)),
        Tan(value) => combinators::unary(UnaryOperator::Tan, child(value)),
        Abs(value) => combinators::unary(UnaryOperator::Absolute, child(value)),
        BinOp(left, right, operator) => {
            let left = child(left);
            let right = child(right);
            match operator {
                BinaryOperator::And => combinators::and(left, right),
                BinaryOperator::Or => combinators::or(left, right),
                BinaryOperator::Implication => combinators::implication::<D>(left, right),
                operator => combinators::binary(operator, left, right),
            }
        }
        If(condition, then_expr, else_expr) => {
            combinators::if_stream::<D>(child(condition), child(then_expr), child(else_expr))
        }
        SIndex(value, offset) => combinators::sindex(child(value), offset),
        Default(value, default) => combinators::default::<D>(child(value), child(default)),
        Init(value, initial) => combinators::init::<D>(child(value), child(initial)),
        Update(value, update) => combinators::update::<D>(child(value), child(update)),
        IsDefined(value) => combinators::is_defined(child(value)),
        When(value) => combinators::when(child(value)),
        Latch(value, trigger) => combinators::latch::<D>(child(value), child(trigger)),
        Dynamic(source, _, scope) => dynamic::dynamic::<AC, D>(
            ctx,
            child(source),
            scope.clone(),
            owner,
            |expr, subctx, owner| evaluate::<AC, D>(expr, subctx, owner),
        ),
        Defer(source, _, scope) => dynamic::defer::<AC, D>(
            ctx,
            child(source),
            scope.clone(),
            owner,
            |expr, subctx, owner| evaluate::<AC, D>(expr, subctx, owner),
        ),
        unsupported => panic!(
            "causal DSRV currently supports scalar expressions only; unsupported expression at {:?}: {unsupported:?}",
            node.span()
        ),
    }
}

fn evaluate_checked<AC, D>(
    expr: &CheckedExpr,
    ctx: &AC::Ctx,
    owner: Option<VarName>,
) -> OutputStream<CausalValue<D>>
where
    D: CausalDomain,
    AC: AsyncConfig<Val = CausalValue<D>, Expr = CheckedExpr>,
    AC::Ctx: StreamContext<AC = AC>,
{
    evaluate_checked_owned::<AC, D>(expr.clone(), ctx, owner)
}

fn evaluate_checked_owned<AC, D>(
    expr: CheckedExpr,
    ctx: &AC::Ctx,
    owner: Option<VarName>,
) -> OutputStream<CausalValue<D>>
where
    D: CausalDomain,
    AC: AsyncConfig<Val = CausalValue<D>, Expr = CheckedExpr>,
    AC::Ctx: StreamContext<AC = AC>,
{
    evaluate_checked_ref::<AC, D>(expr.as_ref(), ctx, owner)
}

fn evaluate_checked_ref<AC, D>(
    node: CheckedExprRef<'_>,
    ctx: &AC::Ctx,
    owner: Option<VarName>,
) -> OutputStream<CausalValue<D>>
where
    D: CausalDomain,
    AC: AsyncConfig<Val = CausalValue<D>, Expr = CheckedExpr>,
    AC::Ctx: StreamContext<AC = AC>,
{
    use ExprView::*;

    let child = |node: CheckedExprRef<'_>| evaluate_checked_ref::<AC, D>(node, ctx, owner.clone());

    match node.view() {
        Val(value) => combinators::constant(value.clone().into_runtime_value()),
        Var(name) => ctx
            .var(name)
            .unwrap_or_else(|| panic!("causal DSRV variable `{name}` was not declared")),
        Not(value) => combinators::unary(UnaryOperator::Not, child(value)),
        Neg(value) => combinators::unary(UnaryOperator::Negate, child(value)),
        Sin(value) => combinators::unary(UnaryOperator::Sin, child(value)),
        Cos(value) => combinators::unary(UnaryOperator::Cos, child(value)),
        Tan(value) => combinators::unary(UnaryOperator::Tan, child(value)),
        Abs(value) => combinators::unary(UnaryOperator::Absolute, child(value)),
        BinOp(left, right, operator) => {
            let left = child(left);
            let right = child(right);
            match operator {
                BinaryOperator::And => combinators::and(left, right),
                BinaryOperator::Or => combinators::or(left, right),
                BinaryOperator::Implication => combinators::implication::<D>(left, right),
                operator => combinators::binary(operator, left, right),
            }
        }
        If(condition, then_expr, else_expr) => {
            combinators::if_stream::<D>(child(condition), child(then_expr), child(else_expr))
        }
        SIndex(value, offset) => combinators::sindex(child(value), offset),
        Default(value, default) => combinators::default::<D>(child(value), child(default)),
        Init(value, initial) => combinators::init::<D>(child(value), child(initial)),
        Update(value, update) => combinators::update::<D>(child(value), child(update)),
        IsDefined(value) => combinators::is_defined(child(value)),
        When(value) => combinators::when(child(value)),
        Latch(value, trigger) => combinators::latch::<D>(child(value), child(trigger)),
        Dynamic(source, _, scope) => dynamic::dynamic_checked::<AC, D>(
            ctx,
            child(source),
            scope.clone(),
            owner,
            node.typ().clone(),
            node.shared_type_environment().clone(),
            evaluate_checked_owned::<AC, D>,
        ),
        Defer(source, _, scope) => dynamic::defer_checked::<AC, D>(
            ctx,
            child(source),
            scope.clone(),
            owner,
            node.typ().clone(),
            node.shared_type_environment().clone(),
            evaluate_checked_owned::<AC, D>,
        ),
        _unsupported => panic!(
            "causal DSRV currently supports scalar expressions only; unsupported expression at {:?}",
            node.expr().span()
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        rc::Rc,
    };

    use futures::StreamExt;
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;

    use crate::causal::{
        CausalDomain, CausalRole, CausalSet, CausalValue, RoleCausalAntichain, RoleCausalSet,
    };
    use crate::core::Runtime;
    use crate::io::{map, testing::manual_output};
    use crate::lang::dsrv::parser::parse_str;
    use crate::runtime::{RuntimeBuilder, semi_sync::SemiSyncRuntimeBuilder};
    use crate::semantics::MonitoringSemantics;
    use crate::{Value, VarName, async_test};

    use super::{
        CausalDsrvSemantics, CausalSemiSyncConfig, RoleCausalDsrvSemantics, annotate_input,
    };

    fn canonical_support(support: &crate::causal::AtomSet) -> Vec<(String, u64)> {
        let mut atoms = support
            .iter()
            .map(|atom| (atom.input.name(), atom.logical_tick))
            .collect::<Vec<_>>();
        atoms.sort();
        atoms
    }

    fn canonical_causes(
        causes: &[crate::causal::RoleCause],
    ) -> Vec<(String, u64, Vec<CausalRole>)> {
        let mut causes = causes
            .iter()
            .map(|cause| {
                (
                    cause.atom.input.name(),
                    cause.atom.logical_tick,
                    cause.roles.iter().collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        causes.sort();
        causes
    }

    #[test]
    fn default_causal_builders_select_reference_domain() {
        let _unchecked = crate::semantics::CausalRuntimeBuilder::new();
        let _checked = crate::semantics::CheckedCausalRuntimeBuilder::new();
    }

    async fn unchecked_output<D, MS>(
        source: &str,
        input: BTreeMap<VarName, Vec<Value>>,
        executor: Rc<LocalExecutor<'static>>,
    ) -> CausalValue<D>
    where
        D: CausalDomain,
        MS: MonitoringSemantics<CausalSemiSyncConfig<D>>,
    {
        let spec = parse_str(source).expect("causal fixture should parse");
        let input = annotate_input::<D>(map::input_stream(input), spec.input_vars().clone());
        let (output_writer, mut rows) = manual_output(spec.output_vars().clone()).await;
        let runtime = SemiSyncRuntimeBuilder::<CausalSemiSyncConfig<D>, MS>::new()
            .executor(executor.clone())
            .model(spec)
            .input(input)
            .output_writer(output_writer)
            .build()
            .await;
        let task = executor.spawn(runtime.run());
        let result = rows
            .next()
            .await
            .expect("causal fixture should produce one output")
            .remove(&VarName::new("result"))
            .expect("causal fixture output should be present");
        assert!(rows.next().await.is_none());
        task.await.unwrap();
        result
    }

    #[apply(async_test)]
    async fn identity_preserves_non_finite_float_values(executor: Rc<LocalExecutor<'static>>) {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let result = unchecked_output::<CausalSet, CausalDsrvSemantics>(
                "in measurement: Float\nout result: Float\nresult = measurement",
                BTreeMap::from([("measurement".into(), vec![Value::Float(value)])]),
                executor.clone(),
            )
            .await;

            let Value::Float(actual) = result.value else {
                panic!("expected a float, got {:?}", result.value);
            };
            if value.is_nan() {
                assert!(actual.is_nan());
            } else {
                assert_eq!(actual, value);
            }
        }
    }

    #[apply(async_test)]
    async fn reference_and_role_semantics_classify_conditional_support(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let source = "in guard\nin x\nin y\nout result\nresult = if guard then x else y";
        let input = BTreeMap::from([
            ("guard".into(), vec![Value::Bool(true)]),
            ("x".into(), vec![Value::Int(11)]),
            ("y".into(), vec![Value::Int(22)]),
        ]);

        let reference = unchecked_output::<CausalSet, CausalDsrvSemantics>(
            source,
            input.clone(),
            executor.clone(),
        )
        .await;
        assert_eq!(
            canonical_support(reference.explanation.support()),
            vec![("guard".to_owned(), 0), ("x".to_owned(), 0)]
        );

        let role = unchecked_output::<RoleCausalSet, RoleCausalDsrvSemantics<RoleCausalSet>>(
            source, input, executor,
        )
        .await;
        assert_eq!(
            canonical_causes(role.explanation.causes()),
            vec![
                ("guard".to_owned(), 0, vec![CausalRole::Selection]),
                ("x".to_owned(), 0, vec![CausalRole::Direct]),
            ]
        );
    }

    #[apply(async_test)]
    async fn false_implication_uses_direct_antecedent_support(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let source = "in antecedent\nin consequent\nout result\nresult = antecedent => consequent";
        let result = unchecked_output::<RoleCausalSet, RoleCausalDsrvSemantics<RoleCausalSet>>(
            source,
            BTreeMap::from([
                ("antecedent".into(), vec![Value::Bool(false)]),
                ("consequent".into(), vec![Value::Bool(true)]),
            ]),
            executor,
        )
        .await;

        assert_eq!(result.value, Value::Bool(true));
        assert_eq!(result.explanation.causes().len(), 1);
        assert_eq!(
            result.explanation.causes()[0]
                .roles
                .iter()
                .collect::<Vec<_>>(),
            [CausalRole::Direct]
        );
    }

    #[apply(async_test)]
    async fn reference_collapses_boolean_alternatives_and_role_antichain_retains_them(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let source = "in a\nin b\nout result\nresult = a || b";
        let input = BTreeMap::from([
            ("a".into(), vec![Value::Bool(true)]),
            ("b".into(), vec![Value::Bool(true)]),
        ]);

        let reference = unchecked_output::<CausalSet, CausalDsrvSemantics>(
            source,
            input.clone(),
            executor.clone(),
        )
        .await;
        assert_eq!(reference.explanation.support().iter().count(), 2);

        let antichain = unchecked_output::<
            RoleCausalAntichain,
            RoleCausalDsrvSemantics<RoleCausalAntichain>,
        >(source, input, executor)
        .await;
        assert_eq!(antichain.explanation.alternatives().len(), 2);
        assert!(
            antichain
                .explanation
                .alternatives()
                .iter()
                .all(|alternative| {
                    alternative.causes().len() == 1
                        && alternative.causes()[0].roles.iter().collect::<Vec<_>>()
                            == [CausalRole::Direct]
                })
        );
    }

    #[apply(async_test)]
    async fn dynamic_selection_is_support_in_reference_and_activation_in_roles(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let source = "in x\nin property\nout result\nresult = dynamic(property)";
        let input = BTreeMap::from([
            ("x".into(), vec![Value::Int(4)]),
            ("property".into(), vec![Value::Str("x + 1".into())]),
        ]);

        let reference = unchecked_output::<CausalSet, CausalDsrvSemantics>(
            source,
            input.clone(),
            executor.clone(),
        )
        .await;
        assert_eq!(reference.value, Value::Int(5));
        assert_eq!(reference.explanation.support().iter().count(), 2);

        let role = unchecked_output::<RoleCausalSet, RoleCausalDsrvSemantics<RoleCausalSet>>(
            source, input, executor,
        )
        .await;
        assert_eq!(role.value, Value::Int(5));
        assert_eq!(
            canonical_causes(role.explanation.causes()),
            vec![
                ("property".to_owned(), 0, vec![CausalRole::Activation]),
                ("x".to_owned(), 0, vec![CausalRole::Direct]),
            ]
        );
    }

    #[apply(async_test)]
    async fn checked_role_builder_keeps_dynamic_type_validation_and_roles(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let checked =
            "in property: Str\nin x: Int\nout result: Int\nresult = dynamic(property : Int)"
                .parse::<crate::CheckedDsrvSpecification>()
                .expect("checked causal fixture should type-check");
        let (output_writer, mut rows) = manual_output(checked.output_vars().clone()).await;
        let runtime = crate::semantics::CheckedCausalRuntimeBuilder::<RoleCausalSet>::role_new()
            .executor(executor.clone())
            .model(checked)
            .input(map::input_stream(BTreeMap::from([
                ("property".into(), vec![Value::Str("x + 1".into())]),
                ("x".into(), vec![Value::Int(4)]),
            ])))
            .output_writer(output_writer)
            .build()
            .await
            .unwrap();
        let task = executor.spawn(runtime.run());
        let result = rows
            .next()
            .await
            .unwrap()
            .remove(&VarName::new("result"))
            .unwrap();
        assert_eq!(result.value, Value::Int(5));
        assert_eq!(
            canonical_causes(result.explanation.causes()),
            vec![
                ("property".to_owned(), 0, vec![CausalRole::Activation]),
                ("x".to_owned(), 0, vec![CausalRole::Direct]),
            ]
        );
        assert!(rows.next().await.is_none());
        task.await.unwrap();
    }

    #[test]
    fn annotation_preserves_sparse_ticks_and_rejects_unknown_inputs() {
        smol::block_on(async {
            let input = Box::pin(futures::stream::iter([Ok(crate::InputBatch::tick(vec![
                crate::InputUpdate::new("x".into(), Value::Int(1)),
            ])
            .unwrap())]));
            let mut annotated = annotate_input::<CausalSet>(
                input,
                BTreeSet::from([VarName::new("x"), VarName::new("y")]),
            );
            let tick = annotated.next().await.unwrap().unwrap();
            let updates = tick.ticks().next().unwrap().to_updates();
            assert_eq!(updates.len(), 2);
            assert_eq!(updates[0].value.explanation.support().iter().count(), 1);
            assert_eq!(updates[1].variable, VarName::new("y"));
            assert_eq!(updates[1].value.value, Value::NoVal);

            let input = Box::pin(futures::stream::iter([Ok(crate::InputBatch::update(
                "unknown",
                Value::Int(1),
            ))]));
            let mut annotated =
                annotate_input::<CausalSet>(input, BTreeSet::from([VarName::new("x")]));
            assert!(
                annotated
                    .next()
                    .await
                    .unwrap()
                    .unwrap_err()
                    .to_string()
                    .contains("undeclared variable")
            );
        });
    }

    #[test]
    fn checked_reference_type_checker_rejects_wrong_dynamic_result_type() {
        use crate::lang::dsrv::parser::parse_expr;
        use crate::lang::dsrv::type_checker::check_expression;

        let spec = "in property: Str\nout result: Int\nresult = dynamic(property : Int)"
            .parse::<crate::CheckedDsrvSpecification>()
            .expect("fixture should type-check");
        let dynamic = spec
            .var_expr_ref(&VarName::new("result"))
            .expect("result expression should exist");
        let expression = parse_expr("true").expect("runtime expression should parse");
        let errors = check_expression(expression, dynamic.typ(), dynamic.shared_type_environment())
            .expect_err("a Bool runtime expression must not satisfy an Int annotation");
        assert!(!errors.is_empty());
    }
}
