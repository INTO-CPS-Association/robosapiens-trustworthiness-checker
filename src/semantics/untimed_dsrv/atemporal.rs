//! Evaluating an atemporal expression at one tick, without building any streams.
//!
//! A stream evaluator turns an expression into a graph of streams that each
//! carry a value per tick. That is the right shape for a specification's
//! equations, and the wrong one for an expression evaluated inside a tick: a
//! list callback runs once per element, and a `match` arm runs only when its
//! pattern is selected, neither of which a stream can advance for.
//!
//! [`eval_atemporal`] evaluates such an expression directly over values. The
//! names it does not bind itself come from an [`Environment`], which the
//! caller fills with what those names hold at this tick, so an atemporal
//! expression sees exactly what the stream path would have seen.
//!
//! Every DSRV expression is free of side effects, so what separates these
//! from the rest is time, not purity: an atemporal expression reads only
//! this tick. A caller checks that before it gets here; meeting a temporal
//! operator anyway is [`AtemporalError::Temporal`], not a panic, because
//! runtime expression source can carry one.

use std::collections::BTreeMap;

use ecow::EcoVec;

use crate::VarName;
use crate::core::values::operations as value_operations;
use crate::core::{BinaryOperator, PartialMarker, UnaryOperator, Value};
use crate::lang::dsrv::IfPolicy;
use crate::lang::dsrv::ast::{ExprRef, ExprView};
use crate::lang::dsrv::patterns::{
    ArmSelection, Bindings, GuardOutcome, MatchArm, MatchPattern, select_arm,
};
use crate::lang::dsrv::span::Span;

/// What the names of an expression hold at the tick it is evaluated in.
pub(crate) trait Environment {
    fn get(&self, name: &VarName) -> Option<Value>;
}

impl Environment for BTreeMap<VarName, Value> {
    fn get(&self, name: &VarName) -> Option<Value> {
        BTreeMap::get(self, name).cloned()
    }
}

/// An environment that answers from a map, then from an outer environment.
pub(crate) struct Scope<'outer> {
    pub(crate) bound: BTreeMap<VarName, Value>,
    pub(crate) outer: Option<&'outer dyn Environment>,
}

impl Environment for Scope<'_> {
    fn get(&self, name: &VarName) -> Option<Value> {
        self.bound
            .get(name)
            .cloned()
            .or_else(|| self.outer.and_then(|outer| outer.get(name)))
    }
}

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub(crate) enum AtemporalError {
    #[error("`{construct}` at {span:?} reads history, so it cannot be evaluated within a tick")]
    Temporal { construct: &'static str, span: Span },
    #[error("`{name}` at {span:?} is not bound where this expression is evaluated")]
    Unbound { name: VarName, span: Span },
    #[error("the value called at {span:?} is not a function")]
    NotCallable { span: Span },
    #[error("no arm of the `match` at {span:?} matched {value}")]
    Unmatched { value: String, span: Span },
    #[error("{0}")]
    Operation(String),
}

type Evaluated = Result<Value, AtemporalError>;

fn apply_function(
    function: ExprRef<'_>,
    arguments: Vec<Value>,
    environment: &dyn Environment,
) -> Evaluated {
    if let ExprView::Lambda(parameters, body) = function.view() {
        if parameters.len() != arguments.len() {
            return Err(AtemporalError::Operation(format!(
                "function expected {} arguments, got {}",
                parameters.len(),
                arguments.len()
            )));
        }
        let bound = parameters
            .iter()
            .map(|(name, _)| name.clone())
            .zip(arguments)
            .collect();
        return eval_atemporal(
            body,
            &Scope {
                bound,
                outer: Some(environment),
            },
        );
    }
    match eval_atemporal(function, environment)? {
        Value::Function(function) if function.has_value_callable() => function
            .call_value(arguments.into())
            .map_err(|error| AtemporalError::Operation(error.to_string())),
        absent @ (Value::NoVal | Value::Deferred) => Ok(absent),
        Value::Function(_) => Err(AtemporalError::NotCallable {
            span: function.span(),
        }),
        other => Err(AtemporalError::Operation(format!(
            "{other} is not a function"
        ))),
    }
}

/// Evaluate `node` at one tick, reading free names from `environment`.
pub(crate) fn eval_atemporal(node: ExprRef<'_>, environment: &dyn Environment) -> Evaluated {
    use ExprView::*;

    // Absence spreads through an operation rather than reaching it, which is
    // what the stream combinators do with the same operands.
    fn lift1(operand: Value, apply: impl FnOnce(Value) -> Evaluated) -> Evaluated {
        match PartialMarker::of(&operand) {
            Some(marker) => Ok(marker.into_value()),
            None => apply(operand),
        }
    }

    fn lift2(
        left: Value,
        right: Value,
        apply: impl FnOnce(Value, Value) -> Evaluated,
    ) -> Evaluated {
        // NoVal wins over Deferred, as `stream_lift2` has it.
        match (PartialMarker::of(&left), PartialMarker::of(&right)) {
            (Some(PartialMarker::NoVal), _) | (_, Some(PartialMarker::NoVal)) => Ok(Value::NoVal),
            (Some(marker), _) | (_, Some(marker)) => Ok(marker.into_value()),
            (None, None) => apply(left, right),
        }
    }

    fn operation(result: Result<Value, value_operations::ValueOpError>) -> Evaluated {
        result.map_err(|error| AtemporalError::Operation(error.to_string()))
    }

    let child = |node: ExprRef<'_>| eval_atemporal(node, environment);

    match node.view() {
        Val(value) => Ok(value.clone().into_runtime_value()),
        Var(name) => environment
            .get(name)
            .ok_or_else(|| AtemporalError::Unbound {
                name: name.clone(),
                span: node.span(),
            }),
        BinOp(left, right, operator) => {
            let (left, right) = (child(left)?, child(right)?);
            match operator {
                // The boolean operators are not strict in their operands,
                // and neither is the combinator: a known-deciding operand
                // settles the result whatever the other one is.
                BinaryOperator::And | BinaryOperator::Or | BinaryOperator::Implication => {
                    operation(value_operations::binary(operator, left, right))
                }
                operator => lift2(left, right, |left, right| {
                    operation(value_operations::binary(operator, left, right))
                }),
            }
        }
        Cast(value, target) | Ascribe(value, target) => lift1(child(value)?, |value| {
            operation(value_operations::cast(value, target))
        }),
        Not(value) => lift1(child(value)?, |value| {
            operation(value_operations::unary(UnaryOperator::Not, value))
        }),
        Neg(value) => lift1(child(value)?, |value| {
            operation(value_operations::unary(UnaryOperator::Negate, value))
        }),
        Sin(value) => unary(child(value)?, UnaryOperator::Sin),
        Cos(value) => unary(child(value)?, UnaryOperator::Cos),
        Tan(value) => unary(child(value)?, UnaryOperator::Tan),
        Abs(value) => unary(child(value)?, UnaryOperator::Absolute),
        Trunc(value) => unary(child(value)?, UnaryOperator::Truncate),
        Floor(value) => unary(child(value)?, UnaryOperator::Floor),
        Ceil(value) => unary(child(value)?, UnaryOperator::Ceiling),
        Round(value) => unary(child(value)?, UnaryOperator::Round),
        // Under `lazy_if`, only the selected branch is evaluated, so the
        // other can neither fail nor make the result absent.
        If(condition, then_value, else_value) if node.if_policy() == IfPolicy::Lazy => {
            match child(condition)? {
                Value::Bool(true) => child(then_value),
                Value::Bool(false) => child(else_value),
                absent @ (Value::NoVal | Value::Deferred) => Ok(absent),
                other => Err(AtemporalError::Operation(format!(
                    "`if` needs a Bool condition, got {other}"
                ))),
            }
        }
        // Both branches are evaluated, as they are on the stream path, but
        // only the selected one decides the value.
        If(condition, then_value, else_value) => {
            let condition = child(condition)?;
            let (then_value, else_value) = (child(then_value)?, child(else_value)?);
            match condition {
                Value::Bool(true) => Ok(then_value),
                Value::Bool(false) => Ok(else_value),
                Value::NoVal => Ok(Value::NoVal),
                Value::Deferred => Ok(Value::Deferred),
                other => Err(AtemporalError::Operation(format!(
                    "`if` needs a Bool condition, got {other}"
                ))),
            }
        }
        List(items) => collect(items, child).map(|values| Value::List(values)),
        Tuple(items) => collect(items, child).map(|values| Value::Tuple(values)),
        // A container holds what its fields hold, absence included: the
        // combinators build the map rather than propagating out of it, as
        // they do for a list or a tuple.
        Map(entries) | Struct(entries) | ObjectLiteral(entries) => {
            let mut map = BTreeMap::new();
            for (key, value) in entries.iter() {
                map.insert(key.clone(), eval_atemporal(value, environment)?);
            }
            Ok(Value::Map(map))
        }
        Constructor(payload, tag, _) => match payload.into_iter().next() {
            Some(payload) => lift1(child(payload)?, |payload| {
                Ok(crate::core::UnionValue::new(tag.clone(), Some(payload)).into())
            }),
            None => Ok(crate::core::UnionValue::new(tag.clone(), None).into()),
        },
        // The scrutinee decides one arm, and only that arm is evaluated, so
        // an arm is free to read a payload only its own tag carries.
        Match(scrutinee, arms, shape) => {
            let value = child(scrutinee)?;
            let children: Vec<ExprRef<'_>> = arms.into_iter().collect();
            let patterns: Vec<MatchPattern> = shape.iter().map(|arm| arm.pattern.clone()).collect();
            let places = arm_places(shape);
            let mut failure = None;
            let selection = select_arm(&patterns, &value, |arm, bindings| {
                let guard = children[places[arm].0?];
                Some(evaluate_guard(guard, bindings, environment, &mut failure))
            });
            if let Some(error) = failure {
                return Err(error);
            }
            match selection {
                ArmSelection::Selected { arm, bindings } => {
                    eval_atemporal(children[places[arm].1], &scope_of(bindings, environment))
                }
                ArmSelection::Absent(value) => Ok(value),
                ArmSelection::Unmatched => Err(AtemporalError::Unmatched {
                    value: value.to_string(),
                    span: node.span(),
                }),
            }
        }
        // Whether the pattern selects, reported as a Bool. A guard that has
        // no value makes the answer absent rather than false.
        Matches(scrutinee, guard, pattern) => {
            let value = child(scrutinee)?;
            let guard = guard.into_iter().next();
            let mut failure = None;
            let selection = select_arm(std::slice::from_ref(pattern), &value, |_, bindings| {
                Some(evaluate_guard(guard?, bindings, environment, &mut failure))
            });
            if let Some(error) = failure {
                return Err(error);
            }
            Ok(match selection {
                ArmSelection::Selected { .. } => Value::Bool(true),
                ArmSelection::Unmatched => Value::Bool(false),
                ArmSelection::Absent(value) => value,
            })
        }
        LIndex(list, index) => lift2(child(list)?, child(index)?, |list, index| {
            operation(value_operations::list_index(list, index))
        }),
        LAppend(list, value) => lift2(child(list)?, child(value)?, |list, value| {
            operation(value_operations::list_append(list, value))
        }),
        LConcat(left, right) => lift2(child(left)?, child(right)?, |left, right| {
            operation(value_operations::list_concat(left, right))
        }),
        LHead(list) => lift1(child(list)?, |list| {
            operation(value_operations::list_head(list))
        }),
        LTail(list) => lift1(child(list)?, |list| {
            operation(value_operations::list_tail(list))
        }),
        LLen(list) => lift1(child(list)?, |list| {
            operation(value_operations::list_len(list))
        }),
        MGet(map, key) => lift1(child(map)?, |map| {
            operation(value_operations::map_get(map, key))
        }),
        MRemove(map, key) => lift1(child(map)?, |map| {
            operation(value_operations::map_remove(map, key))
        }),
        MHasKey(map, key) => lift1(child(map)?, |map| {
            operation(value_operations::map_has_key(map, key))
        }),
        MInsert(map, key, value) => lift2(child(map)?, child(value)?, |map, value| {
            operation(value_operations::map_insert(map, key, value))
        }),
        SGet(value, key) => lift1(child(value)?, |value| {
            match (&value, key.parse::<usize>()) {
                (Value::Tuple(values) | Value::List(values), Ok(index)) => {
                    values.get(index).cloned().ok_or_else(|| {
                        AtemporalError::Operation(format!("no element {index} in {value}"))
                    })
                }
                _ => operation(value_operations::map_get(value, key)),
            }
        }),
        // Both of these read what a value is rather than what it was, so
        // they are evaluated here. Retention has already happened wherever
        // the operand's value came from, which is why only `Deferred`
        // counts as having no value, exactly as the combinators have it.
        IsDefined(value) => Ok(Value::Bool(child(value)? != Value::Deferred)),
        Default(value, fallback) => {
            let value = child(value)?;
            let fallback = child(fallback)?;
            Ok(if value == Value::Deferred {
                fallback
            } else {
                value
            })
        }
        Apply(function, arguments) => {
            let arguments = arguments
                .into_iter()
                .map(child)
                .collect::<Result<Vec<_>, _>>()?;
            apply_function(function, arguments, environment)
        }
        LMap(function, list) => match child(list)? {
            Value::List(values) => Ok(Value::List(
                values
                    .into_iter()
                    .map(|value| apply_function(function, vec![value], environment))
                    .collect::<Result<_, _>>()?,
            )),
            absent @ (Value::NoVal | Value::Deferred) => Ok(absent),
            other => Err(AtemporalError::Operation(format!(
                "List.map requires a list, got {other}"
            ))),
        },
        LFilter(function, list) => match child(list)? {
            Value::List(values) => {
                let mut filtered = EcoVec::new();
                for value in values {
                    match apply_function(function, vec![value.clone()], environment)? {
                        Value::Bool(true) => filtered.push(value),
                        Value::Bool(false) => {}
                        other => {
                            return Err(AtemporalError::Operation(format!(
                                "List.filter returned non-bool value {other}"
                            )));
                        }
                    }
                }
                Ok(Value::List(filtered))
            }
            absent @ (Value::NoVal | Value::Deferred) => Ok(absent),
            other => Err(AtemporalError::Operation(format!(
                "List.filter requires a list, got {other}"
            ))),
        },
        LFold(function, initial, list) => {
            let mut accumulator = child(initial)?;
            if matches!(accumulator, Value::NoVal | Value::Deferred) {
                return Ok(accumulator);
            }
            match child(list)? {
                Value::List(values) => {
                    for value in values {
                        accumulator =
                            apply_function(function, vec![accumulator, value], environment)?;
                    }
                    Ok(accumulator)
                }
                absent @ (Value::NoVal | Value::Deferred) => Ok(absent),
                other => Err(AtemporalError::Operation(format!(
                    "List.fold requires a list, got {other}"
                ))),
            }
        }
        SIndex(..) => Err(temporal("x[n]", node.span())),
        Init(..) => Err(temporal("init", node.span())),
        Update(..) => Err(temporal("update", node.span())),
        Latch(..) => Err(temporal("latch", node.span())),
        When(..) => Err(temporal("when", node.span())),
        Dynamic(..) => Err(temporal("dynamic", node.span())),
        Defer(..) => Err(temporal("defer", node.span())),
        Lambda(..) | Fix(..) | Partial(..) => {
            Err(AtemporalError::NotCallable { span: node.span() })
        }
        MonitoredAt(..) | Dist(..) => Err(AtemporalError::Operation(
            "the distribution primitives are evaluated by the distributed semantics".to_owned(),
        )),
    }
}

/// Where each arm's guard and body sit among a `match` node's children.
fn arm_places(shape: &[MatchArm]) -> Vec<(Option<usize>, usize)> {
    let mut places = Vec::with_capacity(shape.len());
    let mut next = 0;
    for arm in shape {
        let guard = arm.guarded.then(|| {
            next += 1;
            next - 1
        });
        places.push((guard, next));
        next += 1;
    }
    places
}

/// The names an arm bound, over the environment the `match` was evaluated in.
fn scope_of<'outer>(bindings: Bindings, outer: &'outer dyn Environment) -> Scope<'outer> {
    Scope {
        bound: bindings.iter().cloned().collect(),
        outer: Some(outer),
    }
}

/// Evaluate one arm's guard with what its pattern bound in scope. A failure
/// is recorded rather than returned, because arm selection cannot carry one.
fn evaluate_guard(
    guard: ExprRef<'_>,
    bindings: &Bindings,
    environment: &dyn Environment,
    failure: &mut Option<AtemporalError>,
) -> GuardOutcome {
    match eval_atemporal(guard, &scope_of(bindings.clone(), environment)) {
        Ok(Value::Bool(true)) => GuardOutcome::True,
        Ok(Value::Bool(false)) => GuardOutcome::False,
        Ok(value @ (Value::NoVal | Value::Deferred)) => GuardOutcome::Absent(value),
        Ok(other) => {
            *failure = Some(AtemporalError::Operation(format!(
                "a guard decides whether an arm is selected, so it is a Bool, got {other}"
            )));
            GuardOutcome::False
        }
        Err(error) => {
            *failure = Some(error);
            GuardOutcome::False
        }
    }
}

fn unary(operand: Value, operation: UnaryOperator) -> Evaluated {
    match PartialMarker::of(&operand) {
        Some(marker) => Ok(marker.into_value()),
        None => value_operations::unary(operation, operand)
            .map_err(|error| AtemporalError::Operation(error.to_string())),
    }
}

fn temporal(construct: &'static str, span: Span) -> AtemporalError {
    AtemporalError::Temporal { construct, span }
}

/// Evaluate every item, giving the absent value as soon as one is absent, as
/// the list and tuple combinators do.
fn collect<'a>(
    items: impl IntoIterator<Item = ExprRef<'a>>,
    mut child: impl FnMut(ExprRef<'a>) -> Evaluated,
) -> Result<EcoVec<Value>, AtemporalError> {
    let mut values = EcoVec::new();
    for item in items {
        values.push(child(item)?);
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use futures::stream;
    use futures::{FutureExt, StreamExt};
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;

    use super::*;
    use crate::async_test;
    use crate::dsrv_fixtures::TestConfig;
    use crate::lang::dsrv::parser::{parse_expr_with_context, parse_str};
    use crate::runtime::asynchronous::Context;
    use crate::semantics::async_interface::StreamContext;

    fn environment(bindings: [(&str, Value); 2]) -> BTreeMap<VarName, Value> {
        bindings
            .into_iter()
            .map(|(name, value)| (VarName::from(name), value))
            .collect()
    }

    fn evaluate(source: &str, environment: &dyn Environment) -> Evaluated {
        let context = parse_str("use experimental::casts\n")
            .expect("test language settings should parse")
            .source_context()
            .clone();
        let expr = parse_expr_with_context(source, context)
            .unwrap_or_else(|error| panic!("{source}: {error}"));
        eval_atemporal(expr.as_ref(), environment)
    }

    fn evaluate_under(header: &str, source: &str, environment: &dyn Environment) -> Evaluated {
        let context = parse_str(header)
            .expect("test language settings should parse")
            .source_context()
            .clone();
        let expr = parse_expr_with_context(source, context)
            .unwrap_or_else(|error| panic!("{source}: {error}"));
        eval_atemporal(expr.as_ref(), environment)
    }

    // Eager evaluation reads both branches, so an unselected branch can
    // fail it; under `lazy_if` only the selected one is read, and an absent
    // condition reads neither.
    #[test]
    fn a_lazy_if_evaluates_only_its_selected_branch() {
        let source = "if c then List.get(xs, 5) else 0";
        for (condition, lazy) in [
            (Value::Bool(false), Ok(Value::Int(0))),
            (Value::NoVal, Ok(Value::NoVal)),
            (Value::Deferred, Ok(Value::Deferred)),
        ] {
            let bindings = environment([
                ("c", condition.clone()),
                ("xs", Value::List(EcoVec::from([Value::Int(1)]))),
            ]);
            assert!(
                matches!(
                    evaluate_under("", source, &bindings),
                    Err(AtemporalError::Operation(_))
                ),
                "eager {condition:?}"
            );
            assert_eq!(
                evaluate_under("use experimental::lazy_if\n", source, &bindings),
                lazy,
                "lazy {condition:?}"
            );
        }
        let selected = environment([
            ("c", Value::Bool(true)),
            ("xs", Value::List(EcoVec::from([Value::Int(1)]))),
        ]);
        assert!(matches!(
            evaluate_under("use experimental::lazy_if\n", source, &selected),
            Err(AtemporalError::Operation(_))
        ));
    }

    #[test]
    fn higher_order_list_callbacks_use_the_ticks_environment() {
        let bindings = environment([("bias", Value::Int(10)), ("unused", Value::Int(0))]);
        assert_eq!(
            evaluate(
                "List.map(\\x: Int -> (\\v: Int -> v + bias)(x), List(1, 2))",
                &bindings,
            ),
            Ok(Value::List(vec![Value::Int(11), Value::Int(12)].into()))
        );
    }

    #[test]
    fn scalar_casts_evaluate_atemporally() {
        let bindings = environment([("x", Value::Float(-1.6)), ("y", Value::Int(7))]);
        for (source, expected) in [
            ("trunc(x)", Value::Int(-1)),
            ("floor(x)", Value::Int(-2)),
            ("ceil(x)", Value::Int(-1)),
            ("round(x)", Value::Int(-2)),
            ("y as Float", Value::Float(7.0)),
            ("y as Str", Value::Str("7".into())),
            ("x as Str", Value::Str("-1.6".into())),
        ] {
            assert_eq!(evaluate(source, &bindings), Ok(expected), "{source}");
        }
    }

    #[test]
    fn unrepresentable_rounding_is_an_atemporal_operation_error() {
        for operator in ["trunc", "floor", "ceil", "round"] {
            for input in [
                f64::NAN,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::MAX,
                f64::MIN,
            ] {
                let bindings = environment([("x", Value::Float(input)), ("unused", Value::Int(0))]);
                let result = evaluate(&format!("{operator}(x)"), &bindings);
                assert!(
                    matches!(result, Err(AtemporalError::Operation(_))),
                    "{operator}({input:?}) returned {result:?}"
                );
            }
        }
    }

    /// The same expression down the stream path, with each name a
    /// single-value stream, which is what one tick of it looks like.
    async fn through_streams(
        executor: Rc<LocalExecutor<'static>>,
        source: &str,
        bindings: &BTreeMap<VarName, Value>,
    ) -> Value {
        let names: Vec<VarName> = bindings.keys().cloned().collect();
        let streams = bindings
            .values()
            .map(|value| {
                Box::pin(stream::iter(vec![value.clone()])) as crate::core::LocalStream<Value>
            })
            .collect::<Vec<_>>();
        let mut ctx = Context::<TestConfig>::new(executor, names, streams, 8);
        let context = parse_str("use experimental::casts\n")
            .expect("test language settings should parse")
            .source_context()
            .clone();
        let expr = parse_expr_with_context(source, context)
            .unwrap_or_else(|error| panic!("{source}: {error}"));
        let mut output =
            crate::semantics::untimed_dsrv::semantics::evaluate::<TestConfig>(expr, None, &ctx);
        ctx.run().await;
        output.next().await.unwrap_or(Value::NoVal)
    }

    #[apply(async_test)]
    async fn scalar_casts_execute_through_untimed_streams(executor: Rc<LocalExecutor<'static>>) {
        let bindings = environment([("x", Value::Float(-1.6)), ("y", Value::Int(7))]);
        for (source, expected) in [
            ("trunc(x)", Value::Int(-1)),
            ("floor(x)", Value::Int(-2)),
            ("ceil(x)", Value::Int(-1)),
            ("round(x)", Value::Int(-2)),
            ("y as Float", Value::Float(7.0)),
            ("y as Str", Value::Str("7".into())),
            ("x as Str", Value::Str("-1.6".into())),
        ] {
            assert_eq!(
                through_streams(executor.clone(), source, &bindings).await,
                expected,
                "{source}"
            );
        }
    }

    #[apply(async_test)]
    async fn unrepresentable_rounding_panics_in_untimed_streams(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        for operator in ["trunc", "floor", "ceil", "round"] {
            for input in [
                f64::NAN,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::MAX,
                f64::MIN,
            ] {
                let bindings = environment([("x", Value::Float(input)), ("unused", Value::Int(0))]);
                let source = format!("{operator}(x)");
                let evaluation = std::panic::AssertUnwindSafe(through_streams(
                    executor.clone(),
                    &source,
                    &bindings,
                ))
                .catch_unwind()
                .await;
                assert!(
                    evaluation.is_err(),
                    "{operator}({input:?}) produced an untimed value instead of panicking"
                );
            }
        }
    }

    const EXPRESSIONS: &[&str] = &[
        "x + 1",
        "x * y",
        "x - y",
        "-x",
        "x == y",
        "x != y",
        "x < y",
        "x >= y",
        "abs(x)",
        "if x > y then x else y",
        "[x, y]",
        "Tuple(x, y)",
        "List.len([x, y])",
        "List.get([x, y], 1)",
        "List.head([x, y])",
        "List.tail([x, y])",
        "List.append([x], y)",
        "List.concat([x], [y])",
        "Map(\"a\": x, \"b\": y)",
        "Map.get(Map(\"a\": x), \"a\")",
        "Map.has_key(Map(\"a\": x), \"b\")",
        "Map.insert(Map(\"a\": x), \"b\", y)",
        "Map.remove(Map(\"a\": x, \"b\": y), \"a\")",
        "{ \"a\": x }.a",
        "is_defined(x)",
        "default(x, y)",
    ];

    // R10.1: an expression evaluated within a tick gives what the stream
    // path gives for that tick, for present operands and absent ones alike.
    #[apply(async_test)]
    async fn atemporal_evaluation_agrees_with_the_stream_path(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let cases = [
            environment([("x", Value::Int(7)), ("y", Value::Int(2))]),
            environment([("x", Value::Int(2)), ("y", Value::Int(7))]),
            environment([("x", Value::Deferred), ("y", Value::Int(2))]),
            environment([("x", Value::Int(2)), ("y", Value::Deferred)]),
            environment([("x", Value::NoVal), ("y", Value::Int(2))]),
            environment([("x", Value::Int(2)), ("y", Value::NoVal)]),
            environment([("x", Value::NoVal), ("y", Value::Deferred)]),
        ];
        for bindings in cases {
            for source in EXPRESSIONS {
                let expected = through_streams(executor.clone(), source, &bindings).await;
                let actual = evaluate(source, &bindings)
                    .unwrap_or_else(|error| panic!("{source} with {bindings:?}: {error}"));
                assert_eq!(actual, expected, "{source} with {bindings:?}");
            }
        }
    }

    // R10.2: a name the caller did not supply is reported, rather than read
    // from somewhere the caller did not intend.
    #[test]
    fn an_unbound_name_is_reported() {
        let bindings = environment([("x", Value::Int(1)), ("y", Value::Int(2))]);
        assert!(matches!(
            evaluate("x + missing", &bindings),
            Err(AtemporalError::Unbound { .. })
        ));
    }

    // R10.3: an operator that reads history has no meaning within one tick.
    #[test]
    fn a_temporal_operator_is_refused() {
        let bindings = environment([("x", Value::Int(1)), ("y", Value::Int(2))]);
        for source in ["x[1]", "update(x, y)", "latch(x, y)", "when(x)"] {
            assert!(
                matches!(
                    evaluate(source, &bindings),
                    Err(AtemporalError::Temporal { .. })
                ),
                "{source}"
            );
        }
    }

    // R10.4: names resolve through the scope they were given, innermost
    // first, which is what a binder inside an expression needs.
    #[test]
    fn a_scope_answers_before_the_environment_around_it() {
        let outer = environment([("x", Value::Int(1)), ("y", Value::Int(2))]);
        let scope = Scope {
            bound: [(VarName::from("x"), Value::Int(10))].into_iter().collect(),
            outer: Some(&outer),
        };
        assert_eq!(evaluate("x + y", &scope), Ok(Value::Int(12)));
    }
}
