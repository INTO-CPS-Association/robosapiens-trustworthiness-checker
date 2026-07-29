use super::super::compiler::lower::*;
use super::super::ir::*;
use super::super::*;
use super::stream_evaluator::{EvaluationContext, StreamEvaluator};
use super::stream_state::*;
use crate::lang::dsrv::{parser::parse_expr, type_checker::check_expression};

pub(in crate::dataflow) fn evaluate_dynamic_expression(
    current: Value,
    spec: &BoundDynamicExpressionSpec,
    dynamic: &mut DynamicExpressionState,
    context: EvaluationContext<'_>,
) -> Result<Value, DataflowEvaluationError> {
    match current {
        // The active expression is always evaluated so its state keeps advancing; in
        // `Defer` mode its output is also the node result, otherwise the
        // special value propagates.
        special @ (Value::Deferred | Value::NoVal) => {
            dynamic.update_environment(
                context.environment_values,
                context.retained_environment_values,
            );
            let result = evaluate_active_expression(dynamic);
            if spec.mode == DynamicExpressionMode::Defer && dynamic.active_expression.is_some() {
                Ok(result?)
            } else {
                Ok(special)
            }
        }
        Value::Str(source) => {
            update_active_expression(source, spec, dynamic, context.environment_layout)?;
            dynamic.update_environment(
                context.environment_values,
                context.retained_environment_values,
            );
            evaluate_active_expression(dynamic)
        }
        other => Err(DataflowEvaluationError::InvalidExpressionSource(
            other.to_string(),
        )),
    }
}

pub(in crate::dataflow) fn update_active_expression(
    source_text: EcoString,
    spec: &BoundDynamicExpressionSpec,
    dynamic: &mut DynamicExpressionState,
    environment: &Rc<EnvironmentLayout>,
) -> Result<(), DataflowEvaluationError> {
    let should_compile = match spec.mode {
        DynamicExpressionMode::Defer => dynamic.active_expression.is_none(),
        DynamicExpressionMode::Dynamic => {
            dynamic
                .active_expression
                .as_ref()
                .map(|active| &active.source_text)
                != Some(&source_text)
        }
    };
    if !should_compile {
        return Ok(());
    }

    let compiled = compile_dynamic_expression(&source_text, spec, environment)?;
    let evaluator = StreamEvaluator::new(compiled.program);
    if evaluator.program.has_reconfiguration_points() {
        return Err(DataflowEvaluationError::UnsupportedNestedReconfiguration);
    }
    dynamic.active_expression = Some(ActiveExpression {
        source_text,
        evaluator,
        dependency_slots: compiled.dependency_slots,
        environment_slots: compiled.environment_slots,
    });
    dynamic.last_result = None;
    Ok(())
}

fn evaluate_active_expression(
    dynamic: &mut DynamicExpressionState,
) -> Result<Value, DataflowEvaluationError> {
    let DynamicExpressionState {
        active_expression,
        environment_values,
        ..
    } = dynamic;
    let Some(active_expression) = active_expression.as_mut() else {
        return Ok(Value::Deferred);
    };
    active_expression
        .evaluator
        .evaluate_and_stage(environment_values)
}

struct CompiledDynamicExpression {
    program: Rc<StreamProgram>,
    dependency_slots: Vec<EnvironmentSlot>,
    environment_slots: Vec<EnvironmentSlot>,
}

#[cold]
#[inline(never)]
fn compile_dynamic_expression(
    source_text: &EcoString,
    spec: &BoundDynamicExpressionSpec,
    environment: &Rc<EnvironmentLayout>,
) -> Result<CompiledDynamicExpression, DataflowEvaluationError> {
    let expr = parse_expr(source_text.as_ref()).map_err(|error| {
        DataflowEvaluationError::DynamicExpressionParse {
            expression: source_text.clone(),
            message: error.to_string(),
        }
    })?;
    let mut graph = if let Some(DynamicExpressionTyping {
        environment,
        expected_type,
    }) = &spec.typing
    {
        let expr = check_expression(expr, expected_type, environment).map_err(|errors| {
            DataflowEvaluationError::DynamicExpressionType {
                expression: source_text.clone(),
                message: format!("{errors:?}"),
            }
        })?;
        build_checked_expression_graph(expr)
    } else {
        build_expression_graph(expr)
    };
    let allowed_vars = spec
        .scope
        .allowed_variables()
        .expect("dynamic scope should be resolved during stream-program binding");
    graph.restrict_dynamic_scopes(allowed_vars);
    let free_vars = graph.free_vars(None);
    let unsupported = free_vars
        .iter()
        .filter(|input| !allowed_vars.contains(input))
        .cloned()
        .collect::<Vec<_>>();
    if !unsupported.is_empty() {
        return Err(DataflowEvaluationError::DynamicExpressionContext(
            unsupported,
        ));
    }
    let environment_slots = free_vars
        .iter()
        .map(|name| {
            environment
                .slot(name)
                .expect("validated dynamic environment variable must have an environment slot")
        })
        .collect();
    let dependency_slots = graph
        .same_tick_free_vars(None)
        .iter()
        .map(|name| {
            environment
                .slot(name)
                .expect("validated dynamic dependency must have an environment slot")
        })
        .collect();
    let program = graph
        .bind_graph(None, Rc::clone(environment))
        .map_err(DataflowEvaluationError::InvalidDynamicProgram)?;
    Ok(CompiledDynamicExpression {
        program,
        dependency_slots,
        environment_slots,
    })
}
