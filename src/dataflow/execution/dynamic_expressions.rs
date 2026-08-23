use super::super::compiler::lower::*;
use super::super::ir::*;
use super::super::*;
use super::lifting::retain_last_value;
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
        // The active expression is always evaluated so its temporal and lifting state keeps
        // advancing. `Defer` retains the body's last non-`NoVal` published result using the same
        // outer lifting rule as its source; `Dynamic` propagates the effective special value.
        special @ (Value::Deferred | Value::NoVal) => {
            dynamic.update_environment(
                context.environment_values,
                context.retained_environment_values,
            );
            let result = evaluate_active_expression(dynamic)?;
            if spec.mode == DynamicExpressionMode::Defer && dynamic.active_expression.is_some() {
                Ok(retain_last_value(result, &mut dynamic.last_defer_result))
            } else {
                Ok(special)
            }
        }
        Value::Str(source) => {
            update_active_expression_with_change(
                source,
                spec,
                dynamic,
                context.environment_layout,
            )?;
            dynamic.update_environment(
                context.environment_values,
                context.retained_environment_values,
            );
            let result = evaluate_active_expression(dynamic)?;
            if spec.mode == DynamicExpressionMode::Defer {
                Ok(retain_last_value(result, &mut dynamic.last_defer_result))
            } else {
                Ok(result)
            }
        }
        other => Err(DataflowEvaluationError::InvalidExpressionSource(
            other.to_string(),
        )),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::dataflow) enum DynamicExpressionActivation {
    Unchanged,
    Activated { dependency_slots_changed: bool },
    Replaced { dependency_slots_changed: bool },
}

impl DynamicExpressionActivation {
    pub(in crate::dataflow) fn activated(self) -> bool {
        matches!(self, Self::Activated { .. })
    }

    pub(in crate::dataflow) fn dependency_slots_changed(self) -> bool {
        match self {
            Self::Unchanged => false,
            Self::Activated {
                dependency_slots_changed,
            }
            | Self::Replaced {
                dependency_slots_changed,
            } => dependency_slots_changed,
        }
    }
}

/// A locally compiled nested body that has not yet been installed.  Keeping this boundary explicit
/// lets the replacement path transfer state before mutating the owning dynamic node.
pub(in crate::dataflow) struct PreparedDynamicExpression {
    pub(in crate::dataflow) activation: DynamicExpressionActivation,
    pub(in crate::dataflow) active_expression: ActiveExpression,
}

pub(in crate::dataflow) fn prepare_active_expression_with_change(
    source_text: EcoString,
    spec: &BoundDynamicExpressionSpec,
    dynamic: &mut DynamicExpressionState,
    environment: &Rc<EnvironmentLayout>,
    had_active_expression: bool,
    previous_dependency_slots: &[EnvironmentSlot],
) -> Result<Option<PreparedDynamicExpression>, DataflowEvaluationError> {
    let should_activate = match spec.mode {
        DynamicExpressionMode::Defer => !had_active_expression,
        DynamicExpressionMode::Dynamic => true,
    };
    if !should_activate {
        return Ok(None);
    }

    let template = if let Some(template) = dynamic.cached_template(&source_text) {
        template
    } else {
        let compiled = compile_dynamic_expression(&source_text, spec, environment)?;
        if compiled.program.has_reconfiguration_points() {
            return Err(DataflowEvaluationError::UnsupportedNestedReconfiguration);
        }
        let template = Rc::new(DynamicExpressionTemplate {
            source_text,
            program: compiled.program,
            dependency_slots: compiled.dependency_slots,
            environment_slots: compiled.environment_slots,
        });
        dynamic.cache_template(Rc::clone(&template));
        template
    };
    let dependency_slots_changed =
        previous_dependency_slots != template.dependency_slots.as_slice();
    let activation = if had_active_expression {
        DynamicExpressionActivation::Replaced {
            dependency_slots_changed,
        }
    } else {
        DynamicExpressionActivation::Activated {
            dependency_slots_changed,
        }
    };
    Ok(Some(PreparedDynamicExpression {
        activation,
        active_expression: ActiveExpression {
            evaluator: StreamEvaluator::new(Rc::clone(&template.program)),
            template,
        },
    }))
}

pub(in crate::dataflow) fn update_active_expression_with_change(
    source_text: EcoString,
    spec: &BoundDynamicExpressionSpec,
    dynamic: &mut DynamicExpressionState,
    environment: &Rc<EnvironmentLayout>,
) -> Result<DynamicExpressionActivation, DataflowEvaluationError> {
    let had_active_expression = dynamic.active_expression.is_some();
    let should_activate = match spec.mode {
        DynamicExpressionMode::Defer => !had_active_expression,
        DynamicExpressionMode::Dynamic => dynamic
            .active_expression
            .as_ref()
            .is_none_or(|active| &active.source_text != &source_text),
    };
    if !should_activate {
        return Ok(DynamicExpressionActivation::Unchanged);
    }
    let previous_dependency_slots = dynamic
        .active_expression
        .as_ref()
        .map(|active| active.dependency_slots.clone())
        .unwrap_or_default();
    let prepared = prepare_active_expression_with_change(
        source_text,
        spec,
        dynamic,
        environment,
        had_active_expression,
        &previous_dependency_slots,
    )?
    .expect("the activation check was true");
    if spec.mode == DynamicExpressionMode::Defer && !had_active_expression {
        dynamic.last_defer_result = None;
    }
    dynamic.active_expression = Some(prepared.active_expression);
    Ok(prepared.activation)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VarName;

    fn fixture(
        mode: DynamicExpressionMode,
        variables: &[&str],
    ) -> (BoundDynamicExpressionSpec, Rc<EnvironmentLayout>) {
        let variables = variables
            .iter()
            .map(|name| VarName::new(*name))
            .collect::<Vec<_>>();
        let environment = Rc::new(EnvironmentLayout::from_variables(variables.iter().cloned()));
        let spec = BoundDynamicExpressionSpec {
            input: BoundRef::Const(Value::NoVal),
            scope: DynamicExpressionScope::Restricted {
                allowed_variables: variables.into_iter().collect(),
            },
            mode,
            typing: None,
        };
        (spec, environment)
    }

    fn activate(
        source: &str,
        spec: &BoundDynamicExpressionSpec,
        state: &mut DynamicExpressionState,
        environment: &Rc<EnvironmentLayout>,
    ) -> DynamicExpressionActivation {
        update_active_expression_with_change(source.into(), spec, state, environment).unwrap()
    }

    #[test]
    fn activation_reports_lifecycle_and_dependency_changes() {
        let (spec, environment) = fixture(DynamicExpressionMode::Dynamic, &["x", "y"]);
        let mut state = DynamicExpressionState::default();

        assert_eq!(
            activate("x", &spec, &mut state, &environment),
            DynamicExpressionActivation::Activated {
                dependency_slots_changed: true,
            }
        );
        let template = Rc::clone(&state.active_expression.as_ref().unwrap().template);

        assert_eq!(
            activate("x", &spec, &mut state, &environment),
            DynamicExpressionActivation::Unchanged
        );
        assert!(Rc::ptr_eq(
            &template,
            &state.active_expression.as_ref().unwrap().template
        ));

        assert_eq!(
            activate("x + 1", &spec, &mut state, &environment),
            DynamicExpressionActivation::Replaced {
                dependency_slots_changed: false,
            }
        );

        assert_eq!(
            activate("y", &spec, &mut state, &environment),
            DynamicExpressionActivation::Replaced {
                dependency_slots_changed: true,
            }
        );
    }

    #[test]
    fn cached_template_reactivation_uses_fresh_temporal_state() {
        let (spec, environment) = fixture(DynamicExpressionMode::Dynamic, &["x"]);
        let mut state = DynamicExpressionState::default();

        activate("x[1]", &spec, &mut state, &environment);
        let first_template = Rc::clone(&state.active_expression.as_ref().unwrap().template);
        state.update_environment(&[Value::Int(10)], None);
        assert_eq!(
            evaluate_active_expression(&mut state).unwrap(),
            Value::Deferred
        );
        let environment_values = state.environment_values.clone();
        state
            .active_expression
            .as_mut()
            .unwrap()
            .evaluator
            .commit_temporal_state(&environment_values);

        activate("x", &spec, &mut state, &environment);
        activate("x[1]", &spec, &mut state, &environment);
        assert!(Rc::ptr_eq(
            &first_template,
            &state.active_expression.as_ref().unwrap().template
        ));

        state.update_environment(&[Value::Int(20)], None);
        assert_eq!(
            evaluate_active_expression(&mut state).unwrap(),
            Value::Deferred,
            "reactivation must not recover the previous evaluator's delay history"
        );
    }

    #[test]
    fn template_cache_is_four_entry_linear_lru() {
        let (spec, environment) = fixture(DynamicExpressionMode::Dynamic, &[]);
        let mut state = DynamicExpressionState::default();

        activate("1", &spec, &mut state, &environment);
        let first_template = Rc::clone(&state.active_expression.as_ref().unwrap().template);
        for source in ["2", "3", "4"] {
            activate(source, &spec, &mut state, &environment);
        }
        activate("2", &spec, &mut state, &environment);
        activate("5", &spec, &mut state, &environment);

        assert_eq!(
            state.template_cache.len(),
            DYNAMIC_EXPRESSION_CACHE_CAPACITY
        );
        assert_eq!(
            state
                .template_cache
                .iter()
                .map(|template| &*template.source_text)
                .collect::<Vec<&str>>(),
            ["5", "2", "4", "3"]
        );

        activate("1", &spec, &mut state, &environment);
        assert!(!Rc::ptr_eq(
            &first_template,
            &state.active_expression.as_ref().unwrap().template
        ));
    }

    #[test]
    fn defer_compiles_only_its_first_accepted_definition() {
        let (spec, environment) = fixture(DynamicExpressionMode::Defer, &[]);
        let mut state = DynamicExpressionState::default();

        assert!(
            update_active_expression_with_change("(".into(), &spec, &mut state, &environment)
                .is_err()
        );
        assert!(state.active_expression.is_none());
        assert!(state.template_cache.is_empty());

        assert!(matches!(
            activate("1", &spec, &mut state, &environment),
            DynamicExpressionActivation::Activated { .. }
        ));
        assert_eq!(
            activate("(", &spec, &mut state, &environment),
            DynamicExpressionActivation::Unchanged
        );
        assert_eq!(&*state.active_expression.as_ref().unwrap().source_text, "1");
    }

    #[test]
    fn nested_reconfiguration_fails_without_caching_a_template() {
        let (spec, environment) = fixture(DynamicExpressionMode::Dynamic, &[]);
        let mut state = DynamicExpressionState::default();

        let result = update_active_expression_with_change(
            "dynamic(\"1\")".into(),
            &spec,
            &mut state,
            &environment,
        );
        assert!(matches!(
            result,
            Err(DataflowEvaluationError::UnsupportedNestedReconfiguration)
        ));
        assert!(state.active_expression.is_none());
        assert!(state.template_cache.is_empty());
    }
}
