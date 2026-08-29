use super::super::compiler::lower::*;
use super::super::history::HistoryAccess;
use super::super::history_requirements::HistoryRequirements;
use super::super::ir::*;
use super::super::*;
use super::environment_projection::EnvironmentProjection;
use super::evaluator::{EvaluationEnvironment, Evaluator};
use super::evaluator_state::*;
use super::lifting::retain_last_value;
use crate::lang::dsrv::{parser::parse_expr, type_checker::check_expression};

pub(in crate::dataflow) fn evaluate_dynamic_expression(
    current: Value,
    spec: &BoundDynamicExpressionSpec,
    dynamic: &mut DynamicExpressionState,
    context: EvaluationEnvironment<'_>,
    _history_access: Option<HistoryAccess<'_>>,
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
            if spec.kind == ReconfigurableExpressionKind::Deferred
                && dynamic.active_expression.is_some()
            {
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
            if spec.kind == ReconfigurableExpressionKind::Deferred {
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

pub(in crate::dataflow) const SHARED_DYNAMIC_EXPRESSION_CACHE_CAPACITY: usize = 8;

#[derive(Default)]
pub(in crate::dataflow) struct SharedDynamicExpressionCache {
    entries: Vec<SharedDynamicExpressionCacheEntry>,
}

struct SharedDynamicExpressionCacheEntry {
    environment: Rc<EnvironmentLayout>,
    typing: Option<ReconfigurableExpressionTyping>,
    template: Rc<DynamicExpressionTemplate>,
}

impl SharedDynamicExpressionCacheEntry {
    fn matches(
        &self,
        source_text: &EcoString,
        spec: &BoundDynamicExpressionSpec,
        environment: &Rc<EnvironmentLayout>,
    ) -> bool {
        let allowed_variables = spec.scope.allowed_variables();
        &*self.template.source_text == &*source_text
            && Rc::ptr_eq(&self.environment, environment)
            && self.template.nested_environment_slots.iter().all(|slot| {
                environment
                    .variable(*slot)
                    .is_some_and(|variable| allowed_variables.contains(variable))
            })
            && match (&self.typing, &spec.typing) {
                (None, None) => true,
                (Some(cached), Some(requested)) => {
                    Rc::ptr_eq(&cached.environment, &requested.environment)
                        && cached.expected_type == requested.expected_type
                }
                _ => false,
            }
    }
}

impl SharedDynamicExpressionCache {
    pub(in crate::dataflow) fn lookup(
        &self,
        source_text: &EcoString,
        spec: &BoundDynamicExpressionSpec,
        environment: &Rc<EnvironmentLayout>,
    ) -> Option<Rc<DynamicExpressionTemplate>> {
        self.entries
            .iter()
            .rev()
            .find(|entry| entry.matches(source_text, spec, environment))
            .map(|entry| Rc::clone(&entry.template))
    }

    pub(in crate::dataflow) fn insert(
        &mut self,
        spec: &BoundDynamicExpressionSpec,
        environment: &Rc<EnvironmentLayout>,
        template: Rc<DynamicExpressionTemplate>,
    ) {
        if let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.matches(&template.source_text, spec, environment))
        {
            let entry = self.entries.remove(index);
            self.entries.push(entry);
            return;
        }
        if self.entries.len() == SHARED_DYNAMIC_EXPRESSION_CACHE_CAPACITY {
            self.entries.remove(0);
        }
        self.entries.push(SharedDynamicExpressionCacheEntry {
            environment: Rc::clone(environment),
            typing: spec.typing.clone(),
            template,
        });
    }
}

/// A locally compiled nested body that has not yet been installed. Keeping this boundary explicit
/// lets the replacement path transfer state before mutating the owning dynamic node.
pub(in crate::dataflow) struct PreparedDynamicExpression {
    pub(in crate::dataflow) activation: DynamicExpressionActivation,
    pub(in crate::dataflow) template: Rc<DynamicExpressionTemplate>,
    pub(in crate::dataflow) environment_projection: EnvironmentProjection,
}

pub(in crate::dataflow) fn prepare_active_expression_with_change(
    source_text: EcoString,
    spec: &BoundDynamicExpressionSpec,
    template_cache: &[Rc<DynamicExpressionTemplate>],
    shared_template_cache: Option<&SharedDynamicExpressionCache>,
    environment: &Rc<EnvironmentLayout>,
    had_active_expression: bool,
    previous_dependency_slots: &[EnvironmentSlot],
) -> Result<Option<PreparedDynamicExpression>, DataflowEvaluationError> {
    let should_activate = match spec.kind {
        ReconfigurableExpressionKind::Deferred => !had_active_expression,
        ReconfigurableExpressionKind::Dynamic => true,
    };
    if !should_activate {
        return Ok(None);
    }

    let template = if let Some(template) = template_cache
        .iter()
        .find(|template| {
            template.source_text == source_text
                && Rc::ptr_eq(&template.program.environment_layout, environment)
        })
        .cloned()
    {
        template
    } else if let Some(shared_template_cache) = shared_template_cache {
        if let Some(template) = shared_template_cache.lookup(&source_text, spec, environment) {
            template
        } else {
            compile_dynamic_expression_template(source_text, spec, environment)?
        }
    } else {
        compile_dynamic_expression_template(source_text, spec, environment)?
    };
    let environment_projection = EnvironmentProjection::for_template(&template, environment)
        .map_err(|variable| DataflowEvaluationError::DynamicExpressionContext(vec![variable]))?;
    let dependency_slots_changed =
        previous_dependency_slots != environment_projection.outer_dependency_slots();
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
        template,
        environment_projection,
    }))
}

pub(in crate::dataflow) fn update_active_expression_with_change(
    source_text: EcoString,
    spec: &BoundDynamicExpressionSpec,
    dynamic: &mut DynamicExpressionState,
    environment: &Rc<EnvironmentLayout>,
) -> Result<DynamicExpressionActivation, DataflowEvaluationError> {
    let had_active_expression = dynamic.active_expression.is_some();
    let should_activate = match spec.kind {
        ReconfigurableExpressionKind::Deferred => !had_active_expression,
        ReconfigurableExpressionKind::Dynamic => dynamic
            .active_expression
            .as_ref()
            .is_none_or(|active| &active.source_text != &source_text),
    };
    if !should_activate {
        return Ok(DynamicExpressionActivation::Unchanged);
    }
    let DynamicExpressionState {
        active_expression,
        template_cache,
        ..
    } = dynamic;
    let previous_dependency_slots = active_expression.as_ref().map_or(&[][..], |active| {
        active.environment_projection.outer_dependency_slots()
    });
    let prepared = prepare_active_expression_with_change(
        source_text,
        spec,
        template_cache,
        None,
        environment,
        had_active_expression,
        previous_dependency_slots,
    )?
    .expect("the activation check was true");
    let template = prepared.template;
    let environment_projection = prepared.environment_projection;
    dynamic.cache_template(Rc::clone(&template));
    if spec.kind == ReconfigurableExpressionKind::Deferred && !had_active_expression {
        dynamic.last_defer_result = None;
    }
    dynamic.active_expression = Some(ActiveExpression {
        evaluator: Evaluator::new(Rc::clone(&template.program)),
        template,
        environment_projection,
    });
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
        .evaluate_and_stage_with_history(environment_values, None)
}

struct CompiledDynamicExpression {
    program: Rc<StreamProgram>,
    nested_dependency_slots: Vec<EnvironmentSlot>,
    nested_environment_slots: Vec<EnvironmentSlot>,
    nested_history_requirements: HistoryRequirements,
}

#[cold]
#[inline(never)]
fn compile_dynamic_expression_template(
    source_text: EcoString,
    spec: &BoundDynamicExpressionSpec,
    environment: &Rc<EnvironmentLayout>,
) -> Result<Rc<DynamicExpressionTemplate>, DataflowEvaluationError> {
    let compiled = compile_dynamic_expression(&source_text, spec, environment)?;
    if compiled.program.has_reconfigurable_expressions() {
        return Err(DataflowEvaluationError::UnsupportedNestedReconfiguration);
    }
    Ok(Rc::new(DynamicExpressionTemplate {
        source_text,
        program: compiled.program,
        nested_dependency_slots: compiled.nested_dependency_slots,
        nested_environment_slots: compiled.nested_environment_slots,
        nested_history_requirements: compiled.nested_history_requirements,
    }))
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
    let mut graph = if let Some(ReconfigurableExpressionTyping {
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
    let allowed_vars = spec.scope.allowed_variables();
    graph.restrict_reconfigurable_scopes(allowed_vars);
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
    let nested_environment_slots = free_vars
        .iter()
        .map(|name| {
            environment
                .slot(name)
                .expect("validated dynamic environment variable must have an environment slot")
        })
        .collect();
    let nested_dependency_slots = graph
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
    let nested_history_requirements =
        HistoryRequirements::analyze_graph(&program.graph, program.environment_layout.as_ref());
    Ok(CompiledDynamicExpression {
        program,
        nested_dependency_slots,
        nested_environment_slots,
        nested_history_requirements,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VarName;

    fn fixture(
        kind: ReconfigurableExpressionKind,
        variables: &[&str],
    ) -> (BoundDynamicExpressionSpec, Rc<EnvironmentLayout>) {
        let variables = variables
            .iter()
            .map(|name| VarName::new(*name))
            .collect::<Vec<_>>();
        let environment = Rc::new(EnvironmentLayout::from_variables(variables.iter().cloned()));
        let spec = BoundDynamicExpressionSpec {
            input: BoundRef::Const(Value::NoVal),
            scope: ReconfigurableExpressionScope::Restricted {
                allowed_variables: variables.into_iter().collect(),
            },
            kind,
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

    fn restricted_scope(names: &[&str]) -> ReconfigurableExpressionScope {
        ReconfigurableExpressionScope::Restricted {
            allowed_variables: names.iter().map(|name| VarName::new(*name)).collect(),
        }
    }

    fn dynamic_spec(
        scope: ReconfigurableExpressionScope,
        kind: ReconfigurableExpressionKind,
        typing: Option<ReconfigurableExpressionTyping>,
    ) -> BoundDynamicExpressionSpec {
        BoundDynamicExpressionSpec {
            input: BoundRef::Const(Value::NoVal),
            scope,
            kind,
            typing,
        }
    }

    fn cache_template(
        cache: &mut SharedDynamicExpressionCache,
        source: &str,
        spec: &BoundDynamicExpressionSpec,
        environment: &Rc<EnvironmentLayout>,
    ) -> Rc<DynamicExpressionTemplate> {
        let prepared = prepare_active_expression_with_change(
            source.into(),
            spec,
            &[],
            Some(&*cache),
            environment,
            false,
            &[],
        )
        .unwrap()
        .expect("a fresh owner must activate");
        let template = prepared.template;
        cache.insert(spec, environment, Rc::clone(&template));
        template
    }

    #[test]
    fn activation_reports_lifecycle_and_dependency_changes() {
        let (spec, environment) = fixture(ReconfigurableExpressionKind::Dynamic, &["x", "y"]);
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
        let (spec, environment) = fixture(ReconfigurableExpressionKind::Dynamic, &["x"]);
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
            "reactivation must not recover the previous evaluator's local delay history"
        );
    }

    #[test]
    fn same_source_replaces_a_cached_template_from_an_old_layout() {
        let (spec, old_environment) = fixture(ReconfigurableExpressionKind::Dynamic, &["x", "y"]);
        let new_environment = Rc::new(EnvironmentLayout::from_variables([
            VarName::new("added"),
            VarName::new("x"),
            VarName::new("y"),
        ]));
        let mut state = DynamicExpressionState::default();

        activate("x", &spec, &mut state, &old_environment);
        let old_template = Rc::clone(&state.active_expression.as_ref().unwrap().template);
        activate("y", &spec, &mut state, &new_environment);
        activate("x", &spec, &mut state, &new_environment);

        let cached_x = state
            .template_cache
            .iter()
            .filter(|template| template.source_text == "x")
            .collect::<Vec<_>>();
        assert_eq!(cached_x.len(), 1);
        assert!(!Rc::ptr_eq(cached_x[0], &old_template));
        assert!(Rc::ptr_eq(
            &cached_x[0].program.environment_layout,
            &new_environment
        ));
    }

    #[test]
    fn template_cache_is_four_entry_linear_lru() {
        let (spec, environment) = fixture(ReconfigurableExpressionKind::Dynamic, &[]);
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
            ["3", "4", "2", "5"]
        );

        activate("1", &spec, &mut state, &environment);
        assert!(!Rc::ptr_eq(
            &first_template,
            &state.active_expression.as_ref().unwrap().template
        ));
    }

    #[test]
    fn shared_cache_matches_used_variables_across_resolved_scopes() {
        let environment = Rc::new(EnvironmentLayout::from_variables([
            VarName::new("x"),
            VarName::new("y"),
        ]));
        let mut cache = SharedDynamicExpressionCache::default();
        let narrow_dynamic_spec = dynamic_spec(
            restricted_scope(&["x"]),
            ReconfigurableExpressionKind::Dynamic,
            None,
        );
        let dynamic_template = cache_template(&mut cache, "x", &narrow_dynamic_spec, &environment);

        let broad_deferred_spec = dynamic_spec(
            restricted_scope(&["x", "y"]),
            ReconfigurableExpressionKind::Deferred,
            None,
        );
        let shared_template = cache_template(&mut cache, "x", &broad_deferred_spec, &environment);
        assert!(Rc::ptr_eq(&dynamic_template, &shared_template));

        let excluded_scope_spec = dynamic_spec(
            restricted_scope(&["y"]),
            ReconfigurableExpressionKind::Dynamic,
            None,
        );
        let source = EcoString::from("x");
        assert!(
            cache
                .lookup(&source, &excluded_scope_spec, &environment)
                .is_none()
        );
        assert!(matches!(
            prepare_active_expression_with_change(
                source,
                &excluded_scope_spec,
                &[],
                Some(&cache),
                &environment,
                false,
                &[],
            ),
            Err(DataflowEvaluationError::DynamicExpressionContext(_))
        ));

        let unresolved_scope_spec = dynamic_spec(
            ReconfigurableExpressionScope::Automatic {
                allowed_variables: EcoVec::new(),
            },
            ReconfigurableExpressionKind::Dynamic,
            None,
        );
        let source = EcoString::from("x");
        assert!(
            cache
                .lookup(&source, &unresolved_scope_spec, &environment)
                .is_none()
        );
    }

    #[test]
    fn shared_cache_rejects_environment_and_typing_identity_mismatches() {
        let environment_a = Rc::new(EnvironmentLayout::from_variables([VarName::new("x")]));
        let environment_b = Rc::new(EnvironmentLayout::from_variables([VarName::new("x")]));
        let untyped_spec = dynamic_spec(
            restricted_scope(&["x"]),
            ReconfigurableExpressionKind::Dynamic,
            None,
        );
        let mut cache = SharedDynamicExpressionCache::default();
        let first = cache_template(&mut cache, "x", &untyped_spec, &environment_a);
        let different_environment = cache_template(&mut cache, "x", &untyped_spec, &environment_b);
        assert!(!Rc::ptr_eq(&first, &different_environment));

        let type_environment_a = Rc::new(std::collections::BTreeMap::from([(
            VarName::new("x"),
            crate::core::StreamType::Int,
        )]));
        let type_environment_b = Rc::new(std::collections::BTreeMap::from([(
            VarName::new("x"),
            crate::core::StreamType::Int,
        )]));
        let typed_spec_a = dynamic_spec(
            restricted_scope(&["x"]),
            ReconfigurableExpressionKind::Dynamic,
            Some(ReconfigurableExpressionTyping {
                environment: Rc::clone(&type_environment_a),
                expected_type: crate::lang::dsrv::type_checker::TCType::Int,
            }),
        );
        let typed_spec_b = dynamic_spec(
            restricted_scope(&["x"]),
            ReconfigurableExpressionKind::Dynamic,
            Some(ReconfigurableExpressionTyping {
                environment: Rc::clone(&type_environment_b),
                expected_type: crate::lang::dsrv::type_checker::TCType::Int,
            }),
        );
        let typed_first = cache_template(&mut cache, "x", &typed_spec_a, &environment_a);
        assert!(!Rc::ptr_eq(&first, &typed_first));
        let different_type_environment =
            cache_template(&mut cache, "x", &typed_spec_b, &environment_a);
        assert!(!Rc::ptr_eq(&typed_first, &different_type_environment));

        let different_expected_type_spec = dynamic_spec(
            restricted_scope(&["x"]),
            ReconfigurableExpressionKind::Dynamic,
            Some(ReconfigurableExpressionTyping {
                environment: Rc::clone(&type_environment_a),
                expected_type: crate::lang::dsrv::type_checker::TCType::Any,
            }),
        );
        let different_expected_type = cache_template(
            &mut cache,
            "x",
            &different_expected_type_spec,
            &environment_a,
        );
        assert!(!Rc::ptr_eq(&typed_first, &different_expected_type));
    }

    #[test]
    fn defer_compiles_only_its_first_accepted_definition() {
        let (spec, environment) = fixture(ReconfigurableExpressionKind::Deferred, &[]);
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
    fn nested_expression_reconfiguration_fails_without_caching_a_template() {
        let (spec, environment) = fixture(ReconfigurableExpressionKind::Dynamic, &[]);
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
