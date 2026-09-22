use super::super::compiler::lower::*;
use super::super::history::HistoryAccess;
use super::super::history_requirements::HistoryRequirements;
use super::super::ir::*;
use super::super::*;
use super::environment_projection::EnvironmentProjection;
use super::evaluator::{EvaluationEnvironment, Evaluator};
use super::evaluator_state::*;
use super::lifting::retain_last_value;
use crate::lang::dsrv::ast::AstShared;
use crate::lang::dsrv::runtime_expression::{RuntimeExpressionError, RuntimeExpressionTyping};

pub(in crate::dataflow) fn evaluate_reconfigurable_expression(
    current: Value,
    spec: &BoundReconfigurableExpressionSpec,
    expression: &mut ReconfigurableExpressionState,
    context: EvaluationEnvironment<'_>,
    _history_access: Option<HistoryAccess<'_>>,
) -> Result<Value, DataflowEvaluationError> {
    match current {
        // The active expression is always evaluated so its temporal and lifting state keeps
        // advancing. `Defer` retains the body's last non-`NoVal` published result using the same
        // outer lifting rule as its source; `Dynamic` propagates the effective special value.
        special @ (Value::Deferred | Value::NoVal) => {
            expression.update_environment(
                context.environment_values,
                context.retained_environment_values,
            );
            let result = evaluate_active_expression(expression)?;
            if spec.kind == ReconfigurableExpressionKind::Deferred
                && expression.active_expression.is_some()
            {
                Ok(retain_last_value(result, &mut expression.last_defer_result))
            } else {
                Ok(special)
            }
        }
        Value::Str(source) => {
            update_active_expression_with_change(
                source,
                spec,
                expression,
                context.environment_layout,
            )?;
            expression.update_environment(
                context.environment_values,
                context.retained_environment_values,
            );
            let result = evaluate_active_expression(expression)?;
            if spec.kind == ReconfigurableExpressionKind::Deferred {
                Ok(retain_last_value(result, &mut expression.last_defer_result))
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
pub(in crate::dataflow) enum ReconfigurableExpressionActivation {
    Unchanged,
    Activated { dependency_slots_changed: bool },
    Replaced { dependency_slots_changed: bool },
}

impl ReconfigurableExpressionActivation {
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

pub(in crate::dataflow) const SHARED_RECONFIGURABLE_EXPRESSION_CACHE_CAPACITY: usize = 8;

#[derive(Default)]
pub(in crate::dataflow) struct SharedReconfigurableExpressionCache {
    entries: Vec<SharedReconfigurableExpressionCacheEntry>,
}

struct SharedReconfigurableExpressionCacheEntry {
    environment: Rc<EnvironmentLayout>,
    typing: Option<RuntimeExpressionTyping>,
    template: Rc<ReconfigurableExpressionTemplate>,
}

impl SharedReconfigurableExpressionCacheEntry {
    fn matches(
        &self,
        source_text: &EcoString,
        spec: &BoundReconfigurableExpressionSpec,
        environment: &Rc<EnvironmentLayout>,
    ) -> bool {
        let allowed_variables = spec.scope.allowed_variables();
        &*self.template.source_text == &*source_text
            && self.template.site.same_lexical_environment(&spec.site)
            && Rc::ptr_eq(&self.environment, environment)
            && self.template.nested_environment_slots.iter().all(|slot| {
                environment
                    .variable(*slot)
                    .is_some_and(|variable| allowed_variables.contains(variable))
            })
            && match (&self.typing, spec.site.typing()) {
                (None, None) => true,
                (Some(cached), Some(requested)) => {
                    AstShared::ptr_eq(&cached.environment, &requested.environment)
                        && cached.expected == requested.expected
                }
                _ => false,
            }
    }
}

impl SharedReconfigurableExpressionCache {
    #[cfg(test)]
    pub(in crate::dataflow) fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub(in crate::dataflow) fn lookup(
        &self,
        source_text: &EcoString,
        spec: &BoundReconfigurableExpressionSpec,
        environment: &Rc<EnvironmentLayout>,
    ) -> Option<Rc<ReconfigurableExpressionTemplate>> {
        self.entries
            .iter()
            .rev()
            .find(|entry| entry.matches(source_text, spec, environment))
            .map(|entry| Rc::clone(&entry.template))
    }

    pub(in crate::dataflow) fn insert(
        &mut self,
        spec: &BoundReconfigurableExpressionSpec,
        environment: &Rc<EnvironmentLayout>,
        template: Rc<ReconfigurableExpressionTemplate>,
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
        if self.entries.len() == SHARED_RECONFIGURABLE_EXPRESSION_CACHE_CAPACITY {
            self.entries.remove(0);
        }
        self.entries.push(SharedReconfigurableExpressionCacheEntry {
            environment: Rc::clone(environment),
            typing: spec.site.typing().cloned(),
            template,
        });
    }
}

/// A locally compiled nested body that has not yet been installed. Keeping this boundary explicit
/// lets the replacement path transfer state before mutating the owning reconfigurable node.
pub(in crate::dataflow) struct PreparedReconfigurableExpression {
    pub(in crate::dataflow) activation: ReconfigurableExpressionActivation,
    pub(in crate::dataflow) template: Rc<ReconfigurableExpressionTemplate>,
    pub(in crate::dataflow) environment_projection: EnvironmentProjection,
}

pub(in crate::dataflow) fn prepare_active_expression_with_change(
    source_text: EcoString,
    spec: &BoundReconfigurableExpressionSpec,
    template_cache: &[Rc<ReconfigurableExpressionTemplate>],
    shared_template_cache: Option<&SharedReconfigurableExpressionCache>,
    environment: &Rc<EnvironmentLayout>,
    had_active_expression: bool,
    previous_dependency_slots: &[EnvironmentSlot],
) -> Result<Option<PreparedReconfigurableExpression>, DataflowEvaluationError> {
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
                && template.site.same_lexical_environment(&spec.site)
                && Rc::ptr_eq(&template.program.environment_layout, environment)
        })
        .cloned()
    {
        template
    } else if let Some(shared_template_cache) = shared_template_cache {
        if let Some(template) = shared_template_cache.lookup(&source_text, spec, environment) {
            template
        } else {
            compile_reconfigurable_expression_template(source_text, spec, environment)?
        }
    } else {
        compile_reconfigurable_expression_template(source_text, spec, environment)?
    };
    let environment_projection = EnvironmentProjection::for_template(&template, environment)
        .map_err(|variable| {
            DataflowEvaluationError::ReconfigurableExpressionContext(vec![variable])
        })?;
    let dependency_slots_changed =
        previous_dependency_slots != environment_projection.outer_dependency_slots();
    let activation = if had_active_expression {
        ReconfigurableExpressionActivation::Replaced {
            dependency_slots_changed,
        }
    } else {
        ReconfigurableExpressionActivation::Activated {
            dependency_slots_changed,
        }
    };
    Ok(Some(PreparedReconfigurableExpression {
        activation,
        template,
        environment_projection,
    }))
}

pub(in crate::dataflow) fn update_active_expression_with_change(
    source_text: EcoString,
    spec: &BoundReconfigurableExpressionSpec,
    expression: &mut ReconfigurableExpressionState,
    environment: &Rc<EnvironmentLayout>,
) -> Result<ReconfigurableExpressionActivation, DataflowEvaluationError> {
    let had_active_expression = expression.active_expression.is_some();
    let should_activate = match spec.kind {
        ReconfigurableExpressionKind::Deferred => !had_active_expression,
        ReconfigurableExpressionKind::Dynamic => expression
            .active_expression
            .as_ref()
            .is_none_or(|active| &active.source_text != &source_text),
    };
    if !should_activate {
        return Ok(ReconfigurableExpressionActivation::Unchanged);
    }
    let ReconfigurableExpressionState {
        active_expression,
        template_cache,
        ..
    } = expression;
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
    expression.cache_template(Rc::clone(&template));
    if spec.kind == ReconfigurableExpressionKind::Deferred && !had_active_expression {
        expression.last_defer_result = None;
    }
    expression.active_expression = Some(ActiveExpression {
        evaluator: Evaluator::new(Rc::clone(&template.program)),
        template,
        environment_projection,
    });
    Ok(prepared.activation)
}

fn evaluate_active_expression(
    expression: &mut ReconfigurableExpressionState,
) -> Result<Value, DataflowEvaluationError> {
    let ReconfigurableExpressionState {
        active_expression,
        environment_values,
        ..
    } = expression;
    let Some(active_expression) = active_expression.as_mut() else {
        return Ok(Value::Deferred);
    };
    active_expression
        .evaluator
        .evaluate_and_stage_with_history(environment_values, None)
}

struct CompiledReconfigurableExpression {
    program: Rc<StreamProgram>,
    nested_dependency_slots: Vec<EnvironmentSlot>,
    nested_environment_slots: Vec<EnvironmentSlot>,
    nested_history_requirements: HistoryRequirements,
}

#[cold]
#[inline(never)]
fn compile_reconfigurable_expression_template(
    source_text: EcoString,
    spec: &BoundReconfigurableExpressionSpec,
    environment: &Rc<EnvironmentLayout>,
) -> Result<Rc<ReconfigurableExpressionTemplate>, DataflowEvaluationError> {
    let compiled = compile_dynamic_expression(&source_text, spec, environment)?;
    if compiled.program.has_reconfigurable_expressions() {
        return Err(DataflowEvaluationError::UnsupportedNestedReconfiguration);
    }
    Ok(Rc::new(ReconfigurableExpressionTemplate {
        source_text,
        // A template is reused by other occurrences, so it keeps what the
        // site means but not where it was written.
        site: spec.site.without_sources(),
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
    spec: &BoundReconfigurableExpressionSpec,
    environment: &Rc<EnvironmentLayout>,
) -> Result<CompiledReconfigurableExpression, DataflowEvaluationError> {
    // Source is checked on arrival whether or not this graph consults types:
    // against the type and environment elaboration gave the occurrence, or,
    // where it has none, against `Any` (see RuntimeExpressionSite). Checking
    // prepares the sites of any nested occurrences before the graph is built.
    let site = &spec.site;
    let checked = site
        .parse_and_check(source_text.as_ref())
        .map_err(|error| match error {
            RuntimeExpressionError::Parse { .. } => {
                DataflowEvaluationError::ReconfigurableExpressionParse {
                    expression: source_text.clone(),
                    message: error.to_string(),
                }
            }
            RuntimeExpressionError::TypeCheck { .. } => {
                DataflowEvaluationError::ReconfigurableExpressionType {
                    expression: source_text.clone(),
                    message: error.to_string(),
                }
            }
            RuntimeExpressionError::Unsupported { .. } => {
                unreachable!("dataflow parses runtime source before runtime admission")
            }
        })?;
    let mut graph = if spec.specialise {
        build_checked_expression_graph(checked)
    } else {
        build_unspecialised_expression_graph(checked)
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
        return Err(DataflowEvaluationError::ReconfigurableExpressionContext(
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
    Ok(CompiledReconfigurableExpression {
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
    use crate::lang::dsrv::runtime_expression::RuntimeExpressionSite;

    fn fixture(
        kind: ReconfigurableExpressionKind,
        variables: &[&str],
    ) -> (BoundReconfigurableExpressionSpec, Rc<EnvironmentLayout>) {
        let variables = variables
            .iter()
            .map(|name| VarName::new(*name))
            .collect::<Vec<_>>();
        let environment = Rc::new(EnvironmentLayout::from_variables(variables.iter().cloned()));
        let spec = BoundReconfigurableExpressionSpec {
            input: BoundRef::Const(Value::NoVal),
            scope: ReconfigurableExpressionScope::Restricted {
                allowed_variables: variables.into_iter().collect(),
            },
            kind,
            site: RuntimeExpressionSite::default(),
            specialise: false,
        };
        (spec, environment)
    }

    fn activate(
        source: &str,
        spec: &BoundReconfigurableExpressionSpec,
        state: &mut ReconfigurableExpressionState,
        environment: &Rc<EnvironmentLayout>,
    ) -> ReconfigurableExpressionActivation {
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
        typing: Option<RuntimeExpressionTyping>,
    ) -> BoundReconfigurableExpressionSpec {
        BoundReconfigurableExpressionSpec {
            input: BoundRef::Const(Value::NoVal),
            scope,
            kind,
            specialise: typing.is_some(),
            site: RuntimeExpressionSite::new(Default::default(), Default::default(), typing),
        }
    }

    /// An untyped site whose source context names `alias` as `Int`.
    fn alias_site(alias: &str) -> RuntimeExpressionSite {
        let mut builder = crate::lang::dsrv::source::SourceContext::builder();
        builder
            .insert(
                crate::lang::dsrv::source::TypeName::new(alias).unwrap(),
                StreamType::Int,
            )
            .unwrap();
        RuntimeExpressionSite::new(
            crate::lang::dsrv::ast::AstShared::new(builder.build().unwrap()),
            Default::default(),
            None,
        )
    }

    fn cache_template(
        cache: &mut SharedReconfigurableExpressionCache,
        source: &str,
        spec: &BoundReconfigurableExpressionSpec,
        environment: &Rc<EnvironmentLayout>,
    ) -> Rc<ReconfigurableExpressionTemplate> {
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

    const LOCATED_ROOT: &str = "use experimental::{modules, functions}\nmod lib\nuse lib::*\n\
        in s: Str\nin x: Int\nout y: Int\ny = dynamic(s: Int)\n";
    const LOCATED_LIB: &str = "use experimental::{modules, functions}\n\
        def good(n: Int) -> Int = n * 2\ndef bad(n: Int) -> Int = n + true\n";

    /// The prepared site of `y`'s `dynamic` in a program whose files are
    /// labelled with `prefix`, and a probe on its library file.
    fn located_site(
        prefix: &str,
    ) -> (
        RuntimeExpressionSite,
        std::sync::Weak<crate::lang::dsrv::source_map::SourceFile>,
    ) {
        use crate::dsrv_fixtures::WithoutWarnings;
        use crate::lang::dsrv::modules::ModuleCollector;
        use crate::lang::dsrv::source_map::SourceLabel;

        let label = |name: &str| SourceLabel::Path(format!("{prefix}{name}").into());
        let mut collector = ModuleCollector::with_label(LOCATED_ROOT, label("root.dsrv")).unwrap();
        collector
            .supply_labelled(LOCATED_LIB, label("lib.dsrv"))
            .unwrap();
        let spec = crate::lang::dsrv::expand::expand_program(
            collector.finish().unwrap(),
            Default::default(),
        )
        .unwrap();
        let lib = spec
            .sources()
            .files()
            .find(|(_, file)| file.label() == &label("lib.dsrv"))
            .map(|(_, file)| std::sync::Arc::downgrade(file))
            .unwrap();
        let elaborated = spec
            .check_and_elaborate(crate::TypeCheckOptions::STRICT)
            .without_warnings()
            .unwrap();
        let site = elaborated
            .var_expr_ref(&VarName::new("y"))
            .unwrap()
            .runtime_expression()
            .clone();
        (site, lib)
    }

    fn located_spec(site: RuntimeExpressionSite) -> BoundReconfigurableExpressionSpec {
        BoundReconfigurableExpressionSpec {
            input: BoundRef::Const(Value::NoVal),
            scope: restricted_scope(&["x"]),
            kind: ReconfigurableExpressionKind::Dynamic,
            specialise: true,
            site,
        }
    }

    /// A template outlives the occurrence that compiled it and is reused by
    /// others, so it keeps what the site means and none of the files its
    /// occurrence captured: no later text is located at a stale occurrence,
    /// and no archive stays alive for it.
    #[test]
    fn a_cached_template_keeps_no_occurrence_provenance() {
        let environment = Rc::new(EnvironmentLayout::from_variables([VarName::new("x")]));
        let (site, lib) = located_site("first/");
        let spec = located_spec(site);
        assert!(spec.site.sources().is_some());
        let mut cache = SharedReconfigurableExpressionCache::default();
        let template = cache_template(&mut cache, "good(x)", &spec, &environment);
        assert!(template.site.sources().is_none());
        assert!(
            cache
                .lookup(&"good(x)".into(), &spec, &environment)
                .is_some()
        );
        drop(spec);
        assert!(lib.upgrade().is_none(), "the cached template holds no file");

        // Text refused at another occurrence is located there.
        let (other, _) = located_site("second/");
        let other = located_spec(other);
        let Err(DataflowEvaluationError::ReconfigurableExpressionType { message, .. }) =
            compile_dynamic_expression(&"bad(x)".into(), &other, &environment)
        else {
            panic!("bad does not check");
        };
        assert!(message.contains("second/lib.dsrv"), "{message}");
        assert!(!message.contains("first/"), "{message}");
        drop(template);
    }

    #[test]
    fn a_runtime_site_does_not_retain_defs_outside_its_callable_scope() {
        use crate::dsrv_fixtures::WithoutWarnings;
        use crate::lang::dsrv::modules::ModuleCollector;
        use crate::lang::dsrv::source_map::SourceLabel;

        let root = "use experimental::{modules, functions}\n\
            mod lib\nmod unused\nuse lib::*\n\
            in s: Str\nout y: Int\ny = dynamic(s: Int)";
        let mut collector =
            ModuleCollector::with_label(root, SourceLabel::Path("root.dsrv".into())).unwrap();
        collector
            .supply_labelled(
                "use experimental::{modules, functions}\n\
                    def available(n: Int) -> Int = n",
                SourceLabel::Path("lib.dsrv".into()),
            )
            .unwrap();
        collector
            .supply_labelled(
                "use experimental::{modules, functions}\n\
                    def unreachable(n: Int) -> Int = n",
                SourceLabel::Path("unused.dsrv".into()),
            )
            .unwrap();
        let specification = crate::lang::dsrv::expand::expand_program(
            collector.finish().unwrap(),
            Default::default(),
        )
        .unwrap();
        let unused = specification
            .sources()
            .files()
            .find(|(_, file)| file.label() == &SourceLabel::Path("unused.dsrv".into()))
            .map(|(_, file)| std::sync::Arc::downgrade(file))
            .unwrap();
        let elaborated = specification
            .check_and_elaborate(crate::TypeCheckOptions::STRICT)
            .without_warnings()
            .unwrap();
        let site = elaborated
            .var_expr_ref(&VarName::new("y"))
            .unwrap()
            .runtime_expression()
            .clone();

        drop(elaborated);
        assert!(
            unused.upgrade().is_none(),
            "a runtime site must not retain an unreachable module"
        );
        assert!(site.sources().is_some());
    }

    #[test]
    fn runtime_source_uses_the_owning_alias_context() {
        let (mut spec, environment) = fixture(ReconfigurableExpressionKind::Dynamic, &["x"]);
        let source = "\\v: U -> v + x".into();
        assert!(compile_dynamic_expression(&source, &spec, &environment).is_err());
        spec.site = alias_site("U");
        assert!(compile_dynamic_expression(&source, &spec, &environment).is_ok());
    }

    #[test]
    fn source_context_is_part_of_local_and_shared_template_identity() {
        let (mut first, environment) = fixture(ReconfigurableExpressionKind::Dynamic, &["x"]);
        first.site = alias_site("U");
        let mut second = first.clone();
        second.site = alias_site("V");
        let mut cache = SharedReconfigurableExpressionCache::default();
        let template = cache_template(&mut cache, "x", &first, &environment);
        assert!(cache.lookup(&"x".into(), &first, &environment).is_some());
        assert!(cache.lookup(&"x".into(), &second, &environment).is_none());
        assert_ne!(
            template.site.context().fingerprint(),
            second.site.context().fingerprint()
        );
    }

    #[test]
    fn activation_reports_lifecycle_and_dependency_changes() {
        let (spec, environment) = fixture(ReconfigurableExpressionKind::Dynamic, &["x", "y"]);
        let mut state = ReconfigurableExpressionState::default();

        assert_eq!(
            activate("x", &spec, &mut state, &environment),
            ReconfigurableExpressionActivation::Activated {
                dependency_slots_changed: true,
            }
        );
        let template = Rc::clone(&state.active_expression.as_ref().unwrap().template);

        assert_eq!(
            activate("x", &spec, &mut state, &environment),
            ReconfigurableExpressionActivation::Unchanged
        );
        assert!(Rc::ptr_eq(
            &template,
            &state.active_expression.as_ref().unwrap().template
        ));

        assert_eq!(
            activate("x + 1", &spec, &mut state, &environment),
            ReconfigurableExpressionActivation::Replaced {
                dependency_slots_changed: false,
            }
        );

        assert_eq!(
            activate("y", &spec, &mut state, &environment),
            ReconfigurableExpressionActivation::Replaced {
                dependency_slots_changed: true,
            }
        );
    }

    #[test]
    fn cached_template_reactivation_uses_fresh_temporal_state() {
        let (spec, environment) = fixture(ReconfigurableExpressionKind::Dynamic, &["x"]);
        let mut state = ReconfigurableExpressionState::default();

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
        let mut state = ReconfigurableExpressionState::default();

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
        let mut state = ReconfigurableExpressionState::default();

        activate("1", &spec, &mut state, &environment);
        let first_template = Rc::clone(&state.active_expression.as_ref().unwrap().template);
        for source in ["2", "3", "4"] {
            activate(source, &spec, &mut state, &environment);
        }
        activate("2", &spec, &mut state, &environment);
        activate("5", &spec, &mut state, &environment);

        assert_eq!(
            state.template_cache.len(),
            RECONFIGURABLE_EXPRESSION_CACHE_CAPACITY
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
        let mut cache = SharedReconfigurableExpressionCache::default();
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
            Err(DataflowEvaluationError::ReconfigurableExpressionContext(_))
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
        let mut cache = SharedReconfigurableExpressionCache::default();
        let first = cache_template(&mut cache, "x", &untyped_spec, &environment_a);
        let different_environment = cache_template(&mut cache, "x", &untyped_spec, &environment_b);
        assert!(!Rc::ptr_eq(&first, &different_environment));

        let type_environment_a = AstShared::new(std::collections::BTreeMap::from([(
            VarName::new("x"),
            crate::core::StreamType::Int,
        )]));
        let type_environment_b = AstShared::new(std::collections::BTreeMap::from([(
            VarName::new("x"),
            crate::core::StreamType::Int,
        )]));
        let typed_spec_a = dynamic_spec(
            restricted_scope(&["x"]),
            ReconfigurableExpressionKind::Dynamic,
            Some(RuntimeExpressionTyping {
                environment: AstShared::clone(&type_environment_a),
                expected: crate::lang::dsrv::type_checker::TCType::Int,
            }),
        );
        let typed_spec_b = dynamic_spec(
            restricted_scope(&["x"]),
            ReconfigurableExpressionKind::Dynamic,
            Some(RuntimeExpressionTyping {
                environment: AstShared::clone(&type_environment_b),
                expected: crate::lang::dsrv::type_checker::TCType::Int,
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
            Some(RuntimeExpressionTyping {
                environment: AstShared::clone(&type_environment_a),
                expected: crate::lang::dsrv::type_checker::TCType::Any,
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
        let mut state = ReconfigurableExpressionState::default();

        assert!(
            update_active_expression_with_change("(".into(), &spec, &mut state, &environment)
                .is_err()
        );
        assert!(state.active_expression.is_none());
        assert!(state.template_cache.is_empty());

        assert!(matches!(
            activate("1", &spec, &mut state, &environment),
            ReconfigurableExpressionActivation::Activated { .. }
        ));
        assert_eq!(
            activate("(", &spec, &mut state, &environment),
            ReconfigurableExpressionActivation::Unchanged
        );
        assert_eq!(&*state.active_expression.as_ref().unwrap().source_text, "1");
    }

    #[test]
    fn nested_expression_reconfiguration_fails_without_caching_a_template() {
        let (spec, environment) = fixture(ReconfigurableExpressionKind::Dynamic, &[]);
        let mut state = ReconfigurableExpressionState::default();

        // The ascription makes the nested source check, so the refusal comes
        // from lowering rather than from checking the text.
        let result = update_active_expression_with_change(
            "dynamic(\"1\": Int)".into(),
            &spec,
            &mut state,
            &environment,
        );
        assert!(
            matches!(
                result,
                Err(DataflowEvaluationError::UnsupportedNestedReconfiguration)
            ),
            "{result:?}"
        );
        assert!(state.active_expression.is_none());
        assert!(state.template_cache.is_empty());
    }
}
