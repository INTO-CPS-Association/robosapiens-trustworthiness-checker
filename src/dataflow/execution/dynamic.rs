use super::super::compiler::lower::*;
use super::super::plan::*;
use super::super::*;
use super::plan_executor::{EvalContext, PlanExecutor};
use super::state::*;

pub(in crate::dataflow) fn eval_dynamic_value(
    current: Value,
    spec: &BoundDynamicSpec,
    dynamic: &mut DynamicState,
    _tick: usize,
    context: EvalContext<'_>,
) -> Result<Value, DataflowEvalError> {
    match current {
        // The active plan is always ticked so its state keeps advancing; in
        // `Defer` mode its output is also the node result, otherwise the
        // special value propagates.
        special @ (Value::Deferred | Value::NoVal) => {
            let result = tick_dynamic_plan(dynamic, context.dynamic_dependencies);
            if spec.mode == DataflowDynamicMode::Defer && dynamic.active.is_some() {
                Ok(result?)
            } else {
                Ok(special)
            }
        }
        Value::Str(source) => {
            let should_compile = match spec.mode {
                DataflowDynamicMode::Defer => dynamic.active.is_none(),
                DataflowDynamicMode::Dynamic => {
                    dynamic.active.as_ref().map(|active| &active.source) != Some(&source)
                }
            };
            if should_compile {
                let compiled = compile_dynamic_plan(&source, spec, context.environment)?;
                dynamic.active = Some(ActiveDynamic {
                    source,
                    executor: PlanExecutor::new(compiled.plan),
                    dependencies: compiled.dependencies,
                });
                dynamic.result_last = None;
            }
            tick_dynamic_plan(dynamic, context.dynamic_dependencies)
        }
        other => Err(DataflowEvalError::InvalidDynamicValue(other.to_string())),
    }
}

fn tick_dynamic_plan(
    dynamic: &mut DynamicState,
    observed: Option<&std::cell::RefCell<BTreeSet<VarName>>>,
) -> Result<Value, DataflowEvalError> {
    let DynamicState {
        active,
        environment_values,
        ..
    } = dynamic;
    let Some(active) = active.as_mut() else {
        return Ok(Value::Deferred);
    };
    if let Some(observed) = observed {
        observed
            .borrow_mut()
            .extend(active.dependencies.iter().cloned());
    }
    active
        .executor
        .evaluate_observing(environment_values, None, observed)
}

struct CompiledDynamicPlan {
    plan: Rc<ExecutablePlan>,
    dependencies: BTreeSet<VarName>,
}

fn compile_dynamic_plan(
    source: &EcoString,
    spec: &BoundDynamicSpec,
    environment: &Rc<EnvironmentLayout>,
) -> Result<CompiledDynamicPlan, DataflowEvalError> {
    let mut source_ref = source.as_ref();
    let expr =
        LALRParser::parse(&mut source_ref).map_err(|error| DataflowEvalError::DynamicParse {
            expression: source.clone(),
            message: error.to_string(),
        })?;
    let mut program = if let Some((type_info, typ)) = &spec.typed {
        let stream_type = typ
            .to_stream_type()
            .expect("typed dynamic/defer target should be concrete at runtime");
        let mut type_info = type_info.clone();
        let typed = (expr, StreamTypeAscription::Ascribed(stream_type))
            .type_check(&mut type_info)
            .map_err(|errors| DataflowEvalError::DynamicType {
                expression: source.clone(),
                message: format!("{errors:?}"),
            })?;
        lower_typed_expr_plan(typed)
    } else {
        lower_untyped_expr_plan(expr)
    };
    let allowed_vars = spec
        .scope
        .vars()
        .expect("dynamic scope should be resolved during plan binding");
    program.inherit_dynamic_context(allowed_vars);
    let dependencies = program.free_vars(None);
    let unsupported = dependencies
        .iter()
        .filter(|input| !allowed_vars.contains(input))
        .cloned()
        .collect::<Vec<_>>();
    if !unsupported.is_empty() {
        return Err(DataflowEvalError::DynamicRestrictedContext(unsupported));
    }
    let plan = program
        .bind(None, Rc::clone(environment))
        .map_err(DataflowEvalError::DynamicPlan)?;
    Ok(CompiledDynamicPlan { plan, dependencies })
}
