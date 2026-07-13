use super::super::execution::plan_executor::PlanExecutor;
use super::super::monitor::DataflowMonitor;
use super::super::plan::*;
use super::super::*;
use super::lower::*;
use crate::lang::core::DepGraph;

impl TryFrom<UntypedDsrvSpecification> for DataflowMonitor {
    type Error = DataflowCompileError;

    fn try_from(spec: UntypedDsrvSpecification) -> Result<Self, Self::Error> {
        DataflowMonitor::try_compile_untyped(spec)
    }
}

impl TryFrom<TypedDsrvSpecification> for DataflowMonitor {
    type Error = DataflowCompileError;

    fn try_from(spec: TypedDsrvSpecification) -> Result<Self, Self::Error> {
        DataflowMonitor::try_compile_typed(spec)
    }
}

impl DataflowMonitor {
    pub fn try_compile_untyped(
        spec: UntypedDsrvSpecification,
    ) -> Result<Self, DataflowCompileError> {
        Self::try_compile_spec(spec, lower_untyped_expr_plan)
    }

    pub fn try_compile_typed(spec: TypedDsrvSpecification) -> Result<Self, DataflowCompileError> {
        Self::try_compile_spec(spec, lower_typed_expr_plan)
    }

    fn try_compile_spec<S>(
        spec: S,
        lower: impl Fn(S::Expr) -> UnboundPlanBody,
    ) -> Result<Self, DataflowCompileError>
    where
        S: Specification,
    {
        let input_vars = spec.input_vars().into_iter().collect::<Vec<_>>();
        let output_vars = spec.output_vars().into_iter().collect::<Vec<_>>();
        let stream_vars = spec.stream_vars();
        let program = LoweredProgram::build(&input_vars, &stream_vars, |var| {
            spec.var_expr(var).map(&lower)
        })?;
        program.into_monitor(input_vars, output_vars)
    }
}

struct LoweredProgram {
    plans: BTreeMap<VarName, UnboundPlanBody>,
    dependencies: BTreeMap<VarName, BTreeSet<VarName>>,
}

impl LoweredProgram {
    fn build(
        input_vars: &[VarName],
        stream_vars: &BTreeSet<VarName>,
        mut build: impl FnMut(&VarName) -> Option<UnboundPlanBody>,
    ) -> Result<Self, DataflowCompileError> {
        let available = input_vars
            .iter()
            .chain(stream_vars)
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut plans = BTreeMap::new();
        let mut dependencies = BTreeMap::new();
        for var in stream_vars {
            let mut body =
                build(var).ok_or_else(|| DataflowCompileError::MissingExpression(var.clone()))?;
            body.configure_dynamic_context(var, input_vars, stream_vars);
            let inputs = body.free_vars(Some(var));
            let unsupported = inputs
                .iter()
                .filter(|input| !available.contains(*input))
                .cloned()
                .collect::<Vec<_>>();
            if !unsupported.is_empty() {
                return Err(DataflowCompileError::UnavailableInputs {
                    stream: var.clone(),
                    inputs: unsupported,
                });
            }
            dependencies.insert(var.clone(), inputs);
            plans.insert(var.clone(), body);
        }
        Ok(Self {
            plans,
            dependencies,
        })
    }

    fn into_ordered(
        self,
    ) -> Result<
        (
            Vec<(VarName, UnboundPlanBody)>,
            BTreeMap<VarName, BTreeSet<VarName>>,
        ),
        DataflowCompileError,
    > {
        let stream_vars = self.plans.keys().cloned().collect::<BTreeSet<_>>();
        let ordered = DepGraph::from_dependencies(self.dependencies.clone())
            .topological_streams(&stream_vars)
            .map_err(DataflowCompileError::DependencyCycle)?;
        debug_assert_eq!(ordered.len(), self.plans.len());
        let mut plans = self.plans;
        let ordered = ordered
            .into_iter()
            .map(|var| {
                let body = plans
                    .remove(&var)
                    .expect("dependency graph stream must have a lowered plan");
                (var, body)
            })
            .collect();
        debug_assert!(plans.is_empty());
        Ok((ordered, self.dependencies))
    }

    fn into_monitor(
        self,
        input_vars: Vec<VarName>,
        output_vars: Vec<VarName>,
    ) -> Result<DataflowMonitor, DataflowCompileError> {
        let (plans, dependencies) = self.into_ordered()?;
        let stream_vars = plans.iter().map(|(var, _)| var.clone()).collect::<Vec<_>>();
        let environment = Rc::new(EnvironmentLayout::from_vars(
            input_vars
                .iter()
                .cloned()
                .chain(plans.iter().map(|(var, _)| var.clone())),
        ));
        let output_ids = output_vars
            .iter()
            .map(|var| {
                environment
                    .get(var)
                    .ok_or_else(|| DataflowCompileError::UnknownOutput(var.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        debug_assert!(output_ids.iter().all(|id| id.index() < environment.len()));

        let mut stream_executors = Vec::with_capacity(plans.len());
        for (var, body) in plans {
            let plan = body.bind(Some(var), Rc::clone(&environment))?;
            stream_executors.push(PlanExecutor::new(plan));
        }

        Ok(DataflowMonitor::from_compiled_parts(
            input_vars,
            output_vars,
            output_ids,
            stream_vars,
            dependencies,
            stream_executors,
            environment.len(),
        ))
    }
}
