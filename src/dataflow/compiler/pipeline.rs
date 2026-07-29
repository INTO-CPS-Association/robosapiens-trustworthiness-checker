use super::super::execution_plan::{MonitorPlan, StreamSlots};
use super::super::ir::*;
use super::super::monitor::DataflowMonitor;
use super::super::*;
use super::lower::*;
use crate::lang::core::DepGraph as NamedDependencyGraph;
use crate::lang::dsrv::ast::CheckedDsrvSpecification;

impl TryFrom<DsrvSpecification> for DataflowMonitor {
    type Error = DataflowCompilationError;

    fn try_from(specification: DsrvSpecification) -> Result<Self, Self::Error> {
        DataflowMonitor::compile_untyped(specification)
    }
}

impl TryFrom<CheckedDsrvSpecification> for DataflowMonitor {
    type Error = DataflowCompilationError;

    fn try_from(specification: CheckedDsrvSpecification) -> Result<Self, Self::Error> {
        Self::compile_checked(specification)
    }
}

impl DataflowMonitor {
    pub fn compile_checked(
        specification: CheckedDsrvSpecification,
    ) -> Result<Self, DataflowCompilationError> {
        Self::compile_specification(specification, build_checked_expression_graph)
    }

    pub fn compile_untyped(
        specification: DsrvSpecification,
    ) -> Result<Self, DataflowCompilationError> {
        Self::compile_specification(specification, build_expression_graph)
    }

    fn compile_specification<S>(
        specification: S,
        build_graph: impl Fn(S::Expr) -> UnboundEvaluationGraph,
    ) -> Result<Self, DataflowCompilationError>
    where
        S: Specification,
    {
        let input_variables = specification.input_vars().into_iter().collect::<Vec<_>>();
        let output_variables = specification.output_vars().into_iter().collect::<Vec<_>>();
        let stream_variables = specification.stream_vars();
        let dataflow = LoweredDataflow::build(&input_variables, &stream_variables, |variable| {
            specification.var_expr(variable).map(&build_graph)
        })?;
        dataflow.into_monitor(input_variables, output_variables)
    }
}

pub(in crate::dataflow) type NamedDependencies = BTreeMap<VarName, BTreeSet<VarName>>;

struct LoweredDataflow {
    graphs: BTreeMap<VarName, UnboundEvaluationGraph>,
    static_dependencies: NamedDependencies,
}

struct OrderedDataflow {
    streams: Vec<LoweredStream>,
    static_dependencies: NamedDependencies,
}

struct LoweredStream {
    name: VarName,
    graph: UnboundEvaluationGraph,
}

impl LoweredDataflow {
    fn build(
        input_variables: &[VarName],
        stream_variables: &BTreeSet<VarName>,
        mut build_graph: impl FnMut(&VarName) -> Option<UnboundEvaluationGraph>,
    ) -> Result<Self, DataflowCompilationError> {
        let available_variables = input_variables
            .iter()
            .chain(stream_variables)
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut graphs = BTreeMap::new();
        let mut static_dependencies = NamedDependencies::new();
        for variable in stream_variables {
            let mut graph = build_graph(variable)
                .ok_or_else(|| DataflowCompilationError::MissingExpression(variable.clone()))?;
            graph.resolve_automatic_dynamic_scopes(variable, input_variables, stream_variables);
            let unavailable_variables = graph
                .free_vars(Some(variable))
                .into_iter()
                .filter(|dependency| !available_variables.contains(dependency))
                .collect::<Vec<_>>();
            if !unavailable_variables.is_empty() {
                return Err(DataflowCompilationError::UnavailableVariables {
                    stream: variable.clone(),
                    variables: unavailable_variables,
                });
            }
            static_dependencies.insert(variable.clone(), graph.same_tick_free_vars(Some(variable)));
            graphs.insert(variable.clone(), graph);
        }
        Ok(Self {
            graphs,
            static_dependencies,
        })
    }

    fn into_static_order(self) -> Result<OrderedDataflow, DataflowCompilationError> {
        let stream_variables = self.graphs.keys().cloned().collect::<BTreeSet<_>>();
        let ordered_names =
            NamedDependencyGraph::from_dependencies(self.static_dependencies.clone())
                .topological_streams(&stream_variables)
                .map_err(DataflowCompilationError::DependencyCycle)?;
        debug_assert_eq!(ordered_names.len(), self.graphs.len());
        let mut graphs = self.graphs;
        let streams = ordered_names
            .into_iter()
            .map(|name| {
                let graph = graphs
                    .remove(&name)
                    .expect("dependency graph stream must have a lowered evaluation graph");
                LoweredStream { name, graph }
            })
            .collect();
        debug_assert!(graphs.is_empty());
        Ok(OrderedDataflow {
            streams,
            static_dependencies: self.static_dependencies,
        })
    }

    fn into_monitor(
        self,
        input_variables: Vec<VarName>,
        output_variables: Vec<VarName>,
    ) -> Result<DataflowMonitor, DataflowCompilationError> {
        let OrderedDataflow {
            streams,
            static_dependencies,
        } = self.into_static_order()?;
        let stream_variables = streams
            .iter()
            .map(|stream| stream.name.clone())
            .collect::<Vec<_>>();
        let environment_layout = Rc::new(EnvironmentLayout::from_variables(
            input_variables
                .iter()
                .cloned()
                .chain(streams.iter().map(|stream| stream.name.clone())),
        ));
        let output_slots = output_variables
            .iter()
            .map(|variable| {
                environment_layout
                    .slot(variable)
                    .ok_or_else(|| DataflowCompilationError::UnknownOutput(variable.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        debug_assert!(
            output_slots
                .iter()
                .all(|slot| slot.index() < environment_layout.len())
        );

        let stream_programs = streams
            .into_iter()
            .map(|LoweredStream { name, graph }| {
                graph.bind_graph(Some(name), Rc::clone(&environment_layout))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let stream_slots = StreamSlots::new(
            EnvironmentSlot::new(input_variables.len()),
            stream_programs.len(),
        );
        let monitor_plan = MonitorPlan::build(
            stream_slots,
            &stream_variables,
            &static_dependencies,
            &stream_programs,
        )?;
        Ok(DataflowMonitor::new(
            input_variables,
            output_variables,
            output_slots,
            stream_variables,
            stream_programs,
            monitor_plan,
            environment_layout.len(),
        ))
    }
}
