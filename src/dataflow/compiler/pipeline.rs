use super::super::ir::*;
use super::super::monitor::DataflowMonitor;
use super::super::monitor_plan::MonitorPlan;
use super::super::program::DataflowProgram;
use super::super::stream_id::StreamSlots;
use super::super::*;
use super::lower::*;
use crate::core::Semantics;
use crate::lang::core::DepGraph as NamedDependencyGraph;
use crate::lang::dsrv::ElaboratedDsrvSpecification;

impl DataflowProgram {
    /// Compile an elaborated specification into an immutable monitor definition,
    /// using its types for scalar specialisation and for checking runtime text.
    pub fn compile_checked(
        specification: ElaboratedDsrvSpecification,
    ) -> Result<Self, DataflowCompilationError> {
        #[cfg(test)]
        DataflowProgram::record_root_compile(true);
        Self::compile_specification(specification, build_checked_expression_graph)
    }

    /// Compile an elaborated specification with the evaluation strategy of
    /// `semantics`: `untimed` does not consult the types, every other
    /// semantics does, as [`Self::compile_checked`].
    pub fn compile_with_semantics(
        specification: ElaboratedDsrvSpecification,
        semantics: Semantics,
    ) -> Result<Self, DataflowCompilationError> {
        match semantics {
            Semantics::Untimed => {
                #[cfg(test)]
                DataflowProgram::record_root_compile(false);
                Self::compile_specification(specification, build_unspecialised_expression_graph)
            }
            _ => Self::compile_checked(specification),
        }
    }

    fn compile_specification<S>(
        specification: S,
        build_graph: impl Fn(S::Expr) -> UnboundEvaluationGraph,
    ) -> Result<Self, DataflowCompilationError>
    where
        S: Specification,
    {
        crate::core::admit(&specification, crate::dataflow::CAPABILITIES, "dataflow")?;
        let input_variables = specification.input_vars_in_order();
        let output_variables = specification.output_vars_in_order();
        let stream_variables = specification.stream_vars_in_order();
        let type_annotations = specification.type_annotations();
        let dataflow = LoweredDataflow::build(&input_variables, &stream_variables, |variable| {
            specification.var_expr(variable).map(&build_graph)
        })?;
        dataflow.into_program(input_variables, output_variables, type_annotations)
    }
}

impl DataflowMonitor {
    pub fn compile_checked(
        specification: ElaboratedDsrvSpecification,
    ) -> Result<Self, DataflowCompilationError> {
        DataflowProgram::compile_checked(specification).map(Self::from_program)
    }

    /// Compiles a checked monitor and replaces complete eligible scalar graphs with guarded
    /// native-code fast paths. Unsupported graphs continue through the canonical interpreter.
    ///
    /// This is an opt-in prototype: [`Self::compile_checked`] never enables native graph
    /// execution, even when the crate is built with the `jit` feature.
    #[cfg(feature = "jit")]
    pub fn compile_checked_with_jit(
        specification: ElaboratedDsrvSpecification,
        config: JitConfig,
    ) -> Result<Self, DataflowCompilationError> {
        DataflowProgram::compile_checked(specification)
            .map(|program| Self::from_program_with_jit(program, config))
    }

    /// Compile with the evaluation strategy of `semantics`; see
    /// [`DataflowProgram::compile_with_semantics`].
    pub fn compile_with_semantics(
        specification: ElaboratedDsrvSpecification,
        semantics: Semantics,
    ) -> Result<Self, DataflowCompilationError> {
        DataflowProgram::compile_with_semantics(specification, semantics).map(Self::from_program)
    }
}

pub(in crate::dataflow) type NamedDependencies = BTreeMap<VarName, BTreeSet<VarName>>;

struct LoweredDataflow {
    graphs: BTreeMap<VarName, UnboundEvaluationGraph>,
    static_dependencies: NamedDependencies,
    stream_order: Vec<VarName>,
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
        stream_variables: &[VarName],
        mut build_graph: impl FnMut(&VarName) -> Option<UnboundEvaluationGraph>,
    ) -> Result<Self, DataflowCompilationError> {
        let stream_variable_set = stream_variables.iter().cloned().collect::<BTreeSet<_>>();
        let available_variables = input_variables
            .iter()
            .chain(stream_variables.iter())
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut graphs = BTreeMap::new();
        let mut static_dependencies = NamedDependencies::new();
        for variable in stream_variables {
            let mut graph = build_graph(variable)
                .ok_or_else(|| DataflowCompilationError::MissingExpression(variable.clone()))?;
            graph.resolve_automatic_reconfigurable_scopes(
                variable,
                input_variables,
                &stream_variable_set,
            );
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
            stream_order: stream_variables.to_vec(),
        })
    }

    fn into_static_order(self) -> Result<OrderedDataflow, DataflowCompilationError> {
        let LoweredDataflow {
            graphs,
            static_dependencies,
            stream_order,
        } = self;
        let stream_variables = stream_order.iter().cloned().collect::<BTreeSet<_>>();
        let ordered_names = NamedDependencyGraph::from_dependencies(static_dependencies.clone())
            .topological_streams_in_order(&stream_variables, &stream_order)
            .map_err(DataflowCompilationError::DependencyCycle)?;
        debug_assert_eq!(ordered_names.len(), graphs.len());
        let mut graphs = graphs;
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
            static_dependencies,
        })
    }

    fn into_program(
        self,
        input_variables: Vec<VarName>,
        output_variables: Vec<VarName>,
        type_annotations: BTreeMap<VarName, StreamType>,
    ) -> Result<DataflowProgram, DataflowCompilationError> {
        let OrderedDataflow {
            streams,
            static_dependencies,
        } = self.into_static_order()?;
        let stream_variables = streams
            .iter()
            .map(|stream| stream.name.clone())
            .collect::<Vec<_>>();
        let environment_layout = Rc::new(EnvironmentLayout::from_variables_with_types(
            input_variables
                .iter()
                .cloned()
                .chain(streams.iter().map(|stream| stream.name.clone())),
            &type_annotations,
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
        Ok(DataflowProgram::from_parts(
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
