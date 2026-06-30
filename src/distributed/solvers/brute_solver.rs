use std::{collections::BTreeMap, rc::Rc};

use async_stream::stream;
use smol::{
    LocalExecutor,
    stream::{StreamExt, repeat},
};
use tracing::{debug, info};

use crate::{
    InputProvider, OutputStream, UntypedDsrvSpecification, Value, VarName,
    core::Runtime,
    distributed::{
        distribution_graphs::{
            DistributionGraph, LabelledDistGraphStream, LabelledDistributionGraph,
            possible_labelled_dist_graphs,
        },
        scheduling::planning_context::PlanningContext,
    },
    io::testing::ManualOutputHandler,
    runtime::RuntimeBuilder,
    runtime::{asynchronous::AbstractAsyncRuntimeBuilder, distributed::DistAsyncRuntimeBuilder},
    semantics::{
        AbstractContextBuilder, AsyncConfig, MonitoringSemantics,
        distributed::{
            contexts::{DistributedContext, DistributedContextBuilder},
            localisation::Localisable,
        },
    },
};

pub struct BruteForceDistConstraintSolver<S, AC>
where
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>, Spec = UntypedDsrvSpecification>,
    AC::Spec: Localisable,
{
    pub executor: Rc<LocalExecutor<'static>>,
    pub monitor_builder: DistAsyncRuntimeBuilder<AC, S>,
    pub context_builder: Option<DistributedContextBuilder<AC>>,
    pub dist_constraints: Vec<VarName>,
    pub input_vars: Vec<VarName>,
    pub output_vars: Vec<VarName>,
    pub planning_context: Option<PlanningContext>,
}

impl<S, AC> BruteForceDistConstraintSolver<S, AC>
where
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>, Spec = UntypedDsrvSpecification>,
    AC::Spec: Localisable,
{
    fn output_stream_for_graph(
        &self,
        monitor_builder: DistAsyncRuntimeBuilder<AC, S>,
        labelled_graph: Rc<LabelledDistributionGraph>,
    ) -> OutputStream<Vec<bool>> {
        debug!(
            "Output stream for graph with input_vars: {:?} and output_vars: {:?}",
            self.input_vars, self.output_vars
        );

        let context_input_data = self
            .planning_context
            .as_ref()
            .map(|context| context.snapshot().history)
            .unwrap_or_default();

        let input_provider: Box<dyn InputProvider<Val = Value>> = Box::new(context_input_data);
        let mut output_handler = ManualOutputHandler::new(
            self.executor.clone(),
            self.dist_constraints.iter().cloned().collect(),
        );
        let output_stream: OutputStream<BTreeMap<VarName, Value>> =
            Box::pin(output_handler.get_output());

        let potential_dist_graph_stream = Box::pin(repeat(labelled_graph.clone()));
        let context_builder = self
            .context_builder
            .as_ref()
            .map(|b| b.partial_clone())
            .unwrap_or(
                DistributedContextBuilder::new()
                    .graph_stream(potential_dist_graph_stream)
                    .node_names(
                        labelled_graph
                            .dist_graph
                            .graph
                            .node_weights()
                            .cloned()
                            .collect(),
                    ),
            );

        let mut async_builder = monitor_builder.async_monitor_builder.partial_clone();
        async_builder = async_builder
            .context_builder(context_builder)
            .model(
                monitor_builder
                    .async_monitor_builder
                    .model
                    .as_ref()
                    .expect("Model must be set on monitor builder")
                    .clone(),
            )
            .input(input_provider)
            .output(Box::new(output_handler));

        let executor = self.executor.clone();

        // Fix to OutputHandlers previously relying on an implicit ordering based on a
        // Vec<VarName>.
        let order: BTreeMap<VarName, usize> = self
            .dist_constraints
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();

        // Tolerant Value -> bool conversion:
        // - Bool(true/false) => true/false
        // - NoVal/Deferred/other => false
        Box::pin(stream! {
            let runtime = async_builder.build().await;
            executor.spawn(runtime.run()).detach();

            let mut output_stream = output_stream;
            let order = order;
            while let Some(map) = output_stream.next().await {
                let mut v = vec![false; order.len()];
                for (n, val) in map {
                    let idx = order
                        .get(&n)
                        .expect("Output variable not in dist_constraints");
                    match val {
                        Value::Bool(b) => v[*idx] = b,
                        _ => v[*idx] = false,
                    }
                }
                yield v;
            }
        })
    }

    /// Finds all possible labelled distribution graphs given a set of distribution constraints
    /// and a distribution graph.
    pub fn possible_labelled_dist_graph_stream(
        self: Rc<Self>,
        graph: Rc<DistributionGraph>,
    ) -> LabelledDistGraphStream {
        let latest_step: Option<usize> = self
            .planning_context
            .as_ref()
            .and_then(|context| context.snapshot().history.keys().max().copied());

        self.possible_labelled_dist_graph_stream_with_target_step(graph, latest_step)
    }

    /// Finds possible labelled distribution graphs and evaluates constraints at a specific context history step.
    ///
    /// - `target_step = Some(k)`: evaluate constraints at context history step `k`.
    /// - `target_step = None`: evaluate constraints at the first available output row.
    pub fn possible_labelled_dist_graph_stream_with_target_step(
        self: Rc<Self>,
        graph: Rc<DistributionGraph>,
        target_step: Option<usize>,
    ) -> LabelledDistGraphStream {
        let dist_constraints = self.dist_constraints.clone();
        let builder = self.monitor_builder.partial_clone();

        let model = self
            .monitor_builder
            .async_monitor_builder
            .model
            .as_ref()
            .expect("Model must be set on monitor builder");

        // Assignment vars are all non-constraint output vars.
        // Constraint vars must never be sent as local work assignments.
        let assignment_vars: Vec<VarName> = self
            .output_vars
            .iter()
            .filter(|name| !dist_constraints.contains(name))
            .cloned()
            .collect();

        // Localized model is used only for evaluating constraint outputs.
        // Dependencies of constraints become inputs in this localized spec.
        let localised_spec = model.localise(&dist_constraints);
        let builder = builder.model(localised_spec);

        info!(
            "Starting optimized distributed graph generation (target_step={:?})",
            target_step
        );

        Box::pin(async_stream::stream! {
            for (i, labelled_graph) in possible_labelled_dist_graphs(
                graph,
                vec![],
                assignment_vars.clone(),
            )
            .enumerate()
            {
                let labelled_graph = Rc::new(labelled_graph);
                info!("Testing graph {}", i);

                let mut output_stream = self.output_stream_for_graph(
                    builder.partial_clone(),
                    labelled_graph.clone(),
                );

                let evaluation_row: Option<Vec<bool>> = if let Some(step) = target_step {
                    output_stream.nth(step).await
                } else {
                    output_stream.next().await
                };

                let dist_constraints_hold = evaluation_row
                    .as_ref()
                    .is_some_and(|row| !row.is_empty() && row.iter().all(|x| *x));

                info!(
                    "Candidate graph evaluation: index={}, target_step={:?}, row={:?}, constraints_hold={}",
                    i,
                    target_step,
                    evaluation_row,
                    dist_constraints_hold
                );

                if dist_constraints_hold {
                    info!("Found matching graph! index={}, target_step={:?}", i, target_step);
                    yield labelled_graph;
                }
            }
        })
    }
}
