use std::{collections::BTreeMap, rc::Rc};

use smol::{
    LocalExecutor,
    stream::{StreamExt, repeat},
};
use tracing::{debug, info};

use crate::{
    InputProvider, OutputStream, Specification, Value, VarName,
    core::{AbstractMonitorBuilder, Runnable, to_typed_stream_vec},
    distributed::distribution_graphs::{
        DistributionGraph, LabelledDistGraphStream, LabelledDistributionGraph,
        possible_labelled_dist_graphs,
    },
    io::testing::ManualOutputHandler,
    runtime::{asynchronous::AbstractAsyncMonitorBuilder, distributed::DistAsyncMonitorBuilder},
    semantics::{
        AbstractContextBuilder, MonitoringSemantics,
        distributed::{
            contexts::{DistributedContext, DistributedContextBuilder},
            localisation::Localisable,
        },
    },
};

pub struct BruteForceDistConstraintSolver<Expr, S, M>
where
    S: MonitoringSemantics<Expr, Value, DistributedContext<Value>>,
    M: Specification<Expr = Expr> + Localisable,
{
    pub executor: Rc<LocalExecutor<'static>>,
    pub monitor_builder: DistAsyncMonitorBuilder<M, DistributedContext<Value>, Value, Expr, S>,
    pub context_builder: Option<DistributedContextBuilder<Value>>,
    pub dist_constraints: Vec<VarName>,
    pub input_vars: Vec<VarName>,
    pub output_vars: Vec<VarName>,
}

impl<Expr, S, M> BruteForceDistConstraintSolver<Expr, S, M>
where
    Expr: 'static,
    S: MonitoringSemantics<Expr, Value, DistributedContext<Value>>,
    M: Specification<Expr = Expr> + Localisable,
{
    fn output_stream_for_graph(
        &self,
        monitor_builder: DistAsyncMonitorBuilder<M, DistributedContext<Value>, Value, Expr, S>,
        labelled_graph: Rc<LabelledDistributionGraph>,
    ) -> OutputStream<Vec<bool>> {
        // Build a runtime for constructing the output stream
        debug!(
            "Output stream for graph with input_vars: {:?} and output_vars: {:?}",
            self.input_vars, self.output_vars
        );
        let input_provider: Box<dyn InputProvider<Val = Value>> =
            Box::new(BTreeMap::<VarName, OutputStream<Value>>::new());
        let mut output_handler =
            ManualOutputHandler::new(self.executor.clone(), self.dist_constraints.clone());
        let output_stream: OutputStream<Vec<Value>> = Box::pin(output_handler.get_output());
        let potential_dist_graph_stream = Box::pin(repeat(labelled_graph.clone()));
        let context_builder = self
            .context_builder
            .as_ref()
            .map(|b| b.partial_clone())
            .unwrap_or(DistributedContextBuilder::new().graph_stream(potential_dist_graph_stream));
        let runtime = monitor_builder
            .context_builder(context_builder)
            .static_dist_graph((*labelled_graph).clone())
            .input(input_provider)
            .output(Box::new(output_handler))
            .build();
        self.executor.spawn(runtime.run()).detach();

        // Construct a wrapped output stream which makes sure the context starts
        to_typed_stream_vec(output_stream)
    }

    /// Finds all possible labelled distribution graphs given a set of distribution constraints
    /// and a distribution graph
    pub fn possible_labelled_dist_graph_stream(
        self: Rc<Self>,
        graph: Rc<DistributionGraph>,
    ) -> LabelledDistGraphStream {
        // To avoid lifetime and move issues, clone all necessary data for the async block.
        let dist_constraints = self.dist_constraints.clone();
        let builder = self.monitor_builder.partial_clone();
        let non_dist_constraints: Vec<VarName> = self
            .output_vars
            .iter()
            .filter(|name| !dist_constraints.contains(name))
            .cloned()
            .collect();
        let localised_spec = self
            .monitor_builder
            .async_monitor_builder
            .model
            .as_ref()
            .unwrap()
            .localise(&dist_constraints);
        let builder = builder.model(localised_spec);

        info!("Starting optimized distributed graph generation");

        Box::pin(async_stream::stream! {
            for (i, labelled_graph) in possible_labelled_dist_graphs(graph, dist_constraints.clone(), non_dist_constraints).enumerate() {
                let labelled_graph = Rc::new(labelled_graph);

                info!("Testing graph {}", i);

                let mut output_stream = self.output_stream_for_graph(
                    builder.partial_clone(),
                    labelled_graph.clone(),
                );

                let first_output: Vec<bool> = output_stream.next().await.unwrap();
                let dist_constraints_hold = first_output.iter().all(|x| *x);
                if dist_constraints_hold {
                    info!("Found matching graph!");
                    yield labelled_graph;
                }
            }
        })
    }
}
