use std::{
    collections::{BTreeMap, VecDeque},
    rc::Rc,
};

use async_stream::stream;
use smol::{
    LocalExecutor, Task,
    stream::{StreamExt, repeat},
};
use tracing::{debug, info};

use crate::{
    DsrvSpecification, InputStream, LocalStream, Value, VarName,
    core::Runtime,
    distributed::{
        distribution_graphs::{
            DistributionGraph, LabelledDistGraphStream, LabelledDistributionGraph,
            possible_labelled_dist_graphs,
        },
        scheduling::planning_context::PlanningContext,
    },
    io::OutputBackendBuilder,
    io::output::{ManualOutputBackend, OutputBackendConfig, OutputDestination},
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
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>, Spec = DsrvSpecification>,
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

struct CandidateRuntime {
    output_stream: LocalStream<anyhow::Result<Vec<bool>>>,
    executor: Rc<LocalExecutor<'static>>,
    task: Task<anyhow::Result<()>>,
}

impl CandidateRuntime {
    async fn evaluate(self, target_step: Option<usize>) -> anyhow::Result<Option<Vec<bool>>> {
        let Self {
            mut output_stream,
            executor,
            task,
        } = self;

        let evaluation = if let Some(step) = target_step {
            match output_stream.nth(step).await {
                Some(result) => result.map(Some),
                None => Err(anyhow::anyhow!(
                    "candidate output ended before target history step {step}"
                )),
            }
        } else {
            match output_stream.next().await {
                Some(result) => result.map(Some),
                None => Err(anyhow::anyhow!(
                    "candidate output ended before its first complete row"
                )),
            }
        };

        // Release the receiver before cancelling the producer. This lets the owned runtime see
        // the closed manual transport while the task is being joined, instead of leaving a
        // producer and its error-bearing output writer detached from the solver.
        drop(output_stream);
        // `Task::cancel` must be driven by the executor that owns the task. Calling
        // `cancel().await` directly can wait forever when the caller is running on another
        // executor (as embedding callers and isolated tests legitimately do).
        let runtime_result = match executor.run(task.cancel()).await {
            Some(result) => result,
            None => Ok(()),
        };

        match (evaluation, runtime_result) {
            (Ok(row), Ok(())) => Ok(row),
            (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
            (Err(primary), Err(additional)) => {
                Err(anyhow::anyhow!("{primary}; additionally: {additional}"))
            }
        }
    }
}

fn candidate_constraint_stream(
    output_stream: LocalStream<BTreeMap<VarName, Value>>,
    order: BTreeMap<VarName, usize>,
) -> LocalStream<anyhow::Result<Vec<bool>>> {
    Box::pin(stream! {
        if order.is_empty() {
            yield Err(anyhow::anyhow!("candidate has no distribution constraints"));
            return;
        }

        let mut output_stream = output_stream;
        let mut per_variable = vec![VecDeque::<bool>::new(); order.len()];
        while let Some(map) = output_stream.next().await {
            for (name, value) in map {
                let Some(index) = order.get(&name).copied() else {
                    yield Err(anyhow::anyhow!(
                        "candidate output variable `{name}` is not in dist_constraints"
                    ));
                    return;
                };
                per_variable[index].push_back(matches!(value, Value::Bool(true)));
            }
            while per_variable.iter().all(|values| !values.is_empty()) {
                yield Ok(per_variable
                    .iter_mut()
                    .map(|values| values.pop_front().expect("queue was checked"))
                    .collect());
            }
        }

        if per_variable.iter().any(|values| !values.is_empty()) {
            yield Err(anyhow::anyhow!(
                "candidate output ended with an incomplete constraint row"
            ));
        }
    })
}

impl<S, AC> BruteForceDistConstraintSolver<S, AC>
where
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>, Spec = DsrvSpecification>,
    AC::Spec: Localisable,
{
    fn output_stream_for_graph(
        &self,
        monitor_builder: DistAsyncRuntimeBuilder<AC, S>,
        labelled_graph: Rc<LabelledDistributionGraph>,
    ) -> CandidateRuntime {
        debug!(
            "Output stream for graph with input_vars: {:?} and output_vars: {:?}",
            self.input_vars, self.output_vars
        );

        let context_input_data = self
            .planning_context
            .as_ref()
            .map(|context| context.snapshot().history)
            .unwrap_or_default();

        let input_stream: InputStream<Value> = crate::io::file::input_stream(
            context_input_data,
            self.input_vars.iter().cloned().collect(),
        );
        let (manual_backend, receiver) = ManualOutputBackend::<Value>::channel(1);
        let output_stream: LocalStream<BTreeMap<VarName, Value>> = Box::pin(
            futures::stream::unfold(receiver, |mut receiver| async move {
                receiver.recv().await.map(|row| (row, receiver))
            }),
        );
        let output_builder = OutputBackendBuilder::from_destination(OutputDestination::new(
            "solver",
            OutputBackendConfig::Manual(manual_backend.sender().clone()),
        ));

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
            .input(input_stream);

        let executor = self.executor.clone();

        // Preserve a stable variable ordering for the routed output writer.
        let output_variables = self.dist_constraints.clone();
        let order: BTreeMap<VarName, usize> = self
            .dist_constraints
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();

        // Tolerant Value -> bool conversion:
        // - Bool(true/false) => true/false
        // - NoVal/Deferred/other => false
        let output_stream = candidate_constraint_stream(output_stream, order);
        let task = executor.spawn(async move {
            let writer = output_builder
                .build(&output_variables, std::iter::empty::<VarName>(), None)
                .await
                .map_err(|error| {
                    anyhow::anyhow!("distributed solver output pipeline could not open: {error}")
                })?;
            let runtime = async_builder.output_writer(writer).build().await;
            runtime.run().await.map_err(|error| {
                anyhow::anyhow!("distributed solver candidate runtime failed: {error}")
            })
        });

        CandidateRuntime {
            output_stream,
            executor,
            task,
        }
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
    /// This compatibility adapter retains the existing planner API, which can represent only
    /// `Option<LabelledDistributionGraph>`. Candidate failures therefore terminate the stream
    /// without yielding a graph; callers that can preserve errors should use
    /// [`Self::try_possible_labelled_dist_graph_stream_with_target_step`].
    ///
    /// - `target_step = Some(k)`: evaluate constraints at context history step `k`.
    /// - `target_step = None`: evaluate constraints at the first available output row.
    pub fn possible_labelled_dist_graph_stream_with_target_step(
        self: Rc<Self>,
        graph: Rc<DistributionGraph>,
        target_step: Option<usize>,
    ) -> LabelledDistGraphStream {
        let mut fallible =
            self.try_possible_labelled_dist_graph_stream_with_target_step(graph, target_step);
        Box::pin(stream! {
            while let Some(result) = fallible.next().await {
                match result {
                    Ok(labelled_graph) => yield labelled_graph,
                    Err(error) => {
                        debug!(
                            "Brute-force distributed solver stopped after candidate error: {error}"
                        );
                        break;
                    }
                }
            }
        })
    }

    /// Fallible form of [`Self::possible_labelled_dist_graph_stream_with_target_step`].
    ///
    /// Open, input, runtime, output, and incomplete-candidate errors are yielded to the caller
    /// after the candidate task has been cancelled and joined. The existing scheduler planner
    /// currently consumes the compatibility adapter because its trait returns `Option`.
    pub fn try_possible_labelled_dist_graph_stream_with_target_step(
        self: Rc<Self>,
        graph: Rc<DistributionGraph>,
        target_step: Option<usize>,
    ) -> LocalStream<anyhow::Result<Rc<LabelledDistributionGraph>>> {
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

                let candidate = self.output_stream_for_graph(
                    builder.partial_clone(),
                    labelled_graph.clone(),
                );
                let evaluation_row = match candidate.evaluate(target_step).await {
                    Ok(row) => row,
                    Err(error) => {
                        debug!(
                            "Candidate graph evaluation failed: index={}, target_step={:?}, error={error}",
                            i, target_step
                        );
                        yield Err(error);
                        break;
                    }
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
                    yield Ok(labelled_graph);
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    };

    use futures::{FutureExt, StreamExt};

    use super::*;

    struct PendingWithDrop(Rc<Cell<bool>>);

    impl Future for PendingWithDrop {
        type Output = anyhow::Result<()>;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for PendingWithDrop {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }

    #[test]
    fn candidate_runtime_is_cancelled_and_joined_after_a_decision() {
        let executor = Rc::new(LocalExecutor::new());
        let cancelled = Rc::new(Cell::new(false));
        let task = executor.spawn(PendingWithDrop(cancelled.clone()));
        let candidate = CandidateRuntime {
            output_stream: Box::pin(futures::stream::iter([Ok(vec![true])])),
            executor: executor.clone(),
            task,
        };

        let row = smol::block_on(async {
            let evaluation = candidate.evaluate(None).fuse();
            let timeout = async {
                smol::Timer::after(std::time::Duration::from_millis(250)).await;
            }
            .fuse();
            futures::pin_mut!(evaluation, timeout);
            futures::select! {
                result = evaluation => Some(result),
                _ = timeout => None,
            }
        })
        .expect("candidate evaluation/cancellation timed out")
        .expect("complete candidate output should evaluate")
        .expect("candidate should contain a row");

        assert_eq!(row, vec![true]);
        assert!(
            cancelled.get(),
            "candidate task must be cancelled and joined"
        );
    }

    #[test]
    fn candidate_output_ends_with_an_incomplete_row_as_an_error() {
        let output = Box::pin(futures::stream::iter([BTreeMap::from([(
            VarName::new("constraint_a"),
            Value::Bool(true),
        )])])) as LocalStream<BTreeMap<VarName, Value>>;
        let mut output = candidate_constraint_stream(
            output,
            BTreeMap::from([
                (VarName::new("constraint_a"), 0),
                (VarName::new("constraint_b"), 1),
            ]),
        );

        let result = smol::block_on(output.next())
            .expect("incomplete output should report an error")
            .expect_err("incomplete output must not become a candidate row");
        assert!(result.to_string().contains("incomplete constraint row"));
    }

    #[test]
    fn candidate_runtime_rejects_early_output_instead_of_waiting_forever() {
        let executor = Rc::new(LocalExecutor::new());
        let task = executor.spawn(async { Ok(()) });
        let candidate = CandidateRuntime {
            output_stream: Box::pin(futures::stream::empty()),
            executor: executor.clone(),
            task,
        };

        let error = smol::block_on(async {
            let evaluation = candidate.evaluate(Some(1)).fuse();
            let timeout = async {
                smol::Timer::after(std::time::Duration::from_millis(250)).await;
            }
            .fuse();
            futures::pin_mut!(evaluation, timeout);
            futures::select! {
                result = evaluation => Some(result),
                _ = timeout => None,
            }
        })
        .expect("early-output evaluation/cancellation timed out")
        .expect_err("early output termination must be an error");
        assert!(error.to_string().contains("before target history step 1"));
    }
}
