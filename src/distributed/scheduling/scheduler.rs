use std::{cell::RefCell, mem, rc::Rc};

use async_stream::stream;
use futures::{StreamExt, future::join_all};
use tracing::{error, info};
use unsync::broadcast;

use crate::{
    OutputStream,
    distributed::distribution_graphs::{
        LabelledDistGraphStream, LabelledDistributionGraph, graph_to_png,
    },
    io::mqtt::dist_graph_provider::DistGraphProvider,
};

use super::{
    communication::SchedulerCommunicator, executors::SchedulerExecutor,
    planners::core::SchedulerPlanner,
};

#[derive(Debug, Clone)]
pub enum ReplanningCondition {
    ConstraintsFail,
    Always,
    Never,
}

pub struct Scheduler {
    replanning_condition: ReplanningCondition,
    dist_graph_output_stream: Option<LabelledDistGraphStream>,
    planner: Box<dyn SchedulerPlanner>,
    schedule_executor: SchedulerExecutor,
    dist_graph_provider: Box<dyn DistGraphProvider>,
    dist_constraints_streams: Rc<RefCell<Option<Vec<OutputStream<bool>>>>>,
    dist_graph_sender: broadcast::Sender<Rc<LabelledDistributionGraph>>,
    suppress_output: bool,
}

impl Scheduler {
    pub fn new(
        planner: Box<dyn SchedulerPlanner>,
        communicator: Box<dyn SchedulerCommunicator>,
        dist_graph_provider: Box<dyn DistGraphProvider>,
        replanning_condition: ReplanningCondition,
        suppress_output: bool,
    ) -> Self {
        let mut tx = broadcast::channel(10);
        let executor = SchedulerExecutor::new(communicator);
        let mut rx_output = tx.subscribe();
        let dist_graph_output_stream: Option<LabelledDistGraphStream> = Some(Box::pin(stream! {
            while let Some(x) = rx_output.recv().await {
                yield x
            }
        }));

        Scheduler {
            dist_graph_output_stream,
            planner,
            dist_graph_sender: tx,
            dist_constraints_streams: Rc::new(RefCell::new(None)),
            dist_graph_provider,
            schedule_executor: executor,
            replanning_condition,
            suppress_output,
        }
    }

    pub fn take_graph_stream(&mut self) -> LabelledDistGraphStream {
        mem::take(&mut self.dist_graph_output_stream)
            .expect("Take graph stream called more than once")
    }

    pub fn provide_dist_constraints_streams(&mut self, streams: Vec<OutputStream<bool>>) {
        self.dist_constraints_streams = Rc::new(RefCell::new(Some(streams)));
    }

    pub fn all_constraints_hold(vec: Vec<Option<bool>>) -> Option<bool> {
        vec.into_iter().fold(Some(true), |acc, x| match (acc, x) {
            (Some(b), Some(c)) => Some(b && c),
            _ => None,
        })
    }

    pub fn dist_constraints_hold_stream(&mut self) -> OutputStream<bool> {
        let mut dist_constraints_streams = self
            .dist_constraints_streams
            .take()
            .expect("Distribution constraints streams not provided");

        if dist_constraints_streams.len() == 0 {
            return Box::pin(smol::stream::repeat(true));
        }

        Box::pin(stream! {
            // info!("In dist_constraints_hold_stream");
            while let Some(res) = Self::all_constraints_hold(join_all(dist_constraints_streams.iter_mut().map(|x| x.next())).await) {
                yield res
            }
        })
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        // MAPE-K loop for scheduling
        let dist_constraits_hold_stream =
            smol::stream::once(false).chain(self.dist_constraints_hold_stream());
        let dist_graph_stream = self.dist_graph_provider.dist_graph_stream();
        let mut monitor_stream = dist_graph_stream.zip(dist_constraits_hold_stream);

        let mut plan = None;

        info!("In Scheduler loop");

        // Monitor + Analyse phase in let
        while let Some((graph, constraints_hold)) = monitor_stream.next().await {
            if !self.suppress_output {
                info!("Monitored and analysed");
            }

            // Plan phase
            let should_plan = match self.replanning_condition {
                ReplanningCondition::ConstraintsFail => !constraints_hold,
                ReplanningCondition::Always => true,
                // Even if replanning is disabled, we need to plan at least once
                // so we have an initial plan
                ReplanningCondition::Never => plan.is_none(),
            };
            if should_plan {
                if !self.suppress_output {
                    info!("Plan");
                }
                plan = Some(self.planner.plan(graph).await);
            }

            // Execute phase
            if let Some(Some(ref plan)) = plan {
                if !self.suppress_output {
                    info!("Execute");
                }
                self.schedule_executor.execute(plan.clone()).await;
                self.dist_graph_sender.send(plan.clone()).await;
                if should_plan && !self.suppress_output {
                    info!("Plotting graph");
                    // TODO: should this error be propagated?
                    if let Err(e) = graph_to_png(plan.clone(), "distributed_graph.png").await {
                        error!("Failed to plot graph: {}", e);
                    }
                }
            }

            if !self.suppress_output {
                info!("MAPE-K iteration end");
            }
        }

        if !self.suppress_output {
            info!("Scheduler ended");
        }

        Ok(())
    }
}
