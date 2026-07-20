use std::{cell::RefCell, collections::BTreeMap, mem, rc::Rc, time::Instant};

use async_stream::stream;
use futures::{
    FutureExt, StreamExt,
    future::{join_all, pending},
    pin_mut, select,
};
use tracing::{debug, info, warn};
use unsync::broadcast;

use crate::{
    OutputStream, Specification, VarName,
    distributed::distribution_graphs::{
        DistributionGraph, LabelledDistGraphStream, LabelledDistributionGraph,
    },
    io::{config::TopicMapping, mqtt::dist_graph_provider::DistGraphProvider},
    semantics::distributed::localisation::Localisable,
};

use super::{
    communication::SchedulerCommunicator,
    dist_constraint_evaluator::{PlacementLabelling, PlacementLabellingStream},
    executors::SchedulerExecutor,
    planners::core::SchedulerPlanner,
    planning_context::PlanningContext,
};

#[derive(Debug, Clone)]
pub enum ReplanningCondition {
    ConstraintsFail,
    Always,
    Never,
}

fn should_plan_on_constraints_fail(
    plan_attempted: bool,
    constraints_hold: bool,
    previous_constraints_hold: Option<bool>,
    planning_state_changed: bool,
) -> bool {
    !plan_attempted
        || (!constraints_hold
            && (previous_constraints_hold != Some(false) || planning_state_changed))
}

pub struct Scheduler<M: Specification + Localisable> {
    replanning_condition: ReplanningCondition,
    dist_graph_output_stream: Option<LabelledDistGraphStream>,
    planner: Box<dyn SchedulerPlanner>,
    scheduler_executor: SchedulerExecutor<M>,
    dist_graph_provider: Box<dyn DistGraphProvider>,
    dist_constraints_streams: Rc<RefCell<Option<Vec<OutputStream<bool>>>>>,
    dist_graph_sender: broadcast::Sender<Rc<LabelledDistributionGraph>>,
    placement_labelling_output_stream: Option<PlacementLabellingStream>,
    placement_labelling_sender: broadcast::Sender<Rc<PlacementLabelling>>,
    planning_context: Option<PlanningContext>,
    publish_full_graph_each_tick: bool,
    suppress_output: bool,
}

impl<M: Specification + Localisable> Scheduler<M> {
    pub fn new(
        spec: M,
        var_msg_types: BTreeMap<VarName, String>,
        topic_mapping: TopicMapping,
        planner: Box<dyn SchedulerPlanner>,
        communicator: Box<dyn SchedulerCommunicator<M>>,
        dist_graph_provider: Box<dyn DistGraphProvider>,
        replanning_condition: ReplanningCondition,
        planning_context: Option<PlanningContext>,
        publish_full_graph_each_tick: bool,
        suppress_output: bool,
    ) -> Self {
        let mut tx = broadcast::channel(10);
        let mut placement_tx = broadcast::channel(10);
        let scheduler_executor =
            SchedulerExecutor::new(spec, var_msg_types, topic_mapping, communicator);
        let mut rx_output = tx.subscribe();
        let dist_graph_output_stream: Option<LabelledDistGraphStream> = Some(Box::pin(stream! {
            while let Some(x) = rx_output.recv().await {
                yield x
            }
        }));
        let mut placement_rx_output = placement_tx.subscribe();
        let placement_labelling_output_stream: Option<PlacementLabellingStream> =
            Some(Box::pin(stream! {
                while let Some(x) = placement_rx_output.recv().await {
                    yield x
                }
            }));

        Scheduler {
            dist_graph_output_stream,
            placement_labelling_output_stream,
            planner,
            dist_graph_sender: tx,
            placement_labelling_sender: placement_tx,
            dist_constraints_streams: Rc::new(RefCell::new(None)),
            dist_graph_provider,
            scheduler_executor,
            replanning_condition,
            planning_context,
            publish_full_graph_each_tick,
            suppress_output,
        }
    }

    pub fn take_graph_stream(&mut self) -> LabelledDistGraphStream {
        mem::take(&mut self.dist_graph_output_stream)
            .expect("Take graph stream called more than once")
    }

    pub fn take_placement_labelling_stream(&mut self) -> PlacementLabellingStream {
        mem::take(&mut self.placement_labelling_output_stream)
            .expect("Take placement labelling stream called more than once")
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
            loop {
                let constraint_monitoring_started = Instant::now();
                let values = join_all(dist_constraints_streams.iter_mut().map(|x| x.next())).await;
                let stream_count = values.len();
                let values_received = values.iter().filter(|value| value.is_some()).count();
                let res = Self::all_constraints_hold(values);
                let Some(res) = res else {
                    break;
                };
                // ACSOS paper benchmark instrumentation: log additional timing information
                warn!(
                    target: "benchmark",
                    event = "benchmark_constraint_monitoring",
                    stream_count,
                    values_received,
                    constraints_hold = res,
                    duration_ms = constraint_monitoring_started.elapsed().as_secs_f64() * 1000.0,
                    "benchmark_constraint_monitoring"
                );
                yield res
            }
        })
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        // MAPE-K loop for scheduling
        let mut dist_constraits_hold_stream =
            smol::stream::once(false).chain(self.dist_constraints_hold_stream());
        let mut dist_graph_stream = self.dist_graph_provider.dist_graph_stream();
        let mut latest_graph: Option<Rc<DistributionGraph>> = None;
        let mut latest_constraints_hold = false;
        let mut graph_done = false;
        let mut constraints_done = false;

        let mut plan = None;
        let mut scheduler_tick: usize = 0;
        let mut previous_constraints_hold: Option<bool> = None;
        let mut graph_version: usize = 0;
        let mut last_planned_graph_version: Option<usize> = None;
        let mut last_planned_context_version: Option<usize> = None;

        info!("In Scheduler loop");

        // Monitor + Analyse phase in let
        loop {
            let mape_iteration_started = Instant::now();
            let monitor_analyse_started = Instant::now();
            let next_graph = async {
                if graph_done {
                    pending::<Option<Rc<DistributionGraph>>>().await
                } else {
                    dist_graph_stream.next().await
                }
            }
            .fuse();
            let next_constraints = async {
                if constraints_done {
                    pending::<Option<bool>>().await
                } else {
                    dist_constraits_hold_stream.next().await
                }
            }
            .fuse();
            pin_mut!(next_graph, next_constraints);

            let progressed = select! {
                graph = next_graph => {
                    match graph {
                        Some(graph) => {
                            latest_graph = Some(graph);
                            graph_version = graph_version.saturating_add(1);
                            true
                        }
                        None => {
                            graph_done = true;
                            false
                        }
                    }
                },
                constraints_hold = next_constraints => {
                    match constraints_hold {
                        Some(constraints_hold) => {
                            latest_constraints_hold = constraints_hold;
                            true
                        }
                        None => {
                            constraints_done = true;
                            false
                        }
                    }
                },
            };

            if graph_done && constraints_done {
                break;
            }

            if !progressed {
                continue;
            }

            let Some(graph) = latest_graph.clone() else {
                continue;
            };
            let constraints_hold = latest_constraints_hold;

            if !self.suppress_output {
                debug!(
                    tick = scheduler_tick,
                    constraints_hold, "Monitored and analysed"
                );
            }
            if !self.suppress_output {
                // ACSOS paper benchmark instrumentation: log additional timing information
                warn!(
                    target: "benchmark",
                    event = "benchmark_mape_phase",
                    phase = "monitor",
                    tick = scheduler_tick,
                    constraints_hold,
                    duration_ms = monitor_analyse_started.elapsed().as_secs_f64() * 1000.0,
                    "benchmark_mape_phase"
                );
            }

            // Plan phase
            let analyse_started = Instant::now();
            let context_version = self
                .planning_context
                .as_ref()
                .map(PlanningContext::version)
                .unwrap_or(0);
            let planning_state_changed = last_planned_graph_version != Some(graph_version)
                || last_planned_context_version != Some(context_version);
            let should_plan = match self.replanning_condition {
                // Plan initially, on a new failed episode, or when the graph/input context changed
                // while constraints are still failing. This avoids repeating the same plan for the
                // same evidence while still responding to fresh information during an unresolved
                // failure.
                ReplanningCondition::ConstraintsFail => should_plan_on_constraints_fail(
                    plan.is_some(),
                    constraints_hold,
                    previous_constraints_hold,
                    planning_state_changed,
                ),
                ReplanningCondition::Always => true,
                // Even if replanning is disabled, we need to plan at least once
                // so we have an initial plan
                ReplanningCondition::Never => plan.is_none(),
            };
            if !self.suppress_output {
                debug!(
                    tick = scheduler_tick,
                    constraints_hold,
                    should_plan,
                    graph_version,
                    context_version,
                    planning_state_changed,
                    replanning_condition = ?self.replanning_condition,
                    "Planning decision"
                );
                // ACSOS paper benchmark instrumentation: log additional timing information
                warn!(
                    target: "benchmark",
                    event = "benchmark_mape_phase",
                    phase = "analyse",
                    tick = scheduler_tick,
                    constraints_hold,
                    should_plan,
                    duration_ms = analyse_started.elapsed().as_secs_f64() * 1000.0,
                    "benchmark_mape_phase"
                );
            }
            let planned_placement_labelling = if should_plan {
                if !self.suppress_output {
                    info!(tick = scheduler_tick, "Plan");
                }
                let plan_started = Instant::now();
                plan = Some(
                    self.planner
                        .plan(graph, scheduler_tick, self.planning_context.clone())
                        .await,
                );
                last_planned_graph_version = Some(graph_version);
                last_planned_context_version = Some(
                    self.planning_context
                        .as_ref()
                        .map(PlanningContext::version)
                        .unwrap_or(context_version),
                );
                let plan_succeeded = plan.as_ref().is_some_and(|plan| plan.is_some());
                if !self.suppress_output {
                    // ACSOS paper benchmark instrumentation: log additional timing information
                    warn!(
                        target: "benchmark",
                        event = "benchmark_mape_phase",
                        phase = "plan",
                        tick = scheduler_tick,
                        plan_succeeded,
                        duration_ms = plan_started.elapsed().as_secs_f64() * 1000.0,
                        "benchmark_mape_phase"
                    );
                }
                plan.as_ref()
                    .and_then(|plan| plan.as_ref())
                    .map(|plan| Rc::new(PlacementLabelling::from_labelled_graph(plan)))
            } else {
                None
            };

            // Execute phase
            if let Some(Some(ref plan)) = plan {
                let execute_started = Instant::now();
                let is_bootstrap_tick = scheduler_tick == 0;

                if !self.suppress_output {
                    debug!(tick = scheduler_tick, is_bootstrap_tick, "Execute");
                }

                if self.publish_full_graph_each_tick {
                    // Publish dist graph so downstream consumers can progress.
                    self.dist_graph_sender.send(plan.clone()).await;
                }
                if let Some(placement_labelling) = planned_placement_labelling {
                    self.placement_labelling_sender
                        .send(placement_labelling)
                        .await;
                }

                // Dispatch external work whenever a new plan was selected. The initial plan is
                // already computed after the planner has the context it requires, so waiting for a
                // second scheduler tick can leave workers without any reconfiguration if the
                // constraint stream has no immediate follow-up sample.
                let should_execute = should_plan || scheduler_tick == 1;

                if should_execute {
                    self.scheduler_executor.execute(plan.clone()).await;
                } else if !self.suppress_output {
                    debug!(
                        tick = scheduler_tick,
                        should_plan,
                        "Skipping external work execution (no replanning and not initial post-bootstrap tick)"
                    );
                }
                if !self.suppress_output {
                    // ACSOS paper benchmark instrumentation: log additional timing information
                    warn!(
                        target: "benchmark",
                        event = "benchmark_mape_phase",
                        phase = "execute",
                        tick = scheduler_tick,
                        should_execute,
                        duration_ms = execute_started.elapsed().as_secs_f64() * 1000.0,
                        "benchmark_mape_phase"
                    );
                }

                // Graph rendering is diagnostic-only and can dominate startup/replanning
                // latency for larger deployments, so keep it out of the scheduler hot path.
            }

            if !self.suppress_output {
                // ACSOS paper benchmark instrumentation: log additional timing information
                warn!(
                    target: "benchmark",
                    event = "benchmark_mape_phase",
                    phase = "iteration_total",
                    tick = scheduler_tick,
                    duration_ms = mape_iteration_started.elapsed().as_secs_f64() * 1000.0,
                    "benchmark_mape_phase"
                );
                debug!(tick = scheduler_tick, "MAPE-K iteration end");
            }

            if scheduler_tick != 0 {
                previous_constraints_hold = Some(constraints_hold);
            }
            scheduler_tick = scheduler_tick.saturating_add(1);
        }

        if !self.suppress_output {
            info!("Scheduler ended, staying alive");
        }

        // Stay alive forever to keep the output topics open without spinning.
        futures::future::pending::<()>().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, time::Duration};

    use async_stream::stream;
    use async_trait::async_trait;
    use futures::StreamExt;
    use macro_rules_attribute::apply;
    use petgraph::graph::DiGraph;

    use crate::{
        DsrvSpecification, OutputStream, Value, async_test,
        distributed::{
            distribution_graphs::NodeName,
            scheduling::{
                communication::NullSchedulerCommunicator,
                dist_constraint_evaluator::{
                    ConstraintInputBatch, ConstraintInputIndex, dist_constraint_event_stream,
                },
            },
        },
        dsrv_specification,
        io::{config::TopicMapping, mqtt::dist_graph_provider::StaticDistGraphProvider},
        stream_utils::channel_to_output_stream,
    };

    use super::*;

    struct CountingPlanner {
        plan: Rc<LabelledDistributionGraph>,
        calls: Rc<Cell<usize>>,
    }

    #[async_trait(?Send)]
    impl SchedulerPlanner for CountingPlanner {
        async fn plan(
            &self,
            _graph: Rc<DistributionGraph>,
            _scheduler_tick: usize,
            _planning_context: Option<PlanningContext>,
        ) -> Option<Rc<LabelledDistributionGraph>> {
            self.calls.set(self.calls.get() + 1);
            Some(self.plan.clone())
        }
    }

    struct SchedulerConstraintHarness {
        input_sender: unsync::spsc::Sender<ConstraintInputBatch>,
        gate_input_index: usize,
        planning_context: PlanningContext,
        planner_calls: Rc<Cell<usize>>,
        placement_publications: Rc<Cell<usize>>,
        constraint_evaluations: Rc<Cell<usize>>,
    }

    fn count_stream<T: 'static>(
        mut input: OutputStream<T>,
        counter: Rc<Cell<usize>>,
    ) -> OutputStream<T> {
        Box::pin(stream! {
            while let Some(item) = input.next().await {
                counter.set(counter.get() + 1);
                yield item;
            }
        })
    }

    fn scheduler_constraint_spec() -> DsrvSpecification {
        let mut src = r#"
in gate
out work
out distX
work = gate
distX = gate && monitored_at(work, "A")
"#;
        dsrv_specification(&mut src).unwrap()
    }

    fn graph_with_work_at_b() -> Rc<LabelledDistributionGraph> {
        let mut graph = DiGraph::new();
        let a = graph.add_node(NodeName::new("A"));
        let b = graph.add_node(NodeName::new("B"));
        graph.add_edge(a, b, 1);

        let dist_graph = Rc::new(DistributionGraph {
            central_monitor: a,
            graph,
        });
        Rc::new(LabelledDistributionGraph {
            dist_graph,
            var_names: vec![VarName::new("gate"), VarName::new("work")],
            node_labels: BTreeMap::from([(a, vec![]), (b, vec![VarName::new("work")])]),
        })
    }

    fn start_scheduler_constraint_harness(
        executor: Rc<smol::LocalExecutor<'static>>,
    ) -> SchedulerConstraintHarness {
        let spec = scheduler_constraint_spec();
        let labelled_graph = graph_with_work_at_b();
        let planner_calls = Rc::new(Cell::new(0));
        let placement_publications = Rc::new(Cell::new(0));
        let constraint_evaluations = Rc::new(Cell::new(0));
        let planning_context = PlanningContext::new(false);
        let input_index = ConstraintInputIndex::new([VarName::new("gate")]);
        let gate_input_index = input_index.index_of(&VarName::new("gate")).unwrap();
        let (input_sender, input_receiver) = unsync::spsc::channel(16);

        let mut scheduler = Scheduler::new(
            spec.clone(),
            BTreeMap::from([
                (VarName::new("gate"), "Bool".to_string()),
                (VarName::new("work"), "Bool".to_string()),
                (VarName::new("distX"), "Bool".to_string()),
            ]),
            TopicMapping::new(),
            Box::new(CountingPlanner {
                plan: labelled_graph.clone(),
                calls: planner_calls.clone(),
            }),
            Box::new(NullSchedulerCommunicator),
            Box::new(StaticDistGraphProvider::new(
                labelled_graph.dist_graph.clone(),
            )),
            ReplanningCondition::ConstraintsFail,
            Some(planning_context.clone()),
            false,
            true,
        );
        let placement_stream = count_stream(
            scheduler.take_placement_labelling_stream(),
            placement_publications.clone(),
        );
        let constraint_stream = dist_constraint_event_stream(
            spec,
            vec![VarName::new("distX")],
            placement_stream,
            input_index,
            channel_to_output_stream(input_receiver),
        );
        scheduler.provide_dist_constraints_streams(vec![count_stream(
            constraint_stream,
            constraint_evaluations.clone(),
        )]);

        executor
            .spawn(async move {
                scheduler.run().await.unwrap();
            })
            .detach();

        SchedulerConstraintHarness {
            input_sender,
            gate_input_index,
            planning_context,
            planner_calls,
            placement_publications,
            constraint_evaluations,
        }
    }

    async fn wait_for_counter_at_least(counter: &Cell<usize>, expected: usize, description: &str) {
        for _ in 0..100 {
            if counter.get() >= expected {
                return;
            }
            smol::Timer::after(Duration::from_millis(5)).await;
        }
        panic!(
            "{description} did not reach {expected}; last value was {}",
            counter.get()
        );
    }

    async fn wait_for_feedback_to_settle() {
        smol::Timer::after(Duration::from_millis(50)).await;
    }

    #[test]
    fn constraints_fail_replans_when_fresh_inputs_arrive_while_still_false() {
        assert!(should_plan_on_constraints_fail(
            true,
            false,
            Some(false),
            true
        ));
    }

    #[test]
    fn constraints_fail_skips_duplicate_false_for_same_inputs() {
        assert!(!should_plan_on_constraints_fail(
            true,
            false,
            Some(false),
            false
        ));
    }

    #[apply(async_test)]
    async fn scheduler_evaluator_does_not_republish_labelling_without_fresh_evidence(
        executor: Rc<smol::LocalExecutor<'static>>,
    ) {
        let harness = start_scheduler_constraint_harness(executor);

        wait_for_counter_at_least(&harness.placement_publications, 2, "placement publications")
            .await;
        wait_for_feedback_to_settle().await;

        assert_eq!(harness.placement_publications.get(), 2);
    }

    #[apply(async_test)]
    async fn scheduler_evaluator_feedback_loop_has_bounded_side_effects(
        executor: Rc<smol::LocalExecutor<'static>>,
    ) {
        let harness = start_scheduler_constraint_harness(executor);

        wait_for_counter_at_least(&harness.placement_publications, 2, "placement publications")
            .await;
        wait_for_feedback_to_settle().await;

        assert_eq!(harness.placement_publications.get(), 2);
        assert_eq!(harness.planner_calls.get(), 2);
        assert_eq!(harness.constraint_evaluations.get(), 2);
    }

    #[apply(async_test)]
    async fn scheduler_evaluator_replans_once_for_fresh_input_while_constraints_remain_false(
        executor: Rc<smol::LocalExecutor<'static>>,
    ) {
        let mut harness = start_scheduler_constraint_harness(executor);

        wait_for_counter_at_least(&harness.placement_publications, 2, "placement publications")
            .await;
        wait_for_feedback_to_settle().await;

        harness
            .planning_context
            .record_batch([(VarName::new("gate"), Value::Bool(true))]);
        harness
            .input_sender
            .send(vec![(harness.gate_input_index, Value::Bool(true))])
            .await
            .unwrap();

        wait_for_counter_at_least(&harness.placement_publications, 3, "placement publications")
            .await;
        wait_for_feedback_to_settle().await;

        assert_eq!(harness.placement_publications.get(), 3);
        assert_eq!(harness.planner_calls.get(), 3);
        assert_eq!(harness.constraint_evaluations.get(), 4);
    }
}
