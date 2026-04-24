use anyhow::Context;
use tracing::{debug, info};

use crate::cli::args::{Cli, OutputMode};
use crate::distributed::distribution_graphs::NodeName;
use crate::io::builders::output_handler_builder::OutputHandlerSpec;
use crate::{
    VarName, distributed::distribution_graphs::LabelledDistributionGraph,
    io::builders::InputProviderSpec, runtime::distributed::SchedulerCommunication,
};

use super::args::DistributionMode as CliDistributionMode;
use super::args::{DistributionMode, DistributionSolver, InputMode, SchedulingType};
use crate::core::interfaces::Runtime;
use crate::runtime::builder::DistributionMode as BuilderDistributionMode;

impl From<InputMode> for InputProviderSpec {
    fn from(input_mode: InputMode) -> Self {
        match input_mode {
            InputMode {
                input_file: Some(input_file),
                ..
            } => InputProviderSpec::File(input_file),
            InputMode {
                input_ros_file: Some(input_ros_file),
                ..
            } => {
                let json_string = std::fs::read_to_string(&input_ros_file)
                    .expect("Input mapping file could not be read");
                InputProviderSpec::Ros(json_string)
            }
            InputMode {
                input_mqtt_topics: Some(input_mqtt_topics),
                ..
            } => InputProviderSpec::MQTT(Some(input_mqtt_topics)),
            InputMode {
                input_redis_topics: Some(input_redis_topics),
                ..
            } => InputProviderSpec::Redis(Some(input_redis_topics)),
            InputMode {
                mqtt_input: true, ..
            } => InputProviderSpec::MQTT(None),
            InputMode {
                redis_input: true, ..
            } => InputProviderSpec::Redis(None),
            _ => panic!("Invalid input provider specification"),
        }
    }
}

impl From<OutputMode> for OutputHandlerSpec {
    fn from(output_mode: OutputMode) -> Self {
        match output_mode {
            OutputMode {
                output_stdout: true,
                ..
            } => OutputHandlerSpec::Stdout,
            OutputMode {
                output_ros_file: Some(output_ros_file),
                ..
            } => {
                let json_string = std::fs::read_to_string(&output_ros_file)
                    .expect("Output mapping file could not be read");
                OutputHandlerSpec::Ros(json_string)
            }
            OutputMode {
                output_mqtt_topics: Some(output_mqtt_topics),
                ..
            } => OutputHandlerSpec::MQTT(Some(output_mqtt_topics)),
            OutputMode {
                output_redis_topics: Some(output_redis_topics),
                ..
            } => OutputHandlerSpec::Redis(Some(output_redis_topics)),
            OutputMode {
                mqtt_output: true, ..
            } => OutputHandlerSpec::MQTT(None),
            OutputMode {
                redis_output: true, ..
            } => OutputHandlerSpec::Redis(None),
            // Default to stdout if no options provided
            _ => OutputHandlerSpec::Stdout,
        }
    }
}

impl Cli {
    pub fn scheduler_communication(&self) -> SchedulerCommunication {
        match self.scheduling_mode {
            SchedulingType::Mock => SchedulerCommunication::Null,
            SchedulingType::Ros => SchedulerCommunication::Ros {
                ros_node_name: self.scheduler_ros_node_name.clone(),
                reconf_topic: self.scheduler_reconf_topic.clone(),
            },
        }
    }
}

#[derive(Clone)]
pub struct DistributionModeBuilder {
    distribution_mode: CliDistributionMode,
    local_node: Option<NodeName>,
    dist_constraints: Option<Vec<VarName>>,
    mqtt_port: Option<u16>,
    ros_dist_graph_topic: String,
    runtime: Option<Runtime>,
    dist_constraint_solver: DistributionSolver,
}

impl DistributionModeBuilder {
    pub fn new(distribution_mode: CliDistributionMode) -> DistributionModeBuilder {
        DistributionModeBuilder {
            distribution_mode,
            local_node: None,
            dist_constraints: None,
            mqtt_port: None,
            ros_dist_graph_topic: "/dist_graph".to_string(),
            runtime: None,
            dist_constraint_solver: DistributionSolver::BruteForce,
        }
    }

    pub fn local_node(mut self, local_node: String) -> Self {
        self.local_node = Some(local_node.into());
        self
    }

    pub fn maybe_local_node(mut self, local_node: Option<String>) -> Self {
        self.local_node = local_node.map(|x| x.into());
        self
    }

    pub fn dist_constraints(mut self, constraints: Vec<String>) -> Self {
        self.dist_constraints = Some(constraints.into_iter().map(|x| x.into()).collect());
        self
    }

    pub fn maybe_dist_constraints(mut self, constraints: Option<Vec<String>>) -> Self {
        self.dist_constraints = constraints.map(|x| x.into_iter().map(|x| x.into()).collect());
        self
    }

    pub fn mqtt_port(mut self, mqtt_port: u16) -> Self {
        self.mqtt_port = Some(mqtt_port);
        self
    }

    pub fn maybe_mqtt_port(mut self, mqtt_port: Option<u16>) -> Self {
        self.mqtt_port = mqtt_port;
        self
    }

    pub fn ros_dist_graph_topic(mut self, ros_dist_graph_topic: String) -> Self {
        self.ros_dist_graph_topic = ros_dist_graph_topic;
        self
    }

    pub fn runtime(mut self, runtime: Runtime) -> Self {
        self.runtime = Some(runtime);
        self
    }

    pub fn dist_constraint_solver(mut self, dist_constraint_solver: DistributionSolver) -> Self {
        self.dist_constraint_solver = dist_constraint_solver;
        self
    }

    pub async fn build(self) -> anyhow::Result<BuilderDistributionMode> {
        let distribution_mode = self.distribution_mode.clone();

        let wants_dist_constraint_optimization_mode = matches!(
            distribution_mode,
            DistributionMode {
                mqtt_static_optimized: Some(_),
                ..
            } | DistributionMode {
                mqtt_dynamic_optimized: Some(_),
                ..
            } | DistributionMode {
                ros_static_optimized: Some(_),
                ..
            } | DistributionMode {
                ros_dynamic_optimized: Some(_),
                ..
            } | DistributionMode {
                distribution_graph: Some(_),
                ..
            }
        ) && self.dist_constraints.is_some();

        if matches!(self.dist_constraint_solver, DistributionSolver::Sat)
            && !wants_dist_constraint_optimization_mode
        {
            anyhow::bail!(
                "--dist-constraint-solver sat requires an optimized distribution mode with --distribution-constraints"
            );
        }

        Ok(match (distribution_mode, self.runtime) {
            (
                DistributionMode {
                    distribution_graph: Some(file_path),
                    ..
                },
                runtime,
            ) => {
                debug!("distribution graph mode");
                let f = smol::fs::read_to_string(&file_path)
                    .await
                    .context("Distribution graph file could not be read")?;
                let distribution_graph: LabelledDistributionGraph =
                    serde_json::from_str(&f).context("Distribution graph could not be parsed")?;

                if matches!(runtime, Some(Runtime::Distributed)) {
                    if let Some(dist_constraints) = self.dist_constraints {
                        match self.dist_constraint_solver {
                            DistributionSolver::BruteForce => {
                                BuilderDistributionMode::DistributedPredefinedOptimized(
                                    distribution_graph,
                                    dist_constraints,
                                )
                            }
                            DistributionSolver::Sat => {
                                BuilderDistributionMode::DistributedPredefinedOptimizedSat(
                                    distribution_graph,
                                    dist_constraints,
                                )
                            }
                        }
                    } else {
                        BuilderDistributionMode::DistributedPredefinedStatic(distribution_graph)
                    }
                } else {
                    let local_node = self.local_node.context(
                        "--distribution-graph requires --local-node unless --runtime distributed is used",
                    )?;
                    BuilderDistributionMode::LocalMonitor(Box::new((
                        local_node,
                        distribution_graph,
                    )))
                }
            }
            (
                DistributionMode {
                    local_topics: Some(topics),
                    ..
                },
                _,
            ) => {
                debug!("local node with topics: {:?}", topics);
                BuilderDistributionMode::LocalMonitor(Box::new(
                    topics
                        .into_iter()
                        .map(|v| v.into())
                        .collect::<Vec<VarName>>(),
                ))
            }
            (
                DistributionMode {
                    mqtt_centralised_distributed: Some(locations),
                    ..
                },
                _,
            ) => {
                debug!("setting up distributed centralised mode");
                BuilderDistributionMode::DistributedCentralised(locations)
            }
            (
                DistributionMode {
                    mqtt_randomized_distributed: Some(locations),
                    ..
                },
                _,
            ) => {
                debug!("setting up distributed random mode");
                BuilderDistributionMode::DistributedRandom(locations)
            }
            (
                DistributionMode {
                    mqtt_static_optimized: Some(locations),
                    ..
                },
                _,
            ) => {
                info!("setting up static optimization mode");
                let dist_constraints: Vec<VarName> = self
                    .dist_constraints
                    .context("Distribution constraints must be provided")?
                    .into_iter()
                    .map(|x| x.into())
                    .collect();
                match self.dist_constraint_solver {
                    DistributionSolver::BruteForce => {
                        BuilderDistributionMode::DistributedOptimizedStatic(
                            locations,
                            dist_constraints,
                        )
                    }
                    DistributionSolver::Sat => {
                        BuilderDistributionMode::DistributedOptimizedStaticSat(
                            locations,
                            dist_constraints,
                        )
                    }
                }
            }
            (
                DistributionMode {
                    mqtt_dynamic_optimized: Some(locations),
                    ..
                },
                _,
            ) => {
                info!("setting up dynamic optimization mode");
                let dist_constraints: Vec<VarName> = self
                    .dist_constraints
                    .context("Distribution constraints must be provided")?
                    .into_iter()
                    .map(|x| x.into())
                    .collect();
                match self.dist_constraint_solver {
                    DistributionSolver::BruteForce => {
                        BuilderDistributionMode::DistributedOptimizedDynamic(
                            locations,
                            dist_constraints,
                        )
                    }
                    DistributionSolver::Sat => {
                        BuilderDistributionMode::DistributedOptimizedDynamicSat(
                            locations,
                            dist_constraints,
                        )
                    }
                }
            }
            (
                DistributionMode {
                    ros_centralised_distributed: Some(_locations),
                    ..
                },
                _,
            ) => {
                #[cfg(not(feature = "ros"))]
                panic!("ROS distribution modes require building with feature 'ros'");
                #[cfg(feature = "ros")]
                {
                    debug!(
                        "setting up ROS distributed centralised mode using dist graph topic: {}",
                        self.ros_dist_graph_topic
                    );
                    BuilderDistributionMode::DistributedRosCentralised(
                        _locations,
                        self.ros_dist_graph_topic.clone(),
                    )
                }
            }
            (
                DistributionMode {
                    ros_randomized_distributed: Some(_locations),
                    ..
                },
                _,
            ) => {
                #[cfg(not(feature = "ros"))]
                panic!("ROS distribution modes require building with feature 'ros'");
                #[cfg(feature = "ros")]
                {
                    debug!(
                        "setting up ROS distributed random mode using dist graph topic: {}",
                        self.ros_dist_graph_topic
                    );
                    BuilderDistributionMode::DistributedRosRandom(
                        _locations,
                        self.ros_dist_graph_topic.clone(),
                    )
                }
            }
            (
                DistributionMode {
                    ros_static_optimized: Some(_locations),
                    ..
                },
                _,
            ) => {
                #[cfg(not(feature = "ros"))]
                panic!("ROS distribution modes require building with feature 'ros'");
                #[cfg(feature = "ros")]
                {
                    info!(
                        "setting up ROS static optimization mode using dist graph topic: {}",
                        self.ros_dist_graph_topic
                    );
                    let dist_constraints: Vec<VarName> = self
                        .dist_constraints
                        .context("Distribution constraints must be provided")?
                        .into_iter()
                        .map(|x| x.into())
                        .collect();
                    match self.dist_constraint_solver {
                        DistributionSolver::BruteForce => {
                            BuilderDistributionMode::DistributedRosOptimizedStatic(
                                _locations,
                                dist_constraints,
                                self.ros_dist_graph_topic.clone(),
                            )
                        }
                        DistributionSolver::Sat => {
                            BuilderDistributionMode::DistributedRosOptimizedStaticSat(
                                _locations,
                                dist_constraints,
                                self.ros_dist_graph_topic.clone(),
                            )
                        }
                    }
                }
            }
            (
                DistributionMode {
                    ros_dynamic_optimized: Some(_locations),
                    ..
                },
                _,
            ) => {
                #[cfg(not(feature = "ros"))]
                panic!("ROS distribution modes require building with feature 'ros'");
                #[cfg(feature = "ros")]
                {
                    info!(
                        "setting up ROS dynamic optimization mode using dist graph topic: {}",
                        self.ros_dist_graph_topic
                    );
                    let dist_constraints: Vec<VarName> = self
                        .dist_constraints
                        .context("Distribution constraints must be provided")?
                        .into_iter()
                        .map(|x| x.into())
                        .collect();
                    match self.dist_constraint_solver {
                        DistributionSolver::BruteForce => {
                            BuilderDistributionMode::DistributedRosOptimizedDynamic(
                                _locations,
                                dist_constraints,
                                self.ros_dist_graph_topic.clone(),
                            )
                        }
                        DistributionSolver::Sat => {
                            BuilderDistributionMode::DistributedRosOptimizedDynamicSat(
                                _locations,
                                dist_constraints,
                                self.ros_dist_graph_topic.clone(),
                            )
                        }
                    }
                }
            }

            (
                DistributionMode {
                    centralised: true, ..
                },
                _,
            ) => BuilderDistributionMode::CentralMonitor,
            _ => unreachable!(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "ros")]
    use crate::async_test;
    use crate::runtime::distributed::SchedulerCommunication;
    use clap::Parser;
    #[cfg(feature = "ros")]
    use macro_rules_attribute::apply;
    #[cfg(feature = "ros")]
    use smol::LocalExecutor;
    #[cfg(feature = "ros")]
    use std::rc::Rc;

    #[test]
    fn test_scheduler_communication_mock_mode_maps_to_null() {
        let cli = Cli::parse_from([
            "trustworthiness_checker",
            "model.dsrv",
            "--input-file",
            "input.txt",
            "--output-stdout",
            "--scheduling-mode",
            "mock",
        ]);

        assert!(matches!(
            cli.scheduler_communication(),
            SchedulerCommunication::Null
        ));
    }

    #[test]
    fn test_scheduler_communication_ros_mode_uses_default_values() {
        let cli = Cli::parse_from([
            "trustworthiness_checker",
            "model.dsrv",
            "--input-file",
            "input.txt",
            "--output-stdout",
            "--scheduling-mode",
            "ros",
        ]);

        assert_eq!(
            cli.scheduler_communication(),
            SchedulerCommunication::Ros {
                ros_node_name: "tc_scheduler".to_string(),
                reconf_topic: "reconfig".to_string(),
            }
        );
    }

    #[test]
    fn test_scheduler_communication_ros_mode_uses_explicit_values() {
        let cli = Cli::parse_from([
            "trustworthiness_checker",
            "model.dsrv",
            "--input-file",
            "input.txt",
            "--output-stdout",
            "--scheduling-mode",
            "ros",
            "--scheduler-ros-node-name",
            "custom_scheduler_node",
            "--scheduler-reconf-topic",
            "custom_reconfig_topic",
        ]);

        assert_eq!(
            cli.scheduler_communication(),
            SchedulerCommunication::Ros {
                ros_node_name: "custom_scheduler_node".to_string(),
                reconf_topic: "custom_reconfig_topic".to_string(),
            }
        );
    }

    #[cfg(feature = "ros")]
    #[apply(async_test)]
    async fn test_distribution_mode_builder_ros_centralised_maps_correctly(
        _executor: Rc<LocalExecutor<'static>>,
    ) {
        let distribution_mode = DistributionMode {
            centralised: false,
            distribution_graph: None,
            local_topics: None,
            mqtt_centralised_distributed: None,
            mqtt_randomized_distributed: None,
            mqtt_static_optimized: None,
            mqtt_dynamic_optimized: None,
            ros_centralised_distributed: Some(vec!["r1".to_string(), "r2".to_string()]),
            ros_randomized_distributed: None,
            ros_static_optimized: None,
            ros_dynamic_optimized: None,
            distributed_work: false,
        };

        let builder = DistributionModeBuilder::new(distribution_mode);
        let mode = builder.build().await.expect("build should succeed");

        assert!(matches!(
            mode,
            BuilderDistributionMode::DistributedRosCentralised(locs, topic)
                if locs == vec!["r1".to_string(), "r2".to_string()] && topic == "/dist_graph"
        ));
    }

    #[cfg(feature = "ros")]
    #[apply(async_test)]
    async fn test_distribution_mode_builder_ros_random_maps_correctly(
        _executor: Rc<LocalExecutor<'static>>,
    ) {
        let distribution_mode = DistributionMode {
            centralised: false,
            distribution_graph: None,
            local_topics: None,
            mqtt_centralised_distributed: None,
            mqtt_randomized_distributed: None,
            mqtt_static_optimized: None,
            mqtt_dynamic_optimized: None,
            ros_centralised_distributed: None,
            ros_randomized_distributed: Some(vec!["r1".to_string(), "r2".to_string()]),
            ros_static_optimized: None,
            ros_dynamic_optimized: None,
            distributed_work: false,
        };

        let builder = DistributionModeBuilder::new(distribution_mode);
        let mode = builder.build().await.expect("build should succeed");

        assert!(matches!(
            mode,
            BuilderDistributionMode::DistributedRosRandom(locs, topic)
                if locs == vec!["r1".to_string(), "r2".to_string()] && topic == "/dist_graph"
        ));
    }

    #[cfg(feature = "ros")]
    #[apply(async_test)]
    async fn test_distribution_mode_builder_ros_static_optimized_maps_correctly(
        _executor: Rc<LocalExecutor<'static>>,
    ) {
        let distribution_mode = DistributionMode {
            centralised: false,
            distribution_graph: None,
            local_topics: None,
            mqtt_centralised_distributed: None,
            mqtt_randomized_distributed: None,
            mqtt_static_optimized: None,
            mqtt_dynamic_optimized: None,
            ros_centralised_distributed: None,
            ros_randomized_distributed: None,
            ros_static_optimized: Some(vec!["r1".to_string(), "r2".to_string()]),
            ros_dynamic_optimized: None,
            distributed_work: false,
        };

        let builder = DistributionModeBuilder::new(distribution_mode)
            .dist_constraints(vec!["c1".to_string(), "c2".to_string()]);
        let mode = builder.build().await.expect("build should succeed");

        assert!(matches!(
            mode,
            BuilderDistributionMode::DistributedRosOptimizedStatic(locs, constraints, topic)
                if locs == vec!["r1".to_string(), "r2".to_string()]
                && constraints == vec![VarName::new("c1"), VarName::new("c2")]
                && topic == "/dist_graph"
        ));
    }

    #[cfg(feature = "ros")]
    #[apply(async_test)]
    async fn test_distribution_mode_builder_ros_dynamic_optimized_maps_correctly(
        _executor: Rc<LocalExecutor<'static>>,
    ) {
        let distribution_mode = DistributionMode {
            centralised: false,
            distribution_graph: None,
            local_topics: None,
            mqtt_centralised_distributed: None,
            mqtt_randomized_distributed: None,
            mqtt_static_optimized: None,
            mqtt_dynamic_optimized: None,
            ros_centralised_distributed: None,
            ros_randomized_distributed: None,
            ros_static_optimized: None,
            ros_dynamic_optimized: Some(vec!["r1".to_string(), "r2".to_string()]),
            distributed_work: false,
        };

        let builder = DistributionModeBuilder::new(distribution_mode)
            .dist_constraints(vec!["c1".to_string(), "c2".to_string()]);
        let mode = builder.build().await.expect("build should succeed");

        assert!(matches!(
            mode,
            BuilderDistributionMode::DistributedRosOptimizedDynamic(locs, constraints, topic)
                if locs == vec!["r1".to_string(), "r2".to_string()]
                && constraints == vec![VarName::new("c1"), VarName::new("c2")]
                && topic == "/dist_graph"
        ));
    }

    #[test]
    fn test_distribution_mode_builder_mqtt_static_optimized_sat_maps_correctly() {
        let distribution_mode = DistributionMode {
            centralised: false,
            distribution_graph: None,
            local_topics: None,
            mqtt_centralised_distributed: None,
            mqtt_randomized_distributed: None,
            mqtt_static_optimized: Some(vec!["n1".to_string(), "n2".to_string()]),
            mqtt_dynamic_optimized: None,
            ros_centralised_distributed: None,
            ros_randomized_distributed: None,
            ros_static_optimized: None,
            ros_dynamic_optimized: None,
            distributed_work: false,
        };

        let builder = DistributionModeBuilder::new(distribution_mode)
            .dist_constraint_solver(DistributionSolver::Sat)
            .dist_constraints(vec!["c1".to_string(), "c2".to_string()]);
        let mode = smol::block_on(builder.build()).expect("build should succeed");

        assert!(matches!(
            mode,
            BuilderDistributionMode::DistributedOptimizedStaticSat(locs, constraints)
                if locs == vec!["n1".to_string(), "n2".to_string()]
                && constraints == vec![VarName::new("c1"), VarName::new("c2")]
        ));
    }

    #[test]
    fn test_distribution_mode_builder_mqtt_dynamic_optimized_sat_maps_correctly() {
        let distribution_mode = DistributionMode {
            centralised: false,
            distribution_graph: None,
            local_topics: None,
            mqtt_centralised_distributed: None,
            mqtt_randomized_distributed: None,
            mqtt_static_optimized: None,
            mqtt_dynamic_optimized: Some(vec!["n1".to_string(), "n2".to_string()]),
            ros_centralised_distributed: None,
            ros_randomized_distributed: None,
            ros_static_optimized: None,
            ros_dynamic_optimized: None,
            distributed_work: false,
        };

        let builder = DistributionModeBuilder::new(distribution_mode)
            .dist_constraint_solver(DistributionSolver::Sat)
            .dist_constraints(vec!["c1".to_string(), "c2".to_string()]);
        let mode = smol::block_on(builder.build()).expect("build should succeed");

        assert!(matches!(
            mode,
            BuilderDistributionMode::DistributedOptimizedDynamicSat(locs, constraints)
                if locs == vec!["n1".to_string(), "n2".to_string()]
                && constraints == vec![VarName::new("c1"), VarName::new("c2")]
        ));
    }

    #[cfg(feature = "ros")]
    #[apply(async_test)]
    async fn test_distribution_mode_builder_ros_static_optimized_sat_maps_correctly(
        _executor: Rc<LocalExecutor<'static>>,
    ) {
        let distribution_mode = DistributionMode {
            centralised: false,
            distribution_graph: None,
            local_topics: None,
            mqtt_centralised_distributed: None,
            mqtt_randomized_distributed: None,
            mqtt_static_optimized: None,
            mqtt_dynamic_optimized: None,
            ros_centralised_distributed: None,
            ros_randomized_distributed: None,
            ros_static_optimized: Some(vec!["r1".to_string(), "r2".to_string()]),
            ros_dynamic_optimized: None,
            distributed_work: false,
        };

        let builder = DistributionModeBuilder::new(distribution_mode)
            .dist_constraint_solver(DistributionSolver::Sat)
            .dist_constraints(vec!["c1".to_string(), "c2".to_string()]);
        let mode = builder.build().await.expect("build should succeed");

        assert!(matches!(
            mode,
            BuilderDistributionMode::DistributedRosOptimizedStaticSat(locs, constraints, topic)
                if locs == vec!["r1".to_string(), "r2".to_string()]
                && constraints == vec![VarName::new("c1"), VarName::new("c2")]
                && topic == "/dist_graph"
        ));
    }

    #[cfg(feature = "ros")]
    #[apply(async_test)]
    async fn test_distribution_mode_builder_ros_dynamic_optimized_sat_maps_correctly(
        _executor: Rc<LocalExecutor<'static>>,
    ) {
        let distribution_mode = DistributionMode {
            centralised: false,
            distribution_graph: None,
            local_topics: None,
            mqtt_centralised_distributed: None,
            mqtt_randomized_distributed: None,
            mqtt_static_optimized: None,
            mqtt_dynamic_optimized: None,
            ros_centralised_distributed: None,
            ros_randomized_distributed: None,
            ros_static_optimized: None,
            ros_dynamic_optimized: Some(vec!["r1".to_string(), "r2".to_string()]),
            distributed_work: false,
        };

        let builder = DistributionModeBuilder::new(distribution_mode)
            .dist_constraint_solver(DistributionSolver::Sat)
            .dist_constraints(vec!["c1".to_string(), "c2".to_string()]);
        let mode = builder.build().await.expect("build should succeed");

        assert!(matches!(
            mode,
            BuilderDistributionMode::DistributedRosOptimizedDynamicSat(locs, constraints, topic)
                if locs == vec!["r1".to_string(), "r2".to_string()]
                && constraints == vec![VarName::new("c1"), VarName::new("c2")]
                && topic == "/dist_graph"
        ));
    }
}
