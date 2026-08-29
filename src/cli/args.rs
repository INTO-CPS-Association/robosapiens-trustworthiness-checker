use std::{
    num::{NonZeroU32, NonZeroUsize},
    path::PathBuf,
};

use clap::{ArgAction, Args, Parser, ValueEnum, builder::OsStr};
use strum_macros::Display;

use crate::core::{ExecutionPolicy, RuntimeSpec, Semantics};

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Display)]
#[strum(serialize_all = "kebab-case")]
pub enum DistributionSolver {
    BruteForce,
    Sat,
}

/// Specification languages supported for runtime verification
///
/// Different formal specification languages that can be used to define
/// monitoring properties and system behavior constraints.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Display)]
#[strum(serialize_all = "kebab-case")]
pub enum Language {
    /// DSRV runtime-verification language
    ///
    /// A stream-based specification language for runtime verification that supports
    /// temporal logic properties and dynamic spawning of new monitors
    DSRV,
    /// Signal Temporal Logic properties monitored by the MSTLO runtime
    MSTLO,
}

/// Runtime engines selectable for DSRV specifications. MSTLO specifications
/// select their dedicated runtime through `--language mstlo`.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Display)]
#[strum(serialize_all = "kebab-case")]
pub enum RuntimeKind {
    #[default]
    Async,
    Dataflow,
    Distributed,
    SemiSync,
    /// Independent semisynchronous reconfiguration reference runtime.
    ReconfSemiSync,
    /// Reconfigurable dataflow runtime.
    ReconfDataflow,
}

impl RuntimeKind {
    pub fn with_policy(self, policy: ExecutionPolicy) -> RuntimeSpec {
        match self {
            Self::Async => RuntimeSpec::Async,
            Self::Dataflow => RuntimeSpec::Dataflow(policy),
            Self::Distributed => RuntimeSpec::Distributed,
            Self::SemiSync => RuntimeSpec::SemiSync,
            Self::ReconfSemiSync => RuntimeSpec::ReconfSemiSync,
            Self::ReconfDataflow => RuntimeSpec::ReconfDataflow(policy),
        }
    }
}

const DSRV_TYPED_SEMANTICS: &[Semantics] = &[
    Semantics::Untimed,
    Semantics::TypedUntimed,
    Semantics::GradualTypedUntimed,
];
const DSRV_DISTRIBUTED_SEMANTICS: &[Semantics] = &[Semantics::Untimed];

fn supported_dsrv_semantics(runtime: RuntimeKind) -> &'static [Semantics] {
    match runtime {
        RuntimeKind::Distributed => DSRV_DISTRIBUTED_SEMANTICS,
        RuntimeKind::Async
        | RuntimeKind::Dataflow
        | RuntimeKind::SemiSync
        | RuntimeKind::ReconfSemiSync
        | RuntimeKind::ReconfDataflow => DSRV_TYPED_SEMANTICS,
    }
}

fn validate_dsrv_runtime_semantics(
    runtime: RuntimeKind,
    semantics: Semantics,
) -> anyhow::Result<()> {
    let supported = supported_dsrv_semantics(runtime);
    anyhow::ensure!(
        supported.contains(&semantics),
        "--runtime {runtime} does not support --semantics {semantics} for DSRV specifications; supports only: {}",
        supported
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    );
    Ok(())
}

pub fn resolve_runtime(
    language: Language,
    runtime: RuntimeKind,
    policy: ExecutionPolicy,
    runtime_was_explicit: bool,
) -> anyhow::Result<RuntimeSpec> {
    match language {
        Language::DSRV => {
            if policy == ExecutionPolicy::Synchronous
                && !matches!(runtime, RuntimeKind::Dataflow | RuntimeKind::ReconfDataflow)
            {
                anyhow::bail!(
                    "--execution-policy synchronous requires --runtime dataflow for DSRV specifications"
                );
            }
            Ok(runtime.with_policy(policy))
        }
        Language::MSTLO => {
            if runtime_was_explicit {
                anyhow::bail!("--runtime is selected by --language mstlo; omit --runtime");
            }
            Ok(RuntimeSpec::Mstlo(policy))
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Display)]
#[strum(serialize_all = "kebab-case")]
pub enum MstloAlgorithm {
    Naive,
    Incremental,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Display)]
#[strum(serialize_all = "kebab-case")]
pub enum MstloSynchronizationStrategy {
    None,
    ZeroOrderHold,
    Linear,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum, Display)]
#[strum(serialize_all = "kebab-case")]
pub enum InputWindowMode {
    Batch,
    AtomicStep,
}

/// Input source configuration for monitoring data
///
/// Specifies how the monitoring system should receive input data streams.
/// Exactly one input mode must be selected from the available options.
/// Supports file-based, MQTT, Redis, and ROS input sources.
#[derive(Args, Clone, Debug)]
#[group(required = true, multiple = false)]
pub struct InputMode {
    #[clap(long, help = "Path to input file containing trace data")]
    pub input_file: Option<String>,

    #[clap(long, help = "MQTT topics configuration file for input")]
    pub input_mqtt_file: Option<PathBuf>,

    #[clap(long, help = "Enable generic MQTT input mode")]
    pub mqtt_input: bool,

    #[clap(long, help = "Redis topics configuration file for input")]
    pub input_redis_file: Option<PathBuf>,

    #[clap(long, help = "Enable generic Redis Pub/Sub channel input mode")]
    pub redis_input: bool,

    #[clap(
        long,
        help = "Enable Redis knowledge-state input mode (selected keys, not Pub/Sub channels)"
    )]
    pub redis_knowledge_input: bool,

    // #[cfg(feature = "ros")]
    #[clap(
        long,
        help = "ROS topics configuration file for input (requires running with `--features ros`)"
    )]
    pub input_ros_file: Option<PathBuf>,

    #[clap(long, help = "Named multi-source input configuration file")]
    pub input_config: Option<PathBuf>,
}

/// Local output selection for monitoring results.
#[derive(Args, Clone, Debug)]
#[group(required = false, multiple = false)]
pub struct OutputSelection {
    #[clap(long, help = "Output monitoring results to stdout")]
    pub output_stdout: bool,

    #[clap(long, help = "Enable generic MQTT output mode")]
    pub mqtt_output: bool,

    #[clap(long, help = "MQTT topics configuration file for output")]
    pub output_mqtt_file: Option<PathBuf>,

    #[clap(long, help = "Enable generic Redis output mode")]
    pub redis_output: bool,

    #[clap(long, help = "Redis channels configuration file for output")]
    pub output_redis_file: Option<PathBuf>,

    #[clap(long, help = "ROS topics configuration file for output")]
    pub output_ros_file: Option<PathBuf>,

    #[clap(long, help = "Advanced multi-destination output configuration file")]
    pub output_config: Option<PathBuf>,
}

/// Distribution and deployment configuration for monitoring
///
/// Controls how monitoring is distributed across multiple nodes.
/// Supports centralized monitoring (default) as well as various distributed
/// strategies including MQTT-based coordination and optimization algorithms.
#[derive(Args, Clone, Debug)]
#[group(required = false, multiple = false)]
pub struct DistributionMode {
    #[clap(
        long,
        default_value_t = true,
        help = "Run monitoring in centralised mode (default)"
    )]
    pub centralised: bool,

    #[clap(
        long,
        help = "Path to distribution graph JSON5 file for local monitoring"
    )]
    pub distribution_graph: Option<String>,

    #[clap(long, help = "List of local topics to monitor in distributed mode")]
    pub local_topics: Option<Vec<String>>,

    #[clap(long, value_delimiter = ' ', num_args = 1.., help = "Node locations for MQTT-based centralised distributed monitoring")]
    pub mqtt_centralised_distributed: Option<Vec<String>>,

    #[clap(long, value_delimiter = ' ', num_args = 1.., help = "Node locations for MQTT-based randomized distributed monitoring")]
    pub mqtt_randomized_distributed: Option<Vec<String>>,

    #[clap(long, value_delimiter = ' ', num_args = 1.., help = "Node locations for MQTT-based static optimized distributed monitoring")]
    #[clap(requires = "distribution_constraints")]
    pub mqtt_static_optimized: Option<Vec<String>>,

    #[clap(long, value_delimiter = ' ', num_args = 1.., help = "Node locations for MQTT-based dynamic optimized distributed monitoring")]
    #[clap(requires = "distribution_constraints")]
    pub mqtt_dynamic_optimized: Option<Vec<String>>,

    #[clap(long, value_delimiter = ' ', num_args = 1.., help = "Node locations for ROS-based centralised distributed monitoring")]
    pub ros_centralised_distributed: Option<Vec<String>>,

    #[clap(long, value_delimiter = ' ', num_args = 1.., help = "Node locations for ROS-based randomized distributed monitoring")]
    pub ros_randomized_distributed: Option<Vec<String>>,

    #[clap(long, value_delimiter = ' ', num_args = 1.., help = "Node locations for ROS-based static optimized distributed monitoring")]
    #[clap(requires = "distribution_constraints")]
    pub ros_static_optimized: Option<Vec<String>>,

    #[clap(long, value_delimiter = ' ', num_args = 1.., help = "Node locations for ROS-based dynamic optimized distributed monitoring")]
    #[clap(requires = "distribution_constraints")]
    pub ros_dynamic_optimized: Option<Vec<String>>,

    #[clap(
        long,
        help = "Wait for work assignment from scheduler in distributed mode"
    )]
    #[clap(requires = "local_node")]
    pub distributed_work: bool,
}

/// Scheduling strategies for distributed monitoring coordination
///
/// Different approaches to coordinate work distribution across multiple
/// monitoring nodes in a distributed system.
#[derive(ValueEnum, Debug, Clone)]
pub enum SchedulingType {
    /// Mock scheduler implementation for testing and development
    ///
    /// Provides a simple, predictable scheduling behavior primarily
    /// used for testing and single-node deployments.
    Mock,
    /// ROS-topic--based distributed scheduler for production environments
    ///
    /// Uses ROS topic messaging for real-time coordination between
    /// monitoring nodes, enabling dynamic work distribution.
    Ros,
}

impl Into<&'static str> for SchedulingType {
    fn into(self) -> &'static str {
        match self {
            SchedulingType::Mock => "mock",
            SchedulingType::Ros => "ros",
        }
    }
}

impl Into<String> for SchedulingType {
    fn into(self) -> String {
        match self {
            SchedulingType::Mock => "mock".to_string(),
            SchedulingType::Ros => "ros".to_string(),
        }
    }
}

impl Into<OsStr> for SchedulingType {
    fn into(self) -> OsStr {
        match self {
            SchedulingType::Mock => (&"mock").into(),
            SchedulingType::Ros => (&"ros").into(),
        }
    }
}

/// Trustworthiness Checker - A runtime verification tool for distributed systems
///
/// This tool monitors system behavior against formal specifications written in DSRV,
/// supporting both centralized and distributed monitoring modes with various input/output
/// mechanisms including MQTT, Redis, ROS, and file-based sources.
#[derive(Parser, Clone, Debug)]
#[command(name = "trustworthiness-checker")]
#[command(about = "A runtime verification tool for distributed systems")]
#[command(
    long_about = "Trustworthiness Checker monitors system behavior against formal specifications. It supports centralized and distributed monitoring with MQTT, Redis, ROS, and file-based inputs/outputs."
)]
pub struct Cli {
    #[arg(help = "Path to the model specification file")]
    pub model: String,

    // The mode of input to use
    #[command(flatten)]
    pub input_mode: InputMode,

    // The mode of output to use
    #[command(flatten)]
    pub output_selection: OutputSelection,

    #[arg(long, help = "Write tracing logs to this file")]
    pub log_file: Option<String>,

    #[arg(long, help = "Specification language to use", default_value_t = Language::DSRV)]
    pub language: Language,
    #[arg(long, help = "Semantics engine to use for monitoring", default_value_t = Semantics::GradualTypedUntimed)]
    pub semantics: Semantics,
    #[arg(long, help = "DSRV runtime system to use for execution", default_value_t = RuntimeKind::Async)]
    pub runtime: RuntimeKind,

    #[arg(
        long,
        help = "Input execution policy for Dataflow and MSTLO runtimes",
        default_value_t = ExecutionPolicy::Buffered
    )]
    pub execution_policy: ExecutionPolicy,

    #[arg(long, help = "MSTLO monitor algorithm", default_value_t = MstloAlgorithm::Incremental)]
    pub mstlo_algorithm: MstloAlgorithm,

    #[arg(long, help = "MSTLO multi-signal synchronization strategy", default_value_t = MstloSynchronizationStrategy::ZeroOrderHold)]
    pub mstlo_synchronization: MstloSynchronizationStrategy,

    #[arg(long, value_delimiter = ' ', num_args = 1.., help = "MSTLO variable bindings as name=value pairs")]
    pub mstlo_vars: Option<Vec<String>>,

    #[command(flatten)]
    pub distribution_mode: DistributionMode,

    #[arg(long, help = "Identifier for this node in distributed monitoring")]
    pub local_node: Option<String>,

    #[arg(long, default_value = SchedulingType::Mock, help = "Scheduling mode for distributed coordination")]
    pub scheduling_mode: SchedulingType,

    #[clap(long, value_delimiter = ' ', num_args = 1.., help = "Distribution constraints for optimized scheduling")]
    pub distribution_constraints: Option<Vec<String>>,

    #[arg(
        long,
        default_value_t = DistributionSolver::BruteForce,
        help = "Solver used for distributed optimized scheduling"
    )]
    pub dist_constraint_solver: DistributionSolver,

    #[arg(
        long,
        help = "ROS node name to use for scheduler communicator (used with --scheduling-mode ros)",
        default_value = "tc_scheduler"
    )]
    pub scheduler_ros_node_name: String,

    #[arg(
        long,
        help = "Base ROS topic for scheduler work/reconfiguration messages (used with --scheduling-mode ros)",
        default_value = "reconfig",
        requires = "scheduler_ros_node_name"
    )]
    pub scheduler_reconf_topic: String,

    #[arg(long, help = "Port number for MQTT broker connection")]
    pub mqtt_port: Option<u16>,

    #[arg(
        long = "mqtt-paho",
        conflicts_with = "mqtt_rumqttc",
        help = "Use the legacy Paho MQTT input backend"
    )]
    pub mqtt_paho: bool,

    #[arg(
        long = "mqtt-rumqttc",
        conflicts_with = "mqtt_paho",
        help = "Use the rumqttc MQTT input backend (default)"
    )]
    pub mqtt_rumqttc: bool,

    #[arg(long, help = "Port number for Redis server connection")]
    pub redis_port: Option<u16>,

    #[arg(
        long = "redis-knowledge-key",
        value_name = "INPUT=KEY",
        action = ArgAction::Append,
        help = "Map a checker input variable to a Redis knowledge key; repeatable"
    )]
    pub redis_knowledge_keys: Vec<String>,

    #[arg(
        long,
        help = "Override the targeted Redis knowledge source database (default: 2)"
    )]
    pub redis_knowledge_database: Option<u32>,

    #[arg(
        long = "redis-knowledge-publish-initial",
        value_name = "BOOL",
        action = ArgAction::Set,
        conflicts_with = "redis_knowledge_no_initial",
        help = "Override whether Redis knowledge emits its initial snapshot into the checker input stream (true or false); this does not publish a Redis Pub/Sub message"
    )]
    pub redis_knowledge_publish_initial: Option<bool>,

    #[arg(
        long = "redis-knowledge-no-initial",
        conflicts_with = "redis_knowledge_publish_initial",
        help = "Disable the Redis knowledge initial snapshot"
    )]
    pub redis_knowledge_no_initial: bool,

    #[arg(
        long,
        value_name = "ATTEMPTS",
        conflicts_with = "redis_knowledge_retry_forever",
        help = "Maximum Redis knowledge connection attempts, including the initial attempt"
    )]
    pub redis_knowledge_retry_max_attempts: Option<NonZeroU32>,

    #[arg(
        long = "redis-knowledge-retry-forever",
        conflicts_with = "redis_knowledge_retry_max_attempts",
        help = "Retry Redis knowledge transport failures forever"
    )]
    pub redis_knowledge_retry_forever: bool,

    #[arg(
        long,
        value_name = "MILLISECONDS",
        help = "Initial Redis knowledge retry backoff delay"
    )]
    pub redis_knowledge_retry_initial_delay_ms: Option<u64>,

    #[arg(
        long,
        value_name = "MILLISECONDS",
        help = "Maximum Redis knowledge retry backoff delay"
    )]
    pub redis_knowledge_retry_max_delay_ms: Option<u64>,

    #[arg(
        long,
        value_name = "SOURCE_ID",
        help = "Target this Redis knowledge source when --input-config declares more than one"
    )]
    pub redis_knowledge_source: Option<String>,

    #[arg(long, help = "Maximum input window delay in milliseconds")]
    pub input_window_ms: Option<u64>,

    #[arg(
        long,
        value_enum,
        help = "Input window semantics: batch preserves ticks; atomic-step applies last-update-wins"
    )]
    pub input_window_mode: Option<InputWindowMode>,

    #[arg(
        long,
        help = "Input-window flush threshold in variable updates; a logical tick is never split"
    )]
    pub input_window_update_limit: Option<NonZeroUsize>,

    #[arg(
        long,
        help = "Override the selected source's reconfiguration route; otherwise use its configured route or `reconf`"
    )]
    pub reconf_topic: Option<String>,

    #[arg(
        long = "no-context-transfer",
        default_value_t = false,
        help = "Disable context transfer between old and new runtimes during reconfiguration"
    )]
    pub no_context_transfer: bool,

    #[arg(
        long,
        help = "Topic name used by ROS distribution graph provider",
        default_value = "/dist_graph"
    )]
    pub ros_dist_graph_topic: String,
}

impl Cli {
    pub fn validate(&self) -> anyhow::Result<()> {
        let reconfigurable = matches!(
            self.runtime,
            RuntimeKind::ReconfSemiSync | RuntimeKind::ReconfDataflow
        );
        anyhow::ensure!(
            reconfigurable || (!self.no_context_transfer && self.reconf_topic.is_none()),
            "monitor reconfiguration flags require --runtime reconf-semi-sync or --runtime reconf-dataflow"
        );
        if let Some(topic) = &self.reconf_topic {
            anyhow::ensure!(!topic.trim().is_empty(), "--reconf-topic cannot be empty");
        }
        if matches!(self.language, Language::DSRV) {
            validate_dsrv_runtime_semantics(self.runtime, self.semantics)?;
        }
        if reconfigurable {
            anyhow::ensure!(
                self.input_mode.input_file.is_none(),
                "--input-file cannot be used with --runtime {}",
                self.runtime
            );
        }

        let has_redis_knowledge_override = !self.redis_knowledge_keys.is_empty()
            || self.redis_knowledge_database.is_some()
            || self.redis_knowledge_publish_initial.is_some()
            || self.redis_knowledge_no_initial
            || self.redis_knowledge_retry_max_attempts.is_some()
            || self.redis_knowledge_retry_forever
            || self.redis_knowledge_retry_initial_delay_ms.is_some()
            || self.redis_knowledge_retry_max_delay_ms.is_some()
            || self.redis_knowledge_source.is_some();
        anyhow::ensure!(
            !has_redis_knowledge_override
                || self.input_mode.redis_knowledge_input
                || self.input_mode.input_config.is_some(),
            "Redis knowledge options require --redis-knowledge-input or --input-config"
        );
        anyhow::ensure!(
            self.redis_knowledge_source.is_none() || self.input_mode.input_config.is_some(),
            "--redis-knowledge-source requires --input-config"
        );
        anyhow::ensure!(
            !(matches!(self.language, Language::MSTLO) && self.input_mode.redis_knowledge_input),
            "Redis knowledge input produces ordinary `Value` input and is unsupported for MSTLO"
        );
        anyhow::ensure!(
            !(self.redis_knowledge_retry_forever
                && self.redis_knowledge_retry_max_attempts.is_some()),
            "--redis-knowledge-retry-forever conflicts with --redis-knowledge-retry-max-attempts"
        );
        if let (Some(initial), Some(maximum)) = (
            self.redis_knowledge_retry_initial_delay_ms,
            self.redis_knowledge_retry_max_delay_ms,
        ) {
            anyhow::ensure!(
                initial > 0 && maximum > 0 && maximum >= initial,
                "Redis knowledge retry delays must be positive and max delay must be at least initial delay"
            );
        }
        if self.redis_knowledge_no_initial {
            anyhow::ensure!(
                self.redis_knowledge_publish_initial.is_none(),
                "--redis-knowledge-no-initial conflicts with --redis-knowledge-publish-initial"
            );
        }

        if self.input_window_mode.is_some()
            && self.input_window_ms.is_none()
            && self.input_window_update_limit.is_none()
        {
            anyhow::bail!(
                "--input-window-mode requires --input-window-ms or --input-window-update-limit"
            );
        }
        if matches!(self.input_window_mode, Some(InputWindowMode::AtomicStep))
            && self.input_mode.input_file.is_some()
            && self.input_window_update_limit.is_none()
        {
            anyhow::bail!(
                "atomic-step windows for --input-file require --input-window-update-limit"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod runtime_tests {
    use super::*;

    #[test]
    fn dataflow_runtime_carries_its_execution_policy() {
        assert_eq!(
            RuntimeKind::Dataflow.with_policy(ExecutionPolicy::Synchronous),
            RuntimeSpec::Dataflow(ExecutionPolicy::Synchronous),
        );
    }

    #[test]
    fn reconfigurable_runtime_names_select_independent_implementations() {
        assert_eq!(
            RuntimeKind::ReconfSemiSync.with_policy(ExecutionPolicy::Buffered),
            RuntimeSpec::ReconfSemiSync,
        );
        assert_eq!(
            RuntimeKind::ReconfDataflow.with_policy(ExecutionPolicy::Synchronous),
            RuntimeSpec::ReconfDataflow(ExecutionPolicy::Synchronous),
        );
    }

    #[test]
    fn reconfigurable_flags_are_accepted_by_both_runtimes() {
        for runtime in ["reconf-semi-sync", "reconf-dataflow"] {
            let cli = Cli::try_parse_from([
                "trustworthiness_checker",
                "model.dsrv",
                "--mqtt-input",
                "--output-stdout",
                "--runtime",
                runtime,
                "--reconf-topic",
                "control",
                "--no-context-transfer",
            ])
            .unwrap();
            cli.validate().unwrap();
        }
    }

    #[test]
    fn every_dsrv_runtime_semantics_pair_matches_the_builder_matrix() {
        const ALL_SEMANTICS: [Semantics; 7] = [
            Semantics::Untimed,
            Semantics::TypedUntimed,
            Semantics::GradualTypedUntimed,
            Semantics::DelayedQuantitative,
            Semantics::DelayedQualitative,
            Semantics::EagerQualitative,
            Semantics::RobustnessInterval,
        ];
        let cases = [
            (
                RuntimeKind::Async,
                [true, true, true, false, false, false, false],
            ),
            (
                RuntimeKind::Dataflow,
                [true, true, true, false, false, false, false],
            ),
            (
                RuntimeKind::Distributed,
                [true, false, false, false, false, false, false],
            ),
            (
                RuntimeKind::SemiSync,
                [true, true, true, false, false, false, false],
            ),
            (
                RuntimeKind::ReconfSemiSync,
                [true, true, true, false, false, false, false],
            ),
            (
                RuntimeKind::ReconfDataflow,
                [true, true, true, false, false, false, false],
            ),
        ];

        for (runtime, expected) in cases {
            for (semantics, should_accept) in ALL_SEMANTICS.iter().copied().zip(expected) {
                let cli = Cli::try_parse_from([
                    "trustworthiness_checker".to_owned(),
                    "model.dsrv".to_owned(),
                    "--mqtt-input".to_owned(),
                    "--output-stdout".to_owned(),
                    "--runtime".to_owned(),
                    runtime.to_string(),
                    "--semantics".to_owned(),
                    semantics.to_string(),
                ])
                .unwrap();
                assert_eq!(
                    cli.validate().is_ok(),
                    should_accept,
                    "unexpected DSRV validation result for {runtime:?} + {semantics:?}"
                );
            }
        }
    }

    #[test]
    fn mstlo_keeps_all_semantics_available() {
        const ALL_SEMANTICS: [Semantics; 7] = [
            Semantics::Untimed,
            Semantics::TypedUntimed,
            Semantics::GradualTypedUntimed,
            Semantics::DelayedQuantitative,
            Semantics::DelayedQualitative,
            Semantics::EagerQualitative,
            Semantics::RobustnessInterval,
        ];

        for semantics in ALL_SEMANTICS {
            let cli = Cli::try_parse_from([
                "trustworthiness_checker",
                "model.mstlo",
                "--mqtt-input",
                "--output-stdout",
                "--language",
                "mstlo",
                "--semantics",
                semantics.to_string().as_str(),
            ])
            .unwrap();
            cli.validate().unwrap();
        }
    }

    #[test]
    fn both_reconfigurable_runtimes_reject_file_input_without_panicking() {
        for runtime in ["reconf-semi-sync", "reconf-dataflow"] {
            let cli = Cli::try_parse_from([
                "trustworthiness_checker",
                "model.dsrv",
                "--input-file",
                "trace.input",
                "--output-stdout",
                "--runtime",
                runtime,
            ])
            .unwrap();
            let error = cli.validate().unwrap_err();
            assert!(error.to_string().contains("--input-file cannot be used"));
        }
    }

    #[test]
    fn runtimes_without_a_completion_boundary_do_not_carry_policy_state() {
        assert_eq!(
            RuntimeKind::Async.with_policy(ExecutionPolicy::Buffered),
            RuntimeSpec::Async,
        );
        assert_eq!(
            RuntimeKind::SemiSync.with_policy(ExecutionPolicy::Buffered),
            RuntimeSpec::SemiSync,
        );
    }

    #[test]
    fn runtime_resolution_rejects_unsupported_synchronous_execution() {
        assert!(
            resolve_runtime(
                Language::DSRV,
                RuntimeKind::Async,
                ExecutionPolicy::Synchronous,
                false,
            )
            .is_err()
        );
    }

    #[test]
    fn output_config_is_parsed_as_a_distinct_output_selection() {
        let cli = Cli::try_parse_from([
            "trustworthiness_checker",
            "checker.dsrv",
            "--input-file",
            "trace.json5",
            "--output-config",
            "outputs.json5",
        ])
        .unwrap();

        assert_eq!(
            cli.output_selection.output_config,
            Some(PathBuf::from("outputs.json5"))
        );
        assert!(!cli.output_selection.output_stdout);
    }

    #[test]
    fn output_config_conflicts_with_shortcut_output_modes() {
        for mode in [
            vec!["--output-stdout"],
            vec!["--mqtt-output"],
            vec!["--output-mqtt-file", "routes.json5"],
            vec!["--redis-output"],
            vec!["--output-redis-file", "routes.json5"],
            vec!["--output-ros-file", "routes.json5"],
        ] {
            let mut arguments = vec![
                "trustworthiness_checker",
                "checker.dsrv",
                "--input-file",
                "trace.json5",
                "--output-config",
                "outputs.json5",
            ];
            arguments.extend(mode.iter().copied());
            let result = Cli::try_parse_from(arguments);
            assert!(result.is_err(), "accepted output config with {mode:?}");
        }
    }

    #[test]
    fn mstlo_rejects_simple_redis_knowledge_before_source_opening() {
        let cli = Cli::try_parse_from([
            "trustworthiness_checker",
            "checker.mstlo",
            "--language",
            "mstlo",
            "--redis-knowledge-input",
            "--redis-knowledge-key",
            "signal=robot:signal",
            "--output-stdout",
        ])
        .unwrap();
        let error = cli.validate().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("ordinary `Value` input and is unsupported for MSTLO")
        );
    }

    #[test]
    fn runtime_resolution_selects_policy_bearing_mstlo() {
        assert_eq!(
            resolve_runtime(
                Language::MSTLO,
                RuntimeKind::Async,
                ExecutionPolicy::Synchronous,
                false,
            )
            .unwrap(),
            RuntimeSpec::Mstlo(ExecutionPolicy::Synchronous),
        );
    }
}
