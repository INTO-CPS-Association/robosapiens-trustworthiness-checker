use std::{num::NonZeroUsize, path::PathBuf};

use clap::{Args, Parser, ValueEnum, builder::OsStr};
use strum_macros::Display;

use crate::core::{RuntimeSpec, Semantics};

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

/// Parser implementation strategies for specification parsing
///
/// Different parsing approaches available for processing specification files,
/// each with different performance and feature characteristics.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Display)]
#[strum(serialize_all = "kebab-case")]
pub enum ParserMode {
    /// Parser combinator implementation using the Winnow library
    ///
    /// Provides flexible, composable parsing with good error messages.
    Combinator,
    /// LALR(1) parser implementation using lalrpop
    ///
    /// Generates efficient parsers from grammar definitions.
    /// Recommended for most use cases.
    Lalr,
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
pub enum InputAggregationMode {
    PreserveTicks,
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

    #[clap(long, help = "Enable generic Redis input mode")]
    pub redis_input: bool,

    // #[cfg(feature = "ros")]
    #[clap(
        long,
        help = "ROS topics configuration file for input (requires running with `--features ros`)"
    )]
    pub input_ros_file: Option<String>,
}

/// Output handler configuration for monitoring results
///
/// Specifies either to use a given mode of output (Files, ROS, MQTT, Redis) or to select the
/// output mode an provide an additional configuration file at the same time. This configuration
/// file is mandatory for ROS since the message types need to be specified, or optional for all
/// other protocols (with the default behaviour being to directly use the stream variable
/// names as topic names).
#[derive(Args, Clone, Debug)]
#[group(required = false, multiple = false)]
pub struct OutputMode {
    #[clap(long, help = "Output monitoring results to stdout")]
    pub output_stdout: bool,

    #[clap(long, help = "Enable generic MQTT output mode")]
    pub mqtt_output: bool,

    #[clap(long, help = "MQTT topics configuration file for output")]
    pub output_mqtt_file: Option<PathBuf>,

    #[clap(long, help = "Enable generic Redis output mode")]
    pub redis_output: bool,

    #[clap(long, help = "ROS topics configuration file for output")]
    pub output_redis_file: Option<PathBuf>,

    #[clap(long, help = "ROS topics configuration file for output")]
    pub output_ros_file: Option<PathBuf>,
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
        help = "Path to distribution graph JSON file for local monitoring"
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
    pub output_mode: OutputMode,

    #[arg(long, help = "Write tracing logs to this file")]
    pub log_file: Option<String>,

    #[arg(long, help = "Parser mode to use for model parsing", default_value_t = ParserMode::Lalr)]
    pub parser: ParserMode,
    #[arg(long, help = "Specification language to use", default_value_t = Language::DSRV)]
    pub language: Language,
    #[arg(long, help = "Semantics engine to use for monitoring", default_value_t = Semantics::GradualTypedUntimed)]
    pub semantics: Semantics,
    #[arg(long, help = "Runtime system to use for execution", default_value_t = RuntimeSpec::Async)]
    pub runtime: RuntimeSpec,

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

    #[arg(long, help = "Port number for Redis server connection")]
    pub redis_port: Option<u16>,

    #[arg(
        long,
        help = "Maximum aggregation delay for independent-event inputs, in milliseconds"
    )]
    pub input_aggregation_delay_ms: Option<u64>,

    #[arg(
        long,
        value_enum,
        requires = "input_aggregation_delay_ms",
        help = "Whether aggregated events preserve logical ticks or form one atomic tick"
    )]
    pub input_aggregation_mode: Option<InputAggregationMode>,

    #[arg(
        long,
        requires = "input_aggregation_delay_ms",
        help = "Optional raw-event limit that emits an input aggregation early"
    )]
    pub input_aggregation_event_limit: Option<NonZeroUsize>,

    #[arg(
        long,
        help = "Topic name for reconfiguration when using reconfiguration runtime",
        default_value = "reconfig"
    )]
    pub reconf_topic: String,

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
