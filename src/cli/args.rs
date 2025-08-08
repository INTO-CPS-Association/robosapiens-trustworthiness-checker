use clap::{Args, Parser, ValueEnum, builder::OsStr};

use crate::core::{Runtime, Semantics};

/// Specification languages supported for runtime verification
///
/// Different formal specification languages that can be used to define
/// monitoring properties and system behavior constraints.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum Language {
    /// DynSRV runtime-verification language
    ///
    /// A stream-based specification language for runtime verification that supports
    /// temporal logic properties and dynamic spawning of new monitors
    #[value(name = "dynsrv")]
    DynSRV,
    /// LOLA: a synonym for DynSRV for legacy compatibility
    Lola,
}

/// Parser implementation strategies for specification parsing
///
/// Different parsing approaches available for processing specification files,
/// each with different performance and feature characteristics.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum ParserMode {
    /// Parser combinator implementation using the Winnow library
    ///
    /// Provides flexible, composable parsing with good error messages.
    /// Recommended for most use cases.
    Combinator,
    /// LALR(1) parser implementation using lalrpop
    ///
    /// Generates efficient parsers from grammar definitions.
    /// Currently not implemented.
    Lalr,
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

    #[clap(long, value_delimiter = ' ', num_args = 1.., help = "List of MQTT topics to subscribe to for input")]
    pub input_mqtt_topics: Option<Vec<String>>,

    #[clap(long, value_delimiter = ' ', num_args = 1.., help = "List of MQTT topics with custom variable mapping for input")]
    pub input_map_mqtt_topics: Option<Vec<String>>,

    #[clap(long, help = "Enable generic MQTT input mode")]
    pub mqtt_input: bool,

    #[clap(long, value_delimiter = ' ', num_args = 1.., help = "List of Redis channels to subscribe to for input")]
    pub input_redis_topics: Option<Vec<String>>,

    #[clap(long, help = "Enable generic Redis input mode")]
    pub redis_input: bool,

    // #[cfg(feature = "ros")]
    #[clap(long, help = "ROS topics configuration file for input")]
    pub input_ros_topics: Option<String>,
}

/// Output handler configuration for monitoring results
#[derive(Args, Clone)]
#[group(required = false, multiple = false)]
pub struct OutputMode {
    #[clap(long, help = "Output monitoring results to stdout")]
    pub output_stdout: bool,

    #[clap(long, value_delimiter = ' ', num_args = 1.., help = "List of MQTT topics to publish monitoring results to")]
    pub output_mqtt_topics: Option<Vec<String>>,

    #[clap(long, help = "Enable generic MQTT output mode")]
    pub mqtt_output: bool,

    #[clap(long, value_delimiter = ' ', num_args = 1.., help = "List of Redis channels to publish monitoring results to")]
    pub output_redis_topics: Option<Vec<String>>,

    #[clap(long, help = "Enable generic Redis output mode")]
    pub redis_output: bool,

    // #[cfg(feature = "ros")]
    // TODO: Implement ROS output support
    #[clap(long, help = "ROS topics configuration file for output")]
    pub output_ros_topics: Option<String>,
}

/// Distribution and deployment configuration for monitoring
///
/// Controls how monitoring is distributed across multiple nodes.
/// Supports centralized monitoring (default) as well as various distributed
/// strategies including MQTT-based coordination and optimization algorithms.
#[derive(Args, Clone)]
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
    #[clap(requires = "local_node")]
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
    /// MQTT-based distributed scheduler for production environments
    ///
    /// Uses MQTT messaging for real-time coordination between monitoring
    /// nodes, enabling dynamic work distribution and load balancing.
    Mqtt,
}

impl Into<&'static str> for SchedulingType {
    fn into(self) -> &'static str {
        match self {
            SchedulingType::Mock => "mock",
            SchedulingType::Mqtt => "mqtt",
        }
    }
}

impl Into<String> for SchedulingType {
    fn into(self) -> String {
        match self {
            SchedulingType::Mock => "mock".to_string(),
            SchedulingType::Mqtt => "mqtt".to_string(),
        }
    }
}

impl Into<OsStr> for SchedulingType {
    fn into(self) -> OsStr {
        match self {
            SchedulingType::Mock => (&"mock").into(),
            SchedulingType::Mqtt => (&"mqtt").into(),
        }
    }
}

/// Trustworthiness Checker - A runtime verification tool for distributed systems
///
/// This tool monitors system behavior against formal specifications written in LOLA,
/// supporting both centralized and distributed monitoring modes with various input/output
/// mechanisms including MQTT, Redis, ROS, and file-based sources.
#[derive(Parser, Clone)]
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

    #[arg(long, help = "Parser mode to use for model parsing")]
    pub parser_mode: Option<ParserMode>,
    #[arg(long, help = "Specification language to use")]
    pub language: Option<Language>,
    #[arg(long, help = "Semantics engine to use for monitoring")]
    pub semantics: Option<Semantics>,
    #[arg(long, help = "Runtime system to use for execution")]
    pub runtime: Option<Runtime>,

    #[command(flatten)]
    pub distribution_mode: DistributionMode,

    #[arg(long, help = "Identifier for this node in distributed monitoring")]
    pub local_node: Option<String>,

    #[arg(long, default_value = SchedulingType::Mock, help = "Scheduling mode for distributed coordination")]
    pub scheduling_mode: SchedulingType,

    #[clap(long, value_delimiter = ' ', num_args = 1.., help = "Distribution constraints for optimized scheduling")]
    pub distribution_constraints: Option<Vec<String>>,

    #[arg(long, help = "Port number for MQTT broker connection")]
    pub mqtt_port: Option<u16>,

    #[arg(long, help = "Port number for Redis server connection")]
    pub redis_port: Option<u16>,
}

/// ROS-specific Trustworthiness Checker configuration
///
/// Specialized version for Robot Operating System (ROS) environments with
/// ROS topic mapping and integration capabilities.
#[derive(Parser)]
#[command(name = "trustworthiness-checker-ros")]
#[command(about = "ROS-specific runtime verification tool")]
#[command(
    long_about = "ROS-specific version of the Trustworthiness Checker with native ROS topic integration and mapping capabilities."
)]
pub struct CliROS {
    #[arg(help = "Path to the model specification file")]
    pub model: String,
    #[arg(help = "Path to ROS input mapping configuration file")]
    pub ros_input_mapping_file: String,

    #[arg(long, help = "Specification language to use")]
    pub language: Option<Language>,
    #[arg(long, help = "Semantics engine to use for monitoring")]
    pub semantics: Option<Semantics>,
    #[arg(long, help = "Runtime system to use for execution")]
    pub runtime: Option<Runtime>,
}
