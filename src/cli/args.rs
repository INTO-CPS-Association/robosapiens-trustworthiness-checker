use clap::{Args, Parser, ValueEnum, builder::OsStr};

use crate::core::{Runtime, Semantics};

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum Language {
    /// LOLA + Eval language
    Lola,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum ParserMode {
    /// Parser Combinator using Winnow
    Combinator,
    /// LALR parser using lalrpop
    LALR,
}

#[derive(Args, Clone)]
#[group(required = true, multiple = false)]
pub struct InputMode {
    #[clap(long)]
    pub input_file: Option<String>,

    #[clap(long, value_delimiter = ' ', num_args = 1..)]
    pub input_mqtt_topics: Option<Vec<String>>,

    #[clap(long, value_delimiter = ' ', num_args = 1..)]
    pub input_map_mqtt_topics: Option<Vec<String>>,

    #[clap(long)]
    pub mqtt_input: bool,

    // #[cfg(feature = "ros")]
    #[clap(long)]
    pub input_ros_topics: Option<String>,
}

#[derive(Args, Clone)]
#[group(required = false, multiple = false)]
pub struct OutputMode {
    #[clap(long)]
    pub output_stdout: bool,

    #[clap(long, value_delimiter = ' ', num_args = 1..)]
    pub output_mqtt_topics: Option<Vec<String>>,

    #[clap(long)]
    pub mqtt_output: bool,

    // #[cfg(feature = "ros")]
    // TODO: Implement ROS output support
    #[clap(long)]
    pub output_ros_topics: Option<String>,
}

#[derive(Args, Clone)]
#[group(required = false, multiple = false)]
pub struct DistributionMode {
    #[clap(long, default_value_t = true)]
    pub centralised: bool,

    #[clap(long)]
    #[clap(requires = "local_node")]
    pub distribution_graph: Option<String>,

    #[clap(long)]
    pub local_topics: Option<Vec<String>>,

    #[clap(long, value_delimiter = ' ', num_args = 1..)]
    pub mqtt_centralised_distributed: Option<Vec<String>>,

    #[clap(long, value_delimiter = ' ', num_args = 1..)]
    pub mqtt_randomized_distributed: Option<Vec<String>>,

    #[clap(long, value_delimiter = ' ', num_args = 1..)]
    #[clap(requires = "distribution_constraints")]
    pub mqtt_static_optimized: Option<Vec<String>>,

    #[clap(long, value_delimiter = ' ', num_args = 1..)]
    #[clap(requires = "distribution_constraints")]
    pub mqtt_dynamic_optimized: Option<Vec<String>>,

    #[clap(long)]
    #[clap(requires = "local_node")]
    pub distributed_work: bool,
}

#[derive(ValueEnum, Debug, Clone)]
pub enum SchedulingType {
    Mock,
    MQTT,
}

impl Into<&'static str> for SchedulingType {
    fn into(self) -> &'static str {
        match self {
            SchedulingType::Mock => "mock",
            SchedulingType::MQTT => "mqtt",
        }
    }
}

impl Into<String> for SchedulingType {
    fn into(self) -> String {
        match self {
            SchedulingType::Mock => "mock".to_string(),
            SchedulingType::MQTT => "mqtt".to_string(),
        }
    }
}

impl Into<OsStr> for SchedulingType {
    fn into(self) -> OsStr {
        match self {
            SchedulingType::Mock => (&"mock").into(),
            SchedulingType::MQTT => (&"mqtt").into(),
        }
    }
}

#[derive(Parser, Clone)]
pub struct Cli {
    pub model: String,

    // The mode of input to use
    #[command(flatten)]
    pub input_mode: InputMode,

    // The mode of output to use
    #[command(flatten)]
    pub output_mode: OutputMode,

    #[arg(long)]
    pub parser_mode: Option<ParserMode>,
    #[arg(long)]
    pub language: Option<Language>,
    #[arg(long)]
    pub semantics: Option<Semantics>,
    #[arg(long)]
    pub runtime: Option<Runtime>,

    #[command(flatten)]
    pub distribution_mode: DistributionMode,

    #[arg(long)]
    pub local_node: Option<String>,

    #[arg(long, default_value = SchedulingType::Mock)]
    pub scheduling_mode: SchedulingType,

    #[clap(long, value_delimiter = ' ', num_args = 1..)]
    pub distribution_constraints: Option<Vec<String>>,
}

#[derive(Parser)]
pub struct CliROS {
    pub model: String,
    pub ros_input_mapping_file: String,

    #[arg(long)]
    pub language: Option<Language>,
    #[arg(long)]
    pub semantics: Option<Semantics>,
    #[arg(long)]
    pub runtime: Option<Runtime>,
}
