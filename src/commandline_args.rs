use clap::{Args, Parser, ValueEnum};

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum Language {
    /// LOLA + Eval language
    Lola,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum Semantics {
    Untimed,
    TypedUntimed,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum Runtime {
    Async,
    Queuing,
    Constraints,
}

#[derive(Args)]
#[group(required = true, multiple = false)]
pub struct InputMode {
    #[clap(long)]
    pub input_file: Option<String>,

    #[clap(long, value_delimiter = ' ', num_args = 1..)]
    pub input_mqtt_topics: Option<Vec<String>>,

    // #[cfg(feature = "ros")]
    #[clap(long)]
    pub input_ros_topics: Option<String>,
}

#[derive(Args)]
#[group(required = false, multiple = false)]
pub struct OutputMode {
    #[clap(long)]
    pub output_stdout: bool,

    #[clap(long, value_delimiter = ' ', num_args = 1..)]
    pub output_mqtt_topics: Option<Vec<String>>,

    // #[cfg(feature = "ros")]
    // TODO: Implement ROS output support
    #[clap(long)]
    pub output_ros_topics: Option<String>,
}

#[derive(Parser)]
pub struct Cli {
    pub model: String,

    // The mode of input to use
    #[command(flatten)]
    pub input_mode: InputMode,

    // The mode of output to use
    #[command(flatten)]
    pub output_mode: OutputMode,

    #[arg(long)]
    pub language: Option<Language>,
    #[arg(long)]
    pub semantics: Option<Semantics>,
    #[arg(long)]
    pub runtime: Option<Runtime>,
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
