use clap::{Parser, ValueEnum};

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

#[derive(Parser)]
pub struct Cli {
    pub model: String,
    pub input_file: String,

    #[arg(long)]
    pub language: Option<Language>,
    #[arg(long)]
    pub semantics: Option<Semantics>,
    #[arg(long)]
    pub runtime: Option<Runtime>,
}
