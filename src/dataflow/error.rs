use super::*;

/// A structural problem detected while validating or binding an evaluation graph.
#[derive(Debug, thiserror::Error)]
pub enum StreamProgramError {
    #[error("recursive output may only be read through a positive stream delay")]
    UnguardedRecursiveOutput,
    #[error("dataflow variable `{0}` is not available in this stream program")]
    UnknownVariable(VarName),
    #[error("temporal operator `{operator}` is not supported inside a function graph")]
    TemporalFunctionBody { operator: &'static str },
}

#[derive(Debug, thiserror::Error)]
pub enum DataflowEvaluationError {
    #[error("dataflow input contains {actual} values, expected {expected}")]
    InputCountMismatch { expected: usize, actual: usize },
    #[error("dataflow output contains {actual} values, expected {expected}")]
    OutputCountMismatch { expected: usize, actual: usize },
    #[error("invalid dynamic expression `{expression}`: {message}")]
    DynamicExpressionParse {
        expression: EcoString,
        message: String,
    },
    #[error("dynamic expression `{expression}` failed runtime type checking: {message}")]
    DynamicExpressionType {
        expression: EcoString,
        message: String,
    },
    #[error("dynamic expression references variables outside its allowed context: {0:?}")]
    DynamicExpressionContext(Vec<VarName>),
    #[error("dynamic/defer expected a string property, got {0}")]
    InvalidExpressionSource(String),
    #[error("invalid dynamically compiled stream program: {0}")]
    InvalidDynamicProgram(StreamProgramError),
    #[error("runtime dependency cycle contains stream `{0}`")]
    DynamicDependencyCycle(VarName),
    #[error("nested dynamic dependency reconfiguration is not supported")]
    UnsupportedNestedReconfiguration,
    #[error("dataflow monitor cannot continue after a previous evaluation failure")]
    MonitorFailed,
}

#[derive(Debug, thiserror::Error)]
pub enum DataflowCompilationError {
    #[error("invalid stream program: {0}")]
    InvalidStreamProgram(#[from] StreamProgramError),
    #[error("output `{0}` is not a declared stream")]
    UnknownOutput(VarName),
    #[error("stream `{0}` has no expression")]
    MissingExpression(VarName),
    #[error("stream `{stream}` references unavailable variables: {variables:?}")]
    UnavailableVariables {
        stream: VarName,
        variables: Vec<VarName>,
    },
    #[error("computed dependency cycle contains stream `{0}`")]
    DependencyCycle(VarName),
    #[error("stream `{stream}` uses unsupported reconfiguration: {reason}")]
    UnsupportedReconfiguration {
        stream: VarName,
        reason: &'static str,
    },
}
