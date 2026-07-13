use super::*;

/// A structural problem detected while validating or binding a plan body.
#[derive(Debug, thiserror::Error)]
pub enum PlanValidationError {
    #[error("recursive output may only be read through a positive stream delay")]
    UnguardedRecursiveOutput,
    #[error("dataflow variable `{0}` is not available in this plan")]
    UnknownVariable(VarName),
    #[error("temporal operator `{operator}` is not supported inside a function body")]
    TemporalFunctionBody { operator: &'static str },
}

#[derive(Debug, thiserror::Error)]
pub enum DataflowEvalError {
    #[error("dataflow input contains {actual} values, expected {expected}")]
    InputCount { expected: usize, actual: usize },
    #[error("dataflow output contains {actual} values, expected {expected}")]
    OutputCount { expected: usize, actual: usize },
    #[error("invalid dynamic expression `{expression}`: {message}")]
    DynamicParse {
        expression: EcoString,
        message: String,
    },
    #[error("dynamic expression `{expression}` failed runtime type checking: {message}")]
    DynamicType {
        expression: EcoString,
        message: String,
    },
    #[error("dynamic expression references variables outside its allowed context: {0:?}")]
    DynamicRestrictedContext(Vec<VarName>),
    #[error("dynamic/defer expected a string property, got {0}")]
    InvalidDynamicValue(String),
    #[error("invalid dynamically compiled dataflow plan: {0}")]
    DynamicPlan(PlanValidationError),
    #[error("runtime dependency cycle contains stream `{0}`")]
    DynamicDependencyCycle(VarName),
    #[error("runtime dependency scheduling did not converge")]
    DynamicSchedulingDidNotConverge,
    #[error("dataflow monitor cannot continue after a previous evaluation failure")]
    MonitorFailed,
}

#[derive(Debug, thiserror::Error)]
pub enum DataflowCompileError {
    #[error("invalid dataflow plan: {0}")]
    InvalidPlan(#[from] PlanValidationError),
    #[error("output `{0}` is not a declared stream")]
    UnknownOutput(VarName),
    #[error("stream `{0}` has no expression")]
    MissingExpression(VarName),
    #[error("stream `{stream}` references unavailable inputs: {inputs:?}")]
    UnavailableInputs {
        stream: VarName,
        inputs: Vec<VarName>,
    },
    #[error("computed dependency cycle contains stream `{0}`")]
    DependencyCycle(VarName),
}
