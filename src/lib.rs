#![recursion_limit = "256"]
#![deny(warnings)]

pub mod core;
pub use crate::core::Value;
pub use core::{
    InputProvider, Monitor, MonitoringSemantics, OutputStream, Specification, StreamContext,
    VarName,
};
pub mod ast;
pub use ast::{LOLASpecification, SExpr};
pub mod async_runtime;
pub use async_runtime::AsyncMonitorRunner;
pub mod constraint_based_runtime;
pub mod constraint_solver;
pub mod monitoring_semantics;
pub use monitoring_semantics::UntimedLolaSemantics;
pub use typed_monitoring_semantics::TypedUntimedLolaSemantics;
pub mod parser;
pub use parser::{lola_expression, lola_input_file, lola_specification};
pub mod file_handling;
pub mod file_input_provider;
pub mod queuing_runtime;
pub mod type_checking;
pub mod untimed_monitoring_combinators;
pub use file_handling::parse_file;
pub mod commandline_args;
pub mod typed_monitoring_combinators;
pub mod typed_monitoring_semantics;
