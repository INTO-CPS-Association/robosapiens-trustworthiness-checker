#![recursion_limit = "256"]

pub mod benches_common;
pub mod causal;
pub mod core;
pub use core::{
    DynOutputSink, ExecutionPolicy, FileInputValue, InputBatch, InputStream, InputTick,
    InputUpdate, InputUpdateRef, JsonStreamValue, LocalStream, OutputBackend, OutputBatch,
    OutputError, OutputInterface, OutputRole, OutputRoute, OutputUpdate, OutputWriter,
    RosStreamValue, Runtime, SharedOutputBackend, Specification, Value, VarName,
};
pub mod cli;
pub mod dataflow;
pub mod io;
pub use io::file::parse_file;
pub mod lang;
pub use lang::dsrv::{
    DsrvPipelineError, TypeCheckMode, TypeCheckOptions,
    ast::{CheckedDsrvSpecification, DsrvSpecification},
};
pub mod distributed;
pub mod dsrv_fixtures;
mod fingerprint;
pub mod macros;
pub mod runtime;
pub mod semantics;
pub mod stream_utils;
pub mod utils;
