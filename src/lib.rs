#![recursion_limit = "256"]

pub mod benches_common;
pub mod causal;
pub mod core;
pub use core::{
    DynOutputSink, ErrorDetails, ExecutionPolicy, FileInputValue, FormatId, InputBatch,
    InputBinding, InputError, InputStream, InputTick, InputUpdate, InputUpdateRef, IoErrorKind,
    JsonStreamValue, LocalStream, OutputBatch, OutputBinding, OutputError, OutputInterface,
    OutputRole, OutputUpdate, OutputWriter, RosStreamValue, Route, Runtime, Specification, Value,
    VarName,
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
