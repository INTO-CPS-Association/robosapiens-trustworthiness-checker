#![recursion_limit = "256"]

pub mod benches_common;
pub mod core;
pub use core::{
    ExecutionPolicy, InputBatch, InputEvent, InputStream, OutputStream, Runtime, Specification,
    Value, VarName,
};
pub(crate) use core::{InputTickStream, into_tick_stream};
pub mod cli;
pub mod dataflow;
pub mod io;
pub use io::file::parse_file;
pub mod lang;
pub use lang::dsrv::{
    ast::{SExpr, UntypedDsrvSpecification},
    parser::dsrv_specification,
};
pub mod distributed;
pub mod dsrv_fixtures;
pub mod macros;
pub mod runtime;
pub mod semantics;
pub mod stream_utils;
pub mod utils;
