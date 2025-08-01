#![recursion_limit = "256"]

pub mod benches_common;
pub mod core;
pub use crate::core::Value;
pub use core::{InputProvider, Monitor, OutputStream, Specification, VarName};
pub mod cli;
pub mod dep_manage;
pub mod io;
pub use io::file::parse_file;
pub mod lang;
pub use lang::dynamic_lola::{
    ast::{LOLASpecification, SExpr},
    parser::lola_specification,
};
pub mod distributed;
pub mod lola_fixtures;
pub mod macros;
pub mod runtime;
pub mod semantics;
pub mod stream_utils;
pub mod utils;
