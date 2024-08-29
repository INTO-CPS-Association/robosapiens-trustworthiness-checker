use core::{IndexedVarName, Monitor, StreamData, VarName};
use std::{
    collections::{BTreeMap, HashMap},
    pin::Pin,
};

mod ast;
mod core;
mod parser;
use ast::*;
mod constraint_solver;
use constraint_solver::*;
mod constraint_based_runtime;
use futures::{stream, StreamExt};
mod untimed_monitoring_combinators;
use constraint_based_runtime::*;
mod async_runtime;
use async_runtime::*;
use monitoring_semantics::UNTIMED_LOLA_SEMANTICS;
mod monitoring_semantics;

#[tokio::main]
async fn main() {
    println!("Hello, world!");
}
