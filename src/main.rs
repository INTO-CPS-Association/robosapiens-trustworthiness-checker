mod ast;
mod async_runtime;
mod constraint_based_runtime;
mod constraint_solver;
mod core;
mod monitoring_semantics;
mod parser;
mod untimed_monitoring_combinators;

#[tokio::main]
async fn main() {
    println!("Hello, world!");
}
