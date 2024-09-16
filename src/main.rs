mod ast;
mod async_runtime;
mod constraint_based_runtime;
mod constraint_solver;
mod core;
mod queuing_runtime;
mod monitoring_semantics;
mod parser;
mod ring_buffer;
mod untimed_monitoring_combinators;

#[tokio::main]
async fn main() {
    println!("Hello, world!");
}
