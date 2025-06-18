// This file defines the common functions used by the benchmarks.
// Dead code is allowed as it is only used when compiling benchmarks.

use std::collections::BTreeMap;
use std::rc::Rc;

use crate::LOLASpecification;
use crate::OutputStream;
use crate::Value;
use crate::VarName;
use crate::core::AbstractMonitorBuilder;
use crate::core::OutputHandler;
use crate::core::Runnable;
use crate::core::Runtime;
use crate::core::Semantics;
use crate::dep_manage::interface::DependencyManager;
use crate::io::testing::null_output_handler::{LimitedNullOutputHandler, NullOutputHandler};
use crate::lang::dynamic_lola::type_checker::TypedLOLASpecification;
use crate::runtime::RuntimeBuilder;
use crate::runtime::asynchronous::AsyncMonitorBuilder;
use crate::runtime::asynchronous::Context;
use crate::runtime::constraints::runtime::ConstraintBasedRuntime;

use smol::LocalExecutor;

pub async fn monitor_runtime_outputs(
    runtime: Runtime,
    semantics: Semantics,
    executor: Rc<LocalExecutor<'static>>,
    spec: LOLASpecification,
    input_streams: BTreeMap<VarName, OutputStream<Value>>,
    dep_manager: DependencyManager,
    output_limit: Option<usize>,
) {
    let output_handler: Box<dyn OutputHandler<Val = Value>> = match output_limit {
        Some(output_limit) => Box::new(LimitedNullOutputHandler::new(
            executor.clone(),
            spec.output_vars.clone(),
            output_limit,
        )),
        None => Box::new(NullOutputHandler::new(
            executor.clone(),
            spec.output_vars.clone(),
        )),
    };

    let monitor = RuntimeBuilder::new()
        .runtime(runtime)
        .semantics(semantics)
        .executor(executor)
        .model(spec)
        .output(output_handler)
        .input(Box::new(input_streams))
        .dependencies(dep_manager)
        .build();
    monitor.run().await.expect("Error running monitor");
}

pub async fn monitor_outputs_untyped_constraints(
    executor: Rc<LocalExecutor<'static>>,
    spec: LOLASpecification,
    input_streams: BTreeMap<VarName, OutputStream<Value>>,
    dep_manager: DependencyManager,
) {
    monitor_runtime_outputs(
        Runtime::Constraints,
        Semantics::Untimed,
        executor,
        spec,
        input_streams,
        dep_manager,
        None,
    )
    .await;
}

pub async fn monitor_outputs_untyped_async_limited(
    executor: Rc<LocalExecutor<'static>>,
    spec: LOLASpecification,
    input_streams: BTreeMap<VarName, OutputStream<Value>>,
    dep_manager: DependencyManager,
    limit: usize,
) {
    monitor_runtime_outputs(
        Runtime::Async,
        Semantics::Untimed,
        executor,
        spec,
        input_streams,
        dep_manager,
        Some(limit),
    )
    .await;
}

pub async fn monitor_outputs_untyped_async(
    executor: Rc<LocalExecutor<'static>>,
    spec: LOLASpecification,
    input_streams: BTreeMap<VarName, OutputStream<Value>>,
    dep_manager: DependencyManager,
) {
    monitor_runtime_outputs(
        Runtime::Async,
        Semantics::Untimed,
        executor,
        spec,
        input_streams,
        dep_manager,
        None,
    )
    .await;
}

pub async fn monitor_outputs_typed_async(
    executor: Rc<LocalExecutor<'static>>,
    spec: TypedLOLASpecification,
    input_streams: BTreeMap<VarName, OutputStream<Value>>,
    dep_manager: DependencyManager,
) {
    // Currently cannot be deduplicated since it includes the type
    // checking
    let output_handler = Box::new(NullOutputHandler::new(
        executor.clone(),
        spec.output_vars.clone(),
    ));
    let async_monitor = AsyncMonitorBuilder::<
        _,
        Context<Value>,
        _,
        _,
        crate::semantics::TypedUntimedLolaSemantics,
    >::new()
    .executor(executor.clone())
    .model(spec.clone())
    .input(Box::new(input_streams))
    .output(output_handler)
    .dependencies(dep_manager)
    .build();
    async_monitor.run().await.expect("Error running monitor");
}

pub fn monitor_outputs_untyped_constraints_no_overhead(
    spec: LOLASpecification,
    num_outputs: i64,
    dep_manager: DependencyManager,
) {
    let size = num_outputs;
    let mut xs = (0..size).map(|i| Value::Int(i * 2));
    let mut ys = (0..size).map(|i| Value::Int(i * 2 + 1));
    let mut runtime = ConstraintBasedRuntime::new(dep_manager);
    runtime.store_from_spec(spec);

    for _ in 0..size {
        let inputs = BTreeMap::from([
            ("x".into(), xs.next().unwrap()),
            ("y".into(), ys.next().unwrap()),
        ]);
        runtime.step(inputs.iter());
        runtime.cleanup();
    }
}
