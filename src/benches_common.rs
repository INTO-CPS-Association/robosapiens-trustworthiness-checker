// This file defines the common functions used by the benchmarks.
// Dead code is allowed as it is only used when compiling benchmarks.

use std::rc::Rc;

use crate::LOLASpecification;
use crate::Value;
use crate::core::AbstractMonitorBuilder;
use crate::core::OutputHandler;
use crate::core::Runnable;
use crate::core::Runtime;
use crate::core::Semantics;
use crate::io::map::MapInputProvider;
use crate::io::testing::null_output_handler::{LimitedNullOutputHandler, NullOutputHandler};
use crate::lang::dynamic_lola::lalr_parser::LALRParser;
use crate::lang::dynamic_lola::type_checker::TypedLOLASpecification;
use crate::runtime::RuntimeBuilder;
use crate::runtime::asynchronous::AsyncMonitorBuilder;
use crate::runtime::builder::TypedValueConfig;

use smol::LocalExecutor;

pub async fn monitor_runtime_outputs(
    runtime: Runtime,
    semantics: Semantics,
    executor: Rc<LocalExecutor<'static>>,
    spec: LOLASpecification,
    input_provider: MapInputProvider,
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
        .input(Box::new(input_provider))
        .build();
    monitor.run().await.expect("Error running monitor");
}

pub async fn monitor_outputs_untyped_async_limited(
    executor: Rc<LocalExecutor<'static>>,
    spec: LOLASpecification,
    input_provider: MapInputProvider,
    limit: usize,
) {
    monitor_runtime_outputs(
        Runtime::Async,
        Semantics::Untimed,
        executor,
        spec,
        input_provider,
        Some(limit),
    )
    .await;
}

pub async fn monitor_outputs_untyped_async(
    executor: Rc<LocalExecutor<'static>>,
    spec: LOLASpecification,
    input_values: MapInputProvider,
) {
    monitor_runtime_outputs(
        Runtime::Async,
        Semantics::Untimed,
        executor,
        spec,
        input_values,
        None,
    )
    .await;
}

pub async fn monitor_outputs_untyped_little(
    executor: Rc<LocalExecutor<'static>>,
    spec: LOLASpecification,
    input_provider: MapInputProvider,
) {
    monitor_runtime_outputs(
        Runtime::SemiSync,
        Semantics::Untimed,
        executor,
        spec,
        input_provider,
        None,
    )
    .await;
}

pub async fn monitor_outputs_typed_async(
    executor: Rc<LocalExecutor<'static>>,
    spec: TypedLOLASpecification,
    input_provider: MapInputProvider,
) {
    // Currently cannot be deduplicated since it includes the type
    // checking
    let output_handler = Box::new(NullOutputHandler::new(
        executor.clone(),
        spec.output_vars.clone(),
    ));
    let async_monitor = AsyncMonitorBuilder::<
        _,
        TypedValueConfig,
        crate::semantics::TypedUntimedLolaSemantics<LALRParser>,
    >::new()
    .executor(executor.clone())
    .model(spec.clone())
    .input(Box::new(input_provider))
    .output(output_handler)
    .build();
    async_monitor.run().await.expect("Error running monitor");
}
