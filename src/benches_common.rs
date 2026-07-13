// This file defines the common functions used by the benchmarks.
// Dead code is allowed as it is only used when compiling benchmarks.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::rc::Rc;

use crate::InputStream;
use crate::UntypedDsrvSpecification;
use crate::Value;
use crate::VarName;
use crate::core::OutputHandler;
use crate::core::Runtime;
use crate::core::RuntimeSpec;
use crate::core::Semantics;
use crate::io::InputStreamFactory;
use crate::io::testing::{LimitedNullOutputHandler, NullOutputHandler};
use crate::io::{OutputHandlerBuilder, OutputHandlerSpec};
use crate::lang::dsrv::lalr_parser::LALRParser;
use crate::lang::dsrv::type_checker::TypedDsrvSpecification;
use crate::runtime::asynchronous::AsyncRuntimeBuilder;
use crate::runtime::builder::RuntimeBuilder;
use crate::runtime::builder::SemiSyncValueConfig;
use crate::runtime::builder::TypedValueConfig;
use crate::runtime::reconfigurable_semi_sync::ReconfSemiSyncRuntimeBuilder;
use crate::semantics::UntimedDsrvSemantics;
use crate::stream_utils::Fanout;
use crate::stream_utils::FanoutSender;

use async_unsync::bounded;
use smol::LocalExecutor;

pub const RECONF_TOPIC: &str = "R";

pub async fn monitor_runtime_outputs(
    runtime: RuntimeSpec,
    semantics: Semantics,
    executor: Rc<LocalExecutor<'static>>,
    spec: UntypedDsrvSpecification,
    input_stream: InputStream<Value>,
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

    let monitor = crate::runtime::GeneralRuntimeBuilder::new()
        .runtime(runtime)
        .semantics(semantics)
        .executor(executor)
        .model(spec)
        .output(output_handler)
        .input(input_stream)
        .build()
        .await;
    monitor.run().await.expect("Error running monitor");
}

pub async fn monitor_outputs_untyped_async_limited(
    executor: Rc<LocalExecutor<'static>>,
    spec: UntypedDsrvSpecification,
    input_stream: InputStream<Value>,
    limit: usize,
) {
    monitor_runtime_outputs(
        RuntimeSpec::Async,
        Semantics::Untimed,
        executor,
        spec,
        input_stream,
        Some(limit),
    )
    .await;
}

pub async fn monitor_outputs_untyped_reconf_limited(
    executor: Rc<LocalExecutor<'static>>,
    spec: UntypedDsrvSpecification,
    input_factory: InputStreamFactory,
    output_handler_builder: OutputHandlerBuilder,
    use_context_transfer: bool,
) {
    let builder: ReconfSemiSyncRuntimeBuilder<
        SemiSyncValueConfig,
        UntimedDsrvSemantics<LALRParser>,
        LALRParser,
    > = ReconfSemiSyncRuntimeBuilder::new()
        .executor(executor)
        .model(spec)
        .input_factory(input_factory)
        .output_builder(output_handler_builder)
        .reconf_topic(RECONF_TOPIC.into())
        .use_context_transfer(use_context_transfer);
    let monitor = Box::new(builder).build().await;
    monitor.run().await.expect("Error running monitor");
}

pub async fn monitor_outputs_untyped_async(
    executor: Rc<LocalExecutor<'static>>,
    spec: UntypedDsrvSpecification,
    input_stream: InputStream<Value>,
) {
    monitor_runtime_outputs(
        RuntimeSpec::Async,
        Semantics::Untimed,
        executor,
        spec,
        input_stream,
        None,
    )
    .await;
}

pub async fn monitor_outputs_untyped_little(
    executor: Rc<LocalExecutor<'static>>,
    spec: UntypedDsrvSpecification,
    input_stream: InputStream<Value>,
) {
    monitor_runtime_outputs(
        RuntimeSpec::SemiSync,
        Semantics::Untimed,
        executor,
        spec,
        input_stream,
        None,
    )
    .await;
}

pub async fn monitor_outputs_typed_async(
    executor: Rc<LocalExecutor<'static>>,
    spec: TypedDsrvSpecification,
    input_stream: InputStream<Value>,
) {
    // Currently cannot be deduplicated since it includes the type
    // checking
    let output_handler = Box::new(NullOutputHandler::new(
        executor.clone(),
        spec.output_vars.clone(),
    ));
    let async_monitor = AsyncRuntimeBuilder::<
        TypedValueConfig,
        crate::semantics::TypedUntimedDsrvSemantics<LALRParser>,
    >::new()
    .executor(executor.clone())
    .model(spec.clone())
    .input(input_stream)
    .output(output_handler)
    .build()
    .await;
    async_monitor.run().await.expect("Error running monitor");
}

pub fn input_factory_dsrv_paper_bench(
    var_names: BTreeSet<VarName>,
) -> (InputStreamFactory, BTreeMap<VarName, FanoutSender<Value>>) {
    let mut tx_fans: BTreeMap<VarName, FanoutSender<Value>> = BTreeMap::new();
    let mut fanouts: BTreeMap<VarName, Rc<Fanout<Value>>> = BTreeMap::new();

    for name in var_names {
        let (tx, fan) = Fanout::new();
        fanouts.insert(name.clone(), fan);
        tx_fans.insert(name, tx);
    }
    let (tx_r, fr) = Fanout::new();
    fanouts.insert(RECONF_TOPIC.into(), fr);
    tx_fans.insert(RECONF_TOPIC.into(), tx_r);

    let input_factory = crate::io::testing::input_factory(fanouts);

    (input_factory, tx_fans)
}

pub fn output_builder_dsrv_paper_bench(
    output_var_names: BTreeSet<VarName>,
    ex: Rc<LocalExecutor<'static>>,
) -> (
    OutputHandlerBuilder,
    bounded::Receiver<BTreeMap<VarName, Value>>,
) {
    let (out_tx, out_rx) = bounded::channel::<BTreeMap<VarName, Value>>(1024).into_split();
    let output_spec = OutputHandlerSpec::Manual(out_tx);
    let output_builder = OutputHandlerBuilder::new(output_spec)
        .executor(ex.clone())
        .output_var_names(output_var_names)
        .aux_info(vec![]);

    (output_builder, out_rx)
}
