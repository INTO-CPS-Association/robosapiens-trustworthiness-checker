// This file defines the common functions used by the benchmarks.
// Dead code is allowed as it is only used when compiling benchmarks.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::rc::Rc;

use crate::DsrvSpecification;
use crate::InputStream;
use crate::Value;
use crate::VarName;
use crate::core::ExecutionPolicy;
use crate::dataflow::ContextTransferPolicy;

use crate::core::Runtime;
use crate::core::RuntimeSpec;
use crate::core::Semantics;
use crate::io::output::OutputBackendConfig;
use crate::io::{InputPipeline, InputSource, OutputBackendBuilder};
use crate::lang::dsrv::ast::CheckedDsrvSpecification;
use crate::runtime::asynchronous::AsyncRuntimeBuilder;
use crate::runtime::builder::RuntimeBuilder;
use crate::runtime::builder::{
    CheckedSemiSyncValueConfig, CheckedValueConfig, SemiSyncValueConfig,
};
use crate::runtime::dataflow::{DataflowRuntimeBuilder, ReconfigurableDataflowRuntimeBuilder};
use crate::runtime::reconfigurable_semi_sync::ReconfSemiSyncRuntimeBuilder;
use crate::runtime::semi_sync::SemiSyncRuntimeBuilder;
use crate::semantics::{CheckedUntimedDsrvSemantics, UntimedDsrvSemantics};
use crate::stream_utils::Fanout;
use crate::stream_utils::FanoutSender;

use async_unsync::bounded;
use smol::LocalExecutor;

pub const RECONF_TOPIC: &str = "R";
#[cfg(feature = "jit")]
pub const KEY_BENCHMARK_JIT_HOTNESS_EVENTS: u64 = 1_024;

pub fn function_binding_benchmark(terms: usize, checked: bool) -> impl FnMut() -> usize {
    assert!(terms > 0);
    let expression = std::iter::repeat_n("x", terms)
        .collect::<Vec<_>>()
        .join(" + ");
    let source = format!("in n: Int\nout result: Int\nresult = (\\x: Int -> {expression})(n)");
    let runtime_expr = if checked {
        source
            .parse::<CheckedDsrvSpecification>()
            .expect("function binding fixture should type-check")
            .var_expr(&VarName::new("result"))
            .unwrap()
            .expr()
            .clone()
    } else {
        source
            .parse::<DsrvSpecification>()
            .expect("function binding fixture should parse")
            .var_expr(&VarName::new("result"))
            .unwrap()
    };
    let crate::lang::dsrv::ast::ExprView::Apply(function, mut args) = runtime_expr.as_ref().view()
    else {
        panic!("function binding fixture should be an application");
    };
    let argument = args
        .next()
        .expect("function binding fixture needs an argument");
    let crate::lang::dsrv::ast::ExprView::Lambda(params, body) = function.view() else {
        panic!("function binding fixture should contain a lambda");
    };
    let params = params.clone();
    let body = runtime_expr.subtree(body);
    let argument = runtime_expr.subtree(argument);

    move || {
        crate::semantics::untimed_dsrv::semantics::bind_expression_for_benchmark(
            body.clone(),
            &params,
            ecow::EcoVec::from([argument.clone()]),
        )
    }
}

pub async fn monitor_runtime_outputs(
    runtime: RuntimeSpec,
    semantics: Semantics,
    executor: Rc<LocalExecutor<'static>>,
    spec: DsrvSpecification,
    input_stream: InputStream<Value>,
    output_limit: Option<usize>,
) {
    let output_backend = match output_limit {
        Some(limit) => OutputBackendConfig::limited_null(limit),
        None => OutputBackendConfig::null(),
    };
    let output_builder = OutputBackendBuilder::new(output_backend);

    let monitor = crate::runtime::GeneralRuntimeBuilder::new()
        .runtime(runtime)
        .semantics(semantics)
        .executor(executor)
        .model(spec)
        .output_pipeline_builder(output_builder)
        .input(input_stream)
        .build()
        .await
        .expect("monitor runtime could not be built");
    monitor.run().await.expect("Error running monitor");
}

pub async fn monitor_outputs_untyped_async_limited(
    executor: Rc<LocalExecutor<'static>>,
    spec: DsrvSpecification,
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

pub async fn monitor_outputs_untyped_dataflow_limited(
    executor: Rc<LocalExecutor<'static>>,
    spec: DsrvSpecification,
    input_stream: InputStream<Value>,
    limit: usize,
) {
    monitor_runtime_outputs(
        RuntimeSpec::Dataflow(ExecutionPolicy::Buffered),
        Semantics::Untimed,
        executor,
        spec,
        input_stream,
        Some(limit),
    )
    .await;
}

pub async fn monitor_outputs_specialized_dataflow_limited(
    executor: Rc<LocalExecutor<'static>>,
    spec: DsrvSpecification,
    input_stream: InputStream<Value>,
    limit: usize,
) {
    monitor_runtime_outputs(
        RuntimeSpec::Dataflow(ExecutionPolicy::Buffered),
        Semantics::GradualTypedUntimed,
        executor,
        spec,
        input_stream,
        Some(limit),
    )
    .await;
}

pub async fn monitor_outputs_untyped_semisync_limited(
    executor: Rc<LocalExecutor<'static>>,
    spec: DsrvSpecification,
    input_stream: InputStream<Value>,
    limit: usize,
) {
    monitor_runtime_outputs(
        RuntimeSpec::SemiSync,
        Semantics::Untimed,
        executor,
        spec,
        input_stream,
        Some(limit),
    )
    .await;
}

pub async fn monitor_outputs_untyped_dataflow(
    executor: Rc<LocalExecutor<'static>>,
    spec: DsrvSpecification,
    input_stream: InputStream<Value>,
) {
    monitor_runtime_outputs(
        RuntimeSpec::Dataflow(ExecutionPolicy::Buffered),
        Semantics::Untimed,
        executor,
        spec,
        input_stream,
        None,
    )
    .await;
}

pub async fn monitor_outputs_specialized_dataflow(
    executor: Rc<LocalExecutor<'static>>,
    spec: DsrvSpecification,
    input_stream: InputStream<Value>,
) {
    monitor_runtime_outputs(
        RuntimeSpec::Dataflow(ExecutionPolicy::Buffered),
        Semantics::GradualTypedUntimed,
        executor,
        spec,
        input_stream,
        None,
    )
    .await;
}

pub async fn monitor_outputs_typed_semisync(
    executor: Rc<LocalExecutor<'static>>,
    spec: CheckedDsrvSpecification,
    input_stream: InputStream<Value>,
) {
    let output_builder = OutputBackendBuilder::new(OutputBackendConfig::null());
    let writer = output_builder
        .build(spec.output_vars(), spec.aux_vars(), None)
        .await
        .expect("typed semi-sync output pipeline should open");

    let monitor =
        SemiSyncRuntimeBuilder::<CheckedSemiSyncValueConfig, CheckedUntimedDsrvSemantics>::new()
            .executor(executor)
            .model(spec)
            .output_writer(writer)
            .input(input_stream)
            .build()
            .await;
    monitor.run().await.expect("Error running monitor");
}

pub async fn monitor_outputs_typed_dataflow(
    executor: Rc<LocalExecutor<'static>>,
    spec: CheckedDsrvSpecification,
    input_stream: InputStream<Value>,
    semantics: Semantics,
) {
    if !matches!(
        semantics,
        Semantics::TypedUntimed | Semantics::GradualTypedUntimed
    ) {
        panic!(
            "dataflow typed runtime only supports typed/gradual typed semantics, got {semantics:?}",
        );
    }

    monitor_outputs_quickened_dataflow(executor, spec, input_stream).await;
}

/// Run the checked dataflow runtime without quickening or native compilation.
pub async fn monitor_outputs_dataflow(
    executor: Rc<LocalExecutor<'static>>,
    spec: CheckedDsrvSpecification,
    input_stream: InputStream<Value>,
) {
    let output_builder = OutputBackendBuilder::new(OutputBackendConfig::null());
    let writer = output_builder
        .build(spec.output_vars(), spec.aux_vars(), None)
        .await
        .expect("dataflow output pipeline should open");
    let runtime = DataflowRuntimeBuilder::<CheckedDsrvSpecification>::new()
        .execution_policy(ExecutionPolicy::Buffered)
        .quickening(false)
        .executor(executor)
        .model(spec)
        .output_writer(writer)
        .input(input_stream)
        .build()
        .await;
    runtime.run().await.expect("Error running monitor");
}

/// Run a fixed number of checked dataflow outputs without quickening or native compilation.
pub async fn monitor_outputs_dataflow_limited(
    executor: Rc<LocalExecutor<'static>>,
    spec: CheckedDsrvSpecification,
    input_stream: InputStream<Value>,
    limit: usize,
) {
    let output_builder = OutputBackendBuilder::new(OutputBackendConfig::limited_null(limit));
    let writer = output_builder
        .build(spec.output_vars(), spec.aux_vars(), None)
        .await
        .expect("limited dataflow output pipeline should open");
    let runtime = DataflowRuntimeBuilder::<CheckedDsrvSpecification>::new()
        .execution_policy(ExecutionPolicy::Buffered)
        .quickening(false)
        .executor(executor)
        .model(spec)
        .output_writer(writer)
        .input(input_stream)
        .build()
        .await;
    runtime.run().await.expect("Error running monitor");
}

/// Run the checked dataflow interpreter through its scheduler-plan quickening tier.
pub async fn monitor_outputs_quickened_dataflow(
    executor: Rc<LocalExecutor<'static>>,
    spec: CheckedDsrvSpecification,
    input_stream: InputStream<Value>,
) {
    let output_builder = OutputBackendBuilder::new(OutputBackendConfig::null());
    let writer = output_builder
        .build(spec.output_vars(), spec.aux_vars(), None)
        .await
        .expect("quickened dataflow output pipeline should open");
    let runtime = DataflowRuntimeBuilder::<CheckedDsrvSpecification>::new()
        .execution_policy(ExecutionPolicy::Buffered)
        .executor(executor)
        .model(spec)
        .output_writer(writer)
        .input(input_stream)
        .build()
        .await;
    runtime.run().await.expect("Error running monitor");
}

/// Run a fixed number of checked dataflow outputs through scheduler-plan quickening.
pub async fn monitor_outputs_quickened_dataflow_limited(
    executor: Rc<LocalExecutor<'static>>,
    spec: CheckedDsrvSpecification,
    input_stream: InputStream<Value>,
    limit: usize,
) {
    let output_builder = OutputBackendBuilder::new(OutputBackendConfig::limited_null(limit));
    let writer = output_builder
        .build(spec.output_vars(), spec.aux_vars(), None)
        .await
        .expect("limited quickened dataflow output pipeline should open");
    let runtime = DataflowRuntimeBuilder::<CheckedDsrvSpecification>::new()
        .execution_policy(ExecutionPolicy::Buffered)
        .executor(executor)
        .model(spec)
        .output_writer(writer)
        .input(input_stream)
        .build()
        .await;
    runtime.run().await.expect("Error running monitor");
}

/// Run the same checked dataflow runtime with native compilation after the common dashboard
/// hotness threshold.
#[cfg(feature = "jit")]
pub async fn monitor_outputs_jit_dataflow(
    executor: Rc<LocalExecutor<'static>>,
    spec: CheckedDsrvSpecification,
    input_stream: InputStream<Value>,
) {
    let output_builder = OutputBackendBuilder::new(OutputBackendConfig::null());
    let writer = output_builder
        .build(spec.output_vars(), spec.aux_vars(), None)
        .await
        .expect("JIT dataflow output pipeline should open");
    let runtime = DataflowRuntimeBuilder::<CheckedDsrvSpecification>::new()
        .execution_policy(ExecutionPolicy::Buffered)
        .jit(crate::dataflow::JitConfig::after_events(
            KEY_BENCHMARK_JIT_HOTNESS_EVENTS,
        ))
        .executor(executor)
        .model(spec)
        .output_writer(writer)
        .input(input_stream)
        .build()
        .await;
    runtime.run().await.expect("Error running monitor");
}

/// Run a fixed number of checked dataflow outputs with native compilation after the common
/// dashboard hotness threshold.
#[cfg(feature = "jit")]
pub async fn monitor_outputs_jit_dataflow_limited(
    executor: Rc<LocalExecutor<'static>>,
    spec: CheckedDsrvSpecification,
    input_stream: InputStream<Value>,
    limit: usize,
) {
    let output_builder = OutputBackendBuilder::new(OutputBackendConfig::limited_null(limit));
    let writer = output_builder
        .build(spec.output_vars(), spec.aux_vars(), None)
        .await
        .expect("limited JIT dataflow output pipeline should open");
    let runtime = DataflowRuntimeBuilder::<CheckedDsrvSpecification>::new()
        .execution_policy(ExecutionPolicy::Buffered)
        .jit(crate::dataflow::JitConfig::after_events(
            KEY_BENCHMARK_JIT_HOTNESS_EVENTS,
        ))
        .executor(executor)
        .model(spec)
        .output_writer(writer)
        .input(input_stream)
        .build()
        .await;
    runtime.run().await.expect("Error running monitor");
}

pub async fn monitor_outputs_untyped_reconf_limited(
    executor: Rc<LocalExecutor<'static>>,
    spec: DsrvSpecification,
    input_source: InputSource,
    output_pipeline_builder: OutputBackendBuilder,
    use_context_transfer: bool,
) {
    let builder: ReconfSemiSyncRuntimeBuilder<SemiSyncValueConfig, UntimedDsrvSemantics> =
        ReconfSemiSyncRuntimeBuilder::new()
            .parse_spec(|source| source.parse().map_err(anyhow::Error::from))
            .executor(executor)
            .model(spec)
            .input_pipeline(InputPipeline::new(input_source))
            .output_builder(output_pipeline_builder)
            .reconf_topic(RECONF_TOPIC.into())
            .use_context_transfer(use_context_transfer);
    let monitor = Box::new(builder).build().await;
    monitor.run().await.expect("Error running monitor");
}

/// Run the untyped reconfigurable dataflow runtime through the benchmark input harness.
pub async fn monitor_outputs_untyped_dataflow_reconf_limited(
    executor: Rc<LocalExecutor<'static>>,
    spec: DsrvSpecification,
    input_pipeline: InputPipeline,
    output_backend_builder: OutputBackendBuilder,
    use_context_transfer: bool,
) {
    let transfer_policy = if use_context_transfer {
        ContextTransferPolicy::Compatible
    } else {
        ContextTransferPolicy::None
    };
    let builder = ReconfigurableDataflowRuntimeBuilder::<DsrvSpecification>::new()
        .parse_spec(|source| source.parse().map_err(anyhow::Error::from))
        .executor(executor)
        .model(spec)
        .input_pipeline(input_pipeline)
        .output_builder(output_backend_builder)
        .reconf_topic(RECONF_TOPIC)
        .context_transfer(transfer_policy)
        .quickening(false);
    let runtime = Box::new(builder).build().await;
    runtime.run().await.expect("Error running monitor");
}

/// Run the checked reconfigurable dataflow runtime without quickening or native compilation.
pub async fn monitor_outputs_dataflow_reconf_limited(
    executor: Rc<LocalExecutor<'static>>,
    spec: DsrvSpecification,
    input_pipeline: InputPipeline,
    output_backend_builder: OutputBackendBuilder,
    use_context_transfer: bool,
) {
    let transfer_policy = if use_context_transfer {
        ContextTransferPolicy::Compatible
    } else {
        ContextTransferPolicy::None
    };
    let checked = spec
        .to_string()
        .parse::<CheckedDsrvSpecification>()
        .expect("reconfiguration benchmark specification should type check");
    let builder = ReconfigurableDataflowRuntimeBuilder::<CheckedDsrvSpecification>::new()
        .parse_spec(|source| source.parse().map_err(anyhow::Error::from))
        .executor(executor)
        .model(checked)
        .input_pipeline(input_pipeline)
        .output_builder(output_backend_builder)
        .reconf_topic(RECONF_TOPIC)
        .context_transfer(transfer_policy)
        .quickening(false);
    let runtime = Box::new(builder).build().await;
    runtime.run().await.expect("Error running monitor");
}

/// Run the checked reconfigurable dataflow runtime through its quickened tier.
pub async fn monitor_outputs_quickened_dataflow_reconf_limited(
    executor: Rc<LocalExecutor<'static>>,
    spec: DsrvSpecification,
    input_pipeline: InputPipeline,
    output_backend_builder: OutputBackendBuilder,
    use_context_transfer: bool,
) {
    let transfer_policy = if use_context_transfer {
        ContextTransferPolicy::Compatible
    } else {
        ContextTransferPolicy::None
    };
    let checked = spec
        .to_string()
        .parse::<CheckedDsrvSpecification>()
        .expect("reconfiguration benchmark specification should type check");
    let builder = ReconfigurableDataflowRuntimeBuilder::<CheckedDsrvSpecification>::new()
        .parse_spec(|source| source.parse().map_err(anyhow::Error::from))
        .executor(executor)
        .model(checked)
        .input_pipeline(input_pipeline)
        .output_builder(output_backend_builder)
        .reconf_topic(RECONF_TOPIC)
        .context_transfer(transfer_policy);
    let runtime = Box::new(builder).build().await;
    runtime.run().await.expect("Error running monitor");
}

/// Run the checked reconfigurable dataflow runtime with native compilation.
#[cfg(feature = "jit")]
pub async fn monitor_outputs_jit_dataflow_reconf_limited(
    executor: Rc<LocalExecutor<'static>>,
    spec: DsrvSpecification,
    input_pipeline: InputPipeline,
    output_backend_builder: OutputBackendBuilder,
    use_context_transfer: bool,
) {
    let transfer_policy = if use_context_transfer {
        ContextTransferPolicy::Compatible
    } else {
        ContextTransferPolicy::None
    };
    let checked = spec
        .to_string()
        .parse::<CheckedDsrvSpecification>()
        .expect("reconfiguration benchmark specification should type check");
    let builder = ReconfigurableDataflowRuntimeBuilder::<CheckedDsrvSpecification>::new()
        .parse_spec(|source| source.parse().map_err(anyhow::Error::from))
        .executor(executor)
        .model(checked)
        .input_pipeline(input_pipeline)
        .output_builder(output_backend_builder)
        .reconf_topic(RECONF_TOPIC)
        .context_transfer(transfer_policy)
        .jit(crate::dataflow::JitConfig::after_events(
            KEY_BENCHMARK_JIT_HOTNESS_EVENTS,
        ));
    let runtime = Box::new(builder).build().await;
    runtime.run().await.expect("Error running monitor");
}

pub async fn monitor_outputs_untyped_async(
    executor: Rc<LocalExecutor<'static>>,
    spec: DsrvSpecification,
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
    spec: DsrvSpecification,
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
    spec: CheckedDsrvSpecification,
    input_stream: InputStream<Value>,
) {
    // Currently cannot be deduplicated since it includes the type
    // checking. The async runtime keeps independent named streams, so it uses
    // the drain adapter over the sink-based null backend.
    let output_builder = OutputBackendBuilder::<Value>::new(OutputBackendConfig::null());
    let writer = output_builder
        .build(spec.output_vars(), spec.aux_vars(), None)
        .await
        .expect("typed async output pipeline should open");
    let async_monitor =
        AsyncRuntimeBuilder::<CheckedValueConfig, CheckedUntimedDsrvSemantics>::new()
            .executor(executor.clone())
            .model(spec)
            .input(input_stream)
            .output_writer(writer)
            .build()
            .await;
    async_monitor.run().await.expect("Error running monitor");
}

pub fn input_source_dsrv_paper_bench(
    var_names: BTreeSet<VarName>,
) -> (InputSource, BTreeMap<VarName, FanoutSender<Value>>) {
    let mut tx_fans: BTreeMap<VarName, FanoutSender<Value>> = BTreeMap::new();
    let mut fanouts: BTreeMap<VarName, Rc<Fanout<Value>>> = BTreeMap::new();

    for name in var_names {
        let (tx, fan) = Fanout::new();
        fanouts.insert(name.clone(), fan);
        tx_fans.insert(name, tx);
    }
    let (tx_r, control) = Fanout::new();
    tx_fans.insert(RECONF_TOPIC.into(), tx_r);

    let input_source = crate::io::testing::input_source_with_control(fanouts, control);

    (input_source, tx_fans)
}

pub fn input_factory_dsrv_paper_bench(
    var_names: BTreeSet<VarName>,
) -> (InputPipeline, BTreeMap<VarName, FanoutSender<Value>>) {
    let (input_source, tx_fans) = input_source_dsrv_paper_bench(var_names);
    (InputPipeline::new(input_source), tx_fans)
}

pub fn output_builder_dsrv_paper_bench(
    _output_var_names: BTreeSet<VarName>,
    _ex: Rc<LocalExecutor<'static>>,
) -> (
    OutputBackendBuilder,
    bounded::Receiver<BTreeMap<VarName, Value>>,
) {
    let (out_tx, out_rx) = bounded::channel::<BTreeMap<VarName, Value>>(1024).into_split();
    let output_builder = OutputBackendBuilder::new(OutputBackendConfig::manual(out_tx));

    (output_builder, out_rx)
}
