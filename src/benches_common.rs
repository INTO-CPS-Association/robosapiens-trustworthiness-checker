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
use crate::core::OutputHandler;
use crate::core::Runtime;
use crate::core::RuntimeSpec;
use crate::core::Semantics;
use crate::io::InputStreamFactory;
use crate::io::testing::{LimitedNullOutputHandler, NullOutputHandler};
use crate::io::{OutputHandlerBuilder, OutputHandlerSpec};
use crate::lang::dsrv::ast::CheckedDsrvSpecification;
use crate::lang::dsrv::lalr_parser::LALRParser;
use crate::lang::dsrv::lalr_parser::parse_str;
use crate::lang::dsrv::type_checker::type_check;
use crate::runtime::asynchronous::AsyncRuntimeBuilder;
use crate::runtime::builder::RuntimeBuilder;
use crate::runtime::builder::{CheckedValueConfig, SemiSyncValueConfig};
use crate::runtime::dataflow::DataflowRuntimeBuilder;
use crate::runtime::reconfigurable_semi_sync::ReconfSemiSyncRuntimeBuilder;
use crate::semantics::{CheckedUntimedDsrvSemantics, UntimedDsrvSemantics};
use crate::stream_utils::Fanout;
use crate::stream_utils::FanoutSender;

use async_unsync::bounded;
use smol::LocalExecutor;

pub const RECONF_TOPIC: &str = "R";

pub fn function_binding_benchmark(terms: usize, checked: bool) -> impl FnMut() -> usize {
    assert!(terms > 0);
    let expression = std::iter::repeat_n("x", terms)
        .collect::<Vec<_>>()
        .join(" + ");
    let source = format!("in n: Int\nout result: Int\nresult = (\\x: Int -> {expression})(n)");
    let parsed = parse_str(&source).expect("function binding fixture should parse");
    let runtime_expr = if checked {
        type_check(parsed)
            .expect("function binding fixture should type check")
            .var_expr(&VarName::new("result"))
            .unwrap()
            .expr()
            .clone()
    } else {
        parsed.exprs()[&VarName::new("result")].clone()
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

    let output_handler = Box::new(NullOutputHandler::new(
        executor.clone(),
        spec.output_vars().clone(),
    ));

    let monitor = DataflowRuntimeBuilder::<CheckedDsrvSpecification>::new()
        .execution_policy(ExecutionPolicy::Buffered)
        .executor(executor)
        .model(spec)
        .output(output_handler)
        .input(input_stream)
        .build()
        .await;
    monitor.run().await.expect("Error running monitor");
}

pub async fn monitor_outputs_untyped_reconf_limited(
    executor: Rc<LocalExecutor<'static>>,
    spec: DsrvSpecification,
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
    // checking
    let output_handler = Box::new(NullOutputHandler::new(
        executor.clone(),
        spec.output_vars().clone(),
    ));
    let async_monitor =
        AsyncRuntimeBuilder::<CheckedValueConfig, CheckedUntimedDsrvSemantics<LALRParser>>::new()
            .executor(executor.clone())
            .model(spec)
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
