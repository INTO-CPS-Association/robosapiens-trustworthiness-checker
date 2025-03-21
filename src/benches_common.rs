// This file defines the common functions used by the benchmarks.
// Dead code is allowed as it is only used when compiling benchmarks.

use std::collections::BTreeMap;
use std::rc::Rc;

use crate::LOLASpecification;
use crate::Monitor;
use crate::OutputStream;
use crate::Value;
use crate::VarName;
use crate::dep_manage::interface::DependencyManager;
use crate::io::testing::null_output_handler::NullOutputHandler;
use crate::lang::dynamic_lola::type_checker::TypedLOLASpecification;
use crate::runtime::constraints::runtime::ConstraintBasedMonitor;
use crate::runtime::constraints::runtime::ConstraintBasedRuntime;
use futures::StreamExt;
use smol::LocalExecutor;
use std::fmt::Debug;

pub fn to_typed_stream<T: TryFrom<Value, Error = ()> + Debug>(
    stream: OutputStream<Value>,
) -> OutputStream<T> {
    Box::pin(stream.map(|x| x.try_into().expect("Type error")))
}

pub async fn monitor_outputs_untyped_constraints(
    executor: Rc<LocalExecutor<'static>>,
    spec: LOLASpecification,
    mut input_streams: BTreeMap<VarName, OutputStream<Value>>,
    dep_manager: DependencyManager,
) {
    let output_handler = Box::new(NullOutputHandler::new(
        executor.clone(),
        spec.output_vars.clone(),
    ));
    let async_monitor = ConstraintBasedMonitor::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
        output_handler,
        dep_manager,
    );
    async_monitor.run().await;
}

pub async fn monitor_outputs_untyped_async(
    executor: Rc<LocalExecutor<'static>>,
    spec: LOLASpecification,
    mut input_streams: BTreeMap<VarName, OutputStream<Value>>,
    dep_manager: DependencyManager,
) {
    let output_handler = Box::new(NullOutputHandler::new(
        executor.clone(),
        spec.output_vars.clone(),
    ));
    let async_monitor = crate::runtime::asynchronous::AsyncMonitorRunner::<
        _,
        _,
        crate::semantics::UntimedLolaSemantics,
        crate::LOLASpecification,
    >::new(
        executor,
        spec,
        &mut input_streams,
        output_handler,
        dep_manager,
    );
    async_monitor.run().await;
}

pub async fn monitor_outputs_typed_async(
    executor: Rc<LocalExecutor<'static>>,
    spec: TypedLOLASpecification,
    mut input_streams: BTreeMap<VarName, OutputStream<Value>>,
    dep_manager: DependencyManager,
) {
    let output_handler = Box::new(NullOutputHandler::new(
        executor.clone(),
        spec.output_vars.clone(),
    ));
    let async_monitor = crate::runtime::asynchronous::AsyncMonitorRunner::<
        _,
        _,
        crate::semantics::TypedUntimedLolaSemantics,
        _,
    >::new(
        executor,
        spec,
        &mut input_streams,
        output_handler,
        dep_manager,
    );
    async_monitor.run().await;
}

pub async fn monitor_outputs_untyped_queuing(
    executor: Rc<LocalExecutor<'static>>,
    spec: LOLASpecification,
    mut input_streams: BTreeMap<VarName, OutputStream<Value>>,
    dep_manager: DependencyManager,
) {
    let output_handler = Box::new(NullOutputHandler::new(
        executor.clone(),
        spec.output_vars.clone(),
    ));
    let async_monitor = crate::runtime::queuing::QueuingMonitorRunner::<
        _,
        _,
        crate::semantics::UntimedLolaSemantics,
        crate::LOLASpecification,
    >::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
        output_handler,
        dep_manager,
    );
    async_monitor.run().await;
}

pub async fn monitor_outputs_typed_queuing(
    executor: Rc<LocalExecutor<'static>>,
    spec: TypedLOLASpecification,
    mut input_streams: BTreeMap<VarName, OutputStream<Value>>,
    dep_manager: DependencyManager,
) {
    let output_handler = Box::new(NullOutputHandler::new(
        executor.clone(),
        spec.output_vars.clone(),
    ));
    let async_monitor = crate::runtime::queuing::QueuingMonitorRunner::<
        _,
        _,
        crate::semantics::TypedUntimedLolaSemantics,
        _,
    >::new(
        executor,
        spec,
        &mut input_streams,
        output_handler,
        dep_manager,
    );
    async_monitor.run().await;
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
