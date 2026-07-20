use std::collections::BTreeMap;
use std::rc::Rc;

use async_trait::async_trait;
use futures::future::LocalBoxFuture;
use futures::pin_mut;
use futures::{FutureExt, StreamExt, select};
use smol::LocalExecutor;
use unsync::spsc;

use crate::core::{ExecutionPolicy, InputStream, OutputHandler, OutputStream, Runtime, Value};
use crate::dataflow::{DataflowCompileError, DataflowMonitor};
use crate::runtime::builder::RuntimeBuilder;
use crate::stream_utils::channel_to_output_stream;

const DATAFLOW_RUNTIME_BATCH_SIZE: usize = 256;
const DATAFLOW_OUTPUT_BATCH_CHANNEL_SIZE: usize = 1024;

pub struct DataflowRuntime {
    input_stream: InputStream<Value>,
    output_handler: Box<dyn OutputHandler<Val = Value>>,
    monitor: Result<DataflowMonitor, DataflowCompileError>,
    execution_policy: ExecutionPolicy,
}

pub struct DataflowRuntimeBuilder<S>
where
    S: 'static,
    DataflowMonitor: TryFrom<S, Error = DataflowCompileError>,
{
    model: Option<S>,
    input: Option<InputStream<Value>>,
    output: Option<Box<dyn OutputHandler<Val = Value>>>,
    execution_policy: ExecutionPolicy,
}

impl<S> DataflowRuntimeBuilder<S>
where
    S: 'static,
    DataflowMonitor: TryFrom<S, Error = DataflowCompileError>,
{
    pub fn execution_policy(self, execution_policy: ExecutionPolicy) -> Self {
        Self {
            execution_policy,
            ..self
        }
    }

    pub fn controlled_input(self, input: InputStream<Value>) -> (Self, crate::io::InputController) {
        let (input, controller) = crate::io::controlled(input);
        (
            self.execution_policy(ExecutionPolicy::Synchronous)
                .input(input),
            controller,
        )
    }
}

impl<S> RuntimeBuilder<S, Value> for DataflowRuntimeBuilder<S>
where
    S: 'static,
    DataflowMonitor: TryFrom<S, Error = DataflowCompileError>,
{
    type Runtime = DataflowRuntime;

    fn new() -> Self {
        Self {
            model: None,
            input: None,
            output: None,
            execution_policy: ExecutionPolicy::Buffered,
        }
    }

    fn executor(self, _executor: Rc<LocalExecutor<'static>>) -> Self {
        self
    }

    fn model(self, model: S) -> Self {
        Self {
            model: Some(model),
            ..self
        }
    }

    fn input(self, input: InputStream<Value>) -> Self {
        Self {
            input: Some(input),
            ..self
        }
    }

    fn output(self, output: Box<dyn OutputHandler<Val = Value>>) -> Self {
        Self {
            output: Some(output),
            ..self
        }
    }

    fn build(self) -> LocalBoxFuture<'static, Self::Runtime> {
        Box::pin(async move {
            let model = self.model.expect("Model not supplied");
            let monitor = DataflowMonitor::try_from(model);
            DataflowRuntime {
                input_stream: self.input.expect("Input stream not supplied"),
                output_handler: self.output.expect("Output handler not supplied"),
                monitor,
                execution_policy: self.execution_policy,
            }
        })
    }
}

#[async_trait(?Send)]
impl Runtime for DataflowRuntime {
    async fn run_boxed(mut self: Box<Self>) -> anyhow::Result<()> {
        let monitor = self.monitor?;
        let output_vars = monitor.output_vars().to_vec();
        let mut output_senders = Vec::with_capacity(output_vars.len());
        let output_streams = output_vars
            .iter()
            .map(|var| {
                let (sender, receiver) = spsc::channel(DATAFLOW_OUTPUT_BATCH_CHANNEL_SIZE);
                output_senders.push(sender);
                let batches = channel_to_output_stream(receiver);
                (
                    var.clone(),
                    Box::pin(batches.flat_map(futures::stream::iter)) as OutputStream<Value>,
                )
            })
            .collect::<BTreeMap<_, _>>();

        self.output_handler.provide_streams(output_streams);
        let output_fut = self.output_handler.run().fuse();
        let engine_fut = run_dataflow_engine(
            self.input_stream,
            monitor,
            output_senders,
            self.execution_policy,
        )
        .fuse();
        pin_mut!(output_fut, engine_fut);

        select! {
            output = output_fut => output,
            engine = engine_fut => {
                engine?;
                output_fut.await
            },
        }
    }
}

async fn run_dataflow_engine(
    mut input_stream: InputStream<Value>,
    monitor: DataflowMonitor,
    output_senders: Vec<spsc::Sender<Vec<Value>>>,
    execution_policy: ExecutionPolicy,
) -> anyhow::Result<()> {
    let mut engine = DataflowEngine::new(monitor, output_senders);
    let mut pending = 0;

    while let Some(batch) = input_stream.next().await {
        let batch = batch?;
        for tick in batch.ticks() {
            engine.evaluate_tick(tick)?;
            pending += 1;
            let flush = execution_policy == ExecutionPolicy::Synchronous
                || pending == DATAFLOW_RUNTIME_BATCH_SIZE;
            if flush {
                if !engine.flush().await {
                    return Ok(());
                }
                pending = 0;
            }
        }
    }

    if pending != 0 {
        let _ = engine.flush().await;
    }

    Ok(())
}

struct DataflowEngine {
    monitor: DataflowMonitor,
    output_senders: Vec<spsc::Sender<Vec<Value>>>,
    output_batches: Vec<Vec<Value>>,
    input_row: Vec<Value>,
    output_row: Vec<Value>,
    input_ids: BTreeMap<crate::VarName, usize>,
}

impl DataflowEngine {
    fn new(monitor: DataflowMonitor, output_senders: Vec<spsc::Sender<Vec<Value>>>) -> Self {
        let output_batches = output_senders
            .iter()
            .map(|_| Vec::with_capacity(DATAFLOW_RUNTIME_BATCH_SIZE))
            .collect();
        let input_row = vec![Value::NoVal; monitor.input_vars().len()];
        let output_row = vec![Value::NoVal; output_senders.len()];
        let input_ids = monitor
            .input_vars()
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, var)| (var, index))
            .collect();
        Self {
            monitor,
            output_senders,
            output_batches,
            input_row,
            output_row,
            input_ids,
        }
    }

    fn evaluate_tick(&mut self, events: &[crate::core::InputEvent<Value>]) -> anyhow::Result<()> {
        let mut slots = Vec::with_capacity(events.len());
        for event in events {
            let Some(&slot) = self.input_ids.get(&event.var) else {
                return Err(anyhow::anyhow!(
                    "input stream emitted undeclared dataflow variable `{}`",
                    event.var
                ));
            };
            slots.push(slot);
            self.input_row[slot] = event.value.clone();
        }

        let result = self.monitor.evaluate(&self.input_row, &mut self.output_row);
        for slot in slots {
            self.input_row[slot] = Value::NoVal;
        }
        result?;
        self.push_outputs();
        Ok(())
    }

    fn push_outputs(&mut self) {
        for (batch, value) in self.output_batches.iter_mut().zip(&self.output_row) {
            batch.push(value.clone());
        }
    }

    async fn flush(&mut self) -> bool {
        for (sender, batch) in self.output_senders.iter_mut().zip(&mut self.output_batches) {
            if sender.send(std::mem::take(batch)).await.is_err() {
                return false;
            }
            *batch = Vec::with_capacity(DATAFLOW_RUNTIME_BATCH_SIZE);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::rc::Rc;

    use macro_rules_attribute::apply;
    use smol::LocalExecutor;

    use crate::VarName;
    use crate::io::map;
    use crate::io::testing::LimitedNullOutputHandler;
    use crate::io::testing::ManualOutputHandler;
    use crate::lang::dsrv::type_checker::{type_check, type_check_gradual};
    use crate::{DsrvSpecification, Value, async_test, dsrv_specification};

    use super::*;

    async fn run_dataflow_runtime(
        executor: Rc<LocalExecutor<'static>>,
        spec_src: &'static str,
        inputs: BTreeMap<VarName, Vec<Value>>,
        limit: usize,
    ) {
        let mut spec_src = spec_src;
        let spec = dsrv_specification(&mut spec_src).unwrap();
        let output_handler = Box::new(LimitedNullOutputHandler::new(
            executor.clone(),
            spec.output_vars().clone(),
            limit,
        ));
        let runtime = DataflowRuntimeBuilder::<DsrvSpecification>::new()
            .executor(executor.clone())
            .model(spec)
            .input(map::input_stream(inputs))
            .output(output_handler)
            .build()
            .await;

        runtime.run().await.expect("dataflow runtime should run");
    }

    #[apply(async_test)]
    async fn dataflow_runtime_evaluates_simple_arithmetic(executor: Rc<LocalExecutor<'static>>) {
        run_dataflow_runtime(
            executor,
            "in x\nin y\nout z\nz = x + y",
            BTreeMap::from([
                (
                    VarName::new("x"),
                    vec![Value::Int(1), Value::Int(2), Value::Int(3)],
                ),
                (
                    VarName::new("y"),
                    vec![Value::Int(10), Value::Int(20), Value::Int(30)],
                ),
            ]),
            3,
        )
        .await;
    }

    #[apply(async_test)]
    async fn dataflow_synchronous_controller_acknowledges_processed_ticks(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let mut spec_src = "in x\nout z\nz = x + 1";
        let spec = dsrv_specification(&mut spec_src).unwrap();
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars().clone(),
        ));
        let mut outputs = output_handler.get_output();
        let input = map::input_stream(BTreeMap::from([(
            VarName::new("x"),
            vec![Value::Int(1), Value::Int(2)],
        )]));
        let (builder, controller) = DataflowRuntimeBuilder::<DsrvSpecification>::new()
            .executor(executor)
            .model(spec)
            .controlled_input(input);
        let runtime = builder.output(output_handler).build().await;

        let control = async move {
            controller.advance().await.unwrap();
            let first = outputs.next().await.unwrap();
            assert_eq!(first.get(&VarName::new("z")), Some(&Value::Int(2)));
            controller.advance().await.unwrap();
            let second = outputs.next().await.unwrap();
            assert_eq!(second.get(&VarName::new("z")), Some(&Value::Int(3)));
        };
        let (result, ()) = futures::join!(runtime.run(), control);
        result.unwrap();
    }
    #[apply(async_test)]
    async fn dataflow_runtime_evaluates_recursive_accumulator(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        run_dataflow_runtime(
            executor,
            "in x\nout z\nz = default(z[1], 0) + x",
            BTreeMap::from([(
                VarName::new("x"),
                vec![
                    Value::Int(1),
                    Value::Int(2),
                    Value::Int(3),
                    Value::Int(4),
                    Value::Int(5),
                ],
            )]),
            5,
        )
        .await;
    }

    #[apply(async_test)]
    async fn typed_dataflow_runtime_evaluates_simple_arithmetic(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let mut spec_src = "in x: Int\nin y: Int\nout z: Int\nz = x + y";
        let spec = type_check(dsrv_specification(&mut spec_src).unwrap()).unwrap();
        let output_handler = Box::new(LimitedNullOutputHandler::new(
            executor.clone(),
            spec.output_vars().clone(),
            3,
        ));
        let runtime =
            DataflowRuntimeBuilder::<crate::lang::dsrv::ast::CheckedDsrvSpecification>::new()
                .executor(executor.clone())
                .model(spec)
                .input(map::input_stream(BTreeMap::from([
                    (VarName::new("x"), vec![1.into(), 2.into(), 3.into()]),
                    (VarName::new("y"), vec![10.into(), 20.into(), 30.into()]),
                ])))
                .output(output_handler)
                .build()
                .await;

        runtime
            .run()
            .await
            .expect("typed dataflow runtime should run");
    }

    #[apply(async_test)]
    async fn gradual_typed_dataflow_runtime_uses_value_fallback(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let mut spec_src = "in x: Any\nin y: Any\nout z: Any\nz = x + y";
        let spec = type_check_gradual(dsrv_specification(&mut spec_src).unwrap()).unwrap();
        let output_handler = Box::new(LimitedNullOutputHandler::new(
            executor.clone(),
            spec.output_vars().clone(),
            3,
        ));
        let runtime =
            DataflowRuntimeBuilder::<crate::lang::dsrv::ast::CheckedDsrvSpecification>::new()
                .executor(executor.clone())
                .model(spec)
                .input(map::input_stream(BTreeMap::from([
                    (VarName::new("x"), vec![1.into(), 2.into(), 3.into()]),
                    (VarName::new("y"), vec![10.into(), 20.into(), 30.into()]),
                ])))
                .output(output_handler)
                .build()
                .await;

        runtime
            .run()
            .await
            .expect("gradual typed dataflow runtime should run");
    }

    #[apply(async_test)]
    async fn gradual_typed_dataflow_runtime_casts_untyped_input(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let mut spec_src = "in x\nout z\nz = x + 1";
        let spec = type_check_gradual(dsrv_specification(&mut spec_src).unwrap()).unwrap();
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars().clone(),
        ));
        let outputs = output_handler.get_output();
        let runtime =
            DataflowRuntimeBuilder::<crate::lang::dsrv::ast::CheckedDsrvSpecification>::new()
                .executor(executor.clone())
                .model(spec)
                .input(map::input_stream(BTreeMap::from([(
                    VarName::new("x"),
                    vec![41.into(), 1.into()],
                )])))
                .output(output_handler)
                .build()
                .await;

        executor.spawn(runtime.run()).detach();
        let outputs = tc_testutils::streams::with_timeout(
            outputs.collect::<Vec<_>>(),
            5,
            "gradual typed dataflow cast output collection",
        )
        .await
        .expect("dataflow cast output collection should finish");

        assert_eq!(
            outputs,
            vec![
                BTreeMap::from([(VarName::new("z"), Value::Int(42))]),
                BTreeMap::from([(VarName::new("z"), Value::Int(2))]),
            ]
        );
    }

    #[apply(async_test)]
    async fn dataflow_runtime_drains_outputs_after_input_finishes(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let mut spec_src = "in x\nout z\nz = x + 10";
        let spec = dsrv_specification(&mut spec_src).unwrap();
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars().clone(),
        ));
        let outputs = output_handler.get_output();
        let runtime = DataflowRuntimeBuilder::<DsrvSpecification>::new()
            .executor(executor.clone())
            .model(spec)
            .input(map::input_stream(BTreeMap::from([(
                VarName::new("x"),
                vec![Value::Int(1), Value::Int(2)],
            )])))
            .output(output_handler)
            .build()
            .await;

        executor.spawn(runtime.run()).detach();
        let outputs = tc_testutils::streams::with_timeout(
            outputs.collect::<Vec<_>>(),
            5,
            "dataflow manual output collection",
        )
        .await
        .expect("dataflow output collection should finish");

        assert_eq!(
            outputs,
            vec![
                BTreeMap::from([(VarName::new("z"), Value::Int(11))]),
                BTreeMap::from([(VarName::new("z"), Value::Int(12))]),
            ]
        );
    }

    #[apply(async_test)]
    async fn dataflow_runtime_flushes_full_and_partial_internal_batches(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let mut spec_src = "in x\nout z\nz = x + 1";
        let spec = dsrv_specification(&mut spec_src).unwrap();
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars().clone(),
        ));
        let outputs = output_handler.get_output();
        let runtime = DataflowRuntimeBuilder::<DsrvSpecification>::new()
            .executor(executor.clone())
            .model(spec)
            .input(map::input_stream(BTreeMap::from([(
                VarName::new("x"),
                (0..300).map(Value::Int).collect(),
            )])))
            .output(output_handler)
            .build()
            .await;

        executor.spawn(runtime.run()).detach();
        let outputs = tc_testutils::streams::with_timeout(
            outputs.collect::<Vec<_>>(),
            5,
            "privately batched dataflow output collection",
        )
        .await
        .expect("privately batched output collection should finish");

        assert_eq!(outputs.len(), 300);
        assert_eq!(outputs[0][&VarName::new("z")], Value::Int(1));
        assert_eq!(outputs[299][&VarName::new("z")], Value::Int(300));
    }

    #[apply(async_test)]
    async fn dataflow_runtime_preserves_logical_ticks_inside_transport_batches(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let mut spec_src = "in x\nin y\nout z\nz = 42";
        let spec = dsrv_specification(&mut spec_src).unwrap();
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars().clone(),
        ));
        let outputs = output_handler.get_output();
        let simultaneous = crate::InputBatch::step(vec![
            crate::InputEvent::new("x".into(), Value::Int(1)),
            crate::InputEvent::new("y".into(), Value::Int(10)),
        ]);
        let independent = Ok(crate::InputBatch::events(vec![
            crate::InputEvent::new("x".into(), Value::Int(2)),
            crate::InputEvent::new("y".into(), Value::Int(20)),
        ]));
        let input = Box::pin(futures::stream::iter([simultaneous, independent]));
        let runtime = DataflowRuntimeBuilder::<DsrvSpecification>::new()
            .executor(executor.clone())
            .model(spec)
            .input(input)
            .output(output_handler)
            .build()
            .await;

        executor.spawn(runtime.run()).detach();
        let outputs = tc_testutils::streams::with_timeout(
            outputs.collect::<Vec<_>>(),
            5,
            "dataflow logical tick output collection",
        )
        .await
        .expect("dataflow logical tick output collection should finish");

        assert_eq!(outputs.len(), 3);
        assert!(
            outputs
                .iter()
                .all(|row| row[&VarName::new("z")] == Value::Int(42))
        );
    }
}
