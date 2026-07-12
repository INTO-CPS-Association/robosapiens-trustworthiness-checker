use std::{collections::BTreeMap, fmt::Debug, rc::Rc, time::Duration};

use anyhow::{Context, anyhow};
use async_trait::async_trait;

use futures::StreamExt;
use futures::future::LocalBoxFuture;
use mstlo::{
    Algorithm, DelayedQualitative, DelayedQuantitative, EagerQualitative, FormulaDefinition,
    RobustnessInterval, RobustnessSemantics, Rosi, Semantics, Step, StlMonitor,
    SynchronizationStrategy, Variables,
};
use smol::LocalExecutor;

use crate::{
    ExecutionPolicy, InputStream, OutputStream, Runtime, Value, VarName, core::OutputHandler,
    lang::mstlo::MstloSpecification, runtime::builder::RuntimeBuilder, stream_utils,
};

const MSTLO_OUTPUT_CHANNEL_SIZE: usize = 1024;

trait MstloOutputValue {
    fn into_value(self) -> Value;
}

impl MstloOutputValue for f64 {
    fn into_value(self) -> Value {
        Value::Float(self)
    }
}

impl MstloOutputValue for bool {
    fn into_value(self) -> Value {
        Value::Bool(self)
    }
}

impl MstloOutputValue for RobustnessInterval {
    fn into_value(self) -> Value {
        Value::Map(BTreeMap::from([
            ("lower".into(), Value::Float(self.0)),
            ("upper".into(), Value::Float(self.1)),
        ]))
    }
}

struct MstloRuntime<RS> {
    _executor: Rc<LocalExecutor<'static>>,
    input_vars: Vec<VarName>,
    monitors: BTreeMap<VarName, StlMonitor<f64, RS>>,
    input_stream: InputStream<Value>,
    output_handler: Box<dyn OutputHandler<Val = Value>>,
    execution_policy: ExecutionPolicy,
}

struct MstloInputState<'a, RS> {
    signal_names: &'a BTreeMap<VarName, &'static str>,
    monitors: &'a mut BTreeMap<VarName, StlMonitor<f64, RS>>,
    outputs: BTreeMap<VarName, MstloOutput>,
    blocked: bool,
}

struct MstloOutput {
    sender: unsync::spsc::Sender<Value>,
    pending: Vec<Value>,
}

impl MstloOutput {
    fn push(&mut self, value: Value) -> bool {
        if self.pending.is_empty() {
            if let Err(unsync::spsc::SendError(value)) = self.sender.try_send(value) {
                self.pending.push(value);
                return true;
            }
        } else {
            self.pending.push(value);
        }
        false
    }
}

pub struct MstloRuntimeBuilder {
    executor: Option<Rc<LocalExecutor<'static>>>,
    formula: Option<MstloSpecification>,
    algorithm: Algorithm,
    semantics: Semantics,
    synchronization_strategy: SynchronizationStrategy,
    variables: Variables,
    input: Option<InputStream<Value>>,
    output: Option<Box<dyn OutputHandler<Val = Value>>>,
    execution_policy: ExecutionPolicy,
}

impl MstloRuntimeBuilder {
    pub fn execution_policy(mut self, execution_policy: ExecutionPolicy) -> Self {
        self.execution_policy = execution_policy;
        self
    }

    pub fn controlled_input(self, input: InputStream<Value>) -> (Self, crate::io::InputController) {
        let (input, controller) = crate::io::controlled(input);
        (
            self.execution_policy(ExecutionPolicy::Synchronous)
                .input(input),
            controller,
        )
    }

    pub fn algorithm(mut self, algorithm: Algorithm) -> Self {
        self.algorithm = algorithm;
        self
    }

    pub fn semantics(mut self, semantics: Semantics) -> Self {
        self.semantics = semantics;
        self
    }

    pub fn synchronization_strategy(mut self, strategy: SynchronizationStrategy) -> Self {
        self.synchronization_strategy = strategy;
        self
    }

    pub fn variables(mut self, variables: Variables) -> Self {
        self.variables = variables;
        self
    }

    fn monitor_builder(
        formula: FormulaDefinition,
        algorithm: Algorithm,
        synchronization_strategy: SynchronizationStrategy,
        variables: Variables,
    ) -> mstlo::StlMonitorBuilder<f64, f64> {
        StlMonitor::builder()
            .formula(formula)
            .algorithm(algorithm)
            .synchronization_strategy(synchronization_strategy)
            .variables(variables)
    }

    fn runtime<RS>(
        executor: Rc<LocalExecutor<'static>>,
        input_vars: Vec<VarName>,
        monitors: BTreeMap<VarName, StlMonitor<f64, RS>>,
        input_stream: InputStream<Value>,
        output_handler: Box<dyn OutputHandler<Val = Value>>,
        execution_policy: ExecutionPolicy,
    ) -> Box<dyn Runtime>
    where
        RS: RobustnessSemantics + MstloOutputValue + Debug + 'static,
    {
        Box::new(MstloRuntime {
            _executor: executor,
            input_vars,
            monitors,
            input_stream,
            output_handler,
            execution_policy,
        })
    }
}

impl RuntimeBuilder<MstloSpecification, Value> for MstloRuntimeBuilder {
    type Runtime = Box<dyn Runtime>;

    fn new() -> Self {
        Self {
            executor: None,
            formula: None,
            algorithm: Algorithm::default(),
            semantics: Semantics::default(),
            synchronization_strategy: SynchronizationStrategy::default(),
            variables: Variables::new(),
            input: None,
            output: None,
            execution_policy: ExecutionPolicy::Buffered,
        }
    }

    fn executor(mut self, ex: Rc<LocalExecutor<'static>>) -> Self {
        self.executor = Some(ex);
        self
    }

    fn model(mut self, model: MstloSpecification) -> Self {
        self.formula = Some(model);
        self
    }

    fn input(mut self, input: InputStream<Value>) -> Self {
        self.input = Some(input);
        self
    }

    fn output(mut self, output: Box<dyn OutputHandler<Val = Value>>) -> Self {
        self.output = Some(output);
        self
    }

    fn build(self) -> LocalBoxFuture<'static, Self::Runtime> {
        Box::pin(async move {
            let executor = self.executor.expect("MSTLO runtime executor must be set");
            let formulae = self.formula.expect("MSTLO formula/spec must be set");
            let input_vars = formulae.var_names().to_vec();
            let formulae = formulae.into_formulae();
            let algorithm = self.algorithm;
            let semantics = self.semantics;
            let synchronization_strategy = self.synchronization_strategy;
            let variables = self.variables;
            let input_stream = self.input.expect("MSTLO input stream must be set");
            let output_handler = self.output.expect("MSTLO output handler must be set");
            let execution_policy = self.execution_policy;

            match semantics {
                Semantics::DelayedQuantitative => {
                    let monitors = formulae
                        .into_iter()
                        .map(|(name, formula)| {
                            let monitor = Self::monitor_builder(
                                formula,
                                algorithm,
                                synchronization_strategy,
                                variables.clone(),
                            )
                            .semantics(DelayedQuantitative)
                            .build()
                            .expect("Failed to build MSTLO monitor");
                            (name, monitor)
                        })
                        .collect();
                    Self::runtime(
                        executor,
                        input_vars,
                        monitors,
                        input_stream,
                        output_handler,
                        execution_policy,
                    )
                }
                Semantics::DelayedQualitative => {
                    let monitors = formulae
                        .into_iter()
                        .map(|(name, formula)| {
                            let monitor = Self::monitor_builder(
                                formula,
                                algorithm,
                                synchronization_strategy,
                                variables.clone(),
                            )
                            .semantics(DelayedQualitative)
                            .build()
                            .expect("Failed to build MSTLO monitor");
                            (name, monitor)
                        })
                        .collect();
                    Self::runtime(
                        executor,
                        input_vars,
                        monitors,
                        input_stream,
                        output_handler,
                        execution_policy,
                    )
                }
                Semantics::EagerQualitative => {
                    let monitors = formulae
                        .into_iter()
                        .map(|(name, formula)| {
                            let monitor = Self::monitor_builder(
                                formula,
                                algorithm,
                                synchronization_strategy,
                                variables.clone(),
                            )
                            .semantics(EagerQualitative)
                            .build()
                            .expect("Failed to build MSTLO monitor");
                            (name, monitor)
                        })
                        .collect();
                    Self::runtime(
                        executor,
                        input_vars,
                        monitors,
                        input_stream,
                        output_handler,
                        execution_policy,
                    )
                }
                Semantics::RobustnessInterval => {
                    let monitors = formulae
                        .into_iter()
                        .map(|(name, formula)| {
                            let monitor = Self::monitor_builder(
                                formula,
                                algorithm,
                                synchronization_strategy,
                                variables.clone(),
                            )
                            .semantics(Rosi)
                            .build()
                            .expect("Failed to build MSTLO monitor");
                            (name, monitor)
                        })
                        .collect();
                    Self::runtime(
                        executor,
                        input_vars,
                        monitors,
                        input_stream,
                        output_handler,
                        execution_policy,
                    )
                }
            }
        })
    }
}

impl<RS> MstloRuntime<RS>
where
    RS: MstloOutputValue,
{
    #[inline(always)]
    fn output_value(timestamp: Duration, value: RS) -> anyhow::Result<Value> {
        let time = i64::try_from(timestamp.as_millis())
            .context("MSTLO output timestamp does not fit in i64 milliseconds")?;

        Ok(Value::Map(BTreeMap::from([
            ("time".into(), Value::Int(time)),
            ("value".into(), value.into_value()),
        ])))
    }

    #[inline(always)]
    fn parse_input_time(value: &Value, context: &'static str) -> anyhow::Result<u64> {
        match value {
            Value::Int(time) => u64::try_from(*time)
                .with_context(|| format!("MSTLO input `{context}` must be a non-negative integer")),
            other => Err(anyhow!(
                "MSTLO input `{context}` must be an integer, got {other:?}"
            )),
        }
    }

    #[inline(always)]
    fn parse_input_number(value: &Value, context: &'static str) -> anyhow::Result<f64> {
        match value {
            Value::Float(value) => Ok(*value),
            Value::Int(value) => Ok(*value as f64),
            other => Err(anyhow!(
                "MSTLO input `{context}` must be numeric, got {other:?}"
            )),
        }
    }

    fn signal_names(input_vars: &[VarName]) -> BTreeMap<VarName, &'static str> {
        input_vars
            .iter()
            .map(|name| {
                // Leak here due to requirement to provide a static string.
                let signal = Box::leak(name.name().into_boxed_str()) as &'static str;
                (name.clone(), signal)
            })
            .collect()
    }

    #[inline(always)]
    fn step_from_input_value(
        signal_names: &BTreeMap<VarName, &'static str>,
        name: &VarName,
        value: &Value,
    ) -> anyhow::Result<Option<Step<f64>>> {
        if matches!(value, Value::NoVal) {
            return Ok(None);
        }

        let (timestamp, value) = Self::parse_input_value(value)
            .with_context(|| format!("Invalid MSTLO input for variable `{name}`"))?;
        let signal = signal_names
            .get(name)
            .copied()
            .ok_or_else(|| anyhow!("MSTLO input for unknown variable `{name}`"))?;
        Ok(Some(Step::new(signal, value, timestamp)))
    }

    #[inline(always)]
    fn parse_input_value(value: &Value) -> anyhow::Result<(Duration, f64)> {
        let (time, value) = match value {
            // Compact fast path for in-memory inputs and benchmarks: [time_ms, value].
            // This avoids constructing and string-key probing a BTreeMap for every sample.
            Value::List(values) => {
                let [time, value]: &[Value; 2] = values.as_slice().try_into().map_err(|_| {
                    anyhow!(
                        "MSTLO compact input value must be a two-element list `[time_ms, value]`"
                    )
                })?;
                (
                    Self::parse_input_time(time, "time_ms")?,
                    Self::parse_input_number(value, "value")?,
                )
            }
            // Backwards-compatible external representation.
            Value::Map(map) => {
                let time = map
                    .get("time")
                    .ok_or_else(|| anyhow!("MSTLO input map is missing `time` field"))?;
                let value = map
                    .get("value")
                    .ok_or_else(|| anyhow!("MSTLO input map is missing `value` field"))?;
                (
                    Self::parse_input_time(time, "time")?,
                    Self::parse_input_number(value, "value")?,
                )
            }
            other => {
                return Err(anyhow!(
                    "MSTLO input value must be either `[time_ms, value]` or a map with `time` and `value` fields, got {other:?}"
                ));
            }
        };

        Ok((Duration::from_millis(time), value))
    }
}

impl<RS> MstloInputState<'_, RS>
where
    RS: RobustnessSemantics + MstloOutputValue + Debug + 'static,
{
    fn process_event(&mut self, event: &crate::InputEvent<Value>) -> anyhow::Result<()> {
        let Some(step) =
            MstloRuntime::<RS>::step_from_input_value(self.signal_names, &event.var, &event.value)?
        else {
            return Ok(());
        };
        for (formula_name, monitor) in self.monitors.iter_mut() {
            let output = monitor.update(&step);
            let destination = self.outputs.get_mut(formula_name).ok_or_else(|| {
                anyhow!("Missing output stream for MSTLO formula `{formula_name}`")
            })?;
            for verdict in output.into_verdicts() {
                let value = MstloRuntime::<RS>::output_value(verdict.timestamp, verdict.value)?;
                self.blocked |= destination.push(value);
            }
        }
        Ok(())
    }

    fn process_step(&mut self, events: &[crate::InputEvent<Value>]) -> anyhow::Result<()> {
        let mut steps = Vec::with_capacity(events.len());
        for event in events {
            let Some(step) = MstloRuntime::<RS>::step_from_input_value(
                self.signal_names,
                &event.var,
                &event.value,
            )?
            else {
                continue;
            };
            steps.push(step);
        }
        // `mstlo` otherwise preserves the input iteration order for equal
        // timestamps, so use the signal name as a stable tie-breaker.
        steps.sort_by(|left, right| {
            left.timestamp
                .cmp(&right.timestamp)
                .then_with(|| left.signal.cmp(right.signal))
        });
        if steps.is_empty() {
            return Ok(());
        }

        for (formula_name, monitor) in self.monitors.iter_mut() {
            let destination = self.outputs.get_mut(formula_name).ok_or_else(|| {
                anyhow!("Missing output stream for MSTLO formula `{formula_name}`")
            })?;
            for step in &steps {
                for verdict in monitor.update(step).into_verdicts() {
                    let value = MstloRuntime::<RS>::output_value(verdict.timestamp, verdict.value)?;
                    self.blocked |= destination.push(value);
                }
            }
        }
        Ok(())
    }

    async fn flush_pending(&mut self) -> bool {
        if !self.blocked {
            return true;
        }
        for output in self.outputs.values_mut() {
            for value in output.pending.drain(..) {
                if output.sender.send(value).await.is_err() {
                    return false;
                }
            }
        }
        self.blocked = false;
        true
    }
}

#[async_trait(?Send)]
impl<RS> Runtime for MstloRuntime<RS>
where
    RS: RobustnessSemantics + MstloOutputValue + std::fmt::Debug + 'static,
{
    async fn run_boxed(mut self: Box<MstloRuntime<RS>>) -> anyhow::Result<()> {
        let signal_names = Self::signal_names(&self.input_vars);

        let (outputs, output_streams): (
            BTreeMap<VarName, MstloOutput>,
            BTreeMap<VarName, OutputStream<Value>>,
        ) = self
            .monitors
            .keys()
            .cloned()
            .map(|name| {
                let (sender, receiver) = unsync::spsc::channel(MSTLO_OUTPUT_CHANNEL_SIZE);
                let output_stream = stream_utils::channel_to_output_stream(receiver);
                (
                    (
                        name.clone(),
                        MstloOutput {
                            sender,
                            pending: Vec::new(),
                        },
                    ),
                    (name, output_stream),
                )
            })
            .unzip();
        self.output_handler.provide_streams(output_streams);
        let output_task = self._executor.spawn(self.output_handler.run());
        let mut input_batches = self.input_stream;

        let mut input = MstloInputState {
            signal_names: &signal_names,
            monitors: &mut self.monitors,
            outputs,
            blocked: false,
        };
        let input_res = async {
            while let Some(batch) = input_batches.next().await {
                let batch = batch?;
                for tick in batch.ticks() {
                    match tick {
                        [event] => {
                            input.process_event(event)?;
                            if self.execution_policy == ExecutionPolicy::Synchronous
                                && !input.flush_pending().await
                            {
                                return Ok(());
                            }
                        }
                        events => {
                            input.process_step(events)?;
                            let flush = self.execution_policy == ExecutionPolicy::Synchronous
                                || input.blocked;
                            if flush && !input.flush_pending().await {
                                return Ok(());
                            }
                        }
                    }
                }
                if self.execution_policy == ExecutionPolicy::Buffered
                    && !input.flush_pending().await
                {
                    return Ok(());
                }
            }
            let _ = input.flush_pending().await;
            Ok::<_, anyhow::Error>(())
        }
        .await;

        drop(input);

        input_res.context("Input stream/MSTLO processing failed")?;
        output_task.await.context("OutputHandler failed")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InputBatch, InputStream};

    use crate::async_test;
    use crate::io::testing::{ManualOutputHandler, NullOutputHandler};
    use crate::runtime::builder::RuntimeBuilder;
    use futures::{StreamExt, stream};
    use macro_rules_attribute::apply;
    use mstlo::FormulaDefinition;
    use smol::LocalExecutor;
    use std::{
        collections::{BTreeMap, BTreeSet},
        rc::Rc,
        time::Duration,
    };
    use tc_testutils::streams::with_timeout;

    fn failing_input() -> InputStream<Value> {
        Box::pin(stream::iter([Err(anyhow::anyhow!("input failed"))]))
    }

    fn static_input(inputs: BTreeMap<VarName, Vec<Value>>) -> anyhow::Result<InputStream<Value>> {
        let streams = inputs.into_iter().map(|(var, values)| {
            Box::pin(
                stream::iter(values).map(move |value| crate::InputEvent::new(var.clone(), value)),
            ) as OutputStream<crate::InputEvent<Value>>
        });
        let streams = futures::stream::select_all(streams);
        if streams.is_empty() {
            anyhow::bail!("no MSTLO input streams configured");
        }
        Ok(Box::pin(
            streams.map(|event| Ok(InputBatch::events(vec![event]))),
        ))
    }

    fn timed_value(time_ms: i64, value: f64) -> Value {
        Value::Map(BTreeMap::from([
            ("time".into(), Value::Int(time_ms)),
            ("value".into(), Value::Float(value)),
        ]))
    }

    fn compact_timed_value(time_ms: i64, value: f64) -> Value {
        Value::List(vec![Value::Int(time_ms), Value::Float(value)].into())
    }

    fn output_value(row: &BTreeMap<VarName, Value>, var: &str) -> (i64, f64) {
        let Value::Map(map) = row.get(&VarName::new(var)).expect("output var exists") else {
            panic!("MSTLO output must be a map");
        };
        let Value::Int(time) = map.get("time").expect("output has time") else {
            panic!("MSTLO output time must be an int");
        };
        let Value::Float(value) = map.get("value").expect("output has value") else {
            panic!("MSTLO output value must be a float");
        };
        (*time, *value)
    }

    #[test]
    fn parses_compact_and_map_mstlo_input_values() {
        assert_eq!(
            MstloRuntime::<f64>::parse_input_value(&compact_timed_value(10, 2.5)).unwrap(),
            (Duration::from_millis(10), 2.5),
        );
        assert_eq!(
            MstloRuntime::<f64>::parse_input_value(&timed_value(20, 3.5)).unwrap(),
            (Duration::from_millis(20), 3.5),
        );
    }

    #[apply(async_test)]
    async fn builder_runs_quantitative_formula(executor: Rc<LocalExecutor<'static>>) {
        let formula = MstloSpecification::single(
            VarName::new("out"),
            FormulaDefinition::GreaterThan("x", 5.0),
        );
        let input_stream = static_input(BTreeMap::from([(
            VarName::new("x"),
            vec![compact_timed_value(0, 7.0), compact_timed_value(10, 4.0)],
        )]))
        .unwrap();
        let output_var = VarName::new("out");
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            BTreeSet::from([output_var.clone()]),
        ));
        let outputs = output_handler.get_output();

        let runtime = MstloRuntimeBuilder::new()
            .executor(executor.clone())
            .model(formula)
            .input(input_stream)
            .output(output_handler)
            .build()
            .await;

        let outputs: Vec<_> = with_timeout(
            async {
                let (run_result, outputs) = futures::join!(runtime.run(), outputs.collect());
                run_result.unwrap();
                outputs
            },
            1,
            "mstlo outputs",
        )
        .await
        .unwrap();

        assert_eq!(outputs.len(), 2);
        assert_eq!(output_value(&outputs[0], "out"), (0, 2.0));
        assert_eq!(output_value(&outputs[1], "out"), (10, -1.0));
    }

    #[apply(async_test)]
    async fn synchronous_controller_acknowledges_processed_mstlo_ticks(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let formula = MstloSpecification::single(
            VarName::new("out"),
            FormulaDefinition::GreaterThan("x", 5.0),
        );
        let input = static_input(BTreeMap::from([(
            VarName::new("x"),
            vec![compact_timed_value(0, 7.0), compact_timed_value(10, 4.0)],
        )]))
        .unwrap();
        let output_var = VarName::new("out");
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            BTreeSet::from([output_var]),
        ));
        let mut outputs = output_handler.get_output();
        let (builder, controller) = MstloRuntimeBuilder::new()
            .executor(executor)
            .model(formula)
            .controlled_input(input);
        let runtime = builder.output(output_handler).build().await;

        let control = async move {
            controller.advance().await.unwrap();
            assert_eq!(
                output_value(&outputs.next().await.unwrap(), "out"),
                (0, 2.0)
            );
            controller.advance().await.unwrap();
            assert_eq!(
                output_value(&outputs.next().await.unwrap(), "out"),
                (10, -1.0)
            );
        };
        let (runtime, ()) = futures::join!(runtime.run(), control);
        runtime.unwrap();
    }

    #[apply(async_test)]
    async fn simultaneous_inputs_have_deterministic_signal_order(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        async fn run_with_order(
            executor: Rc<LocalExecutor<'static>>,
            vars: [&str; 2],
        ) -> Vec<BTreeMap<VarName, Value>> {
            let formula = MstloSpecification::single(
                VarName::new("out"),
                FormulaDefinition::And(
                    Box::new(FormulaDefinition::GreaterThan("x", 0.0)),
                    Box::new(FormulaDefinition::GreaterThan("y", 0.0)),
                ),
            );
            let (input_stream, mut input) = crate::io::testing::channel();
            input
                .send_step(
                    vars.into_iter()
                        .map(|var| {
                            crate::InputEvent::new(VarName::new(var), compact_timed_value(0, 1.0))
                        })
                        .collect(),
                )
                .await
                .unwrap();
            drop(input);

            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                BTreeSet::from([VarName::new("out")]),
            ));
            let outputs = output_handler.get_output();
            let runtime = MstloRuntimeBuilder::new()
                .executor(executor)
                .model(formula)
                .input(input_stream)
                .output(output_handler)
                .build()
                .await;

            let (result, outputs) = futures::join!(runtime.run(), outputs.collect());
            result.unwrap();
            outputs
        }

        let xy = run_with_order(executor.clone(), ["x", "y"]).await;
        let yx = run_with_order(executor, ["y", "x"]).await;
        assert_eq!(xy, yx);
        assert_eq!(xy.len(), 1);
        assert_eq!(output_value(&xy[0], "out"), (0, 1.0));
    }

    #[apply(async_test)]
    async fn runtime_propagates_input_errors(executor: Rc<LocalExecutor<'static>>) {
        let formula = MstloSpecification::single(
            VarName::new("out"),
            FormulaDefinition::GreaterThan("x", 5.0),
        );
        let output = Box::new(NullOutputHandler::new(
            executor.clone(),
            BTreeSet::from([VarName::new("out")]),
        ));
        let runtime = MstloRuntimeBuilder::new()
            .executor(executor)
            .model(formula)
            .input(failing_input())
            .output(output)
            .build()
            .await;

        let error = runtime.run().await.unwrap_err();
        assert!(format!("{error:#}").contains("input failed"));
    }

    #[apply(async_test)]
    async fn builder_runs_multiple_named_formulae(executor: Rc<LocalExecutor<'static>>) {
        let formula = MstloSpecification::new(BTreeMap::from([
            (VarName::new("gt"), FormulaDefinition::GreaterThan("x", 5.0)),
            (VarName::new("lt"), FormulaDefinition::LessThan("y", 3.0)),
        ]));
        assert_eq!(formula.var_names(), &[VarName::new("x"), VarName::new("y")]);

        let input_stream = static_input(BTreeMap::from([
            (
                VarName::new("x"),
                vec![timed_value(0, 7.0), timed_value(10, 4.0)],
            ),
            (
                VarName::new("y"),
                vec![timed_value(0, 2.0), timed_value(10, 5.0)],
            ),
        ]))
        .unwrap();
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            BTreeSet::from([VarName::new("gt"), VarName::new("lt")]),
        ));
        let outputs = output_handler.get_output();

        let runtime = MstloRuntimeBuilder::new()
            .executor(executor.clone())
            .model(formula)
            .input(input_stream)
            .output(output_handler)
            .build()
            .await;

        let outputs: Vec<_> = with_timeout(
            async {
                let (run_result, outputs) = futures::join!(runtime.run(), outputs.collect());
                run_result.unwrap();
                outputs
            },
            1,
            "mstlo multi outputs",
        )
        .await
        .unwrap();

        assert_eq!(outputs.len(), 2);
        assert_eq!(output_value(&outputs[0], "gt"), (0, 2.0));
        assert_eq!(output_value(&outputs[0], "lt"), (0, 1.0));
        assert_eq!(output_value(&outputs[1], "gt"), (10, -1.0));
        assert_eq!(output_value(&outputs[1], "lt"), (10, -2.0));
    }

    #[apply(async_test)]
    async fn builder_uses_variables_as_parameters_not_input_streams(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let formula = MstloSpecification::single(
            VarName::new("out"),
            FormulaDefinition::GreaterThanVar("x", "threshold"),
        );
        assert_eq!(formula.var_names(), &[VarName::new("x")]);

        let variables = Variables::new();
        variables.set("threshold", 2.0);

        let input_stream = static_input(BTreeMap::from([(
            VarName::new("x"),
            vec![timed_value(0, 3.5), timed_value(10, 1.0)],
        )]))
        .unwrap();
        let output_var = VarName::new("out");
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            BTreeSet::from([output_var.clone()]),
        ));
        let outputs = output_handler.get_output();

        let runtime = MstloRuntimeBuilder::new()
            .executor(executor.clone())
            .model(formula)
            .variables(variables)
            .input(input_stream)
            .output(output_handler)
            .build()
            .await;

        let outputs: Vec<_> = with_timeout(
            async {
                let (run_result, outputs) = futures::join!(runtime.run(), outputs.collect());
                run_result.unwrap();
                outputs
            },
            1,
            "mstlo variable outputs",
        )
        .await
        .unwrap();

        assert_eq!(outputs.len(), 2);
        assert_eq!(output_value(&outputs[0], "out"), (0, 1.5));
        assert_eq!(output_value(&outputs[1], "out"), (10, -1.0));
    }
}
