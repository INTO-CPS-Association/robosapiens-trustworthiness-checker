use std::{collections::BTreeMap, fmt::Debug, rc::Rc, time::Duration};

use anyhow::{Context, anyhow};
use async_stream::stream;
use async_trait::async_trait;

use futures::{StreamExt, future::LocalBoxFuture, stream::select_all};
use mstlo::{
    Algorithm, DelayedQualitative, DelayedQuantitative, EagerQualitative, FormulaDefinition,
    RobustnessInterval, RobustnessSemantics, Rosi, Semantics, Step, StlMonitor,
    SynchronizationStrategy, Variables,
};
use smol::LocalExecutor;

use crate::{
    InputProvider, OutputStream, Runtime, Value, VarName, core::OutputHandler,
    lang::mstlo::MstloFormula, runtime::builder::RuntimeBuilder, stream_utils,
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
    input_provider: Box<dyn InputProvider<Val = Value>>,
    output_handler: Box<dyn OutputHandler<Val = Value>>,
}

pub struct MstloRuntimeBuilder {
    executor: Option<Rc<LocalExecutor<'static>>>,
    formula: Option<MstloFormula>,
    algorithm: Algorithm,
    semantics: Semantics,
    synchronization_strategy: SynchronizationStrategy,
    variables: Variables,
    input: Option<Box<dyn InputProvider<Val = Value>>>,
    output: Option<Box<dyn OutputHandler<Val = Value>>>,
}

impl MstloRuntimeBuilder {
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
        input_provider: Box<dyn InputProvider<Val = Value>>,
        output_handler: Box<dyn OutputHandler<Val = Value>>,
    ) -> Box<dyn Runtime>
    where
        RS: RobustnessSemantics + MstloOutputValue + Debug + 'static,
    {
        Box::new(MstloRuntime {
            _executor: executor,
            input_vars,
            monitors,
            input_provider,
            output_handler,
        })
    }
}

impl RuntimeBuilder<MstloFormula, Value> for MstloRuntimeBuilder {
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
        }
    }

    fn executor(mut self, ex: Rc<LocalExecutor<'static>>) -> Self {
        self.executor = Some(ex);
        self
    }

    fn model(mut self, model: MstloFormula) -> Self {
        self.formula = Some(model);
        self
    }

    fn input(mut self, input: Box<dyn InputProvider<Val = Value>>) -> Self {
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
            let input_provider = self.input.expect("MSTLO input provider must be set");
            let output_handler = self.output.expect("MSTLO output handler must be set");

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
                        input_provider,
                        output_handler,
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
                        input_provider,
                        output_handler,
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
                        input_provider,
                        output_handler,
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
                        input_provider,
                        output_handler,
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
        value: Value,
    ) -> anyhow::Result<Option<Step<f64>>> {
        if value == Value::NoVal {
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
    fn parse_input_value(value: Value) -> anyhow::Result<(Duration, f64)> {
        let (time, value) = match value {
            // Compact fast path for in-memory providers/benchmarks: [time_ms, value].
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

#[async_trait(?Send)]
impl<RS> Runtime for MstloRuntime<RS>
where
    RS: RobustnessSemantics + MstloOutputValue + std::fmt::Debug + 'static,
{
    async fn run_boxed(mut self: Box<MstloRuntime<RS>>) -> anyhow::Result<()> {
        let signal_names = Self::signal_names(&self.input_vars);

        let mut input_streams = self
            .input_vars
            .iter()
            .cloned()
            .map(|name| {
                let mut input_stream = self
                    .input_provider
                    .var_stream(&name)
                    .unwrap_or_else(|| panic!("Failed to get var stream for {name}"));
                Box::pin(stream! {
                    while let Some(value) = input_stream.next().await {
                        yield (name.clone(), value);
                    }
                }) as OutputStream<(VarName, Value)>
            })
            .collect::<Vec<_>>();

        let input_stream = if input_streams.len() == 1 {
            input_streams.pop().expect("input stream exists")
        } else {
            Box::pin(select_all(input_streams)) as OutputStream<(VarName, Value)>
        };

        let mut input_control_stream = self.input_provider.control_stream().await;
        let input_control_task = self._executor.spawn(async move {
            while let Some(res) = input_control_stream.next().await {
                res?;
            }
            Ok::<(), anyhow::Error>(())
        });

        let (mut output_senders, output_streams): (
            BTreeMap<VarName, unsync::spsc::Sender<Value>>,
            BTreeMap<VarName, OutputStream<Value>>,
        ) = self
            .monitors
            .keys()
            .cloned()
            .map(|name| {
                let (sender, receiver) = unsync::spsc::channel(MSTLO_OUTPUT_CHANNEL_SIZE);
                let output_stream = stream_utils::channel_to_output_stream(receiver);
                ((name.clone(), sender), (name, output_stream))
            })
            .unzip();
        self.output_handler.provide_streams(output_streams);
        let output_task = self._executor.spawn(self.output_handler.run());

        let mut input_stream = input_stream;
        let input_res = async {
            while let Some((name, value)) = input_stream.next().await {
                let Some(step) = Self::step_from_input_value(&signal_names, &name, value)? else {
                    continue;
                };

                for (formula_name, monitor) in self.monitors.iter_mut() {
                    let output = monitor.update(&step);
                    let sender = output_senders.get_mut(formula_name).ok_or_else(|| {
                        anyhow!("Missing output stream for MSTLO formula `{formula_name}`")
                    })?;

                    for verdict in output.into_verdicts() {
                        let output_value = Self::output_value(verdict.timestamp, verdict.value)?;
                        sender.send(output_value).await.map_err(|_| {
                            anyhow!("Failed to send MSTLO output for formula `{formula_name}`")
                        })?;
                    }
                }
            }

            Ok::<(), anyhow::Error>(())
        }
        .await;

        drop(output_senders);

        input_res.context("InputProvider/MSTLO processing failed")?;
        output_task.await.context("OutputHandler failed")?;
        input_control_task
            .await
            .context("InputProvider control stream failed")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::async_test;
    use crate::io::testing::ManualOutputHandler;
    use crate::runtime::builder::RuntimeBuilder;
    use async_trait::async_trait;
    use futures::{StreamExt, stream};
    use macro_rules_attribute::apply;
    use mstlo::FormulaDefinition;
    use smol::LocalExecutor;
    use std::{
        collections::{BTreeMap, BTreeSet},
        rc::Rc,
    };
    use tc_testutils::streams::with_timeout;

    struct StaticInputProvider {
        streams: BTreeMap<VarName, Option<OutputStream<Value>>>,
    }

    impl StaticInputProvider {
        fn new(inputs: BTreeMap<VarName, Vec<Value>>) -> Self {
            let streams = inputs
                .into_iter()
                .map(|(name, values)| {
                    let stream: OutputStream<Value> = Box::pin(stream::iter(values));
                    (name, Some(stream))
                })
                .collect();

            Self { streams }
        }
    }

    #[async_trait(?Send)]
    impl InputProvider for StaticInputProvider {
        type Val = Value;

        fn var_stream(&mut self, var: &VarName) -> Option<OutputStream<Self::Val>> {
            self.streams.get_mut(var)?.take()
        }

        async fn control_stream(&mut self) -> OutputStream<anyhow::Result<()>> {
            Box::pin(stream::empty())
        }
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
            MstloRuntime::<f64>::parse_input_value(compact_timed_value(10, 2.5)).unwrap(),
            (Duration::from_millis(10), 2.5),
        );
        assert_eq!(
            MstloRuntime::<f64>::parse_input_value(timed_value(20, 3.5)).unwrap(),
            (Duration::from_millis(20), 3.5),
        );
    }

    #[apply(async_test)]
    async fn builder_runs_quantitative_atomic_formula(executor: Rc<LocalExecutor<'static>>) {
        let formula = MstloFormula::single(
            VarName::new("out"),
            FormulaDefinition::GreaterThan("x", 5.0),
        );
        let input_provider = StaticInputProvider::new(BTreeMap::from([(
            VarName::new("x"),
            vec![compact_timed_value(0, 7.0), compact_timed_value(10, 4.0)],
        )]));
        let output_var = VarName::new("out");
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            BTreeSet::from([output_var.clone()]),
        ));
        let outputs = output_handler.get_output();

        let runtime = MstloRuntimeBuilder::new()
            .executor(executor.clone())
            .model(formula)
            .input(Box::new(input_provider))
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
    async fn builder_runs_multiple_named_formulae(executor: Rc<LocalExecutor<'static>>) {
        let formula = MstloFormula::new(BTreeMap::from([
            (VarName::new("gt"), FormulaDefinition::GreaterThan("x", 5.0)),
            (VarName::new("lt"), FormulaDefinition::LessThan("y", 3.0)),
        ]));
        assert_eq!(formula.var_names(), &[VarName::new("x"), VarName::new("y")]);

        let input_provider = StaticInputProvider::new(BTreeMap::from([
            (
                VarName::new("x"),
                vec![timed_value(0, 7.0), timed_value(10, 4.0)],
            ),
            (
                VarName::new("y"),
                vec![timed_value(0, 2.0), timed_value(10, 5.0)],
            ),
        ]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            BTreeSet::from([VarName::new("gt"), VarName::new("lt")]),
        ));
        let outputs = output_handler.get_output();

        let runtime = MstloRuntimeBuilder::new()
            .executor(executor.clone())
            .model(formula)
            .input(Box::new(input_provider))
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
        let formula = MstloFormula::single(
            VarName::new("out"),
            FormulaDefinition::GreaterThanVar("x", "threshold"),
        );
        assert_eq!(formula.var_names(), &[VarName::new("x")]);

        let variables = Variables::new();
        variables.set("threshold", 2.0);

        let input_provider = StaticInputProvider::new(BTreeMap::from([(
            VarName::new("x"),
            vec![timed_value(0, 3.5), timed_value(10, 1.0)],
        )]));
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
            .input(Box::new(input_provider))
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
