use std::{collections::BTreeMap, fmt::Debug, rc::Rc, time::Duration};

use anyhow::{Context, anyhow};
use async_trait::async_trait;

use futures::StreamExt;
use futures::future::{Either, LocalBoxFuture};
use mstlo::{
    Algorithm, DelayedQualitative, DelayedQuantitative, EagerQualitative, FormulaDefinition,
    RobustnessInterval, RobustnessSemantics, Rosi, Semantics, Step, StlMonitor,
    SynchronizationStrategy, Variables,
};
use smol::LocalExecutor;

use crate::{
    ExecutionPolicy, InputStream, OutputStream, Runtime, Value, VarName,
    core::{FileInputValue, JsonStreamValue, OutputHandler, StreamData},
    lang::mstlo::MstloSpecification,
    runtime::builder::RuntimeBuilder,
    stream_utils,
};

const MSTLO_OUTPUT_CHANNEL_SIZE: usize = 1024;

/// A timestamped stream value.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TimedValue<T> {
    pub timestamp: Duration,
    pub value: T,
}

impl<T> TimedValue<T> {
    pub const fn new(timestamp: Duration, value: T) -> Self {
        Self { timestamp, value }
    }
}

/// Values supported by native MSTLO streams.
///
/// Inputs are floats; the other variants represent MSTLO outputs and missing
/// input samples.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MstloValue {
    Float(f64),
    Bool(bool),
    RobustnessInterval(f64, f64),
    NoVal,
}

/// Native MSTLO input/output value.
pub type MstloTimedValue = TimedValue<MstloValue>;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct MstloWireObject {
    time: i64,
    value: MstloWireValue,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
enum MstloWire {
    Object(MstloWireObject),
    Compact((i64, MstloWireValue)),
}

#[derive(Debug, serde::Deserialize)]
struct MstloMqttEnvelope {
    value: MstloWire,
}

#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
enum MstloMqttPayload {
    Envelope(MstloMqttEnvelope),
    Value(MstloWire),
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
enum MstloWireValue {
    Float(f64),
    Bool(bool),
    RobustnessInterval { lower: f64, upper: f64 },
}

impl MstloWireValue {
    fn into_mstlo_value(self) -> MstloValue {
        match self {
            Self::Float(value) => MstloValue::Float(value),
            Self::Bool(value) => MstloValue::Bool(value),
            Self::RobustnessInterval { lower, upper } => {
                MstloValue::RobustnessInterval(lower, upper)
            }
        }
    }
}

fn decode_json_or_json5<T: serde::de::DeserializeOwned>(
    payload: &[u8],
    description: &str,
) -> anyhow::Result<T> {
    let text = std::str::from_utf8(payload)
        .map_err(|error| anyhow!(error).context(format!("{description} is not UTF-8")))?;
    match serde_json::from_slice(payload) {
        Ok(value) => Ok(value),
        Err(json_error) => serde_json5::from_str(text).map_err(|json5_error| {
            anyhow!(json5_error).context(format!(
                "failed to decode {description} as JSON or JSON5; JSON error: {json_error}"
            ))
        }),
    }
}

impl MstloWire {
    fn into_timed_value(self) -> anyhow::Result<MstloTimedValue> {
        let (time, value) = match self {
            Self::Object(MstloWireObject { time, value }) => (time, value),
            Self::Compact((time, value)) => (time, value),
        };
        let time = u64::try_from(time)
            .context("MSTLO timestamp must be a non-negative integer in milliseconds")?;
        Ok(TimedValue::new(
            Duration::from_millis(time),
            value.into_mstlo_value(),
        ))
    }
}

impl StreamData for MstloTimedValue {
    fn is_no_val(&self) -> bool {
        matches!(self.value, MstloValue::NoVal)
    }
}

impl JsonStreamValue for MstloTimedValue {
    fn decode_json(payload: &[u8]) -> anyhow::Result<Self> {
        decode_json_or_json5::<MstloWire>(payload, "MstloTimedValue")?.into_timed_value()
    }

    fn encode_json(&self) -> anyhow::Result<String> {
        if self.is_no_val() {
            anyhow::bail!("MstloTimedValue::NoVal must not be encoded externally");
        }
        let time = i64::try_from(self.timestamp.as_millis())
            .context("MSTLO timestamp does not fit in a signed millisecond value")?;
        let value = match self.value {
            MstloValue::Float(value) => MstloWireValue::Float(value),
            MstloValue::Bool(value) => MstloWireValue::Bool(value),
            MstloValue::RobustnessInterval(lower, upper) => {
                MstloWireValue::RobustnessInterval { lower, upper }
            }
            MstloValue::NoVal => unreachable!("NoVal was checked before encoding"),
        };
        serde_json5::to_string(&MstloWireObject { time, value })
            .map_err(|error| anyhow!(error).context("failed to encode MstloTimedValue as JSON5"))
    }

    fn decode_mqtt_payload(payload: &[u8]) -> anyhow::Result<Self> {
        let wire = match decode_json_or_json5::<MstloMqttPayload>(payload, "MSTLO MQTT payload")? {
            MstloMqttPayload::Envelope(envelope) => envelope.value,
            MstloMqttPayload::Value(value) => value,
        };
        wire.into_timed_value()
    }
}

impl FileInputValue for MstloTimedValue {
    fn missing_value() -> Self {
        TimedValue::new(Duration::ZERO, MstloValue::NoVal)
    }
}

/// An MSTLO monitor result before conversion to a stream value.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MstloOutput {
    Quantitative(f64),
    Qualitative(bool),
    RobustnessInterval(f64, f64),
}

/// Converts a semantic result to [`MstloOutput`].
pub trait IntoMstloOutput {
    fn into_mstlo_output(self) -> MstloOutput;
}

impl IntoMstloOutput for f64 {
    fn into_mstlo_output(self) -> MstloOutput {
        MstloOutput::Quantitative(self)
    }
}

impl IntoMstloOutput for bool {
    fn into_mstlo_output(self) -> MstloOutput {
        MstloOutput::Qualitative(self)
    }
}

impl IntoMstloOutput for RobustnessInterval {
    fn into_mstlo_output(self) -> MstloOutput {
        MstloOutput::RobustnessInterval(self.0, self.1)
    }
}

/// Converts MSTLO stream values to monitor inputs and outputs.
pub trait MstloStreamValue: StreamData {
    fn step_from_input_value(
        signal_names: &BTreeMap<VarName, &'static str>,
        name: &VarName,
        value: &Self,
    ) -> anyhow::Result<Option<Step<f64>>>;

    fn output_value<RS: IntoMstloOutput>(timestamp: Duration, value: RS) -> anyhow::Result<Self>;
}

struct MstloRuntime<RS, V = Value> {
    _executor: Rc<LocalExecutor<'static>>,
    input_vars: Vec<VarName>,
    monitors: BTreeMap<VarName, StlMonitor<f64, RS>>,
    input_stream: InputStream<V>,
    output_handler: Box<dyn OutputHandler<Val = V>>,
    execution_policy: ExecutionPolicy,
}

struct MstloInputState<'a, RS, V = Value> {
    signal_names: &'a BTreeMap<VarName, &'static str>,
    monitors: &'a mut BTreeMap<VarName, StlMonitor<f64, RS>>,
    outputs: BTreeMap<VarName, MstloOutputStream<V>>,
    blocked: bool,
}

struct MstloOutputStream<V> {
    sender: unsync::spsc::Sender<V>,
    pending: Vec<V>,
}

impl<V> MstloOutputStream<V> {
    fn push(&mut self, value: V) -> bool {
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

pub struct MstloRuntimeBuilder<V = Value> {
    executor: Option<Rc<LocalExecutor<'static>>>,
    formula: Option<MstloSpecification>,
    algorithm: Algorithm,
    semantics: Semantics,
    synchronization_strategy: SynchronizationStrategy,
    variables: Variables,
    input: Option<InputStream<V>>,
    output: Option<Box<dyn OutputHandler<Val = V>>>,
    execution_policy: ExecutionPolicy,
}

impl<V> MstloRuntimeBuilder<V>
where
    V: MstloStreamValue,
{
    pub fn execution_policy(mut self, execution_policy: ExecutionPolicy) -> Self {
        self.execution_policy = execution_policy;
        self
    }

    pub fn controlled_input(self, input: InputStream<V>) -> (Self, crate::io::InputController) {
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
        input_stream: InputStream<V>,
        output_handler: Box<dyn OutputHandler<Val = V>>,
        execution_policy: ExecutionPolicy,
    ) -> Box<dyn Runtime>
    where
        RS: RobustnessSemantics + IntoMstloOutput + Debug + 'static,
        V: MstloStreamValue,
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

impl<V> RuntimeBuilder<MstloSpecification, V> for MstloRuntimeBuilder<V>
where
    V: MstloStreamValue,
{
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

    fn input(mut self, input: InputStream<V>) -> Self {
        self.input = Some(input);
        self
    }

    fn output(mut self, output: Box<dyn OutputHandler<Val = V>>) -> Self {
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

impl<RS> MstloRuntime<RS, Value> {
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

impl<RS, V> MstloRuntime<RS, V>
where
    V: MstloStreamValue,
{
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
}

impl MstloStreamValue for Value {
    #[inline(always)]
    fn step_from_input_value(
        signal_names: &BTreeMap<VarName, &'static str>,
        name: &VarName,
        value: &Self,
    ) -> anyhow::Result<Option<Step<f64>>> {
        if matches!(value, Value::NoVal) {
            return Ok(None);
        }

        let (timestamp, value) = MstloRuntime::<(), Value>::parse_input_value(value)
            .with_context(|| format!("Invalid MSTLO input for variable `{name}`"))?;
        let signal = signal_names
            .get(name)
            .copied()
            .ok_or_else(|| anyhow!("MSTLO input for unknown variable `{name}`"))?;
        Ok(Some(Step::new(signal, value, timestamp)))
    }

    #[inline(always)]
    fn output_value<RS: IntoMstloOutput>(timestamp: Duration, value: RS) -> anyhow::Result<Self> {
        let time = i64::try_from(timestamp.as_millis())
            .context("MSTLO output timestamp does not fit in i64 milliseconds")?;
        let value = match value.into_mstlo_output() {
            MstloOutput::Quantitative(value) => Value::Float(value),
            MstloOutput::Qualitative(value) => Value::Bool(value),
            MstloOutput::RobustnessInterval(lower, upper) => Value::Map(BTreeMap::from([
                ("lower".into(), Value::Float(lower)),
                ("upper".into(), Value::Float(upper)),
            ])),
        };

        Ok(Value::Map(BTreeMap::from([
            ("time".into(), Value::Int(time)),
            ("value".into(), value),
        ])))
    }
}

impl MstloStreamValue for MstloTimedValue {
    #[inline(always)]
    fn step_from_input_value(
        signal_names: &BTreeMap<VarName, &'static str>,
        name: &VarName,
        value: &Self,
    ) -> anyhow::Result<Option<Step<f64>>> {
        let number = match value.value {
            MstloValue::NoVal => return Ok(None),
            MstloValue::Float(number) => number,
            other => {
                return Err(anyhow!(
                    "MSTLO typed input for variable `{name}` must contain a float, got {other:?}"
                ));
            }
        };
        let signal = signal_names
            .get(name)
            .copied()
            .ok_or_else(|| anyhow!("MSTLO input for unknown variable `{name}`"))?;
        Ok(Some(Step::new(signal, number, value.timestamp)))
    }

    #[inline(always)]
    fn output_value<RS: IntoMstloOutput>(timestamp: Duration, value: RS) -> anyhow::Result<Self> {
        i64::try_from(timestamp.as_millis())
            .context("MSTLO output timestamp does not fit in i64 milliseconds")?;
        let value = match value.into_mstlo_output() {
            MstloOutput::Quantitative(value) => MstloValue::Float(value),
            MstloOutput::Qualitative(value) => MstloValue::Bool(value),
            MstloOutput::RobustnessInterval(lower, upper) => {
                MstloValue::RobustnessInterval(lower, upper)
            }
        };
        Ok(TimedValue::new(timestamp, value))
    }
}

impl TimedValue<MstloValue> {
    /// Convert a typed MSTLO value to the backwards-compatible dynamic wire
    /// representation used by the existing external output handlers.
    pub fn try_into_value(self) -> anyhow::Result<Value> {
        if matches!(self.value, MstloValue::NoVal) {
            return Ok(Value::NoVal);
        }
        let time = i64::try_from(self.timestamp.as_millis())
            .context("MSTLO output timestamp does not fit in i64 milliseconds")?;
        let value = match self.value {
            MstloValue::Float(value) => Value::Float(value),
            MstloValue::Bool(value) => Value::Bool(value),
            MstloValue::RobustnessInterval(lower, upper) => Value::Map(BTreeMap::from([
                ("lower".into(), Value::Float(lower)),
                ("upper".into(), Value::Float(upper)),
            ])),
            MstloValue::NoVal => unreachable!("NoVal was handled above"),
        };
        Ok(Value::Map(BTreeMap::from([
            ("time".into(), Value::Int(time)),
            ("value".into(), value),
        ])))
    }
}

impl TryFrom<Value> for MstloTimedValue {
    type Error = anyhow::Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        if matches!(value, Value::NoVal) {
            return Ok(TimedValue::new(Duration::ZERO, MstloValue::NoVal));
        }
        let (timestamp, value) = MstloRuntime::<(), Value>::parse_input_value(&value)?;
        Ok(TimedValue::new(timestamp, MstloValue::Float(value)))
    }
}

/// Convert a dynamic input stream to typed MSTLO values without changing tick boundaries.
pub fn value_input_stream(input: InputStream<Value>) -> InputStream<MstloTimedValue> {
    Box::pin(async_stream::try_stream! {
        let mut input = input;
        while let Some(batch) = input.next().await {
            yield batch?.try_map_values(MstloTimedValue::try_from)?;
        }
    })
}

/// Convert typed MSTLO outputs for a dynamic `Value` output handler.
pub struct MstloValueOutputAdapter {
    inner: Box<dyn OutputHandler<Val = Value>>,
    conversion_error_tx: Option<async_channel::Sender<anyhow::Error>>,
    conversion_error_rx: async_channel::Receiver<anyhow::Error>,
}

impl MstloValueOutputAdapter {
    pub fn new(inner: Box<dyn OutputHandler<Val = Value>>) -> Self {
        let (conversion_error_tx, conversion_error_rx) = async_channel::unbounded();
        Self {
            inner,
            conversion_error_tx: Some(conversion_error_tx),
            conversion_error_rx,
        }
    }
}

impl OutputHandler for MstloValueOutputAdapter {
    type Val = MstloTimedValue;

    fn provide_streams(&mut self, streams: BTreeMap<VarName, OutputStream<MstloTimedValue>>) {
        let error_tx = self
            .conversion_error_tx
            .take()
            .expect("MSTLO output streams already provided");
        let converted = streams
            .into_iter()
            .map(|(name, mut stream)| {
                let error_tx = error_tx.clone();
                let stream = async_stream::stream! {
                    while let Some(value) = stream.next().await {
                        match value.try_into_value() {
                            Ok(value) => yield value,
                            Err(error) => {
                                let _ = error_tx.try_send(error);
                                break;
                            }
                        }
                    }
                };
                (name, Box::pin(stream) as OutputStream<Value>)
            })
            .collect();
        drop(error_tx);
        self.inner.provide_streams(converted);
    }

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let conversion_error = self.conversion_error_rx.clone();
        let run = self.inner.run();
        Box::pin(async move {
            match futures::future::select(Box::pin(conversion_error.recv()), run).await {
                Either::Left((Ok(error), _)) => Err(error),
                Either::Left((Err(_), run)) => run.await,
                Either::Right((result, _)) => result,
            }
        })
    }
}

impl<RS, V> MstloInputState<'_, RS, V>
where
    RS: RobustnessSemantics + IntoMstloOutput + Debug + 'static,
    V: MstloStreamValue,
{
    fn process_event(&mut self, event: crate::core::InputEventRef<'_, V>) -> anyhow::Result<()> {
        let Some(step) = V::step_from_input_value(self.signal_names, event.var, event.value)?
        else {
            return Ok(());
        };
        for (formula_name, monitor) in self.monitors.iter_mut() {
            let output = monitor.update(&step);
            let destination = self.outputs.get_mut(formula_name).ok_or_else(|| {
                anyhow!("Missing output stream for MSTLO formula `{formula_name}`")
            })?;
            for verdict in output.into_verdicts() {
                let value = V::output_value(verdict.timestamp, verdict.value)?;
                self.blocked |= destination.push(value);
            }
        }
        Ok(())
    }

    fn process_step(&mut self, tick: &crate::core::InputTick<'_, V>) -> anyhow::Result<()> {
        let mut steps = Vec::with_capacity(tick.len());
        for event in tick.iter() {
            let Some(step) = V::step_from_input_value(self.signal_names, event.var, event.value)?
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
                    let value = V::output_value(verdict.timestamp, verdict.value)?;
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
impl<RS, V> Runtime for MstloRuntime<RS, V>
where
    RS: RobustnessSemantics + IntoMstloOutput + std::fmt::Debug + 'static,
    V: MstloStreamValue,
{
    async fn run_boxed(mut self: Box<MstloRuntime<RS, V>>) -> anyhow::Result<()> {
        let signal_names = Self::signal_names(&self.input_vars);

        let (outputs, output_streams): (
            BTreeMap<VarName, MstloOutputStream<V>>,
            BTreeMap<VarName, OutputStream<V>>,
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
                        MstloOutputStream {
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

        let input = MstloInputState {
            signal_names: &signal_names,
            monitors: &mut self.monitors,
            outputs,
            blocked: false,
        };
        let execution_policy = self.execution_policy;
        let input_task = Box::pin(async move {
            let mut input = input;
            while let Some(batch) = input_batches.next().await {
                let batch = batch?;
                for tick in batch.ticks() {
                    if tick.len() == 1 {
                        if let Some(event) = tick.iter().next() {
                            input.process_event(event)?;
                            if execution_policy == ExecutionPolicy::Synchronous
                                && !input.flush_pending().await
                            {
                                return Ok(());
                            }
                        }
                    } else {
                        input.process_step(&tick)?;
                        let flush =
                            execution_policy == ExecutionPolicy::Synchronous || input.blocked;
                        if flush && !input.flush_pending().await {
                            return Ok(());
                        }
                    }
                }
                if execution_policy == ExecutionPolicy::Buffered && !input.flush_pending().await {
                    return Ok(());
                }
            }
            let _ = input.flush_pending().await;
            Ok::<_, anyhow::Error>(())
        });

        match futures::future::select(input_task, output_task).await {
            Either::Left((input_result, output_task)) => {
                input_result.context("Input stream/MSTLO processing failed")?;
                output_task.await.context("OutputHandler failed")
            }
            Either::Right((output_result, input_task)) => {
                drop(input_task);
                output_result.context("OutputHandler failed")
            }
        }
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

    struct FailingTypedOutputHandler;

    impl OutputHandler for FailingTypedOutputHandler {
        type Val = MstloTimedValue;

        fn provide_streams(&mut self, _streams: BTreeMap<VarName, OutputStream<MstloTimedValue>>) {}

        fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
            Box::pin(async { anyhow::bail!("output failed") })
        }
    }

    #[derive(Default)]
    struct PendingValueOutputHandler {
        streams: Option<BTreeMap<VarName, OutputStream<Value>>>,
    }

    impl OutputHandler for PendingValueOutputHandler {
        type Val = Value;

        fn provide_streams(&mut self, streams: BTreeMap<VarName, OutputStream<Value>>) {
            self.streams = Some(streams);
        }

        fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
            let mut stream = self
                .streams
                .take()
                .expect("output streams not provided")
                .into_values()
                .next()
                .expect("output stream missing");
            Box::pin(async move {
                let _ = stream.next().await;
                futures::future::pending().await
            })
        }
    }

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

    fn static_typed_input(values: Vec<MstloTimedValue>) -> InputStream<MstloTimedValue> {
        let events = values
            .into_iter()
            .map(|value| crate::InputEvent::new(VarName::new("x"), value))
            .collect();
        Box::pin(stream::once(async move { Ok(InputBatch::events(events)) }))
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

    #[test]
    fn typed_mstlo_values_round_trip_wire_format() {
        let input = MstloTimedValue::try_from(compact_timed_value(10, 2.5)).unwrap();
        assert_eq!(
            input,
            MstloTimedValue::new(Duration::from_millis(10), MstloValue::Float(2.5))
        );
        assert_eq!(input.try_into_value().unwrap(), timed_value(10, 2.5));

        let output = MstloTimedValue::new(Duration::from_millis(20), MstloValue::Bool(true));
        assert_eq!(
            output.try_into_value().unwrap(),
            Value::Map(BTreeMap::from([
                ("time".into(), Value::Int(20)),
                ("value".into(), Value::Bool(true)),
            ]))
        );
    }

    #[test]
    fn typed_json_round_trips_all_wire_variants() {
        let samples: &[(&[u8], MstloTimedValue)] = &[
            (
                br#"{"time":1000,"value":4.0}"#.as_slice(),
                MstloTimedValue::new(Duration::from_secs(1), MstloValue::Float(4.0)),
            ),
            (
                br#"{"time":1000,"value":true}"#.as_slice(),
                MstloTimedValue::new(Duration::from_secs(1), MstloValue::Bool(true)),
            ),
            (
                br#"{"time":1000,"value":{"lower":-1.0,"upper":2.0}}"#.as_slice(),
                MstloTimedValue::new(
                    Duration::from_secs(1),
                    MstloValue::RobustnessInterval(-1.0, 2.0),
                ),
            ),
        ];
        for &(payload, expected) in samples {
            assert_eq!(MstloTimedValue::decode_json(payload).unwrap(), expected);
            let encoded = expected.encode_json().unwrap();
            assert_eq!(
                MstloTimedValue::decode_json(encoded.as_bytes()).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn typed_json_preserves_non_finite_robustness_values() {
        for value in [f64::INFINITY, f64::NEG_INFINITY] {
            let sample = MstloTimedValue::new(Duration::ZERO, MstloValue::Float(value));
            let decoded =
                MstloTimedValue::decode_json(sample.encode_json().unwrap().as_bytes()).unwrap();
            assert_eq!(decoded, sample);
        }

        let sample = MstloTimedValue::new(Duration::ZERO, MstloValue::Float(f64::NAN));
        let decoded =
            MstloTimedValue::decode_json(sample.encode_json().unwrap().as_bytes()).unwrap();
        assert!(matches!(decoded.value, MstloValue::Float(value) if value.is_nan()));

        let sample = MstloTimedValue::new(
            Duration::ZERO,
            MstloValue::RobustnessInterval(f64::NEG_INFINITY, f64::INFINITY),
        );
        let decoded =
            MstloTimedValue::decode_json(sample.encode_json().unwrap().as_bytes()).unwrap();
        assert_eq!(decoded, sample);
    }

    #[test]
    fn typed_json_rejects_invalid_timestamps_and_payloads() {
        assert!(MstloTimedValue::decode_json(br#"{"time":-1,"value":1.0}"#).is_err());
        assert!(MstloTimedValue::decode_json(br#"{"time":1.5,"value":1.0}"#).is_err());
        assert!(MstloTimedValue::decode_json(br#"{"time":1}"#).is_err());
        assert!(
            MstloTimedValue::new(Duration::ZERO, MstloValue::NoVal)
                .encode_json()
                .is_err()
        );
    }

    #[apply(async_test)]
    async fn typed_builder_runs_formula(executor: Rc<LocalExecutor<'static>>) {
        let formula = MstloSpecification::single(
            VarName::new("out"),
            FormulaDefinition::GreaterThan("x", 5.0),
        );
        let input_stream = static_typed_input(vec![
            MstloTimedValue::new(Duration::ZERO, MstloValue::Float(7.0)),
            MstloTimedValue::new(Duration::from_millis(10), MstloValue::Float(4.0)),
        ]);
        let output_var = VarName::new("out");
        let mut output_handler = Box::new(ManualOutputHandler::<MstloTimedValue>::new(
            executor.clone(),
            BTreeSet::from([output_var]),
        ));
        let outputs = output_handler.get_output();

        let runtime = MstloRuntimeBuilder::<MstloTimedValue>::new()
            .executor(executor)
            .model(formula)
            .input(input_stream)
            .output(output_handler)
            .build()
            .await;

        let (runtime_result, output) =
            futures::join!(runtime.run(), async { outputs.collect::<Vec<_>>().await });
        runtime_result.unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(
            output[0][&VarName::new("out")],
            MstloTimedValue::new(Duration::ZERO, MstloValue::Float(2.0))
        );
        assert_eq!(
            output[1][&VarName::new("out")],
            MstloTimedValue::new(Duration::from_millis(10), MstloValue::Float(-1.0))
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
    async fn runtime_propagates_output_errors_while_input_is_pending(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let formula = MstloSpecification::single(
            VarName::new("out"),
            FormulaDefinition::GreaterThan("x", 5.0),
        );
        let input: InputStream<MstloTimedValue> = Box::pin(stream::pending());
        let runtime = MstloRuntimeBuilder::<MstloTimedValue>::new()
            .executor(executor)
            .model(formula)
            .input(input)
            .output(Box::new(FailingTypedOutputHandler))
            .build()
            .await;

        let error = with_timeout(runtime.run(), 1, "MSTLO output failure")
            .await
            .unwrap()
            .unwrap_err();
        assert!(format!("{error:#}").contains("output failed"));
    }

    #[test]
    fn output_adapter_reports_conversion_errors_without_waiting_for_inner_handler() {
        smol::block_on(async {
            let mut adapter =
                MstloValueOutputAdapter::new(Box::new(PendingValueOutputHandler::default()));
            let invalid = MstloTimedValue::new(
                Duration::from_millis(i64::MAX as u64 + 1),
                MstloValue::Float(1.0),
            );
            adapter.provide_streams(BTreeMap::from([(
                VarName::new("out"),
                Box::pin(stream::iter([invalid])) as OutputStream<MstloTimedValue>,
            )]));

            let error = with_timeout(adapter.run(), 1, "MSTLO output conversion failure")
                .await
                .unwrap()
                .unwrap_err();
            assert!(format!("{error:#}").contains("does not fit in i64 milliseconds"));
        });
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
