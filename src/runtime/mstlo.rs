use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    mem,
    rc::Rc,
    time::Duration,
};

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
    ExecutionPolicy, InputStream, Runtime, Value, VarName,
    core::{
        FileInputValue, JsonStreamValue, OutputBatch, OutputError, OutputUpdate, OutputWriter,
        StreamData, input,
    },
    io::OpenedInput,
    lang::mstlo::MstloSpecification,
    runtime::builder::RuntimeBuilder,
};

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

fn decode_json5<T: serde::de::DeserializeOwned>(
    payload: &[u8],
    description: &str,
) -> anyhow::Result<T> {
    let text = std::str::from_utf8(payload)
        .map_err(|error| anyhow!(error).context(format!("{description} is not UTF-8")))?;
    json5::from_str(text)
        .map_err(|error| anyhow!(error).context(format!("failed to decode {description} as JSON5")))
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
        decode_json5::<MstloWire>(payload, "MstloTimedValue")?.into_timed_value()
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
        let wire = MstloWireObject { time, value };
        let requires_json5 = match &wire.value {
            MstloWireValue::Float(value) => !value.is_finite(),
            MstloWireValue::Bool(_) => false,
            MstloWireValue::RobustnessInterval { lower, upper } => {
                !lower.is_finite() || !upper.is_finite()
            }
        };
        if requires_json5 {
            json5::to_string(&wire).map_err(|error| {
                anyhow!(error).context("failed to encode MstloTimedValue as JSON5")
            })
        } else {
            serde_json::to_string(&wire)
                .map_err(|error| anyhow!(error).context("failed to encode MstloTimedValue as JSON"))
        }
    }

    fn decode_mqtt_payload(payload: &[u8]) -> anyhow::Result<Self> {
        let wire = match decode_json5::<MstloMqttPayload>(payload, "MSTLO MQTT payload")? {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MstloRouting {
    /// Preserve the historical behaviour. This is required for RoSI, whose
    /// refinements expose repeated updates at the same timestamp, and for
    /// formulas with no input signals, whose output cadence is the input
    /// cadence.
    FanOut,
    /// Deliver every referenced sample and one additional sample per input
    /// timestamp to keep temporal operators' clocks and synchronizer timelines
    /// unchanged without evaluating every unrelated sample.
    ReferencedWithClock,
}

struct MstloRuntime<RS, V = Value> {
    _executor: Option<Rc<LocalExecutor<'static>>>,
    input_vars: Vec<VarName>,
    monitors: BTreeMap<VarName, StlMonitor<f64, RS>>,
    monitor_signals: BTreeMap<VarName, BTreeSet<&'static str>>,
    routing: MstloRouting,
    input_stream: OpenedInput<V>,
    output_writer: Option<OutputWriter<V>>,
    execution_policy: ExecutionPolicy,
}

type MstloMonitorId = usize;

struct MstloMonitorSlot<RS> {
    name: VarName,
    monitor: StlMonitor<f64, RS>,
}

struct MstloInputState<'a, RS, V = Value> {
    signal_names: &'a BTreeMap<VarName, &'static str>,
    monitors: Vec<MstloMonitorSlot<RS>>,
    signal_monitors: BTreeMap<&'static str, Vec<MstloMonitorId>>,
    fanout_monitors: Vec<MstloMonitorId>,
    clocked_monitors: Vec<MstloMonitorId>,
    routing: MstloRouting,
    last_clock_timestamp: Option<Duration>,
    /// MSTLO verdicts are sparse singleton ticks and may repeat a variable.
    direct_events: Vec<OutputUpdate<V>>,
    blocked: bool,
}

pub struct MstloRuntimeBuilder<V = Value> {
    executor: Option<Rc<LocalExecutor<'static>>>,
    formula: Option<MstloSpecification>,
    algorithm: Algorithm,
    semantics: Semantics,
    synchronization_strategy: SynchronizationStrategy,
    variables: Variables,
    input: Option<OpenedInput<V>>,
    output_writer: Option<OutputWriter<V>>,
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
                .input(input.into()),
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

    pub fn output_writer(mut self, writer: OutputWriter<V>) -> Self {
        self.output_writer = Some(writer);
        self
    }

    fn runtime<RS>(
        executor: Option<Rc<LocalExecutor<'static>>>,
        input_vars: Vec<VarName>,
        monitors: BTreeMap<VarName, StlMonitor<f64, RS>>,
        monitor_signals: BTreeMap<VarName, BTreeSet<&'static str>>,
        routing: MstloRouting,
        input_stream: OpenedInput<V>,
        output_writer: Option<OutputWriter<V>>,
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
            monitor_signals,
            routing,
            input_stream,
            output_writer,
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
            output_writer: None,
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

    fn input(mut self, input: OpenedInput<V>) -> Self {
        self.input = Some(input);
        self
    }

    fn output_writer(mut self, writer: OutputWriter<V>) -> Self {
        self.output_writer = Some(writer);
        self
    }

    fn build(self) -> LocalBoxFuture<'static, Self::Runtime> {
        Box::pin(async move {
            let executor = self.executor;
            let formulae = self.formula.expect("MSTLO formula/spec must be set");
            let input_vars = formulae.var_names().to_vec();
            let monitor_signals = formulae.formula_signals().clone();
            let formulae = formulae.into_formulae();
            let algorithm = self.algorithm;
            let semantics = self.semantics;
            let routing = if semantics == Semantics::RobustnessInterval {
                MstloRouting::FanOut
            } else {
                MstloRouting::ReferencedWithClock
            };
            let synchronization_strategy = self.synchronization_strategy;
            let variables = self.variables;
            let input_stream = self.input.expect("MSTLO input stream must be set");
            let output_writer = self.output_writer;
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
                        monitor_signals,
                        routing,
                        input_stream,
                        output_writer,
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
                        monitor_signals,
                        routing,
                        input_stream,
                        output_writer,
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
                        monitor_signals,
                        routing,
                        input_stream,
                        output_writer,
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
                        monitor_signals,
                        routing,
                        input_stream,
                        output_writer,
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
    /// Convert a typed MSTLO value to its dynamic [`Value`] representation.
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
    input::try_map_input_values(input, MstloTimedValue::try_from)
}

impl<'a, RS, V> MstloInputState<'a, RS, V>
where
    RS: RobustnessSemantics + IntoMstloOutput + Debug + 'static,
    V: MstloStreamValue,
{
    fn new_direct(
        signal_names: &'a BTreeMap<VarName, &'static str>,
        monitors: BTreeMap<VarName, StlMonitor<f64, RS>>,
        mut monitor_signals: BTreeMap<VarName, BTreeSet<&'static str>>,
        routing: MstloRouting,
    ) -> anyhow::Result<Self> {
        let mut monitor_slots = Vec::with_capacity(monitors.len());
        let mut signal_monitors: BTreeMap<&'static str, Vec<MstloMonitorId>> = BTreeMap::new();
        let mut fanout_monitors = Vec::new();
        let mut clocked_monitors = Vec::new();

        for (formula_name, monitor) in monitors {
            let signals = monitor_signals.remove(&formula_name).ok_or_else(|| {
                anyhow!("Missing signal metadata for MSTLO formula `{formula_name}")
            })?;
            let monitor_id = monitor_slots.len();
            if signals.is_empty() || routing == MstloRouting::FanOut {
                fanout_monitors.push(monitor_id);
            } else {
                for signal in signals {
                    signal_monitors.entry(signal).or_default().push(monitor_id);
                }
                clocked_monitors.push(monitor_id);
            }
            monitor_slots.push(MstloMonitorSlot {
                name: formula_name,
                monitor,
            });
        }

        if let Some(formula_name) = monitor_signals.keys().next() {
            return Err(anyhow!(
                "Missing MSTLO monitor for signal metadata `{formula_name}`"
            ));
        }

        Ok(Self {
            signal_names,
            monitors: monitor_slots,
            signal_monitors,
            fanout_monitors,
            clocked_monitors,
            routing,
            last_clock_timestamp: None,
            direct_events: Vec::new(),
            blocked: false,
        })
    }

    fn deliver_monitor_step(
        slot: &mut MstloMonitorSlot<RS>,
        direct_events: &mut Vec<OutputUpdate<V>>,
        blocked: &mut bool,
        step: &Step<f64>,
    ) -> anyhow::Result<()> {
        let output = slot.monitor.update(step);
        for verdict in output.into_verdicts() {
            let value = V::output_value(verdict.timestamp, verdict.value)?;
            direct_events.push(OutputUpdate::new(slot.name.clone(), value));
            *blocked = true;
        }
        Ok(())
    }

    fn process_routed_step(&mut self, step: &Step<f64>) -> anyhow::Result<()> {
        match self.routing {
            MstloRouting::FanOut => {
                for &monitor_id in &self.fanout_monitors {
                    Self::deliver_monitor_step(
                        &mut self.monitors[monitor_id],
                        &mut self.direct_events,
                        &mut self.blocked,
                        step,
                    )?;
                }
            }
            MstloRouting::ReferencedWithClock => {
                let relevant_monitors = self.signal_monitors.get(step.signal);
                let last_timestamp = self.last_clock_timestamp;
                let timestamp_regressed = last_timestamp.is_some_and(|last| step.timestamp < last);
                let new_timestamp = last_timestamp.map_or(true, |last| step.timestamp > last);

                if timestamp_regressed {
                    // The input path is normally chronological, but preserve
                    // the old behaviour for a late event rather than silently
                    // changing a monitor's ordering.
                    for &monitor_id in &self.clocked_monitors {
                        Self::deliver_monitor_step(
                            &mut self.monitors[monitor_id],
                            &mut self.direct_events,
                            &mut self.blocked,
                            step,
                        )?;
                    }
                } else {
                    if new_timestamp {
                        // A temporal monitor must still observe the input
                        // clock, and a multi-signal monitor's synchronizer
                        // must still see every timestamp. One representative
                        // step is enough; repeated unrelated steps at the
                        // same timestamp do not add information for the
                        // non-RoSI semantics.
                        let relevant_ids = relevant_monitors.map_or(&[][..], Vec::as_slice);
                        let mut relevant_index = 0;
                        for &monitor_id in &self.clocked_monitors {
                            while relevant_ids
                                .get(relevant_index)
                                .is_some_and(|&relevant_id| relevant_id < monitor_id)
                            {
                                relevant_index += 1;
                            }
                            if relevant_ids.get(relevant_index) == Some(&monitor_id) {
                                relevant_index += 1;
                                continue;
                            }
                            Self::deliver_monitor_step(
                                &mut self.monitors[monitor_id],
                                &mut self.direct_events,
                                &mut self.blocked,
                                step,
                            )?;
                        }
                        self.last_clock_timestamp = Some(step.timestamp);
                    }

                    if let Some(monitor_ids) = relevant_monitors {
                        for &monitor_id in monitor_ids {
                            Self::deliver_monitor_step(
                                &mut self.monitors[monitor_id],
                                &mut self.direct_events,
                                &mut self.blocked,
                                step,
                            )?;
                        }
                    }
                }

                // A formula with no signal references is intentionally kept on
                // the input cadence; otherwise even `True` would change its
                // observable output stream.
                for &monitor_id in &self.fanout_monitors {
                    Self::deliver_monitor_step(
                        &mut self.monitors[monitor_id],
                        &mut self.direct_events,
                        &mut self.blocked,
                        step,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn process_event(&mut self, event: crate::core::InputUpdateRef<'_, V>) -> anyhow::Result<()> {
        let Some(step) = V::step_from_input_value(self.signal_names, event.variable, event.value)?
        else {
            return Ok(());
        };
        self.process_routed_step(&step)
    }

    fn process_step(&mut self, tick: &crate::core::InputTick<'_, V>) -> anyhow::Result<()> {
        let mut steps = Vec::with_capacity(tick.len());
        for event in tick.iter() {
            let Some(step) =
                V::step_from_input_value(self.signal_names, event.variable, event.value)?
            else {
                continue;
            };
            steps.push(step);
        }
        // `mstlo` otherwise preserves the input iteration order for equal
        // timestamps, so use the signal name as a stable tie-breaker. The
        // routed path consumes this same sorted sequence, so filtering cannot
        // reorder the samples seen by any one monitor.
        steps.sort_by(|left, right| {
            left.timestamp
                .cmp(&right.timestamp)
                .then_with(|| left.signal.cmp(right.signal))
        });
        for step in &steps {
            self.process_routed_step(step)?;
        }
        Ok(())
    }

    fn take_direct_events(&mut self) -> Vec<OutputUpdate<V>> {
        self.blocked = false;
        mem::take(&mut self.direct_events)
    }

    async fn flush_direct(&mut self, writer: &mut OutputWriter<V>) -> Result<(), OutputError> {
        let events = self.take_direct_events();
        if events.is_empty() {
            return Ok(());
        }
        let ticks = events.into_iter().map(|event| vec![event]).collect();
        let batch = OutputBatch::from_ticks(ticks)?;
        crate::runtime::output::submit_batch(writer, batch).await
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

        let Some(mut writer) = self.output_writer.take() else {
            let primary = anyhow!("MSTLO output writer must be set");
            let mut drain = self.input_stream.into_drain();
            let mut error = primary;
            while let Some(item) = drain.next().await {
                if let Err(cleanup) = item {
                    error = combine_errors(error, anyhow::Error::new(cleanup));
                }
            }
            return Err(error);
        };
        {
            let mut input = MstloInputState::new_direct(
                &signal_names,
                self.monitors,
                self.monitor_signals,
                self.routing,
            )?;
            let mut input_batches = self.input_stream;
            let execution_policy = self.execution_policy;
            let mut first_error: Option<anyhow::Error> = None;
            let mut downstream_closed = false;

            'input: while let Some(batch) = input_batches.next().await {
                let batch = match batch {
                    Ok(batch) => batch,
                    Err(error) => {
                        first_error = Some(error.into());
                        break;
                    }
                };
                for tick in batch.ticks() {
                    let process_result = if tick.len() == 1 {
                        tick.iter()
                            .next()
                            .map_or(Ok(()), |event| input.process_event(event))
                    } else {
                        input.process_step(&tick)
                    };
                    if let Err(error) = process_result {
                        first_error = Some(error);
                        break 'input;
                    }

                    if execution_policy == ExecutionPolicy::Synchronous {
                        match input.flush_direct(&mut writer).await {
                            Ok(()) => {}
                            Err(error) if error.is_closed() => {
                                downstream_closed = true;
                                break 'input;
                            }
                            Err(error) => {
                                first_error = Some(anyhow::Error::new(error));
                                break 'input;
                            }
                        }
                    }
                }
                if execution_policy == ExecutionPolicy::Buffered {
                    match input.flush_direct(&mut writer).await {
                        Ok(()) => {}
                        Err(error) if error.is_closed() => {
                            downstream_closed = true;
                            break;
                        }
                        Err(error) => {
                            first_error = Some(anyhow::Error::new(error));
                            break;
                        }
                    }
                }
            }

            // Stop live sources and await their cleanup before closing output.
            // Both phases share the writer's one absolute shutdown deadline.
            let deadline = writer.shutdown_deadline();
            let mut drain = input_batches.into_drain_with_deadline(deadline);
            while let Some(batch) = drain.next().await {
                if let Err(error) = batch {
                    let cleanup = anyhow::Error::new(error);
                    first_error = Some(match first_error {
                        Some(primary) => combine_errors(primary, cleanup),
                        None => cleanup,
                    });
                }
            }

            // Preserve output produced before a source/evaluation failure, then
            // make close the externally meaningful completion barrier.
            if !downstream_closed {
                match input.flush_direct(&mut writer).await {
                    Ok(()) => {}
                    Err(error) if error.is_closed() => {}
                    Err(error) if first_error.is_none() => {
                        first_error = Some(anyhow::Error::new(error));
                    }
                    Err(_) => {}
                }
            }

            let cleanup =
                crate::runtime::output::finish_writer_with_deadline(&mut writer, deadline).await;
            match first_error {
                Some(primary) => match cleanup {
                    Ok(()) => Err(primary.context("Input stream/MSTLO processing failed")),
                    Err(cleanup) => Err(combine_errors(primary, cleanup)
                        .context("Input stream/MSTLO processing failed")),
                },
                None => cleanup.map_err(|error| error.context("MSTLO output failed")),
            }
        }
    }
}

fn combine_errors(primary: anyhow::Error, additional: anyhow::Error) -> anyhow::Error {
    let primary_message = primary.to_string();
    let additional_message = additional.to_string();
    if primary_message == additional_message {
        primary
    } else {
        anyhow::anyhow!("{primary_message}; additionally: {additional_message}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InputBatch, InputStream, LocalStream, OutputBatch, OutputError, OutputWriter};

    use crate::async_test;
    use crate::io::testing::{channel_output, null_output};
    use crate::runtime::builder::RuntimeBuilder;
    use futures::{Sink, StreamExt, stream};
    use macro_rules_attribute::apply;
    use mstlo::{
        Algorithm, DelayedQualitative, DelayedQuantitative, EagerQualitative, FormulaDefinition,
        RobustnessInterval, Rosi, Step, StlMonitor, SynchronizationStrategy, Variables, parse_stl,
    };
    use smol::LocalExecutor;
    use std::{
        cell::Cell,
        collections::{BTreeMap, BTreeSet},
        pin::Pin,
        rc::Rc,
        task::{Context, Poll},
        time::Duration,
    };
    use tc_testutils::streams::with_timeout;

    fn failing_input() -> InputStream<Value> {
        Box::pin(stream::iter([Err(crate::InputError::source(
            "input failed",
        ))]))
    }

    struct CleanupFailingSink {
        flush_error: Option<OutputError>,
        close_error: Option<OutputError>,
        closes: Rc<Cell<usize>>,
    }

    impl Sink<OutputBatch<Value>> for CleanupFailingSink {
        type Error = OutputError;

        fn poll_ready(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, _batch: OutputBatch<Value>) -> Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            let this = self.get_mut();
            Poll::Ready(this.flush_error.take().map_or(Ok(()), Err))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            let this = self.get_mut();
            this.closes.set(this.closes.get() + 1);
            Poll::Ready(this.close_error.take().map_or(Ok(()), Err))
        }
    }

    fn static_input(inputs: BTreeMap<VarName, Vec<Value>>) -> anyhow::Result<InputStream<Value>> {
        let streams = inputs.into_iter().map(|(var, values)| {
            Box::pin(
                stream::iter(values).map(move |value| crate::InputUpdate::new(var.clone(), value)),
            ) as LocalStream<crate::InputUpdate<Value>>
        });
        let streams = futures::stream::select_all(streams);
        if streams.is_empty() {
            anyhow::bail!("no MSTLO input streams configured");
        }
        Ok(Box::pin(streams.map(|event| Ok(InputBatch::from(event)))))
    }

    fn static_typed_input(values: Vec<MstloTimedValue>) -> InputStream<MstloTimedValue> {
        let events: Vec<crate::InputUpdate<MstloTimedValue>> = values
            .into_iter()
            .map(|value| crate::InputUpdate::new(VarName::new("x"), value))
            .collect();
        Box::pin(stream::once(async move {
            Ok(
                InputBatch::from_ticks(events.into_iter().map(|event| vec![event]).collect())
                    .expect("static typed input ticks are valid"),
            )
        }))
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

    fn run_direct_routing<RS, Build>(
        formula: FormulaDefinition,
        _synchronization_strategy: SynchronizationStrategy,
        routing: MstloRouting,
        trace: &[Step<f64>],
        build: Build,
    ) -> Vec<MstloTimedValue>
    where
        RS: RobustnessSemantics + IntoMstloOutput + Debug + 'static,
        Build: FnOnce(FormulaDefinition) -> StlMonitor<f64, RS>,
    {
        let formula_name = VarName::new("out");
        let specification = MstloSpecification::single(formula_name.clone(), formula.clone());
        let input_vars = ["x", "y", "z"]
            .into_iter()
            .map(VarName::new)
            .collect::<Vec<_>>();
        let signal_names = MstloRuntime::<RS, MstloTimedValue>::signal_names(&input_vars);
        let monitors = BTreeMap::from([(formula_name, build(formula))]);
        let mut state = MstloInputState::new_direct(
            &signal_names,
            monitors,
            specification.formula_signals().clone(),
            routing,
        )
        .unwrap();
        for step in trace {
            state.process_routed_step(step).unwrap();
        }
        state
            .direct_events
            .into_iter()
            .map(|event| event.value)
            .collect()
    }

    fn correctness_trace() -> Vec<Step<f64>> {
        vec![
            Step::new("y", 4.0, Duration::from_millis(0)),
            Step::new("x", 2.0, Duration::from_millis(0)),
            Step::new("z", 3.0, Duration::from_millis(0)),
            Step::new("y", -4.0, Duration::from_millis(1_000)),
            Step::new("x", -2.0, Duration::from_millis(2_000)),
            Step::new("z", 3.0, Duration::from_millis(3_000)),
            Step::new("y", 4.0, Duration::from_millis(4_000)),
            Step::new("x", 2.0, Duration::from_millis(4_000)),
            Step::new("z", -3.0, Duration::from_millis(5_000)),
            Step::new("y", -4.0, Duration::from_millis(6_000)),
            Step::new("x", -2.0, Duration::from_millis(6_000)),
            Step::new("z", 3.0, Duration::from_millis(7_000)),
        ]
    }

    #[test]
    fn indexed_route_preserves_non_rosi_verdicts_across_sync_strategies() {
        let formula = parse_stl("G[0,2]((x > 0) && (z > 0))").unwrap();
        let trace = correctness_trace();
        let strategies = [
            SynchronizationStrategy::None,
            SynchronizationStrategy::ZeroOrderHold,
            SynchronizationStrategy::Linear,
        ];

        macro_rules! assert_same_route {
            ($marker:expr, $output:ty, $algorithms:expr) => {
                for algorithm in $algorithms {
                    for strategy in strategies {
                        let fanout = run_direct_routing::<$output, _>(
                            formula.clone(),
                            strategy,
                            MstloRouting::FanOut,
                            &trace,
                            |formula| {
                                StlMonitor::builder()
                                    .formula(formula)
                                    .algorithm(algorithm)
                                    .synchronization_strategy(strategy)
                                    .variables(Variables::new())
                                    .semantics($marker)
                                    .build()
                                    .unwrap()
                            },
                        );
                        let routed = run_direct_routing::<$output, _>(
                            formula.clone(),
                            strategy,
                            MstloRouting::ReferencedWithClock,
                            &trace,
                            |formula| {
                                StlMonitor::builder()
                                    .formula(formula)
                                    .algorithm(algorithm)
                                    .synchronization_strategy(strategy)
                                    .variables(Variables::new())
                                    .semantics($marker)
                                    .build()
                                    .unwrap()
                            },
                        );
                        assert_eq!(
                            routed, fanout,
                            "routing changed {algorithm:?}/{strategy:?} verdicts"
                        );
                    }
                }
            };
        }

        assert_same_route!(
            DelayedQuantitative,
            f64,
            [Algorithm::Naive, Algorithm::Incremental]
        );
        assert_same_route!(
            DelayedQualitative,
            bool,
            [Algorithm::Naive, Algorithm::Incremental]
        );
        assert_same_route!(EagerQualitative, bool, [Algorithm::Incremental]);
    }

    #[test]
    fn rosi_and_signal_free_routes_keep_their_cadence_sensitive_behaviour() {
        let formula = parse_stl("(x > 0) && (z > 0)").unwrap();
        let trace = vec![
            Step::new("x", 2.0, Duration::ZERO),
            Step::new("y", 4.0, Duration::ZERO),
            Step::new("z", 3.0, Duration::ZERO),
            Step::new("x", -2.0, Duration::from_millis(1_000)),
            Step::new("y", -4.0, Duration::from_millis(1_000)),
            Step::new("z", 3.0, Duration::from_millis(1_000)),
        ];
        let strategy = SynchronizationStrategy::ZeroOrderHold;
        let fanout = run_direct_routing::<RobustnessInterval, _>(
            formula.clone(),
            strategy,
            MstloRouting::FanOut,
            &trace,
            |formula| {
                StlMonitor::builder()
                    .formula(formula)
                    .algorithm(Algorithm::Incremental)
                    .synchronization_strategy(strategy)
                    .variables(Variables::new())
                    .semantics(Rosi)
                    .build()
                    .unwrap()
            },
        );
        let clocked = run_direct_routing::<RobustnessInterval, _>(
            formula,
            strategy,
            MstloRouting::ReferencedWithClock,
            &trace,
            |formula| {
                StlMonitor::builder()
                    .formula(formula)
                    .algorithm(Algorithm::Incremental)
                    .synchronization_strategy(strategy)
                    .variables(Variables::new())
                    .semantics(Rosi)
                    .build()
                    .unwrap()
            },
        );
        assert_ne!(
            clocked, fanout,
            "the RoSI route must not be assumed equivalent"
        );

        let formula = MstloSpecification::single(VarName::new("out"), FormulaDefinition::True);
        let signal_names = MstloRuntime::<bool, MstloTimedValue>::signal_names(&[
            VarName::new("x"),
            VarName::new("y"),
        ]);
        assert!(formula.formula_signals()[&VarName::new("out")].is_empty());
        let monitors = BTreeMap::from([(
            VarName::new("out"),
            StlMonitor::builder()
                .formula(FormulaDefinition::True)
                .algorithm(Algorithm::Incremental)
                .semantics(DelayedQualitative)
                .build()
                .unwrap(),
        )]);
        let mut state = MstloInputState::<bool, MstloTimedValue>::new_direct(
            &signal_names,
            monitors,
            formula.formula_signals().clone(),
            MstloRouting::ReferencedWithClock,
        )
        .unwrap();
        for step in [
            Step::new("x", 1.0, Duration::ZERO),
            Step::new("y", 2.0, Duration::ZERO),
        ] {
            state.process_routed_step(&step).unwrap();
        }
        assert_eq!(state.direct_events.len(), 2);
    }

    #[test]
    fn formula_with_no_present_signal_stays_silent() {
        let formula = parse_stl("G[0,2](x > 0)").unwrap();
        let trace = vec![
            Step::new("y", 1.0, Duration::ZERO),
            Step::new("z", 1.0, Duration::from_millis(1_000)),
            Step::new("y", 2.0, Duration::from_millis(2_000)),
        ];
        for strategy in [
            SynchronizationStrategy::None,
            SynchronizationStrategy::ZeroOrderHold,
            SynchronizationStrategy::Linear,
        ] {
            let fanout = run_direct_routing::<bool, _>(
                formula.clone(),
                strategy,
                MstloRouting::FanOut,
                &trace,
                |formula| {
                    StlMonitor::builder()
                        .formula(formula)
                        .algorithm(Algorithm::Incremental)
                        .synchronization_strategy(strategy)
                        .semantics(DelayedQualitative)
                        .build()
                        .unwrap()
                },
            );
            let routed = run_direct_routing::<bool, _>(
                formula.clone(),
                strategy,
                MstloRouting::ReferencedWithClock,
                &trace,
                |formula| {
                    StlMonitor::builder()
                        .formula(formula)
                        .algorithm(Algorithm::Incremental)
                        .synchronization_strategy(strategy)
                        .semantics(DelayedQualitative)
                        .build()
                        .unwrap()
                },
            );
            assert_eq!(routed, fanout);
            assert!(routed.is_empty());
        }
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
            (
                br#"{/* JSON5 */ time: 1000, value: {lower: -1.0, upper: 2.0,},}"#.as_slice(),
                MstloTimedValue::new(
                    Duration::from_secs(1),
                    MstloValue::RobustnessInterval(-1.0, 2.0),
                ),
            ),
        ];
        for &(payload, expected) in samples {
            assert_eq!(MstloTimedValue::decode_json(payload).unwrap(), expected);
            let encoded = expected.encode_json().unwrap();
            assert!(!encoded.contains('\n'));
            assert!(serde_json::from_str::<serde_json::Value>(&encoded).is_ok());
            assert_eq!(
                MstloTimedValue::decode_json(encoded.as_bytes()).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn typed_mqtt_decoder_accepts_json5_envelopes() {
        let decoded = MstloTimedValue::decode_mqtt_payload(
            br#"{value: {/* sample */ time: 25, value: true,},}"#,
        )
        .unwrap();
        assert_eq!(
            decoded,
            MstloTimedValue::new(Duration::from_millis(25), MstloValue::Bool(true))
        );
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
    async fn direct_typed_writer_reports_output_timestamp_conversion_errors(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let formula = MstloSpecification::single(
            VarName::new("out"),
            FormulaDefinition::GreaterThan("x", 5.0),
        );
        let input = static_typed_input(vec![MstloTimedValue::new(
            Duration::from_millis(i64::MAX as u64 + 1),
            MstloValue::Float(7.0),
        )]);
        let output_writer: OutputWriter<MstloTimedValue> =
            null_output(BTreeSet::from([VarName::new("out")])).await;
        let runtime = MstloRuntimeBuilder::<MstloTimedValue>::new()
            .executor(executor)
            .model(formula)
            .semantics(Semantics::EagerQualitative)
            .input(input.into())
            .output_writer(output_writer)
            .build()
            .await;

        let error = runtime.run().await.unwrap_err();
        assert!(format!("{error:#}").contains("does not fit in i64 milliseconds"));
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
        let (output_writer, outputs) = channel_output(BTreeSet::from([output_var])).await;

        let runtime = MstloRuntimeBuilder::<MstloTimedValue>::new()
            .executor(executor)
            .model(formula)
            .input(input_stream.into())
            .output_writer(output_writer)
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
        let (output_writer, outputs) = channel_output(BTreeSet::from([output_var.clone()])).await;

        let runtime = MstloRuntimeBuilder::new()
            .executor(executor.clone())
            .model(formula)
            .input(input_stream.into())
            .output_writer(output_writer)
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
        let (output_writer, mut outputs) = channel_output(BTreeSet::from([output_var])).await;
        let (builder, controller) = MstloRuntimeBuilder::new()
            .executor(executor)
            .model(formula)
            .controlled_input(input);
        let runtime = builder.output_writer(output_writer).build().await;

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
            let (input_stream, mut input) = crate::io::channel::channel();
            input
                .send_tick(
                    vars.into_iter()
                        .map(|var| {
                            crate::InputUpdate::new(VarName::new(var), compact_timed_value(0, 1.0))
                        })
                        .collect(),
                )
                .await
                .unwrap();
            drop(input);

            let (output_writer, outputs) =
                channel_output(BTreeSet::from([VarName::new("out")])).await;
            let runtime = MstloRuntimeBuilder::new()
                .executor(executor)
                .model(formula)
                .input(input_stream.into())
                .output_writer(output_writer)
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
        let output_writer = null_output(BTreeSet::from([VarName::new("out")])).await;
        let runtime = MstloRuntimeBuilder::new()
            .executor(executor)
            .model(formula)
            .input(failing_input().into())
            .output_writer(output_writer)
            .build()
            .await;

        let error = runtime.run().await.unwrap_err();
        assert!(format!("{error:#}").contains("input failed"));
    }

    #[apply(async_test)]
    async fn runtime_preserves_input_error_when_cleanup_fails(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let closes = Rc::new(Cell::new(0));
        let formula = MstloSpecification::single(
            VarName::new("out"),
            FormulaDefinition::GreaterThan("x", 5.0),
        );
        let output_writer = OutputWriter::from_sink(CleanupFailingSink {
            flush_error: Some(OutputError::backend("MSTLO flush failed")),
            close_error: Some(OutputError::backend("MSTLO close failed")),
            closes: Rc::clone(&closes),
        });
        let runtime = MstloRuntimeBuilder::new()
            .executor(executor)
            .model(formula)
            .input(failing_input().into())
            .output_writer(output_writer)
            .build()
            .await;

        let error = runtime.run().await.unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("input failed"));
        assert!(message.contains("MSTLO flush failed"));
        assert!(message.contains("MSTLO close failed"));
        assert_eq!(closes.get(), 1);
    }

    #[apply(async_test)]
    async fn direct_writer_emits_sparse_repeated_verdict_events(
        _executor: Rc<LocalExecutor<'static>>,
    ) {
        let formula = MstloSpecification::new(BTreeMap::from([
            (VarName::new("gt"), FormulaDefinition::GreaterThan("x", 5.0)),
            (
                VarName::new("gt_high"),
                FormulaDefinition::GreaterThan("x", 6.0),
            ),
        ]));
        let input_stream = static_input(BTreeMap::from([(
            VarName::new("x"),
            vec![timed_value(0, 7.0), timed_value(10, 4.0)],
        )]))
        .unwrap();
        let captured = Rc::new(std::cell::RefCell::new(Vec::new()));
        let capture = Rc::clone(&captured);
        let writer = crate::core::OutputWriter::from_sink(crate::io::output::local_batch_sink(
            move |batch| {
                let capture = Rc::clone(&capture);
                async move {
                    capture.borrow_mut().push(batch);
                    Ok(())
                }
            },
        ));

        let runtime = MstloRuntimeBuilder::new()
            .model(formula)
            .input(input_stream.into())
            .output_writer(writer)
            .build()
            .await;
        runtime.run().await.unwrap();

        let batches = Rc::try_unwrap(captured).unwrap().into_inner();
        assert!(
            batches
                .iter()
                .all(|batch| batch.ticks().all(|tick| tick.len() == 1))
        );
        let events = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .updates()
                    .map(|event| (event.variable.clone(), event.value.clone()))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            events,
            vec![
                (VarName::new("gt"), timed_value(0, 2.0)),
                (VarName::new("gt_high"), timed_value(0, 1.0)),
                (VarName::new("gt"), timed_value(10, -1.0)),
                (VarName::new("gt_high"), timed_value(10, -2.0)),
            ]
        );
    }

    #[apply(async_test)]
    async fn direct_writer_propagates_sink_errors(_executor: Rc<LocalExecutor<'static>>) {
        let formula = MstloSpecification::single(
            VarName::new("out"),
            FormulaDefinition::GreaterThan("x", 5.0),
        );
        let input_stream = static_input(BTreeMap::from([(
            VarName::new("x"),
            vec![timed_value(0, 7.0)],
        )]))
        .unwrap();
        let writer =
            crate::core::OutputWriter::from_sink(crate::io::output::local_batch_sink(|_| async {
                Err(crate::core::OutputError::backend("direct sink failed"))
            }));
        let runtime = MstloRuntimeBuilder::new()
            .model(formula)
            .input(input_stream.into())
            .output_writer(writer)
            .build()
            .await;

        let error = runtime.run().await.unwrap_err();
        assert!(format!("{error:#}").contains("direct sink failed"));
    }

    #[apply(async_test)]
    async fn builder_runs_multiple_named_formulae(executor: Rc<LocalExecutor<'static>>) {
        let formula = MstloSpecification::new(BTreeMap::from([
            (VarName::new("gt"), FormulaDefinition::GreaterThan("x", 5.0)),
            (
                VarName::new("gt_high"),
                FormulaDefinition::GreaterThan("x", 6.0),
            ),
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
        let (output_writer, outputs) = channel_output(BTreeSet::from([
            VarName::new("gt"),
            VarName::new("gt_high"),
            VarName::new("lt"),
        ]))
        .await;

        let runtime = MstloRuntimeBuilder::new()
            .executor(executor.clone())
            .model(formula)
            .input(input_stream.into())
            .output_writer(output_writer)
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

        assert_eq!(outputs.len(), 6);
        assert_eq!(output_value(&outputs[0], "gt"), (0, 2.0));
        assert_eq!(output_value(&outputs[1], "gt_high"), (0, 1.0));
        assert_eq!(output_value(&outputs[2], "lt"), (0, 1.0));
        assert_eq!(output_value(&outputs[3], "gt"), (10, -1.0));
        assert_eq!(output_value(&outputs[4], "gt_high"), (10, -2.0));
        assert_eq!(output_value(&outputs[5], "lt"), (10, -2.0));
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
        let (output_writer, outputs) = channel_output(BTreeSet::from([output_var.clone()])).await;

        let runtime = MstloRuntimeBuilder::new()
            .executor(executor.clone())
            .model(formula)
            .variables(variables)
            .input(input_stream.into())
            .output_writer(output_writer)
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
