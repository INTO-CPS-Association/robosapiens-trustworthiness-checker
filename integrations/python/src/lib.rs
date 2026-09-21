// TODO: Convert between Python values and the Rust `trustworthiness_checker` core types.
// TODO: Keep runtime and parser logic delegated to the root library crate.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures::{FutureExt, StreamExt};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};
use smol::LocalExecutor;
use tc_core::causal::{
    CausalCauseReport, CausalDomain, CausalResultReport, CausalRole, CausalSet, CausalValue,
    RoleCausalAntichain, RoleCausalSet, causality_report,
};
use tc_core::core::{
    ExecutionPolicy, LocalStream, OutputInterface, OutputWriter, Runtime, RuntimeSpec, Semantics,
};
use tc_core::io::InputController;
use tc_core::io::channel::{
    ChannelInputController, ChannelOutputReceiver, channel, open_output, output,
};

use tc_core::lang::dsrv::diagnostics::{SemanticErrors, SemanticWarning as CoreSemanticWarning};
use tc_core::runtime::builder::{GeneralRuntimeBuilder, type_check_options};
use tc_core::semantics::CausalRuntimeBuilder;
use tc_core::{
    DsrvSpecification, ElaboratedDsrvSpecification, InputUpdate, TypeCheckOptions, Value, VarName,
};

pyo3::create_exception!(
    trustworthiness_checker,
    SemanticAnalysisError,
    pyo3::exceptions::PyRuntimeError,
    "The model failed semantic checking. `warnings` holds the warnings the \
     failed analysis proved."
);

type OutputBatch = BTreeMap<VarName, Value>;
type CausalSetOutputBatch = BTreeMap<VarName, CausalValue<CausalSet>>;
type RoleCausalSetOutputBatch = BTreeMap<VarName, CausalValue<RoleCausalSet>>;
type RoleCausalAntichainOutputBatch = BTreeMap<VarName, CausalValue<RoleCausalAntichain>>;

async fn channel_causal_output<D: CausalDomain>(
    output_vars: BTreeSet<VarName>,
) -> anyhow::Result<(
    OutputWriter<CausalValue<D>>,
    LocalStream<BTreeMap<VarName, CausalValue<D>>>,
)> {
    let (sender, receiver): (_, ChannelOutputReceiver<CausalValue<D>>) = output(1024);
    let interface = OutputInterface::outputs(output_vars)?;
    let writer = open_output(sender, interface).await?;
    let outputs = Box::pin(futures::stream::unfold(
        receiver,
        |mut receiver| async move { receiver.recv().await.map(|output| (output, receiver)) },
    ));
    Ok((writer, outputs))
}

enum RuntimeMode {
    Ordinary(Semantics),
    ReferenceCausalSet,
    RoleCausalSet,
    RoleCausalAntichain,
}

impl RuntimeMode {
    /// The policy the model is checked with. The causal runtimes evaluate
    /// without consulting types, as `untimed` does, and are checked as it is.
    fn type_check_options(&self) -> TypeCheckOptions {
        match self {
            RuntimeMode::Ordinary(semantics) => type_check_options(*semantics),
            RuntimeMode::ReferenceCausalSet
            | RuntimeMode::RoleCausalSet
            | RuntimeMode::RoleCausalAntichain => type_check_options(Semantics::Untimed),
        }
    }
}

enum RuntimeOutputs {
    Ordinary(LocalStream<OutputBatch>),
    ReferenceCausalSet(LocalStream<CausalSetOutputBatch>),
    RoleCausalSet(LocalStream<RoleCausalSetOutputBatch>),
    RoleCausalAntichain(LocalStream<RoleCausalAntichainOutputBatch>),
}

enum RuntimeOutputBatch {
    Ordinary(OutputBatch),
    ReferenceCausalSet(CausalSetOutputBatch),
    RoleCausalSet(RoleCausalSetOutputBatch),
    RoleCausalAntichain(RoleCausalAntichainOutputBatch),
}

#[pyclass(module = "trustworthiness_checker", frozen)]
struct DeferredValue;

#[pymethods]
impl DeferredValue {
    #[new]
    fn new() -> Self {
        Self
    }

    fn __repr__(&self) -> &'static str {
        "DeferredValue()"
    }
}

#[pyclass(module = "trustworthiness_checker", frozen)]
struct NoValue;

#[pymethods]
impl NoValue {
    #[new]
    fn new() -> Self {
        Self
    }

    fn __repr__(&self) -> &'static str {
        "NoValue()"
    }
}

/// A value of a tagged union: its tag, and its payload converted as any other
/// value is, or `None` for a tag without one. Only the checker creates them.
#[pyclass(module = "trustworthiness_checker", frozen)]
struct UnionValue {
    #[pyo3(get)]
    tag: String,
    #[pyo3(get)]
    payload: Option<Py<PyAny>>,
}

#[pymethods]
impl UnionValue {
    #[classattr]
    fn __match_args__() -> (&'static str, &'static str) {
        ("tag", "payload")
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let tag = PyString::new(py, &self.tag).repr()?;
        let payload = match &self.payload {
            Some(payload) => payload.bind(py).repr()?.to_string(),
            None => "None".to_owned(),
        };
        Ok(format!("UnionValue(tag={tag}, payload={payload})"))
    }
}

/// A warning semantic checking proved about the model. `span` is the byte
/// range it applies to in the model text, or `None` when it has no position.
#[pyclass(module = "trustworthiness_checker", frozen)]
struct SemanticWarning {
    #[pyo3(get)]
    code: String,
    #[pyo3(get)]
    message: String,
    #[pyo3(get)]
    span: Option<(u32, u32)>,
}

#[pymethods]
impl SemanticWarning {
    fn __repr__(&self) -> String {
        format!(
            "SemanticWarning(code={:?}, message={:?}, span={:?})",
            self.code, self.message, self.span
        )
    }
}

/// The warnings of one analysis, as an immutable Python tuple, in report order.
fn warnings_tuple(py: Python<'_>, warnings: &[CoreSemanticWarning]) -> PyResult<Py<PyTuple>> {
    let warnings = warnings
        .iter()
        .map(|warning| {
            Py::new(
                py,
                SemanticWarning {
                    code: warning.code().to_owned(),
                    message: warning.message().to_owned(),
                    span: warning.span().map(|span| (span.start, span.end)),
                },
            )
        })
        .collect::<PyResult<Vec<_>>>()?;
    Ok(PyTuple::new(py, warnings)?.unbind())
}

/// A semantic-check failure, carrying the warnings the failed analysis proved.
fn semantic_analysis_error(
    py: Python<'_>,
    errors: &SemanticErrors,
    warnings: Py<PyTuple>,
) -> PyErr {
    let error = SemanticAnalysisError::new_err(format!(
        "failed to initialise trustworthiness checker runtime: model failed semantic checking: \
         {errors:?}"
    ));
    match error.value(py).setattr("warnings", warnings) {
        Ok(()) => error,
        Err(setattr_error) => setattr_error,
    }
}

fn initialisation_error(error: anyhow::Error) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(format!(
        "failed to initialise trustworthiness checker runtime: {error:#}"
    ))
}

#[pyclass(module = "trustworthiness_checker", unsendable)]
struct TcRuntime {
    executor: Rc<LocalExecutor<'static>>,
    input_vars: BTreeSet<VarName>,
    input_controller: ChannelInputController<Value>,
    tick_controller: InputController,
    outputs: RuntimeOutputs,
    warnings: Py<PyTuple>,
}

#[pymethods]
impl TcRuntime {
    #[new]
    #[pyo3(signature = (model, *, semantics="typed-untimed"))]
    fn new(py: Python<'_>, model: &Bound<'_, PyAny>, semantics: &str) -> PyResult<Self> {
        let model = model_to_text(model)?;
        let semantics = parse_semantics(semantics)?;
        Self::from_model(py, model, semantics)
    }

    #[staticmethod]
    #[pyo3(signature = (model, semantics=None))]
    fn from_text(py: Python<'_>, model: &str, semantics: Option<&str>) -> PyResult<Self> {
        let semantics = parse_semantics(semantics.unwrap_or("typed-untimed"))?;
        Self::from_model(py, model.to_string(), semantics)
    }

    #[staticmethod]
    #[pyo3(signature = (path, semantics=None))]
    fn from_path(
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        semantics: Option<&str>,
    ) -> PyResult<Self> {
        let model = read_pathlike(path)?;
        let semantics = parse_semantics(semantics.unwrap_or("typed-untimed"))?;
        Self::from_model(py, model, semantics)
    }

    /// The warnings semantic checking proved about the model, in report order.
    #[getter]
    fn warnings(&self, py: Python<'_>) -> Py<PyTuple> {
        self.warnings.clone_ref(py)
    }

    fn is_initialized(&self) -> bool {
        true
    }

    fn step(&self) -> bool {
        self.executor.try_tick()
    }

    #[pyo3(signature = (inputs=None, **kwargs))]
    fn provide_inputs(
        &mut self,
        inputs: Option<&Bound<'_, PyDict>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let mut values = BTreeMap::new();

        if let Some(inputs) = inputs {
            collect_input_values(inputs, &mut values)?;
        }
        if let Some(kwargs) = kwargs {
            collect_input_values(kwargs, &mut values)?;
        }

        let extra = values
            .keys()
            .filter(|variable| !self.input_vars.contains(*variable))
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        if !extra.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "input batch contains unknown model inputs: {extra:?}"
            )));
        }
        for var in &self.input_vars {
            values.entry(var.clone()).or_insert(Value::NoVal);
        }

        let events = values
            .into_iter()
            .map(|(var, value)| InputUpdate::new(var, value))
            .collect();
        smol::block_on(self.executor.run(async {
            self.input_controller.send_tick(events).await?;
            self.tick_controller.advance().await
        }))
        .map_err(|error| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "failed to evaluate checker input step: {error}"
            ))
        })?;

        Ok(())
    }

    #[pyo3(signature = (timeout=None))]
    fn next_output(
        &mut self,
        py: Python<'_>,
        timeout: Option<f64>,
    ) -> PyResult<Option<Py<PyDict>>> {
        let output = self.wait_for_next_output(py, timeout)?;
        let Some(output) = output else {
            return Ok(None);
        };
        Ok(Some(match output {
            RuntimeOutputBatch::Ordinary(output) => output_to_py_dict(py, output)?,
            RuntimeOutputBatch::ReferenceCausalSet(output) => causal_output_to_py_dict(py, output)?,
            RuntimeOutputBatch::RoleCausalSet(output) => causal_output_to_py_dict(py, output)?,
            RuntimeOutputBatch::RoleCausalAntichain(output) => {
                causal_output_to_py_dict(py, output)?
            }
        }))
    }

    fn run(&mut self, py: Python<'_>) -> PyResult<Vec<Py<PyDict>>> {
        let mut outputs = Vec::new();
        while let Some(output) = self.next_output(py, None)? {
            outputs.push(output);
        }
        Ok(outputs)
    }
}

fn model_to_text(model: &Bound<'_, PyAny>) -> PyResult<String> {
    if model.is_instance_of::<PyString>() {
        model.extract()
    } else {
        read_pathlike(model)
    }
}

fn read_pathlike(path: &Bound<'_, PyAny>) -> PyResult<String> {
    let path = path.call_method0("__fspath__")?.extract::<String>()?;
    std::fs::read_to_string(&path).map_err(|err| {
        pyo3::exceptions::PyOSError::new_err(format!("failed to read model file {path:?}: {err}"))
    })
}

fn parse_semantics(semantics: &str) -> PyResult<RuntimeMode> {
    match semantics {
        "typed" | "typed-untimed" => Ok(RuntimeMode::Ordinary(Semantics::TypedUntimed)),
        "untyped" | "untimed" => Ok(RuntimeMode::Ordinary(Semantics::Untimed)),
        "gradual" | "gradual-typed" | "gradual-typed-untimed" => {
            Ok(RuntimeMode::Ordinary(Semantics::GradualTypedUntimed))
        }
        "causal" | "causal-set" => Ok(RuntimeMode::ReferenceCausalSet),
        "role-causal-set" => Ok(RuntimeMode::RoleCausalSet),
        "role-causal-antichain" => Ok(RuntimeMode::RoleCausalAntichain),
        _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "unsupported semantics {semantics:?}; expected ordinary untimed semantics or one of 'causal', 'causal-set', 'role-causal-set', 'role-causal-antichain'"
        ))),
    }
}

fn collect_input_values(
    inputs: &Bound<'_, PyDict>,
    values: &mut BTreeMap<VarName, Value>,
) -> PyResult<()> {
    for (key, value) in inputs.iter() {
        let key = key.extract::<String>()?;
        let var = VarName::new(&key);
        values.insert(var, py_to_value(&value)?);
    }
    Ok(())
}

#[pymodule]
fn trustworthiness_checker(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<DeferredValue>()?;
    m.add_class::<NoValue>()?;
    m.add_class::<TcRuntime>()?;
    m.add_class::<SemanticWarning>()?;
    m.add_class::<UnionValue>()?;
    m.add(
        "SemanticAnalysisError",
        m.py().get_type::<SemanticAnalysisError>(),
    )?;
    Ok(())
}

impl TcRuntime {
    /// Parse, then check and elaborate the model before any runtime is built.
    /// A semantic failure raises `SemanticAnalysisError` with its warnings;
    /// every other failure keeps its existing channel.
    fn from_model(py: Python<'_>, model: String, semantics: RuntimeMode) -> PyResult<Self> {
        let spec = model.parse::<DsrvSpecification>().map_err(|err| {
            initialisation_error(anyhow::anyhow!(
                "example model could not be parsed: {err:?}"
            ))
        })?;
        let (checked, warnings) = spec
            .check_and_elaborate(semantics.type_check_options())
            .into_parts();
        let warnings = warnings_tuple(py, &warnings)?;
        let spec = checked
            .map_err(|errors| semantic_analysis_error(py, &errors, warnings.clone_ref(py)))?;

        let executor = Rc::new(LocalExecutor::new());
        let (input_vars, input_controller, tick_controller, outputs) =
            build_runtime(executor.clone(), spec, semantics).map_err(initialisation_error)?;

        Ok(Self {
            executor,
            input_vars,
            input_controller,
            tick_controller,
            outputs,
            warnings,
        })
    }

    fn wait_for_next_output(
        &mut self,
        py: Python<'_>,
        timeout: Option<f64>,
    ) -> PyResult<Option<RuntimeOutputBatch>> {
        let timeout = match timeout {
            Some(timeout) if timeout < 0.0 || !timeout.is_finite() => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "timeout must be finite and non-negative",
                ));
            }
            Some(timeout) => Some(Duration::from_secs_f64(timeout)),
            None => None,
        };
        let started = Instant::now();

        loop {
            let output = match &mut self.outputs {
                RuntimeOutputs::Ordinary(outputs) => outputs
                    .next()
                    .now_or_never()
                    .map(|output| output.map(RuntimeOutputBatch::Ordinary)),
                RuntimeOutputs::ReferenceCausalSet(outputs) => outputs
                    .next()
                    .now_or_never()
                    .map(|output| output.map(RuntimeOutputBatch::ReferenceCausalSet)),
                RuntimeOutputs::RoleCausalSet(outputs) => outputs
                    .next()
                    .now_or_never()
                    .map(|output| output.map(RuntimeOutputBatch::RoleCausalSet)),
                RuntimeOutputs::RoleCausalAntichain(outputs) => outputs
                    .next()
                    .now_or_never()
                    .map(|output| output.map(RuntimeOutputBatch::RoleCausalAntichain)),
            };
            if let Some(output) = output {
                return Ok(output);
            }

            self.executor.try_tick();
            py.check_signals()?;

            if timeout.is_some_and(|timeout| started.elapsed() >= timeout) {
                return Ok(None);
            }

            py.detach(|| std::thread::sleep(Duration::from_millis(1)));
        }
    }
}

fn build_runtime(
    executor: Rc<LocalExecutor<'static>>,
    spec: ElaboratedDsrvSpecification,
    semantics: RuntimeMode,
) -> anyhow::Result<(
    BTreeSet<VarName>,
    ChannelInputController<Value>,
    InputController,
    RuntimeOutputs,
)> {
    let runtime_executor = executor.clone();

    smol::block_on(
        executor.run(async move {
            let input_vars = spec.input_vars().clone();
            let (input, input_controller) = channel();
            let (input, tick_controller) = tc_core::io::controlled(input);

            let outputs = match semantics {
                RuntimeMode::Ordinary(semantics) => {
                    let (sender, receiver) = output(1024);
                    let outputs: LocalStream<OutputBatch> = Box::pin(futures::stream::unfold(
                        receiver,
                        |mut receiver| async move {
                            receiver.recv().await.map(|output| (output, receiver))
                        },
                    ));
                    let output_writer = open_output(
                        sender,
                        OutputInterface::outputs(spec.output_vars().clone())?,
                    )
                    .await?;
                    let monitor =
                        GeneralRuntimeBuilder::<ElaboratedDsrvSpecification, Value>::new()
                            .executor(runtime_executor.clone())
                            .model(spec)
                            .input(input)
                            .output_writer(output_writer)
                            .runtime(RuntimeSpec::Dataflow(ExecutionPolicy::Synchronous))
                            .semantics(semantics)
                            .build()
                            .await?;
                    runtime_executor.spawn(monitor.run()).detach();
                    RuntimeOutputs::Ordinary(outputs)
                }
                RuntimeMode::ReferenceCausalSet => {
                    let (output_writer, outputs) =
                        channel_causal_output::<CausalSet>(spec.output_vars().clone()).await?;
                    let monitor = CausalRuntimeBuilder::<CausalSet>::new()
                        .executor(runtime_executor.clone())
                        .model(spec)
                        .input(input)
                        .output_writer(output_writer)
                        .build()
                        .await?;
                    runtime_executor.spawn(monitor.run()).detach();
                    RuntimeOutputs::ReferenceCausalSet(outputs)
                }
                RuntimeMode::RoleCausalSet => {
                    let (output_writer, outputs) =
                        channel_causal_output::<RoleCausalSet>(spec.output_vars().clone()).await?;
                    let monitor = CausalRuntimeBuilder::<RoleCausalSet>::role_new()
                        .executor(runtime_executor.clone())
                        .model(spec)
                        .input(input)
                        .output_writer(output_writer)
                        .build()
                        .await?;
                    runtime_executor.spawn(monitor.run()).detach();
                    RuntimeOutputs::RoleCausalSet(outputs)
                }
                RuntimeMode::RoleCausalAntichain => {
                    let (output_writer, outputs) =
                        channel_causal_output::<RoleCausalAntichain>(spec.output_vars().clone())
                            .await?;
                    let monitor = CausalRuntimeBuilder::<RoleCausalAntichain>::role_new()
                        .executor(runtime_executor.clone())
                        .model(spec)
                        .input(input)
                        .output_writer(output_writer)
                        .build()
                        .await?;
                    runtime_executor.spawn(monitor.run()).detach();
                    RuntimeOutputs::RoleCausalAntichain(outputs)
                }
            };

            Ok((input_vars, input_controller, tick_controller, outputs))
        }),
    )
}

fn py_to_value(value: &Bound<'_, PyAny>) -> PyResult<Value> {
    if value.is_none() {
        Ok(Value::Unit)
    } else if value.is_instance_of::<PyBool>() {
        Ok(Value::Bool(value.extract()?))
    } else if value.is_instance_of::<PyInt>() {
        Ok(Value::Int(value.extract()?))
    } else if value.is_instance_of::<PyFloat>() {
        Ok(Value::Float(value.extract()?))
    } else if value.is_instance_of::<PyString>() {
        Ok(Value::Str(value.extract::<String>()?.into()))
    } else if let Ok(list) = value.cast::<PyList>() {
        let values = list
            .iter()
            .map(|item| py_to_value(&item))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(Value::List(values.into()))
    } else if let Ok(tuple) = value.cast::<PyTuple>() {
        let values = tuple
            .iter()
            .map(|item| py_to_value(&item))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(Value::Tuple(values.into()))
    } else if let Ok(dict) = value.cast::<PyDict>() {
        let mut values = BTreeMap::new();
        for (key, value) in dict.iter() {
            values.insert(key.extract::<String>()?.into(), py_to_value(&value)?);
        }
        Ok(Value::Map(values))
    } else {
        Err(pyo3::exceptions::PyTypeError::new_err(format!(
            "unsupported input value type: {}",
            value.get_type().name()?
        )))
    }
}

fn output_to_py_dict(py: Python<'_>, output: OutputBatch) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    for (name, value) in output {
        dict.set_item(name.to_string(), value_to_py(py, value)?)?;
    }
    Ok(dict.unbind())
}

fn causal_output_to_py_dict<D: CausalDomain>(
    py: Python<'_>,
    output: BTreeMap<VarName, CausalValue<D>>,
) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    let values = PyDict::new(py);
    let causality = PyDict::new(py);
    for (name, value) in output {
        let CausalValue { value, explanation } = value;
        let name = name.to_string();
        values.set_item(&name, value_to_py(py, value)?)?;
        causality.set_item(name, causality_to_py(py, causality_report(&explanation))?)?;
    }
    dict.set_item("values", values)?;
    dict.set_item("causality", causality)?;
    Ok(dict.unbind())
}

fn causality_to_py(py: Python<'_>, report: CausalResultReport) -> PyResult<Py<PyDict>> {
    let alternatives = PyList::empty(py);
    for report_explanation in report.alternatives {
        let explanation = PyDict::new(py);
        explanation.set_item("causes", causes_to_py(py, report_explanation.causes)?)?;
        alternatives.append(explanation)?;
    }
    let result = PyDict::new(py);
    result.set_item("alternatives", alternatives)?;
    Ok(result.unbind())
}

fn causes_to_py<'py>(
    py: Python<'py>,
    causes: impl IntoIterator<Item = CausalCauseReport>,
) -> PyResult<Py<PyList>> {
    let result = PyList::empty(py);
    for cause in causes {
        let item = PyDict::new(py);
        item.set_item("input", cause.input)?;
        item.set_item("logical_tick", cause.logical_tick)?;
        item.set_item(
            "roles",
            cause
                .roles
                .into_iter()
                .map(role_to_py_name)
                .collect::<Vec<_>>(),
        )?;
        result.append(item)?;
    }
    Ok(result.unbind())
}

fn role_to_py_name(role: CausalRole) -> &'static str {
    match role {
        CausalRole::Direct => "direct",
        CausalRole::Selection => "selection",
        CausalRole::Retention => "retention",
        CausalRole::Initialization => "initialization",
        CausalRole::Activation => "activation",
    }
}

fn value_to_py(py: Python<'_>, value: Value) -> PyResult<Py<PyAny>> {
    match value {
        Value::Int(value) => Ok(value.into_pyobject(py)?.unbind().into_any()),
        Value::Float(value) => Ok(value.into_pyobject(py)?.unbind().into_any()),
        Value::Str(value) => Ok(value.to_string().into_pyobject(py)?.unbind().into_any()),
        Value::Bool(value) => Ok(PyBool::new(py, value).to_owned().unbind().into_any()),
        Value::Unit => Ok(py.None()),
        Value::List(values) | Value::Tuple(values) => {
            let values = values
                .into_iter()
                .map(|value| value_to_py(py, value))
                .collect::<PyResult<Vec<_>>>()?;
            Ok(PyList::new(py, values)?.unbind().into_any())
        }
        Value::Map(values) => {
            let dict = PyDict::new(py);
            for (key, value) in values {
                dict.set_item(key.to_string(), value_to_py(py, value)?)?;
            }
            Ok(dict.unbind().into_any())
        }
        Value::Deferred => Ok(Py::new(py, DeferredValue)?.into_any()),
        Value::NoVal => Ok(Py::new(py, NoValue)?.into_any()),
        Value::Function(function) => {
            Ok(function.to_string().into_pyobject(py)?.unbind().into_any())
        }
        Value::Union(value) => {
            let payload = value
                .payload()
                .cloned()
                .map(|payload| value_to_py(py, payload))
                .transpose()?;
            Ok(Py::new(
                py,
                UnionValue {
                    tag: value.tag().to_string(),
                    payload,
                },
            )?
            .into_any())
        }
    }
}
