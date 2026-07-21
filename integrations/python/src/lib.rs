// TODO: Convert between Python values and the Rust `trustworthiness_checker` core types.
// TODO: Keep runtime and parser logic delegated to the root library crate.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures::{FutureExt, StreamExt};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};
use smol::LocalExecutor;
use tc_core::core::{ExecutionPolicy, OutputStream, Runtime, RuntimeSpec, Semantics};
use tc_core::io::InputController;
use tc_core::io::testing::{ManualInputController, ManualOutputHandler, channel};
use tc_core::runtime::RuntimeBuilder;
use tc_core::runtime::builder::GeneralRuntimeBuilder;
use tc_core::{DsrvSpecification, InputEvent, Value, VarName, lang::dsrv::parser::parse_str};

type OutputBatch = BTreeMap<VarName, Value>;

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

#[pyclass(module = "trustworthiness_checker", unsendable)]
struct TcRuntime {
    executor: Rc<LocalExecutor<'static>>,
    input_vars: BTreeSet<VarName>,
    input_controller: ManualInputController<Value>,
    tick_controller: InputController,
    outputs: OutputStream<OutputBatch>,
}

#[pymethods]
impl TcRuntime {
    #[new]
    #[pyo3(signature = (model, *, semantics="typed-untimed"))]
    fn new(model: &Bound<'_, PyAny>, semantics: &str) -> PyResult<Self> {
        let model = model_to_text(model)?;
        let semantics = parse_semantics(semantics)?;
        Self::from_model(model, semantics)
    }

    #[staticmethod]
    #[pyo3(signature = (model, semantics=None))]
    fn from_text(model: &str, semantics: Option<&str>) -> PyResult<Self> {
        let semantics = parse_semantics(semantics.unwrap_or("typed-untimed"))?;
        Self::from_model(model.to_string(), semantics)
    }

    #[staticmethod]
    #[pyo3(signature = (path, semantics=None))]
    fn from_path(path: &Bound<'_, PyAny>, semantics: Option<&str>) -> PyResult<Self> {
        let model = read_pathlike(path)?;
        let semantics = parse_semantics(semantics.unwrap_or("typed-untimed"))?;
        Self::from_model(model, semantics)
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

        let provided = values.keys().cloned().collect::<BTreeSet<_>>();
        let extra = provided
            .difference(&self.input_vars)
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
            .map(|(var, value)| InputEvent::new(var, value))
            .collect();
        smol::block_on(self.executor.run(async {
            self.input_controller.send_step(events).await?;
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
        Ok(Some(output_to_py_dict(py, output)?))
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

fn parse_semantics(semantics: &str) -> PyResult<Semantics> {
    match normalize_option(semantics).as_str() {
        "typed" | "typed-untimed" => Ok(Semantics::TypedUntimed),
        "untyped" | "untimed" => Ok(Semantics::Untimed),
        "gradual" | "gradual-typed" | "gradual-typed-untimed" => Ok(Semantics::GradualTypedUntimed),
        _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "unsupported semantics {semantics:?}; expected 'typed-untimed', 'untimed', or 'gradual-typed-untimed'"
        ))),
    }
}

fn normalize_option(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace('_', "-")
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
    Ok(())
}

impl TcRuntime {
    fn from_model(model: String, semantics: Semantics) -> PyResult<Self> {
        let executor = Rc::new(LocalExecutor::new());
        let (input_vars, input_controller, tick_controller, outputs) =
            build_runtime(executor.clone(), model, semantics).map_err(|err| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "failed to initialise trustworthiness checker runtime: {err:#}"
                ))
            })?;

        Ok(Self {
            executor,
            input_vars,
            input_controller,
            tick_controller,
            outputs,
        })
    }

    fn wait_for_next_output(
        &mut self,
        py: Python<'_>,
        timeout: Option<f64>,
    ) -> PyResult<Option<OutputBatch>> {
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
            if let Some(output) = self.outputs.next().now_or_never() {
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
    model: String,
    semantics: Semantics,
) -> anyhow::Result<(
    BTreeSet<VarName>,
    ManualInputController<Value>,
    InputController,
    OutputStream<OutputBatch>,
)> {
    let runtime_executor = executor.clone();

    smol::block_on(executor.run(async move {
        let spec: DsrvSpecification = parse_str(model.as_str())
            .map_err(|err| anyhow::anyhow!("example model could not be parsed: {err:?}"))?;
        let input_vars = spec.input_vars().clone();
        let (input, input_controller) = channel();
        let (input, tick_controller) = tc_core::io::controlled(input);

        let mut output_handler = Box::new(ManualOutputHandler::new(
            runtime_executor.clone(),
            spec.output_vars().clone(),
        ));
        let outputs = output_handler.get_output();

        let monitor = GeneralRuntimeBuilder::<DsrvSpecification, Value>::new()
            .executor(runtime_executor.clone())
            .model(spec)
            .input(input)
            .output(output_handler)
            .runtime(RuntimeSpec::Dataflow(ExecutionPolicy::Synchronous))
            .semantics(semantics)
            .build()
            .await;

        runtime_executor.spawn(monitor.run()).detach();

        Ok((input_vars, input_controller, tick_controller, outputs))
    }))
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
    }
}
