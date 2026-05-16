#![allow(unsafe_op_in_unsafe_fn)]

use dsline_core::{Backpressure as CoreBackpressure, ChannelError, DslineError, SpscConfig};
use dsline_ops::Record;
use dsline_shm::ShmSpscChannel;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyModule, PyType};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

// ── Python exceptions ──

create_exception!(_dsline, DslineErrorPy, PyException);
create_exception!(_dsline, ChannelErrorPy, DslineErrorPy);
create_exception!(_dsline, ChannelClosedError, ChannelErrorPy);
create_exception!(_dsline, BufferFullError, ChannelErrorPy);
create_exception!(_dsline, BufferEmptyError, ChannelErrorPy);
create_exception!(_dsline, MessageTooLargeError, ChannelErrorPy);
create_exception!(_dsline, CorruptedMessageError, ChannelErrorPy);
create_exception!(_dsline, SequenceMismatchError, ChannelErrorPy);
create_exception!(_dsline, PipelineBuildError, DslineErrorPy);

// ── Backpressure ──

#[pyclass]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Backpressure {
    Block,
    Raise,
}

impl From<Backpressure> for CoreBackpressure {
    fn from(value: Backpressure) -> Self {
        match value {
            Backpressure::Block => Self::Block,
            Backpressure::Raise => Self::Raise,
        }
    }
}

// ── ShmChannel ──

#[pyclass]
struct ShmChannel {
    name: String,
    channel: Mutex<ShmSpscChannel>,
}

#[pymethods]
impl ShmChannel {
    #[new]
    #[pyo3(signature = (name, capacity=1024, slot_size=4096, backpressure=Backpressure::Block, timeout=None))]
    fn new(
        name: String,
        capacity: usize,
        slot_size: usize,
        backpressure: Backpressure,
        timeout: Option<f64>,
    ) -> PyResult<Self> {
        if name.trim().is_empty() {
            return Err(PyValueError::new_err("channel name must not be empty"));
        }
        if matches!(timeout, Some(value) if value < 0.0) {
            return Err(PyValueError::new_err("timeout must be non-negative"));
        }

        let config = SpscConfig {
            capacity,
            slot_size,
            backpressure: backpressure.into(),
            timeout: timeout.map(Duration::from_secs_f64),
        };
        let channel = ShmSpscChannel::new(config).map_err(to_py_err)?;

        Ok(Self {
            name,
            channel: Mutex::new(channel),
        })
    }

    fn send(&self, py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<()> {
        let bytes_type = PyModule::import_bound(py, "builtins")?.getattr("bytes")?;
        let bytes_obj = bytes_type.call1((data,))?;
        let bytes = bytes_obj.downcast::<PyBytes>()?.as_bytes();
        let mut channel = self.lock_channel()?;
        channel.send(bytes).map_err(to_py_err)
    }

    fn recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let mut channel = self.lock_channel()?;
        let data = channel.recv().map_err(to_py_err)?;
        Ok(PyBytes::new_bound(py, &data))
    }

    fn recv_with_seq<'py>(&self, py: Python<'py>) -> PyResult<(u64, Bound<'py, PyBytes>)> {
        let mut channel = self.lock_channel()?;
        let message = channel.recv_with_seq().map_err(to_py_err)?;
        Ok((message.seq, PyBytes::new_bound(py, &message.payload)))
    }

    fn close(&self) -> PyResult<()> {
        let mut channel = self.lock_channel()?;
        channel.close();
        Ok(())
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        _exc_type: Option<&Bound<'_, PyType>>,
        _exc: Option<&Bound<'_, PyAny>>,
        _tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.close()?;
        Ok(false)
    }

    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    #[getter]
    fn closed(&self) -> PyResult<bool> {
        Ok(self.lock_channel()?.is_closed())
    }

    #[getter]
    fn capacity(&self) -> PyResult<usize> {
        Ok(self.lock_channel()?.capacity())
    }

    #[getter]
    fn slot_size(&self) -> PyResult<usize> {
        Ok(self.lock_channel()?.payload_slot_size())
    }

    #[getter]
    fn empty(&self) -> PyResult<bool> {
        Ok(self.lock_channel()?.is_empty())
    }

    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let channel = self.lock_channel()?;
        let stats = PyDict::new_bound(py);
        stats.set_item("name", &self.name)?;
        stats.set_item("backend", "inprocess-prototype")?;
        stats.set_item("queue_depth", channel.len())?;
        stats.set_item("queue_capacity", channel.capacity())?;
        stats.set_item("slot_size", channel.payload_slot_size())?;
        stats.set_item("next_sequence", channel.next_sequence())?;
        stats.set_item("expected_recv_sequence", channel.expected_recv_sequence())?;
        stats.set_item("last_received_sequence", channel.last_received_sequence())?;
        stats.set_item("closed", channel.is_closed())?;
        stats.set_item("empty", channel.is_empty())?;
        Ok(stats)
    }

    fn __len__(&self) -> PyResult<usize> {
        Ok(self.lock_channel()?.len())
    }
}

impl ShmChannel {
    fn lock_channel(&self) -> PyResult<std::sync::MutexGuard<'_, ShmSpscChannel>> {
        self.channel
            .lock()
            .map_err(|_| PyRuntimeError::new_err("channel lock is poisoned"))
    }
}

// ── Pipeline ──

#[doc(hidden)]
#[derive(Clone)]
enum PipelineOp {
    FilterExpr(dsline_ops::Expr),
    MapExpr(dsline_ops::Expr),
    FilterPy(PyObject),
    MapPy(PyObject),
}

/// A composable pipeline of operators applied to an in-process data source.
///
/// Build a pipeline by calling `.source()`, then chain `.filter_expr()`,
/// `.map_expr()`, `.filter_py()`, `.map_py()`, and finally `.collect()`.
///
/// ```python
/// p = dsline.Pipeline()
/// result = p.source([1, 2, 3, 4]).filter_expr("x > 2").map_expr("x * 10").collect()
/// assert result == [30.0, 40.0]
/// ```
#[pyclass]
struct PyPipeline {
    ops: Vec<PipelineOp>,
}

#[pymethods]
impl PyPipeline {
    #[new]
    fn new() -> Self {
        Self { ops: Vec::new() }
    }

    /// Add an expr-lite filter stage.
    fn filter_expr(&mut self, expr: &str) -> PyResult<()> {
        let ast = dsline_ops::parse_expr(expr)
            .map_err(|e| PipelineBuildError::new_err(format!("invalid filter expression: {e}")))?;
        self.ops.push(PipelineOp::FilterExpr(ast));
        Ok(())
    }

    /// Add an expr-lite map stage.
    fn map_expr(&mut self, expr: &str) -> PyResult<()> {
        let ast = dsline_ops::parse_expr(expr)
            .map_err(|e| PipelineBuildError::new_err(format!("invalid map expression: {e}")))?;
        self.ops.push(PipelineOp::MapExpr(ast));
        Ok(())
    }

    /// Add a Python-callable filter stage (slow path).
    ///
    /// The callable receives one item and should return a truthy/falsy
    /// value. Items for which it returns falsy are dropped.
    fn filter_py(&mut self, callable: PyObject) {
        self.ops.push(PipelineOp::FilterPy(callable));
    }

    /// Add a Python-callable map stage (slow path).
    ///
    /// The callable receives one item and should return the transformed value.
    fn map_py(&mut self, callable: PyObject) {
        self.ops.push(PipelineOp::MapPy(callable));
    }

    /// Execute the pipeline against a Python iterable and collect results.
    ///
    /// Each source item passes through all operators in order. Items dropped
    /// by filter stages are excluded from the output.
    fn collect(&self, py: Python<'_>, source: &Bound<'_, PyAny>) -> PyResult<PyObject> {
        let results = PyList::empty_bound(py);
        let mut iter = source.iter()?;

        'outer: loop {
            let item = match iter.next() {
                Some(Ok(v)) => v,
                Some(Err(e)) => return Err(e),
                None => break,
            };

            let mut current = Item::from_py(&item)?;

            for op in &self.ops {
                match op {
                    PipelineOp::FilterExpr(ast) => {
                        let rec = current.as_record();
                        let keep = dsline_ops::eval_bool(ast, &rec).unwrap_or(false);
                        if !keep {
                            continue 'outer;
                        }
                    }
                    PipelineOp::MapExpr(ast) => {
                        let rec = current.as_record();
                        let val = dsline_ops::eval(ast, &rec).unwrap_or(f64::NAN);
                        current = Item::Float(val);
                    }
                    PipelineOp::FilterPy(callable) => {
                        let py_item = current.to_py(py)?;
                        let result = callable.call1(py, (py_item,))?;
                        if !result.is_truthy(py)? {
                            continue 'outer;
                        }
                    }
                    PipelineOp::MapPy(callable) => {
                        let py_item = current.to_py(py)?;
                        let result = callable.call1(py, (py_item,))?;
                        current = Item::from_py(result.bind(py))?;
                    }
                }
            }

            results.append(current.to_py(py)?)?;
        }

        Ok(results.into())
    }

    fn __repr__(&self) -> String {
        if self.ops.is_empty() {
            "Pipeline(empty)".into()
        } else {
            format!("Pipeline({} stages)", self.ops.len())
        }
    }
}

// ── Item: union type for pipeline values ──

enum Item {
    Float(f64),
    Dict(HashMap<String, f64>),
}

impl Item {
    fn from_py(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        // dict-like
        if let Ok(dict) = obj.downcast::<PyDict>() {
            let mut map = HashMap::new();
            for (k, v) in dict {
                let key: String = k.extract()?;
                let val: f64 = match v.extract::<f64>() {
                    Ok(f) => f,
                    Err(_) => {
                        return Err(PyValueError::new_err(format!(
                            "pipeline dict value for key '{key}' is not numeric"
                        )));
                    }
                };
                map.insert(key, val);
            }
            return Ok(Item::Dict(map));
        }

        // numeric scalar
        let val: f64 = obj.extract().map_err(|_| {
            PyValueError::new_err("pipeline item must be a number or dict[str, number]")
        })?;
        Ok(Item::Float(val))
    }

    fn to_py(&self, py: Python<'_>) -> PyResult<PyObject> {
        match self {
            Item::Float(v) => Ok(v.to_object(py)),
            Item::Dict(map) => {
                let dict = PyDict::new_bound(py);
                for (k, v) in map {
                    dict.set_item(k, v)?;
                }
                Ok(dict.into())
            }
        }
    }

    fn as_record(&self) -> RecordAdapter<'_> {
        RecordAdapter(self)
    }
}

/// Adapter that presents an `Item` as an ops `Record`.
struct RecordAdapter<'a>(&'a Item);

impl Record for RecordAdapter<'_> {
    fn column(&self, name: &str) -> Option<f64> {
        match self.0 {
            Item::Float(v) => match name {
                "x" => Some(*v),
                _ => None,
            },
            Item::Dict(map) => map.get(name).copied(),
        }
    }
}

// ── error mapping ──

fn to_py_err(err: DslineError) -> PyErr {
    match err {
        DslineError::Channel(ChannelError::Closed) => ChannelClosedError::new_err(err.to_string()),
        DslineError::Channel(ChannelError::BufferFull) => BufferFullError::new_err(err.to_string()),
        DslineError::Channel(ChannelError::BufferEmpty) => {
            BufferEmptyError::new_err(err.to_string())
        }
        DslineError::Channel(ChannelError::MessageTooLarge { .. }) => {
            MessageTooLargeError::new_err(err.to_string())
        }
        DslineError::Channel(ChannelError::CorruptedMessage) => {
            CorruptedMessageError::new_err(err.to_string())
        }
        DslineError::Channel(ChannelError::SequenceMismatch { .. }) => {
            SequenceMismatchError::new_err(err.to_string())
        }
        DslineError::Channel(ChannelError::InvalidConfig(_)) => {
            PyValueError::new_err(err.to_string())
        }
        DslineError::Channel(ChannelError::StorageIo(_)) => {
            ChannelErrorPy::new_err(err.to_string())
        }
        DslineError::Protocol(_) => DslineErrorPy::new_err(err.to_string()),
        DslineError::Transport(_) => DslineErrorPy::new_err(err.to_string()),
    }
}

// ── module init ──

#[pymodule]
fn _dsline(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Backpressure>()?;
    module.add_class::<ShmChannel>()?;
    module.add_class::<PyPipeline>()?;
    module.add("DslineError", _py.get_type_bound::<DslineErrorPy>())?;
    module.add("ChannelError", _py.get_type_bound::<ChannelErrorPy>())?;
    module.add(
        "ChannelClosedError",
        _py.get_type_bound::<ChannelClosedError>(),
    )?;
    module.add("BufferFullError", _py.get_type_bound::<BufferFullError>())?;
    module.add("BufferEmptyError", _py.get_type_bound::<BufferEmptyError>())?;
    module.add(
        "MessageTooLargeError",
        _py.get_type_bound::<MessageTooLargeError>(),
    )?;
    module.add(
        "CorruptedMessageError",
        _py.get_type_bound::<CorruptedMessageError>(),
    )?;
    module.add(
        "SequenceMismatchError",
        _py.get_type_bound::<SequenceMismatchError>(),
    )?;
    module.add(
        "PipelineBuildError",
        _py.get_type_bound::<PipelineBuildError>(),
    )?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
