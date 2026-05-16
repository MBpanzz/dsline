#![allow(unsafe_op_in_unsafe_fn)]

use dsline_core::{Backpressure as CoreBackpressure, ChannelError, DslineError, SpscConfig};
use dsline_shm::ShmSpscChannel;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyModule, PyType};
use std::sync::Mutex;
use std::time::Duration;

create_exception!(_dsline, DslineErrorPy, PyException);
create_exception!(_dsline, ChannelErrorPy, DslineErrorPy);
create_exception!(_dsline, ChannelClosedError, ChannelErrorPy);
create_exception!(_dsline, BufferFullError, ChannelErrorPy);
create_exception!(_dsline, BufferEmptyError, ChannelErrorPy);
create_exception!(_dsline, MessageTooLargeError, ChannelErrorPy);
create_exception!(_dsline, CorruptedMessageError, ChannelErrorPy);

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
        DslineError::Channel(ChannelError::InvalidConfig(_)) => {
            PyValueError::new_err(err.to_string())
        }
        DslineError::Channel(ChannelError::StorageIo(_)) => {
            ChannelErrorPy::new_err(err.to_string())
        }
        DslineError::Protocol(_) => DslineErrorPy::new_err(err.to_string()),
    }
}

#[pymodule]
fn _dsline(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Backpressure>()?;
    module.add_class::<ShmChannel>()?;
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
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
