#![allow(unsafe_op_in_unsafe_fn)]

use dsline_core::{Backpressure as CoreBackpressure, ChannelError, DslineError, SpscConfig};
use dsline_ops::Record;
use dsline_shm::{shm_region_path, MmapSlotStorage, PersistentShmChannel};
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyModule, PySlice, PyType};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

/// Backend for named shared-memory channels: a named paging-file section on
/// Windows (coherent across processes without any flush), the file-backed
/// mapping elsewhere (already flush-free there).
#[cfg(windows)]
use dsline_shm::SectionShmSlotStorage;
#[cfg(windows)]
type NamedShmStorage = SectionShmSlotStorage;
#[cfg(not(windows))]
type NamedShmStorage = MmapSlotStorage;

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
    DropNewest,
    DropOldest,
}

impl From<Backpressure> for CoreBackpressure {
    fn from(value: Backpressure) -> Self {
        match value {
            Backpressure::Block => Self::Block,
            Backpressure::Raise => Self::Raise,
            Backpressure::DropNewest => Self::DropNewest,
            Backpressure::DropOldest => Self::DropOldest,
        }
    }
}

// ── ShmChannel (named OS shared memory, cross-process) ──

/// A shared-memory channel addressed by name.
///
/// All processes that use the same `name` (and the same capacity and
/// slot_size) connect to the same memory-mapped region, so one process can
/// send while another receives.
#[pyclass]
struct ShmChannel {
    name: String,
    path: String,
    channel: Mutex<PersistentShmChannel<NamedShmStorage>>,
}

#[pymethods]
impl ShmChannel {
    #[new]
    #[pyo3(signature = (name, capacity=1024, slot_size=4096, backpressure=Backpressure::Block, timeout=None, multi_producer=false))]
    fn new(
        name: String,
        capacity: usize,
        slot_size: usize,
        backpressure: Backpressure,
        timeout: Option<f64>,
        multi_producer: bool,
    ) -> PyResult<Self> {
        if name.trim().is_empty() {
            return Err(PyValueError::new_err("channel name must not be empty"));
        }

        let config = channel_config(capacity, slot_size, backpressure, timeout, multi_producer)?;
        let path = shm_region_path(&name).map_err(to_py_err)?;
        let path = path.to_string_lossy().into_owned();
        let channel = PersistentShmChannel::<NamedShmStorage>::create_or_open(&path, config)
            .map_err(to_py_err)?;

        Ok(Self {
            name,
            path,
            channel: Mutex::new(channel),
        })
    }

    fn send(&self, py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<()> {
        let bytes_type = PyModule::import_bound(py, "builtins")?.getattr("bytes")?;
        let bytes_obj = bytes_type.call1((data,))?;
        let bytes = bytes_obj.downcast::<PyBytes>()?.as_bytes();
        // Release the GIL for the send: with `Block` backpressure the Rust
        // side spins until the consumer frees space, and a consumer running
        // as another thread of this same process needs the GIL to drain the
        // ring — holding both deadlocks until the send timeout.
        py.allow_threads(|| {
            let mut channel = self.lock_channel()?;
            channel.send(bytes).map_err(to_py_err)
        })
    }

    fn recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let mut channel = self.lock_channel()?;
        let data = channel.recv().map_err(to_py_err)?;
        Ok(PyBytes::new_bound(py, &data))
    }

    /// Non-blocking receive: `bytes`, or `None` when nothing is readable
    /// right now. This is the call to spin on — `recv()` costs a bounded
    /// COMMITTED grace poll and raises `BufferEmptyError` on every empty
    /// attempt, so an exception-driven poll loop measures the interpreter's
    /// exception path rather than the channel.
    fn try_recv<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let mut channel = self.lock_channel()?;
        match channel.try_recv().map_err(to_py_err)? {
            Some(data) => Ok(Some(PyBytes::new_bound(py, &data))),
            None => Ok(None),
        }
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
    fn path(&self) -> &str {
        &self.path
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
        stats.set_item("path", &self.path)?;
        stats.set_item("backend", "shm")?;
        stats.set_item("multi_producer", channel.multi_producer())?;
        stats.set_item("queue_depth", channel.len())?;
        stats.set_item("queue_capacity", channel.capacity())?;
        stats.set_item("slot_size", channel.payload_slot_size())?;
        set_message_size_limit_items(&stats, channel.message_size_limits())?;
        stats.set_item("next_sequence", channel.next_sequence())?;
        stats.set_item("expected_recv_sequence", channel.expected_recv_sequence())?;
        stats.set_item("last_received_sequence", channel.last_received_sequence())?;
        stats.set_item("reclaimed_slots", channel.reclaimed_slots())?;
        stats.set_item("pinned_slots", channel.pinned_slots().map_err(to_py_err)?)?;
        stats.set_item("closed", channel.is_closed())?;
        stats.set_item("empty", channel.is_empty())?;
        Ok(stats)
    }

    /// Explicitly reclaim crashed writers' leftovers whose identity is
    /// provably dead (roadmap §6 "owner executes cleanup"). Returns the
    /// number of slots freed. Leftovers deeper in the ring are reclaimed
    /// automatically when the consumer's tail reaches them.
    fn reclaim_stale(&self) -> PyResult<u64> {
        let mut channel = self.lock_channel()?;
        channel.reclaim_stale().map_err(to_py_err)
    }

    /// Test/diagnostics aid: leave a WRITING header with an arbitrary
    /// writer identity at the producer's next slot, exactly as a writer
    /// killed mid-send would leave the region. Not part of the production
    /// protocol.
    #[pyo3(signature = (seq, pid, start_time, reserved_slots=1))]
    fn _debug_leave_zombie(
        &self,
        seq: u64,
        pid: u32,
        start_time: u64,
        reserved_slots: u32,
    ) -> PyResult<()> {
        let mut channel = self.lock_channel()?;
        channel
            .debug_leave_zombie(seq, pid, start_time, reserved_slots)
            .map_err(to_py_err)
    }

    // ── Zero-copy alloc/publish (roadmap §4.2) ──

    /// Reserve a slot for a zero-copy message and return a writable buffer
    /// whose writes land directly in the shared region. Publish it with
    /// `publish()` (or free it with `abort()`); the buffer seals on
    /// publish and rejects further writes.
    fn alloc(&self, size: usize) -> PyResult<ShmWriteBuffer> {
        let mut channel = self.lock_channel()?;
        let lease = channel.alloc(size).map_err(to_py_err)?;
        Ok(ShmWriteBuffer {
            inner: Mutex::new(Some(lease)),
        })
    }

    /// Seal and commit a zero-copy buffer allocated by `alloc`: checksum
    /// the payload in place, write the frame header, flip the slot to
    /// COMMITTED, publish the head cursor. The buffer seals — further
    /// writes raise `ValueError`.
    fn publish(&self, buffer: &Bound<'_, ShmWriteBuffer>) -> PyResult<u64> {
        let mut channel = self.lock_channel()?;
        let buf_ref = buffer.borrow();
        let mut slot = buf_ref
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("buffer lock is poisoned"))?;
        let lease = slot
            .as_mut()
            .ok_or_else(|| PyValueError::new_err("buffer was already aborted"))?;
        channel.publish(lease).map_err(to_py_err)
    }

    /// Free an unpublished `alloc` reservation. Only the newest
    /// unpublished buffer of the handle can be unwound.
    fn abort(&self, buffer: &Bound<'_, ShmWriteBuffer>) -> PyResult<()> {
        let mut channel = self.lock_channel()?;
        let buf_ref = buffer.borrow();
        let mut slot = buf_ref
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("buffer lock is poisoned"))?;
        let lease = slot
            .take()
            .ok_or_else(|| PyValueError::new_err("buffer was already aborted"))?;
        channel.abort(lease).map_err(to_py_err)
    }

    /// Receive the next message as a zero-copy view over the shared
    /// region. The view pins its slot until released (`release()`, the
    /// `with` block, or dropping the object): while pinned, producers
    /// cannot wrap the ring onto it. Partial reads (`view.read(off, n)`)
    /// copy only the requested bytes.
    fn recv_view(&self) -> PyResult<ShmReadView> {
        let mut channel = self.lock_channel()?;
        let view = channel.recv_view().map_err(to_py_err)?;
        Ok(ShmReadView {
            inner: Mutex::new(Some(view)),
        })
    }

    fn __len__(&self) -> PyResult<usize> {
        Ok(self.lock_channel()?.len())
    }
}

impl ShmChannel {
    fn lock_channel(
        &self,
    ) -> PyResult<std::sync::MutexGuard<'_, PersistentShmChannel<NamedShmStorage>>> {
        self.channel
            .lock()
            .map_err(|_| PyRuntimeError::new_err("channel lock is poisoned"))
    }
}

fn channel_config(
    capacity: usize,
    slot_size: usize,
    backpressure: Backpressure,
    timeout: Option<f64>,
    multi_producer: bool,
) -> PyResult<SpscConfig> {
    if matches!(timeout, Some(value) if value < 0.0) {
        return Err(PyValueError::new_err("timeout must be non-negative"));
    }

    Ok(SpscConfig {
        capacity,
        slot_size,
        backpressure: backpressure.into(),
        timeout: timeout.map(Duration::from_secs_f64),
        multi_producer,
    })
}

fn set_message_size_limit_items(
    stats: &Bound<'_, PyDict>,
    limits: dsline_shm::MessageSizeLimits,
) -> PyResult<()> {
    stats.set_item("chunk_metadata_size", limits.chunk_metadata_size)?;
    stats.set_item("chunk_payload_size", limits.chunk_payload_size)?;
    stats.set_item("max_message_size", limits.max_message_size)?;
    Ok(())
}

// ── FileChannel (mmap-backed file, cross-process) ──

/// A channel backed by a memory-mapped file that can be shared across
/// processes.
///
/// One process calls `create()` to initialise the file, then both
/// producer and consumer call `open()` to connect.
#[pyclass]
struct FileChannel {
    path: String,
    channel: Mutex<PersistentShmChannel<MmapSlotStorage>>,
}

#[pymethods]
impl FileChannel {
    /// Create a new file-backed channel, initialising the backing file.
    #[staticmethod]
    #[pyo3(signature = (path, capacity, slot_size, backpressure=Backpressure::Raise, timeout=None, multi_producer=false))]
    fn create(
        path: String,
        capacity: usize,
        slot_size: usize,
        backpressure: Backpressure,
        timeout: Option<f64>,
        multi_producer: bool,
    ) -> PyResult<Self> {
        let config = channel_config(capacity, slot_size, backpressure, timeout, multi_producer)?;
        let channel =
            PersistentShmChannel::<MmapSlotStorage>::create(&path, config).map_err(to_py_err)?;
        Ok(Self {
            path,
            channel: Mutex::new(channel),
        })
    }

    /// Open an existing file-backed channel.
    #[staticmethod]
    #[pyo3(signature = (path, capacity, slot_size, backpressure=Backpressure::Raise, timeout=None, multi_producer=false))]
    fn open(
        path: String,
        capacity: usize,
        slot_size: usize,
        backpressure: Backpressure,
        timeout: Option<f64>,
        multi_producer: bool,
    ) -> PyResult<Self> {
        let config = channel_config(capacity, slot_size, backpressure, timeout, multi_producer)?;
        let channel =
            PersistentShmChannel::<MmapSlotStorage>::open(&path, config).map_err(to_py_err)?;
        Ok(Self {
            path,
            channel: Mutex::new(channel),
        })
    }

    fn send(&self, py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<()> {
        let bytes_type = PyModule::import_bound(py, "builtins")?.getattr("bytes")?;
        let bytes_obj = bytes_type.call1((data,))?;
        let bytes = bytes_obj.downcast::<PyBytes>()?.as_bytes();
        // See `ShmChannel::send`: a blocking send must not hold the GIL.
        py.allow_threads(|| {
            let mut channel = self
                .channel
                .lock()
                .map_err(|_| PyRuntimeError::new_err("file channel lock poisoned"))?;
            channel.send(bytes).map_err(to_py_err)
        })
    }

    fn recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let mut channel = self
            .channel
            .lock()
            .map_err(|_| PyRuntimeError::new_err("file channel lock poisoned"))?;
        let data = channel.recv().map_err(to_py_err)?;
        Ok(PyBytes::new_bound(py, &data))
    }

    /// Non-blocking receive: `bytes`, or `None` when nothing is readable
    /// right now (see `ShmChannel::try_recv`).
    fn try_recv<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let mut channel = self
            .channel
            .lock()
            .map_err(|_| PyRuntimeError::new_err("file channel lock poisoned"))?;
        match channel.try_recv().map_err(to_py_err)? {
            Some(data) => Ok(Some(PyBytes::new_bound(py, &data))),
            None => Ok(None),
        }
    }

    fn recv_with_seq<'py>(&self, py: Python<'py>) -> PyResult<(u64, Bound<'py, PyBytes>)> {
        let mut channel = self
            .channel
            .lock()
            .map_err(|_| PyRuntimeError::new_err("file channel lock poisoned"))?;
        let message = channel.recv_with_seq().map_err(to_py_err)?;
        Ok((message.seq, PyBytes::new_bound(py, &message.payload)))
    }

    fn close(&self) -> PyResult<()> {
        let mut channel = self
            .channel
            .lock()
            .map_err(|_| PyRuntimeError::new_err("file channel lock poisoned"))?;
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
    fn path(&self) -> &str {
        &self.path
    }

    #[getter]
    fn closed(&self) -> PyResult<bool> {
        Ok(self
            .channel
            .lock()
            .map_err(|_| PyRuntimeError::new_err("file channel lock poisoned"))?
            .is_closed())
    }

    #[getter]
    fn empty(&self) -> PyResult<bool> {
        Ok(self
            .channel
            .lock()
            .map_err(|_| PyRuntimeError::new_err("file channel lock poisoned"))?
            .is_empty())
    }

    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let channel = self
            .channel
            .lock()
            .map_err(|_| PyRuntimeError::new_err("file channel lock poisoned"))?;
        let stats = PyDict::new_bound(py);
        stats.set_item("path", &self.path)?;
        stats.set_item("backend", "file")?;
        stats.set_item("multi_producer", channel.multi_producer())?;
        stats.set_item("queue_depth", channel.len())?;
        stats.set_item("queue_capacity", channel.capacity())?;
        stats.set_item("slot_size", channel.payload_slot_size())?;
        set_message_size_limit_items(&stats, channel.message_size_limits())?;
        stats.set_item("next_sequence", channel.next_sequence())?;
        stats.set_item("expected_recv_sequence", channel.expected_recv_sequence())?;
        stats.set_item("last_received_sequence", channel.last_received_sequence())?;
        stats.set_item("reclaimed_slots", channel.reclaimed_slots())?;
        stats.set_item("pinned_slots", channel.pinned_slots().map_err(to_py_err)?)?;
        stats.set_item("closed", channel.is_closed())?;
        stats.set_item("empty", channel.is_empty())?;
        Ok(stats)
    }

    /// Explicitly reclaim crashed writers' leftovers whose identity is
    /// provably dead. Returns the number of slots freed.
    fn reclaim_stale(&self) -> PyResult<u64> {
        let mut channel = self
            .channel
            .lock()
            .map_err(|_| PyRuntimeError::new_err("file channel lock poisoned"))?;
        channel.reclaim_stale().map_err(to_py_err)
    }

    /// Test/diagnostics aid: leave a WRITING header with an arbitrary
    /// writer identity at the producer's next slot. Not part of the
    /// production protocol.
    #[pyo3(signature = (seq, pid, start_time, reserved_slots=1))]
    fn _debug_leave_zombie(
        &self,
        seq: u64,
        pid: u32,
        start_time: u64,
        reserved_slots: u32,
    ) -> PyResult<()> {
        let mut channel = self
            .channel
            .lock()
            .map_err(|_| PyRuntimeError::new_err("file channel lock poisoned"))?;
        channel
            .debug_leave_zombie(seq, pid, start_time, reserved_slots)
            .map_err(to_py_err)
    }

    // ── Zero-copy alloc/publish (roadmap §4.2) ──
    //
    // Same protocol as ShmChannel; on FileChannel the "shared region" is
    // the mapped file (a deleted-while-mapped caveat applies on Windows —
    // a live view keeps the mapping, and therefore the file, alive).

    /// Reserve a slot for a zero-copy message and return a writable buffer
    /// whose writes land directly in the mapped file.
    fn alloc(&self, size: usize) -> PyResult<ShmWriteBuffer> {
        let mut channel = self
            .channel
            .lock()
            .map_err(|_| PyRuntimeError::new_err("file channel lock poisoned"))?;
        let lease = channel.alloc(size).map_err(to_py_err)?;
        Ok(ShmWriteBuffer {
            inner: Mutex::new(Some(lease)),
        })
    }

    /// Seal and commit a zero-copy buffer allocated by `alloc`.
    fn publish(&self, buffer: &Bound<'_, ShmWriteBuffer>) -> PyResult<u64> {
        let mut channel = self
            .channel
            .lock()
            .map_err(|_| PyRuntimeError::new_err("file channel lock poisoned"))?;
        let buf_ref = buffer.borrow();
        let mut slot = buf_ref
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("buffer lock is poisoned"))?;
        let lease = slot
            .as_mut()
            .ok_or_else(|| PyValueError::new_err("buffer was already aborted"))?;
        channel.publish(lease).map_err(to_py_err)
    }

    /// Free an unpublished `alloc` reservation.
    fn abort(&self, buffer: &Bound<'_, ShmWriteBuffer>) -> PyResult<()> {
        let mut channel = self
            .channel
            .lock()
            .map_err(|_| PyRuntimeError::new_err("file channel lock poisoned"))?;
        let buf_ref = buffer.borrow();
        let mut slot = buf_ref
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("buffer lock is poisoned"))?;
        let lease = slot
            .take()
            .ok_or_else(|| PyValueError::new_err("buffer was already aborted"))?;
        channel.abort(lease).map_err(to_py_err)
    }

    /// Receive the next message as a zero-copy view over the mapped file.
    fn recv_view(&self) -> PyResult<ShmReadView> {
        let mut channel = self
            .channel
            .lock()
            .map_err(|_| PyRuntimeError::new_err("file channel lock poisoned"))?;
        let view = channel.recv_view().map_err(to_py_err)?;
        Ok(ShmReadView {
            inner: Mutex::new(Some(view)),
        })
    }

    fn __len__(&self) -> PyResult<usize> {
        Ok(self
            .channel
            .lock()
            .map_err(|_| PyRuntimeError::new_err("file channel lock poisoned"))?
            .len())
    }
}

// ── Zero-copy buffer/view objects (shared by both channel types) ──

/// A writable handle into the shared region returned by
/// `ShmChannel.alloc()` / `FileChannel.alloc()`.
///
/// Write the payload with `write()` (or slice assignment
/// ``buf[0:4] = b"...."``), then hand it to the channel's ``publish()``.
/// Publishing seals the buffer: further writes raise `ValueError`, and the
/// message becomes visible to consumers. Dropping the buffer without
/// publishing leaves the slot WRITING — freed when this process exits
/// (crash recovery), or immediately via `channel.abort(buf)`.
#[pyclass]
struct ShmWriteBuffer {
    inner: Mutex<Option<dsline_shm::ShmWriteLease>>,
}

#[pymethods]
impl ShmWriteBuffer {
    /// Write `data` (a bytes-like object) into the payload at `offset`,
    /// directly into the shared region.
    #[pyo3(signature = (data, offset=0))]
    fn write(&self, py: Python<'_>, data: &Bound<'_, PyAny>, offset: usize) -> PyResult<()> {
        let bytes_type = PyModule::import_bound(py, "builtins")?.getattr("bytes")?;
        let bytes_obj = bytes_type.call1((data,))?;
        let bytes = bytes_obj.downcast::<PyBytes>()?.as_bytes();
        let slot = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("buffer lock is poisoned"))?;
        let lease = slot
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("buffer was already aborted"))?;
        lease.write(offset, bytes).map_err(to_py_err)
    }

    /// Slice assignment: ``buf[start:stop] = data`` writes into the
    /// region in place (step must be 1). Int keys are not supported — use
    /// ``write()`` with an offset instead.
    fn __setitem__(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let slice = key
            .downcast::<PySlice>()
            .map_err(|_| PyValueError::new_err("buffer indices must be slices"))?;
        let size = self.size()?;
        let indices = slice.indices(size as std::os::raw::c_long)?;
        if indices.step != 1 {
            return Err(PyValueError::new_err("slice step must be 1"));
        }
        let start = indices.start as usize;
        let length = indices.slicelength as usize;
        let bytes_type = PyModule::import_bound(py, "builtins")?.getattr("bytes")?;
        let bytes_obj = bytes_type.call1((value,))?;
        let bytes = bytes_obj.downcast::<PyBytes>()?.as_bytes();
        if bytes.len() != length {
            return Err(PyValueError::new_err(format!(
                "slice length {} does not match data length {}",
                length,
                bytes.len()
            )));
        }
        self.write(py, value, start as usize)
    }

    fn __len__(&self) -> PyResult<usize> {
        self.size()
    }

    #[getter]
    fn size(&self) -> PyResult<usize> {
        self.with_lease(|lease| Ok(lease.size()))
    }

    #[getter]
    fn seq(&self) -> PyResult<u64> {
        self.with_lease(|lease| Ok(lease.seq()))
    }

    #[getter]
    fn slot_index(&self) -> PyResult<usize> {
        self.with_lease(|lease| Ok(lease.slot_index()))
    }

    #[getter]
    fn sealed(&self) -> PyResult<bool> {
        self.with_lease(|lease| Ok(lease.is_sealed()))
    }

    fn __repr__(&self) -> PyResult<String> {
        self.with_lease(|lease| {
            Ok(format!(
                "<ShmWriteBuffer seq={} size={} sealed={}>",
                lease.seq(),
                lease.size(),
                lease.is_sealed()
            ))
        })
    }
}

impl ShmWriteBuffer {
    /// Run `f` with the lease, under the buffer lock. Aborted buffers
    /// (lease already taken by `abort`) raise `ValueError`.
    fn with_lease<R>(
        &self,
        f: impl FnOnce(&dsline_shm::ShmWriteLease) -> PyResult<R>,
    ) -> PyResult<R> {
        let slot = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("buffer lock is poisoned"))?;
        match slot.as_ref() {
            Some(lease) => f(lease),
            None => Err(PyValueError::new_err("buffer was already aborted")),
        }
    }
}

/// A read-only zero-copy view returned by `ShmChannel.recv_view()` /
/// `FileChannel.recv_view()`.
///
/// The view pins its slot: the message is consumed, but the payload bytes
/// stay valid in the shared region until the view is released
/// (`release()`, leaving the `with` block, or dropping the object).
/// While any view is pinned, producers cannot wrap the ring onto its slot
/// — release views promptly or the ring fills to a standstill. Reads copy
/// exactly the requested bytes out of the region; the transport itself
/// never moves them.
#[pyclass]
struct ShmReadView {
    inner: Mutex<Option<dsline_shm::ShmRecvView>>,
}

#[pymethods]
impl ShmReadView {
    /// Copy `size` bytes (default: to the end) of the payload starting at
    /// `offset` out of the shared region.
    #[pyo3(signature = (offset=0, size=None))]
    fn read<'py>(
        &self,
        py: Python<'py>,
        offset: usize,
        size: Option<usize>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        self.with_view(|view| {
            let len = view.size();
            let end = size.unwrap_or(len.saturating_sub(offset));
            let data = view.read(offset, end).map_err(to_py_err)?;
            Ok(PyBytes::new_bound(py, &data))
        })
    }

    /// Copy the whole payload out of the shared region.
    fn read_all<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        self.with_view(|view| {
            let data = view.read_all().map_err(to_py_err)?;
            Ok(PyBytes::new_bound(py, &data))
        })
    }

    /// ``view[i]`` → one payload byte as int; ``view[a:b]`` → bytes.
    fn __getitem__<'py>(&self, py: Python<'py>, key: &Bound<'_, PyAny>) -> PyResult<PyObject> {
        if let Ok(index) = key.extract::<usize>() {
            return self.with_view(|view| {
                let byte = view.read(index, 1).map_err(to_py_err)?;
                Ok((byte[0] as i32).to_object(py))
            });
        }
        if let Ok(slice) = key.downcast::<PySlice>() {
            let size = self.with_view(|view| Ok(view.size()))?;
            let indices = slice.indices(size as std::os::raw::c_long)?;
            if indices.step != 1 {
                return Err(PyValueError::new_err("slice step must be 1"));
            }
            let start = indices.start as usize;
            let length = indices.slicelength as usize;
            return self.with_view(|view| {
                let data = view.read(start as usize, length).map_err(to_py_err)?;
                Ok(PyBytes::new_bound(py, &data).to_object(py))
            });
        }
        Err(PyValueError::new_err(
            "view indices must be integers or slices",
        ))
    }

    fn __len__(&self) -> PyResult<usize> {
        self.with_view(|view| Ok(view.size()))
    }

    /// Release the view: its slot returns to FREE and the pin floor
    /// advances. Idempotence is rejected — a double release is a bug.
    fn release(&self) -> PyResult<()> {
        let mut slot = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("view lock is poisoned"))?;
        match slot.as_mut() {
            Some(view) => view.release().map_err(to_py_err),
            None => Err(PyValueError::new_err("view is already released")),
        }
    }

    #[getter]
    fn released(&self) -> PyResult<bool> {
        let slot = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("view lock is poisoned"))?;
        Ok(slot.as_ref().map(|view| view.is_released()).unwrap_or(true))
    }

    #[getter]
    fn seq(&self) -> PyResult<u64> {
        self.with_view(|view| Ok(view.seq()))
    }

    #[getter]
    fn size(&self) -> PyResult<usize> {
        self.with_view(|view| Ok(view.size()))
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
        self.release()?;
        Ok(false)
    }

    fn __repr__(&self) -> PyResult<String> {
        self.with_view(|view| {
            Ok(format!(
                "<ShmReadView seq={} size={}>",
                view.seq(),
                view.size()
            ))
        })
    }
}

impl ShmReadView {
    /// Run `f` with the view, under the view lock. Released views raise
    /// `ValueError` (an access-after-release is a bug, not an empty read).
    fn with_view<R>(&self, f: impl FnOnce(&dsline_shm::ShmRecvView) -> PyResult<R>) -> PyResult<R> {
        let slot = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("view lock is poisoned"))?;
        match slot.as_ref() {
            Some(view) => f(view),
            None => Err(PyValueError::new_err("view is already released")),
        }
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
    /// Apply a per-element callable inside a batch, reassemble results.
    MapPyBatch(PyObject),
    /// Filter elements inside a batch with a per-element callable.
    FilterPyBatch(PyObject),
    /// Accumulate items into batches of the given size.
    Batch(usize),
    /// Remove all but the named columns from dict items.
    SelectDict(Vec<String>),
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
    /// Optional transport URL for lazy source resolution.
    transport_source: Option<String>,
}

#[pymethods]
impl PyPipeline {
    #[new]
    fn new() -> Self {
        Self {
            ops: Vec::new(),
            transport_source: None,
        }
    }

    /// Create a pipeline with a transport URL as source.
    ///
    /// Supported schemes:
    /// - `shm://name?capacity=N&slot_size=N` — opens a shared-memory channel
    ///
    /// After building the pipeline, call `.collect()` without arguments to
    /// drain the transport source and process it.
    #[staticmethod]
    fn from_transport(url: &str) -> PyResult<Self> {
        let parsed: dsline_transport::TransportUrl = url
            .parse()
            .map_err(|e: DslineError| PyValueError::new_err(e.to_string()))?;

        let scheme = parsed.scheme.as_str();
        if scheme != "shm" && scheme != "file" {
            return Err(PyValueError::new_err(format!(
                "unsupported transport scheme '{scheme}'; supported: shm, file"
            )));
        }
        // Validate query params parse.
        let _: usize = parsed
            .query_param("capacity")
            .unwrap_or("64")
            .parse()
            .map_err(|_| PyValueError::new_err("invalid capacity"))?;
        let _: usize = parsed
            .query_param("slot_size")
            .unwrap_or("4096")
            .parse()
            .map_err(|_| PyValueError::new_err("invalid slot_size"))?;

        Ok(Self {
            ops: Vec::new(),
            transport_source: Some(url.to_string()),
        })
    }

    /// Resolve the transport source to a Python list by draining the channel.
    fn _drain_transport_source(&self, py: Python<'_>) -> PyResult<PyObject> {
        let url_str = self
            .transport_source
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("no transport source configured"))?;

        let parsed: dsline_transport::TransportUrl = url_str
            .parse()
            .map_err(|e: DslineError| PyValueError::new_err(e.to_string()))?;

        let capacity: usize = parsed
            .query_param("capacity")
            .unwrap_or("64")
            .parse()
            .map_err(|_| PyValueError::new_err("invalid capacity"))?;
        let slot_size: usize = parsed
            .query_param("slot_size")
            .unwrap_or("4096")
            .parse()
            .map_err(|_| PyValueError::new_err("invalid slot_size"))?;

        let config = SpscConfig {
            capacity,
            slot_size,
            backpressure: CoreBackpressure::Raise,
            timeout: None,
            multi_producer: false,
        };

        let mut items: Vec<PyObject> = Vec::new();

        match parsed.scheme.as_str() {
            "shm" => {
                let path = shm_region_path(parsed.target()).map_err(to_py_err)?;
                let mut channel =
                    PersistentShmChannel::<NamedShmStorage>::create_or_open(&path, config)
                        .map_err(to_py_err)?;
                while let Ok(data) = channel.recv() {
                    push_parsed_item(&data, &mut items, py);
                }
            }
            "file" => {
                let mut channel =
                    PersistentShmChannel::<MmapSlotStorage>::open(parsed.target(), config)
                        .map_err(to_py_err)?;
                while let Ok(data) = channel.recv() {
                    push_parsed_item(&data, &mut items, py);
                }
            }
            _ => unreachable!(),
        }

        Ok(PyList::new_bound(py, &items).into())
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

    /// Add a Python-callable map stage that operates per-element inside a batch.
    ///
    /// The callable receives a single element from each batch. The Rust
    /// side iterates over the batch, calls the function per element, and
    /// reassembles the results into a new batch.
    fn map_py_batch(&mut self, callable: PyObject) {
        self.ops.push(PipelineOp::MapPyBatch(callable));
    }

    /// Add a Python-callable filter stage that operates per-element inside a batch.
    ///
    /// The callable receives a single element and returns truthy/falsy.
    /// Elements returning falsy are removed from the batch.
    fn filter_py_batch(&mut self, callable: PyObject) {
        self.ops.push(PipelineOp::FilterPyBatch(callable));
    }

    /// Group items into batches of `size`.
    ///
    /// After this operator, subsequent stages receive Python lists.
    /// A trailing partial batch is emitted as-is.
    fn batch(&mut self, size: usize) -> PyResult<()> {
        if size == 0 {
            return Err(PyValueError::new_err(
                "batch size must be greater than zero",
            ));
        }
        self.ops.push(PipelineOp::Batch(size));
        Ok(())
    }

    /// Keep only the named columns in each dict item.
    fn select(&mut self, columns: Vec<String>) {
        self.ops.push(PipelineOp::SelectDict(columns));
    }

    /// Parse a transport URL and return the scheme and target.
    ///
    /// Returns `(scheme, target, params)` where params is a dict of query
    /// parameters. Used by `from_transport` to decide how to connect.
    #[staticmethod]
    fn parse_transport_url(url: &str) -> PyResult<(String, String, PyObject)> {
        let parsed: dsline_transport::TransportUrl = url
            .parse()
            .map_err(|e: DslineError| PyValueError::new_err(e.to_string()))?;
        let py = unsafe { Python::assume_gil_acquired() };
        let params = PyDict::new_bound(py);
        for (k, v) in parsed.query_pairs() {
            params.set_item(k, v)?;
        }
        Ok((
            parsed.scheme.to_string(),
            parsed.target().to_string(),
            params.into(),
        ))
    }

    /// Execute the pipeline against a Python iterable and collect results.
    ///
    /// Each source item passes through all operators in order. Items dropped
    /// by filter stages are excluded from the output.
    ///
    /// If `source` is `None` and the pipeline was created via
    /// `from_transport()`, the transport channel is drained and used.
    #[pyo3(signature = (source=None))]
    fn collect(&self, py: Python<'_>, source: Option<&Bound<'_, PyAny>>) -> PyResult<PyObject> {
        let src_py: PyObject = match source {
            Some(s) => s.clone().into(),
            None => self._drain_transport_source(py)?,
        };
        let src = src_py.bind(py);

        let results = PyList::empty_bound(py);
        let mut iter = src.iter()?;

        // Locate the batch operator, if any.
        let batch_idx = self
            .ops
            .iter()
            .position(|op| matches!(op, PipelineOp::Batch(_)));
        let batch_size = batch_idx.map(|i| match &self.ops[i] {
            PipelineOp::Batch(n) => *n,
            _ => unreachable!(),
        });

        let pre_ops = &self.ops[..batch_idx.unwrap_or(self.ops.len())];
        let post_ops = &self.ops[batch_idx.map(|i| i + 1).unwrap_or(self.ops.len())..];

        let mut buf: Vec<Item> = Vec::new();
        let buf_cap = batch_size.unwrap_or(0);

        loop {
            let py_item = match iter.next() {
                Some(Ok(v)) => v,
                Some(Err(e)) => return Err(e),
                None => break,
            };

            // Run pre-batch ops per-item.
            let item = match run_ops(pre_ops, Item::from_py(&py_item)?, py)? {
                Some(it) => it,
                None => continue,
            };

            if let Some(n) = batch_size {
                buf.push(item);
                if buf.len() >= n {
                    let chunk = std::mem::replace(&mut buf, Vec::with_capacity(buf_cap));
                    let batch = Item::Batch(chunk);
                    if let Some(processed) = run_ops(post_ops, batch, py)? {
                        results.append(processed.to_py(py)?)?;
                    }
                }
            } else {
                // No batch: run remaining ops directly.
                if let Some(processed) = run_ops(post_ops, item, py)? {
                    results.append(processed.to_py(py)?)?;
                }
            }
        }

        // Flush trailing partial batch.
        if !buf.is_empty() {
            let remainder = std::mem::take(&mut buf);
            let batch = Item::Batch(remainder);
            if let Some(processed) = run_ops(post_ops, batch, py)? {
                results.append(processed.to_py(py)?)?;
            }
        }

        Ok(results.into())
    }

    /// Run the pipeline against a source and send results to a channel.
    ///
    /// Each output item is formatted via `str()` and sent as bytes.
    /// Returns the number of items sent.
    fn send_to(
        &self,
        py: Python<'_>,
        source: &Bound<'_, PyAny>,
        channel: &Bound<'_, ShmChannel>,
    ) -> PyResult<usize> {
        let results = self.collect(py, Some(source))?;
        let list = results.bind(py).downcast::<PyList>()?;
        let ch = channel.borrow_mut();
        let mut inner = ch.lock_channel()?;
        let mut sent = 0usize;
        for item in list {
            let s = item.str()?.to_string_lossy().to_string();
            inner.send(s.as_bytes()).map_err(to_py_err)?;
            sent += 1;
        }
        Ok(sent)
    }

    /// Drain all items from a channel, apply pipeline operators, and
    /// return the results as a list.
    fn receive_from(&self, py: Python<'_>, channel: &Bound<'_, ShmChannel>) -> PyResult<PyObject> {
        let ch = channel.borrow_mut();
        let mut inner = ch.lock_channel()?;
        let mut items: Vec<PyObject> = Vec::new();
        while let Ok(data) = inner.recv() {
            let s = String::from_utf8_lossy(&data).to_string();
            if s.is_empty() {
                continue;
            }
            if let Ok(v) = s.parse::<f64>() {
                items.push(v.to_object(py));
            } else {
                items.push(s.to_object(py));
            }
        }
        drop(inner);
        drop(ch);

        self.collect(py, Some(PyList::new_bound(py, &items).as_any()))
    }

    fn __repr__(&self) -> String {
        if self.ops.is_empty() {
            "Pipeline(empty)".into()
        } else {
            format!("Pipeline({} stages)", self.ops.len())
        }
    }
}

/// Run a slice of ops against an item. Returns `Ok(None)` when a filter
/// drops the item.
fn run_ops(ops: &[PipelineOp], mut item: Item, py: Python<'_>) -> PyResult<Option<Item>> {
    for op in ops {
        let next = match op {
            PipelineOp::FilterExpr(ast) => {
                let rec = item.as_record();
                let keep = dsline_ops::eval_bool(ast, &rec).unwrap_or(false);
                if keep {
                    Some(item)
                } else {
                    None
                }
            }
            PipelineOp::MapExpr(ast) => {
                let rec = item.as_record();
                let val = dsline_ops::eval(ast, &rec).unwrap_or(f64::NAN);
                Some(Item::Float(val))
            }
            PipelineOp::FilterPy(callable) => {
                let py_item = item.to_py(py)?;
                let result = callable.call1(py, (py_item,))?;
                if result.is_truthy(py)? {
                    Some(item)
                } else {
                    None
                }
            }
            PipelineOp::MapPy(callable) => {
                let py_item = item.to_py(py)?;
                let result = callable.call1(py, (py_item,))?;
                Some(Item::from_py(result.bind(py))?)
            }
            PipelineOp::MapPyBatch(callable) => {
                match &item {
                    Item::Batch(elements) => {
                        let mut results = Vec::with_capacity(elements.len());
                        for elem in elements {
                            let py_elem = elem.to_py(py)?;
                            let result = callable.call1(py, (py_elem,))?;
                            results.push(Item::from_py(result.bind(py))?);
                        }
                        Some(Item::Batch(results))
                    }
                    _ => {
                        // Not a batch — treat as regular map_py.
                        let py_item = item.to_py(py)?;
                        let result = callable.call1(py, (py_item,))?;
                        Some(Item::from_py(result.bind(py))?)
                    }
                }
            }
            PipelineOp::FilterPyBatch(callable) => {
                match &item {
                    Item::Batch(elements) => {
                        let mut kept = Vec::with_capacity(elements.len());
                        for elem in elements {
                            let py_elem = elem.to_py(py)?;
                            let result = callable.call1(py, (py_elem,))?;
                            if result.is_truthy(py)? {
                                kept.push(elem.clone());
                            }
                        }
                        Some(Item::Batch(kept))
                    }
                    _ => {
                        // Not a batch — treat as regular filter_py.
                        let py_item = item.to_py(py)?;
                        let result = callable.call1(py, (py_item,))?;
                        if result.is_truthy(py)? {
                            Some(item)
                        } else {
                            None
                        }
                    }
                }
            }
            PipelineOp::SelectDict(columns) => match &mut item {
                Item::Dict(map) => {
                    let keys: Vec<String> = map.keys().cloned().collect();
                    for k in keys {
                        if !columns.contains(&k) {
                            map.remove(&k);
                        }
                    }
                    Some(item)
                }
                _ => Some(item),
            },
            PipelineOp::Batch(_) => Some(item),
        };
        match next {
            Some(it) => item = it,
            None => return Ok(None),
        }
    }
    Ok(Some(item))
}

// ── Item: union type for pipeline values ──

#[derive(Clone)]
enum Item {
    Float(f64),
    Dict(HashMap<String, f64>),
    /// A batch of items emitted by the `Batch` operator.
    Batch(Vec<Item>),
}

impl Item {
    fn from_py(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        // dict-like — store numeric values, silently skip non-numeric columns.
        if let Ok(dict) = obj.downcast::<PyDict>() {
            let mut map = HashMap::new();
            for (k, v) in dict {
                let key: String = k.extract()?;
                if let Ok(val) = v.extract::<f64>() {
                    map.insert(key, val);
                }
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
            Item::Batch(items) => {
                let list = PyList::empty_bound(py);
                for item in items {
                    list.append(item.to_py(py)?)?;
                }
                Ok(list.into())
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
            Item::Batch(_) => None,
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

fn push_parsed_item(data: &[u8], items: &mut Vec<PyObject>, py: Python<'_>) {
    let s = String::from_utf8_lossy(data).to_string();
    if s.is_empty() {
        return;
    }
    if let Ok(v) = s.parse::<f64>() {
        items.push(v.to_object(py));
    } else {
        items.push(s.to_object(py));
    }
}

#[pyfunction]
fn process_identity() -> (u32, u64) {
    let identity = dsline_shm::current_process_identity();
    (identity.pid, identity.start_time)
}

#[pymodule]
fn _dsline(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Backpressure>()?;
    module.add_class::<ShmChannel>()?;
    module.add_class::<FileChannel>()?;
    module.add_class::<ShmWriteBuffer>()?;
    module.add_class::<ShmReadView>()?;
    module.add_class::<PyPipeline>()?;
    module.add_function(wrap_pyfunction!(process_identity, module)?)?;
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
