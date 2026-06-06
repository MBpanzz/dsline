# Changelog

## 0.0.1

Initial test version for repository publication.

- Adds Rust workspace skeleton for core, shm, transport, pipeline, ops, and Python bindings.
- Implements fixed-slot SPSC bytes channel prototype over in-process storage.
- Adds frame header, chunk flags, and metadata TLV encode/decode.
- Adds memory and file-backed fixed-slot storage backends.
- Adds multi-slot chunking for bytes messages larger than one slot.
- Exposes Python `dsline.ShmChannel` through PyO3.
- Adds `DropNewest` and `DropOldest` backpressure strategies for fixed-slot SPSC channels.
- Aligns Python `FileChannel` stats, `recv_with_seq`, and backpressure options with `ShmChannel`.
- Aligns Python `FileChannel.send()` bytes-like input handling with `ShmChannel.send()`.
- Reports chunk metadata, chunk payload, and maximum message size in channel stats.
- Adds `dsline info` and `dsline bench shm` CLI entry points.
- Documents zero-copy boundaries and current prototype status.
