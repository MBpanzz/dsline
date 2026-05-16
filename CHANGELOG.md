# Changelog

## 0.0.1

Initial test version for repository publication.

- Adds Rust workspace skeleton for core, shm, transport, pipeline, ops, and Python bindings.
- Implements fixed-slot SPSC bytes channel prototype over in-process storage.
- Adds frame header and metadata TLV encode/decode.
- Adds memory and file-backed fixed-slot storage backends.
- Exposes Python `dsline.ShmChannel` through PyO3.
- Adds `dsline info` and `dsline bench shm` CLI entry points.
- Documents zero-copy boundaries and current prototype status.
