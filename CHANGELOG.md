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
- Adds real cross-process shared-memory channels: `ShmChannel` is now a named mmap-backed channel (region files in the platform runtime directory), and `FileChannel` memory-maps its backing file instead of per-operation seek/flush syscalls.
- Adds shared head/tail cursors as the ring position source: the per-operation O(capacity) slot-header scan is replaced by an O(1) `refresh_cursors` over two shared monotonic slot counts in the region header, killing the refresh scan cost documented by the benchmarks. The full scan runs once per open (recovery): it rebuilds the producer's next message sequence, the consumer's expected sequence, and steps over in-flight WRITING slots left by a crashed writer; `recovery_bias` keeps those leftovers in the queue depth so the send guard never over-commits a physically full ring.
- Adds a COMMITTED gate with a bounded poll to the receive path: the head cursor is published together with the whole batch, so a crash-free run never exposes an uncommitted slot, and a WRITING slot that survives the grace period (a crash leftover) is surfaced as an empty ring instead of an infinite poll.
- Publishes batches with one flush per message on Windows (page-local flushes fall back for large offsets): the raw FlushViewOfFile cost (~45 µs per call on this machine) is measured and documented in `docs/benchmarks.md`.
- Replaces the file-backed mapping with a named paging-file section as the Windows storage backend for named channels (`SectionShmSlotStorage`): `CreateFileMappingW` on the system paging file (`multiprocessing.shared_memory` style), which is coherent across processes without `FlushViewOfFile` — the measured per-message flush cost disappears from the Windows send/recv path. The section name derives from the resolved region path (`Local\dsline-<digest>.bin`), no backing file is created on Windows, and the section dies with its last handle (no stale-region cleanup). `ShmChannel` selects the backend per platform: paging-file section on Windows, file-backed mapping elsewhere.
- Grows the region header to 20 bytes (`[head: u64 @ 0] [tail: u64 @ 8] [magic: u32 @ 16]`): the creator initializes every slot header and publishes `REGION_MAGIC` last (release store); openers poll for it before recovering the ring, so no peer observes a half-initialized region — required by the section backend, where a fresh region is zero-filled and indistinguishable from mid-init. `create_or_open` on named sections is atomic (the OS guarantees exactly one creator).
- Extends `verify/check_invariants.py` with cursor-protocol checks: Z3 proves the wrap guard partitions the circle and the send guard (incl. the crash-recovery bias) never writes into an occupied index; CP-SAT proves the cursor gate theorem (crash-free depth never includes an uncommitted slot) and witnesses the crash-leftover zombie state; SCIP verifies the cursor-depth and batch-flush bounds as MIP optima; the dual-handle replay (R4) runs a producer and a consumer over one region through the real implementation.
- Adds Rust unit tests for the cursor protocol: dual-handle round trips, crash-recovery zombie skipping, the biased send guard, WRITING-only ring reads, and rejoin-after-drain.
- Adds `SlotRegionIo` byte-range storage trait with file and memory-mapped backends, `MmapSlotStorage` (memmap2), and name-to-region resolution (`shm_region_path`).
- Generalizes `PersistentShmChannel` over the storage backend with a `create_or_open` constructor for named channels.
- Reports chunk metadata, chunk payload, and maximum message size in channel stats.
- Adds `dsline info` and `dsline bench shm` CLI entry points.
- Adds first cross-process tests (spawn-based producer/consumer round-trips for `ShmChannel` and `FileChannel`, including chunked messages).
- Adds solver-based invariant verification (`verify/check_invariants.py`) with Z3, CP-SAT, and SCIP: sequence-ordering properties, send-plan arithmetic, ring invariants, formal reproduction of the `len`-underflow bug, recovery inference, and replay against the implementation.
- Adds cross-process benchmark harness (`benches/cross_process.py`) vs mp.Queue/mp.Pipe/stdlib shared_memory/ZeroMQ/pynng with throughput, latency, and in-process sections; results in `docs/benchmarks.md`.
- Fixes cross-process races found by the stress tests: Windows file-backed mmap visibility now uses `flush_async_range` (FlushViewOfFile) instead of `flush_range` (FlushFileBuffers disk sync, ~1.2 ms/message); tail inference is now min-seq over committed/pinned slots (correct for gapped rings); a WRITING-only ring behaves as empty; chunked-receive error paths release the pinned slot.
- Documents zero-copy boundaries and current prototype status.
