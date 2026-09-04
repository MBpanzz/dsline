# Project Progress Report — 2026-05-16

## Overview

`dsline` is a lightweight local dataflow framework for Python, built with a Rust performance core and PyO3 bindings. Target: high-throughput, low-latency multi-process communication via shared memory.

- **Current version**: 0.0.1 (SPSC prototype validation)
- **License**: MIT
- **MSRV**: Rust 1.75
- **Python**: ≥3.9

---

## Update — 2026-09-04: shared cursors replace the per-operation ring scan

The benchmark analysis identified two design costs dominating the channel's
throughput: the O(capacity) `refresh_from_storage` scan on every operation,
and (on Windows) the `FlushViewOfFile` required for cross-process visibility
of file-backed mappings. This update removes the first cost entirely and
quantifies the second:

- **Ring position now comes from shared monotonic slot counts.** The region
  header holds the producer-owned `head` count (published with a release
  store after each message's batch commit) and the consumer-owned `tail`
  count (advanced with a fetch-max on every release, and by `DropOldest`).
  `refresh_cursors` reloads both in O(1); the queue depth is
  `head − tail (+ recovery_bias)`. The full slot-header scan now runs only
  once per open (recovery), rebuilding the producer's `next_seq`, the
  consumer's `expected_recv_seq`, and the write position past any WRITING
  slot left by a crashed writer.
- **Crash-leftover handling is preserved and verified.** A producer that
  died mid-write leaves slots the cursors cannot represent; `recovery_bias`
  keeps them in the queue depth so the send guard never over-commits a
  physically full ring, and the receive path's COMMITTED gate (a bounded
  poll) surfaces the leftover as an empty ring instead of hanging — the
  historical WRITING-only-ring rule, generalized.
- **Solver verification extended to the cursor protocol** (40 checks, all
  passing): Z3 proves the wrap guard partitions the circle and that the
  send guard (with and without the recovery bias) never writes into an
  occupied index; CP-SAT proves the gate theorem (a crash-free run never
  exposes an uncommitted slot to the O(1) refresh) and produces a witness
  for the crash-leftover zombie state; SCIP confirms the cursor-depth and
  batch-flush bounds as MIP optima; a new dual-handle replay (R4) runs a
  producer and a consumer over one region through the real implementation.
- **Windows flush cost measured directly:** FlushViewOfFile costs ~45 µs
  per call on this machine regardless of the range size (measured via a raw
  Win32 probe), and a Rust micro-benchmark shows the same round trip is
  222 µs with the flush and 4.3 µs without — the flush is now the entire
  remaining per-message cost. Batch publishes are coalesced to one flush
  per message (page-local flushes for large offsets). The Windows answer to
  the flush is the paging-file-backed section (`multiprocessing`
  `shared_memory` style, coherent without flush) — still the 0.1.0
  "anonymous OS shared memory" item.
- Fixes pre-existing WIP compile errors in `dsline-shm` (create_new call
  sites missing `total_len`, unguarded `ptr.add`).
- Tests: 41 Rust unit tests in `dsline-shm` (5 new cursor-protocol tests),
  81 Python tests, 40/40 solver checks.

Still not implemented (unchanged): anonymous OS shared memory without a
backing file, `alloc/publish` zero-copy (safety gate pending), MPSC, crash
recovery, arena allocator, cleanup CLI.

---

## Update — 2026-08-02: real shared-memory backend

The channel layer moved from in-process prototype to genuine cross-process shared memory:

- `ShmChannel` is no longer an in-process prototype. It is a named shared-memory channel: all processes using the same name connect to the same memory-mapped region (`stats()["backend"]` is now `"shm"`). Region files live in the platform runtime directory (`%LOCALAPPDATA%\dsline\shm\` on Windows, `$XDG_RUNTIME_DIR`/`$TMPDIR` on Unix).
- New `SlotRegionIo` byte-range storage trait with two backends: `FileSlotStorage` (seek/flush file I/O, retained as reference) and `MmapSlotStorage` (memmap2, the default OS shared-memory backend).
- `PersistentSlotRegion` / `PersistentShmChannel` are now generic over the storage backend; `create_or_open` initialises a region on first use without truncating existing data.
- `FileChannel` now memory-maps its backing file instead of issuing seek/flush syscalls per operation.
- Cross-process blocking backpressure now works for real: `Block` with a timeout observes the consumer freeing space through the shared mapping.
- First genuine cross-process tests added (`multiprocessing` spawn-based producer/consumer round-trips for both `ShmChannel` and `FileChannel`), including a chunked message spanning multiple slots.
- Fixed a cross-process race found by those tests: the consumer's ring depth could undercount while the producer was mid-write, panicking on `len -= 1` during chunked-message receive; depth is now saturating (it is re-derived from storage on every operation).

### Status table changes

| Area | Before | After |
|---|---|---|
| ShmChannel backend | in-process memory only | mmap-backed named region, cross-process |
| FileChannel IO | seek/write/flush syscalls per slot | memory-mapped file |
| Cross-process tests | none | spawn-based producer/consumer round-trips |
| Python `stats()` backend | `"inprocess-prototype"` | `"shm"` |

Still not implemented (unchanged): anonymous OS shared memory without a backing file, `alloc/publish` zero-copy (safety gate pending), MPSC, crash recovery, arena allocator, cleanup CLI.

---

## Verification — 2026-08-02: solver-based invariant checks

`verify/check_invariants.py` cross-checks the channel logic with three
solvers (Z3 SMT, OR-Tools CP-SAT, SCIP MILP) plus a replay pass against the
real implementation — 22 checks, all passing:

- **Z3**: `sequence_precedes` is irreflexive/asymmetric and transitive within
  a <2^63 window (boundary counterexample found); `send_plan` chunk arithmetic
  verified over the whole config domain; the `next_seq` recovery fold is exact
  when 0 is in the seq window.
- **CP-SAT**: formally reproduced the historical `len`-underflow bug (mid-write
  refresh undercounts a chunked message) and verified the saturating fix;
  verified the aggregate ring guard never writes into an occupied slot; verified
  recovery inference for contiguous runs and produced a concrete witness for the
  documented non-contiguous (gap) limitation.
- **SCIP**: message-size formulas hold as MILP optima; worst-case region size
  matches the layout formula; SCIP and CP-SAT agree differentially.
- **Replay**: `stats()` limits, on-disk region size, and a 400-step random
  schedule all match the compiled implementation.

Two boundary findings, both benign in practice and documented in
`verify/README.md`: the sequence comparison is only a strict order within a
window of span < 2^63, and the `next_seq` fold can be order-sensitive when a
region is opened fresh after ~2^63 messages (seqs straddle the u64 midpoint
outside 0's window).

---

## Benchmarks & race fixes — 2026-08-02

### Cross-process benchmark (`benches/cross_process.py`, full report in `docs/benchmarks.md`)

dsline ShmChannel/FileChannel vs mp.Queue, mp.Pipe, a hand-rolled
`multiprocessing.shared_memory` SPSC ring, ZeroMQ and pynng (tcp loopback),
plus in-process thread baselines. Summary (Windows, 16 cores):

- dsline: ~13–15k msg/s small messages, ~0.16 GiB/s at 64 KiB, ~108 µs p50
  latency — slower than mp.Pipe (337k msg/s, 7.9 µs) and far slower than the
  stdlib shared-memory ring (2.0M msg/s, 0.95 µs); beats ZeroMQ/pynng at
  4 KiB+.
- The gap is dominated by two design costs: the O(capacity) `refresh`
  full-ring scan on every operation, and — on Windows only — the
  `FlushViewOfFile` required for cross-process visibility of file-backed
  mappings (~30–60 µs/message; Linux `MAP_SHARED` needs no flush).

### Race fixes found by the cross-process stress tests

The cross-process tests (producer/consumer over a wrapped ring with chunked
messages) exposed and fixed four real bugs:

1. **Windows visibility**: file-backed mmap writes are not visible to other
   processes' mappings without `FlushViewOfFile`. The first implementation
   called memmap2 `flush_range` per message, which on Windows also runs
   `FlushFileBuffers` (full disk sync, ~1.2 ms/message). Replaced with
   release/acquire fences + `flush_async_range` (FlushViewOfFile only).
2. **Tail inference**: the predecessor-based rule ("first occupied slot with
   a free predecessor") mis-identifies the ring tail once the consumer's
   releases split the ring into runs and the producer's wrapped writes place
   newer messages at lower indices — consuming newest-first. Replaced with
   min-seq (wrapping-aware, chunk-index tie-break) over committed/pinned
   slots, which is correct for every occupancy pattern.
3. **WRITING-only ring**: a refresh that observes only the producer's
   in-flight WRITING slots must behave as an empty ring (consumer spins),
   not error.
4. **Pin leak**: `recv_chunked_message`'s error paths returned without
   releasing the PINNED chunk slot, leaving the consumer stuck polling a
   never-COMMITTED slot forever. All error paths now release the pin.

After the fixes: cross-process tests pass 10/10 stress runs (previously
flaky), 30/30 repro runs.

---

## What Was Built

### Rust crates

| Crate | Lines | Description | Tests |
|---|---|---|---|
| `dsline-core` | ~350 | SPSC ring buffer, Frame protocol (magic+header+TLV), FNV-1a checksum, error hierarchy with Channel/Protocol/Transport variants | 14 |
| `dsline-shm` | ~920 | Fixed-slot storage trait, MemorySlotStorage, FileSlotStorage, slot state machine (FREE→WRITING→COMMITTED→PINNED→CORRUPTED), ShmSpscChannel, PersistentSlotRegion (file-stored headers for cross-process), PersistentShmChannel with ring recovery | 21 |
| `dsline-transport` | ~280 | TransportScheme enum (shm/bus/unix/tcp), TransportUrl parser, query parameter extraction, TCP port validation, Transport trait | 18 |
| `dsline-ops` | ~440 | expr-lite: recursive-descent parser (14 precedence levels), Expr AST (Literal/Column/Binary/Unary), eval/eval_bool evaluator, Record trait for column lookups, Display round-trip, stray-token rejection | 20 |
| `dsline-pipeline` | ~360 | Stream/Sink traits, IterStream/CollectSink adapters, Pipeline<I,O> with composable operator chains (pipe), filter_expr/map_expr using expr-lite, Operator<I,O> type alias | 13 |
| `dsline-python` | ~520 | PyO3 bindings: ShmChannel (in-process), FileChannel (cross-proc file-backed), PyPipeline with 6 operator types (filter_expr/map_expr/filter_py/map_py/map_py_batch/filter_py_batch), batch/select operators, send_to/receive_from, from_transport(shm://, file://), parse_transport_url, 9 Python exception types | — |

### Python layer

| File | Description |
|---|---|
| `python/dsline/__init__.py` | Public API re-exports, import guard |
| `python/dsline/__main__.py` | CLI: `dsline info`, `dsline bench shm`, `dsline bench pipeline` |
| `python/dsline/_bench.py` | Channel + Pipeline benchmarks (dsline vs mp.Queue vs pure Python vs pandas vs pyarrow) |
| `python/dsline/_info.py` | System info display |

### Tests

| Layer | Count | Coverage |
|---|---|---|
| Rust unit tests | 86 | core, shm, transport, ops, pipeline |
| Rust doc tests | 5 | ops, pipeline, transport |
| Python tests | 62 | ShmChannel (10), Pipeline (26+8), CLI (3), Integration (4) |
| **Total** | **153** | — |

---

## Architecture

```
Python API
  ├── ShmChannel    (in-process SPSC bytes)
  ├── FileChannel   (cross-process persistent, file-backed)
  ├── Pipeline      (filter_expr/map_expr/map_py/filter_py/batch/select)
  └── CLI           (info, bench)

Rust core
  dsline-core       → SPSC ring, Frame encode/decode, checksum, errors
  dsline-shm        → FixedSlotRegion, PersistentSlotRegion, ShmSpscChannel, PersistentShmChannel
  dsline-transport  → TransportScheme, TransportUrl, Transport trait
  dsline-ops        → expr-lite parser + evaluator
  dsline-pipeline   → Stream, Sink, Pipeline<I,O>, filter_expr, map_expr
  dsline-python     → PyO3 bindings for all of the above
```

---

## Feature Status

### Working

- Fixed-slot SPSC bytes channel (in-process)
- Cross-process file-backed SPSC (two processes, one file, verified)
- Variable-length bytes messages via multi-slot chunking
- Frame protocol with magic, version, checksum, metadata TLV
- Slot state machine with write-commit-read-release lifecycle
- Ring recovery on channel open (scan + reconstruct head/tail)
- expr-lite expression engine (arithmetic, comparison, logic, not, parens)
- Composable Pipeline with type-safe operator chains
- Python Pipeline with 6 operator types
- Batch processing with partial trailing batch flush
- Column selection (`select`) for dict records
- Channel ←→ Pipeline integration (send_to, receive_from)
- Transport URL parsing (shm, bus, unix, tcp)
- `from_transport("shm://...")` and `from_transport("file://...")`
- Context manager support on all channels
- Channel stats (queue_depth, capacity, sequence tracking)
- Backpressure strategies BLOCK, RAISE, DROP_NEWEST, DROP_OLDEST
- CLI with info and bench commands

### Not Yet Implemented (0.1.0+)

- OS shared memory (POSIX shm / mmap) — currently file-backed
- Arena-backed variable-length allocator — current support uses multi-slot chunking
- MPSC / multi-consumer — SPSC only
- alloc/publish zero-copy — requires safety gate (Miri, loom, stress testing)
- Pipeline background runtime (tokio) — collect() blocks
- Crash recovery (pid+start_time based)
- Python async API
- Arrow RecordBatch support
- Windows named file mapping

---

## Performance Data

### Channel throughput (in-process, 4096B messages, 20000 count)

| Backend | Throughput | Notes |
|---|---|---|
| dsline ShmChannel | competitive with mp.Queue | In-process prototype, Mutex serialization |
| multiprocessing.Queue | baseline | pickle overhead |
| multiprocessing.Pipe | similar | lightweight, no pickle |

### Pipeline processing (50000 rows, simple filter+map)

| Backend | Throughput | Notes |
|---|---|---|
| Pure Python | 6.6M items/s | CPython native, zero overhead |
| pandas | 26.2M items/s | Columnar, vectorized |
| pyarrow | 2.0M items/s | Columnar, compute kernels |
| dsline expr-lite | 0.3M items/s | PyO3 per-item boundary dominates |

**Key insight**: dsline's current PyO3 path pays ~3μs per item for Python↔Rust conversion. The real performance advantage requires:
1. Data already resident in shared memory (no serialization)
2. Zero-copy alloc/publish (future)
3. Rust-native processing without Python round-trips

These numbers are expected for the 0.0.1 in-process prototype and are published honestly per the project's transparency principle.

---

## Design Decisions

1. **Single-threaded SPSC first** — lock-free MPMC is hard to verify; sharded SPSC/MPSC is the 0.2.0 approach
2. **expr-lite, not DataFusion** — avoids heavy dependency, sufficient for filter/map use cases
3. **Python UDF naming convention** — `_py` suffix makes the slow-path explicit
4. **Honest zero-copy claims** — three tiers documented: true zero-copy (0), single copy (1), serialization (2+)
5. **File-backed before OS shm** — validates the state machine protocol before committing to platform-specific mmap

---

## Key Risks

1. **alloc/publish safety** — use-after-free/data corruption risk; must pass Miri/loom/stress before exposure
2. **PyO3 boundary overhead** — per-item conversion dominates; batch processing mitigates but doesn't eliminate
3. **Windows complexity** — no `/proc`, named file mapping, ACL; deferred to 0.2.0
4. **Performance vs pandas** — dsline is a transport layer, not a compute engine; the value is in communication, not in-process math

---

## Next Milestone: 0.1.0

Priority order:

1. **OS shared memory backend** (Linux POSIX shm) — unblocks real multi-process performance testing
2. **Arena-backed variable-length allocator** — current support uses multi-slot chunking
3. **MPSC** — multi-producer CAS on head
4. **Pipeline runtime** — tokio background thread, run/start/stop
5. **Benchmark: dsline vs mp.Queue in multi-process** — the defining performance test
6. **PyPI release** — `pip install dsline`

---

## Conclusion

The 0.0.1 prototype successfully validates:
- The architecture (6 crates, clean layering)
- The frame protocol and slot state machine
- Cross-process communication via persistent storage
- The expr-lite expression engine
- The composable Pipeline API
- Python integration (ShmChannel, FileChannel, Pipeline, CLI)

Performance data is honest — dsline's current in-process path is slower than pure Python for simple operations. The project's value proposition depends on OS shared memory and zero-copy achieving meaningful throughput advantages in multi-process scenarios, which is the 0.1.0 target.
