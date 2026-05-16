# Project Progress Report — 2026-05-16

## Overview

`dsline` is a lightweight local dataflow framework for Python, built with a Rust performance core and PyO3 bindings. Target: high-throughput, low-latency multi-process communication via shared memory.

- **Current version**: 0.0.1 (SPSC prototype validation)
- **License**: MIT
- **MSRV**: Rust 1.75
- **Python**: ≥3.9

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
- CLI with info and bench commands

### Not Yet Implemented (0.1.0+)

- OS shared memory (POSIX shm / mmap) — currently file-backed
- Variable-length messages — fixed slot_size per channel
- MPSC / multi-consumer — SPSC only
- alloc/publish zero-copy — requires safety gate (Miri, loom, stress testing)
- Pipeline background runtime (tokio) — collect() blocks
- Crash recovery (pid+start_time based)
- Backpressure strategies DROP_NEWEST/DROP_OLDEST — BLOCK/RAISE only
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
2. **Variable-length messages** — remove fixed slot_size constraint
3. **MPSC** — multi-producer CAS on head
4. **Backpressure strategies** — DROP_NEWEST, DROP_OLDEST
5. **Pipeline runtime** — tokio background thread, run/start/stop
6. **Benchmark: dsline vs mp.Queue in multi-process** — the defining performance test
7. **PyPI release** — `pip install dsline`

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
