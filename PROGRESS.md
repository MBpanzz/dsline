# Project Progress Report — 2026-05-16

## Overview

`dsline` is a lightweight local dataflow framework for Python, built with a Rust performance core and PyO3 bindings. Target: high-throughput, low-latency multi-process communication via shared memory.

- **Current version**: 0.0.1 (SPSC prototype validation)
- **License**: MIT
- **MSRV**: Rust 1.75
- **Python**: ≥3.9

---

## Update — 2026-10-02: zero-copy alloc/publish (roadmap §4.2/§4.3/§4.4)

The last open 0.1.0 feature lands: the zero-copy path with the lease/seal/pin
safety model (ADR-005).

- **alloc/publish** (single-producer, single-slot): `alloc(size)` reserves a
  slot and returns a writable lease into the shared region — the payload is
  born in shared memory. `publish` verifies the checksum **in place**
  (new `SlotRegionIo::with_slice` lock-window read, no copy-out), writes the
  frame header over the payload, seals the lease, and commits. Python:
  `ShmWriteBuffer` with `write()`/slice assignment, `ShmReadView` with
  partial `read()`/`read_all()` — the transport never moves the bytes;
  Python-side reads are explicit copies of the requested range until the
  pyo3 0.23 buffer-protocol migration.
- **Pin floor**: a `pin_floor` word in the region header (now 32 bytes,
  format 3) records the smallest pinned cursor; every producer's send guard
  derives occupancy from it, so the ring cannot wrap onto a slot a live
  view reads. Guard theorem proven by Z3 (Z6.1 sound / Z6.2 tight / Z6.3
  sentinel degeneration), bounded-model-checked by CP-SAT at the wrap
  boundary with and without the guard (C13/C13b). CAS-structured updates:
  a lost race at worst costs a spurious `BufferFull`, never a silent
  overwrite.
- **Crash recovery, consumer side**: dead consumers' pins are reclaimed by
  identity (same decision table as dead writers) at open and in
  `reclaim_stale()`; the open path rebuilds the floor from the ring; the
  recovery bias no longer double-books pins in the normal cursor regime.
- **Boundaries**: `alloc` is SPSC-only (MPSC reservation would head-of-line
  block), zero-copy ops reject `DropOldest`, chunked messages stay on the
  copy path, and Python reads copy the requested range (buffer-protocol
  views are future work with pyo3 0.23).
- **Tests**: 12 new Rust unit tests (68 → 80), 18 new Python tests
  (114 → 132, including a cross-process test where a spinning producer must
  not overwrite the pinned message), 7 new solver checks (77 → 84: Z6.1–3,
  C13/C13b, S10, R8).

---

## Update — 2026-09-06: sharded multi-consumer channel (分片 MPMC)

The channel layer gains the roadmap §5.5 multi-consumer design — not a
lock-free MPMC ring, but the prescribed composition: **N 个 SPSC/MPSC
channel + 路由层**, exposed as `dsline.ShardedChannel`.

- **Three routing strategies.** `round_robin` routes with a per-producer
  local counter (per-producer fairness; the shards together see the whole
  stream). `hash` routes `FNV-1a-32(key or payload) % shards` — stateless,
  so every process routes the same key to the same shard, and per-key
  ordering survives because a shard is FIFO. `broadcast` sends every
  message to every shard; a consumer bound to one shard (one consumer
  group) sees the full stream. `send()` returns the number of shards used
  (1, or N for broadcast).
- **Consumer surface.** `shard(i)` binds one shard (an ordinary
  `ShmChannel`), `shards()` lists them, `recv_any()` polls all shards in a
  round-robin rotation and returns `(shard_index, payload)` or `None`
  (meaningful for round-robin/hash; a broadcast message exists on every
  shard). `shard_for()` exposes the routing function; `stats()` is
  per-shard. All processes must agree on `name`/`shards`/`capacity`/
  `slot_size`/`multi_producer` (the shard regions enforce capacity and
  mode; the shard count is a convention).
- **Solver verification: 62 → 77 checks, all passing.** Z3 proves the
  round-robin bijection (Z5.1: N consecutive sends cover every shard
  exactly once), the hash kernel's injectivity and fold composition
  (Z5.2a/b/c), and the shard range (Z5.3). CP-SAT bounded-checks the
  composition's accounting invariant (C11: a shard's delivered count
  cannot differ from its assigned count while the total stays loss-less —
  no lost or duplicated message at the routing layer) and the broadcast
  amplification (C12: every shard receives exactly the full stream). SCIP
  proves the round-robin balance optimality as MIP optima under the cycle
  constraint (S9: min-max load == ceil(P/N), max-min == floor(P/N)). The
  new R7 replay section runs the real cross-process fan-out: 2 producer
  processes × 3 consumer processes reassemble the sent multiset exactly
  once (R7a), broadcast consumer groups each see the full in-order stream
  (R7b), and hash routing agrees across handles (R7c).
- Tests: 10 new Python tests (104 → 114), including the cross-process
  exactly-once and broadcast-consumer-group runs through real named
  channels. The cross-process workers open their shard handles with
  `multi_producer=True` — every opener of an MPSC region must agree on the
  mode (this surfaced as a real mode-mismatch error during development,
  which is the protocol working as designed).

Still not implemented: multi-consumer on a single ring (the sharded fan-out
is the roadmap's answer), `alloc/publish` zero-copy (safety gate pending),
arena allocator, metrics/`BusTransport`.
---

## Update — 2026-09-06: crash recovery via writer identity

The channel layer gains the roadmap §6 crash recovery (§0.1.0 Channel
checklist): a producer killed mid-send no longer permanently costs the ring
its in-flight slots.

- **Writer identity in every WRITING slot header** (slot header 13 → 48
  bytes): `pid + start_time` (the roadmap's process identity), a per-handle
  epoch, a unix-ns timestamp, the message's reserved slot count, and an
  identity magic published last behind a release fence — a reader that
  observes the magic never sees a torn identity (Z4.2). The region header
  gains a format word (20 → 24 bytes, format = 2); older regions are
  rejected at open instead of misread (delete stale region files when
  upgrading).
- **Liveness probe** (`crates/dsline-shm/src/process.rs`): Windows
  `OpenProcess` + `GetProcessTimes` (creation FILETIME), Linux
  `/proc/<pid>/stat` field 22 (parsed after the last `)` — `comm` may
  contain spaces/parens), other platforms answer `Unknown`. A slot is
  reclaimable only when the recorded identity classifies `Gone` (PID
  missing, or PID alive with a different start time — PID reuse);
  `Alive` and `Unknown` never reclaim, and headers without the identity
  magic are never auto-reclaimed (the conservative roadmap default).
- **Reclaim points.** SPSC: at open (scan) and at the recv gate (after the
  COMMITTED grace poll expires) — beyond-cursor leftovers retire their
  `recovery_bias` credit and the freed slot is reused by the recovered
  producer; the cursors never move. MPSC: at the recv gate — the dead
  reservation is cursor-counted (head is reserved before the write), so the
  reclaim frees exactly the recorded extent (after verifying every crossed
  slot is the dead writer's WRITING or FREE) and CAS-advances the shared
  tail past it; the live message behind a dead producer becomes deliverable
  instead of wedging the ring. `reclaim_stale()` is the explicit owner
  cleanup; `stats()["reclaimed_slots"]` reports the total.
- **Sequence resync.** A crash legitimately breaks sequence contiguity: the
  handle that reclaimed (or opened with nothing readable) adopts the first
  observed sequence on the next read, then enforces the strict chain again
  (Z4.3).
- **Latent bug fixed (orphan cursor regime).** A consumer that read a
  COMMITTED slot whose head count was not yet published (producer mid-batch,
  or crashed between slot commits and the head store) advanced tail past
  head and the next refresh rejected the ring as inconsistent — reachable
  deterministically in the crash scenario. `refresh_cursors` now recognizes
  tail-ahead-by-up-to-capacity (orphan regime, depth from the bias);
  consuming an orphan retires its bias credit instead of corrupting the
  cursors (CP-SAT C10 reproduces the pre-fix inversion and proves the
  regime rule covers every protocol-reachable state).
- **Solver verification: 43 → 62 checks, all passing.** Z3: the reclaim
  decision table is sound (Z4.1/1b/1c — only a provably dead identity is
  reclaimed; identityless/torn headers never), the identity magic +
  double-read never fabricate an identity (Z4.2), the resync adopts the
  post-crash sequence exactly once (Z4.3), and the bias accounting stays
  collision-free after reclaims (Z4.4). CP-SAT: a dead reservation wedges
  the ring pre-fix and the reclaim restores progress post-fix (C8), a
  reclaim never crosses a live message — reservation exclusivity (C9), the
  orphan cursor regimes (C10). SCIP: the region-size formula with the
  identity header (S7, cross-solver with CP-SAT) and the minimal tail
  advance == recorded extent (S8). Replay R6 exercises open-time reclaim,
  gate reclaim + resync, the live-writer default, and `reclaim_stale`
  through the real implementation.
- Tests: 9 new Rust unit tests in `dsline-shm` (59 → 68: dead/live/
  identityless zombies at open, gate reclaim SPSC + MPSC extent, the
  extent-crossing-COMMITTED refusal, `reclaim_stale`, format-word
  rejection, process-identity probes) and 5 new Python test cases
  (99 → 104: a spawned worker leaves a zombie and exits; the parent
  verifies reclaim at open and at the gate across real named channels).

Still not implemented: multi-consumer, `alloc/publish` zero-copy (safety
gate pending), arena allocator, metrics/`BusTransport`, and owner-side
cleanup of a panicked-but-alive writer's own in-flight slots (a panicked
writer's leftovers are reclaimed only after the process exits, since the
identity still probes alive).
---

## Update — 2026-09-06: declarative Pipeline API and region-management CLI

Two 0.1 工程 items land, both closing documented roadmap surfaces:

- **The roadmap's chaining API now works as written** (§11.2):
  `dsline.Pipeline().from_transport("shm://sensor").pipe(
  ops.filter_expr(...), ops.map_expr(...), ops.batch(...)).sink("shm://out")
  .run()`. A pure-Python layer (`python/dsline/ops.py` deferred operator
  specs + a `Pipeline` wrapper with `pipe`/`sink`/`run`) composes the
  existing Rust pull engine: `run()` drains the transport source (or an
  explicit iterable), applies the operators, and sends each result into the
  sink channel as bytes. All imperative methods (`filter_expr`, `collect`,
  `send_to`, `receive_from`, ...) delegate unchanged, so the existing bench
  and tests keep working.
- **`dsline shm` CLI** (§11.5): `dir`, `list`, `inspect <name>`,
  `cleanup --older-than 1h`, `remove <name>`, all with `--json`. The
  name→file mapping mirrors `dsline-shm` exactly (FNV-1a checksum32), with
  the runtime directory from `LOCALAPPDATA` / `XDG_RUNTIME_DIR`. On
  Windows named channels leave no files (paging-file sections), so the
  commands honestly report empty state there.
- Tests: 99 Python tests (14 new: declarative chaining, end-to-end
  `from_transport → pipe → sink → run` through real named channels, and
  CLI region management against a redirected runtime dir).

Still not implemented: multi-consumer, `alloc/publish` zero-copy (safety
gate pending), crash recovery, arena allocator, metrics/`BusTransport`.

---

## Update — 2026-09-06: MPSC — CAS reservation protocol

The channel layer gains the 0.1.0 milestone's multi-producer capability,
opt-in so the verified SPSC path (head published only after commit, crash-free
consumer never polls) is untouched:

- **`multi_producer=True`** on `ShmChannel` / `FileChannel` lets several
  processes (or threads with one handle each) send concurrently. Producers
  claim disjoint slot ranges with a hardware CAS on the shared head cursor —
  the reservation *is* the linearization point, and the send guard runs on
  the reservation attempt, so concurrent producers can never over-commit the
  ring even when both observe free space at once (CP-SAT/SMT-backed: W1/W2
  prove the guard keeps every reserved count free and winning CAS
  reservations disjoint). The whole message is written `WRITING` →
  `COMMITTED` as in SPSC, and the consumer's existing per-slot COMMITTED
  gate absorbs the in-flight window (the SPSC gate theorem still holds for
  the default mode).
- **Per-message sequences are not enforced for contiguity in MPSC mode**:
  the head is reserved before the write and messages commit in reservation
  order, so a single counter's order is not the ring order; ordering and
  integrity come from ring position + the commit gate + checksum. Sequences
  remain per-handle informational counters.
- **Mode is baked into the region magic** (`DSL1` SPSC / `DSL2` MPSC,
  published last by the initializer): openers that disagree on
  `multi_producer` fail fast at open instead of misinterpreting the ring.
- **`DropOldest` under concurrency**: the tail advance is a CAS over the
  dropped message's slot count (concurrent droppers never drop the same
  message twice), and the winning dropper force-releases the slot (the
  consumer may have pinned it between peek and CAS). The write side waits
  out the release window of a concurrent drop, and the drop side retries
  through torn reads; the only refusal without progress is the COMMITTED
  gate rejecting a crashed writer's leftover, exactly like SPSC.
- Tests: 53 Rust unit tests in `dsline-shm` (5 new MPSC tests, including a
  two-thread producer stress with chunked interleaving and a concurrent
  DropOldest double-drop check), 85 Python tests (two-process producer
  round trip, FileChannel round trip, mode-mismatch rejection), 43/43
  solver checks (W1/W2 added to Z3, R5 two-producer replay added).

Still not implemented: multi-consumer, `alloc/publish` zero-copy (safety
gate pending), crash recovery, arena allocator, cleanup CLI.

---

## Update — 2026-09-06: Windows named channels move to paging-file sections

The flush cost measured in the September 4 update (FlushViewOfFile, ~45 µs
per call) is gone from the Windows send/recv path:

- **`ShmChannel` on Windows is now backed by a named paging-file section**
  (`SectionShmSlotStorage`): `CreateFileMappingW` with `INVALID_HANDLE_VALUE`
  backs the region with the system paging file, the same mechanism
  `multiprocessing.shared_memory` uses minus the file. The memory manager
  keeps views coherent without any flush, so a named channel round trip
  drops the measured per-message flush cost entirely. The section name is
  derived from the resolved region path (`Local\dsline-<digest>.bin`), no
  backing file is created on Windows, and the section is destroyed when the
  last handle closes — no stale-region cleanup. The Python `ShmChannel`
  selects the backend per platform (`SectionShmSlotStorage` on Windows,
  file-backed `MmapSlotStorage` elsewhere); `FileChannel` keeps the file
  mapping.
- **Region header grows to 20 bytes** — `[head: u64 @ 0] [tail: u64 @ 8]
  [magic: u32 @ 16]` — and gains an initialization-magic protocol: the
  creator zero-fills every slot header and publishes `REGION_MAGIC` last
  (release store); every other opener polls for the magic before recovering
  the ring (acquire load). Required because a fresh section is zero-filled,
  which is both the empty-ring state and the mid-init state — without the
  magic an opener could mistake a half-initialized region for an empty one.
  `create_or_open` is now atomic on named sections (the OS guarantees
  exactly one creator), so exactly one process runs the init pass.
- Tests: 48 Rust unit tests in `dsline-shm` (6 new section-backend tests:
  byte-range I/O, create-or-open single-creator arbitration, open-without-
  create failure, message exchange through one section, magic wait on a
  creator that died mid-init, and section lifetime), 81 Python tests, 40/40
  solver checks (verify script constants updated for the 20-byte header).

Still not implemented: `alloc/publish` zero-copy (safety gate pending), MPSC,
crash recovery, arena allocator, cleanup CLI. (Anonymous OS shared memory is
now effectively delivered on Windows via the paging-file section; it remains
future work on Unix.)

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

Done since this list was written: MPSC (CAS reservation), Windows
paging-file sections, sharded MPMC, crash recovery, zero-copy alloc/publish.

Remaining for 0.1.0:

1. **Benchmark: dsline vs mp.Queue in multi-process** — the defining performance test
2. **PyPI release** — `pip install dsline`

Then (0.2.0+):

- **Linux POSIX shm backend** — unblocks real multi-process performance testing there
- **MPSC zero-copy alloc** — needs the Cancelled slot state so the gate can skip unwound reservations
- **Buffer-protocol views / numpy interop** — pyo3 0.23 `__getbuffer__` migration
- **Arena-backed variable-length allocator** — current support uses multi-slot chunking
- **Pipeline runtime** — tokio background thread, run/start/stop

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
