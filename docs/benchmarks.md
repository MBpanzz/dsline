# Benchmark report — 2026-09-04

Machine: Windows 11 (10.0.26200), 16 cores, CPython 3.13.12.
Harness: `benches/cross_process.py` — results in `docs/benchmark_results.json`.
Method: warmup messages precede every timed region; all backends transfer raw
byte payloads of identical sizes; `mp.Queue` additionally pays pickle
serialization; dsline and the stdlib ring consumers spin on an empty ring
while pipe/zmq consumers block.

This report supersedes the 2026-08-02 one: the channel now uses shared
head/tail cursors (the O(capacity) refresh scan is gone — see
`PROGRESS.md`), and the Windows flush cost has been measured directly.

## Cross-process throughput (2026-09-04, latest)

| backend | transport | 64 B | 1 KiB | 4 KiB | 64 KiB |
|---|---|---|---|---|---|
| **dsline ShmChannel** | shared memory (file-backed mmap) | 10.4k msg/s | 7.3k | 8.1k | 2.4k (0.14 GiB/s) |
| **dsline FileChannel** | mmap file | 10.7k | 9.3k | 7.9k | 2.4k (0.15 GiB/s) |
| mp.Queue | pipe + pickle | 162k | 87k | 78k | 24k (1.5 GiB/s) |
| mp.Pipe | pipe (raw) | 290k | 110k | 98k | 33k (2.0 GiB/s) |
| stdlib shared_memory ring | paging-file shm (spin) | 1.9M | 1.5M | 1.1M | 334k (20.4 GiB/s) |
| ZeroMQ | tcp loopback | 692k | 55k | 14k | 1.0k |
| pynng | tcp loopback | 23k | 18k | 10k | 1.0k |

(The 1 KiB ShmChannel row shows run-to-run variance; FileChannel at the same
size measured 7.6–9.3k across runs.)

## Cross-process latency (one-way, RTT/2, p50)

| backend | 64 B | 4 KiB |
|---|---|---|
| **dsline ShmChannel** | 130.0 µs | 162.7 µs |
| mp.Pipe | 10.3 µs | 19.4 µs |
| stdlib shared_memory ring | 1.05 µs | 1.95 µs |
| ZeroMQ | 88.3 µs | 129.8 µs |
| pynng | 45.6 µs | 83.1 µs |

## What changed since the 2026-08-02 report

- **The O(capacity) refresh scan is gone.** Every operation used to re-scan
  all `capacity` slot headers to re-derive the ring state (10–30 µs at
  capacity 64, ~1 ms in-process at the default capacity 1024). Ring position
  now comes from two shared monotonic cursor words in the region header
  (`refresh_cursors` = two atomic loads); the full scan runs once per open
  (recovery) to rebuild the sequence state. The cursor protocol is verified
  by the solver harness (`verify/check_invariants.py`, Z3/CP-SAT/SCIP +
  replay) — see `verify/README.md`.
- **Flushes are batched per message**: one flush covers the cursor words and
  the message's slots when they sit in a small region prefix, with
  page-local flushes as the fallback for large offsets. (FlushViewOfFile
  scans the range it is given, so the fallback avoids flushing pages far
  from the touched ones.)
- **The receive path polls briefly for COMMITTED** instead of error-returning
  into a Python exception loop, absorbing the producer's batch-flush window;
  a slot that stays WRITING past the grace period (a crash leftover) is
  still surfaced as an empty ring.

## Flush cost — measured directly

Two measurements isolate the remaining cost:

- A raw Win32 probe: `FlushViewOfFile` costs **~45–52 µs per call** on this
  machine, essentially independent of the range size (16 bytes ≈ 1 page ≈
  256 KiB within noise).
- A Rust micro-benchmark over the real channel (send+recv round trip,
  capacity 1024):
  - flush-enabled: **222 µs / round trip**,
  - flush disabled (no-op `flush_range` wrapper): **4.3 µs / round trip**.

So the Windows file-backed flush is now the *entire* remaining per-message
cost; the O(1) cursor refresh, the payload copies, and the PyO3 boundary
are negligible next to it. Baseline for comparison: the same machine's
`multiprocessing.shared_memory` ring (paging-file-backed, no flush) reaches
1.9M msg/s at 64 B — the flush is exactly the difference.

## Interpretation (honest)

- dsline at 64 B is ~10.4k msg/s (~130 µs one-way latency), around
  40–75 % of the 2026-08-02 numbers: the design costs changed (the scan
  disappeared, one extra cursor flush per message appeared), and the flush
  dominates both. The comparison to the previous report is secondary —
  what matters is that correctness is now verified end-to-end by the solver
  harness and the remaining cost is one measurable, addressable syscall.
- dsline beats ZeroMQ/pynng over tcp loopback for 4 KiB+ messages and
  pynng everywhere, but loses to mp.Pipe and the stdlib ring by 1–2 orders
  of magnitude on small messages, for the same reason.
- **What would close the gap** (0.1.0 roadmap): a paging-file-backed section
  on Windows (`CreateFileMapping` with a name, like
  `multiprocessing.shared_memory` — coherent across processes without any
  flush) or Linux-first where `MAP_SHARED` needs no flush. The cursor
  protocol is ready for either: position comes from the shared cursors, and
  the slot state machine still guards every write/read.
- The in-process comparison from the 2026-08-02 report is unchanged in
  spirit: dsline is a transport layer, not an in-process queue; deque and
  queue.Queue remain the right tools in-process.

## Historical note

The first mmap implementation called memmap2 `flush_range` per message,
which on Windows performs `FlushFileBuffers` — a full disk sync (~1.2
ms/message, 40× slower). That was replaced by release/acquire fences plus
`flush_async_range` (FlushViewOfFile only), restoring correctness of
cross-process visibility at ~45–52 µs per call. The current numbers above
reflect the fixed implementation.