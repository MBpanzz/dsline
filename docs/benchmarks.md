# Benchmark report — Linux (WSL Ubuntu 24.04), 2026-10-20

Machine: WSL2 Ubuntu 24.04.3, 12 cores, CPython 3.12.3, rustc 1.99.1.
Harness: `benches/cross_process.py` (counts are `COUNTS // 10` — the `// 10`
applies in every mode; `--quick` only narrows the sizes).
Method: warmup messages precede every timed region; all backends transfer raw
byte payloads of identical sizes; `mp.Queue` additionally pays pickle
serialization; the dsline consumers poll with `try_recv()` (no exception, no
grace poll), the shm_stdlib consumer spins on a raw buffer, mp/zmq consumers
block. **The extension is a release build** (`scripts/wsl-test.sh bench`
builds it that way; `maturin develop` defaults to debug).
Machine-readable results: `docs/benchmark_results_linux_2026-10-20.json`.

**This is the tier-1 measurement** (roadmap §13.1). Linux `MAP_SHARED` is
coherent across processes without any flush, so the `FlushViewOfFile` cost
that dominated every Windows number in the report below does not exist here.
The code was also only ever *compiled* on Linux for the first time on this
date (see `docs/development.md` §9), so these are the first Linux numbers.

## Cross-process throughput (2026-10-20)

| backend | transport | 64 B | 1 KiB | 4 KiB | 64 KiB |
|---|---|---|---|---|---|
| **dsline ShmChannel** | shared memory (file-backed mmap) | 842.4k msg/s | 618.3k | 567.4k (2.16 GiB/s) | 148.3k (9.05 GiB/s) |
| **dsline FileChannel** | mmap file | 843.9k | 643.6k | 572.2k (2.18 GiB/s) | 151.2k (9.23 GiB/s) |
| mp.Queue | pipe + pickle | 327.7k | 308.4k | 249.6k (0.95 GiB/s) | 20.8k (1.27 GiB/s) |
| mp.Pipe | pipe (raw) | 300.5k | 254.2k | 224.1k (0.86 GiB/s) | 76.0k (4.64 GiB/s) |
| stdlib shared_memory ring | paging-file shm (spin) | 963.0k | 988.2k | 785.2k (3.00 GiB/s) | 392.5k (24.0 GiB/s) |
| ZeroMQ | tcp loopback | 1.91M | 557.2k | 185.1k (0.71 GiB/s) | 31.7k (1.94 GiB/s) |
| pynng | tcp loopback | 24.9k | 24.6k | 25.0k | 18.0k (1.10 GiB/s) |

(Run-to-run variance on this machine is ~3–8%, enough to swap the order of
neighbours; the numbers above and
`docs/benchmark_results_linux_2026-10-20.json` are the same run.)

Caveat on the queueing transports: ZeroMQ and `mp.Queue` buffer internally, so
their timed loop measures enqueue/drain against a queue rather than a
synchronous transfer, which is why ZeroMQ's 64 B figure exceeds the raw
in-process ring's.

## Cross-process latency (one-way, RTT/2, p50/p99/p999 µs)

| backend | 64 B | 4 KiB |
|---|---|---|
| **dsline ShmChannel** | **1.39 / 8.79 / 12.75** | **16.87 / 22.28 / 57.44** |
| mp.Pipe | 17.22 / 52.42 / 121.00 | 22.63 / 60.90 / 147.97 |
| stdlib shared_memory ring | 32.00 / 78.96 / 132.26 | 31.92 / 73.81 / 119.28 |
| ZeroMQ | 63.01 / 154.37 / 216.12 | 64.75 / 151.36 / 213.26 |
| pynng | 82.54 / 138.57 / 161.82 | 84.24 / 142.35 / 210.90 |

## In-process (threads)

| backend | 64 B | 4 KiB |
|---|---|---|
| dsline ShmChannel | 298.9k msg/s | 236.5k msg/s |
| deque + Condition | 26.4M | 25.2M |
| queue.Queue | 763.4k | 1.05M |
| ZeroMQ inproc | 607.7k | 366.3k |
| pynng inproc | 25.7k | 24.5k |

## What produced these numbers

Two corrections since the first Linux run earlier the same day, worth
separating from the transport itself:

1. **The extension was built in debug.** `maturin develop` defaults to debug
   (wheels and `maturin build` are release), and in debug a single 4 KiB
   checksum costs ~16 µs — more than the entire send. Benchmarking now goes
   through `scripts/wsl-test.sh bench`, which builds release first.
2. **The frame checksum was a serial byte-at-a-time FNV-1a** — ~63% of the
   copy path's per-message time at 4 KiB in *release*. It is now CRC-32
   (`crc32fast`, ADR-006), which bumps the frame version to 2 and the region
   format to 4.

Measured `dsline_shm` cross-process, before → after both corrections:

| | 64 B | 1 KiB | 4 KiB | 64 KiB | latency p50 @64 B |
|---|---|---|---|---|---|
| debug build, FNV-1a | 211.7k msg/s | 109.1k | 39.2k | 3.5k | 5.87 µs |
| release, CRC-32 | **842.4k** | **618.3k** | **567.4k** | **148.3k** | **1.39 µs** |

Single-process cost breakdown (release extension, `python`-level):

| 4 KiB | FNV-1a | CRC-32 |
|---|---|---|
| send | 8.29 µs | 3.16 µs |
| receive (in-process handle) | 4.47 µs | 1.59 µs |
| empty `try_recv()` poll | 0.089 µs | 0.089 µs |

The Rust-side probe behind those ratios is kept in `dsline-shm` as an ignored
test: `cargo test --release -p dsline-shm --lib timing_probe -- --ignored
--nocapture`.

## Interpretation (honest)

- **Latency is a clear win.** 1.39 µs one-way p50 at 64 B, 12× better than
  `mp.Pipe` (17.2 µs) and 23× better than the hand-rolled stdlib ring
  (32.0 µs); also ~90× better than dsline's own Windows figure from the
  2026-09-04 report (130 µs). At 4 KiB dsline is again first (16.9 µs vs 22.6
  for `mp.Pipe`).
- **Throughput is now competitive and third-party transports are behind it**
  at every size measured: 567k msg/s at 4 KiB against 250k (`mp.Queue`), 224k
  (`mp.Pipe`) and 185k (ZeroMQ). The remaining gap is to the raw stdlib ring
  (785k at 4 KiB, 393k at 64 KiB) — a fixed-slot ring that does no framing, no
  checksum and no slot state machine, i.e. a memcpy plus a flag byte. That is
  the honest cost of the protocol features (integrity, crash recovery,
  zero-copy pins) and it is now bounded rather than mysterious.
- **In-process remains GIL-bound** (299k msg/s at 64 B vs 26.4M for
  `deque`+`Condition`): each message costs two Python-level calls, so the
  interpreter, not the ring, is the limit. This is the documented position —
  dsline is a transport, not an in-process queue — and the number is now
  measurable instead of hanging (a blocking `send` used to hold the GIL and
  deadlock the consumer thread).

## Open items from this run

- **Zero-copy `alloc`/`publish`/`recv_view` are not benchmarked here.** They
  avoid the copy path entirely for single-slot messages, so they should beat
  the numbers above for large payloads; the harness does not measure them yet.
- Two MPSC bugs were found while chasing a once-observed test hang, both of
  the shape "a bound expressed as a yield count instead of wall-clock time, on
  a path that must not fail": a `DropOldest` send could fail with
  `BufferFullError` when it raced another producer's in-flight write
  (reproduced 2/20 on Windows), and a `Block` send's timeout applied per wait
  attempt so a producer that kept losing the head CAS never returned. Both are
  fixed (wall-clock bounds; the drop distinguishes a live writer from a dead
  one with the identity probe) and the suite went from 2 failures in 20 runs
  to 0 in 25. See `PROGRESS.md` for the mechanism.
- The `shm_stdlib` baseline used to abort runs intermittently (its consumer
  opened the block while the creator was still sizing it); the harness now
  retries that window and unlinks the blocks it creates.
- pynng prints `ConnectionRefused` on its dial race and retries
  asynchronously; its numbers are produced but the log is noisy.

---

# Benchmark report — Windows (paging-file section backend), 2026-10-20

Machine: Windows 11 (10.0.26200), 16 cores, CPython 3.13.12, rustc 1.99.1.
Same harness and release-extension rule as above.
Machine-readable results: `docs/benchmark_results_windows_2026-10-20.json`.

This closes the item left open by the 2026-09-04 report below: the named
channel has used the paging-file section backend since 2026-09-06, and this is
the first measurement with it (plus the release build and CRC-32 checksum).

## Cross-process throughput

| backend | 64 B | 1 KiB | 4 KiB | 64 KiB |
|---|---|---|---|---|
| **dsline ShmChannel** (section) | 991.0k msg/s | 920.3k | 766.7k (2.93 GiB/s) | 49.2k (3.00 GiB/s) |
| **dsline FileChannel** (mmap file) | 14.5k | 14.5k | 13.9k (0.05 GiB/s) | 7.4k (0.45 GiB/s) |
| mp.Queue | 177.6k | 108.0k | 99.4k (0.38 GiB/s) | 28.9k (1.77 GiB/s) |
| mp.Pipe | 341.6k | 132.3k | 115.6k (0.44 GiB/s) | 34.7k (2.12 GiB/s) |
| stdlib shared_memory ring | 1.82M | 1.83M | 1.20M (4.56 GiB/s) | 191.0k (11.7 GiB/s) |
| ZeroMQ | 954.8k | 68.0k | 17.6k | 1.1k |
| pynng | 30.5k | 23.5k | 12.0k | 1.2k |

## Cross-process latency (one-way, RTT/2, p50/p99/p999 µs)

| backend | 64 B | 4 KiB |
|---|---|---|
| **dsline ShmChannel** | 1.40 / 2.20 / 15.94 | 6.20 / 10.35 / 37.29 |
| mp.Pipe | 7.60 / 19.20 / 113.98 | 13.90 / 40.05 / 60.90 |
| stdlib shared_memory ring | 0.95 / 1.60 / 6.20 | 1.55 / 2.75 / 19.59 |
| ZeroMQ | 61.40 / 136.13 / 221.86 | 83.65 / 174.74 / 248.19 |
| pynng | 35.90 / 71.55 / 134.63 | 55.45 / 115.89 / 166.43 |

## In-process (threads)

| backend | 64 B | 4 KiB |
|---|---|---|
| dsline ShmChannel | 543.6k msg/s | 218.8k msg/s |
| queue.Queue | 645.9k | 656.9k |
| ZeroMQ inproc | 604.3k | 84.1k |

## Interpretation

- **The paging-file section did what it promised.** `ShmChannel` at 64 B went
  from 10.4k msg/s and 130 µs one-way latency (2026-09-04 report, file-backed
  mapping with a `FlushViewOfFile` per message) to **991k msg/s and 1.40 µs**.
  The Windows transport is no longer the story; the copy path was, and CRC-32
  fixed that on both platforms.
- **`FileChannel` remains flush-bound** at 14.5k msg/s (~69 µs/message): an
  explicit file mapping still needs `FlushViewOfFile` for cross-process
  visibility on Windows. This is documented behaviour, not a regression — use
  the named channel (`ShmChannel`, section backend) on Windows.
- The raw stdlib ring is still ahead (1.82M at 64 B, and it also has the
  better 64 B latency here, 0.95 µs vs 1.40 µs); it does no framing, no
  checksum and no slot state machine.

---

# Benchmark report — Windows (historical), 2026-09-04

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