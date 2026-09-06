# dsline

`dsline` is a lightweight local dataflow framework for Python data processing and inter-process communication.

Current development follows `ROADMAP.md`. The first implementation target is a fixed-slot SPSC bytes channel. `ShmChannel` and `FileChannel` are backed by a memory-mapped region that is shared across processes, so a producer and a consumer in separate processes exchange messages through the same mapping.

## Status

This repository is at the first test-version stage. It is suitable for API, packaging, protocol, and benchmark harness validation, but it is not yet the final dual-process shared-memory transport with zero-copy.

Current boundaries:

- `ShmChannel` is a named shared-memory channel: every process that uses the same `name` (and the same capacity and slot_size) connects to the same memory-mapped region. On Windows the region lives in a named paging-file section (no backing file, destroyed when the last handle closes); elsewhere region files live in the platform runtime directory (e.g. `%LOCALAPPDATA%\dsline\shm\` on Windows, `$XDG_RUNTIME_DIR`/`$TMPDIR` on Unix, used only when falling back to the file backend). `FileChannel` memory-maps an explicit file path the same way.
- `send/recv` for existing bytes uses a copy path.
- Messages larger than one slot are supported through multi-slot chunking; each chunk still uses fixed-slot storage.
- `alloc/publish` zero-copy APIs are not exposed.
- Multi-consumer delivery is available through the sharded fan-out (`ShardedChannel`); a lock-free multi-consumer ring and anonymous OS shared memory (no backing file) are still future work on Unix. (`multi_producer=True` gives multi-producer / single-consumer.) Crash recovery is implemented: writers record `pid + start_time` in every WRITING slot header, and a leftover slot is reclaimed — at open or at the receive gate — once a platform probe proves the writer identity is gone (`GetProcessTimes` on Windows, `/proc` on Linux; unknown platforms default to no auto-reclaim, and a live writer's slots are never touched). In multi-producer mode the reclaim frees the dead reservation's recorded extent and advances the tail past it, so a live message behind a dead producer is delivered instead of wedging the ring. `reclaim_stale()` exposes the explicit owner cleanup, and `stats()["reclaimed_slots"]` reports what a handle has reclaimed.
- On Windows a `FileChannel` region file cannot be deleted while a channel still holds its mapping (standard memory-mapped file behavior); drop the channel first.

## Install for development

```bash
python -m pip install "maturin>=1.5,<2"
python -m maturin develop --manifest-path crates/dsline-python/Cargo.toml
```

```python
import dsline

with dsline.ShmChannel("demo", capacity=4, slot_size=64) as ch:
    ch.send(b"hello")
    ch.send(memoryview(b"view"))
    assert ch.stats()["queue_depth"] == 2
    assert ch.recv_with_seq() == (0, b"hello")
    assert ch.recv() == b"view"
```

Cross-process usage: two processes that open the same name see the same region.

```python
# process 1 (producer)
import dsline

with dsline.ShmChannel("demo", capacity=4, slot_size=64) as ch:
    ch.send(b"hello")
```

```python
# process 2 (consumer)
import dsline

with dsline.ShmChannel("demo", capacity=4, slot_size=64) as ch:
    assert ch.recv() == b"hello"
```

Backpressure strategies are available on `ShmChannel` and `FileChannel` through
`dsline.Backpressure`: `Block`, `Raise`, `DropNewest`, and `DropOldest`. Drop
strategies keep `send()` as a successful call: `DropNewest` discards the
incoming message when full, while `DropOldest` releases the oldest queued
message before writing the new one.

Multi-producer channels: pass `multi_producer=True` to `ShmChannel` /
`FileChannel` to allow several processes (or threads with one handle each)
to `send()` concurrently into the same ring. Producers claim disjoint
slot ranges with a CAS reservation on the shared head cursor; the consumer
waits per slot through the COMMITTED gate. The mode is fixed when the
region is created (it is encoded in the region magic), so every opener must
agree on `multi_producer` — a mismatch fails at open. Note the tradeoff:
the single-producer mode publishes the head cursor only after a commit, so
a crash-free consumer never polls; the multi-producer mode publishes the
reservation first and the consumer briefly polls in-flight slots.

Messages may exceed `slot_size` when the channel has enough capacity. The
current prototype splits larger payloads into multiple fixed slots and
reassembles them on `recv()`. This is variable-length framing over fixed-slot
storage, not the future shared-memory arena allocator.

Zero-copy wording follows the project rule:

> dsline provides true zero-copy transfer only for eligible shared-memory alloc/publish payloads after the safety gate is complete. Existing `bytes`, `bytearray`, and ndarray values sent through `send/recv` are expected to use a single copy into shared memory.

Implemented prototype pieces:

- `dsline-core`: fixed-slot SPSC bytes ring, checksum, Frame header, chunk flags, and metadata TLV encode/decode.
- `dsline-shm`: slot-region byte-range storage trait with file and memory-mapped (OS shared memory) backends, region state model, cross-process SPSC bytes channel over `FREE`, `WRITING`, `COMMITTED`, `PINNED`, and `CORRUPTED` with shared head/tail cursor words in the region header (O(1) per-operation refresh; the slot-header scan runs once at open for sequence recovery and crashed-writer cleanup), multi-slot chunked messages, an opt-in multi-producer (MPSC) mode that reserves head with a hardware CAS, crash recovery via writer identity (`pid + start_time` probed through platform APIs before reclaim), and name→region resolution for named channels.
- `dsline-python`: PyO3 `ShmChannel` (named, cross-process shared memory) and `FileChannel` (mmap-backed file) bindings over the `dsline-shm` core, Python exception exports, `send()` support for bytes-like inputs (`bytes`, `bytearray`, `memoryview`, and compatible buffer objects), `recv_with_seq()`, `Block`/`Raise`/`DropNewest`/`DropOldest` backpressure, and `stats()` with queue, sequence, and message-size limits.
- Multi-consumer delivery is provided by the sharded fan-out (roadmap §5.5): `dsline.ShardedChannel(name, shards=N, strategy="round_robin"|"hash"|"broadcast")` composes N named channels and routes each message to one shard (per-producer round-robin, or stateless FNV-1a hash of a routing key so per-key ordering survives) or to all of them (broadcast). Consumers bind one shard via `fan.shard(i)` — the roadmap's "N 个 channel + 路由层" MPMC, no lock-free multi-consumer ring involved.
- The Python `Pipeline` supports the declarative roadmap API: `from_transport("shm://...").pipe(ops.filter_expr(...), ops.map_expr(...), ops.batch(...)).sink("shm://...")` with `dsline.ops` deferred operators, plus `run()` that drains the source, applies the operators, and sends each result into the sink channel as bytes.

## CLI

```bash
python -m dsline info
python -m dsline bench shm --message-size 4096 --count 100000 --json
python -m dsline shm list
python -m dsline shm inspect demo
python -m dsline shm cleanup --older-than 1h
python -m dsline shm remove demo
```

After installation, the console script is also available:

```bash
dsline info
dsline bench shm --message-size 4096 --count 100000 --json
dsline shm list
```

`dsline shm` manages named-channel regions in the platform runtime
directory (`dsline-*.bin` files; on Windows named channels use paging-file
sections and leave no files, so the commands mostly report empty state
there). `dir` prints the runtime directory, `list` shows the region files,
`inspect <name>` shows one channel's region file, `cleanup --older-than
<90s|30m|2h|1d>` deletes stale regions, and `remove <name>` deletes one
channel's region file.

## Verification

```bash
cargo fmt --all --check
cargo test
python -m maturin build --manifest-path crates/dsline-python/Cargo.toml --interpreter python
python -m unittest discover -s tests/python
```

## License

MIT License.
