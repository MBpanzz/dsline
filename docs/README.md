# dsline Documentation

Documentation will be filled as implementation milestones land.

Initial required topics:

- Quickstart
- Zero-copy semantics
- Channel API
- Backpressure
- Benchmark reports
- Platform support

## Channels

`ShmChannel` is a named shared-memory channel: all processes using the same
name (and the same capacity and slot_size) connect to the same memory-mapped
region. On Windows the region lives in a named paging-file section; elsewhere
it is a file-backed mapping in the platform runtime directory.
`FileChannel` memory-maps an explicit file path. Both share the same
fixed-slot core and the same 48-byte per-slot header layout (state, sequence,
length, writer identity for crash recovery). Ring position comes from shared
monotonic cursor words in the region header (32 bytes before the slot area:
head, tail, mode magic, format word, zero-copy pin floor): the producer
publishes the head count with each message batch and the consumer advances
the tail count on each release, so every operation refreshes in O(1); the
slot-header scan runs once per open (recovery) to rebuild the sequence state.
The cursor and pin-floor protocols are verified by
`verify/check_invariants.py` (Z3, CP-SAT, SCIP, replay).

## Receiving

- `recv()` / `recv_with_seq()` are the blocking-style reads: they run the
  COMMITTED gate's bounded grace poll and raise `BufferEmptyError` when
  nothing is readable. Use them when the consumer is driven by data (or by an
  outer loop with a sleep) rather than polling in a tight loop.
- `try_recv()` is the non-blocking poll: it returns `bytes`, or `None` when
  nothing is readable right now (an empty ring, or a producer mid-batch). One
  header read per attempt, no grace poll, no exception — this is the call to
  spin on. Crash recovery still applies (a dead writer's leftover at the tail
  is reclaimed on the spot), and it advances the same sequence state as
  `recv()`, so the two can be mixed.

**One handle per thread.** A handle is serialized by an internal lock, so two
threads sharing one handle starve each other: a blocking `send` holds the lock
while it waits for space, and the consumer thread can never drain the ring. In
process, give the producer and the consumer their own handle (two
`ShmChannel` objects with the same name) — that is also the pattern the
multi-producer mode documents ("threads with one handle each"). A blocking
`send` releases the GIL while it waits, so the other thread keeps running.

## Backpressure
`ShmChannel` and `FileChannel` support four fixed-slot SPSC backpressure strategies:

- `Backpressure.Block`: wait for space until the configured timeout, then raise `BufferFullError`.
- `Backpressure.Raise`: raise `BufferFullError` immediately when the channel is full.
- `Backpressure.DropNewest`: keep queued messages and discard the incoming message; `send()` returns successfully.
- `Backpressure.DropOldest`: release the oldest queued message, then enqueue the incoming message; `send()` returns successfully.

Drop strategies are lossy by design. Use `recv_with_seq()` or `stats()` sequence fields when consumers need to detect gaps.

## Zero-Copy (alloc/publish, roadmap §4.2)

For single-slot messages the channel exposes the roadmap's zero-copy path:
the payload is born in the shared region and the consumer reads it in place.

```python
ch = dsline.ShmChannel("demo", capacity=8, slot_size=4096)

buf = ch.alloc(1024)          # writable lease into the shared region
buf.write(b"...", offset=0)   # writes land directly in shared memory
buf[0:4] = b"...."            # slice assignment works too
seq = ch.publish(buf)         # seals the buffer, commits the slot

view = ch.recv_view()         # read-only view over the same bytes
view.read(0, 16)              # copy only the bytes you need
view.release()                # un-pins the slot (or: del / with-block)
```

Safety model (roadmap §4.3, ADR-005):

- **Seal**: after `publish()` the buffer rejects further writes; the frame
  header (with the checksum) is written over the payload at commit time.
- **Pin**: `recv_view()` transitions the slot COMMITTED → PINNED and records
  the consumer's identity. The slot stays reserved until the view is
  released; a dropped view releases best-effort, and a dead consumer's pins
  are reclaimed at open exactly like a dead writer's WRITING slots.
- **Pin floor**: a shared word in the region header records the smallest
  pinned cursor value. Every producer's send guard derives its occupancy
  from it, so the ring can never wrap onto a slot a live view is reading.
  Releasing the floor's view rescans forward for the next pin (or raises
  the floor to the tail sentinel).
- **Verification**: Z3 proves the guard sound and tight (Z6), CP-SAT
  bounded-checks the wrap-boundary schedule with and without the guard
  (C13), SCIP cross-checks the single-slot payload bound (S10), and the
  replay section (R8) exercises the real channel end to end.

Boundaries of this milestone:

- `alloc()` is single-producer only (an unpublished MPSC reservation would
  head-of-line block the ring) and rejects `DropOldest` channels (a forced
  drop may free a pinned slot). `recv_view()` is available in both modes
  but also rejects `DropOldest`.
- Zero-copy messages are single-slot (`size <= slot_size`); chunked
  multi-slot messages stay on the copy path (their bytes are not contiguous
  in the region).
- Python-side reads (`view.read()` / `read_all()`) are explicit copies of
  the requested range — the transport itself never moves the bytes. True
  Python buffer-protocol views (numpy interop) need the pyo3 0.23
  `__getbuffer__` API and are future work.
- The on-region layout version is now 3 (the pin floor word was added).
  Regions written by older builds are rejected at open; delete stale
  region files when upgrading.

## Message Size

`slot_size` is still the fixed storage unit, but bytes messages can exceed one
slot when enough slots are available. Larger payloads are chunked into multiple
frames and reassembled by `recv()` / `recv_with_seq()`. The maximum payload is
configuration-dependent because chunk metadata consumes space in each slot.

This is the first variable-length message path. It does not expose the future
arena allocator.

`stats()` exposes the effective limits:

- `slot_size`: configured payload size for a single-slot message.
- `chunk_metadata_size`: bytes reserved per chunk for chunk metadata.
- `chunk_payload_size`: usable payload bytes per chunked slot.
- `max_message_size`: largest message accepted by this channel configuration.
