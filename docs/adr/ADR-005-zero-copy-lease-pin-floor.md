# ADR-005: Zero-copy lease/seal/pin protocol for alloc/publish

Date: 2026-10-02
Status: Accepted (implemented for the single-producer, single-slot milestone)
Related: [ADR-001](ADR-001-shared-memory-backend.md) (shared-memory backend),
[ADR-002](ADR-002-variable-length-messages.md) (variable-length messages),
roadmap §4.2 (alloc/publish path), §4.3 (safety gate), §4.4 (slot reuse
conditions), §6 (crash recovery).

## Context

The roadmap defines three data paths. The copy path (`send`/`recv`) is
implemented; the zero-copy path (§4.2) is the project's defining value
proposition and the last open 0.1.0 feature:

```python
buf = ch.alloc(size)      # payload is born in shared memory
buf.write(b"...", 0)      # producer writes into the region directly
seq = ch.publish(buf)     # seal + commit
view = ch.recv_view()     # consumer reads the same physical bytes
```

§4.3 gates the API behind a safety model: the payload must be sealed after
publish, a slot must not be reused while any view is alive, crash leftovers
must not wedge the ring, and the whole state machine must be verified. The
gate names Miri/loom; the repository's established verification style is
solver-based (Z3/CP-SAT/SCIP) plus replay against the real channel — the
cross-process atomics on mmap-backed storage are outside what Miri/loom can
model anyway (FFI and OS memory semantics).

## Decision

### Lease (producer side)

`alloc(size)` reserves one slot through the existing send guard, stamps the
WRITING header with the writer identity, and returns a lease object
(`ShmWriteLease`) holding the payload's absolute offset. Writes go through
the storage backend straight into the region — there is no intermediate
frame buffer. `publish` computes the checksum **in place** (a new
`SlotRegionIo::with_slice` window reads the mapping under the backend's
lock instead of copying out), writes the 50-byte frame header over the
payload, flips the slot to COMMITTED, and publishes the head cursor exactly
like `send` does. The lease is sealed: further writes are rejected.

Publish consumes the SPSC head only at commit, so an unpublished lease is
invisible to consumers and an aborted (or crashed) lease behaves like any
crashed writer's leftover: SPSC `abort()` rewinds the local head and frees
the slot; a hard crash leaves WRITING for the existing identity-based
recovery.

### Pin floor (consumer side)

`recv_view()` validates the frame in place (header parse + checksum over
the mapped payload), transitions COMMITTED → PINNED with the **consumer's**
identity (the mirror of the writer stamp), advances the tail cursor
(logical consume), and returns a view holding a `RegionAccess` handle — an
object-safe facade over the region (`Arc<dyn RegionAccess>`, so the Python
objects can be non-generic and outlive individual channel calls).

The safety problem: the slot is logically consumed but must not be
physically reused while the view is alive. A new **pin floor** word in the
region header records the smallest pinned cursor value (sentinel
`PIN_FLOOR_NONE = u64::MAX` when nothing is pinned — never 0, which is a
legal pin cursor). Every producer's send guard computes its occupancy as

```text
guard_len = (head_cursor - floor_eff) + recovery_bias
floor_eff = min(pin_floor, tail_cursor)   # sentinel maps to the tail
```

Guard theorem: a claim of `needed` slots at `head` touches a pinned
physical slot iff `head + needed > floor + capacity` (claims are contiguous
from the head, and within one ring window each residue appears exactly
once, so the congruent pinned cursor is exactly `c - capacity`). Proven by
Z3 over bitvectors (Z6.1 sound, Z6.2 tight, Z6.3 sentinel degeneration)
and bounded-model-checked by CP-SAT at the wrap boundary with and without
the guard (C13/C13b).

Ordering and races:

- Pin: write the PINNED header first (release fence, identity magic last),
  then CAS-lower the floor (`floor_min`). A producer reading a stale-high
  floor in the window at worst over-commits into the Free-state check → a
  spurious `BufferFull` — never a silent overwrite.
- Release: clear the payload, write FREE, then rescan forward from the
  released cursor for the next pinned one (or the tail sentinel) and raise
  the floor with a **single CAS from the entry snapshot** (`floor_cas`) — a
  concurrent pinner that lowered the floor during the scan makes the CAS
  fail and keeps its better-informed value.
- Open recovery rebuilds the floor from the ring scan (the oldest pinned
  cursor in `[tail - capacity, tail)`) with the same single-CAS raise, so
  phantom floors self-heal.

### Crash recovery (consumer side)

Dead consumers are the mirror of dead writers: the PINNED header carries
`pid + start_time` behind the identity magic, and `reclaim_dead_pinned`
applies the identical decision table (double-read stability, magic, platform
liveness probe; `Alive`/`Unknown` pins are never touched) at open and in
`reclaim_stale()`. The recovery bias excludes pins in the normal cursor
regime (the floor counts them), so the guard never double-books a slot; in
the orphan regime pins stay in the bias, which is merely conservative.

### Rejected alternatives

- **Cancelled slot state for MPSC abort.** An MPSC reservation is
  cursor-counted at claim time; unwinding it safely needs a sixth slot
  state and gate changes so the tail can skip cancelled extents. Deferred:
  zero-copy `alloc` is SPSC-only for now (`recv_view` works in both
  modes), and MPSC producers keep the copy path.
- **Region-wide per-slot refcounts.** The §4.4 reuse conditions name
  refcounts for multiple views per slot. The ring currently hands each
  slot to exactly one view; a boolean PINNED state plus the floor captures
  the whole protocol. Refcounts return with the multi-view/numpy-lease
  work.
- **Holding backend locks in views.** Handing out `&mut [u8]` from behind
  the backends' `Mutex` cannot be done safely without holding the guard
  across Python calls (unsendable) or raw pointers. Views read through
  short lock windows (`with_slice`) and per-call copies instead; true
  buffer-protocol export waits for the pyo3 0.23 `__getbuffer__` API.

## Consequences

- Region header grows to 32 bytes (format version 3); older regions are
  rejected at open.
- `alloc`/`recv_view` reject `DropOldest` channels (a forced drop may free
  a pinned slot) and zero-copy `alloc` rejects multi-producer mode.
- Chunked multi-slot messages stay on the copy path (their bytes are not
  contiguous in the region).
- Gate status (§4.3), honestly: slot/lease/seal state machine — unit tests
  (12 new) + Z6/C13/S10 solver checks + R8 replay; concurrent schedules —
  CP-SAT bounded checking and the existing cross-process stress suites;
  GC/view-holding tests — Python suite (`test_zero_copy.py`); Miri/loom —
  not applicable to the FFI/mmap atomics, replaced by the solver/replay
  approach documented here.
- Future work: MPSC zero-copy alloc (Cancelled state), buffer-protocol
  views and numpy interop (pyo3 0.23 migration), multi-view refcounts,
  arena allocator.
