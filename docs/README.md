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
region file. `FileChannel` memory-maps an explicit file path. Both share the
same fixed-slot SPSC core and the same 13-byte per-slot header layout. Ring
position comes from two shared monotonic cursor words in the region header
(16 bytes before the slot area): the producer publishes the head count with
each message batch and the consumer advances the tail count on each release,
so every operation refreshes in O(1); the slot-header scan runs once per
open (recovery) to rebuild the sequence state. The cursor protocol is
verified by `verify/check_invariants.py` (Z3, CP-SAT, SCIP, replay).

## Backpressure

`ShmChannel` and `FileChannel` support four fixed-slot SPSC backpressure strategies:

- `Backpressure.Block`: wait for space until the configured timeout, then raise `BufferFullError`.
- `Backpressure.Raise`: raise `BufferFullError` immediately when the channel is full.
- `Backpressure.DropNewest`: keep queued messages and discard the incoming message; `send()` returns successfully.
- `Backpressure.DropOldest`: release the oldest queued message, then enqueue the incoming message; `send()` returns successfully.

Drop strategies are lossy by design. Use `recv_with_seq()` or `stats()` sequence fields when consumers need to detect gaps.

## Message Size

`slot_size` is still the fixed storage unit, but bytes messages can exceed one
slot when enough slots are available. Larger payloads are chunked into multiple
frames and reassembled by `recv()` / `recv_with_seq()`. The maximum payload is
configuration-dependent because chunk metadata consumes space in each slot.

This is the first variable-length message path. It does not expose the future
arena allocator or zero-copy `alloc/publish` API.

`stats()` exposes the effective limits:

- `slot_size`: configured payload size for a single-slot message.
- `chunk_metadata_size`: bytes reserved per chunk for chunk metadata.
- `chunk_payload_size`: usable payload bytes per chunked slot.
- `max_message_size`: largest message accepted by this channel configuration.
