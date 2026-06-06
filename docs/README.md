# dsline Documentation

Documentation will be filled as implementation milestones land.

Initial required topics:

- Quickstart
- Zero-copy semantics
- Channel API
- Backpressure
- Benchmark reports
- Platform support

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
