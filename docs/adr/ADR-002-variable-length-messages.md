# ADR-002: Variable Length Messages

Status: draft

## Decision

0.0.1 will not implement variable-length allocation. It will use fixed-size slots for SPSC bytes messages.

Before 0.1, the project will choose between:

- fixed slots with multi-slot chaining,
- metadata ring plus payload arena,
- small-message inline slots plus large-message arena.

The working preference is small-message inline slots plus a large-message arena because it keeps common messages simple while avoiding pathological waste for large payloads.

## Consequences

The first channel can validate ordering, checksum, timeout, and PyO3 behavior without locking in allocator design. Oversized messages fail explicitly with `MessageTooLarge`.
