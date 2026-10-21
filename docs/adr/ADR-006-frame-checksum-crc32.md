# ADR-006: Frame checksum — CRC-32 instead of FNV-1a

Date: 2026-10-20
Status: Accepted (implemented)
Related: [ADR-002](ADR-002-variable-length-messages.md) (framing),
[ADR-005](ADR-005-zero-copy-lease-pin-floor.md) (region layout / format
word), roadmap §6.4 (checksum), §12.1 (frame header), §16 (benchmarks).

## Context

The frame header carries a 32-bit payload checksum (roadmap §12.1), used for
corruption detection only — it is explicitly not authentication (§6.4). It is
computed over every payload byte on the send path (`encode_frame`) and again
on every receive path (`validate_bytes_frame`, `dsline_core::spsc`).

The first Linux benchmark run (2026-10-20, `docs/benchmarks.md`) localised the
copy path's per-byte cost and it was almost entirely this checksum:

| 4 KiB payload, release build | send | receive |
|---|---:|---:|
| `checksum32` alone (previous byte-at-a-time FNV-1a) | 4.19 µs | 4.19 µs |
| whole operation | 8.29 µs | 4.47 µs |

FNV-1a is a serial dependency chain (`h = (h ^ byte) * prime`), so it cannot
be vectorised or unrolled: ~1 ns/byte, and it dominated a 4 KiB message on
both sides. Every other component (frame encode, the region write, the slot
state transitions, the Python boundary) is ~0.2–1.5 µs.

## Decision

Replace the byte-at-a-time FNV-1a with **CRC-32 (IEEE)**, implemented by the
`crc32fast` crate (carry-less-multiplication folding where the CPU supports
it, a table otherwise). The function keeps its name and signature
(`dsline_core::checksum::checksum32(&[u8]) -> u32`), so no caller changes.

Because the stored checksum values change, this is a protocol change:

- the frame version is bumped to 2 (`dsline_core::frame::FRAME_VERSION`), so a
  reader rejects frames from another version instead of mis-validating them;
- the on-region format word is bumped to 4 (`dsline_shm::REGION_FORMAT`), so
  regions written by older builds are rejected at open (the documented
  "delete stale region files when upgrading" step).

The Python-side name → region-path mirror in `python/dsline/_shm.py` switches
with it (`zlib.crc32`, the same CRC-32/ISO-HDLC). The **sharded fan-out's
routing hash stays FNV-1a** (`_fnv1a32`): it is a documented, solver-checked
contract (`ShardedChannel` routes by `FNV-1a-32(key) % shards`, verified by
`verify/check_invariants.py` Z5.2), not a checksum, and must not move with it.

## Consequences

- **4 KiB copy path, measured (release):** `checksum32` 4.19 µs → 0.11 µs;
  send 8.29 µs → 3.16 µs; receive 4.47 µs → 1.59 µs. The fixed per-message
  cost is unchanged (64 B: send 1.03 µs, receive 0.93 µs), so small messages
  are unaffected and large ones gain directly.
- **A dependency is added** to `dsline-core` (`crc32fast`, plus its `cfg-if`):
  the crate previously had none and the workspace had exactly one
  (`memmap2`). It is small, stable, widely deployed, and purpose-built for
  corruption detection — preferred here over hand-rolling a fast hash.
- CRC-32 detects the same class of corruption (burst errors up to 32 bits,
  all single-bit flips, all odd-bit-count errors); it is not a cryptographic
  hash and was never used as one.
- Regions and frames from older builds are rejected rather than misread.
  `verify/check_invariants.py` is unaffected (its layout arithmetic and replay
  checks do not depend on checksum values) and still passes 84/84.

## Alternatives considered

- **Keep FNV-1a, make the checksum optional.** Loses corruption detection on
  the default path, which roadmap §6.4 requires.
- **Hand-rolled word-at-a-time FNV.** Not possible: FNV's multiply is in the
  integer ring, not GF(2), so the per-byte chain has no parallel expansion
  with identical output; it would be a new hash either way, with more risk
  than a vetted crate.
- **Checksum only a prefix/sample of the payload.** Weakens detection exactly
  where large payloads need it least but tear most.
