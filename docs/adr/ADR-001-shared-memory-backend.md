# ADR-001: Shared Memory Backend

Status: implemented (mmap-backed file with shared cursors, 2026-09-04)

## Decision

For 0.0.1, target Linux first and implement the fixed-slot SPSC backend on top of a mmap-backed file in a private runtime directory. POSIX shm remains an implementation candidate, but the first backend should optimize debuggability and cleanup while the state machine is still changing.

macOS support is a smoke-test target using the same mmap-backed-file strategy where possible. Windows named file mapping is deferred to the 0.1 technical spike and is not a 0.0.1 blocker.

## Implementation status

The mmap-backed-file backend is implemented and is the default for both Python
channel types:

- `MmapSlotStorage` (memmap2) implements the `SlotRegionIo` byte-range storage trait; the 13-byte per-slot header layout is unchanged from the original design.
- `ShmChannel` resolves names to region files in the platform runtime directory (`shm_region_path`); `FileChannel` maps an explicit file path.
- Cross-process producer/consumer round-trip tests pass on Windows (spawn-based `multiprocessing`).
- Ring position comes from two shared monotonic cursor words in the region
  header (16 bytes): the producer publishes the head count after each
  message batch, the consumer advances the tail count on each release, so
  every operation refreshes in O(1) (the per-operation full-ring scan is
  gone). The slot-header scan runs once per open for sequence recovery and
  crashed-writer cleanup. The cursor protocol is verified by
  `verify/check_invariants.py` (Z3 guard/collision theorems, CP-SAT gate
  theorem + crash witness, SCIP bounds, dual-handle replay).
- The remaining Windows cost is the `FlushViewOfFile` needed for
  file-backed cross-process visibility (~45 µs per call, measured); the
  measured path to removing it is a paging-file-backed section
  (`CreateFileMapping`, coherent without flush).

Remaining candidates from this ADR, not yet done:

- POSIX shm / anonymous OS shared memory without a backing file.
- Native Windows named file mapping (the current backend is a file-backed mapping, which behaves like named shared memory for channel purposes).
- Stale-region discovery and the cleanup CLI.

## Naming and Permissions

User-facing channel names must not be used directly as OS object names. The backend will derive an internal name containing a random token and create files with current-user-only permissions where the platform allows it.

## Cleanup

0.0.1 will prefer owner-process cleanup and explicit close. Stale resource discovery and cleanup CLI are planned after the basic backend exists.

## Consequences

The initial backend is easier to inspect than POSIX shm and avoids committing to cross-platform handle semantics too early. It may not produce final performance numbers until the Linux POSIX shm path is evaluated.
