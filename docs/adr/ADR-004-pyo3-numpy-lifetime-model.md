# ADR-004: PyO3 and NumPy Lifetime Model

Status: draft

## Decision

The public alloc/publish API remains unavailable until the safety gate in `ROADMAP.md` is complete.

The internal lifetime model is:

- `MmapRegion`: shared region owner, held by `Arc`.
- `SlotLease`: prevents a committed or pinned slot from being reused.
- `BufferLease`: exclusive write permission during alloc.
- `ExportGuard`: Python base object attached to exported views.
- Python or NumPy views must keep `ExportGuard` alive until released.

## Slot Reuse Invariant

A slot can be reused only when it is not writing, has been logically consumed, all Rust leases are dropped, all Python views are released, and no publish or recv operation is in progress.

## Safety Gate

The API requires Rust unit tests, Miri coverage for unsafe/lifetime paths, loom or equivalent concurrency model tests, PyO3 integration tests, and multi-process stress tests involving GC, delayed view release, slicing, and forced cleanup.
