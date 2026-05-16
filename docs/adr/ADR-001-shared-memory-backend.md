# ADR-001: Shared Memory Backend

Status: draft

## Decision

For 0.0.1, target Linux first and implement the fixed-slot SPSC backend on top of a mmap-backed file in a private runtime directory. POSIX shm remains an implementation candidate, but the first backend should optimize debuggability and cleanup while the state machine is still changing.

macOS support is a smoke-test target using the same mmap-backed-file strategy where possible. Windows named file mapping is deferred to the 0.1 technical spike and is not a 0.0.1 blocker.

## Naming and Permissions

User-facing channel names must not be used directly as OS object names. The backend will derive an internal name containing a random token and create files with current-user-only permissions where the platform allows it.

## Cleanup

0.0.1 will prefer owner-process cleanup and explicit close. Stale resource discovery and cleanup CLI are planned after the basic backend exists.

## Consequences

The initial backend is easier to inspect than POSIX shm and avoids committing to cross-platform handle semantics too early. It may not produce final performance numbers until the Linux POSIX shm path is evaluated.
