# Solver-based verification

`check_invariants.py` verifies the fixed-slot SPSC channel logic
(`crates/dsline-shm`) with three solvers in three paradigms, plus a replay
pass against the real implementation:

| Solver | Paradigm | Role |
|---|---|---|
| Z3 | SMT (bitvectors) | proof-style properties of the wrapping sequence comparison, the send-plan chunk arithmetic, and the shared-cursor ring arithmetic (wrap guard, send-guard no-collision theorems) |
| CP-SAT | constraint programming | bounded model checking: search for invariant-violating schedules, including formal reproduction of the historical `len`-underflow bug, the cursor gate theorem, and the crash-leftover zombie reachability |
| SCIP | MILP | message-size formulas, cursor-depth and batch-flush bounds as optima, cross-solver agreement |
| replay | — | differential check of the same invariants against the compiled channel |

## Running

```bash
pip install z3-solver ortools pyscipopt
python verify/check_invariants.py            # all sections
python verify/check_invariants.py --z3       # one section at a time
python verify/check_invariants.py --skip-replay
```

The script exits non-zero if any expectation fails. The replay section
requires the built `dsline` extension (it is skipped with a notice if the
extension is not importable).

## What is checked

### Z3 (SMT)

- **Z1.1–Z1.5** — `sequence_precedes` (`right.wrapping_sub(left) < 1<<63`):
  irreflexive, asymmetric, and transitive inside a common window of span
  < 2^63 (in practice the in-flight seq window is bounded by `2 * capacity`).
  Transitivity fails outside that window — the checker finds the exact
  boundary counterexample (three seqs straddling 2^63).
- **Z1.6 / Z1.6b** — the `next_seq` recovery fold (start at 0, adopt each
  candidate when it follows the current value): yields the wrapping maximum
  whenever 0 is part of the candidates' window. Boundary finding: if a
  region is opened fresh after ~2^63 messages (candidates straddle 2^63,
  outside 0's window), the fold is order-sensitive and can skip the true
  maximum. Benign in practice — steady-state folds start from the previous
  `next_seq`, which is inside the window.
- **Z2.1–Z2.3** — `send_plan` / `message_size_limits` arithmetic over the
  whole config domain (capacity 1–64, slot_size 1–4096, len 0–2^31):
  acceptance ⇔ `len <= max_message_size`, chunk coverage
  `needed * chunk >= len`, and single-slot path ⇔ `len <= slot_size`.
- **Z3.7 / Z3.7b** — the shared-cursor wrap guard used by `refresh_cursors`
  (`head.wrapping_sub(tail) < 2^63`): the directed distance partitions the
  circle (its inverse cannot also be < 2^63), so the guard accepts exactly
  the head-ahead half and the error path fires only on divergent counts.
  Boundary documentation: forward-drifted counts (unreachable in real SPSC
  runs, where the in-flight depth never exceeds capacity) also pass the
  guard, so it is a defensive corruption check, not the ring invariant.
- **Z3.8** — the send guard `head - tail + k <= N` is sufficient: for every
  offset pair (j < k write targets, j' < dist occupied slots) the write
  index `(head + j) mod N` never equals an occupied index
  `(tail + j') mod N` — verified for k ∈ {1, 2, 4, 7}.
- **Z3.9** — the same no-collision guarantee with the recovery bias: a
  recovered producer writes from `head + bias` (stepping over the crashed
  batch) and the biased guard `head - tail + bias + k <= N` still implies
  disjoint write/occupied index sets.

### CP-SAT (bounded model checking)

- **C1 / C2** — formal reproduction of the historical underflow bug and
  verification of the fix: when a consumer refreshes its ring depth while
  the producer is mid-way through a chunked (multi-slot) send, the depth
  undercounts; consuming all chunks with the old `len -= 1` drives it below
  zero (found: SAT, with witness schedule). The saturating decrement makes
  this impossible (INFEASIBLE).
- **C3** — ring invariant: with exact bookkeeping, the aggregate guard
  `capacity - len >= k` is sufficient — no schedule (up to horizon 10, mixed
  1/2/4-slot messages) writes into an occupied slot.
- **C4 / C5** — crash recovery inference. C4 checks the current rule
  (minimum-seq over committed/pinned slots, wrapping-aware with a chunk-index
  tie-break) returns the true tail for contiguous runs. C5 documents why the
  predecessor-based start rule was replaced: for non-contiguous patterns
  (gaps created by the consumer's releases plus wrapped producer writes) the
  predecessor rule can pick a newer message as the tail — the checker finds
  a concrete witness. The min-seq rule is correct for every pattern within
  the seq window (see Z1.4/Z1.6).
- **C6** — the cursor gate theorem (the load-bearing property of the
  shared-cursor design): in a crash-free interleaving model of the protocol
  (producer starts a write at its frontier, finishes it to COMMITTED and
  advances the head count; consumer reads only COMMITTED slots), no reachable
  state has a positive depth while the slot at the consumer's tail is not
  COMMITTED. The head cursor is published after the whole batch commits, so
  an O(1) cursor refresh can never expose a half-committed message.
- **C7** — with a producer crash and a recovered successor, a WRITING slot
  does become visible at the consumer's tail (the count-based depth keeps a
  phantom slot over the crashed batch): reachability check with witness.
  This is why the recv path gates on COMMITTED instead of polling
  forever — the gate converts the zombie state into an empty-ring decision.

### SCIP (MILP)

- **S1 / S2** — `max_message_size = capacity * (slot_size - 42)` and
  `ceil(len / chunk)` hold as optima of the natural MILP formulations.
- **S3** — worst-case region file size over the config domain
  (64 × (4096 + 50 + 13) bytes, plus the 16-byte region header) matches the
  layout formula.
- **S4** — SCIP and CP-SAT independently compute `max_message_size` for the
  same configs and agree with the formula (differential cross-check).
- **S5** — the cursor-derived queue depth's MIP optimum is exactly the
  capacity: the clamp in `refresh_cursors` never masks a real over-commit.
- **S6** — the per-message flush end offset (region header + last slot +
  frame, maximized over the index domain and frame length) equals the total
  region size: the Windows batch flush range never exceeds the mapping.

### Replay vs implementation

- **R1** — `stats()` chunk/max-size limits equal the formulas for real
  channels.
- **R2** — the on-disk region file is exactly
  `16 + capacity * (slot_size + 50 + 13)` bytes (16 = the two shared cursor
  words in the region header).
- **R3** — a 400-step seeded random schedule of sends (including chunked
  messages) and receives under `Raise` backpressure matches the reference
  model (queue depth, sequence numbers, full/empty errors).
- **R4** — dual-handle replay: a producer channel and a consumer channel
  over one region in one process (the in-process equivalent of the
  cross-process SPSC, exercising the shared cursors end to end). A 300-step
  seeded random schedule matches the reference model on sequences, payloads,
  and the cursor-derived queue depth.

## Scope

The checks verify the *logic* of the channel (the algorithms and invariants
as encoded by the models) and its agreement with the real implementation via
replay. They are not a formal proof of the compiled code (no symbolic
execution of the Rust sources) and do not cover the multi-process state
machine races (check-then-act without locks), which remain covered by the
integration tests in `tests/python`.
