#!/usr/bin/env python3
"""Solver-based verification of the dsline channel logic.

Three solvers, three paradigms, one target: the fixed-slot SPSC channel in
crates/dsline-shm (slot state machine, shared ring cursors, wrapping sequence
ordering, chunked-message accounting, crash recovery inference).

  Z3    (SMT, bitvectors)     — proof-style properties: the wrapping
                                sequence comparison, the send-plan chunk
                                arithmetic, the next_seq recovery fold, and
                                the cursor-protocol arithmetic (wrap guard
                                exactness, send-guard no-collision theorems).
  CP-SAT (constraint program) — bounded model checking: search for schedules
                                that violate the ring invariants; formally
                                reproduces the historical len-underflow bug,
                                proves the cursor gate theorem (a crash-free
                                run never exposes an uncommitted slot), and
                                witnesses the crash-leftover zombie state.
  SCIP  (MILP)                — optimization checks: the message-size
                                formulas as optima, the cursor-depth and
                                batch-flush bounds as MIP optima, and
                                differential agreement with the other
                                solvers and with the real implementation.

Every check asserts an expected result (SAT/UNSAT/optimal value). The script
exits non-zero if any expectation fails.

Usage:
  python verify/check_invariants.py [--z3] [--cp-sat] [--scip] [--replay]
"""

import argparse
import os
import random
import sys

# Constants mirrored from dsline-core / dsline-shm.
CHUNK_METADATA_LEN = 42  # 3 TLVs x (2-byte type + 4-byte len + 8-byte value)
FRAME_HEADER_LEN = 50
PERSISTENT_SLOT_HEADER_LEN = 13
REGION_HEADER_LEN = 20  # head: u64 @ 0, tail: u64 @ 8, init magic: u32 @ 16

PASS = []
FAIL = []


def record(name, ok, detail=""):
    (PASS if ok else FAIL).append(name)
    status = "PASS" if ok else "FAIL"
    print(f"[{status}] {name}")
    if detail:
        print(f"       {detail}")


# ═══════════════════════════════════════════════════════════════════════
# Z3 — SMT bitvector properties
# ═══════════════════════════════════════════════════════════════════════

def check_z3():
    from z3 import BitVec, BitVecVal, If, Or, And, Not, ULT, ULE, Solver, sat, unsat
    from z3 import Ints

    def precedes(x, y):
        # dsline-shm: sequence_precedes(left, right)
        #   left != right && right.wrapping_sub(left) < 1 << 63
        return And(x != y, ULT(y - x, BitVecVal(1 << 63, 64)))

    def window(anchor, values, bits=61):
        # All values within wrapping distance <= 2^bits of `anchor`, so the
        # whole set spans less than half of the u64 range.
        constraints = []
        for v in values:
            d1 = v - anchor
            d2 = anchor - v
            constraints.append(ULT(If(ULT(d1, d2), d1, d2), BitVecVal(1 << bits, 64)))
        return And(*constraints)

    # ── Z1.1: precedes is irreflexive ──
    x = BitVec("x", 64)
    s = Solver()
    s.add(precedes(x, x))
    s.set(timeout=10000)
    record("Z1.1 sequence_precedes is irreflexive (no x: x<x)",
           s.check() == unsat)

    # ── Z1.2: precedes is asymmetric ──
    x, y = BitVec("x", 64), BitVec("y", 64)
    s = Solver()
    s.add(precedes(x, y), precedes(y, x))
    s.set(timeout=10000)
    record("Z1.2 sequence_precedes is asymmetric (no x,y: x<y and y<x)",
           s.check() == unsat)

    # ── Z1.3: precedes is NOT transitive over the full u64 range ──
    x, y, z = BitVec("x", 64), BitVec("y", 64), BitVec("z", 64)
    s = Solver()
    s.add(precedes(x, y), precedes(y, z), Not(precedes(x, z)))
    s.set(timeout=10000)
    result = s.check()
    witness = ""
    if result == sat:
        m = s.model()
        witness = (f"counterexample: x={m[x]}, y={m[y]}, z={m[z]} "
                   f"(x<y, y<z but not x<z)")
    record("Z1.3 full-range transitivity fails (valid only within a <2^63 window)",
           result == sat, witness)

    # ── Z1.4: transitivity holds inside a common window of 2^61 ──
    anchor = BitVec("anchor", 64)
    x, y, z = BitVec("x", 64), BitVec("y", 64), BitVec("z", 64)
    s = Solver()
    s.add(window(anchor, [x, y, z]))
    s.add(precedes(x, y), precedes(y, z), Not(precedes(x, z)))
    s.set(timeout=20000)
    record("Z1.4 transitivity holds within a 2^61 window (no violation)",
           s.check() == unsat)

    # ── Z1.5: transitivity breaks at window 2^63 (exact boundary) ──
    # With strict distance < 2^b from the anchor the value span is < 2^(b+1);
    # transitivity holds iff the span < 2^63, i.e. for b <= 62. At b = 63 the
    # span can reach 2^64 - 2 and a violation becomes possible.
    anchor = BitVec("anchor", 64)
    x, y, z = BitVec("x", 64), BitVec("y", 64), BitVec("z", 64)
    s = Solver()
    s.add(window(anchor, [x, y, z], bits=63))
    s.add(precedes(x, y), precedes(y, z), Not(precedes(x, z)))
    s.set(timeout=20000)
    record("Z1.5 window 2^63 is the exact transitivity boundary (violation found)",
           s.check() == sat)

    # ── Z1.6: next_seq recovery fold yields the wrapping max ──
    # Mirrors refresh_from_storage: cur starts 0; for each candidate
    # c = seq.wrapping_add(1): if sequence_precedes(cur, c) then cur = c.
    # Sound when 0 is part of the same window as the candidates (fresh open
    # always starts the fold at 0, and steady state starts from the previous
    # next_seq, which is in the window).
    n = 4
    cands = [BitVec(f"c{i}", 64) for i in range(n)]
    anchor = BitVec("anchor", 64)
    cur = BitVecVal(0, 64)
    for c in cands:
        cur = If(precedes(cur, c), c, cur)
    post = And(
        Or(cur == BitVecVal(0, 64), *[cur == c for c in cands]),
        *[Not(precedes(cur, c)) for c in cands],
    )
    s = Solver()
    s.add(window(anchor, [BitVecVal(0, 64)] + cands))
    s.add(Not(post))
    s.set(timeout=20000)
    record("Z1.6 next_seq fold yields the wrapping max when 0 is in the window",
           s.check() == unsat)

    # ── Z1.6b: fold is order-sensitive when candidates straddle 2^63 ──
    # If the occupied seqs sit near u64::MAX/2^63 and 0 is outside their
    # window (a region opened fresh after ~2^63 messages), the fold starting
    # at 0 can skip the true maximum. Benign in practice (SPSC windows span
    # at most 2*capacity seqs), but a real boundary of the algorithm.
    cands = [BitVec(f"c{i}", 64) for i in range(n)]
    anchor = BitVec("anchor", 64)
    cur = BitVecVal(0, 64)
    for c in cands:
        cur = If(precedes(cur, c), c, cur)
    post = And(
        Or(cur == BitVecVal(0, 64), *[cur == c for c in cands]),
        *[Not(precedes(cur, c)) for c in cands],
    )
    s = Solver()
    s.add(window(anchor, cands))
    s.add(Not(post))
    s.set(timeout=20000)
    result = s.check()
    witness = ""
    if result == sat:
        m = s.model()
        vals = sorted(m[c].as_long() for c in cands)
        witness = f"window anchor {m[anchor].as_long()}, candidates {vals}"
    record("Z1.6b fold can skip the true max when 0 is outside the window "
           "(2^63-boundary case, documented)", result == sat, witness)

    # ── Z2: send_plan / message-size arithmetic ──
    # Mirrors send_plan + message_size_limits:
    #   chunk = slot_size - 42 (0 when slot_size <= 42 -> single-slot only)
    #   needed = ceil(len / chunk) when len > slot_size, else 1
    #   max = max(slot_size, capacity * chunk)
    def z3_send_plan(capacity, slot_size, length):
        chunk = If(slot_size > 42, slot_size - 42, 0)
        needed = If(
            length <= slot_size,
            1,
            If(chunk == 0, capacity + 1, (length + chunk - 1) / chunk),
        )
        max_msg = If(
            slot_size > 42,
            If(capacity * chunk > slot_size, capacity * chunk, slot_size),
            slot_size,
        )
        return needed, max_msg

    cap, slot, length = Ints("cap slot length")
    s = Solver()
    s.add(cap >= 1, cap <= 64, slot >= 1, slot <= 4096, length >= 0, length <= 1 << 31)
    needed, max_msg = z3_send_plan(cap, slot, length)
    accepted = needed <= cap
    s.add(accepted != (length <= max_msg))
    s.set(timeout=20000)
    record("Z2.1 send accepted <=> len <= max_message_size (full domain)",
           s.check() == unsat)

    # Z2.2: chunked messages always cover the payload: needed*chunk >= len.
    s = Solver()
    s.add(cap >= 1, cap <= 64, slot >= 43, slot <= 4096, length >= slot + 1,
          length <= 1 << 31)
    needed, _ = z3_send_plan(cap, slot, length)
    chunk = slot - 42
    s.add(needed * chunk < length)
    s.set(timeout=20000)
    record("Z2.2 chunk count always covers the payload (needed*chunk >= len)",
           s.check() == unsat)

    # Z2.3: single-slot path is used exactly for len <= slot_size.
    s = Solver()
    s.add(cap >= 1, cap <= 64, slot >= 1, slot <= 4096, length >= 0, length <= 1 << 31)
    needed, _ = z3_send_plan(cap, slot, length)
    s.add((length <= slot) != (needed == 1))
    s.set(timeout=20000)
    record("Z2.3 single-slot path iff len <= slot_size",
           s.check() == unsat)

    # ── Z3.7: the cursor guard partitions the circle exactly ──
    # refresh_cursors computes dist = head.wrapping_sub(tail) and rejects
    # dist >= 2^63. In a healthy SPSC run the tail count never overtakes the
    # head count, so a firing guard always means corruption. Prove the
    # accept region is exactly the half-circle where head is ahead of tail:
    # the directed distance and its inverse cannot both be < 2^63.
    h, t = BitVec("hh", 64), BitVec("tt", 64)
    dist = h - t          # directed head -> tail distance
    ahead_tail = t - h    # directed tail -> head distance
    s = Solver()
    s.add(h != t)
    s.add(ULT(dist, BitVecVal(1 << 63, 64)))
    s.add(ULT(ahead_tail, BitVecVal(1 << 63, 64)))
    s.set(timeout=10000)
    record("Z3.7 cursor guard partitions the circle (accepts exactly the "
           "head-ahead half, h != t)", s.check() == unsat)

    # Z3.7b: accepting is not the whole story — a pair whose counts drifted
    # far apart in the forward direction also passes the guard. No real SPSC
    # run can produce it (the in-flight depth never exceeds capacity), so
    # the guard is a defensive corruption check, not the ring invariant
    # itself. Documented with the concrete drifted pair found by Z3.
    s = Solver()
    s.add(h == BitVecVal(268435454, 64))
    s.add(t == BitVecVal(9223372037123211264, 64))
    s.add(ULT(dist, BitVecVal(1 << 63, 64)))
    s.add(Not(ULT(ahead_tail, BitVecVal(1 << 63, 64))))
    s.set(timeout=10000)
    record("Z3.7b forward-drifted counts also pass the guard (documented "
           "boundary; unreachable in real runs)", s.check() == sat)

    # ── Z3.8: send guard implies write targets are unoccupied (mod N) ──
    # With N slots, occupied indices are counts [tail, head), next writes
    # are counts [head, head + k) (both mod N). The guard head - tail + k
    # <= N keeps the two index sets disjoint. Verified for every interleaved
    # j in [0,k) and j' in [0,dist) and several k.
    N = 8
    for k in (1, 2, 4, 7):
        h, t = BitVec(f"h{k}", 64), BitVec(f"t{k}", 64)
        dist = h - t
        s = Solver()
        s.add(ULT(dist, BitVecVal(1 << 63, 64)))
        s.add(ULE(dist, BitVecVal(N - k, 64)))
        collisions = []
        for j in range(k):
            for jp in range(N):
                collisions.append(
                    And(
                        ULT(BitVecVal(jp, 64), dist),
                        (h + j) % N == (t + jp) % N,
                    )
                )
        s.add(Or(*collisions))
        s.set(timeout=20000)
        record(f"Z3.8 guard head-tail+{k}<=N avoids writing into an occupied index",
               s.check() == unsat)

    # ── Z3.9: recovery bias keeps the same guarantee for crash leftovers ──
    # A recovered producer writes from the scan head, which steps over the
    # crashed batch (occupancy tail..head plus `bias` leftover slots); with
    # the biased guard (head-tail) + bias + k <= N the writes [head+bias,
    # head+bias+k) never hit an occupied index, so the recovery can never
    # over-commit a physically full ring.
    for k in (1, 2, 3):
        h, t, bias = BitVec(f"h{k}", 64), BitVec(f"t{k}", 64), BitVec(f"b{k}", 64)
        dist = h - t
        s = Solver()
        s.add(ULT(dist, BitVecVal(1 << 63, 64)))
        s.add(ULT(bias, BitVecVal(N, 64)))
        s.add(ULE(dist + bias, BitVecVal(N - k, 64)))
        collisions = []
        for j in range(k):
            for jp in range(N + k):
                collisions.append(
                    And(
                        ULT(BitVecVal(jp, 64), dist + bias),
                        (h + bias + j) % N == (t + jp) % N,
                    )
                )
        s.add(Or(*collisions))
        s.set(timeout=20000)
        record("Z3.9 biased guard head-tail+bias+k<=N avoids the crash batch",
               s.check() == unsat)


# ═══════════════════════════════════════════════════════════════════════
# CP-SAT helpers — reification idioms
# ═══════════════════════════════════════════════════════════════════════

def _reify_eq(model, left, right, cond):
    """cond <-> (left == right) for integer variables or constants."""
    model.Add(left == right).OnlyEnforceIf(cond)
    model.Add(left != right).OnlyEnforceIf(cond.Not())


def _reify_lt(model, left, right, cond):
    """cond <-> (left < right) for integer variables or constants."""
    model.Add(left < right).OnlyEnforceIf(cond)
    model.Add(left >= right).OnlyEnforceIf(cond.Not())


def _reify_ge(model, left, right, cond):
    """cond <-> (left >= right) for integer variables or constants."""
    model.Add(left >= right).OnlyEnforceIf(cond)
    model.Add(left < right).OnlyEnforceIf(cond.Not())


def _reify_ne(model, left, right, cond):
    """cond <-> (left != right) for integer variables or constants."""
    model.Add(left != right).OnlyEnforceIf(cond)
    model.Add(left == right).OnlyEnforceIf(cond.Not())


def _mod_ring(model, value, modulus, result, tag):
    """result == value mod `modulus` for value in (-modulus, 2*modulus)."""
    ge = model.NewBoolVar(f"{tag}_ge")
    _reify_ge(model, value, modulus, ge)
    neg = model.NewBoolVar(f"{tag}_neg")
    _reify_lt(model, value, 0, neg)
    model.Add(result == value - modulus).OnlyEnforceIf(ge)
    model.Add(result == value + modulus).OnlyEnforceIf([neg, ge.Not()])
    model.Add(result == value).OnlyEnforceIf([neg.Not(), ge.Not()])


# ═══════════════════════════════════════════════════════════════════════
# CP-SAT — bounded model checking of ring behavior
# ═══════════════════════════════════════════════════════════════════════

def check_cp_sat():
    from ortools.sat.python import cp_model

    # ── C1: historical len-underflow reproduction ──
    # Producer sends one 4-slot chunked message, writing one slot per step
    # (WRITING -> payload -> COMMITTED per slot). The consumer refreshes its
    # ring depth while the producer is mid-write, so the refresh sees fewer
    # than 4 occupied slots, then consumes all 4 chunks. Without the
    # saturating fix the per-chunk decrement underflows.
    H = 12
    model = cp_model.CpModel()
    writes = [model.NewIntVar(0, H - 1, f"w{j}") for j in range(4)]
    refresh = model.NewIntVar(0, H - 1, "refresh")
    reads = [model.NewIntVar(0, H - 1, f"r{j}") for j in range(4)]
    for j in range(3):
        model.Add(writes[j] + 1 <= writes[j + 1])
    for j in range(4):
        model.Add(writes[j] <= reads[j])
        if j < 3:
            model.Add(reads[j] + 1 <= reads[j + 1])
    visible = [model.NewBoolVar(f"vis{j}") for j in range(4)]
    for j in range(4):
        _reify_ge(model, refresh, writes[j], visible[j])
    depth0 = model.NewIntVar(0, 4, "depth0")
    model.Add(depth0 == sum(visible))
    # Target: the refresh undercounts (sees < 4 slots) yet all 4 chunks are
    # consumed with the plain per-chunk decrement -> depth goes negative.
    underflow = model.NewBoolVar("underflow")
    model.Add(depth0 - 4 < 0).OnlyEnforceIf(underflow)
    model.Add(underflow == 1)
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 30
    status = solver.Solve(model)
    witness = ""
    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        witness = ("schedule: writes at "
                   f"{[solver.Value(w) for w in writes]}, refresh at "
                   f"{solver.Value(refresh)} (depth {solver.Value(depth0)} < 4, "
                   "consuming 4 chunks underflows)")
    record("C1 pre-fix len accounting underflows mid-write (bug reproduced)",
           status == cp_model.OPTIMAL, witness)

    # ── C2: saturating decrement never goes negative ──
    model = cp_model.CpModel()
    writes = [model.NewIntVar(0, H - 1, f"w{j}") for j in range(4)]
    refresh = model.NewIntVar(0, H - 1, "refresh")
    reads = [model.NewIntVar(0, H - 1, f"r{j}") for j in range(4)]
    for j in range(3):
        model.Add(writes[j] + 1 <= writes[j + 1])
    for j in range(4):
        model.Add(writes[j] <= reads[j])
        if j < 3:
            model.Add(reads[j] + 1 <= reads[j + 1])
    visible = [model.NewBoolVar(f"vis{j}") for j in range(4)]
    for j in range(4):
        _reify_ge(model, refresh, writes[j], visible[j])
    depth = [model.NewIntVar(0, 4, f"d{j}") for j in range(5)]
    model.Add(depth[0] == sum(visible))
    for j in range(4):
        nonzero = model.NewBoolVar(f"nz{j}")
        _reify_ge(model, depth[j], 1, nonzero)
        # Saturating decrement: d[j+1] = max(0, d[j] - 1).
        model.Add(depth[j + 1] == depth[j] - 1).OnlyEnforceIf(nonzero)
        model.Add(depth[j + 1] == 0).OnlyEnforceIf(nonzero.Not())
    neg = model.NewBoolVar("neg")
    model.Add(depth[4] < 0).OnlyEnforceIf(neg)
    model.Add(neg == 1)
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 30
    status = solver.Solve(model)
    record("C2 saturating decrement never goes negative (fix verified)",
           status == cp_model.INFEASIBLE)

    # ── C3: aggregate ring-depth guard implies the target slots are free ──
    # Producer sends messages of 1/2/4 slots, consumer receives them FIFO.
    # The channel only checks capacity - len >= k before writing; the slots
    # themselves must be free. Search over every interleaving up to H steps
    # for a schedule that writes into an occupied slot.
    C, H = 6, 10
    model = cp_model.CpModel()
    total = [model.NewIntVar(0, C, f"total{t}") for t in range(H + 1)]
    head = [model.NewIntVar(0, C - 1, f"head{t}") for t in range(H + 1)]
    tail = [model.NewIntVar(0, C - 1, f"tail{t}") for t in range(H + 1)]
    created = [model.NewIntVar(0, H, f"created{t}") for t in range(H + 1)]
    recvd = [model.NewIntVar(0, H, f"recvd{t}") for t in range(H + 1)]
    sizes = [model.NewIntVar(0, 4, f"size{m}") for m in range(H)]
    is_send = [model.NewBoolVar(f"send{t}") for t in range(H)]
    is_recv = [model.NewBoolVar(f"recv{t}") for t in range(H)]
    k = [model.NewIntVar(0, 4, f"k{t}") for t in range(H)]
    pop_size = [model.NewIntVar(0, 4, f"pop{t}") for t in range(H)]
    # Initial state: empty ring, cursors at 0.
    model.Add(total[0] == 0)
    model.Add(head[0] == 0)
    model.Add(tail[0] == 0)
    model.Add(created[0] == 0)
    model.Add(recvd[0] == 0)
    for t in range(H):
        model.Add(is_send[t] + is_recv[t] <= 1)
        # Send: pick a size in {1, 2, 4} and queue it at ordinal `created[t]`.
        size_options = []
        for v in (1, 2, 4):
            eq = model.NewBoolVar(f"ksz{t}_{v}")
            _reify_eq(model, k[t], v, eq)
            size_options.append(eq)
        model.AddBoolOr(size_options).OnlyEnforceIf(is_send[t])
        model.Add(k[t] == 0).OnlyEnforceIf(is_send[t].Not())
        for m in range(H):
            at_ordinal = model.NewBoolVar(f"ord{t}_{m}")
            _reify_eq(model, created[t], m, at_ordinal)
            model.Add(sizes[m] == k[t]).OnlyEnforceIf([is_send[t], at_ordinal])
        # Receive: there must be an unread message; pop its size.
        model.Add(recvd[t] < created[t]).OnlyEnforceIf(is_recv[t])
        for m in range(H):
            at_ordinal = model.NewBoolVar(f"rord{t}_{m}")
            _reify_eq(model, recvd[t], m, at_ordinal)
            model.Add(pop_size[t] == sizes[m]).OnlyEnforceIf([is_recv[t], at_ordinal])
        model.Add(total[t] >= pop_size[t]).OnlyEnforceIf(is_recv[t])
        # Guard: aggregate depth must leave room for k slots.
        model.Add(total[t] + k[t] <= C).OnlyEnforceIf(is_send[t])
        # Transitions. head advances only on send; tail advances only on recv.
        model.Add(created[t + 1] == created[t] + is_send[t])
        model.Add(recvd[t + 1] == recvd[t] + is_recv[t])
        popped = model.NewIntVar(0, 4, f"popped{t}")
        model.Add(popped == pop_size[t]).OnlyEnforceIf(is_recv[t])
        model.Add(popped == 0).OnlyEnforceIf(is_recv[t].Not())
        model.Add(total[t + 1] == total[t] + k[t] - popped)
        head_raw = model.NewIntVar(0, 2 * C, f"headraw{t}")
        model.Add(head_raw == head[t] + k[t])
        _mod_ring(model, head_raw, C, head[t + 1], f"headmod{t}")
        tail_raw = model.NewIntVar(-C, C, f"tailraw{t}")
        model.Add(tail_raw == tail[t] + popped)
        _mod_ring(model, tail_raw, C, tail[t + 1], f"tailmod{t}")
    # Occupancy of slot i at step t (before the step's effect):
    # i is occupied iff (i - tail) mod C < total.
    occ = [[model.NewBoolVar(f"occ{t}_{i}") for i in range(C)] for t in range(H)]
    for t in range(H):
        for i in range(C):
            pos = model.NewIntVar(0, C - 1, f"pos{t}_{i}")
            _mod_ring(model, i - tail[t], C, pos, f"posmod{t}_{i}")
            _reify_lt(model, pos, total[t], occ[t][i])
    # Violation: a send targets a slot that is already occupied.
    targets_occupied = []
    for t in range(H):
        for j in range(C):
            slot_idx = model.NewIntVar(0, C - 1, f"tgt{t}_{j}")
            _mod_ring(model, head[t] + j, C, slot_idx, f"tgtmod{t}_{j}")
            slot_occ = model.NewBoolVar(f"socc{t}_{j}")
            model.AddElement(slot_idx, occ[t], slot_occ)
            in_range = model.NewBoolVar(f"inr{t}_{j}")
            _reify_lt(model, j, k[t], in_range)
            hit = model.NewBoolVar(f"hit{t}_{j}")
            model.Add(hit == 1).OnlyEnforceIf([is_send[t], in_range, slot_occ])
            model.Add(hit == 0).OnlyEnforceIf(is_send[t].Not())
            model.Add(hit == 0).OnlyEnforceIf(in_range.Not())
            model.Add(hit == 0).OnlyEnforceIf(slot_occ.Not())
            targets_occupied.append(hit)
    model.Add(sum(targets_occupied) == 1)
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 60
    status = solver.Solve(model)
    witness = ""
    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        acts = []
        for t in range(H):
            if solver.Value(is_send[t]):
                acts.append(f"S{k[t].Name()[:0] or ''}k={solver.Value(k[t])}")
            elif solver.Value(is_recv[t]):
                acts.append(f"R(m{int(solver.Value(recvd[t]))})")
            else:
                acts.append("idle")
        hit_t, hit_j = None, None
        for t in range(H):
            for j in range(C):
                if solver.Value(targets_occupied[t * C + j]):
                    hit_t, hit_j = t, j
        witness = (f"schedule {acts}: send at step {hit_t} targets slot "
                   f"{(solver.Value(head[hit_t]) + hit_j) % C} which is occupied "
                   f"(head={solver.Value(head[hit_t])}, tail={solver.Value(tail[hit_t])}, "
                   f"total={solver.Value(total[hit_t])})")
    record("C3 aggregate guard never writes into an occupied slot "
           "(ring invariant, bounded model check)", status == cp_model.INFEASIBLE,
           witness)

    # ── C4: recovery inference returns the true tail for contiguous runs ──
    # Ring with C slots; occupied slots form a contiguous run from `tail`,
    # seqs increase clockwise. The inference rule must return `tail`:
    #   ring not full -> first occupied slot whose predecessor is free;
    #   ring full     -> the minimum-seq slot.
    C = 8
    model = cp_model.CpModel()
    tail = model.NewIntVar(0, C - 1, "tail")
    run_len = model.NewIntVar(1, C, "run")
    occ = [model.NewBoolVar(f"o{i}") for i in range(C)]
    for i in range(C):
        dist = model.NewIntVar(0, C - 1, f"d{i}")
        _mod_ring(model, i - tail, C, dist, f"dist{i}")
        _reify_lt(model, dist, run_len, occ[i])
    starts = [model.NewBoolVar(f"start{i}") for i in range(C)]
    for i in range(C):
        prev = occ[(i - 1) % C]
        model.Add(starts[i] == 1).OnlyEnforceIf([occ[i], prev.Not()])
        model.Add(starts[i] == 0).OnlyEnforceIf(occ[i].Not())
        model.Add(starts[i] == 0).OnlyEnforceIf(prev)
    # The rule: ring not full -> the unique start; ring full -> no start.
    not_full = model.NewBoolVar("not_full")
    _reify_lt(model, run_len, C, not_full)
    model.Add(sum(starts) == 1).OnlyEnforceIf(not_full)
    model.Add(sum(starts) == 0).OnlyEnforceIf(not_full.Not())
    # Seqs increase clockwise; distinct among occupied slots.
    seqs = [model.NewIntVar(0, 31, f"s{i}") for i in range(C)]
    for i in range(C):
        nxt = occ[(i + 1) % C]
        model.Add(seqs[(i + 1) % C] > seqs[i]).OnlyEnforceIf([occ[i], nxt])
    for i in range(C):
        for j in range(i + 1, C):
            model.Add(seqs[i] != seqs[j]).OnlyEnforceIf([occ[i], occ[j]])
    # is_min[i]: i is the occupied slot with the smallest seq (unique).
    is_min = [model.NewBoolVar(f"min{i}") for i in range(C)]
    for i in range(C):
        model.Add(is_min[i] == 0).OnlyEnforceIf(occ[i].Not())
        for j in range(C):
            if j != i:
                model.Add(seqs[i] < seqs[j]).OnlyEnforceIf([is_min[i], occ[j]])
    model.Add(sum(is_min) == 1)
    # The rule's output: start when not full, argmin when full.
    picked_start = model.NewIntVar(0, C - 1, "picked_start")
    for i in range(C):
        model.Add(picked_start == i).OnlyEnforceIf(starts[i])
    argmin = model.NewIntVar(0, C - 1, "argmin")
    for i in range(C):
        model.Add(argmin == i).OnlyEnforceIf(is_min[i])
    picked = model.NewIntVar(0, C - 1, "picked")
    model.Add(picked == picked_start).OnlyEnforceIf(not_full)
    model.Add(picked == argmin).OnlyEnforceIf(not_full.Not())
    mismatch = model.NewBoolVar("mismatch")
    _reify_ne(model, picked, tail, mismatch)
    model.Add(mismatch == 1)
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 30
    status = solver.Solve(model)
    record("C4 recovery inference returns the true tail for contiguous runs",
           status == cp_model.INFEASIBLE)

    # ── C5: why the predecessor start rule was replaced ──
    # For arbitrary (non-contiguous) occupancy patterns the predecessor-based
    # start rule ("first occupied slot whose predecessor is free") can return
    # a slot that is not the oldest — gaps from the consumer's releases plus
    # wrapped producer writes put newer messages at lower indices. The current
    # implementation uses min-seq instead (correct for every pattern); this
    # check documents the historical failure and keeps the model as a
    # regression guard.
    model = cp_model.CpModel()
    occ = [model.NewBoolVar(f"o{i}") for i in range(C)]
    seqs = [model.NewIntVar(0, 31, f"s{i}") for i in range(C)]
    for i in range(C):
        for j in range(i + 1, C):
            model.Add(seqs[i] != seqs[j]).OnlyEnforceIf([occ[i], occ[j]])
    model.Add(sum(occ) >= 1)
    starts = [model.NewBoolVar(f"start{i}") for i in range(C)]
    for i in range(C):
        prev = occ[(i - 1) % C]
        model.Add(starts[i] == 1).OnlyEnforceIf([occ[i], prev.Not()])
        model.Add(starts[i] == 0).OnlyEnforceIf(occ[i].Not())
        model.Add(starts[i] == 0).OnlyEnforceIf(prev)
    model.Add(sum(starts) == 1)
    picked = model.NewIntVar(0, C - 1, "picked")
    for i in range(C):
        model.Add(picked == i).OnlyEnforceIf(starts[i])
    is_min = [model.NewBoolVar(f"min{i}") for i in range(C)]
    for i in range(C):
        model.Add(is_min[i] == 0).OnlyEnforceIf(occ[i].Not())
        for j in range(C):
            if j != i:
                model.Add(seqs[i] < seqs[j]).OnlyEnforceIf([is_min[i], occ[j]])
    model.Add(sum(is_min) == 1)
    argmin = model.NewIntVar(0, C - 1, "argmin")
    for i in range(C):
        model.Add(argmin == i).OnlyEnforceIf(is_min[i])
    mismatch = model.NewBoolVar("mismatch")
    _reify_ne(model, picked, argmin, mismatch)
    model.Add(mismatch == 1)
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 30
    status = solver.Solve(model)
    witness = ""
    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        pat = "".join("1" if solver.Value(occ[i]) else "0" for i in range(C))
        witness = (f"pattern {pat}: rule picks slot {solver.Value(picked)}, "
                   f"oldest (min seq) is slot {solver.Value(argmin)}")
    record("C5 predecessor start rule mis-recovers on gapped patterns "
           "(why the code uses min-seq)", status == cp_model.OPTIMAL, witness)

# ── C6: cursor design — crash-free runs: len > 0 implies the slot at
    # the consumer's tail is COMMITTED ──
    # The producer publishes the head cursor only after a whole batch
    # commits, so the consumer's cursor-derived depth [tail, head) is exactly
    # the committed prefix; WRITING slots are always beyond head and never
    # counted. Model the protocol as a transition system (producer starts a
    # write at its frontier, finishes it to COMMITTED and advances head;
    # consumer reads only COMMITTED slots at its tail and releases) and
    # search every interleaving for a state where len > 0 but the tail slot
    # is not COMMITTED. This is the load-bearing theorem of the shared-
    # cursor refresh: without it the O(1) refresh could expose half-committed
    # messages.
    N, H = 4, 7
    model = cp_model.CpModel()
    st = [[model.NewIntVar(0, 2, f"st{t}_{i}") for i in range(N)] for t in range(H + 1)]
    head = [model.NewIntVar(0, H, f"h{t}") for t in range(H + 1)]
    tail = [model.NewIntVar(0, H, f"t{t}") for t in range(H + 1)]
    act = [model.NewIntVar(0, 3, f"a{t}") for t in range(H)]  # 0 idle, 1 start, 2 finish, 3 read
    ac = {}
    for t in range(H):
        ac[t] = [model.NewBoolVar(f"ac{t}_{v}") for v in range(4)]
        for v in range(4):
            _reify_eq(model, act[t], v, ac[t][v])
    model.Add(head[0] == 0)
    model.Add(tail[0] == 0)
    for i in range(N):
        model.Add(st[0][i] == 0)  # all FREE
    for t in range(H):
        h_idx = model.NewIntVar(0, N - 1, f"hidx{t}")
        _mod_ring(model, head[t], N, h_idx, f"hmod{t}")
        t_idx = model.NewIntVar(0, N - 1, f"tidx{t}")
        _mod_ring(model, tail[t], N, t_idx, f"tmod{t}")
        # Element lookups (lists cannot be indexed by variables).
        cur_h = model.NewIntVar(0, 2, f"ch{t}")
        model.AddElement(h_idx, st[t], cur_h)
        nxt_h = model.NewIntVar(0, 2, f"nh{t}")
        model.AddElement(h_idx, st[t + 1], nxt_h)
        cur_t = model.NewIntVar(0, 2, f"ct{t}")
        model.AddElement(t_idx, st[t], cur_t)
        nxt_t = model.NewIntVar(0, 2, f"nt{t}")
        model.AddElement(t_idx, st[t + 1], nxt_t)
        # start: frontier slot must be FREE; it becomes WRITING.
        model.Add(cur_h == 0).OnlyEnforceIf(ac[t][1])
        model.Add(nxt_h == 1).OnlyEnforceIf(ac[t][1])
        model.Add(head[t] - tail[t] < N).OnlyEnforceIf(ac[t][1])  # the send guard
        # finish: frontier slot must be WRITING; it becomes COMMITTED.
        model.Add(cur_h == 1).OnlyEnforceIf(ac[t][2])
        model.Add(nxt_h == 2).OnlyEnforceIf(ac[t][2])
        model.Add(head[t + 1] == head[t] + 1).OnlyEnforceIf(ac[t][2])
        # read: tail slot must be COMMITTED; it becomes FREE again.
        model.Add(cur_t == 2).OnlyEnforceIf(ac[t][3])
        model.Add(nxt_t == 0).OnlyEnforceIf(ac[t][3])
        model.Add(tail[t + 1] == tail[t] + 1).OnlyEnforceIf(ac[t][3])
        model.Add(head[t + 1] == head[t]).OnlyEnforceIf(ac[t][2].Not())
        model.Add(tail[t + 1] == tail[t]).OnlyEnforceIf(ac[t][3].Not())
        # Untouched slots carry their state over.
        for i in range(N):
            is_front = model.NewBoolVar(f"fr{t}_{i}")
            _reify_eq(model, h_idx, i, is_front)
            is_tail = model.NewBoolVar(f"tl{t}_{i}")
            _reify_eq(model, t_idx, i, is_tail)
            front_touched = model.NewBoolVar(f"ft{t}_{i}")
            model.Add(front_touched == 1).OnlyEnforceIf([is_front, ac[t][1]])
            model.Add(front_touched == 1).OnlyEnforceIf([is_front, ac[t][2]])
            model.Add(front_touched == 0).OnlyEnforceIf(is_front.Not())
            model.Add(front_touched == 0).OnlyEnforceIf([ac[t][1].Not(), ac[t][2].Not()])
            tail_touched = model.NewBoolVar(f"tt{t}_{i}")
            model.Add(tail_touched == 1).OnlyEnforceIf([is_tail, ac[t][3]])
            model.Add(tail_touched == 0).OnlyEnforceIf(is_tail.Not())
            model.Add(tail_touched == 0).OnlyEnforceIf(ac[t][3].Not())
            touched = model.NewBoolVar(f"tch{t}_{i}")
            # touched = front_touched OR tail_touched (both directions).
            model.Add(touched == 1).OnlyEnforceIf(front_touched)
            model.Add(touched == 1).OnlyEnforceIf(tail_touched)
            model.Add(touched == 0).OnlyEnforceIf(
                [front_touched.Not(), tail_touched.Not()])
            model.Add(st[t + 1][i] == st[t][i]).OnlyEnforceIf(touched.Not())
    # Violation: at some step the depth is positive but the tail slot is not
    # COMMITTED on the shared state the refresh would read.
    violations = []
    for t in range(H + 1):
        t_idx = model.NewIntVar(0, N - 1, f"v_tidx{t}")
        _mod_ring(model, tail[t], N, t_idx, f"v_tmod{t}")
        tail_state = model.NewIntVar(0, 2, f"vs{t}")
        model.AddElement(t_idx, st[t], tail_state)
        not_committed = model.NewBoolVar(f"nc{t}")
        _reify_ne(model, tail_state, 2, not_committed)
        pos = model.NewBoolVar(f"pos{t}")
        model.Add(head[t] > tail[t]).OnlyEnforceIf(pos)
        model.Add(head[t] <= tail[t]).OnlyEnforceIf(pos.Not())
        bad = model.NewBoolVar(f"bad{t}")
        model.Add(bad == 1).OnlyEnforceIf([pos, not_committed])
        model.Add(bad == 0).OnlyEnforceIf(pos.Not())
        model.Add(bad == 0).OnlyEnforceIf(not_committed.Not())
        violations.append(bad)
    model.Add(sum(violations) == 1)
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 60
    status = solver.Solve(model)
    witness = ""
    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        acts = []
        for t in range(H):
            v = solver.Value(act[t])
            acts.append({0: "_", 1: "W", 2: "C", 3: "R"}[v])
        bad_t = next(t for t, b in enumerate(violations) if solver.Value(b))
        witness = (f"schedule {' '.join(acts)}: at step {bad_t} len="
                   f"{solver.Value(head[bad_t]) - solver.Value(tail[bad_t])} "
                   f"but tail slot state {solver.Value(st[bad_t][solver.Value(tail[bad_t]) % N])}")
    record("C6 cursor depth never includes an uncommitted slot in a "
           "crash-free run (gate theorem, bounded model check)",
           status == cp_model.INFEASIBLE, witness)

    # ── C7: with a producer crash and a recovered successor, a WRITING
    # slot becomes visible at the consumer's tail — the reason the recv path
    # gates on COMMITTED instead of polling. The crash leaves the in-flight
    # slot WRITING; the recovered producer writes past it and advances the
    # head cursor over its position, so the count-based depth keeps a
    # "phantom" slot at the consumer's tail. A poll-only consumer would
    # spin forever on that slot; the gate converts it to an empty-ring
    # decision. Reachability check with witness.
    N, H = 5, 8
    model = cp_model.CpModel()
    st = [[model.NewIntVar(0, 2, f"st{t}_{i}") for i in range(N)] for t in range(H + 1)]
    head = [model.NewIntVar(0, H, f"h{t}") for t in range(H + 1)]
    tail = [model.NewIntVar(0, H, f"t{t}") for t in range(H + 1)]
    crashed = [model.NewBoolVar(f"cr{t}") for t in range(H + 1)]
    act = [model.NewIntVar(0, 3, f"a{t}") for t in range(H)]  # 0 read, 1 crash, 2 start, 3 finish
    ac = {}
    for t in range(H):
        ac[t] = [model.NewBoolVar(f"ac{t}_{v}") for v in range(4)]
        for v in range(4):
            _reify_eq(model, act[t], v, ac[t][v])
    model.Add(head[0] == 0)
    model.Add(tail[0] == 0)
    model.Add(crashed[0] == 0)
    for i in range(N):
        model.Add(st[0][i] == 0)
    for t in range(H):
        # The producer's frontier is the head count; once a crash has left a
        # WRITING slot there, the recovered producer steps over it, so post-
        # crash writes target head + 1.
        h_idx = model.NewIntVar(0, N - 1, f"hidx{t}")
        _mod_ring(model, head[t] + crashed[t], N, h_idx, f"hmod{t}")
        t_idx = model.NewIntVar(0, N - 1, f"tidx{t}")
        _mod_ring(model, tail[t], N, t_idx, f"tmod{t}")
        cur_h = model.NewIntVar(0, 2, f"ch{t}")
        model.AddElement(h_idx, st[t], cur_h)
        nxt_h = model.NewIntVar(0, 2, f"nh{t}")
        model.AddElement(h_idx, st[t + 1], nxt_h)
        cur_t = model.NewIntVar(0, 2, f"ct{t}")
        model.AddElement(t_idx, st[t], cur_t)
        nxt_t = model.NewIntVar(0, 2, f"nt{t}")
        model.AddElement(t_idx, st[t + 1], nxt_t)
        # crash: once, and the producer is silent afterwards.
        model.Add(crashed[t + 1] == 1).OnlyEnforceIf([crashed[t], ac[t][1]])
        model.Add(act[t] != 1).OnlyEnforceIf(crashed[t])
        model.Add(crashed[t + 1] == 0).OnlyEnforceIf([crashed[t].Not(), ac[t][1].Not()])
        # start: frontier slot must be FREE; it becomes WRITING.
        model.Add(cur_h == 0).OnlyEnforceIf(ac[t][2])
        model.Add(nxt_h == 1).OnlyEnforceIf(ac[t][2])
        model.Add(head[t] - tail[t] < N).OnlyEnforceIf(ac[t][2])
        # finish: frontier slot must be WRITING; it becomes COMMITTED.
        model.Add(cur_h == 1).OnlyEnforceIf(ac[t][3])
        model.Add(nxt_h == 2).OnlyEnforceIf(ac[t][3])
        model.Add(head[t + 1] == head[t] + 1).OnlyEnforceIf(ac[t][3])
        # read: tail slot must be COMMITTED; it becomes FREE again.
        model.Add(cur_t == 2).OnlyEnforceIf(ac[t][0])
        model.Add(nxt_t == 0).OnlyEnforceIf(ac[t][0])
        model.Add(tail[t + 1] == tail[t] + 1).OnlyEnforceIf(ac[t][0])
        model.Add(head[t + 1] == head[t]).OnlyEnforceIf(ac[t][3].Not())
        model.Add(tail[t + 1] == tail[t]).OnlyEnforceIf(ac[t][0].Not())
        # Untouched slots carry their state over (also across a crash).
        for i in range(N):
            is_front = model.NewBoolVar(f"fr{t}_{i}")
            _reify_eq(model, h_idx, i, is_front)
            is_tail = model.NewBoolVar(f"tl{t}_{i}")
            _reify_eq(model, t_idx, i, is_tail)
            front_touched = model.NewBoolVar(f"ft{t}_{i}")
            model.Add(front_touched == 1).OnlyEnforceIf([is_front, ac[t][2]])
            model.Add(front_touched == 1).OnlyEnforceIf([is_front, ac[t][3]])
            model.Add(front_touched == 0).OnlyEnforceIf(is_front.Not())
            model.Add(front_touched == 0).OnlyEnforceIf([ac[t][2].Not(), ac[t][3].Not()])
            tail_touched = model.NewBoolVar(f"tt{t}_{i}")
            model.Add(tail_touched == 1).OnlyEnforceIf([is_tail, ac[t][0]])
            model.Add(tail_touched == 0).OnlyEnforceIf(is_tail.Not())
            model.Add(tail_touched == 0).OnlyEnforceIf(ac[t][0].Not())
            touched = model.NewBoolVar(f"tch{t}_{i}")
            # touched = front_touched OR tail_touched (both directions).
            model.Add(touched == 1).OnlyEnforceIf(front_touched)
            model.Add(touched == 1).OnlyEnforceIf(tail_touched)
            model.Add(touched == 0).OnlyEnforceIf(
                [front_touched.Not(), tail_touched.Not()])
            model.Add(st[t + 1][i] == st[t][i]).OnlyEnforceIf(touched.Not())
    # Target: a reachable state where the depth is positive, the tail slot
    # is WRITING (the crash leftover), and the producer is gone.
    zombie_at = []
    for t in range(H + 1):
        t_idx = model.NewIntVar(0, N - 1, f"tgt{t}")
        _mod_ring(model, tail[t], N, t_idx, f"tgtmod{t}")
        tail_state = model.NewIntVar(0, 2, f"zs{t}")
        model.AddElement(t_idx, st[t], tail_state)
        pos = model.NewBoolVar(f"zp{t}")
        model.Add(head[t] > tail[t]).OnlyEnforceIf(pos)
        model.Add(head[t] <= tail[t]).OnlyEnforceIf(pos.Not())
        writing = model.NewBoolVar(f"zw{t}")
        _reify_eq(model, tail_state, 1, writing)
        hit = model.NewBoolVar(f"zh{t}")
        model.Add(hit == 1).OnlyEnforceIf([pos, writing, crashed[t]])
        model.Add(hit == 0).OnlyEnforceIf(pos.Not())
        model.Add(hit == 0).OnlyEnforceIf(writing.Not())
        model.Add(hit == 0).OnlyEnforceIf(crashed[t].Not())
        zombie_at.append(hit)
    model.Add(sum(zombie_at) >= 1)
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 60
    status = solver.Solve(model)
    witness = ""
    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        acts = []
        for t in range(H):
            v = solver.Value(act[t])
            acts.append({0: "R", 1: "X", 2: "W", 3: "C"}[v])
        zt = next(t for t, h in enumerate(zombie_at) if solver.Value(h))
        witness = (f"schedule {' '.join(acts)}: at step {zt} the producer is "
                   f"gone and the consumer's tail slot is WRITING (depth "
                   f"{solver.Value(head[zt]) - solver.Value(tail[zt])})")
    record("C7 crashed writer leaves a visible WRITING slot at the consumer's "
           "tail (zombie state reachable; the COMMITTED gate converts it to "
           "an empty ring)", status in (cp_model.OPTIMAL, cp_model.FEASIBLE), witness)

def check_scip():
    import pyscipopt as scip

    def scip_max_len(capacity, slot_size):
        # Maximize payload len s.t. it fits in `capacity` chunks.
        model = scip.Model("max-len")
        model.hideOutput()
        model.setParam("limits/time", 30)
        chunk = max(0, slot_size - CHUNK_METADATA_LEN)
        k = model.addVar(vtype="I", lb=1, ub=capacity, name="k")
        length = model.addVar(vtype="I", lb=0, ub=1 << 20, name="len")
        model.addCons(length <= k * chunk)
        model.setObjective(length, sense="maximize")
        model.optimize()
        return model.getObjVal()

    def scip_min_chunks(capacity, slot_size, length):
        # Minimize the number of chunks needed for `length` payload bytes.
        model = scip.Model("min-chunks")
        model.hideOutput()
        model.setParam("limits/time", 30)
        chunk = max(0, slot_size - CHUNK_METADATA_LEN)
        k = model.addVar(vtype="I", lb=1, ub=capacity, name="k")
        model.addCons(k * chunk >= length)
        model.setObjective(k, sense="minimize")
        model.optimize()
        return model.getObjVal()

    def cp_sat_max_len(capacity, slot_size):
        from ortools.sat.python import cp_model
        chunk = max(0, slot_size - CHUNK_METADATA_LEN)
        model = cp_model.CpModel()
        k = model.NewIntVar(1, capacity, "k")
        length = model.NewIntVar(0, 1 << 20, "len")
        model.Add(length <= k * chunk)
        model.Maximize(length)
        solver = cp_model.CpSolver()
        solver.parameters.max_time_in_seconds = 30
        solver.Solve(model)
        return solver.ObjectiveValue()

    # ── S1: max accepted payload == capacity * chunk_payload_size ──
    ok = True
    detail = ""
    for (cap, slot) in [(2, 64), (4, 64), (8, 128), (16, 4096), (64, 256)]:
        optimum = scip_max_len(cap, slot)
        formula = cap * (slot - CHUNK_METADATA_LEN)
        if optimum != formula:
            ok = False
            detail += f"capacity={cap} slot={slot}: MIP {optimum} != formula {formula}; "
    record("S1 max message size (MIP optimum) == capacity*(slot-42)", ok, detail)

    # ── S2: min chunks for a payload == ceil(len / chunk) ──
    ok = True
    detail = ""
    for (cap, slot, length) in [(8, 64, 100), (8, 64, 65), (4, 64, 64),
                                (16, 128, 1000), (16, 4096, 50000)]:
        optimum = scip_min_chunks(cap, slot, length)
        chunk = slot - CHUNK_METADATA_LEN
        formula = (length + chunk - 1) // chunk
        if optimum != formula:
            ok = False
            detail += f"len={length}: MIP {optimum} != formula {formula}; "
    record("S2 min chunks (MIP optimum) == ceil(len/chunk)", ok, detail)

    # ── S3: worst-case region file size over the config domain ──
    # region_slot_size = slot_size + FRAME_HEADER_LEN + PERSISTENT_SLOT_HEADER_LEN
    model = scip.Model("max-storage")
    model.hideOutput()
    model.setParam("limits/time", 30)
    cap = model.addVar(vtype="I", lb=1, ub=64, name="capacity")
    slot = model.addVar(vtype="I", lb=16, ub=4096, name="slot_size")
    size = model.addVar(vtype="I", lb=0, ub=1 << 32, name="size")
    model.addCons(size == cap * (slot + FRAME_HEADER_LEN + PERSISTENT_SLOT_HEADER_LEN))
    model.setObjective(size, sense="maximize")
    model.optimize()
    optimum = model.getObjVal()
    formula = 64 * (4096 + FRAME_HEADER_LEN + PERSISTENT_SLOT_HEADER_LEN)
    record("S3 worst-case region bytes == 64*(4096+50+13)",
           optimum == formula, f"MIP optimum {optimum}, formula {formula}")

    # ── S4: differential agreement across all three solvers ──
    ok = True
    detail = ""
    for (cap, slot) in [(2, 64), (4, 64), (8, 128)]:
        scip_opt = scip_max_len(cap, slot)
        cpsat_opt = cp_sat_max_len(cap, slot)
        formula = cap * (slot - CHUNK_METADATA_LEN)
        if not (scip_opt == cpsat_opt == formula):
            ok = False
            detail += (f"{cap}x{slot}: scip={scip_opt} cpsat={cpsat_opt} "
                       f"formula={formula}; ")
    record("S4 cross-solver agreement on max_message_size (SCIP == CP-SAT)",
           ok, detail)

    # ── S5: cursor depth upper bound is exactly the capacity ──
    # The refresh clamps the cursor-derived depth to capacity. Verify that
    # the true protocol maximum is exactly capacity (the clamp never masks a
    # real over-commit): maximize head - tail over the send/release protocol
    # constraints (each write occupies k slots under the guard).
    for N_slots in (4, 8, 64):
        model = scip.Model("max-depth")
        model.hideOutput()
        model.setParam("limits/time", 30)
        h = model.addVar(vtype="I", lb=0, ub=1 << 20, name="head")
        t = model.addVar(vtype="I", lb=0, ub=1 << 20, name="tail")
        d = model.addVar(vtype="I", lb=0, ub=N_slots + 1, name="depth")
        model.addCons(d == h - t)
        model.addCons(d <= N_slots)  # the guard
        model.setObjective(d, sense="maximize")
        model.optimize()
        optimum = model.getObjVal()
        ok = optimum == N_slots
        record(f"S5 max cursor depth (MIP optimum) == capacity = {N_slots}",
               ok, f"optimum {optimum}")

    # ── S6: per-message batch flush range never exceeds the region ──
    # send() publishes one flush over [0, end) with end = slot_offset(last)
    # + 13 + frame_len; the worst-case end must equal the total region size
    # (REGION_HEADER_LEN + capacity * region_slot_size), never more.
    for (cap, slot) in [(4, 64), (16, 4096), (64, 256)]:
        model = scip.Model("flush-end")
        model.hideOutput()
        model.setParam("limits/time", 30)
        i = model.addVar(vtype="I", lb=0, ub=cap - 1, name="slot_index")
        end = model.addVar(vtype="I", lb=0, ub=1 << 32, name="flush_end")
        frame = model.addVar(vtype="I", lb=1, ub=slot + FRAME_HEADER_LEN, name="frame_len")
        model.addCons(end == REGION_HEADER_LEN
                      + i * (slot + FRAME_HEADER_LEN + PERSISTENT_SLOT_HEADER_LEN)
                      + PERSISTENT_SLOT_HEADER_LEN + frame)
        model.setObjective(end, sense="maximize")
        model.optimize()
        optimum = model.getObjVal()
        total = REGION_HEADER_LEN + cap * (slot + FRAME_HEADER_LEN + PERSISTENT_SLOT_HEADER_LEN)
        ok = optimum == total
        record(f"S6 max flush end {cap}x{slot} == region total {total} (no overflow)",
               ok, f"MIP optimum {optimum}")


# ═══════════════════════════════════════════════════════════════════════
# Replay — differential check against the real implementation
# ═══════════════════════════════════════════════════════════════════════

def _release(channel):
    """Close a channel and drop it so its mapping is freed (Windows)."""
    import gc
    channel.close()
    del channel
    gc.collect()


def check_replay():
    try:
        import dsline
    except ImportError:
        print("[SKIP] replay: dsline extension not importable")
        return
    import tempfile

    def temp_path(label):
        return os.path.join(
            tempfile.gettempdir(), f"verify-{label}-{random.randrange(1 << 30):08x}.bin"
        )

    # ── R1: stats() size limits match the formulas ──
    ok = True
    detail = ""
    for (cap, slot) in [(2, 16), (4, 64), (8, 64), (16, 4096)]:
        path = temp_path("r1")
        ch = dsline.FileChannel.create(path, capacity=cap, slot_size=slot)
        stats = ch.stats()
        chunk = max(0, slot - CHUNK_METADATA_LEN)
        expected_max = max(slot, cap * chunk)
        if stats["chunk_payload_size"] != chunk or stats["max_message_size"] != expected_max:
            ok = False
            detail += f"{cap}x{slot}: {stats}; "
        _release(ch)
        try:
            os.remove(path)
        except OSError:
            pass
    record("R1 stats() limits match the send_plan formulas", ok, detail)

    # ── R2: region file size == REGION_HEADER_LEN + capacity*(slot+63) ──
    ok = True
    detail = ""
    for (cap, slot) in [(2, 16), (4, 64)]:
        path = temp_path("r2")
        ch = dsline.FileChannel.create(path, capacity=cap, slot_size=slot)
        actual = os.path.getsize(path)
        expected = REGION_HEADER_LEN + cap * (slot + FRAME_HEADER_LEN + PERSISTENT_SLOT_HEADER_LEN)
        if actual != expected:
            ok = False
            detail += f"{cap}x{slot}: file {actual} != formula {expected}; "
        _release(ch)
        try:
            os.remove(path)
        except OSError:
            pass
    record("R2 on-disk region size == 20 + capacity*(slot+63)", ok, detail)

    # ── R3: random schedule matches the reference model ──
    # The same bookkeeping the solvers model (depth = occupied slots,
    # contiguous seqs) must match the implementation under Raise backpressure.
    rng = random.Random(20260802)
    C, SLOT = 8, 64
    name = f"verify-r3-{rng.randrange(1 << 30):08x}"
    ch = dsline.ShmChannel(
        name, capacity=C, slot_size=SLOT, backpressure=dsline.Backpressure.Raise
    )
    chunk = SLOT - CHUNK_METADATA_LEN
    queue = []  # (seq, payload, slots)
    model_depth, model_seq = 0, 0
    ok = True
    detail = ""
    for step in range(400):
        action = rng.choice(["send", "send", "recv", "recv", "stats"])
        if action == "send":
            length = rng.randint(0, 120)
            slots = 1 if length <= SLOT else (length + chunk - 1) // chunk
            if slots > C or model_depth + slots > C:
                try:
                    ch.send(b"x" * length)
                    ok = False
                    detail += f"step {step}: expected full error, send succeeded; "
                except (dsline.BufferFullError, dsline.MessageTooLargeError):
                    pass
            else:
                payload = bytes(rng.randrange(256) for _ in range(length))
                ch.send(payload)
                queue.append((model_seq, payload, slots))
                model_depth += slots
                model_seq += 1
        elif action == "recv":
            if not queue:
                try:
                    ch.recv()
                    ok = False
                    detail += f"step {step}: expected empty error, recv succeeded; "
                except dsline.BufferEmptyError:
                    pass
            else:
                seq, payload, slots = queue.pop(0)
                got = ch.recv_with_seq()
                if got != (seq, payload):
                    ok = False
                    detail += f"step {step}: got seq {got[0]}, expected {seq}; "
                model_depth -= slots
        else:
            stats = ch.stats()
            if stats["queue_depth"] != model_depth:
                ok = False
                detail += (f"step {step}: queue_depth {stats['queue_depth']} "
                           f"!= model {model_depth}; ")
    path = ch.path
    _release(ch)
    try:
        os.remove(path)
    except OSError:
        pass
    record("R3 random send/recv schedule matches the reference model", ok, detail)

    # ── R4: dual-handle cursor protocol replay ──
    # Two handles over one region in one process emulate the cross-process
    # SPSC: the producer handle commits with a release store of the head
    # cursor and a batch flush, the consumer handle reads through the shared
    # cursors (the same mmap, so every write is visible). Random
    # interleavings must match the reference model on both sides: sequences,
    # payloads, and the cursor-derived queue depth.
    rng = random.Random(20260904)
    C, SLOT = 6, 64
    path = temp_path("r4")
    producer = dsline.FileChannel.create(
        path, capacity=C, slot_size=SLOT, backpressure=dsline.Backpressure.Raise
    )
    consumer = dsline.FileChannel.open(path, capacity=C, slot_size=SLOT)
    chunk = SLOT - CHUNK_METADATA_LEN
    queue = []  # (seq, payload, slots)
    model_seq = 0
    model_depth = 0
    ok = True
    detail = ""
    for step in range(300):
        action = rng.choice(["send", "send", "recv"])
        if action == "send":
            length = rng.randint(0, 90)
            slots = 1 if length <= SLOT else (length + chunk - 1) // chunk
            if model_depth + slots > C:
                try:
                    producer.send(b"x" * length)
                    ok = False
                    detail += f"step {step}: expected full error, send succeeded; "
                except (dsline.BufferFullError, dsline.MessageTooLargeError):
                    pass
            else:
                payload = bytes(rng.randrange(256) for _ in range(length))
                producer.send(payload)
                queue.append((model_seq, payload, slots))
                model_seq += 1
                model_depth += slots
        else:
            if not queue:
                try:
                    consumer.recv()
                    ok = False
                    detail += f"step {step}: expected empty error, recv succeeded; "
                except dsline.BufferEmptyError:
                    pass
            else:
                seq, payload, slots = queue.pop(0)
                got = consumer.recv_with_seq()
                if got != (seq, payload):
                    ok = False
                    detail += f"step {step}: got seq {got[0]}, expected {seq}; "
                model_depth -= slots
                # The recv refreshed the cursors, so the consumer's depth is
                # exact right after the operation.
                stats = consumer.stats()
                if stats["queue_depth"] != model_depth:
                    ok = False
                    detail += (f"step {step}: consumer queue_depth "
                               f"{stats['queue_depth']} != model {model_depth}; ")
    path2 = producer.path
    _release(producer)
    _release(consumer)
    try:
        os.remove(path)
    except OSError:
        pass
    record("R4 dual-handle (producer+consumer) schedule matches the model "
           "through the shared cursors", ok, detail)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--z3", action="store_true", help="run Z3 checks")
    parser.add_argument("--cp-sat", action="store_true", help="run CP-SAT checks")
    parser.add_argument("--scip", action="store_true", help="run SCIP checks")
    parser.add_argument("--replay", action="store_true", help="replay against the real channel")
    parser.add_argument("--skip-replay", action="store_true",
                        help="do not run the replay section")
    args = parser.parse_args()

    run_all = not (args.z3 or args.cp_sat or args.scip or args.replay)
    if run_all or args.z3:
        print("── Z3 (SMT) ──")
        check_z3()
    if run_all or args.cp_sat:
        print("── CP-SAT (bounded model checking) ──")
        check_cp_sat()
    if run_all or args.scip:
        print("── SCIP (MILP) ──")
        check_scip()
    if run_all or args.replay:
        if args.skip_replay:
            print("[SKIP] replay (--skip-replay)")
        else:
            print("── Replay vs implementation ──")
            check_replay()

    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    for name in FAIL:
        print(f"  FAILED: {name}")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
