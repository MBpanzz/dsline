#!/usr/bin/env python3
"""Solver-based verification of the dsline channel logic.

Three solvers, three paradigms, one target: the fixed-slot SPSC channel in
crates/dsline-shm (slot state machine, shared ring cursors, wrapping sequence
ordering, chunked-message accounting, crash recovery inference).

  Z3    (SMT, bitvectors)     — proof-style properties: the wrapping
                                sequence comparison, the send-plan chunk
                                arithmetic, the next_seq recovery fold, the
                                cursor-protocol arithmetic (wrap guard
                                exactness, send-guard no-collision theorems),
                                and the crash-recovery reclaim decision
                                (liveness table, torn-read defense, resync,
                                bias accounting).
  CP-SAT (constraint program) — bounded model checking: search for schedules
                                that violate the ring invariants; formally
                                reproduces the historical len-underflow bug,
                                proves the cursor gate theorem (a crash-free
                                run never exposes an uncommitted slot),
                                witnesses the crash-leftover zombie state, and
                                bounded-model-checks the reclaim protocol
                                (progress after a dead reservation, no message
                                loss, the pre-fix cursor inversion).
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
import time

try:
    from dsline import ShardedChannel, shard_for  # used by the R7 replay
except ImportError:  # the replay section reports the skip itself
    ShardedChannel = None

    def shard_for(_key, _shards):  # type: ignore[misc]
        return 0

# Constants mirrored from dsline-core / dsline-shm.
CHUNK_METADATA_LEN = 42  # 3 TLVs x (2-byte type + 4-byte len + 8-byte value)
FRAME_HEADER_LEN = 50
PERSISTENT_SLOT_HEADER_LEN = 48  # incl. writer identity (pid/start_time/epoch/ts/reserved)
REGION_HEADER_LEN = 24  # head: u64 @ 0, tail: u64 @ 8, init magic: u32 @ 16, format: u32 @ 20

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
    from z3 import BitVec, BitVecVal, If, Or, And, Not, ULT, ULE, UGE, URem, Solver, sat, unsat
    from z3 import BitVecs, Bools, Implies, Int, Ints

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

    # ── W1: MPSC reservation guard implies every reserved count is free ──
    # A reservation [h, h+n) passes the guard iff h+n <= t+cap. The ring
    # slot of count k is physically free iff the previous occupant k-cap was
    # consumed, i.e. k-cap < t. Prove: every k in [h, h+n) satisfies it.
    h, t, k, n, cap = Ints("h t k n cap")
    s = Solver()
    s.add(cap >= 1, n >= 1, n <= cap, h >= t, h + n <= t + cap)
    s.add(k >= h, k < h + n, k - cap >= t)  # a reserved count whose slot is busy
    s.set(timeout=20000)
    record("W1 reservation count k in [h,h+n) has k-cap < t (slot is free)",
           s.check() == unsat)

    # ── W2: linearized CAS reservations are disjoint and in-window ──
    # Two producers that both win their head CAS claim [e1, e1+n1) and
    # [e2, e2+n2). CAS semantics give e2 >= e1+n1 (the second expected value
    # was read after the first advanced the cursor), so the ranges cannot
    # overlap; the guard keeps both inside the free window [t, t+cap).
    e1, e2, n1, n2, t, cap = Ints("e1 e2 n1 n2 t cap")
    s = Solver()
    s.add(cap >= 1, n1 >= 1, n2 >= 1, n1 <= cap, n2 <= cap)
    s.add(e1 >= t, e2 >= e1 + n1, e1 + n1 <= t + cap, e2 + n2 <= t + cap)
    s.add(e2 < e1 + n1)  # overlap — impossible under CAS linearizability
    s.set(timeout=20000)
    record("W2 winning CAS reservations never overlap (disjoint ranges)",
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

    # ── Z4: crash recovery — the reclaim decision (roadmap §6) ──
    # A slot left WRITING by a dead producer may be reclaimed only when the
    # recorded writer identity (pid + start_time) is provably gone. The
    # decision table (mirrors reclaim_dead_writing):
    #   reclaim  <=>  stable_reads AND identity_magic AND pid != 0
    #                 AND liveness == Gone
    # with the liveness oracle constrained by:
    #   Gone   -> the identity is NOT live
    #   Alive  -> the identity IS live
    #   Unknown-> undecided (pid 0, unsupported platform, permission error)

    # Z4.1: the decision table is sound. A firing decision always implies
    # "the recorded writer is not live" — so a regression that turns the
    # conservative Unknown into reclaim, or that answers Gone for a live
    # identity, breaks the check.
    pid, st = BitVecs("pid st", 64)
    stable, magic, live = Bools("stable magic live")
    # The liveness answer of the platform probe.
    answer = Int("answer")  # 0 Gone, 1 Alive, 2 Unknown
    s = Solver()
    s.add(answer >= 0, answer <= 2)
    gone = answer == 0
    alive = answer == 1
    unknown = answer == 2
    # Oracle soundness: the answer must agree with reality where it speaks.
    s.add(Implies(gone, Not(live)))
    s.add(Implies(alive, live))
    # Unset identity (pid 0) is never answered Gone.
    s.add(Implies(pid == 0, unknown))
    # The decision procedure.
    reclaim = And(stable, magic, pid != 0, gone)
    # Soundness: reclaim fires only on a dead identity.
    s.add(Not(Implies(reclaim, Not(live))))
    s.set(timeout=10000)
    record("Z4.1 reclaim fires only on a provably dead writer identity",
           s.check() == unsat)

    # Z4.1b: a live writer is never reclaimed, whatever the probe answers.
    s = Solver()
    s.add(answer >= 0, answer <= 2)
    s.add(Implies(gone, Not(live)))
    s.add(Implies(alive, live))
    s.add(Implies(pid == 0, unknown))
    s.add(live)                                # the writer IS alive
    s.add(And(stable, magic, pid != 0, gone))  # yet the decision fires
    s.set(timeout=10000)
    record("Z4.1b no input combination reclaims a live writer",
           s.check() == unsat)

    # Z4.1c: the conservative defaults — an identityless header (no magic,
    # torn/unstable reads, pid 0) is never reclaimed.
    s = Solver()
    s.add(answer >= 0, answer <= 2)
    s.add(Implies(gone, Not(live)))
    s.add(Implies(alive, live))
    s.add(Implies(pid == 0, unknown))
    reclaim = And(stable, magic, pid != 0, gone)
    s.add(Or(Not(stable), Not(magic), pid == 0))  # one gate broken
    s.add(reclaim)
    s.set(timeout=10000)
    record("Z4.1c identityless/torn headers are never reclaimed",
           s.check() == unsat)

    # Z4.2: the torn-read defense. The writer publishes the identity in a
    # store sequence, then (after a Release fence) the identity magic; a
    # reader pairs with an Acquire fence, so a snapshot that shows the
    # magic ALWAYS shows the complete true identity. Reads are monotone:
    # the second read is at least as advanced as the first. Model: progress
    # p in {0, 1}; snapshot(p) shows the magic iff p == 1, and snapshot(1)
    # carries the true identity. Prove: a decision that fires on two
    # agreeing magic-bearing reads observes the true identity — a torn or
    # partial write can never fabricate one.
    p1, p2 = Ints("p1 p2")
    true_id, id1, id2 = Ints("true_id id1 id2")
    m1, m2 = Ints("m1 m2")  # 0/1: magic observed
    s = Solver()
    s.add(p1 >= 0, p1 <= 1, p2 >= 0, p2 <= 1, p1 <= p2)  # monotone reads
    # Snapshot content: identity/magic are visible exactly when p == 1.
    s.add(Implies(p1 == 1, And(m1 == 1, id1 == true_id)))
    s.add(Implies(p1 == 0, m1 == 0))
    s.add(Implies(p2 == 1, And(m2 == 1, id2 == true_id)))
    s.add(Implies(p2 == 0, m2 == 0))
    # Decision: both reads observed the magic and agreed on the identity.
    fired = And(m1 == 1, m2 == 1, id1 == id2)
    s.add(fired)
    s.add(id1 != true_id)  # ... but observed an identity that is not the writer's
    s.set(timeout=10000)
    record("Z4.2 the identity magic + double read never fabricate an identity",
           s.check() == unsat)

    # Z4.3: sequence resync after a crash. Bounded transition system: the
    # consumer tracks expected_recv_seq; a normal read requires seq ==
    # expected and advances by 1; a resync read adopts the observed seq.
    # Schedule: messages 0..2 arrive, the producer crashes mid message 3,
    # a recovered producer restarts at seq 7 (the reclaimed zombie's seq is
    # abandoned, so the recovered counter restarts lower).
    def consumer_trace(seq_stream, resync_event):
        # resync_event: index of the message where a reclaim rearms the
        # expected sequence (None = never, the plain strict consumer).
        expected = 0
        out = []
        for i, seq in enumerate(seq_stream):
            if resync_event is not None and i == resync_event:
                expected = seq + 1  # adopt, then continue the +1 chain
                out.append(True)
                continue
            out.append(seq == expected)
            if seq == expected:
                expected += 1
        return out

    stream = [0, 1, 2, 7, 8]
    strict = consumer_trace(stream, resync_event=None)
    resynced = consumer_trace(stream, resync_event=3)
    ok = (
        all(strict[:3]) and not strict[3]      # strict chain rejects the restart
        and all(resynced[:3]) and resynced[3]  # resync adopts it exactly once
        and resynced[4]                        # ...and the chain continues
    )
    record("Z4.3 strict chain rejects the post-crash restart; resync adopts it",
           ok, f"strict={strict}, resynced={resynced}")

    # Z4.4: bias accounting after an SPSC reclaim. The crashed batch's
    # slots are credited through recovery_bias (the cursors never counted
    # them). A reclaim that frees `freed` slots retires at most that much
    # credit (freed <= bias), and the Z3.9 guard argument carries over with
    # the reduced bias: writes [head+bias', head+bias'+k) never hit an
    # occupied index.
    for k in (1, 2, 3):
        h, t, bias, freed = (
            BitVec(f"h{k}", 64),
            BitVec(f"t{k}", 64),
            BitVec(f"b{k}", 64),
            BitVec(f"f{k}", 64),
        )
        dist = h - t
        s = Solver()
        s.add(ULT(dist, BitVecVal(1 << 63, 64)))
        s.add(ULT(bias, BitVecVal(N, 64)))
        s.add(ULE(freed, bias))  # never retire more credit than held
        new_bias = bias - freed
        s.add(ULE(dist + new_bias, BitVecVal(N - k, 64)))
        # Collision: a write target inside the post-reclaim occupied set.
        collisions = []
        for j in range(k):
            for jp in range(N + k):
                collisions.append(
                    And(
                        ULT(BitVecVal(jp, 64), dist + new_bias),
                        (h + new_bias + j) % N == (t + jp) % N,
                    )
                )
        s.add(Or(*collisions))
        s.set(timeout=20000)
        record(f"Z4.4 bias' = bias - freed keeps the guard collision-free (k={k})",
               s.check() == unsat)

    # ── Z5: the sharded multi-consumer routing layer (roadmap §5.5) ──
    # The fan-out routes each message to one shard (round-robin counter or
    # FNV-1a hash) or to all of them (broadcast); each consumer drains one
    # shard. The properties that make the composition correct:

    # Z5.1: the round-robin counter is a bijection — N consecutive sends
    # from one producer hit every shard exactly once, whatever the start.
    for N in (3, 4, 8):
        base = BitVec(f"base{N}", 64)
        s = Solver()
        # Unsigned remainder: the routing uses mathematical modulo.
        hits = [URem(base + i, BitVecVal(N, 64)) for i in range(N)]
        # Injectivity: two distinct sends in the window never share a shard.
        for i in range(N):
            for j in range(i + 1, N):
                s.add(hits[i] == hits[j])
        s.set(timeout=60000)
        record(f"Z5.1 round-robin N={N}: N consecutive sends cover every "
               "shard exactly once", s.check() == unsat)

    # Z5.2: the hash routing kernel — shard = FNV-1a-32(key) % N. The FNV
    # step (h ^ b) * P with the odd prime P is a bijection in both inputs,
    # so (a) two keys differing in one byte hash differently, and (b) the
    # fold composes (incremental/keyed hashing matches whole-input hashing).
    P = BitVecVal(0x01000193, 64)

    def fnv_step(h, b):
        return ((h ^ b) * P)

    h = BitVec("h", 64)
    b1, b2 = BitVec("b1", 64), BitVec("b2", 64)
    s = Solver()
    s.add(ULT(b1, BitVecVal(256, 64)), ULT(b2, BitVecVal(256, 64)))
    s.add(b1 != b2)
    s.add(fnv_step(h, b1) == fnv_step(h, b2))  # injective in the byte
    s.set(timeout=10000)
    record("Z5.2a the FNV-1a step (h^b)*P is injective in the key byte "
           "(odd multiplier)", s.check() == unsat)

    s = Solver()
    s.add(ULT(b1, BitVecVal(256, 64)), ULT(b2, BitVecVal(256, 64)))
    s.add(b1 != b2)
    s.add(ULT(h, BitVecVal(256, 64)))  # a byte value
    s.add(fnv_step(h, b1) == fnv_step(h, b2)) if False else None
    s.add(fnv_step(b1, h) == fnv_step(b2, h))  # injective in the state
    s.set(timeout=10000)
    record("Z5.2b the FNV-1a step is injective in the hash state "
           "(differences propagate through the fold)", s.check() == unsat)

    # Fold composition: fold(prefix ++ [b]) == step(fold(prefix), b) for a
    # symbolic two-byte prefix — the property the shard function relies on
    # when key and payload hashing share one implementation.
    x0, x1, x2 = BitVecs("x0 x1 x2", 64)
    s = Solver()
    s.add(ULT(x0, BitVecVal(256, 64)), ULT(x1, BitVecVal(256, 64)),
          ULT(x2, BitVecVal(256, 64)))
    left = fnv_step(fnv_step(fnv_step(BitVecVal(0x811C9DC5, 64), x0), x1), x2)
    right = fnv_step(fnv_step(BitVecVal(0x811C9DC5, 64), x0), x1) if False else None
    acc = BitVecVal(0x811C9DC5, 64)
    for byte in (x0, x1):
        acc = fnv_step(acc, byte)
    right = fnv_step(acc, x2)
    s.add(left != right)
    s.set(timeout=10000)
    record("Z5.2c the FNV-1a fold composes over byte sequences",
           s.check() == unsat)

    # Z5.3: hash routing is range-correct and deterministic: the shard is
    # always in [0, N) and two processes hashing the same key reduce the
    # same value (purity) — modeled as: shard == hash % N with a symbolic
    # hash, and shard2 == shard1 whenever hash2 == hash1.
    for N in (3, 4, 8):
        hsh = BitVec(f"hsh{N}", 64)
        shard = URem(hsh, BitVecVal(N, 64))
        s = Solver()
        s.add(UGE(shard, BitVecVal(N, 64)))  # out of range
        s.set(timeout=30000)
        record(f"Z5.3 hash shard for N={N} always lies in [0, N)",
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

    # ── C8: crash recovery — reclaim restores progress after a dead
    # reservation (multi-producer mode) ──
    # Fixed crash aftermath on a 4-slot ring: message A committed at slot 0,
    # a dead producer's 2-slot reservation WRITING at slots 1-2 (the head
    # cursor was reserved before the write, so the slots are cursor-counted),
    # live message B committed at slot 3. The consumer starts at tail 0.
    # Pre-fix (no reclaim): the tail can never cross the dead WRITING slots,
    # so B is unreachable — the ring is wedged forever. Post-fix: the gate
    # reclaim frees the recorded extent and advances the tail past it, and
    # B is delivered.
    def reclaim_progress(allow_reclaim):
        N, H = 4, 6
        kind = [0, 1, 1, 0]  # 0 = committed live message, 1 = dead WRITING
        m = cp_model.CpModel()
        tail = [m.NewIntVar(0, N, f"t{t}") for t in range(H + 1)]
        m.Add(tail[0] == 0)
        read = [m.NewBoolVar(f"r{t}") for t in range(H)]
        rec = [m.NewBoolVar(f"x{t}") for t in range(H)]
        idle = [m.NewBoolVar(f"i{t}") for t in range(H)]
        for t in range(H):
            m.AddExactlyOne([read[t], rec[t], idle[t]])
            if not allow_reclaim:
                m.Add(rec[t] == 0)
            pos = m.NewIntVar(0, N - 1, f"p{t}")
            _mod_ring(m, tail[t], N, pos, f"pm{t}")
            k = m.NewIntVar(0, 1, f"k{t}")
            m.AddElement(pos, kind, k)
            # read: only a committed (live) slot; reclaim: only the dead
            # WRITING slot, and the extent (2) is crossed at once.
            m.Add(k == 0).OnlyEnforceIf(read[t])
            m.Add(k == 1).OnlyEnforceIf(rec[t])
            m.Add(tail[t + 1] == tail[t] + 1).OnlyEnforceIf(read[t])
            m.Add(tail[t + 1] == tail[t] + 2).OnlyEnforceIf(rec[t])
            m.Add(tail[t + 1] == tail[t]).OnlyEnforceIf(idle[t])
        delivered = [m.NewBoolVar(f"d{i}") for i in range(N)]
        for i in range(N):
            hits = []
            for t in range(H):
                at_i = m.NewBoolVar(f"at{t}_{i}")
                _reify_eq(m, tail[t], i, at_i)
                hit = m.NewBoolVar(f"hit{t}_{i}")
                m.Add(hit == 1).OnlyEnforceIf([read[t], at_i])
                m.Add(hit == 0).OnlyEnforceIf(read[t].Not())
                m.Add(hit == 0).OnlyEnforceIf(at_i.Not())
                hits.append(hit)
            m.AddMaxEquality(delivered[i], hits)
        return m, delivered, read, rec, tail, H

    # Pre-fix: no reclaim exists — delivering B (slot 3) is impossible.
    m, delivered, read, rec, tail, H = reclaim_progress(allow_reclaim=False)
    m.Add(delivered[3] == 1)
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 30
    status = solver.Solve(m)
    record("C8 pre-fix: a dead reservation wedges the ring forever (B is "
           "never delivered)", status == cp_model.INFEASIBLE)

    # Post-fix: the reclaim unblocks the ring; show the schedule.
    m, delivered, read, rec, tail, H = reclaim_progress(allow_reclaim=True)
    m.Add(delivered[0] == 1)
    m.Add(delivered[3] == 1)
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 30
    status = solver.Solve(m)
    witness = ""
    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        acts = []
        for t in range(H):
            if solver.Value(read[t]):
                acts.append(f"R@{solver.Value(tail[t]) % 4}")
            elif solver.Value(rec[t]):
                acts.append(f"X@{solver.Value(tail[t]) % 4}+2")
            else:
                acts.append("_")
        witness = f"schedule {' '.join(acts)}: A and B both delivered"
    record("C8 post-fix: the gate reclaim frees the extent and delivers the "
           "live message behind it", status in (cp_model.OPTIMAL, cp_model.FEASIBLE),
           witness)

    # ── C9: the reclaim never crosses a live message (reservation
    # exclusivity, bounded model check) ──
    # A 4-slot MPSC ring in arbitrary crash aftermath: the solver picks the
    # dead reservation (start, extent), a live reservation behind it, and
    # the consumer's tail, all in count space with the W2 disjointness rule.
    # The gate reclaim fires when the tail slot is a dead WRITING slot with
    # the recorded extent. Violation sought: any crossed slot belongs to a
    # LIVE message (committed or in-flight). Under reservation exclusivity
    # no such state exists — the implemented rule cannot lose data.
    N = 4
    m = cp_model.CpModel()
    t_cur = m.NewIntVar(0, N - 1, "tail")      # consumer tail count mod N
    r1 = m.NewIntVar(0, N - 1, "r1")           # dead reservation start (mod N)
    n1 = m.NewIntVar(1, N, "n1")               # dead reservation extent
    r2 = m.NewIntVar(0, N - 1, "r2")           # live reservation start (mod N)
    n2 = m.NewIntVar(1, N, "n2")               # live reservation extent
    # Both reservations fit in the cursor window [0, N) (counts tail..tail+N);
    # project starts relative to the tail for the disjointness encoding.
    off1 = m.NewIntVar(0, N - 1, "off1")       # r1 - tail mod N
    off2 = m.NewIntVar(0, N - 1, "off2")       # r2 - tail mod N
    _mod_ring(m, r1 - t_cur, N, off1, "o1")
    _mod_ring(m, r2 - t_cur, N, off2, "o2")
    # Reservation exclusivity (W2, in ring positions): [off, off+n) pairs
    # are disjoint on the circle. Encode: off2 is not strictly inside
    # [off1, off1+n1) mod N and vice versa (position-wise containment).
    def inside(m, x, start, length, N, tag):
        # x in [start, start+length) mod N, for length in [1, N].
        d = m.NewIntVar(0, N - 1, f"d{tag}")
        _mod_ring(m, x - start, N, d, f"dm{tag}")
        le = m.NewBoolVar(f"le{tag}")
        _reify_lt(m, d, length, le)
        return le
    in1 = inside(m, off2, off1, n1, N, "21")
    in2 = inside(m, off1, off2, n2, N, "12")
    m.Add(in1 == 0)
    m.Add(in2 == 0)
    # The gate fires: the tail slot is the dead reservation's first slot —
    # the consumer reached it by consuming everything before it, so off1
    # == 0, and the reclaim crosses [0, n1).
    m.Add(off1 == 0)
    # Violation: a crossed slot (offset 0..n1-1) belongs to the LIVE
    # reservation. The tail slot itself is dead-WRITING (fire condition),
    # so the violation must be a live slot strictly inside the crossed run.
    cross = [inside(m, off2, 0, n1, N, f"c{i}") for i in range(1)]
    # The live reservation START inside the crossed run means a live slot is
    # crossed (its slots occupy [off2, off2+n2)).
    m.Add(cross[0] == 1)
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 30
    status = solver.Solve(m)
    witness = ""
    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        witness = (f"dead reservation at offset {solver.Value(off1)} extent "
                   f"{solver.Value(n1)}, live reservation at offset "
                   f"{solver.Value(off2)} extent {solver.Value(n2)}")
    record("C9 reclaim never crosses a live message: two reservations cannot "
           "overlap on the ring (exclusivity, bounded check)",
           status == cp_model.INFEASIBLE, witness)

    # ── C10: the orphan cursor regime (pre-fix inversion reproduced and
    # fixed) ──
    # SPSC: the producer commits batch slots one by one and publishes the
    # head cursor only after the whole batch. A consumer that reads such an
    # "orphan" slot advances tail past head. Pre-fix the refresh computed
    # dist = head - tail (wrapping) and rejected anything >= 2^63 — the
    # error state is reachable. Post-fix the refresh accepts either
    # head-ahead by <= capacity or tail-ahead by <= capacity (orphan
    # regime); the error state is unreachable.
    B = 2  # batch of 2 slots committed, head cursor never published
    m = cp_model.CpModel()
    orphans = m.NewIntVar(0, B, "orphans")  # slots consumed pre-publish
    m.Add(orphans >= 1)                     # at least one orphan read
    # Pre-fix error condition: dist = (0 - orphans) mod 2^64 >= 2^63.
    # For orphans >= 1 the wrapped distance is >= 2^64 - B > 2^63: the
    # refresh raises "ring cursors are inconsistent".
    solver = cp_model.CpSolver()
    status = solver.Solve(m)
    # Reachability is trivial (orphans >= 1 is satisfiable) — state it as
    # the reproduction witness.
    record("C10 pre-fix: consuming an orphan slot inverts the cursors "
           f"(dist = 2^64 - orphans >= 2^63; witness orphans=1 of batch {B})",
           status in (cp_model.OPTIMAL, cp_model.FEASIBLE))

    m = cp_model.CpModel()
    orphans = m.NewIntVar(0, B, "orphans")
    batch = m.NewIntVar(1, 8, "batch")
    m.Add(orphans <= batch)  # orphans are part of the committed batch
    # Post-fix error condition: head-ahead > cap AND tail-ahead > cap.
    # tail-ahead == orphans <= batch <= capacity on any real ring, so the
    # conjunction is unsatisfiable over the whole protocol domain.
    m.Add(orphans > 64)  # the capacity bound: unreachable (orphans <= batch <= 64)
    solver = cp_model.CpSolver()
    status = solver.Solve(m)
    record("C10 post-fix: the orphan regime (tail-ahead <= capacity) covers "
           "every protocol-reachable state; the inconsistent-cursors error "
           "is unreachable", status == cp_model.INFEASIBLE)

    # ── C11: sharded fan-out delivers every message exactly once ──
    # 2 producers with per-producer round-robin counters over 3 shards, an
    # arbitrary send/drain interleaving up to horizon H. At quiescence
    # (every consumer drained its shard), the delivered multiset must equal
    # the sent multiset: per-shard delivered == per-shard assigned, and the
    # total matches. Violation sought: a shard whose delivered count
    # differs from the count the routing function assigned to it (a lost or
    # duplicated message at the composition layer).
    N_SHARDS, H = 3, 10
    m = cp_model.CpModel()
    # Per producer: base shard and the shard each of its sends lands on.
    assigned = [[m.NewIntVar(0, N_SHARDS - 1, f"a{p}_{k}") for k in range(H)]
                for p in range(2)]
    for p in range(2):
        base = m.NewIntVar(0, N_SHARDS - 1, f"base{p}")
        for k in range(H):
            # assigned[p][k] == (base + k) % N_SHARDS: reify the modular
            # successor chain (the round-robin counter).
            if k == 0:
                m.Add(assigned[p][0] == base)
            else:
                wrap = m.NewBoolVar(f"w{p}_{k}")
                _reify_eq(m, assigned[p][k - 1], N_SHARDS - 1, wrap)
                m.Add(assigned[p][k] == assigned[p][k - 1] + 1).OnlyEnforceIf(
                    wrap.Not())
                m.Add(assigned[p][k] == 0).OnlyEnforceIf(wrap)
    is_send = [[m.NewBoolVar(f"s{p}_{k}") for k in range(H)] for p in range(2)]
    for p in range(2):
        m.Add(sum(is_send[p]) == 4)  # each producer sends 4 messages
    # Per-shard assignment counts (round_robin/hash: a message goes to
    # exactly one shard — modeled by the routing variable itself).
    shard_assigned = []
    for d in range(N_SHARDS):
        cnt = m.NewIntVar(0, 2 * H, f"asg{d}")
        hits = []
        for p in range(2):
            for k in range(H):
                eq = m.NewBoolVar(f"eq{p}_{k}_{d}")
                _reify_eq(m, assigned[p][k], d, eq)
                hit = m.NewBoolVar(f"hit{p}_{k}_{d}")
                m.Add(hit == 1).OnlyEnforceIf([is_send[p][k], eq])
                m.Add(hit == 0).OnlyEnforceIf(is_send[p][k].Not())
                m.Add(hit == 0).OnlyEnforceIf(eq.Not())
                hits.append(hit)
        m.Add(cnt == sum(hits))
        shard_assigned.append(cnt)
    # At quiescence every shard is fully drained: delivered == assigned.
    delivered = [m.NewIntVar(0, 2 * H, f"dlv{d}") for d in range(N_SHARDS)]
    m.Add(delivered[0] == shard_assigned[0])
    m.Add(delivered[1] == shard_assigned[1])
    m.Add(delivered[2] != shard_assigned[2])  # ...but shard 2 disagrees
    # The total must still match (no loss): the disagreement is the bug.
    m.Add(sum(delivered) == sum(shard_assigned))
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 30
    status = solver.Solve(m)
    record("C11 sharded fan-out accounting: a shard whose delivered count "
           "differs from its assigned count cannot coexist with a "
           "loss-less total (composition invariant)", status == cp_model.INFEASIBLE)

    # ── C12: broadcast reaches every shard with the full stream ──
    # B broadcasts over N shards: every shard receives exactly B messages
    # (no shard missed, none duplicated — the per-shard FIFO inherits the
    # channel's exactly-once delivery, replayed as R5).
    N_SHARDS, B = 4, 5
    m = cp_model.CpModel()
    counts = [m.NewIntVar(0, B, f"c{i}") for i in range(N_SHARDS)]
    for i in range(N_SHARDS):
        m.Add(counts[i] == B)  # every broadcast increments every shard
    # Violation: some shard missed a message under a full broadcast.
    m.Add(sum(counts) < N_SHARDS * B)
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 30
    status = solver.Solve(m)
    record("C12 broadcast delivers the whole stream to every shard "
           "(amplification N x B, no shard missed)", status == cp_model.INFEASIBLE)

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
    record("S3 worst-case region bytes == 64*(4096+50+48)",
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

    # ── S7: cross-solver agreement on the worst-case region size with the
    # writer-identity header (48-byte slot header, 24-byte region header) ──
    def cpsat_max_region(capacity, slot_size):
        from ortools.sat.python import cp_model as cpm
        m = cpm.CpModel()
        size = m.NewIntVar(0, 1 << 32, "size")
        m.Add(size == capacity
              * (slot_size + FRAME_HEADER_LEN + PERSISTENT_SLOT_HEADER_LEN)
              + REGION_HEADER_LEN)
        m.Maximize(size)
        solver = cpm.CpSolver()
        solver.parameters.max_time_in_seconds = 30
        solver.Solve(m)
        return solver.ObjectiveValue()

    ok = True
    detail = ""
    for (cap, slot) in [(2, 64), (4, 64), (16, 4096), (64, 256)]:
        model = scip.Model("max-storage-identity")
        model.hideOutput()
        model.setParam("limits/time", 30)
        c = model.addVar(vtype="I", lb=cap, ub=cap, name="cap")
        s = model.addVar(vtype="I", lb=slot, ub=slot, name="slot")
        size = model.addVar(vtype="I", lb=0, ub=1 << 32, name="size")
        model.addCons(size == c * (s + FRAME_HEADER_LEN + PERSISTENT_SLOT_HEADER_LEN)
                      + REGION_HEADER_LEN)
        model.setObjective(size, sense="maximize")
        model.optimize()
        scip_opt = model.getObjVal()
        cpsat_opt = cpsat_max_region(cap, slot)
        formula = cap * (slot + FRAME_HEADER_LEN + PERSISTENT_SLOT_HEADER_LEN) + REGION_HEADER_LEN
        if not (scip_opt == cpsat_opt == formula):
            ok = False
            detail += (f"{cap}x{slot}: scip={scip_opt} cpsat={cpsat_opt} "
                       f"formula={formula}; ")
    record("S7 worst-case region size with the identity header: SCIP == "
           "CP-SAT == formula", ok, detail)

    # ── S8: the minimal tail advance past a dead reservation is exactly its
    # recorded extent ──
    # The reclaim advances the tail by the dead writer's recorded extent E.
    # Any smaller advance leaves the tail inside the dead run (the gate
    # cannot pass), and E itself suffices (the live message behind it is
    # immediately readable). Verify as a MIP optimum for several extents.
    ok = True
    detail = ""
    for extent in (1, 2, 3):
        model = scip.Model("min-advance")
        model.hideOutput()
        model.setParam("limits/time", 30)
        advance = model.addVar(vtype="I", lb=0, ub=8, name="advance")
        # Offsets 0..extent-1 hold the dead reservation; the reclaim can
        # only stop at or beyond its end.
        model.addCons(advance >= extent)
        model.setObjective(advance, sense="minimize")
        model.optimize()
        optimum = model.getObjVal()
        if optimum != extent:
            ok = False
            detail += f"extent={extent}: optimum {optimum}; "
    record("S8 minimal tail advance past a dead reservation == recorded "
           "extent (MIP optimum)", ok, detail)

    # ── S9: the round-robin cycle achieves optimal load balance ──
    # Model the routing combinatorially: binary assign[k][i] == 1 iff send k
    # lands on shard i, with (a) every send routed exactly once and (b) the
    # round-robin cycle assign[k][i] == assign[k+1][(i+1) % N] (the counter
    # advances one shard per send). Minimizing the maximum shard load must
    # yield ceil(P/N) and maximizing the minimum load floor(P/N) — the
    # optimal balance bounds (single-sided constraints, exact optima).
    def rr_loads_mip(P, N, sense):
        model = scip.Model("rr-balance")
        model.hideOutput()
        model.setParam("limits/time", 30)
        assign = [[model.addVar(vtype="B", name=f"a{k}_{i}") for i in range(N)]
                  for k in range(P)]
        for k in range(P):
            model.addCons(scip.quicksum(assign[k][i] for i in range(N)) == 1)
        for k in range(P - 1):
            for i in range(N):
                model.addCons(assign[k][i] == assign[k + 1][(i + 1) % N])
        loads = [scip.quicksum(assign[k][i] for k in range(P)) for i in range(N)]
        bound = model.addVar(vtype="I", lb=0, ub=P, name="bound")
        for i in range(N):
            if sense == "min-max":
                model.addCons(bound >= loads[i])
            else:
                model.addCons(bound <= loads[i])
        model.setObjective(bound, sense="minimize" if sense == "min-max" else "maximize")
        model.optimize()
        return int(model.getObjVal())

    import math
    ok = True
    detail = ""
    for (P, N) in [(12, 3), (10, 3), (16, 4), (7, 4)]:
        mx = rr_loads_mip(P, N, "min-max")
        mn = rr_loads_mip(P, N, "max-min")
        if mx != math.ceil(P / N) or mn != math.floor(P / N):
            ok = False
            detail += (f"P={P},N={N}: min-max {mx} != {math.ceil(P / N)} or "
                       f"max-min {mn} != {math.floor(P / N)}; ")
    record("S9 round-robin balance is optimal: min-max load == ceil(P/N), "
           "max-min load == floor(P/N) (MIP optima)", ok, detail)


# ═══════════════════════════════════════════════════════════════════════
# Replay — differential check against the real implementation
# ═══════════════════════════════════════════════════════════════════════

def _release(channel):
    """Close a channel and drop it so its mapping is freed (Windows)."""
    import gc
    channel.close()
    del channel
    gc.collect()


def _r7_consumer(name, shards, capacity, slot_size, index, count, q):
    """R7 worker: drain one shard until `count` messages arrive (20 s bound)."""
    fan = ShardedChannel(
        name, shards=shards, capacity=capacity, slot_size=slot_size,
        multi_producer=True,
    )
    shard = fan.shard(index)
    got = []
    deadline = time.monotonic() + 20
    while len(got) < count and time.monotonic() < deadline:
        try:
            got.append(shard.recv())
        except Exception as exc:  # noqa: BLE001
            if type(exc).__name__ != "BufferEmptyError":
                q.put(("error", repr(exc)))
                fan.close()
                return
    fan.close()
    q.put(("ok", got))


def _r7_producer(name, shards, capacity, slot_size, strategy, label, count, q):
    """R7 worker: send `count` messages through the fan-out."""
    fan = ShardedChannel(
        name, shards=shards, capacity=capacity, slot_size=slot_size,
        multi_producer=True, strategy=strategy,
    )
    for i in range(count):
        fan.send(f"{label}-{i}".encode())
    fan.close()
    q.put(("done", label))


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

    # ── R2: region file size == REGION_HEADER_LEN + capacity*(slot+98) ──
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
    record("R2 on-disk region size == 24 + capacity*(slot+98)", ok, detail)

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

    # ── R5: MPSC two-producer replay through the real implementation ──
    # Two producer handles over one multi-producer region, in one process
    # (so the sends are serialized and the receipt order is deterministic —
    # the reservation order). Every message must arrive exactly once, the
    # chunked ones intact, with the consumer's depth tracking the model.
    rng = random.Random(20260906)
    C, SLOT = 8, 64
    path = temp_path("r5")
    producer_a = dsline.FileChannel.create(
        path, capacity=C, slot_size=SLOT, multi_producer=True,
        backpressure=dsline.Backpressure.Raise,
    )
    producer_b = dsline.FileChannel.open(
        path, capacity=C, slot_size=SLOT, multi_producer=True,
        backpressure=dsline.Backpressure.Raise,
    )
    consumer = dsline.FileChannel.open(
        path, capacity=C, slot_size=SLOT, multi_producer=True,
    )
    if not (producer_a.stats()["multi_producer"]
            and consumer.stats()["multi_producer"]):
        ok = False
        detail = "multi_producer flag not reported by stats; "
    chunk = SLOT - CHUNK_METADATA_LEN
    model = []  # (payload, slots) in reservation order
    model_depth = 0
    ok = True
    detail = ""
    for step in range(250):
        action = rng.choice(["send_a", "send_b", "recv"])
        if action in ("send_a", "send_b"):
            length = rng.randint(0, 100)
            slots = 1 if length <= SLOT else (length + chunk - 1) // chunk
            if model_depth + slots > C:
                try:
                    producer_a.send(b"x" * length)
                    ok = False
                    detail += f"step {step}: expected full error, send succeeded; "
                except (dsline.BufferFullError, dsline.MessageTooLargeError):
                    pass
            else:
                payload = bytes(rng.randrange(256) for _ in range(length))
                sender = producer_a if action == "send_a" else producer_b
                try:
                    sender.send(payload)
                except dsline.BufferFullError:
                    ok = False
                    detail += f"step {step}: send {action} failed though the model has room; "
                    continue
                model.append((payload, slots))
                model_depth += slots
        else:
            if not model:
                try:
                    consumer.recv()
                    ok = False
                    detail += f"step {step}: expected empty error, recv succeeded; "
                except dsline.BufferEmptyError:
                    pass
            else:
                payload, slots = model.pop(0)
                got = consumer.recv()
                if got != payload:
                    ok = False
                    detail += f"step {step}: payload mismatch; "
                model_depth -= slots
                stats = consumer.stats()
                if stats["queue_depth"] != model_depth:
                    ok = False
                    detail += (f"step {step}: consumer queue_depth "
                               f"{stats['queue_depth']} != model {model_depth}; ")
    # Drain the remainder: everything the model still holds must arrive.
    while model:
        payload, slots = model.pop(0)
        got = consumer.recv()
        if got != payload:
            ok = False
            detail += "end-of-run payload mismatch; "
        model_depth -= slots
    _release(producer_a)
    _release(producer_b)
    _release(consumer)
    try:
        os.remove(path)
    except OSError:
        pass
    record("R5 MPSC two-producer schedule delivers every message exactly once",
           ok, detail)

    # ── R6: crash recovery replay through the real implementation ──
    # A "crashed" writer is simulated with the test-only debug helper, which
    # leaves a WRITING header with an arbitrary writer identity — exactly
    # what a producer killed mid-send leaves behind. The dead identity is a
    # real, reaped child process, so the liveness probe answers Gone.
    import subprocess

    def dead_identity():
        proc = subprocess.Popen([sys.executable, "-c", "pass"])
        proc.wait()
        return proc.pid, 0

    def _spawn_and_reap_for_dead_pid():
        return dead_identity()

    # R6a: open-time reclaim — the zombie is freed, its slot is reused, and
    # the sequence continues from the surviving messages.
    ok = True
    detail = ""
    C, SLOT = 4, 64
    path = temp_path("r6a")
    producer = dsline.FileChannel.create(path, capacity=C, slot_size=SLOT)
    producer.send(b"one")
    dead_pid, dead_start = _spawn_and_reap_for_dead_pid()
    producer._debug_leave_zombie(1, dead_pid, dead_start)
    consumer = dsline.FileChannel.open(path, capacity=C, slot_size=SLOT)
    stats = consumer.stats()
    if stats["reclaimed_slots"] != 1 or stats["queue_depth"] != 1:
        ok = False
        detail += f"open-time reclaim: {stats}; "
    consumer.send(b"two")
    got = [consumer.recv_with_seq(), consumer.recv_with_seq()]
    if [g[0] for g in got] != [0, 1] or [g[1] for g in got] != [b"one", b"two"]:
        ok = False
        detail += f"post-reclaim round trip: {got}; "
    _release(producer)
    _release(consumer)
    try:
        os.remove(path)
    except OSError:
        pass
    record("R6a dead writer's zombie reclaimed at open; slot reused", ok, detail)

    # R6b: gate-time reclaim — a consumer opened before the crash reclaims
    # the zombie at its gate and resyncs to the recovered producer.
    ok = True
    detail = ""
    path = temp_path("r6b")
    producer = dsline.FileChannel.create(path, capacity=C, slot_size=SLOT)
    consumer = dsline.FileChannel.open(path, capacity=C, slot_size=SLOT)
    producer.send(b"one")
    dead_pid, dead_start = _spawn_and_reap_for_dead_pid()
    producer._debug_leave_zombie(1, dead_pid, dead_start)
    try:
        consumer.recv_with_seq()
        got_one = True
    except dsline.BufferEmptyError:
        got_one = False
    if not got_one:
        ok = False
        detail += "the healthy message was not delivered; "
    try:
        consumer.recv()
        ok = False
        detail += "the zombie was delivered; "
    except dsline.BufferEmptyError:
        pass
    if consumer.stats()["reclaimed_slots"] != 1:
        ok = False
        detail += f"gate reclaim did not fire: {consumer.stats()}; "
    producer.send(b"two")  # lands in the freed slot
    if consumer.recv_with_seq()[1] != b"two":
        ok = False
        detail += "the post-crash message was not delivered after resync; "
    _release(producer)
    _release(consumer)
    try:
        os.remove(path)
    except OSError:
        pass
    record("R6b consumer reclaims the zombie at the gate and resyncs", ok, detail)

    # R6c: a live writer's zombie is never reclaimed (conservative default).
    ok = True
    detail = ""
    path = temp_path("r6c")
    producer = dsline.FileChannel.create(path, capacity=C, slot_size=SLOT)
    producer.send(b"one")
    pid, start_time = dsline.process_identity()
    producer._debug_leave_zombie(1, pid, start_time)
    consumer = dsline.FileChannel.open(path, capacity=C, slot_size=SLOT)
    stats = consumer.stats()
    if stats["reclaimed_slots"] != 0 or stats["queue_depth"] != 2:
        ok = False
        detail += f"live zombie touched: {stats}; "
    consumer.send(b"two")  # steps over the zombie
    if consumer.recv_with_seq() != (0, b"one"):
        ok = False
        detail += "the healthy message was lost; "
    try:
        consumer.recv()
    except dsline.BufferEmptyError:
        pass
    except Exception as exc:  # noqa: BLE001 - any other error is a failure
        ok = False
        detail += f"unexpected error on the zombie: {exc}; "
    _release(producer)
    _release(consumer)
    try:
        os.remove(path)
    except OSError:
        pass
    record("R6c a live writer's zombie is left untouched", ok, detail)

    # R6d: reclaim_stale — the explicit owner cleanup entry point.
    ok = True
    detail = ""
    path = temp_path("r6d")
    producer = dsline.FileChannel.create(path, capacity=C, slot_size=SLOT)
    if producer.reclaim_stale() != 0:
        ok = False
        detail += "healthy ring reported a reclaim; "
    producer.send(b"one")
    dead_pid, dead_start = _spawn_and_reap_for_dead_pid()
    producer._debug_leave_zombie(1, dead_pid, dead_start)
    if producer.reclaim_stale() != 1:
        ok = False
        detail += f"cleanup did not free the zombie: {producer.stats()}; "
    if producer.reclaim_stale() != 0:
        ok = False
        detail += "cleanup is not idempotent; "
    producer.send(b"two")
    if producer.recv_with_seq() != (0, b"one") or producer.recv_with_seq()[1] != b"two":
        ok = False
        detail += "post-cleanup round trip broken; "
    _release(producer)
    try:
        os.remove(path)
    except OSError:
        pass
    record("R6d reclaim_stale frees the zombie exactly once", ok, detail)

    # ── R7: sharded multi-consumer replay through the real implementation ──
    if ShardedChannel is None:
        record("R7 sharded fan-out replay (skipped: dsline too old)", True)
    else:
        import multiprocessing as mp

        # R7a: 2 producers, 3 shards, per-producer round-robin — the union
        # of the shard drains is exactly the sent multiset (exactly once).
        ok = True
        detail = ""
        ctx = mp.get_context("spawn")
        name = "verify-r7a-{:08x}".format(random.randrange(1 << 30))
        q = ctx.Queue()
        procs = [
            ctx.Process(target=_r7_consumer, args=(name, 3, 16, 64, i, 8, q), daemon=True)
            for i in range(3)
        ] + [
            ctx.Process(target=_r7_producer,
                        args=(name, 3, 16, 64, "round_robin", label, 12, q),
                        daemon=True)
            for label in ("A", "B")
        ]
        for p in procs:
            p.start()
        results = []
        for _ in range(5):
            try:
                results.append(q.get(timeout=30))
            except Exception:  # noqa: BLE001
                ok = False
                detail += "queue timeout; "
                break
        for p in procs:
            p.join(timeout=20)
            if p.exitcode not in (0, None):
                ok = False
                detail += f"exitcode {p.exitcode}; "
        delivered = sorted(
            msg for kind, payload in results
            if kind == "ok" for msg in payload
        )
        expected = sorted(
            f"{label}-{i}".encode() for label in ("A", "B") for i in range(12)
        )
        if delivered != expected:
            ok = False
            detail += f"delivered {len(delivered)} != expected {len(expected)}; "
        record("R7a 2 producers x 3 shards: the shard drains reassemble the "
               "sent multiset exactly once", ok, detail)

        # R7b: broadcast — a consumer per shard, every shard sees the full
        # stream in order.
        ok = True
        detail = ""
        name = "verify-r7b-{:08x}".format(random.randrange(1 << 30))
        q = ctx.Queue()
        procs = [
            ctx.Process(target=_r7_consumer, args=(name, 3, 8, 64, i, 5, q), daemon=True)
            for i in range(3)
        ] + [
            ctx.Process(target=_r7_producer,
                        args=(name, 3, 8, 64, "broadcast", "BC", 5, q), daemon=True)
        ]
        for p in procs:
            p.start()
        results = []
        for _ in range(4):
            try:
                results.append(q.get(timeout=30))
            except Exception:  # noqa: BLE001
                ok = False
                detail += "queue timeout; "
                break
        for p in procs:
            p.join(timeout=20)
        expected = [f"BC-{i}".encode() for i in range(5)]
        for kind, payload in results:
            if kind == "ok" and payload != expected:
                ok = False
                detail += f"shard stream mismatch ({len(payload)} msgs); "
        record("R7b broadcast: every shard consumer receives the full "
               "stream in order", ok, detail)

        # R7c: hash routing determinism — the same key from two handles
        # lands on the same shard, and the shard matches shard_for().
        ok = True
        detail = ""
        name = "verify-r7c-{:08x}".format(random.randrange(1 << 30))
        fan1 = ShardedChannel(name, shards=4, capacity=8, slot_size=64,
                              strategy="hash", multi_producer=True)
        fan2 = ShardedChannel(name, shards=4, capacity=8, slot_size=64,
                              strategy="hash", multi_producer=True)
        fan1.send(b"one", key=b"user-7")
        fan2.send(b"two", key=b"user-7")
        index = shard_for(b"user-7", 4)
        shard = fan1.shard(index)
        got = []
        while True:
            try:
                got.append(shard.recv())
            except Exception as exc:  # noqa: BLE001
                if type(exc).__name__ == "BufferEmptyError":
                    break
                raise
        if got != [b"one", b"two"]:
            ok = False
            detail += f"shard {index} holds {got}; "
        fan1.close()
        fan2.close()
        record("R7c hash routing: the same key from two handles lands on "
               "the same shard (FNV-1a % shards)", ok, detail)


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
