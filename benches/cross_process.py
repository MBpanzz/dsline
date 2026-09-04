#!/usr/bin/env python3
"""Cross-process throughput and latency benchmark: dsline vs industry baselines.

Backends (cross-process, spawn):

  dsline_shm    dsline ShmChannel — named shared-memory channel (mmap region)
  dsline_file   dsline FileChannel — mmap-backed file channel
  mp_queue      multiprocessing.Queue (pickle + pipe + feeder thread)
  mp_pipe       multiprocessing.Pipe, raw bytes
  shm_stdlib    hand-rolled SPSC ring over multiprocessing.shared_memory
                (the ROADMAP "手写 shared_memory 方案" comparison object)
  zmq_tcp       ZeroMQ PUSH/PULL (throughput) / REQ/REP (latency), tcp loopback
  pynng_tcp     nanomsg Push0/Pull0 (throughput) / Pair0 (latency), tcp loopback

In-process section (threads): dsline ShmChannel, deque+Condition,
queue.Queue, ZeroMQ inproc, pynng inproc.

Notes on fairness:
  - every backend transfers raw byte payloads of the same sizes;
  - mp.Queue additionally pays pickle serialization (documented);
  - dsline/shm_stdlib consumers spin on an empty ring; mp/zmq consumers block;
  - warmup messages precede the timed region in every backend.

Usage:
  python benches/cross_process.py                # full run
  python benches/cross_process.py --quick        # smaller counts
  python benches/cross_process.py --json-out out.json
"""

import argparse
import collections
import json
import os
import queue
import random
import struct
import sys
import tempfile
import threading
import time
import uuid
from statistics import quantiles

try:
    from multiprocessing import shared_memory
except ImportError:  # pragma: no cover
    shared_memory = None

import multiprocessing as mp

WARMUP = 1000
SIZES = [64, 1024, 4096, 65536]
COUNTS = {64: 200_000, 1024: 200_000, 4096: 100_000, 65536: 10_000}
LATENCY_SIZES = [64, 4096]
LATENCY_COUNT = 2000
INPROC_SIZES = [64, 4096]
INPROC_COUNT = 20_000


def make_payload(size: int) -> bytes:
    return bytes((i % 251) for i in range(size))


def now() -> float:
    return time.perf_counter()


# ═══════════════════════════════════════════════════════════════════════
# worker plumbing
# ═══════════════════════════════════════════════════════════════════════

def _ready(control):
    control.put(("ready", os.getpid()))


def _fail(control, exc):
    control.put(("error", repr(exc)))
    sys.exit(2)


def _spin_wait():
    time.sleep(0)


# ═══════════════════════════════════════════════════════════════════════
# dsline workers
# ═══════════════════════════════════════════════════════════════════════

def dsline_producer(params, n_warm, n, msg_size, control, results, start):
    import dsline
    try:
        if params["kind"] == "shm":
            ch = dsline.ShmChannel(
                params["name"],
                capacity=params["capacity"],
                slot_size=params["slot_size"],
                backpressure=dsline.Backpressure.Block,
                timeout=60.0,
            )
        else:
            ch = dsline.FileChannel.create(
                params["path"],
                capacity=params["capacity"],
                slot_size=params["slot_size"],
                backpressure=dsline.Backpressure.Block,
                timeout=60.0,
            )
        payload = make_payload(msg_size)
        _ready(control)
        start.wait(60)
        for _ in range(n_warm):
            ch.send(payload)
        t0 = now()
        for _ in range(n):
            ch.send(payload)
        t1 = now()
        results.put(("time", t1 - t0))
        ch.close()
    except BaseException as e:  # noqa: BLE001
        _fail(control, e)


def dsline_consumer(params, n_warm, n, msg_size, control, results, start):
    import dsline
    try:
        if params["kind"] == "shm":
            ch = dsline.ShmChannel(
                params["name"],
                capacity=params["capacity"],
                slot_size=params["slot_size"],
            )
        else:
            # The producer may not have created the file yet — retry.
            while True:
                try:
                    ch = dsline.FileChannel.open(
                        params["path"],
                        capacity=params["capacity"],
                        slot_size=params["slot_size"],
                    )
                    break
                except (dsline.ChannelError, ValueError):
                    _spin_wait()
        _ready(control)
        start.wait(60)
        got = 0
        while got < n_warm:
            try:
                ch.recv()
            except dsline.BufferEmptyError:
                _spin_wait()
                continue
            got += 1
        t0 = now()
        got = 0
        while got < n:
            try:
                ch.recv()
            except dsline.BufferEmptyError:
                _spin_wait()
                continue
            got += 1
        t1 = now()
        results.put(("time", t1 - t0))
        ch.close()
    except BaseException as e:  # noqa: BLE001
        _fail(control, e)


def dsline_latency_producer(params, n, msg_size, control, results, start):
    import dsline
    try:
        ping = dsline.ShmChannel(
            params["ping_name"],
            capacity=16,
            slot_size=max(64, msg_size) + 64,
            backpressure=dsline.Backpressure.Block,
            timeout=60.0,
        )
        pong = dsline.ShmChannel(
            params["pong_name"],
            capacity=16,
            slot_size=max(64, msg_size) + 64,
            backpressure=dsline.Backpressure.Block,
            timeout=60.0,
        )
        payload = make_payload(msg_size)
        _ready(control)
        start.wait(60)
        rtts = []
        for _ in range(n):
            t0 = now()
            ping.send(payload)
            while True:
                try:
                    pong.recv()
                    break
                except dsline.BufferEmptyError:
                    continue
            rtts.append(now() - t0)
        results.put(("lat", rtts))
        ping.close()
        pong.close()
    except BaseException as e:  # noqa: BLE001
        _fail(control, e)


def dsline_latency_consumer(params, n, msg_size, control, results, start):
    import dsline
    try:
        ping = dsline.ShmChannel(
            params["ping_name"],
            capacity=16,
            slot_size=max(64, msg_size) + 64,
        )
        pong = dsline.ShmChannel(
            params["pong_name"],
            capacity=16,
            slot_size=max(64, msg_size) + 64,
        )
        _ready(control)
        start.wait(60)
        for _ in range(n):
            while True:
                try:
                    data = ping.recv()
                    break
                except dsline.BufferEmptyError:
                    continue
            while True:
                try:
                    pong.send(data)
                    break
                except dsline.BufferFullError:
                    _spin_wait()
        ping.close()
        pong.close()
    except BaseException as e:  # noqa: BLE001
        _fail(control, e)


# ═══════════════════════════════════════════════════════════════════════
# multiprocessing workers
# ═══════════════════════════════════════════════════════════════════════

def mp_queue_producer(params, n_warm, n, msg_size, control, results, start):
    payload = make_payload(msg_size)
    _ready(control)
    start.wait(60)
    for _ in range(n_warm):
        params["queue"].put(payload)
    t0 = now()
    for _ in range(n):
        params["queue"].put(payload)
    t1 = now()
    results.put(("time", t1 - t0))


def mp_queue_consumer(params, n_warm, n, msg_size, control, results, start):
    _ready(control)
    start.wait(60)
    for _ in range(n_warm):
        params["queue"].get()
    t0 = now()
    for _ in range(n):
        params["queue"].get()
    t1 = now()
    results.put(("time", t1 - t0))


def mp_pipe_producer(params, n_warm, n, msg_size, control, results, start):
    payload = make_payload(msg_size)
    conn = params["conn"]
    _ready(control)
    start.wait(60)
    for _ in range(n_warm):
        conn.send_bytes(payload)
    t0 = now()
    for _ in range(n):
        conn.send_bytes(payload)
    t1 = now()
    results.put(("time", t1 - t0))


def mp_pipe_consumer(params, n_warm, n, msg_size, control, results, start):
    conn = params["conn"]
    _ready(control)
    start.wait(60)
    for _ in range(n_warm):
        conn.recv_bytes()
    t0 = now()
    for _ in range(n):
        conn.recv_bytes()
    t1 = now()
    results.put(("time", t1 - t0))


def mp_pipe_latency_producer(params, n, msg_size, control, results, start):
    conn = params["conn"]
    payload = make_payload(msg_size)
    _ready(control)
    start.wait(60)
    rtts = []
    for _ in range(n):
        t0 = now()
        conn.send_bytes(payload)
        conn.recv_bytes()
        rtts.append(now() - t0)
    results.put(("lat", rtts))


def mp_pipe_latency_consumer(params, n, msg_size, control, results, start):
    conn = params["conn"]
    _ready(control)
    start.wait(60)
    for _ in range(n):
        data = conn.recv_bytes()
        conn.send_bytes(data)


# ═══════════════════════════════════════════════════════════════════════
# stdlib shared_memory hand-rolled SPSC ring
# ═══════════════════════════════════════════════════════════════════════

class ShmRing:
    """Fixed-slot SPSC ring over multiprocessing.shared_memory.

    Per-slot layout: [state u8][seq u64][payload]. Producer writes payload,
    then seq, then state FULL (last); consumer reads and resets state to
    EMPTY. Spin-based handoff, mirrors the dsline slot protocol.
    """

    HEADER = 9

    def __init__(self, name, slot_count, slot_size, create):
        self.slot_count = slot_count
        self.slot_size = slot_size
        total = slot_count * slot_size
        if create:
            self.shm = shared_memory.SharedMemory(name=name, create=True, size=total)
        else:
            # The producer may not have created the block yet — retry.
            while True:
                try:
                    self.shm = shared_memory.SharedMemory(name=name, create=False)
                    break
                except FileNotFoundError:
                    _spin_wait()
        self.buf = self.shm.buf
        self.head = 0
        self.tail = 0
        self.seq = 0
        if create:
            for i in range(slot_count):
                self.buf[i * slot_size] = 0

    def send(self, data):
        buf, ss, sc = self.buf, self.slot_size, self.slot_count
        while True:
            head = self.head
            if buf[head * ss] == 0:
                break
            _spin_wait()
        off = head * ss
        buf[off + self.HEADER:off + self.HEADER + len(data)] = data
        struct.pack_into("<Q", buf, off + 1, self.seq)
        buf[off] = 1  # FULL, last
        self.head = (head + 1) % sc
        self.seq += 1

    def recv(self):
        buf, ss, sc = self.buf, self.slot_size, self.slot_count
        while True:
            tail = self.tail
            if buf[tail * ss] == 1:
                break
            _spin_wait()
        off = tail * ss
        data = bytes(buf[off + self.HEADER:off + ss])
        buf[off] = 0  # EMPTY
        self.tail = (tail + 1) % sc
        return data

    def close(self):
        self.shm.close()

    def unlink(self):
        try:
            self.shm.unlink()
        except FileNotFoundError:
            pass


def shm_stdlib_producer(params, n_warm, n, msg_size, control, results, start):
    ring = ShmRing(params["name"], params["slots"], params["slot_size"], create=True)
    payload = make_payload(msg_size)
    _ready(control)
    start.wait(60)
    for _ in range(n_warm):
        ring.send(payload)
    t0 = now()
    for _ in range(n):
        ring.send(payload)
    t1 = now()
    results.put(("time", t1 - t0))
    ring.close()


def shm_stdlib_consumer(params, n_warm, n, msg_size, control, results, start):
    ring = ShmRing(params["name"], params["slots"], params["slot_size"], create=False)
    _ready(control)
    start.wait(60)
    for _ in range(n_warm):
        ring.recv()
    t0 = now()
    for _ in range(n):
        ring.recv()
    t1 = now()
    results.put(("time", t1 - t0))
    ring.close()


def shm_stdlib_latency_producer(params, n, msg_size, control, results, start):
    ping = ShmRing(params["ping_name"], params["slots"], params["slot_size"], create=True)
    pong = ShmRing(params["pong_name"], params["slots"], params["slot_size"], create=True)
    payload = make_payload(msg_size)
    _ready(control)
    start.wait(60)
    rtts = []
    for _ in range(n):
        t0 = now()
        ping.send(payload)
        pong.recv()
        rtts.append(now() - t0)
    results.put(("lat", rtts))
    ping.close()
    pong.close()


def shm_stdlib_latency_consumer(params, n, msg_size, control, results, start):
    ping = ShmRing(params["ping_name"], params["slots"], params["slot_size"], create=False)
    pong = ShmRing(params["pong_name"], params["slots"], params["slot_size"], create=False)
    _ready(control)
    start.wait(60)
    for _ in range(n):
        data = ping.recv()
        pong.send(data)
    ping.close()
    pong.close()


# ═══════════════════════════════════════════════════════════════════════
# ZeroMQ / pynng workers
# ═══════════════════════════════════════════════════════════════════════

def zmq_producer(params, n_warm, n, msg_size, control, results, start):
    import zmq
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    sock.setsockopt(zmq.SNDHWM, 4096)
    sock.bind(f"tcp://127.0.0.1:{params['port']}")
    payload = make_payload(msg_size)
    _ready(control)
    start.wait(60)
    for _ in range(n_warm):
        sock.send(payload)
    t0 = now()
    for _ in range(n):
        sock.send(payload)
    t1 = now()
    results.put(("time", t1 - t0))
    sock.close()
    ctx.term()


def zmq_consumer(params, n_warm, n, msg_size, control, results, start):
    import zmq
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PULL)
    sock.setsockopt(zmq.RCVHWM, 4096)
    sock.connect(f"tcp://127.0.0.1:{params['port']}")
    _ready(control)
    start.wait(60)
    for _ in range(n_warm):
        sock.recv()
    t0 = now()
    for _ in range(n):
        sock.recv()
    t1 = now()
    results.put(("time", t1 - t0))
    sock.close()
    ctx.term()


def zmq_latency_producer(params, n, msg_size, control, results, start):
    import zmq
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, 30000)
    sock.bind(f"tcp://127.0.0.1:{params['port']}")
    payload = make_payload(msg_size)
    _ready(control)
    start.wait(60)
    rtts = []
    for _ in range(n):
        t0 = now()
        sock.send(payload)
        sock.recv()
        rtts.append(now() - t0)
    results.put(("lat", rtts))
    sock.close()
    ctx.term()


def zmq_latency_consumer(params, n, msg_size, control, results, start):
    import zmq
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REP)
    sock.setsockopt(zmq.RCVTIMEO, 30000)
    sock.connect(f"tcp://127.0.0.1:{params['port']}")
    _ready(control)
    start.wait(60)
    for _ in range(n):
        data = sock.recv()
        sock.send(data)
    sock.close()
    ctx.term()


def pynng_producer(params, n_warm, n, msg_size, control, results, start):
    import pynng
    sock = pynng.Push0()
    sock.listen(f"tcp://127.0.0.1:{params['port']}")
    payload = make_payload(msg_size)
    _ready(control)
    start.wait(60)
    for _ in range(n_warm):
        sock.send(payload)
    t0 = now()
    for _ in range(n):
        sock.send(payload)
    t1 = now()
    results.put(("time", t1 - t0))
    sock.close()


def pynng_consumer(params, n_warm, n, msg_size, control, results, start):
    import pynng
    sock = pynng.Pull0()
    sock.dial(f"tcp://127.0.0.1:{params['port']}")
    _ready(control)
    start.wait(60)
    for _ in range(n_warm):
        sock.recv()
    t0 = now()
    for _ in range(n):
        sock.recv()
    t1 = now()
    results.put(("time", t1 - t0))
    sock.close()


def pynng_latency_producer(params, n, msg_size, control, results, start):
    import pynng
    sock = pynng.Pair0()
    sock.listen(f"tcp://127.0.0.1:{params['port']}")
    payload = make_payload(msg_size)
    _ready(control)
    start.wait(60)
    rtts = []
    for _ in range(n):
        t0 = now()
        sock.send(payload)
        sock.recv()
        rtts.append(now() - t0)
    results.put(("lat", rtts))
    sock.close()


def pynng_latency_consumer(params, n, msg_size, control, results, start):
    import pynng
    sock = pynng.Pair0()
    sock.dial(f"tcp://127.0.0.1:{params['port']}")
    _ready(control)
    start.wait(60)
    for _ in range(n):
        data = sock.recv()
        sock.send(data)
    sock.close()


# ═══════════════════════════════════════════════════════════════════════
# in-process (threads)
# ═══════════════════════════════════════════════════════════════════════

def inproc_dsline(params, n_warm, n, msg_size):
    import dsline
    # Default capacity 1024: every send/recv re-derives the ring depth by
    # scanning all slot headers, so the in-process number is dominated by the
    # O(capacity) refresh (honest out-of-the-box measurement). The producer
    # must use Block with a timeout: the default (Block, no timeout) raises
    # BufferFullError immediately when the ring is full, which would kill the
    # producer thread and leave the consumer spinning forever.
    ch = dsline.ShmChannel(
        params["name"],
        capacity=1024,
        slot_size=max(4096, msg_size + 64),
        backpressure=dsline.Backpressure.Block,
        timeout=120.0,
    )
    payload = make_payload(msg_size)
    barrier = threading.Barrier(3)
    errors = []

    def producer():
        try:
            barrier.wait()
            for _ in range(n_warm):
                ch.send(payload)
            t0 = now()
            for _ in range(n):
                ch.send(payload)
            results.put(("time", now() - t0))
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    def consumer():
        try:
            barrier.wait()
            got = 0
            while got < n_warm:
                try:
                    ch.recv()
                except dsline.BufferEmptyError:
                    _spin_wait()
                    continue
                got += 1
            t0 = now()
            got = 0
            while got < n:
                try:
                    ch.recv()
                except dsline.BufferEmptyError:
                    _spin_wait()
                    continue
                got += 1
            results.put(("time", now() - t0))
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    results = queue.Queue()
    threads = [
        threading.Thread(target=producer),
        threading.Thread(target=consumer),
    ]
    for t in threads:
        t.start()
    barrier.wait()
    for t in threads:
        t.join()
    if errors:
        raise RuntimeError(errors[0])
    ch.close()
    return max(results.get()[1], results.get()[1])


def inproc_deque(params, n_warm, n, msg_size):
    payload = make_payload(msg_size)
    dq = collections.deque()
    cv = threading.Condition()
    results = queue.Queue()
    errors = []

    def producer():
        try:
            with cv:
                for _ in range(n_warm):
                    dq.append(payload)
                cv.notify_all()
                t0 = now()
                for _ in range(n):
                    dq.append(payload)
                cv.notify_all()
                results.put(("time", now() - t0))
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    def consumer():
        try:
            with cv:
                got = 0
                while got < n_warm:
                    while not dq:
                        cv.wait()
                    dq.popleft()
                    got += 1
                t0 = now()
                got = 0
                while got < n:
                    while not dq:
                        cv.wait()
                    dq.popleft()
                    got += 1
                results.put(("time", now() - t0))
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=producer), threading.Thread(target=consumer)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise RuntimeError(errors[0])
    return max(results.get()[1], results.get()[1])


def inproc_queue(params, n_warm, n, msg_size):
    payload = make_payload(msg_size)
    q = queue.Queue(maxsize=1024)
    results = queue.Queue()
    errors = []

    def producer():
        try:
            for _ in range(n_warm):
                q.put(payload)
            t0 = now()
            for _ in range(n):
                q.put(payload)
            results.put(("time", now() - t0))
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    def consumer():
        try:
            for _ in range(n_warm):
                q.get()
            t0 = now()
            for _ in range(n):
                q.get()
            results.put(("time", now() - t0))
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=producer), threading.Thread(target=consumer)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise RuntimeError(errors[0])
    return max(results.get()[1], results.get()[1])


def inproc_zmq(params, n_warm, n, msg_size):
    import zmq
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    pull.bind("inproc://dsl-bench")
    push.connect("inproc://dsl-bench")
    payload = make_payload(msg_size)
    results = queue.Queue()
    errors = []

    def producer():
        try:
            for _ in range(n_warm):
                push.send(payload)
            t0 = now()
            for _ in range(n):
                push.send(payload)
            results.put(("time", now() - t0))
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    def consumer():
        try:
            for _ in range(n_warm):
                pull.recv()
            t0 = now()
            for _ in range(n):
                pull.recv()
            results.put(("time", now() - t0))
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=producer), threading.Thread(target=consumer)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise RuntimeError(errors[0])
    push.close()
    pull.close()
    ctx.term()
    return max(results.get()[1], results.get()[1])


def inproc_pynng(params, n_warm, n, msg_size):
    import pynng
    pull = pynng.Pull0()
    push = pynng.Push0()
    pull.listen("inproc://dsl-bench-pynng")
    push.dial("inproc://dsl-bench-pynng")
    payload = make_payload(msg_size)
    results = queue.Queue()
    errors = []

    def producer():
        try:
            for _ in range(n_warm):
                push.send(payload)
            t0 = now()
            for _ in range(n):
                push.send(payload)
            results.put(("time", now() - t0))
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    def consumer():
        try:
            for _ in range(n_warm):
                pull.recv()
            t0 = now()
            for _ in range(n):
                pull.recv()
            results.put(("time", now() - t0))
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=producer), threading.Thread(target=consumer)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise RuntimeError(errors[0])
    push.close()
    pull.close()
    return max(results.get()[1], results.get()[1])


# ═══════════════════════════════════════════════════════════════════════
# driver
# ═══════════════════════════════════════════════════════════════════════

def run_pair(prod_fn, cons_fn, params_p, params_c, n_warm, n, msg_size,
             timeout=300):
    ctx = mp.get_context("spawn")
    control = ctx.Queue()
    results = ctx.Queue()
    start = ctx.Event()
    pp = ctx.Process(target=prod_fn, args=(params_p, n_warm, n, msg_size,
                                           control, results, start))
    pc = ctx.Process(target=cons_fn, args=(params_c, n_warm, n, msg_size,
                                           control, results, start))
    pp.start()
    pc.start()
    try:
        ready = []
        while len(ready) < 2:
            item = control.get(timeout=timeout)
            if item[0] == "error":
                raise RuntimeError(f"{pp.pid}/{pc.pid}: {item[1]}")
            ready.append(item)
        start.set()
        # Read results BEFORE joining: a worker whose atexit blocks while its
        # queue feeder waits for the pipe to drain would otherwise deadlock
        # (Windows pipe buffers are small).
        times = [results.get(timeout=timeout)[1], results.get(timeout=timeout)[1]]
        pp.join(timeout=timeout)
        pc.join(timeout=timeout)
        if pp.exitcode != 0 or pc.exitcode != 0:
            raise RuntimeError(f"worker exited non-zero: {pp.exitcode}/{pc.exitcode}")
        return max(times)
    finally:
        for p in (pp, pc):
            if p.is_alive():
                p.terminate()
                p.join(timeout=5)


def run_pair_latency(prod_fn, cons_fn, params_p, params_c, n, msg_size,
                     timeout=180):
    ctx = mp.get_context("spawn")
    control = ctx.Queue()
    results = ctx.Queue()
    start = ctx.Event()
    pp = ctx.Process(target=prod_fn, args=(params_p, n, msg_size, control,
                                           results, start))
    pc = ctx.Process(target=cons_fn, args=(params_c, n, msg_size, control,
                                           results, start))
    pp.start()
    pc.start()
    try:
        ready = []
        while len(ready) < 2:
            item = control.get(timeout=timeout)
            if item[0] == "error":
                raise RuntimeError(f"{pp.pid}/{pc.pid}: {item[1]}")
            ready.append(item)
        start.set()
        # Read the result before joining — see run_pair for the pipe-buffer
        # deadlock rationale.
        lat = results.get(timeout=timeout)[1]
        pp.join(timeout=timeout)
        pc.join(timeout=timeout)
        if pp.exitcode != 0 or pc.exitcode != 0:
            raise RuntimeError(f"worker exited non-zero: {pp.exitcode}/{pc.exitcode}")
        return lat
    finally:
        for p in (pp, pc):
            if p.is_alive():
                p.terminate()
                p.join(timeout=5)


def unique():
    return uuid.uuid4().hex[:12]


def throughput_configs(quick):
    sizes = [4096] if quick else SIZES
    counts = {s: max(10_000, COUNTS.get(s, 10_000) // 10) for s in sizes}
    backends = [
        ("dsline_shm", "shared memory (mmap)"),
        ("dsline_file", "mmap file"),
        ("mp_queue", "pipe + pickle"),
        ("mp_pipe", "pipe (raw)"),
        ("shm_stdlib", "shared memory (spin)"),
        ("zmq_tcp", "tcp loopback"),
        ("pynng_tcp", "tcp loopback"),
    ]
    return sizes, counts, backends


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quick", action="store_true",
                        help="smaller counts for a smoke run")
    parser.add_argument("--json-out", metavar="FILE", help="write results as JSON")
    parser.add_argument("--skip-inproc", action="store_true")
    args = parser.parse_args()

    ctx = mp.get_context("spawn")
    out = {
        "platform": sys.platform,
        "python": sys.version.split()[0],
        "cpu": os.cpu_count(),
        "throughput": [],
        "latency": [],
        "inproc": [],
    }

    sizes, counts, backends = throughput_configs(args.quick)
    print(f"platform: {sys.platform}, python: {sys.version.split()[0]}, "
          f"cpus: {os.cpu_count()}")
    print("\n── cross-process throughput ──")
    print(f"{'backend':<12} {'msg':>7} {'msgs/s':>10} {'GiB/s':>9} "
          f"{'wall':>7}")
    prod_fn = {
        "dsline_shm": "dsline_producer", "dsline_file": "dsline_producer",
        "mp_queue": "mp_queue_producer", "mp_pipe": "mp_pipe_producer",
        "shm_stdlib": "shm_stdlib_producer", "zmq_tcp": "zmq_producer",
        "pynng_tcp": "pynng_producer",
    }
    cons_fn = {
        "dsline_shm": "dsline_consumer", "dsline_file": "dsline_consumer",
        "mp_queue": "mp_queue_consumer", "mp_pipe": "mp_pipe_consumer",
        "shm_stdlib": "shm_stdlib_consumer", "zmq_tcp": "zmq_consumer",
        "pynng_tcp": "pynng_consumer",
    }
    for name, transport in backends:
        for size in sizes:
            n = counts[size]
            slot_size = size + 64
            if name in ("dsline_shm", "dsline_file"):
                if name == "dsline_shm":
                    p = {"kind": "shm", "name": f"dsl-bench-{unique()}"}
                else:
                    p = {"kind": "file",
                         "path": os.path.join(tempfile.gettempdir(),
                                              f"dsl-bench-{unique()}.bin")}
                params_p = {**p, "capacity": 64, "slot_size": slot_size}
                params_c = {**p, "capacity": 64, "slot_size": slot_size}
            elif name == "mp_queue":
                q = ctx.Queue()
                params_p = {"queue": q}
                params_c = {"queue": q}
            elif name == "mp_pipe":
                conn1, conn2 = ctx.Pipe(duplex=True)
                params_p = {"conn": conn1}
                params_c = {"conn": conn2}
            elif name == "shm_stdlib":
                shm_name = f"dsl-sr-{unique()}"
                params_p = {"name": shm_name, "slots": 64, "slot_size": slot_size + 16}
                params_c = {"name": shm_name, "slots": 64, "slot_size": slot_size + 16}
            else:
                port = random.randint(20000, 60000)
                params_p = {"port": port}
                params_c = {"port": port}
            wall = run_pair(globals()[prod_fn[name]], globals()[cons_fn[name]],
                            params_p, params_c, WARMUP, n, size)
            msgs_s = n / wall
            gib_s = size * n / wall / (1 << 30)
            out["throughput"].append({
                "backend": name, "transport": transport, "msg_size": size,
                "msgs_s": round(msgs_s), "gib_s": round(gib_s, 3),
                "wall_s": round(wall, 3),
            })
            print(f"{name:<12} {size:>7} {msgs_s:>10,.0f} {gib_s:>9.3f} "
                  f"{wall:>6.2f}s")
            if name == "dsline_file":
                try:
                    os.remove(params_p["path"])
                except OSError:
                    pass
    print()

    print("── cross-process latency (one-way, RTT/2) ──")
    lat_backends = [
        ("dsline_shm", "dsline_latency_producer", "dsline_latency_consumer",
         {"kind": "shm"}),
        ("mp_pipe", "mp_pipe_latency_producer", "mp_pipe_latency_consumer", {}),
        ("shm_stdlib", "shm_stdlib_latency_producer", "shm_stdlib_latency_consumer", {}),
        ("zmq_tcp", "zmq_latency_producer", "zmq_latency_consumer", {}),
        ("pynng_tcp", "pynng_latency_producer", "pynng_latency_consumer", {}),
    ]
    print(f"{'backend':<12} {'msg':>7} {'p50 µs':>8} {'p99 µs':>8} {'p999 µs':>9}")
    for name, prod_name, cons_name, params in lat_backends:
        for size in LATENCY_SIZES:
            n = LATENCY_COUNT
            if name == "dsline_shm":
                ping = f"dsl-lat-ping-{unique()}"
                pong = f"dsl-lat-pong-{unique()}"
                params_p = {"kind": "shm", "ping_name": ping, "pong_name": pong}
                params_c = {"kind": "shm", "ping_name": ping, "pong_name": pong}
            elif name == "shm_stdlib":
                ping = f"dsl-lsr-ping-{unique()}"
                pong = f"dsl-lsr-pong-{unique()}"
                slot_size = size + 64 + 16
                params_p = {"slots": 64, "slot_size": slot_size,
                            "ping_name": ping, "pong_name": pong}
                params_c = {"slots": 64, "slot_size": slot_size,
                            "ping_name": ping, "pong_name": pong}
            elif name == "mp_pipe":
                conn1, conn2 = ctx.Pipe(duplex=True)
                params_p = {"conn": conn1}
                params_c = {"conn": conn2}
            else:
                port = random.randint(20000, 60000)
                params_p = {"port": port}
                params_c = {"port": port}
            rtts = run_pair_latency(globals()[prod_name], globals()[cons_name],
                                    params_p, params_c, n, size)
            one_way = [r / 2 for r in rtts]
            p50 = quantiles(one_way, n=1000)[499]
            p99 = quantiles(one_way, n=100)[98]
            p999 = quantiles(one_way, n=1000)[998]
            out["latency"].append({
                "backend": name, "msg_size": size, "n": n,
                "p50_us": round(p50 * 1e6, 2), "p99_us": round(p99 * 1e6, 2),
                "p999_us": round(p999 * 1e6, 2),
            })
            print(f"{name:<12} {size:>7} {p50 * 1e6:>8.2f} {p99 * 1e6:>8.2f} "
                  f"{p999 * 1e6:>9.2f}")
    print()

    if not args.skip_inproc:
        print("── in-process (threads) ──")
        inproc_backends = [
            ("dsline", inproc_dsline),
            ("deque+cv", inproc_deque),
            ("queue.Queue", inproc_queue),
            ("zmq_inproc", inproc_zmq),
            ("pynng_inproc", inproc_pynng),
        ]
        print(f"{'backend':<14} {'msg':>7} {'msgs/s':>10} {'GiB/s':>9}")
        for name, fn in inproc_backends:
            for size in INPROC_SIZES:
                n = INPROC_COUNT
                params = {"name": f"dsl-bench-inproc-{unique()}"}
                wall = fn(params, 2000, n, size)
                msgs_s = n / wall
                gib_s = size * n / wall / (1 << 30)
                out["inproc"].append({
                    "backend": name, "msg_size": size, "msgs_s": round(msgs_s),
                    "gib_s": round(gib_s, 3),
                })
                print(f"{name:<14} {size:>7} {msgs_s:>10,.0f} {gib_s:>9.3f}")

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
        print(f"\nwrote {args.json_out}")


if __name__ == "__main__":
    main()
