import gc
import multiprocessing as mp
import os
import subprocess
import sys
import threading
import time
import unittest
import uuid

import dsline


def unique_name(label: str) -> str:
    return f"test-{label}-{uuid.uuid4().hex[:12]}"


def remove_region(channel) -> None:
    """Close a channel and delete its region file.

    The channel object must be dropped (which releases the memory mapping)
    before the file can be removed on Windows.
    """
    path = channel.path
    channel.close()
    del channel
    gc.collect()
    try:
        os.remove(path)
    except OSError:
        pass


def _shm_consumer(name, capacity, slot_size, count, result_queue):
    """Child process worker: open the named channel and receive messages.

    `recv_with_seq` raises `BufferEmptyError` when the ring is momentarily
    empty, so the worker retries until it has received `count` messages.
    """
    with dsline.ShmChannel(name, capacity=capacity, slot_size=slot_size) as ch:
        received = []
        while len(received) < count:
            try:
                seq, payload = ch.recv_with_seq()
            except dsline.BufferEmptyError:
                time.sleep(0.001)
                continue
            received.append((seq, payload))
        result_queue.put(received)


class ShmChannelTests(unittest.TestCase):
    def make_channel(self, label, **kwargs):
        ch = dsline.ShmChannel(unique_name(label), **kwargs)
        self.addCleanup(remove_region, ch)
        return ch

    def test_send_recv_bytes(self) -> None:
        ch = self.make_channel("send-recv", capacity=2, slot_size=16)
        ch.send(b"one")
        ch.send(b"two")

        self.assertEqual(ch.recv(), b"one")
        self.assertEqual(ch.recv(), b"two")

    def test_recv_with_seq_returns_sequence_and_payload(self) -> None:
        ch = self.make_channel("seq", capacity=2, slot_size=16)
        ch.send(b"one")
        ch.send(b"two")

        self.assertEqual(ch.recv_with_seq(), (0, b"one"))
        self.assertEqual(ch.recv_with_seq(), (1, b"two"))

    def test_send_accepts_bytearray(self) -> None:
        ch = self.make_channel("bytearray", capacity=1, slot_size=16)
        ch.send(bytearray(b"mutable"))

        self.assertEqual(ch.recv(), b"mutable")

    def test_send_accepts_memoryview(self) -> None:
        ch = self.make_channel("memoryview", capacity=1, slot_size=16)
        ch.send(memoryview(b"view"))

        self.assertEqual(ch.recv(), b"view")

    def test_send_recv_message_larger_than_slot_size(self) -> None:
        payload = b"x" * 80
        ch = self.make_channel("chunked", capacity=4, slot_size=64)
        ch.send(payload)

        self.assertEqual(ch.stats()["queue_depth"], 4)
        self.assertEqual(ch.recv_with_seq(), (0, payload))
        self.assertTrue(ch.empty)

    def test_rejects_message_larger_than_chunked_capacity(self) -> None:
        ch = self.make_channel("too-large", capacity=2, slot_size=64)

        with self.assertRaises(dsline.MessageTooLargeError):
            ch.send(b"x" * 100)

    def test_raise_backpressure_maps_exception(self) -> None:
        ch = self.make_channel(
            "full",
            capacity=1,
            slot_size=16,
            backpressure=dsline.Backpressure.Raise,
        )
        ch.send(b"one")

        with self.assertRaises(dsline.BufferFullError):
            ch.send(b"two")

    def test_drop_newest_backpressure_discards_incoming_message(self) -> None:
        ch = self.make_channel(
            "drop-newest",
            capacity=1,
            slot_size=16,
            backpressure=dsline.Backpressure.DropNewest,
        )
        ch.send(b"one")
        ch.send(b"two")

        self.assertEqual(ch.stats()["queue_depth"], 1)
        self.assertEqual(ch.stats()["next_sequence"], 1)
        self.assertEqual(ch.recv_with_seq(), (0, b"one"))

        with self.assertRaises(dsline.BufferEmptyError):
            ch.recv()

    def test_drop_oldest_backpressure_discards_oldest_message(self) -> None:
        ch = self.make_channel(
            "drop-oldest",
            capacity=2,
            slot_size=16,
            backpressure=dsline.Backpressure.DropOldest,
        )
        ch.send(b"one")
        ch.send(b"two")
        ch.send(b"three")

        self.assertEqual(ch.stats()["queue_depth"], 2)
        self.assertEqual(ch.stats()["next_sequence"], 3)
        self.assertEqual(ch.stats()["expected_recv_sequence"], 1)
        self.assertEqual(ch.recv_with_seq(), (1, b"two"))
        self.assertEqual(ch.recv_with_seq(), (2, b"three"))

    def test_context_manager_closes_channel(self) -> None:
        ch = self.make_channel("close")
        with ch:
            self.assertFalse(ch.closed)

        self.assertTrue(ch.closed)

    def test_channel_state_properties_and_stats(self) -> None:
        ch = self.make_channel("stats", capacity=3, slot_size=16)

        self.assertEqual(ch.capacity, 3)
        self.assertEqual(ch.slot_size, 16)
        self.assertTrue(ch.empty)
        self.assertEqual(len(ch), 0)

        ch.send(b"one")
        stats = ch.stats()

        self.assertEqual(stats["name"], ch.name)
        self.assertEqual(stats["path"], ch.path)
        self.assertEqual(stats["backend"], "shm")
        self.assertEqual(stats["queue_depth"], 1)
        self.assertEqual(stats["queue_capacity"], 3)
        self.assertEqual(stats["slot_size"], 16)
        self.assertEqual(stats["chunk_metadata_size"], 42)
        self.assertEqual(stats["chunk_payload_size"], 0)
        self.assertEqual(stats["max_message_size"], 16)
        self.assertEqual(stats["next_sequence"], 1)
        self.assertEqual(stats["expected_recv_sequence"], 0)
        self.assertIsNone(stats["last_received_sequence"])
        self.assertFalse(stats["closed"])
        self.assertFalse(stats["empty"])
        self.assertEqual(len(ch), 1)

        self.assertEqual(ch.recv(), b"one")
        stats = ch.stats()
        self.assertEqual(stats["expected_recv_sequence"], 1)
        self.assertEqual(stats["last_received_sequence"], 0)
        self.assertTrue(ch.empty)

    def test_stats_report_chunked_message_size_limits(self) -> None:
        ch = self.make_channel("size-limits", capacity=4, slot_size=64)
        stats = ch.stats()

        self.assertEqual(stats["slot_size"], 64)
        self.assertEqual(stats["chunk_metadata_size"], 42)
        self.assertEqual(stats["chunk_payload_size"], 22)
        self.assertEqual(stats["max_message_size"], 88)


def _mpsc_producer(name, capacity, slot_size, label, count, result_queue):
    """Child process worker: send `count` labeled messages into a named
    multi-producer channel, then report completion."""
    with dsline.ShmChannel(
        name,
        capacity=capacity,
        slot_size=slot_size,
        multi_producer=True,
        timeout=30.0,
    ) as ch:
        for i in range(count):
            ch.send(f"{label}-{i}".encode())
    result_queue.put((label, count))


class MultiProducerShmChannelTests(unittest.TestCase):
    def test_multi_producer_flag_in_stats(self) -> None:
        ch = dsline.ShmChannel(
            unique_name("mp-stats"), capacity=4, slot_size=64, multi_producer=True
        )
        self.addCleanup(remove_region, ch)
        self.assertTrue(ch.stats()["multi_producer"])
        ch.send(b"one")
        self.assertEqual(ch.recv(), b"one")

    def test_multi_producer_mode_mismatch_is_rejected(self) -> None:
        # The channel mode is baked into the region magic at creation: an
        # opener that disagrees on multi_producer must fail instead of
        # misinterpreting the ring.
        name = unique_name("mp-mode")
        ch = dsline.ShmChannel(name, capacity=4, slot_size=64)
        self.addCleanup(remove_region, ch)
        with self.assertRaises(ValueError):
            dsline.ShmChannel(name, capacity=4, slot_size=64, multi_producer=True)

    def test_two_producer_processes_deliver_every_message_once(self) -> None:
        """Two producer processes race on one named channel; the consumer
        receives every message exactly once regardless of interleaving."""
        ctx = mp.get_context("spawn")
        result_queue = ctx.Queue()
        name = unique_name("mpsc")
        capacity = 8
        count = 30
        ch = dsline.ShmChannel(
            name,
            capacity=capacity,
            slot_size=64,
            multi_producer=True,
            timeout=30.0,
        )
        self.addCleanup(remove_region, ch)

        producer_a = ctx.Process(
            target=_mpsc_producer, args=(name, capacity, 64, "a", count, result_queue)
        )
        producer_b = ctx.Process(
            target=_mpsc_producer, args=(name, capacity, 64, "b", count, result_queue)
        )
        producer_a.start()
        producer_b.start()

        received = []
        while len(received) < 2 * count:
            try:
                _, payload = ch.recv_with_seq()
            except dsline.BufferEmptyError:
                time.sleep(0.001)
                continue
            received.append(payload)
        producer_a.join(timeout=180)
        producer_b.join(timeout=180)
        if producer_a.is_alive():
            producer_a.terminate()
        if producer_b.is_alive():
            producer_b.terminate()

        self.assertEqual(producer_a.exitcode, 0)
        self.assertEqual(producer_b.exitcode, 0)
        expected = {
            f"{label}-{i}".encode()
            for label in ("a", "b")
            for i in range(count)
        }
        self.assertEqual(set(received), expected, "every message delivered")
        self.assertEqual(len(received), 2 * count, "no duplicate deliveries")
        completions = {result_queue.get(timeout=180) for _ in range(2)}
        self.assertEqual(
            completions,
            {("a", count), ("b", count)},
            "both producers finished",
        )

    def test_multi_producer_file_channel_roundtrip(self) -> None:
        path = os.path.join(
            os.environ.get("TEMP", os.getcwd()), f"dsline-mpsc-{uuid.uuid4().hex[:12]}.bin"
        )
        self.addCleanup(self._remove_file, path)
        ch1 = dsline.FileChannel.create(
            path, capacity=8, slot_size=64, multi_producer=True
        )
        ch2 = dsline.FileChannel.open(
            path, capacity=8, slot_size=64, multi_producer=True
        )
        ch3 = dsline.FileChannel.open(
            path, capacity=8, slot_size=64, multi_producer=True
        )
        self.assertTrue(ch1.stats()["multi_producer"])
        ch1.send(b"alpha")
        ch2.send(b"beta")
        ch1.send(b"x" * 80)  # chunked (4 slots at slot_size 64)
        ch2.send(b"gamma")
        self.assertEqual(ch3.recv(), b"alpha")
        self.assertEqual(ch3.recv(), b"beta")
        self.assertEqual(ch3.recv(), b"x" * 80)
        self.assertEqual(ch3.recv(), b"gamma")
        ch1.close()
        ch2.close()
        ch3.close()

    @staticmethod
    def _remove_file(path: str) -> None:
        try:
            os.remove(path)
        except OSError:
            pass


class CrossProcessShmChannelTests(unittest.TestCase):
    def test_cross_process_roundtrip_with_backpressure(self) -> None:
        """A producer and a consumer in separate processes exchange messages
        over one named shared-memory region, exercising cross-process
        blocking when the ring fills up."""
        ctx = mp.get_context("spawn")
        result_queue = ctx.Queue()
        name = unique_name("cross-process")
        capacity = 8
        payloads = [f"msg-{i}".encode() for i in range(40)]
        payloads.append(b"x" * 80)  # chunked message larger than slot_size

        ch = dsline.ShmChannel(
            name,
            capacity=capacity,
            slot_size=64,
            backpressure=dsline.Backpressure.Block,
            timeout=10.0,
        )
        self.addCleanup(remove_region, ch)

        process = ctx.Process(
            target=_shm_consumer,
            args=(name, capacity, 64, len(payloads), result_queue),
        )
        process.start()

        try:
            for payload in payloads:
                ch.send(payload)
            received = result_queue.get(timeout=180)
        finally:
            process.join(timeout=180)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)

        self.assertEqual(process.exitcode, 0)
        self.assertEqual(received, list(enumerate(payloads)))


# ── crash recovery (writer identity, roadmap §6) ──

# The reclaim tests below need a platform that can prove a writer is gone:
# only Windows and Linux implement the probe, and every other platform answers
# "unknown", which the protocol reads as "never reclaim" (roadmap §6.2). The
# refusal-side tests keep running everywhere.
_PROBE_PLATFORM = sys.platform.startswith(("linux", "win32"))
_PROBE_REASON = "reclaim needs a process-identity probe (Linux/Windows)"



def _dead_process_identity():
    """Spawn a child that exits immediately and return its (pid, 0)."""
    proc = subprocess.Popen(["cmd", "/c", "exit"] if os.name == "nt" else ["true"])
    proc.wait()
    return proc.pid, 0


def _crashed_writer_worker(name, capacity, slot_size, result_queue):
    """Child process worker: mimic a producer killed mid-send.

    Opens the named channel, leaves a WRITING header with a provably dead
    writer identity at its recovered write position, and exits.
    """
    with dsline.ShmChannel(name, capacity=capacity, slot_size=slot_size) as ch:
        dead_pid, dead_start = _dead_process_identity()
        ch._debug_leave_zombie(1, dead_pid, dead_start)
    result_queue.put("left-zombie")


class CrashRecoveryShmChannelTests(unittest.TestCase):
    def make_channel(self, label, **kwargs):
        ch = dsline.ShmChannel(unique_name(label), **kwargs)
        self.addCleanup(remove_region, ch)
        return ch

    def _spawn_crashed_writer(self, name, capacity, slot_size):
        ctx = mp.get_context("spawn")
        result_queue = ctx.Queue()
        process = ctx.Process(
            target=_crashed_writer_worker,
            args=(name, capacity, slot_size, result_queue),
        )
        process.start()
        result_queue.get(timeout=180)
        process.join(timeout=180)
        self.assertEqual(process.exitcode, 0)

    @unittest.skipUnless(_PROBE_PLATFORM, _PROBE_REASON)
    def test_dead_writer_zombie_is_reclaimed_at_open(self) -> None:
        ch = self.make_channel("crash-open", capacity=4, slot_size=64)
        ch.send(b"one")
        self._spawn_crashed_writer(ch.name, 4, 64)

        recovered = dsline.ShmChannel(ch.name, capacity=4, slot_size=64)
        self.addCleanup(remove_region, recovered)
        stats = recovered.stats()
        self.assertEqual(stats["reclaimed_slots"], 1)
        self.assertEqual(stats["queue_depth"], 1)
        # The recovered producer reuses the freed slot.
        recovered.send(b"two")
        self.assertEqual(recovered.recv_with_seq(), (0, b"one"))
        self.assertEqual(recovered.recv_with_seq()[1], b"two")

    @unittest.skipUnless(_PROBE_PLATFORM, _PROBE_REASON)
    def test_consumer_reclaims_dead_writer_at_the_gate(self) -> None:
        ch = self.make_channel("crash-gate", capacity=4, slot_size=64)
        consumer = dsline.ShmChannel(ch.name, capacity=4, slot_size=64)
        self.addCleanup(remove_region, consumer)
        ch.send(b"one")
        self._spawn_crashed_writer(ch.name, 4, 64)

        self.assertEqual(consumer.recv(), b"one")
        # The gate grace poll expires, the dead writer's slot is reclaimed,
        # and the leftover is reported as an empty ring.
        with self.assertRaises(dsline.BufferEmptyError):
            consumer.recv()
        self.assertEqual(consumer.stats()["reclaimed_slots"], 1)

        # A recovered producer reuses the freed slot; the consumer adopts
        # the new sequence through the resync rule.
        ch.send(b"two")
        self.assertEqual(consumer.recv(), b"two")

    def test_live_writer_zombie_is_not_reclaimed(self) -> None:
        ch = self.make_channel("crash-live", capacity=4, slot_size=64)
        ch.send(b"one")
        pid, start_time = dsline.process_identity()
        ch._debug_leave_zombie(1, pid, start_time)

        recovered = dsline.ShmChannel(ch.name, capacity=4, slot_size=64)
        self.addCleanup(remove_region, recovered)
        self.assertEqual(recovered.stats()["reclaimed_slots"], 0)
        # The zombie still counts in the depth; the write steps over it.
        self.assertEqual(recovered.stats()["queue_depth"], 2)
        recovered.send(b"two")
        self.assertEqual(recovered.recv_with_seq(), (0, b"one"))
        with self.assertRaises(dsline.BufferEmptyError):
            recovered.recv()

    @unittest.skipUnless(_PROBE_PLATFORM, _PROBE_REASON)
    def test_reclaim_stale_is_explicit_cleanup(self) -> None:
        ch = self.make_channel("crash-stale", capacity=4, slot_size=64)
        self.assertEqual(ch.reclaim_stale(), 0)
        ch.send(b"one")
        dead_pid, dead_start = _dead_process_identity()
        ch._debug_leave_zombie(1, dead_pid, dead_start)
        self.assertEqual(ch.reclaim_stale(), 1)
        self.assertEqual(ch.reclaim_stale(), 0, "idempotent after cleanup")
        self.assertEqual(ch.stats()["reclaimed_slots"], 1)
        ch.send(b"two")
        self.assertEqual(ch.recv_with_seq(), (0, b"one"))
        self.assertEqual(ch.recv_with_seq()[1], b"two")

    def test_stats_report_reclaimed_slots(self) -> None:
        ch = self.make_channel("crash-stats", capacity=4, slot_size=64)
        self.assertEqual(ch.stats()["reclaimed_slots"], 0)


class TryRecvTests(unittest.TestCase):
    """`try_recv()` is the non-blocking, non-raising poll.

    `recv()` runs the COMMITTED grace poll and raises `BufferEmptyError` on
    every empty attempt, so a spin-based consumer pays the interpreter's
    exception path per poll; `try_recv()` returns `None` instead.
    """

    def make_channel(self, label, **kwargs):
        ch = dsline.ShmChannel(unique_name(label), **kwargs)
        self.addCleanup(remove_region, ch)
        return ch

    def test_try_recv_returns_none_while_empty(self) -> None:
        ch = self.make_channel("try-empty", capacity=2, slot_size=16)

        self.assertIsNone(ch.try_recv())
        self.assertIsNone(ch.try_recv())

    def test_try_recv_returns_payloads_then_none(self) -> None:
        ch = self.make_channel("try-drain", capacity=2, slot_size=16)
        ch.send(b"one")
        ch.send(b"two")

        self.assertEqual(ch.try_recv(), b"one")
        self.assertEqual(ch.try_recv(), b"two")
        self.assertIsNone(ch.try_recv())

    def test_try_recv_shares_the_sequence_state_with_recv(self) -> None:
        ch = self.make_channel("try-seq", capacity=2, slot_size=16)
        ch.send(b"one")
        ch.send(b"two")

        self.assertEqual(ch.try_recv(), b"one")
        # The strict +1 chain continues across the two receive paths.
        self.assertEqual(ch.recv_with_seq(), (1, b"two"))

    def test_try_recv_reports_a_closed_empty_channel(self) -> None:
        ch = self.make_channel("try-closed", capacity=2, slot_size=16)
        ch.close()

        with self.assertRaises(dsline.ChannelClosedError):
            ch.try_recv()


class ThreadingTests(unittest.TestCase):
    """In-process producer/consumer threads use one handle each.

    A handle is serialized by an internal lock, so two threads sharing one
    handle would starve each other (a blocking send holds the lock). With a
    handle per thread the remaining hazard is the GIL: a blocking send spins
    in Rust until the consumer frees space, and it must release the GIL or the
    consumer thread can never drain the ring — the send would then block until
    its timeout and fail.
    """

    def make_channel(self, label, **kwargs):
        ch = dsline.ShmChannel(unique_name(label), **kwargs)
        self.addCleanup(remove_region, ch)
        return ch

    def test_blocking_send_and_a_consumer_thread_make_progress(self) -> None:
        count = 200
        name = unique_name("threads")
        producer_ch = dsline.ShmChannel(
            name,
            capacity=4,
            slot_size=256,
            backpressure=dsline.Backpressure.Block,
            timeout=10.0,
        )
        self.addCleanup(remove_region, producer_ch)
        consumer_ch = dsline.ShmChannel(name, capacity=4, slot_size=256)
        self.addCleanup(remove_region, consumer_ch)

        payload = b"x" * 128
        errors = []
        received = []

        def producer():
            try:
                for _ in range(count):
                    producer_ch.send(payload)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        def consumer():
            try:
                while len(received) < count:
                    data = consumer_ch.try_recv()
                    if data is None:
                        time.sleep(0)
                        continue
                    received.append(data)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [
            threading.Thread(target=producer),
            threading.Thread(target=consumer),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        self.assertEqual(errors, [])
        self.assertEqual(len(received), count)
        self.assertTrue(all(not thread.is_alive() for thread in threads))


if __name__ == "__main__":
    unittest.main()
