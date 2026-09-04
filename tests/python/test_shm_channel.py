import gc
import multiprocessing as mp
import os
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
            received = result_queue.get(timeout=30)
        finally:
            process.join(timeout=30)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)

        self.assertEqual(process.exitcode, 0)
        self.assertEqual(received, list(enumerate(payloads)))


if __name__ == "__main__":
    unittest.main()
