import gc
import multiprocessing as mp
import os
import tempfile
import time
import unittest
import uuid

import dsline


def temp_path(label: str) -> str:
    return os.path.join(
        tempfile.gettempdir(), f"dsline-test-{label}-{uuid.uuid4().hex[:8]}.bin"
    )


def remove_region(channel) -> None:
    """Close a channel and delete its backing file.

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


def _file_consumer(path, capacity, slot_size, count, result_queue):
    """Child process worker: open the file channel and receive messages.

    `recv_with_seq` raises `BufferEmptyError` when the ring is momentarily
    empty, so the worker retries until it has received `count` messages.
    """
    with dsline.FileChannel.open(path, capacity=capacity, slot_size=slot_size) as ch:
        received = []
        while len(received) < count:
            try:
                seq, payload = ch.recv_with_seq()
            except dsline.BufferEmptyError:
                time.sleep(0.001)
                continue
            received.append((seq, payload))
        result_queue.put(received)


class FileChannelTests(unittest.TestCase):
    def make_channel(self, label, **kwargs):
        path = temp_path(label)
        ch = dsline.FileChannel.create(path, **kwargs)
        self.addCleanup(remove_region, ch)
        return ch

    def test_create_send_recv_with_seq_and_stats(self) -> None:
        path = temp_path("basic")
        ch = dsline.FileChannel.create(path, capacity=2, slot_size=16)
        self.addCleanup(remove_region, ch)

        self.assertEqual(ch.path, path)
        self.assertTrue(ch.empty)
        self.assertEqual(len(ch), 0)

        ch.send(b"one")
        stats = ch.stats()

        self.assertEqual(stats["path"], path)
        self.assertEqual(stats["backend"], "file")
        self.assertEqual(stats["queue_depth"], 1)
        self.assertEqual(stats["queue_capacity"], 2)
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

        self.assertEqual(ch.recv_with_seq(), (0, b"one"))
        self.assertTrue(ch.empty)
        self.assertEqual(ch.stats()["last_received_sequence"], 0)

    def test_stats_report_chunked_message_size_limits(self) -> None:
        ch = self.make_channel("size-limits", capacity=4, slot_size=64)
        stats = ch.stats()

        self.assertEqual(stats["slot_size"], 64)
        self.assertEqual(stats["chunk_metadata_size"], 42)
        self.assertEqual(stats["chunk_payload_size"], 22)
        self.assertEqual(stats["max_message_size"], 88)

    def test_send_accepts_bytearray(self) -> None:
        ch = self.make_channel("bytearray", capacity=1, slot_size=16)
        ch.send(bytearray(b"mutable"))

        self.assertEqual(ch.recv(), b"mutable")

    def test_send_accepts_memoryview(self) -> None:
        ch = self.make_channel("memoryview", capacity=1, slot_size=16)
        ch.send(memoryview(b"view"))

        self.assertEqual(ch.recv(), b"view")

    def test_send_rejects_oversized_message(self) -> None:
        ch = self.make_channel("oversized", capacity=1, slot_size=4)

        with self.assertRaises(dsline.MessageTooLargeError):
            ch.send(b"abcde")

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

    def test_open_existing_channel_recovers_queued_messages(self) -> None:
        path = temp_path("recover")
        created = dsline.FileChannel.create(path, capacity=2, slot_size=16)
        self.addCleanup(remove_region, created)
        created.send(b"one")
        created.send(b"two")

        opened = dsline.FileChannel.open(path, capacity=2, slot_size=16)
        self.addCleanup(remove_region, opened)

        self.assertEqual(opened.stats()["queue_depth"], 2)
        self.assertEqual(opened.recv_with_seq(), (0, b"one"))
        self.assertEqual(opened.recv_with_seq(), (1, b"two"))

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
        ch = self.make_channel("close", capacity=1, slot_size=16)
        with ch:
            self.assertFalse(ch.closed)

        self.assertTrue(ch.closed)


class CrossProcessFileChannelTests(unittest.TestCase):
    def test_cross_process_roundtrip_with_backpressure(self) -> None:
        """A producer and a consumer in separate processes exchange messages
        over one file-backed region, exercising cross-process blocking when
        the ring fills up."""
        ctx = mp.get_context("spawn")
        result_queue = ctx.Queue()
        path = temp_path("cross-process")
        capacity = 8
        payloads = [f"file-{i}".encode() for i in range(40)]
        payloads.append(b"y" * 80)  # chunked message larger than slot_size

        ch = dsline.FileChannel.create(
            path,
            capacity=capacity,
            slot_size=64,
            backpressure=dsline.Backpressure.Block,
            timeout=10.0,
        )
        self.addCleanup(remove_region, ch)

        process = ctx.Process(
            target=_file_consumer,
            args=(path, capacity, 64, len(payloads), result_queue),
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


if __name__ == "__main__":
    unittest.main()
