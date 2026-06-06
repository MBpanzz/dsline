import tempfile
import unittest
from pathlib import Path

import dsline


class FileChannelTests(unittest.TestCase):
    def test_create_send_recv_with_seq_and_stats(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "channel.bin")
            ch = dsline.FileChannel.create(path, capacity=2, slot_size=16)

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
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "channel.bin")
            ch = dsline.FileChannel.create(path, capacity=4, slot_size=64)
            stats = ch.stats()

            self.assertEqual(stats["slot_size"], 64)
            self.assertEqual(stats["chunk_metadata_size"], 42)
            self.assertEqual(stats["chunk_payload_size"], 22)
            self.assertEqual(stats["max_message_size"], 88)

    def test_send_accepts_bytearray(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "channel.bin")
            ch = dsline.FileChannel.create(path, capacity=1, slot_size=16)

            ch.send(bytearray(b"mutable"))

            self.assertEqual(ch.recv(), b"mutable")

    def test_send_accepts_memoryview(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "channel.bin")
            ch = dsline.FileChannel.create(path, capacity=1, slot_size=16)

            ch.send(memoryview(b"view"))

            self.assertEqual(ch.recv(), b"view")

    def test_send_rejects_oversized_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "channel.bin")
            ch = dsline.FileChannel.create(path, capacity=1, slot_size=4)

            with self.assertRaises(dsline.MessageTooLargeError):
                ch.send(b"abcde")

    def test_send_recv_message_larger_than_slot_size(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "channel.bin")
            payload = b"x" * 80
            ch = dsline.FileChannel.create(path, capacity=4, slot_size=64)

            ch.send(payload)

            self.assertEqual(ch.stats()["queue_depth"], 4)
            self.assertEqual(ch.recv_with_seq(), (0, payload))
            self.assertTrue(ch.empty)

    def test_rejects_message_larger_than_chunked_capacity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "channel.bin")
            ch = dsline.FileChannel.create(path, capacity=2, slot_size=64)

            with self.assertRaises(dsline.MessageTooLargeError):
                ch.send(b"x" * 100)

    def test_open_existing_channel_recovers_queued_messages(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "channel.bin")
            created = dsline.FileChannel.create(path, capacity=2, slot_size=16)
            created.send(b"one")
            created.send(b"two")

            opened = dsline.FileChannel.open(path, capacity=2, slot_size=16)

            self.assertEqual(opened.stats()["queue_depth"], 2)
            self.assertEqual(opened.recv_with_seq(), (0, b"one"))
            self.assertEqual(opened.recv_with_seq(), (1, b"two"))

    def test_raise_backpressure_maps_exception(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "channel.bin")
            ch = dsline.FileChannel.create(
                path,
                capacity=1,
                slot_size=16,
                backpressure=dsline.Backpressure.Raise,
            )
            ch.send(b"one")

            with self.assertRaises(dsline.BufferFullError):
                ch.send(b"two")

    def test_drop_newest_backpressure_discards_incoming_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "channel.bin")
            ch = dsline.FileChannel.create(
                path,
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
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "channel.bin")
            ch = dsline.FileChannel.create(
                path,
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
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "channel.bin")
            with dsline.FileChannel.create(path, capacity=1, slot_size=16) as ch:
                self.assertFalse(ch.closed)

            self.assertTrue(ch.closed)


if __name__ == "__main__":
    unittest.main()
