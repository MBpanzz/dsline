import unittest

import dsline


class ShmChannelTests(unittest.TestCase):
    def test_send_recv_bytes(self) -> None:
        with dsline.ShmChannel("test-send-recv", capacity=2, slot_size=16) as ch:
            ch.send(b"one")
            ch.send(b"two")

            self.assertEqual(ch.recv(), b"one")
            self.assertEqual(ch.recv(), b"two")

    def test_recv_with_seq_returns_sequence_and_payload(self) -> None:
        with dsline.ShmChannel("test-seq", capacity=2, slot_size=16) as ch:
            ch.send(b"one")
            ch.send(b"two")

            self.assertEqual(ch.recv_with_seq(), (0, b"one"))
            self.assertEqual(ch.recv_with_seq(), (1, b"two"))

    def test_send_accepts_bytearray(self) -> None:
        with dsline.ShmChannel("test-bytearray", capacity=1, slot_size=16) as ch:
            ch.send(bytearray(b"mutable"))

            self.assertEqual(ch.recv(), b"mutable")

    def test_send_accepts_memoryview(self) -> None:
        with dsline.ShmChannel("test-memoryview", capacity=1, slot_size=16) as ch:
            ch.send(memoryview(b"view"))

            self.assertEqual(ch.recv(), b"view")

    def test_send_recv_message_larger_than_slot_size(self) -> None:
        payload = b"x" * 80
        with dsline.ShmChannel("test-chunked", capacity=4, slot_size=64) as ch:
            ch.send(payload)

            self.assertEqual(ch.stats()["queue_depth"], 4)
            self.assertEqual(ch.recv_with_seq(), (0, payload))
            self.assertTrue(ch.empty)

    def test_rejects_message_larger_than_chunked_capacity(self) -> None:
        with dsline.ShmChannel("test-too-large", capacity=2, slot_size=64) as ch:
            with self.assertRaises(dsline.MessageTooLargeError):
                ch.send(b"x" * 100)

    def test_raise_backpressure_maps_exception(self) -> None:
        ch = dsline.ShmChannel(
            "test-full",
            capacity=1,
            slot_size=16,
            backpressure=dsline.Backpressure.Raise,
        )
        ch.send(b"one")

        with self.assertRaises(dsline.BufferFullError):
            ch.send(b"two")

    def test_drop_newest_backpressure_discards_incoming_message(self) -> None:
        ch = dsline.ShmChannel(
            "test-drop-newest",
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
        ch = dsline.ShmChannel(
            "test-drop-oldest",
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
        with dsline.ShmChannel("test-close") as ch:
            self.assertFalse(ch.closed)

        self.assertTrue(ch.closed)

    def test_channel_state_properties_and_stats(self) -> None:
        ch = dsline.ShmChannel("test-stats", capacity=3, slot_size=16)

        self.assertEqual(ch.capacity, 3)
        self.assertEqual(ch.slot_size, 16)
        self.assertTrue(ch.empty)
        self.assertEqual(len(ch), 0)

        ch.send(b"one")
        stats = ch.stats()

        self.assertEqual(stats["name"], "test-stats")
        self.assertEqual(stats["backend"], "inprocess-prototype")
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
        ch = dsline.ShmChannel("test-size-limits", capacity=4, slot_size=64)
        stats = ch.stats()

        self.assertEqual(stats["slot_size"], 64)
        self.assertEqual(stats["chunk_metadata_size"], 42)
        self.assertEqual(stats["chunk_payload_size"], 22)
        self.assertEqual(stats["max_message_size"], 88)


if __name__ == "__main__":
    unittest.main()
