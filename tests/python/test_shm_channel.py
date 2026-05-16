import unittest

import dsline


class ShmChannelTests(unittest.TestCase):
    def test_send_recv_bytes(self) -> None:
        with dsline.ShmChannel("test-send-recv", capacity=2, slot_size=16) as ch:
            ch.send(b"one")
            ch.send(b"two")

            self.assertEqual(ch.recv(), b"one")
            self.assertEqual(ch.recv(), b"two")

    def test_send_accepts_bytearray(self) -> None:
        with dsline.ShmChannel("test-bytearray", capacity=1, slot_size=16) as ch:
            ch.send(bytearray(b"mutable"))

            self.assertEqual(ch.recv(), b"mutable")

    def test_send_accepts_memoryview(self) -> None:
        with dsline.ShmChannel("test-memoryview", capacity=1, slot_size=16) as ch:
            ch.send(memoryview(b"view"))

            self.assertEqual(ch.recv(), b"view")

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
        self.assertFalse(stats["closed"])
        self.assertFalse(stats["empty"])
        self.assertEqual(len(ch), 1)

        self.assertEqual(ch.recv(), b"one")
        self.assertTrue(ch.empty)


if __name__ == "__main__":
    unittest.main()
