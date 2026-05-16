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


if __name__ == "__main__":
    unittest.main()
