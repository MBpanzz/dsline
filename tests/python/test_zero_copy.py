"""Zero-copy alloc/publish and view tests (roadmap §4.2).

Covers the Python surface of the zero-copy path: the write buffer (born in
shared memory, sealed by publish), the receive view (pins its slot, partial
reads, released explicitly or by GC), the pin floor's effect on producers,
and cross-process delivery through real named channels.
"""

import gc
import multiprocessing as mp
import os
import tempfile
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


def _zero_copy_producer(name, capacity, slot_size, messages, result_queue):
    """Child process: alloc/write/publish each message through the named
    channel and report the assigned sequences."""
    try:
        with dsline.ShmChannel(name, capacity=capacity, slot_size=slot_size) as ch:
            seqs = []
            for index, payload in enumerate(messages):
                buf = ch.alloc(len(payload))
                buf.write(payload)
                seqs.append(ch.publish(buf))
            result_queue.put(("ok", seqs))
    except Exception as exc:  # pragma: no cover - surfaced through the queue
        result_queue.put(("error", repr(exc)))


def _pinned_ring_producer(name, capacity, slot_size, count, result_queue):
    """Child process: send `count` messages, retrying on BufferFullError.

    Used against a consumer that holds a zero-copy view open: the pin
    shrinks the ring the producer sees, and the retries must eventually
    drain it — without ever overwriting the pinned message.
    """
    try:
        with dsline.ShmChannel(
            name, capacity=capacity, slot_size=slot_size, backpressure=dsline.Backpressure.Raise
        ) as ch:
            sent = 0
            while sent < count:
                try:
                    ch.send(f"message-{sent}".encode())
                except dsline.BufferFullError:
                    time.sleep(0.001)
                    continue
                sent += 1
            result_queue.put(("ok", sent))
    except Exception as exc:  # pragma: no cover - surfaced through the queue
        result_queue.put(("error", repr(exc)))


# Cross-process waits are generous: a CI runner has 2-4 cores and a cold
# page cache, and each spawned process pays a fresh interpreter start
# plus the extension import.

class ZeroCopyShmChannelTests(unittest.TestCase):
    def make_channel(self, label, **kwargs):
        ch = dsline.ShmChannel(unique_name(label), **kwargs)
        self.addCleanup(remove_region, ch)
        return ch

    def test_alloc_publish_recv_view_roundtrip(self) -> None:
        ch = self.make_channel("roundtrip", capacity=4, slot_size=256)
        payload = bytes(range(64))

        buf = ch.alloc(len(payload))
        self.assertEqual(buf.size, len(payload))
        self.assertFalse(buf.sealed)
        buf.write(payload)
        seq = ch.publish(buf)
        self.assertEqual(seq, 0)
        self.assertTrue(buf.sealed)

        view = ch.recv_view()
        self.assertEqual(view.seq, 0)
        self.assertEqual(view.size, len(payload))
        self.assertEqual(view.read_all(), payload)

        view.release()
        self.assertTrue(view.released)
        self.assertEqual(ch.stats()["pinned_slots"], 0)
        self.assertEqual(len(ch), 0)

    def test_write_buffer_slice_assignment(self) -> None:
        ch = self.make_channel("slices", capacity=2, slot_size=64)
        buf = ch.alloc(11)
        buf.write(b"hello")
        buf.write(b"world", offset=5)
        buf[10:11] = b"!"
        ch.publish(buf)

        view = ch.recv_view()
        self.assertEqual(view.read_all(), b"helloworld!")

    def test_publish_seals_the_buffer(self) -> None:
        ch = self.make_channel("seal", capacity=2, slot_size=64)
        buf = ch.alloc(8)
        buf.write(b"abcdefgh")
        ch.publish(buf)

        with self.assertRaises(ValueError):
            buf.write(b"x")
        with self.assertRaises(ValueError):
            ch.publish(buf)

    def test_write_bounds_are_enforced(self) -> None:
        ch = self.make_channel("bounds", capacity=2, slot_size=64)
        buf = ch.alloc(4)
        with self.assertRaises(ValueError):
            buf.write(b"abcde")
        with self.assertRaises(ValueError):
            buf.write(b"ab", offset=3)
        ch.abort(buf)

    def test_partial_read_copies_only_the_requested_bytes(self) -> None:
        ch = self.make_channel("partial", capacity=2, slot_size=256)
        payload = bytes(range(128))
        buf = ch.alloc(len(payload))
        buf.write(payload)
        ch.publish(buf)

        view = ch.recv_view()
        self.assertEqual(view.read(0, 4), payload[:4])
        self.assertEqual(view.read(120), payload[120:])
        self.assertEqual(view[0], payload[0])
        self.assertEqual(view[10:14], payload[10:14])
        self.assertEqual(len(view), len(payload))

    def test_view_pins_slot_and_release_unblocks_producer(self) -> None:
        ch = self.make_channel(
            "pin-guard", capacity=4, slot_size=64, backpressure=dsline.Backpressure.Raise
        )
        for i in range(4):
            ch.send(f"m{i}".encode())

        view = ch.recv_view()
        self.assertEqual(view.read_all(), b"m0")
        self.assertEqual(ch.stats()["pinned_slots"], 1)
        self.assertEqual(ch.recv(), b"m1")
        self.assertEqual(ch.recv(), b"m2")

        # Logically one message deep (m3), but the pin on slot 0 blocks the
        # wraparound a send of m4 would need.
        with self.assertRaises(dsline.BufferFullError):
            ch.send(b"m4")
        # The pinned bytes are untouched by the failed attempt.
        self.assertEqual(view.read_all(), b"m0")

        view.release()
        ch.send(b"m4")
        self.assertEqual(ch.recv(), b"m3")
        self.assertEqual(ch.recv(), b"m4")

    def test_double_release_and_use_after_release_raise(self) -> None:
        ch = self.make_channel("release-errors", capacity=2, slot_size=64)
        ch.send(b"one")
        view = ch.recv_view()
        view.release()
        with self.assertRaises(ValueError):
            view.release()
        with self.assertRaises(ValueError):
            view.read_all()
        with self.assertRaises(ValueError):
            view[0]

    def test_out_of_order_release_keeps_the_floor(self) -> None:
        ch = self.make_channel(
            "two-views", capacity=4, slot_size=64, backpressure=dsline.Backpressure.Raise
        )
        for i in range(3):
            ch.send(f"m{i}".encode())
        older = ch.recv_view()
        newer = ch.recv_view()

        newer.release()
        ch.send(b"m3")
        with self.assertRaises(dsline.BufferFullError):
            ch.send(b"m4")

        older.release()
        ch.send(b"m4")

    def test_dropping_view_releases_the_slot(self) -> None:
        ch = self.make_channel("gc", capacity=2, slot_size=64)
        ch.send(b"payload")
        view = ch.recv_view()
        self.assertEqual(ch.stats()["pinned_slots"], 1)

        del view
        gc.collect()
        self.assertEqual(ch.stats()["pinned_slots"], 0)
        ch.send(b"next")
        self.assertEqual(ch.recv(), b"next")

    def test_with_block_releases_on_exit(self) -> None:
        ch = self.make_channel("with-block", capacity=2, slot_size=64)
        ch.send(b"payload")
        with ch.recv_view() as view:
            self.assertEqual(view.read_all(), b"payload")
            self.assertEqual(ch.stats()["pinned_slots"], 1)
        self.assertEqual(ch.stats()["pinned_slots"], 0)

    def test_recv_view_rejects_chunked_message(self) -> None:
        ch = self.make_channel("chunked", capacity=8, slot_size=64)
        ch.send(bytes(100))  # spans 3 slots at 34 payload bytes per chunk

        with self.assertRaises(ValueError):
            ch.recv_view()
        # The chunked message is untouched; the copy path still gets it.
        self.assertEqual(ch.recv(), bytes(100))

    def test_alloc_rejected_on_multi_producer(self) -> None:
        ch = self.make_channel("mpsc-alloc", capacity=4, slot_size=64, multi_producer=True)
        with self.assertRaises(ValueError):
            ch.alloc(8)
        # The zero-copy view (consumer side) still works in MPSC mode.
        ch.send(b"mpsc")
        with ch.recv_view() as view:
            self.assertEqual(view.read_all(), b"mpsc")

    def test_zero_copy_rejected_on_drop_oldest(self) -> None:
        ch = self.make_channel(
            "drop-oldest", capacity=4, slot_size=64, backpressure=dsline.Backpressure.DropOldest
        )
        with self.assertRaises(ValueError):
            ch.alloc(8)
        ch.send(b"one")
        with self.assertRaises(ValueError):
            ch.recv_view()
        self.assertEqual(ch.recv(), b"one")

    def test_abort_frees_the_reservation(self) -> None:
        ch = self.make_channel("abort", capacity=2, slot_size=64)
        buf = ch.alloc(8)
        buf.write(b"garbage!")
        ch.abort(buf)

        with self.assertRaises(ValueError):
            buf.write(b"x")
        with self.assertRaises(ValueError):
            ch.publish(buf)

        ch.send(b"hello")
        seq, payload = ch.recv_with_seq()
        self.assertEqual((seq, payload), (0, b"hello"))

    def test_cross_process_zero_copy_roundtrip(self) -> None:
        ch = self.make_channel("cross-process", capacity=8, slot_size=256)
        messages = [f"payload-{i}".encode() for i in range(4)]

        ctx = mp.get_context("spawn")
        result_queue = ctx.Queue()
        producer = ctx.Process(
            target=_zero_copy_producer,
            args=(ch.name, 8, 256, messages, result_queue),
        )
        producer.start()
        self.addCleanup(producer.join, 10)

        received = []
        while len(received) < len(messages):
            try:
                with ch.recv_view() as view:
                    received.append((view.seq, view.read_all()))
            except dsline.BufferEmptyError:
                time.sleep(0.001)

        status, seqs = result_queue.get(timeout=10)
        self.assertEqual(status, "ok")
        self.assertEqual([seq for seq, _ in received], seqs)
        self.assertEqual([payload for _, payload in received], messages)

    def test_cross_process_pin_survives_concurrent_producer(self) -> None:
        ch = self.make_channel(
            "cross-pin", capacity=4, slot_size=64, backpressure=dsline.Backpressure.Raise
        )

        ctx = mp.get_context("spawn")
        result_queue = ctx.Queue()
        producer = ctx.Process(
            target=_pinned_ring_producer,
            args=(ch.name, 4, 64, 6, result_queue),
        )
        producer.start()
        self.addCleanup(producer.join, 15)

        # Pin the first message as soon as it arrives: the child's ring
        # then cannot wrap onto that slot, so its next send blocks even
        # after the rest of the ring drains.
        view = None
        deadline = time.monotonic() + 120
        while view is None:
            try:
                view = ch.recv_view()
            except dsline.BufferEmptyError:
                if time.monotonic() > deadline:
                    self.fail("producer never delivered the first message")
                time.sleep(0.001)
        self.assertEqual(view.read_all(), b"message-0")

        # Drain the rest of the first batch (messages 1-3).
        received = [b"message-0"]
        while len(received) < 4:
            try:
                received.append(ch.recv())
            except dsline.BufferEmptyError:
                time.sleep(0.001)

        # The child is now spinning on a full ring: the next claimable
        # cursor is the pinned slot itself. Give it time to hammer the
        # guard, then verify the pinned bytes were never overwritten.
        time.sleep(0.3)
        self.assertEqual(view.read_all(), b"message-0")

        # Releasing the pin un-blocks the child's remaining sends.
        view.release()
        while len(received) < 6:
            try:
                received.append(ch.recv())
            except dsline.BufferEmptyError:
                time.sleep(0.001)

        status, sent = result_queue.get(timeout=60)
        self.assertEqual(status, "ok")
        self.assertEqual(sent, 6)
        self.assertEqual(received, [f"message-{i}".encode() for i in range(6)])


class ZeroCopyFileChannelTests(unittest.TestCase):
    def make_file_channel(self, label, **kwargs):
        path = os.path.join(
            tempfile.mkdtemp(prefix="dsline-zero-copy-"), f"{label}-{uuid.uuid4().hex[:8]}.bin"
        )
        ch = dsline.FileChannel.create(path, capacity=4, slot_size=256, **kwargs)
        self.addCleanup(remove_region, ch)
        return ch

    def test_file_channel_zero_copy_roundtrip(self) -> None:
        ch = self.make_file_channel("file-roundtrip")
        payload = b"file-backed zero copy"

        buf = ch.alloc(len(payload))
        buf.write(payload)
        seq = ch.publish(buf)
        self.assertEqual(seq, 0)

        with ch.recv_view() as view:
            self.assertEqual(view.read_all(), payload)
        self.assertEqual(ch.stats()["pinned_slots"], 0)

    def test_file_channel_view_partial_read(self) -> None:
        ch = self.make_file_channel("file-partial")
        buf = ch.alloc(16)
        buf.write(b"0123456789abcdef")
        ch.publish(buf)

        view = ch.recv_view()
        self.assertEqual(view.read(0, 4), b"0123")
        self.assertEqual(view[4:8], b"4567")
        view.release()


if __name__ == "__main__":
    unittest.main()
