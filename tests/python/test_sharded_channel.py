"""Tests for the sharded multi-consumer channel (roadmap §5.5)."""

import gc
import multiprocessing as mp
import os
import time
import unittest
import uuid

import dsline
from dsline.sharded import shard_for


def unique_name(label: str) -> str:
    return f"test-{label}-{uuid.uuid4().hex[:12]}"


def remove_region(channel) -> None:
    """Close a channel and drop it so its mapping is freed (Windows)."""
    channel.close()
    del channel
    gc.collect()


def _shard_consumer(name, shards, capacity, slot_size, shard_index, count, result_queue):
    """Child process worker: drain one shard until `count` messages arrive.

    The shards are multi-producer regions (the producers route with
    `multi_producer=True`), and every opener must agree on the mode, so the
    consumer opens its shard handle with the same flag. A 20 s deadline
    bounds the spin so a broken flow reports partial results instead of
    hanging the suite.
    """
    fan = dsline.ShardedChannel(
        name, shards=shards, capacity=capacity, slot_size=slot_size,
        multi_producer=True,
    )
    shard = fan.shard(shard_index)
    received = []
    deadline = time.monotonic() + 20
    while len(received) < count and time.monotonic() < deadline:
        try:
            received.append(shard.recv())
        except dsline.BufferEmptyError:
            continue
    fan.close()
    result_queue.put(received)


def _sharded_producer(name, shards, capacity, slot_size, label, count, result_queue):
    """Child process worker: send `count` uniquely-labeled messages."""
    fan = dsline.ShardedChannel(
        name, shards=shards, capacity=capacity, slot_size=slot_size,
        multi_producer=True,
    )
    for i in range(count):
        fan.send(f"{label}-{i}".encode())
    fan.close()
    result_queue.put("done")


# Cross-process waits are generous: a CI runner has 2-4 cores and a cold
# page cache, and each spawned process pays a fresh interpreter start
# plus the extension import.

class ShardedChannelTests(unittest.TestCase):
    def make_fanout(self, label, **kwargs):
        kwargs.setdefault("shards", 3)
        kwargs.setdefault("capacity", 8)
        kwargs.setdefault("slot_size", 64)
        fan = dsline.ShardedChannel(unique_name(label), **kwargs)
        self.addCleanup(remove_region, fan)
        return fan

    def test_round_robin_spreads_and_preserves_per_shard_order(self) -> None:
        fan = self.make_fanout("rr")
        for i in range(6):
            fan.send(f"m{i}".encode())
        # 6 messages over 3 shards, per-producer round-robin: shard i holds
        # m_i and m_{i+3}, in FIFO order.
        drained = []
        for i in range(3):
            shard = fan.shard(i)
            drained.append(shard.recv())
            drained.append(shard.recv())
            with self.assertRaises(dsline.BufferEmptyError):
                shard.recv()
        self.assertEqual(drained, [b"m0", b"m3", b"m1", b"m4", b"m2", b"m5"])

    def test_send_returns_the_shard_count_used(self) -> None:
        fan = self.make_fanout("ret")
        self.assertEqual(fan.send(b"x"), 1)
        fan_b = self.make_fanout("ret-b", strategy="broadcast", shards=2)
        self.assertEqual(fan_b.send(b"x"), 2)

    def test_hash_routing_is_deterministic_and_per_key_fifo(self) -> None:
        fan = self.make_fanout("hash", shards=4, strategy="hash")
        # The same key always routes to the same shard, computed by the
        # pure FNV-1a-32 % shards function.
        shard_index = shard_for(b"user-42", 4)
        self.assertEqual(shard_for(b"user-42", 4), shard_index)
        fan.send(b"first", key=b"user-42")
        fan.send(b"second", key=b"other")
        fan.send(b"third", key=b"user-42")
        # Both user-42 messages landed on one shard, in FIFO order; the
        # other message went wherever its key hashed (possibly the same).
        shard = fan.shard(shard_index)
        got = []
        while True:
            try:
                got.append(shard.recv())
            except dsline.BufferEmptyError:
                break
        self.assertIn(b"first", got)
        self.assertIn(b"third", got)
        self.assertLess(got.index(b"first"), got.index(b"third"))

    def test_broadcast_reaches_every_shard(self) -> None:
        fan = self.make_fanout("bc", shards=3, strategy="broadcast")
        fan.send(b"m1")
        fan.send(b"m2")
        for i in range(3):
            shard = fan.shard(i)
            self.assertEqual(shard.recv(), b"m1")
            self.assertEqual(shard.recv(), b"m2")

    def test_recv_any_polls_all_shards(self) -> None:
        fan = self.make_fanout("any", shards=3)
        self.assertIsNone(fan.recv_any())
        # Route one message to shard 2 (the third round-robin target).
        fan.send(b"a")
        fan.send(b"b")
        fan.send(b"c")
        hits = set()
        for _ in range(3):
            result = fan.recv_any()
            self.assertIsNotNone(result)
            hits.add(result[0])
        self.assertEqual(hits, {0, 1, 2})
        self.assertIsNone(fan.recv_any())

    def test_stats_and_properties(self) -> None:
        fan = self.make_fanout("stats", shards=2, strategy="hash")
        self.assertEqual(fan.shards_count, 2)
        self.assertEqual(fan.strategy, "hash")
        self.assertEqual(len(fan.stats()), 2)
        self.assertEqual(fan.stats()[0]["queue_depth"], 0)
        self.assertIn("shard0", fan.shard(0).name)

    def test_invalid_configuration_rejected(self) -> None:
        with self.assertRaises(ValueError):
            dsline.ShardedChannel(unique_name("bad"), shards=0)
        with self.assertRaises(ValueError):
            dsline.ShardedChannel(unique_name("bad"), strategy="random")
        with self.assertRaises(ValueError):
            dsline.ShardedChannel("")
        fan = self.make_fanout("idx")
        with self.assertRaises(IndexError):
            fan.shard(99)

    def test_context_manager_closes_all_shards(self) -> None:
        name = unique_name("ctx")
        with dsline.ShardedChannel(name, shards=2, capacity=4, slot_size=32) as fan:
            fan.send(b"x")
            self.assertTrue(all(s.closed for s in fan.shards()) is False)
        self.assertTrue(all(s.closed for s in fan.shards()))


class ShardedCrossProcessTests(unittest.TestCase):
    def _ctx(self):
        return mp.get_context("spawn")

    def test_two_producers_three_consumers_exactly_once(self) -> None:
        """The load-bearing property: N producers route through their local
        round-robin counters, M consumers each drain one shard, and the
        union of the deliveries is exactly the sent multiset — no message
        lost, no message duplicated (bounded-checked by C11, replayed
        here through the real implementation)."""
        ctx = self._ctx()
        name = unique_name("xproc")
        shards, per_producer = 3, 12
        result_queue = ctx.Queue()
        consumers = [
            ctx.Process(
                target=_shard_consumer,
                args=(name, shards, 16, 64, i, 2 * per_producer // shards, result_queue),
            )
            for i in range(shards)
        ]
        producers = [
            ctx.Process(
                target=_sharded_producer,
                args=(name, shards, 16, 64, label, per_producer, result_queue),
            )
            for label in ("A", "B")
        ]
        for process in consumers + producers:
            process.start()
        results = [result_queue.get(timeout=60) for _ in range(shards + 2)]
        for process in consumers + producers:
            process.join(timeout=60)
            self.assertEqual(process.exitcode, 0)
        delivered = [r for r in results if isinstance(r, list)]
        self.assertEqual(len(delivered), shards)
        union = [msg for shard_msgs in delivered for msg in shard_msgs]
        # Payloads are unique per message, so "no duplicate payloads" plus
        # the exact count is exactly-once delivery.
        self.assertEqual(len(union), len(set(union)), "no duplicate deliveries")
        self.assertEqual(len(union), 2 * per_producer)
        expected = {f"{label}-{i}".encode() for label in ("A", "B") for i in range(per_producer)}
        self.assertEqual(set(union), expected)

    def test_broadcast_consumer_per_shard_sees_everything(self) -> None:
        ctx = self._ctx()
        name = unique_name("bc")
        shards, count = 3, 5
        result_queue = ctx.Queue()
        consumers = [
            ctx.Process(
                target=_shard_consumer,
                args=(name, shards, 8, 64, i, count, result_queue),
            )
            for i in range(shards)
        ]
        for process in consumers:
            process.start()
        producer = ctx.Process(
            target=_bc_producer, args=(name, shards, 8, 64, count, result_queue)
        )
        producer.start()
        results = [result_queue.get(timeout=60) for _ in range(shards + 1)]
        for process in consumers + [producer]:
            process.join(timeout=60)
            self.assertEqual(process.exitcode, 0)
        delivered = [r for r in results if isinstance(r, list)]
        self.assertEqual(len(delivered), shards)
        expected = [f"bc-{i}".encode() for i in range(count)]
        for shard_msgs in delivered:
            # Every consumer (one per shard) sees every broadcast message.
            self.assertEqual(shard_msgs, expected)


def _bc_producer(name, shards, capacity, slot_size, count, result_queue):
    # The consumers created the shards as multi-producer regions; every
    # opener must agree on the mode.
    fan = dsline.ShardedChannel(
        name, shards=shards, capacity=capacity, slot_size=slot_size,
        strategy="broadcast", multi_producer=True,
    )
    for i in range(count):
        fan.send(f"bc-{i}".encode())
    fan.close()
    result_queue.put("done")


if __name__ == "__main__":
    unittest.main()
