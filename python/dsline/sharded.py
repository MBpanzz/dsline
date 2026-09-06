"""Sharded multi-consumer channel (roadmap §5.5 分片 MPMC).

Composes the Rust-backed named channels into an N-way sharded fan-out:
`N 个 SPSC/MPSC channel + 路由层`. One producer-side handle routes every
message into one shard (round-robin or hash) or into all of them
(broadcast); each consumer drains one shard, which gives multi-consumer
delivery without a lock-free MPMC ring:

    import dsline

    fan = dsline.ShardedChannel("events", shards=4, strategy="round_robin")
    fan.send(b"m1")
    fan.send(b"m2", key=b"user-42")   # hash routing uses the key

    # another process: a consumer binds one shard
    fan = dsline.ShardedChannel("events", shards=4, strategy="round_robin")
    shard = fan.shard(2)
    shard.recv()

Strategies:

* ``round_robin`` — each producer handle spreads sends across the shards
  with a local counter (per-producer fairness; the shards together see the
  whole stream).
* ``hash`` — ``FNV-1a-32(key or payload) % shards``: stateless, so every
  process routes the same key to the same shard (per-key ordering is
  preserved because a shard is FIFO).
* ``broadcast`` — every shard receives every message; each consumer bound
  to one shard sees the whole stream (a shard per consumer group).

All processes must agree on ``name``, ``shards``, ``capacity`` and
``slot_size`` — the shards are ordinary named channels underneath, so a
capacity/slot_size mismatch fails at open.
"""

from __future__ import annotations

from ._dsline import Backpressure, BufferEmptyError, ShmChannel
from ._shm import _checksum32

STRATEGIES = ("round_robin", "hash", "broadcast")


def shard_for(key: bytes, shards: int) -> int:
    """The shard a routing key lands on: ``FNV-1a-32(key) % shards``.

    Uses the project's FNV-1a checksum32 (the same function the name →
    region mapping uses), so the routing decision is a pure function of the
    key and the shard count — stateless across processes.
    """
    if shards <= 0:
        raise ValueError("shards must be positive")
    return _checksum32(bytes(key)) % shards


class ShardedChannel:
    """A producer/consumer handle over ``shards`` named shard channels."""

    def __init__(
        self,
        name: str,
        *,
        shards: int = 4,
        capacity: int = 64,
        slot_size: int = 4096,
        backpressure=Backpressure.Block,
        timeout: float | None = None,
        multi_producer: bool = False,
        strategy: str = "round_robin",
    ) -> None:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("channel name must not be empty")
        if not isinstance(shards, int) or shards < 1:
            raise ValueError("shards must be a positive integer")
        if strategy not in STRATEGIES:
            raise ValueError(
                f"unknown strategy '{strategy}'; supported: {', '.join(STRATEGIES)}"
            )
        self._name = name
        self._shards_count = shards
        self._strategy = strategy
        self._rr_counter = 0  # per-producer round-robin counter
        self._recv_rr = 0  # recv_any poll rotation
        self._shard_channels: list[ShmChannel] = [
            ShmChannel(
                _shard_name(name, index),
                capacity=capacity,
                slot_size=slot_size,
                backpressure=backpressure,
                timeout=timeout,
                multi_producer=multi_producer,
            )
            for index in range(shards)
        ]

    # ── producer side ──

    def send(self, data, key: bytes | None = None) -> int:
        """Route one message per the strategy; returns the shard count used.

        ``round_robin``/``hash`` deliver to exactly one shard and return 1;
        ``broadcast`` delivers to every shard and returns ``shards``. With
        ``hash`` routing the shard is chosen from ``key`` when given, else
        from the payload bytes.
        """
        if self._strategy == "round_robin":
            index = self._rr_counter % self._shards_count
            self._rr_counter += 1
            self._shard_channels[index].send(data)
            return 1
        if self._strategy == "hash":
            routing_key = bytes(key) if key is not None else bytes(data)
            index = shard_for(routing_key, self._shards_count)
            self._shard_channels[index].send(data)
            return 1
        # broadcast: every shard gets every message.
        for shard in self._shard_channels:
            shard.send(data)
        return self._shards_count

    # ── consumer side ──

    def shard(self, index: int) -> ShmChannel:
        """The named channel backing shard ``index`` (a consumer binds one)."""
        return self._shard_channels[self._check_index(index)]

    def shards(self) -> list[ShmChannel]:
        """All shard channels (a broadcast reader drains them in order)."""
        return list(self._shard_channels)

    def recv_any(self) -> tuple[int, bytes] | None:
        """Poll every shard once (round-robin) for one message.

        Returns ``(shard_index, payload)`` from the first shard that has a
        message, or ``None`` when all shards are empty. Only meaningful for
        ``round_robin``/``hash`` fan-outs — a broadcast message exists on
        every shard, so a ``recv_any`` reader would see it once per shard.
        """
        for index in self._poll_order():
            try:
                return index, self._shard_channels[index].recv()
            except BufferEmptyError:
                continue
        return None

    def _poll_order(self) -> list[int]:
        base = self._recv_rr % self._shards_count
        self._recv_rr += 1
        return [(base + i) % self._shards_count for i in range(self._shards_count)]

    # ── shared surface ──

    def close(self) -> None:
        for shard in self._shard_channels:
            shard.close()

    def __enter__(self) -> "ShardedChannel":
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> bool:
        self.close()
        return False

    @property
    def name(self) -> str:
        return self._name

    @property
    def shards_count(self) -> int:
        return self._shards_count

    @property
    def strategy(self) -> str:
        return self._strategy

    def stats(self) -> list[dict]:
        """Per-shard stats, one dict per shard (in shard order)."""
        return [shard.stats() for shard in self._shard_channels]

    def _check_index(self, index: int) -> int:
        if not isinstance(index, int) or not 0 <= index < self._shards_count:
            raise IndexError(f"shard index out of range: {index!r}")
        return index

    def __repr__(self) -> str:
        return (
            f"ShardedChannel({self._name!r}, shards={self._shards_count}, "
            f"strategy={self._strategy!r})"
        )


def _shard_name(name: str, index: int) -> str:
    return f"{name}#shard{index}"


__all__ = ["STRATEGIES", "ShardedChannel", "shard_for"]
