import json
import multiprocessing as mp
import platform
import time
from collections.abc import Callable
from typing import Any


def bench_dsline(message: bytes, count: int, capacity: int) -> dict[str, Any]:
    import dsline

    ch = dsline.ShmChannel(
        "bench-shm-spsc-bytes",
        capacity=capacity,
        slot_size=len(message),
        backpressure=dsline.Backpressure.Raise,
    )
    started = time.perf_counter()
    remaining = count
    while remaining:
        batch = min(capacity, remaining)
        for _ in range(batch):
            ch.send(message)
        for _ in range(batch):
            payload = ch.recv()
            if payload != message:
                raise RuntimeError("dsline payload verification failed")
        remaining -= batch
    elapsed = time.perf_counter() - started
    ch.close()

    return make_result(
        benchmark="shm_spsc_bytes",
        backend="dsline-inprocess-prototype",
        message_size=len(message),
        count=count,
        elapsed_s=elapsed,
        copy_count=1,
    )


def bench_queue(message: bytes, count: int, capacity: int) -> dict[str, Any]:
    queue: mp.Queue[bytes] = mp.Queue(maxsize=capacity)
    started = time.perf_counter()
    remaining = count
    while remaining:
        batch = min(capacity, remaining)
        for _ in range(batch):
            queue.put(message)
        for _ in range(batch):
            payload = queue.get()
            if payload != message:
                raise RuntimeError("multiprocessing.Queue payload verification failed")
        remaining -= batch
    elapsed = time.perf_counter() - started
    queue.close()
    queue.join_thread()

    return make_result(
        benchmark="queue_bytes",
        backend="multiprocessing.Queue-inprocess-baseline",
        message_size=len(message),
        count=count,
        elapsed_s=elapsed,
        copy_count=None,
    )


def run_shm_bench(
    *,
    message_size: int,
    count: int,
    capacity: int,
    backend: str,
) -> list[dict[str, Any]]:
    validate_positive("message_size", message_size)
    validate_positive("count", count)
    validate_positive("capacity", capacity)
    message = bytes([0x61]) * message_size

    benches: list[Callable[[bytes, int, int], dict[str, Any]]] = []
    if backend in ("dsline", "both"):
        benches.append(bench_dsline)
    if backend in ("multiprocessing-queue", "both"):
        benches.append(bench_queue)
    if not benches:
        raise ValueError(f"unsupported backend: {backend}")

    return [bench(message, count, capacity) for bench in benches]


def make_result(
    *,
    benchmark: str,
    backend: str,
    message_size: int,
    count: int,
    elapsed_s: float,
    copy_count: int | None,
) -> dict[str, Any]:
    total_bytes = message_size * count
    throughput_msg_s = count / elapsed_s if elapsed_s else 0.0
    throughput_gib_s = total_bytes / elapsed_s / (1024**3) if elapsed_s else 0.0
    return {
        "benchmark": benchmark,
        "backend": backend,
        "message_size": message_size,
        "count": count,
        "elapsed_s": elapsed_s,
        "throughput_msg_s": throughput_msg_s,
        "throughput_gib_s": throughput_gib_s,
        "copy_count": copy_count,
        "platform": platform.platform(),
        "python": platform.python_version(),
    }


def format_results(results: list[dict[str, Any]], json_lines: bool) -> str:
    if json_lines:
        return "\n".join(json.dumps(result, sort_keys=True) for result in results)

    chunks: list[str] = []
    for result in results:
        chunks.append("\n".join(f"{key}: {value}" for key, value in result.items()))
    return "\n\n".join(chunks)


def validate_positive(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
