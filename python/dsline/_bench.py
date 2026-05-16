import json
import multiprocessing as mp
import platform
import time
from collections.abc import Callable
from typing import Any

# ── shared helpers ──


def make_result(
    *,
    benchmark: str,
    backend: str,
    message_size: int | None,
    count: int,
    elapsed_s: float,
    copy_count: int | None = None,
    batch_size: int | None = None,
) -> dict[str, Any]:
    total_bytes = (message_size or 1) * count
    throughput_msg_s = count / elapsed_s if elapsed_s else 0.0
    throughput_gib_s = total_bytes / elapsed_s / (1024**3) if elapsed_s else 0.0
    result: dict[str, Any] = {
        "benchmark": benchmark,
        "backend": backend,
        "count": count,
        "elapsed_s": elapsed_s,
        "throughput_msg_s": throughput_msg_s,
        "throughput_gib_s": throughput_gib_s,
        "platform": platform.platform(),
        "python": platform.python_version(),
    }
    if copy_count is not None:
        result["copy_count"] = copy_count
    if message_size is not None:
        result["message_size"] = message_size
    if batch_size is not None:
        result["batch_size"] = batch_size
    return result


def validate_positive(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")


# ── channel benchmarks ──


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


# ── pipeline benchmarks ──

# Produce a stream of dicts that simulate sensor readings.
def _sensor_stream(count: int) -> list[dict[str, float]]:
    return [{"temperature": 20.0 + (i % 30), "humidity": 50.0 + (i % 40)} for i in range(count)]


def bench_pipeline_expr(count: int, batch_size: int | None) -> dict[str, Any]:
    """dsline Pipeline with Rust filter_expr + map_expr (fast path)."""
    import dsline

    data = _sensor_stream(count)

    p = dsline.Pipeline()
    p.filter_expr("temperature > 25 and humidity < 80")
    p.map_expr("temperature * 1.8 + 32")
    if batch_size:
        p.batch(batch_size)

    started = time.perf_counter()
    result = p.collect(data)
    elapsed = time.perf_counter() - started
    _ = len(result)  # force materialisation

    return make_result(
        benchmark="pipeline_filter_map_expr",
        backend="dsline-pipeline-rust-ops",
        message_size=None,
        count=count,
        elapsed_s=elapsed,
        batch_size=batch_size,
    )


def bench_pipeline_py(count: int, batch_size: int | None) -> dict[str, Any]:
    """dsline Pipeline with Python UDF map + filter (slow path)."""
    import dsline

    data = _sensor_stream(count)

    p = dsline.Pipeline()
    p.filter_py(lambda d: d["temperature"] > 25 and d["humidity"] < 80)
    p.map_py(lambda d: d["temperature"] * 1.8 + 32)
    if batch_size:
        p.batch(batch_size)

    started = time.perf_counter()
    result = p.collect(data)
    elapsed = time.perf_counter() - started
    _ = len(result)

    return make_result(
        benchmark="pipeline_filter_map_py",
        backend="dsline-pipeline-python-udf",
        message_size=None,
        count=count,
        elapsed_s=elapsed,
        batch_size=batch_size,
    )


def bench_pure_python(count: int) -> dict[str, Any]:
    """Pure Python list comprehension baseline."""
    data = _sensor_stream(count)

    started = time.perf_counter()
    result = [
        d["temperature"] * 1.8 + 32
        for d in data
        if d["temperature"] > 25 and d["humidity"] < 80
    ]
    elapsed = time.perf_counter() - started
    _ = len(result)

    return make_result(
        benchmark="pipeline_filter_map",
        backend="pure-python-list-comprehension",
        message_size=None,
        count=count,
        elapsed_s=elapsed,
    )


def run_pipeline_bench(
    *,
    count: int,
    backends: str,
    batch_sizes: list[int] | None = None,
) -> list[dict[str, Any]]:
    validate_positive("count", count)
    results: list[dict[str, Any]] = []

    if backends in ("pure-python", "all"):
        results.append(bench_pure_python(count))

    if backends in ("dsline-expr", "all"):
        results.append(bench_pipeline_expr(count, batch_size=None))
        if batch_sizes:
            for bs in batch_sizes:
                results.append(bench_pipeline_expr(count, batch_size=bs))

    if backends in ("dsline-py-udf", "all"):
        results.append(bench_pipeline_py(count, batch_size=None))
        if batch_sizes:
            for bs in batch_sizes:
                results.append(bench_pipeline_py(count, batch_size=bs))

    return results


# ── formatting ──


def format_results(results: list[dict[str, Any]], json_lines: bool) -> str:
    if json_lines:
        return "\n".join(json.dumps(result, sort_keys=True) for result in results)

    chunks: list[str] = []
    for result in results:
        chunks.append("\n".join(f"{key}: {value}" for key, value in result.items()))
    return "\n\n".join(chunks)
