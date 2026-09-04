"""Comprehensive benchmark: dsline vs common Python libraries.

Compares:
  1. Channel throughput: dsline vs mp.Queue vs mp.Pipe
  2. Pipeline processing: dsline vs pure Python vs pandas vs pyarrow
  3. Batch-size scaling for Python UDFs

Usage:
  python benches/full_comparison.py [--count N] [--json]
"""

import argparse
import json
import multiprocessing as mp
import platform
import sys
import time
from collections.abc import Callable
from typing import Any


# ═══════════════════════════════════════════════════════════════════
# helpers
# ═══════════════════════════════════════════════════════════════════

def _fmt(n: float) -> str:
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.0f}K"
    return str(int(n))


def _throughput(count: int, elapsed: float) -> float:
    return count / elapsed if elapsed > 0 else 0.0


def _run_bench(name: str, fn: Callable[[], Any], count: int) -> dict[str, Any]:
    started = time.perf_counter()
    fn()
    elapsed = time.perf_counter() - started
    return {
        "benchmark": name,
        "count": count,
        "elapsed_s": round(elapsed, 6),
        "throughput_msg_s": round(_throughput(count, elapsed)),
    }


# ═══════════════════════════════════════════════════════════════════
# 1. Channel throughput
# ═══════════════════════════════════════════════════════════════════

def _channel_roundtrip(send_fn, recv_fn, count: int, capacity: int):
    """Interleave send/recv in batches to avoid buffer-full stalls."""
    remaining = count
    while remaining:
        batch = min(capacity, remaining)
        for _ in range(batch):
            send_fn()
        for _ in range(batch):
            recv_fn()
        remaining -= batch


def bench_channel_dsline(count: int, capacity: int, message: bytes):
    import dsline

    ch = dsline.ShmChannel("bench-ch", capacity=capacity, slot_size=len(message),
                           backpressure=dsline.Backpressure.Raise)

    def send():
        ch.send(message)

    def recv():
        data = ch.recv()
        assert data == message

    _channel_roundtrip(send, recv, count, capacity)
    ch.close()


def bench_channel_queue(count: int, capacity: int, message: bytes):
    q: mp.Queue[bytes] = mp.Queue(maxsize=capacity)

    def send():
        q.put(message)

    def recv():
        data = q.get()
        assert data == message

    _channel_roundtrip(send, recv, count, capacity)
    q.close()
    q.join_thread()


def bench_channel_pipe(count: int, capacity: int, message: bytes):
    r, w = mp.Pipe(duplex=False)

    def send():
        w.send(message)

    def recv():
        data = r.recv()
        assert data == message

    _channel_roundtrip(send, recv, count, capacity)
    r.close()
    w.close()


def run_channel_benches(count: int) -> list[dict[str, Any]]:
    capacity = 1024
    results: list[dict[str, Any]] = []

    for size in [64, 256, 1024, 4096, 65536]:
        msg = b"x" * size
        size_label = _fmt(size)

        results.append(_run_bench(
            f"channel_bytes_dsline_{size_label}B",
            lambda c=count, cap=capacity, m=msg: bench_channel_dsline(c, cap, m), count))

        results.append(_run_bench(
            f"channel_bytes_queue_{size_label}B",
            lambda c=count, cap=capacity, m=msg: bench_channel_queue(c, cap, m), count))

        results.append(_run_bench(
            f"channel_bytes_pipe_{size_label}B",
            lambda c=count, m=msg: bench_channel_pipe(c, 1024, m), count))

    return results


# ═══════════════════════════════════════════════════════════════════
# 2. Pipeline / data processing
# ═══════════════════════════════════════════════════════════════════

def _gen_data(count: int) -> list[dict[str, float]]:
    return [
        {"temperature": 20.0 + (i % 30), "humidity": 50.0 + (i % 40),
         "pressure": 1000.0 + (i % 50)}
        for i in range(count)
    ]


def bench_pure_python(data: list[dict[str, float]]):
    result = [
        d["temperature"] * 1.8 + 32
        for d in data
        if d["temperature"] > 25 and d["humidity"] < 80
    ]
    return len(result)


def bench_dsline_expr(data: list[dict[str, float]]):
    import dsline

    p = dsline.Pipeline()
    p.filter_expr("temperature > 25 and humidity < 80")
    p.map_expr("temperature * 1.8 + 32")
    result = p.collect(data)
    return len(result)


def bench_dsline_py_udf(data: list[dict[str, float]]):
    import dsline

    p = dsline.Pipeline()
    p.filter_py(lambda d: d["temperature"] > 25 and d["humidity"] < 80)
    p.map_py(lambda d: d["temperature"] * 1.8 + 32)
    result = p.collect(data)
    return len(result)


def bench_pandas(data: list[dict[str, float]]):
    import pandas as pd

    df = pd.DataFrame(data)
    mask = (df["temperature"] > 25) & (df["humidity"] < 80)
    result = df.loc[mask, "temperature"] * 1.8 + 32
    return len(result)


def bench_pyarrow(data: list[dict[str, float]]):
    import pyarrow as pa
    import pyarrow.compute as pc

    table = pa.Table.from_pylist(data)
    mask = pc.and_(
        pc.greater(table.column("temperature"), 25),
        pc.less(table.column("humidity"), 80),
    )
    filtered = table.filter(mask)
    result = pc.add(pc.multiply(filtered.column("temperature"), 1.8), 32)
    return len(result)


def bench_pipe_batch_py(data: list[dict[str, float]], batch_size: int):
    import dsline

    p = dsline.Pipeline()
    p.batch(batch_size)
    p.map_py_batch(lambda d: d["temperature"] * 1.8 + 32)
    p.filter_py_batch(lambda v: v > 77.0)  # > 25°C in Fahrenheit
    result = p.collect(data)
    return len(result)


def run_pipeline_benches(count: int, batch_sizes: list[int] | None = None) -> list[dict[str, Any]]:
    data = _gen_data(count)
    results: list[dict[str, Any]] = []

    results.append(_run_bench("pipeline_pure_python", lambda: bench_pure_python(data), count))
    results.append(_run_bench("pipeline_dsline_expr", lambda: bench_dsline_expr(data), count))
    results.append(_run_bench("pipeline_dsline_py_udf", lambda: bench_dsline_py_udf(data), count))
    results.append(_run_bench("pipeline_pandas", lambda: bench_pandas(data), count))
    results.append(_run_bench("pipeline_pyarrow", lambda: bench_pyarrow(data), count))

    if batch_sizes:
        for bs in batch_sizes:
            results.append(_run_bench(
                f"pipeline_dsline_batch_{bs}",
                lambda bs=bs: bench_pipe_batch_py(data, bs), count))

    return results


# ═══════════════════════════════════════════════════════════════════
# 3. Batch-size scaling for Python UDFs
# ═══════════════════════════════════════════════════════════════════

def bench_batch_udf(data: list[dict[str, float]], batch_size: int | None):
    import dsline

    p = dsline.Pipeline()
    if batch_size:
        p.batch(batch_size)
        p.map_py_batch(lambda d: d["temperature"] * 1.8 + 32)
    else:
        p.map_py(lambda d: d["temperature"] * 1.8 + 32)
    result = p.collect(data)
    return len(result)


def run_batch_scaling_benches(count: int) -> list[dict[str, Any]]:
    data = _gen_data(count)
    results: list[dict[str, Any]] = []

    results.append(_run_bench("batch_udf_no_batch", lambda: bench_batch_udf(data, None), count))
    for bs in [1, 8, 32, 128, 512, 2048]:
        results.append(_run_bench(
            f"batch_udf_{bs}", lambda bs=bs: bench_batch_udf(data, bs), count))

    return results


# ═══════════════════════════════════════════════════════════════════
# main / formatting
# ═══════════════════════════════════════════════════════════════════

def format_table(results: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    header = f"{'Benchmark':<48} {'Count':>10} {'Elapsed(s)':>12} {'Throughput/s':>16}"
    lines.append(header)
    lines.append("-" * len(header))
    for r in results:
        lines.append(
            f"{r['benchmark']:<48} {r['count']:>10} {r['elapsed_s']:>12.6f} {r['throughput_msg_s']:>16,}"
        )
    return "\n".join(lines)


def format_json(results: list[dict[str, Any]]) -> str:
    return "\n".join(json.dumps(r) for r in results)


def main() -> int:
    parser = argparse.ArgumentParser(description="dsline full comparison benchmark")
    parser.add_argument("--count", type=int, default=200_000,
                        help="Items per benchmark (default: 200000)")
    parser.add_argument("--section", choices=("channel", "pipeline", "batch", "all"),
                        default="all")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--batch-sizes", type=int, nargs="*",
                        default=[16, 64, 256, 1024])
    args = parser.parse_args()

    count = args.count
    all_results: list[dict[str, Any]] = []

    print(f"dsline {__import__('dsline').__version__}  |  "
          f"Python {platform.python_version()}  |  "
          f"{platform.platform()}")
    print(f"Count per benchmark: {count:,}")
    print()

    if args.section in ("channel", "all"):
        print("=== Channel throughput ===")
        results = run_channel_benches(min(count, 50000))
        all_results.extend(results)
        print(format_table(results) if not args.json else format_json(results))
        print()

    if args.section in ("pipeline", "all"):
        print("=== Pipeline processing ===")
        results = run_pipeline_benches(count, args.batch_sizes)
        all_results.extend(results)
        print(format_table(results) if not args.json else format_json(results))
        print()

    if args.section in ("batch", "all"):
        print("=== Batch-size scaling ===")
        results = run_batch_scaling_benches(min(count, 50000))
        all_results.extend(results)
        print(format_table(results) if not args.json else format_json(results))
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
