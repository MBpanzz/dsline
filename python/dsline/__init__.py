"""Python API for dsline."""

from __future__ import annotations

from . import ops
from ._dsline import (
    Backpressure,
    BufferEmptyError,
    BufferFullError,
    ChannelClosedError,
    ChannelError,
    CorruptedMessageError,
    DslineError,
    FileChannel,
    MessageTooLargeError,
    PipelineBuildError,
    PyPipeline as _Pipeline,
    SequenceMismatchError,
    ShmChannel,
    __version__,
)

__all__ = [
    "Backpressure",
    "BufferEmptyError",
    "BufferFullError",
    "ChannelClosedError",
    "ChannelError",
    "CorruptedMessageError",
    "DslineError",
    "FileChannel",
    "MessageTooLargeError",
    "Pipeline",
    "PipelineBuildError",
    "SequenceMismatchError",
    "ShmChannel",
    "ops",
    "__version__",
]


class Pipeline:
    """Declarative pipeline: source -> operators -> sink.

    Composes the Rust pull engine behind the roadmap's chaining API::

        import dsline
        from dsline import ops

        p = dsline.Pipeline()
        p.from_transport("shm://sensor").pipe(
            ops.filter_expr("temperature > 20"),
            ops.map_expr("temperature * 1.8 + 32"),
            ops.batch(256),
        ).sink("shm://output")
        p.run()

    ``run()`` drains the transport source (or an explicit ``source``
    iterable), pushes every output item through the operators, and sends
    each result into the sink channel as ``str(item).encode()`` bytes.

    The plain method names (``filter_expr``, ``map_expr``, ``batch``,
    ``select``, ``filter_py``, ``map_py``, ...) remain available and chain.
    """

    def __init__(self) -> None:
        self._inner = _Pipeline()
        self._sink = None  # type: ShmChannel | FileChannel | None

    # ── sources ──

    def from_transport(self, url: str) -> "Pipeline":
        """Use a transport URL as the source (``shm://`` or ``file://``)."""
        self._inner = _Pipeline.from_transport(url)
        return self

    # ── operators ──

    def pipe(self, *ops) -> "Pipeline":
        """Append deferred operators (``dsline.ops`` factory results)."""
        for op in ops:
            self._apply(op)
        return self

    def _apply(self, op) -> None:
        if isinstance(op, ops.FilterExpr):
            self._inner.filter_expr(op.expr)
        elif isinstance(op, ops.MapExpr):
            self._inner.map_expr(op.expr)
        elif isinstance(op, ops.FilterPy):
            self._inner.filter_py(op.fn)
        elif isinstance(op, ops.MapPy):
            self._inner.map_py(op.fn)
        elif isinstance(op, ops.FilterPyBatch):
            self._inner.filter_py_batch(op.fn)
        elif isinstance(op, ops.MapPyBatch):
            self._inner.map_py_batch(op.fn)
        elif isinstance(op, ops.Batch):
            self._inner.batch(op.size)
        elif isinstance(op, ops.Select):
            self._inner.select(op.columns)
        else:
            raise TypeError(f"not a dsline operator: {op!r}")

    # ── sinks and execution ──

    def sink(self, url: str) -> "Pipeline":
        """Send results to a transport URL (``shm://`` or ``file://``)."""
        scheme, target, params = _Pipeline.parse_transport_url(url)
        capacity = int(params.get("capacity", "64"))
        slot_size = int(params.get("slot_size", "4096"))
        multi = str(params.get("multi_producer", "false")).lower() in (
            "1",
            "true",
            "yes",
        )
        if scheme == "shm":
            self._sink = ShmChannel(
                target,
                capacity=capacity,
                slot_size=slot_size,
                multi_producer=multi,
            )
        elif scheme == "file":
            self._sink = FileChannel.open(
                target,
                capacity=capacity,
                slot_size=slot_size,
                multi_producer=multi,
            )
        else:
            raise ValueError(
                f"unsupported sink scheme '{scheme}'; supported: shm, file"
            )
        return self

    def run(self, source=None) -> int:
        """Execute the pipeline and send every result to the sink.

        With no ``source``, the transport source configured by
        ``from_transport`` is drained. Returns the number of items sent.
        """
        if self._sink is None:
            raise RuntimeError("pipeline has no sink; call .sink(url) first")
        items = list(self._inner.collect(source))
        for item in items:
            if isinstance(item, (bytes, bytearray, memoryview)):
                data = bytes(item)
            else:
                data = str(item).encode("utf-8")
            self._sink.send(data)
        return len(items)

    # ── parity with the Rust pipeline (chainable) ──

    def filter_expr(self, expr: str) -> "Pipeline":
        self._inner.filter_expr(expr)
        return self

    def map_expr(self, expr: str) -> "Pipeline":
        self._inner.map_expr(expr)
        return self

    def filter_py(self, fn) -> "Pipeline":
        self._inner.filter_py(fn)
        return self

    def map_py(self, fn) -> "Pipeline":
        self._inner.map_py(fn)
        return self

    def filter_py_batch(self, fn) -> "Pipeline":
        self._inner.filter_py_batch(fn)
        return self

    def map_py_batch(self, fn) -> "Pipeline":
        self._inner.map_py_batch(fn)
        return self

    def batch(self, size: int) -> "Pipeline":
        self._inner.batch(size)
        return self

    def select(self, columns) -> "Pipeline":
        self._inner.select(columns)
        return self

    # ── parity with the Rust pipeline (plain) ──

    def collect(self, source=None) -> list:
        """Apply the operators to `source` (or the transport source)."""
        return list(self._inner.collect(source))

    def send_to(self, source, channel) -> int:
        """Apply the operators to `source` and send results into `channel`."""
        return self._inner.send_to(source, channel)

    def receive_from(self, channel) -> list:
        """Drain `channel`, apply the operators, return the results."""
        return list(self._inner.receive_from(channel))

    def __repr__(self) -> str:
        return repr(self._inner)