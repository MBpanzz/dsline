"""Deferred operator specifications for `Pipeline.pipe(...)`.

These objects are pure data: they describe a stage without executing
anything. `Pipeline.pipe` applies them to the pipeline in order:

    from dsline import ops

    p = dsline.Pipeline()
    p.from_transport("shm://sensor").pipe(
        ops.filter_expr("temperature > 20"),
        ops.map_expr("temperature * 1.8 + 32"),
        ops.batch(256),
    )
"""

from __future__ import annotations


class _Operator:
    """Base class for deferred operator specifications."""

    __slots__ = ()

    def __repr__(self) -> str:
        fields = ", ".join(f"{k}={v!r}" for k, v in self._spec().items())
        return f"{type(self).__name__}({fields})"

    def _spec(self) -> dict:
        raise NotImplementedError


class FilterExpr(_Operator):
    """Keep items whose expr-lite expression evaluates truthy."""

    __slots__ = ("expr",)

    def __init__(self, expr: str) -> None:
        self.expr = expr

    def _spec(self) -> dict:
        return {"expr": self.expr}


class MapExpr(_Operator):
    """Replace each item with the value of an expr-lite expression."""

    __slots__ = ("expr",)

    def __init__(self, expr: str) -> None:
        self.expr = expr

    def _spec(self) -> dict:
        return {"expr": self.expr}


class FilterPy(_Operator):
    """Keep items for which the Python callable returns truthy (slow path)."""

    __slots__ = ("fn",)

    def __init__(self, fn) -> None:
        self.fn = fn

    def _spec(self) -> dict:
        return {"fn": self.fn}


class MapPy(_Operator):
    """Replace each item with the Python callable's result (slow path)."""

    __slots__ = ("fn",)

    def __init__(self, fn) -> None:
        self.fn = fn

    def _spec(self) -> dict:
        return {"fn": self.fn}


class FilterPyBatch(_Operator):
    """Drop batch elements for which the callable returns falsy."""

    __slots__ = ("fn",)

    def __init__(self, fn) -> None:
        self.fn = fn

    def _spec(self) -> dict:
        return {"fn": self.fn}


class MapPyBatch(_Operator):
    """Map each batch element through the callable."""

    __slots__ = ("fn",)

    def __init__(self, fn) -> None:
        self.fn = fn

    def _spec(self) -> dict:
        return {"fn": self.fn}


class Batch(_Operator):
    """Group items into batches of `size`; a trailing partial batch is kept."""

    __slots__ = ("size",)

    def __init__(self, size: int) -> None:
        self.size = size

    def _spec(self) -> dict:
        return {"size": self.size}


class Select(_Operator):
    """Keep only the named columns in each dict item."""

    __slots__ = ("columns",)

    def __init__(self, columns) -> None:
        self.columns = list(columns)

    def _spec(self) -> dict:
        return {"columns": self.columns}


def filter_expr(expr: str) -> FilterExpr:
    """Deferred `filter_expr` stage (expr-lite fast path)."""
    return FilterExpr(expr)


def map_expr(expr: str) -> MapExpr:
    """Deferred `map_expr` stage (expr-lite fast path)."""
    return MapExpr(expr)


def filter_py(fn) -> FilterPy:
    """Deferred `filter_py` stage (Python slow path)."""
    return FilterPy(fn)


def map_py(fn) -> MapPy:
    """Deferred `map_py` stage (Python slow path)."""
    return MapPy(fn)


def filter_py_batch(fn) -> FilterPyBatch:
    """Deferred per-element `filter_py` stage inside batches."""
    return FilterPyBatch(fn)


def map_py_batch(fn) -> MapPyBatch:
    """Deferred per-element `map_py` stage inside batches."""
    return MapPyBatch(fn)


def batch(size: int) -> Batch:
    """Deferred `batch` stage."""
    return Batch(size)


def select(columns) -> Select:
    """Deferred `select` stage."""
    return Select(columns)


__all__ = [
    "Batch",
    "FilterExpr",
    "FilterPy",
    "FilterPyBatch",
    "MapExpr",
    "MapPy",
    "MapPyBatch",
    "Select",
    "batch",
    "filter_expr",
    "filter_py",
    "filter_py_batch",
    "map_expr",
    "map_py",
    "map_py_batch",
    "select",
]