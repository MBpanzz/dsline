"""Python API for dsline."""

try:
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
        PyPipeline as Pipeline,
        SequenceMismatchError,
        ShmChannel,
        __version__,
    )
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "dsline native extension is not built. Install with maturin or pip before importing."
    ) from exc

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
    "__version__",
]
