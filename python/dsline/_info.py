import json
import platform
from typing import Any


def get_info() -> dict[str, Any]:
    import dsline

    return {
        "name": "dsline",
        "version": dsline.__version__,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "channel_backend": "inprocess-prototype",
        "storage_backends": ["memory", "file"],
        "zero_copy_alloc_publish": "unavailable",
    }


def format_info(info: dict[str, Any], json_output: bool) -> str:
    if json_output:
        return json.dumps(info, sort_keys=True)
    return "\n".join(f"{key}: {format_value(value)}" for key, value in info.items())


def format_value(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    return str(value)
