"""`dsline shm` — named-channel region management.

Named channels on Windows live in paging-file sections (no backing file,
destroyed with the last handle); everywhere else they live as `dsline-*.bin`
region files in the platform runtime directory. These commands manage the
runtime directory and its region files.

The name → file mapping mirrors `dsline-shm` exactly (FNV-1a checksum32 of
the UTF-8 name): `dsline-<8 hex digits>.bin`.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

_FNV_OFFSET = 0x811C_9DC5
_FNV_PRIME = 0x0100_0193


def _checksum32(data: bytes) -> int:
    """FNV-1a 32-bit, mirroring dsline-core checksum32."""
    value = _FNV_OFFSET
    for byte in data:
        value ^= byte
        value = (value * _FNV_PRIME) & 0xFFFF_FFFF
    return value


def runtime_dir() -> str:
    """The runtime directory holding named-channel region files."""
    if os.name == "nt":
        base = os.environ.get("LOCALAPPDATA") or os.environ.get("TEMP") or os.getcwd()
    else:
        base = os.environ.get("XDG_RUNTIME_DIR") or os.environ.get("TMPDIR") or os.getcwd()
    return os.path.join(base, "dsline", "shm")


def region_path(name: str) -> str:
    """The region file path a named channel resolves to."""
    digest = _checksum32(name.encode("utf-8"))
    return os.path.join(runtime_dir(), f"dsline-{digest:08x}.bin")


def _entries() -> list[dict[str, Any]]:
    directory = runtime_dir()
    try:
        names = os.listdir(directory)
    except OSError:
        return []
    entries = []
    for name in names:
        if not name.startswith("dsline-") or not name.endswith(".bin"):
            continue
        path = os.path.join(directory, name)
        try:
            stat = os.stat(path)
        except OSError:
            continue
        entries.append(
            {
                "file": name,
                "bytes": stat.st_size,
                "mtime": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            }
        )
    return entries


def list_regions(json_output: bool) -> str:
    import json

    entries = _entries()
    if json_output:
        return json.dumps(entries, sort_keys=True)
    if not entries:
        return "(no region files in " + runtime_dir() + ")"
    lines = [f"{entry['file']}  {entry['bytes']:>9} bytes  {entry['mtime']}" for entry in entries]
    return "\n".join(lines)


def inspect(name: str, json_output: bool) -> str:
    import json

    path = region_path(name)
    try:
        stat = os.stat(path)
    except OSError:
        return json.dumps({"name": name, "exists": False}) if json_output else (
            f"channel '{name}' has no region file"
        )
    info = {
        "name": name,
        "path": path,
        "exists": True,
        "bytes": stat.st_size,
        "mtime": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
    }
    if json_output:
        return json.dumps(info, sort_keys=True)
    return "\n".join(f"{key}: {value}" for key, value in info.items())


def _parse_age(text: str) -> timedelta:
    """Parse a duration like `90s`, `30m`, `2h`, `1d`."""
    text = text.strip().lower()
    if text.endswith("s"):
        return timedelta(seconds=float(text[:-1]))
    if text.endswith("m"):
        return timedelta(minutes=float(text[:-1]))
    if text.endswith("h"):
        return timedelta(hours=float(text[:-1]))
    if text.endswith("d"):
        return timedelta(days=float(text[:-1]))
    raise ValueError(f"invalid duration '{text}' (use e.g. 90s, 30m, 2h, 1d)")


def cleanup(older_than: str, json_output: bool) -> str:
    import json

    age = _parse_age(older_than)
    now = datetime.now(tz=timezone.utc)
    removed: list[str] = []
    for entry in _entries():
        mtime = datetime.fromisoformat(entry["mtime"])
        if now - mtime > age:
            path = os.path.join(runtime_dir(), entry["file"])
            try:
                os.remove(path)
                removed.append(entry["file"])
            except OSError:
                pass
    if json_output:
        return json.dumps({"removed": removed}, sort_keys=True)
    if not removed:
        return "nothing to clean up"
    return "\n".join(removed)


def remove(name: str, json_output: bool) -> str:
    import json

    path = region_path(name)
    try:
        os.remove(path)
        removed = True
    except OSError:
        removed = False
    if json_output:
        return json.dumps({"name": name, "removed": removed}, sort_keys=True)
    if removed:
        return f"removed {path}"
    return f"no region file for '{name}' (windows named channels use sections and leave no files)"


def dir_path(json_output: bool) -> str:
    import json

    path = runtime_dir()
    if json_output:
        return json.dumps({"dir": path}, sort_keys=True)
    return path